#![allow(unused_imports)]
use crate::pv::*;
// ------------------------------------------------------------------ MergeVersionsExec
/// Blocking per-partition merge over forge_model::merge (latest non-null wins, Tombstones::Drop).
/// Requires hash(project_id, trace_id) input so every version of a key is in one partition; if the input is
/// not so partitioned it gathers ALL input partitions into one output partition (always correct).
#[derive(Debug)]
pub struct MergeVersionsExec {
    input: Arc<dyn ExecutionPlan>,
    per_partition: bool,
    props: Arc<PlanProperties>,
}

fn hash_exprs(schema: &Schema) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    PARTITION_COLUMNS
        .iter()
        .map(|n| Ok(Arc::new(Column::new(n, schema.index_of(n)?)) as Arc<dyn PhysicalExpr>))
        .collect()
}

impl MergeVersionsExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let schema = input.schema();
        let want = hash_exprs(&schema)?;
        // Hash on (project_id, trace_id) or a subset, or one partition: every version of a key is in one
        // input partition, so merge per partition. Otherwise gather everything into partition 0.
        let p = input.output_partitioning();
        let ok = p.satisfaction(&Distribution::KeyPartitioned(want), input.equivalence_properties(), true)
            != PartitioningSatisfaction::NotSatisfied;
        let (per_partition, part) = if ok {
            (true, p.clone())
        } else {
            (false, Partitioning::UnknownPartitioning(1))
        };
        let props = PlanProperties::new(
            EquivalenceProperties::new(schema),
            part,
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Ok(Self { input, per_partition, props: Arc::new(props) })
    }
}

impl DisplayAs for MergeVersionsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MergeVersionsExec: keys=[project_id, trace_id, span_id], tombstones=drop")
    }
}

impl ExecutionPlan for MergeVersionsExec {
    fn name(&self) -> &str {
        "MergeVersionsExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::KeyPartitioned(hash_exprs(&self.input.schema()).unwrap())]
    }
    // NOTE: gather_filters_for_pushdown is NOT overridden: the default marks every parent filter unsupported,
    // which is what keeps FilterExec / TopK dynamic filters above the merge (decision 5).
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _o: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("MergeVersionsExec expects 1 child");
        }
        Ok(Arc::new(Self::try_new(children.swap_remove(0))?))
    }
    #[allow(deprecated)]
    fn with_new_children(self: Arc<Self>, c: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(c, ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute))
    }
    fn execute(&self, partition: usize, ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let parts: Vec<usize> = if self.per_partition {
            vec![partition]
        } else if partition == 0 {
            (0..self.input.output_partitioning().partition_count()).collect()
        } else {
            return exec_err!("MergeVersionsExec has 1 output partition, got {partition}");
        };
        let inputs = parts
            .into_iter()
            .map(|p| self.input.execute(p, ctx.clone()))
            .collect::<Result<Vec<_>>>()?;
        let schema = self.schema();
        let batch_size = ctx.session_config().batch_size();
        let reservation =
            MemoryConsumer::new(format!("MergeVersionsExec[{partition}]")).register(ctx.memory_pool());
        let s2 = schema.clone();
        let cancel = Cancel::new();
        let guard = CancelOnDrop(cancel.clone());           // moved into the output stream below
        let fut = async move {
            let mut batches = Vec::new();
            for mut s in inputs {                            // async drain: cancelled by drop
                while let Some(b) = s.next().await { let b = b?; reservation.try_grow(b.get_array_memory_size())?; batches.push(b); }
            }
            let c = cancel.clone();
            let merged = tokio::task::spawn_blocking(move || merge_versions_projected(&s2, &batches, Tombstones::Drop, 64 << 20, &c))
                .await.map_err(ext)?.map_err(ext)?;
            reservation.try_grow(merged.iter().map(|b| b.get_array_memory_size()).sum())?;
            let out: Vec<Result<RecordBatch>> = merged.iter()
                .flat_map(|b| (0..b.num_rows()).step_by(batch_size).map(|o| Ok(b.slice(o, batch_size.min(b.num_rows() - o)))))
                .collect();
            Ok::<_, DataFusionError>((out, reservation, guard))
        };
        let stream = futures::stream::once(fut).map_ok(move |(out, r, g)| {
            let keep = (r, g);                           // reservation + cancel guard live until the stream is dropped
            futures::stream::iter(out).inspect(move |_| { let _ = &keep; })
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream.try_flatten())))
    }
}

