//! `MergeVersionsExec`: the blocking per-partition version merge (`forge_model::merge`, latest non-null wins,
//! tombstones dropped) with cancellation (D8).
use crate::ext;
use crate::session::QueryPermit;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result, exec_err, plan_err};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PartitioningSatisfaction, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, ReplaceChildrenOptions,
};
use forge_model::merge::{Cancel, CancelOnDrop, Tombstones, merge_versions_projected};
use forge_model::schema::col;
use futures::{StreamExt, TryStreamExt};
use std::fmt;
use std::sync::Arc;

/// Every version of a key lands in one partition when the input is hash-partitioned on these.
const PARTITION_COLUMNS: [&str; 2] = [col::PROJECT_ID, col::TRACE_ID];

/// Requires hash(project_id, trace_id) input so every version of a key is in one partition; if the input is not
/// so partitioned it gathers ALL input partitions into one output partition (always correct).
#[derive(Debug)]
pub(crate) struct MergeVersionsExec {
    input: Arc<dyn ExecutionPlan>,
    per_partition: bool,
    props: Arc<PlanProperties>,
    /// Tests: makes each partition's `Cancel` and remembers the latest one.
    #[cfg(test)]
    new_cancel: fn() -> Cancel,
    #[cfg(test)]
    last_cancel: std::sync::Mutex<Option<Cancel>>,
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
        // Hash on (project_id, trace_id) or a subset, or one partition: merge per partition. Otherwise gather
        // everything into partition 0.
        let p = input.output_partitioning();
        let ok = p.satisfaction(
            &Distribution::KeyPartitioned(want),
            input.equivalence_properties(),
            true,
        ) != PartitioningSatisfaction::NotSatisfied;
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
        Ok(Self {
            input,
            per_partition,
            props: Arc::new(props),
            #[cfg(test)]
            new_cancel: Cancel::new,
            #[cfg(test)]
            last_cancel: Default::default(),
        })
    }
}

impl DisplayAs for MergeVersionsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MergeVersionsExec: keys=[project_id, trace_id, span_id], tombstones=drop"
        )
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
        vec![Distribution::KeyPartitioned(
            hash_exprs(&self.input.schema()).expect("merge columns"),
        )]
    }
    // NOTE: gather_filters_for_pushdown is NOT overridden: the default marks every parent filter unsupported,
    // which is what keeps FilterExec / TopK dynamic filters above the merge (invariant 4).
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
        let batch_size = ctx.session_config().batch_size().max(1);
        let permit = ctx.session_config().get_extension::<QueryPermit>();
        let reservation = MemoryConsumer::new(format!("MergeVersionsExec[{partition}]")).register(ctx.memory_pool());
        let s2 = schema.clone();
        #[cfg(not(test))]
        let cancel = Cancel::new();
        #[cfg(test)]
        let cancel = (self.new_cancel)();
        #[cfg(test)]
        {
            *self.last_cancel.lock().unwrap() = Some(cancel.clone());
        }
        let fut = async move {
            // Owned by the future: dropping the stream at any point sets the flag.
            let guard = CancelOnDrop(cancel.clone());
            let mut batches = Vec::new();
            for mut s in inputs {
                while let Some(b) = s.next().await {
                    let b = b?;
                    reservation.try_grow(b.get_array_memory_size())?;
                    batches.push(b);
                }
            }
            let merged = tokio::task::spawn_blocking(move || {
                let _slot = permit; // the SQL slot is held until the CPU work stops
                merge_versions_projected(&s2, &batches, Tombstones::Drop, 64 << 20, &cancel)
            })
            .await
            .map_err(ext)?
            .map_err(ext)?;
            // The inputs are freed: count the output only.
            reservation.resize(merged.iter().map(|b| b.get_array_memory_size()).sum());
            let out: Vec<Result<RecordBatch>> = merged
                .iter()
                .flat_map(|b| {
                    (0..b.num_rows())
                        .step_by(batch_size)
                        .map(|o| Ok(b.slice(o, batch_size.min(b.num_rows() - o))))
                })
                .collect();
            Ok::<_, DataFusionError>((out, reservation, guard))
        };
        let stream = futures::stream::once(fut).map_ok(|(out, r, g)| {
            let keep = (r, g); // reservation + guard live until the stream ends
            futures::stream::iter(out).inspect(move |_| {
                let _ = &keep;
            })
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream.try_flatten())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::SchemaRef;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
    use forge_model::error::ModelError;
    use forge_model::schema::spans_schema;
    use forge_model::testing::{Rng, batch_from_rows, random_row};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Yields `batches`, counts the batches pulled, then stays pending forever if `hang`.
    #[derive(Debug)]
    struct Input {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        hang: bool,
        pulled: Arc<AtomicUsize>,
    }
    impl PartitionStream for Input {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }
        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            let pulled = self.pulled.clone();
            let s = futures::stream::iter(self.batches.clone()).map(move |b| {
                pulled.fetch_add(1, Ordering::SeqCst);
                Ok(b)
            });
            let s = s.chain(futures::stream::pending().take(usize::from(self.hang)));
            Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), s))
        }
    }

    fn batches() -> Vec<RecordBatch> {
        let mut r = Rng(9);
        let rows: Vec<_> = (1..=400u64)
            .map(|v| random_row(&mut r, "p", v % 5, v % 40, v))
            .collect();
        rows.chunks(50).map(batch_from_rows).collect()
    }

    fn exec(hang: bool) -> (MergeVersionsExec, Arc<AtomicUsize>) {
        let pulled = Arc::new(AtomicUsize::new(0));
        let input = Input {
            schema: spans_schema(),
            batches: batches(),
            hang,
            pulled: pulled.clone(),
        };
        let leaf =
            StreamingTableExec::try_new(spans_schema(), vec![Arc::new(input)], None, vec![], false, None).unwrap();
        (MergeVersionsExec::try_new(Arc::new(leaf)).unwrap(), pulled)
    }

    fn ctx(pool: Arc<dyn MemoryPool>) -> Arc<TaskContext> {
        let rt = RuntimeEnvBuilder::new().with_memory_pool(pool).build_arc().unwrap();
        Arc::new(TaskContext::default().with_runtime(rt))
    }

    /// (a) Dropping the output stream while the input is still pending sets the exec's flag.
    #[tokio::test]
    async fn dropping_the_stream_cancels() {
        let (e, pulled) = exec(true);
        let mut s = e.execute(0, ctx(Arc::new(GreedyMemoryPool::new(1 << 30)))).unwrap();
        let r = tokio::time::timeout(std::time::Duration::from_millis(100), s.next()).await;
        assert!(r.is_err(), "the input never ends");
        assert_eq!(pulled.load(Ordering::SeqCst), 8);
        let c = e.last_cancel.lock().unwrap().clone().unwrap();
        assert!(!c.is_cancelled());
        drop(s);
        assert!(c.is_cancelled());
    }

    /// (b) A cancellation that hits inside the merge (after the input is drained) ends the stream with Cancelled.
    #[tokio::test]
    async fn cancel_inside_the_merge_is_an_error() {
        let (mut e, pulled) = exec(false);
        e.new_cancel = || Cancel::after(1);
        let s = e.execute(0, ctx(Arc::new(GreedyMemoryPool::new(1 << 30)))).unwrap();
        let r: Result<Vec<RecordBatch>> = s.try_collect().await;
        assert_eq!(pulled.load(Ordering::SeqCst), 8, "drained first");
        let err = r.unwrap_err();
        let DataFusionError::External(b) = err else {
            panic!("{err}")
        };
        assert!(
            matches!(b.downcast_ref::<ModelError>(), Some(ModelError::Cancelled)),
            "{b}"
        );
    }

    /// (c) After the merge the reservation holds the output only, not input + output.
    #[tokio::test]
    async fn reservation_is_the_output_size() {
        let (e, _) = exec(false);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
        let mut s = e.execute(0, ctx(pool.clone())).unwrap();
        let first = s.next().await.unwrap().unwrap();
        assert!(first.num_rows() > 0);
        let input: usize = batches().iter().map(|b| b.get_array_memory_size()).sum();
        let out: usize = merge_versions_projected(
            &spans_schema(),
            &batches(),
            Tombstones::Drop,
            64 << 20,
            &Cancel::never(),
        )
        .unwrap()
        .iter()
        .map(|b| b.get_array_memory_size())
        .sum();
        assert_eq!(pool.reserved(), out);
        assert!(out < input, "{out} vs {input}");
        drop(s);
        assert_eq!(pool.reserved(), 0);
    }
}
