#![allow(unused_imports)]
use crate::pv::*;
// ------------------------------------------------------------------ SegmentScanExec
/// Leaf over `SegmentStore::scan`: one partition per (segment, row selection). Prints its projection so
/// EXPLAIN shows exactly which columns are read.
#[derive(Debug)]
pub struct SegmentScanExec {
    store: Arc<dyn SegmentStore>,
    parts: Vec<SegmentPart>,
    cols: Vec<&'static str>,
    filter: Option<vortex::expr::Expression>,
    props: Arc<PlanProperties>,
}

impl SegmentScanExec {
    pub fn new(
        store: Arc<dyn SegmentStore>,
        parts: Vec<SegmentPart>,
        cols: Vec<&'static str>,
        schema: SchemaRef,
        filter: Option<vortex::expr::Expression>,
    ) -> Self {
        let props = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(parts.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self { store, parts, cols, filter, props: Arc::new(props) }
    }
}

impl DisplayAs for SegmentScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let segs: BTreeSet<_> = self.parts.iter().map(|p| p.seg.id).collect();
        write!(f, "SegmentScanExec: segments={}, parts={}, projection=[{}]", segs.len(), self.parts.len(), self.cols.join(", "))?;
        if let Some(x) = &self.filter {
            write!(f, ", filter={x}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for SegmentScanExec {
    fn name(&self) -> &str {
        "SegmentScanExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn replace_children(
        self: Arc<Self>,
        _c: Vec<Arc<dyn ExecutionPlan>>,
        _o: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    #[allow(deprecated)]
    fn with_new_children(self: Arc<Self>, c: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(c, ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute))
    }
    fn execute(&self, partition: usize, _ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let Some(part) = self.parts.get(partition).cloned() else {
            return exec_err!("SegmentScanExec has {} partitions, got {partition}", self.parts.len());
        };
        let req = ScanRequest {
            projection: Some(self.cols.clone()),
            rows: part.rows,
            filter: self.filter.clone(),
            limit: None,
        };
        let store = self.store.clone();
        let s = futures::stream::once(async move {
            let s = store.scan(&part.seg, req).await.map_err(ext)?;
            Ok::<_, DataFusionError>(s.map_err(ext))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), s)))
    }
}
