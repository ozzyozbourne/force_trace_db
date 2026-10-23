//! Test wrappers around the metastore and the segment store, and a hand-made L0 flush (used by forge-query and
//! forge-server tests; feature `test-util`).
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use forge_meta::{FlushCommit, MetaResult, Metastore, NewSegment, Project, SegmentFilter, WalCheckpoint};
use forge_model::catalog::{HourBucket, Level, SegmentId, SegmentMeta, TraceLocation};
use forge_model::ids::{ProjectId, TraceId};
use forge_model::merge::{Tombstones, merge_versions_chunked, split_chunks_by_project};
use forge_storage::{BatchStream, ScanRequest, SegmentSpec, SegmentStore, StorageResult, WrittenSegment};
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// One L0 flush of `batch` (spans_schema rows, versions already assigned): merge (Keep) -> one segment per project
/// (chunks of at most `max_chunk` bytes) -> one metastore commit.
pub async fn write_flush(
    meta: &dyn Metastore,
    store: &dyn SegmentStore,
    batch: &RecordBatch,
    hour: HourBucket,
    max_chunk: usize,
    seq: u64,
    max_version: u64,
) {
    let mut segments = Vec::new();
    if batch.num_rows() > 0 {
        let merged = merge_versions_chunked(std::slice::from_ref(batch), Tombstones::Keep, max_chunk).unwrap();
        for (p, batches) in split_chunks_by_project(&merged).unwrap() {
            let spec = SegmentSpec {
                id: SegmentId::new(),
                project_id: p,
                level: Level::L0,
                partition_hour: hour,
            };
            let s = futures::stream::iter(batches.into_iter().map(Ok));
            let w = store.write_segment(spec, Box::pin(s)).await.unwrap();
            segments.push(NewSegment {
                meta: w.meta,
                traces: w.traces,
            });
        }
    }
    let c = FlushCommit {
        node_id: "test".into(),
        segments,
        flushed_wal_seq: seq,
        max_version,
    };
    meta.commit_flush(c).await.unwrap();
}

type Hook = Box<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

/// Delegates to `inner`; after each `list_segments` / `locate_trace` / `locate_traces` it computes the real result,
/// then awaits `hook` before returning it.
pub struct HookedMetastore {
    inner: Arc<dyn Metastore>,
    hook: Hook,
}

impl std::fmt::Debug for HookedMetastore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HookedMetastore").field("inner", &self.inner).finish()
    }
}

impl HookedMetastore {
    /// Every read sleeps `d` (deterministic timeouts, including the planning phase).
    pub fn slow(inner: Arc<dyn Metastore>, d: Duration) -> Self {
        Self {
            inner,
            hook: Box::new(move || Box::pin(tokio::time::sleep(d))),
        }
    }
    /// The FIRST read runs `f` (e.g. a flush that commits and releases a memtable) before returning its
    /// pre-hook result.
    pub fn once(inner: Arc<dyn Metastore>, f: impl FnOnce() -> BoxFuture<'static, ()> + Send + 'static) -> Self {
        let f = Mutex::new(Some(f));
        let hook: Hook = Box::new(move || match f.lock().unwrap().take() {
            Some(f) => f(),
            None => Box::pin(async {}),
        });
        Self { inner, hook }
    }
}

#[async_trait]
impl Metastore for HookedMetastore {
    async fn schema_version(&self) -> MetaResult<u32> {
        self.inner.schema_version().await
    }
    async fn create_project(&self, p: Project) -> MetaResult<()> {
        self.inner.create_project(p).await
    }
    async fn get_project(&self, id: &ProjectId) -> MetaResult<Option<Project>> {
        self.inner.get_project(id).await
    }
    async fn list_projects(&self) -> MetaResult<Vec<Project>> {
        self.inner.list_projects().await
    }
    async fn commit_flush(&self, c: FlushCommit) -> MetaResult<()> {
        self.inner.commit_flush(c).await
    }
    async fn wal_checkpoint(&self, node_id: &str) -> MetaResult<Option<WalCheckpoint>> {
        self.inner.wal_checkpoint(node_id).await
    }
    async fn max_version(&self) -> MetaResult<u64> {
        self.inner.max_version().await
    }
    async fn get_segment(&self, id: SegmentId) -> MetaResult<Option<SegmentMeta>> {
        self.inner.get_segment(id).await
    }
    async fn list_segments(&self, f: &SegmentFilter) -> MetaResult<Vec<SegmentMeta>> {
        let r = self.inner.list_segments(f).await;
        (self.hook)().await;
        r
    }
    async fn locate_trace(&self, project: &ProjectId, trace: TraceId) -> MetaResult<Vec<TraceLocation>> {
        let r = self.inner.locate_trace(project, trace).await;
        (self.hook)().await;
        r
    }
    async fn locate_traces(
        &self,
        project: &ProjectId,
        traces: &[TraceId],
    ) -> MetaResult<HashMap<TraceId, Vec<TraceLocation>>> {
        let r = self.inner.locate_traces(project, traces).await;
        (self.hook)().await;
        r
    }
}

/// Delegates to `inner`, records every scan's projection, and optionally sleeps before each scan.
#[derive(Debug)]
pub struct RecordingSegmentStore {
    inner: Arc<dyn SegmentStore>,
    delay: Duration,
    projections: Mutex<Vec<Vec<&'static str>>>,
}

impl RecordingSegmentStore {
    pub fn new(inner: Arc<dyn SegmentStore>) -> Self {
        Self {
            inner,
            delay: Duration::ZERO,
            projections: Mutex::default(),
        }
    }
    pub fn slow(inner: Arc<dyn SegmentStore>, delay: Duration) -> Self {
        Self {
            delay,
            ..Self::new(inner)
        }
    }
    /// The projections scanned since the last call (None = all columns).
    pub fn take_projections(&self) -> Vec<Vec<&'static str>> {
        std::mem::take(&mut self.projections.lock().unwrap())
    }
}

#[async_trait]
impl SegmentStore for RecordingSegmentStore {
    async fn write_segment(&self, spec: SegmentSpec, rows: BatchStream) -> StorageResult<WrittenSegment> {
        self.inner.write_segment(spec, rows).await
    }
    async fn scan(&self, seg: &SegmentMeta, req: ScanRequest) -> StorageResult<BatchStream> {
        let cols = req
            .projection
            .clone()
            .unwrap_or_else(|| forge_model::schema::ALL_COLUMNS.to_vec());
        self.projections.lock().unwrap().push(cols);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        self.inner.scan(seg, req).await
    }
    fn object_store(&self) -> Arc<dyn object_store::ObjectStore> {
        self.inner.object_store()
    }
}
