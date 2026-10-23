//! Test wrappers around the metastore, plus re-exports of the segment-store wrappers and the hand-made L0 flush
//! (used by forge-query and forge-server tests; feature `test-util`).
use async_trait::async_trait;
pub use forge_ingest::testing::write_flush;
use forge_meta::{
    CompactionCommit, CompactionJob, FlushCommit, JobRequest, MetaError, MetaResult, Metastore, Project, SegmentFilter,
    WalCheckpoint,
};
use forge_model::catalog::{JobStatus, SegmentId, SegmentMeta, TraceLocation, TraceRange};
use forge_model::ids::{ProjectId, TraceId};
pub use forge_storage::testing::{HookedSegmentStore, RecordingSegmentStore, memory_store};
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

type Hook = Box<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

/// Delegates to `inner`; after each `list_segments` / `locate_trace` / `locate_traces` it computes the real result,
/// then awaits `hook` before returning it.
pub struct HookedMetastore {
    inner: Arc<dyn Metastore>,
    hook: Hook,
    /// `commit_compaction`: (run the real commit first?, the error every call returns).
    commit: Option<(bool, fn() -> MetaError)>,
    commit_calls: AtomicUsize,
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
            commit: None,
            commit_calls: AtomicUsize::new(0),
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
        Self {
            inner,
            hook,
            commit: None,
            commit_calls: AtomicUsize::new(0),
        }
    }
    /// Every `commit_compaction` runs the real commit if `land` (its result is ignored), then returns `err()`
    /// (e.g. an error after a commit that landed; plans/phase-4 D7).
    pub fn on_commit(inner: Arc<dyn Metastore>, land: bool, err: fn() -> MetaError) -> Self {
        Self {
            inner,
            hook: Box::new(|| Box::pin(async {})),
            commit: Some((land, err)),
            commit_calls: AtomicUsize::new(0),
        }
    }
    /// `commit_compaction` calls so far.
    pub fn commit_calls(&self) -> usize {
        self.commit_calls.load(Ordering::SeqCst)
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
    async fn expire_segments(&self, p: &ProjectId, cutoff_ns: i64, max_hours: usize) -> MetaResult<Vec<SegmentMeta>> {
        self.inner.expire_segments(p, cutoff_ns, max_hours).await
    }
    async fn expire_candidates(&self, p: &ProjectId, cutoff_ns: i64, max_hours: usize) -> MetaResult<Vec<SegmentMeta>> {
        self.inner.expire_candidates(p, cutoff_ns, max_hours).await
    }
    async fn purge_candidates(&self, before_ms: i64, limit: usize) -> MetaResult<Vec<SegmentMeta>> {
        self.inner.purge_candidates(before_ms, limit).await
    }
    async fn forget_segments(&self, ids: &[SegmentId]) -> MetaResult<usize> {
        self.inner.forget_segments(ids).await
    }
    async fn segment_paths(&self, p: &ProjectId) -> MetaResult<std::collections::HashSet<String>> {
        self.inner.segment_paths(p).await
    }
    async fn begin_compaction(&self, r: JobRequest) -> MetaResult<Option<CompactionJob>> {
        self.inner.begin_compaction(r).await
    }
    async fn commit_compaction(&self, c: CompactionCommit) -> MetaResult<()> {
        self.commit_calls.fetch_add(1, Ordering::SeqCst);
        match self.commit {
            Some((land, err)) => {
                if land {
                    let _ = self.inner.commit_compaction(c).await;
                }
                Err(err())
            }
            None => self.inner.commit_compaction(c).await,
        }
    }
    async fn fail_compaction(&self, job_id: i64, error: &str) -> MetaResult<JobStatus> {
        self.inner.fail_compaction(job_id, error).await
    }
    async fn reset_compactions(&self) -> MetaResult<usize> {
        self.inner.reset_compactions().await
    }
    async fn segment_traces(&self, id: SegmentId) -> MetaResult<Vec<TraceRange>> {
        self.inner.segment_traces(id).await
    }
    async fn prune_compaction_jobs(&self, before_ms: i64) -> MetaResult<usize> {
        self.inner.prune_compaction_jobs(before_ms).await
    }
    async fn set_retention_days(&self, p: &ProjectId, days: Option<u32>) -> MetaResult<()> {
        self.inner.set_retention_days(p, days).await
    }
    async fn blob_refs(&self, p: &ProjectId) -> MetaResult<std::collections::HashSet<String>> {
        self.inner.blob_refs(p).await
    }
}
