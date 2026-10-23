//! Test wrappers around the metastore and the index reader, plus re-exports of the segment-store wrappers and the
//! hand-made L0 flush, and the search paging loop (used by forge-query and forge-server tests; feature `test-util`).
use crate::runs::{Pos, RunsPage};
use crate::search::SearchRequest;
use crate::{FtsIndexReader, QueryEngine};
use arrow::array::{Array, AsArray, RecordBatch};
use async_trait::async_trait;
use forge_index::{FtsExpr, FtsHits, IndexReader, IndexResult};
pub use forge_ingest::testing::write_flush;
use forge_meta::{
    CompactionCommit, CompactionJob, FlushCommit, JobRequest, MetaError, MetaResult, Metastore, Project, SegmentFilter,
    WalCheckpoint,
};
use forge_model::catalog::{HourBucket, JobStatus, SegmentId, SegmentMeta, TraceLocation, TraceRange};
use forge_model::ids::{ProjectId, SpanId, TraceId};
use forge_model::row::SpanRow;
use forge_model::schema::col;
use forge_model::testing::batch_from_rows;
use forge_storage::SegmentStore;
pub use forge_storage::testing::{HookedSegmentStore, RecordingSegmentStore, memory_store};
use futures::future::BoxFuture;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The real `.fts` reader; segments in `hidden` are reported as sidecar-less (brute force), and `edit` changes the
/// segment metadata the reader sees (a wrong size or row count).
#[derive(Debug)]
pub struct HidingIndexReader {
    inner: FtsIndexReader,
    pub hidden: Mutex<HashSet<SegmentId>>,
    pub edit: Mutex<Option<(SegmentId, fn(&mut SegmentMeta))>>,
}

impl HidingIndexReader {
    pub fn new(store: Arc<dyn object_store::ObjectStore>) -> Self {
        Self {
            inner: FtsIndexReader::new(store),
            hidden: Mutex::default(),
            edit: Mutex::default(),
        }
    }
}

#[async_trait]
impl IndexReader for HidingIndexReader {
    async fn search(&self, seg: &SegmentMeta, e: &FtsExpr) -> IndexResult<FtsHits> {
        if self.hidden.lock().unwrap().contains(&seg.id) {
            return Ok(FtsHits::NoIndex);
        }
        let mut seg = seg.clone();
        if let Some((id, f)) = *self.edit.lock().unwrap()
            && id == seg.id
        {
            f(&mut seg);
        }
        self.inner.search(&seg, e).await
    }
}

/// Versions 1, 2, ... in order.
pub fn stamp_versions<'a>(rows: impl IntoIterator<Item = &'a mut SpanRow>) {
    for (v, r) in (1..).zip(rows) {
        r.version = v;
    }
}

/// One `write_flush` per flush (sequence i + 1, max version = the rows so far), flush `i` in `hour(i)`.
pub async fn write_flushes(
    meta: &dyn Metastore,
    store: Arc<dyn SegmentStore>,
    flushes: &[Vec<SpanRow>],
    hour: impl Fn(usize) -> HourBucket,
    node_id: &str,
    max_chunk: usize,
) -> Vec<SegmentMeta> {
    let (mut segs, mut max_version) = (Vec::new(), 0);
    for (i, f) in flushes.iter().enumerate() {
        max_version += f.len() as u64;
        let b = batch_from_rows(f);
        segs.extend(
            write_flush(
                meta,
                store.clone(),
                &b,
                hour(i),
                node_id,
                max_chunk,
                i as u64 + 1,
                max_version,
            )
            .await,
        );
    }
    segs
}

/// The (start, trace, span) of every row (rows of a runs or search page: start is never null).
pub fn positions(b: &RecordBatch) -> Vec<Pos> {
    let s = b.schema();
    let st = b
        .column(s.index_of(col::START_TIME).unwrap())
        .as_primitive::<arrow::datatypes::TimestampNanosecondType>();
    let t = b.column(s.index_of(col::TRACE_ID).unwrap()).as_binary::<i32>();
    let sp = b.column(s.index_of(col::SPAN_ID).unwrap()).as_binary::<i32>();
    (0..b.num_rows())
        .map(|i| {
            assert!(st.is_valid(i), "a page row without a start");
            (
                st.value(i),
                TraceId::from_slice(t.value(i)).unwrap(),
                SpanId::from_slice(sp.value(i)).unwrap(),
            )
        })
        .collect()
}

/// `/v1/search` pages of `req(cursor)`, following `next_cursor` until it is null or `max_pages` were read.
pub async fn search_pages(
    e: &QueryEngine,
    req: impl Fn(Option<String>) -> SearchRequest,
    max_pages: usize,
) -> Vec<RunsPage> {
    let (mut out, mut cursor) = (Vec::new(), None);
    while out.len() < max_pages {
        let r = req(cursor.take());
        let page = e.search(&r).await.unwrap();
        cursor = page.next_cursor.map(|c| r.encode_cursor(&c));
        out.push(page);
        if cursor.is_none() {
            break;
        }
    }
    out
}

type Hook = Box<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

/// Delegates to `inner`; after each `list_segments` / `locate_trace` / `locate_traces` / `purge_candidates` (a GC
/// pass) it computes the real result, then awaits `hook` before returning it.
pub struct HookedMetastore {
    inner: Arc<dyn Metastore>,
    hook: Hook,
    /// `commit_compaction`: (run the real commit first?, the error every call returns).
    commit: Option<(bool, fn() -> MetaError)>,
    commit_calls: AtomicUsize,
    /// The first this many `fail_compaction` calls return the commit error without running.
    failing_fails: AtomicUsize,
    /// Awaited after each real `begin_compaction`, before it returns.
    after_begin: Option<Hook>,
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
            failing_fails: AtomicUsize::new(0),
            after_begin: None,
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
            failing_fails: AtomicUsize::new(0),
            after_begin: None,
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
            failing_fails: AtomicUsize::new(0),
            after_begin: None,
        }
    }
    /// Every `begin_compaction` runs the real begin, then awaits `hook` before returning its result.
    pub fn after_begin(
        inner: Arc<dyn Metastore>,
        hook: impl Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            after_begin: Some(Box::new(hook)),
            ..Self::slow(inner, Duration::ZERO)
        }
    }
    /// The first `n` `fail_compaction` calls also return the commit error, without running.
    pub fn failing_fails(self, n: usize) -> Self {
        self.failing_fails.store(n, Ordering::SeqCst);
        self
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
    async fn locate_l1_traces(
        &self,
        project: &ProjectId,
        traces: &[TraceId],
        exclude: &std::collections::BTreeSet<SegmentId>,
    ) -> MetaResult<HashMap<TraceId, Vec<TraceLocation>>> {
        self.inner.locate_l1_traces(project, traces, exclude).await
    }
    async fn expire_segments(&self, p: &ProjectId, cutoff_ns: i64, max_hours: usize) -> MetaResult<Vec<SegmentMeta>> {
        self.inner.expire_segments(p, cutoff_ns, max_hours).await
    }
    async fn expire_candidates(&self, p: &ProjectId, cutoff_ns: i64, max_hours: usize) -> MetaResult<Vec<SegmentMeta>> {
        self.inner.expire_candidates(p, cutoff_ns, max_hours).await
    }
    async fn purge_candidates(&self, before_ms: i64, limit: usize) -> MetaResult<Vec<SegmentMeta>> {
        let r = self.inner.purge_candidates(before_ms, limit).await;
        (self.hook)().await;
        r
    }
    async fn purge_stats(&self, before_ms: i64) -> MetaResult<(usize, u64)> {
        self.inner.purge_stats(before_ms).await
    }
    async fn forget_segments(&self, ids: &[SegmentId]) -> MetaResult<usize> {
        self.inner.forget_segments(ids).await
    }
    async fn segment_paths(&self, p: &ProjectId) -> MetaResult<std::collections::HashSet<String>> {
        self.inner.segment_paths(p).await
    }
    async fn begin_compaction(&self, r: JobRequest) -> MetaResult<Option<CompactionJob>> {
        let r = self.inner.begin_compaction(r).await;
        if let Some(h) = &self.after_begin {
            h().await;
        }
        r
    }
    async fn commit_compaction(&self, c: Arc<CompactionCommit>) -> MetaResult<()> {
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
        let failing = self
            .failing_fails
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1));
        if let (Ok(_), Some((_, err))) = (failing, self.commit) {
            return Err(err());
        }
        self.inner.fail_compaction(job_id, error).await
    }
    async fn reset_compactions(&self, keep: &[i64]) -> MetaResult<usize> {
        self.inner.reset_compactions(keep).await
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
    async fn referenced_blobs(&self, p: &ProjectId, paths: &[String]) -> MetaResult<std::collections::HashSet<String>> {
        self.inner.referenced_blobs(p, paths).await
    }
}
