//! forge-query: the read path (M2). SQL over the `spans` table (DataFusion, `SpansTableProvider` +
//! `MergeVersionsExec`) and two fast paths that do not use DataFusion: the trace tree and the runs list.
//!
//! Every path takes the memtable snapshot BEFORE any metastore call (invariant 1), merges every version of a key
//! with `forge_model::merge` (latest non-null wins, tombstones dropped) and only then applies non-key predicates
//! (invariant 4). `total_tokens` is the effective total (decision 15) on every path.
mod derived;
mod filters;
mod fts;
mod fts_scan;
pub use fts_scan::FtsIndexReader;
mod json;
mod merge_exec;
mod provider;
mod read;
pub mod runs;
mod scan;
pub mod search;
mod session;
pub mod sql;
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub mod testing;
pub mod trace;
pub mod tree;

use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation, TrackConsumersPool,
};
use forge_meta::Metastore;
use forge_model::MemtableView;
use forge_model::config::QueryConfig;
use forge_model::error::{Classify, ErrorKind, Unimplemented};
use forge_model::merge::{Cancel, CancelOnDrop};
use forge_storage::SegmentStore;
use read::ReadView;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    Model(#[from] forge_model::error::ModelError),
    #[error(transparent)]
    Meta(#[from] forge_meta::MetaError),
    #[error(transparent)]
    Storage(#[from] forge_storage::StorageError),
    #[error(transparent)]
    Index(#[from] forge_index::IndexError),
    #[error(transparent)]
    Unimplemented(#[from] Unimplemented),
    #[error("{0}")]
    InvalidInput(String),
    #[error("{0}")]
    NotFound(String),
    /// "too many concurrent queries".
    #[error("{0}")]
    Transient(String),
    #[error("query timed out after {0:?}")]
    Timeout(Duration),
    #[error("blocking task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("{0}")]
    DataFusion(DataFusionError),
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Our crates' errors that DataFusion carries as `External`: the ONE list behind `From<DataFusionError>` (owned)
/// and `classify_datafusion` (borrowed).
macro_rules! external_errors {
    ($($v:ident($t:ty)),*) => {
        fn from_external(b: BoxError) -> Result<QueryError, BoxError> {
            let b = match b.downcast::<QueryError>() {
                Ok(q) => return Ok(*q),
                Err(b) => b,
            };
            $(let b = match b.downcast::<$t>() {
                Ok(e) => return Ok(QueryError::$v(*e)),
                Err(b) => b,
            };)*
            Err(b)
        }
        pub(crate) fn external_kind(b: &(dyn std::error::Error + Send + Sync + 'static)) -> Option<ErrorKind> {
            $(if let Some(e) = b.downcast_ref::<$t>() {
                return Some(e.kind());
            })*
            None
        }
    };
}
external_errors!(
    Meta(forge_meta::MetaError),
    Storage(forge_storage::StorageError),
    Model(forge_model::error::ModelError),
    Index(forge_index::IndexError)
);

/// Unwraps `External` errors raised by our own code (metastore, storage, the merge's `Cancelled`).
impl From<DataFusionError> for QueryError {
    fn from(e: DataFusionError) -> Self {
        match e {
            DataFusionError::External(b) => {
                from_external(b).unwrap_or_else(|b| Self::DataFusion(DataFusionError::External(b)))
            }
            e => Self::DataFusion(e),
        }
    }
}

impl From<arrow::error::ArrowError> for QueryError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::Model(e.into())
    }
}

impl Classify for QueryError {
    fn kind(&self) -> ErrorKind {
        match self {
            Self::Model(e) => e.kind(),
            Self::Meta(e) => e.kind(),
            Self::Storage(e) => e.kind(),
            Self::Index(e) => e.kind(),
            Self::Unimplemented(e) => e.kind(),
            Self::InvalidInput(_) => ErrorKind::InvalidInput,
            Self::NotFound(_) => ErrorKind::NotFound,
            Self::Transient(_) => ErrorKind::Transient,
            Self::Timeout(_) => ErrorKind::Timeout,
            Self::Join(_) => ErrorKind::Internal,
            Self::DataFusion(e) => session::classify_datafusion(e.find_root()),
        }
    }
    fn message(&self) -> String {
        match self {
            Self::Meta(e) => e.message(),
            Self::DataFusion(e) if matches!(e.find_root(), DataFusionError::ResourcesExhausted(_)) => {
                format!("query exceeded the memory limit: {}", e.find_root())
            }
            other => other.to_string(),
        }
    }
}

/// Test hook: runs on the blocking pool with the task's label and `Cancel`, before the blocking work. Labels:
/// `trace.mem`, `trace` (merge, tree, finish), `runs.mem`, `runs.candidates`, `runs.verify`, `runs.json`.
#[cfg(any(test, feature = "test-util"))]
pub type BlockingHook = Arc<dyn Fn(&str, &Cancel) + Send + Sync>;

/// What the FTS paths did, summed over every query of an engine (tests diff two snapshots; D4's brute-force count).
#[derive(Debug, Default)]
pub(crate) struct FtsCounters(Mutex<FtsCountersSnapshot>);

impl FtsCounters {
    /// Adds `n` to the counter `f` picks.
    pub(crate) fn add(&self, n: u64, f: impl FnOnce(&mut FtsCountersSnapshot) -> &mut u64) {
        *f(&mut self.0.lock().expect("poisoned")) += n;
    }
    pub(crate) fn get(&self) -> FtsCountersSnapshot {
        *self.0.lock().expect("poisoned")
    }
}

/// A copy of the engine's FTS counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FtsCountersSnapshot {
    /// Segment parts answered from a `.fts` sidecar.
    pub index_parts: u64,
    /// Segment parts brute-forced (no sidecar, or an unusable one).
    pub brute_parts: u64,
    /// Rows answered from the index alone (single-source).
    pub exact_rows: u64,
    /// Candidate rows merged and re-checked.
    pub merge_rows: u64,
    /// Segments whose hit keys or brute-force column were scanned.
    pub segments_read: u64,
    /// `/v1/search` hits on start-less rows (verified before their position is known).
    pub unpositioned: u64,
}

impl FtsCountersSnapshot {
    fn zip(self, o: Self, f: fn(u64, u64) -> u64) -> Self {
        Self {
            index_parts: f(self.index_parts, o.index_parts),
            brute_parts: f(self.brute_parts, o.brute_parts),
            exact_rows: f(self.exact_rows, o.exact_rows),
            merge_rows: f(self.merge_rows, o.merge_rows),
            segments_read: f(self.segments_read, o.segments_read),
            unpositioned: f(self.unpositioned, o.unpositioned),
        }
    }
}

impl std::ops::Add for FtsCountersSnapshot {
    type Output = Self;
    fn add(self, o: Self) -> Self {
        self.zip(o, |a, b| a + b)
    }
}

/// `later - earlier`: what happened in between.
impl std::ops::Sub for FtsCountersSnapshot {
    type Output = Self;
    fn sub(self, o: Self) -> Self {
        self.zip(o, |a, b| a - b)
    }
}

pub struct QueryEngine {
    pub(crate) meta: Arc<dyn Metastore>,
    pub(crate) store: Arc<dyn SegmentStore>,
    pub(crate) mem: Arc<dyn MemtableView>,
    pub(crate) cfg: QueryConfig,
    /// `.fts` lookups (default: `FtsIndexReader` over the segment store).
    pub(crate) index: Arc<dyn forge_index::IndexReader>,
    pub(crate) sql_slots: Arc<tokio::sync::Semaphore>,
    pub(crate) fts: Arc<FtsCounters>,
    /// `/v1/search`: segments read per round, and the lookahead window of index lookups (D10).
    pub(crate) read_group: usize,
    pub(crate) prefetch: usize,
    /// DataFusion `target_partitions` (default: the CPU count).
    pub target_partitions: usize,
    #[cfg(any(test, feature = "test-util"))]
    pub blocking_hook: Option<BlockingHook>,
}

/// What a query's envelope holds (`QueryEngine::begin`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueryKind {
    /// `/v1/sql`: waits for a concurrency slot inside the deadline (Transient when it expires); TrackConsumersPool.
    Sql,
    /// `sql_batches` / `session()`: no slot; TrackConsumersPool.
    Tool,
    /// Fast paths: no slot; a plain GreedyMemoryPool of `query.memory_limit` per call (D15).
    Fast,
}

/// One query's deadline, memory pool, concurrency slot and blocking-work wrapper (D8).
pub(crate) struct QueryCtx {
    deadline: tokio::time::Instant,
    timeout: Duration,
    pub(crate) pool: Arc<dyn MemoryPool>,
    pub(crate) permit: Option<tokio::sync::OwnedSemaphorePermit>,
    #[cfg(any(test, feature = "test-util"))]
    hook: Option<BlockingHook>,
}

impl QueryCtx {
    /// `fut` under the deadline: planning (metastore calls), execution and JSON writing.
    pub(crate) async fn run<T>(&self, fut: impl Future<Output = Result<T, QueryError>>) -> Result<T, QueryError> {
        tokio::time::timeout_at(self.deadline, fut)
            .await
            .map_err(|_| QueryError::Timeout(self.timeout))?
    }

    /// Reserves `batches` against the call's pool: an oversized input fails like an oversized query.
    pub(crate) fn reserve(&self, name: &str, batches: &[RecordBatch]) -> Result<MemoryReservation, QueryError> {
        reserve(&self.pool, name, batches)
    }

    /// `f` on the blocking pool under a fresh `Cancel` that dropping this future sets.
    pub(crate) async fn blocking<T: Send + 'static>(
        &self,
        #[cfg_attr(not(any(test, feature = "test-util")), allow(unused_variables))] label: &'static str,
        f: impl FnOnce(&Cancel) -> Result<T, QueryError> + Send + 'static,
    ) -> Result<T, QueryError> {
        let cancel = Cancel::new();
        let _guard = CancelOnDrop(cancel.clone());
        #[cfg(any(test, feature = "test-util"))]
        let hook = self.hook.clone();
        tokio::task::spawn_blocking(move || {
            #[cfg(any(test, feature = "test-util"))]
            if let Some(h) = hook {
                h(label, &cancel);
            }
            f(&cancel)
        })
        .await?
    }
}

impl QueryEngine {
    pub fn new(
        meta: Arc<dyn Metastore>,
        store: Arc<dyn SegmentStore>,
        mem: Arc<dyn MemtableView>,
        cfg: QueryConfig,
    ) -> Self {
        let cpus = std::thread::available_parallelism().map_or(4, |n| n.get());
        let slots = match cfg.sql_concurrency {
            0 => (cpus / 2).max(2),
            n => n,
        };
        Self {
            index: Arc::new(FtsIndexReader::new(store.object_store())),
            meta,
            store,
            mem,
            cfg,
            sql_slots: Arc::new(tokio::sync::Semaphore::new(slots)),
            fts: Arc::default(),
            read_group: 8,
            prefetch: 64,
            target_partitions: cpus,
            #[cfg(any(test, feature = "test-util"))]
            blocking_hook: None,
        }
    }

    /// Replaces the index reader (tests; a caching reader in M5).
    pub fn with_index(mut self, index: Arc<dyn forge_index::IndexReader>) -> Self {
        self.index = index;
        self
    }

    /// `/v1/search` segments per round and lookahead window (defaults 8 and 64; tests use small values).
    pub fn with_search_sizes(mut self, read_group: usize, prefetch: usize) -> Self {
        self.read_group = read_group.max(1);
        self.prefetch = prefetch.max(1);
        self
    }

    pub fn fts_counters(&self) -> FtsCountersSnapshot {
        self.fts.get()
    }

    /// The memtable snapshot, taken now (before any metastore call made through the view).
    pub(crate) fn read_view(&self) -> ReadView {
        ReadView::new(&*self.mem, &self.meta, &self.store)
    }

    /// A query envelope without a slot (`begin` adds the `Sql` slot).
    pub(crate) fn ctx(&self, kind: QueryKind, timeout: Duration) -> QueryCtx {
        let limit = self.cfg.memory_limit.as_u64() as usize;
        let pool: Arc<dyn MemoryPool> = match kind {
            QueryKind::Fast => Arc::new(GreedyMemoryPool::new(limit)),
            QueryKind::Sql | QueryKind::Tool => Arc::new(TrackConsumersPool::new(
                GreedyMemoryPool::new(limit),
                std::num::NonZeroUsize::new(5).unwrap(),
            )),
        };
        QueryCtx {
            deadline: tokio::time::Instant::now() + timeout,
            timeout,
            pool,
            permit: None,
            #[cfg(any(test, feature = "test-util"))]
            hook: self.blocking_hook.clone(),
        }
    }

    /// `ctx`, plus for `Sql` a concurrency slot (`query.sql_concurrency`) awaited inside the deadline.
    pub(crate) async fn begin(&self, kind: QueryKind, timeout: Duration) -> Result<QueryCtx, QueryError> {
        let mut ctx = self.ctx(kind, timeout);
        if kind == QueryKind::Sql {
            let p = tokio::time::timeout_at(ctx.deadline, self.sql_slots.clone().acquire_owned())
                .await
                .map_err(|_| QueryError::Transient("too many concurrent queries".into()))?
                .map_err(|_| QueryError::Transient("query engine closed".into()))?;
            ctx.permit = Some(p);
        }
        Ok(ctx)
    }

    /// All result batches of a read-only SQL query (tests and tools).
    pub async fn sql_batches(&self, sql: &str) -> Result<Vec<RecordBatch>, QueryError> {
        let mut q = self.ctx(QueryKind::Tool, self.cfg.timeout);
        let ctx = self.session_with(&mut q)?;
        q.run(async {
            let df = ctx.sql_with_options(sql, session::read_only()).await?;
            Ok(df.collect().await?)
        })
        .await
    }
}

/// Reserves the size of `batches` against `pool`.
pub(crate) fn reserve(
    pool: &Arc<dyn MemoryPool>,
    name: &str,
    batches: &[RecordBatch],
) -> Result<MemoryReservation, QueryError> {
    let r = MemoryConsumer::new(name).register(pool);
    r.try_grow(batches.iter().map(|b| b.get_array_memory_size()).sum())?;
    Ok(r)
}

pub(crate) fn ext<E: std::error::Error + Send + Sync + 'static>(e: E) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Our crates' errors keep their own kind (and message) across the DataFusion boundary.
    #[test]
    fn external_errors_keep_their_kind() {
        let wrap = |e: BoxError| QueryError::from(DataFusionError::External(e));
        let q = wrap(Box::new(forge_index::IndexError::Query("bad".into())));
        assert!(matches!(q, QueryError::Index(_)), "{q:?}");
        assert_eq!(q.kind(), ErrorKind::InvalidInput);
        let q = wrap(Box::new(QueryError::NotFound("gone".into())));
        assert!(matches!(q, QueryError::NotFound(_)), "{q:?}");
        let df = DataFusionError::External(Box::new(forge_index::IndexError::Query("bad".into())));
        assert_eq!(session::classify_datafusion(&df), ErrorKind::InvalidInput);
    }
}
