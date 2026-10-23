//! forge-query: the read path (M2). SQL over the `spans` table (DataFusion, `SpansTableProvider` +
//! `MergeVersionsExec`) and two fast paths that do not use DataFusion: the trace tree and the runs list.
//!
//! Every path takes the memtable snapshot BEFORE any metastore call (invariant 1), merges every version of a key
//! with `forge_model::merge` (latest non-null wins, tombstones dropped) and only then applies non-key predicates
//! (invariant 4). `total_tokens` is the effective total (decision 15) on every path.
pub mod derived;
pub mod filters;
pub mod json;
pub mod merge_exec;
pub mod provider;
pub mod runs;
pub mod scan;
pub mod session;
pub mod sql;
#[doc(hidden)]
pub mod testing;
pub mod trace;
pub mod tree;

use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation};
use forge_meta::Metastore;
use forge_model::MemtableView;
use forge_model::config::QueryConfig;
use forge_model::error::{Classify, ErrorKind, Unimplemented};
use forge_model::merge::Cancel;
use forge_storage::SegmentStore;
use provider::SpansTableProvider;
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

/// Unwraps `External` errors raised by our own code (metastore, storage, the merge's `Cancelled`).
impl From<DataFusionError> for QueryError {
    fn from(e: DataFusionError) -> Self {
        let DataFusionError::External(b) = e else {
            return Self::DataFusion(e);
        };
        let b = match b.downcast::<forge_meta::MetaError>() {
            Ok(m) => return Self::Meta(*m),
            Err(b) => b,
        };
        let b = match b.downcast::<forge_storage::StorageError>() {
            Ok(s) => return Self::Storage(*s),
            Err(b) => b,
        };
        match b.downcast::<forge_model::error::ModelError>() {
            Ok(m) => Self::Model(*m),
            Err(b) => Self::DataFusion(DataFusionError::External(b)),
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

pub struct QueryEngine {
    pub(crate) meta: Arc<dyn Metastore>,
    pub(crate) store: Arc<dyn SegmentStore>,
    pub(crate) mem: Arc<dyn MemtableView>,
    pub(crate) provider: Arc<SpansTableProvider>,
    pub(crate) cfg: QueryConfig,
    pub(crate) sql_slots: Arc<tokio::sync::Semaphore>,
    /// DataFusion `target_partitions` (default: the CPU count).
    pub target_partitions: usize,
    /// The `Cancel` of the latest fast-path call (test hook).
    last_cancel: Mutex<Option<Cancel>>,
    /// Test hook: the trace path takes the memtable snapshot AFTER `locate_trace` (the wrong order of
    /// invariant 1), to prove the mid-query-flush test can fail.
    #[doc(hidden)]
    pub snapshot_after_list: bool,
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
        let provider = Arc::new(SpansTableProvider {
            meta: meta.clone(),
            segments: store.clone(),
            memtable: mem.clone(),
        });
        Self {
            meta,
            store,
            mem,
            provider,
            cfg,
            sql_slots: Arc::new(tokio::sync::Semaphore::new(slots)),
            target_partitions: cpus,
            last_cancel: Mutex::default(),
            snapshot_after_list: false,
        }
    }

    /// `fut` under a deadline: planning (metastore calls), execution and JSON writing (D8).
    pub(crate) async fn deadline<T>(
        &self,
        d: Duration,
        fut: impl Future<Output = Result<T, QueryError>>,
    ) -> Result<T, QueryError> {
        tokio::time::timeout(d, fut).await.map_err(|_| QueryError::Timeout(d))?
    }

    /// All result batches of a read-only SQL query (tests and tools).
    pub async fn sql_batches(&self, sql: &str) -> Result<Vec<RecordBatch>, QueryError> {
        let ctx = self.session()?;
        self.deadline(self.cfg.timeout, async {
            let df = ctx.sql_with_options(sql, session::read_only()).await?;
            Ok(df.collect().await?)
        })
        .await
    }

    /// A fresh cancellation flag for a fast-path call (D8).
    pub(crate) fn new_cancel(&self) -> Cancel {
        let c = Cancel::new();
        *self.last_cancel.lock().unwrap_or_else(|e| e.into_inner()) = Some(c.clone());
        c
    }

    #[doc(hidden)]
    pub fn last_cancel(&self) -> Option<Cancel> {
        self.last_cancel.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Fast paths (not behind the SQL semaphore) reserve their merge input against a per-call pool of
    /// `query.memory_limit` (D15): an oversized trace fails like an oversized query.
    pub(crate) fn fast_reservation(
        &self,
        name: &str,
        batches: &[RecordBatch],
    ) -> Result<MemoryReservation, QueryError> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(self.cfg.memory_limit.as_u64() as usize));
        let r = MemoryConsumer::new(name).register(&pool);
        r.try_grow(batches.iter().map(|b| b.get_array_memory_size()).sum())?;
        Ok(r)
    }
}

pub(crate) fn ext<E: std::error::Error + Send + Sync + 'static>(e: E) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}
