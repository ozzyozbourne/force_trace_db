//! forge-query: the read path (M2).
mod pv;
pub mod derived;
pub mod filters;
pub mod json;
pub mod merge_exec;
pub mod provider;
pub mod runs;
pub mod scan;
pub mod session;
pub mod sql;
pub mod trace;
pub mod tree;

use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use forge_meta::Metastore;
use forge_model::MemtableView;
use forge_model::config::QueryConfig;
use forge_model::error::{Classify, ErrorKind, Unimplemented};
use forge_storage::SegmentStore;
use provider::SpansTableProvider;
use std::sync::Arc;

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
    #[error("{0}")]
    Transient(String),
    #[error("query timed out after {0:?}")]
    Timeout(std::time::Duration),
    #[error("{0}")]
    DataFusion(DataFusionError),
    #[error("blocking task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

impl From<DataFusionError> for QueryError {
    fn from(e: DataFusionError) -> Self {
        match e {
            DataFusionError::External(b) => match b.downcast::<forge_meta::MetaError>() {
                Ok(m) => Self::Meta(*m),
                Err(b) => match b.downcast::<forge_storage::StorageError>() {
                    Ok(s) => Self::Storage(*s),
                    Err(b) => match b.downcast::<forge_model::error::ModelError>() {
                        Ok(m) => Self::Model(*m),
                        Err(b) => Self::DataFusion(DataFusionError::External(b)),
                    },
                },
            },
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
            Self::DataFusion(e) => session::classify_datafusion(e.find_root()),
            Self::Join(_) => ErrorKind::Internal,
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
    pub target_partitions: usize,
}

impl QueryEngine {
    pub fn new(meta: Arc<dyn Metastore>, store: Arc<dyn SegmentStore>, mem: Arc<dyn MemtableView>, cfg: QueryConfig) -> Self {
        let cpus = std::thread::available_parallelism().map_or(4, |n| n.get());
        let provider = Arc::new(SpansTableProvider { meta: meta.clone(), segments: store.clone(), memtable: mem.clone() });
        Self { meta, store, mem, provider, cfg, sql_slots: Arc::new(tokio::sync::Semaphore::new((cpus / 2).max(2))), target_partitions: cpus }
    }

    pub(crate) async fn deadline<T>(
        &self,
        d: std::time::Duration,
        fut: impl std::future::Future<Output = Result<T, QueryError>>,
    ) -> Result<T, QueryError> {
        tokio::time::timeout(d, fut).await.map_err(|_| QueryError::Timeout(d))?
    }

    pub async fn sql_batches(&self, sql: &str) -> Result<Vec<RecordBatch>, QueryError> {
        let ctx = self.session()?;
        self.deadline(self.cfg.timeout, async {
            let df = ctx.sql_with_options(sql, session::read_only()).await?;
            Ok(df.collect().await?)
        })
        .await
    }
}
