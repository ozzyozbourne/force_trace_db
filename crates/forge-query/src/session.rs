use crate::{QueryEngine, QueryError};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SQLOptions;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{GreedyMemoryPool, TrackConsumersPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use forge_model::error::{Classify, ErrorKind};
use std::sync::Arc;

impl QueryEngine {
    /// Per-query SessionContext (PV §1.3) with `spans` registered.
    pub fn session(&self) -> Result<SessionContext, QueryError> {
        self.session_with(None)
    }

    pub(crate) fn session_with(&self, permit: Option<Arc<QueryPermit>>) -> Result<SessionContext, QueryError> {
        let pool = TrackConsumersPool::new(
            GreedyMemoryPool::new(self.cfg.memory_limit.as_u64() as usize),
            std::num::NonZeroUsize::new(5).unwrap(),
        );
        let rt = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(pool))
            .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled))
            .build_arc()?;
        let mut cfg = SessionConfig::new()
            .with_target_partitions(self.target_partitions)
            .with_information_schema(false);
        cfg.options_mut().optimizer.repartition_file_scans = false;
        if let Some(p) = permit {
            cfg = cfg.with_extension(p);
        }
        let ctx = SessionContext::new_with_config_rt(cfg, rt);
        ctx.register_table("spans", self.provider.clone())?;
        Ok(ctx)
    }
}

/// The `/v1/sql` concurrency slot, as a `SessionConfig` extension: `MergeVersionsExec` moves it into its blocking
/// merge, so a slot frees only once the CPU work has stopped (D8). Absent for `sql_batches`.
#[derive(Debug)]
pub struct QueryPermit(pub tokio::sync::OwnedSemaphorePermit);

pub fn read_only() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

/// FP §3.3, after `find_root()`.
pub fn classify_datafusion(e: &DataFusionError) -> ErrorKind {
    use arrow::error::ArrowError as A;
    match e {
        DataFusionError::SQL(..)
        | DataFusionError::Plan(_)
        | DataFusionError::SchemaError(..)
        | DataFusionError::NotImplemented(_)
        | DataFusionError::ResourcesExhausted(_) => ErrorKind::InvalidInput,
        DataFusionError::ArrowError(a, _) => match a.as_ref() {
            A::CastError(_) | A::ParseError(_) | A::DivideByZero | A::InvalidArgumentError(_) | A::ComputeError(_) => {
                ErrorKind::InvalidInput
            }
            _ => ErrorKind::Internal,
        },
        DataFusionError::External(b) => {
            if let Some(q) = b.downcast_ref::<QueryError>() {
                q.kind()
            } else if let Some(s) = b.downcast_ref::<forge_storage::StorageError>() {
                s.kind()
            } else if let Some(m) = b.downcast_ref::<forge_meta::MetaError>() {
                m.kind()
            } else if let Some(m) = b.downcast_ref::<forge_model::error::ModelError>() {
                m.kind()
            } else {
                ErrorKind::Internal
            }
        }
        _ => ErrorKind::Internal,
    }
}
