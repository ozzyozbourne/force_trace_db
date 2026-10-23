use crate::provider::SpansTableProvider;
use crate::{QueryCtx, QueryEngine, QueryError, QueryKind, external_kind};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SQLOptions;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use forge_model::error::{Classify, ErrorKind};
use std::sync::Arc;

impl QueryEngine {
    /// Per-query SessionContext (PV §1.3) with `spans` registered.
    pub fn session(&self) -> Result<SessionContext, QueryError> {
        self.session_with(&mut self.ctx(QueryKind::Tool, self.cfg.timeout))
    }

    /// A session over `q`'s memory pool; `q`'s slot (if any) moves into it as a [`QueryPermit`].
    pub(crate) fn session_with(&self, q: &mut QueryCtx) -> Result<SessionContext, QueryError> {
        let rt = RuntimeEnvBuilder::new()
            .with_memory_pool(q.pool.clone())
            .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled))
            .build_arc()?;
        let mut cfg = SessionConfig::new()
            .with_target_partitions(self.target_partitions)
            .with_information_schema(false);
        cfg.options_mut().optimizer.repartition_file_scans = false;
        if let Some(p) = q.permit.take() {
            cfg = cfg.with_extension(Arc::new(QueryPermit { _slot: p }));
        }
        let ctx = SessionContext::new_with_config_rt(cfg, rt);
        let provider = SpansTableProvider {
            meta: self.meta.clone(),
            segments: self.store.clone(),
            memtable: self.mem.clone(),
        };
        ctx.register_table("spans", Arc::new(provider))?;
        Ok(ctx)
    }
}

/// The `/v1/sql` concurrency slot, as a `SessionConfig` extension: `MergeVersionsExec` moves it into its blocking
/// merge, so a slot frees only once the CPU work has stopped (D8). Absent for `sql_batches`.
#[derive(Debug)]
pub(crate) struct QueryPermit {
    _slot: tokio::sync::OwnedSemaphorePermit,
}

pub(crate) fn read_only() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

/// FP §3.3, after `find_root()`.
pub(crate) fn classify_datafusion(e: &DataFusionError) -> ErrorKind {
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
        DataFusionError::External(b) => match b.downcast_ref::<QueryError>() {
            Some(q) => q.kind(),
            None => external_kind(b.as_ref()).unwrap_or(ErrorKind::Internal),
        },
        _ => ErrorKind::Internal,
    }
}
