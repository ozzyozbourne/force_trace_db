//! forge-meta: the catalog of projects, segments, trace locations and WAL checkpoints.

use async_trait::async_trait;
use forge_model::catalog::{Level, SegmentId, SegmentMeta, TraceLocation, TraceRange};
use forge_model::config::MetastoreConfig;
use forge_model::ids::{ProjectId, TraceId};
use std::path::PathBuf;
use std::sync::Arc;

pub mod migrations;
pub mod sqlite;
pub use sqlite::SqliteMetastore;

#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("migration {version} failed: {msg}")]
    Migration { version: u32, msg: String },
    #[error("database schema v{db} is newer than this binary (v{binary})")]
    SchemaTooNew { db: u32, binary: u32 },
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("corrupt row in {table}: {msg}")]
    Corrupt { table: &'static str, msg: String },
    #[error("invalid metastore url {0:?}")]
    Config(String),
    #[error("not implemented until {0}")]
    Unimplemented(&'static str),
    #[error("blocking task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}
pub type MetaResult<T> = Result<T, MetaError>;

#[derive(Clone, Debug, PartialEq)]
pub struct Project {
    pub id: ProjectId,
    pub name: String,
    /// sha256 of the bearer key (M6).
    pub api_key_hash: Option<[u8; 32]>,
    /// None = keep forever.
    pub retention_days: Option<u32>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug)]
pub struct NewSegment {
    pub meta: SegmentMeta,
    pub traces: Vec<TraceRange>,
}

/// One flush = one segment per project in the frozen memtable, one transaction.
#[derive(Clone, Debug)]
pub struct FlushCommit {
    pub node_id: String,
    pub segments: Vec<NewSegment>,
    /// WATERMARK: every WAL record with seq <= this is contained in this or an earlier committed
    /// flush (invariant 7). Not simply "the max seq present". Stored as MAX(old, new).
    pub flushed_wal_seq: u64,
    /// Highest version handed out so far (survives a wiped WAL dir). Stored as MAX(old, new).
    pub max_version: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalCheckpoint {
    pub flushed_seq: u64,
    pub max_version: u64,
}

/// Candidate-segment query. Conservative: may return extra segments, never misses one.
#[derive(Clone, Debug, Default)]
pub struct SegmentFilter {
    pub project_id: Option<ProjectId>,
    pub levels: Option<Vec<Level>>,
    /// [start, end) on span start_time; segments with start_time nulls are always returned.
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    /// Default false: only live + compacting.
    pub include_deleted: bool,
    /// Order by max_start_ns DESC NULLS FIRST, id DESC. Early stop must still compare the
    /// remaining segments' max_start_ns (L0 holds late spans of any hour). Else ordered by id.
    pub newest_first: bool,
    /// Applied after all filters.
    pub limit: Option<usize>,
}

#[async_trait]
pub trait Metastore: Send + Sync + std::fmt::Debug + 'static {
    async fn schema_version(&self) -> MetaResult<u32>;
    /// `Conflict` if the id exists.
    async fn create_project(&self, p: Project) -> MetaResult<()>;
    async fn get_project(&self, id: &ProjectId) -> MetaResult<Option<Project>>;
    /// Ordered by id.
    async fn list_projects(&self) -> MetaResult<Vec<Project>>;
    /// One BEGIN IMMEDIATE txn: auto-create projects, insert segments (live) + trace_index, upsert checkpoint.
    async fn commit_flush(&self, c: FlushCommit) -> MetaResult<()>;
    async fn wal_checkpoint(&self, node_id: &str) -> MetaResult<Option<WalCheckpoint>>;
    /// max(segments.max_version, wal_checkpoints.max_version); 0 when empty.
    async fn max_version(&self) -> MetaResult<u64>;
    async fn get_segment(&self, id: SegmentId) -> MetaResult<Option<SegmentMeta>>;
    async fn list_segments(&self, f: &SegmentFilter) -> MetaResult<Vec<SegmentMeta>>;
    /// Visible segments holding the trace, with row ranges; ordered by segment id.
    async fn locate_trace(&self, project: &ProjectId, trace: TraceId) -> MetaResult<Vec<TraceLocation>>;
}

/// `sqlite::memory:` | `sqlite://<path>` (relative to the cwd unless it starts with `/`) | `postgres://` (later).
pub async fn open(cfg: &MetastoreConfig) -> MetaResult<Arc<dyn Metastore>> {
    let url = cfg.url.as_str();
    if url == "sqlite::memory:" {
        let m = tokio::task::spawn_blocking(SqliteMetastore::open_in_memory).await??;
        return Ok(Arc::new(m));
    }
    if let Some(p) = url.strip_prefix("sqlite://").filter(|p| !p.is_empty()) {
        let path = PathBuf::from(p);
        let m = tokio::task::spawn_blocking(move || {
            if let Some(parent) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent)
                    .map_err(|e| MetaError::Config(format!("cannot create {}: {e}", parent.display())))?;
            }
            SqliteMetastore::open_path(&path)
        })
        .await??;
        return Ok(Arc::new(m));
    }
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        return Err(MetaError::Unimplemented("postgres metastore"));
    }
    Err(MetaError::Config(cfg.url.clone()))
}
