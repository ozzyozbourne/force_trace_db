//! forge-meta: the catalog of projects, segments, trace locations and WAL checkpoints.

use async_trait::async_trait;
use forge_model::catalog::{Level, SegmentId, SegmentMeta, TraceLocation, TraceRange};
use forge_model::config::{MetastoreConfig, split_scheme};
use forge_model::error::{Classify, ErrorKind, Unimplemented};
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
    #[error("invalid metastore url {url:?}: {msg}")]
    Config { url: String, msg: String },
    #[error("cannot create {}: {source}", path.display())]
    CreateDir { path: PathBuf, source: std::io::Error },
    #[error("{0}")]
    InvalidInput(String),
    #[error(transparent)]
    Unimplemented(#[from] Unimplemented),
    #[error("blocking task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}
pub type MetaResult<T> = Result<T, MetaError>;

impl Classify for MetaError {
    fn kind(&self) -> ErrorKind {
        match self {
            Self::Conflict(_) => ErrorKind::Conflict,
            Self::InvalidInput(_) => ErrorKind::InvalidInput,
            Self::Unimplemented(e) => e.kind(),
            // Another process held the database past busy_timeout: retry later.
            Self::Sqlite(rusqlite::Error::SqliteFailure(f, _))
                if matches!(
                    f.code,
                    rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                ) =>
            {
                ErrorKind::Transient
            }
            _ => ErrorKind::Internal,
        }
    }
    fn message(&self) -> String {
        match self {
            Self::Conflict(m) | Self::InvalidInput(m) => m.clone(),
            other => other.to_string(),
        }
    }
}

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

impl Project {
    /// The one place new-project defaults live: `name` defaults to the id, no API key, retention must be > 0.
    pub fn new(
        id: ProjectId,
        name: Option<String>,
        retention_days: Option<u32>,
        created_at_ms: i64,
    ) -> MetaResult<Self> {
        if retention_days == Some(0) {
            return Err(MetaError::InvalidInput("retention_days must be > 0".into()));
        }
        Ok(Self {
            name: name.unwrap_or_else(|| id.to_string()),
            id,
            api_key_hash: None,
            retention_days,
            created_at_ms,
        })
    }
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
    /// Stale commits insert nothing (invariant 8): when `flushed_wal_seq` <= the node's stored checkpoint, the
    /// segments and trace ranges are skipped (their objects become orphans for GC), only the checkpoint upsert
    /// runs, and the result is Ok. So an identical retry after an ambiguous error (timeout, cancelled future) or
    /// a re-flush of already-committed rows is safe. Otherwise, a segment already committed with the same id,
    /// path, project, level, partition, row count and byte size is skipped; an existing id with other metadata
    /// is `Conflict`. The checkpoint (flushed_seq, max_version) never moves backwards.
    /// `InvalidInput` for trace ranges that do not cover the segment's rows exactly (overlap, gap, past the
    /// end), a segment id listed twice in one commit, or u64 values > i64::MAX.
    async fn commit_flush(&self, c: FlushCommit) -> MetaResult<()>;
    async fn wal_checkpoint(&self, node_id: &str) -> MetaResult<Option<WalCheckpoint>>;
    /// max(segments.max_version, wal_checkpoints.max_version); 0 when empty.
    async fn max_version(&self) -> MetaResult<u64>;
    async fn get_segment(&self, id: SegmentId) -> MetaResult<Option<SegmentMeta>>;
    async fn list_segments(&self, f: &SegmentFilter) -> MetaResult<Vec<SegmentMeta>>;
    /// Visible segments holding the trace, with row ranges; ordered by segment id.
    async fn locate_trace(&self, project: &ProjectId, trace: TraceId) -> MetaResult<Vec<TraceLocation>>;
    /// Batched `locate_trace`: visible segments per trace (traces with no rows are absent). One read; chunked IN.
    async fn locate_traces(
        &self,
        project: &ProjectId,
        traces: &[TraceId],
    ) -> MetaResult<std::collections::HashMap<TraceId, Vec<TraceLocation>>>;
}

/// Read-only open (`sqlite://path` only): no create, no migrations, no journal_mode change.
pub async fn open_read_only(cfg: &MetastoreConfig) -> MetaResult<Arc<dyn Metastore>> {
    let url = cfg.url.clone();
    let path = match split_scheme(&url) {
        Some(("sqlite", p)) if !p.is_empty() => PathBuf::from(p),
        _ => {
            return Err(MetaError::Config { url, msg: "read-only open needs sqlite://<path>".into() });
        }
    };
    let m = tokio::task::spawn_blocking(move || {
        if !path.is_file() {
            return Err(MetaError::Config { url, msg: format!("no metastore at {}", path.display()) });
        }
        SqliteMetastore::open_read_only(&path)
    })
    .await??;
    Ok(Arc::new(m))
}

/// `sqlite::memory:` | `sqlite://<path>` (relative to the cwd unless it starts with `/`) | `postgres://` (later).
pub async fn open(cfg: &MetastoreConfig) -> MetaResult<Arc<dyn Metastore>> {
    let url = cfg.url.as_str();
    let config_err = |msg: &str| MetaError::Config {
        url: url.to_string(),
        msg: msg.to_string(),
    };
    let path = match split_scheme(url) {
        _ if url == "sqlite::memory:" => None,
        Some(("sqlite", p)) if !p.is_empty() => Some(PathBuf::from(p)),
        Some(("postgres" | "postgresql", _)) => return Err(config_err("postgres metastore is not supported yet")),
        _ => return Err(config_err("expected sqlite://<path> or sqlite::memory:")),
    };
    // All file work (mkdir, open, migrate) happens on the blocking pool.
    let m = tokio::task::spawn_blocking(move || match path {
        None => SqliteMetastore::open_in_memory(),
        Some(path) => {
            if let Some(parent) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent).map_err(|source| MetaError::CreateDir {
                    path: parent.to_path_buf(),
                    source,
                })?;
            }
            SqliteMetastore::open_path(&path)
        }
    })
    .await??;
    Ok(Arc::new(m))
}
