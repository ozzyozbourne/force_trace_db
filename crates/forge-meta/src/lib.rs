//! forge-meta: the catalog of projects, segments, trace locations and WAL checkpoints.

use async_trait::async_trait;
use forge_model::catalog::{JobKind, JobStatus, Level, SegmentId, SegmentMeta, TraceLocation, TraceRange};
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
    #[error("cannot copy {} to {}: {source}", from.display(), to.display())]
    Copy {
        from: PathBuf,
        to: PathBuf,
        source: std::io::Error,
    },
    #[error(
        "{} changed during {attempts} copy attempts (a server is writing it): query the running server with \
         `forge sql --server`",
        path.display()
    )]
    Unstable { path: PathBuf, attempts: u32 },
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

    /// The retention cutoff at `data_now_ns` (`forge_model::catalog::retention_cutoff_ns`); None = keep forever.
    pub fn retention_cutoff_ns(&self, data_now_ns: i64) -> Option<i64> {
        self.retention_days
            .map(|d| forge_model::catalog::retention_cutoff_ns(d, data_now_ns))
    }
}

#[derive(Clone, Debug)]
pub struct NewSegment {
    pub meta: SegmentMeta,
    pub traces: Vec<TraceRange>,
    /// Distinct blob paths (`blobs/<project>/<sha256>`) referenced by the segment's rows
    /// (`forge_model::blob::blob_refs_in`); written to `blob_refs`.
    pub blobs: Vec<String>,
}

/// A compaction job request (plans/phase-4 D1).
#[derive(Clone, Debug)]
pub struct JobRequest {
    pub project_id: ProjectId,
    pub kind: JobKind,
    /// A: an L0 prefix + every visible L1 segment of each folded hour; B: a newest version suffix of one hour.
    pub inputs: Vec<SegmentId>,
}

#[derive(Clone, Debug)]
pub struct CompactionJob {
    pub id: i64,
    pub project_id: ProjectId,
    pub kind: JobKind,
    /// state = Compacting, ordered by min_version.
    pub inputs: Vec<SegmentMeta>,
}

/// Outputs: L1, same project, any hours.
#[derive(Clone, Debug)]
pub struct CompactionCommit {
    pub job_id: i64,
    pub outputs: Vec<NewSegment>,
    /// D15: the commit is refused (Conflict) when the job is older than this, so the orphan sweep
    /// (`orphan_grace` = 2 x this) never deletes an upload that is about to be committed.
    pub max_age_ms: i64,
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
    /// Batched `locate_trace`: visible segments per trace, ordered by segment id; traces with no rows are absent.
    async fn locate_traces(
        &self,
        project: &ProjectId,
        traces: &[TraceId],
    ) -> MetaResult<std::collections::HashMap<TraceId, Vec<TraceLocation>>>;

    // ---- lifecycle (M3): retention, purge, orphan GC ----

    /// Retention (plans/phase-4 D10, data clock): one BEGIN IMMEDIATE txn marks `deleted` (deleted_at_ms = now)
    /// every LIVE L1 segment of the oldest `max_hours` hours of `project` with `partition_hour < floor(cutoff / H)`
    /// that have no compacting segment, and every LIVE L0 segment with no null start_time and
    /// `max_start_ns < cutoff_ns` (`SegmentMeta::expires`). Returns them. trace_index rows stay until
    /// `forget_segments`; queries already ignore deleted segments.
    async fn expire_segments(
        &self,
        project: &ProjectId,
        cutoff_ns: i64,
        max_hours: usize,
    ) -> MetaResult<Vec<SegmentMeta>>;
    /// What `expire_segments` would mark now, without marking (the same SELECT; `forge gc --dry-run`).
    async fn expire_candidates(
        &self,
        project: &ProjectId,
        cutoff_ns: i64,
        max_hours: usize,
    ) -> MetaResult<Vec<SegmentMeta>>;
    /// Deleted segments with `deleted_at_ms < deleted_before_ms`, oldest deletion first, at most `limit`.
    async fn purge_candidates(&self, deleted_before_ms: i64, limit: usize) -> MetaResult<Vec<SegmentMeta>>;
    /// Removes the rows of DELETED segments (others in `ids` are ignored): their trace_index rows in short chunked
    /// txns, then the segments rows. Call only after their objects are gone.
    /// Returns the number of segments rows removed.
    async fn forget_segments(&self, ids: &[SegmentId]) -> MetaResult<usize>;
    /// Object paths (`.vortex`) of every segments row of `project`, any state: the orphan GC's "known" set.
    async fn segment_paths(&self, project: &ProjectId) -> MetaResult<std::collections::HashSet<String>>;
    /// Blob paths referenced by any segments row of `project` (any state: a deleted segment's blobs are kept until
    /// its row is forgotten, i.e. after its objects are gone).
    async fn blob_refs(&self, project: &ProjectId) -> MetaResult<std::collections::HashSet<String>>;
    /// Sets (None = keep forever) a project's retention, creating the project if absent. `InvalidInput` for 0.
    async fn set_retention_days(&self, project: &ProjectId, days: Option<u32>) -> MetaResult<()>;

    // ---- compaction (M3; one process per data dir, plans/phase-4 D2) ----

    /// One IMMEDIATE txn. None when the D1 rules fail or an input is not live; nothing is written then.
    /// `InvalidInput` for no inputs or more than `MAX_JOB_INPUTS`.
    async fn begin_compaction(&self, r: JobRequest) -> MetaResult<Option<CompactionJob>>;
    /// The atomic swap (D6). Ok if the job is already done (idempotent retry). Conflict if it is failed, or older
    /// than `max_age_ms`.
    async fn commit_compaction(&self, c: CompactionCommit) -> MetaResult<()>;
    /// running -> failed, inputs -> live. Returns the job's final status (Done if the commit had landed).
    async fn fail_compaction(&self, job_id: i64, error: &str) -> MetaResult<JobStatus>;
    /// Startup (one process per data dir): every running job -> failed, its inputs -> live. Returns the count.
    async fn reset_compactions(&self) -> MetaResult<usize>;
    /// trace_index of one segment, by row_start.
    async fn segment_traces(&self, id: SegmentId) -> MetaResult<Vec<TraceRange>>;
    /// Deletes done/failed jobs finished before `finished_before_ms`.
    async fn prune_compaction_jobs(&self, finished_before_ms: i64) -> MetaResult<usize>;
}

/// Most inputs `begin_compaction` accepts. The picker's own caps (forge-ingest `MAX_INPUTS`) must stay within it.
pub const MAX_JOB_INPUTS: usize = 64;

/// Read-only open (`sqlite://path` only): no create, no migrations, no journal_mode change. A missing file is a
/// `Config` error and nothing is created.
pub async fn open_read_only(cfg: &MetastoreConfig) -> MetaResult<Arc<dyn Metastore>> {
    let url = cfg.url.clone();
    let path = match split_scheme(&url) {
        Some(("sqlite", p)) if !p.is_empty() => PathBuf::from(p),
        _ => {
            return Err(MetaError::Config {
                url,
                msg: "read-only open needs sqlite://<path>".into(),
            });
        }
    };
    let m = tokio::task::spawn_blocking(move || {
        if !path.is_file() {
            return Err(MetaError::Config {
                url,
                msg: format!("no metastore at {}", path.display()),
            });
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
