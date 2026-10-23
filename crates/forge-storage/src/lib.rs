//! forge-storage: object store construction and Vortex segment files.

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use forge_model::catalog::{HourBucket, Level, SegmentId, SegmentMeta, TraceRange};
use forge_model::error::{Classify, ErrorKind, Unimplemented};
use forge_model::ids::ProjectId;
use futures::stream::BoxStream;
use object_store::ObjectStore;
use std::sync::Arc;

pub mod inspect;
pub mod segment;
pub mod store;
pub use segment::VortexSegmentStore;
#[cfg(any(test, feature = "test-util"))]
pub mod testing;
pub use store::{BuiltStore, DurableLocalFs, build_object_store, delete_all, file_url_path};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("vortex: {0}")]
    Vortex(#[from] vortex::error::VortexError),
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Model(#[from] forge_model::error::ModelError),
    #[error(
        "segment rows must be strictly increasing by (project_id, trace_id, dotted_order NULLS LAST, span_id) \
         within one project, with one merged row per span: {0}"
    )]
    Unsorted(String),
    #[error("{0}")]
    InvalidInput(String),
    #[error("segment has no rows")]
    EmptySegment,
    #[error("invalid storage url {url:?}: {msg}")]
    Config { url: String, msg: String },
    #[error(transparent)]
    Unimplemented(#[from] Unimplemented),
}

impl Classify for StorageError {
    /// Transient is an allow-list (D6): retry with backoff, the flush keeps the memtable frozen.
    fn kind(&self) -> ErrorKind {
        use std::io::ErrorKind as K;
        match self {
            Self::ObjectStore(object_store::Error::Generic { .. } | object_store::Error::JoinError { .. }) => {
                ErrorKind::Transient
            }
            Self::Io(e)
                if matches!(
                    e.kind(),
                    K::TimedOut
                        | K::Interrupted
                        | K::ConnectionReset
                        | K::ConnectionAborted
                        | K::ConnectionRefused
                        | K::BrokenPipe
                        | K::UnexpectedEof
                ) =>
            {
                ErrorKind::Transient
            }
            Self::Model(e) => e.kind(),
            Self::InvalidInput(_) => ErrorKind::InvalidInput,
            Self::Unimplemented(e) => e.kind(),
            _ => ErrorKind::Internal,
        }
    }
}

impl StorageError {
    pub fn is_transient(&self) -> bool {
        self.kind() == ErrorKind::Transient
    }
}

pub type StorageResult<T> = Result<T, StorageError>;
pub type BatchStream = BoxStream<'static, StorageResult<RecordBatch>>;

/// What the caller decides; storage computes the rest of `SegmentMeta` from the rows.
#[derive(Clone, Debug)]
pub struct SegmentSpec {
    pub id: SegmentId,
    pub project_id: ProjectId,
    pub level: Level,
    pub partition_hour: HourBucket,
}

/// `meta.state = Live`, `created_at_ms = now`, `fts_byte_size = None`.
#[derive(Clone, Debug)]
pub struct WrittenSegment {
    pub meta: SegmentMeta,
    pub traces: Vec<TraceRange>,
}

#[derive(Clone, Debug, Default)]
pub enum RowSelection {
    #[default]
    All,
    /// trace_index row range (file row positions).
    Range(std::ops::Range<u64>),
}

/// `rs` sorted by start, with ranges whose gap is at most `max_gap` rows merged (0: overlapping or adjacent only,
/// i.e. exactly the same rows).
pub fn coalesce_ranges(mut rs: Vec<std::ops::Range<u64>>, max_gap: u64) -> Vec<std::ops::Range<u64>> {
    rs.sort_by_key(|r| r.start);
    let mut out: Vec<std::ops::Range<u64>> = Vec::with_capacity(rs.len());
    for r in rs {
        match out.last_mut() {
            Some(l) if r.start <= l.end.saturating_add(max_gap) => l.end = l.end.max(r.end),
            _ => out.push(r),
        }
    }
    out
}

#[derive(Clone, Debug, Default)]
pub struct ScanRequest {
    /// `forge_model::schema::col` names; None = all. Output uses spans_schema types (Utf8, not Utf8View).
    pub projection: Option<Vec<&'static str>>,
    pub rows: RowSelection,
    /// Unbound Vortex expression; storage binds it to the file dtype. KEY-column predicates only when
    /// the result feeds a merge (invariant 4).
    pub filter: Option<vortex::expr::Expression>,
    pub limit: Option<u64>,
}

#[async_trait]
pub trait SegmentStore: Send + Sync + std::fmt::Debug + 'static {
    /// rows: spans_schema, one project, strictly increasing by (project_id, trace_id, dotted_order NULLS LAST,
    /// span_id), one row per span key (versions already merged, invariant 3); checked, else `Unsorted`. Writes one Vortex file (PutMode::Create). A retry with the
    /// same spec after an ambiguous error is safe: an existing object of the same size counts as written.
    /// Invisible until the metastore commit.
    async fn write_segment(&self, spec: SegmentSpec, rows: BatchStream) -> StorageResult<WrittenSegment>;
    /// `InvalidInput` for a row range with start > end or end > `seg.row_count`.
    async fn scan(&self, seg: &SegmentMeta, req: ScanRequest) -> StorageResult<BatchStream>;
    /// The full store stack (later: prefix + fault injection + caching); forge-index reads .fts through it.
    fn object_store(&self) -> Arc<dyn ObjectStore>;
    /// `scan`, collected.
    async fn scan_all(&self, seg: &SegmentMeta, req: ScanRequest) -> StorageResult<Vec<RecordBatch>> {
        use futures::TryStreamExt;
        self.scan(seg, req).await?.try_collect().await
    }
}
