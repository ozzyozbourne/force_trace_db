//! forge-ingest: the write path (M1): OTLP normalization, WAL, memtable, flush to L0 segments.

use forge_model::error::{Classify, ErrorKind, Unimplemented};

pub mod blob;
pub mod flush;
pub mod memtable;
pub mod normalize;
pub mod pipeline;
pub mod version;
pub mod wal;
pub use pipeline::{IngestTasks, OpenReport, Pipeline};

#[async_trait::async_trait]
pub trait Ingest: Send + Sync + std::fmt::Debug + 'static {
    /// Normalized rows (version ignored). Budget check -> blob offload -> WAL (versions assigned there, in seq
    /// order) -> fsync -> memtable insert -> Ok. Ok = durable AND queryable. Once queued to the WAL thread,
    /// dropping this future does not undo the write (a client retry adds a newer version).
    async fn submit(&self, rows: Vec<forge_model::row::SpanRow>) -> Result<(), IngestError>;
}

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error(transparent)]
    Model(#[from] forge_model::error::ModelError),
    #[error(transparent)]
    Meta(#[from] forge_meta::MetaError),
    #[error(transparent)]
    Storage(#[from] forge_storage::StorageError),
    #[error(transparent)]
    Index(#[from] forge_index::IndexError),
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("memtable memory budget exceeded")]
    Backpressure,
    #[error(transparent)]
    Unimplemented(#[from] Unimplemented),
    #[error(transparent)]
    Wal(#[from] wal::WalError),
    #[error("internal: {0}")]
    Internal(String),
}

impl Classify for IngestError {
    fn kind(&self) -> ErrorKind {
        match self {
            Self::Model(e) => e.kind(),
            Self::Meta(e) => e.kind(),
            Self::Storage(e) => e.kind(),
            Self::Index(e) => e.kind(),
            Self::Invalid(_) => ErrorKind::InvalidInput,
            Self::Backpressure => ErrorKind::Backpressure,
            Self::Unimplemented(e) => e.kind(),
            Self::Wal(wal::WalError::Closed | wal::WalError::Io(_)) => ErrorKind::Transient,
            Self::Wal(_) | Self::Internal(_) => ErrorKind::Internal,
        }
    }
    fn message(&self) -> String {
        match self {
            Self::Meta(e) => e.message(),
            other => other.to_string(),
        }
    }
}
