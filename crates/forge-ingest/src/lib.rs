//! forge-ingest: the write path (M1). M0 defines the error type only.

use forge_model::error::{Classify, ErrorKind, Unimplemented};

pub mod blob;
pub mod flush;
pub mod memtable;
pub mod normalize;
pub mod pipeline;
pub mod version;
pub mod wal;
pub use pipeline::{OpenReport, Pipeline, run_flusher};

#[derive(Debug, Clone, PartialEq)]
pub struct IngestAck {
    pub rows: usize,
    pub versions: std::ops::Range<u64>,
    pub wal_seq: u64,
}

#[async_trait::async_trait]
pub trait Ingest: Send + Sync + std::fmt::Debug + 'static {
    /// Rows already normalized (version 0).
    async fn submit(&self, rows: Vec<forge_model::row::SpanRow>) -> Result<IngestAck, IngestError>;
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
