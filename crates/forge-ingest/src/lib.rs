//! forge-ingest: the write path (M1). M0 defines the error type only.

use forge_model::error::{Classify, ErrorKind, Unimplemented};

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
        }
    }
    fn message(&self) -> String {
        match self {
            Self::Meta(e) => e.message(),
            other => other.to_string(),
        }
    }
}
