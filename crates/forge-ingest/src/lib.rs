//! forge-ingest: the write path (M1). M0 defines the error type only.
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
    Invalid(String), // -> 400 / INVALID_ARGUMENT
    #[error("memtable memory budget exceeded")]
    Backpressure, // -> 429 / RESOURCE_EXHAUSTED
    #[error("not implemented until {0}")]
    Unimplemented(&'static str),
}
