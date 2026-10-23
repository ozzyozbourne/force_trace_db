//! forge-query: the read path (M2). M0 defines the error type only.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    Model(#[from] forge_model::error::ModelError),
    #[error(transparent)]
    Meta(#[from] forge_meta::MetaError),
    #[error(transparent)]
    Storage(#[from] forge_storage::StorageError),
    #[error(transparent)]
    Index(#[from] forge_index::IndexError),
    #[error("not implemented until {0}")]
    Unimplemented(&'static str),
}
