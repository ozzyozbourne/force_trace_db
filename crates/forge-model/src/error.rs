#[derive(Debug, thiserror::Error)]
pub enum ModelError {
    #[error("invalid {what}: expected {expected} bytes, got {got}")]
    InvalidIdLength {
        what: &'static str,
        expected: usize,
        got: usize,
    },
    #[error("invalid {what} hex: {source}")]
    InvalidIdHex {
        what: &'static str,
        #[source]
        source: hex::FromHexError,
    },
    #[error("invalid project id {0:?}: 1-128 chars of [A-Za-z0-9._-], not starting with '.'")]
    InvalidProjectId(String),
    #[error("unknown {what} {value:?}")]
    UnknownEnum { what: &'static str, value: String },
    #[error("batch does not match the spans schema: {0}")]
    Schema(String),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
}
pub type Result<T, E = ModelError> = std::result::Result<T, E>;
