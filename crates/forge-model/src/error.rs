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
    #[error("hour bucket {0} is outside the supported time range")]
    HourOutOfRange(i64),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("query cancelled")]
    Cancelled,
}
pub type Result<T, E = ModelError> = std::result::Result<T, E>;

/// A feature that lands in a later milestone. Every crate error wraps it transparently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("not implemented until {0}")]
pub struct Unimplemented(pub &'static str);

/// How an error should be reported to a client (HTTP status, gRPC code) or handled by a caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    InvalidInput,
    Conflict,
    Unimplemented,
    /// Over the memory budget; the client should back off.
    Backpressure,
    /// Worth retrying with backoff.
    Transient,
    Internal,
    /// The addressed entity does not exist.
    NotFound,
    /// The request ran past its deadline.
    Timeout,
}

/// Implemented once per crate error; wrappers delegate to the wrapped error.
pub trait Classify: std::fmt::Display {
    fn kind(&self) -> ErrorKind;
    /// Text shown to clients; defaults to `Display`.
    fn message(&self) -> String {
        self.to_string()
    }
}

impl Classify for ModelError {
    fn kind(&self) -> ErrorKind {
        match self {
            Self::Cancelled => ErrorKind::Internal,
            _ => ErrorKind::InvalidInput,
        }
    }
}

impl Classify for Unimplemented {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Unimplemented
    }
}
