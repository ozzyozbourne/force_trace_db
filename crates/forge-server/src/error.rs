use axum::Json;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use forge_model::error::{Classify, ErrorKind};

/// API error; the body is `{"error": msg}`. Built from any classified crate error.
#[derive(Debug)]
pub struct ApiError {
    kind: ErrorKind,
    msg: String,
}

impl<E: Classify> From<E> for ApiError {
    fn from(e: E) -> Self {
        let kind = e.kind();
        if kind == ErrorKind::Internal {
            tracing::error!(error = %e, "internal error");
        }
        Self { kind, msg: e.message() }
    }
}

impl ApiError {
    pub fn invalid_input(msg: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::InvalidInput,
            msg: msg.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (http_status(self.kind), Json(serde_json::json!({ "error": self.msg }))).into_response()
    }
}

// ---- the one ErrorKind -> wire mapping (API, OTLP/HTTP, OTLP/gRPC) ----

pub(crate) fn http_status(k: ErrorKind) -> StatusCode {
    match k {
        ErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
        ErrorKind::Conflict => StatusCode::CONFLICT,
        ErrorKind::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        ErrorKind::Backpressure => StatusCode::TOO_MANY_REQUESTS,
        ErrorKind::Transient => StatusCode::SERVICE_UNAVAILABLE,
        ErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// M6: attach google.rpc.RetryInfo (tonic-types) to Backpressure so exporters retry with the given delay.
pub(crate) fn grpc_code(k: ErrorKind) -> tonic::Code {
    match k {
        ErrorKind::InvalidInput => tonic::Code::InvalidArgument,
        ErrorKind::Backpressure => tonic::Code::ResourceExhausted,
        ErrorKind::Transient => tonic::Code::Unavailable,
        ErrorKind::Unimplemented => tonic::Code::Unimplemented,
        ErrorKind::Conflict => tonic::Code::Aborted,
        ErrorKind::Internal => tonic::Code::Internal,
    }
}

/// `Retry-After` for retryable kinds (sent by the OTLP/HTTP receiver).
pub(crate) fn retry_after(k: ErrorKind) -> Option<HeaderValue> {
    matches!(k, ErrorKind::Backpressure | ErrorKind::Transient).then(|| HeaderValue::from_static("1"))
}
