use axum::Json;
use axum::http::StatusCode;
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
        let code = match self.kind {
            ErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
            ErrorKind::Conflict => StatusCode::CONFLICT,
            ErrorKind::Unimplemented => StatusCode::NOT_IMPLEMENTED,
            ErrorKind::Backpressure => StatusCode::TOO_MANY_REQUESTS,
            ErrorKind::Transient => StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (code, Json(serde_json::json!({ "error": self.msg }))).into_response()
    }
}
