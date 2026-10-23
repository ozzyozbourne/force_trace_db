use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use forge_meta::MetaError;
use forge_model::error::ModelError;

/// API error; the body is `{"error": msg}`.
#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    Conflict(String),
    Unimplemented(&'static str),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, msg) = match self {
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            Self::Conflict(m) => (StatusCode::CONFLICT, m),
            Self::Unimplemented(m) => (StatusCode::NOT_IMPLEMENTED, format!("not implemented until {m}")),
            Self::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
        };
        (code, Json(serde_json::json!({ "error": msg }))).into_response()
    }
}

impl From<MetaError> for ApiError {
    fn from(e: MetaError) -> Self {
        match e {
            MetaError::Conflict(m) => Self::Conflict(m),
            MetaError::Unimplemented(m) => Self::Unimplemented(m),
            other => {
                tracing::error!(error = %other, "metastore error");
                Self::Internal(other.to_string())
            }
        }
    }
}

impl From<ModelError> for ApiError {
    fn from(e: ModelError) -> Self {
        Self::BadRequest(e.to_string())
    }
}
