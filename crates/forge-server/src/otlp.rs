//! OTLP receivers (forge-server): gRPC TraceService on 4317 and POST /v1/traces on 4318.
use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use forge_ingest::Ingest;
use forge_ingest::normalize::{NormalizeConfig, Normalized, PROJECT_HEADER, normalize_request};
use forge_model::error::{Classify, ErrorKind};
use forge_model::ids::ProjectId;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{TraceService, TraceServiceServer};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use prost::Message;
use std::sync::Arc;
use tonic::codec::CompressionEncoding;
use tower_http::decompression::RequestDecompressionLayer;
use tower_http::limit::RequestBodyLimitLayer;

/// A client-facing failure: classification + message. Built from any `Classify` error.
#[derive(Debug, Clone, PartialEq)]
pub struct ExportError {
    pub kind: ErrorKind,
    pub msg: String,
}
impl<E: Classify> From<E> for ExportError {
    fn from(e: E) -> Self {
        Self {
            kind: e.kind(),
            msg: e.message(),
        }
    }
}
impl ExportError {
    fn invalid(msg: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::InvalidInput,
            msg: msg.into(),
        }
    }
    fn internal(msg: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Internal,
            msg: msg.into(),
        }
    }
}

/// Shared by both transports.
#[derive(Clone, Debug)]
pub struct OtlpReceiver {
    pub ingest: Arc<dyn Ingest>,
    pub cfg: Arc<NormalizeConfig>,
}

impl OtlpReceiver {
    pub fn new(ingest: Arc<dyn Ingest>, cfg: &forge_model::config::IngestConfig) -> anyhow::Result<Self> {
        Ok(Self {
            ingest,
            cfg: Arc::new(NormalizeConfig {
                default_project: ProjectId::new(cfg.default_project.clone())?,
                pricing: Arc::new(forge_ingest::normalize::pricing::Pricing::builtin()),
            }),
        })
    }
}

fn parse_project(h: Option<&str>) -> Result<Option<ProjectId>, ExportError> {
    h.map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| ProjectId::new(s).map_err(|e| ExportError::invalid(format!("{PROJECT_HEADER}: {e}"))))
        .transpose()
}

fn response(n_rejected: u64, first_error: Option<String>) -> ExportTraceServiceResponse {
    ExportTraceServiceResponse {
        partial_success: (n_rejected > 0).then(|| ExportTracePartialSuccess {
            rejected_spans: i64::try_from(n_rejected).unwrap_or(i64::MAX),
            error_message: format!(
                "{n_rejected} span(s) rejected; first: {}",
                first_error.unwrap_or_default()
            ),
        }),
    }
}

impl OtlpReceiver {
    /// Submit normalized rows (if any) and build the OTLP response. Rejected spans never fail the request:
    /// they are reported in partial_success (OTLP: the client must not retry them).
    pub async fn finish(&self, n: Normalized) -> Result<ExportTraceServiceResponse, ExportError> {
        let Normalized {
            rows,
            rejected,
            first_error,
        } = n;
        if !rows.is_empty() {
            self.ingest.submit(rows).await?;
        }
        Ok(response(rejected, first_error))
    }

    /// Normalization is CPU work proportional to the request (up to 64 MiB): run it off the async workers.
    async fn normalize(
        &self,
        req: ExportTraceServiceRequest,
        project: Option<ProjectId>,
    ) -> Result<Normalized, ExportError> {
        let cfg = self.cfg.clone();
        tokio::task::spawn_blocking(move || normalize_request(req, project.as_ref(), &cfg))
            .await
            .map_err(|e| ExportError::internal(format!("normalize task: {e}")))
    }
}

// ------------------------------------------------------------------------------------------------
// gRPC
// ------------------------------------------------------------------------------------------------

pub fn to_status(e: ExportError) -> tonic::Status {
    use tonic::Status;
    match e.kind {
        ErrorKind::InvalidInput => Status::invalid_argument(e.msg),
        // M6: attach google.rpc.RetryInfo (tonic-types) so exporters retry with the given delay.
        ErrorKind::Backpressure => Status::resource_exhausted(e.msg),
        ErrorKind::Transient => Status::unavailable(e.msg),
        ErrorKind::Unimplemented => Status::unimplemented(e.msg),
        ErrorKind::Conflict => Status::aborted(e.msg),
        ErrorKind::Internal => Status::internal(e.msg),
    }
}

#[tonic::async_trait]
impl TraceService for OtlpReceiver {
    async fn export(
        &self,
        req: tonic::Request<ExportTraceServiceRequest>,
    ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        let project =
            parse_project(req.metadata().get(PROJECT_HEADER).and_then(|v| v.to_str().ok())).map_err(to_status)?;
        let n = self.normalize(req.into_inner(), project).await.map_err(to_status)?;
        self.finish(n).await.map(tonic::Response::new).map_err(to_status)
    }
}

/// `max_bytes` caps the decoded (decompressed) message; tonic answers RESOURCE_EXHAUSTED without
/// RetryInfo above it, which OTLP exporters treat as non-retryable.
pub fn grpc_service(r: OtlpReceiver, max_bytes: usize) -> TraceServiceServer<OtlpReceiver> {
    TraceServiceServer::new(r)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Gzip) // only used when the client advertises it
        .max_decoding_message_size(max_bytes)
}

// ------------------------------------------------------------------------------------------------
// HTTP
// ------------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Encoding {
    Proto,
    Json,
}
impl Encoding {
    fn content_type(self) -> &'static str {
        match self {
            Self::Proto => "application/x-protobuf",
            Self::Json => "application/json",
        }
    }
    fn from_headers(h: &HeaderMap) -> Option<Self> {
        let ct = h.get(header::CONTENT_TYPE)?.to_str().ok()?;
        match ct.split(';').next()?.trim().to_ascii_lowercase().as_str() {
            "application/x-protobuf" | "application/protobuf" => Some(Self::Proto),
            "application/json" => Some(Self::Json),
            _ => None,
        }
    }
}

/// google.rpc.Status (OTLP/HTTP error body). `details` omitted.
#[derive(Clone, PartialEq, prost::Message)]
pub struct RpcStatus {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: String,
}

fn http_status(k: ErrorKind) -> (StatusCode, tonic::Code) {
    match k {
        ErrorKind::InvalidInput => (StatusCode::BAD_REQUEST, tonic::Code::InvalidArgument),
        ErrorKind::Backpressure => (StatusCode::TOO_MANY_REQUESTS, tonic::Code::ResourceExhausted),
        ErrorKind::Transient => (StatusCode::SERVICE_UNAVAILABLE, tonic::Code::Unavailable),
        ErrorKind::Unimplemented => (StatusCode::NOT_IMPLEMENTED, tonic::Code::Unimplemented),
        ErrorKind::Conflict => (StatusCode::CONFLICT, tonic::Code::Aborted),
        ErrorKind::Internal => (StatusCode::INTERNAL_SERVER_ERROR, tonic::Code::Internal),
    }
}

fn encode_ok(enc: Encoding, resp: &ExportTraceServiceResponse) -> Response {
    let body = match enc {
        Encoding::Proto => resp.encode_to_vec(),
        // with-serde writes `"partialSuccess":null` for None; `{}` is the canonical empty message.
        Encoding::Json if resp.partial_success.is_none() => b"{}".to_vec(),
        Encoding::Json => serde_json::to_vec(resp).expect("serializable"),
    };
    ([(header::CONTENT_TYPE, enc.content_type())], body).into_response()
}

fn encode_err(enc: Encoding, e: ExportError) -> Response {
    let (status, code) = http_status(e.kind);
    let body = match enc {
        Encoding::Proto => RpcStatus {
            code: code as i32,
            message: e.msg,
        }
        .encode_to_vec(),
        Encoding::Json => {
            serde_json::to_vec(&serde_json::json!({"code": code as i32, "message": e.msg})).expect("json")
        }
    };
    let mut r = (status, [(header::CONTENT_TYPE, enc.content_type())], body).into_response();
    if matches!(status, StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE) {
        r.headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    }
    r
}

fn decode(enc: Encoding, body: &[u8]) -> Result<ExportTraceServiceRequest, ExportError> {
    match enc {
        Encoding::Proto => {
            ExportTraceServiceRequest::decode(body).map_err(|e| ExportError::invalid(format!("protobuf: {e}")))
        }
        Encoding::Json => serde_json::from_slice(body).map_err(|e| ExportError::invalid(format!("json: {e}"))),
    }
}

async fn otlp_http(State(r): State<OtlpReceiver>, headers: HeaderMap, body: Bytes) -> Response {
    let Some(enc) = Encoding::from_headers(&headers) else {
        let msg = "content-type must be application/x-protobuf or application/json";
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            [(header::CONTENT_TYPE, "application/json")],
            serde_json::json!({"code": 3, "message": msg}).to_string(),
        )
            .into_response();
    };
    let project = match parse_project(headers.get(PROJECT_HEADER).and_then(|v| v.to_str().ok())) {
        Ok(p) => p,
        Err(e) => return encode_err(enc, e),
    };
    // Decode + normalize together on the blocking pool (both scale with the body).
    let cfg = r.cfg.clone();
    let n = tokio::task::spawn_blocking(move || {
        decode(enc, &body).map(|req| normalize_request(req, project.as_ref(), &cfg))
    })
    .await;
    let n = match n {
        Ok(Ok(n)) => n,
        Ok(Err(e)) => return encode_err(enc, e),
        Err(e) => return encode_err(enc, ExportError::internal(format!("normalize task: {e}"))),
    };
    match r.finish(n).await {
        Ok(resp) => encode_ok(enc, &resp),
        Err(e) => encode_err(enc, e),
    }
}

/// Two-layer limit: RequestBodyLimitLayer (outer) caps bytes on the wire (and rejects a too-large
/// Content-Length up front); DefaultBodyLimit (inner, applies to the `Bytes` extractor) caps the
/// decompressed body, which stops gzip/zstd bombs. Unsupported Content-Encoding -> 415 (tower-http).
pub fn http_router(r: OtlpReceiver, max_bytes: usize) -> Router {
    Router::new()
        .route("/v1/traces", post(otlp_http))
        .with_state(r)
        .layer(DefaultBodyLimit::max(max_bytes))
        .layer(RequestDecompressionLayer::new()) // gzip + zstd (crate features); deflate/br not compiled -> 415
        .layer(RequestBodyLimitLayer::new(max_bytes))
}
