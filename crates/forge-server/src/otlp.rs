use axum::Router;
use axum::http::StatusCode;
use axum::routing::post;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{TraceService, TraceServiceServer};
use opentelemetry_proto::tonic::collector::trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse};
use tonic::{Request, Response, Status};

/// Both OTLP receivers answer with this until ingest lands.
const STUB_MSG: &str = "OTLP ingest lands in M1";

#[derive(Debug, Default)]
pub struct OtlpGrpcStub;

#[tonic::async_trait]
impl TraceService for OtlpGrpcStub {
    async fn export(
        &self,
        _req: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        Err(Status::unimplemented(STUB_MSG))
    }
}

pub fn grpc_service() -> TraceServiceServer<OtlpGrpcStub> {
    TraceServiceServer::new(OtlpGrpcStub)
}

pub fn http_router() -> Router {
    Router::new().route("/v1/traces", post(|| async { (StatusCode::NOT_IMPLEMENTED, STUB_MSG) }))
}
