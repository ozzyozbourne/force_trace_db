use axum::Router;
use axum::http::StatusCode;
use axum::routing::post;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{TraceService, TraceServiceServer};
use opentelemetry_proto::tonic::collector::trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse};
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
pub struct OtlpGrpcStub;

#[tonic::async_trait]
impl TraceService for OtlpGrpcStub {
    async fn export(
        &self,
        _req: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        Err(Status::unimplemented("OTLP ingest lands in M1"))
    }
}

pub fn grpc_service() -> TraceServiceServer<OtlpGrpcStub> {
    TraceServiceServer::new(OtlpGrpcStub)
}

pub fn http_router() -> Router {
    Router::new().route(
        "/v1/traces",
        post(|| async { (StatusCode::NOT_IMPLEMENTED, "OTLP ingest lands in M1") }),
    )
}
