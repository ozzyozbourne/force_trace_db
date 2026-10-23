//! End to end: tonic + axum receivers on ephemeral ports with a fake Ingest.
#![allow(clippy::print_stderr, clippy::print_stdout)]
use async_trait::async_trait;
use forge_ingest::wal::WalError;
use forge_ingest::{Ingest, IngestError};
use forge_model::config::IngestConfig;
use forge_model::row::SpanRow;
use forge_server::otlp::{OtlpReceiver, RpcStatus, grpc_service, http_router};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse};
use prost::Message;
use std::io::Write;
use std::sync::{Arc, Mutex};
use tonic::codec::CompressionEncoding;

const MAX: usize = 1 << 20; // small limit so the tests can exceed it

#[derive(Debug, Default)]
struct FakeIngest {
    rows: Mutex<Vec<SpanRow>>,
    fail: Mutex<Option<fn() -> IngestError>>,
}
#[async_trait]
impl Ingest for FakeIngest {
    async fn submit(&self, rows: Vec<SpanRow>) -> Result<(), IngestError> {
        if let Some(f) = *self.fail.lock().unwrap() {
            return Err(f());
        }
        self.rows.lock().unwrap().extend(rows);
        Ok(())
    }
}

struct Harness {
    ingest: Arc<FakeIngest>,
    grpc: String,
    http: String,
}

async fn start() -> Harness {
    let ingest = Arc::new(FakeIngest::default());
    let r = OtlpReceiver::new(ingest.clone(), &IngestConfig::default()).unwrap();
    let gl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc = format!("http://{}", gl.local_addr().unwrap());
    let svc = grpc_service(r.clone(), MAX);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(gl))
            .await
            .unwrap()
    });
    let hl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = format!("http://{}/v1/traces", hl.local_addr().unwrap());
    let app = http_router(r, MAX);
    tokio::spawn(async move { axum::serve(hl, app).await.unwrap() });
    Harness { ingest, grpc, http }
}

fn fixture_json() -> Vec<u8> {
    forge_model::testing::fixture_bytes("genai_semconv_agent.json")
}
fn fixture() -> ExportTraceServiceRequest {
    serde_json::from_slice(&fixture_json()).unwrap()
}
fn gzip(b: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    e.write_all(b).unwrap();
    e.finish().unwrap()
}

#[tokio::test]
async fn grpc_export() {
    let h = start().await;
    for enc in [None, Some(CompressionEncoding::Gzip), Some(CompressionEncoding::Zstd)] {
        let mut c = TraceServiceClient::connect(h.grpc.clone()).await.unwrap();
        if let Some(e) = enc {
            c = c.send_compressed(e).accept_compressed(e);
        }
        let mut req = tonic::Request::new(fixture());
        req.metadata_mut().insert("x-forge-project", "proj-a".parse().unwrap());
        let resp = c.export(req).await.unwrap().into_inner();
        assert_eq!(resp.partial_success, None, "{enc:?}");
    }
    {
        let rows = h.ingest.rows.lock().unwrap();
        assert_eq!(rows.len(), 21);
        assert!(rows.iter().all(|r| r.project_id.as_ref().unwrap().as_str() == "proj-a"));
    }

    // one bad span -> partial success, the rest is ingested
    let mut c = TraceServiceClient::connect(h.grpc.clone()).await.unwrap();
    let mut req = fixture();
    req.resource_spans[0].scope_spans[0].spans[2].span_id = vec![1, 2, 3];
    let resp = c.export(req).await.unwrap().into_inner();
    let ps = resp.partial_success.unwrap();
    assert_eq!(ps.rejected_spans, 1);
    assert_eq!(
        ps.error_message,
        "1 span(s) rejected; first: resource_spans[0].scope_spans[0].spans[2]: span_id must be 8 bytes, got 3"
    );
    assert_eq!(h.ingest.rows.lock().unwrap().len(), 27);

    // bad project header -> INVALID_ARGUMENT
    let mut req = tonic::Request::new(fixture());
    req.metadata_mut().insert("x-forge-project", "a/b".parse().unwrap());
    assert_eq!(c.export(req).await.unwrap_err().code(), tonic::Code::InvalidArgument);

    // ingest errors map to codes
    for (f, code) in [
        (
            (|| IngestError::Backpressure) as fn() -> IngestError,
            tonic::Code::ResourceExhausted,
        ),
        (
            || IngestError::Wal(WalError::Io(std::io::Error::other("disk"))),
            tonic::Code::Unavailable,
        ),
        (|| IngestError::Wal(WalError::Closed), tonic::Code::Unavailable),
        (|| IngestError::Internal("bug".into()), tonic::Code::Internal),
    ] {
        *h.ingest.fail.lock().unwrap() = Some(f);
        assert_eq!(c.export(fixture()).await.unwrap_err().code(), code);
    }
    *h.ingest.fail.lock().unwrap() = None;

    // > max decoded size (gzip on the wire is tiny): RESOURCE_EXHAUSTED from tonic itself
    let mut big = fixture();
    big.resource_spans[0].scope_spans[0].spans[0].name = "x".repeat(MAX + 1);
    let mut cz = TraceServiceClient::connect(h.grpc.clone())
        .await
        .unwrap()
        .send_compressed(CompressionEncoding::Gzip);
    let e = cz.export(big).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::ResourceExhausted, "{e}");
}

#[tokio::test]
async fn http_export() {
    let h = start().await;
    let c = reqwest::Client::new();
    let post = |ct: &'static str, ce: Option<&'static str>, body: Vec<u8>| {
        let mut r = c.post(&h.http).header("content-type", ct).body(body);
        if let Some(ce) = ce {
            r = r.header("content-encoding", ce);
        }
        r.send()
    };

    // JSON -> JSON `{}`
    let r = post("application/json", None, fixture_json()).await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["content-type"], "application/json");
    assert_eq!(r.text().await.unwrap(), "{}");
    // protobuf + gzip -> protobuf
    let r = post("application/x-protobuf", Some("gzip"), gzip(&fixture().encode_to_vec()))
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["content-type"], "application/x-protobuf");
    assert_eq!(
        ExportTraceServiceResponse::decode(r.bytes().await.unwrap()).unwrap(),
        ExportTraceServiceResponse::default()
    );
    // JSON + zstd
    let r = post(
        "application/json; charset=utf-8",
        Some("zstd"),
        zstd::encode_all(&fixture_json()[..], 3).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(h.ingest.rows.lock().unwrap().len(), 21);

    // partial success, JSON shape
    let mut req = fixture();
    req.resource_spans[0].scope_spans[0].spans[0].trace_id = vec![0; 16];
    let r = post("application/json", None, serde_json::to_vec(&req).unwrap())
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body = r.text().await.unwrap();
    println!("partial success JSON body: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v["partialSuccess"]["errorMessage"],
        "1 span(s) rejected; first: resource_spans[0].scope_spans[0].spans[0]: trace_id must not be all zeros"
    );

    // errors
    let r = post("text/plain", None, b"x".to_vec()).await.unwrap();
    assert_eq!(r.status(), 415);
    assert_eq!(r.headers()["content-type"], "application/json");
    assert_eq!(
        r.text().await.unwrap(),
        r#"{"code":3,"message":"content-type must be application/x-protobuf or application/json"}"#
    );
    let r = post("application/json", Some("br"), b"x".to_vec()).await.unwrap();
    assert_eq!(r.status(), 415, "unsupported content-encoding");
    let r = post("application/json", None, b"{not json".to_vec()).await.unwrap();
    assert_eq!(r.status(), 400);
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["code"], 3);
    let r = post("application/x-protobuf", None, vec![0xff, 0xff, 0xff])
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let st = RpcStatus::decode(r.bytes().await.unwrap()).unwrap();
    assert_eq!(st.code, 3);
    assert!(st.message.starts_with("protobuf:"), "{}", st.message);
    // two-layer limit: raw body over the wire limit, and a gzip bomb under it
    let r = post("application/json", None, vec![b' '; MAX + 1]).await.unwrap();
    assert_eq!(r.status(), 413);
    let bomb = gzip(&vec![b' '; 8 * MAX]);
    assert!(bomb.len() < MAX);
    let r = post("application/json", Some("gzip"), bomb).await.unwrap();
    assert_eq!(r.status(), 413, "decompressed limit");

    // ingest errors -> status + google.rpc.Status in the request's encoding
    *h.ingest.fail.lock().unwrap() = Some(|| IngestError::Backpressure);
    let r = post("application/x-protobuf", None, fixture().encode_to_vec())
        .await
        .unwrap();
    assert_eq!(r.status(), 429);
    assert_eq!(r.headers()["retry-after"], "1");
    assert_eq!(
        RpcStatus::decode(r.bytes().await.unwrap()).unwrap().code,
        tonic::Code::ResourceExhausted as i32
    );
    *h.ingest.fail.lock().unwrap() = Some(|| IngestError::Wal(WalError::Closed));
    let r = post("application/json", None, fixture_json()).await.unwrap();
    assert_eq!(r.status(), 503);
    *h.ingest.fail.lock().unwrap() = Some(|| IngestError::Invalid("x".into()));
    assert_eq!(
        post("application/json", None, fixture_json()).await.unwrap().status(),
        400
    );
}
