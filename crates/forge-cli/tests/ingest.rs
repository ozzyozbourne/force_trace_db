//! M1 EXIT TEST 1: the four golden OTLP fixtures go through a real `forge serve` over every transport and come out
//! of the committed L0 segments as exactly the normalized rows, merged per span.

mod common;

use common::*;
use flate2::Compression;
use flate2::write::GzEncoder;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse};
use prost::Message;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::io::Write;
use std::time::Duration;
use tonic::codec::CompressionEncoding;

fn fixture(name: &str) -> String {
    std::fs::read_to_string(forge_model::testing::fixture_path(name)).expect("fixture")
}

fn parse(json: &str) -> ExportTraceServiceRequest {
    serde_json::from_str(json).expect("OTLP JSON")
}

/// The genai fixture's second post: b106 ends (OK), b102 is renamed, b103 drops `gen_ai.input.messages`.
fn genai_repost(src: &str) -> String {
    let mut v: Value = serde_json::from_str(src).expect("json");
    for s in v["resourceSpans"][0]["scopeSpans"][0]["spans"]
        .as_array_mut()
        .expect("spans")
    {
        match s["spanId"].as_str().expect("spanId") {
            "eee19b7ec3c1b106" => {
                let start: u64 = s["startTimeUnixNano"].as_str().expect("start").parse().expect("u64");
                s["endTimeUnixNano"] = json!((start + 500_000_000).to_string());
                s["status"] = json!({"code": 1});
            }
            "eee19b7ec3c1b102" => s["name"] = json!("execute_tool get_weather v2"),
            "eee19b7ec3c1b103" => s["attributes"]
                .as_array_mut()
                .expect("attributes")
                .retain(|a| a["key"] != "gen_ai.input.messages"),
            _ => {}
        }
    }
    v.to_string()
}

fn assert_ok_body(body: &[u8], json: bool, what: &str) {
    if json {
        assert_eq!(body, b"{}", "{what}: {}", String::from_utf8_lossy(body));
    } else {
        let r = ExportTraceServiceResponse::decode(body).expect("protobuf response");
        assert_eq!(r.partial_success, None, "{what}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn golden_fixtures_ingest_end_to_end() {
    tokio::time::timeout(Duration::from_secs(120), run())
        .await
        .expect("E1 timed out");
}

async fn run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let cfg = write_config(dir, "5s");
    let env = [("FORGE_MEMTABLE__FLUSH_INTERVAL", "1h")];
    let (mut s, p) = spawn_ready(dir, Some(&cfg), &env).await;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("client");
    let url = format!("http://{}/v1/traces", p.otlp_http);

    let (langgraph, oi, genai, legacy) = (
        fixture("openllmetry_langgraph.json"),
        fixture("openinference_langchain_rag.json"),
        fixture("genai_semconv_agent.json"),
        fixture("openllmetry_legacy.json"),
    );
    let repost = genai_repost(&genai);
    // (request, x-forge-project) in post order: the expected rows are their normalization, stamped in this order.
    let posts = [
        (parse(&langgraph), None),
        (parse(&oi), None),
        (parse(&genai), None),
        (parse(&legacy), Some("legacy")),
        (parse(&repost), None),
    ];

    // 1. langgraph: HTTP JSON.
    let r = http
        .post(&url)
        .header("content-type", "application/json")
        .body(langgraph.clone())
        .send()
        .await
        .expect("post langgraph");
    assert_eq!(r.status(), 200, "{}", s.logs());
    assert_ok_body(&r.bytes().await.expect("body"), true, "langgraph");

    // 2. openinference: HTTP protobuf + gzip.
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    gz.write_all(&posts[1].0.encode_to_vec()).expect("gzip");
    let r = http
        .post(&url)
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "gzip")
        .body(gz.finish().expect("gzip"))
        .send()
        .await
        .expect("post openinference");
    assert_eq!(r.status(), 200, "{}", s.logs());
    assert_ok_body(&r.bytes().await.expect("body"), false, "openinference");

    // 3. genai: gRPC + zstd. 4. legacy: gRPC plain with x-forge-project.
    let mut zstd = TraceServiceClient::connect(format!("http://{}", p.otlp_grpc))
        .await
        .expect("grpc connect")
        .send_compressed(CompressionEncoding::Zstd);
    let resp = zstd
        .export(posts[2].0.clone())
        .await
        .expect("genai export")
        .into_inner();
    assert_eq!(resp.partial_success, None);
    let mut plain = TraceServiceClient::connect(format!("http://{}", p.otlp_grpc))
        .await
        .expect("grpc connect");
    let mut req = tonic::Request::new(posts[3].0.clone());
    req.metadata_mut()
        .insert("x-forge-project", "legacy".parse().expect("metadata"));
    let resp = plain.export(req).await.expect("legacy export").into_inner();
    assert_eq!(resp.partial_success, None);

    // 5. genai re-post: HTTP JSON (end post of b106, changed b102 name, b103 without input messages).
    let r = http
        .post(&url)
        .header("content-type", "application/json")
        .body(repost)
        .send()
        .await
        .expect("post repost");
    assert_eq!(r.status(), 200, "{}", s.logs());
    assert_ok_body(&r.bytes().await.expect("body"), true, "repost");

    s.sigterm();
    let st = s.wait_exit(Duration::from_secs(20)).await;
    let logs = s.logs();
    assert!(st.success(), "{st}\n{logs}");
    assert!(logs.contains("final flush done"), "{logs}");
    drop(s);

    // The committed state.
    let (segs, rows) = visible(dir).await;
    let projects: BTreeSet<String> = segs.iter().map(|s| s.project_id.to_string()).collect();
    assert_eq!(
        projects,
        ["default", "legacy", "support-bot"].map(String::from).into(),
        "{segs:#?}"
    );
    assert_eq!(segs.len(), 3, "one L0 segment per project: {segs:#?}");
    for seg in &segs {
        assert_eq!(seg.level, forge_model::catalog::Level::L0);
        let out = inspect(dir, seg).await;
        assert!(out.contains("contract:  OK"), "{out}");
    }
    let normalized: Vec<_> = posts.iter().map(|(r, h)| normalize(r, *h)).collect();
    assert_eq!(normalized.iter().map(Vec::len).sum::<usize>(), 27, "27 rows ingested");
    assert_eq!(rows.len(), 20, "one merged row per span");

    let got = merged(&rows);
    let want = expected(&normalized);
    assert_eq!(got.keys().collect::<Vec<_>>(), want.keys().collect::<Vec<_>>());
    for (k, w) in &want {
        assert_eq!(&got[k], w, "{k:?}");
    }
    let span = |id: &str| {
        got.values()
            .find(|r| hex::encode(r.span_id.0) == id)
            .unwrap_or_else(|| panic!("span {id}"))
    };
    let b106 = span("eee19b7ec3c1b106");
    assert!(b106.end_time_ns.is_some(), "{b106:?}");
    assert_eq!(b106.status, Some(forge_model::enums::SpanStatus::Ok));
    assert_eq!(
        span("eee19b7ec3c1b102").name.as_deref(),
        Some("execute_tool get_weather v2")
    );
    let first_b103 = normalized[2]
        .iter()
        .find(|r| hex::encode(r.span_id.0) == "eee19b7ec3c1b103")
        .expect("b103");
    assert!(first_b103.inputs.is_some());
    assert_eq!(
        span("eee19b7ec3c1b103").inputs,
        first_b103.inputs,
        "dropped field keeps the older value"
    );

    // Restart: nothing to replay, nothing new committed.
    let (mut s, _) = spawn_ready(dir, Some(&cfg), &env).await;
    s.sigterm();
    let st = s.wait_exit(Duration::from_secs(20)).await;
    let logs = s.logs();
    assert!(st.success(), "{st}\n{logs}");
    assert_eq!(
        log_field(&logs, "wal replay done", "applied").as_u64(),
        Some(0),
        "{logs}"
    );
    drop(s);
    assert_eq!(visible(dir).await.0.len(), 3);
}
