use axum::Router;
use axum::body::{Body, to_bytes};
use forge_model::config::ForgeConfig;
use forge_server::{Forge, api_router, ops_router};
use http::{Request, StatusCode};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tower::ServiceExt;

async fn forge() -> Arc<Forge> {
    let mut cfg = ForgeConfig::default();
    cfg.metastore.url = "sqlite::memory:".into();
    cfg.storage.url = "memory://".into();
    // leaked tempdir: the WAL lock must be per test
    cfg.wal.dir = tempfile::tempdir().unwrap().keep();
    Forge::open(cfg).await.unwrap()
}

async fn call(r: &Router, req: Request<Body>) -> (StatusCode, Value) {
    let (st, _, b) = body(r, req).await;
    (
        st,
        serde_json::from_slice(&b).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&b).into())),
    )
}

fn get(uri: &str) -> Request<Body> {
    get_h(uri, &[])
}

fn post(uri: &str, body: Value) -> Request<Body> {
    Request::post(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

#[tokio::test]
async fn ops_endpoints() {
    let f = forge().await;
    let ops = ops_router(f.clone());
    assert_eq!(
        call(&ops, get("/healthz")).await,
        (StatusCode::OK, Value::String("ok".into()))
    );
    assert_eq!(call(&ops, get("/readyz")).await.0, StatusCode::SERVICE_UNAVAILABLE);
    f.ready.store(true, Ordering::Release);
    assert_eq!(call(&ops, get("/readyz")).await.0, StatusCode::OK);
    f.shutdown.cancel();
    assert_eq!(
        call(&ops, get("/readyz")).await,
        (StatusCode::SERVICE_UNAVAILABLE, Value::String("shutting down".into()))
    );
    let resp = ops.clone().oneshot(get("/metrics")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()["content-type"], "text/plain; version=0.0.4");
}

#[tokio::test]
async fn projects_api() {
    let api = api_router(forge().await);
    assert_eq!(call(&api, get("/v1/projects")).await, (StatusCode::OK, json!([])));

    let (st, v) = call(&api, post("/v1/projects", json!({"id": "demo", "retention_days": 7}))).await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    assert_eq!(
        (&v["id"], &v["name"], &v["retention_days"]),
        (&json!("demo"), &json!("demo"), &json!(7))
    );
    assert!(v["created_at_ms"].as_i64().unwrap() > 0);

    let (st, v) = call(&api, post("/v1/projects", json!({"id": "demo"}))).await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert!(v["error"].is_string(), "{v}");
    let (st, v) = call(&api, post("/v1/projects", json!({"id": "bad/id"}))).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert!(v["error"].is_string(), "{v}");
    let (st, v) = call(&api, post("/v1/projects", json!({"id": "zero", "retention_days": 0}))).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");

    // Body rejections (missing field, wrong type, bad JSON, no content-type) use the same 400 JSON shape.
    let no_type = Request::post("/v1/projects")
        .body(Body::from(json!({"id": "x"}).to_string()))
        .unwrap();
    let bad_json = Request::post("/v1/projects")
        .header("content-type", "application/json")
        .body(Body::from("{"))
        .unwrap();
    for req in [
        post("/v1/projects", json!({"name": "no id"})),
        post("/v1/projects", json!({"id": "neg", "retention_days": -1})),
        bad_json,
        no_type,
    ] {
        let (st, v) = call(&api, req).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
        assert!(v["error"].is_string(), "{v}");
    }

    let (st, v) = call(&api, get("/v1/projects")).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v.as_array().unwrap().len(), 1, "{v}");

    assert_eq!(call(&api, post("/v1/sql", json!({}))).await.0, StatusCode::BAD_REQUEST);
    assert_eq!(call(&api, get("/v1/traces/abc")).await.0, StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------- read API (M2)

use forge_ingest::Ingest;
use forge_model::row::SpanRow;
use forge_model::testing::{pid, sid, tid};

/// The same Forge with its query engine over `store` and `cfg`.
fn with_engine(
    f: &Arc<Forge>,
    store: Arc<dyn forge_storage::SegmentStore>,
    cfg: forge_model::config::QueryConfig,
) -> Arc<Forge> {
    let query = forge_query::QueryEngine::new(f.meta.clone(), store, f.ingest.memtable_view(), cfg);
    Arc::new(Forge {
        config: f.config.clone(),
        meta: f.meta.clone(),
        segments: f.segments.clone(),
        ingest: f.ingest.clone(),
        query: Arc::new(query),
        ready: std::sync::atomic::AtomicBool::new(true),
        shutdown: tokio_util::sync::CancellationToken::new(),
        data_lock: None,
    })
}

const T0: i64 = 1_700_000_000_123_456_789;

fn span(project: &str, trace: u64, s: u64, parent: Option<u64>) -> SpanRow {
    SpanRow {
        project_id: Some(pid(project)),
        trace_id: tid(trace),
        span_id: sid(s),
        parent_span_id: parent.map(sid),
        name: Some(format!("span-{s}")),
        start_time_ns: Some(T0 + s as i64 * 1_000_000),
        tags: Some(vec![]),
        metadata: Some("{\"k\":1}".into()),
        inputs: Some(format!("{{\"a\":{s}}}")),
        ..Default::default()
    }
}

async fn body(r: &Router, req: Request<Body>) -> (StatusCode, http::HeaderMap, Vec<u8>) {
    let resp = r.clone().oneshot(req).await.unwrap();
    let (parts, b) = resp.into_parts();
    (
        parts.status,
        parts.headers,
        to_bytes(b, usize::MAX).await.unwrap().to_vec(),
    )
}

fn get_h(uri: &str, h: &[(&str, &str)]) -> Request<Body> {
    let mut r = Request::get(uri);
    for (k, v) in h {
        r = r.header(*k, *v);
    }
    r.body(Body::empty()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traces_api() {
    let f = forge().await;
    let api = api_router(f.clone());
    f.ingest
        .submit(vec![span("default", 1, 1, None), span("default", 1, 2, Some(1))])
        .await
        .unwrap();
    f.ingest
        .submit(vec![SpanRow {
            end_time_ns: Some(T0 + 3_000_000),
            ..span("default", 1, 2, None)
        }])
        .await
        .unwrap();
    f.ingest.submit(vec![span("other", 7, 1, None)]).await.unwrap();
    let id = tid(1).to_string();
    let (st, v) = call(&api, get(&format!("/v1/traces/{id}"))).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let root = &v["roots"][0];
    let child = &root["children"][0];
    assert_eq!(
        (v["project"].as_str(), v["trace_id"].as_str(), v["span_count"].as_u64()),
        (Some("default"), Some(id.as_str()), Some(2))
    );
    assert_eq!(root["span_id"], sid(1).to_string());
    assert_eq!(root["start_time"], "2023-11-14T22:13:20.124456789Z");
    assert_eq!(root["latency_ms"], Value::Null);
    assert_eq!(
        (&root["tags"], &root["metadata"], &root["inputs"]),
        (&json!([]), &json!({"k": 1}), &json!({"a": 1}))
    );
    assert_eq!(root["children"].as_array().unwrap().len(), 1);
    assert_eq!(
        (&child["name"], &child["parent_span_id"], &child["latency_ms"]),
        (&json!("span-2"), &json!(sid(1).to_string()), &json!(1.0))
    );
    assert_eq!(child["children"], json!([]));
    // Uppercase hex and a dashed UUID.
    let upper = id.to_uppercase();
    let dashed = format!(
        "{}-{}-{}-{}-{}",
        &id[..8],
        &id[8..12],
        &id[12..16],
        &id[16..20],
        &id[20..]
    );
    for alt in [upper, dashed] {
        assert_eq!(
            call(&api, get(&format!("/v1/traces/{alt}"))).await,
            (StatusCode::OK, v.clone())
        );
    }
    let (_, flat) = call(&api, get(&format!("/v1/traces/{id}?format=flat&payloads=false"))).await;
    let spans = flat["spans"].as_array().unwrap();
    assert_eq!((spans.len(), &spans[1]["depth"]), (2, &json!(1)));
    assert!(
        spans
            .iter()
            .all(|s| s.get("inputs").is_none() && s.get("children").is_none())
    );
    for (uri, want) in [
        ("/v1/traces/xyz".to_string(), StatusCode::BAD_REQUEST),
        (format!("/v1/traces/{id}?format=nested"), StatusCode::BAD_REQUEST),
        (format!("/v1/traces/{id}?payloads=maybe"), StatusCode::BAD_REQUEST),
        (format!("/v1/traces/{}", tid(2)), StatusCode::NOT_FOUND),
        (format!("/v1/traces/{id}?project=Bad%20Id"), StatusCode::BAD_REQUEST),
    ] {
        let (st, v) = call(&api, get(&uri)).await;
        assert_eq!(st, want, "{uri}: {v}");
        assert!(v["error"].is_string(), "{uri}: {v}");
    }
    // Project: param, then header, then the default.
    let other = format!("/v1/traces/{}", tid(7));
    assert_eq!(call(&api, get(&other)).await.0, StatusCode::NOT_FOUND);
    assert_eq!(
        call(&api, get(&format!("{other}?project=other"))).await.0,
        StatusCode::OK
    );
    assert_eq!(
        call(&api, get_h(&other, &[("x-forge-project", "other")])).await.0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &api,
            get_h(&format!("{other}?project=default"), &[("x-forge-project", "other")])
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    // gzip.
    let (st, h, gz) = body(&api, get_h(&format!("/v1/traces/{id}"), &[("accept-encoding", "gzip")])).await;
    assert_eq!((st, h["content-encoding"].to_str().unwrap()), (StatusCode::OK, "gzip"));
    let mut plain = Vec::new();
    std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(&gz[..]), &mut plain).unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&plain).unwrap(), v);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runs_api() {
    let f = forge().await;
    let api = api_router(f.clone());
    for t in 0..5 {
        f.ingest
            .submit(vec![span("default", t, 1, None), span("default", t, 2, Some(1))])
            .await
            .unwrap();
    }
    f.ingest
        .submit(vec![SpanRow {
            start_time_ns: Some(T0 + 10_000_000),
            ..span("default", 3, 3, None)
        }])
        .await
        .unwrap();
    let (st, v) = call(&api, get("/v1/runs")).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let all: Vec<(String, String)> = v["runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r["start_time"].to_string(), r["trace_id"].to_string()))
        .collect();
    assert_eq!(all.len(), 6);
    assert!(all.windows(2).all(|w| w[0] >= w[1]), "newest first: {all:?}");
    assert_eq!(v["runs"][0]["span_id"], sid(3).to_string());
    assert_eq!(v["next_cursor"], Value::Null);
    // Paging with limit 4.
    let (_, p1) = call(&api, get("/v1/runs?limit=4")).await;
    let cursor = p1["next_cursor"].as_str().unwrap().to_string();
    let (_, p2) = call(&api, get(&format!("/v1/runs?limit=4&cursor={cursor}"))).await;
    let pages: Vec<Value> = p1["runs"]
        .as_array()
        .unwrap()
        .iter()
        .chain(p2["runs"].as_array().unwrap())
        .cloned()
        .collect();
    assert_eq!(
        (pages, &p2["next_cursor"]),
        (v["runs"].as_array().unwrap().clone(), &Value::Null)
    );
    for bad in [
        "/v1/runs?limit=0".to_string(),
        "/v1/runs?limit=1001".into(),
        "/v1/runs?status=bogus".into(),
        "/v1/runs?run_type=llm,nope".into(),
        "/v1/runs?start=yesterday".into(),
        "/v1/runs?cursor=00ff".into(),
        format!("/v1/runs?limit=4&status=ok&cursor={cursor}"),
    ] {
        let (st, v) = call(&api, get(&bad)).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{bad}: {v}");
    }
    assert_eq!(
        call(&api, get("/v1/runs?project=nobody")).await,
        (StatusCode::OK, json!({"runs": [], "next_cursor": null}))
    );
    let (_, w) = call(
        &api,
        get("/v1/runs?start=2023-11-14T22:13:20.130Z&end=2023-11-14T22:13:21Z"),
    )
    .await;
    assert_eq!(w["runs"].as_array().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_api() {
    let f = forge().await;
    let api = api_router(f.clone());
    f.ingest
        .submit(vec![span("default", 1, 1, None), span("default", 1, 2, Some(1))])
        .await
        .unwrap();
    let q = |query: &str, extra: Value| {
        let mut b = json!({"query": query});
        b.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        post("/v1/sql", b)
    };
    let (st, v) = call(
        &api,
        q(
            "SELECT trace_id, name, start_time, tags, metadata FROM spans ORDER BY name",
            json!({}),
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["columns"][0], json!({"name": "trace_id", "type": "Binary"}));
    assert_eq!(v["columns"][2]["type"], "Timestamp(ns, \"UTC\")");
    let r0 = &v["rows"][0];
    assert_eq!(r0["trace_id"], tid(1).to_string());
    assert!(
        r0["start_time"]
            .as_str()
            .unwrap()
            .starts_with("2023-11-14T22:13:20.124456789"),
        "{r0}"
    );
    assert_eq!((&r0["tags"], &r0["metadata"]), (&json!([]), &json!("{\"k\":1}")));
    assert_eq!((&v["row_count"], &v["truncated"]), (&json!(2), &json!(false)));
    assert!(v["elapsed_ms"].as_f64().unwrap() >= 0.0);
    let (_, v) = call(&api, q("SELECT name FROM spans", json!({"max_rows": 2}))).await;
    assert_eq!((&v["row_count"], &v["truncated"]), (&json!(2), &json!(false)));
    let (_, v) = call(&api, q("SELECT name FROM spans", json!({"max_rows": 1}))).await;
    assert_eq!(
        (&v["row_count"], &v["truncated"], v["rows"].as_array().unwrap().len()),
        (&json!(1), &json!(true), 1)
    );
    for (req, what) in [
        (q("SELECT 1", json!({"max_rows": 100_001})), "max_rows"),
        (q("DROP TABLE spans", json!({})), "DDL"),
        (q("SELEC 1", json!({})), "syntax"),
        (
            Request::post("/v1/sql")
                .header("content-type", "application/json")
                .body(Body::from("{nope"))
                .unwrap(),
            "json",
        ),
    ] {
        let (st, v) = call(&api, req).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{what}: {v}");
    }
}

/// Timeouts (504, clamped by the engine timeout), the memory limit (400), the concurrency limit (503), the body cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_api_limits() {
    use forge_model::config::QueryConfig;
    use forge_query::testing::RecordingSegmentStore;
    let f = forge().await;
    let rows: Vec<SpanRow> = (0..300).map(|s| span("default", s % 7, s, None)).collect();
    f.ingest.submit(rows[..150].to_vec()).await.unwrap();
    f.ingest.flush_all().await.unwrap();
    f.ingest.submit(rows[150..].to_vec()).await.unwrap();
    let slow = Arc::new(RecordingSegmentStore::slow(
        f.segments.clone(),
        std::time::Duration::from_millis(700),
    ));
    // Timeouts.
    let api = api_router(with_engine(
        &f,
        slow.clone(),
        QueryConfig {
            timeout: std::time::Duration::from_millis(300),
            ..Default::default()
        },
    ));
    for ms in [50, 600_000] {
        let t = std::time::Instant::now();
        let (st, v) = call(
            &api,
            post("/v1/sql", json!({"query": "SELECT name FROM spans", "timeout_ms": ms})),
        )
        .await;
        assert_eq!(st, StatusCode::GATEWAY_TIMEOUT, "{v}");
        assert!(
            t.elapsed() < std::time::Duration::from_secs(2),
            "{ms}: {:?}",
            t.elapsed()
        );
    }
    // Memory: a sort that cannot fit in 16 KiB over a merge; the default limit succeeds.
    let order = json!({"query": "SELECT * FROM spans ORDER BY name, inputs"});
    let small = api_router(with_engine(
        &f,
        f.segments.clone(),
        QueryConfig {
            memory_limit: bytesize::ByteSize::kib(16),
            ..Default::default()
        },
    ));
    let (st, v) = call(&small, post("/v1/sql", order.clone())).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert!(v["error"].as_str().unwrap().contains("memory limit"), "{v}");
    let (st, v) = call(&api_router(f.clone()), post("/v1/sql", order)).await;
    assert_eq!((st, &v["row_count"]), (StatusCode::OK, &json!(300)));
    // Concurrency: one slot, held by a slow query; the second waits 50 ms, then 503 + Retry-After.
    slow.take_projections();
    let one = api_router(with_engine(
        &f,
        slow.clone(),
        QueryConfig {
            sql_concurrency: 1,
            ..Default::default()
        },
    ));
    let held = tokio::spawn({
        let one = one.clone();
        async move { call(&one, post("/v1/sql", json!({"query": "SELECT name FROM spans"}))).await }
    });
    // The held query scans (and sleeps) only after taking the slot.
    while slow.take_projections().is_empty() {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let (st, h, _) = body(&one, post("/v1/sql", json!({"query": "SELECT 1", "timeout_ms": 50}))).await;
    assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE);
    assert!(h.contains_key("retry-after"));
    assert_eq!(held.await.unwrap().0, StatusCode::OK);
    // Body cap: 1 KiB.
    let capped = api_router(with_engine(
        &f,
        f.segments.clone(),
        QueryConfig {
            max_response_bytes: bytesize::ByteSize::kib(1),
            ..Default::default()
        },
    ));
    let (st, _, b) = body(
        &capped,
        post("/v1/sql", json!({"query": "SELECT name, inputs FROM spans"})),
    )
    .await;
    let v: Value = serde_json::from_slice(&b).unwrap();
    assert_eq!((st, &v["truncated"]), (StatusCode::OK, &json!(true)));
    let n = v["row_count"].as_u64().unwrap();
    assert!(
        n > 0 && n < 300 && b.len() < 1024 + 64 * 64,
        "{n} rows, {} bytes",
        b.len()
    );
}

/// `POST /v1/search`: memtable hits (freshness), paging, no payload, 400s, 405 for GET.
#[tokio::test]
async fn search_api() {
    let f = forge().await;
    let api = api_router(f.clone());
    for t in 0..5 {
        let mut s = span("default", t, 1, None);
        s.inputs = Some(format!(
            "{{\"messages\":[{{\"content\":\"please refund order {t}\"}}]}}"
        ));
        f.ingest.submit(vec![s, span("default", t, 2, Some(1))]).await.unwrap();
    }
    let (st, v) = call(&api, post("/v1/search", json!({"q": "refund"}))).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let hits = v["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 5, "{v}");
    assert!(hits.iter().all(|h| h.get("inputs").is_none()), "no payload: {v}");
    assert_eq!(v["next_cursor"], Value::Null);
    let (_, p1) = call(&api, post("/v1/search", json!({"q": "refund", "limit": 3}))).await;
    let c = p1["next_cursor"].as_str().unwrap().to_string();
    let (st, p2) = call(
        &api,
        post("/v1/search", json!({"q": "refund", "limit": 3, "cursor": c})),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{p2}");
    assert_eq!(
        p1["hits"].as_array().unwrap().len() + p2["hits"].as_array().unwrap().len(),
        5
    );
    assert_eq!(p2["next_cursor"], Value::Null);
    for bad in [
        json!({"q": ""}),
        json!({"q": "x", "limit": 0}),
        json!({"q": "x", "limit": 1001}),
        json!({"q": "x", "nope": 1}),
        json!({"q": "x", "cursor": "zz"}),
        json!({"q": "x", "columns": ["model"]}),
    ] {
        let (st, v) = call(&api, post("/v1/search", bad.clone())).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{bad} -> {v}");
    }
    let resp = api.clone().oneshot(get("/v1/search")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
}

/// `POST /v1/search` edges (plans/phase-5 §5.4) and the runs `q=` token semantics (D14).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_api_edges() {
    let f = forge().await;
    let api = api_router(f.clone());
    let mut rows = Vec::new();
    for t in 0..60 {
        let mut s = span("default", t, 1000 + t, None);
        s.inputs = Some(format!("{{\"q\":\"common word {t}\"}}"));
        s.name = Some(["span-1", "span-12", "cafe"][t as usize % 3].into());
        rows.push(s);
    }
    f.ingest.submit(rows).await.unwrap();
    let hits = |v: &Value| v["hits"].as_array().unwrap().len();
    // Default limit 50.
    let (st, v) = call(&api, post("/v1/search", json!({"q": "common"}))).await;
    assert_eq!((st, hits(&v)), (StatusCode::OK, 50), "{v}");
    assert!(v["next_cursor"].is_string());
    // q length bounds; no tokens; empty columns.
    let (st, v) = call(&api, post("/v1/search", json!({"q": "a".repeat(1024)}))).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let (st, _) = call(&api, post("/v1/search", json!({"q": "a".repeat(1025)}))).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let (st, v) = call(&api, post("/v1/search", json!({"q": "!!!"}))).await;
    assert_eq!((st, hits(&v)), (StatusCode::OK, 0), "{v}");
    let (st, _) = call(&api, post("/v1/search", json!({"q": "x", "columns": []}))).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    // Cursors of other listings.
    let (_, v) = call(&api, post("/v1/search", json!({"q": "common", "limit": 10}))).await;
    let c = v["next_cursor"].as_str().unwrap().to_string();
    let (_, runs) = call(&api, get("/v1/runs?limit=1")).await;
    let runs_cursor = runs["next_cursor"].as_str().unwrap().to_string();
    for bad in [
        json!({"q": "common", "limit": 10, "cursor": runs_cursor}),
        json!({"q": "common", "limit": 10, "cursor": c, "project": "other"}),
        json!({"q": "common", "limit": 11, "cursor": c}),
    ] {
        let (st, v) = call(&api, post("/v1/search", bad.clone())).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{bad} -> {v}");
    }
    // Exactly `limit` hits: no cursor when the listing is known to be complete (here: every source read); a cursor
    // otherwise, and then page 2 is empty and ends the listing.
    let (_, p1) = call(&api, post("/v1/search", json!({"q": "word 7", "limit": 1}))).await;
    assert_eq!(hits(&p1), 1, "{p1}");
    if let Some(c) = p1["next_cursor"].as_str() {
        let (_, p2) = call(
            &api,
            post("/v1/search", json!({"q": "word 7", "limit": 1, "cursor": c})),
        )
        .await;
        assert_eq!((hits(&p2), &p2["next_cursor"]), (0, &Value::Null), "{p2}");
    }
    // Runs q=: tokens, not substrings; case and accent folded; prefixes.
    let names = |v: &Value| -> std::collections::BTreeSet<String> {
        v["runs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["name"].as_str().unwrap().to_string())
            .collect()
    };
    let (_, v) = call(&api, get("/v1/runs?q=span-1&limit=1000")).await;
    assert_eq!(names(&v), ["span-1".to_string()].into(), "{v}");
    let (_, v) = call(&api, get("/v1/runs?q=Caf%C3%A9&limit=1000")).await;
    assert_eq!(names(&v), ["cafe".to_string()].into(), "{v}");
    let (_, v) = call(&api, get("/v1/runs?q=span-1*&limit=1000")).await;
    assert_eq!(names(&v), ["span-1".to_string(), "span-12".to_string()].into(), "{v}");
}

/// A deterministic 504: every segment scan takes 200 ms, the query timeout is 50 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_api_timeout() {
    use forge_query::testing::RecordingSegmentStore;
    let f = forge().await;
    let mut s = span("default", 1, 1, None);
    s.inputs = Some("{\"q\":\"refund\"}".into());
    f.ingest.submit(vec![s]).await.unwrap();
    f.ingest.flush_all().await.unwrap();
    let slow = Arc::new(RecordingSegmentStore::slow(
        f.segments.clone(),
        std::time::Duration::from_millis(200),
    ));
    let cfg = forge_model::config::QueryConfig {
        timeout: std::time::Duration::from_millis(50),
        ..Default::default()
    };
    let api = api_router(with_engine(&f, slow, cfg));
    let (st, v) = call(&api, post("/v1/search", json!({"q": "refund"}))).await;
    assert_eq!(st, StatusCode::GATEWAY_TIMEOUT, "{v}");
}
