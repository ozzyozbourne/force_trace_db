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
    let resp = r.clone().oneshot(req).await.unwrap();
    let st = resp.status();
    let b = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    (
        st,
        serde_json::from_slice(&b).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&b).into())),
    )
}

fn get(uri: &str) -> Request<Body> {
    Request::get(uri).body(Body::empty()).unwrap()
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

#[tokio::test]
async fn p3v_read_api() {
    use forge_ingest::Ingest;
    use forge_model::row::SpanRow;
    use forge_model::testing::{pid, sid, tid};
    let f = forge().await;
    let api = api_router(f.clone());
    let root = SpanRow { project_id: Some(pid("default")), trace_id: tid(1), span_id: sid(1), name: Some("root".into()),
        start_time_ns: Some(1_700_000_000_123_456_789), inputs: Some("{\"a\":1}".into()), ..Default::default() };
    let child = SpanRow { span_id: sid(2), parent_span_id: Some(sid(1)), name: Some("child".into()), start_time_ns: Some(1_700_000_000_200_000_000), ..root.clone() };
    f.ingest.submit(vec![root, child]).await.unwrap();
    let (st, v) = call(&api, get(&format!("/v1/traces/{}", tid(1)))).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    eprintln!("{v}");
    assert_eq!(v["roots"][0]["children"][0]["name"], "child");
    assert_eq!(v["roots"][0]["start_time"], "2023-11-14T22:13:20.123456789Z");
    assert_eq!(v["roots"][0]["inputs"]["a"], 1);
    let (st, _) = call(&api, get(&format!("/v1/traces/{}", tid(9)))).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, v) = call(&api, post("/v1/sql", json!({"query": "SELECT name, start_time FROM spans ORDER BY name"}))).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    eprintln!("{v}");
    assert_eq!(v["row_count"], 2);
    let (st, v) = call(&api, post("/v1/sql", json!({"query": "DROP TABLE spans"}))).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    let req = Request::get(format!("/v1/traces/{}", tid(1))).header("accept-encoding", "gzip").body(Body::empty()).unwrap();
    let resp = api.clone().oneshot(req).await.unwrap();
    eprintln!("gzip: {:?}", resp.headers().get("content-encoding"));
}
