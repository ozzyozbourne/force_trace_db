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

    let (st, v) = call(&api, get("/v1/projects")).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v.as_array().unwrap().len(), 1, "{v}");

    assert_eq!(
        call(&api, post("/v1/sql", json!({}))).await.0,
        StatusCode::NOT_IMPLEMENTED
    );
    assert_eq!(call(&api, get("/v1/traces/abc")).await.0, StatusCode::NOT_IMPLEMENTED);
}
