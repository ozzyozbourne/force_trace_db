use crate::AppState;
use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::routing::get;
use std::sync::atomic::Ordering;

pub fn ops_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/readyz",
            get(|State(s): State<AppState>| async move {
                if s.ready.load(Ordering::Acquire) {
                    (StatusCode::OK, "ready")
                } else {
                    (StatusCode::SERVICE_UNAVAILABLE, "starting")
                }
            }),
        )
        .route(
            "/metrics",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
                    "# forge: metrics not implemented until M6\n",
                )
            }),
        )
        .with_state(state)
}
