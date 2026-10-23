use crate::AppState;
use crate::error::ApiError;
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::routing::{get, patch, post};
use axum::response::IntoResponse;
use axum::{Json, Router};
use forge_meta::Project;
use forge_model::error::Unimplemented;
use forge_model::ids::ProjectId;
use forge_model::time::now_ms;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct CreateProject {
    pub id: String,
    /// Defaults to `id`.
    pub name: Option<String>,
    pub retention_days: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct ProjectView {
    pub id: String,
    pub name: String,
    pub retention_days: Option<u32>,
    pub created_at_ms: i64,
}

impl From<Project> for ProjectView {
    fn from(p: Project) -> Self {
        Self {
            id: p.id.to_string(),
            name: p.name,
            retention_days: p.retention_days,
            created_at_ms: p.created_at_ms,
        }
    }
}

async fn list(State(s): State<AppState>) -> Result<Json<Vec<ProjectView>>, ApiError> {
    Ok(Json(
        s.meta
            .list_projects()
            .await?
            .into_iter()
            .map(ProjectView::from)
            .collect(),
    ))
}

async fn create(
    State(s): State<AppState>,
    req: Result<Json<CreateProject>, JsonRejection>,
) -> Result<(StatusCode, Json<ProjectView>), ApiError> {
    // Bad bodies get the same 400 {"error": ..} shape as other invalid input (axum defaults to 415/422 text).
    let Json(req) = req.map_err(|e| ApiError::invalid_input(e.body_text()))?;
    let id = ProjectId::new(req.id)?;
    let p = Project::new(id, req.name, req.retention_days, now_ms())?;
    s.meta.create_project(p.clone()).await?;
    Ok((StatusCode::CREATED, Json(p.into())))
}

/// A route that answers 501 `not implemented until <milestone>`.
fn stub(milestone: &'static str) -> impl Fn() -> std::future::Ready<ApiError> + Clone + Send + Sync + 'static {
    move || std::future::ready(Unimplemented(milestone).into())
}

pub fn api_router(state: AppState) -> Router {
    Router::new()
        .route("/v1/projects", get(list).post(create))
        .route("/v1/runs", post(stub("M6")).get(stub("M2")))
        .route("/v1/runs/{id}", patch(stub("M6")))
        .route("/v1/runs/batch", post(stub("M6")))
        .route("/v1/traces/{trace_id}", get(trace))
        .route("/v1/sql", post(sql))
        .route("/v1/search", post(stub("M4")))
        .layer(tower_http::compression::CompressionLayer::new().gzip(true))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
pub struct TraceParams {
    pub project: Option<String>,
    pub payloads: Option<bool>,
    pub format: Option<String>,
}

fn json_body(b: Vec<u8>) -> axum::response::Response {
    ([(axum::http::header::CONTENT_TYPE, "application/json")], b).into_response()
}

async fn trace(
    State(s): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
    q: Result<axum::extract::Query<TraceParams>, axum::extract::rejection::QueryRejection>,
) -> Result<axum::response::Response, ApiError> {
    let axum::extract::Query(q) = q.map_err(|e| ApiError::invalid_input(e.body_text()))?;
    let project = ProjectId::new(q.project.unwrap_or_else(|| "default".into()))?;
    let req = forge_query::trace::TraceRequest::parse(&id, project, q.payloads, q.format.as_deref())?;
    Ok(json_body(s.query.trace_json(&req).await?))
}

async fn sql(
    State(s): State<AppState>,
    req: Result<Json<forge_query::sql::SqlRequest>, JsonRejection>,
) -> Result<axum::response::Response, ApiError> {
    let Json(req) = req.map_err(|e| ApiError::invalid_input(e.body_text()))?;
    Ok(json_body(s.query.sql_json(&req).await?))
}
