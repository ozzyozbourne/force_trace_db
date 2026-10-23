use crate::AppState;
use crate::error::ApiError;
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use forge_ingest::normalize::PROJECT_HEADER;
use forge_meta::Project;
use forge_model::error::Unimplemented;
use forge_model::ids::ProjectId;
use forge_model::time::now_ms;
use forge_query::runs::{RunsParams, RunsRequest};
use forge_query::sql::SqlRequest;
use forge_query::trace::TraceRequest;
use serde::{Deserialize, Serialize};
use tower_http::compression::CompressionLayer;

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
        .route("/v1/runs", post(stub("M6")).get(runs))
        .route("/v1/runs/{id}", patch(stub("M6")))
        .route("/v1/runs/batch", post(stub("M6")))
        .route("/v1/traces/{trace_id}", get(trace))
        .route("/v1/sql", post(sql))
        .route("/v1/search", post(stub("M4")))
        .layer(CompressionLayer::new().gzip(true))
        .with_state(state)
}

/// The `project` param, else the `x-forge-project` header, else `ingest.default_project`.
fn project(s: &AppState, param: Option<String>, headers: &HeaderMap) -> Result<ProjectId, ApiError> {
    let raw = match (param, headers.get(PROJECT_HEADER)) {
        (Some(p), _) => p,
        (None, Some(h)) => h
            .to_str()
            .map_err(|_| ApiError::invalid_input(format!("{PROJECT_HEADER}: not UTF-8")))?
            .to_string(),
        (None, None) => s.config.ingest.default_project.clone(),
    };
    Ok(ProjectId::new(raw)?)
}

fn json_body(b: Vec<u8>) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], b).into_response()
}

fn query_params<T>(q: Result<Query<T>, QueryRejection>) -> Result<T, ApiError> {
    q.map(|Query(q)| q).map_err(|e| ApiError::invalid_input(e.body_text()))
}

#[derive(Debug, Deserialize)]
pub struct TraceParams {
    pub project: Option<String>,
    pub payloads: Option<bool>,
    pub format: Option<String>,
}

/// `GET /v1/traces/{trace_id}?project=&payloads=&format=tree|flat`.
async fn trace(
    State(s): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    q: Result<Query<TraceParams>, QueryRejection>,
) -> Result<Response, ApiError> {
    let q = query_params(q)?;
    let project = project(&s, q.project, &headers)?;
    let req = TraceRequest::parse(&id, project, q.payloads, q.format.as_deref())?;
    Ok(json_body(s.query.trace_json(&req).await?))
}

/// `GET /v1/runs?project=&start=&end=&run_type=&status=&q=&limit=&cursor=`.
async fn runs(
    State(s): State<AppState>,
    headers: HeaderMap,
    q: Result<Query<RunsParams>, QueryRejection>,
) -> Result<Response, ApiError> {
    let mut q = query_params(q)?;
    let project = project(&s, q.project.take(), &headers)?;
    let req = RunsRequest::parse(q, project)?;
    Ok(json_body(s.query.runs_json(&req).await?))
}

/// `POST /v1/sql {"query", "max_rows"?, "timeout_ms"?}`.
async fn sql(State(s): State<AppState>, req: Result<Json<SqlRequest>, JsonRejection>) -> Result<Response, ApiError> {
    let Json(req) = req.map_err(|e| ApiError::invalid_input(e.body_text()))?;
    Ok(json_body(s.query.sql_json(&req).await?))
}
