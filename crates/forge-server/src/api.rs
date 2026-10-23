use crate::AppState;
use crate::error::ApiError;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use forge_meta::Project;
use forge_model::ids::ProjectId;
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
    Json(req): Json<CreateProject>,
) -> Result<(StatusCode, Json<ProjectView>), ApiError> {
    let id = ProjectId::new(req.id)?;
    if req.retention_days == Some(0) {
        return Err(ApiError::BadRequest("retention_days must be > 0".into()));
    }
    let created_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
    let p = Project {
        name: req.name.unwrap_or_else(|| id.to_string()),
        id,
        api_key_hash: None,
        retention_days: req.retention_days,
        created_at_ms,
    };
    s.meta.create_project(p.clone()).await?;
    Ok((StatusCode::CREATED, Json(p.into())))
}

async fn m2() -> ApiError {
    ApiError::Unimplemented("M2")
}
async fn m4() -> ApiError {
    ApiError::Unimplemented("M4")
}
async fn m6() -> ApiError {
    ApiError::Unimplemented("M6")
}

pub fn api_router(state: AppState) -> Router {
    Router::new()
        .route("/v1/projects", get(list).post(create))
        .route("/v1/runs", post(m6).get(m2))
        .route("/v1/runs/{id}", patch(m6))
        .route("/v1/runs/batch", post(m6))
        .route("/v1/traces/{trace_id}", get(m2))
        .route("/v1/sql", post(m2))
        .route("/v1/search", post(m4))
        .with_state(state)
}
