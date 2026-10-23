//! M3 compaction (plans/phase-4-compaction.md): picker -> lease -> run_job -> swap.
use crate::IngestError;
use forge_meta::{JobRequest, Metastore, SegmentFilter};
use forge_model::catalog::{JobKind, Level};
use forge_model::config::CompactionConfig;
use forge_model::ids::ProjectId;
use forge_model::merge::Cancel;
use forge_model::time::{Clock, now_ms};
use forge_storage::SegmentStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

pub mod job;
pub mod pick;

pub const MAX_INPUTS: usize = 16;
pub const L1_MAX_RUNS: usize = 4;
pub const MAX_OPEN_WRITERS: usize = 4;
/// Jobs per `compact_project` run (a guard; the summary reports `capped`).
pub const COMPACT_MAX_JOBS: usize = 64;
pub const JOB_RETENTION: Duration = Duration::from_secs(7 * 86_400);
const DAY_NS: i64 = 86_400 * 1_000_000_000;

pub struct CompactCtx {
    pub meta: Arc<dyn Metastore>,
    pub segments: Arc<dyn SegmentStore>,
    pub cfg: CompactionConfig,
    /// D11: hour closed / expired. L0 age and the job-age guard use `now_ms()`.
    pub data_clock: Clock,
    /// Rotating start for `run_next`.
    pub next_project: AtomicUsize,
    /// Shutdown: checked between slices, never during the commit (D7).
    pub cancel: Cancel,
}

/// Coverage counters of one job (tests, logs).
#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub struct JobStats {
    pub slices: u32,
    pub fills: u64,
    pub backfills: u64,
    pub orphan_outputs: u32,
    pub rotated: u32,
    pub folded_hours: u32,
    /// Writers finished early because `MAX_OPEN_WRITERS` were open.
    pub writer_cap: u32,
}

#[derive(Debug, serde::Serialize)]
pub struct JobReport {
    pub project: String,
    pub kind: &'static str,
    pub l0_inputs: usize,
    pub l1_inputs: usize,
    /// (hour, orphan, rows, bytes)
    pub outputs: Vec<(i64, bool, u64, u64)>,
    pub rows_in: u64,
    pub elapsed_ms: u64,
    pub stats: JobStats,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct CompactSummary {
    pub jobs: Vec<JobReport>,
    pub skipped: usize,
    pub failed: Vec<String>,
    pub capped: bool,
}

/// The project's pick, or None. `force` = triggers off (`forge compact`).
async fn pick_for(
    cx: &CompactCtx,
    p: &ProjectId,
    force: bool,
) -> Result<Option<(JobKind, Vec<forge_model::catalog::SegmentId>)>, IngestError> {
    let segs = cx
        .meta
        .list_segments(&SegmentFilter {
            project_id: Some(p.clone()),
            ..Default::default()
        })
        .await?;
    let data_now_ns = (cx.data_clock)().saturating_mul(1_000_000);
    let days = cx.meta.get_project(p).await?.and_then(|p| p.retention_days);
    let input = pick::PickInput {
        segs: &segs,
        wall_now_ms: now_ms(),
        data_now_ns,
        expired_before_ns: days.map(|d| data_now_ns.saturating_sub(i64::from(d) * DAY_NS)),
        force,
    };
    Ok(pick::pick(&input, &cx.cfg))
}

/// Pick -> lease -> run. Ok(None): nothing due, or the lease raced.
async fn one(cx: &CompactCtx, p: &ProjectId, force: bool) -> Result<Option<Option<JobReport>>, IngestError> {
    let Some((kind, inputs)) = pick_for(cx, p, force).await? else {
        return Ok(None);
    };
    let req = JobRequest {
        project_id: p.clone(),
        kind,
        inputs,
    };
    let Some(job) = cx.meta.begin_compaction(req).await? else {
        return Ok(Some(None)); // raced / rules failed
    };
    Ok(Some(Some(job::run_job(cx, job).await?)))
}

/// Server slot: projects in rotating order; the first that yields a job. None = nothing due or all raced.
pub async fn run_next(cx: &CompactCtx) -> Result<Option<JobReport>, IngestError> {
    let projects = cx.meta.list_projects().await?;
    if projects.is_empty() {
        return Ok(None);
    }
    let start = cx.next_project.fetch_add(1, Ordering::Relaxed);
    for i in 0..projects.len() {
        let p = &projects[(start + i) % projects.len()].id;
        if let Some(Some(r)) = one(cx, p, false).await? {
            return Ok(Some(r));
        }
    }
    Ok(None)
}

/// `forge compact`: triggers off; pick + run on one project until the picker returns None. A rejected begin counts
/// as skipped and ends the loop; failed jobs are collected (3 end the loop). At most `COMPACT_MAX_JOBS` jobs.
pub async fn compact_project(cx: &CompactCtx, p: &ProjectId) -> Result<CompactSummary, IngestError> {
    let mut s = CompactSummary::default();
    for _ in 0..COMPACT_MAX_JOBS {
        match one(cx, p, true).await {
            Ok(None) => return Ok(s),
            Ok(Some(None)) => {
                s.skipped += 1;
                return Ok(s);
            }
            Ok(Some(Some(r))) => s.jobs.push(r),
            Err(e) => {
                tracing::warn!(project = %p, error = %e, "compaction failed");
                s.failed.push(e.to_string());
                if s.failed.len() >= 3 {
                    return Ok(s);
                }
            }
        }
    }
    s.capped = true;
    Ok(s)
}

pub(crate) fn level_counts(inputs: &[forge_model::catalog::SegmentMeta]) -> (usize, usize) {
    let l0 = inputs.iter().filter(|s| s.level == Level::L0).count();
    (l0, inputs.len() - l0)
}
