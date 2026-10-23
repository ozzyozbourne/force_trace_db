//! M3 compaction (plans/phase-4-compaction.md): picker -> lease -> run_job -> swap.
use crate::IngestError;
use forge_meta::{JobRequest, Metastore, SegmentFilter};
use forge_model::catalog::{JobKind, SegmentId};
use forge_model::config::CompactionConfig;
use forge_model::ids::ProjectId;
use forge_model::merge::Cancel;
use forge_model::time::Clocks;
use forge_storage::SegmentStore;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

pub mod job;
pub mod pick;

/// The picker's input cap: begin_compaction's own cap.
pub use forge_meta::MAX_JOB_INPUTS as MAX_INPUTS;
pub const L1_MAX_RUNS: usize = 4;
pub const MAX_OPEN_WRITERS: usize = 4;
/// D3: a compaction output closes at the next trace boundary once its `.fts` builder holds this many (term, row)
/// pairs (~200-250 MiB of builder memory; x MAX_OPEN_WRITERS = 4).
pub const MAX_SEGMENT_POSTINGS: u64 = 24 << 20;
/// Jobs per `compact_project` run (a guard; the summary reports `capped`).
const COMPACT_MAX_JOBS: usize = 64;
pub const JOB_RETENTION: Duration = Duration::from_secs(7 * 86_400);

pub struct CompactCtx {
    pub meta: Arc<dyn Metastore>,
    pub segments: Arc<dyn SegmentStore>,
    pub cfg: CompactionConfig,
    /// D11: `data` decides hour closed / expired, `wall` the L0 age (the job-age guard is the metastore's clock).
    pub clocks: Clocks,
    /// `merge_versions_projected`'s output bound per merged batch (`flush::MERGE_CHUNK_BYTES`).
    pub merge_chunk_bytes: usize,
    /// Rotating start for `run_next`.
    pub next_project: AtomicUsize,
    /// Shutdown: checked between slices, never during the commit (D7).
    pub cancel: Cancel,
    /// The jobs this process runs (review 1 #4).
    pub running: RunningJobs,
}

/// Ids of the jobs this process runs: added by `one` right after the begin, removed when the job's future ends, also
/// by panic or abort. `reset_dead_jobs` fails every other running job.
#[derive(Default)]
pub struct RunningJobs {
    ids: std::sync::Mutex<HashSet<i64>>,
    /// Held (read) from a begin until its id is in `ids`; `reset_dead_jobs` holds it (write) so it never sees a
    /// begun job that is not registered yet.
    begin_lock: tokio::sync::RwLock<()>,
    /// Review 1 #10: input sets whose job failed -> (consecutive failures, not picked again before). Server ticks
    /// pass over such a set until then and pick the project's next candidate (`forge compact` does not: it stops
    /// after 3 failures). Not distinguished: a transient store outage grows the backoff like a poisoned input.
    backoff: std::sync::Mutex<HashMap<Vec<SegmentId>, (u32, Instant)>>,
}

impl RunningJobs {
    /// Jobs running now.
    pub fn count(&self) -> usize {
        self.ids.lock().unwrap().len()
    }
}

/// The longest failure backoff of one input set.
const MAX_BACKOFF: Duration = Duration::from_secs(3600);

/// Removes a job from `RunningJobs` when dropped.
struct Registered<'a>(&'a RunningJobs, i64);

impl Drop for Registered<'_> {
    fn drop(&mut self) {
        self.0.ids.lock().unwrap().remove(&self.1);
    }
}

impl CompactCtx {
    pub fn new(
        meta: Arc<dyn Metastore>,
        segments: Arc<dyn SegmentStore>,
        cfg: CompactionConfig,
        clocks: Clocks,
    ) -> Self {
        Self {
            meta,
            segments,
            cfg,
            clocks,
            merge_chunk_bytes: crate::flush::MERGE_CHUNK_BYTES,
            next_project: AtomicUsize::new(0),
            cancel: Cancel::new(),
            running: RunningJobs::default(),
        }
    }
}

/// Fails every running job of the metastore that no task of this process runs (its task panicked or was aborted,
/// or its `fail_compaction` failed). One process owns the data dir (D2). Returns the count.
pub async fn reset_dead_jobs(cx: &CompactCtx) -> Result<usize, IngestError> {
    let _no_begin = cx.running.begin_lock.write().await;
    let keep: Vec<i64> = cx.running.ids.lock().unwrap().iter().copied().collect();
    let n = cx.meta.reset_compactions(&keep).await?;
    if n > 0 {
        tracing::warn!(jobs = n, "compaction: reset running jobs that no task runs");
    }
    Ok(n)
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

/// The project's pick, or None. `force` = triggers off (`forge compact`); otherwise input sets backing off after a
/// failure are passed over and the next candidate is picked (review 2 R2-4).
async fn pick_for(
    cx: &CompactCtx,
    p: &ProjectId,
    force: bool,
) -> Result<Option<(JobKind, Vec<SegmentId>)>, IngestError> {
    let segs = cx
        .meta
        .list_segments(&SegmentFilter {
            project_id: Some(p.clone()),
            ..Default::default()
        })
        .await?;
    let data_now_ns = (cx.clocks.data)().saturating_mul(1_000_000);
    let project = cx.meta.get_project(p).await?;
    let input = pick::PickInput {
        segs: &segs,
        wall_now_ms: (cx.clocks.wall)(),
        data_now_ns,
        expired_before_ns: project.and_then(|p| p.retention_cutoff_ns(data_now_ns)),
        force,
    };
    let backoff = cx.running.backoff.lock().unwrap();
    let now = Instant::now();
    let backing_off = |ids: &[SegmentId]| !force && backoff.get(ids).is_some_and(|(_, until)| now < *until);
    Ok(pick::pick_excluding(&input, &cx.cfg, backing_off))
}

/// What one pick -> begin -> run did.
enum Outcome {
    /// Nothing due.
    Idle,
    /// The begin rules failed (raced).
    Refused,
    Ran(JobReport),
}

async fn one(cx: &CompactCtx, p: &ProjectId, force: bool) -> Result<Outcome, IngestError> {
    let Some((kind, inputs)) = pick_for(cx, p, force).await? else {
        return Ok(Outcome::Idle);
    };
    let backoff = &cx.running.backoff;
    let req = JobRequest {
        project_id: p.clone(),
        kind,
        inputs: inputs.clone(),
    };
    let begin = cx.running.begin_lock.read().await;
    let Some(job) = cx.meta.begin_compaction(req).await? else {
        return Ok(Outcome::Refused);
    };
    let _registered = Registered(&cx.running, job.id);
    cx.running.ids.lock().unwrap().insert(job.id);
    drop(begin);
    let r = job::run_job(cx, job).await;
    let mut b = backoff.lock().unwrap();
    if r.is_ok() {
        b.remove(&inputs);
    } else {
        let n = b.get(&inputs).map_or(0, |e| e.0) + 1;
        // Bounded: entries go MAX_BACKOFF after their backoff ended, so a set failing again within that keeps its
        // count (this set's was read above).
        let now = Instant::now();
        b.retain(|_, (_, until)| *until + MAX_BACKOFF > now);
        let wait = cx
            .cfg
            .interval
            .max(Duration::from_secs(1))
            .saturating_mul(1 << n.min(16))
            .min(MAX_BACKOFF);
        b.insert(inputs, (n, Instant::now() + wait));
    }
    Ok(Outcome::Ran(r?))
}

/// Server slot: projects in rotating order; the first that yields a job. None = nothing due or all raced. A
/// project's error is logged and the next project is tried; the first error is returned if no job ran.
pub async fn run_next(cx: &CompactCtx) -> Result<Option<JobReport>, IngestError> {
    let projects = cx.meta.list_projects().await?;
    if projects.is_empty() {
        return Ok(None);
    }
    let start = cx.next_project.fetch_add(1, Ordering::Relaxed);
    let mut first_err = None;
    for i in 0..projects.len() {
        let p = &projects[(start + i) % projects.len()].id;
        match one(cx, p, false).await {
            Ok(Outcome::Ran(r)) => return Ok(Some(r)),
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(project = %p, error = %e, "compaction failed; trying the next project");
                first_err.get_or_insert(e);
            }
        }
    }
    first_err.map_or(Ok(None), Err)
}

/// `forge compact`: triggers off; pick + run on one project until the picker returns None. A rejected begin counts
/// as skipped and ends the loop; failed jobs are collected (3 end the loop). At most `COMPACT_MAX_JOBS` jobs.
pub async fn compact_project(cx: &CompactCtx, p: &ProjectId) -> Result<CompactSummary, IngestError> {
    let mut s = CompactSummary::default();
    for _ in 0..COMPACT_MAX_JOBS {
        match one(cx, p, true).await {
            Ok(Outcome::Idle) => return Ok(s),
            Ok(Outcome::Refused) => {
                s.skipped += 1;
                return Ok(s);
            }
            Ok(Outcome::Ran(r)) => s.jobs.push(r),
            Err(e) => {
                tracing::warn!(project = %p, error = %e, "compaction failed");
                s.failed.push(e.to_string());
                if s.failed.len() >= 3 {
                    return Ok(s);
                }
            }
        }
    }
    s.capped = pick_for(cx, p, true).await?.is_some();
    Ok(s)
}
