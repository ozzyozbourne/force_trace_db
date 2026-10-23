//! The lifecycle task: compaction picker + GC pass on intervals. Stops between jobs.
use crate::app::Forge;
use forge_ingest::compact::{CompactCtx, run_next};
use forge_ingest::lifecycle::{GcOptions, run_gc};
use forge_model::merge::Cancel;
use forge_model::time::{Clock, Clocks, wall_clock};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub struct LifecycleTasks {
    stop: CancellationToken,
    /// The running jobs' cancel flag (`CompactCtx::cancel`).
    cancel: Cancel,
    handle: tokio::task::JoinHandle<()>,
}

impl LifecycleTasks {
    /// Stop picking new jobs and cancel running ones (they stop at their next slice boundary, never inside the
    /// commit); wait up to `grace`, then abort. An aborted job is safe: the swap is one txn (all or nothing), its
    /// outputs become orphans, and the next start resets the job (`Forge::open`).
    pub async fn shutdown(self, grace: Duration) {
        self.stop.cancel();
        self.cancel.cancel();
        let abort = self.handle.abort_handle();
        if tokio::time::timeout(grace, self.handle).await.is_err() {
            tracing::warn!(?grace, "lifecycle: running compactions abandoned at shutdown");
            abort.abort();
        }
    }
}

/// Call after `Forge::open` (WAL replay done) and before the listeners.
pub fn start(f: Arc<Forge>) -> LifecycleTasks {
    start_with(f, wall_clock())
}

/// `data_clock`: retention cutoff and "hour closed/expired" (D10); everything else is the wall clock.
pub fn start_with(f: Arc<Forge>, data_clock: Clock) -> LifecycleTasks {
    let clocks = Clocks {
        data: data_clock,
        wall: wall_clock(),
    };
    let cx = Arc::new(CompactCtx::new(
        f.meta.clone(),
        f.segments.clone(),
        f.config.compaction.clone(),
        clocks,
    ));
    let stop = CancellationToken::new();
    let cancel = cx.cancel.clone();
    let handle = tokio::spawn(run(f, cx, stop.clone()));
    LifecycleTasks { stop, cancel, handle }
}

/// A period of zero disables the tick (tokio's `interval` panics on zero).
fn ticker(period: Duration, first_after: bool) -> Option<tokio::time::Interval> {
    if period.is_zero() {
        return None;
    }
    let start = tokio::time::Instant::now() + if first_after { period } else { Duration::ZERO };
    let mut t = tokio::time::interval_at(start, period);
    t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    Some(t)
}

async fn tick(t: &mut Option<tokio::time::Interval>) {
    match t {
        Some(t) => {
            t.tick().await;
        }
        None => std::future::pending().await,
    }
}

async fn run(f: Arc<Forge>, cx: Arc<CompactCtx>, stop: CancellationToken) {
    let c = f.config.compaction.clone();
    let mut jobs: JoinSet<()> = JoinSet::new();
    let mut compact_tick = ticker(c.interval, false);
    let mut gc_tick = ticker(c.gc_interval, true);
    let blobs = f.ingest.blob_guard();
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = tick(&mut compact_tick) => {
                for _ in jobs.len()..c.concurrency.max(1) {
                    let cx = cx.clone();
                    jobs.spawn(async move {
                        match run_next(&cx).await {
                            Ok(Some(r)) => tracing::info!(?r, "compaction done"),
                            Ok(None) => {}
                            Err(e) => tracing::warn!(error = %e, "compaction failed; retried on a later tick"),
                        }
                    });
                }
            }
            _ = tick(&mut gc_tick) => {
                let store = f.segments.object_store();
                match run_gc(f.meta.as_ref(), store.as_ref(), &cx.clocks, &GcOptions::from_config(&c), blobs).await {
                    Ok(r) => tracing::info!(?r, "gc pass done"),
                    Err(e) => tracing::warn!(error = %e, "gc pass failed; retried next interval"),
                }
            }
            Some(_) = jobs.join_next(), if !jobs.is_empty() => {}
        }
    }
    while jobs.join_next().await.is_some() {}
}
