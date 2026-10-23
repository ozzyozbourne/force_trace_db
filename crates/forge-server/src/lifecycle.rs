//! The lifecycle task: compaction picker + GC pass on intervals. Stops between jobs.
use crate::app::Forge;
use forge_ingest::compact::{CompactCtx, reset_dead_jobs, run_next};
use forge_ingest::lifecycle::{GcOptions, run_gc};
use forge_model::time::{Clock, Clocks, wall_clock};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub struct LifecycleTasks {
    stop: CancellationToken,
    cx: Arc<CompactCtx>,
    handle: tokio::task::JoinHandle<()>,
}

impl LifecycleTasks {
    /// Stop picking new jobs, abort a running GC pass (every GC step is idempotent and metastore-first) and cancel
    /// running jobs (they stop at their next slice boundary, never inside the commit); wait up to `grace`, then
    /// abort. An aborted job is safe: the swap is one txn (all or nothing), its outputs become orphans, and the
    /// next start resets the job (`Forge::open`).
    pub async fn shutdown(self, grace: Duration) {
        self.stop.cancel();
        self.cx.cancel.cancel();
        let abort = self.handle.abort_handle();
        if tokio::time::timeout(grace, self.handle).await.is_err() {
            let jobs = self.cx.running.count();
            tracing::warn!(
                jobs,
                ?grace,
                "lifecycle: shutdown grace elapsed; aborting running compactions"
            );
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
    let handle = tokio::spawn(run(f, cx.clone(), stop.clone()));
    LifecycleTasks { stop, cx, handle }
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

/// One GC pass, logged.
async fn gc_pass(f: Arc<Forge>, cx: Arc<CompactCtx>) {
    let store = f.segments.object_store();
    let o = GcOptions::from_config(&f.config.compaction);
    match run_gc(f.meta.as_ref(), store.as_ref(), &cx.clocks, &o, f.ingest.blob_guard()).await {
        Ok(r) => tracing::info!(?r, "gc pass done"),
        Err(e) => tracing::warn!(error = %e, "gc pass failed; retried next interval"),
    }
}

async fn run(f: Arc<Forge>, cx: Arc<CompactCtx>, stop: CancellationToken) {
    let c = f.config.compaction.clone();
    let mut jobs: JoinSet<()> = JoinSet::new();
    // GC runs as its own task (at most one), so a long pass neither delays the compaction ticks nor the stop.
    let mut gc: Option<tokio::task::JoinHandle<()>> = None;
    let mut compact_tick = ticker(c.interval, false);
    let mut gc_tick = ticker(c.gc_interval, true);
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = tick(&mut compact_tick) => {
                // Jobs whose task died (panic, abort) or whose fail_compaction failed (review 1 #4).
                if let Err(e) = reset_dead_jobs(&cx).await {
                    tracing::warn!(error = %e, "compaction: resetting dead jobs failed");
                }
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
                if gc.as_ref().is_none_or(|h| h.is_finished()) {
                    gc = Some(tokio::spawn(gc_pass(f.clone(), cx.clone())));
                }
            }
            Some(r) = jobs.join_next(), if !jobs.is_empty() => {
                if let Err(e) = r {
                    tracing::error!(error = %e, "compaction task died; its job is reset on the next tick");
                }
            }
        }
    }
    if let Some(h) = gc {
        h.abort();
        let _ = h.await;
    }
    while jobs.join_next().await.is_some() {}
}
