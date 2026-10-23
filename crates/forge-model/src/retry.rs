//! Classify-driven retry with exponential backoff: unbounded (the flush steps) and bounded (the compaction commit).
use crate::error::{Classify, ErrorKind};
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    pub initial: Duration,
    pub max: Duration,
}
impl Default for Backoff {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(30),
        }
    }
}

/// Retries `op` forever: the rows are safe in the WAL, the frozen memtable stays queryable, and
/// backpressure (429) bounds memory meanwhile. Non-transient errors are logged loudly and retried
/// at the max backoff (an operator can fix permissions/disk/metastore without losing data). The
/// caller bounds the total time with `tokio::time::timeout` on shutdown.
///
/// Known limit (L4): a deterministic non-transient upload or commit error (a bug) retries forever, here. Ingest
/// then returns 429 once over the memory budget, and on restart `Pipeline::open` keeps retrying the replay-time
/// flush. A merge error or panic is not retried here: the background flusher logs it and retries the whole flush
/// after `backoff.max`, but during replay it fails `Pipeline::open` (startup exits 1). Signals still stop the
/// process (during `Forge::open` too), and the rows stay safe in the WAL either way.
pub async fn retry<T, E, F, Fut>(what: &str, backoff: Backoff, mut op: F) -> T
where
    E: Classify,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut delay = backoff.initial;
    let mut attempt = 0u32;
    loop {
        match op().await {
            Ok(v) => return v,
            Err(e) if e.kind() == ErrorKind::Transient => {
                tracing::warn!(what, attempt, error = %e, ?delay, "{what} failed (transient); retrying");
            }
            Err(e) => {
                delay = backoff.max;
                tracing::error!(what, attempt, error = %e, ?delay, "{what} failed (non-transient); retrying; data is safe in the WAL");
            }
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(backoff.max);
        attempt += 1;
    }
}

/// Runs `op` at most `attempts` times (at least once), retrying only Transient errors after the backoff delay
/// (`initial`, doubling up to `max`). Returns the first Ok or the last error.
pub async fn retry_bounded<T, E, F, Fut>(what: &str, backoff: Backoff, attempts: u32, mut op: F) -> Result<T, E>
where
    E: Classify,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut delay = backoff.initial;
    let mut attempt = 1u32;
    loop {
        match op().await {
            Err(e) if attempt < attempts && e.kind() == ErrorKind::Transient => {
                tracing::warn!(what, attempt, error = %e, ?delay, "{what} failed (transient); retrying");
            }
            r => return r,
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(backoff.max);
        attempt += 1;
    }
}
