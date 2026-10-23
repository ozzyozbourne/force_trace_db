//! Classify-driven retry with exponential backoff (today: the flush steps).
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
/// Known limit (L4): a deterministic non-transient error (a bug) retries forever. Ingest then returns 429 once
/// over the memory budget, and on restart `Pipeline::open` keeps retrying the replay-time flush. Signals still
/// stop the process (during `Forge::open` too), and the rows stay safe in the WAL.
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
                tracing::warn!(what, attempt, error = %e, ?delay, "flush step failed (transient); retrying");
            }
            Err(e) => {
                delay = backoff.max;
                tracing::error!(what, attempt, error = %e, ?delay, "flush step failed (non-transient); retrying; data is safe in the WAL");
            }
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(backoff.max);
        attempt += 1;
    }
}
