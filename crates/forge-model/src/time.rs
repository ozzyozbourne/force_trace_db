/// Wall-clock unix milliseconds (`*_ms` bookkeeping columns). 0 before the epoch, saturating far after.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// Milliseconds since the epoch. Production: the wall clock. The soak passes a faster data clock (plan D10).
pub type Clock = std::sync::Arc<dyn Fn() -> i64 + Send + Sync>;
pub fn wall_clock() -> Clock {
    std::sync::Arc::new(now_ms)
}
