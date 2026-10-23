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

/// The two clocks of the lifecycle (plans/phase-4 D11): `data` decides retention and "hour closed" (the soak runs
/// it faster); `wall` decides L0 age, grace periods and the orphan sweep.
#[derive(Clone)]
pub struct Clocks {
    pub data: Clock,
    pub wall: Clock,
}

impl Clocks {
    /// Both on the wall clock (production).
    pub fn wall() -> Self {
        Self {
            data: wall_clock(),
            wall: wall_clock(),
        }
    }
    /// Fixed instants (tests, and one `forge gc` pass).
    pub fn at(data_ms: i64, wall_ms: i64) -> Self {
        Self {
            data: std::sync::Arc::new(move || data_ms),
            wall: std::sync::Arc::new(move || wall_ms),
        }
    }
}

impl std::fmt::Debug for Clocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Clocks")
            .field("data", &(self.data)())
            .field("wall", &(self.wall)())
            .finish()
    }
}
