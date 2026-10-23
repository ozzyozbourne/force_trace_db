//! Per-row version allocation, monotonic across restarts.
use forge_model::row::SpanRow;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct VersionAllocator {
    /// Next version to hand out. Versions start at 1.
    next: AtomicU64,
}

impl VersionAllocator {
    /// `seed = max(Metastore::max_version(), max version seen in WAL replay)`.
    pub fn new(seed: u64) -> Self {
        Self {
            next: AtomicU64::new(seed + 1),
        }
    }
    /// Reserves `n` consecutive versions. Panics past i64::MAX (the Arrow column is Int64): unreachable
    /// at 1e6 versions/s for 290k years.
    pub fn reserve(&self, n: u64) -> Range<u64> {
        let start = self.next.fetch_add(n, Ordering::AcqRel);
        let end = start
            .checked_add(n)
            .filter(|e| *e <= i64::MAX as u64)
            .expect("version space exhausted");
        start..end
    }
    /// Replay: never hand out a version <= v.
    pub fn observe(&self, v: u64) {
        self.next.fetch_max(v + 1, Ordering::AcqRel);
    }
    /// Highest version handed out so far (`FlushCommit.max_version`).
    pub fn high_water(&self) -> u64 {
        self.next.load(Ordering::Acquire) - 1
    }
}

/// Stamps `r` onto `rows` in row order (a later row for the same span wins the merge).
pub fn stamp(rows: &mut [SpanRow], r: Range<u64>) {
    assert_eq!(rows.len() as u64, r.end - r.start);
    for (row, v) in rows.iter_mut().zip(r) {
        row.version = v;
    }
}
