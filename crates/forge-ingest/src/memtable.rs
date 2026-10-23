//! Memtable set: one active + at most one frozen memtable.
//!
//! Inserts come ONLY from the WAL writer thread (after fsync, in seq order) and from replay (in seq
//! order), so every memtable holds a contiguous run of WAL seqs and `Frozen::last_seq` is a valid
//! watermark (invariant 7). `insert` enforces contiguity instead of trusting it.
use arrow::record_batch::RecordBatch;
use forge_model::{MemtableSnapshot, MemtableView};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Memory charged for one batch: the bytes its arrays actually reference
/// (`get_slice_memory_size`) + a fixed per-batch overhead. Identical for a freshly built batch and
/// the same batch decoded from IPC on replay. NOT `get_array_memory_size`, which charges every
/// column of an IPC-decoded batch the whole shared message buffer (58x over-count measured).
pub fn accounted_bytes(b: &RecordBatch) -> usize {
    const PER_BATCH: usize = 512;
    PER_BATCH
        + b.columns()
            .iter()
            .map(|c| {
                c.to_data()
                    .get_slice_memory_size()
                    .unwrap_or_else(|_| c.get_array_memory_size())
            })
            .sum::<usize>()
}

#[derive(Debug)]
pub struct Frozen {
    pub batches: Vec<RecordBatch>,
    /// Accounted bytes (sum of `accounted_bytes`).
    pub bytes: usize,
    /// Flush watermark: every seq <= this is in this or an earlier memtable.
    pub last_seq: u64,
}

#[derive(Debug)]
struct Active {
    batches: Vec<RecordBatch>,
    bytes: usize,
    /// When the first batch went in; meaningless while `batches` is empty.
    created: Instant,
    /// Due regardless of size and age (`mark_due`); cleared by `freeze` (a fresh `Active`).
    due: bool,
}

impl Active {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
            bytes: 0,
            created: Instant::now(),
            due: false,
        }
    }
}

#[derive(Debug)]
struct State {
    active: Active,
    frozen: Option<Arc<Frozen>>,
    /// Highest seq inserted (starts at the checkpoint's flushed_seq).
    last_seq: u64,
    /// Cached `snapshot()`; dropped by every mutation.
    snapshot: Option<Arc<[RecordBatch]>>,
}

#[derive(Debug)]
pub struct MemtableSet {
    state: Mutex<State>,
    /// active + frozen accounted bytes; read lock-free by receivers for backpressure.
    unflushed: AtomicUsize,
    flush_bytes: usize,
    flush_interval: Duration,
    /// Woken when the active memtable crosses `flush_bytes`.
    pub flush_wanted: tokio::sync::Notify,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum InsertError {
    /// seq is not last_seq + 1: a hole would make the flush watermark lie (invariant 7).
    #[error("WAL seq {got} inserted, expected {expected} (hole in the WAL)")]
    NotContiguous { expected: u64, got: u64 },
}

impl MemtableSet {
    pub fn new(flushed_seq: u64, flush_bytes: usize, flush_interval: Duration) -> Self {
        Self {
            state: Mutex::new(State {
                active: Active::new(),
                frozen: None,
                last_seq: flushed_seq,
                snapshot: None,
            }),
            unflushed: AtomicUsize::new(0),
            flush_bytes,
            flush_interval,
            flush_wanted: tokio::sync::Notify::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `bytes` = `accounted_bytes(&batch)`, computed by the caller off the WAL thread.
    pub fn insert(&self, seq: u64, batch: RecordBatch, bytes: usize) -> Result<(), InsertError> {
        let mut s = self.lock();
        if seq != s.last_seq + 1 {
            return Err(InsertError::NotContiguous {
                expected: s.last_seq + 1,
                got: seq,
            });
        }
        s.last_seq = seq;
        let a = &mut s.active;
        if a.batches.is_empty() {
            a.created = Instant::now();
        }
        a.bytes += bytes;
        a.batches.push(batch);
        let full = a.bytes >= self.flush_bytes;
        s.snapshot = None;
        // Under the lock: once unlocked, a flush can freeze and `release` this batch, which subtracts its bytes.
        self.unflushed.fetch_add(bytes, Ordering::SeqCst);
        drop(s);
        if full {
            self.flush_wanted.notify_one();
        }
        Ok(())
    }

    /// Bytes not yet in a committed segment (active + frozen). Backpressure: > memory_budget -> 429.
    pub fn unflushed_bytes(&self) -> usize {
        self.unflushed.load(Ordering::SeqCst)
    }

    /// A frozen memtable is left over (its flush failed), or the size/age trigger on the active one.
    pub fn flush_due(&self, now: Instant) -> bool {
        let s = self.lock();
        let a = &s.active;
        s.frozen.is_some()
            || !a.batches.is_empty()
                && (a.due || a.bytes >= self.flush_bytes || now.duration_since(a.created) >= self.flush_interval)
    }

    /// Makes the active memtable due now, until it is frozen. After replay: replayed rows are as old as the crash,
    /// so a restart loop shorter than `flush_interval` must not keep postponing them. No effect when empty.
    pub fn mark_due(&self) {
        let mut s = self.lock();
        let a = &mut s.active;
        a.due = !a.batches.is_empty();
    }

    pub fn flush_interval(&self) -> Duration {
        self.flush_interval
    }

    pub fn has_frozen(&self) -> bool {
        self.lock().frozen.is_some()
    }

    /// Freezes the active memtable. Returns the pending frozen one instead if a flush has not
    /// committed yet (at most one frozen memtable: flushes commit in seq order, and memory stays
    /// bounded by backpressure). None when there is nothing to flush.
    pub fn freeze(&self) -> Option<Arc<Frozen>> {
        let mut s = self.lock();
        if let Some(f) = &s.frozen {
            return Some(f.clone());
        }
        if s.active.batches.is_empty() {
            return None;
        }
        let a = std::mem::replace(&mut s.active, Active::new());
        let f = Arc::new(Frozen {
            batches: a.batches,
            bytes: a.bytes,
            last_seq: s.last_seq,
        });
        s.frozen = Some(f.clone());
        // snapshot content is unchanged (same batches), keep the cache
        Some(f)
    }

    /// After `commit_flush` returned Ok (invariant 1): the segment is now visible, drop the rows.
    pub fn release(&self, f: &Arc<Frozen>) {
        let mut s = self.lock();
        if s.frozen.as_ref().is_some_and(|x| Arc::ptr_eq(x, f)) {
            s.frozen = None;
            s.snapshot = None;
            self.unflushed.fetch_sub(f.bytes, Ordering::SeqCst); // under the lock, like `insert`'s add
        }
    }

    pub fn last_seq(&self) -> u64 {
        self.lock().last_seq
    }
}

impl MemtableView for MemtableSet {
    /// Frozen first, then active. Cheap when nothing was inserted since the last call.
    fn snapshot(&self) -> MemtableSnapshot {
        let mut s = self.lock();
        if let Some(c) = &s.snapshot {
            return MemtableSnapshot { batches: c.clone() };
        }
        let mut v: Vec<RecordBatch> = Vec::new();
        if let Some(f) = &s.frozen {
            v.extend(f.batches.iter().cloned());
        }
        v.extend(s.active.batches.iter().cloned());
        let batches: Arc<[RecordBatch]> = v.into();
        s.snapshot = Some(batches.clone());
        MemtableSnapshot { batches }
    }
}
