//! The memtable as seen by queries: an immutable, pointer-only snapshot of the unflushed rows.
//!
//! forge-ingest owns the writer side (`MemtableSet`, which stores each memtable in an [`AppendLog`]);
//! forge-query only sees `Arc<dyn MemtableView>` and [`MemtableSnapshot`].
use arrow::record_batch::RecordBatch;
use std::ops::RangeInclusive;
use std::sync::{Arc, OnceLock};

/// Implemented by forge-ingest's memtable set; consumed by forge-query.
pub trait MemtableView: Send + Sync + std::fmt::Debug + 'static {
    /// Every row inserted into the memtable before this call (in particular every acknowledged write, since the
    /// WAL thread inserts before it acks), and possibly rows that a concurrent flush has already committed.
    /// Copies O(blocks) pointers under a short lock; never copies or clones batches.
    fn snapshot(&self) -> MemtableSnapshot;
}

/// First block capacity; capacities double up to [`MAX_BLOCK`].
const FIRST_BLOCK: usize = 64;
const MAX_BLOCK: usize = 4096;

/// A fixed-capacity run of batches. Slots fill in order, are written once, and are never moved, so a reader
/// holding `(Arc<Block>, len)` can read slots `..len` while the writer fills later ones.
#[derive(Debug)]
struct Block {
    slots: Box<[OnceLock<RecordBatch>]>,
}

/// Append-only batch storage for ONE memtable. Single writer (`&mut self`, under the memtable lock); readers take
/// [`AppendLog::view`] (O(blocks) Arc clones; the log of a 32k-batch memtable has 13 blocks).
#[derive(Debug, Default)]
pub struct AppendLog {
    blocks: Vec<Arc<Block>>,
    /// Filled slots of the last block.
    tail_len: usize,
    batches: usize,
    rows: usize,
}

impl AppendLog {
    pub fn push(&mut self, b: RecordBatch) {
        let full = self.blocks.last().is_none_or(|l| self.tail_len == l.slots.len());
        if full {
            let cap = self
                .blocks
                .last()
                .map_or(FIRST_BLOCK, |l| (l.slots.len() * 2).min(MAX_BLOCK));
            self.blocks.push(Arc::new(Block {
                slots: (0..cap).map(|_| OnceLock::new()).collect(),
            }));
            self.tail_len = 0;
        }
        self.rows += b.num_rows();
        let last = self.blocks.last().expect("pushed above");
        last.slots[self.tail_len]
            .set(b)
            .unwrap_or_else(|_| unreachable!("slot written twice"));
        self.tail_len += 1;
        self.batches += 1;
    }
    pub fn is_empty(&self) -> bool {
        self.batches == 0
    }
    pub fn num_batches(&self) -> usize {
        self.batches
    }
    pub fn num_rows(&self) -> usize {
        self.rows
    }
    /// The batches pushed so far. Later pushes are not visible through it.
    pub fn view(&self) -> LogView {
        let n = self.blocks.len();
        LogView {
            blocks: self
                .blocks
                .iter()
                .enumerate()
                .map(|(i, b)| (b.clone(), if i + 1 == n { self.tail_len } else { b.slots.len() }))
                .collect(),
            rows: self.rows,
        }
    }
}

/// An immutable prefix of an [`AppendLog`]. Cheap to clone (one Arc).
#[derive(Clone, Debug, Default)]
pub struct LogView {
    blocks: Arc<[(Arc<Block>, usize)]>,
    rows: usize,
}

impl LogView {
    pub fn iter(&self) -> impl Iterator<Item = &RecordBatch> + '_ {
        self.blocks.iter().flat_map(|(b, n)| {
            b.slots[..*n]
                .iter()
                // Set before the view was taken (same lock, or a happens-before through the Arc).
                .map(|s| s.get().expect("slot below the view length is filled"))
        })
    }
    pub fn num_rows(&self) -> usize {
        self.rows
    }
    /// Owned batches (Arc clones of the columns), e.g. for `merge_versions(&[RecordBatch])`. Call off the lock.
    pub fn to_vec(&self) -> Vec<RecordBatch> {
        self.iter().cloned().collect()
    }
}

/// One memtable in a snapshot.
#[derive(Clone, Debug)]
pub struct MemtablePart {
    /// Frozen = being flushed: its rows may ALSO be in a segment committed after the snapshot was taken.
    pub frozen: bool,
    /// Inclusive WAL seq range (contiguous, invariant 7). Parts in a snapshot are never empty.
    pub seqs: RangeInclusive<u64>,
    /// Inclusive version range of the rows (versions follow seqs, invariant 9).
    pub versions: RangeInclusive<u64>,
    /// spans_schema batches, unsorted, may hold several versions per key.
    pub batches: LogView,
}

/// The memtables at one instant, oldest first (frozen, then active). Parts are seq- and version-disjoint.
#[derive(Clone, Debug, Default)]
pub struct MemtableSnapshot {
    pub parts: Vec<MemtablePart>,
}

impl MemtableSnapshot {
    pub fn batches(&self) -> impl Iterator<Item = &RecordBatch> + '_ {
        self.parts.iter().flat_map(|p| p.batches.iter())
    }
    pub fn to_vec(&self) -> Vec<RecordBatch> {
        self.batches().cloned().collect()
    }
    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }
    pub fn num_rows(&self) -> usize {
        self.parts.iter().map(|p| p.batches.num_rows()).sum()
    }
    /// Highest version in the snapshot (0 when empty).
    pub fn max_version(&self) -> u64 {
        self.parts.last().map_or(0, |p| *p.versions.end())
    }
    /// Drops the parts whose every seq is <= `flushed_seq`: a checkpoint read AFTER the snapshot proves those rows
    /// are in committed segments that a segment listing taken after that read includes. Exact dedupe for
    /// readers that count rows without a version merge; merge readers may skip it (L6: duplicates are harmless).
    pub fn without_flushed(mut self, flushed_seq: u64) -> Self {
        self.parts.retain(|p| *p.seqs.end() > flushed_seq);
        self
    }
}

/// No unflushed rows (M0, offline tools).
#[derive(Debug, Default)]
pub struct EmptyMemtable;

impl MemtableView for EmptyMemtable {
    fn snapshot(&self) -> MemtableSnapshot {
        MemtableSnapshot::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch(v: i32) -> RecordBatch {
        let s = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        RecordBatch::try_new(s, vec![Arc::new(Int32Array::from(vec![v]))]).unwrap()
    }
    fn values(v: &LogView) -> Vec<i32> {
        v.iter()
            .map(|b| b.column(0).as_any().downcast_ref::<Int32Array>().unwrap().value(0))
            .collect()
    }

    #[test]
    fn empty_memtable_has_no_batches() {
        assert!(EmptyMemtable.snapshot().is_empty());
        assert_eq!(EmptyMemtable.snapshot().batches().count(), 0);
    }

    #[test]
    fn a_view_is_a_stable_prefix() {
        let mut log = AppendLog::default();
        let mut views = Vec::new();
        for i in 0..10_000 {
            log.push(batch(i));
            if i % 977 == 0 {
                views.push((i, log.view()));
            }
        }
        for (i, v) in &views {
            assert_eq!(values(v), (0..=*i).collect::<Vec<_>>(), "view at {i}");
        }
        assert_eq!(values(&log.view()).len(), 10_000);
        // 64+128+..+4096 = 8128, then one 4096 block.
        assert_eq!(log.view().blocks.len(), 8);
    }
}
