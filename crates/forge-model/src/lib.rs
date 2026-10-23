//! forge-model: ids, enums, the spans Arrow schema, rows, catalog types and config shared by every crate.
//!
//! # Cross-crate invariants
//!
//! 1. **Snapshot order for reads:** take `MemtableView::snapshot()` *before* calling `Metastore::list_segments` /
//!    `locate_trace`.
//!    - The reverse order has a gap: a flush can commit and drop its frozen memtable between the two calls, and
//!      those rows would be missed.
//!    - With this order the worst case is seeing the same rows twice (memtable + new segment). Identical versions
//!      dedupe in the merge.
//!    - The flusher must therefore remove a frozen memtable from the view only **after** `commit_flush` returns.
//! 2. **Row ids** used in `trace_index`, `.fts` postings and `RowSelection` are 0-based row positions in the
//!    `.vortex` file. The index writer must see the same batches, in the same order, as the segment writer.
//! 3. **Segment contents:** one project, one row per span key (already merged), sorted by the physical key,
//!    tombstones kept.
//! 4. **Filters below the merge:** only predicates on key columns (`project_id`, `trace_id`, `span_id`) and FTS
//!    selections, where "matches any version" is acceptable and documented, may be pushed into `ScanRequest.filter`
//!    or the selection. Time pruning happens only at segment level, and only when `start_time_nulls == 0`
//!    (`SegmentMeta::may_overlap_time`).
//! 5. **Tombstones** may be dropped by compaction only when the job's inputs include every visible segment that
//!    holds the affected trace (M3 defines the check). Otherwise use `Tombstones::Keep`.
//! 6. **Commit points:** objects are written first and the metastore commit comes second. Nothing is visible
//!    before the commit. The WAL is truncated only after the commit.
//! 7. **WAL watermark.** `FlushCommit.flushed_wal_seq` is a watermark: every WAL record with seq ≤ it is in a
//!    committed segment. When a memtable freezes, it must hold a **contiguous prefix** of WAL seqs, with no hole
//!    below its max. Either insert into the memtable in seq order (for example, the WAL writer thread performs or
//!    orders the inserts), or track the lowest in-flight seq and flush with `min_inflight − 1`. Otherwise, a crash
//!    after the flush loses acknowledged writes on replay (`seq > flushed_seq`).

pub mod catalog;
pub mod config;
pub mod dotted;
pub mod enums;
pub mod error;
pub mod ids;
pub mod memtable;
pub mod row;
pub mod schema;
#[cfg(any(test, feature = "test-util"))]
pub mod testing;
pub mod time;

pub use memtable::{EmptyMemtable, MemtableSnapshot, MemtableView};
