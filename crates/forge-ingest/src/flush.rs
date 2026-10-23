//! Flush of one frozen memtable.
//!
//! merge (spawn_blocking) -> one L0 segment per project (fresh SegmentId per attempt) -> ONE
//! commit_flush{segments, flushed_wal_seq = frozen.last_seq, max_version} (same FlushCommit on every
//! retry: idempotent) -> release the frozen memtable from the view -> truncate the WAL.
use crate::IngestError;
use crate::memtable::{Frozen, MemtableSet};
use crate::version::VersionAllocator;
use crate::wal::WalLog;
use forge_meta::{FlushCommit, Metastore, NewSegment};
use forge_model::catalog::{HourBucket, Level, SegmentId};
use forge_model::failpoint;
use forge_model::merge::{Tombstones, merge_versions_chunked, split_chunks_by_project};
use forge_model::retry::{Backoff, retry};
use forge_storage::{SegmentSpec, SegmentStore};
use futures::StreamExt;
use std::sync::Arc;

/// Rows per batch handed to the segment writer (zero-copy slices; Vortex re-chunks anyway).
const WRITE_BATCH_ROWS: usize = 8192;
/// Default bound on each merged output batch's variable-length bytes, far below the 2 GiB i32-offset limit per
/// column.
pub const MERGE_CHUNK_BYTES: usize = 256 << 20;

pub struct FlushCtx {
    pub node_id: String,
    pub segments: Arc<dyn SegmentStore>,
    pub meta: Arc<dyn Metastore>,
    pub mem: Arc<MemtableSet>,
    pub wal: Arc<WalLog>,
    pub versions: Arc<VersionAllocator>,
    pub backoff: Backoff,
    /// `merge_versions_chunked`'s bound for this flush's merged batches.
    pub merge_chunk_bytes: usize,
}

/// The flush knobs `Pipeline::open_with` takes (`Pipeline::open` uses the defaults).
#[derive(Clone, Copy, Debug)]
pub struct FlushOptions {
    /// The flush steps' retry, and the flusher's wait after a failed flush.
    pub backoff: Backoff,
    pub merge_chunk_bytes: usize,
}

impl Default for FlushOptions {
    fn default() -> Self {
        Self {
            backoff: Backoff::default(),
            merge_chunk_bytes: MERGE_CHUNK_BYTES,
        }
    }
}

#[derive(Debug)]
pub struct FlushReport {
    pub segments: Vec<NewSegment>,
    pub watermark: u64,
    pub rows_in: usize,
    pub rows_out: u64,
    pub wal_segments_deleted: usize,
    /// Batches the merge produced (> 1 when the frozen memtable exceeded `merge_chunk_bytes`).
    pub merged_batches: usize,
}

/// Errors are Internal: merge errors and panics are bugs (inserts are validated). The upload and commit steps
/// retry inside (`retry`), so only the merge returns an error; the flusher logs it and retries after backoff.
pub async fn flush_frozen(cx: &FlushCtx, f: Arc<Frozen>) -> Result<FlushReport, IngestError> {
    let internal = |e: &dyn std::fmt::Display| IngestError::Internal(format!("merge: {e}"));
    // 1. Merge + physical sort: CPU work, off the async workers. Bounded output batches (no offset overflow).
    let (batches, max_chunk) = (f.part.batches.to_vec(), cx.merge_chunk_bytes);
    let merged = tokio::task::spawn_blocking(move || {
        if failpoint::fires("flush_merge_panic") {
            panic!("FORGE_FAILPOINT flush_merge_panic hit");
        }
        merge_versions_chunked(&batches, Tombstones::Keep, max_chunk)
    })
    .await
    .map_err(|e| internal(&e))?
    .map_err(|e| internal(&e))?;
    let parts = split_chunks_by_project(&merged).map_err(|e| internal(&e))?;
    failpoint::hit("flush_before_upload");

    // 2. One L0 segment per project. A fresh SegmentId per ATTEMPT: a put that landed but reported
    //    an error leaves an orphan (GC'd after 24 h) instead of depending on byte-identical re-encoding.
    let hour = HourBucket::from_nanos(forge_model::time::now_ms().saturating_mul(1_000_000));
    let mut written = Vec::with_capacity(parts.len());
    for (project, rows) in parts {
        let chunks: Vec<_> = rows
            .iter()
            .flat_map(|b| {
                (0..b.num_rows())
                    .step_by(WRITE_BATCH_ROWS)
                    .map(|o| b.slice(o, WRITE_BATCH_ROWS.min(b.num_rows() - o)))
            })
            .collect();
        let w = retry("write_segment", cx.backoff, || {
            let spec = SegmentSpec {
                id: SegmentId::new(),
                project_id: project.clone(),
                level: Level::L0,
                partition_hour: hour,
            };
            let stream = futures::stream::iter(chunks.clone().into_iter().map(Ok)).boxed();
            cx.segments.write_segment(spec, stream)
        })
        .await;
        written.push(NewSegment {
            meta: w.meta,
            traces: w.traces,
        });
    }
    failpoint::hit("flush_after_upload");

    // 3. The commit point. Identical FlushCommit on every retry (idempotent in forge-meta).
    let commit = FlushCommit {
        node_id: cx.node_id.clone(),
        segments: written,
        flushed_wal_seq: f.last_seq(),
        max_version: cx.versions.high_water(),
    };
    retry("commit_flush", cx.backoff, || cx.meta.commit_flush(commit.clone())).await;
    failpoint::hit("flush_after_commit");

    // 4. Visible as a segment now; drop from the memtable view (invariant 1), then the WAL.
    cx.mem.release(&f);
    let wal = cx.wal.clone();
    let watermark = f.last_seq();
    let deleted = match tokio::task::spawn_blocking(move || wal.truncate(watermark)).await {
        Ok(Ok(n)) => n,
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "WAL truncate failed; retried after the next flush");
            0
        }
        Err(e) => {
            tracing::warn!(error = %e, "WAL truncate task failed");
            0
        }
    };
    failpoint::hit("flush_after_truncate");
    Ok(FlushReport {
        rows_out: commit.segments.iter().map(|s| s.meta.row_count).sum(),
        segments: commit.segments,
        watermark,
        rows_in: f.part.batches.num_rows(),
        wal_segments_deleted: deleted,
        merged_batches: merged.len(),
    })
}
