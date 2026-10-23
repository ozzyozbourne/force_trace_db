//! Flush of one frozen memtable.
//!
//! merge (spawn_blocking) -> one L0 segment per project (fresh SegmentId per attempt) -> ONE
//! commit_flush{segments, flushed_wal_seq = frozen.last_seq, max_version} (same FlushCommit on every
//! retry: idempotent) -> release the frozen memtable from the view -> truncate the WAL.
use crate::memtable::{Frozen, MemtableSet};
use crate::version::VersionAllocator;
use crate::wal::WalLog;
use forge_meta::{FlushCommit, Metastore, NewSegment};
use forge_model::catalog::{HourBucket, Level, SegmentId};
use forge_model::failpoint;
use forge_model::merge::{Tombstones, merge_versions, split_by_project};
use forge_model::retry::{Backoff, retry};
use forge_storage::{SegmentSpec, SegmentStore};
use futures::StreamExt;
use std::sync::Arc;

/// Rows per batch handed to the segment writer (zero-copy slices; Vortex re-chunks anyway).
const WRITE_BATCH_ROWS: usize = 8192;

pub struct FlushCtx {
    pub node_id: String,
    pub segments: Arc<dyn SegmentStore>,
    pub meta: Arc<dyn Metastore>,
    pub mem: Arc<MemtableSet>,
    pub wal: Arc<WalLog>,
    pub versions: Arc<VersionAllocator>,
    pub backoff: Backoff,
}

#[derive(Debug)]
pub struct FlushReport {
    pub segments: Vec<NewSegment>,
    pub watermark: u64,
    pub rows_in: usize,
    pub rows_out: u64,
    pub wal_segments_deleted: usize,
}

pub async fn flush_frozen(cx: &FlushCtx, f: Arc<Frozen>) -> Result<FlushReport, forge_model::error::ModelError> {
    // 1. Merge + physical sort: CPU work, off the async workers. A merge error is a bug (inserts are
    //    validated), so it is returned and the caller retries later at max backoff.
    let batches = f.batches.clone();
    let merged = tokio::task::spawn_blocking(move || merge_versions(&batches, Tombstones::Keep))
        .await
        .expect("merge task panicked")?;
    let parts = split_by_project(&merged)?;
    failpoint::hit("flush_before_upload");

    // 2. One L0 segment per project. A fresh SegmentId per ATTEMPT: a put that landed but reported
    //    an error leaves an orphan (GC'd after 24 h) instead of depending on byte-identical re-encoding.
    let hour = HourBucket::from_nanos(forge_model::time::now_ms().saturating_mul(1_000_000));
    let mut written = Vec::with_capacity(parts.len());
    for (project, rows) in parts {
        let chunks: Vec<_> = (0..rows.num_rows())
            .step_by(WRITE_BATCH_ROWS)
            .map(|o| rows.slice(o, WRITE_BATCH_ROWS.min(rows.num_rows() - o)))
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
        flushed_wal_seq: f.last_seq,
        max_version: cx.versions.high_water(),
    };
    retry("commit_flush", cx.backoff, || cx.meta.commit_flush(commit.clone())).await;
    failpoint::hit("flush_after_commit");

    // 4. Visible as a segment now; drop from the memtable view (invariant 1), then the WAL.
    cx.mem.release(&f);
    let wal = cx.wal.clone();
    let watermark = f.last_seq;
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
        rows_in: f.rows,
        wal_segments_deleted: deleted,
    })
}
