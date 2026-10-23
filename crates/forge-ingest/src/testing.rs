//! A hand-made L0 flush for other crates' tests (feature `test-util`).
use crate::sink::SegmentSink;
use arrow::record_batch::RecordBatch;
use forge_meta::{FlushCommit, Metastore};
use forge_model::catalog::{HourBucket, Level, SegmentId, SegmentMeta};
use forge_model::merge::{Tombstones, merge_versions_chunked, split_chunks_by_project};
use forge_storage::{SegmentSpec, SegmentStore};
use std::sync::Arc;

/// One L0 flush of `batch` (spans_schema rows, versions already assigned), like the real flush: merge (Keep) ->
/// one segment per project (merged batches of at most `max_chunk` bytes) -> one `commit_flush` of `node_id`.
/// Returns the segments written, in project order.
#[allow(clippy::too_many_arguments)]
pub async fn write_flush(
    meta: &dyn Metastore,
    store: Arc<dyn SegmentStore>,
    batch: &RecordBatch,
    hour: HourBucket,
    node_id: &str,
    max_chunk: usize,
    seq: u64,
    max_version: u64,
) -> Vec<SegmentMeta> {
    let mut segments = Vec::new();
    if batch.num_rows() > 0 {
        let merged = merge_versions_chunked(std::slice::from_ref(batch), Tombstones::Keep, max_chunk).unwrap();
        for (p, batches) in split_chunks_by_project(&merged).unwrap() {
            let spec = SegmentSpec {
                id: SegmentId::new(),
                project_id: p,
                level: Level::L0,
                partition_hour: hour,
            };
            let mut sink = SegmentSink::open(store.clone(), spec).unwrap();
            for b in batches {
                sink.push(b).await.unwrap();
            }
            segments.push(sink.finish().await.unwrap());
        }
    }
    let metas = segments.iter().map(|s| s.meta.clone()).collect();
    let c = FlushCommit {
        node_id: node_id.into(),
        segments,
        flushed_wal_seq: seq,
        max_version,
    };
    meta.commit_flush(c).await.unwrap();
    metas
}
