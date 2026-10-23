//! The read structure every path shares (SQL provider, trace, runs).
//!
//! A [`ReadView`] takes the memtable snapshot when it is built, so no metastore call made through it can precede
//! the snapshot (invariant 1). Segment rows come as [`SegmentPart`]s, memtable rows are masked by key filters only
//! (`KeyFilters::key_mask`; invariant 4), and [`needs_merge`] is the one merge-skip rule.
use crate::QueryError;
use crate::filters::KeyFilters;
use arrow::array::{AsArray, RecordBatch};
use arrow::compute::{concat_batches, filter_record_batch, not};
use arrow::datatypes::SchemaRef;
use forge_meta::{MetaResult, Metastore, SegmentFilter};
use forge_model::catalog::{Level, SegmentMeta, TraceLocation, trace_ranges_overlap};
use forge_model::derived::with_effective_total_tokens;
use forge_model::ids::TraceId;
use forge_model::merge::{Cancel, MERGE_COLUMNS, Tombstones, merge_projected};
use forge_model::schema::{ALL_COLUMNS, col, col_idx, project_to};
use forge_model::{MemtableSnapshot, MemtableView};
use forge_storage::{RowSelection, ScanRequest, SegmentStore, coalesce_ranges};
use futures::{StreamExt, TryStreamExt};
use std::collections::BTreeSet;
use std::sync::Arc;

/// Fast-path segment scans in flight per call.
const SCAN_CONCURRENCY: usize = 8;

/// One segment scan: a segment and a row selection (one `SegmentScanExec` partition, or one fast-path scan).
#[derive(Clone, Debug)]
pub(crate) struct SegmentPart {
    pub seg: SegmentMeta,
    pub rows: RowSelection,
}

impl From<&TraceLocation> for SegmentPart {
    fn from(l: &TraceLocation) -> Self {
        Self {
            seg: l.segment.clone(),
            rows: RowSelection::Range(l.rows()),
        }
    }
}

/// The request for one part (both scanners build it here).
pub(crate) fn scan_request(
    part: &SegmentPart,
    cols: &[&'static str],
    filter: Option<vortex::expr::Expression>,
) -> ScanRequest {
    ScanRequest {
        projection: Some(cols.to_vec()),
        rows: part.rows.clone(),
        filter,
        limit: None,
    }
}

/// Output columns -> the columns a merge-then-derive read must fetch: the output + MERGE_COLUMNS (+ the counts when
/// total_tokens is derived, decision 15), in spans_schema order.
pub(crate) fn read_columns(out: &[&str]) -> Vec<&'static str> {
    let mut s: BTreeSet<usize> = out.iter().map(|n| col_idx(n)).collect();
    s.extend(MERGE_COLUMNS.iter().map(|n| col_idx(n)));
    if s.contains(&col_idx(col::TOTAL_TOKENS)) {
        s.extend([col_idx(col::PROMPT_TOKENS), col_idx(col::COMPLETION_TOKENS)]);
    }
    s.into_iter().map(|i| ALL_COLUMNS[i]).collect()
}

/// The merge-skip rule (D14 + M3). Segment rows are already one per key (invariant 3), so the merge can be skipped
/// (only tombstones are filtered) when there are no memtable rows, no L0 part, and the L1 parts are pairwise
/// key-disjoint. Invariant 10 makes L1 segments of different (project, partition_hour) key-disjoint; inside one hour,
/// segments with disjoint trace-id ranges are too.
pub(crate) fn needs_merge(parts: &[SegmentPart], mem_rows: bool) -> bool {
    if mem_rows {
        return true;
    }
    let mut segs: Vec<&SegmentMeta> = parts.iter().map(|p| &p.seg).collect();
    segs.sort_by_key(|s| s.id);
    segs.dedup_by_key(|s| s.id);
    if segs.len() <= 1 {
        return false;
    }
    if segs.iter().any(|s| s.level == Level::L0) {
        return true;
    }
    segs.sort_by(|a, b| (&a.project_id, a.partition_hour).cmp(&(&b.project_id, b.partition_hour)));
    segs.chunk_by_mut(|a, b| (&a.project_id, a.partition_hour) == (&b.project_id, b.partition_hour))
        .any(trace_ranges_overlap)
}

/// Fast paths: the merged rows of `batches` (tombstones dropped; only the tombstone filter when `!merge`), with
/// the effective total_tokens.
pub(crate) fn merge_or_filter(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    merge: bool,
    cancel: &Cancel,
) -> Result<RecordBatch, QueryError> {
    let m = if merge {
        merge_projected(schema, batches, Tombstones::Drop, cancel)?
    } else {
        cancel.check()?;
        let all = concat_batches(schema, batches)?;
        let live = not(all.column(schema.index_of(col::DELETED)?).as_boolean())?;
        filter_record_batch(&all, &live)?
    };
    Ok(with_effective_total_tokens(&m)?)
}

/// The memtable snapshot plus the metastore and segment store it was taken against.
pub(crate) struct ReadView {
    mem: MemtableSnapshot,
    meta: Arc<dyn Metastore>,
    store: Arc<dyn SegmentStore>,
}

impl ReadView {
    /// Takes the memtable snapshot FIRST (invariant 1): every metastore call goes through the view.
    pub fn new(mem: &dyn MemtableView, meta: &Arc<dyn Metastore>, store: &Arc<dyn SegmentStore>) -> Self {
        Self {
            mem: mem.snapshot(),
            meta: meta.clone(),
            store: store.clone(),
        }
    }

    pub async fn list_segments(&self, f: &SegmentFilter) -> MetaResult<Vec<SegmentMeta>> {
        self.meta.list_segments(f).await
    }

    /// Every location of `k`'s traces in each of its projects (one `locate_traces` per project, D16), minus
    /// segments outside `k`'s time range, in (segment, row) order. `k.projects` and `k.traces` must be set.
    pub async fn locate_versions(&self, k: &KeyFilters) -> MetaResult<Vec<SegmentPart>> {
        let (Some(ps), Some(ts)) = (&k.projects, &k.traces) else {
            unreachable!("locate_versions needs project and trace keys")
        };
        let traces: Vec<TraceId> = ts.iter().copied().collect();
        let mut out = Vec::new();
        for p in ps {
            for loc in self.meta.locate_traces(p, &traces).await?.into_values().flatten() {
                if loc.segment.may_overlap_time(k.start_ns, k.end_ns) {
                    out.push(SegmentPart::from(&loc));
                }
            }
        }
        out.sort_by_key(|p| (p.seg.id, row_start(&p.rows)));
        Ok(out)
    }

    /// The snapshot (cheap to clone: Arcs), e.g. to mask it on the blocking pool beside the I/O.
    pub fn mem(&self) -> &MemtableSnapshot {
        &self.mem
    }

    pub fn mem_versions(&self, k: &KeyFilters, schema: &SchemaRef) -> Result<Vec<RecordBatch>, QueryError> {
        mem_versions(&self.mem, k, schema, &Cancel::never())
    }

    /// Scans `parts`, `SCAN_CONCURRENCY` at a time.
    pub async fn scan(
        &self,
        parts: &[SegmentPart],
        cols: &[&'static str],
        filter: Option<vortex::expr::Expression>,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        let reqs: Vec<(SegmentMeta, ScanRequest)> = parts
            .iter()
            .map(|p| (p.seg.clone(), scan_request(p, cols, filter.clone())))
            .collect();
        let out: Vec<Vec<RecordBatch>> = futures::stream::iter(reqs)
            .map(|(seg, req)| {
                let store = self.store.clone();
                async move { store.scan_all(&seg, req).await }
            })
            .buffer_unordered(SCAN_CONCURRENCY)
            .try_collect()
            .await?;
        Ok(out.into_iter().flatten().collect())
    }
}

/// The memtable rows that pass `k`'s key filters (mask on the full batch, then project + filter: only the read
/// columns are copied), projected to `schema`; batches without a match are dropped. Checks `cancel` every 64
/// batches.
pub(crate) fn mem_versions(
    snap: &MemtableSnapshot,
    k: &KeyFilters,
    schema: &SchemaRef,
    cancel: &Cancel,
) -> Result<Vec<RecordBatch>, QueryError> {
    let mut out = Vec::new();
    for (i, b) in snap.batches().enumerate() {
        if i % 64 == 0 {
            cancel.check()?;
        }
        match k.key_mask(b)? {
            None => out.push(project_to(b, schema)?),
            Some(m) if m.true_count() > 0 => out.push(filter_record_batch(&project_to(b, schema)?, &m)?),
            Some(_) => {}
        }
    }
    Ok(out)
}

/// `parts` (sorted by segment, then row) with the row ranges of each segment merged where the gap between them is
/// at most `max_gap` rows (0: contiguous or overlapping only, i.e. exactly the same rows). The extra rows of a
/// gap must be dropped by the caller's key mask.
pub(crate) fn coalesce(parts: Vec<SegmentPart>, max_gap: u64) -> Vec<SegmentPart> {
    let range = |p: &SegmentPart| match &p.rows {
        RowSelection::Range(r) => Some(r.clone()),
        RowSelection::All => None,
    };
    let mut out: Vec<SegmentPart> = Vec::with_capacity(parts.len());
    // Runs of consecutive ranges of one segment; an `All` part stays alone.
    let is_range = |p: &SegmentPart| matches!(p.rows, RowSelection::Range(_));
    for run in parts.chunk_by(|a, b| a.seg.id == b.seg.id && is_range(a) && is_range(b)) {
        match run.iter().map(range).collect::<Option<Vec<_>>>() {
            Some(rs) => out.extend(coalesce_ranges(rs, max_gap).into_iter().map(|r| SegmentPart {
                seg: run[0].seg.clone(),
                rows: RowSelection::Range(r),
            })),
            None => out.extend_from_slice(run),
        }
    }
    out
}

fn row_start(r: &RowSelection) -> u64 {
    match r {
        RowSelection::All => 0,
        RowSelection::Range(r) => r.start,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_model::catalog::HourBucket;
    use forge_model::schema::spans_schema;
    use forge_model::testing::{FixedMemtable, Rng, batch_from_rows, random_row};

    /// The memtable mask stops on a cancelled flag (checked every 64 batches).
    #[test]
    fn mem_versions_checks_cancel() {
        let mut r = Rng(5);
        let batches = (0..200u64)
            .map(|i| batch_from_rows(&[random_row(&mut r, "p", 1, i, i + 1)]))
            .collect();
        let snap = FixedMemtable(batches).snapshot();
        let k = KeyFilters::default();
        let run = |c: &Cancel| mem_versions(&snap, &k, &spans_schema(), c);
        assert!(matches!(
            run(&Cancel::after(1)),
            Err(QueryError::Model(forge_model::error::ModelError::Cancelled))
        ));
        assert_eq!(run(&Cancel::after(u64::MAX)).unwrap().len(), 200);
    }

    /// Plan §6.3 merge-skip cases.
    #[test]
    fn needs_merge_cases() {
        use forge_model::testing::{segment_meta, tid};
        let part = |level, hour, lo: u64, hi: u64| SegmentPart {
            seg: SegmentMeta {
                min_trace_id: tid(lo),
                max_trace_id: tid(hi),
                ..segment_meta("p", level, HourBucket(hour))
            },
            rows: RowSelection::All,
        };
        // tid(n) puts n % 7 in byte 0: use n = 7k for ordered ids.
        let (a, b) = (part(Level::L1, 1, 7, 14), part(Level::L1, 2, 7, 14));
        assert!(!needs_merge(&[a.clone(), b], false), "different hours");
        assert!(
            needs_merge(&[a.clone(), part(Level::L1, 1, 14, 21)], false),
            "overlapping runs"
        );
        assert!(
            !needs_merge(&[a.clone(), part(Level::L1, 1, 21, 28)], false),
            "disjoint runs"
        );
        assert!(needs_merge(&[a.clone(), part(Level::L0, 1, 21, 28)], false), "L1 + L0");
        assert!(needs_merge(std::slice::from_ref(&a), true), "memtable rows");
        assert!(!needs_merge(&[a.clone(), a], false), "one segment, two ranges");
    }
}
