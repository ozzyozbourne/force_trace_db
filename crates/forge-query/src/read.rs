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
use arrow::error::ArrowError;
use forge_meta::{MetaResult, Metastore, SegmentFilter};
use forge_model::catalog::{SegmentMeta, TraceLocation};
use forge_model::derived::with_effective_total_tokens;
use forge_model::ids::{ProjectId, TraceId};
use forge_model::merge::{Cancel, MERGE_COLUMNS, Tombstones, merge_projected};
use forge_model::schema::{ALL_COLUMNS, col, col_idx, project_to};
use forge_model::{MemtableSnapshot, MemtableView};
use forge_storage::{RowSelection, ScanRequest, SegmentStore};
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

/// The merge-skip rule (D14): ONE segment's rows are already one per key (invariant 3), so without memtable rows
/// only tombstones must go. M3 extends it to pairwise key-disjoint segments.
pub(crate) fn needs_merge(parts: &[SegmentPart], mem_rows: bool) -> bool {
    mem_rows || parts.iter().map(|p| p.seg.id).collect::<BTreeSet<_>>().len() != 1
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

    /// Every location of `k`'s traces in each of its valid projects (one `locate_traces` per project, D16), minus
    /// segments outside `k`'s time range, in (segment, row) order. `k.projects` and `k.traces` must be set.
    pub async fn locate_versions(&self, k: &KeyFilters) -> MetaResult<Vec<SegmentPart>> {
        let (Some(ps), Some(ts)) = (&k.projects, &k.traces) else {
            unreachable!("locate_versions needs project and trace keys")
        };
        let traces: Vec<TraceId> = ts.iter().copied().collect();
        let mut out = Vec::new();
        for p in ps.iter().filter_map(|p| ProjectId::new(p.as_str()).ok()) {
            for loc in self.meta.locate_traces(&p, &traces).await?.into_values().flatten() {
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

    pub fn mem_versions(&self, k: &KeyFilters, schema: &SchemaRef) -> Result<Vec<RecordBatch>, ArrowError> {
        mem_versions(&self.mem, k, schema)
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
                async move { store.scan(&seg, req).await?.try_collect::<Vec<_>>().await }
            })
            .buffer_unordered(SCAN_CONCURRENCY)
            .try_collect()
            .await?;
        Ok(out.into_iter().flatten().collect())
    }
}

/// The memtable rows that pass `k`'s key filters (mask on the full batch, then project + filter: only the read
/// columns are copied), projected to `schema`; batches without a match are dropped.
pub(crate) fn mem_versions(
    snap: &MemtableSnapshot,
    k: &KeyFilters,
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>, ArrowError> {
    let mut out = Vec::new();
    for b in snap.batches() {
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
    let mut out: Vec<SegmentPart> = Vec::with_capacity(parts.len());
    for p in parts {
        if let (Some(last), RowSelection::Range(r)) = (out.last_mut(), &p.rows)
            && last.seg.id == p.seg.id
            && let RowSelection::Range(l) = &mut last.rows
            && r.start <= l.end.saturating_add(max_gap)
        {
            l.end = l.end.max(r.end);
            continue;
        }
        out.push(p);
    }
    out
}

fn row_start(r: &RowSelection) -> u64 {
    match r {
        RowSelection::All => 0,
        RowSelection::Range(r) => r.start,
    }
}
