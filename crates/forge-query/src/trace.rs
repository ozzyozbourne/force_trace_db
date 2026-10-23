//! Trace fast path (D3): `locate_trace` row ranges + memtable rows -> merge -> tree -> JSON. No DataFusion.
use crate::tree::TraceTree;
use crate::{QueryEngine, QueryError};
use arrow::array::{AsArray, BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use forge_model::MemtableSnapshot;
use forge_model::catalog::SegmentMeta;
use forge_model::derived::with_effective_total_tokens;
use forge_model::ids::{ProjectId, TraceId};
use forge_model::merge::{Cancel, CancelOnDrop, Tombstones, merge_projected};
use forge_model::schema::{ALL_COLUMNS, PAYLOAD_COLUMNS, col, col_idx, projected_schema};
use forge_storage::{RowSelection, ScanRequest, SegmentStore};
use futures::{StreamExt, TryStreamExt};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct TraceRequest {
    pub project: ProjectId,
    pub trace: TraceId,
    /// false: no inputs / outputs / events / attributes (not read, not written).
    pub payloads: bool,
    pub format: TraceFormat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceFormat {
    Tree,
    Flat,
}

impl TraceRequest {
    /// `trace_id`: 32 hex digits (any case) or a dashed UUID. `format`: `tree` (default) or `flat`.
    pub fn parse(
        trace_id: &str,
        project: ProjectId,
        payloads: Option<bool>,
        format: Option<&str>,
    ) -> Result<Self, QueryError> {
        let hex = if trace_id.len() == 36 {
            trace_id.replace('-', "")
        } else {
            trace_id.to_string()
        };
        let trace = hex
            .to_ascii_lowercase()
            .parse::<TraceId>()
            .map_err(|e| QueryError::InvalidInput(format!("bad trace id {trace_id:?}: {e}")))?;
        let format = match format {
            None | Some("tree") => TraceFormat::Tree,
            Some("flat") => TraceFormat::Flat,
            Some(f) => return Err(QueryError::InvalidInput(format!("bad format {f:?} (tree or flat)"))),
        };
        Ok(Self {
            project,
            trace,
            payloads: payloads.unwrap_or(true),
            format,
        })
    }
}

/// Every column but dotted_order (D4: the tree comes from parent_span_id), and no payloads when `!payloads`.
fn trace_columns(payloads: bool) -> Vec<&'static str> {
    ALL_COLUMNS
        .iter()
        .copied()
        .filter(|c| *c != col::DOTTED_ORDER && (payloads || !PAYLOAD_COLUMNS.contains(c)))
        .collect()
}

pub struct LoadedTrace {
    /// Merged rows (trace columns, effective total_tokens), in key order.
    pub rows: RecordBatch,
    pub tree: TraceTree,
}

/// Scans (segment, selection) parts, 8 at a time.
pub(crate) async fn scan_parts(
    store: &Arc<dyn SegmentStore>,
    parts: Vec<(SegmentMeta, RowSelection)>,
    cols: &[&'static str],
) -> Result<Vec<RecordBatch>, QueryError> {
    let out: Vec<Vec<RecordBatch>> = futures::stream::iter(parts)
        .map(|(seg, rows)| {
            let store = store.clone();
            let req = ScanRequest {
                projection: Some(cols.to_vec()),
                rows,
                filter: None,
                limit: None,
            };
            async move { store.scan(&seg, req).await?.try_collect::<Vec<_>>().await }
        })
        .buffer_unordered(8)
        .try_collect()
        .await?;
    Ok(out.into_iter().flatten().collect())
}

/// Memtable rows projected to `schema`, masked by `mask` (evaluated on the full batch); empty results dropped.
pub(crate) fn mem_rows(
    snap: &MemtableSnapshot,
    schema: &SchemaRef,
    mask: impl Fn(&RecordBatch) -> BooleanArray,
) -> Result<Vec<RecordBatch>, QueryError> {
    let idx: Vec<usize> = schema.fields().iter().map(|f| col_idx(f.name())).collect();
    let mut out = Vec::new();
    for b in snap.batches() {
        let m = mask(b);
        if m.true_count() > 0 {
            out.push(filter_record_batch(&b.project(&idx)?, &m)?);
        }
    }
    Ok(out)
}

/// `project_id == project` (and `trace_id == trace`) on a spans_schema batch.
pub(crate) fn key_mask(b: &RecordBatch, project: &ProjectId, trace: Option<TraceId>) -> BooleanArray {
    let p = b.column(col_idx(col::PROJECT_ID)).as_string::<i32>();
    let t = b.column(col_idx(col::TRACE_ID)).as_binary::<i32>();
    (0..b.num_rows())
        .map(|i| Some(p.value(i) == project.as_str() && trace.is_none_or(|x| t.value(i) == x.as_bytes())))
        .collect()
}

impl QueryEngine {
    /// Merged rows and tree of one trace; NotFound when it has no live span.
    pub async fn load_trace(&self, r: &TraceRequest) -> Result<LoadedTrace, QueryError> {
        self.deadline(
            self.cfg.timeout,
            self.trace_blocking(r, |rows, tree, _| Ok(LoadedTrace { rows, tree })),
        )
        .await
    }

    /// The trace as JSON (D5), nested or flat.
    pub async fn trace_json(&self, r: &TraceRequest) -> Result<Vec<u8>, QueryError> {
        let (project, trace, flat) = (
            r.project.to_string(),
            r.trace.to_string(),
            r.format == TraceFormat::Flat,
        );
        let write = move |rows: RecordBatch, tree: TraceTree, c: &Cancel| {
            let mut out = Vec::with_capacity(rows.get_array_memory_size());
            crate::json::write_trace(&mut out, &project, &trace, &rows, &tree, flat, c)?;
            Ok(out)
        };
        self.deadline(self.cfg.timeout, self.trace_blocking(r, write)).await
    }

    /// Gathers every version of the trace's keys, then merges, builds the tree and runs `finish` on the blocking
    /// pool under one `Cancel` (dropping this future sets it).
    async fn trace_blocking<T: Send + 'static>(
        &self,
        r: &TraceRequest,
        finish: impl FnOnce(RecordBatch, TraceTree, &Cancel) -> Result<T, QueryError> + Send + 'static,
    ) -> Result<T, QueryError> {
        let first = (!self.snapshot_after_list).then(|| self.mem.snapshot()); // FIRST (invariant 1)
        let locs = self.meta.locate_trace(&r.project, r.trace).await?;
        let snap = first.unwrap_or_else(|| self.mem.snapshot());
        let cols = trace_columns(r.payloads);
        let schema = projected_schema(&cols);
        let parts = locs
            .iter()
            .map(|l| (l.segment.clone(), RowSelection::Range(l.rows())))
            .collect();
        let mut batches = scan_parts(&self.store, parts, &cols).await?;
        batches.extend(mem_rows(&snap, &schema, |b| key_mask(b, &r.project, Some(r.trace)))?);
        drop(snap);
        let reservation = self.fast_reservation("trace", &batches)?;
        let cancel = self.new_cancel();
        let _guard = CancelOnDrop(cancel.clone());
        let not_found = format!("trace {} not found in project {}", r.trace, r.project);
        let out = tokio::task::spawn_blocking(move || {
            let m = with_effective_total_tokens(&merge_projected(&schema, &batches, Tombstones::Drop, &cancel)?)?;
            drop(batches);
            if m.num_rows() == 0 {
                return Err(QueryError::NotFound(not_found));
            }
            let tree = TraceTree::build(&m, &cancel)?;
            finish(m, tree, &cancel)
        })
        .await??;
        drop(reservation);
        Ok(out)
    }
}
