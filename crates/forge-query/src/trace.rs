//! Trace fast path (no DataFusion).
use crate::tree::TraceTree;
use crate::{QueryEngine, QueryError};
use arrow::array::{AsArray, BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use forge_model::MemtableSnapshot;
use forge_model::derived::with_effective_total_tokens;
use forge_model::ids::{ProjectId, TraceId};
use forge_model::merge::{Cancel, CancelOnDrop, Tombstones, merge_projected};
use forge_model::schema::{ALL_COLUMNS, PAYLOAD_COLUMNS, col, col_idx, projected_schema};
use forge_model::catalog::SegmentMeta;
use forge_storage::{RowSelection, ScanRequest, SegmentStore};
use futures::{StreamExt, TryStreamExt};
use std::sync::Arc;

pub struct TraceRequest {
    pub project: ProjectId,
    pub trace: TraceId,
    pub payloads: bool,
    pub format: TraceFormat,
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TraceFormat {
    Tree,
    Flat,
}
impl TraceRequest {
    pub fn parse(trace_id: &str, project: ProjectId, payloads: Option<bool>, format: Option<&str>) -> Result<Self, QueryError> {
        let hex: String = if trace_id.len() == 36 { trace_id.replace('-', "") } else { trace_id.to_string() };
        let trace = hex
            .to_ascii_lowercase()
            .parse::<TraceId>()
            .map_err(|e| QueryError::InvalidInput(format!("bad trace id {trace_id:?}: {e}")))?;
        let format = match format {
            None | Some("tree") => TraceFormat::Tree,
            Some("flat") => TraceFormat::Flat,
            Some(f) => return Err(QueryError::InvalidInput(format!("bad format {f:?}"))),
        };
        Ok(Self { project, trace, payloads: payloads.unwrap_or(true), format })
    }
}

fn trace_columns(payloads: bool) -> Vec<&'static str> {
    ALL_COLUMNS
        .iter()
        .copied()
        .filter(|c| *c != col::DOTTED_ORDER && (payloads || !PAYLOAD_COLUMNS.contains(c)))
        .collect()
}

pub struct LoadedTrace {
    pub rows: RecordBatch,
    pub tree: TraceTree,
}

/// Scans (segment, selection) parts with concurrency 8.
pub async fn scan_parts(
    store: &Arc<dyn SegmentStore>,
    parts: Vec<(SegmentMeta, RowSelection)>,
    cols: &[&'static str],
    filter: Option<vortex::expr::Expression>,
) -> Result<Vec<RecordBatch>, QueryError> {
    let out: Vec<Vec<RecordBatch>> = futures::stream::iter(parts)
        .map(|(seg, rows)| {
            let store = store.clone();
            let req = ScanRequest { projection: Some(cols.to_vec()), rows, filter: filter.clone(), limit: None };
            async move {
                let s = store.scan(&seg, req).await?;
                s.try_collect::<Vec<_>>().await
            }
        })
        .buffer_unordered(8)
        .try_collect()
        .await?;
    Ok(out.into_iter().flatten().collect())
}

/// Memtable rows projected to `schema`, masked by `mask` (evaluated on the full batch).
pub fn mem_rows(
    snap: &MemtableSnapshot,
    schema: &SchemaRef,
    mask: impl Fn(&RecordBatch) -> BooleanArray,
) -> Result<Vec<RecordBatch>, QueryError> {
    let idx: Vec<usize> = schema.fields().iter().map(|f| col_idx(f.name())).collect();
    snap.batches()
        .map(|b| {
            let m = mask(b);
            Ok(filter_record_batch(&b.project(&idx)?, &m)?)
        })
        .filter(|r: &Result<RecordBatch, QueryError>| r.as_ref().map_or(true, |b| b.num_rows() > 0))
        .collect()
}

pub fn key_mask(b: &RecordBatch, project: &ProjectId, trace: Option<TraceId>) -> BooleanArray {
    let p = b.column(col_idx(col::PROJECT_ID)).as_string::<i32>();
    let t = b.column(col_idx(col::TRACE_ID)).as_binary::<i32>();
    (0..b.num_rows())
        .map(|i| Some(p.value(i) == project.as_str() && trace.is_none_or(|x| t.value(i) == x.as_bytes())))
        .collect()
}

impl QueryEngine {
    pub async fn load_trace(&self, r: &TraceRequest) -> Result<LoadedTrace, QueryError> {
        self.deadline(self.cfg.timeout, self.load_trace_inner(r)).await
    }

    async fn load_trace_inner(&self, r: &TraceRequest) -> Result<LoadedTrace, QueryError> {
        let snap = self.mem.snapshot(); // FIRST (invariant 1)
        let locs = self.meta.locate_trace(&r.project, r.trace).await?;
        let cols = trace_columns(r.payloads);
        let schema = projected_schema(&cols);
        let parts = locs.iter().map(|l| (l.segment.clone(), RowSelection::Range(l.rows()))).collect();
        let mut batches = scan_parts(&self.store, parts, &cols, None).await?;
        batches.extend(mem_rows(&snap, &schema, |b| key_mask(b, &r.project, Some(r.trace)))?);
        let cancel = Cancel::new();
        let _g = CancelOnDrop(cancel.clone());
        let (rows, tree) = tokio::task::spawn_blocking(move || {
            let m = merge_projected(&schema, &batches, Tombstones::Drop, &cancel)?;
            let m = with_effective_total_tokens(&m)?;
            let tree = TraceTree::build(&m, &cancel)?;
            Ok::<_, QueryError>((m, tree))
        })
        .await??;
        if rows.num_rows() == 0 {
            return Err(QueryError::NotFound(format!("trace {} not found in project {}", r.trace, r.project)));
        }
        Ok(LoadedTrace { rows, tree })
    }

    pub async fn trace_json(&self, r: &TraceRequest) -> Result<Vec<u8>, QueryError> {
        let t = self.load_trace(r).await?;
        let mut out = Vec::with_capacity(t.rows.get_array_memory_size());
        crate::json::write_trace(&mut out, r.project.as_str(), &r.trace.to_string(), &t.rows, &t.tree, r.format == TraceFormat::Flat);
        Ok(out)
    }
}
