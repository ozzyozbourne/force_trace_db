//! `SpansTableProvider`: the `spans` table. Per project, segment scans + memtable rows are merged by
//! `MergeVersionsExec` (or, for one segment and no memtable rows, only tombstones are filtered: D14); every
//! non-key filter stays above that in DataFusion's `FilterExec` (invariant 4).
use crate::derived::effective_total_tokens_expr;
use crate::ext;
use crate::filters::{KeyFilters, classify};
use crate::merge_exec::MergeVersionsExec;
use crate::scan::SegmentScanExec;
use arrow::array::{Array, AsArray, RecordBatch, UInt32Array};
use arrow::compute::{filter_record_batch, take_record_batch};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, Result};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::prelude::{col, not};
use forge_meta::{Metastore, SegmentFilter};
use forge_model::MemtableView;
use forge_model::catalog::{SegmentId, SegmentMeta};
use forge_model::ids::{ProjectId, TraceId};
use forge_model::merge::MERGE_COLUMNS;
use forge_model::schema::{ALL_COLUMNS, col as c, col_idx, spans_schema};
use forge_storage::{RowSelection, SegmentStore};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Debug)]
pub struct SpansTableProvider {
    pub meta: Arc<dyn Metastore>,
    pub segments: Arc<dyn SegmentStore>,
    pub memtable: Arc<dyn MemtableView>,
}

/// One segment's partitions: one per row range (trace_index) or one `All`.
#[derive(Clone, Debug)]
pub struct SegmentPart {
    pub seg: SegmentMeta,
    pub rows: RowSelection,
}

impl SpansTableProvider {
    /// Candidate segment parts by project. Key filters go to the metastore; time pruning = may_overlap_time
    /// (start_time_nulls == 0 only, decision 5).
    async fn plan_segments(&self, k: &KeyFilters) -> Result<BTreeMap<String, Vec<SegmentPart>>> {
        let mut out: BTreeMap<String, Vec<SegmentPart>> = BTreeMap::new();
        let valid = |p: &String| ProjectId::new(p.as_str()).ok();
        match (&k.projects, &k.traces) {
            (Some(ps), Some(ts)) => {
                // One batched locate per project (D16).
                let traces: Vec<TraceId> = ts.iter().copied().collect();
                let mut by_seg: BTreeMap<SegmentId, (SegmentMeta, Vec<std::ops::Range<u64>>)> = BTreeMap::new();
                for p in ps.iter().filter_map(valid) {
                    for loc in self
                        .meta
                        .locate_traces(&p, &traces)
                        .await
                        .map_err(ext)?
                        .into_values()
                        .flatten()
                    {
                        if !loc.segment.may_overlap_time(k.start_ns, k.end_ns) {
                            continue;
                        }
                        let e = by_seg
                            .entry(loc.segment.id)
                            .or_insert_with(|| (loc.segment.clone(), vec![]));
                        e.1.push(loc.rows());
                    }
                }
                for (_, (seg, ranges)) in by_seg {
                    let parts = out.entry(seg.project_id.to_string()).or_default();
                    parts.extend(ranges.into_iter().map(|r| SegmentPart {
                        seg: seg.clone(),
                        rows: RowSelection::Range(r),
                    }));
                }
            }
            _ => {
                let filters: Vec<SegmentFilter> = match &k.projects {
                    Some(ps) => ps
                        .iter()
                        .filter_map(valid)
                        .map(|p| SegmentFilter {
                            project_id: Some(p),
                            ..Default::default()
                        })
                        .collect(),
                    None => vec![SegmentFilter::default()],
                };
                for mut f in filters {
                    (f.start_ns, f.end_ns) = (k.start_ns, k.end_ns);
                    for seg in self.meta.list_segments(&f).await.map_err(ext)? {
                        out.entry(seg.project_id.to_string()).or_default().push(SegmentPart {
                            seg,
                            rows: RowSelection::All,
                        });
                    }
                }
            }
        }
        Ok(out)
    }
}

/// Unprojected column indices the plan must read: the output columns + merge columns (+ the counts when
/// total_tokens is derived). Ascending = spans_schema order.
pub fn read_columns(out: &[usize]) -> Vec<usize> {
    let mut s: BTreeSet<usize> = out.iter().copied().collect();
    s.extend(MERGE_COLUMNS.iter().map(|n| col_idx(n)));
    if s.contains(&col_idx(c::TOTAL_TOKENS)) {
        s.extend([col_idx(c::PROMPT_TOKENS), col_idx(c::COMPLETION_TOKENS)]);
    }
    s.into_iter().collect()
}

/// The trace_id IN filter for scans that cannot use trace_index (no project filter). Key column: allowed
/// below the merge (invariant 4).
fn vortex_trace_filter(ts: &BTreeSet<TraceId>) -> Option<vortex::expr::Expression> {
    use vortex::buffer::ByteBuffer;
    use vortex::dtype::Nullability;
    use vortex::expr::{eq, lit, or};
    use vortex::scalar::Scalar;
    ts.iter()
        .map(|t| {
            eq(
                vortex::expr::col(c::TRACE_ID),
                lit(Scalar::binary(
                    ByteBuffer::copy_from(t.as_bytes()),
                    Nullability::NonNullable,
                )),
            )
        })
        .reduce(or)
}

#[async_trait]
impl TableProvider for SpansTableProvider {
    fn schema(&self) -> SchemaRef {
        spans_schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    /// Inexact for what we can prune with (key sets, start_time range); Unsupported otherwise. Never Exact:
    /// DataFusion keeps every filter in a FilterExec ABOVE our plan (= above the merge).
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| match classify(f) {
                Some(_) => TableProviderFilterPushDown::Inexact,
                None => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>, // never below the merge; DataFusion keeps its own limit above
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Invariant 1: memtable snapshot BEFORE any metastore call.
        let mem = self.memtable.snapshot();
        let k = KeyFilters::from_exprs(filters);
        let full = spans_schema();
        let out_cols: Vec<usize> = projection
            .cloned()
            .unwrap_or_else(|| (0..full.fields().len()).collect());
        let read = read_columns(&out_cols);
        let read_schema: SchemaRef = Arc::new(full.project(&read)?);
        let names: Vec<&'static str> = read.iter().map(|&i| ALL_COLUMNS[i]).collect();

        let mut groups: BTreeMap<String, (Vec<SegmentPart>, Vec<RecordBatch>)> = BTreeMap::new();
        if !k.empty() {
            for (p, parts) in self.plan_segments(&k).await? {
                groups.entry(p).or_default().0 = parts;
            }
            // Memtable rows: key filters only, then projected and split by project.
            for b in mem.batches() {
                let b = match k.key_mask(b) {
                    Some(m) => filter_record_batch(b, &m)?,
                    None => b.clone(),
                };
                for (p, part) in split_rows_by_project(&b.project(&read)?)? {
                    groups.entry(p).or_default().1.push(part);
                }
            }
        }
        let filter = match (&k.projects, &k.traces) {
            (None, Some(ts)) => vortex_trace_filter(ts),
            _ => None,
        };

        let df_read = DFSchema::try_from(read_schema.as_ref().clone())?;
        let mut plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        for (_, (parts, mem)) in groups {
            let mut inputs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
            let distinct: BTreeSet<SegmentId> = parts.iter().map(|p| p.seg.id).collect();
            if !parts.is_empty() {
                inputs.push(Arc::new(SegmentScanExec::new(
                    self.segments.clone(),
                    parts,
                    names.clone(),
                    read_schema.clone(),
                    filter.clone(),
                )));
            }
            let no_mem = mem.is_empty();
            if !no_mem {
                inputs.push(MemorySourceConfig::try_new_exec(&[mem], read_schema.clone(), None)?);
            }
            // Disjoint: ONE segment and no memtable rows for this project. Its rows are already one per key
            // (invariant 3); only tombstones must go.
            let plan: Arc<dyn ExecutionPlan> = if no_mem && distinct.len() == 1 {
                let pred = state.create_physical_expr(not(col(c::DELETED)), &df_read)?;
                Arc::new(FilterExec::try_new(pred, inputs.pop().unwrap())?)
            } else {
                Arc::new(MergeVersionsExec::try_new(UnionExec::try_new(inputs)?)?)
            };
            plans.push(plan);
        }
        let merged: Arc<dyn ExecutionPlan> = match plans.len() {
            0 => Arc::new(EmptyExec::new(read_schema.clone())),
            _ => UnionExec::try_new(plans)?, // projects are disjoint key spaces
        };

        // Output projection, with the derived total_tokens (decision 15) for EVERY read path.
        let exprs = out_cols
            .iter()
            .map(|&i| {
                let name = ALL_COLUMNS[i];
                let e: Arc<dyn PhysicalExpr> = if name == c::TOTAL_TOKENS {
                    state.create_physical_expr(effective_total_tokens_expr(), &df_read)?
                } else {
                    Arc::new(Column::new(name, read_schema.index_of(name)?))
                };
                Ok((e, name.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(ProjectionExec::try_new(exprs, merged)?))
    }
}

/// Zero-copy-ish split of an unsorted batch by project (memtable batches are unsorted).
fn split_rows_by_project(b: &RecordBatch) -> Result<Vec<(String, RecordBatch)>> {
    if b.num_rows() == 0 {
        return Ok(vec![]);
    }
    let p = b.column_by_name(c::PROJECT_ID).unwrap().as_string::<i32>();
    let first = p.value(0);
    if (0..p.len()).all(|i| p.value(i) == first) {
        return Ok(vec![(first.to_string(), b.clone())]);
    }
    let mut idx: BTreeMap<&str, Vec<u32>> = BTreeMap::new();
    for i in 0..p.len() {
        idx.entry(p.value(i)).or_default().push(i as u32);
    }
    idx.into_iter()
        .map(|(k, v)| Ok((k.to_string(), take_record_batch(b, &UInt32Array::from(v))?)))
        .collect()
}
