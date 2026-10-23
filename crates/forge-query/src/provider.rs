//! `SpansTableProvider`: the `spans` table. Per project, segment scans + memtable rows are merged by
//! `MergeVersionsExec` (or, for one segment and no memtable rows, only tombstones are filtered: D14); every
//! non-key filter stays above that in DataFusion's `FilterExec` (invariant 4).
use crate::derived::effective_total_tokens_expr;
use crate::ext;
use crate::filters::{KeyFilters, classify};
use crate::fts::{fts_columns, fts_filter};
use crate::merge_exec::MergeVersionsExec;
use crate::read::{ReadView, SegmentPart, coalesce, needs_merge, read_columns};
use crate::scan::SegmentScanExec;
use arrow::array::{Array, AsArray, RecordBatch, UInt32Array};
use arrow::compute::take_record_batch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, Result};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::prelude::{col, not};
use forge_index::FtsExpr;
use forge_meta::{Metastore, SegmentFilter};
use forge_model::MemtableView;
use forge_model::ids::{ProjectId, TraceId};
use forge_model::schema::{ALL_COLUMNS, col as c, projected_schema, spans_schema};
use forge_storage::{RowSelection, SegmentStore};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct SpansTableProvider {
    pub meta: Arc<dyn Metastore>,
    pub segments: Arc<dyn SegmentStore>,
    pub memtable: Arc<dyn MemtableView>,
    pub index: Arc<dyn forge_index::IndexReader>,
}

/// More candidate keys than this per project: scan with a post-merge re-check instead of key lookups.
const MAX_FTS_KEYS: usize = 100_000;

/// Candidate segment parts by project. Key filters go to the metastore; time pruning = may_overlap_time
/// (start_time_nulls == 0 only, decision 5).
async fn plan_segments(view: &ReadView, k: &KeyFilters) -> Result<BTreeMap<String, Vec<SegmentPart>>> {
    let mut out: BTreeMap<String, Vec<SegmentPart>> = BTreeMap::new();
    if k.projects.is_some() && k.traces.is_some() {
        for part in coalesce(view.locate_versions(k).await.map_err(ext)?, 0) {
            out.entry(part.seg.project_id.to_string()).or_default().push(part);
        }
        return Ok(out);
    }
    let filters: Vec<SegmentFilter> = match &k.projects {
        Some(ps) => ps
            .iter()
            .map(|p| SegmentFilter {
                project_id: Some(p.clone()),
                ..Default::default()
            })
            .collect(),
        None => vec![SegmentFilter::default()],
    };
    for mut f in filters {
        (f.start_ns, f.end_ns) = (k.start_ns, k.end_ns);
        for seg in view.list_segments(&f).await.map_err(ext)? {
            out.entry(seg.project_id.to_string()).or_default().push(SegmentPart {
                seg,
                rows: RowSelection::All,
            });
        }
    }
    Ok(out)
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
    /// Inexact for what we can prune with (key sets, start_time range); Exact for index-answerable FTS filters;
    /// Unsupported otherwise. Inexact filters stay in a FilterExec ABOVE our plan (= above the merge).
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| match (classify(f), fts_filter(f)) {
                (Some(_), _) => TableProviderFilterPushDown::Inexact,
                // Exact: `scan` applies it itself, on merged rows (single-source index hits, or a re-check above
                // the merge), so DataFusion drops it and its payload column leaves the projection.
                (None, Some(_)) => TableProviderFilterPushDown::Exact,
                (None, None) => TableProviderFilterPushDown::Unsupported,
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
        let view = ReadView::new(&*self.memtable, &self.meta, &self.segments); // FIRST (invariant 1)
        let k = KeyFilters::from_exprs(filters);
        let out_cols: Vec<&'static str> = match projection {
            Some(p) => p.iter().map(|&i| ALL_COLUMNS[i]).collect(),
            None => ALL_COLUMNS.to_vec(),
        };
        let names = read_columns(&out_cols);
        let read_schema = projected_schema(&names);
        let fts: Vec<&Expr> = filters
            .iter()
            .filter(|f| classify(f).is_none() && fts_filter(f).is_some())
            .collect();
        // The FTS filters' columns, read only where a re-check runs (merged candidates, fallback).
        let mut recheck_cols: BTreeSet<&'static str> = names.iter().copied().collect();
        for f in &fts {
            fts_columns(&fts_filter(f).expect("fts filter"), &mut recheck_cols);
        }
        let wide: Vec<&'static str> = ALL_COLUMNS
            .iter()
            .copied()
            .filter(|c| recheck_cols.contains(c))
            .collect();
        let wide_schema = projected_schema(&wide);
        let df_wide = DFSchema::try_from(wide_schema.as_ref().clone())?;
        // `fts` on merged `wide` rows, projected back to `names`: the post-merge re-check.
        let recheck = |input: Arc<dyn ExecutionPlan>| -> Result<Arc<dyn ExecutionPlan>> {
            let pred = conjunction(fts.iter().map(|f| (*f).clone())).expect("non-empty");
            let pred = state.create_physical_expr(pred, &df_wide)?;
            let filtered: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(pred, input)?);
            let exprs = names
                .iter()
                .map(|&n| {
                    Ok((
                        Arc::new(Column::new(n, wide_schema.index_of(n)?)) as Arc<dyn PhysicalExpr>,
                        n.to_string(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(ProjectionExec::try_new(exprs, filtered)?))
        };

        let mut groups: BTreeMap<String, (Vec<SegmentPart>, Vec<RecordBatch>)> = BTreeMap::new();
        if !k.empty() {
            for (p, parts) in plan_segments(&view, &k).await? {
                groups.entry(p).or_default().0 = parts;
            }
            // Memtable rows: key filters only, split by project (FTS: `select` reads them per candidate trace,
            // but a project with only memtable rows still needs a group).
            for b in view.mem_versions(&k, &read_schema).map_err(ext)? {
                for (p, part) in split_rows_by_project(&b)? {
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
        let fts_expr = (!fts.is_empty()).then(|| {
            let mut v: Vec<FtsExpr> = fts.iter().map(|f| fts_filter(f).expect("fts filter")).collect();
            if v.len() == 1 {
                v.pop().expect("one")
            } else {
                FtsExpr::And(v)
            }
        });
        if let Some(expr) = &fts_expr {
            for (p, (parts, _)) in groups {
                let pid = ProjectId::new(p).map_err(ext)?;
                let sel = crate::fts_scan::select(&view, &*self.index, &pid, &k, &parts, expr, MAX_FTS_KEYS)
                    .await
                    .map_err(ext)?;
                let (exact, merge_parts, mk) = match sel {
                    Some(sel) => {
                        let mk = KeyFilters {
                            projects: Some([pid.clone()].into()),
                            traces: Some(sel.merge_traces),
                            ..KeyFilters::default()
                        };
                        (sel.exact, sel.merge, mk)
                    }
                    // Too many candidates: every part, merged, re-checked.
                    None => (
                        vec![],
                        parts,
                        KeyFilters {
                            projects: Some([pid.clone()].into()),
                            ..k.clone()
                        },
                    ),
                };
                if !exact.is_empty() {
                    // Single-source rows that match: no merge, no payload column, no re-check (tombstones dropped).
                    let scan = Arc::new(SegmentScanExec::new(
                        self.segments.clone(),
                        exact,
                        names.clone(),
                        read_schema.clone(),
                        None,
                    ));
                    let pred = state.create_physical_expr(not(col(c::DELETED)), &df_read)?;
                    plans.push(Arc::new(FilterExec::try_new(pred, scan)?));
                }
                let mem = if mk.traces.as_ref().is_some_and(|t| t.is_empty()) {
                    vec![]
                } else {
                    view.mem_versions(&mk, &wide_schema).map_err(ext)?
                };
                if merge_parts.is_empty() && mem.is_empty() {
                    continue;
                }
                let mut inputs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
                if !merge_parts.is_empty() {
                    inputs.push(Arc::new(SegmentScanExec::new(
                        self.segments.clone(),
                        merge_parts,
                        wide.clone(),
                        wide_schema.clone(),
                        None,
                    )));
                }
                if !mem.is_empty() {
                    inputs.push(MemorySourceConfig::try_new_exec(&[mem], wide_schema.clone(), None)?);
                }
                let merged: Arc<dyn ExecutionPlan> = Arc::new(MergeVersionsExec::try_new(UnionExec::try_new(inputs)?)?);
                plans.push(recheck(merged)?);
            }
            groups = BTreeMap::new();
        }
        for (_, (parts, mem)) in groups {
            let merge = needs_merge(&parts, !mem.is_empty());
            let mut inputs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
            if !parts.is_empty() {
                inputs.push(Arc::new(SegmentScanExec::new(
                    self.segments.clone(),
                    parts,
                    names.clone(),
                    read_schema.clone(),
                    filter.clone(),
                )));
            }
            if !mem.is_empty() {
                inputs.push(MemorySourceConfig::try_new_exec(&[mem], read_schema.clone(), None)?);
            }
            plans.push(if merge {
                Arc::new(MergeVersionsExec::try_new(UnionExec::try_new(inputs)?)?)
            } else {
                // One segment: its rows are already one per key; only tombstones must go.
                let pred = state.create_physical_expr(not(col(c::DELETED)), &df_read)?;
                Arc::new(FilterExec::try_new(pred, inputs.pop().unwrap())?)
            });
        }
        let merged: Arc<dyn ExecutionPlan> = match plans.len() {
            0 => Arc::new(EmptyExec::new(read_schema.clone())),
            _ => UnionExec::try_new(plans)?, // projects are disjoint key spaces
        };

        // Output projection, with the derived total_tokens (decision 15) for EVERY read path.
        let exprs = out_cols
            .iter()
            .map(|&name| {
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
