//! Trace fast path (D3): `locate_trace` row ranges + memtable rows -> merge -> tree -> JSON. No DataFusion.
use crate::filters::KeyFilters;
use crate::read::{mem_versions, merge_or_filter, needs_merge, read_columns};
use crate::tree::TraceTree;
use crate::{QueryEngine, QueryError, QueryKind};
use arrow::array::RecordBatch;
use datafusion::execution::memory_pool::MemoryReservation;
use forge_model::ids::{ProjectId, TraceId};
use forge_model::merge::Cancel;
use forge_model::schema::{ALL_COLUMNS, PAYLOAD_COLUMNS, col, projected_schema};

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
    /// `trace_id`: 32 hex digits (any case) or a dashed UUID (`8-4-4-4-12`). `format`: `tree` (default) or `flat`.
    pub fn parse(
        trace_id: &str,
        project: ProjectId,
        payloads: Option<bool>,
        format: Option<&str>,
    ) -> Result<Self, QueryError> {
        let bad = |e: &dyn std::fmt::Display| QueryError::InvalidInput(format!("bad trace id {trace_id:?}: {e}"));
        let dashes = trace_id.char_indices().filter(|(_, c)| *c == '-').map(|(i, _)| i);
        let hex = if trace_id.len() == 36 && dashes.eq([8, 13, 18, 23]) {
            trace_id.replace('-', "")
        } else {
            trace_id.to_string()
        };
        let trace = hex.parse::<TraceId>().map_err(|e| bad(&e))?;
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
pub(crate) fn trace_columns(payloads: bool) -> Vec<&'static str> {
    ALL_COLUMNS
        .iter()
        .copied()
        .filter(|c| *c != col::DOTTED_ORDER && (payloads || !PAYLOAD_COLUMNS.contains(c)))
        .collect()
}

pub struct LoadedTrace {
    /// Merged rows (trace columns, effective total_tokens), one per span; in key order unless they come from one
    /// segment alone (then in its physical order).
    pub rows: RecordBatch,
    pub tree: TraceTree,
}

impl QueryEngine {
    /// Merged rows and tree of one trace; NotFound when it has no live span.
    pub async fn load_trace(&self, r: &TraceRequest) -> Result<LoadedTrace, QueryError> {
        self.trace_blocking(r, |rows, tree, _, _| Ok(LoadedTrace { rows, tree }))
            .await
    }

    /// The trace as JSON (D5), nested or flat.
    pub async fn trace_json(&self, r: &TraceRequest) -> Result<Vec<u8>, QueryError> {
        let (project, trace, flat) = (
            r.project.to_string(),
            r.trace.to_string(),
            r.format == TraceFormat::Flat,
        );
        let write = move |rows: RecordBatch, tree: TraceTree, c: &Cancel, res: &MemoryReservation| {
            // The body: reserved up front at the row size, then at its real capacity.
            let held = res.size();
            res.try_grow(rows.get_array_memory_size())?;
            let mut out = Vec::with_capacity(rows.get_array_memory_size());
            crate::json::write_trace(&mut out, &project, &trace, &rows, &tree, flat, c)?;
            res.try_resize(held + out.capacity())?;
            Ok(out)
        };
        self.trace_blocking(r, write).await
    }

    /// Gathers every version of the trace's keys, then merges, builds the tree and runs `finish` on the blocking
    /// pool under one `Cancel` (dropping this future sets it). One reservation per call (D15) covers the input, then
    /// the merged rows and tree, then whatever `finish` adds: past `query.memory_limit` it is a 400.
    async fn trace_blocking<T: Send + 'static>(
        &self,
        r: &TraceRequest,
        finish: impl FnOnce(RecordBatch, TraceTree, &Cancel, &MemoryReservation) -> Result<T, QueryError> + Send + 'static,
    ) -> Result<T, QueryError> {
        let q = self.ctx(QueryKind::Fast, self.cfg.timeout);
        q.run(async {
            let view = self.read_view(); // FIRST (invariant 1)
            let k = KeyFilters::project(&r.project, Some(r.trace));
            let cols = read_columns(&trace_columns(r.payloads));
            let schema = projected_schema(&cols);
            // The memtable mask runs on the blocking pool while the segments are located and scanned.
            let (snap, k2, s2) = (view.mem().clone(), k.clone(), schema.clone());
            let mem = q.blocking("trace.mem", move |c| mem_versions(&snap, &k2, &s2, c));
            let segs = async {
                let parts = view.locate_versions(&k).await?;
                let batches = view.scan(&parts, &cols, None).await?;
                Ok::<_, QueryError>((parts, batches))
            };
            let ((parts, mut batches), mem) = tokio::try_join!(segs, mem)?;
            let merge = needs_merge(&parts, !mem.is_empty());
            batches.extend(mem);
            drop(view);
            let reservation = q.reserve("trace", &batches)?;
            let not_found = format!("trace {} not found in project {}", r.trace, r.project);
            let out = q
                .blocking("trace", move |cancel| {
                    let m = merge_or_filter(&schema, &batches, merge, cancel)?;
                    reservation.try_grow(m.get_array_memory_size())?;
                    drop(batches);
                    reservation.resize(m.get_array_memory_size());
                    if m.num_rows() == 0 {
                        return Err(QueryError::NotFound(not_found));
                    }
                    let tree = TraceTree::build(&m, cancel)?;
                    reservation.try_grow(tree.memory_size())?;
                    finish(m, tree, cancel, &reservation)
                })
                .await?;
            Ok(out)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_id_forms() {
        let p = ProjectId::new("p").unwrap();
        let parse = |s: &str| TraceRequest::parse(s, p.clone(), None, None).map(|r| r.trace);
        let want: TraceId = "0af7651916cd43dd8448eb211c80319c".parse().unwrap();
        assert_eq!(parse("0AF7651916CD43DD8448EB211C80319C").unwrap(), want);
        assert_eq!(parse("0af76519-16cd-43dd-8448-eb211c80319c").unwrap(), want);
        // 36 characters with 4 dashes, but not at 8, 13, 18 and 23.
        for bad in [
            "0af7651-916cd-43dd-8448-eb211c80319c",
            "0-af765191-6cd43dd8448eb2-11c80-319c",
        ] {
            assert_eq!(bad.len(), 36);
            assert!(matches!(parse(bad), Err(QueryError::InvalidInput(_))), "{bad}");
        }
    }
}
