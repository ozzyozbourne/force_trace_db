//! `POST /v1/search` (M4, plans/phase-5 §3.5): newest-first full-text hits over the shared `page` loop.
use crate::filters::KeyFilters;
use crate::fts_scan::{Tree, Unit, lookup, part_hits, scan_hits, single_source, split};
use crate::read::{ReadView, SegmentPart, coalesce, mem_versions, merge_or_filter, needs_merge, read_columns};
use crate::runs::{
    Candidate, CandidateSource, Cursor, Ids, RUN_COLUMNS, RunsPage, bad, decode_cursor, encode_cursor, in_set,
    in_window, newest_first_segments, page, page_json, parse_list, parse_time, upper_bound,
};
use crate::{FtsCounters, QueryCtx, QueryEngine, QueryError, QueryKind};
use arrow::array::{Array, AsArray, BooleanArray, RecordBatch, StringArray, UInt32Array};
use arrow::compute::{concat_batches, filter_record_batch, take_record_batch};
use datafusion::execution::memory_pool::MemoryReservation;
use forge_index::{Analyzer, FtsColumn, FtsExpr, FtsPredicate, IndexError, IndexReader, Matcher};
use forge_model::catalog::{SegmentId, SegmentMeta};
use forge_model::enums::{RunType, SpanStatus};
use forge_model::ids::{ProjectId, SpanId, TraceId};
use forge_model::schema::{col, project_to, projected_schema};
use forge_storage::RowSelection;
use futures::{StreamExt, TryStreamExt};
use roaring::RoaringTreemap;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const MAX_Q: usize = 1024;
const CURSOR_TAG: u8 = 2;

/// The JSON body of `POST /v1/search`.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchBody {
    pub q: String,
    pub project: Option<String>,
    pub columns: Option<Vec<String>>,
    pub filters: Option<SearchFilters>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchFilters {
    pub start: Option<String>,
    pub end: Option<String>,
    pub run_type: Option<Vec<String>>,
    pub status: Option<Vec<String>>,
    pub roots_only: Option<bool>,
    pub json: Option<Vec<JsonFilter>>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonFilter {
    pub column: String,
    pub path: String,
    pub text: String,
}

#[derive(Clone, Debug)]
pub struct SearchRequest {
    pub project: ProjectId,
    pub q: String,
    pub columns: Vec<FtsColumn>,
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub run_types: BTreeSet<RunType>,
    pub statuses: BTreeSet<SpanStatus>,
    pub roots_only: bool,
    pub json: Vec<(FtsColumn, String, String)>,
    pub limit: usize,
    pub cursor: Option<Cursor>,
}

impl SearchRequest {
    pub fn parse(b: SearchBody, project: ProjectId) -> Result<Self, QueryError> {
        let limit = b.limit.unwrap_or(50);
        if !(1..=crate::runs::MAX_LIMIT).contains(&limit) {
            return Err(bad(format!(
                "limit must be 1..={}, got {limit}",
                crate::runs::MAX_LIMIT
            )));
        }
        if b.q.trim().is_empty() || b.q.len() > MAX_Q {
            return Err(bad(format!("q must be non-blank and at most {MAX_Q} bytes")));
        }
        let column = |c: &str| FtsColumn::from_name(c).ok_or_else(|| bad(format!("not a search column: {c:?}")));
        let mut columns = match b.columns {
            Some(cs) => cs.iter().map(|c| column(c)).collect::<Result<Vec<_>, _>>()?,
            None => FtsColumn::ALL.to_vec(),
        };
        columns.sort_by_key(|c| *c as u8);
        columns.dedup();
        if columns.is_empty() {
            return Err(bad("columns must not be empty"));
        }
        let f = b.filters.unwrap_or_default();
        let json = f
            .json
            .unwrap_or_default()
            .into_iter()
            .map(|j| {
                let c = column(&j.column)?;
                if !c.is_json() {
                    return Err(bad(format!("{} is not a JSON column", j.column)));
                }
                Ok((c, j.path, j.text))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut r = Self {
            project,
            q: b.q,
            columns,
            start_ns: parse_time("start", f.start.as_deref())?,
            end_ns: parse_time("end", f.end.as_deref())?,
            run_types: parse_list(f.run_type.map(|v| v.join(",")).as_deref())?,
            statuses: parse_list(f.status.map(|v| v.join(",")).as_deref())?,
            roots_only: f.roots_only.unwrap_or(false),
            json,
            limit,
            cursor: None,
        };
        // A syntax error is a 400 now, not mid-query.
        Matcher::for_expr(&mut Analyzer::default(), &r.expr())?;
        r.cursor = b.cursor.map(|c| r.decode_cursor(&c)).transpose()?;
        Ok(r)
    }

    /// And([Or(columns: search(c, q)), json_key_search...]).
    pub fn expr(&self) -> FtsExpr {
        let mut and = vec![FtsExpr::Or(
            self.columns
                .iter()
                .map(|c| {
                    FtsExpr::Pred(FtsPredicate::Search {
                        column: *c,
                        query: self.q.clone(),
                    })
                })
                .collect(),
        )];
        and.extend(self.json.iter().map(|(c, path, text)| {
            FtsExpr::Pred(FtsPredicate::JsonKeySearch {
                column: *c,
                path: path.clone(),
                text: text.clone(),
            })
        }));
        FtsExpr::And(and)
    }

    fn hash(&self) -> u32 {
        let rt: Vec<&str> = self.run_types.iter().map(|r| r.as_str()).collect();
        let st: Vec<&str> = self.statuses.iter().map(|r| r.as_str()).collect();
        let cols: Vec<&str> = self.columns.iter().map(|c| c.name()).collect();
        let norm = format!(
            "{}|{:?}|{cols:?}|{:?}|{:?}|{rt:?}|{st:?}|{}|{:?}|{}",
            self.project, self.q, self.start_ns, self.end_ns, self.roots_only, self.json, self.limit
        );
        crc32fast::hash(norm.as_bytes())
    }

    pub fn encode_cursor(&self, c: &Cursor) -> String {
        encode_cursor(CURSOR_TAG, c, self.hash())
    }

    fn decode_cursor(&self, s: &str) -> Result<Cursor, QueryError> {
        decode_cursor(CURSOR_TAG, s, self.hash(), "cursor does not match the search")
    }
}

/// One segment's index answer for this call (D8, D10, D11).
struct Hits {
    /// Per unit; None = no usable index (brute-forced when the segment is read).
    per_unit: Vec<Option<RoaringTreemap>>,
    /// `Tree::rows(per_unit)` when every unit has rows: the single-source rows that match (D11).
    tree_rows: Option<RoaringTreemap>,
    /// Some hit row has a null start (D8).
    start_less: bool,
}

impl Hits {
    fn new(tree: &Tree, per_unit: Vec<Option<RoaringTreemap>>, start_less: bool) -> Self {
        let rows: Option<Vec<RoaringTreemap>> = per_unit.iter().cloned().collect();
        Self {
            tree_rows: rows.map(|r| tree.rows(&r)),
            per_unit,
            start_less,
        }
    }
}

struct FtsSource<'a> {
    r: &'a SearchRequest,
    hi: Option<i64>,
    index: &'a dyn IndexReader,
    counters: Arc<FtsCounters>,
    units: Arc<Vec<Unit>>,
    tree: Arc<Tree>,
    read_group: usize,
    /// Index answers of this call, per segment.
    memo: Mutex<HashMap<SegmentId, Arc<Hits>>>,
    /// One index lookup per segment, shared by concurrent callers (`start`, `prefetch`, `segments`).
    lookups: moka::future::Cache<SegmentId, Arc<Hits>>,
    /// Per segment: its hit rows (ascending) and their RUN_COLUMNS, when at most a page of them (verify takes its
    /// single-source rows from here instead of reading them again), charged to the call's pool.
    hit_rows: Mutex<HashMap<SegmentId, Arc<(RoaringTreemap, RecordBatch, MemoryReservation)>>>,
    /// Traces with memtable rows in the snapshot (D11).
    mem_traces: Mutex<BTreeSet<TraceId>>,
    /// Segments brute-forced by this call (D4).
    bruted: AtomicUsize,
}

impl<'a> FtsSource<'a> {
    fn new(
        r: &'a SearchRequest,
        hi: Option<i64>,
        index: &'a dyn IndexReader,
        counters: Arc<FtsCounters>,
        read_group: usize,
    ) -> Result<Self, QueryError> {
        let mut units = Vec::new();
        let tree = split(&mut Analyzer::default(), &r.expr(), &mut units)?;
        Ok(Self {
            r,
            hi,
            index,
            counters,
            units: Arc::new(units),
            tree: Arc::new(tree),
            read_group,
            memo: Mutex::default(),
            lookups: moka::future::Cache::new(u64::MAX),
            hit_rows: Mutex::default(),
            mem_traces: Mutex::default(),
            bruted: AtomicUsize::new(0),
        })
    }

    async fn lookup(&self, seg: &SegmentMeta) -> Result<Arc<Hits>, QueryError> {
        if let Some(h) = self.memo.lock().expect("poisoned").get(&seg.id) {
            return Ok(h.clone());
        }
        let h = self
            .lookups
            .try_get_with(seg.id, async {
                let (per_unit, start_less) = lookup(self.index, seg, &self.units).await?;
                Ok::<_, IndexError>(Arc::new(Hits::new(&self.tree, per_unit, start_less)))
            })
            .await
            .map_err(|e| QueryError::from((*e).clone()))?;
        Ok(self.memo.lock().expect("poisoned").entry(seg.id).or_insert(h).clone())
    }

    /// `lookup` for every segment of `segs` not looked up yet, concurrently. Ahead of the reads, so a failed one
    /// (a query error such as "prefix too broad") is not memoised and fails the call only if `part` reads that
    /// segment; meanwhile its frontier stays conservative.
    async fn lookup_all(&self, segs: &[&SegmentMeta]) -> Result<(), QueryError> {
        let mut futs = Vec::with_capacity(segs.len());
        for s in segs {
            futs.push(self.lookup(s));
        }
        futures::stream::iter(futs)
            .buffer_unordered(EAGER_LOOKUPS)
            .for_each(|_| async {})
            .await;
        Ok(())
    }

    /// One part: per-unit rows (brute force for NoIndex), then the key + start of every hit row.
    async fn part(&self, q: &QueryCtx, view: &ReadView, part: &SegmentPart) -> Result<Vec<Candidate>, QueryError> {
        let hits = self.lookup(&part.seg).await?;
        let mut h = part_hits(view, &q.pool, &self.counters, part, &self.units, hits.per_unit.clone()).await?;
        // At most a page of hits: read what verify needs too (key, start and RUN_COLUMNS), saving its read of them.
        // More (a common term): verify reads at most a page of them, so reading every hit row wide costs more than
        // it saves.
        let all = h.rows();
        let wide = all.len() <= self.r.limit as u64;
        let cols = if wide {
            read_columns(&RUN_COLUMNS)
        } else {
            vec![col::TRACE_ID, col::SPAN_ID, col::START_TIME]
        };
        scan_hits(view, &mut h, &cols).await?;
        if wide && !all.is_empty() {
            let b = concat_batches(&projected_schema(&cols), &h.batches)?;
            // Past the memory limit, verify reads them again instead.
            if let Ok(res) = q.reserve("search.hit_rows", std::slice::from_ref(&b)) {
                self.hit_rows
                    .lock()
                    .expect("poisoned")
                    .insert(part.seg.id, Arc::new((all, b, res)));
            }
        }
        let mut out = Vec::new();
        for b in &h.batches {
            let ids = Ids::of(b)?;
            for i in 0..b.num_rows() {
                let (t, sp) = ids.key(i)?;
                if ids.start.is_valid(i) {
                    let p = (ids.start.value(i), t, sp);
                    if in_window(self.r.start_ns, self.hi, self.r.cursor, &p) {
                        out.push(Candidate::At(p));
                    }
                } else {
                    self.counters.add(1, |c| &mut c.unpositioned);
                    out.push(Candidate::Unpositioned(t, sp));
                }
            }
        }
        if h.bruted {
            self.bruted.fetch_add(1, Ordering::Relaxed);
            // Complete now: single-source rows can be answered from it (no null-start bitmap: conservative).
            let hits = Arc::new(Hits::new(&self.tree, h.per_unit.into_iter().map(Some).collect(), true));
            self.memo.lock().expect("poisoned").insert(part.seg.id, hits);
        }
        Ok(out)
    }
}

/// Index lookups in flight for `start` and `prefetch`.
const EAGER_LOOKUPS: usize = 256;

#[async_trait::async_trait]
impl CandidateSource for FtsSource<'_> {
    /// D8. Conservative until looked up (+inf with null starts); a looked-up segment is +inf only if a hit row is
    /// start-less, None without hits. A NoIndex unit keeps the conservative bound (it is brute-forced).
    fn frontier(&self, s: &SegmentMeta) -> Option<i64> {
        let conservative = if s.start_time_nulls > 0 {
            Some(i64::MAX)
        } else {
            s.max_start_ns
        };
        match self.memo.lock().expect("poisoned").get(&s.id) {
            None => conservative,
            Some(h) if h.per_unit.iter().any(Option::is_none) => conservative,
            Some(h) if h.per_unit.iter().flatten().all(RoaringTreemap::is_empty) => None,
            Some(h) if h.start_less => Some(i64::MAX),
            Some(_) => s.max_start_ns,
        }
    }

    /// Looks up every segment whose conservative frontier is +inf, so only start-less hits keep it (D8).
    async fn start(&self, _q: &QueryCtx, _view: &ReadView, segs: &[SegmentMeta]) -> Result<(), QueryError> {
        let todo: Vec<&SegmentMeta> = segs.iter().filter(|s| s.start_time_nulls > 0).collect();
        self.lookup_all(&todo).await
    }

    async fn prefetch(&self, _q: &QueryCtx, _view: &ReadView, next: &[SegmentMeta]) -> Result<(), QueryError> {
        let todo: Vec<&SegmentMeta> = next.iter().collect();
        self.lookup_all(&todo).await
    }

    fn group_end(&self, _segs: &[SegmentMeta], i: usize) -> usize {
        i + self.read_group
    }

    async fn mem(&self, q: &QueryCtx, view: &ReadView) -> Result<Vec<Candidate>, QueryError> {
        let mut cols: BTreeSet<&'static str> = [col::PROJECT_ID, col::TRACE_ID, col::SPAN_ID, col::START_TIME].into();
        cols.extend(self.units.iter().map(|u| u.col.name()));
        let cols: Vec<&'static str> = cols.into_iter().collect();
        let (snap, p, units) = (view.mem().clone(), self.r.project.clone(), self.units.clone());
        let (r, hi) = (self.r.clone(), self.hi);
        let (cands, traces, unpositioned) = q
            .blocking("search.mem", move |cancel| {
                let mut an = Analyzer::default();
                let mut out = Vec::new();
                let mut traces = BTreeSet::new();
                let mut unpositioned = 0;
                // One prefix count per unit and predicate: the memtable is one source (D13).
                let mut seen: Vec<Vec<forge_index::PrefixSeen>> = vec![Vec::new(); units.len()];
                for b in mem_versions(&snap, &KeyFilters::project(&p, None), &projected_schema(&cols), cancel)? {
                    cancel.check()?;
                    let s = b.schema_ref();
                    let ids = Ids::of(&b)?;
                    let cells: Vec<&StringArray> = units
                        .iter()
                        .map(|u| Ok(b.column(s.index_of(u.col.name())?).as_string::<i32>()))
                        .collect::<Result<_, QueryError>>()?;
                    for i in 0..b.num_rows() {
                        let tid = TraceId::from_slice(ids.trace.value(i))?;
                        traces.insert(tid);
                        let mut hit = false;
                        for ((u, c), seen) in units.iter().zip(&cells).zip(&mut seen) {
                            hit |= c.is_valid(i) && u.matches_counting(&mut an, c.value(i), seen)?;
                        }
                        if !hit {
                            continue;
                        }
                        let sid = SpanId::from_slice(ids.span.value(i))?;
                        if ids.start.is_valid(i) {
                            let pos = (ids.start.value(i), tid, sid);
                            if in_window(r.start_ns, hi, r.cursor, &pos) {
                                out.push(Candidate::At(pos));
                            }
                        } else {
                            unpositioned += 1;
                            out.push(Candidate::Unpositioned(tid, sid));
                        }
                    }
                }
                Ok((out, traces, unpositioned))
            })
            .await?;
        self.counters.add(unpositioned, |c| &mut c.unpositioned);
        *self.mem_traces.lock().expect("poisoned") = traces;
        Ok(cands)
    }

    async fn segments(
        &self,
        q: &QueryCtx,
        view: &ReadView,
        parts: &[SegmentPart],
    ) -> Result<Vec<Candidate>, QueryError> {
        let mut futs = Vec::with_capacity(parts.len());
        for p in parts {
            futs.push(self.part(q, view, p));
        }
        let v: Vec<Vec<Candidate>> = futures::stream::iter(futs)
            .buffer_unordered(parts.len().max(1))
            .try_collect()
            .await?;
        Ok(v.into_iter().flatten().collect())
    }

    async fn verify(
        &self,
        q: &QueryCtx,
        view: &ReadView,
        wave: &[Candidate],
        res: MemoryReservation,
    ) -> Result<(RecordBatch, MemoryReservation), QueryError> {
        let keys: BTreeSet<(TraceId, SpanId)> = wave.iter().map(Candidate::key).collect();
        let traces: BTreeSet<TraceId> = keys.iter().map(|k| k.0).collect();
        let kf = KeyFilters {
            traces: Some(traces.clone()),
            spans: Some(keys.clone()),
            ..KeyFilters::project(&self.r.project, None)
        };
        let locs = view.locate_by_trace(&kf).await?;
        let memo = self.memo.lock().expect("poisoned").clone();
        let mem_traces = self.mem_traces.lock().expect("poisoned").clone();
        let all_exact = self.units.iter().all(|u| u.exact);
        // D11, as in SQL `select`: a segment's rows were computed in this call when every unit has rows.
        let single = single_source(locs, all_exact, &mem_traces, &|id| {
            memo.get(&id).is_some_and(|h| h.tree_rows.is_some())
        });
        let exact_parts: Vec<SegmentPart> = single
            .rows
            .into_values()
            .filter_map(|(seg, keep)| {
                // Only rows that satisfy the tree on this segment (RD H2): the union was a key-level superset.
                let rows = memo[&seg.id].tree_rows.as_ref().expect("single") & keep;
                (!rows.is_empty()).then(|| SegmentPart {
                    seg,
                    rows: RowSelection::Rows(Arc::new(rows)),
                })
            })
            .collect();
        let merge_traces: BTreeSet<TraceId> = traces.iter().copied().filter(|t| !single.traces.contains(t)).collect();
        let merge_parts = coalesce(single.merge, crate::runs::VERIFY_GAP);
        let out_cols = read_columns(&RUN_COLUMNS);
        let mut wide: BTreeSet<&'static str> = out_cols.iter().copied().collect();
        wide.extend(self.units.iter().map(|u| u.col.name()));
        let wide: Vec<&'static str> = forge_model::schema::ALL_COLUMNS
            .iter()
            .copied()
            .filter(|c| wide.contains(c))
            .collect();
        // Single-source rows already read by `part` are taken from there; the others are read now.
        let (mut exact, mut unread) = (Vec::new(), Vec::new());
        {
            let memo = self.hit_rows.lock().expect("poisoned");
            for p in exact_parts {
                let RowSelection::Rows(rows) = &p.rows else {
                    unreachable!("exact parts are Rows")
                };
                match memo.get(&p.seg.id) {
                    Some(m) if rows.is_subset(&m.0) => {
                        let idx: UInt32Array = rows.iter().map(|r| (m.0.rank(r) - 1) as u32).collect();
                        exact.push(take_record_batch(&m.1, &idx)?);
                    }
                    _ => unread.push(p),
                }
            }
        }
        // As in runs: the wave's span ids pushed into the merge scan.
        let filter = crate::runs::span_filter(&keys.iter().map(|k| k.1).collect());
        let (read, merged) = futures::try_join!(
            view.scan(&unread, &out_cols, None),
            view.scan(&merge_parts, &wide, filter)
        )?;
        exact.extend(read);
        let (snap, r, hi) = (view.mem().clone(), self.r.clone(), self.hi);
        let (units, tree, counters) = (self.units.clone(), self.tree.clone(), self.counters.clone());
        let mk = KeyFilters {
            traces: Some(merge_traces),
            ..kf.clone()
        };
        q.blocking("search.verify", move |cancel| {
            let out_schema = projected_schema(&out_cols);
            let wide_schema = projected_schema(&wide);
            let mut an = Analyzer::default();
            // Single-source rows: the index answer is the answer (tombstones still dropped).
            let ex: Vec<RecordBatch> = exact.into_iter().map(|b| kf.apply(b)).collect::<Result<_, _>>()?;
            // The rest: every version, merged, re-checked on the merged cells.
            let mut batches: Vec<RecordBatch> = merged.into_iter().map(|b| kf.apply(b)).collect::<Result<_, _>>()?;
            let mem = mem_versions(&snap, &mk, &wide_schema, cancel)?;
            let merge = needs_merge(&merge_parts, !mem.is_empty());
            batches.extend(mem);
            // Charged like runs: the inputs, then the merged rows, then the page.
            let size = |bs: &[RecordBatch]| bs.iter().map(|b| b.get_array_memory_size()).sum::<usize>();
            let kept = res.size();
            res.try_grow(size(&ex) + size(&batches))?;
            let ex = merge_or_filter(&out_schema, &ex, false, cancel)?;
            let m = merge_or_filter(&wide_schema, &batches, merge, cancel)?;
            res.try_grow(ex.get_array_memory_size() + m.get_array_memory_size())?;
            drop(batches);
            res.resize(kept + ex.get_array_memory_size() + m.get_array_memory_size());
            let s = m.schema_ref();
            let cells: Vec<&StringArray> = units
                .iter()
                .map(|u| Ok(m.column(s.index_of(u.col.name())?).as_string::<i32>()))
                .collect::<Result<_, QueryError>>()?;
            let fts: BooleanArray = (0..m.num_rows())
                .map(|i| {
                    let b: Vec<bool> = units
                        .iter()
                        .zip(&cells)
                        .map(|(u, c)| c.is_valid(i) && u.matches(&mut an, c.value(i)))
                        .collect();
                    Some(tree.eval(&b))
                })
                .collect();
            let m = project_to(&filter_record_batch(&m, &fts)?, &out_schema)?;
            counters.add(ex.num_rows() as u64, |c| &mut c.exact_rows);
            counters.add(m.num_rows() as u64, |c| &mut c.merge_rows);
            let all = concat_batches(&out_schema, [&ex, &m])?;
            let out = filter_record_batch(&all, &row_mask(&r, hi, &all)?)?;
            res.try_grow(out.get_array_memory_size())?;
            drop((ex, m, all));
            res.resize(kept + out.get_array_memory_size());
            Ok((out, res))
        })
        .await
    }
}

/// Window, cursor, project, run_type, status, roots_only on merged rows.
fn row_mask(r: &SearchRequest, hi: Option<i64>, m: &RecordBatch) -> Result<BooleanArray, QueryError> {
    let s = m.schema_ref();
    let par = m.column(s.index_of(col::PARENT_SPAN_ID)?);
    let project = m.column(s.index_of(col::PROJECT_ID)?).as_string::<i32>();
    let rt = m.column(s.index_of(col::RUN_TYPE)?).as_string::<i32>();
    let st = m.column(s.index_of(col::STATUS)?).as_string::<i32>();
    let ids = Ids::of(m)?;
    (0..m.num_rows())
        .map(|i| {
            let (t, sp) = ids.key(i)?;
            let pos = (ids.start.value(i), t, sp);
            Ok(Some(
                ids.start.is_valid(i)
                    && project.value(i) == r.project.as_str()
                    && (!r.roots_only || par.is_null(i))
                    && in_window(r.start_ns, hi, r.cursor, &pos)
                    && in_set(&r.run_types, rt, i)
                    && in_set(&r.statuses, st, i),
            ))
        })
        .collect()
}

impl QueryEngine {
    pub async fn search(&self, r: &SearchRequest) -> Result<RunsPage, QueryError> {
        let q = self.ctx(QueryKind::Fast, self.cfg.timeout);
        q.run(async { Ok(self.search_inner(&q, r).await?.0) }).await
    }

    /// `{"hits":[…],"next_cursor":hex|null}`.
    pub async fn search_json(&self, r: &SearchRequest) -> Result<Vec<u8>, QueryError> {
        let q = self.ctx(QueryKind::Fast, self.cfg.timeout);
        q.run(async {
            let (page, res) = self.search_inner(&q, r).await?;
            let cursor = page.next_cursor.map(|c| r.encode_cursor(&c));
            page_json(&q, "search.json", "hits", page, res, cursor).await
        })
        .await
    }

    async fn search_inner(&self, q: &QueryCtx, r: &SearchRequest) -> Result<(RunsPage, MemoryReservation), QueryError> {
        let view = self.read_view(); // FIRST (invariant 1)
        let hi = upper_bound(r.end_ns, r.cursor);
        let segs = newest_first_segments(&view, &r.project, r.start_ns, hi).await?;
        let src = FtsSource::new(r, hi, &*self.index, self.fts.clone(), self.read_group)?;
        let out = page(q, &view, &src, segs, r.limit, self.prefetch).await?;
        let n = src.bruted.load(Ordering::Relaxed);
        if n > 0 {
            tracing::warn!(project = %r.project, segments = n, "search: segments without a usable .fts were scanned by brute force");
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_model::catalog::{HourBucket, Level};
    use forge_model::testing::segment_meta;

    /// D8: conservative until looked up; then +inf only for start-less hits, max_start for other hits, None
    /// without hits; a NoIndex unit keeps the conservative bound.
    #[test]
    fn fts_frontier() {
        let p = ProjectId::new("p".to_string()).unwrap();
        let r = SearchRequest::parse(
            SearchBody {
                q: "refund".into(),
                ..Default::default()
            },
            p,
        )
        .unwrap();
        let src = FtsSource::new(&r, None, &forge_index::NoopIndexReader, Arc::default(), 8).unwrap();
        let n = src.units.len();
        let mut s = segment_meta("p", Level::L0, HourBucket(0));
        s.max_start_ns = Some(5);
        s.start_time_nulls = 3;
        assert_eq!(src.frontier(&s), Some(i64::MAX), "not looked up, nulls");
        let set = |per_unit: Vec<Option<RoaringTreemap>>, start_less: bool| {
            let h = Arc::new(Hits::new(&src.tree, per_unit, start_less));
            src.memo.lock().unwrap().insert(s.id, h);
        };
        let hit = || Some(RoaringTreemap::from_iter([1u64]));
        set(vec![hit(); n], true);
        assert_eq!(src.frontier(&s), Some(i64::MAX), "start-less hits");
        set(vec![hit(); n], false);
        assert_eq!(src.frontier(&s), Some(5), "hits disjoint from the null starts");
        set(vec![Some(RoaringTreemap::new()); n], false);
        assert_eq!(src.frontier(&s), None, "no hits");
        let mut v = vec![Some(RoaringTreemap::new()); n];
        v[0] = None;
        set(v, false);
        assert_eq!(src.frontier(&s), Some(i64::MAX), "no sidecar with nulls");
        s.start_time_nulls = 0;
        assert_eq!(src.frontier(&s), Some(5), "no sidecar, no nulls");
        src.memo.lock().unwrap().clear();
        assert_eq!(src.frontier(&s), Some(5), "not looked up, no nulls");
    }

    #[test]
    fn parse_rejects_bad_requests() {
        let p = ProjectId::new("p".to_string()).unwrap();
        let body = |f: &dyn Fn(&mut SearchBody)| {
            let mut b = SearchBody {
                q: "refund".into(),
                ..Default::default()
            };
            f(&mut b);
            SearchRequest::parse(b, p.clone())
        };
        assert!(body(&|_| {}).is_ok());
        assert!(body(&|b| b.limit = Some(0)).is_err());
        assert!(body(&|b| b.limit = Some(1001)).is_err());
        assert!(body(&|b| b.q = " ".into()).is_err());
        assert!(body(&|b| b.q = "x".repeat(1025)).is_err());
        assert!(body(&|b| b.columns = Some(vec!["model".into()])).is_err());
        assert!(
            body(&|b| b.filters = Some(SearchFilters {
                json: Some(vec![JsonFilter {
                    column: "name".into(),
                    path: "$.a".into(),
                    text: "x".into()
                }]),
                ..Default::default()
            }))
            .is_err()
        );
        let r = body(&|_| {}).unwrap();
        let c = r.encode_cursor(&Cursor((1, TraceId([1; 16]), SpanId([2; 8]))));
        assert_eq!(
            body(&|b| b.cursor = Some(c.clone())).unwrap().cursor.map(|c| c.0.0),
            Some(1)
        );
        let mut tag1 = hex::decode(&c).unwrap();
        tag1[0] = 1;
        assert!(body(&|b| b.cursor = Some(hex::encode(&tag1))).is_err());
        assert!(
            body(&|b| {
                b.cursor = Some(c.clone());
                b.q = "other".into();
            })
            .is_err()
        );
        assert!(
            body(&|b| {
                b.cursor = Some(c.clone());
                b.limit = Some(7);
            })
            .is_err(),
            "the cursor is bound to the limit (D7)"
        );
    }
}
