//! `POST /v1/search` (M4, plans/phase-5 §3.5): newest-first full-text hits over the shared `page` loop.
use crate::filters::KeyFilters;
use crate::fts_scan::{Tree, Unit, brute, lookup_unit, split};
use crate::read::{ReadView, SegmentPart, coalesce, mem_versions, merge_or_filter, needs_merge, read_columns};
use crate::runs::{
    Candidate, CandidateSource, Cursor, Pos, RUN_COLUMNS, RunsPage, bad, page, parse_list, parse_time, upper_bound,
};
use crate::{FtsCounters, QueryCtx, QueryEngine, QueryError, QueryKind};
use arrow::array::{Array, AsArray, BooleanArray, RecordBatch, StringArray};
use arrow::compute::{concat_batches, filter_record_batch};
use arrow::datatypes::TimestampNanosecondType;
use datafusion::execution::memory_pool::MemoryReservation;
use forge_index::{Analyzer, FtsColumn, FtsExpr, FtsPredicate, IndexReader, Matcher};
use forge_meta::SegmentFilter;
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
            return Err(bad(format!("limit must be 1..=1000, got {limit}")));
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
        let mut an = Analyzer::default();
        let mut ps = Vec::new();
        preds(&r.expr(), &mut ps);
        for p in ps {
            Matcher::new(&mut an, &p)?;
        }
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
        let (start, trace, span) = c.0;
        let mut b = vec![CURSOR_TAG];
        b.extend_from_slice(&start.to_be_bytes());
        b.extend_from_slice(trace.as_bytes());
        b.extend_from_slice(span.as_bytes());
        b.extend_from_slice(&self.hash().to_be_bytes());
        hex::encode(b)
    }

    fn decode_cursor(&self, s: &str) -> Result<Cursor, QueryError> {
        let b = hex::decode(s).map_err(|_| bad("bad cursor"))?;
        if b.len() != 37 || b[0] != CURSOR_TAG {
            return Err(bad("bad cursor"));
        }
        if b[33..37] != self.hash().to_be_bytes() {
            return Err(bad("cursor does not match the search"));
        }
        let start = i64::from_be_bytes(b[1..9].try_into().expect("8 bytes"));
        Ok(Cursor((
            start,
            TraceId::from_slice(&b[9..25])?,
            SpanId::from_slice(&b[25..33])?,
        )))
    }

    fn in_window(&self, hi: Option<i64>, p: &Pos) -> bool {
        self.start_ns.is_none_or(|x| p.0 >= x)
            && hi.is_none_or(|x| p.0 < x)
            && self.cursor.is_none_or(|Cursor(c)| *p < c)
    }
}

fn preds(e: &FtsExpr, out: &mut Vec<FtsPredicate>) {
    match e {
        FtsExpr::Pred(p) => out.push(p.clone()),
        FtsExpr::And(v) | FtsExpr::Or(v) => v.iter().for_each(|c| preds(c, out)),
    }
}

/// The tree on per-unit booleans of one merged row (exact: one value per column).
fn eval(t: &Tree, b: &[bool]) -> bool {
    match t {
        Tree::Unit(u) => b[*u],
        Tree::And(v) => v.iter().all(|c| eval(c, b)),
        Tree::Or(v) => v.iter().any(|c| eval(c, b)),
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
    units: Vec<Unit>,
    tree: Tree,
    read_group: usize,
    prefetch: usize,
    /// Index answers of this call, per segment.
    memo: Mutex<HashMap<SegmentId, Arc<Hits>>>,
    /// Traces with memtable rows in the snapshot (D11).
    mem_traces: Mutex<BTreeSet<TraceId>>,
    /// Segments brute-forced by this call (D4).
    bruted: AtomicUsize,
}

impl FtsSource<'_> {
    async fn lookup(&self, seg: &SegmentMeta) -> Result<Arc<Hits>, QueryError> {
        if let Some(h) = self.memo.lock().expect("poisoned").get(&seg.id) {
            return Ok(h.clone());
        }
        let looked = futures::future::try_join_all(self.units.iter().map(|u| lookup_unit(self.index, seg, u))).await?;
        let start_less = looked.iter().flatten().any(|(_, sl)| *sl);
        let h = Arc::new(Hits::new(
            &self.tree,
            looked.into_iter().map(|x| x.map(|(rows, _)| rows)).collect(),
            start_less,
        ));
        self.memo.lock().expect("poisoned").insert(seg.id, h.clone());
        Ok(h)
    }

    /// `lookup` for every segment of `segs` not looked up yet, concurrently.
    async fn lookup_all(&self, segs: &[&SegmentMeta]) -> Result<(), QueryError> {
        let mut futs = Vec::with_capacity(segs.len());
        for s in segs {
            futs.push(self.lookup(s));
        }
        futures::stream::iter(futs)
            .buffer_unordered(EAGER_LOOKUPS)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    /// One part: per-unit rows (brute force for NoIndex), then the key + start of every hit row.
    async fn part(&self, q: &QueryCtx, view: &ReadView, part: &SegmentPart) -> Result<Vec<Candidate>, QueryError> {
        let mut hits = self.lookup(&part.seg).await?;
        let bruted = hits.per_unit.iter().any(Option::is_none);
        FtsCounters::add(
            if bruted {
                &self.counters.brute_parts
            } else {
                &self.counters.index_parts
            },
            1,
        );
        if bruted {
            self.bruted.fetch_add(1, Ordering::Relaxed);
            let mut per_unit = Vec::with_capacity(self.units.len());
            for (u, h) in self.units.iter().zip(hits.per_unit.iter()) {
                per_unit.push(Some(match h {
                    Some(rows) => rows.clone(),
                    None => brute(view, &q.pool, part, u).await?,
                }));
            }
            // Complete now: single-source rows can be answered from it (no null-start bitmap: conservative).
            hits = Arc::new(Hits::new(&self.tree, per_unit, true));
            self.memo.lock().expect("poisoned").insert(part.seg.id, hits.clone());
        }
        // Key-level superset (FQ Lemma 2): OR of every unit's rows.
        let all = hits.per_unit.iter().flatten().fold(RoaringTreemap::new(), |a, b| a | b);
        if bruted || !all.is_empty() {
            FtsCounters::add(&self.counters.segments_read, 1);
        }
        if all.is_empty() {
            return Ok(Vec::new());
        }
        let hit = SegmentPart {
            seg: part.seg.clone(),
            rows: RowSelection::Rows(Arc::new(all)),
        };
        let batches = view
            .scan(&[hit], &[col::TRACE_ID, col::SPAN_ID, col::START_TIME], None)
            .await?;
        let mut out = Vec::new();
        for b in &batches {
            let s = b.schema_ref();
            let t = b.column(s.index_of(col::TRACE_ID)?).as_binary::<i32>();
            let sp = b.column(s.index_of(col::SPAN_ID)?).as_binary::<i32>();
            let st = b
                .column(s.index_of(col::START_TIME)?)
                .as_primitive::<TimestampNanosecondType>();
            for i in 0..b.num_rows() {
                let (t, sp) = (TraceId::from_slice(t.value(i))?, SpanId::from_slice(sp.value(i))?);
                if st.is_valid(i) {
                    let p = (st.value(i), t, sp);
                    if self.r.in_window(self.hi, &p) {
                        out.push(Candidate::At(p));
                    }
                } else {
                    FtsCounters::add(&self.counters.unpositioned, 1);
                    out.push(Candidate::Unpositioned(t, sp));
                }
            }
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

    fn prefetch_window(&self) -> usize {
        self.prefetch
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
        let (snap, p) = (view.mem().clone(), self.r.project.clone());
        // Matching needs the units on the blocking pool: rebuild them there (cheap, from the same expression).
        let expr = self.r.expr();
        let (r, hi) = (self.r.clone(), self.hi);
        let (cands, traces, unpositioned) = q
            .blocking("search.mem", move |cancel| {
                let mut an = Analyzer::default();
                let mut units = Vec::new();
                split(&mut an, &expr, &mut units)?;
                let mut out = Vec::new();
                let mut traces = BTreeSet::new();
                let mut unpositioned = 0;
                let mut seen = forge_index::PrefixSeen::default();
                for b in mem_versions(&snap, &KeyFilters::project(&p, None), &projected_schema(&cols), cancel)? {
                    let s = b.schema_ref();
                    let t = b.column(s.index_of(col::TRACE_ID)?).as_binary::<i32>();
                    let sp = b.column(s.index_of(col::SPAN_ID)?).as_binary::<i32>();
                    let st = b
                        .column(s.index_of(col::START_TIME)?)
                        .as_primitive::<TimestampNanosecondType>();
                    let cells: Vec<&StringArray> = units
                        .iter()
                        .map(|u| Ok(b.column(s.index_of(u.col.name())?).as_string::<i32>()))
                        .collect::<Result<_, QueryError>>()?;
                    for i in 0..b.num_rows() {
                        let tid = TraceId::from_slice(t.value(i))?;
                        traces.insert(tid);
                        let mut hit = false;
                        for (u, c) in units.iter().zip(&cells) {
                            hit |= c.is_valid(i) && u.matches_counting(&mut an, c.value(i), &mut seen)?;
                        }
                        if !hit {
                            continue;
                        }
                        let sid = SpanId::from_slice(sp.value(i))?;
                        if st.is_valid(i) {
                            let pos = (st.value(i), tid, sid);
                            if r.in_window(hi, &pos) {
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
        FtsCounters::add(&self.counters.unpositioned, unpositioned);
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
        let mut segs_of: HashMap<TraceId, BTreeSet<SegmentId>> = HashMap::new();
        for (t, l) in &locs {
            segs_of.entry(*t).or_default().insert(l.seg.id);
        }
        let memo = self.memo.lock().expect("poisoned").clone();
        let mem_traces = self.mem_traces.lock().expect("poisoned").clone();
        let all_exact = self.units.iter().all(|u| u.exact);
        // D11: every unit exact, no memtable rows, exactly one location, and that segment's rows computed in this call.
        let single = |t: &TraceId| {
            all_exact
                && !mem_traces.contains(t)
                && segs_of.get(t).is_some_and(|s| {
                    s.len() == 1 && s.iter().all(|id| memo.get(id).is_some_and(|h| h.tree_rows.is_some()))
                })
        };
        let mut single_rows: HashMap<SegmentId, (SegmentMeta, RoaringTreemap)> = HashMap::new();
        let mut merge_locs = Vec::new();
        for (t, l) in locs {
            match &l.rows {
                RowSelection::Range(r) if single(&t) => {
                    let e = single_rows
                        .entry(l.seg.id)
                        .or_insert_with(|| (l.seg.clone(), RoaringTreemap::new()));
                    e.1.insert_range(r.clone());
                }
                _ => merge_locs.push(l),
            }
        }
        let exact_parts: Vec<SegmentPart> = single_rows
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
        let merge_traces: BTreeSet<TraceId> = traces.iter().copied().filter(|t| !single(t)).collect();
        let merge_parts = coalesce(merge_locs, crate::runs::VERIFY_GAP);
        let out_cols = read_columns(&RUN_COLUMNS);
        let mut wide: BTreeSet<&'static str> = out_cols.iter().copied().collect();
        wide.extend(self.units.iter().map(|u| u.col.name()));
        let wide: Vec<&'static str> = forge_model::schema::ALL_COLUMNS
            .iter()
            .copied()
            .filter(|c| wide.contains(c))
            .collect();
        let (exact, merged) = futures::try_join!(
            view.scan(&exact_parts, &out_cols, None),
            view.scan(&merge_parts, &wide, None)
        )?;
        let (snap, r, hi, expr) = (view.mem().clone(), self.r.clone(), self.hi, self.r.expr());
        let counters = self.counters.clone();
        let mk = KeyFilters {
            traces: Some(merge_traces),
            ..kf.clone()
        };
        q.blocking("search.verify", move |cancel| {
            let out_schema = projected_schema(&out_cols);
            let wide_schema = projected_schema(&wide);
            let mut an = Analyzer::default();
            let mut units = Vec::new();
            let tree = split(&mut an, &expr, &mut units)?;
            let mask_keys = |b: RecordBatch| -> Result<RecordBatch, QueryError> {
                Ok(match kf.key_mask(&b)? {
                    Some(m) => filter_record_batch(&b, &m)?,
                    None => b,
                })
            };
            // Single-source rows: the index answer is the answer (tombstones still dropped).
            let ex: Vec<RecordBatch> = exact.into_iter().map(mask_keys).collect::<Result<_, _>>()?;
            let ex = merge_or_filter(&out_schema, &ex, false, cancel)?;
            // The rest: every version, merged, re-checked on the merged cells.
            let mut batches: Vec<RecordBatch> = merged.into_iter().map(mask_keys).collect::<Result<_, _>>()?;
            let mem = mem_versions(&snap, &mk, &wide_schema, cancel)?;
            let merge = needs_merge(&merge_parts, !mem.is_empty());
            batches.extend(mem);
            let m = merge_or_filter(&wide_schema, &batches, merge, cancel)?;
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
                    Some(eval(&tree, &b))
                })
                .collect();
            let m = project_to(&filter_record_batch(&m, &fts)?, &out_schema)?;
            FtsCounters::add(&counters.exact_rows, ex.num_rows() as u64);
            FtsCounters::add(&counters.merge_rows, m.num_rows() as u64);
            let all = concat_batches(&out_schema, [&ex, &m])?;
            let out = filter_record_batch(&all, &row_mask(&r, hi, &all)?)?;
            res.try_grow(out.get_array_memory_size())?;
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
    let start = m
        .column(s.index_of(col::START_TIME)?)
        .as_primitive::<TimestampNanosecondType>();
    let t = m.column(s.index_of(col::TRACE_ID)?).as_binary::<i32>();
    let sp = m.column(s.index_of(col::SPAN_ID)?).as_binary::<i32>();
    (0..m.num_rows())
        .map(|i| {
            let pos = (
                start.value(i),
                TraceId::from_slice(t.value(i))?,
                SpanId::from_slice(sp.value(i))?,
            );
            Ok(Some(
                start.is_valid(i)
                    && project.value(i) == r.project.as_str()
                    && (!r.roots_only || par.is_null(i))
                    && r.in_window(hi, &pos)
                    && (r.run_types.is_empty()
                        || rt.is_valid(i) && rt.value(i).parse().is_ok_and(|x| r.run_types.contains(&x)))
                    && (r.statuses.is_empty()
                        || st.is_valid(i) && st.value(i).parse().is_ok_and(|x| r.statuses.contains(&x))),
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
            q.blocking("search.json", move |cancel| {
                let held = res.size();
                res.try_grow(page.rows.get_array_memory_size())?;
                let mut out = Vec::with_capacity(page.rows.get_array_memory_size());
                crate::json::write_runs(&mut out, "hits", &page.rows, cursor.as_deref(), cancel)?;
                res.try_resize(held + out.capacity())?;
                Ok(out)
            })
            .await
        })
        .await
    }

    async fn search_inner(&self, q: &QueryCtx, r: &SearchRequest) -> Result<(RunsPage, MemoryReservation), QueryError> {
        let view = self.read_view(); // FIRST (invariant 1)
        let hi = upper_bound(r.end_ns, r.cursor);
        let filter = SegmentFilter {
            project_id: Some(r.project.clone()),
            start_ns: r.start_ns,
            end_ns: hi,
            newest_first: true,
            ..Default::default()
        };
        let segs = view.list_segments(&filter).await?;
        let mut an = Analyzer::default();
        let mut units = Vec::new();
        let tree = split(&mut an, &r.expr(), &mut units)?;
        let src = FtsSource {
            r,
            hi,
            index: &*self.index,
            units,
            tree,
            counters: self.fts.clone(),
            read_group: self.read_group,
            prefetch: self.prefetch,
            memo: Mutex::default(),
            mem_traces: Mutex::default(),
            bruted: AtomicUsize::new(0),
        };
        let out = page(
            q,
            &view,
            &src,
            segs,
            r.limit,
            &projected_schema(&read_columns(&RUN_COLUMNS)),
        )
        .await?;
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
        let mut units = Vec::new();
        let tree = split(&mut Analyzer::default(), &r.expr(), &mut units).unwrap();
        let n = units.len();
        let src = FtsSource {
            r: &r,
            hi: None,
            index: &forge_index::NoopIndexReader,
            counters: Arc::default(),
            units,
            tree,
            read_group: 8,
            prefetch: 64,
            memo: Mutex::default(),
            mem_traces: Mutex::default(),
            bruted: AtomicUsize::new(0),
        };
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
