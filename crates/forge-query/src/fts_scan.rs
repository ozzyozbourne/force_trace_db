//! Index-driven candidates for FTS filters (shared by the provider and the fast paths).
//!
//! Correctness (research/phase-5/fts_query.md §1): a predicate over ONE column holds on a merged row only if it
//! holds on the version row that supplied that column, so the keys of rows matching it in ANY source (index hits,
//! brute-forced memtable rows and sidecar-less segments) are a superset of the answer. Single-column subtrees
//! ("units") are therefore evaluated on rows; AND / OR across columns only on keys. Candidates are then either
//! single-source (every version of the trace is one segment row: the index answer is the answer) or re-read in
//! full, merged and re-checked.
use crate::FtsCounters;
use crate::QueryError;
use crate::filters::KeyFilters;
use crate::read::{ReadView, SegmentPart, coalesce, scan_request};
use arrow::array::{Array, AsArray, RecordBatch};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use forge_index::{Analyzer, FtsColumn, FtsExpr, FtsHits, FtsPredicate, IndexReader, Matcher, PrefixSeen};
use forge_model::catalog::SegmentId;
use forge_model::ids::{ProjectId, SpanId, TraceId};
use forge_model::schema::{col, projected_schema};
use forge_storage::RowSelection;
use futures::{StreamExt, TryStreamExt};
use roaring::RoaringTreemap;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub(crate) type Key = (TraceId, SpanId);

/// Index lookups + hit-key reads in flight.
const INDEX_CONCURRENCY: usize = 8;

/// A single-column subtree of the filter: evaluated per row (index or brute force).
#[derive(Debug)]
pub(crate) struct Unit {
    pub col: FtsColumn,
    pub expr: FtsExpr,
    /// The predicates of `expr` in depth-first order.
    matchers: Vec<Matcher>,
    /// No predicate needs a re-check: index rows == brute-force rows.
    pub exact: bool,
}

/// AND / OR of units, evaluated on key sets (or, for single-source traces, on row sets).
#[derive(Debug)]
pub(crate) enum Tree {
    Unit(usize),
    And(Vec<Tree>),
    Or(Vec<Tree>),
}

fn column_of(e: &FtsExpr, out: &mut BTreeSet<u8>) {
    match e {
        FtsExpr::Pred(FtsPredicate::Search { column, .. } | FtsPredicate::JsonKeySearch { column, .. }) => {
            out.insert(*column as u8);
        }
        FtsExpr::And(v) | FtsExpr::Or(v) => v.iter().for_each(|c| column_of(c, out)),
    }
}

fn preds<'a>(e: &'a FtsExpr, out: &mut Vec<&'a FtsPredicate>) {
    match e {
        FtsExpr::Pred(p) => out.push(p),
        FtsExpr::And(v) | FtsExpr::Or(v) => v.iter().for_each(|c| preds(c, out)),
    }
}

impl Unit {
    fn new(an: &mut Analyzer, col: FtsColumn, expr: FtsExpr) -> Result<Self, QueryError> {
        let mut ps = Vec::new();
        preds(&expr, &mut ps);
        let matchers = ps
            .into_iter()
            .map(|p| Matcher::new(an, p))
            .collect::<Result<Vec<_>, _>>()?;
        let exact = matchers.iter().all(|m| !m.query().needs_recheck);
        Ok(Self {
            col,
            expr,
            matchers,
            exact,
        })
    }

    /// The unit on one cell (null cells match nothing).
    pub fn matches(&self, an: &mut Analyzer, cell: &str) -> bool {
        self.eval(an, cell, None).expect("no prefix counting")
    }

    /// `matches` on a brute-forced source, counting prefix expansions (D13).
    pub fn matches_counting(&self, an: &mut Analyzer, cell: &str, seen: &mut PrefixSeen) -> Result<bool, QueryError> {
        self.eval(an, cell, Some(seen))
    }

    fn eval(&self, an: &mut Analyzer, cell: &str, mut seen: Option<&mut PrefixSeen>) -> Result<bool, QueryError> {
        fn go(
            e: &FtsExpr,
            ms: &[Matcher],
            next: &mut usize,
            an: &mut Analyzer,
            cell: &str,
            seen: &mut Option<&mut PrefixSeen>,
        ) -> Result<bool, QueryError> {
            Ok(match e {
                FtsExpr::Pred(_) => {
                    *next += 1;
                    let m = &ms[*next - 1];
                    match seen.as_deref_mut() {
                        Some(s) => m.matches_counting(an, cell, s)?,
                        None => m.matches(an, cell),
                    }
                }
                // Evaluate every child so `next` stays in step (no short-circuit).
                FtsExpr::And(v) => {
                    let mut all = true;
                    for c in v {
                        all &= go(c, ms, next, an, cell, seen)?;
                    }
                    all
                }
                FtsExpr::Or(v) => {
                    let mut any = false;
                    for c in v {
                        any |= go(c, ms, next, an, cell, seen)?;
                    }
                    any
                }
            })
        }
        go(&self.expr, &self.matchers, &mut 0, an, cell, &mut seen)
    }
}

/// Splits `e` into single-column units (pushed to `units`) under a key-level tree.
pub(crate) fn split(an: &mut Analyzer, e: &FtsExpr, units: &mut Vec<Unit>) -> Result<Tree, QueryError> {
    let mut cols = BTreeSet::new();
    column_of(e, &mut cols);
    if cols.len() == 1 {
        let col = FtsColumn::ALL[usize::from(*cols.first().expect("one"))];
        units.push(Unit::new(an, col, e.clone())?);
        return Ok(Tree::Unit(units.len() - 1));
    }
    Ok(match e {
        FtsExpr::And(v) => Tree::And(v.iter().map(|c| split(an, c, units)).collect::<Result<_, _>>()?),
        FtsExpr::Or(v) => Tree::Or(v.iter().map(|c| split(an, c, units)).collect::<Result<_, _>>()?),
        FtsExpr::Pred(_) => unreachable!("a predicate has one column"),
    })
}

impl Tree {
    /// Key level: sound across columns (each unit's set covers every key whose MERGED row satisfies it).
    pub fn keys(&self, per_unit: &[BTreeSet<Key>]) -> BTreeSet<Key> {
        match self {
            Tree::Unit(u) => per_unit[*u].clone(),
            Tree::And(v) => v
                .iter()
                .map(|c| c.keys(per_unit))
                .reduce(|a, b| a.intersection(&b).copied().collect())
                .unwrap_or_default(),
            Tree::Or(v) => v.iter().flat_map(|c| c.keys(per_unit)).collect(),
        }
    }
    /// Row level, within one segment: exact only for keys with a single version row.
    pub fn rows(&self, per_unit: &[RoaringTreemap]) -> RoaringTreemap {
        match self {
            Tree::Unit(u) => per_unit[*u].clone(),
            Tree::And(v) => v
                .iter()
                .map(|c| c.rows(per_unit))
                .reduce(|a, b| a & b)
                .unwrap_or_default(),
            Tree::Or(v) => v
                .iter()
                .map(|c| c.rows(per_unit))
                .fold(RoaringTreemap::new(), |a, b| a | b),
        }
    }
}

/// What a filter selects in one project.
#[derive(Debug, Default)]
pub(crate) struct Selected {
    /// Hit rows of single-source traces (`RowSelection::Rows`): the merged rows ARE these rows, and they match
    /// (no payload read, no re-check; tombstones still dropped).
    pub exact: Vec<SegmentPart>,
    /// Every location of the other candidate traces: merge with their memtable rows, then re-check.
    pub merge: Vec<SegmentPart>,
    /// The candidate traces that need the merge (mask the memtable with these).
    pub merge_traces: BTreeSet<TraceId>,
}

/// One part's hits: per-unit rows and the key of every hit row.
struct PartHits {
    seg: SegmentId,
    per_unit: Vec<RoaringTreemap>,
    keys: BTreeMap<u64, Key>,
}

fn clip(mut rows: RoaringTreemap, sel: &RowSelection) -> RoaringTreemap {
    match sel {
        RowSelection::All => rows,
        RowSelection::Range(r) => {
            rows.remove_range(..r.start);
            rows.remove_range(r.end..);
            rows
        }
        RowSelection::Rows(s) => rows & s.as_ref(),
    }
}

/// Rows a part selects.
pub(crate) fn part_rows(p: &SegmentPart) -> u64 {
    match &p.rows {
        RowSelection::All => p.seg.row_count,
        RowSelection::Range(r) => r.end - r.start,
        RowSelection::Rows(s) => s.len(),
    }
}

fn keys_of(b: &RecordBatch) -> Result<Vec<Key>, QueryError> {
    let s = b.schema_ref();
    let t = b.column(s.index_of(col::TRACE_ID)?).as_binary::<i32>();
    let sp = b.column(s.index_of(col::SPAN_ID)?).as_binary::<i32>();
    (0..b.num_rows())
        .map(|i| Ok((TraceId::from_slice(t.value(i))?, SpanId::from_slice(sp.value(i))?)))
        .collect()
}

/// Rows of `part` (file row ids) where `unit` holds, by scanning its column (no sidecar, or `NoIndex`). Streams:
/// one batch at a time, charged to `pool` (D5).
pub(crate) async fn brute(
    view: &ReadView,
    pool: &Arc<dyn MemoryPool>,
    part: &SegmentPart,
    unit: &Unit,
) -> Result<RoaringTreemap, QueryError> {
    let start = match &part.rows {
        RowSelection::All => 0,
        RowSelection::Range(r) => r.start,
        RowSelection::Rows(_) => unreachable!("candidate parts are All or Range"),
    };
    let req = scan_request(part, &[unit.col.name()], None);
    let mut stream = view.store().scan(&part.seg, req).await?;
    let res = MemoryConsumer::new("fts.brute").register(pool);
    let (mut an, mut seen) = (Analyzer::default(), PrefixSeen::default());
    let mut out = RoaringTreemap::new();
    let mut row = start;
    while let Some(b) = stream.try_next().await? {
        res.try_grow(b.get_array_memory_size())?;
        let c = b.column(0).as_string::<i32>();
        for i in 0..c.len() {
            if c.is_valid(i) && unit.matches_counting(&mut an, c.value(i), &mut seen)? {
                out.insert(row + i as u64);
            }
        }
        row += b.num_rows() as u64;
        res.shrink(b.get_array_memory_size());
    }
    Ok(out)
}

/// One unit's index answer for `seg`: `Ok(None)` when there is no usable sidecar (D4: every index error but a query
/// error falls back to brute force).
pub(crate) async fn lookup_unit(
    index: &dyn IndexReader,
    seg: &forge_model::catalog::SegmentMeta,
    u: &Unit,
) -> Result<Option<(RoaringTreemap, bool)>, QueryError> {
    match index.search(seg, &u.expr, None).await {
        Ok(FtsHits::Rows { rows, start_less, .. }) => Ok(Some((rows, start_less))),
        Ok(FtsHits::NoIndex) => Ok(None),
        Err(e @ forge_index::IndexError::Query(_)) => Err(e.into()),
        Err(e) => {
            tracing::debug!(segment = %seg.id, error = %e, ".fts unusable; brute force");
            Ok(None)
        }
    }
}

async fn part_hits(
    view: &ReadView,
    index: &dyn IndexReader,
    part: &SegmentPart,
    units: &[Unit],
    counters: &FtsCounters,
    pool: &Arc<dyn MemoryPool>,
) -> Result<PartHits, QueryError> {
    let looked = futures::future::try_join_all(units.iter().map(|u| lookup_unit(index, &part.seg, u))).await?;
    let bruted = looked.iter().any(Option::is_none);
    FtsCounters::add(
        if bruted {
            &counters.brute_parts
        } else {
            &counters.index_parts
        },
        1,
    );
    let mut per_unit = Vec::with_capacity(units.len());
    for (u, h) in units.iter().zip(looked) {
        per_unit.push(match h {
            Some((rows, _)) => clip(rows, &part.rows),
            None => brute(view, pool, part, u).await?,
        });
    }
    let all = per_unit.iter().fold(RoaringTreemap::new(), |a, b| a | b);
    if bruted || !all.is_empty() {
        FtsCounters::add(&counters.segments_read, 1);
    }
    let ids: Vec<u64> = all.iter().collect();
    let hit_part = SegmentPart {
        seg: part.seg.clone(),
        rows: RowSelection::Rows(Arc::new(all)),
    };
    let batches = view.scan(&[hit_part], &[col::TRACE_ID, col::SPAN_ID], None).await?;
    let mut keys = BTreeMap::new();
    let mut ids = ids.into_iter();
    for b in &batches {
        for k in keys_of(b)? {
            keys.insert(ids.next().expect("one row per selected id"), k);
        }
    }
    Ok(PartHits {
        seg: part.seg.id,
        per_unit,
        keys,
    })
}

/// The candidates of `expr` in project `p` over `parts` (this project's segment parts) and the memtable snapshot,
/// split into single-source rows and traces to merge. None: more than `max_keys` candidate keys (the caller falls
/// back to scanning with a post-merge re-check).
pub(crate) async fn select(
    view: &ReadView,
    index: &dyn IndexReader,
    counters: &FtsCounters,
    pool: &Arc<dyn MemoryPool>,
    p: &ProjectId,
    k: &KeyFilters,
    parts: &[SegmentPart],
    expr: &FtsExpr,
    max_keys: usize,
) -> Result<Option<Selected>, QueryError> {
    let mut an = Analyzer::default();
    let mut units = Vec::new();
    let tree = split(&mut an, expr, &mut units)?;
    let mut per_unit: Vec<BTreeSet<Key>> = vec![BTreeSet::new(); units.len()];

    // Memtable rows of the project (key filters only), brute force per unit.
    let mk = KeyFilters {
        projects: Some([p.clone()].into()),
        traces: k.traces.clone(),
        ..KeyFilters::default()
    };
    let mut mem_cols: BTreeSet<&'static str> = [col::PROJECT_ID, col::TRACE_ID, col::SPAN_ID].into();
    mem_cols.extend(units.iter().map(|u| u.col.name()));
    let mem_cols: Vec<&'static str> = mem_cols.into_iter().collect();
    let mut mem_traces: BTreeSet<TraceId> = BTreeSet::new();
    let mut seen = PrefixSeen::default();
    for b in view.mem_versions(&mk, &projected_schema(&mem_cols))? {
        let keys = keys_of(&b)?;
        mem_traces.extend(keys.iter().map(|k| k.0));
        for (ui, u) in units.iter().enumerate() {
            let c = b.column(b.schema().index_of(u.col.name())?).as_string::<i32>();
            for (i, key) in keys.iter().enumerate() {
                if c.is_valid(i) && u.matches_counting(&mut an, c.value(i), &mut seen)? {
                    per_unit[ui].insert(*key);
                }
            }
        }
    }

    // Segments: index (or brute force) per unit, then the keys of the hit rows.
    // A loop, not a closure: a closure over `&SegmentPart` trips the async_trait Send check (HRTB).
    let bruted0 = counters.brute_parts.load(Ordering::Relaxed);
    let mut futs = Vec::with_capacity(parts.len());
    for part in parts {
        futs.push(part_hits(view, index, part, &units, counters, pool));
    }
    let hits: Vec<PartHits> = futures::stream::iter(futs)
        .buffer_unordered(INDEX_CONCURRENCY)
        .try_collect()
        .await?;
    // Other queries add to the counter too: the count is approximate when they overlap.
    let n = counters.brute_parts.load(Ordering::Relaxed).saturating_sub(bruted0);
    if n > 0 {
        tracing::warn!(project = %p, segments = n, "search: segments without a usable .fts were scanned by brute force");
    }
    for h in &hits {
        for (ui, rows) in h.per_unit.iter().enumerate() {
            per_unit[ui].extend(rows.iter().map(|r| h.keys[&r]));
        }
    }
    let cand = tree.keys(&per_unit);
    if cand.len() > max_keys {
        return Ok(None);
    }
    let traces: BTreeSet<TraceId> = cand.iter().map(|k| k.0).collect();
    if traces.is_empty() {
        return Ok(Some(Selected::default()));
    }

    // Where every version of the candidate traces is.
    let lk = KeyFilters {
        projects: Some([p.clone()].into()),
        traces: Some(traces.clone()),
        start_ns: k.start_ns,
        end_ns: k.end_ns,
        ..KeyFilters::default()
    };
    let locs = view.locate_by_trace(&lk).await?;
    let mut segs_of: HashMap<TraceId, BTreeSet<SegmentId>> = HashMap::new();
    for (t, l) in &locs {
        segs_of.entry(*t).or_default().insert(l.seg.id);
    }
    let scanned: BTreeSet<SegmentId> = hits.iter().map(|h| h.seg).collect();
    let all_exact = units.iter().all(|u| u.exact);
    // Every version of the trace is one row of one scanned segment (one row per key per segment, invariant 3).
    let single = |t: &TraceId| {
        all_exact
            && !mem_traces.contains(t)
            && segs_of
                .get(t)
                .is_some_and(|s| s.len() == 1 && s.iter().all(|id| scanned.contains(id)))
    };

    let mut sel = Selected::default();
    // Exact rows: the unit tree on the rows of each segment, restricted to its single-source traces' ranges.
    let mut single_rows: HashMap<SegmentId, RoaringTreemap> = HashMap::new();
    for (t, l) in &locs {
        if let RowSelection::Range(r) = &l.rows
            && single(t)
        {
            single_rows.entry(l.seg.id).or_default().insert_range(r.clone());
        }
    }
    for h in &hits {
        if let Some(keep) = single_rows.get(&h.seg) {
            let rows = tree.rows(&h.per_unit) & keep;
            if !rows.is_empty() {
                let seg = parts.iter().find(|p| p.seg.id == h.seg).expect("hit part").seg.clone();
                FtsCounters::add(&counters.exact_rows, rows.len());
                sel.exact.push(SegmentPart {
                    seg,
                    rows: RowSelection::Rows(Arc::new(rows)),
                });
            }
        }
    }
    // Everything else: all versions, merged and re-checked (memtable-only traces too).
    sel.merge_traces = traces.into_iter().filter(|t| !single(t)).collect();
    let merge_locs: Vec<SegmentPart> = locs
        .into_iter()
        .filter(|(t, _)| sel.merge_traces.contains(t))
        .map(|(_, l)| l)
        .collect();
    sel.merge = coalesce(merge_locs, 0);
    FtsCounters::add(&counters.merge_rows, sel.merge.iter().map(part_rows).sum());
    Ok(Some(sel))
}

/// `IndexReader` over `.fts` sidecars in the segment store (`paths::fts_path`); open files (footer + fetched FST
/// blocks) are cached per segment. A segment without `fts_byte_size` is `NoIndex`.
#[derive(Debug)]
pub struct FtsIndexReader {
    store: Arc<dyn object_store::ObjectStore>,
    files: moka::future::Cache<SegmentId, Arc<forge_index::FtsFile>>,
}

impl FtsIndexReader {
    pub fn new(store: Arc<dyn object_store::ObjectStore>) -> Self {
        Self {
            store,
            files: moka::future::Cache::builder()
                .max_capacity(256 << 20)
                .weigher(|_, f: &Arc<forge_index::FtsFile>| {
                    let d = &f.footer().dict;
                    u32::try_from(d.end - d.start + 4096).unwrap_or(u32::MAX)
                })
                .build(),
        }
    }
}

type Fetch = futures::future::BoxFuture<'static, forge_index::IndexResult<Vec<bytes::Bytes>>>;

#[async_trait::async_trait]
impl IndexReader for FtsIndexReader {
    async fn search(
        &self,
        seg: &forge_model::catalog::SegmentMeta,
        expr: &FtsExpr,
        _limit_hint: Option<u64>,
    ) -> forge_index::IndexResult<FtsHits> {
        let Some(len) = seg.fts_byte_size else {
            return Ok(FtsHits::NoIndex);
        };
        let path = forge_model::catalog::paths::fts_path(&seg.path);
        let (store, loc) = (self.store.clone(), object_store::path::Path::from(path.as_str()));
        let fetch = move |rs: Vec<std::ops::Range<u64>>| -> Fetch {
            let (store, loc, p) = (store.clone(), loc.clone(), loc.to_string());
            Box::pin(async move {
                store
                    .get_ranges(&loc, &rs)
                    .await
                    .map_err(|e| forge_index::IndexError::Corrupt {
                        path: p,
                        msg: e.to_string(),
                    })
            })
        };
        let file = match self.files.get(&seg.id).await {
            Some(f) => f,
            None => {
                let f = Arc::new(forge_index::FtsFile::open(&path, len, &fetch).await?.0);
                self.files.insert(seg.id, f.clone()).await;
                f
            }
        };
        if u64::from(file.footer().num_rows) != seg.row_count {
            return Err(forge_index::IndexError::Corrupt {
                path,
                msg: format!("{} rows, the segment has {}", file.footer().num_rows, seg.row_count),
            });
        }
        // Every predicate's lookups at once: one wave of FST blocks, one of postings (D1).
        let mut an = Analyzer::default();
        let mut ps = Vec::new();
        preds(expr, &mut ps);
        let ms = ps
            .into_iter()
            .map(|p| Matcher::new(&mut an, p))
            .collect::<Result<Vec<_>, _>>()?;
        let needs_recheck = ms.iter().any(|m| m.query().needs_recheck);
        let bms = futures::future::try_join_all(ms.iter().map(|m| file.execute(m.query(), &fetch))).await?;
        let rows = combine(expr, &mut bms.into_iter().map(|(b, _)| b));
        let start_less = !rows.is_disjoint(file.null_starts());
        Ok(FtsHits::Rows {
            rows: RoaringTreemap::from_bitmaps([(0, rows)]),
            needs_recheck,
            start_less,
        })
    }
}

/// AND / OR of the per-predicate bitmaps, taken from `bms` in `preds` order.
fn combine(e: &FtsExpr, bms: &mut dyn Iterator<Item = roaring::RoaringBitmap>) -> roaring::RoaringBitmap {
    match e {
        FtsExpr::Pred(_) => bms.next().expect("one bitmap per predicate"),
        FtsExpr::And(v) => v
            .iter()
            .map(|c| combine(c, bms))
            .reduce(|a, b| a & b)
            .unwrap_or_default(),
        FtsExpr::Or(v) => v
            .iter()
            .map(|c| combine(c, bms))
            .fold(roaring::RoaringBitmap::new(), |a, b| a | b),
    }
}
