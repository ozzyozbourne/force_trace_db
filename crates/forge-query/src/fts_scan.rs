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
use crate::read::{ReadView, SegmentPart, coalesce, mem_versions, scan_request};
use arrow::array::{Array, AsArray, RecordBatch};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use forge_index::{
    Analyzer, FtsColumn, FtsExpr, FtsFile, FtsHits, IndexError, IndexReader, IndexResult, Matcher, PrefixSeen,
};
use forge_model::catalog::{SegmentId, SegmentMeta};
use forge_model::ids::{ProjectId, SpanId, TraceId};
use forge_model::merge::{Cancel, CancelOnDrop};
use forge_model::row::span_keys;
use forge_model::schema::{col, projected_schema};
use forge_storage::RowSelection;
use futures::{StreamExt, TryStreamExt};
use roaring::{MultiOps, RoaringBitmap, RoaringTreemap};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

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

impl Unit {
    fn new(an: &mut Analyzer, col: FtsColumn, expr: FtsExpr) -> Result<Self, QueryError> {
        let matchers = Matcher::for_expr(an, &expr)?;
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

    /// `matches` on a brute-forced source, counting prefix expansions per predicate and leaf (D13): `seen` is this
    /// unit's state for one source (one segment, or the memtable), fed every non-null cell of it.
    pub fn matches_counting(
        &self,
        an: &mut Analyzer,
        cell: &str,
        seen: &mut Vec<PrefixSeen>,
    ) -> Result<bool, QueryError> {
        seen.resize_with(self.matchers.len(), Default::default);
        self.eval(an, cell, Some(seen))
    }

    /// Every predicate (no short-circuit: each one counts its prefix expansions), then the expression on them.
    fn eval(&self, an: &mut Analyzer, cell: &str, seen: Option<&mut Vec<PrefixSeen>>) -> Result<bool, QueryError> {
        let bs = match seen {
            Some(seen) => self
                .matchers
                .iter()
                .zip(seen.iter_mut())
                .map(|(m, s)| m.matches_counting(an, cell, s))
                .collect::<Result<Vec<bool>, _>>()?,
            None => self.matchers.iter().map(|m| m.matches(an, cell)).collect(),
        };
        let mut bs = bs.into_iter();
        Ok(self.expr.fold(
            &mut |_| bs.next().expect("one per predicate"),
            &|v| v.into_iter().all(|b| b),
            &|v| v.into_iter().any(|b| b),
        ))
    }
}

/// Splits `e` into single-column units (pushed to `units`) under a key-level tree.
pub(crate) fn split(an: &mut Analyzer, e: &FtsExpr, units: &mut Vec<Unit>) -> Result<Tree, QueryError> {
    let cols = e.columns();
    if cols.len() == 1 {
        let col = *cols.first().expect("one");
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
    fn fold<T>(&self, leaf: &dyn Fn(usize) -> T, and: &dyn Fn(Vec<T>) -> T, or: &dyn Fn(Vec<T>) -> T) -> T {
        match self {
            Tree::Unit(u) => leaf(*u),
            Tree::And(v) => and(v.iter().map(|c| c.fold(leaf, and, or)).collect()),
            Tree::Or(v) => or(v.iter().map(|c| c.fold(leaf, and, or)).collect()),
        }
    }
    /// Key level: sound across columns (each unit's set covers every key whose MERGED row satisfies it).
    pub fn keys(&self, per_unit: &[BTreeSet<Key>]) -> BTreeSet<Key> {
        self.fold(
            &|u| per_unit[u].clone(),
            &|v| {
                v.into_iter()
                    .reduce(|a, b| a.intersection(&b).copied().collect())
                    .unwrap_or_default()
            },
            &|v| v.into_iter().flatten().collect(),
        )
    }
    /// Row level, within one segment: exact only for keys with a single version row.
    pub fn rows(&self, per_unit: &[RoaringTreemap]) -> RoaringTreemap {
        self.fold(
            &|u| per_unit[u].clone(),
            &|v| v.into_iter().reduce(|a, b| a & b).unwrap_or_default(),
            &|v| v.union(),
        )
    }
    /// A lower bound on `keys` from a lower bound on each unit's key count (AND: none).
    pub fn min_keys(&self, per_unit: &[usize]) -> usize {
        self.fold(&|u| per_unit[u], &|_| 0, &|v| v.into_iter().max().unwrap_or(0))
    }
    /// On per-unit booleans of one merged row (exact: one value per column).
    pub fn eval(&self, b: &[bool]) -> bool {
        self.fold(&|u| b[u], &|v| v.into_iter().all(|x| x), &|v| v.into_iter().any(|x| x))
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

/// Rows of `part` (file row ids) where each unit of `which` holds, by scanning their columns in one pass (no
/// sidecar, or `NoIndex`). Streams: one batch at a time, charged to `pool` (D5), matched on the blocking pool, with
/// one prefix count per unit (D13: per source).
pub(crate) async fn brute(
    view: &ReadView,
    pool: &Arc<dyn MemoryPool>,
    part: &SegmentPart,
    units: &Arc<Vec<Unit>>,
    which: &[usize],
) -> Result<Vec<RoaringTreemap>, QueryError> {
    debug_assert!(
        !matches!(part.rows, RowSelection::Rows(_)),
        "candidate parts are All or Range"
    );
    let cols: BTreeSet<&'static str> = which.iter().map(|&u| units[u].col.name()).collect();
    let req = scan_request(part, &cols.into_iter().collect::<Vec<_>>(), None);
    let mut stream = view.store().scan(&part.seg, req).await?;
    let res = MemoryConsumer::new("fts.brute").register(pool);
    let mut state: Vec<(Vec<PrefixSeen>, RoaringTreemap)> = which.iter().map(|_| Default::default()).collect();
    let mut row = part.rows.first_row();
    while let Some(b) = stream.try_next().await? {
        let (size, n) = (b.get_array_memory_size(), b.num_rows() as u64);
        res.try_grow(size)?;
        let (units, which) = (units.clone(), which.to_vec());
        state = tokio::task::spawn_blocking(move || -> Result<_, QueryError> {
            let mut an = Analyzer::default();
            for (&u, (seen, out)) in which.iter().zip(&mut state) {
                let unit = &units[u];
                let c = b.column(b.schema_ref().index_of(unit.col.name())?).as_string::<i32>();
                for i in 0..c.len() {
                    if c.is_valid(i) && unit.matches_counting(&mut an, c.value(i), seen)? {
                        out.insert(row + i as u64);
                    }
                }
            }
            Ok(state)
        })
        .await??;
        row += n;
        res.shrink(size);
    }
    Ok(state.into_iter().map(|(_, out)| out).collect())
}

/// Each unit's index answer for `seg`, from one batched lookup (None: no usable sidecar; D4: every index error but
/// a query error falls back to brute force), and whether some hit row has a null start (D8).
pub(crate) async fn lookup(
    index: &dyn IndexReader,
    seg: &SegmentMeta,
    units: &[Unit],
) -> IndexResult<(Vec<Option<RoaringTreemap>>, bool)> {
    let exprs: Vec<FtsExpr> = units.iter().map(|u| u.expr.clone()).collect();
    match index.search(seg, &exprs).await {
        Ok(FtsHits::Rows { rows, start_less }) => Ok((rows.into_iter().map(Some).collect(), start_less)),
        Ok(FtsHits::NoIndex) => Ok((vec![None; units.len()], false)),
        Err(e @ IndexError::Query(_)) => Err(e),
        Err(e) => {
            tracing::warn!(segment = %seg.id, error = %e, ".fts unusable; brute force");
            Ok((vec![None; units.len()], false))
        }
    }
}

/// One part's hits: per-unit rows (the index's, completed by brute force) and the hit rows.
pub(crate) struct PartHits {
    pub seg: SegmentMeta,
    pub per_unit: Vec<RoaringTreemap>,
    /// Some unit had no usable index (D4).
    pub bruted: bool,
    /// `cols` of the hit rows (OR of the units: a key-level superset, FQ Lemma 2), in row order.
    pub batches: Vec<RecordBatch>,
}

impl PartHits {
    /// The hit rows, in `batches` order.
    pub fn rows(&self) -> RoaringTreemap {
        self.per_unit.iter().union()
    }
}

/// The per-segment pipeline of the SQL and `/v1/search` paths, first half: `looked` (each unit's index answer for
/// the part's segment, from `lookup`) clipped to the part, the NoIndex units brute-forced, the part counters.
/// `scan_hits` reads the hit rows.
pub(crate) async fn part_hits(
    view: &ReadView,
    pool: &Arc<dyn MemoryPool>,
    counters: &FtsCounters,
    part: &SegmentPart,
    units: &Arc<Vec<Unit>>,
    looked: Vec<Option<RoaringTreemap>>,
) -> Result<PartHits, QueryError> {
    let missing: Vec<usize> = (0..looked.len()).filter(|&u| looked[u].is_none()).collect();
    let bruted = !missing.is_empty();
    counters.add(1, |c| if bruted { &mut c.brute_parts } else { &mut c.index_parts });
    let mut forced = if bruted {
        brute(view, pool, part, units, &missing).await?
    } else {
        Vec::new()
    }
    .into_iter();
    let per_unit = looked
        .into_iter()
        .map(|h| match h {
            Some(rows) => part.rows.intersect(rows),
            None => forced.next().expect("one per missing unit"),
        })
        .collect();
    let h = PartHits {
        seg: part.seg.clone(),
        per_unit,
        bruted,
        batches: Vec::new(),
    };
    if bruted || !h.rows().is_empty() {
        counters.add(1, |c| &mut c.segments_read);
    }
    Ok(h)
}

/// Second half of the per-segment pipeline: `cols` of every hit row.
pub(crate) async fn scan_hits(view: &ReadView, h: &mut PartHits, cols: &[&'static str]) -> Result<(), QueryError> {
    let all = h.rows();
    if !all.is_empty() {
        let hit = SegmentPart {
            seg: h.seg.clone(),
            rows: RowSelection::Rows(Arc::new(all)),
        };
        h.batches = view.scan(&[hit], cols, None).await?;
    }
    Ok(())
}

/// D11's split of `locs` (every location of the candidate traces). A trace is single-source when every unit is
/// exact, it has no memtable rows, and all of its versions are in one segment whose rows this call computed
/// (`computed`): one row per key per segment (invariant 3), so the index answer is the answer.
pub(crate) struct SingleSource {
    pub traces: BTreeSet<TraceId>,
    /// Per segment: the rows of its single-source traces (AND them with the segment's tree rows).
    pub rows: HashMap<SegmentId, (SegmentMeta, RoaringTreemap)>,
    /// The other locations: merged with the memtable and re-checked.
    pub merge: Vec<SegmentPart>,
}

pub(crate) fn single_source(
    locs: Vec<(TraceId, SegmentPart)>,
    all_exact: bool,
    mem_traces: &BTreeSet<TraceId>,
    computed: &dyn Fn(SegmentId) -> bool,
) -> SingleSource {
    let mut segs_of: HashMap<TraceId, BTreeSet<SegmentId>> = HashMap::new();
    for (t, l) in &locs {
        segs_of.entry(*t).or_default().insert(l.seg.id);
    }
    let traces = segs_of
        .into_iter()
        .filter(|(t, s)| all_exact && !mem_traces.contains(t) && s.len() == 1 && s.iter().all(|id| computed(*id)))
        .map(|(t, _)| t)
        .collect::<BTreeSet<_>>();
    let mut out = SingleSource {
        traces,
        rows: HashMap::new(),
        merge: Vec::new(),
    };
    for (t, l) in locs {
        match &l.rows {
            RowSelection::Range(r) if out.traces.contains(&t) => {
                let e = out
                    .rows
                    .entry(l.seg.id)
                    .or_insert_with(|| (l.seg.clone(), RoaringTreemap::new()));
                e.1.insert_range(r.clone());
            }
            _ => out.merge.push(l),
        }
    }
    out
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
    let mut units = Vec::new();
    let tree = split(&mut Analyzer::default(), expr, &mut units)?;
    let units = Arc::new(units);

    // Memtable rows of the project (key filters only), brute force per unit, on the blocking pool.
    let mk = KeyFilters {
        projects: Some([p.clone()].into()),
        traces: k.traces.clone(),
        ..KeyFilters::default()
    };
    let mut mem_cols: BTreeSet<&'static str> = [col::PROJECT_ID, col::TRACE_ID, col::SPAN_ID].into();
    mem_cols.extend(units.iter().map(|u| u.col.name()));
    let mem_cols: Vec<&'static str> = mem_cols.into_iter().collect();
    let (snap, mem_units) = (view.mem().clone(), units.clone());
    let mem = async {
        // Dropping the query (timeout, error) stops the pass.
        let cancel = Cancel::new();
        let _guard = CancelOnDrop(cancel.clone());
        tokio::task::spawn_blocking(move || -> Result<_, QueryError> {
            let mut an = Analyzer::default();
            let mut per_unit: Vec<BTreeSet<Key>> = vec![BTreeSet::new(); mem_units.len()];
            let mut mem_traces: BTreeSet<TraceId> = BTreeSet::new();
            // One prefix count per unit and predicate: the memtable is one source (D13).
            let mut seen: Vec<Vec<PrefixSeen>> = vec![Vec::new(); mem_units.len()];
            for b in mem_versions(&snap, &mk, &projected_schema(&mem_cols), &cancel)? {
                cancel.check()?;
                let keys = span_keys(&b)?;
                mem_traces.extend(keys.iter().map(|k| k.0));
                for (ui, u) in mem_units.iter().enumerate() {
                    let c = b.column(b.schema().index_of(u.col.name())?).as_string::<i32>();
                    for (i, key) in keys.iter().enumerate() {
                        if c.is_valid(i) && u.matches_counting(&mut an, c.value(i), &mut seen[ui])? {
                            per_unit[ui].insert(*key);
                        }
                    }
                }
            }
            Ok((per_unit, mem_traces))
        })
        .await?
    };

    // Segments: index (or brute force) per unit; then, unless that already rules the index out, the keys of the
    // hit rows. A loop, not a closure: a closure over `&SegmentPart` trips the async_trait Send check (HRTB).
    // One lookup per segment, however many parts of it there are.
    let segs = async {
        let mut seen = BTreeSet::new();
        let mut futs = Vec::new();
        for seg in parts.iter().map(|p| &p.seg).filter(|s| seen.insert(s.id)) {
            let units = &units;
            futs.push(async move { Ok::<_, QueryError>((seg.id, lookup(index, seg, units).await?.0)) });
        }
        let looked: HashMap<SegmentId, Vec<Option<RoaringTreemap>>> = futures::stream::iter(futs)
            .buffer_unordered(INDEX_CONCURRENCY)
            .try_collect()
            .await?;
        let mut futs = Vec::with_capacity(parts.len());
        for part in parts {
            futs.push(part_hits(
                view,
                pool,
                counters,
                part,
                &units,
                looked[&part.seg.id].clone(),
            ));
        }
        futures::stream::iter(futs)
            .buffer_unordered(INDEX_CONCURRENCY)
            .try_collect::<Vec<PartHits>>()
            .await
    };
    let ((mut per_unit, mem_traces), mut hits) = futures::try_join!(mem, segs)?;
    let n = hits.iter().filter(|h| h.bruted).count();
    if n > 0 {
        tracing::warn!(project = %p, segments = n, "search: segments without a usable .fts were scanned by brute force");
    }
    // A segment holds one row per key: a unit's rows in one part are that many distinct candidate keys.
    let at_least: Vec<usize> = (0..units.len())
        .map(|u| {
            hits.iter()
                .map(|h| h.per_unit[u].len() as usize)
                .chain([per_unit[u].len()])
                .max()
                .unwrap_or(0)
        })
        .collect();
    if tree.min_keys(&at_least) > max_keys {
        return Ok(None);
    }
    let mut futs = Vec::with_capacity(hits.len());
    for h in &mut hits {
        futs.push(scan_hits(view, h, &[col::TRACE_ID, col::SPAN_ID]));
    }
    futures::stream::iter(futs)
        .buffer_unordered(INDEX_CONCURRENCY)
        .try_collect::<Vec<()>>()
        .await?;
    for h in &hits {
        let mut keys = BTreeMap::new();
        let mut ids = h.rows().into_iter();
        for b in &h.batches {
            for key in span_keys(b)? {
                keys.insert(ids.next().expect("one row per hit id"), key);
            }
        }
        for (ui, rows) in h.per_unit.iter().enumerate() {
            per_unit[ui].extend(rows.iter().map(|r| keys[&r]));
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
    let scanned: BTreeSet<SegmentId> = hits.iter().map(|h| h.seg.id).collect();
    let all_exact = units.iter().all(|u| u.exact);
    let single = single_source(locs, all_exact, &mem_traces, &|id| scanned.contains(&id));

    let mut sel = Selected::default();
    // Exact rows: the unit tree on the rows of each part, restricted to its single-source traces' ranges.
    for h in &hits {
        if let Some((_, keep)) = single.rows.get(&h.seg.id) {
            let rows = tree.rows(&h.per_unit) & keep;
            if !rows.is_empty() {
                counters.add(rows.len(), |c| &mut c.exact_rows);
                sel.exact.push(SegmentPart {
                    seg: h.seg.clone(),
                    rows: RowSelection::Rows(Arc::new(rows)),
                });
            }
        }
    }
    // Everything else: all versions, merged and re-checked (memtable-only traces too).
    sel.merge_traces = traces.into_iter().filter(|t| !single.traces.contains(t)).collect();
    sel.merge = coalesce(single.merge, 0);
    counters.add(sel.merge.iter().map(|p| p.rows.len(p.seg.row_count)).sum(), |c| {
        &mut c.merge_rows
    });
    Ok(Some(sel))
}

/// `IndexReader` over `.fts` sidecars in the segment store (`paths::fts_path`); open files (footer + fetched FST
/// blocks) are cached per segment. A segment without `fts_byte_size` is `NoIndex`.
#[derive(Debug)]
pub struct FtsIndexReader {
    store: Arc<dyn object_store::ObjectStore>,
    files: moka::future::Cache<SegmentId, Arc<FtsFile>>,
}

impl FtsIndexReader {
    pub fn new(store: Arc<dyn object_store::ObjectStore>) -> Self {
        Self {
            store,
            files: moka::future::Cache::builder()
                .max_capacity(256 << 20)
                .weigher(|_, f: &Arc<FtsFile>| {
                    let d = &f.footer().dict;
                    u32::try_from(d.end - d.start + 4096).unwrap_or(u32::MAX)
                })
                .build(),
        }
    }
}

type Fetch = futures::future::BoxFuture<'static, IndexResult<Vec<bytes::Bytes>>>;

#[async_trait::async_trait]
impl IndexReader for FtsIndexReader {
    async fn search(&self, seg: &SegmentMeta, exprs: &[FtsExpr]) -> IndexResult<FtsHits> {
        let Some(len) = seg.fts_byte_size else {
            return Ok(FtsHits::NoIndex);
        };
        let path = forge_model::catalog::paths::fts_path(&seg.path);
        let (store, loc) = (self.store.clone(), object_store::path::Path::from(path.as_str()));
        let fetch = move |rs: Vec<std::ops::Range<u64>>| -> Fetch {
            let (store, loc, p) = (store.clone(), loc.clone(), loc.to_string());
            Box::pin(async move {
                // A missing file is a broken sidecar; anything else may be transient (both fall back, D4).
                store.get_ranges(&loc, &rs).await.map_err(|e| match e {
                    object_store::Error::NotFound { .. } => IndexError::Corrupt {
                        path: p,
                        msg: e.to_string(),
                    },
                    e => IndexError::Io {
                        path: p,
                        msg: e.to_string(),
                    },
                })
            })
        };
        // One open per segment, shared by concurrent lookups; a file for another row count is not cached.
        let file = self
            .files
            .try_get_with(seg.id, async {
                let f = FtsFile::open(&path, len, &fetch).await?.0;
                if u64::from(f.footer().num_rows) != seg.row_count {
                    return Err(IndexError::Corrupt {
                        path: path.clone(),
                        msg: format!("{} rows, the segment has {}", f.footer().num_rows, seg.row_count),
                    });
                }
                Ok(Arc::new(f))
            })
            .await
            .map_err(|e| (*e).clone())?;
        // Every predicate of every expression at once: one wave of FST blocks, one of postings (D1).
        let mut an = Analyzer::default();
        let ms: Vec<Vec<Matcher>> = exprs
            .iter()
            .map(|e| Matcher::for_expr(&mut an, e))
            .collect::<IndexResult<_>>()?;
        let qs: Vec<&forge_index::query::Query> = ms.iter().flatten().map(Matcher::query).collect();
        let mut bms = file.execute_many(&qs, &fetch).await?.0.into_iter();
        let rows: Vec<RoaringBitmap> = exprs
            .iter()
            .map(|e| {
                e.fold(
                    &mut |_| bms.next().expect("one bitmap per predicate"),
                    &|v| v.into_iter().reduce(|a, b| a & b).unwrap_or_default(),
                    &|v| v.union(),
                )
            })
            .collect();
        let start_less = rows.iter().any(|r| !r.is_disjoint(file.null_starts()));
        Ok(FtsHits::Rows {
            rows: rows
                .into_iter()
                .map(|r| RoaringTreemap::from_bitmaps([(0, r)]))
                .collect(),
            start_less,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Tree;

    /// The >100k fallback bound: AND has no lower bound, OR the largest child's.
    #[test]
    fn min_keys_is_a_lower_bound() {
        let or = || Tree::Or(vec![Tree::Unit(0), Tree::Unit(1)]);
        assert_eq!(Tree::And(vec![Tree::Unit(0), Tree::Unit(1)]).min_keys(&[200_000, 3]), 0);
        assert_eq!(or().min_keys(&[200_000, 3]), 200_000);
        assert_eq!(Tree::And(vec![or(), Tree::Unit(2)]).min_keys(&[200_000, 3, 1]), 0);
    }
}
