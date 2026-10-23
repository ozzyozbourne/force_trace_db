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
use forge_index::{
    Analyzer, FtsColumn, FtsExpr, FtsFile, FtsHits, IndexError, IndexReader, IndexResult, Matcher, PrefixSeen,
};
use forge_model::catalog::{SegmentId, SegmentMeta};
use forge_model::ids::{ProjectId, SpanId, TraceId};
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

    /// `matches` on a brute-forced source, counting prefix expansions (D13).
    pub fn matches_counting(&self, an: &mut Analyzer, cell: &str, seen: &mut PrefixSeen) -> Result<bool, QueryError> {
        self.eval(an, cell, Some(seen))
    }

    /// Every predicate (no short-circuit: each one counts its prefix expansions), then the expression on them.
    fn eval(&self, an: &mut Analyzer, cell: &str, mut seen: Option<&mut PrefixSeen>) -> Result<bool, QueryError> {
        let bs = self
            .matchers
            .iter()
            .map(|m| match seen.as_deref_mut() {
                Some(s) => m.matches_counting(an, cell, s),
                None => Ok(m.matches(an, cell)),
            })
            .collect::<Result<Vec<bool>, _>>()?;
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

/// Rows of `part` (file row ids) where `unit` holds, by scanning its column (no sidecar, or `NoIndex`). Streams:
/// one batch at a time, charged to `pool` (D5).
pub(crate) async fn brute(
    view: &ReadView,
    pool: &Arc<dyn MemoryPool>,
    part: &SegmentPart,
    unit: &Unit,
) -> Result<RoaringTreemap, QueryError> {
    debug_assert!(
        !matches!(part.rows, RowSelection::Rows(_)),
        "candidate parts are All or Range"
    );
    let start = part.rows.first_row();
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

/// Each unit's index answer for `seg` (None: no usable sidecar; D4: every index error but a query error falls back
/// to brute force), and whether some hit row has a null start (D8).
pub(crate) async fn lookup(
    index: &dyn IndexReader,
    seg: &SegmentMeta,
    units: &[Unit],
) -> IndexResult<(Vec<Option<RoaringTreemap>>, bool)> {
    let looked = futures::future::try_join_all(units.iter().map(|u| async move {
        match index.search(seg, &u.expr).await {
            Ok(FtsHits::Rows { rows, start_less }) => Ok(Some((rows, start_less))),
            Ok(FtsHits::NoIndex) => Ok(None),
            Err(e @ IndexError::Query(_)) => Err(e),
            Err(e) => {
                tracing::debug!(segment = %seg.id, error = %e, ".fts unusable; brute force");
                Ok(None)
            }
        }
    }))
    .await?;
    let start_less = looked.iter().flatten().any(|(_, sl)| *sl);
    Ok((
        looked.into_iter().map(|x| x.map(|(rows, _)| rows)).collect(),
        start_less,
    ))
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

/// The per-segment pipeline of the SQL and `/v1/search` paths: `looked` (each unit's index answer for the part's
/// segment, from `lookup`) clipped to the part, the NoIndex units brute-forced, the part counters, then `cols` of
/// every hit row.
pub(crate) async fn part_hits(
    view: &ReadView,
    pool: &Arc<dyn MemoryPool>,
    counters: &FtsCounters,
    part: &SegmentPart,
    units: &[Unit],
    looked: Vec<Option<RoaringTreemap>>,
    cols: &[&'static str],
) -> Result<PartHits, QueryError> {
    let bruted = looked.iter().any(Option::is_none);
    counters.add(1, |c| if bruted { &mut c.brute_parts } else { &mut c.index_parts });
    let mut per_unit = Vec::with_capacity(units.len());
    for (u, h) in units.iter().zip(looked) {
        per_unit.push(match h {
            Some(rows) => part.rows.intersect(rows),
            None => brute(view, pool, part, u).await?,
        });
    }
    let mut h = PartHits {
        seg: part.seg.clone(),
        per_unit,
        bruted,
        batches: Vec::new(),
    };
    let all = h.rows();
    if bruted || !all.is_empty() {
        counters.add(1, |c| &mut c.segments_read);
    }
    if !all.is_empty() {
        let hit = SegmentPart {
            seg: part.seg.clone(),
            rows: RowSelection::Rows(Arc::new(all)),
        };
        h.batches = view.scan(&[hit], cols, None).await?;
    }
    Ok(h)
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
        let keys = span_keys(&b)?;
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
    let bruted0 = counters.get().brute_parts;
    let mut futs = Vec::with_capacity(parts.len());
    for part in parts {
        let units = &units;
        futs.push(async move {
            let (looked, _) = lookup(index, &part.seg, units).await?;
            part_hits(
                view,
                pool,
                counters,
                part,
                units,
                looked,
                &[col::TRACE_ID, col::SPAN_ID],
            )
            .await
        });
    }
    let hits: Vec<PartHits> = futures::stream::iter(futs)
        .buffer_unordered(INDEX_CONCURRENCY)
        .try_collect()
        .await?;
    // Other queries add to the counter too: the count is approximate when they overlap.
    let n = counters.get().brute_parts.saturating_sub(bruted0);
    if n > 0 {
        tracing::warn!(project = %p, segments = n, "search: segments without a usable .fts were scanned by brute force");
    }
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
    async fn search(&self, seg: &SegmentMeta, expr: &FtsExpr) -> IndexResult<FtsHits> {
        let Some(len) = seg.fts_byte_size else {
            return Ok(FtsHits::NoIndex);
        };
        let path = forge_model::catalog::paths::fts_path(&seg.path);
        let (store, loc) = (self.store.clone(), object_store::path::Path::from(path.as_str()));
        let fetch = move |rs: Vec<std::ops::Range<u64>>| -> Fetch {
            let (store, loc, p) = (store.clone(), loc.clone(), loc.to_string());
            Box::pin(async move {
                store.get_ranges(&loc, &rs).await.map_err(|e| IndexError::Corrupt {
                    path: p,
                    msg: e.to_string(),
                })
            })
        };
        // One open per segment, shared by concurrent lookups.
        let file = self
            .files
            .try_get_with(seg.id, async {
                Ok::<_, IndexError>(Arc::new(FtsFile::open(&path, len, &fetch).await?.0))
            })
            .await
            .map_err(|e| (*e).clone())?;
        if u64::from(file.footer().num_rows) != seg.row_count {
            return Err(IndexError::Corrupt {
                path,
                msg: format!("{} rows, the segment has {}", file.footer().num_rows, seg.row_count),
            });
        }
        // Every predicate's lookups at once: one wave of FST blocks, one of postings (D1).
        let ms = Matcher::for_expr(&mut Analyzer::default(), expr)?;
        let bms = futures::future::try_join_all(ms.iter().map(|m| file.execute(m.query(), &fetch))).await?;
        let mut bms = bms.into_iter().map(|(b, _)| b);
        let rows: RoaringBitmap = expr.fold(
            &mut |_| bms.next().expect("one bitmap per predicate"),
            &|v| v.into_iter().reduce(|a, b| a & b).unwrap_or_default(),
            &|v| v.union(),
        );
        let start_less = !rows.is_disjoint(file.null_starts());
        Ok(FtsHits::Rows {
            rows: RoaringTreemap::from_bitmaps([(0, rows)]),
            start_less,
        })
    }
}
