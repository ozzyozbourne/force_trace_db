//! Runs list fast path (D6): root spans of a project, newest first, keyset-paged.
//!
//! Candidates come from rows that LOOK like roots in the window (`parent_span_id IS NULL`, start in range): these
//! pre-merge predicates are safe for candidate generation only, because a merged root has a null parent in every
//! version and decision 5 makes start_time immutable (FP §2.2; invariant 4). Every candidate is then verified
//! against ALL of its versions (every `locate_traces` location + the memtable), merged, and only the merged rows
//! are filtered. Segments are read newest `max_start` first; the page is final once K verified rows all start
//! after the next unread segment's `max_start`. No budget: a deep page costs O(segments overlapping the window)
//! (M3's per-hour L1 bounds that).
use crate::filters::KeyFilters;
use crate::read::{ReadView, SegmentPart, coalesce, mem_versions, merge_or_filter, needs_merge, read_columns};
use crate::{QueryCtx, QueryEngine, QueryError, QueryKind};
use arrow::array::{Array, AsArray, BooleanArray, RecordBatch, StringArray};
use arrow::compute::{filter_record_batch, interleave_record_batch};
use arrow::datatypes::{SchemaRef, TimestampNanosecondType};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use forge_index::{Analyzer, FtsColumn, FtsPredicate, Matcher};
use forge_meta::SegmentFilter;
use forge_model::catalog::SegmentMeta;
use forge_model::enums::{RunType, SpanStatus};
use forge_model::ids::{ProjectId, SpanId, TraceId};
use forge_model::schema::{col, projected_schema};
use forge_storage::RowSelection;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

/// What a list returns: never payload, never dotted_order (== `trace_columns(false)`).
pub const RUN_COLUMNS: [&str; 21] = [
    col::PROJECT_ID,
    col::TRACE_ID,
    col::SPAN_ID,
    col::PARENT_SPAN_ID,
    col::NAME,
    col::RUN_TYPE,
    col::START_TIME,
    col::END_TIME,
    col::STATUS,
    col::ERROR,
    col::MODEL,
    col::PROVIDER,
    col::PROMPT_TOKENS,
    col::COMPLETION_TOKENS,
    col::TOTAL_TOKENS,
    col::COST_USD,
    col::SESSION_ID,
    col::TAGS,
    col::METADATA,
    col::VERSION,
    col::DELETED,
];

pub const MAX_LIMIT: usize = 1000;
const MAX_Q: usize = 256;

/// A run's place in the listing; pages run in DESCENDING order.
pub type Pos = (i64, TraceId, SpanId);

/// Exclusive: the next page starts strictly after this position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cursor(pub Pos);

#[derive(Clone, Debug)]
pub struct RunsRequest {
    pub project: ProjectId,
    /// `[start_ns, end_ns)` on start_time.
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    /// Empty = any.
    pub run_types: BTreeSet<RunType>,
    pub statuses: BTreeSet<SpanStatus>,
    /// `search(name, q)` (M4: the FTS query syntax and analyzer, on the merged name).
    pub q: Option<String>,
    /// 1..=1000.
    pub limit: usize,
    pub cursor: Option<Cursor>,
}

/// `GET /v1/runs` query string. `run_type` / `status` are comma-separated; `start` / `end` RFC3339.
#[derive(Debug, Default, serde::Deserialize)]
pub struct RunsParams {
    pub project: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub run_type: Option<String>,
    pub status: Option<String>,
    pub q: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

pub(crate) fn bad(msg: impl Into<String>) -> QueryError {
    QueryError::InvalidInput(msg.into())
}

pub(crate) fn parse_list<T: Ord + std::str::FromStr<Err = forge_model::error::ModelError>>(
    s: Option<&str>,
) -> Result<BTreeSet<T>, QueryError> {
    s.into_iter()
        .flat_map(|s| s.split(','))
        .filter(|x| !x.is_empty())
        .map(|x| x.trim().parse::<T>().map_err(|e| bad(e.to_string())))
        .collect()
}

pub(crate) fn parse_time(what: &str, s: Option<&str>) -> Result<Option<i64>, QueryError> {
    s.map(|s| {
        chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .and_then(|t| t.timestamp_nanos_opt())
            .ok_or_else(|| bad(format!("{what}: not an RFC3339 time in range: {s:?}")))
    })
    .transpose()
}

impl RunsRequest {
    /// Strict: anything malformed is InvalidInput. `project` was resolved by the caller (param, header, default).
    pub fn parse(p: RunsParams, project: ProjectId) -> Result<Self, QueryError> {
        let limit = p.limit.unwrap_or(50);
        if !(1..=MAX_LIMIT).contains(&limit) {
            return Err(bad(format!("limit must be 1..={MAX_LIMIT}, got {limit}")));
        }
        // Not lowercased: the analyzer folds case, and `OR` is an operator.
        let q = p.q.filter(|q| !q.trim().is_empty());
        if q.as_ref().is_some_and(|q| q.len() > MAX_Q) {
            return Err(bad(format!("q is longer than {MAX_Q} bytes")));
        }
        let mut r = Self {
            project,
            start_ns: parse_time("start", p.start.as_deref())?,
            end_ns: parse_time("end", p.end.as_deref())?,
            run_types: parse_list(p.run_type.as_deref())?,
            statuses: parse_list(p.status.as_deref())?,
            q,
            limit,
            cursor: None,
        };
        r.cursor = p.cursor.map(|c| r.decode_cursor(&c)).transpose()?;
        Ok(r)
    }

    /// crc32 of the normalized filters: a cursor only continues the listing it came from.
    fn filter_hash(&self) -> u32 {
        let rt: Vec<&str> = self.run_types.iter().map(|r| r.as_str()).collect();
        let st: Vec<&str> = self.statuses.iter().map(|r| r.as_str()).collect();
        let norm = format!(
            "{}|{:?}|{:?}|{rt:?}|{st:?}|{:?}",
            self.project, self.start_ns, self.end_ns, self.q
        );
        crc32fast::hash(norm.as_bytes())
    }

    fn encode_cursor(&self, c: &Cursor) -> String {
        encode_cursor(1, c, self.filter_hash())
    }

    fn decode_cursor(&self, s: &str) -> Result<Cursor, QueryError> {
        decode_cursor(1, s, self.filter_hash(), "cursor does not match the query's filters")
    }
}

/// hex(`[tag][start_ns BE][trace 16][span 8][hash BE]`): 74 hex digits. `hash` binds the cursor to its request.
pub(crate) fn encode_cursor(tag: u8, c: &Cursor, hash: u32) -> String {
    let (start, trace, span) = c.0;
    let mut b = vec![tag];
    b.extend_from_slice(&start.to_be_bytes());
    b.extend_from_slice(trace.as_bytes());
    b.extend_from_slice(span.as_bytes());
    b.extend_from_slice(&hash.to_be_bytes());
    hex::encode(b)
}

pub(crate) fn decode_cursor(tag: u8, s: &str, hash: u32, mismatch: &str) -> Result<Cursor, QueryError> {
    let b = hex::decode(s).map_err(|_| bad("bad cursor"))?;
    if b.len() != 37 || b[0] != tag {
        return Err(bad("bad cursor"));
    }
    if b[33..37] != hash.to_be_bytes() {
        return Err(bad(mismatch));
    }
    let start = i64::from_be_bytes(b[1..9].try_into().expect("8 bytes"));
    Ok(Cursor((
        start,
        TraceId::from_slice(&b[9..25])?,
        SpanId::from_slice(&b[25..33])?,
    )))
}

pub struct RunsPage {
    /// RUN_COLUMNS, merged (effective total_tokens), in page order.
    pub rows: RecordBatch,
    pub next_cursor: Option<Cursor>,
}

/// What candidate generation reads (`candidates`).
const CANDIDATE_COLUMNS: [&str; 5] = [
    col::PROJECT_ID,
    col::TRACE_ID,
    col::SPAN_ID,
    col::PARENT_SPAN_ID,
    col::START_TIME,
];
/// Verify reads a segment's located ranges as one scan when they are at most this many rows apart (the rows in
/// between are dropped by the key mask).
pub(crate) const VERIFY_GAP: u64 = 4096;
/// Up to this many distinct wave span ids are pushed into the verify scans as a key filter.
const MAX_FILTER_SPANS: usize = 256;

/// The id and start columns of a batch, by name.
pub(crate) struct Ids<'a> {
    pub start: &'a arrow::array::TimestampNanosecondArray,
    pub trace: &'a arrow::array::BinaryArray,
    pub span: &'a arrow::array::BinaryArray,
}

impl<'a> Ids<'a> {
    pub fn of(b: &'a RecordBatch) -> Result<Self, QueryError> {
        let s = b.schema_ref();
        Ok(Self {
            start: b
                .column(s.index_of(col::START_TIME)?)
                .as_primitive::<TimestampNanosecondType>(),
            trace: b.column(s.index_of(col::TRACE_ID)?).as_binary::<i32>(),
            span: b.column(s.index_of(col::SPAN_ID)?).as_binary::<i32>(),
        })
    }
    /// (trace, span) of row `i`.
    pub fn key(&self, i: usize) -> Result<(TraceId, SpanId), QueryError> {
        Ok((
            TraceId::from_slice(self.trace.value(i))?,
            SpanId::from_slice(self.span.value(i))?,
        ))
    }
    /// (start, trace, span) of row `i`; None where start is null.
    fn pos(&self, i: usize) -> Option<Pos> {
        let ids = (
            TraceId::from_slice(self.trace.value(i)).ok()?,
            SpanId::from_slice(self.span.value(i)).ok()?,
        );
        self.start.is_valid(i).then(|| (self.start.value(i), ids.0, ids.1))
    }
}

/// `parent_span_id IS NULL`: a first cut for candidate scans (candidates() re-checks every row).
fn roots_filter() -> vortex::expr::Expression {
    vortex::expr::is_null(vortex::expr::col(col::PARENT_SPAN_ID))
}

/// `span_id IN spans` (a key column: allowed below the merge, invariant 4), for at most MAX_FILTER_SPANS ids.
pub(crate) fn span_filter(spans: &BTreeSet<SpanId>) -> Option<vortex::expr::Expression> {
    use vortex::buffer::ByteBuffer;
    use vortex::dtype::Nullability;
    use vortex::expr::{eq, lit, or};
    use vortex::scalar::Scalar;
    if spans.len() > MAX_FILTER_SPANS {
        return None;
    }
    spans
        .iter()
        .map(|s| {
            let v = Scalar::binary(ByteBuffer::copy_from(s.as_bytes()), Nullability::NonNullable);
            eq(vortex::expr::col(col::SPAN_ID), lit(v))
        })
        .reduce(or)
}

impl QueryEngine {
    pub async fn list_runs(&self, r: &RunsRequest) -> Result<RunsPage, QueryError> {
        let q = self.ctx(QueryKind::Fast, self.cfg.timeout);
        q.run(async { Ok(self.list_runs_inner(&q, r).await?.0) }).await
    }

    /// `{"runs":[…],"next_cursor":hex|null}`; spans written like the trace path, plus `trace_id`, no children.
    pub async fn runs_json(&self, r: &RunsRequest) -> Result<Vec<u8>, QueryError> {
        let q = self.ctx(QueryKind::Fast, self.cfg.timeout);
        q.run(async {
            let (page, res) = self.list_runs_inner(&q, r).await?;
            let cursor = page.next_cursor.map(|c| r.encode_cursor(&c));
            page_json(&q, "runs.json", "runs", page, res, cursor).await
        })
        .await
    }

    /// The page, and the call's reservation (D15), which holds the page's rows.
    async fn list_runs_inner(
        &self,
        q: &QueryCtx,
        r: &RunsRequest,
    ) -> Result<(RunsPage, MemoryReservation), QueryError> {
        let view = self.read_view(); // FIRST (invariant 1)
        let hi = upper_bound(r.end_ns, r.cursor);
        let segs = newest_first_segments(&view, &r.project, r.start_ns, hi).await?;
        page(q, &view, &RootScan { r, hi }, segs, r.limit, 0).await
    }
}

/// `{"<key>":[…],"next_cursor":hex|null}` on the blocking pool (`label`); the body is reserved up front at the
/// page's size, then at its real capacity.
pub(crate) async fn page_json(
    q: &QueryCtx,
    label: &'static str,
    key: &'static str,
    page: RunsPage,
    res: MemoryReservation,
    cursor: Option<String>,
) -> Result<Vec<u8>, QueryError> {
    q.blocking(label, move |cancel| {
        let held = res.size();
        res.try_grow(page.rows.get_array_memory_size())?;
        let mut out = Vec::with_capacity(page.rows.get_array_memory_size());
        crate::json::write_runs(&mut out, key, &page.rows, cursor.as_deref(), cancel)?;
        res.try_resize(held + out.capacity())?;
        Ok(out)
    })
    .await
}

/// The project's segments overlapping `[start_ns, hi)`, newest first (the input of `page`).
pub(crate) async fn newest_first_segments(
    view: &ReadView,
    project: &ProjectId,
    start_ns: Option<i64>,
    hi: Option<i64>,
) -> Result<Vec<SegmentMeta>, QueryError> {
    let filter = SegmentFilter {
        project_id: Some(project.clone()),
        start_ns,
        end_ns: hi,
        newest_first: true,
        ..Default::default()
    };
    Ok(view.list_segments(&filter).await?)
}

/// Exclusive upper bound on start: the window end and, via the cursor, start <= cursor.start.
pub(crate) fn upper_bound(end_ns: Option<i64>, cursor: Option<Cursor>) -> Option<i64> {
    match cursor.and_then(|Cursor(c)| c.0.checked_add(1)) {
        Some(c) => Some(end_ns.map_or(c, |e| e.min(c))),
        None => end_ns,
    }
}

/// Where candidate positions come from (runs: root-looking rows; search: index hits).
#[async_trait::async_trait]
pub(crate) trait CandidateSource: Send + Sync {
    /// Upper bound on the merged start_time of every row this segment can contribute (None: none).
    fn frontier(&self, s: &SegmentMeta) -> Option<i64>;
    /// Called once before the segments are sorted by frontier (search: the eager lookups of D8).
    async fn start(&self, _q: &QueryCtx, _view: &ReadView, _segs: &[SegmentMeta]) -> Result<(), QueryError> {
        Ok(())
    }
    /// Lookahead over one whole window of the next segments in frontier order (search: index lookups, D10; `page`'s
    /// `prefetch` sets the window).
    async fn prefetch(&self, _q: &QueryCtx, _view: &ReadView, _next: &[SegmentMeta]) -> Result<(), QueryError> {
        Ok(())
    }
    /// The end of the next round's segments, read from `segs[i..]` (runs: every segment sharing `segs[i]`'s
    /// frontier; search: `read_group` segments).
    fn group_end(&self, segs: &[SegmentMeta], i: usize) -> usize;
    /// Positions in the memtable snapshot (called once).
    async fn mem(&self, q: &QueryCtx, view: &ReadView) -> Result<Vec<Candidate>, QueryError>;
    /// Positions in `parts` (one round of segments, read concurrently).
    async fn segments(
        &self,
        q: &QueryCtx,
        view: &ReadView,
        parts: &[SegmentPart],
    ) -> Result<Vec<Candidate>, QueryError>;
    /// The merged rows of `wave` that pass every filter.
    async fn verify(
        &self,
        q: &QueryCtx,
        view: &ReadView,
        wave: &[Candidate],
        res: MemoryReservation,
    ) -> Result<(RecordBatch, MemoryReservation), QueryError>;
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Candidate {
    At(Pos),
    /// A hit row with a null start (a patch): verified with the current wave (search only).
    Unpositioned(TraceId, SpanId),
}

impl Candidate {
    pub(crate) fn key(&self) -> (TraceId, SpanId) {
        match *self {
            Candidate::At((_, t, s)) | Candidate::Unpositioned(t, s) => (t, s),
        }
    }
}

const MIN_CHUNK: usize = 256;

/// The shared early-stop loop (D8): the newest `k` verified rows of `src` over `segs` (RUN_COLUMNS), as a page.
/// `prefetch`: the lookahead window (0 = none).
pub(crate) async fn page(
    q: &QueryCtx,
    view: &ReadView,
    src: &dyn CandidateSource,
    mut segs: Vec<SegmentMeta>,
    k: usize,
    prefetch: usize,
) -> Result<(RunsPage, MemoryReservation), QueryError> {
    let k = k.max(1);
    // The eager lookups (I/O) and the memtable scan (CPU) at once.
    let (_, mem) = futures::try_join!(src.start(q, view, &segs), src.mem(q, view))?;
    segs.retain(|s| src.frontier(s).is_some());
    segs.sort_by_key(|s| Reverse((src.frontier(s), s.id)));
    let mut res = MemoryConsumer::new("page").register(&q.pool);
    let mut cand: BTreeSet<Reverse<Pos>> = BTreeSet::new();
    let mut pending: Vec<Candidate> = Vec::new();
    let push = |cand: &mut BTreeSet<Reverse<Pos>>, pending: &mut Vec<Candidate>, c: Candidate| match c {
        Candidate::At(p) => {
            cand.insert(Reverse(p));
        }
        u @ Candidate::Unpositioned(..) => pending.push(u),
    };
    for c in mem {
        push(&mut cand, &mut pending, c);
    }
    let mut merged: Vec<RecordBatch> = Vec::new();
    let mut verified: BTreeMap<Reverse<Pos>, (usize, usize)> = BTreeMap::new();
    let mut seen: BTreeSet<(TraceId, SpanId)> = BTreeSet::new();
    let chunk = MIN_CHUNK.max(2 * k);
    let (mut i, mut horizon) = (0, 0);
    loop {
        // A segment whose lookup found no hits has nothing to read (search).
        while segs.get(i).is_some_and(|s| src.frontier(s).is_none()) {
            i += 1;
        }
        let frontier = segs.get(i).and_then(|s| src.frontier(s));
        let above = |p: &Pos| frontier.is_none_or(|f| p.0 > f);
        // Done when no Unpositioned candidate is pending (its merged position is unknown until verified), the K-th
        // verified row is above the frontier, and no unverified candidate is newer than it (a row verified from an
        // Unpositioned candidate can land below positioned candidates not yet verified).
        let kth_above =
            |v: &BTreeMap<Reverse<Pos>, (usize, usize)>, cand: &BTreeSet<Reverse<Pos>>, pending: &[Candidate]| {
                pending.is_empty()
                    && v.keys()
                        .nth(k - 1)
                        .is_some_and(|p| above(&p.0) && cand.first().is_none_or(|c| c.0 < p.0))
            };
        // Newest-first chunks above the frontier (+ every unpositioned candidate) until K are verified.
        while !kth_above(&verified, &cand, &pending) {
            let wave: Vec<Candidate> = cand
                .iter()
                .take_while(|p| above(&p.0))
                .take(chunk)
                .map(|p| Candidate::At(p.0))
                .chain(pending.drain(..))
                .collect();
            if wave.is_empty() {
                break;
            }
            for c in &wave {
                if let Candidate::At(p) = c {
                    cand.remove(&Reverse(*p));
                }
                seen.insert(c.key());
            }
            let (b, r2) = src.verify(q, view, &wave, res).await?;
            res = r2;
            let ids = Ids::of(&b)?;
            for row in 0..b.num_rows() {
                let p = ids.pos(row).expect("verified rows have a start");
                verified.insert(Reverse(p), (merged.len(), row));
            }
            merged.push(b);
        }
        if kth_above(&verified, &cand, &pending) || frontier.is_none() {
            break;
        }
        let end = src.group_end(&segs, i).min(segs.len());
        let w = prefetch;
        while w > 0 && horizon < end {
            // Whole windows: one round of lookups per `w` segments, not one per group (RA M3).
            horizon = horizon.max(i);
            let he = (horizon + w).min(segs.len());
            src.prefetch(q, view, &segs[horizon..he]).await?;
            horizon = he;
        }
        let parts: Vec<SegmentPart> = segs[i..end]
            .iter()
            .map(|s| SegmentPart {
                seg: s.clone(),
                rows: RowSelection::All,
            })
            .collect();
        i = end;
        for c in src.segments(q, view, &parts).await? {
            // A key verified once yields the same merged row again.
            if !seen.contains(&c.key()) {
                push(&mut cand, &mut pending, c);
            }
        }
    }
    let more = verified.len() > k || i < segs.len() || !cand.is_empty();
    let next_cursor = (verified.len() >= k && more).then(|| Cursor(verified.keys().nth(k - 1).expect("k rows").0));
    let idx: Vec<(usize, usize)> = verified.values().take(k).copied().collect();
    let rows = if idx.is_empty() {
        RecordBatch::new_empty(projected_schema(&read_columns(&RUN_COLUMNS)))
    } else {
        interleave_record_batch(&merged.iter().collect::<Vec<_>>(), &idx)?
    };
    res.resize(rows.get_array_memory_size());
    Ok((RunsPage { rows, next_cursor }, res))
}

/// Runs: candidates are root-looking rows (`candidates`), verified by `verify`.
struct RootScan<'a> {
    r: &'a RunsRequest,
    hi: Option<i64>,
}

#[async_trait::async_trait]
impl CandidateSource for RootScan<'_> {
    fn frontier(&self, s: &SegmentMeta) -> Option<i64> {
        // All-null-start segments hold no candidates (their rows are still read by verify via locate_traces).
        s.max_start_ns
    }

    /// One frontier value per round (the pre-M4 loop).
    fn group_end(&self, segs: &[SegmentMeta], i: usize) -> usize {
        let f = segs[i].max_start_ns;
        i + segs[i..].iter().take_while(|s| s.max_start_ns == f).count()
    }

    async fn mem(&self, q: &QueryCtx, view: &ReadView) -> Result<Vec<Candidate>, QueryError> {
        let (snap, rr, hi) = (view.mem().clone(), self.r.clone(), self.hi);
        q.blocking("runs.mem", move |cancel| {
            let k = KeyFilters::project(&rr.project, None);
            let mut v = Vec::new();
            for b in mem_versions(&snap, &k, &projected_schema(&CANDIDATE_COLUMNS), cancel)? {
                v.extend(candidates(&rr, hi, &b)?.into_iter().map(Candidate::At));
            }
            Ok(v)
        })
        .await
    }

    async fn segments(
        &self,
        q: &QueryCtx,
        view: &ReadView,
        parts: &[SegmentPart],
    ) -> Result<Vec<Candidate>, QueryError> {
        let batches = view.scan(parts, &CANDIDATE_COLUMNS, Some(roots_filter())).await?;
        let (rr, hi) = (self.r.clone(), self.hi);
        q.blocking("runs.candidates", move |cancel| {
            let mut v = Vec::new();
            for b in &batches {
                cancel.check()?;
                v.extend(candidates(&rr, hi, b)?.into_iter().map(Candidate::At));
            }
            Ok(v)
        })
        .await
    }

    async fn verify(
        &self,
        q: &QueryCtx,
        view: &ReadView,
        wave: &[Candidate],
        res: MemoryReservation,
    ) -> Result<(RecordBatch, MemoryReservation), QueryError> {
        let wave: Vec<Pos> = wave
            .iter()
            .map(|c| match c {
                Candidate::At(p) => *p,
                Candidate::Unpositioned(..) => unreachable!("a root candidate has a start"),
            })
            .collect();
        verify(q, view, self.r, self.hi, &wave, res).await
    }
}

/// Every version of the wave's keys (all `locate_traces` locations, range-scanned, + the memtable), merged;
/// the merged rows that pass EVERY filter. `res` (holding the rows kept so far) grows by the wave's input and
/// merged rows and ends at what was kept plus the returned rows.
async fn verify(
    q: &QueryCtx,
    view: &ReadView,
    r: &RunsRequest,
    hi: Option<i64>,
    wave: &[Pos],
    res: MemoryReservation,
) -> Result<(RecordBatch, MemoryReservation), QueryError> {
    let cols = read_columns(&RUN_COLUMNS);
    let keys: BTreeSet<(TraceId, SpanId)> = wave.iter().map(|p| (p.1, p.2)).collect();
    let filter = span_filter(&keys.iter().map(|k| k.1).collect());
    let k = KeyFilters {
        traces: Some(keys.iter().map(|k| k.0).collect()),
        spans: Some(keys),
        ..KeyFilters::project(&r.project, None)
    };
    let parts = coalesce(view.locate_versions(&k).await?, VERIFY_GAP);
    let scanned = view.scan(&parts, &cols, filter).await?;
    let schema: SchemaRef = projected_schema(&cols);
    let name_q =
        r.q.as_ref()
            .map(|q| {
                let p = FtsPredicate::Search {
                    column: FtsColumn::Name,
                    query: q.clone(),
                };
                Matcher::new(&mut Analyzer::default(), &p)
            })
            .transpose()?;
    let (snap, r) = (view.mem().clone(), r.clone());
    q.blocking("runs.verify", move |cancel| {
        let mut batches: Vec<RecordBatch> = scanned.into_iter().map(|b| k.apply(b)).collect::<Result<_, _>>()?;
        let mem = mem_versions(&snap, &k, &schema, cancel)?;
        let merge = needs_merge(&parts, !mem.is_empty());
        batches.extend(mem);
        let kept = res.size();
        res.try_grow(batches.iter().map(|b| b.get_array_memory_size()).sum())?;
        let m = merge_or_filter(&schema, &batches, merge, cancel)?;
        res.try_grow(m.get_array_memory_size())?;
        drop(batches);
        res.resize(kept + m.get_array_memory_size());
        let s = m.schema_ref();
        let par = m.column(s.index_of(col::PARENT_SPAN_ID)?);
        let project = m.column(s.index_of(col::PROJECT_ID)?).as_string::<i32>();
        let rt = m.column(s.index_of(col::RUN_TYPE)?).as_string::<i32>();
        let st = m.column(s.index_of(col::STATUS)?).as_string::<i32>();
        let name = m.column(s.index_of(col::NAME)?).as_string::<i32>();
        let ids = Ids::of(&m)?;
        let mut an = Analyzer::default();
        let mask: BooleanArray = (0..m.num_rows())
            .map(|i| {
                Some(
                    par.is_null(i)
                        && project.value(i) == r.project.as_str()
                        && ids.pos(i).is_some_and(|p| in_window(r.start_ns, hi, r.cursor, &p))
                        && in_set(&r.run_types, rt, i)
                        && in_set(&r.statuses, st, i)
                        && name_q
                            .as_ref()
                            .is_none_or(|m| name.is_valid(i) && m.matches(&mut an, name.value(i))),
                )
            })
            .collect();
        let out = filter_record_batch(&m, &mask)?;
        res.try_grow(out.get_array_memory_size())?;
        drop(m);
        res.resize(kept + out.get_array_memory_size());
        Ok((out, res))
    })
    .await
}

/// Row `i` of `a` parses to a member of `set` (an empty set matches anything).
pub(crate) fn in_set<T: Ord + std::str::FromStr>(set: &BTreeSet<T>, a: &StringArray, i: usize) -> bool {
    set.is_empty() || a.is_valid(i) && a.value(i).parse().is_ok_and(|x| set.contains(&x))
}

/// start in `[start_ns, hi)` and strictly after the cursor.
pub(crate) fn in_window(start_ns: Option<i64>, hi: Option<i64>, cursor: Option<Cursor>, p: &Pos) -> bool {
    start_ns.is_none_or(|x| p.0 >= x) && hi.is_none_or(|x| p.0 < x) && cursor.is_none_or(|Cursor(c)| *p < c)
}

/// Pre-merge predicates used for candidate generation only (FP §2.2 safety argument, invariant 4): rows of the
/// project that look like roots, with a start in the window, below the cursor. The null-parent test comes first:
/// ids are parsed only for root-looking rows.
fn candidates(r: &RunsRequest, hi: Option<i64>, b: &RecordBatch) -> Result<Vec<Pos>, QueryError> {
    let s = b.schema_ref();
    let par = b.column(s.index_of(col::PARENT_SPAN_ID)?);
    let p = b.column(s.index_of(col::PROJECT_ID)?).as_string::<i32>();
    let ids = Ids::of(b)?;
    Ok((0..b.num_rows())
        .filter(|&i| par.is_null(i) && p.value(i) == r.project.as_str())
        .filter_map(|i| ids.pos(i))
        .filter(|pos| in_window(r.start_ns, hi, r.cursor, pos))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The list returns the trace path's columns without payloads, and reads nothing more.
    #[test]
    fn run_columns() {
        assert_eq!(crate::trace::trace_columns(false), RUN_COLUMNS);
        assert_eq!(read_columns(&RUN_COLUMNS), RUN_COLUMNS);
        assert_eq!(
            read_columns(&crate::trace::trace_columns(true)),
            crate::trace::trace_columns(true)
        );
    }
}
