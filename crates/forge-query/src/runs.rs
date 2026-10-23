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
use forge_meta::SegmentFilter;
use forge_model::enums::{RunType, SpanStatus};
use forge_model::ids::{ProjectId, SpanId, TraceId};
use forge_model::schema::{col, projected_schema};
use forge_storage::RowSelection;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

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
    /// Lowercased case-insensitive substring of `name` (FTS in M4).
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

fn bad(msg: impl Into<String>) -> QueryError {
    QueryError::InvalidInput(msg.into())
}

fn parse_list<T: Ord + std::str::FromStr<Err = forge_model::error::ModelError>>(
    s: Option<&str>,
) -> Result<BTreeSet<T>, QueryError> {
    s.into_iter()
        .flat_map(|s| s.split(','))
        .filter(|x| !x.is_empty())
        .map(|x| x.trim().parse::<T>().map_err(|e| bad(e.to_string())))
        .collect()
}

fn parse_time(what: &str, s: Option<&str>) -> Result<Option<i64>, QueryError> {
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
        let q = p.q.filter(|q| !q.is_empty()).map(|q| q.to_lowercase());
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

    /// hex(`[1][start_ns BE][trace 16][span 8][filter_hash BE]`): 74 hex digits.
    fn encode_cursor(&self, c: &Cursor) -> String {
        let (start, trace, span) = c.0;
        let mut b = vec![1u8];
        b.extend_from_slice(&start.to_be_bytes());
        b.extend_from_slice(trace.as_bytes());
        b.extend_from_slice(span.as_bytes());
        b.extend_from_slice(&self.filter_hash().to_be_bytes());
        hex::encode(b)
    }

    fn decode_cursor(&self, s: &str) -> Result<Cursor, QueryError> {
        let b = hex::decode(s).map_err(|_| bad("bad cursor"))?;
        if b.len() != 37 || b[0] != 1 {
            return Err(bad("bad cursor"));
        }
        if b[33..37] != self.filter_hash().to_be_bytes() {
            return Err(bad("cursor does not match the query's filters"));
        }
        let start = i64::from_be_bytes(b[1..9].try_into().expect("8 bytes"));
        Ok(Cursor((
            start,
            TraceId::from_slice(&b[9..25])?,
            SpanId::from_slice(&b[25..33])?,
        )))
    }

    fn below_cursor(&self, p: &Pos) -> bool {
        self.cursor.is_none_or(|Cursor(c)| *p < c)
    }
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
const VERIFY_GAP: u64 = 4096;
/// Up to this many distinct wave span ids are pushed into the verify scans as a key filter.
const MAX_FILTER_SPANS: usize = 256;

/// The id and start columns of a batch, by name.
struct Ids<'a> {
    start: &'a arrow::array::TimestampNanosecondArray,
    trace: &'a arrow::array::BinaryArray,
    span: &'a arrow::array::BinaryArray,
}

impl<'a> Ids<'a> {
    fn of(b: &'a RecordBatch) -> Result<Self, QueryError> {
        let s = b.schema_ref();
        Ok(Self {
            start: b
                .column(s.index_of(col::START_TIME)?)
                .as_primitive::<TimestampNanosecondType>(),
            trace: b.column(s.index_of(col::TRACE_ID)?).as_binary::<i32>(),
            span: b.column(s.index_of(col::SPAN_ID)?).as_binary::<i32>(),
        })
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

/// The smallest `Reverse` position with start <= `start` (candidates are kept newest first).
fn at_or_below(start: i64) -> Reverse<Pos> {
    Reverse((start, TraceId([0xff; 16]), SpanId([0xff; 8])))
}

/// `parent_span_id IS NULL`: a first cut for candidate scans (candidates() re-checks every row).
fn roots_filter() -> vortex::expr::Expression {
    vortex::expr::is_null(vortex::expr::col(col::PARENT_SPAN_ID))
}

/// `span_id IN spans` (a key column: allowed below the merge, invariant 4), for at most MAX_FILTER_SPANS ids.
fn span_filter(spans: &BTreeSet<SpanId>) -> Option<vortex::expr::Expression> {
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
            q.blocking("runs.json", move |cancel| {
                // The body: reserved up front at the page's size, then at its real capacity.
                let held = res.size();
                res.try_grow(page.rows.get_array_memory_size())?;
                let mut out = Vec::with_capacity(page.rows.get_array_memory_size());
                crate::json::write_runs(&mut out, &page.rows, cursor.as_deref(), cancel)?;
                res.try_resize(held + out.capacity())?;
                Ok(out)
            })
            .await
        })
        .await
    }

    /// The page, and the call's reservation (D15), which holds the page's rows.
    async fn list_runs_inner(
        &self,
        q: &QueryCtx,
        r: &RunsRequest,
    ) -> Result<(RunsPage, MemoryReservation), QueryError> {
        let k = r.limit.max(1);
        let mut res = MemoryConsumer::new("runs").register(&q.pool);
        let view = self.read_view(); // FIRST (invariant 1)
        let cols = read_columns(&RUN_COLUMNS);
        let schema = projected_schema(&cols);
        // Exclusive upper bound on start: the window end and, via the cursor, start <= cursor.start.
        let hi = match r.cursor.and_then(|Cursor(c)| c.0.checked_add(1)) {
            Some(c) => Some(r.end_ns.map_or(c, |e| e.min(c))),
            None => r.end_ns,
        };
        let filter = SegmentFilter {
            project_id: Some(r.project.clone()),
            start_ns: r.start_ns,
            end_ns: hi,
            newest_first: true,
            ..Default::default()
        };
        // All-null-start segments hold no candidates (their rows are still read by verify via locate_traces).
        let segs: Vec<_> = view
            .list_segments(&filter)
            .await?
            .into_iter()
            .filter(|s| s.max_start_ns.is_some())
            .collect();
        let mut cand: BTreeSet<Reverse<Pos>> = BTreeSet::new();
        let (snap, rr) = (view.mem().clone(), r.clone());
        let mem = q
            .blocking("runs.mem", move |cancel| {
                let k = KeyFilters::project(&rr.project, None);
                let mut v = Vec::new();
                for b in mem_versions(&snap, &k, &projected_schema(&CANDIDATE_COLUMNS), cancel)? {
                    v.extend(candidates(&rr, hi, &b)?);
                }
                Ok(v)
            })
            .await?;
        cand.extend(mem.into_iter().map(Reverse));
        // Candidates from `unchecked` on (in `cand`'s order) are not verified yet; None: all are. Segments are
        // read in descending max_start, so every verified candidate starts above every unverified one.
        let mut unchecked = Some(Bound::Unbounded);
        // The verified rows of every wave. Only the last wave can take `verified` past K (the loop stops at the
        // first wave that reaches K), so they stay bounded by K + one wave.
        let mut merged: Vec<RecordBatch> = Vec::new();
        let mut verified: BTreeMap<Reverse<Pos>, (usize, usize)> = BTreeMap::new();
        let mut i = 0;
        loop {
            let frontier = segs.get(i).and_then(|s| s.max_start_ns);
            let upper = frontier.map(at_or_below);
            if let Some(lo) = unchecked.take() {
                let end = upper.map_or(Bound::Unbounded, Bound::Excluded);
                let wave: Vec<Pos> = cand.range((lo, end)).map(|p| p.0).collect();
                if !wave.is_empty() {
                    let (b, r2) = self.verify(q, &view, r, hi, &cols, &wave, res).await?;
                    res = r2;
                    let ids = Ids::of(&b)?;
                    for row in 0..b.num_rows() {
                        let p = ids.pos(row).expect("verified rows have a start");
                        verified.insert(Reverse(p), (merged.len(), row));
                    }
                    merged.push(b);
                }
            }
            unchecked = upper.map(Bound::Included);
            let kth = verified.keys().nth(k - 1).map(|p| p.0);
            if kth.is_some_and(|p| frontier.is_none_or(|f| p.0 > f)) {
                break;
            }
            let Some(f) = frontier else { break };
            // Read segs[i] and every following segment with the same max_start (ties together).
            let mut parts = Vec::new();
            while segs.get(i).is_some_and(|s| s.max_start_ns == Some(f)) {
                parts.push(SegmentPart {
                    seg: segs[i].clone(),
                    rows: RowSelection::All,
                });
                i += 1;
            }
            let batches = view.scan(&parts, &CANDIDATE_COLUMNS, Some(roots_filter())).await?;
            let rr = r.clone();
            let found = q
                .blocking("runs.candidates", move |cancel| {
                    let mut v = Vec::new();
                    for b in &batches {
                        cancel.check()?;
                        v.extend(candidates(&rr, hi, b)?);
                    }
                    Ok(v)
                })
                .await?;
            cand.extend(found.into_iter().map(Reverse));
        }
        let unverified = unchecked.is_some_and(|lo| cand.range((lo, Bound::Unbounded)).next().is_some());
        let more = verified.len() > k || i < segs.len() || unverified;
        let next_cursor = (verified.len() >= k && more).then(|| Cursor(verified.keys().nth(k - 1).expect("k rows").0));
        let idx: Vec<(usize, usize)> = verified.values().take(k).copied().collect();
        let rows = if idx.is_empty() {
            RecordBatch::new_empty(schema)
        } else {
            interleave_record_batch(&merged.iter().collect::<Vec<_>>(), &idx)?
        };
        res.resize(rows.get_array_memory_size());
        Ok((RunsPage { rows, next_cursor }, res))
    }

    /// Every version of the wave's keys (all `locate_traces` locations, range-scanned, + the memtable), merged;
    /// the merged rows that pass EVERY filter. `res` (holding the rows kept so far) grows by the wave's input and
    /// merged rows and ends at what was kept plus the returned rows.
    #[allow(clippy::too_many_arguments)]
    async fn verify(
        &self,
        q: &QueryCtx,
        view: &ReadView,
        r: &RunsRequest,
        hi: Option<i64>,
        cols: &[&'static str],
        wave: &[Pos],
        res: MemoryReservation,
    ) -> Result<(RecordBatch, MemoryReservation), QueryError> {
        let keys: BTreeSet<(TraceId, SpanId)> = wave.iter().map(|p| (p.1, p.2)).collect();
        let filter = span_filter(&keys.iter().map(|k| k.1).collect());
        let k = KeyFilters {
            traces: Some(keys.iter().map(|k| k.0).collect()),
            spans: Some(keys),
            ..KeyFilters::project(&r.project, None)
        };
        let parts = coalesce(view.locate_versions(&k).await?, VERIFY_GAP);
        let scanned = view.scan(&parts, cols, filter).await?;
        let schema: SchemaRef = projected_schema(cols);
        let (snap, r) = (view.mem().clone(), r.clone());
        q.blocking("runs.verify", move |cancel| {
            let mut batches = Vec::new();
            for b in scanned {
                batches.push(match k.key_mask(&b)? {
                    Some(m) => filter_record_batch(&b, &m)?,
                    None => b,
                });
            }
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
            let mask: BooleanArray = (0..m.num_rows())
                .map(|i| {
                    Some(
                        par.is_null(i)
                            && project.value(i) == r.project.as_str()
                            && ids.pos(i).is_some_and(|p| in_window(&r, hi, &p))
                            && in_set(&r.run_types, rt, i)
                            && in_set(&r.statuses, st, i)
                            && r.q
                                .as_ref()
                                .is_none_or(|q| name.is_valid(i) && name.value(i).to_lowercase().contains(q.as_str())),
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
}

/// Row `i` of `a` parses to a member of `set` (an empty set matches anything).
fn in_set<T: Ord + std::str::FromStr>(set: &BTreeSet<T>, a: &StringArray, i: usize) -> bool {
    set.is_empty() || a.is_valid(i) && a.value(i).parse().is_ok_and(|x| set.contains(&x))
}

/// start in `[start_ns, hi)` and strictly after the cursor.
fn in_window(r: &RunsRequest, hi: Option<i64>, p: &Pos) -> bool {
    r.start_ns.is_none_or(|x| p.0 >= x) && hi.is_none_or(|x| p.0 < x) && r.below_cursor(p)
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
        .filter(|pos| in_window(r, hi, pos))
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
