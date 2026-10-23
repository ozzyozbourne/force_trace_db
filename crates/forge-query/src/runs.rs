//! Runs list fast path (D6): root spans of a project, newest first, keyset-paged.
//!
//! Candidates come from rows that LOOK like roots in the window (`parent_span_id IS NULL`, start in range): these
//! pre-merge predicates are safe for candidate generation only, because a merged root has a null parent in every
//! version and decision 5 makes start_time immutable (FP §2.2; invariant 4). Every candidate is then verified
//! against ALL of its versions (every `locate_traces` location + the memtable), merged, and only the merged rows
//! are filtered. Segments are read newest `max_start` first; the page is final once K verified rows all start
//! after the next unread segment's `max_start`. No budget: a deep page costs O(segments overlapping the window)
//! (M3's per-hour L1 bounds that).
use crate::json::SpanCols;
use crate::trace::{key_mask, mem_rows, scan_parts};
use crate::{QueryEngine, QueryError};
use arrow::array::{Array, AsArray, BooleanArray, RecordBatch};
use arrow::compute::{filter_record_batch, interleave_record_batch};
use arrow::datatypes::{SchemaRef, TimestampNanosecondType};
use forge_meta::SegmentFilter;
use forge_model::MemtableSnapshot;
use forge_model::derived::with_effective_total_tokens;
use forge_model::enums::{RunType, SpanStatus};
use forge_model::ids::{ProjectId, SpanId, TraceId};
use forge_model::merge::{CancelOnDrop, Tombstones, merge_projected};
use forge_model::schema::{col, projected_schema};
use forge_storage::RowSelection;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashSet};

/// What a list reads: never payload, never dotted_order.
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
    pub fn filter_hash(&self) -> u32 {
        let rt: Vec<&str> = self.run_types.iter().map(|r| r.as_str()).collect();
        let st: Vec<&str> = self.statuses.iter().map(|r| r.as_str()).collect();
        let norm = format!(
            "{}|{:?}|{:?}|{rt:?}|{st:?}|{:?}",
            self.project, self.start_ns, self.end_ns, self.q
        );
        crc32fast::hash(norm.as_bytes())
    }

    /// hex(`[1][start_ns BE][trace 16][span 8][filter_hash BE]`): 74 hex digits.
    pub fn encode_cursor(&self, c: &Cursor) -> String {
        let (start, trace, span) = c.0;
        let mut b = vec![1u8];
        b.extend_from_slice(&start.to_be_bytes());
        b.extend_from_slice(trace.as_bytes());
        b.extend_from_slice(span.as_bytes());
        b.extend_from_slice(&self.filter_hash().to_be_bytes());
        hex::encode(b)
    }

    pub fn decode_cursor(&self, s: &str) -> Result<Cursor, QueryError> {
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

/// (start, trace, span) of each row; None where start is null.
fn positions(b: &RecordBatch) -> Vec<Option<Pos>> {
    let s = b.schema_ref();
    let st = b
        .column(s.index_of(col::START_TIME).expect("start"))
        .as_primitive::<TimestampNanosecondType>();
    let t = b.column(s.index_of(col::TRACE_ID).expect("trace")).as_binary::<i32>();
    let sp = b.column(s.index_of(col::SPAN_ID).expect("span")).as_binary::<i32>();
    (0..b.num_rows())
        .map(|i| {
            let ids = (
                TraceId::from_slice(t.value(i)).ok()?,
                SpanId::from_slice(sp.value(i)).ok()?,
            );
            st.is_valid(i).then(|| (st.value(i), ids.0, ids.1))
        })
        .collect()
}

impl QueryEngine {
    pub async fn list_runs(&self, r: &RunsRequest) -> Result<RunsPage, QueryError> {
        self.deadline(self.cfg.timeout, self.list_runs_inner(r)).await
    }

    /// `{"runs":[…],"next_cursor":hex|null}`; spans written like the trace path, plus `trace_id`, no children.
    pub async fn runs_json(&self, r: &RunsRequest) -> Result<Vec<u8>, QueryError> {
        self.deadline(self.cfg.timeout, async {
            let page = self.list_runs_inner(r).await?;
            let cursor = page.next_cursor.map(|c| r.encode_cursor(&c));
            let cancel = self.new_cancel();
            let _guard = CancelOnDrop(cancel.clone());
            let out = tokio::task::spawn_blocking(move || {
                let cols = SpanCols::new(&page.rows, true);
                let mut out = b"{\"runs\":[".to_vec();
                for i in 0..page.rows.num_rows() {
                    if i % 1024 == 0 {
                        cancel.check()?;
                    }
                    if i > 0 {
                        out.push(b',');
                    }
                    out.push(b'{');
                    cols.write_span(&mut out, i, false, false);
                    out.push(b'}');
                }
                out.extend_from_slice(b"],\"next_cursor\":");
                match cursor {
                    Some(c) => out.extend_from_slice(format!("\"{c}\"").as_bytes()),
                    None => out.extend_from_slice(b"null"),
                }
                out.push(b'}');
                Ok::<_, QueryError>(out)
            })
            .await??;
            Ok(out)
        })
        .await
    }

    async fn list_runs_inner(&self, r: &RunsRequest) -> Result<RunsPage, QueryError> {
        let k = r.limit.max(1);
        let schema = projected_schema(&RUN_COLUMNS);
        let snap = self.mem.snapshot(); // FIRST (invariant 1)
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
        let segs: Vec<_> = self
            .meta
            .list_segments(&filter)
            .await?
            .into_iter()
            .filter(|s| s.max_start_ns.is_some())
            .collect();
        let mut cand: BTreeSet<Reverse<Pos>> = BTreeSet::new();
        for b in mem_rows(&snap, &schema, |b| key_mask(b, &r.project, None))? {
            candidates(r, hi, &b, &mut cand);
        }
        let mut checked: HashSet<Pos> = HashSet::new();
        let mut merged: Vec<RecordBatch> = Vec::new();
        let mut verified: BTreeMap<Reverse<Pos>, (usize, usize)> = BTreeMap::new();
        let mut i = 0;
        loop {
            let frontier = segs.get(i).and_then(|s| s.max_start_ns);
            let wave: Vec<Pos> = cand
                .iter()
                .map(|p| p.0)
                .filter(|p| !checked.contains(p) && frontier.is_none_or(|f| p.0 > f))
                .collect();
            if !wave.is_empty() {
                let b = self.verify(r, hi, &schema, &snap, &wave).await?;
                checked.extend(wave);
                for (row, p) in positions(&b).into_iter().enumerate() {
                    verified.insert(Reverse(p.expect("verified rows have a start")), (merged.len(), row));
                }
                merged.push(b);
            }
            let kth = verified.keys().nth(k - 1).map(|p| p.0);
            if kth.is_some_and(|p| frontier.is_none_or(|f| p.0 > f)) {
                break;
            }
            let Some(f) = frontier else { break };
            // Read segs[i] and every following segment with the same max_start (ties together).
            let mut parts = Vec::new();
            while segs.get(i).is_some_and(|s| s.max_start_ns == Some(f)) {
                parts.push((segs[i].clone(), RowSelection::All));
                i += 1;
            }
            for b in scan_parts(&self.store, parts, &RUN_COLUMNS).await? {
                candidates(r, hi, &b, &mut cand);
            }
        }
        let more = verified.len() > k || i < segs.len() || cand.iter().any(|p| !checked.contains(&p.0));
        let next_cursor = (verified.len() >= k && more).then(|| Cursor(verified.keys().nth(k - 1).expect("k rows").0));
        let idx: Vec<(usize, usize)> = verified.values().take(k).copied().collect();
        let rows = if idx.is_empty() {
            RecordBatch::new_empty(schema)
        } else {
            interleave_record_batch(&merged.iter().collect::<Vec<_>>(), &idx)?
        };
        Ok(RunsPage { rows, next_cursor })
    }

    /// Every version of the wave's keys (all `locate_traces` locations, range-scanned, + the memtable), merged;
    /// the merged rows that pass EVERY filter.
    async fn verify(
        &self,
        r: &RunsRequest,
        hi: Option<i64>,
        schema: &SchemaRef,
        snap: &MemtableSnapshot,
        wave: &[Pos],
    ) -> Result<RecordBatch, QueryError> {
        let keys: HashSet<(TraceId, SpanId)> = wave.iter().map(|p| (p.1, p.2)).collect();
        let traces: Vec<TraceId> = keys.iter().map(|k| k.0).collect::<BTreeSet<_>>().into_iter().collect();
        let locs = self.meta.locate_traces(&r.project, &traces).await?;
        let parts = locs
            .into_values()
            .flatten()
            .map(|l| (l.segment.clone(), RowSelection::Range(l.rows())))
            .collect();
        let in_keys = |b: &RecordBatch| -> BooleanArray {
            let s = b.schema_ref();
            let p = b
                .column(s.index_of(col::PROJECT_ID).expect("project"))
                .as_string::<i32>();
            let t = b.column(s.index_of(col::TRACE_ID).expect("trace")).as_binary::<i32>();
            let sp = b.column(s.index_of(col::SPAN_ID).expect("span")).as_binary::<i32>();
            (0..b.num_rows())
                .map(|i| {
                    let key = TraceId::from_slice(t.value(i))
                        .ok()
                        .zip(SpanId::from_slice(sp.value(i)).ok());
                    Some(p.value(i) == r.project.as_str() && key.is_some_and(|k| keys.contains(&k)))
                })
                .collect()
        };
        let mut batches = Vec::new();
        for b in scan_parts(&self.store, parts, &RUN_COLUMNS).await? {
            batches.push(filter_record_batch(&b, &in_keys(&b))?);
        }
        batches.extend(mem_rows(snap, schema, in_keys)?);
        let reservation = self.fast_reservation("runs", &batches)?;
        let cancel = self.new_cancel();
        let _guard = CancelOnDrop(cancel.clone());
        let s2 = schema.clone();
        let m = tokio::task::spawn_blocking(move || {
            Ok::<_, QueryError>(with_effective_total_tokens(&merge_projected(
                &s2,
                &batches,
                Tombstones::Drop,
                &cancel,
            )?)?)
        })
        .await??;
        drop(reservation);
        let s = m.schema_ref();
        let par = m.column(s.index_of(col::PARENT_SPAN_ID)?);
        let project = m.column(s.index_of(col::PROJECT_ID)?).as_string::<i32>();
        let rt = m.column(s.index_of(col::RUN_TYPE)?).as_string::<i32>();
        let st = m.column(s.index_of(col::STATUS)?).as_string::<i32>();
        let name = m.column(s.index_of(col::NAME)?).as_string::<i32>();
        let one_of = |set_empty: bool, a: &arrow::array::StringArray, i: usize, f: &dyn Fn(&str) -> bool| {
            set_empty || a.is_valid(i) && f(a.value(i))
        };
        let mask: BooleanArray = positions(&m)
            .into_iter()
            .enumerate()
            .map(|(i, p)| {
                let Some(p) = p else { return Some(false) };
                Some(
                    project.value(i) == r.project.as_str()
                        && par.is_null(i)
                        && in_window(r, hi, &p)
                        && one_of(r.run_types.is_empty(), rt, i, &|v| {
                            v.parse().is_ok_and(|x| r.run_types.contains(&x))
                        })
                        && one_of(r.statuses.is_empty(), st, i, &|v| {
                            v.parse().is_ok_and(|x| r.statuses.contains(&x))
                        })
                        && r.q
                            .as_ref()
                            .is_none_or(|q| name.is_valid(i) && name.value(i).to_lowercase().contains(q.as_str())),
                )
            })
            .collect();
        Ok(filter_record_batch(&m, &mask)?)
    }
}

/// start in `[start_ns, hi)` and strictly after the cursor.
fn in_window(r: &RunsRequest, hi: Option<i64>, p: &Pos) -> bool {
    r.start_ns.is_none_or(|x| p.0 >= x) && hi.is_none_or(|x| p.0 < x) && r.below_cursor(p)
}

/// Pre-merge predicates used for candidate generation only (FP §2.2 safety argument, invariant 4): rows of the
/// project that look like roots, with a start in the window, below the cursor.
fn candidates(r: &RunsRequest, hi: Option<i64>, b: &RecordBatch, out: &mut BTreeSet<Reverse<Pos>>) {
    let s = b.schema_ref();
    let par = b.column(s.index_of(col::PARENT_SPAN_ID).expect("parent"));
    let p = b
        .column(s.index_of(col::PROJECT_ID).expect("project"))
        .as_string::<i32>();
    for (i, pos) in positions(b).into_iter().enumerate() {
        if let Some(pos) = pos
            && par.is_null(i)
            && p.value(i) == r.project.as_str()
            && in_window(r, hi, &pos)
        {
            out.insert(Reverse(pos));
        }
    }
}
