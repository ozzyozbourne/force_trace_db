//! Runs list fast path (D6, FP §2.3).
use crate::trace::{mem_rows, scan_parts};
use crate::{QueryEngine, QueryError};
use arrow::array::{Array, AsArray, BooleanArray, RecordBatch};
use arrow::compute::interleave_record_batch;
use arrow::datatypes::TimestampNanosecondType;
use forge_meta::SegmentFilter;
use forge_model::derived::with_effective_total_tokens;
use forge_model::enums::{RunType, SpanStatus};
use forge_model::ids::{ProjectId, SpanId, TraceId};
use forge_model::merge::{Cancel, CancelOnDrop, Tombstones, merge_projected};
use forge_model::schema::{col, projected_schema};
use forge_storage::RowSelection;
use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

pub const RUN_COLUMNS: [&str; 21] = [
    col::PROJECT_ID, col::TRACE_ID, col::SPAN_ID, col::PARENT_SPAN_ID, col::NAME, col::RUN_TYPE, col::START_TIME,
    col::END_TIME, col::STATUS, col::ERROR, col::MODEL, col::PROVIDER, col::PROMPT_TOKENS, col::COMPLETION_TOKENS,
    col::TOTAL_TOKENS, col::COST_USD, col::SESSION_ID, col::TAGS, col::METADATA, col::VERSION, col::DELETED,
];
const S_MAX: usize = 64;
const T_MAX: Duration = Duration::from_millis(250);

// NOTE: the plan says BTreeSet<RunType>; RunType/SpanStatus are not Ord, so HashSet here.
pub struct RunsRequest {
    pub project: ProjectId,
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub run_types: HashSet<RunType>,
    pub statuses: HashSet<SpanStatus>,
    pub q: Option<String>,
    pub limit: usize,
    pub cursor: Option<Cursor>,
    pub budget: Option<(usize, Duration)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cursor {
    pub start_ns: i64,
    pub trace: TraceId,
    pub span: SpanId,
}

impl Cursor {
    pub fn encode(&self, hash: u32) -> String {
        let mut b = vec![1u8];
        b.extend_from_slice(&self.start_ns.to_be_bytes());
        b.extend_from_slice(self.trace.as_bytes());
        b.extend_from_slice(self.span.as_bytes());
        b.extend_from_slice(&hash.to_be_bytes());
        hex::encode(b)
    }
    pub fn decode(s: &str, hash: u32) -> Result<Self, QueryError> {
        let bad = || QueryError::InvalidInput("bad cursor".into());
        let b = hex::decode(s).map_err(|_| bad())?;
        if b.len() != 37 || b[0] != 1 {
            return Err(bad());
        }
        if u32::from_be_bytes(b[33..37].try_into().unwrap()) != hash {
            return Err(QueryError::InvalidInput("cursor does not match the query".into()));
        }
        Ok(Self {
            start_ns: i64::from_be_bytes(b[1..9].try_into().unwrap()),
            trace: TraceId::from_slice(&b[9..25])?,
            span: SpanId::from_slice(&b[25..33])?,
        })
    }
}

impl RunsRequest {
    pub fn filter_hash(&self) -> u32 {
        let mut rt: Vec<&str> = self.run_types.iter().map(|r| r.as_str()).collect();
        rt.sort();
        let mut st: Vec<&str> = self.statuses.iter().map(|r| r.as_str()).collect();
        st.sort();
        crc32fast::hash(format!("{}|{:?}|{:?}|{rt:?}|{st:?}|{:?}", self.project, self.start_ns, self.end_ns, self.q).as_bytes())
    }
}

pub struct RunsPage {
    pub rows: RecordBatch,
    pub next_cursor: Option<Cursor>,
}

type Pos = (i64, TraceId, SpanId);

fn pos_of(b: &RecordBatch, i: usize) -> Option<Pos> {
    let s = b.schema();
    let st = b.column(s.index_of(col::START_TIME).ok()?).as_primitive::<TimestampNanosecondType>();
    let t = b.column(s.index_of(col::TRACE_ID).ok()?).as_binary::<i32>();
    let sp = b.column(s.index_of(col::SPAN_ID).ok()?).as_binary::<i32>();
    st.is_valid(i).then(|| (st.value(i), TraceId::from_slice(t.value(i)).unwrap(), SpanId::from_slice(sp.value(i)).unwrap()))
}

impl QueryEngine {
    pub async fn list_runs(&self, r: &RunsRequest) -> Result<RunsPage, QueryError> {
        self.deadline(self.cfg.timeout, self.list_runs_inner(r)).await
    }

    /// Pre-merge predicates used for candidate generation only (FP §2.2 safety argument; invariant 4).
    fn candidates(&self, r: &RunsRequest, hi: Option<i64>, b: &RecordBatch, out: &mut BTreeMap<Reverse<Pos>, ()>) {
        let s = b.schema();
        let par = b.column(s.index_of(col::PARENT_SPAN_ID).unwrap());
        let p = b.column(s.index_of(col::PROJECT_ID).unwrap()).as_string::<i32>();
        for i in 0..b.num_rows() {
            if par.is_valid(i) || p.value(i) != r.project.as_str() {
                continue;
            }
            let Some(pos) = pos_of(b, i) else { continue };
            if r.start_ns.is_some_and(|x| pos.0 < x) || hi.is_some_and(|x| pos.0 >= x) {
                continue;
            }
            if r.cursor.is_some_and(|c| pos >= (c.start_ns, c.trace, c.span)) {
                continue;
            }
            out.insert(Reverse(pos), ());
        }
    }

    async fn list_runs_inner(&self, r: &RunsRequest) -> Result<RunsPage, QueryError> {
        let (s_max, t_max) = r.budget.unwrap_or((S_MAX, T_MAX));
        let k = r.limit;
        let schema = projected_schema(&RUN_COLUMNS);
        let snap = self.mem.snapshot(); // FIRST
        let hi = match (r.end_ns, r.cursor) {
            (e, Some(c)) => Some(e.map_or(c.start_ns.saturating_add(1), |e| e.min(c.start_ns.saturating_add(1)))),
            (e, None) => e,
        };
        let segs: Vec<_> = self
            .meta
            .list_segments(&SegmentFilter {
                project_id: Some(r.project.clone()),
                start_ns: r.start_ns,
                end_ns: hi,
                newest_first: true,
                ..Default::default()
            })
            .await?
            .into_iter()
            .filter(|s| s.max_start_ns.is_some())
            .collect();
        let mem = mem_rows(&snap, &schema, |b| crate::trace::key_mask(b, &r.project, None))?;
        let mut cand: BTreeMap<Reverse<Pos>, ()> = BTreeMap::new();
        for b in &mem {
            self.candidates(r, hi, b, &mut cand);
        }
        let mut checked: HashSet<(TraceId, SpanId)> = HashSet::new();
        let mut merged: Vec<RecordBatch> = Vec::new();
        let mut verified: BTreeMap<Reverse<Pos>, (usize, usize)> = BTreeMap::new();
        let (mut i, t0) = (0usize, Instant::now());
        loop {
            let frontier = segs.get(i).and_then(|s| s.max_start_ns).map(|m| hi.map_or(m, |h| m.min(h - 1)));
            let wave: Vec<Pos> = cand
                .keys()
                .map(|x| x.0)
                .filter(|p| !checked.contains(&(p.1, p.2)) && frontier.is_none_or(|f| p.0 > f))
                .collect();
            if !wave.is_empty() {
                let b = self.verify(r, hi, &schema, &snap, &wave).await?;
                for p in &wave {
                    checked.insert((p.1, p.2));
                }
                let bi = merged.len();
                for row in 0..b.num_rows() {
                    if let Some(p) = pos_of(&b, row) {
                        verified.insert(Reverse(p), (bi, row));
                    }
                }
                merged.push(b);
            }
            let kth = verified.keys().nth(k.saturating_sub(1)).map(|x| x.0);
            if verified.len() >= k && (frontier.is_none() || kth.is_some_and(|p| frontier.is_some_and(|f| p.0 > f))) {
                break;
            }
            let Some(f) = frontier else { break };
            if (i >= s_max || t0.elapsed() >= t_max) && r.cursor.is_none_or(|c| f < c.start_ns) {
                let idx: Vec<(usize, usize)> = verified.iter().filter(|(p, _)| p.0.0 > f).take(k).map(|(_, v)| *v).collect();
                let rows = take(&schema, &merged, &idx)?;
                return Ok(RunsPage { rows, next_cursor: Some(Cursor { start_ns: f, trace: TraceId([0xff; 16]), span: SpanId([0xff; 8]) }) });
            }
            // the next wave: segs[i] and following segments with an equal max_start
            let mut parts = Vec::new();
            loop {
                parts.push((segs[i].clone(), RowSelection::All));
                i += 1;
                if segs.get(i).is_none_or(|s| s.max_start_ns.map(|m| hi.map_or(m, |h| m.min(h - 1))) != frontier) {
                    break;
                }
            }
            for b in scan_parts(&self.store, parts, &RUN_COLUMNS, None).await? {
                self.candidates(r, hi, &b, &mut cand);
            }
        }
        let idx: Vec<(usize, usize)> = verified.values().take(k).copied().collect();
        let more = verified.len() > k || segs.get(i).is_some() || cand.keys().any(|p| !checked.contains(&(p.0.1, p.0.2)));
        let next_cursor = (verified.len() >= k && more).then(|| {
            let p = verified.keys().nth(k - 1).unwrap().0;
            Cursor { start_ns: p.0, trace: p.1, span: p.2 }
        });
        Ok(RunsPage { rows: take(&schema, &merged, &idx)?, next_cursor })
    }

    /// All versions of the wave's keys, merged; only rows that pass every filter.
    async fn verify(
        &self,
        r: &RunsRequest,
        hi: Option<i64>,
        schema: &arrow::datatypes::SchemaRef,
        snap: &forge_model::MemtableSnapshot,
        wave: &[Pos],
    ) -> Result<RecordBatch, QueryError> {
        let keys: HashSet<(TraceId, SpanId)> = wave.iter().map(|p| (p.1, p.2)).collect();
        let traces: Vec<TraceId> = keys.iter().map(|k| k.0).collect::<HashSet<_>>().into_iter().collect();
        let locs: HashMap<TraceId, Vec<forge_model::catalog::TraceLocation>> = self.meta.locate_traces(&r.project, &traces).await?;
        let parts = locs.values().flatten().map(|l| (l.segment.clone(), RowSelection::Range(l.rows()))).collect();
        let key_filter = |b: &RecordBatch| -> BooleanArray {
            let s = b.schema();
            let t = b.column(s.index_of(col::TRACE_ID).unwrap()).as_binary::<i32>();
            let sp = b.column(s.index_of(col::SPAN_ID).unwrap()).as_binary::<i32>();
            (0..b.num_rows())
                .map(|i| Some(keys.contains(&(TraceId::from_slice(t.value(i)).unwrap(), SpanId::from_slice(sp.value(i)).unwrap()))))
                .collect()
        };
        let mut batches = Vec::new();
        for b in scan_parts(&self.store, parts, &RUN_COLUMNS, None).await? {
            batches.push(arrow::compute::filter_record_batch(&b, &key_filter(&b))?);
        }
        batches.extend(mem_rows(snap, schema, |b| {
            let pm = crate::trace::key_mask(b, &r.project, None);
            let km = key_filter(b);
            arrow::compute::and(&pm, &km).unwrap()
        })?);
        let cancel = Cancel::new();
        let _g = CancelOnDrop(cancel.clone());
        let s2 = schema.clone();
        let m = tokio::task::spawn_blocking(move || {
            let m = merge_projected(&s2, &batches, Tombstones::Drop, &cancel)?;
            Ok::<_, QueryError>(with_effective_total_tokens(&m)?)
        })
        .await??;
        // every filter, on merged rows
        let s = m.schema();
        let par = m.column(s.index_of(col::PARENT_SPAN_ID)?);
        let rt = m.column(s.index_of(col::RUN_TYPE)?).as_string::<i32>();
        let st = m.column(s.index_of(col::STATUS)?).as_string::<i32>();
        let name = m.column(s.index_of(col::NAME)?).as_string::<i32>();
        let mask: BooleanArray = (0..m.num_rows())
            .map(|i| {
                let Some(p) = pos_of(&m, i) else { return Some(false) };
                Some(
                    par.is_null(i)
                        && r.start_ns.is_none_or(|x| p.0 >= x)
                        && hi.is_none_or(|x| p.0 < x)
                        && r.cursor.is_none_or(|c| p < (c.start_ns, c.trace, c.span))
                        && (r.run_types.is_empty() || rt.is_valid(i) && rt.value(i).parse().is_ok_and(|x: RunType| r.run_types.contains(&x)))
                        && (r.statuses.is_empty() || st.is_valid(i) && st.value(i).parse().is_ok_and(|x: SpanStatus| r.statuses.contains(&x)))
                        && r.q.as_ref().is_none_or(|q| name.is_valid(i) && name.value(i).to_lowercase().contains(q.as_str())),
                )
            })
            .collect();
        Ok(arrow::compute::filter_record_batch(&m, &mask)?)
    }

    pub async fn runs_json(&self, r: &RunsRequest) -> Result<Vec<u8>, QueryError> {
        let page = self.list_runs(r).await?;
        let cols = crate::json::SpanCols::new(&page.rows);
        let mut out = b"{\"runs\":[".to_vec();
        for i in 0..page.rows.num_rows() {
            if i > 0 {
                out.push(b',');
            }
            out.push(b'{');
            crate::json::write_span(&mut out, &cols, i, 0);
            out.push(b'}');
        }
        out.extend_from_slice(b"],\"next_cursor\":");
        match page.next_cursor {
            Some(c) => out.extend_from_slice(format!("\"{}\"", c.encode(r.filter_hash())).as_bytes()),
            None => out.extend_from_slice(b"null"),
        }
        out.push(b'}');
        Ok(out)
    }
}

fn take(schema: &arrow::datatypes::SchemaRef, merged: &[RecordBatch], idx: &[(usize, usize)]) -> Result<RecordBatch, QueryError> {
    if idx.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    let refs: Vec<&RecordBatch> = merged.iter().collect();
    Ok(interleave_record_batch(&refs, idx)?)
}
