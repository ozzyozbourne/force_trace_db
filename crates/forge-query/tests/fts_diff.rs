//! E2 (plans/phase-5 §5.3): SQL `search` / `json_key_search` and every `/v1/search` page equal an INDEPENDENT oracle
//! (its own tokenizer, query parser and JSON walk; no forge-index code) over the merged rows, before and after
//! `compact + gc`. Sidecars come from the real segment sink, so this is also the end-to-end check that
//! `FtsBuilder::add_batch` agrees with the row ids of the `.vortex` files (E1 builds through `add_batch` too, but on
//! hand-made batches).
//!
//! The layout: one project, 22 L0 flushes in distinct hours (1 KiB merge chunks, so segments hold several batches),
//! start posts with `name` / `inputs`, end posts with `outputs` and no start (in a later flush, or the memtable), and
//! forced traces (a)-(h) for the single-source rule, re-checks, cross-source AND and the early-stop frontier.
use arrow::array::{AsArray, RecordBatch};
use async_trait::async_trait;
use forge_index::{Analyzer, FtsColumn, FtsExpr, FtsHits, FtsPredicate, IndexReader, IndexResult, Matcher};
use forge_ingest::blob::BlobGuard;
use forge_ingest::compact::{CompactCtx, compact_project};
use forge_ingest::lifecycle::{GcOptions, run_gc};
use forge_meta::{Metastore, SegmentFilter};
use forge_model::MemtableView;
use forge_model::catalog::{HourBucket, Level, SegmentId, SegmentMeta};
use forge_model::config::QueryConfig;
use forge_model::enums::{RunType, SpanStatus};
use forge_model::ids::{SpanId, TraceId};
use forge_model::merge::{Tombstones, merge_versions_chunked};
use forge_model::row::SpanRow;
use forge_model::testing::{FixedMemtable, Rng, batch_from_rows, pid, reference, sid, tid};
use forge_model::time::Clocks;
use forge_query::search::{JsonFilter, SearchBody, SearchFilters, SearchRequest};
use forge_query::testing::{memory_store, write_flush};
use forge_query::{FtsCountersSnapshot, FtsIndexReader, QueryEngine};
use forge_storage::SegmentStore;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BASE: i64 = 1_700_000_000_000_000_000 / HOUR * HOUR;
const HOUR: i64 = 3_600_000_000_000;
const FLUSHES: usize = 22;
const MAX_CHUNK: usize = 1 << 10;
const WORDS: [&str; 12] = [
    "refund", "order", "café", "CAFÉ", "span-1", "span-12", "日本", "alpha", "beta", "gamma", "Straße", "Æther",
];
const NAMES: [&str; 5] = ["span-1", "span-12", "Span-1", "step-3", "日本 step"];

type Key = (TraceId, SpanId);
type Pos = (i64, TraceId, SpanId);

// ---------------------------------------------------------------- layout

struct Layout {
    flushes: Vec<Vec<SpanRow>>,
    mem: Vec<SpanRow>,
}

fn at(flush: usize, off: i64) -> i64 {
    BASE + flush as i64 * HOUR + off
}

fn inputs(contents: &[&str]) -> String {
    json!({"messages": contents.iter().map(|c| json!({"role":"user","content":c})).collect::<Vec<_>>()}).to_string()
}

fn start_post(t: u64, s: u64, flush: usize, off: i64, content: &str) -> SpanRow {
    SpanRow {
        project_id: Some(pid("s")),
        trace_id: tid(t),
        span_id: sid(t * 100 + s),
        parent_span_id: (s > 0).then(|| sid(t * 100)),
        name: Some(format!("step-{s}")),
        start_time_ns: Some(at(flush, off)),
        inputs: Some(inputs(&[content])),
        metadata: Some(format!("{{\"user\":\"u{}\"}}", (t + s) % 3)),
        ..Default::default()
    }
}

fn end_post(t: u64, s: u64, flush: usize, text: &str) -> SpanRow {
    SpanRow {
        project_id: Some(pid("s")),
        trace_id: tid(t),
        span_id: sid(t * 100 + s),
        end_time_ns: Some(at(flush, HOUR - 1)),
        outputs: Some(json!({ "text": text }).to_string()),
        status: Some(SpanStatus::Ok),
        ..Default::default()
    }
}

fn words(r: &mut Rng, n: u64) -> String {
    (0..1 + r.below(n))
        .map(|_| WORDS[r.below(WORDS.len() as u64) as usize])
        .collect::<Vec<_>>()
        .join(" ")
}

/// Forced trace ids.
const T_A: u64 = 1001; // one segment says refund, a memtable post replaces inputs: not returned
const T_B: u64 = 1002; // one segment says refund, tombstoned in the memtable
const T_C: u64 = 1003; // one segment without the term, a memtable post adds it
const T_D: u64 = 1004; // loses the term in a later flush
const T_E: u64 = 1005; // refund in inputs (flush 7), order in outputs of a start-less end post (flush 10)
const T_F1: u64 = 1006; // "order a refund": not the phrase "refund order"
const T_F2: u64 = 1007; // refund only in $.messages[1]
const T_G: u64 = 1008; // single-source: refund in inputs, user u2 in metadata
const T_H: u64 = 1009; // the only `zanzibar` hit of its segment is a start-less patch row (flush 1)

fn fts_layout(seed: u64) -> Layout {
    let mut r = Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1);
    let mut flushes: Vec<Vec<SpanRow>> = vec![Vec::new(); FLUSHES];
    let mut mem = Vec::new();
    for t in 1..=40u64 {
        let spans = 1 + r.below(4);
        let fs = r.below(FLUSHES as u64) as usize;
        for s in 0..spans {
            let mut start = start_post(t, s, fs, r.below(5) as i64 * 1_000, &words(&mut r, 5));
            start.name = Some(NAMES[r.below(NAMES.len() as u64) as usize].to_string());
            start.run_type = Some([RunType::Llm, RunType::Chain, RunType::Tool][r.below(3) as usize]);
            if r.below(20) > 0 {
                flushes[fs].push(start);
            }
            if r.below(10) < 6 {
                let mut end = end_post(t, s, fs, &words(&mut r, 3));
                if r.below(3) == 0 {
                    end.status = Some(SpanStatus::Error);
                }
                match r.below(4) {
                    0 => mem.push(end),
                    _ => flushes[(fs + 1 + r.below(3) as usize).min(FLUSHES - 1)].push(end),
                }
            }
        }
    }
    // (a), (b), (c): one segment each, plus a memtable version.
    flushes[3].push(start_post(T_A, 0, 3, 7, "please refund"));
    mem.push(SpanRow {
        inputs: Some(inputs(&["never mind"])),
        start_time_ns: None,
        name: None,
        metadata: None,
        ..start_post(T_A, 0, 3, 7, "")
    });
    flushes[4].push(start_post(T_B, 0, 4, 7, "refund this"));
    mem.push(SpanRow {
        deleted: true,
        ..end_post(T_B, 0, 4, "")
    });
    flushes[5].push(start_post(T_C, 0, 5, 7, "hello there"));
    mem.push(SpanRow {
        inputs: Some(inputs(&["refund now"])),
        start_time_ns: None,
        name: None,
        metadata: None,
        ..start_post(T_C, 0, 5, 7, "")
    });
    // (d)
    flushes[6].push(start_post(T_D, 0, 6, 7, "refund order"));
    flushes[9].push(SpanRow {
        inputs: Some(inputs(&["no thanks"])),
        start_time_ns: None,
        name: None,
        metadata: None,
        ..start_post(T_D, 0, 6, 7, "")
    });
    // (e)
    flushes[7].push(start_post(T_E, 0, 7, 7, "refund"));
    flushes[10].push(end_post(T_E, 0, 10, "order shipped"));
    // (f)
    flushes[8].push(start_post(T_F1, 0, 8, 7, "order a refund"));
    flushes[8].push(SpanRow {
        inputs: Some(inputs(&["hello", "refund order"])),
        ..start_post(T_F2, 0, 8, 8, "")
    });
    // (g)
    flushes[11].push(SpanRow {
        metadata: Some(r#"{"user":"u2"}"#.into()),
        ..start_post(T_G, 0, 11, 7, "refund")
    });
    // (h): the start row is the newest start of all (in the OLDEST segment, without the term); the hit is the patch
    // in flush 1, whose max_start is old. 60 other `zanzibar` hits sit in the 8 newest hours.
    flushes[0].push(start_post(T_H, 0, FLUSHES, 0, "plain words"));
    flushes[1].push(end_post(T_H, 0, 1, "zanzibar"));
    for j in 0..60u64 {
        let f = FLUSHES - 1 - (j % 8) as usize;
        flushes[f].push(start_post(1100 + j, 0, f, 10 + j as i64, "zanzibar"));
    }
    // Every flush writes a segment.
    for (i, f) in flushes.iter_mut().enumerate() {
        f.push(start_post(1200 + i as u64, 0, i, 20, "filler gamma"));
    }
    Layout { flushes, mem }
}

// ---------------------------------------------------------------- the oracle (no forge-index code)

fn fold(t: &str) -> String {
    t.to_lowercase()
        .chars()
        .map(|c| match c {
            'é' => "e".to_string(),
            'ß' => "ss".to_string(),
            'æ' => "ae".to_string(),
            c => c.to_string(),
        })
        .collect()
}

fn toks(s: &str) -> Vec<String> {
    s.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(fold)
        .collect()
}

/// (path with `[*]`, array indices, leaf text) for every node below the root (containers have no text), the root too
/// when it is a scalar. Not JSON: one text node.
fn nodes(cell: &str, json: bool) -> Vec<(String, Vec<usize>, Option<String>)> {
    fn walk(v: &Value, path: &mut String, idx: &mut Vec<usize>, out: &mut Vec<(String, Vec<usize>, Option<String>)>) {
        let text = match v {
            Value::Object(m) => {
                if path.len() > 1 {
                    out.push((path.clone(), idx.clone(), None));
                }
                for (k, c) in m {
                    let l = path.len();
                    path.push('.');
                    path.push_str(k);
                    walk(c, path, idx, out);
                    path.truncate(l);
                }
                return;
            }
            Value::Array(a) => {
                if path.len() > 1 {
                    out.push((path.clone(), idx.clone(), None));
                }
                let l = path.len();
                path.push_str("[*]");
                for (i, c) in a.iter().enumerate() {
                    idx.push(i);
                    walk(c, path, idx, out);
                    idx.pop();
                }
                path.truncate(l);
                return;
            }
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            Value::Null => None,
        };
        out.push((path.clone(), idx.clone(), text));
    }
    match serde_json::from_str::<Value>(cell) {
        Ok(v) if json => {
            let mut out = Vec::new();
            walk(&v, &mut "$".to_string(), &mut Vec::new(), &mut out);
            out
        }
        _ => vec![(String::new(), vec![], Some(cell.to_string()))],
    }
}

/// OR of groups; a group = terms (prefix or exact) anywhere in `runs`, phrases adjacent within one run.
fn query_matches(q: &str, runs: &[Vec<String>]) -> bool {
    let mut groups: Vec<(Vec<(String, bool)>, Vec<Vec<String>>)> = vec![Default::default()];
    let mut rest = q;
    while let Some(c) = rest.chars().next() {
        if c.is_whitespace() {
            rest = &rest[c.len_utf8()..];
        } else if c == '"' {
            let end = rest[1..].find('"').map_or(rest.len(), |e| e + 1);
            let ph = toks(&rest[1..end]);
            if !ph.is_empty() {
                groups.last_mut().unwrap().1.push(ph);
            }
            rest = &rest[(end + 1).min(rest.len())..];
        } else {
            let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
            let w = &rest[..end];
            rest = &rest[end..];
            if w == "OR" {
                groups.push(Default::default());
                continue;
            }
            let prefix = w.ends_with('*');
            let ts = toks(w.trim_end_matches('*'));
            let n = ts.len();
            for (i, t) in ts.into_iter().enumerate() {
                groups.last_mut().unwrap().0.push((t, prefix && i + 1 == n));
            }
        }
    }
    groups.iter().any(|(terms, phrases)| {
        !(terms.is_empty() && phrases.is_empty())
            && terms.iter().all(|(t, p)| {
                runs.iter()
                    .flatten()
                    .any(|x| if *p { x.starts_with(t.as_str()) } else { x == t })
            })
            && phrases
                .iter()
                .all(|ph| runs.iter().any(|r| r.windows(ph.len()).any(|w| w == ph.as_slice())))
    })
}

fn cell(r: &SpanRow, c: FtsColumn) -> Option<&str> {
    match c {
        FtsColumn::Name => r.name.as_deref(),
        FtsColumn::Error => r.error.as_deref(),
        FtsColumn::Inputs => r.inputs.as_deref(),
        FtsColumn::Outputs => r.outputs.as_deref(),
        FtsColumn::Metadata => r.metadata.as_deref(),
    }
}

fn oracle_search(r: &SpanRow, c: FtsColumn, q: &str) -> bool {
    let Some(v) = cell(r, c) else { return false };
    let runs: Vec<Vec<String>> = nodes(v, c.is_json())
        .into_iter()
        .filter_map(|(_, _, t)| t.map(|t| toks(&t)))
        .collect();
    query_matches(q, &runs)
}

/// Paths used: `$.messages[*].content`, `$.messages[0].content`, `$.user`, `$.messages`.
fn oracle_jks(r: &SpanRow, c: FtsColumn, path: &str, text: &str) -> bool {
    let Some(v) = cell(r, c) else { return false };
    let (norm, idx0) = match path {
        "$.messages[0].content" => ("$.messages[*].content", Some(0)),
        p => (p, None),
    };
    let at: Vec<(Vec<usize>, Option<String>)> = nodes(v, true)
        .into_iter()
        .filter(|(p, idx, _)| p == norm && idx0.is_none_or(|i| idx.first() == Some(&i)))
        .map(|(_, i, t)| (i, t))
        .collect();
    if text.trim().is_empty() {
        return !at.is_empty();
    }
    let runs: Vec<Vec<String>> = at.into_iter().filter_map(|(_, t)| t.map(|t| toks(&t))).collect();
    query_matches(text, &runs)
}

// ---------------------------------------------------------------- predicates

#[derive(Clone, Debug)]
enum P {
    S(FtsColumn, &'static str),
    J(FtsColumn, &'static str, &'static str),
    And(Box<P>, Box<P>),
}

impl P {
    fn sql(&self) -> String {
        match self {
            P::S(c, q) => format!("search({}, '{q}')", c.name()),
            P::J(c, p, t) => format!("json_key_search({}, '{p}', '{t}')", c.name()),
            P::And(a, b) => format!("{} AND {}", a.sql(), b.sql()),
        }
    }
    fn oracle(&self, r: &SpanRow) -> bool {
        match self {
            P::S(c, q) => oracle_search(r, *c, q),
            P::J(c, p, t) => oracle_jks(r, *c, p, t),
            P::And(a, b) => a.oracle(r) && b.oracle(r),
        }
    }
    /// The shared matcher on the same cell (checked against the oracle).
    fn matcher(&self, r: &SpanRow) -> bool {
        let pred = match self {
            P::S(c, q) => FtsPredicate::Search {
                column: *c,
                query: q.to_string(),
            },
            P::J(c, p, t) => FtsPredicate::JsonKeySearch {
                column: *c,
                path: p.to_string(),
                text: t.to_string(),
            },
            P::And(a, b) => return a.matcher(r) && b.matcher(r),
        };
        let an = &mut Analyzer::default();
        let m = Matcher::new(an, &pred).unwrap();
        cell(r, m.column()).is_some_and(|v| m.matches(an, v))
    }
}

fn preds() -> Vec<P> {
    use FtsColumn::*;
    let and = |a: P, b: P| P::And(Box::new(a), Box::new(b));
    vec![
        P::S(Name, "span-1"),
        P::S(Name, "span-1*"),
        P::S(Inputs, "refund"),
        P::S(Inputs, "café OR 日本"),
        P::S(Inputs, "\"refund order\""),
        P::S(Inputs, "straße OR aether"),
        P::S(Outputs, "order"),
        P::S(Outputs, "zanzibar"),
        P::J(Inputs, "$.messages[*].content", "refund"),
        P::J(Inputs, "$.messages[0].content", "refund"),
        P::J(Metadata, "$.user", ""),
        P::J(Inputs, "$.messages", ""),
        and(P::S(Inputs, "refund"), P::S(Outputs, "order")),
        and(P::S(Inputs, "refund"), P::J(Metadata, "$.user", "u1")),
    ]
}

// ---------------------------------------------------------------- fixture

/// Reports NoIndex for `hidden` segments (brute-force fallback).
#[derive(Debug)]
struct HideIndex {
    inner: FtsIndexReader,
    hidden: Mutex<HashSet<SegmentId>>,
}

#[async_trait]
impl IndexReader for HideIndex {
    async fn search(&self, seg: &SegmentMeta, e: &FtsExpr) -> IndexResult<FtsHits> {
        if self.hidden.lock().unwrap().contains(&seg.id) {
            return Ok(FtsHits::NoIndex);
        }
        self.inner.search(seg, e).await
    }
}

struct Fx {
    meta: Arc<dyn Metastore>,
    store: Arc<dyn SegmentStore>,
    mem: Arc<dyn MemtableView>,
    hide: Arc<HideIndex>,
    all: Vec<SpanRow>,
}

impl Fx {
    async fn build(mut l: Layout) -> Self {
        let mut v = 0;
        for r in l.flushes.iter_mut().chain(std::iter::once(&mut l.mem)).flatten() {
            v += 1;
            r.version = v;
        }
        let meta: Arc<dyn Metastore> = Arc::new(forge_meta::SqliteMetastore::open_in_memory().unwrap());
        let store = memory_store();
        let (mut maxv, mut multi_batch) = (0, false);
        for (i, f) in l.flushes.iter().enumerate() {
            maxv += f.len() as u64;
            let b = batch_from_rows(f);
            multi_batch |= merge_versions_chunked(std::slice::from_ref(&b), Tombstones::Keep, MAX_CHUNK)
                .unwrap()
                .len()
                > 1;
            let hour = HourBucket::from_nanos(at(i, 0));
            write_flush(&*meta, store.clone(), &b, hour, "t", MAX_CHUNK, i as u64 + 1, maxv).await;
        }
        assert!(multi_batch, "precondition: some segment holds more than one batch");
        let segs = meta.list_segments(&SegmentFilter::default()).await.unwrap();
        assert_eq!(segs.len(), FLUSHES);
        assert!(segs.iter().all(|s| s.fts_byte_size.is_some()));
        let mem: Arc<dyn MemtableView> = Arc::new(FixedMemtable(vec![batch_from_rows(&l.mem)]));
        let hide = Arc::new(HideIndex {
            inner: FtsIndexReader::new(store.object_store()),
            hidden: Mutex::default(),
        });
        let all = l.flushes.into_iter().chain(std::iter::once(l.mem)).flatten().collect();
        Self {
            meta,
            store,
            mem,
            hide,
            all,
        }
    }

    /// A seeded third of the visible segments, never those of the traces (a)-(c).
    async fn hide_third(&self, seed: u64) {
        let mut keep = HashSet::new();
        for t in [T_A, T_B, T_C] {
            for loc in self.meta.locate_trace(&pid("s"), tid(t)).await.unwrap() {
                keep.insert(loc.segment.id);
            }
        }
        let mut r = Rng(seed | 1);
        let segs = self.meta.list_segments(&SegmentFilter::default()).await.unwrap();
        *self.hide.hidden.lock().unwrap() = segs
            .iter()
            .filter(|s| !keep.contains(&s.id) && r.below(3) == 0)
            .map(|s| s.id)
            .collect();
    }

    fn engine(&self, hidden: bool, sizes: (usize, usize)) -> QueryEngine {
        let e = QueryEngine::new(
            self.meta.clone(),
            self.store.clone(),
            self.mem.clone(),
            QueryConfig::default(),
        )
        .with_search_sizes(sizes.0, sizes.1);
        if hidden { e.with_index(self.hide.clone()) } else { e }
    }

    async fn compact(&self) {
        let mut cx = CompactCtx::new(
            self.meta.clone(),
            self.store.clone(),
            Default::default(),
            Clocks::wall(),
        );
        // Small merge batches and cap: the cap is checked at batch boundaries and its count lags up to 4 batches.
        cx.merge_chunk_bytes = 512;
        cx.max_postings = 50;
        let s = compact_project(&cx, &pid("s")).await.unwrap();
        assert!(s.failed.is_empty() && !s.capped, "{s:?}");
        let rotated: u32 = s.jobs.iter().map(|j| j.stats.rotated).sum();
        assert!(rotated > 0, "precondition: the posting cap rotated outputs");
        let o = GcOptions {
            gc_grace: Duration::ZERO,
            orphan_grace: Duration::ZERO,
            sweep_blobs: true,
            dry_run: false,
        };
        let now = forge_model::time::now_ms() + 1;
        run_gc(
            self.meta.as_ref(),
            self.store.object_store().as_ref(),
            &Clocks::at(now, now),
            &o,
            &BlobGuard::default(),
        )
        .await
        .unwrap();
        let segs = self.meta.list_segments(&SegmentFilter::default()).await.unwrap();
        assert!(segs.iter().all(|s| s.level == Level::L1 && s.fts_byte_size.is_some()));
        let mut per_hour: BTreeMap<HourBucket, usize> = BTreeMap::new();
        for s in &segs {
            *per_hour.entry(s.partition_hour).or_default() += 1;
        }
        assert!(
            per_hour.values().any(|n| *n > 1),
            "precondition: several L1s in one hour"
        );
    }
}

fn keys(batches: &[RecordBatch]) -> BTreeSet<Key> {
    batches
        .iter()
        .flat_map(|b| {
            let t = b.column(0).as_binary::<i32>();
            let s = b.column(1).as_binary::<i32>();
            (0..b.num_rows())
                .map(|i| {
                    (
                        TraceId::from_slice(t.value(i)).unwrap(),
                        SpanId::from_slice(s.value(i)).unwrap(),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Per predicate: (non-empty, not every row) over this layout.
async fn check_sql(e: &QueryEngine, merged: &[SpanRow], ctx: &str) -> Vec<(bool, bool)> {
    let mut cover = Vec::new();
    for p in preds() {
        let want: BTreeSet<Key> = merged
            .iter()
            .filter(|r| p.oracle(r))
            .map(|r| (r.trace_id, r.span_id))
            .collect();
        for r in merged {
            assert_eq!(p.matcher(r), p.oracle(r), "{ctx}: matcher != oracle for {p:?} on {r:?}");
        }
        let q = format!(
            "SELECT trace_id, span_id FROM spans WHERE project_id = 's' AND {}",
            p.sql()
        );
        let got = keys(&e.sql_batches(&q).await.unwrap());
        assert_eq!(got, want, "{ctx}: {}", p.sql());
        let q = format!(
            "SELECT trace_id, span_id FROM spans WHERE project_id = 's' AND CASE WHEN {} THEN true ELSE false END",
            p.sql()
        );
        assert_eq!(keys(&e.sql_batches(&q).await.unwrap()), want, "{ctx}: CASE {}", p.sql());
        let q = format!("SELECT count(*) FROM spans WHERE project_id = 's' AND {}", p.sql());
        let b = e.sql_batches(&q).await.unwrap();
        let n = b[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(n as usize, want.len(), "{ctx}: count {}", p.sql());
        cover.push((!want.is_empty(), want.len() < merged.len()));
    }
    cover
}

#[derive(Clone, Debug)]
struct Req {
    q: &'static str,
    columns: Option<Vec<&'static str>>,
    json: Option<(FtsColumn, &'static str, &'static str)>,
}

fn reqs() -> Vec<Req> {
    let r = |q, columns: Option<Vec<&'static str>>| Req { q, columns, json: None };
    vec![
        r("refund", None),
        r("span-1", Some(vec!["name"])),
        r("café", None),
        r("\"refund order\"", Some(vec!["inputs"])),
        r("order", Some(vec!["outputs"])),
        r("zanzibar", None),
        r("日本 OR span-12*", None),
        Req {
            q: "refund",
            columns: Some(vec!["inputs"]),
            json: Some((FtsColumn::Metadata, "$.user", "u1")),
        },
    ]
}

/// The seeded filter variant of one layout.
#[derive(Clone, Copy, Debug)]
enum Variant {
    Window(i64, i64),
    RunType,
    Status,
    Roots,
    None,
}

fn rfc(ns: i64) -> String {
    chrono::DateTime::from_timestamp_nanos(ns).to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

fn request(req: &Req, v: Variant, limit: usize, cursor: Option<String>) -> SearchRequest {
    let mut f = SearchFilters {
        json: req.json.map(|(c, p, t)| {
            vec![JsonFilter {
                column: c.name().into(),
                path: p.into(),
                text: t.into(),
            }]
        }),
        ..Default::default()
    };
    match v {
        Variant::Window(lo, hi) => {
            f.start = Some(rfc(lo));
            f.end = Some(rfc(hi));
        }
        Variant::RunType => f.run_type = Some(vec!["llm".into(), "tool".into()]),
        Variant::Status => f.status = Some(vec!["ok".into()]),
        Variant::Roots => f.roots_only = Some(true),
        Variant::None => {}
    }
    let body = SearchBody {
        q: req.q.into(),
        columns: req.columns.as_ref().map(|c| c.iter().map(|c| c.to_string()).collect()),
        filters: Some(f),
        limit: Some(limit),
        cursor,
        ..Default::default()
    };
    SearchRequest::parse(body, pid("s")).unwrap()
}

fn oracle_hits(merged: &[SpanRow], req: &Req, v: Variant) -> Vec<Pos> {
    let cols: Vec<FtsColumn> = match &req.columns {
        Some(c) => c.iter().map(|c| FtsColumn::from_name(c).unwrap()).collect(),
        None => FtsColumn::ALL.to_vec(),
    };
    let mut out: Vec<Pos> = merged
        .iter()
        .filter(|r| cols.iter().any(|c| oracle_search(r, *c, req.q)))
        .filter(|r| req.json.is_none_or(|(c, p, t)| oracle_jks(r, c, p, t)))
        .filter(|r| match v {
            Variant::Window(lo, hi) => r.start_time_ns.is_some_and(|s| s >= lo && s < hi),
            Variant::RunType => matches!(r.run_type, Some(RunType::Llm | RunType::Tool)),
            Variant::Status => r.status == Some(SpanStatus::Ok),
            Variant::Roots => r.parent_span_id.is_none(),
            Variant::None => true,
        })
        .filter_map(|r| Some((r.start_time_ns?, r.trace_id, r.span_id)))
        .collect();
    out.sort_by(|a, b| b.cmp(a));
    out
}

/// Pages until the cursor is null (at most `max_pages`); checks the page bound.
async fn pages(e: &QueryEngine, req: &Req, v: Variant, limit: usize, max_pages: usize) -> Vec<Pos> {
    let mut out = Vec::new();
    let mut cursor = None;
    let mut n = 0;
    loop {
        let r = request(req, v, limit, cursor.take());
        let page = e.search(&r).await.unwrap();
        n += 1;
        let b = &page.rows;
        let s = b.schema();
        let st = b
            .column(s.index_of("start_time").unwrap())
            .as_primitive::<arrow::datatypes::TimestampNanosecondType>();
        let t = b.column(s.index_of("trace_id").unwrap()).as_binary::<i32>();
        let sp = b.column(s.index_of("span_id").unwrap()).as_binary::<i32>();
        assert!(b.num_rows() <= limit);
        for i in 0..b.num_rows() {
            out.push((
                st.value(i),
                TraceId::from_slice(t.value(i)).unwrap(),
                SpanId::from_slice(sp.value(i)).unwrap(),
            ));
        }
        match page.next_cursor {
            Some(c) if n < max_pages => cursor = Some(r.encode_cursor(&c)),
            Some(_) => return out,
            None => break,
        }
    }
    assert!(n <= out.len().div_ceil(limit) + 1, "{n} pages for {} hits", out.len());
    out
}

fn delta(a: FtsCountersSnapshot, b: FtsCountersSnapshot) -> FtsCountersSnapshot {
    FtsCountersSnapshot {
        index_parts: b.index_parts - a.index_parts,
        brute_parts: b.brute_parts - a.brute_parts,
        exact_rows: b.exact_rows - a.exact_rows,
        merge_rows: b.merge_rows - a.merge_rows,
        segments_read: b.segments_read - a.segments_read,
        unpositioned: b.unpositioned - a.unpositioned,
    }
}

fn sum(a: FtsCountersSnapshot, b: FtsCountersSnapshot) -> FtsCountersSnapshot {
    FtsCountersSnapshot {
        index_parts: a.index_parts + b.index_parts,
        brute_parts: a.brute_parts + b.brute_parts,
        exact_rows: a.exact_rows + b.exact_rows,
        merge_rows: a.merge_rows + b.merge_rows,
        segments_read: a.segments_read + b.segments_read,
        unpositioned: a.unpositioned + b.unpositioned,
    }
}

struct Outcome {
    cover: Vec<(bool, bool)>,
    counters: FtsCountersSnapshot,
    cross_source_and: bool,
}

/// One layout: SQL and search checks before and after compact + gc. `light` (the random cases): one engine size,
/// limit 50, the seeded filter variant only.
async fn run(seed: u64, light: bool) -> Outcome {
    let fx = Fx::build(fts_layout(seed)).await;
    let merged = reference(&fx.all);
    let mut r = Rng(seed ^ 0x5EED | 1);
    let variant = match r.below(5) {
        0 => {
            let lo = at(2 + r.below(8) as usize, 0);
            Variant::Window(lo, lo + 9 * HOUR)
        }
        1 => Variant::RunType,
        2 => Variant::Status,
        3 => Variant::Roots,
        _ => Variant::None,
    };
    // (e): refund AND order holds on the merged row, but on no single version row.
    let cross_source_and = merged.iter().any(|m| {
        m.trace_id == tid(T_E)
            && oracle_search(m, FtsColumn::Inputs, "refund")
            && oracle_search(m, FtsColumn::Outputs, "order")
    }) && !fx.all.iter().any(|v| {
        v.trace_id == tid(T_E)
            && oracle_search(v, FtsColumn::Inputs, "refund")
            && oracle_search(v, FtsColumn::Outputs, "order")
    });
    let mut cover = vec![(false, false); preds().len()];
    let mut counters = FtsCountersSnapshot::default();
    for phase in ["L0", "L1"] {
        if phase == "L1" {
            fx.compact().await;
        }
        fx.hide_third(seed).await;
        let ctx = format!("seed {seed} {phase}");
        for hidden in [false, true] {
            let e = fx.engine(hidden, (8, 64));
            let c0 = e.fts_counters();
            for (acc, c) in cover
                .iter_mut()
                .zip(check_sql(&e, &merged, &format!("{ctx} hidden {hidden}")).await)
            {
                acc.0 |= c.0;
                acc.1 |= c.1;
            }
            counters = sum(counters, delta(c0, e.fts_counters()));
        }
        let engines: &[((usize, usize), bool)] = if light {
            &[((2, 2), true)]
        } else {
            &[((1, 1), true), ((2, 2), false), ((8, 64), true)]
        };
        let variants: &[Variant] = if light { &[variant] } else { &[Variant::None, variant] };
        for &(sizes, hidden) in engines {
            let e = fx.engine(hidden, sizes);
            let c0 = e.fts_counters();
            for req in reqs() {
                for &v in variants {
                    let want = oracle_hits(&merged, &req, v);
                    let ctx = format!("{ctx} sizes {sizes:?} hidden {hidden} {req:?} {v:?}");
                    if !light {
                        let got = pages(&e, &req, v, 1, 5).await;
                        assert_eq!(got, want[..want.len().min(5)], "{ctx} limit 1");
                    }
                    // Paged to the end at 3 once per layout phase (search_diff pages small limits on 432 cases).
                    let limits: &[usize] = if sizes == (2, 2) && !light { &[3, 50] } else { &[50] };
                    for &limit in limits {
                        assert_eq!(pages(&e, &req, v, limit, usize::MAX).await, want, "{ctx} limit {limit}");
                    }
                }
            }
            counters = sum(counters, delta(c0, e.fts_counters()));
        }
        // (h): page 1 of `zanzibar` holds the start-less hit and reads the first group plus the start-less-hit segment.
        let e = fx.engine(false, (8, 64));
        let req = Req {
            q: "zanzibar",
            columns: None,
            json: None,
        };
        let c0 = e.fts_counters();
        let page1 = pages(&e, &req, Variant::None, 3, 1).await;
        let d = delta(c0, e.fts_counters());
        let h = merged
            .iter()
            .find(|m| m.trace_id == tid(T_H))
            .map(|m| (m.start_time_ns.unwrap(), m.trace_id, m.span_id))
            .unwrap();
        assert_eq!(
            page1.first(),
            Some(&h),
            "seed {seed} {phase}: the start-less hit ranks first"
        );
        assert!(d.segments_read <= 8 + 1, "seed {seed} {phase}: {d:?}");
        // `filler` has one hit, with a start, in every segment (most also hold start-less rows): page 1 reads only
        // the newest group, so a null-start segment is not +inf once its hits are known to be start-less-free.
        let req = Req {
            q: "filler",
            columns: Some(vec!["inputs"]),
            json: None,
        };
        let c0 = e.fts_counters();
        let page1 = pages(&e, &req, Variant::None, 3, 1).await;
        let d = delta(c0, e.fts_counters());
        assert_eq!(
            page1,
            oracle_hits(&merged, &req, Variant::None)[..3],
            "seed {seed} {phase}"
        );
        assert!(d.segments_read <= 8, "seed {seed} {phase}: {d:?}");
    }
    assert!(counters.exact_rows > 0, "seed {seed}: {counters:?}");
    Outcome {
        cover,
        counters,
        cross_source_and,
    }
}

fn assert_coverage(outs: &[Outcome]) {
    let total = outs
        .iter()
        .fold(FtsCountersSnapshot::default(), |a, o| sum(a, o.counters));
    assert!(
        total.merge_rows > 0 && total.index_parts > 0 && total.brute_parts > 0 && total.unpositioned > 0,
        "{total:?}"
    );
    assert!(outs.iter().any(|o| o.cross_source_and), "(e) holds for some seed");
    for (i, p) in preds().iter().enumerate() {
        assert!(outs.iter().any(|o| o.cover[i].0), "{p:?} is empty for every seed");
        assert!(outs.iter().any(|o| o.cover[i].1), "{p:?} holds on every row");
    }
}

macro_rules! seed_tests {
    ($($name:ident: $seed:expr,)*) => {$(
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() {
            assert_coverage(&[run($seed, false).await]);
        }
    )*};
}

seed_tests! {
    fts_diff_seed_1: 1,
    fts_diff_seed_2: 2,
    fts_diff_seed_3: 3,
    fts_diff_seed_4: 4,
    fts_diff_seed_5: 5,
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(8))]
    #[test]
    fn fts_diff_random(seed in 6u64..1_000_000) {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        rt.block_on(async { assert!(run(seed, true).await.counters.exact_rows > 0) });
    }
}
