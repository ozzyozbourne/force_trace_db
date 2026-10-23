//! E3 (plans/phase-5 §6): newest-50 `/v1/search` over 200 L0 segments x 8,000 spans (with start-less end posts in
//! the next segment) plus a ~64 MiB memtable, through a `LatencyStore` that adds a delay to every read request to
//! model S3. Asserted at 75 ms; 0 / 50 / 100 ms are reported.
//!
//! This test REPLACES the spec's M4 exit ("newest-50 search <= 500 ms p50 on 100 M spans, 1 TB, S3"), which this
//! machine cannot run. It does not prove: 100 M spans / 1 TB (60x fewer spans, 8k-span L0s instead of 256 MiB
//! L1s); real S3 variance and bandwidth (InMemory returns 1 MB as fast as 1 KB); needle terms over a 30-day window
//! (about ceil(N/64) x 1-2 round trips); non-ASCII memtable payloads; many concurrent queries.
//!
//! cargo test -p forge-query --profile quick-release --test fts_perf -- --ignored --nocapture
#![allow(clippy::print_stderr)]
use forge_meta::Metastore;
use forge_model::MemtableView;
use forge_model::catalog::HourBucket;
use forge_model::config::QueryConfig;
use forge_model::ids::{SpanId, TraceId};
use forge_model::row::SpanRow;
use forge_model::testing::{FixedMemtable, batch_from_rows, pid};
use forge_query::QueryEngine;
use forge_query::search::{SearchBody, SearchRequest};
use forge_query::testing::{RecordingSegmentStore, write_flush};
use forge_storage::testing::LatencyStore;
use forge_storage::{BuiltStore, SegmentStore, VortexSegmentStore};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

const BASE: i64 = 1_700_000_000_000_000_000;
const HOUR: i64 = 3_600_000_000_000;
const SEGS: u64 = 200;
const TRACES: u64 = 400;
const SPANS: u64 = 20;
/// Memtable: 1,000 batches of 100 spans (~64 MiB).
const MEM_BATCHES: u64 = 1000;
const K: usize = 50;
const TIMED: usize = 10;

type Pos = (i64, TraceId, SpanId);

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Self(0x9E37_79B9_7F4A_7C15 ^ seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn ids(seg: u64, t: u64, s: u64) -> (TraceId, SpanId) {
    let tr = u128::from(seg * TRACES + t);
    (TraceId((tr | 0xA5 << 120).to_be_bytes()), SpanId((s + 1).to_be_bytes()))
}

/// Odd traces get an end post (no start, in the next segment) on ~20% of their spans; even traces never do.
fn end_post(seg: u64, t: u64, s: u64) -> bool {
    t % 2 == 1 && Rng::new(seg << 32 | t << 8 | s).below(5) == 0
}

fn inputs(r: &mut Rng, extra: &[String]) -> String {
    let mut words: Vec<String> = (0..80).map(|_| format!("w{}", r.below(20_000))).collect();
    words.extend_from_slice(extra);
    serde_json::json!({"messages":[{"role":"user","content":words.join(" ")}]}).to_string()
}

/// Segment `seg`: its own start posts (hour `seg`) plus the end posts of segment `seg - 1`'s spans. Marker spans are
/// recorded in `marks`.
fn segment_rows(seg: u64, v: &mut u64, marks: &mut HashMap<String, Vec<Pos>>) -> Vec<SpanRow> {
    let mut r = Rng::new(seg + 1);
    let mut out = Vec::with_capacity((TRACES * SPANS * 11 / 10) as usize);
    for t in 0..TRACES {
        for s in 0..SPANS {
            let (trace_id, span_id) = ids(seg, t, s);
            let start = BASE + seg as i64 * HOUR + (t * SPANS + s) as i64 * 100_000_000;
            let mut extra = Vec::new();
            // Rare: each zebracornN in 1 of 1,000 spans, only in traces without end posts (single-source).
            if t % 2 == 0 && r.below(50) == 0 {
                extra.push(format!("zebracorn{}", r.below(10)));
            }
            for i in 0..10 {
                if r.below(5) == 0 {
                    extra.push(format!("refund{i}"));
                }
            }
            if seg < 10 && r.below(100) == 0 {
                extra.push(format!("kumquat{}", r.below(10)));
            }
            for m in &extra {
                marks.entry(m.clone()).or_default().push((start, trace_id, span_id));
            }
            *v += 1;
            out.push(SpanRow {
                project_id: Some(pid("p")),
                trace_id,
                span_id,
                parent_span_id: (s > 0).then(|| ids(seg, t, 0).1),
                name: Some(format!("step-{s}")),
                start_time_ns: Some(start),
                inputs: Some(inputs(&mut r, &extra)),
                metadata: Some(format!("{{\"user\":\"u{}\"}}", t % 50)),
                version: *v,
                ..Default::default()
            });
        }
    }
    if seg > 0 {
        for t in 0..TRACES {
            for s in (0..SPANS).filter(|s| end_post(seg - 1, t, *s)) {
                let (trace_id, span_id) = ids(seg - 1, t, s);
                *v += 1;
                out.push(SpanRow {
                    project_id: Some(pid("p")),
                    trace_id,
                    span_id,
                    end_time_ns: Some(BASE + seg as i64 * HOUR),
                    outputs: Some(format!(
                        "{{\"text\":\"answer w{} w{}\"}}",
                        r.below(20_000),
                        r.below(20_000)
                    )),
                    version: *v,
                    ..Default::default()
                });
            }
        }
    }
    out
}

/// The memtable: fresh traces (hour SEGS), the same payload generator. Markers are 100x rarer than in segments
/// (about 10-20 hits per term), so page 1 needs segment hits too.
fn memtable(v: &mut u64, marks: &mut HashMap<String, Vec<Pos>>) -> Vec<arrow::record_batch::RecordBatch> {
    let mut r = Rng::new(0xFEED);
    (0..MEM_BATCHES)
        .map(|b| {
            let rows: Vec<SpanRow> = (0..100u64)
                .map(|i| {
                    let n = b * 100 + i;
                    let (trace_id, span_id) = ids(SEGS, n / SPANS, n % SPANS);
                    let start = BASE + SEGS as i64 * HOUR + n as i64 * 10_000_000;
                    let mut extra = Vec::new();
                    if r.below(1000) == 0 {
                        extra.push(format!("zebracorn{}", r.below(10)));
                    }
                    if r.below(500) == 0 {
                        extra.push(format!("refund{}", r.below(10)));
                    }
                    for m in &extra {
                        marks.entry(m.clone()).or_default().push((start, trace_id, span_id));
                    }
                    *v += 1;
                    SpanRow {
                        project_id: Some(pid("p")),
                        trace_id,
                        span_id,
                        parent_span_id: (n % SPANS > 0).then(|| ids(SEGS, n / SPANS, 0).1),
                        name: Some(format!("step-{}", n % SPANS)),
                        start_time_ns: Some(start),
                        inputs: Some(inputs(&mut r, &extra)),
                        metadata: Some(format!("{{\"user\":\"u{}\"}}", n % 50)),
                        version: *v,
                        ..Default::default()
                    }
                })
                .collect();
            batch_from_rows(&rows)
        })
        .collect()
}

fn request(q: &str) -> SearchRequest {
    SearchRequest::parse(
        SearchBody {
            q: q.into(),
            ..Default::default()
        },
        pid("p"),
    )
    .unwrap()
}

/// (trace_id, span_id) hex of each hit, in page order.
fn hit_keys(json: &[u8]) -> Vec<(String, String)> {
    let v: serde_json::Value = serde_json::from_slice(json).unwrap();
    v["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| {
            (
                h["trace_id"].as_str().unwrap().to_string(),
                h["span_id"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

fn expected(marks: &HashMap<String, Vec<Pos>>, term: &str) -> Vec<(String, String)> {
    let mut v = marks.get(term).cloned().unwrap_or_default();
    v.sort_unstable_by(|a, b| b.cmp(a));
    v.dedup();
    v.into_iter()
        .take(K)
        .map(|(_, t, s)| (hex::encode(t.as_bytes()), hex::encode(s.as_bytes())))
        .collect()
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Rare,
    Common,
    Absent,
    Needle,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Rare => "rare",
            Kind::Common => "common",
            Kind::Absent => "absent",
            Kind::Needle => "needle",
        }
    }
    fn term(self, i: usize) -> String {
        match self {
            Kind::Rare => format!("zebracorn{i}"),
            Kind::Common => format!("refund{i}"),
            // Inside the vocabulary's FST key range, never written.
            Kind::Absent => format!("w{}q", 1234 + i * 1777),
            Kind::Needle => format!("kumquat{i}"),
        }
    }
}

struct Row {
    delay: u64,
    kind: Kind,
    cold: Duration,
    cold_gets: u64,
    cold_waves: u64,
    p50: Duration,
    p95: Duration,
    max: Duration,
    gets: f64,
    waves: f64,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn e3_search_newest_50() {
    let lat = Arc::new(LatencyStore::new(Arc::new(object_store::memory::InMemory::new())));
    let vortex = || -> Arc<dyn SegmentStore> {
        Arc::new(VortexSegmentStore::new(BuiltStore {
            store: lat.clone(),
            local_root: None,
        }))
    };
    let writer = vortex();
    let meta: Arc<dyn Metastore> = Arc::new(forge_meta::SqliteMetastore::open_in_memory().unwrap());
    let t0 = Instant::now();
    let mut marks: HashMap<String, Vec<Pos>> = HashMap::new();
    let mut batches = Vec::new();
    let mut v = 0;
    for seg in 0..SEGS {
        let rows = segment_rows(seg, &mut v, &mut marks);
        batches.push((seg, batch_from_rows(&rows), v));
    }
    let mem = memtable(&mut v, &mut marks);
    let mem_bytes: usize = mem.iter().map(|b| b.get_array_memory_size()).sum();
    eprintln!("generated in {:?} (memtable {} MiB)", t0.elapsed(), mem_bytes >> 20);
    let t1 = Instant::now();
    futures::stream::iter(batches)
        .map(|(seg, b, maxv)| {
            let (meta, store) = (meta.clone(), writer.clone());
            async move {
                let hour = HourBucket::from_nanos(BASE + seg as i64 * HOUR);
                write_flush(&*meta, store, &b, hour, &format!("n{seg}"), 8 << 20, 1, maxv).await
            }
        })
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await;
    let segs = meta.list_segments(&Default::default()).await.unwrap();
    assert_eq!(segs.len() as u64, SEGS);
    assert!(segs.iter().all(|s| s.fts_byte_size.is_some()));
    let nulls = segs.iter().filter(|s| s.start_time_nulls > 0).count();
    assert_eq!(
        nulls as u64,
        SEGS - 1,
        "every segment but the first holds start-less end posts"
    );
    let rows: u64 = segs.iter().map(|s| s.row_count).sum();
    eprintln!(
        "wrote {} segments ({nulls} with null starts), {rows} rows in {:?}",
        segs.len(),
        t1.elapsed()
    );
    // Rare-marker precondition: every marker trace has exactly one location.
    for i in 0..10 {
        for (_, t, _) in &marks[&Kind::Rare.term(i)] {
            let seg_of = u128::from_be_bytes(t.0) & !(0xFF << 120);
            if seg_of < u128::from(SEGS * TRACES) {
                let locs = meta.locate_trace(&pid("p"), *t).await.unwrap();
                assert_eq!(locs.len(), 1, "rare-marker trace {t:?} is not single-location");
            }
        }
    }
    let mem: Arc<dyn MemtableView> = Arc::new(FixedMemtable(mem));

    let mut table = Vec::new();
    let mut failed = Vec::new();
    for delay in [0u64, 50, 75, 100] {
        for kind in [Kind::Rare, Kind::Common, Kind::Absent, Kind::Needle] {
            lat.set_delay(Duration::from_millis(delay));
            // Cold: a fresh segment store and engine (empty Vortex footer and `.fts` caches).
            let rec = Arc::new(RecordingSegmentStore::new(vortex()));
            let e = QueryEngine::new(meta.clone(), rec.clone(), mem.clone(), QueryConfig::default());
            let call = |term: String| {
                let (e, marks) = (&e, &marks);
                async move {
                    let t = Instant::now();
                    let out = e.search_json(&request(&term)).await.unwrap();
                    let d = t.elapsed();
                    let got = hit_keys(&out);
                    assert_eq!(
                        got,
                        expected(marks, &term),
                        "{term}: page 1 != the generator's newest 50"
                    );
                    let n = got.len();
                    match kind {
                        Kind::Rare | Kind::Common => assert_eq!(n, K, "{term}"),
                        Kind::Absent => assert_eq!(n, 0, "{term}"),
                        Kind::Needle => {}
                    }
                    d
                }
            };
            let (g0, w0) = (lat.gets.load(Ordering::SeqCst), lat.waves.load(Ordering::SeqCst));
            let cold = call(kind.term(0)).await;
            let cold_gets = lat.gets.load(Ordering::SeqCst) - g0;
            let cold_waves = lat.waves.load(Ordering::SeqCst) - w0;
            if kind == Kind::Needle {
                table.push(Row {
                    delay,
                    kind,
                    cold,
                    cold_gets,
                    cold_waves,
                    p50: Duration::ZERO,
                    p95: Duration::ZERO,
                    max: Duration::ZERO,
                    gets: 0.0,
                    waves: 0.0,
                });
            } else {
                for i in 1..3 {
                    call(kind.term(i)).await;
                }
                rec.take_projections();
                let (g0, w0) = (lat.gets.load(Ordering::SeqCst), lat.waves.load(Ordering::SeqCst));
                let mut ts = Vec::with_capacity(TIMED);
                for i in 0..TIMED {
                    ts.push(call(kind.term(i)).await);
                }
                let gets = (lat.gets.load(Ordering::SeqCst) - g0) as f64 / TIMED as f64;
                let waves = (lat.waves.load(Ordering::SeqCst) - w0) as f64 / TIMED as f64;
                ts.sort();
                let p = |x: f64| ts[((TIMED as f64 * x) as usize).min(TIMED - 1)];
                if kind == Kind::Rare {
                    let payload = rec
                        .take_projections()
                        .into_iter()
                        .filter(|cols| cols.iter().any(|c| *c == "inputs" || *c == "outputs"))
                        .count();
                    if delay == 75 && payload > 0 {
                        failed.push(format!("rare: {payload} scans projected inputs/outputs"));
                    }
                }
                table.push(Row {
                    delay,
                    kind,
                    cold,
                    cold_gets,
                    cold_waves,
                    p50: p(0.5),
                    p95: p(0.95),
                    max: ts[TIMED - 1],
                    gets,
                    waves,
                });
            }
            let r = table.last().unwrap();
            eprintln!(
                "delay {delay:>3} ms {:>6}: cold {:>9.1?} ({} GETs, {} waves)  p50 {:>9.1?} p95 {:>9.1?} max {:>9.1?}  \
                 {:.1} GETs/call, {:.1} waves/call",
                kind.name(),
                r.cold,
                r.cold_gets,
                r.cold_waves,
                r.p50,
                r.p95,
                r.max,
                r.gets,
                r.waves
            );
            if delay == 75 {
                let name = kind.name();
                if r.cold >= Duration::from_millis(1500) || r.cold_waves > 8 {
                    failed.push(format!("{name}: cold {:?}, {} waves", r.cold, r.cold_waves));
                }
                if matches!(kind, Kind::Absent | Kind::Needle) && r.cold_gets == 0 {
                    failed.push(format!("{name}: cold call made no GETs"));
                }
                if kind != Kind::Needle && r.p50 >= Duration::from_millis(500) {
                    failed.push(format!("{name}: p50 {:?}", r.p50));
                }
                let max_waves = if kind == Kind::Absent { 2.0 } else { 5.0 };
                if kind != Kind::Needle && r.waves > max_waves {
                    failed.push(format!("{name}: {:.1} waves/call > {max_waves}", r.waves));
                }
            }
        }
    }
    let p50 = |d: u64, k: Kind| {
        table
            .iter()
            .find(|r| r.delay == d && r.kind == k)
            .map_or(0.0, |r| r.p50.as_secs_f64())
    };
    for k in [Kind::Rare, Kind::Common, Kind::Absent] {
        eprintln!(
            "{:>6}: effective round trips at 75 ms = {:.1}",
            k.name(),
            (p50(75, k) - p50(0, k)) / 0.075
        );
    }
    assert!(failed.is_empty(), "E3 at 75 ms/request: {failed:?}");
}
