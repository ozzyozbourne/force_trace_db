//! Temporary validation test (p3-validate branch only).
use arrow::array::RecordBatch;
use forge_meta::{FlushCommit, Metastore, NewSegment};
use forge_model::catalog::{HourBucket, Level, SegmentId};
use forge_model::config::{QueryConfig, StorageConfig};
use forge_model::enums::{RunType, SpanStatus};
use forge_model::error::{Classify, ErrorKind};
use forge_model::ids::ProjectId;
use forge_model::merge::{Tombstones, merge_versions_chunked, split_chunks_by_project};
use forge_model::row::SpanRow;
use forge_model::schema::spans_schema;
use forge_model::testing::{FixedMemtable, Rng, batch_from_rows, pid, random_row, reference_merge, rows_from_batch, sid, tid};
use forge_query::runs::RunsRequest;
use forge_query::sql::SqlRequest;
use forge_query::trace::TraceRequest;
use forge_query::{QueryEngine, QueryError};
use forge_storage::{SegmentSpec, SegmentStore, VortexSegmentStore, build_object_store};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

const BASE: i64 = 1_700_000_000_000_000_000;

fn start_of(t: u64, s: u64) -> i64 {
    BASE + ((t * 7919 + s * 104_729) % 100_000) as i64 * 1_000_000 + (s as i64 % 3) * 1_000
}

/// Random posts with immutable start_time and parents inside the trace.
fn gen_rows(seed: u64, n: usize) -> Vec<SpanRow> {
    let mut r = Rng(seed);
    let mut rows = Vec::new();
    for v in 1..=n as u64 {
        let p = ["a", "b", "c"][r.below(3) as usize];
        let (t, s) = (r.below(4), r.below(6));
        let mut x = random_row(&mut r, p, t, s, v);
        x.start_time_ns = x.start_time_ns.map(|_| start_of(t, s));
        x.end_time_ns = x.end_time_ns.map(|_| start_of(t, s) + 5_000_123);
        x.parent_span_id = if s == 0 { None } else { { let k = r.below(s); r.opt(50, sid(k)) } };
        x.run_type = { let k = r.below(3) as usize; r.opt(40, [RunType::Llm, RunType::Chain, RunType::Tool][k]) };
        x.status = { let k = r.below(3) as usize; r.opt(40, [SpanStatus::Ok, SpanStatus::Error, SpanStatus::Unset][k]) };
        rows.push(x);
    }
    rows
}

struct Fx {
    engine: QueryEngine,
    all: Vec<SpanRow>,
}

async fn fixture(seed: u64, cfg: QueryConfig) -> Fx {
    let all = gen_rows(seed, 240);
    let meta: Arc<dyn Metastore> = Arc::new(forge_meta::SqliteMetastore::open_in_memory().unwrap());
    let st = VortexSegmentStore::new(build_object_store(&StorageConfig { url: "memory://".into() }).unwrap()).with_footer_cache(1 << 20);
    let store: Arc<dyn SegmentStore> = Arc::new(st);
    let cuts = [0, 72, 144, 204];
    for f in 0..3 {
        let part = &all[cuts[f]..cuts[f + 1]];
        let merged = merge_versions_chunked(&[batch_from_rows(part)], Tombstones::Keep, 4096).unwrap();
        let mut segs = Vec::new();
        for (p, batches) in split_chunks_by_project(&merged).unwrap() {
            let spec = SegmentSpec { id: SegmentId::new(), project_id: p, level: Level::L0, partition_hour: HourBucket(472_222) };
            let s = futures::stream::iter(batches.into_iter().map(Ok));
            let w = store.write_segment(spec, Box::pin(s)).await.unwrap();
            segs.push(NewSegment { meta: w.meta, traces: w.traces });
        }
        meta.commit_flush(FlushCommit { node_id: "n".into(), segments: segs, flushed_wal_seq: f as u64 + 1, max_version: cuts[f + 1] as u64 })
            .await
            .unwrap();
    }
    // memtable: the tail + a re-copy of flush 3's raw rows (snapshot window)
    let mem = FixedMemtable(vec![batch_from_rows(&all[144..204]), batch_from_rows(&all[204..])]);
    Fx { engine: QueryEngine::new(meta, store, Arc::new(mem), cfg), all }
}

fn effective(mut r: SpanRow) -> SpanRow {
    r.total_tokens = r.total_tokens.or(match (r.prompt_tokens, r.completion_tokens) {
        (Some(a), Some(b)) => Some(a + b),
        (a, b) => a.or(b),
    });
    r
}

fn reference(all: &[SpanRow]) -> Vec<SpanRow> {
    let mut v: Vec<SpanRow> = reference_merge(all).into_iter().filter(|r| !r.deleted).map(effective).collect();
    v.sort_by_key(|r| (r.project_id.clone(), r.trace_id, r.span_id));
    v
}

fn to_rows(bs: &[RecordBatch]) -> Vec<SpanRow> {
    let mut v: Vec<SpanRow> = bs
        .iter()
        .flat_map(|b| {
            let b = RecordBatch::try_new(spans_schema(), b.columns().to_vec()).expect("spans-shaped");
            rows_from_batch(&b).unwrap()
        })
        .collect();
    v.sort_by_key(|r| (r.project_id.clone(), r.trace_id, r.span_id));
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_matches_reference() {
    for seed in 1..=4 {
        let mut fx = fixture(seed, QueryConfig::default()).await;
        let want = reference(&fx.all);
        for tp in [1, 2, 4, 8] {
            fx.engine.target_partitions = tp;
            let got = to_rows(&fx.engine.sql_batches("SELECT * FROM spans").await.unwrap());
            assert_eq!(got.len(), want.len(), "seed {seed} tp {tp}");
            assert_eq!(got, want, "seed {seed} tp {tp}");
            let q = format!("SELECT * FROM spans WHERE project_id = 'a' AND trace_id = X'{}'", tid(1));
            let got = to_rows(&fx.engine.sql_batches(&q).await.unwrap());
            let w: Vec<_> = want.iter().filter(|r| r.project_id == Some(pid("a")) && r.trace_id == tid(1)).cloned().collect();
            assert_eq!(got, w);
            let got = to_rows(&fx.engine.sql_batches("SELECT * FROM spans WHERE status = 'error'").await.unwrap());
            let w: Vec<_> = want.iter().filter(|r| r.status == Some(SpanStatus::Error)).cloned().collect();
            assert_eq!(got, w);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explain_has_no_payloads_and_filters_above_merge() {
    let fx = fixture(3, QueryConfig::default()).await;
    let b = fx.engine.sql_batches("EXPLAIN SELECT name FROM spans WHERE status = 'ok'").await.unwrap();
    let txt = arrow::util::pretty::pretty_format_batches(&b).unwrap().to_string();
    eprintln!("{txt}");
    for p in ["inputs", "outputs", "events", "attributes"] {
        assert!(!txt.contains(p), "{p} in plan");
    }
    assert!(txt.contains("MergeVersionsExec"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trace_fast_path_matches_reference_and_json() {
    for seed in 1..=4 {
        let fx = fixture(seed, QueryConfig::default()).await;
        let want = reference(&fx.all);
        let keys: HashSet<(ProjectId, u64)> = fx.all.iter().map(|r| (r.project_id.clone().unwrap(), (0..4).find(|t| tid(*t) == r.trace_id).unwrap())).collect();
        for (p, t) in keys {
            let w: Vec<SpanRow> = want.iter().filter(|r| r.project_id.as_ref() == Some(&p) && r.trace_id == tid(t)).cloned().collect();
            let req = TraceRequest::parse(&tid(t).to_string().to_uppercase(), p.clone(), Some(true), None).unwrap();
            match fx.engine.load_trace(&req).await {
                Err(QueryError::NotFound(_)) => assert!(w.is_empty()),
                Err(e) => panic!("{e}"),
                Ok(lt) => {
                    // rows lack dotted_order: compare without it
                    let mut got = lt.rows.clone();
                    let mut cols = got.columns().to_vec();
                    cols.insert(4, arrow::array::new_null_array(&arrow::datatypes::DataType::Utf8, got.num_rows()));
                    got = RecordBatch::try_new(spans_schema(), cols).unwrap();
                    let wn: Vec<SpanRow> = w.iter().cloned().map(|mut r| { r.dotted_order = None; r }).collect();
                    assert_eq!(to_rows(&[got]), wn, "seed {seed} {p} {t}");
                    assert_eq!(lt.tree.preorder.len(), w.len());
                    let rows: HashSet<u32> = lt.tree.preorder.iter().map(|x| x.0).collect();
                    assert_eq!(rows.len(), w.len(), "each span once");
                    for flat in [None, Some("flat")] {
                        let req = TraceRequest::parse(&tid(t).to_string(), p.clone(), Some(false), flat).unwrap();
                        let j = fx.engine.trace_json(&req).await.unwrap();
                        let v: serde_json::Value = serde_json::from_slice(&j).unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&j)));
                        assert_eq!(v["span_count"], w.len());
                        let s = String::from_utf8_lossy(&j);
                        assert!(!s.contains("\"inputs\""));
                        // count nested
                        fn count(v: &serde_json::Value) -> usize { v.as_array().unwrap().iter().map(|n| 1 + count(&n["children"])).sum() }
                        let n = if flat.is_some() { v["spans"].as_array().unwrap().len() } else { count(&v["roots"]) };
                        assert_eq!(n, w.len());
                        if let Some(ts) = s.split("\"start_time\":\"").nth(1) {
                            let ts = &ts[..ts.find('"').unwrap()];
                            assert_eq!(ts.len(), "2023-11-14T22:13:20.000000000Z".len(), "{ts}");
                        }
                    }
                }
            }
        }
        let req = TraceRequest::parse("ffffffffffffffffffffffffffffffff", pid("a"), None, None).unwrap();
        assert_eq!(fx.engine.load_trace(&req).await.err().unwrap().kind(), ErrorKind::NotFound);
        let uuid = "a0000000-0000-0000-0000-000000000001";
        assert!(TraceRequest::parse(uuid, pid("a"), None, None).is_ok());
    }
}

fn ref_runs(want: &[SpanRow], r: &RunsRequest) -> Vec<(i64, [u8; 16], [u8; 8])> {
    let mut v: Vec<_> = want
        .iter()
        .filter(|x| x.project_id.as_ref() == Some(&r.project) && x.parent_span_id.is_none())
        .filter_map(|x| x.start_time_ns.map(|s| (s, x)))
        .filter(|(s, _)| r.start_ns.is_none_or(|a| *s >= a) && r.end_ns.is_none_or(|b| *s < b))
        .filter(|(_, x)| r.run_types.is_empty() || x.run_type.is_some_and(|t| r.run_types.contains(&t)))
        .filter(|(_, x)| r.statuses.is_empty() || x.status.is_some_and(|t| r.statuses.contains(&t)))
        .map(|(s, x)| (s, x.trace_id.0, x.span_id.0))
        .collect();
    v.sort();
    v.reverse();
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runs_paging_matches_reference() {
    for seed in 1..=6 {
        let fx = fixture(seed, QueryConfig::default()).await;
        let want = reference(&fx.all);
        for p in ["a", "b", "c"] {
            for limit in [1, 3, 50] {
                for budget in [None, Some((1usize, Duration::from_secs(10)))] {
                    for statuses in [HashSet::new(), HashSet::from([SpanStatus::Error])] {
                        let mut req = RunsRequest {
                            project: pid(p), start_ns: None, end_ns: None, run_types: HashSet::new(), statuses: statuses.clone(),
                            q: None, limit, cursor: None, budget,
                        };
                        let exp = ref_runs(&want, &req);
                        let mut got = Vec::new();
                        for _ in 0..1000 {
                            let page = fx.engine.list_runs(&req).await.unwrap();
                            assert!(page.rows.num_rows() <= limit);
                            got.extend(page_keys(&page.rows));
                            match page.next_cursor {
                                Some(c) => {
                                    let h = req.filter_hash();
                                    req.cursor = Some(forge_query::runs::Cursor::decode(&c.encode(h), h).unwrap());
                                }
                                None => break,
                            }
                        }
                        assert_eq!(got, exp, "seed {seed} p {p} limit {limit} budget {budget:?} st {statuses:?}");
                    }
                }
            }
        }
        let j = fx.engine.runs_json(&RunsRequest { project: pid("a"), start_ns: None, end_ns: None, run_types: HashSet::new(), statuses: HashSet::new(), q: None, limit: 2, cursor: None, budget: None }).await.unwrap();
        let _: serde_json::Value = serde_json::from_slice(&j).unwrap();
    }
}

fn pad(b: &RecordBatch) -> RecordBatch {
    let full = spans_schema();
    let cols = full
        .fields()
        .iter()
        .map(|f| match b.schema().index_of(f.name()) {
            Ok(i) => b.column(i).clone(),
            Err(_) => arrow::array::new_null_array(f.data_type(), b.num_rows()),
        })
        .collect();
    RecordBatch::try_new(full, cols).unwrap()
}

fn page_keys(b: &RecordBatch) -> Vec<(i64, [u8; 16], [u8; 8])> {
    rows_from_batch(&pad(b)).unwrap().into_iter().map(|r| (r.start_time_ns.unwrap(), r.trace_id.0, r.span_id.0)).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_envelope_and_limits() {
    let fx = fixture(2, QueryConfig::default()).await;
    let req = |q: &str, max: Option<usize>| SqlRequest { query: q.into(), max_rows: max, timeout_ms: None };
    let j = fx.engine.sql_json(&req("SELECT trace_id, start_time, tags, metadata, total_tokens FROM spans ORDER BY trace_id", None)).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&j).unwrap();
    eprintln!("{}", &serde_json::to_string(&v["columns"]).unwrap());
    eprintln!("{}", serde_json::to_string(&v["rows"][0]).unwrap());
    assert!(v["rows"][0]["trace_id"].as_str().unwrap().len() == 32);
    assert_eq!(v["truncated"], false);
    let j = fx.engine.sql_json(&req("SELECT * FROM spans", Some(1))).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&j).unwrap();
    assert_eq!((v["row_count"].as_u64(), v["truncated"].as_bool()), (Some(1), Some(true)));
    let j = fx.engine.sql_json(&req("SELECT 1 AS x WHERE false", None)).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&j).unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&j)));
    eprintln!("empty: {v}");
    for bad in ["CREATE TABLE x AS SELECT 1", "DROP TABLE spans", "SET datafusion.execution.batch_size = 1", "SELEC 1", "SELECT nope FROM spans"] {
        let e = fx.engine.sql_json(&req(bad, None)).await.unwrap_err();
        assert_eq!(e.kind(), ErrorKind::InvalidInput, "{bad}: {e}");
    }
    assert_eq!(fx.engine.sql_json(&req("SELECT 1", Some(100_001))).await.unwrap_err().kind(), ErrorKind::InvalidInput);

    // memory limit
    let small = fixture(2, QueryConfig { memory_limit: bytesize::ByteSize::kib(16), ..Default::default() }).await;
    let e = small.engine.sql_json(&req("SELECT * FROM spans ORDER BY name", None)).await.unwrap_err();
    eprintln!("mem: kind={:?} msg={}", e.kind(), e.message());
    assert_eq!(e.kind(), ErrorKind::InvalidInput);
    assert!(e.message().contains("memory limit"));

    // timeout
    let t = fixture(2, QueryConfig { timeout: Duration::from_millis(200), ..Default::default() }).await;
    let t0 = std::time::Instant::now();
    let e = t.engine.sql_json(&req("SELECT count(*) FROM spans a, spans b, spans c, spans d", None)).await.unwrap_err();
    eprintln!("timeout: {e} after {:?}", t0.elapsed());
    assert_eq!(e.kind(), ErrorKind::Timeout);
    assert!(t0.elapsed() < Duration::from_secs(2));
}
