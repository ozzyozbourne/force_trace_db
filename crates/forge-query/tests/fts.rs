//! M4 query path: `search` / `json_key_search` as Exact provider filters answered from `.fts` sidecars, checked
//! against the brute-force UDF above the merge, and EXPLAIN / recorded projections without payload columns.
#![allow(clippy::print_stdout)]
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use forge_index::{FtsBuilder, FtsExpr, FtsHits, IndexReader, IndexResult};
use forge_meta::{Metastore, SegmentFilter};
use forge_model::MemtableView;
use forge_model::catalog::{HourBucket, SegmentId, SegmentMeta, paths};
use forge_model::config::QueryConfig;
use forge_model::row::SpanRow;
use forge_model::testing::{FixedMemtable, batch_from_rows, pid, sid, tid};
use forge_query::testing::{RecordingSegmentStore, memory_store, write_flush};
use forge_query::{FtsIndexReader, QueryEngine};
use forge_storage::{ScanRequest, SegmentStore};
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use std::collections::{BTreeSet, HashSet};
use std::sync::{Arc, Mutex};

const BASE: i64 = 1_700_000_000_000_000_000;

/// The real reader, with `fts_byte_size` taken from the object (write_flush commits segments without it) and
/// segments in `hidden` reported as sidecar-less.
#[derive(Debug)]
struct Sidecars {
    inner: FtsIndexReader,
    store: Arc<dyn object_store::ObjectStore>,
    hidden: Mutex<HashSet<SegmentId>>,
}

#[async_trait]
impl IndexReader for Sidecars {
    async fn search(&self, seg: &SegmentMeta, e: &FtsExpr, l: Option<u64>) -> IndexResult<FtsHits> {
        if self.hidden.lock().unwrap().contains(&seg.id) {
            return Ok(FtsHits::NoIndex);
        }
        let loc = paths::fts_path(&seg.path);
        let size = self.store.head(&loc.as_str().into()).await.unwrap().size;
        let seg = SegmentMeta {
            fts_byte_size: Some(size),
            ..seg.clone()
        };
        self.inner.search(&seg, e, l).await
    }
}

fn row(trace: u64, span: u64, inputs: Option<&str>) -> SpanRow {
    SpanRow {
        project_id: Some(pid("p")),
        trace_id: tid(trace),
        span_id: sid(span),
        name: Some(format!("span-{span}")),
        start_time_ns: Some(BASE + span as i64),
        inputs: inputs.map(str::to_string),
        metadata: Some(format!("{{\"user\":\"u{}\"}}", span % 3)),
        ..Default::default()
    }
}

struct Fx {
    engine: QueryEngine,
    store: Arc<RecordingSegmentStore>,
    segs: Vec<SegmentMeta>,
    sidecars: Arc<Sidecars>,
}

async fn fixture() -> Fx {
    let meta: Arc<dyn Metastore> = Arc::new(forge_meta::SqliteMetastore::open_in_memory().unwrap());
    let store = Arc::new(RecordingSegmentStore::new(memory_store()));
    let mut v = 0;
    let mut stamp = |rows: Vec<SpanRow>| -> Vec<SpanRow> {
        rows.into_iter()
            .map(|r| {
                v += 1;
                SpanRow { version: v, ..r }
            })
            .collect()
    };
    // Flush 1: A (a1 says refund, later loses it), B (refund, single-source), E (gains refund in the memtable),
    // F (refund, tombstoned in flush 2), G (100 filler spans so the index matters).
    let mut f1 = vec![
        row(
            1,
            11,
            Some(r#"{"messages":[{"role":"user","content":"Refund please"}]}"#),
        ),
        row(1, 12, Some(r#"{"messages":[{"role":"user","content":"hello"}]}"#)),
        row(
            2,
            21,
            Some(r#"{"messages":[{"role":"user","content":"refund my order"}]}"#),
        ),
        row(5, 51, Some(r#"{"q":"nothing yet"}"#)),
        row(6, 61, Some(r#"{"q":"refund"}"#)),
    ];
    f1.extend((0..100).map(|i| row(7, 700 + i, Some(r#"{"q":"filler text"}"#))));
    let f1 = stamp(f1);
    // Flush 2: a1's inputs replaced (term gone after the merge), F deleted, H (refund) only here.
    let f2 = stamp(vec![
        row(1, 11, Some(r#"{"messages":[{"role":"user","content":"never mind"}]}"#)),
        SpanRow {
            deleted: true,
            ..row(6, 61, None)
        },
        row(8, 81, Some(r#"{"q":"refund later"}"#)),
    ]);
    // Memtable: D (refund, memtable only), E gains refund.
    let m = stamp(vec![
        row(4, 41, Some(r#"{"q":"refund in mem"}"#)),
        row(5, 51, Some(r#"{"q":"refund now"}"#)),
    ]);
    let hour = HourBucket::from_nanos(BASE);
    let inner: Arc<dyn SegmentStore> = store.clone();
    let mut segs = write_flush(
        &*meta,
        inner.clone(),
        &batch_from_rows(&f1),
        hour,
        "t",
        64 << 10,
        1,
        105,
    )
    .await;
    segs.extend(
        write_flush(
            &*meta,
            inner.clone(),
            &batch_from_rows(&f2),
            hour,
            "t",
            64 << 10,
            2,
            108,
        )
        .await,
    );
    // Sidecars, built from each segment's rows in file order (what SegmentSink does at flush).
    for s in &segs {
        let batches: Vec<RecordBatch> = store
            .scan(s, ScanRequest::default())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut b = FtsBuilder::default();
        for x in &batches {
            b.add_batch(x).unwrap();
        }
        let bytes = b.finish().unwrap().0;
        let loc = paths::fts_path(&s.path);
        store
            .object_store()
            .put(&loc.as_str().into(), bytes.into())
            .await
            .unwrap();
    }
    let mem: Arc<dyn MemtableView> = Arc::new(FixedMemtable(vec![batch_from_rows(&m)]));
    let sidecars = Arc::new(Sidecars {
        inner: FtsIndexReader::new(store.object_store()),
        store: store.object_store(),
        hidden: Mutex::default(),
    });
    let engine =
        QueryEngine::new(meta.clone(), store.clone(), mem, QueryConfig::default()).with_index(sidecars.clone());
    let all = meta.list_segments(&SegmentFilter::default()).await.unwrap();
    assert_eq!(all.len(), 2);
    store.take_projections();
    Fx {
        engine,
        store,
        segs,
        sidecars,
    }
}

async fn names(e: &QueryEngine, q: &str) -> BTreeSet<String> {
    let b = e.sql_batches(q).await.unwrap();
    b.iter()
        .flat_map(|b| {
            let c = b.column(0).as_string::<i32>();
            (0..c.len()).map(|i| c.value(i).to_string()).collect::<Vec<_>>()
        })
        .collect()
}

async fn explain(e: &QueryEngine, q: &str) -> String {
    let b = e.sql_batches(&format!("EXPLAIN {q}")).await.unwrap();
    let s = pretty_format_batches(&b).unwrap().to_string();
    s[s.find("physical_plan").unwrap()..].to_string()
}

/// The index answer equals the brute-force UDF over merged rows (CASE hides `search` from pushdown), for indexed
/// segments and with one segment's sidecar hidden (brute-force fallback).
#[tokio::test]
async fn index_matches_brute_force_after_merge() {
    let fx = fixture().await;
    let preds = [
        "search(inputs, 'refund')",
        "search(inputs, 'refund OR hello')",
        "search(inputs, 'ref*')",
        "search(inputs, '\"refund my\"')",
        "search(inputs, '\"my refund\"')",
        "json_key_search(inputs, '$.messages[*].content', 'refund')",
        "json_key_search(inputs, '$.messages[0].content', 'refund')",
        "json_key_search(inputs, '$.q', '')",
        "search(inputs, 'refund') AND json_key_search(metadata, '$.user', 'u1')",
        "search(inputs, 'refund') OR search(name, 'span-12')",
        "search(name, 'span-11')",
    ];
    for hide in [false, true] {
        if hide {
            fx.sidecars.hidden.lock().unwrap().insert(fx.segs[1].id);
        }
        for p in preds {
            let got = names(
                &fx.engine,
                &format!("SELECT name FROM spans WHERE project_id = 'p' AND {p}"),
            )
            .await;
            let want = names(
                &fx.engine,
                &format!("SELECT name FROM spans WHERE project_id = 'p' AND CASE WHEN {p} THEN true ELSE false END"),
            )
            .await;
            assert_eq!(got, want, "{p} (hidden sidecar: {hide})");
        }
    }
    let got = names(
        &fx.engine,
        "SELECT name FROM spans WHERE project_id = 'p' AND search(inputs, 'refund')",
    )
    .await;
    let want: BTreeSet<String> = ["span-21", "span-41", "span-51", "span-81"].map(String::from).into();
    assert_eq!(
        got, want,
        "a1 lost the term, f1 is deleted, d1/e1 come from the memtable"
    );
}

/// When every hit is single-source, no scan reads `inputs` (only footer/FST/postings and the hit rows' narrow
/// columns); EXPLAIN shows a `Rows` part and no FilterExec on search. With candidates that need the merge, only the
/// merge branch reads `inputs`, and only their trace ranges.
#[tokio::test]
async fn payload_not_read_when_index_answers() {
    let fx = fixture().await;
    let q = "SELECT span_id, name FROM spans WHERE project_id = 'p' AND search(inputs, 'order')";
    let plan = explain(&fx.engine, q).await;
    println!("{plan}");
    assert!(!plan.contains("inputs"), "{plan}");
    assert!(!plan.contains("search("), "Exact: no FilterExec re-check\n{plan}");
    fx.store.take_projections();
    assert_eq!(
        names(
            &fx.engine,
            "SELECT name FROM spans WHERE project_id = 'p' AND search(inputs, 'order')"
        )
        .await
        .len(),
        1
    );
    let read = fx.store.take_projections();
    assert!(read.iter().all(|p| !p.contains(&"inputs")), "{read:?}");

    // 'refund': a1 (2 versions), e1 (memtable), d1 (memtable only) need the merge; b1, h1 do not.
    let q = "SELECT span_id, name FROM spans WHERE project_id = 'p' AND search(inputs, 'refund')";
    let plan = explain(&fx.engine, q).await;
    assert!(plan.contains("search(inputs"), "re-check above the merge\n{plan}");
    println!("{plan}");
    fx.store.take_projections();
    names(&fx.engine, &q.replace("span_id, name", "name")).await;
    let read = fx.store.take_projections();
    println!("projections: {read:?}");
    let wide = read.iter().filter(|p| p.contains(&"inputs")).count();
    assert!(wide >= 1, "{read:?}");
    // A plain scan of the same columns reads inputs nowhere: the column comes only from the FTS re-check.
    names(&fx.engine, "SELECT name FROM spans WHERE project_id = 'p'").await;
    assert!(fx.store.take_projections().iter().all(|p| !p.contains(&"inputs")));
}

#[tokio::test]
async fn json_key_and_trace_tree() {
    let fx = fixture().await;
    let b = fx
        .engine
        .sql_batches("SELECT json_key(inputs, '$.messages[0].content') AS c FROM spans WHERE project_id = 'p' AND name = 'span-21'")
        .await
        .unwrap();
    assert_eq!(b[0].column(0).as_string::<i32>().value(0), "refund my order");
    let t = tid(7).to_string();
    let b = fx
        .engine
        .sql_batches(&format!(
            "SELECT span_id, depth, tree_order FROM trace_tree('{t}') ORDER BY tree_order"
        ))
        .await
        .unwrap();
    assert_eq!(b.iter().map(|b| b.num_rows()).sum::<usize>(), 100);
    let plan = explain(
        &fx.engine,
        &format!("SELECT * FROM trace_tree('{t}') ORDER BY tree_order"),
    )
    .await;
    assert!(
        !plan.contains("SortExec"),
        "the provider's declared order satisfies ORDER BY tree_order\n{plan}"
    );
}

/// Scaled perf (plan §6): 8 L0 segments x 50k spans (~1 KB inputs each, 1-in-500 rare term, 1-in-5 common term),
/// every segment with a sidecar, + a 64-batch memtable. cargo test -p forge-query --profile quick-release --test fts
/// -- --ignored --nocapture
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn perf_search_newest_50() {
    use std::time::Instant;
    let meta: Arc<dyn Metastore> = Arc::new(forge_meta::SqliteMetastore::open_in_memory().unwrap());
    let store = Arc::new(RecordingSegmentStore::new(memory_store()));
    let inner: Arc<dyn SegmentStore> = store.clone();
    let (segs, per) = (8u64, 50_000u64);
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut word = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        format!("w{}", x % 50_000)
    };
    let mut v = 0;
    let hour = HourBucket::from_nanos(BASE);
    let t0 = Instant::now();
    for s in 0..segs {
        let rows: Vec<SpanRow> = (0..per)
            .map(|i| {
                let n = s * per + i;
                v += 1;
                let mut text: Vec<String> = (0..120).map(|_| word()).collect();
                if n % 500 == 0 {
                    text.push("zebracorn".into());
                }
                if n % 5 == 0 {
                    text.push("refund".into());
                }
                let inputs = serde_json::json!({"messages":[{"role":"user","content":text.join(" ")}]}).to_string();
                SpanRow {
                    version: v,
                    ..row(n / 20, n, Some(&inputs))
                }
            })
            .collect();
        let written = write_flush(
            &*meta,
            inner.clone(),
            &batch_from_rows(&rows),
            hour,
            "t",
            64 << 20,
            s + 1,
            v,
        )
        .await;
        for seg in &written {
            let batches: Vec<RecordBatch> = store
                .scan(seg, ScanRequest::default())
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let mut b = FtsBuilder::default();
            for x in &batches {
                b.add_batch(x).unwrap();
            }
            let (bytes, st) = b.finish().unwrap();
            println!(
                "segment {}: {} rows, vortex {} B, fts {} B",
                seg.id, seg.row_count, seg.byte_size, st.file_bytes
            );
            store
                .object_store()
                .put(&paths::fts_path(&seg.path).as_str().into(), bytes.into())
                .await
                .unwrap();
        }
    }
    println!("built in {:?}", t0.elapsed());
    let mem_rows: Vec<RecordBatch> = (0..64u64)
        .map(|b| {
            batch_from_rows(
                &(0..100u64)
                    .map(|i| {
                        v += 1;
                        let n = 10_000_000 + b * 100 + i;
                        SpanRow {
                            version: v,
                            ..row(n / 20, n, Some(&format!(r#"{{"q":"{} {}"}}"#, word(), word())))
                        }
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    let mem: Arc<dyn MemtableView> = Arc::new(FixedMemtable(mem_rows));
    let idx = Arc::new(Sidecars {
        inner: FtsIndexReader::new(store.object_store()),
        store: store.object_store(),
        hidden: Mutex::default(),
    });
    let engine = QueryEngine::new(meta, store.clone(), mem, QueryConfig::default()).with_index(idx);
    for (label, term) in [
        ("rare (1/500)", "zebracorn"),
        ("common (1/5)", "refund"),
        ("absent", "qqqzzz"),
    ] {
        let q = format!(
            "SELECT trace_id, span_id, name, start_time FROM spans WHERE project_id = 'p' AND search(inputs, '{term}') \
             ORDER BY start_time DESC LIMIT 50"
        );
        let mut ts = Vec::new();
        let mut n = 0;
        for i in 0..25 {
            let t = Instant::now();
            let b = engine.sql_batches(&q).await.unwrap();
            if i >= 5 {
                ts.push(t.elapsed());
            }
            n = b.iter().map(|b| b.num_rows()).sum::<usize>();
        }
        ts.sort();
        println!(
            "{label:<14} rows {n:>3}  p50 {:?}  p95 {:?}",
            ts[ts.len() / 2],
            ts[ts.len() * 95 / 100]
        );
        let read = store.take_projections();
        let wide = read.iter().filter(|p| p.contains(&"inputs")).count();
        println!(
            "   scans {} (with inputs: {wide}); first: {:?}",
            read.len(),
            read.iter().find(|p| p.contains(&"inputs"))
        );
    }
}
