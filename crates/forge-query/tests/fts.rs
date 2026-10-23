//! M4 query path: `search` / `json_key_search` as Exact provider filters answered from `.fts` sidecars, checked
//! against the brute-force UDF above the merge, and EXPLAIN / recorded projections without payload columns.
#![allow(clippy::print_stdout)]
use arrow::array::{Array, AsArray};
use arrow::util::pretty::pretty_format_batches;
use forge_meta::{Metastore, SegmentFilter};
use forge_model::MemtableView;
use forge_model::catalog::{HourBucket, SegmentMeta, paths};
use forge_model::config::QueryConfig;
use forge_model::row::SpanRow;
use forge_model::testing::{FixedMemtable, batch_from_rows, pid, sid, tid};
use forge_query::QueryEngine;
use forge_query::testing::{HidingIndexReader, RecordingSegmentStore, memory_store, stamp_versions, write_flushes};
use forge_storage::SegmentStore;
use object_store::ObjectStoreExt;
use std::collections::BTreeSet;
use std::sync::Arc;

const BASE: i64 = 1_700_000_000_000_000_000;

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
    meta: Arc<dyn Metastore>,
    mem: Arc<dyn MemtableView>,
    store: Arc<RecordingSegmentStore>,
    segs: Vec<SegmentMeta>,
    sidecars: Arc<HidingIndexReader>,
}

async fn fixture() -> Fx {
    let meta: Arc<dyn Metastore> = Arc::new(forge_meta::SqliteMetastore::open_in_memory().unwrap());
    let store = Arc::new(RecordingSegmentStore::new(memory_store()));
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
    // Flush 2: a1's inputs replaced (term gone after the merge), F deleted, H (refund) only here.
    let mut f2 = vec![
        row(1, 11, Some(r#"{"messages":[{"role":"user","content":"never mind"}]}"#)),
        SpanRow {
            deleted: true,
            ..row(6, 61, None)
        },
        row(8, 81, Some(r#"{"q":"refund later"}"#)),
    ];
    // Memtable: D (refund, memtable only), E gains refund.
    let mut m = vec![
        row(4, 41, Some(r#"{"q":"refund in mem"}"#)),
        row(5, 51, Some(r#"{"q":"refund now"}"#)),
    ];
    stamp_versions(f1.iter_mut().chain(&mut f2).chain(&mut m));
    let hour = HourBucket::from_nanos(BASE);
    let segs = write_flushes(&*meta, store.clone(), &[f1, f2], |_| hour, "t", 64 << 10).await;
    assert!(
        segs.iter().all(|s| s.fts_byte_size.is_some()),
        "the sink builds every sidecar"
    );
    let mem: Arc<dyn MemtableView> = Arc::new(FixedMemtable(vec![batch_from_rows(&m)]));
    let sidecars = Arc::new(HidingIndexReader::new(store.object_store()));
    let engine =
        QueryEngine::new(meta.clone(), store.clone(), mem.clone(), QueryConfig::default()).with_index(sidecars.clone());
    let all = meta.list_segments(&SegmentFilter::default()).await.unwrap();
    assert_eq!(all.len(), 2);
    store.take_projections();
    Fx {
        engine,
        meta,
        mem,
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

/// When every hit is single-source, no scan reads `inputs`: EXPLAIN shows exactly the hit rows (`rows=Rows(1)`)
/// and no FilterExec on search. With the sidecar hidden, the brute-force scan reads only `inputs`, and the answer is
/// the same. With candidates that need the merge, only the merge branch reads `inputs`. `count(*)` is exact.
#[tokio::test]
async fn payload_not_read_when_index_answers() {
    let fx = fixture().await;
    let q = "SELECT span_id, name FROM spans WHERE project_id = 'p' AND search(inputs, 'order')";
    let plan = explain(&fx.engine, q).await;
    assert!(!plan.contains("inputs"), "{plan}");
    assert!(!plan.contains("search("), "Exact: no FilterExec re-check\n{plan}");
    assert!(
        plan.contains("rows=Rows(1),") || plan.contains("rows=Rows(1) "),
        "one hit row\n{plan}"
    );
    fx.store.take_projections();
    let q = "SELECT name FROM spans WHERE project_id = 'p' AND search(inputs, 'order')";
    assert_eq!(names(&fx.engine, q).await, ["span-21".to_string()].into());
    let read = fx.store.take_projections();
    assert!(read.iter().all(|p| !p.contains(&"inputs")), "{read:?}");
    let count = async |q: String| {
        let b = fx.engine.sql_batches(&q).await.unwrap();
        b[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0)
    };
    let cq = |t: &str| format!("SELECT count(*) FROM spans WHERE project_id = 'p' AND search(inputs, '{t}')");
    assert_eq!(count(cq("order")).await, 1);
    assert_eq!(count(cq("refund")).await, 4);

    // Hidden sidecar: brute force reads only `inputs` of that segment; same rows.
    let seg = fx.segs.iter().find(|s| s.row_count > 50).expect("flush 1").id;
    fx.sidecars.hidden.lock().unwrap().insert(seg);
    fx.store.take_projections();
    assert_eq!(names(&fx.engine, q).await, ["span-21".to_string()].into());
    let read = fx.store.take_projections();
    assert!(read.iter().any(|p| *p == ["inputs"]), "brute-force scan: {read:?}");
    assert!(
        read.iter().filter(|p| p.contains(&"inputs")).all(|p| *p == ["inputs"]),
        "only the brute-force scan reads inputs: {read:?}"
    );
    assert_eq!(count(cq("order")).await, 1);
    fx.sidecars.hidden.lock().unwrap().clear();

    // 'refund': a1 (2 versions), e1 (memtable), d1 (memtable only) need the merge; b1, h1 do not.
    let q = "SELECT span_id, name FROM spans WHERE project_id = 'p' AND search(inputs, 'refund')";
    let plan = explain(&fx.engine, q).await;
    assert!(plan.contains("search(inputs"), "re-check above the merge\n{plan}");
    let scans: Vec<&str> = plan.lines().filter(|l| l.contains("SegmentScanExec")).collect();
    for l in &scans {
        assert_eq!(
            l.contains("inputs"),
            !l.contains("rows=Rows("),
            "only the merge branch reads inputs: {l}"
        );
    }
    fx.store.take_projections();
    names(&fx.engine, &q.replace("span_id, name", "name")).await;
    let read = fx.store.take_projections();
    assert!(read.iter().filter(|p| p.contains(&"inputs")).count() >= 1, "{read:?}");
    // A plain scan of the same columns reads inputs nowhere, and reads every row.
    let plain = "SELECT name FROM spans WHERE project_id = 'p'";
    assert!(explain(&fx.engine, plain).await.contains("rows=All"));
    names(&fx.engine, plain).await;
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

/// D4: an unusable sidecar never fails the query and never changes the answer: the segment is brute-forced, and
/// counted once.
#[tokio::test]
async fn corrupt_sidecar_falls_back_to_brute_force() {
    let fx = fixture().await;
    let q = "SELECT name FROM spans WHERE project_id = 'p' AND search(inputs, 'refund')";
    let want: BTreeSet<String> = ["span-21", "span-41", "span-51", "span-81"].map(String::from).into();
    let seg = fx.segs[0].clone();
    let os = fx.store.object_store();
    let loc: object_store::path::Path = paths::fts_path(&seg.path).as_str().into();
    let good = os.get(&loc).await.unwrap().bytes().await.unwrap();
    let fetch = forge_index::read::bytes_fetch(good.clone());
    let footer = forge_index::FtsFile::open("t", good.len() as u64, &fetch)
        .await
        .unwrap()
        .0
        .footer()
        .clone();
    let n = good.len();
    let flip = |r: std::ops::Range<usize>| {
        let mut b = good.to_vec();
        r.for_each(|i| b[i] ^= 0x5A);
        Some(b)
    };
    type Edit = Option<fn(&mut SegmentMeta)>;
    let cases: Vec<(&str, Option<Vec<u8>>, Edit)> = vec![
        ("trailer magic", flip(n - 1..n), None),
        ("footer crc", flip(n - 8..n - 7), None),
        (
            "fst block",
            flip(footer.dict.start as usize..footer.dict.start as usize + 1),
            None,
        ),
        (
            "postings",
            flip(footer.postings.start as usize..footer.postings.end as usize),
            None,
        ),
        (
            "truncated",
            None,
            Some(|s| s.fts_byte_size = s.fts_byte_size.map(|n| n + 100)),
        ),
        ("missing", Some(Vec::new()), None),
        ("row count", None, Some(|s| s.row_count += 1)),
    ];
    for (name, bytes, edit) in cases {
        match bytes {
            Some(b) if b.is_empty() => os.delete(&loc).await.unwrap(),
            Some(b) => {
                os.put(&loc, b.into()).await.unwrap();
            }
            None => {}
        }
        let idx = Arc::new(HidingIndexReader::new(os.clone()));
        *idx.edit.lock().unwrap() = edit.map(|f| (seg.id, f));
        let e = QueryEngine::new(
            fx.meta.clone(),
            fx.store.clone(),
            fx.mem.clone(),
            QueryConfig::default(),
        )
        .with_index(idx);
        assert_eq!(names(&e, q).await, want, "{name}");
        let c = e.fts_counters();
        assert_eq!((c.brute_parts, c.index_parts), (1, 1), "{name}: {c:?}");
        os.put(&loc, good.clone().into()).await.unwrap();
    }
}

/// One segment (sidecar hidden when `hide`) of `seg_rows` plus a memtable of `mem_rows`, under `cfg`.
async fn small_engine(seg_rows: Vec<SpanRow>, mem_rows: Vec<SpanRow>, hide: bool, cfg: QueryConfig) -> QueryEngine {
    let meta: Arc<dyn Metastore> = Arc::new(forge_meta::SqliteMetastore::open_in_memory().unwrap());
    let store = memory_store();
    let (mut seg_rows, mut mem_rows) = (seg_rows, mem_rows);
    stamp_versions(seg_rows.iter_mut().chain(&mut mem_rows));
    let segs = if seg_rows.is_empty() {
        vec![]
    } else {
        write_flushes(
            &*meta,
            store.clone(),
            &[seg_rows],
            |_| HourBucket::from_nanos(BASE),
            "t",
            1 << 20,
        )
        .await
    };
    let mem: Arc<dyn MemtableView> = Arc::new(FixedMemtable(if mem_rows.is_empty() {
        vec![]
    } else {
        vec![batch_from_rows(&mem_rows)]
    }));
    let idx = Arc::new(HidingIndexReader::new(store.object_store()));
    if hide {
        idx.hidden.lock().unwrap().extend(segs.iter().map(|s| s.id));
    }
    QueryEngine::new(meta, store, mem, cfg).with_index(idx)
}

fn search_req(q: &str) -> forge_query::search::SearchRequest {
    let body = forge_query::search::SearchBody {
        q: q.into(),
        columns: Some(vec!["inputs".into()]),
        ..Default::default()
    };
    forge_query::search::SearchRequest::parse(body, pid("p")).unwrap()
}

/// D5: a sidecar-less segment is brute-forced one batch at a time: 20k rows x ~2 KB of `inputs` (~40 MiB) pass
/// under a 16 MiB query memory limit, on the SQL and the `/v1/search` paths.
#[tokio::test]
async fn brute_force_streams_under_memory_limit() {
    let filler = "lorem ipsum dolor sit amet ".repeat(75);
    let rows: Vec<SpanRow> = (0..20_000u64)
        .map(|i| {
            let text = if i % 5_000 == 0 {
                format!("{filler} needle")
            } else {
                filler.clone()
            };
            row(i / 20, i, Some(&serde_json::json!({ "q": text }).to_string()))
        })
        .collect();
    let bytes: usize = rows.iter().map(|r| r.inputs.as_ref().unwrap().len()).sum();
    assert!(bytes > 32 << 20, "the column alone exceeds the limit: {bytes}");
    let cfg = QueryConfig {
        memory_limit: bytesize::ByteSize::mib(16),
        ..QueryConfig::default()
    };
    let e = small_engine(rows, vec![], true, cfg).await;
    let q = "SELECT name FROM spans WHERE project_id = 'p' AND search(inputs, 'needle')";
    assert_eq!(names(&e, q).await.len(), 4);
    assert_eq!(e.search(&search_req("needle")).await.unwrap().rows.num_rows(), 4);
    assert_eq!(e.fts_counters().brute_parts, 2);
}

/// D13: a prefix expanding to more than 10,000 terms in one source is a 400 whether the source is an indexed
/// segment, a brute-forced segment or the memtable.
#[tokio::test]
async fn prefix_too_broad_everywhere() {
    use forge_model::error::{Classify, ErrorKind};
    let rows = || -> Vec<SpanRow> {
        (0..101u64)
            .map(|i| {
                let words: Vec<String> = (0..100).map(|j| format!("w{}", i * 100 + j)).collect();
                row(1, i, Some(&serde_json::json!({ "q": words.join(" ") }).to_string()))
            })
            .collect()
    };
    for (what, seg, mem, hide) in [
        ("indexed segment", rows(), vec![], false),
        ("brute-forced segment", rows(), vec![], true),
        ("memtable", vec![], rows(), false),
    ] {
        let e = small_engine(seg, mem, hide, QueryConfig::default()).await;
        let err = e
            .sql_batches("SELECT name FROM spans WHERE project_id = 'p' AND search(inputs, 'w*')")
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput, "{what} (SQL): {err}");
        assert!(err.to_string().contains("prefix too broad"), "{what}: {err}");
        let err = e.search(&search_req("w*")).await.err().expect("400");
        assert_eq!(err.kind(), ErrorKind::InvalidInput, "{what} (search): {err}");
        // A narrower prefix works.
        assert!(e.search(&search_req("w100*")).await.is_ok(), "{what}");
    }
}

/// `/v1/search`: single-source hits are read with the run columns only, never a payload column.
#[tokio::test]
async fn search_single_source_reads_no_payload() {
    let fx = fixture().await;
    fx.store.take_projections();
    let page = fx.engine.search(&search_req("order")).await.unwrap();
    assert_eq!(page.rows.num_rows(), 1);
    let read = fx.store.take_projections();
    assert!(!read.is_empty());
    assert!(
        read.iter().all(|p| !p
            .iter()
            .any(|c| ["inputs", "outputs", "events", "attributes"].contains(c))),
        "{read:?}"
    );
    assert_eq!(fx.engine.fts_counters().exact_rows, 1);
}
