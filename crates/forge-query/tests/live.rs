//! The read path against a real `Pipeline` (plan §6.4 "Live"): concurrent ingest + flushes, and a deterministic
//! flush between the memtable snapshot and the metastore call (invariant 1).
mod common;

use arrow::array::{AsArray, RecordBatch};
use common::{ref_batch, runs_request, sort_by_key, trace_cols};
use forge_ingest::{Ingest, Pipeline};
use forge_meta::{Metastore, SegmentFilter, SqliteMetastore};
use forge_model::config::{ForgeConfig, QueryConfig, StorageConfig};
use forge_model::ids::{SpanId, TraceId};
use forge_model::row::SpanRow;
use forge_model::schema::col;
use forge_model::testing::{pid, reference, sid, tid};
use forge_query::QueryEngine;
use forge_query::testing::HookedMetastore;
use forge_query::trace::{TraceFormat, TraceRequest};
use forge_storage::{SegmentStore, VortexSegmentStore, build_object_store};
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SPANS: u64 = 8;

fn config(root: &Path, flush_bytes: u64) -> ForgeConfig {
    let mut c = ForgeConfig::default();
    c.wal.dir = root.join("wal");
    c.memtable.flush_bytes = bytesize::ByteSize::b(flush_bytes);
    c.memtable.flush_interval = Duration::from_secs(3600);
    c
}

struct Stack {
    meta: Arc<dyn Metastore>,
    store: Arc<dyn SegmentStore>,
    pipeline: Arc<Pipeline>,
}

async fn stack(root: &Path, flush_bytes: u64) -> Stack {
    let meta: Arc<dyn Metastore> = Arc::new(SqliteMetastore::open_path(&root.join("meta.db")).unwrap());
    let url = format!("file://{}", root.join("data").display());
    let store: Arc<dyn SegmentStore> = Arc::new(VortexSegmentStore::new(
        build_object_store(&StorageConfig { url }).unwrap(),
    ));
    let (pipeline, _) = Pipeline::open(&config(root, flush_bytes), meta.clone(), store.clone())
        .await
        .unwrap();
    Stack { meta, store, pipeline }
}

/// Trace `t`: a root and SPANS-1 children; start posts (version 1) and end posts (version 2).
fn posts(t: u64, end: bool) -> Vec<SpanRow> {
    (0..SPANS)
        .map(|s| {
            let base = SpanRow {
                project_id: Some(pid("live")),
                trace_id: tid(t),
                span_id: sid(s),
                ..Default::default()
            };
            let start = 1_700_000_000_000_000_000 + (t * 100 + s) as i64 * 1000;
            if end {
                SpanRow {
                    end_time_ns: Some(start + 5_000),
                    outputs: Some(format!("{{\"o\":{s}}}")),
                    version: 2,
                    ..base
                }
            } else {
                SpanRow {
                    parent_span_id: (s > 0).then(|| sid(0)),
                    name: Some(format!("s{s}")),
                    start_time_ns: Some(start),
                    inputs: Some(format!("{{\"i\":\"{}\"}}", "x".repeat(200))),
                    version: 1,
                    ..base
                }
            }
        })
        .collect()
}

type Key = (TraceId, SpanId);

fn keys(b: &[RecordBatch]) -> Vec<Key> {
    b.iter()
        .flat_map(|b| {
            let (t, s) = (b.column(1).as_binary::<i32>(), b.column(2).as_binary::<i32>());
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

/// `load_trace` == the reference merge of the trace's posts, ignoring the WAL-assigned versions.
fn assert_trace(rows: &RecordBatch, all: &[SpanRow], ctx: &str) {
    let cols: Vec<&str> = trace_cols(true).into_iter().filter(|c| *c != col::VERSION).collect();
    let got = sort_by_key(rows);
    let got = got
        .project(
            &cols
                .iter()
                .map(|c| got.schema().index_of(c).unwrap())
                .collect::<Vec<_>>(),
        )
        .unwrap();
    assert_eq!(got.columns(), ref_batch(&reference(all), &cols).columns(), "{ctx}");
}

/// Readers race a writer and the background flusher: every acked key is visible exactly once, closed traces are
/// exact, and the run really crossed flushes with rows in both the memtable and segments.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_ingest_and_reads() {
    let dir = tempfile::tempdir().unwrap();
    let s = stack(dir.path(), 48 << 10).await;
    let tasks = s.pipeline.start_background();
    let engine = Arc::new(QueryEngine::new(
        s.meta.clone(),
        s.store.clone(),
        s.pipeline.memtable_view(),
        QueryConfig::default(),
    ));
    let acked: Arc<Mutex<HashSet<Key>>> = Arc::default();
    let closed: Arc<Mutex<Vec<u64>>> = Arc::default();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let (p, acked, closed, stop) = (s.pipeline.clone(), acked.clone(), closed.clone(), stop.clone());
        tokio::spawn(async move {
            let mut rows = 0;
            for t in 0..400u64 {
                if stop.load(std::sync::atomic::Ordering::Relaxed) && rows >= 2_000 {
                    break;
                }
                p.submit(posts(t, false)).await.unwrap();
                acked.lock().unwrap().extend((0..SPANS).map(|s| (tid(t), sid(s))));
                p.submit(posts(t, true)).await.unwrap();
                closed.lock().unwrap().push(t);
                rows += 2 * SPANS as usize;
                tokio::task::yield_now().await;
            }
            rows
        })
    };
    let segments = async || s.meta.list_segments(&SegmentFilter::default()).await.unwrap().len();
    while closed.lock().unwrap().is_empty() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let before = segments().await;
    let mut both = false;
    for i in 0..200 {
        let a: HashSet<Key> = acked.lock().unwrap().clone();
        let mem_rows = s.pipeline.memtable_view().snapshot().num_rows();
        let segs = segments().await;
        let got = keys(
            &engine
                .sql_batches("SELECT project_id, trace_id, span_id FROM spans")
                .await
                .unwrap(),
        );
        let set: HashSet<Key> = got.iter().copied().collect();
        assert_eq!(set.len(), got.len(), "iteration {i}: duplicate keys");
        assert!(
            a.is_subset(&set),
            "iteration {i}: {} acked keys missing",
            a.difference(&set).count()
        );
        both |= mem_rows > 0 && segs > 0;
        if i % 10 == 0 {
            let t = *closed.lock().unwrap().last().unwrap();
            let req = TraceRequest {
                project: pid("live"),
                trace: tid(t),
                payloads: true,
                format: TraceFormat::Tree,
            };
            let lt = engine.load_trace(&req).await.unwrap();
            let all: Vec<SpanRow> = posts(t, false).into_iter().chain(posts(t, true)).collect();
            assert_trace(&lt.rows, &all, &format!("iteration {i} trace {t}"));
        }
    }
    let flushes = segments().await - before;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let rows = writer.await.unwrap();
    tasks.shutdown(Duration::from_secs(30)).await.unwrap().unwrap();
    assert!(flushes >= 3, "only {flushes} flushes during the loop");
    assert!(rows >= 2_000, "only {rows} rows acked");
    assert!(both, "no iteration saw memtable and segment rows together");
    // Everything flushed: SQL == the reference of every post.
    s.pipeline.flush_all().await.unwrap();
    let n = closed.lock().unwrap().len() as u64;
    let all: Vec<SpanRow> = (0..n)
        .flat_map(|t| posts(t, false).into_iter().chain(posts(t, true)))
        .collect();
    let cols: Vec<&str> = trace_cols(true).into_iter().filter(|c| *c != col::VERSION).collect();
    let got = engine
        .sql_batches(&format!("SELECT {} FROM spans", cols.join(", ")))
        .await
        .unwrap();
    let got = sort_by_key(&arrow::compute::concat_batches(&got[0].schema(), &got).unwrap());
    assert_eq!(got.columns(), ref_batch(&reference(&all), &cols).columns());
}

/// A flush that commits and releases the memtable between the snapshot and the metastore call loses nothing on
/// any path; the wrong order (list, then snapshot) loses the rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_between_snapshot_and_listing() {
    let all: Vec<SpanRow> = posts(1, false).into_iter().chain(posts(1, true)).collect();
    for path in ["trace", "runs", "sql", "control"] {
        let dir = tempfile::tempdir().unwrap();
        let s = stack(dir.path(), 1 << 30).await;
        s.pipeline.submit(posts(1, false)).await.unwrap();
        s.pipeline.submit(posts(1, true)).await.unwrap();
        assert_eq!(s.meta.list_segments(&SegmentFilter::default()).await.unwrap().len(), 0);
        let p = s.pipeline.clone();
        let hooked = HookedMetastore::once(s.meta.clone(), move || {
            Box::pin(async move {
                p.flush_now().await.unwrap().expect("flushed");
            })
        });
        let hooked: Arc<dyn Metastore> = Arc::new(hooked);
        let engine = QueryEngine::new(
            hooked.clone(),
            s.store.clone(),
            s.pipeline.memtable_view(),
            QueryConfig::default(),
        );
        let req = TraceRequest {
            project: pid("live"),
            trace: tid(1),
            payloads: true,
            format: TraceFormat::Tree,
        };
        match path {
            "trace" => assert_trace(&engine.load_trace(&req).await.unwrap().rows, &all, path),
            "runs" => {
                let page = engine.list_runs(&runs_request("live")).await.unwrap();
                assert_eq!(page.rows.num_rows(), 1, "the root");
            }
            "sql" => {
                let b = engine
                    .sql_batches("SELECT project_id, trace_id, span_id FROM spans")
                    .await
                    .unwrap();
                assert_eq!(keys(&b).len(), SPANS as usize);
            }
            _ => {
                // The wrong order: the metastore call (which flushes and releases), THEN the snapshot.
                let locs = hooked.locate_trace(&pid("live"), tid(1)).await.unwrap();
                let snap = s.pipeline.memtable_view().snapshot();
                assert!(
                    locs.is_empty() && snap.is_empty(),
                    "the wrong order must lose the trace"
                );
            }
        }
        assert_eq!(
            s.meta.list_segments(&SegmentFilter::default()).await.unwrap().len(),
            1,
            "{path}: flushed"
        );
        assert!(s.pipeline.memtable_view().snapshot().is_empty(), "{path}: released");
    }
}
