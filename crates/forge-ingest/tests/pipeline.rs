#![allow(clippy::print_stderr, clippy::print_stdout)]
mod common;
use bytesize::ByteSize;
use common::*;
use forge_ingest::{Ingest, IngestError, OpenReport, Pipeline};
use forge_meta::{Metastore, SegmentFilter, SqliteMetastore};
use forge_model::config::{ForgeConfig, StorageConfig};
use forge_model::merge::{Tombstones, merge_versions};
use forge_model::row::SpanRow;
use forge_storage::{ScanRequest, SegmentStore, VortexSegmentStore, build_object_store};
use futures::TryStreamExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

struct Stack {
    meta: Arc<dyn Metastore>,
    segs: Arc<VortexSegmentStore>,
}

fn stack(root: &Path) -> Stack {
    let url = format!("file://{}", root.join("data").display());
    Stack {
        meta: Arc::new(SqliteMetastore::open_path(&root.join("meta.db")).unwrap()),
        segs: Arc::new(VortexSegmentStore::new(
            build_object_store(&StorageConfig { url }).unwrap(),
        )),
    }
}

/// Copies the `*.wal` files of `from` into a new dir `to`.
fn copy_wal_files(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let p = e.unwrap().path();
        if p.extension().is_some_and(|x| x == "wal") {
            std::fs::copy(&p, to.join(p.file_name().unwrap())).unwrap();
        }
    }
}

fn options(root: &Path, flush_bytes: usize, seg: u64) -> ForgeConfig {
    let mut c = ForgeConfig::default();
    c.node.id = "node-1".into();
    c.wal.dir = root.join("wal");
    c.wal.segment_bytes = ByteSize::b(seg);
    c.memtable.flush_bytes = ByteSize::b(flush_bytes as u64);
    c.memtable.flush_interval = Duration::from_secs(3600);
    c.memtable.memory_budget = ByteSize::gib(1);
    c
}

async fn open(root: &Path, s: &Stack, flush_bytes: usize, seg: u64) -> (Arc<Pipeline>, OpenReport) {
    Pipeline::open(&options(root, flush_bytes, seg), s.meta.clone(), s.segs.clone())
        .await
        .unwrap()
}

/// Every row of every visible segment (raw, unmerged across segments).
async fn visible_raw(s: &Stack) -> Vec<arrow::array::RecordBatch> {
    let mut out = Vec::new();
    for seg in s.meta.list_segments(&SegmentFilter::default()).await.unwrap() {
        let b: Vec<_> = s
            .segs
            .scan(&seg, ScanRequest::default())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        out.extend(b);
    }
    out
}
fn count(b: &[arrow::array::RecordBatch]) -> usize {
    b.iter().map(|b| b.num_rows()).sum()
}

fn start(project: &str, trace: u64, span: u64) -> SpanRow {
    SpanRow {
        project_id: Some(pid(project)),
        trace_id: tid(trace),
        span_id: sid(span),
        name: Some(format!("span-{span}")),
        start_time_ns: Some(1_700_000_000_000_000_000 + span as i64),
        inputs: Some(format!("{{\"q\":{span}}}")),
        ..Default::default()
    }
}
fn end(project: &str, trace: u64, span: u64) -> SpanRow {
    SpanRow {
        project_id: Some(pid(project)),
        trace_id: tid(trace),
        span_id: sid(span),
        end_time_ns: Some(1_700_000_000_000_000_999 + span as i64),
        outputs: Some(format!("{{\"a\":{span}}}")),
        ..Default::default()
    }
}
fn project_of(span: u64) -> &'static str {
    ["alpha", "beta", "gamma"][(span % 3) as usize]
}

/// `n` spans [from, from+n): 8 concurrent clients, each span = one start post + one end post
/// (separate requests, so versions differ and they may land in different memtables).
async fn ingest_spans(ing: &Arc<Pipeline>, from: u64, n: u64, with_end: bool) {
    let mut tasks = Vec::new();
    for c in 0..8 {
        let ing = ing.clone();
        tasks.push(tokio::spawn(async move {
            for s in (from + c..from + n).step_by(8) {
                ing.submit(vec![start(project_of(s), s / 10, s)]).await.unwrap();
                if with_end {
                    ing.submit(vec![end(project_of(s), s / 10, s)]).await.unwrap();
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_commit_truncate_restart_replay() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let s = stack(root);
    let (ing, rep) = open(root, &s, 1 << 30, 16 << 10).await;
    assert_eq!((rep.replay.applied, rep.next_version), (0, 1));
    ingest_spans(&ing, 0, 400, true).await;
    let r = ing.flush_now().await.unwrap().unwrap();
    assert_eq!(r.segments.len(), 3, "one L0 segment per project");
    assert_eq!(
        (r.rows_in, r.rows_out),
        (800, 400),
        "start+end merged into one row per span"
    );
    assert_eq!(r.watermark, 800);
    assert!(r.wal_segments_deleted > 0);
    let cp = s.meta.wal_checkpoint("node-1").await.unwrap().unwrap();
    assert_eq!((cp.flushed_seq, cp.max_version), (800, 800));
    let rows = rows_from_batch(&merge_versions(&visible_raw(&s).await, Tombstones::Keep).unwrap());
    assert_eq!(rows.len(), 400);
    assert!(
        rows.iter()
            .all(|r| r.name.is_some() && r.end_time_ns.is_some() && r.outputs.is_some())
    );

    // 100 new spans (start only) + end posts for 50 spans already flushed; then "crash" (no flush).
    ingest_spans(&ing, 400, 100, false).await;
    for sp in 0..50 {
        ing.submit(vec![end(project_of(sp), sp / 10, sp)]).await.unwrap();
    }
    drop(ing);

    let (ing, rep) = open(root, &s, 1 << 30, 16 << 10).await;
    assert_eq!(
        (rep.replay.applied, rep.replay.next_seq, rep.next_version),
        (150, 951, 951)
    );
    assert_eq!(ing.memtable().last_seq(), 950);
    ing.flush_now().await.unwrap().unwrap();
    let raw = visible_raw(&s).await;
    assert_eq!(count(&raw), 400 + 100 + 50, "no WAL record flushed twice");
    let merged = merge_versions(&raw, Tombstones::Keep).unwrap();
    assert_eq!(merged.num_rows(), 500);
    assert_eq!(s.meta.wal_checkpoint("node-1").await.unwrap().unwrap().flushed_seq, 950);
    // versions continue after restart: the new writes got 801..950, next is 951
    assert_eq!(s.meta.max_version().await.unwrap(), 950);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_after_commit_before_truncate_skips_by_watermark() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let s = stack(root);
    let (ing, _) = open(root, &s, 1 << 30, 8 << 10).await;
    ingest_spans(&ing, 0, 200, false).await;
    let wal = root.join("wal/node-1");
    let saved = root.join("wal-copy");
    copy_wal_files(&wal, &saved);
    let r = ing.flush_now().await.unwrap().unwrap();
    assert!(r.wal_segments_deleted > 0);
    drop(ing);
    // Undo the truncation = the process died between commit_flush and truncate.
    for e in std::fs::read_dir(&saved).unwrap() {
        let p = e.unwrap().path();
        std::fs::copy(&p, wal.join(p.file_name().unwrap())).unwrap();
    }
    let (ing, rep) = open(root, &s, 1 << 30, 8 << 10).await;
    assert_eq!((rep.replay.applied, rep.replay.skipped), (0, 200));
    assert!(ing.flush_now().await.unwrap().is_none());
    assert_eq!(count(&visible_raw(&s).await), 200);
    // startup truncation removed the stale sealed files
    assert_eq!(forge_ingest::wal::list_segments(&wal).unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphan_upload_before_commit_is_invisible() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let s = stack(root);
    let (ing, _) = open(root, &s, 1 << 30, 1 << 20).await;
    ingest_spans(&ing, 0, 99, false).await;
    // "Upload then die before commit": write a segment of the memtable's rows, never commit it.
    let snap = forge_model::MemtableView::snapshot(&*ing.memtable());
    let merged = merge_versions(&snap.batches, Tombstones::Keep).unwrap();
    let alpha = forge_model::merge::split_by_project(&merged).unwrap().remove(0);
    let spec = forge_storage::SegmentSpec {
        id: forge_model::catalog::SegmentId::new(),
        project_id: alpha.0,
        level: forge_model::catalog::Level::L0,
        partition_hour: forge_model::catalog::HourBucket(0),
    };
    use futures::StreamExt;
    s.segs
        .write_segment(spec, futures::stream::iter([Ok(alpha.1)]).boxed())
        .await
        .unwrap();
    drop(ing);
    let (ing, rep) = open(root, &s, 1 << 30, 1 << 20).await;
    assert_eq!(rep.replay.applied, 99);
    ing.flush_now().await.unwrap().unwrap();
    assert_eq!(
        count(&visible_raw(&s).await),
        99,
        "orphan not visible, rows exactly once"
    );
    let files = walk(&root.join("data"));
    assert_eq!(
        files.iter().filter(|p| p.ends_with(".vortex")).count(),
        4,
        "3 committed + 1 orphan for GC"
    );
}

fn walk(d: &Path) -> Vec<String> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(d).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p.display().to_string());
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_flushes_when_over_flush_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let s = stack(root);
    let (ing, _) = open(root, &s, 1 << 30, 64 << 10).await;
    ingest_spans(&ing, 0, 600, true).await;
    drop(ing);
    // Restart with a small flush_bytes: replay must flush as it goes (bounded memory).
    let (ing, rep) = open(root, &s, 128 << 10, 64 << 10).await;
    assert_eq!(rep.replay.applied, 1200);
    assert!(rep.replay_flushes >= 2, "{rep:?}");
    ing.flush_now().await.unwrap();
    let raw = visible_raw(&s).await;
    let merged = merge_versions(&raw, Tombstones::Keep).unwrap();
    assert_eq!(merged.num_rows(), 600);
    assert!(count(&raw) <= 1200);
    assert_eq!(
        s.meta.wal_checkpoint("node-1").await.unwrap().unwrap().flushed_seq,
        1200
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_flushes_keep_the_watermark_honest() {
    // Invariant 7 under load: flushes freeze while 8 writers are mid-append.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let s = stack(root);
    let (ing, _) = open(root, &s, 1 << 30, 32 << 10).await;
    let w = {
        let ing = ing.clone();
        tokio::spawn(async move { ingest_spans(&ing, 0, 2000, false).await })
    };
    let mut flushes = 0;
    while !w.is_finished() {
        if ing.flush_now().await.unwrap().is_some() {
            flushes += 1;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    w.await.unwrap();
    drop(ing); // crash: whatever is unflushed must come back from the WAL, and nothing twice
    let (ing, rep) = open(root, &s, 1 << 30, 32 << 10).await;
    ing.flush_now().await.unwrap();
    let raw = visible_raw(&s).await;
    eprintln!(
        "flushes during load: {flushes}; replayed after crash: {}",
        rep.replay.applied
    );
    assert!(flushes >= 2);
    assert_eq!(count(&raw), 2000, "every acked span exactly once");
    assert_eq!(merge_versions(&raw, Tombstones::Keep).unwrap().num_rows(), 2000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backpressure_over_budget() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let s = stack(root);
    let mut o = options(root, 1 << 30, 1 << 20);
    o.memtable.memory_budget = ByteSize::kib(20);
    let (ing, _) = Pipeline::open(&o, s.meta.clone(), s.segs.clone()).await.unwrap();
    let mut n = 0;
    let err = loop {
        match ing.submit(vec![start("alpha", 1, n)]).await {
            Ok(_) => n += 1,
            Err(e) => break e,
        }
    };
    assert!(matches!(err, IngestError::Backpressure) && n >= 2, "{err} after {n}");
    eprintln!(
        "backpressure after {n} one-row writes ({} B accounted)",
        ing.memtable().unflushed_bytes()
    );
    ing.flush_now().await.unwrap();
    ing.submit(vec![start("alpha", 1, 999)]).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acked_is_queryable() {
    let tmp = tempfile::tempdir().unwrap();
    let s = stack(tmp.path());
    let (ing, _) = open(tmp.path(), &s, 1 << 30, 1 << 20).await;
    ing.submit(vec![start("alpha", 1, 1), end("alpha", 1, 1)])
        .await
        .unwrap();
    let snap = forge_model::MemtableView::snapshot(&*ing.memtable());
    assert_eq!(count(&snap.batches), 2);
    let versions: Vec<u64> = rows_from_batch(&snap.batches[0]).iter().map(|r| r.version).collect();
    assert_eq!(versions, [1, 2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blob_offload_through_submit() {
    let tmp = tempfile::tempdir().unwrap();
    let s = stack(tmp.path());
    let (ing, _) = open(tmp.path(), &s, 1 << 30, 1 << 20).await;
    let mut r = start("default", 1, 1);
    r.inputs = Some(format!("\"{}\"", "x".repeat(2 << 20)));
    ing.submit(vec![r]).await.unwrap();
    let snap = forge_model::MemtableView::snapshot(&*ing.memtable());
    let rows = rows_from_batch(&snap.batches[0]);
    let (path, size) = forge_model::blob::parse_blob_ref(rows[0].inputs.as_deref().unwrap()).unwrap();
    assert_eq!(size, (2 << 20) + 2);
    assert!(tmp.path().join("data").join(path).is_file());
    assert!(path.starts_with("blobs/default/"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn versions_survive_wiped_wal() {
    let tmp = tempfile::tempdir().unwrap();
    let s = stack(tmp.path());
    let (ing, _) = open(tmp.path(), &s, 1 << 30, 1 << 20).await;
    ing.submit(vec![start("alpha", 1, 1), start("alpha", 1, 2)])
        .await
        .unwrap();
    ing.flush_now().await.unwrap().unwrap();
    let mv = s.meta.max_version().await.unwrap();
    drop(ing);
    std::fs::remove_dir_all(tmp.path().join("wal")).unwrap();
    let (ing, rep) = open(tmp.path(), &s, 1 << 30, 1 << 20).await;
    assert!(rep.next_version > mv);
    ing.submit(vec![start("alpha", 1, 3)]).await.unwrap();
    let snap = forge_model::MemtableView::snapshot(&*ing.memtable());
    assert!(rows_from_batch(&snap.batches[0])[0].version > mv);
}

#[tokio::test]
async fn object_store_wal_mode_is_unimplemented() {
    let tmp = tempfile::tempdir().unwrap();
    let s = stack(tmp.path());
    let mut o = options(tmp.path(), 1 << 30, 1 << 20);
    o.wal.mode = forge_model::config::WalMode::ObjectStore;
    let e = Pipeline::open(&o, s.meta.clone(), s.segs.clone()).await.unwrap_err();
    assert!(matches!(e, IngestError::Unimplemented(_)), "{e}");
}

#[tokio::test]
async fn close_wal_and_flush_all_on_current_thread() {
    let tmp = tempfile::tempdir().unwrap();
    let s = stack(tmp.path());
    let (ing, _) = open(tmp.path(), &s, 1 << 30, 1 << 20).await;
    ing.submit(vec![start("alpha", 1, 1)]).await.unwrap();
    ing.close_wal().await;
    let e = ing.submit(vec![start("alpha", 1, 2)]).await.unwrap_err();
    assert!(
        matches!(e, IngestError::Wal(forge_ingest::wal::WalError::Closed)),
        "{e}"
    );
    ing.flush_all().await.unwrap();
    assert_eq!(count(&visible_raw(&s).await), 1);
}

/// P2 / invariant 9: version order = WAL seq order, even with concurrent submitters.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn versions_follow_wal_order() {
    let tmp = tempfile::tempdir().unwrap();
    let s = stack(tmp.path());
    let (ing, _) = open(tmp.path(), &s, 1 << 30, 64 << 20).await;
    let mut tasks = Vec::new();
    for w in 0..8u64 {
        let ing = ing.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..200u64 {
                let n = w * 1000 + i;
                let rows = (0..1 + n % 3).map(|k| start("alpha", n, n * 10 + k)).collect();
                ing.submit(rows).await.unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    // Read a copy of the live WAL dir.
    let copy = tmp.path().join("wal-copy");
    copy_wal_files(&tmp.path().join("wal/node-1"), &copy);
    let mut ranges = Vec::new();
    let st = forge_ingest::wal::replay(&copy, 0, |_, payload| {
        let rows = rows_from_batch(&forge_ingest::wal::decode_batch(payload).unwrap());
        let v: Vec<u64> = rows.iter().map(|r| r.version).collect();
        assert!(v.windows(2).all(|w| w[1] == w[0] + 1), "row order within a record");
        ranges.push((v[0], *v.last().unwrap()));
        Ok::<_, String>(())
    })
    .unwrap();
    assert_eq!(st.applied, 1600);
    assert!(
        ranges.windows(2).all(|w| w[1].0 > w[0].1),
        "each record's versions are above the previous record's"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn size_trigger_flushes_without_flush_now() {
    let tmp = tempfile::tempdir().unwrap();
    let s = stack(tmp.path());
    let (ing, _) = open(tmp.path(), &s, 64 << 10, 64 << 20).await;
    let tasks = ing.start_background();
    for i in 0..100u64 {
        let mut r = start("alpha", 1, i);
        r.inputs = Some(format!("{{\"pad\":\"{}\"}}", "x".repeat(1000)));
        ing.submit(vec![r]).await.unwrap();
    }
    let t0 = std::time::Instant::now();
    while s
        .meta
        .list_segments(&SegmentFilter::default())
        .await
        .unwrap()
        .is_empty()
    {
        assert!(
            t0.elapsed() < Duration::from_secs(2),
            "no size-triggered flush within 2 s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tasks.shutdown(Duration::from_secs(10)).await.unwrap().unwrap();
}

#[test]
fn failed_frozen_flush_is_retried_while_idle() {
    // A frozen memtable left over by a failed flush makes the flusher try again even with nothing new.
    let m = forge_ingest::memtable::MemtableSet::new(0, 1 << 30, Duration::from_secs(3600));
    let b = batch_from_rows(&[start("alpha", 1, 1)]);
    let bytes = forge_ingest::memtable::accounted_bytes(&b);
    m.insert(1, b, bytes).unwrap();
    let now = std::time::Instant::now();
    assert!(!m.flush_due(now), "not due yet: small and young");
    let f = m.freeze().unwrap();
    assert!(
        m.has_frozen() && m.flush_due(now),
        "the leftover frozen memtable is due"
    );
    m.release(&f);
    assert!(!m.flush_due(now));
}

/// Rows replayed from the WAL are as old as the crash, not the restart: they are due at once, so a crash loop
/// shorter than `flush_interval` still flushes them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replayed_rows_are_due_at_once() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let s = stack(root);
    let mut o = options(root, 1 << 30, 1 << 20);
    o.memtable.flush_interval = Duration::from_millis(200);
    let (ing, _) = Pipeline::open(&o, s.meta.clone(), s.segs.clone()).await.unwrap();
    ing.submit(vec![start("alpha", 1, 1)]).await.unwrap();
    drop(ing); // no flush: the row is only in the WAL
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (ing, rep) = Pipeline::open(&o, s.meta.clone(), s.segs.clone()).await.unwrap();
    assert_eq!(rep.replay.applied, 1);
    assert!(
        ing.memtable().flush_due(std::time::Instant::now()),
        "replayed rows are due right after open"
    );
}

/// The budget counts requests between the check and the memtable insert: concurrent submits cannot all pass a
/// check that sees an empty memtable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_submits_over_budget_get_429() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let s = stack(root);
    let mut o = options(root, 1 << 30, 1 << 20);
    o.memtable.memory_budget = ByteSize::b(1); // below one request
    let (ing, _) = Pipeline::open(&o, s.meta.clone(), s.segs.clone()).await.unwrap();
    let tasks: Vec<_> = (0..16u64)
        .map(|i| {
            let ing = ing.clone();
            tokio::spawn(async move { ing.submit(vec![start("alpha", 1, i)]).await })
        })
        .collect();
    let (mut ok, mut busy) = (0, 0);
    for t in tasks {
        match t.await.unwrap() {
            Ok(()) => ok += 1,
            Err(IngestError::Backpressure) => busy += 1,
            Err(e) => panic!("{e}"),
        }
    }
    assert_eq!((ok, busy), (1, 15), "only the first request fits the budget");
}

/// Child of `failed_flush_is_retried_after_the_configured_backoff` (runs only with FLUSH_RETRY_CHILD_DIR set, and
/// FORGE_FAILPOINT=flush_merge_panic@1): the first flush's merge panics. The flusher must survive it (an error,
/// not a dead task) and retry after the configured backoff (100 ms here, not the default 30 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_retry_child() {
    let Ok(dir) = std::env::var("FLUSH_RETRY_CHILD_DIR") else {
        return;
    };
    let root = Path::new(&dir);
    let s = stack(root);
    let fo = forge_ingest::flush::FlushOptions {
        backoff: forge_model::retry::Backoff {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(100),
        },
        ..Default::default()
    };
    let o = options(root, 16 << 10, 64 << 20);
    let (ing, _) = Pipeline::open_with(&o, s.meta.clone(), s.segs.clone(), fo)
        .await
        .unwrap();
    let tasks = ing.start_background();
    for i in 0..40u64 {
        let mut r = start("alpha", 1, i);
        r.inputs = Some(format!("{{\"pad\":\"{}\"}}", "x".repeat(1000)));
        ing.submit(vec![r]).await.unwrap();
    }
    let t0 = std::time::Instant::now();
    while s
        .meta
        .list_segments(&SegmentFilter::default())
        .await
        .unwrap()
        .is_empty()
    {
        assert!(t0.elapsed() < Duration::from_secs(5), "no flush within 5 s");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tasks.shutdown(Duration::from_secs(10)).await.unwrap().unwrap();
    println!("FLUSH_RETRY_CHILD_OK");
}

#[test]
fn failed_flush_is_retried_after_the_configured_backoff() {
    let tmp = tempfile::tempdir().unwrap();
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["flush_retry_child", "--exact", "--nocapture", "--test-threads=1"])
        .env("FLUSH_RETRY_CHILD_DIR", tmp.path())
        .env("FORGE_FAILPOINT", "flush_merge_panic@1")
        .output()
        .unwrap();
    let (stdout, stderr) = (
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success() && stdout.contains("FLUSH_RETRY_CHILD_OK"),
        "{}\n{stdout}\n{stderr}",
        out.status
    );
    assert!(
        stderr.contains("flush_merge_panic hit"),
        "the failpoint fired: {stderr}"
    );
}

/// `unflushed` never wraps: an insert's bytes are counted before a racing flush can freeze and release them.
#[test]
fn unflushed_bytes_never_wrap_under_concurrent_release() {
    use forge_ingest::memtable::{MemtableSet, accounted_bytes};
    use std::sync::atomic::{AtomicBool, Ordering};
    const N: u64 = 100_000;
    let m = Arc::new(MemtableSet::new(0, 1 << 40, Duration::from_secs(3600)));
    let b = batch_from_rows(&[start("alpha", 1, 1)]);
    let bytes = accounted_bytes(&b);
    let max = N as usize * bytes;
    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let (m, done) = (m.clone(), done.clone());
        std::thread::spawn(move || {
            for seq in 1..=N {
                m.insert(seq, b.clone(), bytes).unwrap();
            }
            done.store(true, Ordering::Release);
        })
    };
    let flusher = {
        let (m, done) = (m.clone(), done.clone());
        std::thread::spawn(move || {
            let mut worst = 0;
            while !done.load(Ordering::Acquire) {
                if let Some(f) = m.freeze() {
                    m.release(&f);
                }
                worst = worst.max(m.unflushed_bytes());
            }
            worst
        })
    };
    let mut worst = 0;
    while !done.load(Ordering::Acquire) {
        worst = worst.max(m.unflushed_bytes());
    }
    writer.join().unwrap();
    worst = worst.max(flusher.join().unwrap());
    assert!(worst <= max, "unflushed wrapped: {worst} > {max}");
    if let Some(f) = m.freeze() {
        m.release(&f);
    }
    assert_eq!(m.unflushed_bytes(), 0);
}

/// `mark_due` makes the active memtable due whatever `flush_interval` is: a flag, not a backdated start time (which
/// `Instant::checked_sub` cannot always represent; here it cannot at all), and the next memtable after `freeze` is
/// not due.
#[test]
fn mark_due_is_a_flag_cleared_by_freeze() {
    let m = forge_ingest::memtable::MemtableSet::new(0, 1 << 30, Duration::MAX);
    let row = |seq| {
        let b = batch_from_rows(&[start("alpha", 1, seq)]);
        let bytes = forge_ingest::memtable::accounted_bytes(&b);
        m.insert(seq, b, bytes).unwrap();
    };
    m.mark_due();
    row(1);
    assert!(
        !m.flush_due(std::time::Instant::now()),
        "mark_due on an empty memtable does nothing"
    );
    m.mark_due();
    assert!(m.flush_due(std::time::Instant::now()), "due right after mark_due");
    let f = m.freeze().unwrap();
    m.release(&f);
    row(2);
    assert!(!m.flush_due(std::time::Instant::now()), "the next memtable is not due");
}

/// L2: the flush merges in bounded batches (`FlushOptions::merge_chunk_bytes`), and the result is unchanged: one
/// segment per project holding exactly the one-batch merge of the frozen rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_merges_in_bounded_batches() {
    use forge_model::MemtableView;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let s = stack(root);
    let fo = forge_ingest::flush::FlushOptions {
        merge_chunk_bytes: 2 << 10,
        ..Default::default()
    };
    let o = options(root, 1 << 30, 64 << 20);
    let (ing, _) = Pipeline::open_with(&o, s.meta.clone(), s.segs.clone(), fo)
        .await
        .unwrap();
    ingest_spans(&ing, 0, 300, true).await;
    let want = rows_from_batch(&merge_versions(&ing.memtable().snapshot().batches, Tombstones::Keep).unwrap());
    let r = ing.flush_now().await.unwrap().unwrap();
    assert!(r.merged_batches > 1, "unbounded merge: {} batch(es)", r.merged_batches);
    assert_eq!(r.segments.len(), 3, "one segment per project");
    let mut got: Vec<SpanRow> = visible_raw(&s).await.iter().flat_map(rows_from_batch).collect();
    got.sort_by_key(forge_model::testing::physical_key);
    assert_eq!(got, want);
}
