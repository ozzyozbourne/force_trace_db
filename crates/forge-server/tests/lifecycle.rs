//! plans/phase-4 §6.5: the lifecycle task compacts and collects in process; zero intervals start no ticker;
//! `Forge::open` resets jobs of an earlier process.
use forge_ingest::Ingest;
use forge_meta::{JobRequest, SegmentFilter};
use forge_model::catalog::{JobKind, Level, SegmentState};
use forge_model::config::ForgeConfig;
use forge_model::testing::{Rng, pid, random_row, tid};
use forge_query::runs::{RunsParams, RunsRequest};
use forge_query::trace::{TraceFormat, TraceRequest};
use std::time::Duration;

fn cfg(dir: &std::path::Path, interval: Duration, gc_interval: Duration) -> ForgeConfig {
    let mut c = ForgeConfig::rooted_at(dir);
    c.compaction.interval = interval;
    c.compaction.l0_trigger = 1;
    c.compaction.gc_interval = gc_interval;
    c.compaction.gc_grace = Duration::from_secs(1);
    c
}

async fn six_flushes(f: &forge_server::Forge) {
    let mut r = Rng(3);
    for flush in 0..6u64 {
        let rows = (0..20)
            .map(|s| {
                let mut x = random_row(&mut r, "p", s % 4, s + flush * 3, 0);
                x.deleted = false;
                x
            })
            .collect();
        f.ingest.submit(rows).await.unwrap();
        f.ingest.flush_now().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_compacts_and_purges() {
    let d = tempfile::tempdir().unwrap();
    let f = forge_server::Forge::open(cfg(d.path(), Duration::from_millis(200), Duration::from_secs(1)))
        .await
        .unwrap();
    six_flushes(&f).await;
    let treq = |t| TraceRequest {
        project: pid("p"),
        trace: tid(t),
        payloads: true,
        format: TraceFormat::Tree,
    };
    let runs = RunsRequest::parse(RunsParams::default(), pid("p")).unwrap();
    let before = (
        f.query.trace_json(&treq(1)).await.unwrap(),
        f.query.runs_json(&runs).await.unwrap(),
    );
    let l0_paths: Vec<String> = f
        .meta
        .list_segments(&SegmentFilter::default())
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.path)
        .collect();
    assert_eq!(l0_paths.len(), 6);
    let lc = forge_server::lifecycle::start(f.clone()).await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let segs = f.meta.list_segments(&SegmentFilter::default()).await.unwrap();
        let gone = l0_paths.iter().all(|p| !d.path().join("data").join(p).exists());
        if !segs.is_empty() && segs.iter().all(|s| s.level == Level::L1) && gone {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "not compacted and purged in 20 s: {segs:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let after = (
        f.query.trace_json(&treq(1)).await.unwrap(),
        f.query.runs_json(&runs).await.unwrap(),
    );
    assert_eq!(before, after, "trace JSON and runs page are byte-identical");
    lc.shutdown(Duration::from_secs(5)).await;
}

/// RA M1: zero intervals create no ticker (tokio's interval panics on zero); the server serves and stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_intervals_start_no_ticker() {
    let d = tempfile::tempdir().unwrap();
    let f = forge_server::Forge::open(cfg(d.path(), Duration::ZERO, Duration::ZERO))
        .await
        .unwrap();
    let lc = forge_server::lifecycle::start(f.clone()).await.unwrap();
    six_flushes(&f).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let segs = f.meta.list_segments(&SegmentFilter::default()).await.unwrap();
    assert_eq!(
        segs.iter().filter(|s| s.level == Level::L0).count(),
        6,
        "no compaction ran"
    );
    assert!(
        f.query
            .trace_json(&TraceRequest {
                project: pid("p"),
                trace: tid(1),
                payloads: false,
                format: TraceFormat::Flat,
            })
            .await
            .is_ok()
    );
    lc.shutdown(Duration::from_secs(5)).await;
}

/// D2: a job left running by an earlier process is failed at open and its inputs are live again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_resets_running_jobs() {
    let d = tempfile::tempdir().unwrap();
    let c = cfg(d.path(), Duration::ZERO, Duration::ZERO);
    {
        let f = forge_server::Forge::open(c.clone()).await.unwrap();
        six_flushes(&f).await;
        let ids = f.meta.list_segments(&SegmentFilter::default()).await.unwrap();
        let job = f
            .meta
            .begin_compaction(JobRequest {
                project_id: pid("p"),
                kind: JobKind::L0,
                inputs: ids.iter().map(|s| s.id).collect(),
            })
            .await
            .unwrap();
        assert!(job.is_some());
        f.ingest.start_background().shutdown(Duration::from_secs(5)).await.ok();
    }
    let f = forge_server::Forge::open(c).await.unwrap();
    let segs = f.meta.list_segments(&SegmentFilter::default()).await.unwrap();
    assert!(segs.iter().all(|s| s.state == SegmentState::Live), "{segs:?}");
}
