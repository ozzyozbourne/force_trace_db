//! Plan §5 step 8, reduced: the lifecycle task compacts L0s into L1 in process; trace JSON unchanged; GC purges.
use forge_ingest::Ingest;
use forge_meta::SegmentFilter;
use forge_model::catalog::Level;
use forge_model::config::ForgeConfig;
use forge_model::testing::{Rng, pid, random_row};
use forge_query::trace::{TraceFormat, TraceRequest};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_compacts_and_purges() {
    let d = tempfile::tempdir().unwrap();
    let mut cfg = ForgeConfig::rooted_at(d.path());
    cfg.compaction.interval = Duration::from_millis(100);
    cfg.compaction.l0_trigger = 3;
    cfg.compaction.gc_interval = Duration::from_millis(300);
    cfg.compaction.gc_grace = Duration::from_secs(1);
    let f = forge_server::Forge::open(cfg).await.unwrap();
    let mut r = Rng(3);
    for flush in 0..6u64 {
        let rows = (0..20)
            .map(|s| {
                let mut x = random_row(&mut r, "p", s % 4, s + flush * 3, 0);
                x.deleted = false;
                x.dotted_order = None;
                x
            })
            .collect();
        f.ingest.submit(rows).await.unwrap();
        f.ingest.flush_now().await.unwrap();
    }
    let req = |t| TraceRequest {
        project: pid("p"),
        trace: forge_model::testing::tid(t),
        payloads: true,
        format: TraceFormat::Flat,
    };
    let before = f.query.trace_json(&req(1)).await.unwrap();
    let l0_paths: Vec<String> = f
        .meta
        .list_segments(&SegmentFilter::default())
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.path)
        .collect();
    assert_eq!(l0_paths.len(), 6);
    assert!(l0_paths.iter().all(|p| d.path().join("data").join(p).exists()));
    let lc = forge_server::lifecycle::start(f.clone()).await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let segs = f.meta.list_segments(&SegmentFilter::default()).await.unwrap();
        if segs.iter().all(|s| s.level == Level::L1) && !segs.is_empty() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "no compaction: {segs:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let after = f.query.trace_json(&req(1)).await.unwrap();
    let strip = |b: &[u8]| {
        let mut v: serde_json::Value = serde_json::from_slice(b).unwrap();
        for s in v["spans"].as_array_mut().into_iter().flatten() {
            s.as_object_mut().unwrap().remove("dotted_order");
        }
        v
    };
    assert_eq!(strip(&before), strip(&after));
    loop {
        if l0_paths.iter().all(|p| !d.path().join("data").join(p).exists()) {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "inputs not purged");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    lc.shutdown(Duration::from_secs(5)).await;
}
