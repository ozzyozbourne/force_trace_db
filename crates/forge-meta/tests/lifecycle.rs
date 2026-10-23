//! Retention (plans/phase-4 D10, §2.5), purge and forget.
use forge_meta::*;
use forge_model::catalog::*;
use forge_model::ids::TraceId;
use forge_model::testing::{pid, segment_meta};

const H: i64 = 3_600_000_000_000;

fn seg(p: &str, level: Level, hour: i64, max_start: Option<i64>, nulls: u64, traces: u64) -> NewSegment {
    let meta = SegmentMeta {
        row_count: traces.max(nulls),
        min_start_ns: max_start,
        max_start_ns: max_start,
        start_time_nulls: nulls,
        ..segment_meta(p, level, HourBucket(hour))
    };
    let traces = (0..traces)
        .map(|i| {
            let mut t = [0u8; 16];
            t[..8].copy_from_slice(&i.to_be_bytes());
            TraceRange {
                trace_id: TraceId(t),
                row_start: i,
                row_count: 1,
            }
        })
        .collect();
    NewSegment {
        meta,
        traces,
        blobs: vec![format!("blobs/{p}/aa")],
    }
}

#[tokio::test]
async fn expire_whole_l1_hours_and_l0_rules() {
    let d = tempfile::tempdir().unwrap();
    let m = SqliteMetastore::open_path(&d.path().join("meta.db")).unwrap();
    let cutoff = 50 * H + 10; // hours < 50 expire whole; hour 50 holds the cutoff and stays
    let l1_old = seg("p", Level::L1, 10, Some(10 * H), 0, 25_000); // expires (chunked forget)
    let l1_old_orphan = seg("p", Level::L1, 10, None, 3, 3); // expires with its hour
    let l1_late_start = seg("p", Level::L1, 49, Some(60 * H), 0, 3); // sticky key edited later: expires with 49
    let l1_cut = seg("p", Level::L1, 50, Some(50 * H), 0, 3); // hour 50 ends after the cutoff: stays
    let l1_busy = seg("p", Level::L1, 20, Some(20 * H), 0, 3); // hour 20 has a compacting segment: skipped
    let mut l1_busy2 = seg("p", Level::L1, 20, None, 2, 2); // newer than l1_busy: a valid kind-B suffix
    (l1_busy2.meta.min_version, l1_busy2.meta.max_version) = (2, 2);
    let l0_old = seg("p", Level::L0, 5, Some(10 * H), 0, 3); // expires
    let l0_nulls = seg("p", Level::L0, 5, Some(10 * H), 1, 3); // null starts: kept
    let l0_new = seg("p", Level::L0, 5, Some(cutoff), 0, 3); // max_start == cutoff: kept
    let l0_nostart = seg("p", Level::L0, 5, None, 3, 3); // kept
    let other = seg("q", Level::L1, 10, Some(10 * H), 0, 3); // other project
    let all = [
        &l1_old,
        &l1_old_orphan,
        &l1_late_start,
        &l1_cut,
        &l1_busy,
        &l1_busy2,
        &l0_old,
        &l0_nulls,
        &l0_new,
        &l0_nostart,
        &other,
    ];
    m.commit_flush(FlushCommit {
        node_id: "n".into(),
        segments: all.iter().map(|s| (*s).clone()).collect(),
        flushed_wal_seq: 1,
        max_version: 1,
    })
    .await
    .unwrap();
    let job = m
        .begin_compaction(JobRequest {
            project_id: pid("p"),
            kind: JobKind::L1,
            inputs: vec![l1_busy2.meta.id],
        })
        .await
        .unwrap();
    assert!(job.is_some());
    let mut gone: Vec<SegmentId> = m
        .expire_segments(&pid("p"), cutoff, 100)
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.id)
        .collect();
    gone.sort();
    let mut want = vec![
        l1_old.meta.id,
        l1_old_orphan.meta.id,
        l1_late_start.meta.id,
        l0_old.meta.id,
    ];
    want.sort();
    assert_eq!(gone, want);
    // Rust == SQL (the compacting hour is the only difference: SQL skips it, `expires` is per segment).
    for s in all
        .iter()
        .filter(|s| s.meta.project_id == pid("p") && s.meta.partition_hour != HourBucket(20))
    {
        assert_eq!(s.meta.expires(cutoff), want.contains(&s.meta.id), "{:?}", s.meta);
    }
    assert!(
        l1_busy.meta.expires(cutoff),
        "skipped only because the hour is compacting"
    );
    assert!(
        m.expire_segments(&pid("p"), cutoff, 100).await.unwrap().is_empty(),
        "idempotent"
    );
    // The hour expires on the next pass once the job ends.
    m.reset_compactions(&[]).await.unwrap();
    assert_eq!(m.expire_segments(&pid("p"), cutoff, 100).await.unwrap().len(), 2);
    // max_hours bounds the L1 hours per call.
    let m2 = SqliteMetastore::open_in_memory().unwrap();
    let hours: Vec<NewSegment> = (0..5).map(|h| seg("p", Level::L1, h, Some(h * H), 0, 1)).collect();
    m2.commit_flush(FlushCommit {
        node_id: "n".into(),
        segments: hours,
        flushed_wal_seq: 1,
        max_version: 1,
    })
    .await
    .unwrap();
    let first = m2.expire_segments(&pid("p"), cutoff, 2).await.unwrap();
    assert_eq!(first.iter().map(|s| s.partition_hour.0).collect::<Vec<_>>(), vec![0, 1]);
    assert_eq!(m2.expire_segments(&pid("p"), cutoff, 100).await.unwrap().len(), 3);

    assert!(
        m.locate_trace(&pid("p"), l1_old.traces[0].trace_id)
            .await
            .unwrap()
            .iter()
            .all(|l| l.segment.id != l1_old.meta.id)
    );
    // Purge: grace not elapsed -> none; then all.
    assert!(
        m.purge_candidates(forge_model::time::now_ms() - 3_600_000, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(m.purge_candidates(i64::MAX, 10).await.unwrap().len(), 6);
    assert_eq!(m.segment_paths(&pid("p")).await.unwrap().len(), 10);
    // forget ignores live ids.
    let n = m
        .forget_segments(&[l1_old.meta.id, l0_old.meta.id, l1_cut.meta.id])
        .await
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(m.segment_paths(&pid("p")).await.unwrap().len(), 8);
    assert!(m.get_segment(l1_cut.meta.id).await.unwrap().is_some());
    assert!(m.segment_traces(l1_old.meta.id).await.unwrap().is_empty());
}
