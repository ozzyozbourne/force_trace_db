use forge_ingest::lifecycle::{GcOptions, run_gc};
use forge_meta::*;
use forge_model::catalog::*;
use forge_model::ids::TraceId;
use forge_model::testing::{pid, segment_meta};
use forge_storage::DurableLocalFs;
use object_store::{ObjectStoreExt, path::Path};
use std::time::{Duration, SystemTime};

fn age(root: &std::path::Path, key: &str, hours: u64) {
    let f = std::fs::File::options().write(true).open(root.join(key)).unwrap();
    f.set_modified(SystemTime::now() - Duration::from_secs(hours * 3600))
        .unwrap();
}

#[tokio::test]
async fn gc_end_to_end_on_local_fs() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("data");
    std::fs::create_dir_all(&root).unwrap();
    let store = DurableLocalFs::new(root.clone()).unwrap();
    let m = SqliteMetastore::open_path(&d.path().join("meta.db")).unwrap();
    let h = 3_600_000_000_000i64;
    let now_ns = forge_model::time::now_ms() * 1_000_000;
    let mk = |max_start: i64, blob: &str| {
        let meta = SegmentMeta {
            min_start_ns: Some(max_start),
            max_start_ns: Some(max_start),
            start_time_nulls: 0,
            fts_byte_size: Some(1),
            created_at_ms: 1,
            ..segment_meta("p", Level::L0, HourBucket(100))
        };
        NewSegment {
            traces: vec![TraceRange {
                trace_id: TraceId([1; 16]),
                row_start: 0,
                row_count: 1,
            }],
            meta,
            blobs: vec![format!("blobs/p/{blob}")],
        }
    };
    let old = mk(now_ns - 72 * h, "b_old"); // 3 days old, retention 1 day
    let live = mk(now_ns, "b_live");
    for s in [&old, &live] {
        store.put(&Path::from(s.meta.path.as_str()), "x".into()).await.unwrap();
        store
            .put(&Path::from(paths::fts_path(&s.meta.path)), "x".into())
            .await
            .unwrap();
    }
    for b in ["b_old", "b_live", "b_orphan_old", "b_orphan_new"] {
        store
            .put(&Path::from(format!("blobs/p/{b}")), "x".into())
            .await
            .unwrap();
        age(&root, &format!("blobs/p/{b}"), if b == "b_orphan_new" { 1 } else { 48 });
    }
    let orphan_seg = "segments/p/L0/2026-01-01/00/01JAAAAAAAAAAAAAAAAAAAAAAA.vortex";
    let young_orphan = "segments/p/L0/2026-01-01/00/01JBBBBBBBBBBBBBBBBBBBBBBB.vortex";
    let stray = "segments/p/README";
    for k in [orphan_seg, young_orphan, stray] {
        store.put(&Path::from(k), "x".into()).await.unwrap();
    }
    age(&root, orphan_seg, 30);
    age(&root, stray, 30);
    m.commit_flush(FlushCommit {
        node_id: "n".into(),
        segments: vec![old.clone(), live.clone()],
        flushed_wal_seq: 1,
        max_version: 1,
    })
    .await
    .unwrap();
    m.set_retention_days(&pid("p"), Some(1)).await.unwrap();

    let mut o = GcOptions {
        gc_grace: Duration::from_secs(3600),
        orphan_grace: Duration::from_secs(86400),
        sweep_blobs: true,
        dry_run: true,
    };
    let now = forge_model::time::now_ms();
    let dry = run_gc(&m, &store, now, now, &o, None).await.unwrap();
    assert_eq!(
        (dry.expired_segments, dry.orphan_segment_objects, dry.orphan_blobs),
        (1, 1, 1),
        "{dry:?}"
    );
    assert_eq!(
        m.list_segments(&SegmentFilter::default()).await.unwrap().len(),
        2,
        "dry run changes nothing"
    );

    o.dry_run = false;
    let r1 = run_gc(&m, &store, now, now, &o, None).await.unwrap();
    assert_eq!(
        (r1.expired_segments, r1.purged_segments),
        (1, 0),
        "grace not over: {r1:?}"
    );
    assert_eq!(r1.orphan_blobs, 1);
    assert!(
        store.head(&Path::from(old.meta.path.as_str())).await.is_ok(),
        "kept for in-flight readers"
    );
    assert!(
        store.head(&Path::from("blobs/p/b_old")).await.is_ok(),
        "still referenced by the deleted row"
    );
    // Two hours later: purge; then the next sweep takes b_old.
    let later = now + 2 * 3_600_000;
    let r2 = run_gc(&m, &store, later, later, &o, None).await.unwrap();
    assert_eq!((r2.purged_segments, r2.deleted_objects), (1, 2), "{r2:?}");
    assert!(store.head(&Path::from(old.meta.path.as_str())).await.is_err());
    assert_eq!(r2.orphan_blobs, 1, "b_old unreferenced after forget");
    for k in [
        live.meta.path.clone(),
        paths::fts_path(&live.meta.path),
        young_orphan.into(),
        stray.into(),
        "blobs/p/b_live".into(),
        "blobs/p/b_orphan_new".into(),
    ] {
        assert!(store.head(&Path::from(k.as_str())).await.is_ok(), "{k} must survive");
    }
    for k in [orphan_seg, "blobs/p/b_orphan_old", "blobs/p/b_old"] {
        assert!(store.head(&Path::from(k)).await.is_err(), "{k} must be gone");
    }
    // Idempotent; purge re-run after a crash between object delete and forget: NotFound = success.
    let r3 = run_gc(&m, &store, later, later, &o, None).await.unwrap();
    assert_eq!(
        (
            r3.expired_segments,
            r3.purged_segments,
            r3.orphan_blobs,
            r3.orphan_segment_objects
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(
        forge_ingest::lifecycle::delete_all(&store, vec![Path::from(orphan_seg)])
            .await
            .unwrap(),
        0
    );
}

/// D10: with the data clock 2 days ahead, a fresh segment expires, but its objects wait for the wall gc_grace.
#[tokio::test]
async fn data_clock_expires_wall_clock_purges() {
    let d = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(d.path().join("data")).unwrap();
    let store = DurableLocalFs::new(d.path().join("data")).unwrap();
    let m = SqliteMetastore::open_path(&d.path().join("meta.db")).unwrap();
    let now = forge_model::time::now_ms();
    let s = NewSegment {
        meta: SegmentMeta {
            min_start_ns: Some(now * 1_000_000),
            max_start_ns: Some(now * 1_000_000),
            start_time_nulls: 0,
            created_at_ms: now,
            ..segment_meta("p", Level::L0, HourBucket(100))
        },
        traces: vec![TraceRange {
            trace_id: TraceId([1; 16]),
            row_start: 0,
            row_count: 1,
        }],
        blobs: vec![],
    };
    store.put(&Path::from(s.meta.path.as_str()), "x".into()).await.unwrap();
    m.commit_flush(FlushCommit {
        node_id: "n".into(),
        segments: vec![s.clone()],
        flushed_wal_seq: 1,
        max_version: 1,
    })
    .await
    .unwrap();
    m.set_retention_days(&pid("p"), Some(1)).await.unwrap();
    let o = GcOptions {
        gc_grace: Duration::from_secs(3600),
        orphan_grace: Duration::from_secs(86400),
        sweep_blobs: true,
        dry_run: false,
    };
    let lock = tokio::sync::RwLock::new(());
    let r = run_gc(&m, &store, now + 2 * 86_400_000, now, &o, Some(&lock))
        .await
        .unwrap();
    assert_eq!((r.expired_segments, r.purged_segments), (1, 0), "{r:?}");
    assert!(store.head(&Path::from(s.meta.path.as_str())).await.is_ok());
    let r = run_gc(&m, &store, now + 2 * 86_400_000, now + 2 * 3_600_000, &o, Some(&lock))
        .await
        .unwrap();
    assert_eq!(r.purged_segments, 1, "{r:?}");
}
