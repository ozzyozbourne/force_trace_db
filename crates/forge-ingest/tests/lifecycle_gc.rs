use forge_ingest::blob::BlobGuard;
use forge_ingest::lifecycle::{GcOptions, run_gc};
use forge_meta::*;
use forge_model::catalog::*;
use forge_model::ids::TraceId;
use forge_model::testing::{pid, segment_meta};
use forge_model::time::Clocks;
use forge_storage::DurableLocalFs;
use object_store::{ObjectMeta, ObjectStoreExt, path::Path};
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
    let dry = run_gc(&m, &store, &Clocks::at(now, now), &o, &BlobGuard::default())
        .await
        .unwrap();
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
    let r1 = run_gc(&m, &store, &Clocks::at(now, now), &o, &BlobGuard::default())
        .await
        .unwrap();
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
    let r2 = run_gc(&m, &store, &Clocks::at(later, later), &o, &BlobGuard::default())
        .await
        .unwrap();
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
    let r3 = run_gc(&m, &store, &Clocks::at(later, later), &o, &BlobGuard::default())
        .await
        .unwrap();
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
        forge_storage::delete_all(&store, vec![Path::from(orphan_seg)])
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
    let guard = BlobGuard::default();
    let r = run_gc(&m, &store, &Clocks::at(now + 2 * 86_400_000, now), &o, &guard)
        .await
        .unwrap();
    assert_eq!((r.expired_segments, r.purged_segments), (1, 0), "{r:?}");
    assert!(store.head(&Path::from(s.meta.path.as_str())).await.is_ok());
    let r = run_gc(
        &m,
        &store,
        &Clocks::at(now + 2 * 86_400_000, now + 2 * 3_600_000),
        &o,
        &guard,
    )
    .await
    .unwrap();
    assert_eq!(r.purged_segments, 1, "{r:?}");
}

/// RA m11 / RD #13: a blob that `put_blobs` writes or reuses after the sweep began is kept, even when it is an
/// old unreferenced candidate; without such a put it is deleted.
#[tokio::test]
async fn blob_guard_keeps_blobs_put_during_the_sweep() {
    use forge_ingest::blob::{BlobGuard, put_blobs};
    let store = object_store::memory::InMemory::new();
    let (a, b) = (Path::from("blobs/p/a"), Path::from("blobs/p/b"));
    for p in [&a, &b] {
        store.put(p, "x".into()).await.unwrap();
    }
    let g = BlobGuard::default();
    g.clear_recent(); // the sweep starts; it lists and heads a and b as candidates ...
    let puts = std::collections::HashMap::from([(a.to_string(), bytes::Bytes::from("x"))]);
    put_blobs(puts, &store, &g).await.unwrap(); // ... ingest reuses a ...
    let gone = g
        .delete_unless_recent(&store, vec![a.clone(), b.clone()])
        .await
        .unwrap();
    assert_eq!(gone, vec![b.clone()]);
    assert!(store.head(&a).await.is_ok(), "a was reused during the sweep");
    assert!(store.head(&b).await.is_err());
}

/// A store whose next `PutMode::Create` of `path` reports AlreadyExists without writing (the blob was swept
/// between the put and the head).
#[derive(Debug)]
struct VanishingBlob {
    inner: object_store::memory::InMemory,
    path: Path,
    fired: std::sync::atomic::AtomicBool,
}

impl std::fmt::Display for VanishingBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VanishingBlob")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for VanishingBlob {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if *location == self.path
            && matches!(opts.mode, object_store::PutMode::Create)
            && !self.fired.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(object_store::Error::AlreadyExists {
                path: location.to_string(),
                source: "test".into(),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: object_store::CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// RD #13: AlreadyExists, then `head` NotFound -> the blob is written again.
#[tokio::test]
async fn reused_blob_gone_before_head_is_put_again() {
    let path = Path::from("blobs/p/c");
    let store = VanishingBlob {
        inner: object_store::memory::InMemory::new(),
        path: path.clone(),
        fired: Default::default(),
    };
    let puts = std::collections::HashMap::from([(path.to_string(), bytes::Bytes::from("x"))]);
    forge_ingest::blob::put_blobs(puts, &store, &Default::default())
        .await
        .unwrap();
    assert!(store.head(&path).await.is_ok());
}
