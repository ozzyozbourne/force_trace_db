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

/// One L0 of project p (hour 100) with one row started at `start_ns`.
fn l0(start_ns: i64, created_at_ms: i64, blobs: Vec<String>) -> NewSegment {
    NewSegment {
        meta: SegmentMeta {
            min_start_ns: Some(start_ns),
            max_start_ns: Some(start_ns),
            start_time_nulls: 0,
            created_at_ms,
            ..segment_meta("p", Level::L0, HourBucket(100))
        },
        traces: vec![TraceRange {
            trace_id: TraceId([1; 16]),
            row_start: 0,
            row_count: 1,
        }],
        blobs,
    }
}

fn flush(segments: Vec<NewSegment>) -> FlushCommit {
    FlushCommit {
        node_id: "n".into(),
        segments,
        flushed_wal_seq: 1,
        max_version: 1,
    }
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
        let mut s = l0(max_start, 1, vec![format!("blobs/p/{blob}")]);
        s.meta.fts_byte_size = Some(1);
        s
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
    m.commit_flush(flush(vec![old.clone(), live.clone()])).await.unwrap();
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
    let s = l0(now * 1_000_000, now, vec![]);
    store.put(&Path::from(s.meta.path.as_str()), "x".into()).await.unwrap();
    m.commit_flush(flush(vec![s.clone()])).await.unwrap();
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

/// RA m11 / RD #13 / T5: a blob that `put_blobs` writes (fresh `Create`: another deleter removed it after the
/// listing) or reuses after the sweep began is kept, even when it is an old unreferenced candidate; without such a
/// put it is deleted. The recording ends with the sweep.
#[tokio::test]
async fn blob_guard_keeps_blobs_put_during_the_sweep() {
    use forge_ingest::blob::{BlobGuard, put_blobs};
    let store = object_store::memory::InMemory::new();
    let (a, b, c) = (
        Path::from("blobs/p/a"),
        Path::from("blobs/p/b"),
        Path::from("blobs/p/c"),
    );
    for p in [&a, &b, &c] {
        store.put(p, "x".into()).await.unwrap();
    }
    let g = BlobGuard::default();
    let rec = g.begin_sweep().await; // the sweep starts; it lists and heads a, b and c as candidates ...
    store.delete(&c).await.unwrap(); // ... another deleter removes c ...
    let puts = std::collections::HashMap::from([
        (a.to_string(), bytes::Bytes::from("x")),
        (c.to_string(), bytes::Bytes::from("x")),
    ]);
    put_blobs(puts, &store, &g).await.unwrap(); // ... ingest reuses a and writes c afresh ...
    let gone = g
        .delete_unless_recent(&store, vec![a.clone(), b.clone(), c.clone()])
        .await
        .unwrap();
    assert_eq!(gone, vec![b.clone()]);
    assert!(store.head(&a).await.is_ok(), "a was reused during the sweep");
    assert!(store.head(&c).await.is_ok(), "c was written during the sweep");
    assert!(store.head(&b).await.is_err());
    drop(rec);
    assert_eq!(g.recorded(), None, "recording ends with the sweep");
}

/// Review 1 #2: with no sweep running (GC off), `put_blobs` records nothing.
#[tokio::test]
async fn blob_guard_records_nothing_without_a_sweep() {
    let store = object_store::memory::InMemory::new();
    let g = BlobGuard::default();
    for i in 0..10 {
        let puts = std::collections::HashMap::from([(format!("blobs/p/{i}"), bytes::Bytes::from("x"))]);
        forge_ingest::blob::put_blobs(puts, &store, &g).await.unwrap();
    }
    assert_eq!(g.recorded(), None);
}

/// Review 1 #1: a `put_blobs` already running when the sweep starts (it reuses an old, unreferenced blob and is
/// parked in its HEAD) must keep that blob: the sweep waits for it, so the blob is refreshed before the listing.
#[tokio::test]
async fn blob_put_in_flight_at_sweep_start_is_kept() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("data");
    std::fs::create_dir_all(&root).unwrap();
    let x = Path::from("blobs/p/x");
    let local = DurableLocalFs::new(root.clone()).unwrap();
    local.put(&x, "x".into()).await.unwrap();
    age(&root, x.as_ref(), 48);
    let store = std::sync::Arc::new(TestStore {
        park_head: Some(x.clone()),
        ..TestStore::new(std::sync::Arc::new(local))
    });
    let m = SqliteMetastore::open_path(&d.path().join("meta.db")).unwrap();
    let g = std::sync::Arc::new(BlobGuard::default());
    let put = {
        let (store, g, x) = (store.clone(), g.clone(), x.clone());
        tokio::spawn(async move {
            let puts = std::collections::HashMap::from([(x.to_string(), bytes::Bytes::from("x"))]);
            forge_ingest::blob::put_blobs(puts, store.as_ref(), &g).await.unwrap();
        })
    };
    store.parked.notified().await; // the put is between AlreadyExists and its HEAD
    let o = GcOptions {
        gc_grace: Duration::from_secs(3600),
        orphan_grace: Duration::from_secs(86400),
        sweep_blobs: true,
        dry_run: false,
    };
    let now = forge_model::time::now_ms();
    let r = run_gc(&m, store.as_ref(), &Clocks::at(now, now), &o, &g).await.unwrap();
    put.await.unwrap();
    assert_eq!(r.orphan_blobs, 0, "{r:?}");
    assert!(
        store.head(&x).await.is_ok(),
        "blob reused by an in-flight put_blobs was swept"
    );
}

/// T3: more than one `referenced_blobs` chunk (500 paths) of old, referenced blobs: every one survives.
#[tokio::test]
async fn referenced_blobs_beyond_one_chunk_survive() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("data");
    std::fs::create_dir_all(&root).unwrap();
    let store = DurableLocalFs::new(root.clone()).unwrap();
    let m = SqliteMetastore::open_path(&d.path().join("meta.db")).unwrap();
    let blobs: Vec<String> = (0..501).map(|i| format!("blobs/p/b{i:04}")).collect();
    for b in &blobs {
        store.put(&Path::from(b.as_str()), "x".into()).await.unwrap();
        age(&root, b, 48);
    }
    let now = forge_model::time::now_ms();
    m.commit_flush(flush(vec![l0(now * 1_000_000, now, blobs.clone())]))
        .await
        .unwrap();
    let o = GcOptions {
        gc_grace: Duration::from_secs(3600),
        orphan_grace: Duration::from_secs(86400),
        sweep_blobs: true,
        dry_run: false,
    };
    let r = run_gc(&m, &store, &Clocks::at(now, now), &o, &BlobGuard::default())
        .await
        .unwrap();
    assert_eq!(r.orphan_blobs, 0, "{r:?}");
    for b in &blobs {
        assert!(store.head(&Path::from(b.as_str())).await.is_ok(), "{b} is referenced");
    }
}

/// Delegates to `inner`. `vanish`: the next `PutMode::Create` of that path reports AlreadyExists without writing
/// (the blob was swept between the put and the head). `park_head`: the first HEAD of that path signals `parked`
/// and waits until a later HEAD of it ran (at most 1 s).
#[derive(Debug)]
struct TestStore {
    inner: std::sync::Arc<dyn object_store::ObjectStore>,
    vanish: Option<Path>,
    fired: std::sync::atomic::AtomicBool,
    park_head: Option<Path>,
    heads: std::sync::atomic::AtomicUsize,
    parked: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl TestStore {
    fn new(inner: std::sync::Arc<dyn object_store::ObjectStore>) -> Self {
        Self {
            inner,
            vanish: None,
            fired: Default::default(),
            park_head: None,
            heads: Default::default(),
            parked: Default::default(),
            release: Default::default(),
        }
    }
}

impl std::fmt::Display for TestStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestStore")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for TestStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if self.vanish.as_ref() == Some(location)
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
        if options.head && self.park_head.as_ref() == Some(location) {
            if self.heads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                self.parked.notify_one();
                let _ = tokio::time::timeout(Duration::from_secs(1), self.release.notified()).await;
            } else {
                let r = self.inner.get_opts(location, options).await;
                self.release.notify_one();
                return r;
            }
        }
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
    let store = TestStore {
        vanish: Some(path.clone()),
        ..TestStore::new(std::sync::Arc::new(object_store::memory::InMemory::new()))
    };
    let puts = std::collections::HashMap::from([(path.to_string(), bytes::Bytes::from("x"))]);
    forge_ingest::blob::put_blobs(puts, &store, &Default::default())
        .await
        .unwrap();
    assert!(store.head(&path).await.is_ok());
}

/// Review 1 #5: the purge dry run counts every candidate, not only the first batch of 1000.
#[tokio::test]
async fn purge_dry_run_counts_past_one_batch() {
    let d = tempfile::tempdir().unwrap();
    let store = object_store::memory::InMemory::new();
    let m = SqliteMetastore::open_path(&d.path().join("meta.db")).unwrap();
    let now = forge_model::time::now_ms();
    let old_ns = (now - 3 * 86_400_000) * 1_000_000;
    let segs: Vec<NewSegment> = (0..1001)
        .map(|_| {
            let mut s = l0(old_ns, 1, vec![]);
            s.meta.byte_size = 10;
            s
        })
        .collect();
    m.commit_flush(flush(segs)).await.unwrap();
    m.set_retention_days(&pid("p"), Some(1)).await.unwrap();
    let mut o = GcOptions {
        gc_grace: Duration::from_secs(3600),
        orphan_grace: Duration::from_secs(86400),
        sweep_blobs: true,
        dry_run: false,
    };
    let g = BlobGuard::default();
    let r = run_gc(&m, &store, &Clocks::at(now, now), &o, &g).await.unwrap();
    assert_eq!(r.expired_segments, 1001);
    o.dry_run = true;
    let later = now + 2 * 3_600_000;
    let r = run_gc(&m, &store, &Clocks::at(later, later), &o, &g).await.unwrap();
    assert_eq!((r.purged_segments, r.freed_bytes), (1001, 10_010), "{r:?}");
}

/// Review 2 R2-1: at orphan_grace = 1 h, a blob reused 10 s before its 1 h mark is refreshed (refresh age
/// orphan_grace / 4), so a sweep 15 s later, before its row is flushed, keeps it.
#[tokio::test]
async fn blob_reused_near_the_grace_survives_a_sweep_before_its_flush() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("data");
    std::fs::create_dir_all(&root).unwrap();
    let store = DurableLocalFs::new(root.clone()).unwrap();
    let m = SqliteMetastore::open_path(&d.path().join("meta.db")).unwrap();
    let x = Path::from("blobs/p/x");
    store.put(&x, "x".into()).await.unwrap();
    let f = std::fs::File::options()
        .write(true)
        .open(root.join(x.as_ref()))
        .unwrap();
    f.set_modified(SystemTime::now() - Duration::from_secs(3590)).unwrap();
    let cfg = forge_model::config::CompactionConfig {
        orphan_grace: Duration::from_secs(3600),
        ..Default::default()
    };
    let g = BlobGuard::with_refresh(cfg.blob_refresh());
    let puts = std::collections::HashMap::from([(x.to_string(), bytes::Bytes::from("x"))]);
    forge_ingest::blob::put_blobs(puts, &store, &g).await.unwrap();
    let o = GcOptions {
        sweep_blobs: true,
        ..GcOptions::from_config(&cfg)
    };
    let later = forge_model::time::now_ms() + 15_000; // before a 30 s flush
    let r = run_gc(&m, &store, &Clocks::at(later, later), &o, &g).await.unwrap();
    assert_eq!(r.orphan_blobs, 0, "{r:?}");
    assert!(
        store.head(&x).await.is_ok(),
        "reused blob swept before its row was flushed"
    );
}
