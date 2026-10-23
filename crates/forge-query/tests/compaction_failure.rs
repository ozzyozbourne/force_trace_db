//! plans/phase-4 D7, §6.5: once a commit attempt has started, compaction never deletes its outputs. A commit that
//! lands and then reports errors is a success; one that fails without landing leaves orphans for GC.
use forge_ingest::compact::{CompactCtx, job};
use forge_meta::{JobRequest, MetaError, Metastore, SegmentFilter, SqliteMetastore};
use forge_model::catalog::{HourBucket, JobKind, Level, SegmentState};
use forge_model::config::StorageConfig;
use forge_model::merge::Cancel;
use forge_model::row::SpanRow;
use forge_model::testing::{batch_from_rows, pid, sid, tid};
use forge_query::testing::{HookedMetastore, write_flush};
use forge_storage::{SegmentStore, VortexSegmentStore, build_object_store};
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

async fn run(land: bool) -> (Arc<dyn Metastore>, Arc<dyn SegmentStore>, Result<(), String>, usize) {
    let inner: Arc<dyn Metastore> = Arc::new(SqliteMetastore::open_in_memory().unwrap());
    let store: Arc<dyn SegmentStore> = Arc::new(VortexSegmentStore::new(
        build_object_store(&StorageConfig {
            url: "memory://".into(),
        })
        .unwrap(),
    ));
    let rows: Vec<SpanRow> = (0..5)
        .map(|t| SpanRow {
            project_id: Some(pid("p")),
            trace_id: tid(t),
            span_id: sid(1),
            start_time_ns: Some(1_700_000_000_000_000_000 + t as i64),
            version: t + 1,
            ..Default::default()
        })
        .collect();
    write_flush(
        inner.as_ref(),
        store.as_ref(),
        &batch_from_rows(&rows),
        HourBucket(1),
        1 << 30,
        1,
        10,
    )
    .await;
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    // Every attempt (the real commit runs first if `land`) reports a transient-looking error.
    let meta: Arc<dyn Metastore> = Arc::new(HookedMetastore::on_commit(
        inner.clone(),
        land,
        Box::new(move |_| {
            c.fetch_add(1, Ordering::SeqCst);
            Err(MetaError::Conflict("injected commit error".into()))
        }),
    ));
    let l0 = inner.list_segments(&SegmentFilter::default()).await.unwrap();
    let j = meta
        .begin_compaction(JobRequest {
            project_id: pid("p"),
            kind: JobKind::L0,
            inputs: l0.iter().map(|s| s.id).collect(),
        })
        .await
        .unwrap()
        .unwrap();
    let cx = CompactCtx {
        meta: meta.clone(),
        segments: store.clone(),
        cfg: Default::default(),
        data_clock: forge_model::time::wall_clock(),
        next_project: Default::default(),
        cancel: Cancel::new(),
    };
    let r = job::run_job(&cx, j).await.map(|_| ()).map_err(|e| e.to_string());
    (inner, store, r, calls.load(Ordering::SeqCst))
}

async fn l1_objects(store: &dyn SegmentStore) -> Vec<object_store::path::Path> {
    store
        .object_store()
        .list(Some(&object_store::path::Path::from("segments/p/L1")))
        .map_ok(|m| m.location)
        .try_collect()
        .await
        .unwrap()
}

#[tokio::test]
async fn commit_lands_then_errors_is_success() {
    let (meta, store, r, calls) = run(true).await;
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(calls, 1);
    let segs = meta.list_segments(&SegmentFilter::default()).await.unwrap();
    assert!(
        !segs.is_empty()
            && segs
                .iter()
                .all(|s| s.level == Level::L1 && s.state == SegmentState::Live)
    );
    for s in &segs {
        let p = object_store::path::Path::from(s.path.as_str());
        assert!(
            store.object_store().head(&p).await.is_ok(),
            "visible output {} has no object",
            s.path
        );
    }
}

#[tokio::test]
async fn commit_that_does_not_land_leaves_orphans() {
    let (meta, store, r, _) = run(false).await;
    assert!(r.is_err());
    let segs = meta.list_segments(&SegmentFilter::default()).await.unwrap();
    assert!(
        segs.iter()
            .all(|s| s.level == Level::L0 && s.state == SegmentState::Live),
        "inputs live again"
    );
    assert!(
        !l1_objects(store.as_ref()).await.is_empty(),
        "outputs are left for orphan GC, not deleted"
    );
}
