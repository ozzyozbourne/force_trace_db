//! plans/phase-4 D7, §6.5: once a commit attempt has started, compaction never deletes its outputs. A commit that
//! lands and then reports errors is a success; one that fails without landing leaves orphans for GC. The injected
//! error is transient, so every case runs the commit's 3 attempts.
use forge_ingest::compact::{CompactCtx, job};
use forge_meta::{JobRequest, MetaError, Metastore, SegmentFilter, SqliteMetastore};
use forge_model::catalog::{HourBucket, JobKind, Level, SegmentState};
use forge_model::error::{Classify, ErrorKind};
use forge_model::row::SpanRow;
use forge_model::testing::{batch_from_rows, pid, sid, tid};
use forge_model::time::Clocks;
use forge_query::testing::{HookedMetastore, memory_store, write_flush};
use forge_storage::SegmentStore;
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use std::sync::Arc;

/// SQLITE_BUSY past the busy timeout: Transient.
fn busy() -> MetaError {
    MetaError::Sqlite(rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
        None,
    ))
}

async fn run(
    land: bool,
    failing_fails: usize,
) -> (Arc<dyn Metastore>, Arc<dyn SegmentStore>, Result<(), String>, usize) {
    assert_eq!(busy().kind(), ErrorKind::Transient);
    let inner: Arc<dyn Metastore> = Arc::new(SqliteMetastore::open_in_memory().unwrap());
    let store = memory_store();
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
    let rows = batch_from_rows(&rows);
    write_flush(
        inner.as_ref(),
        store.clone(),
        &rows,
        HourBucket(1),
        "test",
        1 << 30,
        1,
        10,
    )
    .await;
    // Every attempt (the real commit runs first if `land`) reports a transient error.
    let hooked = Arc::new(HookedMetastore::on_commit(inner.clone(), land, busy).failing_fails(failing_fails));
    let meta: Arc<dyn Metastore> = hooked.clone();
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
    let cx = CompactCtx::new(meta, store.clone(), Default::default(), Clocks::wall());
    let r = job::run_job(&cx, j).await.map(|_| ()).map_err(|e| e.to_string());
    (inner, store, r, hooked.commit_calls())
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
    let (meta, store, r, calls) = run(true, 0).await;
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(calls, 3, "3 attempts, then fail_compaction finds the job done");
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
    // Review 1 #4: the first fail_compaction fails too (transient); its retry still fails the job.
    for failing_fails in [0, 1] {
        commit_does_not_land(failing_fails).await;
    }
}

async fn commit_does_not_land(failing_fails: usize) {
    let (meta, store, r, calls) = run(false, failing_fails).await;
    assert!(r.is_err());
    assert_eq!(calls, 3);
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
