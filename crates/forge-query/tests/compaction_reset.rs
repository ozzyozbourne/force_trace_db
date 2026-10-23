//! Review 2 R2-6: `reset_dead_jobs` never resets a job begun by this process before it is registered (the begin
//! lock closes the window between the begin's commit and the registration).
use forge_ingest::compact::{CompactCtx, reset_dead_jobs, run_next};
use forge_meta::{Metastore, SegmentFilter, SqliteMetastore};
use forge_model::catalog::{HourBucket, Level};
use forge_model::config::CompactionConfig;
use forge_model::row::SpanRow;
use forge_model::testing::{batch_from_rows, pid, sid, tid};
use forge_model::time::Clocks;
use forge_query::testing::{HookedMetastore, memory_store, write_flush};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reset_waits_for_a_begun_job_to_register() {
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
    write_flush(
        inner.as_ref(),
        store.clone(),
        &batch_from_rows(&rows),
        HourBucket(1),
        "n",
        1 << 30,
        1,
        10,
    )
    .await;
    // The begin parks after its txn committed, until released.
    let (begun, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let hook = {
        let (begun, release) = (begun.clone(), release.clone());
        move || {
            let (begun, release) = (begun.clone(), release.clone());
            Box::pin(async move {
                begun.notify_one();
                release.notified().await;
            }) as futures::future::BoxFuture<'static, ()>
        }
    };
    let meta: Arc<dyn Metastore> = Arc::new(HookedMetastore::after_begin(inner.clone(), hook));
    let cfg = CompactionConfig {
        l0_trigger: 1,
        ..Default::default()
    };
    let cx = Arc::new(CompactCtx::new(meta, store, cfg, Clocks::wall()));
    let job = tokio::spawn({
        let cx = cx.clone();
        async move { run_next(&cx).await }
    });
    begun.notified().await; // the job row is `running`, not yet registered
    let reset = tokio::spawn({
        let cx = cx.clone();
        async move { reset_dead_jobs(&cx).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    release.notify_one();
    assert_eq!(reset.await.unwrap().unwrap(), 0, "a live job was reset");
    let r = job.await.unwrap().unwrap().expect("the job commits");
    assert_eq!(r.l0_inputs, 1);
    let segs = inner.list_segments(&SegmentFilter::default()).await.unwrap();
    assert!(segs.iter().all(|s| s.level == Level::L1), "{segs:?}");
}
