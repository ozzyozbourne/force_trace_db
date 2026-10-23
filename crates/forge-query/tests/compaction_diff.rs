//! Exit 2 (plan §6.2), reduced: trace JSON, runs JSON and SQL are identical before and after compact + gc (apart
//! from dotted_order null -> backfilled), and with no memtable rows the SQL plan skips the merge.
mod common;

use common::*;
use datafusion::physical_plan::ExecutionPlan;
use forge_ingest::compact::{CompactCtx, compact_project};
use forge_ingest::lifecycle::{GcOptions, run_gc};
use forge_meta::SegmentFilter;
use forge_model::catalog::Level;
use forge_model::config::QueryConfig;
use forge_model::schema::{ALL_COLUMNS, col};
use forge_query::QueryEngine;
use forge_query::trace::TraceFormat;
use forge_storage::SegmentStore;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

fn strip_dotted(v: &mut Value) {
    match v {
        Value::Object(m) => {
            m.remove("dotted_order");
            m.values_mut().for_each(strip_dotted);
        }
        Value::Array(a) => a.iter_mut().for_each(strip_dotted),
        _ => {}
    }
}

async fn snapshot(fx: &Fixture) -> Vec<String> {
    let cols: Vec<&str> = ALL_COLUMNS
        .iter()
        .copied()
        .filter(|c| *c != col::DOTTED_ORDER)
        .collect();
    let q = format!(
        "SELECT {} FROM spans ORDER BY project_id, trace_id, span_id",
        cols.join(", ")
    );
    let mut out = vec![
        arrow::util::pretty::pretty_format_batches(&[sql(&fx.engine, &q).await])
            .unwrap()
            .to_string(),
        arrow::util::pretty::pretty_format_batches(&[sql(
            &fx.engine,
            "SELECT project_id, count(*), sum(total_tokens) FROM spans GROUP BY project_id ORDER BY project_id",
        )
        .await])
        .unwrap()
        .to_string(),
    ];
    for (p, t) in fx.traces() {
        for payloads in [false, true] {
            for f in [TraceFormat::Tree, TraceFormat::Flat] {
                let mut v: Value = match fx.engine.trace_json(&treq(&p, t, payloads, f)).await {
                    Ok(b) => serde_json::from_slice(&b).unwrap(),
                    Err(e) => Value::String(e.to_string()),
                };
                strip_dotted(&mut v);
                out.push(v.to_string());
            }
        }
        let mut v: Value = serde_json::from_slice(&fx.engine.runs_json(&runs_request(&p)).await.unwrap()).unwrap();
        strip_dotted(&mut v);
        out.push(v.to_string());
    }
    out
}

fn walk(p: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    usize::from(p.name() == name) + p.children().iter().map(|c| walk(c, name)).sum::<usize>()
}

#[tokio::test]
async fn compact_and_gc_change_no_result() {
    let mut merges_after = Vec::new();
    for seed in 0..6u64 {
        let (layout, traps) = if seed == 0 {
            (time_layout(), Default::default())
        } else {
            random_layout(seed)
        };
        let fx = Fixture::build(layout, traps, QueryConfig::default()).await;
        let before = snapshot(&fx).await;
        let cx = CompactCtx {
            meta: fx.meta.clone(),
            segments: fx.store.clone(),
            cfg: Default::default(),
            data_clock: forge_model::time::wall_clock(),
            next_project: Default::default(),
            cancel: forge_model::merge::Cancel::new(),
        };
        for p in fx.meta.list_projects().await.unwrap() {
            let s = compact_project(&cx, &p.id).await.unwrap();
            assert!(s.failed.is_empty() && !s.capped, "seed {seed}: {s:?}");
        }
        let o = GcOptions {
            gc_grace: Duration::ZERO,
            orphan_grace: Duration::ZERO,
            sweep_blobs: true,
            dry_run: false,
        };
        let now = forge_model::time::now_ms() + 1;
        run_gc(fx.meta.as_ref(), fx.store.object_store().as_ref(), now, now, &o, None)
            .await
            .unwrap();
        let segs = fx
            .meta
            .list_segments(&SegmentFilter {
                include_deleted: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(segs.iter().all(|s| s.level == Level::L1), "seed {seed}: L0 left");
        let after = snapshot(&fx).await;
        assert_eq!(before.len(), after.len());
        for (i, (b, a)) in before.iter().zip(&after).enumerate() {
            assert_eq!(b, a, "seed {seed}: result {i} changed");
        }
        let empty = QueryEngine::new(
            fx.meta.clone(),
            fx.store.clone(),
            Arc::new(forge_model::EmptyMemtable),
            QueryConfig::default(),
        );
        let plan = empty
            .session()
            .unwrap()
            .sql("SELECT * FROM spans")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        merges_after.push((seed, segs.len(), walk(&plan, "MergeVersionsExec")));
    }
    // Evidence for the review: how often the merge is skipped after compaction.
    let skipped = merges_after.iter().filter(|m| m.2 == 0).count();
    assert!(skipped > 0, "{merges_after:?}");
    #[allow(clippy::print_stdout)]
    {
        println!("(seed, L1 segments, MergeVersionsExec nodes) after compaction: {merges_after:?}");
    }
}
