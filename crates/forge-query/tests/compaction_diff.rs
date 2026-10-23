//! Exit 2 (plans/phase-4 §6.2): trace JSON and runs pages are byte-identical before and after `compact + gc`, SQL
//! is identical except that a dotted_order may go from null to the oracle value, and all of them equal the
//! reference. With no memtable rows, the SQL plan merges iff some hour's L1 trace ranges intersect.
mod common;

use common::*;
use datafusion::physical_plan::ExecutionPlan;
use forge_ingest::compact::{CompactCtx, compact_project};
use forge_ingest::lifecycle::{GcOptions, run_gc};
use forge_meta::SegmentFilter;
use forge_model::catalog::{HourBucket, Level};
use forge_model::config::QueryConfig;
use forge_model::dotted::depth;
use forge_model::ids::{ProjectId, TraceId};
use forge_model::merge::Cancel;
use forge_model::row::SpanRow;
use forge_model::schema::{ALL_COLUMNS, col};
use forge_model::testing::{RowKey, ref_dotted, ref_tree, reference_merge, row_key};
use forge_query::QueryEngine;
use forge_query::runs::{RunsParams, RunsRequest};
use forge_query::trace::TraceFormat;
use forge_storage::SegmentStore;
use object_store::ObjectStoreExt;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

fn runs_requests(p: &str) -> Vec<RunsRequest> {
    let f = |run_type: Option<&str>, status: Option<&str>| RunsParams {
        run_type: run_type.map(Into::into),
        status: status.map(Into::into),
        ..Default::default()
    };
    [f(None, None), f(None, Some("error")), f(Some("llm"), None)]
        .into_iter()
        .map(|x| RunsRequest::parse(x, forge_model::testing::pid(p)).unwrap())
        .collect()
}

/// Trace JSON (tree/flat x payloads) and runs pages, byte for byte; SQL without dotted_order.
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
    let pretty = |b| arrow::util::pretty::pretty_format_batches(&[b]).unwrap().to_string();
    let mut out = vec![
        pretty(sql(&fx.engine, &q).await),
        pretty(
            sql(
                &fx.engine,
                "SELECT project_id, count(*), sum(total_tokens) FROM spans GROUP BY project_id ORDER BY project_id",
            )
            .await,
        ),
    ];
    for (p, t) in fx.traces() {
        for payloads in [false, true] {
            for f in [TraceFormat::Tree, TraceFormat::Flat] {
                out.push(match fx.engine.trace_json(&treq(&p, t, payloads, f)).await {
                    Ok(b) => String::from_utf8(b).unwrap(),
                    Err(e) => e.to_string(),
                });
            }
        }
    }
    let projects: BTreeSet<String> = fx.traces().into_iter().map(|x| x.0).collect();
    for p in projects {
        for req in runs_requests(&p) {
            out.push(String::from_utf8(fx.engine.runs_json(&req).await.unwrap()).unwrap());
        }
    }
    out
}

/// "Before" equals the reference (trace JSON and runs pages).
async fn assert_reference(fx: &Fixture, ctx: &str) {
    let reference = fx.reference();
    for (p, t) in fx.traces() {
        let rows = rows_of(&reference, &p, t);
        if rows.is_empty() {
            continue;
        }
        let owned: Vec<SpanRow> = rows.iter().map(|r| (*r).clone()).collect();
        let tree = ref_tree(&owned);
        for payloads in [false, true] {
            for (f, flat) in [(TraceFormat::Tree, false), (TraceFormat::Flat, true)] {
                let got: Value =
                    serde_json::from_slice(&fx.engine.trace_json(&treq(&p, t, payloads, f)).await.unwrap()).unwrap();
                assert_eq!(
                    got,
                    expected_trace_json(&p, &rows, &tree, payloads, flat),
                    "{ctx}: trace {p}/{t}"
                );
            }
        }
    }
    let projects: BTreeSet<String> = fx.traces().into_iter().map(|x| x.0).collect();
    for p in projects {
        let pid = forge_model::testing::pid(&p);
        let by_key: BTreeMap<(TraceId, forge_model::ids::SpanId), &SpanRow> = reference
            .iter()
            .filter(|r| r.project_id.as_ref() == Some(&pid))
            .map(|r| ((r.trace_id, r.span_id), r))
            .collect();
        for req in runs_requests(&p) {
            let body: Value = serde_json::from_slice(&fx.engine.runs_json(&req).await.unwrap()).unwrap();
            let want: Vec<Value> = ref_runs(&reference, &req)
                .iter()
                .take(50)
                .map(|x| Value::Object(expected_span(by_key[&(x.1, x.2)], false, true)))
                .collect();
            assert_eq!(body["runs"], Value::Array(want), "{ctx}: runs {p}");
        }
    }
}

async fn dotted_by_key(engine: &QueryEngine) -> BTreeMap<RowKey, Option<String>> {
    use arrow::array::{Array, AsArray};
    let b = sql(engine, "SELECT project_id, trace_id, span_id, dotted_order FROM spans").await;
    let (p, t, s, d) = (
        b.column(0).as_string::<i32>(),
        b.column(1).as_binary::<i32>(),
        b.column(2).as_binary::<i32>(),
        b.column(3).as_string::<i32>(),
    );
    (0..b.num_rows())
        .map(|i| {
            let k = (
                p.value(i).to_string(),
                TraceId::from_slice(t.value(i)).unwrap(),
                forge_model::ids::SpanId::from_slice(s.value(i)).unwrap(),
            );
            (k, d.is_valid(i).then(|| d.value(i).to_string()))
        })
        .collect()
}

fn merge_nodes(p: &Arc<dyn ExecutionPlan>) -> usize {
    usize::from(p.name() == "MergeVersionsExec") + p.children().iter().map(|c| merge_nodes(c)).sum::<usize>()
}

#[tokio::test]
async fn compact_and_gc_change_no_result() {
    let mut plans = Vec::new();
    for seed in 0..9u64 {
        let ctx = format!("seed {seed}");
        let (layout, traps) = if seed == 8 {
            (time_layout(), Default::default())
        } else {
            random_layout(seed)
        };
        let flushed_posts = layout.flushes.iter().map(Vec::len).sum::<usize>() as u64;
        assert!(layout.flushes.len() <= 16, "{ctx}: one kind-A job takes every L0");
        let fx = Fixture::build(layout, traps, QueryConfig::default()).await;
        let before = snapshot(&fx).await;
        assert_reference(&fx, &ctx).await;
        let inputs = fx.meta.list_segments(&SegmentFilter::default()).await.unwrap();
        assert!(inputs.iter().all(|s| s.level == Level::L0));
        assert!(inputs.iter().map(|s| s.byte_size).sum::<u64>() < 512 << 20, "{ctx}");
        let cx = CompactCtx {
            meta: fx.meta.clone(),
            segments: fx.store.clone(),
            cfg: Default::default(),
            data_clock: forge_model::time::wall_clock(),
            next_project: Default::default(),
            cancel: Cancel::new(),
        };
        for p in fx.meta.list_projects().await.unwrap() {
            let s = compact_project(&cx, &p.id).await.unwrap();
            assert!(s.failed.is_empty() && !s.capped && s.skipped == 0, "{ctx}: {s:?}");
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
        assert!(segs.iter().all(|s| s.level == Level::L1), "{ctx}: L0 left");
        for s in &inputs {
            let p = object_store::path::Path::from(s.path.as_str());
            assert!(
                fx.store.object_store().head(&p).await.is_err(),
                "{ctx}: input object {} kept",
                s.path
            );
        }
        let after = snapshot(&fx).await;
        assert_eq!(before.len(), after.len());
        for (i, (b, a)) in before.iter().zip(&after).enumerate() {
            assert_eq!(b, a, "{ctx}: result {i} changed");
        }
        // dotted_order: the memtable's newest non-null value, else ref_dotted over the flushed posts.
        let flushed: Vec<SpanRow> = fx.all.iter().filter(|r| r.version <= flushed_posts).cloned().collect();
        let oracle = ref_dotted(&reference_merge(&flushed));
        let mut mem_dotted: BTreeMap<RowKey, (u64, String)> = BTreeMap::new();
        for r in fx.all.iter().filter(|r| r.version > flushed_posts) {
            if let Some(d) = &r.dotted_order {
                let e = mem_dotted.entry(row_key(r)).or_insert((0, String::new()));
                if r.version > e.0 {
                    *e = (r.version, d.clone());
                }
            }
        }
        let before_dotted = ref_dotted_or_null(&fx.all);
        let (mut filled, mut deep) = (0, 0);
        for (k, got) in dotted_by_key(&fx.engine).await {
            let want = mem_dotted
                .get(&k)
                .map(|x| x.1.clone())
                .or_else(|| oracle.get(&k).cloned().flatten());
            assert_eq!(got, want, "{ctx}: dotted of {k:?}");
            if before_dotted.get(&k).is_some_and(Option::is_none) && got.is_some() {
                filled += 1;
                deep += usize::from(got.as_deref().is_some_and(|d| depth(d) >= 2));
            }
        }
        if seed < 8 {
            assert!(
                filled > 0 && deep > 0,
                "{ctx}: no null -> value change ({filled}, {deep})"
            );
        }
        // Merge-skip: with no memtable rows the plan merges iff some hour's L1 trace ranges intersect (<=).
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
        let mut groups: BTreeMap<(ProjectId, HourBucket), Vec<(TraceId, TraceId)>> = BTreeMap::new();
        for s in &segs {
            groups
                .entry((s.project_id.clone(), s.partition_hour))
                .or_default()
                .push((s.min_trace_id, s.max_trace_id));
        }
        let intersect = groups.values_mut().any(|v| {
            v.sort();
            v.windows(2).any(|w| w[1].0 <= w[0].1)
        });
        let merges = merge_nodes(&plan);
        assert_eq!(merges > 0, intersect, "{ctx}: {merges} merge nodes, groups {groups:?}");
        plans.push((seed, segs.len(), merges));
    }
    assert!(
        plans.iter().any(|p| p.2 == 0) && plans.iter().any(|p| p.2 > 0),
        "{plans:?}"
    );
    #[allow(clippy::print_stdout)]
    {
        println!("(seed, L1 segments, MergeVersionsExec nodes) after compaction: {plans:?}");
    }
}

/// The read's dotted_order before compaction: the newest non-null post value per key (no backfill yet).
fn ref_dotted_or_null(all: &[SpanRow]) -> BTreeMap<RowKey, Option<String>> {
    reference_merge(all)
        .into_iter()
        .map(|r| (row_key(&r), r.dotted_order))
        .collect()
}
