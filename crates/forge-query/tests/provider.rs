//! SQL through `SpansTableProvider` vs the reference merge, plan shape, payload pruning, and limits (plan §6.4).
mod common;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{Float64Type, Int64Type};
use common::*;
use datafusion::physical_plan::ExecutionPlan;
use forge_model::config::QueryConfig;
use forge_model::enums::{RunType, SpanStatus};
use forge_model::error::{Classify, ErrorKind};
use forge_model::merge::Cancel;
use forge_model::row::SpanRow;
use forge_model::schema::{ALL_COLUMNS, KEY_COLUMNS, PAYLOAD_COLUMNS, col};
use forge_model::testing::{pid, tid};
use forge_query::runs::RunsRequest;
use forge_query::testing::{HookedMetastore, RecordingSegmentStore};
use forge_query::trace::{TraceFormat, TraceRequest};
use forge_query::{QueryEngine, QueryError};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

async fn physical(e: &QueryEngine, q: &str) -> Arc<dyn ExecutionPlan> {
    e.session()
        .unwrap()
        .sql(q)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

fn walk<'a>(p: &'a Arc<dyn ExecutionPlan>, out: &mut Vec<&'a Arc<dyn ExecutionPlan>>) {
    out.push(p);
    for c in p.children() {
        walk(c, out);
    }
}

fn nodes(p: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    let mut v = Vec::new();
    walk(p, &mut v);
    v.iter().filter(|n| n.name() == name).count()
}

fn is_leaf(p: &Arc<dyn ExecutionPlan>) -> bool {
    p.children().is_empty() && p.name() != "EmptyExec"
}

/// Invariant 4 on the plan: from every leaf up, the first merge-or-filter node is a `MergeVersionsExec` or the
/// one-source `NOT deleted` filter (possibly combined with a pushed filter), and no limit sits below a merge.
fn check_plan_shape(p: &Arc<dyn ExecutionPlan>, q: &str) {
    fn rec(p: &Arc<dyn ExecutionPlan>, guarded: bool, under_merge: bool, q: &str) {
        let name = p.name();
        assert!(!(under_merge && name.contains("Limit")), "{q}: limit below a merge");
        let (mut g, mut m) = (guarded, under_merge);
        if name == "MergeVersionsExec" {
            (g, m) = (false, true);
        } else if name == "FilterExec" {
            let any: &dyn std::any::Any = p.as_ref();
            let f = any
                .downcast_ref::<datafusion::physical_plan::filter::FilterExec>()
                .unwrap();
            let pred = f.predicate().to_string();
            // Walking down: a filter above a merge is fine; one directly over a leaf must be the D14 filter.
            if !pred.contains("NOT deleted") {
                g = true;
            }
        }
        if is_leaf(p) {
            assert!(!g, "{q}: a non-key filter below every merge over leaf {name}");
        }
        for c in p.children() {
            rec(c, g, m, q);
        }
    }
    rec(p, false, false, q);
}

struct Case {
    sql: String,
    cols: Vec<&'static str>,
    pred: Box<dyn Fn(&SpanRow) -> bool>,
}

fn case(cols: &[&'static str], filter: &str, pred: impl Fn(&SpanRow) -> bool + 'static) -> Case {
    let mut all: Vec<&'static str> = KEY_COLUMNS.to_vec();
    all.extend(cols.iter().filter(|c| !KEY_COLUMNS.contains(c)));
    Case {
        sql: format!("SELECT {} FROM spans WHERE {filter}", all.join(", ")),
        cols: all,
        pred: Box::new(pred),
    }
}

async fn check_case(fx: &Fixture, reference: &[SpanRow], c: &Case) {
    let got = sort_by_key(&sql(&fx.engine, &c.sql).await);
    let want: Vec<SpanRow> = reference.iter().filter(|r| (c.pred)(r)).cloned().collect();
    let want = ref_batch(&want, &c.cols);
    assert_eq!(got.num_rows(), want.num_rows(), "{}", c.sql);
    assert_eq!(got.columns(), want.columns(), "{}", c.sql);
    check_plan_shape(&physical(&fx.engine, &c.sql).await, &c.sql);
}

fn p_is(r: &SpanRow, p: &str) -> bool {
    r.project_id.as_ref().is_some_and(|x| x.as_str() == p)
}

fn ns(x: i64) -> String {
    format!("to_timestamp_nanos({x})")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_matches_reference() {
    for seed in [1, 2] {
        let (layout, traps) = random_layout(seed);
        let mut fx = Fixture::build(layout, traps, QueryConfig::default()).await;
        let reference = fx.reference();
        let t = |n: u64| hex_lit(tid(n).as_bytes());
        let (lo, hi) = (BASE + 10 * STEP, BASE + 30 * STEP);
        let in_time = move |r: &SpanRow| r.start_time_ns.is_some_and(|s| s >= lo && s <= hi);
        let cases = vec![
            case(&ALL_COLUMNS, "true", |_| true),
            case(&ALL_COLUMNS, "status = 'ok'", |r| r.status == Some(SpanStatus::Ok)),
            case(
                &ALL_COLUMNS,
                &format!("project_id = 'a' AND trace_id = {}", t(1)),
                |r| p_is(r, "a") && r.trace_id == tid(1),
            ),
            case(
                &[col::NAME],
                &format!("trace_id IN ({}, {}, {}, {})", t(0), t(1), t(3), t(100)),
                |r| [0, 1, 3, 100].iter().any(|&n| r.trace_id == tid(n)),
            ),
            case(
                &[col::NAME, col::STATUS],
                &format!("project_id = 'b' AND trace_id IN ({}, {})", t(0), t(2)),
                |r| p_is(r, "b") && (r.trace_id == tid(0) || r.trace_id == tid(2)),
            ),
            case(&[col::RUN_TYPE], "project_id IN ('a', 'e')", |r| {
                p_is(r, "a") || p_is(r, "e")
            }),
            case(
                &[col::INPUTS],
                &format!("project_id IN ('a', 'b') AND trace_id IN ({}, {})", t(0), t(2)),
                |r| (p_is(r, "a") || p_is(r, "b")) && (r.trace_id == tid(0) || r.trace_id == tid(2)),
            ),
            // End-post fields survive a start_time filter (it is applied above the merge).
            case(
                &[col::STATUS, col::END_TIME],
                &format!("start_time BETWEEN {} AND {}", ns(lo), ns(hi)),
                in_time,
            ),
            case(&[col::NAME], "project_id = 'a' AND project_id = 'b'", |_| false),
            case(&[col::TOTAL_TOKENS], "total_tokens > 50", |r| {
                r.total_tokens.is_some_and(|t| t > 50)
            }),
            case(&[col::STATUS], "project_id = 'd'", |r| p_is(r, "d")),
            case(&[col::STATUS], "project_id = 'c' AND status = 'ok'", |r| {
                p_is(r, "c") && r.status == Some(SpanStatus::Ok)
            }),
        ];
        for tp in [1, 2, 4, 8] {
            fx.engine.target_partitions = tp;
            for c in &cases {
                check_case(&fx, &reference, c).await;
            }
            let n = sql(&fx.engine, "SELECT count(*) AS n FROM spans").await;
            assert_eq!(
                n.column(0).as_primitive::<Int64Type>().value(0) as usize,
                reference.len()
            );
            let g = sql(
                &fx.engine,
                "SELECT session_id, sum(total_tokens), sum(cost_usd) FROM spans GROUP BY session_id",
            )
            .await;
            let mut want: BTreeMap<Option<String>, (Option<i64>, Option<f64>)> = BTreeMap::new();
            for r in &reference {
                let e = want.entry(r.session_id.clone()).or_default();
                e.0 = match (e.0, r.total_tokens) {
                    (a, None) => a,
                    (a, Some(b)) => Some(a.unwrap_or(0) + b),
                };
                e.1 = match (e.1, r.cost_usd) {
                    (a, None) => a,
                    (a, Some(b)) => Some(a.unwrap_or(0.0) + b),
                };
            }
            let (s, tt, cost) = (
                g.column(0).as_string::<i32>(),
                g.column(1).as_primitive::<Int64Type>(),
                g.column(2).as_primitive::<Float64Type>(),
            );
            let got: BTreeMap<Option<String>, (Option<i64>, Option<f64>)> = (0..g.num_rows())
                .map(|i| {
                    (
                        s.is_valid(i).then(|| s.value(i).to_string()),
                        (
                            tt.is_valid(i).then(|| tt.value(i)),
                            cost.is_valid(i).then(|| cost.value(i)),
                        ),
                    )
                })
                .collect();
            assert_eq!(got, want, "GROUP BY session_id, tp {tp}");
            let l = sql(
                &fx.engine,
                "SELECT project_id, trace_id, span_id, name FROM spans LIMIT 5",
            )
            .await;
            assert_eq!(l.num_rows(), 5);
            let lim = physical(&fx.engine, "SELECT * FROM spans LIMIT 5").await;
            check_plan_shape(&lim, "LIMIT 5");
        }
        // Merge counts: a, b, e (several sources), d (one segment + memtable) merge; c (one segment) is skipped.
        assert_eq!(
            nodes(&physical(&fx.engine, "SELECT * FROM spans").await, "MergeVersionsExec"),
            4
        );
        assert_eq!(
            nodes(
                &physical(&fx.engine, "SELECT * FROM spans WHERE project_id = 'c'").await,
                "MergeVersionsExec"
            ),
            0
        );
        assert_eq!(
            nodes(
                &physical(&fx.engine, "SELECT * FROM spans WHERE project_id = 'd'").await,
                "MergeVersionsExec"
            ),
            1
        );
        let empty = physical(
            &fx.engine,
            "SELECT * FROM spans WHERE project_id = 'a' AND project_id = 'b'",
        )
        .await;
        assert_eq!(nodes(&empty, "EmptyExec"), 1);
        assert_eq!(nodes(&empty, "SegmentScanExec") + nodes(&empty, "DataSourceExec"), 0);
    }
}

/// Segment pruning by start_time (decision 5), and a lossy cast that must not prune (D16).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn time_pruning_and_lossy_cast() {
    let fx = Fixture::build(time_layout(), Traps::default(), QueryConfig::default()).await;
    let explain = async |q: String| {
        let b = sql(&fx.engine, &format!("EXPLAIN {q}")).await;
        arrow::util::pretty::pretty_format_batches(&[b]).unwrap().to_string()
    };
    let from = T_BASE + 100_000_000_000;
    let q = format!("SELECT name FROM spans WHERE start_time >= {}", ns(from));
    assert!(
        explain(q.clone()).await.contains("segments=2"),
        "{}",
        explain(q.clone()).await
    );
    assert_eq!(sql(&fx.engine, &q).await.num_rows(), 4);
    let q = format!("SELECT name FROM spans WHERE start_time = {}", ns(from + 500_000_000));
    assert!(
        explain(q.clone()).await.contains("segments=1"),
        "{}",
        explain(q.clone()).await
    );
    assert_eq!(sql(&fx.engine, &q).await.num_rows(), 2);
    let secs = from / 1_000_000_000;
    let q = format!("SELECT name FROM spans WHERE CAST(start_time AS TIMESTAMP(0)) = to_timestamp_seconds({secs})");
    let b = sql(&fx.engine, &q).await;
    let names: Vec<&str> = b.column(0).as_string::<i32>().iter().map(|x| x.unwrap()).collect();
    assert_eq!(names, ["t1", "t1"], "{q}");
    assert!(explain(q.clone()).await.contains("segments=3"), "{}", explain(q).await);
}

/// List-style queries never read payload columns; `SELECT inputs` does (the control).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explain_has_no_payload_columns() {
    let (layout, traps) = random_layout(3);
    let fx = Fixture::build(layout, traps, QueryConfig::default()).await;
    let leaf_cols = |p: &Arc<dyn ExecutionPlan>| -> Vec<String> {
        let mut v = Vec::new();
        walk(p, &mut v);
        v.iter()
            .filter(|n| is_leaf(n))
            .flat_map(|n| n.schema().fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>())
            .collect()
    };
    for q in [
        "SELECT trace_id, span_id, name, run_type, status, start_time, end_time, total_tokens FROM spans \
         WHERE project_id = 'a' AND parent_span_id IS NULL AND start_time IS NOT NULL \
         ORDER BY start_time DESC, trace_id DESC, span_id DESC LIMIT 50",
        "SELECT count(*) FROM spans",
        "SELECT session_id, sum(total_tokens), sum(cost_usd) FROM spans GROUP BY session_id",
    ] {
        let cols = leaf_cols(&physical(&fx.engine, q).await);
        assert!(!cols.is_empty(), "{q}");
        let b = sql(&fx.engine, &format!("EXPLAIN {q}")).await;
        let text = arrow::util::pretty::pretty_format_batches(&[b]).unwrap().to_string();
        for p in PAYLOAD_COLUMNS {
            assert!(!cols.iter().any(|c| c == p), "{q}: leaf reads {p}: {cols:?}");
            assert!(!text.contains(p), "{q}: {p} in\n{text}");
        }
    }
    let cols = leaf_cols(&physical(&fx.engine, "SELECT inputs FROM spans").await);
    assert!(cols.iter().any(|c| c == col::INPUTS), "control: {cols:?}");
}

#[derive(Clone, Copy, Debug)]
enum Key {
    None,
    Project(usize),
    Projects,
    Traces,
    ProjectTraces(usize),
}

/// Plan §6.4 query grammar: key sets × time range × one non-key predicate × projection × optional ORDER BY/LIMIT.
#[test]
fn query_grammar_matches_reference() {
    use proptest::prelude::*;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let (layout, traps) = random_layout(4);
    let fx = rt.block_on(Fixture::build(layout, traps, QueryConfig::default()));
    let reference = fx.reference();
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(16));
    let strategy = (
        0u8..5,
        0usize..3,
        0u8..3,
        0u8..6,
        any::<u32>(),
        any::<bool>(),
        1usize..20,
        prop_oneof![Just(1usize), Just(2), Just(4), Just(8)],
    );
    runner
        .run(&strategy, |(key, pi, time, nonkey, mask, order, limit, _tp)| {
            let projects = ["a", "b", "e"];
            let key = match key {
                0 => Key::None,
                1 => Key::Project(pi),
                2 => Key::Projects,
                3 => Key::Traces,
                _ => Key::ProjectTraces(pi),
            };
            let tr = |n: u64| hex_lit(tid(n).as_bytes());
            let (key_sql, key_pred): (String, Box<dyn Fn(&SpanRow) -> bool>) = match key {
                Key::None => ("true".into(), Box::new(|_| true)),
                Key::Project(i) => (
                    format!("project_id = '{}'", projects[i]),
                    Box::new(move |r| p_is(r, projects[i])),
                ),
                Key::Projects => (
                    "project_id IN ('a', 'e')".into(),
                    Box::new(|r| p_is(r, "a") || p_is(r, "e")),
                ),
                Key::Traces => (
                    format!("trace_id IN ({}, {}, {}, {})", tr(0), tr(1), tr(2), tr(9)),
                    Box::new(|r| [0, 1, 2, 9].iter().any(|&n| r.trace_id == tid(n))),
                ),
                Key::ProjectTraces(i) => (
                    format!("project_id = '{}' AND trace_id IN ({}, {})", projects[i], tr(1), tr(3)),
                    Box::new(move |r| p_is(r, projects[i]) && (r.trace_id == tid(1) || r.trace_id == tid(3))),
                ),
            };
            let (lo, hi) = (BASE + 5 * STEP, BASE + 25 * STEP);
            let (time_sql, time_pred): (String, Box<dyn Fn(&SpanRow) -> bool>) = match time {
                0 => ("true".into(), Box::new(|_| true)),
                1 => (
                    format!("start_time >= {}", ns(lo)),
                    Box::new(move |r| r.start_time_ns.is_some_and(|s| s >= lo)),
                ),
                _ => (
                    format!("start_time BETWEEN {} AND {}", ns(lo), ns(hi)),
                    Box::new(move |r| r.start_time_ns.is_some_and(|s| s >= lo && s <= hi)),
                ),
            };
            let (nk_sql, nk_pred): (&str, Box<dyn Fn(&SpanRow) -> bool>) = match nonkey {
                0 => ("true", Box::new(|_| true)),
                1 => ("status = 'error'", Box::new(|r| r.status == Some(SpanStatus::Error))),
                2 => ("run_type = 'llm'", Box::new(|r| r.run_type == Some(RunType::Llm))),
                3 => ("parent_span_id IS NULL", Box::new(|r| r.parent_span_id.is_none())),
                4 => (
                    "total_tokens > 60",
                    Box::new(|r| r.total_tokens.is_some_and(|t| t > 60)),
                ),
                _ => (
                    "name LIKE '%1%'",
                    Box::new(|r| r.name.as_ref().is_some_and(|n| n.contains('1'))),
                ),
            };
            let others: Vec<&'static str> = ALL_COLUMNS
                .iter()
                .copied()
                .filter(|c| !KEY_COLUMNS.contains(c))
                .collect();
            let mut cols: Vec<&'static str> = KEY_COLUMNS.to_vec();
            cols.extend(
                others
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| mask & (1 << i) != 0)
                    .map(|(_, c)| *c),
            );
            let mut q = format!(
                "SELECT {} FROM spans WHERE {key_sql} AND {time_sql} AND {nk_sql}",
                cols.join(", ")
            );
            if order {
                q += &format!(" ORDER BY project_id, trace_id, span_id LIMIT {limit}");
            }
            let mut want: Vec<SpanRow> = reference
                .iter()
                .filter(|r| key_pred(r) && time_pred(r) && nk_pred(r))
                .cloned()
                .collect();
            want.sort_by_key(|r| (r.project_id.clone(), r.trace_id, r.span_id));
            if order {
                want.truncate(limit);
            }
            let got = sort_by_key(&rt.block_on(sql(&fx.engine, &q)));
            let want = ref_batch(&want, &cols);
            prop_assert_eq!(got.num_rows(), want.num_rows(), "{}", q);
            prop_assert_eq!(got.columns(), want.columns(), "{}", q);
            Ok(())
        })
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_statements_are_invalid_input() {
    let fx = Fixture::build(time_layout(), Traps::default(), QueryConfig::default()).await;
    for q in [
        "CREATE TABLE x (a INT)",
        "INSERT INTO spans (name) VALUES ('x')",
        "DROP TABLE spans",
        "SET datafusion.execution.batch_size = 0",
        "SELEC 1",
        "SELECT nope FROM spans",
    ] {
        let e = fx.engine.sql_batches(q).await.unwrap_err();
        assert_eq!(e.kind(), ErrorKind::InvalidInput, "{q}: {e}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_limit_is_invalid_input() {
    let (layout, traps) = random_layout(5);
    let q = "SELECT * FROM spans WHERE project_id = 'a' ORDER BY name, inputs";
    let small = QueryConfig {
        memory_limit: bytesize::ByteSize::kib(16),
        ..QueryConfig::default()
    };
    let fx = Fixture::build(layout, traps, small).await;
    assert_eq!(
        nodes(&physical(&fx.engine, q).await, "MergeVersionsExec"),
        1,
        "precondition: a merges"
    );
    let e = fx.engine.sql_batches(q).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::InvalidInput, "{e}");
    assert!(e.message().contains("memory limit"), "{}", e.message());
    let (layout, traps) = random_layout(5);
    let fx = Fixture::build(layout, traps, QueryConfig::default()).await;
    assert!(sql(&fx.engine, q).await.num_rows() > 0, "control");
}

fn timeout_cfg(ms: u64) -> QueryConfig {
    QueryConfig {
        timeout: Duration::from_millis(ms),
        ..QueryConfig::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpu_bound_query_times_out() {
    let fx = Fixture::build(big_trace(300, 2, 10, 16, 1), Traps::default(), timeout_cfg(200)).await;
    assert!(
        fx.reference().len() >= 200,
        "precondition: {} rows",
        fx.reference().len()
    );
    let t = Instant::now();
    let e = fx
        .engine
        .sql_batches("SELECT sum(a.version * b.version) FROM spans a, spans b, spans c, spans d")
        .await;
    assert!(matches!(e, Err(QueryError::Timeout(_))), "{:?}", e.map(|b| b.len()));
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
}

/// Slow metastore (planning) or slow scans: every entry point returns Timeout within timeout + 500 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadlines_cover_planning_and_scans() {
    let (layout, traps) = random_layout(7);
    let fx = Fixture::build(layout, traps, QueryConfig::default()).await;
    let slow = Duration::from_secs(2);
    let engines = [
        QueryEngine::new(
            Arc::new(HookedMetastore::slow(fx.meta.clone(), slow)),
            fx.store.clone(),
            fx.mem.clone(),
            timeout_cfg(200),
        ),
        QueryEngine::new(
            fx.meta.clone(),
            Arc::new(RecordingSegmentStore::slow(fx.store.clone(), slow)),
            fx.mem.clone(),
            timeout_cfg(200),
        ),
    ];
    let trace = TraceRequest {
        project: pid("a"),
        trace: tid(1),
        payloads: true,
        format: TraceFormat::Tree,
    };
    let runs = runs_request("a");
    for (i, e) in engines.iter().chain([&fx.engine]).enumerate() {
        let control = i == 2;
        let t = Instant::now();
        let results: [Result<(), QueryError>; 3] = [
            e.sql_batches("SELECT * FROM spans").await.map(drop),
            e.trace_json(&trace).await.map(drop),
            e.list_runs(&runs).await.map(drop),
        ];
        for (j, r) in results.into_iter().enumerate() {
            if control {
                r.unwrap_or_else(|e| panic!("control {j}: {e}"));
            } else {
                assert!(
                    matches!(r, Err(QueryError::Timeout(_))),
                    "engine {i} call {j}: {:?}",
                    r.err()
                );
            }
        }
        if !control {
            assert!(t.elapsed() < Duration::from_millis(3 * 700), "{:?}", t.elapsed());
        }
    }
}

/// Dropping a trace_json future while its blocking work runs sets that work's cancel flag (the blocking hook
/// holds the work until the flag is set).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_trace_future_cancels() {
    let mut fx = Fixture::build(big_trace(300, 2, 50, 64, 3), Traps::default(), QueryConfig::default()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Cancel>();
    fx.engine.blocking_hook = Some(Arc::new(move |c: &Cancel| {
        tx.send(c.clone()).unwrap();
        let t = Instant::now();
        while !c.is_cancelled() && t.elapsed() < Duration::from_secs(10) {
            std::thread::sleep(Duration::from_millis(1));
        }
    }));
    let req = TraceRequest {
        project: pid("big"),
        trace: tid(999),
        payloads: true,
        format: TraceFormat::Tree,
    };
    let mut fut = Box::pin(fx.engine.trace_json(&req));
    let c = tokio::select! {
        r = &mut fut => panic!("finished before the blocking phase: {:?}", r.map(|b| b.len())),
        c = rx.recv() => c.unwrap(),
    };
    assert!(!c.is_cancelled());
    drop(fut);
    assert!(c.is_cancelled());
}

/// Unknown and other-project traces are NotFound; runs of an unknown project are empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn not_found_and_empty() {
    let fx = Fixture::build(time_layout(), Traps::default(), QueryConfig::default()).await;
    let req = |p: &str, t: u64| TraceRequest {
        project: pid(p),
        trace: tid(t),
        payloads: true,
        format: TraceFormat::Flat,
    };
    assert!(fx.engine.trace_json(&req("t", 0)).await.is_ok());
    for (p, t) in [("t", 9), ("u", 0)] {
        let e = fx.engine.trace_json(&req(p, t)).await.unwrap_err();
        assert_eq!(e.kind(), ErrorKind::NotFound, "{e}");
    }
    let page = fx
        .engine
        .list_runs(&RunsRequest {
            limit: 1,
            ..runs_request("u")
        })
        .await
        .unwrap();
    assert_eq!((page.rows.num_rows(), page.next_cursor.is_none()), (0, true));
}
