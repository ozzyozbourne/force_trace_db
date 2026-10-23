//! Exit test 2 (plan §6.4): the trace and runs fast paths, SQL through the provider, and the in-memory reference
//! merge agree on randomized and forced-edge fixtures.
mod common;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::TimestampNanosecondType;
use common::*;
use forge_model::config::QueryConfig;
use forge_model::dotted::resolve_dotted_orders;
use forge_model::enums::{RunType, SpanStatus};
use forge_model::ids::{SpanId, TraceId};
use forge_model::row::SpanRow;
use forge_model::schema::{PAYLOAD_COLUMNS, col};
use forge_model::testing::{RefNode, Rng, pid, ref_tree, sid, tid};
use forge_query::QueryError;
use forge_query::runs::{Cursor, RUN_COLUMNS, RunsParams, RunsRequest};
use forge_query::trace::{TraceFormat, TraceRequest};
use serde_json::{Value, json};
use std::collections::BTreeSet;

type Pos = (i64, TraceId, SpanId);

fn span_at(b: &RecordBatch, row: usize) -> SpanId {
    let c = b.column(b.schema().index_of(col::SPAN_ID).unwrap()).as_binary::<i32>();
    SpanId::from_slice(c.value(row)).unwrap()
}

fn positions(b: &RecordBatch) -> Vec<Pos> {
    let s = b.schema();
    let st = b
        .column(s.index_of(col::START_TIME).unwrap())
        .as_primitive::<TimestampNanosecondType>();
    let t = b.column(s.index_of(col::TRACE_ID).unwrap()).as_binary::<i32>();
    let sp = b.column(s.index_of(col::SPAN_ID).unwrap()).as_binary::<i32>();
    (0..b.num_rows())
        .map(|i| {
            (
                st.value(i),
                TraceId::from_slice(t.value(i)).unwrap(),
                SpanId::from_slice(sp.value(i)).unwrap(),
            )
        })
        .collect()
}

fn no_payload_or_dotted(projections: &[Vec<&str>], what: &str) {
    assert!(!projections.is_empty(), "{what}: nothing scanned");
    for p in projections {
        assert!(!p.contains(&col::DOTTED_ORDER), "{what} read dotted_order: {p:?}");
        for c in PAYLOAD_COLUMNS {
            assert!(!p.contains(&c), "{what} read {c}: {p:?}");
        }
    }
}

fn treq(project: &str, trace: TraceId, payloads: bool, format: TraceFormat) -> TraceRequest {
    TraceRequest {
        project: pid(project),
        trace,
        payloads,
        format,
    }
}

/// One (project, trace): load_trace == SQL == reference (with and without payloads), the tree == ref_tree, the
/// JSON == the independently built value (tree and flat, with and without payloads).
async fn check_trace(fx: &Fixture, reference: &[SpanRow], project: &str, trace: TraceId) {
    let rows: Vec<SpanRow> = rows_of(reference, project, trace).into_iter().cloned().collect();
    let ctx = format!("{project}/{trace}");
    if rows.is_empty() {
        let e = fx
            .engine
            .load_trace(&treq(project, trace, true, TraceFormat::Tree))
            .await;
        assert!(matches!(e, Err(QueryError::NotFound(_))), "{ctx}: {:?}", e.err());
        return;
    }
    for payloads in [true, false] {
        fx.store.take_projections();
        let lt = fx
            .engine
            .load_trace(&treq(project, trace, payloads, TraceFormat::Tree))
            .await
            .unwrap();
        let proj = fx.store.take_projections();
        assert!(
            proj.iter().all(|p| !p.contains(&col::DOTTED_ORDER)),
            "{ctx}: dotted_order read"
        );
        if !payloads {
            assert!(
                proj.iter().all(|p| PAYLOAD_COLUMNS.iter().all(|c| !p.contains(c))),
                "{ctx}: payload read"
            );
        }
        let cols = trace_cols(payloads);
        assert_eq!(names_of(&lt.rows), cols, "{ctx}");
        let want = ref_batch(&rows, &cols);
        assert_eq!(
            sort_by_key(&lt.rows).columns(),
            want.columns(),
            "{ctx} payloads={payloads}"
        );
        if payloads {
            let q = format!(
                "SELECT {} FROM spans WHERE project_id = '{project}' AND trace_id = {}",
                cols.join(", "),
                hex_lit(trace.as_bytes())
            );
            assert_eq!(
                sort_by_key(&sql(&fx.engine, &q).await).columns(),
                want.columns(),
                "{ctx}: SQL"
            );
            let tree = ref_tree(&rows);
            let got: Vec<RefNode> = lt
                .tree
                .preorder
                .iter()
                .map(|&(row, depth)| {
                    let r = row as usize;
                    RefNode {
                        span: span_at(&lt.rows, r),
                        depth,
                        orphan: lt.tree.orphan[r],
                        cycle: lt.tree.cycle[r],
                    }
                })
                .collect();
            assert_eq!(got, tree, "{ctx}: tree");
            // Fully dotted traces: the preorder is the dotted_order order.
            let mut dotted = rows.clone();
            dotted.iter_mut().for_each(|r| r.dotted_order = None);
            resolve_dotted_orders(&mut dotted);
            if dotted.iter().all(|r| r.dotted_order.is_some()) {
                dotted.sort_by(|a, b| a.dotted_order.cmp(&b.dotted_order));
                let by_dotted: Vec<SpanId> = dotted.iter().map(|r| r.span_id).collect();
                assert_eq!(
                    got.iter().map(|n| n.span).collect::<Vec<_>>(),
                    by_dotted,
                    "{ctx}: dotted order"
                );
            }
        }
        let tree = ref_tree(&rows);
        let refs: Vec<&SpanRow> = rows.iter().collect();
        for format in [TraceFormat::Tree, TraceFormat::Flat] {
            let body = fx
                .engine
                .trace_json(&treq(project, trace, payloads, format))
                .await
                .unwrap();
            let v: Value = serde_json::from_slice(&body).unwrap_or_else(|e| panic!("{ctx}: {e}"));
            let want = expected_trace_json(project, &refs, &tree, payloads, format == TraceFormat::Flat);
            assert_eq!(v, want, "{ctx}: json payloads={payloads} {format:?}");
        }
    }
}

/// Pages `req` until `next_cursor` is None; checks page sizes and that positions strictly decrease.
async fn page_all(fx: &Fixture, mut req: RunsRequest) -> Vec<Pos> {
    let mut out: Vec<Pos> = Vec::new();
    for _ in 0..10_000 {
        let page = fx.engine.list_runs(&req).await.unwrap();
        assert_eq!(names_of(&page.rows), RUN_COLUMNS, "run columns");
        assert!(page.rows.num_rows() <= req.limit);
        out.extend(positions(&page.rows));
        match page.next_cursor {
            Some(c) => {
                assert_eq!(page.rows.num_rows(), req.limit, "a page with a cursor is full");
                assert_eq!(Some(&c.0), out.last(), "cursor = the last row");
                req.cursor = Some(c);
            }
            None => {
                assert!(
                    out.windows(2).all(|w| w[0] > w[1]),
                    "no duplicates, strictly decreasing"
                );
                return out;
            }
        }
    }
    panic!("paging did not end");
}

async fn check_runs(fx: &Fixture, reference: &[SpanRow], seed: u64, limit_one: bool) {
    let mut r = Rng(seed + 77);
    let limits: &[usize] = if limit_one { &[1, 2, 3, 50] } else { &[3, 50] };
    for project in RANDOM_PROJECTS {
        // SQL == reference for the unfiltered listing.
        let base = runs_request(project);
        let q = format!(
            "SELECT start_time, trace_id, span_id FROM spans WHERE project_id = '{project}' AND parent_span_id IS NULL \
             AND start_time IS NOT NULL ORDER BY start_time DESC, trace_id DESC, span_id DESC"
        );
        assert_eq!(
            positions(&sql(&fx.engine, &q).await),
            ref_runs(reference, &base),
            "{project}: SQL runs"
        );
        let mut reqs = vec![base];
        for _ in 0..4 {
            let mut q = runs_request(project);
            if r.chance(50) {
                let a = r.below(40) as i64;
                q.start_ns = Some(BASE + a * STEP);
                let b = a + 1 + r.below(30) as i64;
                q.end_ns = r.opt(70, BASE + b * STEP);
            }
            if r.chance(40) {
                q.run_types = [RunType::Llm, RunType::Tool]
                    .into_iter()
                    .take(1 + r.below(2) as usize)
                    .collect();
            }
            if r.chance(40) {
                q.statuses = [SpanStatus::Error, SpanStatus::Unset]
                    .into_iter()
                    .take(1 + r.below(2) as usize)
                    .collect();
            }
            if r.chance(30) {
                q.q = Some(format!("span-{}", r.below(4)));
            }
            reqs.push(q);
        }
        for req in reqs {
            let want = ref_runs(reference, &req);
            for &limit in limits {
                fx.store.take_projections();
                let got = page_all(
                    fx,
                    RunsRequest {
                        limit,
                        ..clone_req(&req)
                    },
                )
                .await;
                let proj = fx.store.take_projections();
                if !proj.is_empty() {
                    no_payload_or_dotted(&proj, "list_runs");
                }
                assert_eq!(got, want, "seed {seed} {project} limit {limit} {:?}", debug_req(&req));
            }
        }
        // Row contents of one full page == the reference rows.
        let page = fx.engine.list_runs(&runs_request(project)).await.unwrap();
        let keys: BTreeSet<(TraceId, SpanId)> = positions(&page.rows).into_iter().map(|p| (p.1, p.2)).collect();
        let want: Vec<SpanRow> = reference
            .iter()
            .filter(|x| x.project_id.as_ref().unwrap().as_str() == project && keys.contains(&(x.trace_id, x.span_id)))
            .cloned()
            .collect();
        assert_eq!(
            sort_by_key(&page.rows).columns(),
            ref_batch(&want, &RUN_COLUMNS).columns(),
            "{project}: rows"
        );
    }
}

fn clone_req(r: &RunsRequest) -> RunsRequest {
    RunsRequest {
        project: r.project.clone(),
        start_ns: r.start_ns,
        end_ns: r.end_ns,
        run_types: r.run_types.clone(),
        statuses: r.statuses.clone(),
        q: r.q.clone(),
        limit: r.limit,
        cursor: r.cursor.as_ref().map(|c| Cursor(c.0)),
    }
}

fn debug_req(r: &RunsRequest) -> String {
    format!(
        "start={:?} end={:?} rt={:?} st={:?} q={:?}",
        r.start_ns, r.end_ns, r.run_types, r.statuses, r.q
    )
}

/// The forced traps (plan §6.4 "explicit traps") at limit 50, and at limit 1 (+ exact tie order at 1, 2, 3) when
/// `limit_one`.
async fn check_traps(fx: &Fixture, reference: &[SpanRow], limit_one: bool) {
    let t = &fx.traps;
    let listed = |v: &[Pos], k: (TraceId, SpanId)| v.iter().any(|p| (p.1, p.2) == k);
    let limits: &[usize] = if limit_one { &[1, 50] } else { &[50] };
    for &limit in limits {
        let req = |f: &dyn Fn(&mut RunsRequest)| {
            let mut q = runs_request("a");
            q.limit = limit;
            f(&mut q);
            q
        };
        let all = page_all(fx, req(&|_| {})).await;
        for k in [t.p, t.p2, t.null_start] {
            assert!(!listed(&all, k), "{k:?} must not be listed");
        }
        for k in &t.ties {
            assert!(listed(&all, *k), "tie {k:?} missing");
        }
        let err = page_all(fx, req(&|q| q.statuses = [SpanStatus::Error].into())).await;
        assert!(listed(&err, t.r) && listed(&err, t.r2), "status=error lists R and R2");
        let unset = page_all(fx, req(&|q| q.statuses = [SpanStatus::Unset].into())).await;
        assert!(
            !listed(&unset, t.r) && !listed(&unset, t.r2),
            "status=unset lists neither"
        );
        let llm = page_all(fx, req(&|q| q.run_types = [RunType::Llm].into())).await;
        assert!(listed(&llm, t.l), "run_type=llm lists L");
        let new = page_all(fx, req(&|q| q.q = Some("new-name".into()))).await;
        assert!(listed(&new, t.n), "q=new lists N");
        let old = page_all(fx, req(&|q| q.q = Some("old-name".into()))).await;
        assert!(!listed(&old, t.n), "q=old does not list N");
    }
    // Exact tie order at small limits.
    let want = ref_runs(reference, &runs_request("a"));
    let small: &[usize] = if limit_one { &[1, 2, 3] } else { &[] };
    for &limit in small {
        let mut q = runs_request("a");
        q.limit = limit;
        assert_eq!(page_all(fx, q).await, want, "limit {limit}");
    }
}

/// The wire cursor: round trip, filter mismatch, garbage, and an extreme start.
async fn check_cursor(fx: &Fixture, reference: &[SpanRow]) {
    let a = pid("a");
    let params = |status: Option<&str>, cursor: Option<String>| RunsParams {
        status: status.map(Into::into),
        limit: Some(2),
        cursor,
        ..Default::default()
    };
    let req = RunsRequest::parse(params(None, None), a.clone()).unwrap();
    let body: Value = serde_json::from_slice(&fx.engine.runs_json(&req).await.unwrap()).unwrap();
    let page = fx.engine.list_runs(&req).await.unwrap();
    // runs_json == the page, field by field.
    let by_key: std::collections::HashMap<(TraceId, SpanId), &SpanRow> = reference
        .iter()
        .filter(|r| r.project_id.as_ref() == Some(&a))
        .map(|r| ((r.trace_id, r.span_id), r))
        .collect();
    let want: Vec<Value> = positions(&page.rows)
        .iter()
        .map(|p| Value::Object(expected_span(by_key[&(p.1, p.2)], false, true)))
        .collect();
    assert_eq!(body["runs"], Value::Array(want));
    let hex = body["next_cursor"].as_str().expect("a cursor").to_string();
    assert_eq!(hex.len(), 74);
    let again = RunsRequest::parse(params(None, Some(hex.clone())), a.clone()).unwrap();
    assert_eq!(again.cursor.as_ref().map(|c| c.0), page.next_cursor.map(|c| c.0));
    for bad in [
        params(Some("error"), Some(hex.clone())),
        params(None, Some("zz".into())),
        params(None, Some(hex[..72].into())),
    ] {
        let e = RunsRequest::parse(bad, a.clone());
        assert!(matches!(e, Err(QueryError::InvalidInput(_))), "{:?}", e.err());
    }
    let mut q = runs_request("a");
    q.cursor = Some(Cursor((i64::MAX, TraceId([0xff; 16]), SpanId([0xff; 8]))));
    assert_eq!(page_all(fx, clone_req(&q)).await, ref_runs(reference, &q));
}

async fn check_fixture(seed: u64, limit_one: bool) {
    let (layout, traps) = random_layout(seed);
    let fx = Fixture::build(layout, traps, QueryConfig::default()).await;
    let reference = fx.reference();
    for (p, t) in fx.traces() {
        check_trace(&fx, &reference, &p, t).await;
    }
    for (p, t) in [("a", tid(777)), ("b", tid(200)), ("nope", tid(0))] {
        let e = fx.engine.load_trace(&treq(p, t, true, TraceFormat::Tree)).await;
        assert!(matches!(e, Err(QueryError::NotFound(_))), "{p}/{t}: {:?}", e.err());
    }
    check_runs(&fx, &reference, seed, limit_one).await;
    check_traps(&fx, &reference, limit_one).await;
    check_cursor(&fx, &reference).await;
    // The effective-total keys (decision 15).
    let tot = |s: u64| {
        rows_of(&reference, "a", tid(114))
            .iter()
            .find(|r| r.span_id == sid(s))
            .unwrap()
            .total_tokens
    };
    assert_eq!((tot(0), tot(1)), (Some(25), Some(100)));
    let q = format!(
        "SELECT total_tokens FROM spans WHERE project_id = 'a' AND trace_id = {} ORDER BY span_id",
        hex_lit(tid(114).as_bytes())
    );
    let b = sql(&fx.engine, &q).await;
    let mut got: Vec<i64> = b
        .column(0)
        .as_primitive::<arrow::datatypes::Int64Type>()
        .values()
        .to_vec();
    got.sort();
    assert_eq!(got, [25, 100]);
}

/// Seeds `first, first + 5, …, ≤ 20`; `limit=1` paging on seeds 1..=5 only (one per group).
async fn seeds(first: u64) {
    for seed in (first..=20).step_by(5) {
        check_fixture(seed, seed <= 5).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn differential_seeds_1() {
    seeds(1).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn differential_seeds_2() {
    seeds(2).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn differential_seeds_3() {
    seeds(3).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn differential_seeds_4() {
    seeds(4).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn differential_seeds_5() {
    seeds(5).await;
}

fn random_fixtures(seed: u64) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(check_fixture(seed, false));
}

// 16 random fixtures, as 4 proptests of 4 cases (they run in parallel).
proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(4))]
    #[test]
    fn differential_random_fixtures_a(seed in 21u64..1_000_000) { random_fixtures(seed) }
    #[test]
    fn differential_random_fixtures_b(seed in 1_000_000u64..2_000_000) { random_fixtures(seed) }
    #[test]
    fn differential_random_fixtures_c(seed in 2_000_000u64..3_000_000) { random_fixtures(seed) }
    #[test]
    fn differential_random_fixtures_d(seed in 3_000_000u64..4_000_000) { random_fixtures(seed) }
}

/// A 300-span agent trace over 3 flushes and many small memtable batches: rows, tree (µs ties broken by span id,
/// null starts last), dotted order and JSON.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn big_trace_matches_reference() {
    let layout = big_trace(300, 3, 3, 64, 11);
    let fx = Fixture::build(layout, Traps::default(), QueryConfig::default()).await;
    let reference = fx.reference();
    assert_eq!(rows_of(&reference, "big", tid(999)).len(), 300);
    check_trace(&fx, &reference, "big", tid(999)).await;
    let v: Value = serde_json::from_slice(
        &fx.engine
            .trace_json(&treq("big", tid(999), false, TraceFormat::Flat))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(v["span_count"], json!(300));
}
