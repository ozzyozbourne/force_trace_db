//! dotted_order resolution against an oracle computed from the generator's own tree.
use forge_ingest::normalize::resolve_dotted_orders;
use forge_model::dotted::dotted_order;
use forge_model::ids::{SpanId, TraceId};
use forge_model::row::SpanRow;
use forge_model::testing::{Rng, pid, sid, tid};
use proptest::prelude::*;
use std::collections::HashMap;

type Key = (String, TraceId, SpanId);
type Node = (Option<SpanId>, Option<i64>); // (parent, start)

fn key(r: &SpanRow) -> Key {
    (r.project_id.as_ref().unwrap().to_string(), r.trace_id, r.span_id)
}

/// Some(d) iff the whole chain is present, ends at a root and has start times; cycles are None.
fn oracle(k: &Key, nodes: &HashMap<Key, Node>, path: &mut Vec<Key>) -> Option<String> {
    if path.contains(k) {
        return None; // cycle
    }
    let &(parent, start) = nodes.get(k)?;
    let start = start?;
    path.push(k.clone());
    let d = match parent {
        None => Some(dotted_order(None, start, k.2)),
        Some(p) => oracle(&(k.0.clone(), k.1, p), nodes, path).map(|pd| dotted_order(Some(&pd), start, k.2)),
    };
    path.pop();
    d
}

/// Span ids 1..=8 reused across 2 projects x 2 traces with different parents: self-parents, cycles, parents
/// missing from the batch, null starts, duplicates with other starts, and sometimes one 40-deep chain.
fn generate(seed: u64) -> Vec<SpanRow> {
    let mut r = Rng(seed);
    let mut rows = Vec::new();
    for p in ["a", "b"] {
        for t in 1..=2u64 {
            let n = 1 + r.below(8);
            for s in 1..=n {
                let parent = match r.below(10) {
                    0..=2 => None,
                    3 => Some(sid(s)),                // self-parent
                    4 => Some(sid(100 + r.below(3))), // not in the batch
                    _ => Some(sid(1 + r.below(n))),   // in the batch: trees, 2-3-cycles
                };
                let ns = 1_700_000_000_000_000_000 + r.below(1_000_000) as i64;
                let start = r.opt(90, ns);
                rows.push(SpanRow {
                    project_id: Some(pid(p)),
                    trace_id: tid(t),
                    span_id: sid(s),
                    parent_span_id: parent,
                    start_time_ns: start,
                    ..Default::default()
                });
            }
        }
    }
    if r.chance(30) {
        for d in 0..40u64 {
            rows.push(SpanRow {
                project_id: Some(pid("deep")),
                trace_id: tid(9),
                span_id: sid(1000 + d),
                parent_span_id: (d > 0).then(|| sid(1000 + d - 1)),
                start_time_ns: Some(1_700_000_000_000_000_000 + d as i64),
                ..Default::default()
            });
        }
    }
    // In-batch duplicates (start + end post): same key, other start time, sometimes another parent.
    for _ in 0..r.below(4) {
        let mut d = rows[r.below(rows.len() as u64) as usize].clone();
        let t = 1_800_000_000_000_000_000 + r.below(1000) as i64;
        d.start_time_ns = r.opt(80, t);
        if r.chance(30) {
            d.parent_span_id = None;
        }
        rows.push(d);
    }
    // Shuffle so children may come before parents.
    for i in (1..rows.len()).rev() {
        rows.swap(i, r.below(i as u64 + 1) as usize);
    }
    rows
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn dotted_order_matches_tree_oracle(seed in 1u64..) {
        let mut rows = generate(seed);
        // The last occurrence of a key defines its node.
        let nodes: HashMap<Key, Node> = rows.iter().map(|r| (key(r), (r.parent_span_id, r.start_time_ns))).collect();
        resolve_dotted_orders(&mut rows);
        for r in &rows {
            let want = oracle(&key(r), &nodes, &mut Vec::new());
            prop_assert_eq!(&r.dotted_order, &want, "{:?}", key(r));
        }
    }
}

#[test]
fn producer_dotted_order_is_kept() {
    let mut rows = vec![SpanRow {
        project_id: Some(pid("a")),
        trace_id: tid(1),
        span_id: sid(1),
        start_time_ns: Some(1),
        dotted_order: Some("producer".into()),
        ..Default::default()
    }];
    resolve_dotted_orders(&mut rows);
    assert_eq!(rows[0].dotted_order.as_deref(), Some("producer"));
}
