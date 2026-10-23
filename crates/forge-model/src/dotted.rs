use crate::ids::{ProjectId, SpanId, TraceId};
use crate::row::SpanRow;
use chrono::format::{Item, StrftimeItems};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::LazyLock;

/// Length of one segment: 22-char timestamp + 16 hex chars.
pub const SEGMENT_LEN: usize = 22 + 16;

/// One path element, LangSmith-compatible timestamp prefix: `%Y%m%dT%H%M%S%6fZ` + span_id hex.
/// e.g. `20261022T100000040000Zb7ad6b7169203331`. Sorts as (start µs, span_id).
pub fn dotted_segment(start_time_ns: i64, span_id: SpanId) -> String {
    let mut out = String::with_capacity(SEGMENT_LEN);
    push_segment(&mut out, start_time_ns, span_id);
    out
}
/// Child = parent + "." + segment; root = segment.
pub fn dotted_order(parent: Option<&str>, start_time_ns: i64, span_id: SpanId) -> String {
    let mut out = String::with_capacity(parent.map_or(0, |p| p.len() + 1) + SEGMENT_LEN);
    if let Some(p) = parent {
        out.push_str(p);
        out.push('.');
    }
    push_segment(&mut out, start_time_ns, span_id);
    out
}

/// The timestamp pattern, parsed once.
static TS_ITEMS: LazyLock<Vec<Item<'static>>> = LazyLock::new(|| StrftimeItems::new("%Y%m%dT%H%M%S%6fZ").collect());

fn push_segment(out: &mut String, start_time_ns: i64, span_id: SpanId) {
    let ts = DateTime::<Utc>::from_timestamp_nanos(start_time_ns);
    write!(out, "{}{span_id}", ts.format_with_items(TS_ITEMS.iter())).expect("writing to a String");
}
/// Number of ancestors + 1. Works for LangSmith (UUID) and Forge (hex16) segments alike.
pub fn depth(dotted: &str) -> usize {
    dotted.bytes().filter(|&b| b == b'.').count() + 1
}

// ---- resolving: one walk for ingest and compaction (plans/phase-4 D5). An ancestor's existing dotted_order is the
// prefix; a value is computed when the chain ends at a root or at such an ancestor, otherwise it stays null. ----

/// A node of the walk: (parent key, start_time ns, existing dotted_order).
pub type Node<K> = (Option<K>, Option<i64>, Option<String>);

/// The dotted_order of each key in `want` that has no existing value and whose chain resolves. Cycles resolve to
/// null unless they pass through a node with an existing value.
fn walk<K: Clone + Eq + std::hash::Hash>(
    nodes: &HashMap<K, Node<K>>,
    want: impl IntoIterator<Item = K>,
    span: impl Fn(&K) -> SpanId,
) -> HashMap<K, String> {
    let mut memo: HashMap<K, Option<String>> = nodes
        .iter()
        .filter_map(|(k, n)| n.2.clone().map(|d| (k.clone(), Some(d))))
        .collect();
    let mut out = HashMap::new();
    for key in want {
        if memo.get(&key).is_some_and(Option::is_some) {
            continue; // already has one (or resolved as someone's ancestor and recorded then)
        }
        // Walk up until a memoized node, a root, a parent that is not a node, or a cycle.
        // `prefix`: Some(None) = root, Some(Some(p)) = memoized prefix, None = unknown.
        let mut chain: Vec<(K, Option<i64>)> = Vec::new();
        let mut cur = key;
        let mut prefix: Option<Option<String>> = loop {
            if let Some(m) = memo.get(&cur) {
                break m.clone().map(Some);
            }
            let Some((parent, start, _)) = nodes.get(&cur) else {
                break None;
            };
            chain.push((cur.clone(), *start));
            if chain.len() > nodes.len() {
                break None; // cycle
            }
            match parent {
                None => break Some(None),
                Some(p) => cur = p.clone(),
            }
        };
        for (k, start) in chain.into_iter().rev() {
            let v = match (&prefix, start) {
                (Some(p), Some(st)) => Some(dotted_order(p.as_deref(), st, span(&k))),
                _ => None,
            };
            memo.insert(k.clone(), v.clone());
            if let Some(d) = &v {
                out.insert(k, d.clone());
            }
            prefix = v.map(Some);
        }
    }
    out
}

type Key = (ProjectId, TraceId, SpanId);

/// Ingest: sets `dotted_order` on every row that lacks one, whose key has no producer value in `rows`, and whose
/// ancestor chain resolves within `rows` (to a root, or to an ancestor with a dotted_order). For duplicates (the
/// start and end posts of one span) the last occurrence defines parent and start, and the last non-null
/// dotted_order is the key's existing value.
pub fn resolve_dotted_orders(rows: &mut [SpanRow]) {
    let mut nodes: HashMap<Key, Node<Key>> = HashMap::with_capacity(rows.len());
    for r in rows.iter() {
        let Some(p) = &r.project_id else { continue };
        let parent = r.parent_span_id.map(|ps| (p.clone(), r.trace_id, ps));
        let e = nodes.entry((p.clone(), r.trace_id, r.span_id)).or_default();
        *e = (parent, r.start_time_ns, r.dotted_order.clone().or(e.2.take()));
    }
    let want: Vec<Key> = rows
        .iter()
        .filter(|r| r.dotted_order.is_none())
        .filter_map(|r| Some((r.project_id.clone()?, r.trace_id, r.span_id)))
        .collect();
    let resolved = walk(&nodes, want, |k| k.2);
    for r in rows.iter_mut().filter(|r| r.dotted_order.is_none()) {
        let Some(p) = &r.project_id else { continue };
        r.dotted_order = resolved.get(&(p.clone(), r.trace_id, r.span_id)).cloned();
    }
}

/// Compaction backfill (CR §4): `nodes` has every known node of one trace slice (merged job rows + context).
/// Returns the dotted_order for each key in `want` whose own dotted is null and whose chain resolves.
pub fn resolve_with_known(
    nodes: &HashMap<(TraceId, SpanId), Node<(TraceId, SpanId)>>,
    want: impl IntoIterator<Item = (TraceId, SpanId)>,
) -> HashMap<(TraceId, SpanId), String> {
    walk(nodes, want, |k| k.1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use proptest::prelude::*;

    fn sid(x: u64) -> SpanId {
        SpanId(x.to_be_bytes())
    }

    /// The property dotted_order relies on: segments sort as (start µs, span_id).
    fn check_order(t1: i64, s1: SpanId, t2: i64, s2: SpanId) -> Result<(), TestCaseError> {
        let got = dotted_segment(t1, s1).cmp(&dotted_segment(t2, s2));
        let want = (t1.div_euclid(1000), s1).cmp(&(t2.div_euclid(1000), s2));
        prop_assert_eq!(got, want, "t1={} t2={} s1={} s2={}", t1, t2, s1, s2);
        Ok(())
    }

    #[test]
    fn fixture() {
        let ns = chrono::Utc
            .with_ymd_and_hms(2026, 10, 22, 10, 0, 0)
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap()
            + 40_000_000;
        let s: SpanId = "b7ad6b7169203331".parse().unwrap();
        let seg = dotted_segment(ns, s);
        assert_eq!(seg, "20261022T100000040000Zb7ad6b7169203331");
        assert_eq!(seg.len(), SEGMENT_LEN);
        assert_eq!(dotted_order(None, ns, s), seg);
        let child = dotted_order(Some(&seg), ns + 1_000, sid(1));
        assert_eq!(child, format!("{seg}.{}", dotted_segment(ns + 1_000, sid(1))));
        assert_eq!(depth(&seg), 1);
        assert_eq!(depth(&child), 2);
    }

    #[test]
    fn subtree_sorts_before_later_sibling() {
        let a = dotted_order(None, 1_000_000, sid(1));
        let ab = dotted_order(Some(&a), 2_000_000, sid(2));
        let abc = dotted_order(Some(&ab), 9_000_000, sid(9));
        let ac = dotted_order(Some(&a), 3_000_000, sid(3));
        assert!(dotted_segment(3_000_000, sid(3)) > dotted_segment(2_000_000, sid(2)));
        assert!(a < ab && ab < abc && abc < ac, "{a} {ab} {abc} {ac}");
    }

    fn rollover_us() -> impl Strategy<Value = i64> {
        prop_oneof![
            (-1_000_000_000i64..1_000_000_000).prop_map(|k| k * 1_000_000),
            (-10_000_000i64..10_000_000).prop_map(|k| k * 60_000_000),
            (-100_000i64..100_000).prop_map(|k| k * 86_400_000_000),
            (1678i32..2262).prop_map(|y| {
                chrono::Utc
                    .with_ymd_and_hms(y, 1, 1, 0, 0, 0)
                    .unwrap()
                    .timestamp_micros()
            }),
        ]
    }

    proptest! {
        #[test]
        fn order_any(t1 in any::<i64>(), t2 in any::<i64>(), x in any::<u64>(), y in any::<u64>()) {
            check_order(t1, sid(x), t2, sid(y))?;
        }

        #[test]
        fn order_same_micro(base in -9_000_000_000_000_000i64..9_000_000_000_000_000, n1 in 0i64..1000,
                            n2 in 0i64..1000, x in any::<u64>(), first in any::<bool>(), b in 1u8..=255) {
            let t0 = base.div_euclid(1000) * 1000;
            let s1 = sid(x);
            let mut s2 = s1;
            let i = if first { 0 } else { 7 };
            s2.0[i] = s2.0[i].wrapping_add(b);
            check_order(t0 + n1, s1, t0 + n2, s2)?;
            check_order(t0 + n1, s1, t0 + n2, s1)?;
        }

        #[test]
        fn order_across_rollover(b in rollover_us(), n1 in 0i64..1000, n2 in 0i64..1000,
                                 x in any::<u64>(), y in any::<u64>()) {
            let (before, after) = ((b - 1) * 1000 + n1, b * 1000 + n2);
            check_order(before, sid(x), after, sid(y))?;
            check_order(after, sid(x), before, sid(y))?;
        }

        #[test]
        fn sort_by_dotted_order_is_preorder(
            nodes in prop::collection::vec((any::<usize>(), any::<i64>(), any::<u32>()), 1..=60)
        ) {
            // node 0 is the root; node i > 0 has parent p % i.
            let n = nodes.len();
            let start: Vec<i64> = nodes.iter().map(|x| x.1).collect();
            let span: Vec<SpanId> =
                nodes.iter().enumerate().map(|(i, x)| sid((u64::from(x.2) << 32) | i as u64)).collect();
            let parent = |i: usize| nodes[i].0 % i; // only for i >= 1
            let mut dotted: Vec<String> = Vec::with_capacity(n);
            for i in 0..n {
                let p = (i > 0).then(|| dotted[parent(i)].clone());
                dotted.push(dotted_order(p.as_deref(), start[i], span[i]));
            }
            let mut by_dotted: Vec<usize> = (0..n).collect();
            by_dotted.sort_by(|&a, &b| dotted[a].cmp(&dotted[b]));

            let mut children = vec![Vec::new(); n];
            for i in 1..n {
                children[parent(i)].push(i);
            }
            for c in &mut children {
                c.sort_by_key(|&i| (start[i].div_euclid(1000), span[i]));
            }
            let (mut pre, mut stack) = (Vec::new(), vec![0usize]);
            while let Some(i) = stack.pop() {
                pre.push(i);
                stack.extend(children[i].iter().rev());
            }
            prop_assert_eq!(by_dotted, pre);
        }
    }
}
