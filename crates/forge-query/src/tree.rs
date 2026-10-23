//! The trace tree from `parent_span_id` (D4). Iterative throughout; orphans, self-parents and parent-cycle
//! breakers become extra roots.
use crate::QueryError;
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::TimestampNanosecondType;
use forge_model::error::ModelError;
use forge_model::merge::Cancel;
use forge_model::schema::col;
use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub struct TraceTree {
    /// (row, depth) in output order; every row exactly once.
    pub preorder: Vec<(u32, u32)>,
    /// Per row: the parent id is not in the trace.
    pub orphan: Vec<bool>,
    /// Per row: its own parent, or the breaker of a parent cycle.
    pub cycle: Vec<bool>,
}

impl TraceTree {
    /// `rows`: the merged rows of ONE trace (one row per span), with span_id, parent_span_id and start_time.
    ///
    /// 1. null parent = true root; parent == self = extra root (cycle); parent present = child; parent missing =
    ///    extra root (orphan).
    /// 2. Rows not reachable from any root lie in, or hang off, a parent cycle. For each such row in row order,
    ///    walk parent pointers until a row repeats: that is the cycle. Its member with the minimal sibling key is
    ///    the breaker: detached from its parent, flagged `cycle`, an extra root.
    /// 3. Sibling key `(start µs NULLS LAST, span_id bytes)` orders children and both root lists; roots = true
    ///    roots ++ extra roots.
    pub fn build(rows: &RecordBatch, cancel: &Cancel) -> Result<Self, QueryError> {
        let s = rows.schema();
        let span = rows.column(s.index_of(col::SPAN_ID)?).as_binary::<i32>();
        let parent = rows.column(s.index_of(col::PARENT_SPAN_ID)?).as_binary::<i32>();
        let start = rows
            .column(s.index_of(col::START_TIME)?)
            .as_primitive::<TimestampNanosecondType>();
        let n = rows.num_rows();
        let key = |i: usize| {
            let us = start.is_valid(i).then(|| start.value(i).div_euclid(1000));
            (us.is_none(), us.unwrap_or(0), span.value(i))
        };
        let index: HashMap<&[u8], usize> = (0..n).map(|i| (span.value(i), i)).collect();
        if index.len() != n {
            return Err(ModelError::Schema("duplicate span ids in a merged trace".into()).into());
        }
        let (mut orphan, mut cycle) = (vec![false; n], vec![false; n]);
        let mut par: Vec<Option<usize>> = vec![None; n];
        let mut children: Vec<Vec<usize>> = vec![Vec::new(); n];
        let (mut roots, mut extra) = (Vec::new(), Vec::new());
        for i in 0..n {
            if parent.is_null(i) {
                roots.push(i);
                continue;
            }
            match index.get(parent.value(i)) {
                Some(&j) if j == i => {
                    cycle[i] = true;
                    extra.push(i);
                }
                Some(&j) => {
                    par[i] = Some(j);
                    children[j].push(i);
                }
                None => {
                    orphan[i] = true;
                    extra.push(i);
                }
            }
        }
        // Mark what the roots reach (explicit stack, no output).
        let mut reached = vec![false; n];
        let mark = |r: usize, reached: &mut Vec<bool>, children: &Vec<Vec<usize>>| {
            let mut stack = vec![r];
            while let Some(i) = stack.pop() {
                if !std::mem::replace(&mut reached[i], true) {
                    stack.extend(&children[i]);
                }
            }
        };
        for &r in roots.iter().chain(&extra) {
            mark(r, &mut reached, &children);
        }
        cancel.check()?;
        // Break every parent cycle at its minimal-key member.
        let mut seen: HashMap<usize, usize> = HashMap::new();
        for i in 0..n {
            if reached[i] {
                continue;
            }
            seen.clear();
            let mut cur = i;
            while !seen.contains_key(&cur) {
                seen.insert(cur, seen.len());
                cur = par[cur].expect("an unreached row has a parent");
            }
            let mut members = vec![cur];
            let mut m = par[cur].expect("cycle member");
            while m != cur {
                members.push(m);
                m = par[m].expect("cycle member");
            }
            let b = *members.iter().min_by_key(|&&m| key(m)).expect("non-empty cycle");
            let p = par[b].take().expect("cycle member");
            children[p].retain(|&x| x != b);
            cycle[b] = true;
            extra.push(b);
            mark(b, &mut reached, &children);
        }
        cancel.check()?;
        for c in &mut children {
            c.sort_by_key(|&i| key(i));
        }
        roots.sort_by_key(|&i| key(i));
        extra.sort_by_key(|&i| key(i));
        cancel.check()?;
        let mut preorder = Vec::with_capacity(n);
        let mut stack: Vec<(usize, u32)> = roots.iter().chain(&extra).rev().map(|&r| (r, 0)).collect();
        while let Some((i, d)) = stack.pop() {
            preorder.push((i as u32, d));
            stack.extend(children[i].iter().rev().map(|&c| (c, d + 1)));
        }
        debug_assert_eq!(preorder.len(), n);
        Ok(Self {
            preorder,
            orphan,
            cycle,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_model::row::SpanRow;
    use forge_model::schema::projected_schema;
    use forge_model::testing::{Parent, RefNode, Rng, batch_from_rows, pid, ref_tree, sid, tid, tree_parents};
    use proptest::prelude::*;

    fn tree_batch(rows: &[SpanRow]) -> RecordBatch {
        let b = batch_from_rows(rows);
        let s = projected_schema(&[col::SPAN_ID, col::PARENT_SPAN_ID, col::START_TIME]);
        let idx: Vec<usize> = s
            .fields()
            .iter()
            .map(|f| b.schema().index_of(f.name()).unwrap())
            .collect();
        b.project(&idx).unwrap()
    }

    fn got(rows: &[SpanRow]) -> Vec<RefNode> {
        let b = tree_batch(rows);
        let t = TraceTree::build(&b, &Cancel::never()).unwrap();
        t.preorder
            .iter()
            .map(|&(r, depth)| {
                let r = r as usize;
                RefNode {
                    span: rows[r].span_id,
                    depth,
                    orphan: t.orphan[r],
                    cycle: t.cycle[r],
                }
            })
            .collect()
    }

    fn rows(parents: &[Parent], starts: &[Option<i64>]) -> Vec<SpanRow> {
        parents
            .iter()
            .enumerate()
            .map(|(i, p)| SpanRow {
                project_id: Some(pid("p")),
                trace_id: tid(1),
                span_id: sid(i as u64),
                parent_span_id: p.id(9_999),
                start_time_ns: starts[i],
                ..Default::default()
            })
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]
        /// Random parent maps (<= 60 nodes, forced cycles, tails, orphans, null starts, shared µs) == the oracle.
        #[test]
        fn matches_ref_tree(seed in 1u64.., n in 9u64..60) {
            let mut r = Rng(seed);
            let parents = tree_parents(&mut r, n);
            let starts: Vec<Option<i64>> =
                (0..n).map(|_| {
                    let t = 1_000_000 + (r.below(6) * 1000 + r.below(1000)) as i64;
                    r.opt(85, t)
                }).collect();
            let rows = rows(&parents, &starts);
            prop_assert_eq!(got(&rows), ref_tree(&rows));
        }
    }

    /// No recursion: a 100k-deep chain.
    #[test]
    fn deep_chain() {
        let n = 100_000u64;
        let parents: Vec<Parent> = (0..n)
            .map(|i| if i == 0 { Parent::Root } else { Parent::Span(i - 1) })
            .collect();
        let rows = rows(&parents, &vec![Some(5); n as usize]);
        let t = TraceTree::build(&tree_batch(&rows), &Cancel::never()).unwrap();
        assert_eq!(t.preorder.len(), n as usize);
        assert_eq!(t.preorder.last(), Some(&((n - 1) as u32, (n - 1) as u32)));
    }

    #[test]
    fn cancelled() {
        let rows = rows(&[Parent::Root], &[None]);
        let e = TraceTree::build(&tree_batch(&rows), &Cancel::after(0));
        assert!(matches!(e, Err(QueryError::Model(ModelError::Cancelled))));
    }
}
