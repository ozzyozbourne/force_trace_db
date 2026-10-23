//! Trace tree from parent_span_id (D4). Iterative; orphans and cycle breakers become extra roots.
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::TimestampNanosecondType;
use forge_model::error::{ModelError, Result};
use forge_model::merge::Cancel;
use forge_model::schema::col;
use std::collections::HashMap;

pub const ORPHAN: u8 = 1;
pub const CYCLE: u8 = 2;

#[derive(Clone, Debug, Default)]
pub struct TraceTree {
    /// (row, depth) in output order.
    pub preorder: Vec<(u32, u32)>,
    /// Per row: ORPHAN | CYCLE.
    pub flags: Vec<u8>,
}

impl TraceTree {
    pub fn build(b: &RecordBatch, cancel: &Cancel) -> Result<Self> {
        let s = b.schema();
        let span = b.column(s.index_of(col::SPAN_ID)?).as_binary::<i32>();
        let parent = b.column(s.index_of(col::PARENT_SPAN_ID)?).as_binary::<i32>();
        let start = b.column(s.index_of(col::START_TIME)?).as_primitive::<TimestampNanosecondType>();
        let n = b.num_rows();
        // sibling key: (start µs NULLS LAST, span_id)
        let key = |i: usize| ((start.is_null(i)), if start.is_null(i) { 0 } else { start.value(i).div_euclid(1000) }, span.value(i));
        let index: HashMap<&[u8], usize> = (0..n).map(|i| (span.value(i), i)).collect();
        if index.len() != n {
            return Err(ModelError::Schema("duplicate span ids in a merged trace".into()));
        }
        let mut flags = vec![0u8; n];
        let mut par: Vec<Option<usize>> = vec![None; n];
        let mut children: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut roots = Vec::new();
        for i in 0..n {
            if parent.is_null(i) {
                roots.push(i);
                continue;
            }
            let p = parent.value(i);
            match index.get(p) {
                Some(&j) if j == i => {
                    flags[i] |= CYCLE;
                    roots.push(i);
                }
                Some(&j) => {
                    par[i] = Some(j);
                    children[j].push(i);
                }
                None => {
                    flags[i] |= ORPHAN;
                    roots.push(i);
                }
            }
        }
        roots.sort_by(|&a, &b| (flags[a] & ORPHAN, key(a)).cmp(&(flags[b] & ORPHAN, key(b))));
        for c in &mut children {
            c.sort_by(|&a, &b| key(a).cmp(&key(b)));
        }
        cancel.check()?;
        let mut visited = vec![false; n];
        let mut preorder = Vec::with_capacity(n);
        let dfs = |r: usize, visited: &mut Vec<bool>, preorder: &mut Vec<(u32, u32)>, children: &Vec<Vec<usize>>| {
            let mut stack = vec![(r, 0u32)];
            while let Some((i, d)) = stack.pop() {
                if visited[i] {
                    continue;
                }
                visited[i] = true;
                preorder.push((i as u32, d));
                for &c in children[i].iter().rev() {
                    stack.push((c, d + 1));
                }
            }
        };
        for &r in &roots {
            dfs(r, &mut visited, &mut preorder, &children);
        }
        let mut next = 0;
        while preorder.len() < n {
            while visited[next] {
                next += 1;
            }
            // walk parents until a row repeats
            let mut seen = HashMap::new();
            let mut cur = next;
            let mut k = 0;
            while let std::collections::hash_map::Entry::Vacant(e) = seen.entry(cur) {
                e.insert(k);
                k += 1;
                cur = par[cur].expect("unvisited rows have a parent");
            }
            let first = seen[&cur];
            let mut cycle: Vec<usize> = seen.iter().filter(|(_, v)| **v >= first).map(|(r, _)| *r).collect();
            cycle.sort_by(|&a, &b| key(a).cmp(&key(b)));
            let c = cycle[0];
            let p = par[c].take().expect("cycle member has a parent");
            children[p].retain(|&x| x != c);
            flags[c] |= CYCLE;
            dfs(c, &mut visited, &mut preorder, &children);
        }
        Ok(Self { preorder, flags })
    }
}
