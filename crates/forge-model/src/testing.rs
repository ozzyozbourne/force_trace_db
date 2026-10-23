//! Fixtures shared by tests in other crates (feature `test-util`).

use crate::catalog::{HourBucket, Level, SegmentId, SegmentMeta, SegmentState, paths};
use crate::enums::{RunType, SpanStatus};
use crate::error::{ModelError, Result};
use crate::ids::{ProjectId, SpanId, TraceId};
use crate::row::SpanRow;
use crate::schema::{col, col_idx};
use arrow::array::{Array, AsArray, StringArray};
use arrow::record_batch::RecordBatch;

pub fn pid(s: &str) -> ProjectId {
    ProjectId::new(s).expect("valid test project id")
}

/// A one-row live segment of `project` whose start time is null; override fields with `..segment_meta(..)`.
pub fn segment_meta(project: &str, level: Level, hour: HourBucket) -> SegmentMeta {
    let id = SegmentId::new();
    let project_id = pid(project);
    SegmentMeta {
        id,
        path: paths::segment_path(&project_id, level, hour, id).expect("test hour in range"),
        project_id,
        level,
        partition_hour: hour,
        row_count: 1,
        byte_size: 1,
        fts_byte_size: None,
        min_start_ns: None,
        max_start_ns: None,
        start_time_nulls: 1,
        min_trace_id: TraceId::default(),
        max_trace_id: TraceId::default(),
        min_version: 1,
        max_version: 1,
        schema_version: 1,
        created_at_ms: 0,
        state: SegmentState::Live,
        deleted_at_ms: None,
    }
}

pub fn fixture_path(name: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/fixtures/")).join(name)
}

pub fn fixture_bytes(name: &str) -> Vec<u8> {
    std::fs::read(fixture_path(name)).expect("fixture")
}

/// Row identity as tests compare it: (project, trace, span).
pub type RowKey = (String, TraceId, SpanId);
pub fn row_key(r: &SpanRow) -> RowKey {
    (
        r.project_id.as_ref().expect("project").to_string(),
        r.trace_id,
        r.span_id,
    )
}

/// Every file under `dir`, recursively; empty if `dir` is missing.
pub fn files_under(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for e in rd {
        let p = e.expect("dir entry").path();
        if p.is_dir() {
            out.extend(files_under(&p));
        } else {
            out.push(p);
        }
    }
    out
}

pub struct Rng(pub u64);
impl Rng {
    pub fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
    pub fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
    pub fn opt<T>(&mut self, pct: u64, v: T) -> Option<T> {
        self.chance(pct).then_some(v)
    }
}

/// An agent-shaped span tree (perf.rs, soak.rs): span `i`'s parent is among the previous 8 spans, and depth
/// stays <= 10 (a too-deep pick becomes span 0). Call once per span, in order; `depth` holds every span's depth.
pub fn agent_parent(r: &mut Rng, depth: &mut [u32], i: u64) -> Option<u64> {
    (i > 0).then(|| {
        let mut p = i - 1 - r.below(i.min(8));
        if depth[p as usize] >= 9 {
            p = 0;
        }
        depth[i as usize] = depth[p as usize] + 1;
        p
    })
}

pub fn tid(n: u64) -> TraceId {
    let mut b = [0u8; 16];
    b[8..].copy_from_slice(&n.to_be_bytes());
    b[0] = 0xA0 | (n % 7) as u8; // not monotonic in n
    TraceId(b)
}
pub fn sid(n: u64) -> SpanId {
    SpanId(n.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes())
}

/// A random partial upsert of span (project, trace, span) with the given version.
pub fn random_row(r: &mut Rng, project: &str, trace: u64, span: u64, version: u64) -> SpanRow {
    let n = r.below(1000) as i64;
    let (d, ntags) = (r.below(50), r.below(3));
    SpanRow {
        project_id: Some(pid(project)),
        trace_id: tid(trace),
        span_id: sid(span),
        parent_span_id: r.opt(40, sid(span + 1000)),
        dotted_order: r.opt(50, format!("d{d:03}")),
        name: r.opt(50, format!("name-{n}")),
        run_type: r.opt(30, RunType::Llm),
        start_time_ns: r.opt(50, 1_700_000_000_000_000_000 + n),
        end_time_ns: r.opt(30, 1_700_000_000_000_000_000 + n + 5),
        status: r.opt(30, if n % 2 == 0 { SpanStatus::Ok } else { SpanStatus::Error }),
        error: r.opt(10, format!("err {n}")),
        model: r.opt(20, "gpt".into()),
        provider: r.opt(20, "openai".into()),
        prompt_tokens: r.opt(30, n),
        completion_tokens: r.opt(30, n + 1),
        total_tokens: r.opt(30, 2 * n + 1),
        cost_usd: r.opt(30, n as f64 / 100.0),
        session_id: r.opt(20, format!("s{}", n % 5)),
        tags: r.opt(30, (0..ntags).map(|i| format!("t{i}")).collect()),
        metadata: r.opt(30, format!("{{\"k\":{n}}}")),
        attributes: r.opt(30, format!("{{\"a\":{n}}}")),
        events: r.opt(20, "[]".into()),
        inputs: r.opt(
            40,
            format!("{{\"messages\":[{{\"role\":\"user\",\"content\":\"hi {n}\"}}]}}"),
        ),
        outputs: r.opt(40, format!("{{\"text\":\"out {n}\"}}")),
        version,
        deleted: r.chance(5),
    }
}

/// Naive dotted_order oracle (plans/phase-4 D5), written apart from `dotted.rs`: per key (one row each; the last
/// wins), an existing dotted_order wins; otherwise a root with a start is `dotted_order(None, start, span)`, and a
/// child with a start whose parent row exists and resolves is `parent value + "." + segment`. Cycles without an
/// existing value, missing parents and null starts give None.
pub fn ref_dotted(rows: &[SpanRow]) -> std::collections::BTreeMap<RowKey, Option<String>> {
    use std::collections::BTreeMap;
    let by_key: BTreeMap<RowKey, &SpanRow> = rows.iter().map(|r| (row_key(r), r)).collect();
    fn value(k: &RowKey, by_key: &BTreeMap<RowKey, &SpanRow>, seen: &mut Vec<RowKey>) -> Option<String> {
        let r = by_key.get(k)?;
        if let Some(d) = &r.dotted_order {
            return Some(d.clone());
        }
        if seen.contains(k) {
            return None;
        }
        let start = r.start_time_ns?;
        let segment = crate::dotted::dotted_segment(start, r.span_id);
        match r.parent_span_id {
            None => Some(segment),
            Some(p) => {
                seen.push(k.clone());
                let pv = value(&(k.0.clone(), k.1, p), by_key, seen);
                seen.pop();
                Some(format!("{}.{segment}", pv?))
            }
        }
    }
    by_key
        .keys()
        .map(|k| (k.clone(), value(k, &by_key, &mut Vec::new())))
        .collect()
}

/// Naive reference: per key, versions DESC, each field = first Some.
pub fn reference_merge(rows: &[SpanRow]) -> Vec<SpanRow> {
    use std::collections::BTreeMap;
    let mut by_key: BTreeMap<(String, TraceId, SpanId), Vec<&SpanRow>> = BTreeMap::new();
    for r in rows {
        by_key
            .entry((r.project_id.as_ref().unwrap().to_string(), r.trace_id, r.span_id))
            .or_default()
            .push(r);
    }
    let mut out = Vec::new();
    for (_, mut v) in by_key {
        v.sort_by_key(|r| std::cmp::Reverse(r.version));
        macro_rules! pick {
            ($f:ident) => {
                v.iter().find_map(|r| r.$f.clone())
            };
        }
        out.push(SpanRow {
            project_id: v[0].project_id.clone(),
            trace_id: v[0].trace_id,
            span_id: v[0].span_id,
            parent_span_id: pick!(parent_span_id),
            dotted_order: pick!(dotted_order),
            name: pick!(name),
            run_type: pick!(run_type),
            start_time_ns: pick!(start_time_ns),
            end_time_ns: pick!(end_time_ns),
            status: pick!(status),
            error: pick!(error),
            model: pick!(model),
            provider: pick!(provider),
            prompt_tokens: pick!(prompt_tokens),
            completion_tokens: pick!(completion_tokens),
            total_tokens: pick!(total_tokens),
            cost_usd: pick!(cost_usd),
            session_id: pick!(session_id),
            tags: pick!(tags),
            metadata: pick!(metadata),
            attributes: pick!(attributes),
            events: pick!(events),
            inputs: pick!(inputs),
            outputs: pick!(outputs),
            version: v[0].version,
            deleted: v[0].deleted,
        });
    }
    // physical order: project, trace, dotted NULLS LAST, span
    out.sort_by_key(physical_key);
    out
}

/// The physical sort key: (project, trace, dotted_order NULLS LAST, span).
pub fn physical_key(r: &SpanRow) -> (String, TraceId, bool, String, SpanId) {
    (
        r.project_id.as_ref().unwrap().to_string(),
        r.trace_id,
        r.dotted_order.is_none(),
        r.dotted_order.clone().unwrap_or_default(),
        r.span_id,
    )
}

/// Rows -> one spans_schema batch through `SpanBatchBuilder`; panics on an invalid row (tests only).
pub fn batch_from_rows(rows: &[SpanRow]) -> RecordBatch {
    let mut b = crate::row::SpanBatchBuilder::with_capacity(rows.len());
    for r in rows {
        b.push(r).expect("valid test row");
    }
    b.finish().expect("valid test batch")
}

/// Inverse of `batch_from_rows`: a spans_schema batch -> rows.
pub fn rows_from_batch(b: &RecordBatch) -> Result<Vec<SpanRow>> {
    use arrow::datatypes::{Float64Type, Int64Type, TimestampNanosecondType};
    crate::row::validate_batch(b)?;
    let s = |c: &str| b.column(col_idx(c)).as_string::<i32>().clone();
    let bin = |c: &str| b.column(col_idx(c)).as_binary::<i32>().clone();
    let i64c = |c: &str| b.column(col_idx(c)).as_primitive::<Int64Type>().clone();
    let ts = |c: &str| b.column(col_idx(c)).as_primitive::<TimestampNanosecondType>().clone();
    let (p, t, sp, par) = (
        s(col::PROJECT_ID),
        bin(col::TRACE_ID),
        bin(col::SPAN_ID),
        bin(col::PARENT_SPAN_ID),
    );
    let (dot, name, rt, st, et) = (
        s(col::DOTTED_ORDER),
        s(col::NAME),
        s(col::RUN_TYPE),
        ts(col::START_TIME),
        ts(col::END_TIME),
    );
    let (stat, err, model, prov) = (s(col::STATUS), s(col::ERROR), s(col::MODEL), s(col::PROVIDER));
    let (pt, ct, tt) = (
        i64c(col::PROMPT_TOKENS),
        i64c(col::COMPLETION_TOKENS),
        i64c(col::TOTAL_TOKENS),
    );
    let cost = b.column(col_idx(col::COST_USD)).as_primitive::<Float64Type>().clone();
    let sess = s(col::SESSION_ID);
    let tags = b.column(col_idx(col::TAGS)).as_list::<i32>().clone();
    let (md, at, ev, inp, outp) = (
        s(col::METADATA),
        s(col::ATTRIBUTES),
        s(col::EVENTS),
        s(col::INPUTS),
        s(col::OUTPUTS),
    );
    let ver = i64c(col::VERSION);
    let del = b.column(col_idx(col::DELETED)).as_boolean().clone();
    fn o<T>(a: &dyn Array, i: usize, f: impl FnOnce() -> Result<T>) -> Result<Option<T>> {
        if a.is_valid(i) { f().map(Some) } else { Ok(None) }
    }
    let txt = |a: &StringArray, i: usize| -> Option<String> { a.is_valid(i).then(|| a.value(i).to_string()) };
    (0..b.num_rows())
        .map(|i| {
            Ok(SpanRow {
                project_id: Some(ProjectId::new(p.value(i))?),
                trace_id: TraceId::from_slice(t.value(i))?,
                span_id: SpanId::from_slice(sp.value(i))?,
                parent_span_id: o(&par, i, || SpanId::from_slice(par.value(i)))?,
                dotted_order: txt(&dot, i),
                name: txt(&name, i),
                run_type: o(&rt, i, || rt.value(i).parse())?,
                start_time_ns: st.is_valid(i).then(|| st.value(i)),
                end_time_ns: et.is_valid(i).then(|| et.value(i)),
                status: o(&stat, i, || stat.value(i).parse())?,
                error: txt(&err, i),
                model: txt(&model, i),
                provider: txt(&prov, i),
                prompt_tokens: pt.is_valid(i).then(|| pt.value(i)),
                completion_tokens: ct.is_valid(i).then(|| ct.value(i)),
                total_tokens: tt.is_valid(i).then(|| tt.value(i)),
                cost_usd: cost.is_valid(i).then(|| cost.value(i)),
                session_id: txt(&sess, i),
                tags: tags.is_valid(i).then(|| {
                    let v = tags.value(i);
                    let v = v.as_string::<i32>();
                    (0..v.len()).map(|j| v.value(j).to_string()).collect()
                }),
                metadata: txt(&md, i),
                attributes: txt(&at, i),
                events: txt(&ev, i),
                inputs: txt(&inp, i),
                outputs: txt(&outp, i),
                version: u64::try_from(ver.value(i)).map_err(|_| ModelError::Schema("version must be >= 0".into()))?,
                deleted: del.value(i),
            })
        })
        .collect()
}

/// A fixed memtable for query tests.
#[derive(Debug, Default)]
pub struct FixedMemtable(pub Vec<RecordBatch>);
impl crate::memtable::MemtableView for FixedMemtable {
    fn snapshot(&self) -> crate::memtable::MemtableSnapshot {
        use crate::memtable::{AppendLog, MemtablePart, MemtableSnapshot};
        let mut log = AppendLog::default();
        self.0.iter().cloned().for_each(|b| log.push(b));
        MemtableSnapshot {
            parts: MemtablePart::of(&log, false).into_iter().collect(),
        }
    }
}

/// Sorts a (projected) spans batch by (project_id, trace_id, span_id).
pub fn sort_by_key(b: &RecordBatch) -> RecordBatch {
    use arrow::array::{ArrayRef, UInt32Array};
    use arrow::row::{RowConverter, SortField};
    let s = b.schema();
    let cols: Vec<ArrayRef> = [col::PROJECT_ID, col::TRACE_ID, col::SPAN_ID]
        .iter()
        .map(|c| b.column(s.index_of(c).unwrap()).clone())
        .collect();
    let conv = RowConverter::new(cols.iter().map(|a| SortField::new(a.data_type().clone())).collect()).unwrap();
    let rows = conv.convert_columns(&cols).unwrap();
    let mut idx: Vec<u32> = (0..b.num_rows() as u32).collect();
    idx.sort_by(|&x, &y| rows.row(x as usize).cmp(&rows.row(y as usize)));
    arrow::compute::take_record_batch(b, &UInt32Array::from(idx)).unwrap()
}

/// Parent of a generated span; spans are numbered `0..n` (ids `sid(n)`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Parent {
    Root,
    Span(u64),
    /// A parent id that is not in the trace.
    Missing,
}

impl Parent {
    pub fn id(self, missing: u64) -> Option<SpanId> {
        match self {
            Parent::Root => None,
            Parent::Span(s) => Some(sid(s)),
            Parent::Missing => Some(sid(missing)),
        }
    }
}

/// Parent map of one trace of `n >= 9` spans (D4 trees). The last 9 spans are forced: a 2-cycle, a 3-cycle with a
/// 2-node tail hanging off it, and an orphan with a child. Each other span: another span of the trace 60 %, a
/// missing id 10 %, itself 3 %, root otherwise.
pub fn tree_parents(r: &mut Rng, n: u64) -> Vec<Parent> {
    assert!(n >= 9, "tree_parents needs n >= 9");
    let k = n - 9;
    let mut v: Vec<Parent> = (0..k)
        .map(|i| match r.below(100) {
            0..60 => Parent::Span(r.below(n)),
            60..70 => Parent::Missing,
            70..73 => Parent::Span(i),
            _ => Parent::Root,
        })
        .collect();
    v.extend([
        Parent::Span(k + 1), // 2-cycle k <-> k+1
        Parent::Span(k),
        Parent::Span(k + 3), // 3-cycle k+2 -> k+3 -> k+4 -> k+2
        Parent::Span(k + 4),
        Parent::Span(k + 2),
        Parent::Span(k + 2), // tail k+6 -> k+5 -> k+2
        Parent::Span(k + 5),
        Parent::Missing, // orphan k+7 with child k+8
        Parent::Span(k + 7),
    ]);
    v
}

/// The D4 sibling key: (start µs NULLS LAST, span_id bytes).
pub fn sibling_key(r: &SpanRow) -> (bool, i64, SpanId) {
    (
        r.start_time_ns.is_none(),
        r.start_time_ns.map_or(0, |s| s.div_euclid(1000)),
        r.span_id,
    )
}

/// One span of a reference tree, in preorder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefNode {
    pub span: SpanId,
    pub depth: u32,
    pub orphan: bool,
    pub cycle: bool,
}

/// Naive D4 oracle over the merged rows of ONE trace: null parent = true root; self parent = extra root (cycle);
/// missing parent = extra root (orphan); every parent cycle is broken at its member with the minimal sibling key
/// (extra root, cycle). Roots = true roots ++ extra roots, each by sibling key; children by sibling key.
pub fn ref_tree(rows: &[SpanRow]) -> Vec<RefNode> {
    use std::collections::{BTreeSet, HashMap};
    let by_id: HashMap<SpanId, &SpanRow> = rows.iter().map(|r| (r.span_id, r)).collect();
    assert_eq!(by_id.len(), rows.len(), "one row per span");
    let link =
        |r: &SpanRow| -> Option<SpanId> { r.parent_span_id.filter(|p| *p != r.span_id && by_id.contains_key(p)) };
    // The cycle through `r`, if `r` is on one: follow links until `r` comes back (at most n steps).
    let cycle_of = |r: &SpanRow| -> Option<Vec<SpanId>> {
        let mut members = vec![r.span_id];
        let mut cur = link(r)?;
        for _ in 0..rows.len() {
            if cur == r.span_id {
                return Some(members);
            }
            members.push(cur);
            cur = link(by_id[&cur])?;
        }
        None
    };
    let breakers: BTreeSet<SpanId> = rows
        .iter()
        .filter_map(cycle_of)
        .map(|c| *c.iter().min_by_key(|s| sibling_key(by_id[*s])).unwrap())
        .collect();
    let parent_of = |r: &SpanRow| if breakers.contains(&r.span_id) { None } else { link(r) };
    let orphan = |r: &SpanRow| r.parent_span_id.is_some_and(|p| !by_id.contains_key(&p));
    let cycle = |r: &SpanRow| r.parent_span_id == Some(r.span_id) || breakers.contains(&r.span_id);
    let mut sorted: Vec<&SpanRow> = rows.iter().collect();
    sorted.sort_by_key(|r| sibling_key(r));
    let roots: Vec<&SpanRow> = sorted
        .iter()
        .filter(|r| r.parent_span_id.is_none())
        .chain(
            sorted
                .iter()
                .filter(|r| r.parent_span_id.is_some() && parent_of(r).is_none()),
        )
        .copied()
        .collect();
    fn emit<'a>(
        r: &'a SpanRow,
        depth: u32,
        sorted: &[&'a SpanRow],
        parent_of: &dyn Fn(&SpanRow) -> Option<SpanId>,
        flags: &dyn Fn(&SpanRow) -> (bool, bool),
        out: &mut Vec<RefNode>,
    ) {
        let (orphan, cycle) = flags(r);
        out.push(RefNode {
            span: r.span_id,
            depth,
            orphan,
            cycle,
        });
        for c in sorted.iter().filter(|c| parent_of(c) == Some(r.span_id)) {
            emit(c, depth + 1, sorted, parent_of, flags, out);
        }
    }
    let mut out = Vec::with_capacity(rows.len());
    for r in roots {
        emit(r, 0, &sorted, &parent_of, &|r| (orphan(r), cycle(r)), &mut out);
    }
    assert_eq!(out.len(), rows.len(), "every span exactly once");
    out
}

/// Decision 15, plain Rust: the producer total, else prompt + completion if both, else whichever count is set.
pub fn ref_effective_total(r: &SpanRow) -> Option<i64> {
    r.total_tokens.or(match (r.prompt_tokens, r.completion_tokens) {
        (Some(a), Some(b)) => Some(a.wrapping_add(b)),
        (a, b) => a.or(b),
    })
}

/// What every read path returns: the merged rows, tombstones dropped, the effective total_tokens.
pub fn reference(all: &[SpanRow]) -> Vec<SpanRow> {
    reference_merge(all)
        .into_iter()
        .filter(|r| !r.deleted)
        .map(|mut r| {
            r.total_tokens = ref_effective_total(&r);
            r
        })
        .collect()
}
