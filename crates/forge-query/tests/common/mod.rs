//! Read-path fixture (plan §6.3): L0 flushes + a memtable over memory:// storage and in-memory SQLite, plus
//! reference helpers that do not use the code under test.
#![allow(dead_code)]

use arrow::array::RecordBatch;
use forge_meta::Metastore;
use forge_model::MemtableView;
use forge_model::catalog::HourBucket;
use forge_model::config::{QueryConfig, StorageConfig};
use forge_model::enums::{RunType, SpanStatus};
use forge_model::ids::{SpanId, TraceId};
use forge_model::row::SpanRow;
use forge_model::schema::{ALL_COLUMNS, JSON_COLUMNS, PAYLOAD_COLUMNS, col, project_to, projected_schema};
pub use forge_model::testing::sort_by_key;
use forge_model::testing::{FixedMemtable, RefNode, Rng, batch_from_rows, pid, reference, sid, tid, tree_parents};
use forge_query::QueryEngine;
use forge_query::runs::{Cursor, RunsParams, RunsRequest};
use forge_query::testing::{RecordingSegmentStore, write_flush};
use forge_query::trace::{TraceFormat, TraceRequest};
use forge_storage::{SegmentStore, VortexSegmentStore, build_object_store};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub const BASE: i64 = 1_700_000_000_000_000_000;
/// Random starts are `BASE + k * STEP`, k < 40, so ties happen.
pub const STEP: i64 = 1_000_037_011;
/// The forced tie cluster: above every other start in flushes 1 and 2.
pub const TIE: i64 = BASE + 60 * STEP;
pub const RANDOM_PROJECTS: [&str; 3] = ["a", "b", "e"];

/// Posts placed into L0 flushes and memtable batches. Versions are assigned in that order (WAL order).
#[derive(Default)]
pub struct Layout {
    pub flushes: Vec<Vec<SpanRow>>,
    pub mem: Vec<Vec<SpanRow>>,
    /// A copy of this flush's raw rows also sits in the memtable (the snapshot window): before the memtable
    /// tail when `window_first`, else after it.
    pub window: Option<usize>,
    pub window_first: bool,
}

/// Forced runs traps in project `a` (plan §6.3): (trace, span) of each.
#[derive(Clone, Debug, Default)]
pub struct Traps {
    /// status unset at start, `error` in the patches-only flush 4.
    pub r: (TraceId, SpanId),
    /// status ok at start (flush 1), `error` in flush 3 (higher max_start than the start row's segment).
    pub r2: (TraceId, SpanId),
    /// A root reparented in flush 4 / flush 3: never listed.
    pub p: (TraceId, SpanId),
    pub p2: (TraceId, SpanId),
    /// A root started in flush 2 and reparented in flush 5 (an older segment: lower, non-null max_start).
    pub p3: (TraceId, SpanId),
    /// A root started in flush 1 and reparented in the memtable.
    pub p4: (TraceId, SpanId),
    /// run_type chain at start, `llm` in flush 4.
    pub l: (TraceId, SpanId),
    /// renamed in flush 4.
    pub n: (TraceId, SpanId),
    /// A root with a null start in every version.
    pub null_start: (TraceId, SpanId),
    /// The tie cluster's keys.
    pub ties: Vec<(TraceId, SpanId)>,
}
pub const N_OLD: &str = "old-name-n";
pub const N_NEW: &str = "New-Name-N";

pub struct Fixture {
    pub engine: QueryEngine,
    pub meta: Arc<dyn Metastore>,
    pub store: Arc<RecordingSegmentStore>,
    pub mem: Arc<dyn MemtableView>,
    /// Every distinct post, with its version.
    pub all: Vec<SpanRow>,
    pub traps: Traps,
}

impl Fixture {
    pub async fn build(layout: Layout, traps: Traps, cfg: QueryConfig) -> Self {
        Self::build_with(layout, traps, cfg, 4).await
    }

    pub async fn build_with(mut layout: Layout, traps: Traps, cfg: QueryConfig, partitions: usize) -> Self {
        let mut v = 0;
        for r in layout.flushes.iter_mut().chain(layout.mem.iter_mut()).flatten() {
            v += 1;
            r.version = v;
        }
        let meta: Arc<dyn Metastore> = Arc::new(forge_meta::SqliteMetastore::open_in_memory().unwrap());
        let built = build_object_store(&StorageConfig {
            url: "memory://".into(),
        })
        .unwrap();
        let inner: Arc<dyn SegmentStore> = Arc::new(VortexSegmentStore::new(built));
        let store = Arc::new(RecordingSegmentStore::new(inner));
        let mut max_version = 0;
        for (i, f) in layout.flushes.iter().enumerate() {
            max_version += f.len() as u64;
            let b = batch_from_rows(f);
            let hour = HourBucket::from_nanos(BASE);
            write_flush(
                &*meta,
                store.clone(),
                &b,
                hour,
                "test",
                64 << 10,
                i as u64 + 1,
                max_version,
            )
            .await;
        }
        let mut mem: Vec<RecordBatch> = layout
            .mem
            .iter()
            .filter(|b| !b.is_empty())
            .map(|b| batch_from_rows(b))
            .collect();
        if let Some(w) = layout.window.filter(|&w| !layout.flushes[w].is_empty()) {
            let copy = batch_from_rows(&layout.flushes[w]);
            if layout.window_first {
                mem.insert(0, copy)
            } else {
                mem.push(copy)
            }
        }
        let all: Vec<SpanRow> = layout.flushes.into_iter().chain(layout.mem).flatten().collect();
        let mem: Arc<dyn MemtableView> = Arc::new(FixedMemtable(mem));
        let mut engine = QueryEngine::new(meta.clone(), store.clone(), mem.clone(), cfg);
        engine.target_partitions = partitions;
        Self {
            engine,
            meta,
            store,
            mem,
            all,
            traps,
        }
    }

    pub fn reference(&self) -> Vec<SpanRow> {
        reference(&self.all)
    }

    /// The project-`a` segment layout the runs traps rely on (`random_layout`): newest first, flush 3 (TIE + 5
    /// STEP), flushes 1 and 2 tied at TIE (read together), flush 5 last (its old root); flush 4 holds only patches
    /// (no start: never a candidate segment, reached through locate_traces only, e.g. by R).
    pub async fn assert_runs_layout(&self) {
        use forge_meta::SegmentFilter;
        let f = SegmentFilter {
            project_id: Some(pid("a")),
            newest_first: true,
            ..Default::default()
        };
        let segs = self.meta.list_segments(&f).await.unwrap();
        let starts: Vec<Option<i64>> = segs.iter().map(|s| s.max_start_ns).collect();
        assert_eq!(
            starts,
            [
                None,
                Some(TIE + 5 * STEP),
                Some(TIE),
                Some(TIE),
                Some(BASE - 1000 * STEP)
            ],
            "project a segments, newest first"
        );
        let r = self.meta.locate_trace(&pid("a"), self.traps.r.0).await.unwrap();
        assert!(
            r.iter().any(|l| l.segment.id == segs[0].id),
            "R's patch is in the start-less segment"
        );
    }

    /// Every (project, trace) with at least one post.
    pub fn traces(&self) -> BTreeSet<(String, TraceId)> {
        self.all
            .iter()
            .map(|r| (r.project_id.as_ref().unwrap().to_string(), r.trace_id))
            .collect()
    }
}

fn post(project: &str, trace: TraceId, span: SpanId) -> SpanRow {
    SpanRow {
        project_id: Some(pid(project)),
        trace_id: trace,
        span_id: span,
        ..Default::default()
    }
}

const RUN_TYPES: [RunType; 4] = [RunType::Llm, RunType::Chain, RunType::Tool, RunType::Agent];
const STATUSES: [SpanStatus; 3] = [SpanStatus::Ok, SpanStatus::Error, SpanStatus::Unset];

/// The posts of one random span, in order: start, end, patches, reparent, tombstone / re-post.
fn span_posts(r: &mut Rng, project: &str, trace: TraceId, s: u64, parent: Option<SpanId>) -> Vec<SpanRow> {
    let base = post(project, trace, sid(s));
    let k = r.below(40) as i64;
    let start = r.opt(90, BASE + k * STEP);
    let decoy = r.chance(20) && parent.is_some();
    let (sess, cost) = (r.below(3), r.below(1000) as f64 / 64.0);
    let mut out = vec![SpanRow {
        name: Some(format!("Span-{s}-{}", r.below(100))),
        run_type: Some(RUN_TYPES[r.below(4) as usize]),
        start_time_ns: start,
        // A later post sets the final parent (reparenting).
        parent_span_id: if decoy { r.opt(50, sid(s + 500)) } else { parent },
        status: r.opt(30, SpanStatus::Unset),
        session_id: r.opt(50, format!("sess-{sess}")),
        tags: match r.below(4) {
            0 => None,
            1 => Some(vec![]),
            _ => Some(vec!["t1".into(), format!("t{}", r.below(5))]),
        },
        metadata: match r.below(10) {
            0 => Some("\"x\"".into()),
            1 => Some("not json".into()),
            2..5 => None,
            _ => Some(format!("{{\"k\":{}}}", r.below(9))),
        },
        attributes: r.opt(60, format!("{{\"a\":{s}}}")),
        inputs: r.opt(
            70,
            format!("{{\"messages\":[{{\"role\":\"user\",\"content\":\"hi {s}\"}}]}}"),
        ),
        ..base.clone()
    }];
    if r.chance(80) {
        let (p, c) = (r.below(100) as i64, r.below(100) as i64);
        let (pt, ct, tt) = match r.below(4) {
            0 => (Some(p), Some(c), Some(p + c + 7)), // producer total != p + c
            1 => (Some(p), Some(c), None),
            2 => (Some(p), None, None),
            _ => (None, None, None),
        };
        out.push(SpanRow {
            end_time_ns: start.map(|s| s + r.below(3_000_000_000) as i64),
            start_time_ns: start.filter(|_| r.chance(50)),
            status: Some(STATUSES[r.below(3) as usize]),
            error: r.opt(20, "boom".into()),
            model: r.opt(40, "gpt-4o".into()),
            provider: r.opt(40, "openai".into()),
            prompt_tokens: pt,
            completion_tokens: ct,
            total_tokens: tt,
            cost_usd: r.opt(40, cost),
            events: r.opt(30, "[]".into()),
            outputs: match r.below(10) {
                0 => Some(r#"{"$forge_blob":"blobs/a/0f0f","size":2097152}"#.into()),
                1..4 => None,
                _ => Some(format!("{{\"text\":\"out {s}\"}}")),
            },
            ..base.clone()
        });
    }
    if r.chance(30) {
        out.push(match r.below(3) {
            0 => SpanRow {
                name: Some(format!("renamed-{s}")),
                ..base.clone()
            },
            1 => SpanRow {
                status: Some(STATUSES[r.below(3) as usize]),
                ..base.clone()
            },
            _ => SpanRow {
                run_type: Some(RUN_TYPES[r.below(4) as usize]),
                ..base.clone()
            },
        });
    }
    if decoy {
        out.push(SpanRow {
            parent_span_id: parent,
            ..base.clone()
        });
    }
    if r.chance(6) {
        out.push(SpanRow {
            deleted: true,
            ..base.clone()
        });
        if r.chance(50) {
            out.push(SpanRow {
                name: Some(format!("reposted-{s}")),
                ..base.clone()
            });
        }
    }
    out
}

/// Interleaves per-span post lists at random, keeping each list's order.
fn interleave(r: &mut Rng, mut lists: Vec<Vec<SpanRow>>) -> Vec<SpanRow> {
    lists.iter_mut().for_each(|l| l.reverse());
    let mut out = Vec::new();
    loop {
        lists.retain(|l| !l.is_empty());
        if lists.is_empty() {
            return out;
        }
        let i = r.below(lists.len() as u64) as usize;
        out.push(lists[i].pop().unwrap());
    }
}

/// The random part plus every forced edge of plan §6.3, as 5 flushes + memtable batches.
pub fn random_layout(seed: u64) -> (Layout, Traps) {
    let mut r = Rng(seed.wrapping_mul(0x9E37_79B9).max(1));
    let mut lists = Vec::new();
    for p in RANDOM_PROJECTS {
        for t in 0..4 {
            let n = 12 + r.below(5);
            let parents = tree_parents(&mut r, n);
            for (s, par) in parents.iter().enumerate() {
                let parent = par.id(10_000 + s as u64);
                lists.push(span_posts(&mut r, p, tid(t), s as u64, parent));
            }
        }
    }
    let seq = interleave(&mut r, lists);
    let cut = |f: f64| (seq.len() as f64 * f) as usize;
    let (c1, c2, c3) = (cut(0.3), cut(0.6), cut(0.85));
    let mut flushes = vec![
        seq[..c1].to_vec(),
        seq[c1..c2].to_vec(),
        seq[c2..c3].to_vec(),
        vec![],
        vec![],
    ];
    let mut tail = seq[c3..].to_vec();
    let mut traps = Traps::default();
    forced_edges(&mut flushes, &mut tail, &mut traps);
    let mut mem = Vec::new();
    while !tail.is_empty() {
        let k = (1 + r.below(8) as usize).min(tail.len());
        mem.push(tail.drain(..k).collect());
    }
    (
        Layout {
            flushes,
            mem,
            window: Some(2),
            window_first: seed.is_multiple_of(2),
        },
        traps,
    )
}

fn root(project: &str, t: u64, s: u64, start: Option<i64>, name: &str) -> SpanRow {
    SpanRow {
        name: Some(name.into()),
        run_type: Some(RunType::Chain),
        status: Some(SpanStatus::Unset),
        start_time_ns: start,
        ..post(project, tid(t), sid(s))
    }
}

/// Plan §6.3 "forced runs edges" (project a, traces 100+) and the project c / d merge-skip traps.
fn forced_edges(f: &mut [Vec<SpanRow>], mem: &mut Vec<SpanRow>, traps: &mut Traps) {
    let at = |k: i64| Some(BASE + k * STEP);
    let key = |t: u64, s: u64| (tid(t), sid(s));
    // Tie cluster: 7 roots, start TIE, traces 100 (3 roots), 101 (2), 102 (2); flush 1, flush 2 and the memtable.
    for (t, s, place) in [
        (100, 0, 0),
        (100, 1, 0),
        (100, 2, 9),
        (101, 0, 1),
        (101, 1, 1),
        (102, 0, 0),
        (102, 1, 9),
    ] {
        let row = SpanRow {
            status: Some(SpanStatus::Ok),
            ..root("a", t, s, Some(TIE), "tie")
        };
        if place == 9 {
            mem.push(row)
        } else {
            f[place].push(row)
        }
        traps.ties.push(key(t, s));
    }
    // A root whose start is null in every version.
    f[1].push(root("a", 103, 0, None, "nostart"));
    traps.null_start = key(103, 0);
    // R: unset -> error in the patches-only flush 4.
    f[0].push(root("a", 104, 0, at(41), "r-root"));
    f[3].push(SpanRow {
        status: Some(SpanStatus::Error),
        ..post("a", tid(104), sid(0))
    });
    traps.r = key(104, 0);
    // P: a root, reparented under another root of its trace in flush 4.
    f[1].push(root("a", 105, 0, at(42), "p-root"));
    f[1].push(root("a", 105, 2, at(43), "p-new-parent"));
    f[3].push(SpanRow {
        parent_span_id: Some(sid(2)),
        ..post("a", tid(105), sid(0))
    });
    traps.p = key(105, 0);
    // L: run_type chain -> llm in flush 4.
    f[1].push(root("a", 106, 0, at(44), "l-root"));
    f[3].push(SpanRow {
        run_type: Some(RunType::Llm),
        ..post("a", tid(106), sid(0))
    });
    traps.l = key(106, 0);
    // N: renamed in flush 4.
    f[0].push(root("a", 107, 0, at(46), N_OLD));
    f[3].push(SpanRow {
        name: Some(N_NEW.into()),
        ..post("a", tid(107), sid(0))
    });
    traps.n = key(107, 0);
    // R2 / P2: the trap rows sit in flush 3, whose max_start (H) is above their start rows' segment.
    f[0].push(SpanRow {
        status: Some(SpanStatus::Ok),
        ..root("a", 108, 0, at(45), "r2-root")
    });
    f[2].push(SpanRow {
        status: Some(SpanStatus::Error),
        ..post("a", tid(108), sid(0))
    });
    traps.r2 = key(108, 0);
    f[0].push(root("a", 109, 0, at(47), "p2-root"));
    f[0].push(root("a", 109, 1, at(48), "p2-new-parent"));
    f[2].push(SpanRow {
        parent_span_id: Some(sid(1)),
        ..post("a", tid(109), sid(0))
    });
    traps.p2 = key(109, 0);
    f[2].push(root("a", 110, 0, Some(TIE + 5 * STEP), "h-root"));
    // P3 / P4: the reparent sits in an older segment (flush 5) or in the memtable.
    f[1].push(root("a", 116, 0, at(53), "p3-root"));
    f[1].push(root("a", 116, 1, at(53), "p3-new-parent"));
    f[4].push(SpanRow {
        parent_span_id: Some(sid(1)),
        ..post("a", tid(116), sid(0))
    });
    traps.p3 = key(116, 0);
    f[0].push(root("a", 117, 0, at(54), "p4-root"));
    f[0].push(root("a", 117, 1, at(54), "p4-new-parent"));
    mem.push(SpanRow {
        parent_span_id: Some(sid(1)),
        ..post("a", tid(117), sid(0))
    });
    traps.p4 = key(117, 0);
    // A trace whose every span is tombstoned.
    f[0].push(root("a", 115, 0, at(52), "deleted"));
    f[1].push(SpanRow {
        deleted: true,
        ..post("a", tid(115), sid(0))
    });
    // Flush 5: one very old root plus patches; its max_start sorts last.
    f[4].push(root("a", 111, 0, Some(BASE - 1000 * STEP), "old-root"));
    f[4].push(SpanRow {
        name: Some("h-renamed".into()),
        ..post("a", tid(110), sid(0))
    });
    f[4].push(SpanRow {
        name: Some("tie-patched".into()),
        ..post("a", tid(100), sid(0))
    });
    // A trace only in the memtable; a trace whose start is in a segment and whose end is in the memtable.
    mem.push(root("a", 112, 0, at(49), "mem-only"));
    mem.push(SpanRow {
        parent_span_id: Some(sid(0)),
        ..root("a", 112, 1, at(49), "mem-child")
    });
    f[1].push(root("a", 113, 0, at(50), "start-in-seg"));
    mem.push(SpanRow {
        end_time_ns: Some(BASE + 50 * STEP + 7),
        status: Some(SpanStatus::Ok),
        ..post("a", tid(113), sid(0))
    });
    // Effective total_tokens across versions: 10 + 5, then p = 20 -> 25; total 100, then p = c = 1 -> 100.
    f[0].push(SpanRow {
        prompt_tokens: Some(10),
        completion_tokens: Some(5),
        ..root("a", 114, 0, at(51), "tok")
    });
    f[1].push(SpanRow {
        prompt_tokens: Some(20),
        ..post("a", tid(114), sid(0))
    });
    f[0].push(SpanRow {
        total_tokens: Some(100),
        parent_span_id: Some(sid(0)),
        ..root("a", 114, 1, at(51), "tok2")
    });
    mem.push(SpanRow {
        prompt_tokens: Some(1),
        completion_tokens: Some(1),
        ..post("a", tid(114), sid(1))
    });
    // Project c: one segment, no memtable rows (merge skip). Project d: one segment + a memtable patch (merges).
    for (p, place) in [("c", 0), ("d", 1)] {
        for s in 0..4 {
            let parent = (s > 0).then(|| sid(0));
            f[place].push(SpanRow {
                parent_span_id: parent,
                ..root(p, 200, s, at(10 + s as i64), "cd")
            });
            f[place].push(SpanRow {
                status: Some(SpanStatus::Ok),
                end_time_ns: at(20),
                ..post(p, tid(200), sid(s))
            });
        }
    }
    mem.push(SpanRow {
        status: Some(SpanStatus::Error),
        ..post("d", tid(200), sid(0))
    });
    // Project c: a span tombstoned inside its one segment (the merge skip must still drop it).
    f[0].push(root("c", 200, 4, at(14), "cd-deleted"));
    f[0].push(SpanRow {
        deleted: true,
        ..post("c", tid(200), sid(4))
    });
}

/// An agent-shaped trace (project `big`, trace 999): parent among the previous 8 spans, depth <= 10, start / end
/// posts and patches spread over `flushes` flushes and the memtable (many small batches). Siblings share a µs
/// with ns in the reverse of span_id order; ~5 % of spans have no start. `payload_bytes` per llm-ish span.
pub fn big_trace(spans: u64, flushes: usize, mem_batch: usize, payload_bytes: usize, seed: u64) -> Layout {
    let mut r = Rng(seed);
    let trace = tid(999);
    let mut depth = vec![0u32; spans as usize];
    let mut parent: Vec<Option<u64>> = vec![None; spans as usize];
    for i in 1..spans {
        let mut p = i - 1 - r.below(i.min(8));
        if depth[p as usize] >= 9 {
            p = 0;
        }
        parent[i as usize] = Some(p);
        depth[i as usize] = depth[p as usize] + 1;
    }
    // Groups of 4 consecutive spans share a µs; within a group, ns rank = reverse span_id order.
    let mut start: Vec<Option<i64>> = vec![None; spans as usize];
    for g in (0..spans).step_by(4) {
        let mut ids: Vec<u64> = (g..(g + 4).min(spans)).collect();
        ids.sort_by_key(|&i| std::cmp::Reverse(sid(i)));
        for (rank, &i) in ids.iter().enumerate() {
            start[i as usize] = (i == 0 || !r.chance(5)).then_some(BASE + g as i64 * 1_000_000 + rank as i64);
        }
    }
    let pad = "x".repeat(payload_bytes / 2);
    let places = flushes + 1;
    let mut layout = Layout {
        flushes: vec![vec![]; flushes],
        ..Default::default()
    };
    let mut mem_rows = Vec::new();
    for i in 0..spans {
        let base = post("big", trace, sid(i));
        let llm = i % 3 == 1;
        let mut posts = vec![SpanRow {
            name: Some(format!("step-{i}")),
            run_type: Some(if llm { RunType::Llm } else { RunType::Chain }),
            start_time_ns: start[i as usize],
            parent_span_id: parent[i as usize].map(sid),
            session_id: Some("thread-17".into()),
            tags: Some(vec!["prod".into()]),
            metadata: Some(format!("{{\"user\":\"u{}\",\"i\":{i}}}", i % 7)),
            attributes: Some(format!("{{\"http.status_code\":200,\"i\":{i}}}")),
            inputs: llm.then(|| format!("{{\"messages\":[{{\"role\":\"user\",\"content\":\"{pad}\"}}]}}")),
            ..base.clone()
        }];
        posts.push(SpanRow {
            end_time_ns: start[i as usize].map(|s| s + 1_000_000 + i as i64),
            status: Some(SpanStatus::Ok),
            model: llm.then(|| "gpt-4o".into()),
            provider: llm.then(|| "openai".into()),
            prompt_tokens: llm.then_some(100 + i as i64),
            completion_tokens: llm.then_some(20),
            cost_usd: llm.then_some(0.25),
            events: Some("[]".into()),
            outputs: llm.then(|| format!("{{\"text\":\"{pad}\"}}")),
            ..base.clone()
        });
        if r.chance(10) {
            posts.push(SpanRow {
                name: Some(format!("step-{i}-patched")),
                ..base.clone()
            });
        }
        let mut place = 0;
        for p in posts {
            place = (place + r.below(2) as usize).min(places - 1);
            if place == flushes {
                mem_rows.push(p)
            } else {
                layout.flushes[place].push(p)
            }
        }
    }
    layout.mem = mem_rows.chunks(mem_batch.max(1)).map(|c| c.to_vec()).collect();
    layout
}

/// One batch of the query's rows (with the query's schema when there are none).
pub async fn sql(e: &QueryEngine, q: &str) -> RecordBatch {
    let b = e.sql_batches(q).await.unwrap_or_else(|err| panic!("{q}: {err}"));
    let schema = match b.first() {
        Some(b) => b.schema(),
        None => Arc::new(e.session().unwrap().sql(q).await.unwrap().schema().as_arrow().clone()),
    };
    arrow::compute::concat_batches(&schema, &b).unwrap()
}

// ---------------------------------------------------------------- reference helpers

/// Reference rows as a batch with exactly `b`'s columns, in key order.
pub fn ref_batch(rows: &[SpanRow], cols: &[&str]) -> RecordBatch {
    project_to(&sort_by_key(&batch_from_rows(rows)), &projected_schema(cols)).unwrap()
}

/// (trace, span) of every row, by column name.
pub fn keys_of(b: &RecordBatch) -> Vec<(TraceId, SpanId)> {
    forge_model::row::span_keys(b).unwrap()
}

pub fn names_of(b: &RecordBatch) -> Vec<&str> {
    b.schema_ref().fields().iter().map(|f| f.name().as_str()).collect()
}

pub fn trace_cols(payloads: bool) -> Vec<&'static str> {
    ALL_COLUMNS
        .iter()
        .copied()
        .filter(|c| *c != col::DOTTED_ORDER && (payloads || !PAYLOAD_COLUMNS.contains(c)))
        .collect()
}

pub fn rows_of<'a>(reference: &'a [SpanRow], project: &str, trace: TraceId) -> Vec<&'a SpanRow> {
    reference
        .iter()
        .filter(|r| r.project_id.as_ref().unwrap().as_str() == project && r.trace_id == trace)
        .collect()
}

fn ts(ns: Option<i64>) -> Value {
    ns.map_or(Value::Null, |n| {
        json!(chrono::DateTime::from_timestamp_nanos(n).to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
    })
}

fn json_col(s: &Option<String>) -> Value {
    match s {
        None => Value::Null,
        Some(s) => serde_json::from_str(s).unwrap_or_else(|_| json!(s)),
    }
}

/// The expected fast-path JSON of one span (plan D5 / FP §1.5), without children.
pub fn expected_span(r: &SpanRow, payloads: bool, list: bool) -> serde_json::Map<String, Value> {
    let mut m = serde_json::Map::new();
    let s = |v: &Option<String>| v.as_ref().map_or(Value::Null, |x| json!(x));
    if list {
        m.insert("trace_id".into(), json!(r.trace_id.to_string()));
    }
    m.insert("span_id".into(), json!(r.span_id.to_string()));
    m.insert(
        "parent_span_id".into(),
        r.parent_span_id.map_or(Value::Null, |p| json!(p.to_string())),
    );
    m.insert("name".into(), s(&r.name));
    m.insert("run_type".into(), r.run_type.map_or(Value::Null, |x| json!(x.as_str())));
    m.insert("status".into(), r.status.map_or(Value::Null, |x| json!(x.as_str())));
    m.insert("error".into(), s(&r.error));
    m.insert("start_time".into(), ts(r.start_time_ns));
    m.insert("end_time".into(), ts(r.end_time_ns));
    let latency = match (r.start_time_ns, r.end_time_ns) {
        (Some(a), Some(b)) => json!((b - a) as f64 / 1e6),
        _ => Value::Null,
    };
    m.insert("latency_ms".into(), latency);
    m.insert("model".into(), s(&r.model));
    m.insert("provider".into(), s(&r.provider));
    m.insert("prompt_tokens".into(), json!(r.prompt_tokens));
    m.insert("completion_tokens".into(), json!(r.completion_tokens));
    m.insert("total_tokens".into(), json!(r.total_tokens));
    m.insert("cost_usd".into(), json!(r.cost_usd));
    m.insert("session_id".into(), s(&r.session_id));
    m.insert("tags".into(), json!(r.tags));
    m.insert("metadata".into(), json_col(&r.metadata));
    if payloads {
        for (c, v) in [
            (col::ATTRIBUTES, &r.attributes),
            (col::EVENTS, &r.events),
            (col::INPUTS, &r.inputs),
            (col::OUTPUTS, &r.outputs),
        ] {
            m.insert(c.into(), json_col(v));
        }
    }
    m.insert("version".into(), json!(r.version));
    debug_assert!(JSON_COLUMNS.contains(&col::METADATA));
    m
}

/// The expected `trace_json` body, from reference rows and the `ref_tree` oracle.
pub fn expected_trace_json(project: &str, rows: &[&SpanRow], tree: &[RefNode], payloads: bool, flat: bool) -> Value {
    let by_id: BTreeMap<SpanId, &SpanRow> = rows.iter().map(|r| (r.span_id, *r)).collect();
    let node = |n: &RefNode| {
        let mut m = expected_span(by_id[&n.span], payloads, false);
        if n.orphan {
            m.insert("orphan".into(), json!(true));
        }
        if n.cycle {
            m.insert("cycle".into(), json!(true));
        }
        m
    };
    let trace = rows[0].trace_id.to_string();
    if flat {
        let spans: Vec<Value> = tree
            .iter()
            .map(|n| {
                let mut m = node(n);
                m.insert("depth".into(), json!(n.depth));
                Value::Object(m)
            })
            .collect();
        return json!({"project": project, "trace_id": trace, "span_count": rows.len(), "spans": spans});
    }
    // Rebuild the nesting from (preorder, depth) with an explicit stack.
    let mut stack: Vec<(serde_json::Map<String, Value>, Vec<Value>)> = Vec::new();
    let mut roots: Vec<Value> = Vec::new();
    let close = |stack: &mut Vec<(serde_json::Map<String, Value>, Vec<Value>)>, roots: &mut Vec<Value>| {
        let (mut m, ch) = stack.pop().unwrap();
        m.insert("children".into(), Value::Array(ch));
        match stack.last_mut() {
            Some(p) => p.1.push(Value::Object(m)),
            None => roots.push(Value::Object(m)),
        }
    };
    for n in tree {
        while stack.len() > n.depth as usize {
            close(&mut stack, &mut roots);
        }
        stack.push((node(n), Vec::new()));
    }
    while !stack.is_empty() {
        close(&mut stack, &mut roots);
    }
    json!({"project": project, "trace_id": trace, "span_count": rows.len(), "roots": roots})
}

/// FP §2.1 semantics in plain Rust: every listed run of `req`, in page order (ignores `limit`).
pub fn ref_runs(reference: &[SpanRow], req: &RunsRequest) -> Vec<(i64, TraceId, SpanId)> {
    let mut out: Vec<(i64, TraceId, SpanId)> = reference
        .iter()
        .filter(|r| r.project_id.as_ref() == Some(&req.project) && r.parent_span_id.is_none())
        .filter_map(|r| Some((r, r.start_time_ns?)))
        .filter(|(_, s)| req.start_ns.is_none_or(|x| *s >= x) && req.end_ns.is_none_or(|x| *s < x))
        .filter(|(r, _)| req.run_types.is_empty() || r.run_type.is_some_and(|x| req.run_types.contains(&x)))
        .filter(|(r, _)| req.statuses.is_empty() || r.status.is_some_and(|x| req.statuses.contains(&x)))
        .filter(|(r, _)| {
            req.q.as_ref().is_none_or(|q| {
                let mut an = forge_index::Analyzer::default();
                let p = forge_index::FtsPredicate::Search {
                    column: forge_index::FtsColumn::Name,
                    query: q.clone(),
                };
                let m = forge_index::Matcher::new(&mut an, &p).unwrap();
                r.name.as_ref().is_some_and(|n| m.matches(&mut an, n))
            })
        })
        .map(|(r, s)| (s, r.trace_id, r.span_id))
        .filter(|p| req.cursor.as_ref().is_none_or(|Cursor(c)| p < c))
        .collect();
    out.sort_by(|a, b| b.cmp(a));
    out
}

/// The default listing of `project` (limit 50).
pub fn runs_request(project: &str) -> RunsRequest {
    RunsRequest::parse(RunsParams::default(), pid(project)).unwrap()
}

pub fn treq(project: &str, trace: TraceId, payloads: bool, format: TraceFormat) -> TraceRequest {
    TraceRequest {
        project: pid(project),
        trace,
        payloads,
        format,
    }
}

pub fn hex_lit(b: &[u8]) -> String {
    format!("X'{}'", hex::encode(b))
}

/// Project `t`: 3 flushes, trace k with 2 roots starting at `BASE + k * 100 s + 0.5 s` (one segment each).
pub fn time_layout() -> Layout {
    let flushes = (0..3)
        .map(|k| {
            (0..2)
                .map(|s| SpanRow {
                    name: Some(format!("t{k}")),
                    status: Some(SpanStatus::Ok),
                    start_time_ns: Some(BASE + k as i64 * 100_000_000_000 + 500_000_000),
                    ..post("t", tid(k), sid(s))
                })
                .collect()
        })
        .collect();
    Layout {
        flushes,
        ..Default::default()
    }
}
