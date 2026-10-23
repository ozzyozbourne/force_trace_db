//! Exit 1 (plans/phase-4 §6.1): randomized histories over the real metastore, segment store, picker and job.
//! After every job the merged read equals `testing::reference_merge`, and the structural checks hold: start
//! completeness, the orphan split, the placement oracle, the per-job dotted_order oracle, sticky placement
//! (invariant 10), blob_refs and trace_index.
//!
//! Default: 8 seeds in 4 shards. `FORGE_PROP_SEEDS=N` (400 in quick-release), `FORGE_PROP_SEED=s` (one seed).
use arrow::record_batch::RecordBatch;
use forge_ingest::blob::BlobGuard;
use forge_ingest::compact::{CompactCtx, JobReport, job, pick};
use forge_ingest::lifecycle::{GcOptions, run_gc};
use forge_ingest::testing::write_flush;
use forge_meta::{CompactionJob, JobRequest, Metastore, SegmentFilter, SqliteMetastore};
use forge_model::blob::{blob_ref, blob_refs_in};
use forge_model::catalog::{HourBucket, JobKind, Level, SegmentId, SegmentMeta, SegmentState};
use forge_model::config::CompactionConfig;
use forge_model::dotted::{SEGMENT_LEN, depth};
use forge_model::ids::{SpanId, TraceId};
use forge_model::merge::{Tombstones, merge_versions};
use forge_model::row::SpanRow;
use forge_model::testing::{
    Parent, Rng, RowKey, batch_from_rows, random_row, ref_dotted, reference_merge, row_key, rows_from_batch,
    tree_parents,
};
use forge_model::time::Clocks;
use forge_storage::testing::{HookedSegmentStore, memory_store};
use forge_storage::{ScanRequest, SegmentSpec, SegmentStore};
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const H: i64 = HourBucket::NANOS;
const BASE_H: i64 = 1_700_000_000 / 3600;
const BASE: i64 = BASE_H * H;
const PROJECTS: [&str; 2] = ["p", "q"];
const TRACES: u64 = 3;
const MISSING: u64 = 500;
const MAX_POSTS: u64 = 120;

// ---------------------------------------------------------------------------------------------------------------
// A segment store whose writes into some hours are slowed (varies the commit order of concurrent jobs, step e).

/// (hour, slow inside the hour?): writes with (partition_hour == hour) == slow_inside sleep.
type Delay = Arc<Mutex<Option<(HourBucket, bool)>>>;

fn delay_store(inner: Arc<dyn SegmentStore>, delay: Delay) -> Arc<dyn SegmentStore> {
    Arc::new(HookedSegmentStore::new(
        inner,
        Box::new(move |spec| {
            let slow = matches!(*delay.lock().unwrap(), Some((h, inside)) if (spec.partition_hour == h) == inside);
            Box::pin(async move {
                if slow {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                }
                Ok(())
            })
        }),
    ))
}

// ---------------------------------------------------------------------------------------------------------------
// Coverage (printed always, asserted when >= 8 seeds ran in total).

const COV: [&str; 21] = [
    "picker_suffix",
    "jobs_a",
    "jobs_b",
    "folds",
    "b_multi_group",
    "b_suffix",
    "fills",
    "backfills",
    "backfills_depth2",
    "orphan_outputs",
    "multi_slice",
    "rotations",
    "writer_cap",
    "cut_prefix",
    "inv_non_prefix",
    "inv_partial_fold",
    "inv_non_suffix",
    "inv_second_a",
    "interleavings",
    "b_committed_first",
    "gc_passes",
];
static TOTALS: [AtomicU64; 21] = [const { AtomicU64::new(0) }; 21];
static SEEDS_RUN: AtomicU64 = AtomicU64::new(0);
static SHARDS_DONE: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct Cov([u64; 21]);
impl Cov {
    fn bump(&mut self, name: &str, n: u64) {
        self.0[COV.iter().position(|c| *c == name).expect("counter")] += n;
    }
}

// ---------------------------------------------------------------------------------------------------------------
// The generator.

struct KeyInfo {
    parent: Option<SpanId>,
    alt_parent: Option<SpanId>,
    flagged: bool,
    reparented: bool,
    home_start: i64,
    other_start: i64,
}

fn keys(r: &mut Rng) -> BTreeMap<(usize, u64, u64), KeyInfo> {
    let mut out = BTreeMap::new();
    for p in 0..PROJECTS.len() {
        for t in 0..TRACES {
            let n = 9 + r.below(4);
            for (s, parent) in tree_parents(r, n).into_iter().enumerate() {
                let home = r.below(3) as i64;
                let home_start = BASE + home * H + r.below(1_000_000_000) as i64;
                out.insert(
                    (p, t, s as u64),
                    KeyInfo {
                        parent: parent.id(MISSING),
                        alt_parent: Parent::Span(r.below(n)).id(MISSING),
                        flagged: r.chance(10),
                        reparented: false,
                        home_start,
                        other_start: BASE + ((home + 1 + r.below(2) as i64) % 3) * H + 5,
                    },
                );
            }
        }
    }
    out
}

fn post(r: &mut Rng, keys: &mut BTreeMap<(usize, u64, u64), KeyInfo>, v: u64) -> SpanRow {
    let k = *keys.keys().nth(r.below(keys.len() as u64) as usize).unwrap();
    let info = keys.get_mut(&k).unwrap();
    let mut x = random_row(r, PROJECTS[k.0], k.1, k.2, v);
    if info.flagged && !info.reparented && info.parent.is_some() && r.chance(25) {
        info.reparented = true;
    }
    let parent = if info.reparented { info.alt_parent } else { info.parent };
    x.parent_span_id = parent.filter(|_| r.chance(60));
    x.start_time_ns = match r.below(100) {
        0..45 => Some(info.home_start),
        45..50 if info.flagged => Some(info.other_start),
        45..50 => Some(info.home_start),
        _ => None,
    };
    x.end_time_ns = r.opt(60, info.home_start + 2_000);
    let tag = r.below(1000);
    x.dotted_order = r.opt(10, format!("d{tag:03}"));
    if r.chance(10) {
        x.outputs = Some(blob_ref(&format!("blobs/{}/b{}", PROJECTS[k.0], r.below(8)), 10));
    }
    x
}

// ---------------------------------------------------------------------------------------------------------------
// The world: metastore + store + posts + model of backfills.

struct World {
    seed: u64,
    _dir: tempfile::TempDir,
    meta: Arc<dyn Metastore>,
    raw: rusqlite::Connection,
    delay: Delay,
    store: Arc<dyn SegmentStore>,
    posts: Vec<SpanRow>,
    mem: Vec<SpanRow>,
    seq: u64,
    flushes: i64,
    /// key -> (version, dotted) of every backfill seen in a job output.
    backfills: HashMap<RowKey, Vec<(u64, String)>>,
    cov: Cov,
}

async fn scan(store: &dyn SegmentStore, s: &SegmentMeta) -> Vec<RecordBatch> {
    store
        .scan(s, ScanRequest::default())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

fn key_of(k: &RowKey) -> String {
    format!("{}/{}/{}", k.0, k.1, k.2)
}

/// `pick::groups` as segment ids.
fn groups(segs: &[&SegmentMeta]) -> Vec<Vec<SegmentId>> {
    pick::groups(segs)
        .into_iter()
        .map(|g| g.iter().map(|s| s.id).collect())
        .collect()
}

/// State read before a job begins (placement and dotted oracles).
struct Before {
    visible: HashSet<SegmentId>,
    sticky: HashMap<(String, TraceId, SpanId), HourBucket>,
    trace_hour: HashMap<(String, TraceId), HourBucket>,
}

impl World {
    async fn new(seed: u64) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let meta: Arc<dyn Metastore> = Arc::new(SqliteMetastore::open_path(&path).unwrap());
        let raw = rusqlite::Connection::open(&path).unwrap();
        let delay = Delay::default();
        Self {
            seed,
            _dir: dir,
            meta,
            raw,
            store: delay_store(memory_store(), delay.clone()),
            delay,
            posts: Vec::new(),
            mem: Vec::new(),
            seq: 0,
            flushes: 0,
            backfills: HashMap::new(),
            cov: Cov::default(),
        }
    }

    fn ctx(&self, cfg: CompactionConfig) -> CompactCtx {
        CompactCtx::new(self.meta.clone(), self.store.clone(), cfg, Clocks::wall())
    }

    /// One L0 per project of the unflushed rows, one commit (like the real flush). Blobs of the flushed rows are
    /// (re)put, as ingest does before the WAL append.
    async fn flush(&mut self) {
        if self.mem.is_empty() {
            return;
        }
        self.seq += 1;
        let hour = HourBucket(BASE_H + self.flushes % 4);
        self.flushes += 1;
        let merged = merge_versions(&[batch_from_rows(&self.mem)], Tombstones::Keep).unwrap();
        for b in blob_refs_in(std::slice::from_ref(&merged)) {
            let opts = PutOptions::from(PutMode::Create);
            let _ = self
                .store
                .object_store()
                .put_opts(&Path::from(b.as_str()), b"blob".to_vec().into(), opts)
                .await;
        }
        let max_version = self.posts.iter().map(|r| r.version).max().unwrap_or(0);
        write_flush(
            self.meta.as_ref(),
            self.store.clone(),
            &merged,
            hour,
            "n",
            1 << 30,
            self.seq,
            max_version,
        )
        .await;
        self.mem.clear();
    }

    async fn visible(&self, p: Option<&str>) -> Vec<SegmentMeta> {
        self.meta
            .list_segments(&SegmentFilter {
                project_id: p.map(forge_model::testing::pid),
                ..Default::default()
            })
            .await
            .unwrap()
    }

    async fn before(&self) -> Before {
        let mut b = Before {
            visible: HashSet::new(),
            sticky: HashMap::new(),
            trace_hour: HashMap::new(),
        };
        for s in self.visible(None).await {
            b.visible.insert(s.id);
            if s.level != Level::L1 {
                continue;
            }
            for batch in scan(self.store.as_ref(), &s).await {
                for r in rows_from_batch(&batch).unwrap() {
                    let k = row_key(&r);
                    b.sticky.insert(k.clone(), s.partition_hour);
                    let e = b.trace_hour.entry((k.0, k.1)).or_insert(s.partition_hour);
                    *e = (*e).min(s.partition_hour);
                }
            }
        }
        b
    }

    /// The dotted_order the job can know (§6.1 check 2): ref_dotted over reference_merge of the rows of the job's
    /// inputs and every other visible L1 segment of the project, read before the job runs.
    async fn dotted_oracle(&self, job: &CompactionJob) -> BTreeMap<RowKey, Option<String>> {
        let ids: HashSet<SegmentId> = job.inputs.iter().map(|s| s.id).collect();
        let mut rows = Vec::new();
        for s in self.visible(Some(job.project_id.as_str())).await {
            if ids.contains(&s.id) || s.level == Level::L1 {
                for b in scan(self.store.as_ref(), &s).await {
                    rows.extend(rows_from_batch(&b).unwrap());
                }
            }
        }
        ref_dotted(&reference_merge(&rows))
    }

    /// Latest non-null dotted_order of `k` over posts and recorded backfills with version <= `v`.
    fn expected_dotted(&self, k: &RowKey, v: u64) -> Option<String> {
        let posts = self
            .posts
            .iter()
            .filter(|r| row_key(r) == *k && r.version <= v)
            .filter_map(|r| Some((r.version, r.dotted_order.clone()?)));
        let fills = self
            .backfills
            .get(k)
            .into_iter()
            .flatten()
            .filter(|(bv, _)| *bv <= v)
            .cloned();
        posts.chain(fills).max_by_key(|x| x.0).map(|x| x.1)
    }

    /// Checks the outputs of the job(s) that ran since `before`; records backfills. `oracle`: the exact dotted
    /// oracle (sequential steps); None = only well-formedness (step e). `fallback`: the newest input L0's hour.
    async fn check_outputs(
        &mut self,
        before: &Before,
        oracle: Option<&BTreeMap<RowKey, Option<String>>>,
        fallback: Option<HourBucket>,
    ) {
        let seed = self.seed;
        for s in self.visible(None).await {
            if before.visible.contains(&s.id) || s.level != Level::L1 {
                continue;
            }
            let orphan = s.start_time_nulls == s.row_count;
            for b in scan(self.store.as_ref(), &s).await {
                for r in rows_from_batch(&b).unwrap() {
                    let k = row_key(&r);
                    let ks = key_of(&k);
                    // Placement oracle (D3).
                    let want = before
                        .sticky
                        .get(&k)
                        .copied()
                        .or_else(|| r.start_time_ns.or(r.end_time_ns).map(HourBucket::from_nanos))
                        .or_else(|| before.trace_hour.get(&(k.0.clone(), k.1)).copied())
                        .or(fallback)
                        .unwrap_or_else(|| panic!("seed {seed}: no placement for {ks}"));
                    assert_eq!(s.partition_hour, want, "seed {seed}: placement of {ks}");
                    assert_eq!(orphan, r.start_time_ns.is_none(), "seed {seed}: orphan split of {ks}");
                    // dotted_order: existing values win; a new value is a backfill.
                    match (&r.dotted_order, self.expected_dotted(&k, r.version)) {
                        (Some(d), Some(e)) => assert_eq!(d, &e, "seed {seed}: existing dotted of {ks}"),
                        (Some(d), None) => {
                            self.cov.bump("backfills", 1);
                            self.cov.bump("backfills_depth2", u64::from(depth(d) >= 2));
                            self.backfills
                                .entry(k.clone())
                                .or_default()
                                .push((r.version, d.clone()));
                            if oracle.is_none() {
                                // Well-formed: ends in the span's own segment (any prefix: a producer value).
                                let own = d.rsplit('.').next().unwrap_or_default();
                                assert!(
                                    own.len() == SEGMENT_LEN && own.ends_with(&k.2.to_string()),
                                    "seed {seed}: malformed backfill {d} of {ks}"
                                );
                            }
                        }
                        (None, Some(e)) => panic!("seed {seed}: {ks} lost its dotted {e}"),
                        (None, None) => {}
                    }
                    if let Some(o) = oracle {
                        assert_eq!(&r.dotted_order, &o[&k], "seed {seed}: dotted oracle of {ks}");
                    }
                }
            }
        }
    }

    /// Global checks after every step (§6.1 checks 1, 3, 4, 6, 7, 8, 9).
    async fn check(&self) {
        let seed = self.seed;
        let segs = self.visible(None).await;
        assert!(
            segs.iter().all(|s| s.state == SegmentState::Live),
            "seed {seed}: a segment is left compacting"
        );
        let running: i64 = self
            .raw
            .query_row(
                "SELECT count(*) FROM compaction_jobs WHERE status = 'running'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(running, 0, "seed {seed}: a job is left running");
        let mut batches = Vec::new();
        let mut hour_of: HashMap<RowKey, HourBucket> = HashMap::new();
        // trace -> segment -> row positions
        let mut trace_rows: BTreeMap<(String, TraceId), BTreeMap<SegmentId, BTreeSet<u64>>> = BTreeMap::new();
        for s in &segs {
            let seg_batches = scan(self.store.as_ref(), s).await;
            // blob_refs == blob_refs_in(scan(segment)).
            let mut st = self
                .raw
                .prepare("SELECT path FROM blob_refs WHERE segment_id = ?1 ORDER BY path")
                .unwrap();
            let refs: Vec<String> = st
                .query_map([s.id.to_string()], |r| r.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert_eq!(refs, blob_refs_in(&seg_batches), "seed {seed}: blob_refs of {}", s.id);
            let mut row = 0u64;
            for b in &seg_batches {
                for r in rows_from_batch(b).unwrap() {
                    let k = row_key(&r);
                    trace_rows
                        .entry((k.0.clone(), k.1))
                        .or_default()
                        .entry(s.id)
                        .or_default()
                        .insert(row);
                    row += 1;
                    if s.level != Level::L1 {
                        continue;
                    }
                    // Sticky (invariant 10).
                    let h = *hour_of.entry(k.clone()).or_insert(s.partition_hour);
                    assert_eq!(h, s.partition_hour, "seed {seed}: sticky broken for {}", key_of(&k));
                    // Start completeness.
                    let upto: Vec<SpanRow> = self
                        .posts
                        .iter()
                        .filter(|x| row_key(x) == k && x.version <= r.version)
                        .cloned()
                        .collect();
                    assert_eq!(
                        r.start_time_ns,
                        reference_merge(&upto)[0].start_time_ns,
                        "seed {seed}: start completeness of {}",
                        key_of(&k)
                    );
                }
            }
            // Orphan split.
            if s.level == Level::L1 {
                assert!(
                    s.start_time_nulls == 0 || s.start_time_nulls == s.row_count,
                    "seed {seed}: mixed L1 segment {s:?}"
                );
            }
            batches.extend(seg_batches);
        }
        // trace_index: locate_traces covers exactly each trace's visible rows.
        for p in PROJECTS {
            let traces: Vec<TraceId> = trace_rows.keys().filter(|k| k.0 == p).map(|k| k.1).collect();
            let located = self
                .meta
                .locate_traces(&forge_model::testing::pid(p), &traces)
                .await
                .unwrap();
            for t in traces {
                let mut got: BTreeMap<SegmentId, BTreeSet<u64>> = BTreeMap::new();
                for l in located.get(&t).into_iter().flatten() {
                    got.entry(l.segment.id).or_default().extend(l.rows());
                }
                assert_eq!(
                    got,
                    trace_rows[&(p.to_string(), t)],
                    "seed {seed}: trace_index of {p}/{t}"
                );
            }
        }
        // Reads.
        if !self.mem.is_empty() {
            batches.push(batch_from_rows(&self.mem));
        }
        let strip = |v: Vec<SpanRow>| -> Vec<SpanRow> {
            let mut v: Vec<SpanRow> = v
                .into_iter()
                .map(|r| SpanRow {
                    dotted_order: None,
                    ..r
                })
                .collect();
            v.sort_by_key(row_key);
            v
        };
        let read = rows_from_batch(&merge_versions(&batches, Tombstones::Keep).unwrap()).unwrap();
        let reference = reference_merge(&self.posts);
        assert_eq!(
            strip(read.clone()),
            strip(reference.clone()),
            "seed {seed}: read != reference"
        );
        let dropped = rows_from_batch(&merge_versions(&batches, Tombstones::Drop).unwrap()).unwrap();
        let want: Vec<SpanRow> = reference.iter().filter(|r| !r.deleted).cloned().collect();
        assert_eq!(strip(dropped), strip(want), "seed {seed}: read (Drop) != reference");
        for r in &read {
            let k = row_key(r);
            assert_eq!(
                r.dotted_order,
                self.expected_dotted(&k, u64::MAX),
                "seed {seed}: read dotted of {}",
                key_of(&k)
            );
        }
    }

    fn record(&mut self, kind: JobKind, rep: &JobReport) {
        self.cov.bump(if kind == JobKind::L0 { "jobs_a" } else { "jobs_b" }, 1);
        let st = rep.stats;
        self.cov.bump("folds", u64::from(st.folded_hours));
        self.cov.bump("fills", st.fills);
        self.cov.bump("orphan_outputs", u64::from(st.orphan_outputs));
        self.cov.bump("multi_slice", u64::from(st.slices > 1));
        self.cov.bump("rotations", u64::from(st.rotated));
        self.cov.bump("writer_cap", u64::from(st.writer_cap));
    }

    /// Coverage of a request before it runs (kind-B groups/suffix, cut prefixes).
    async fn request_cov(&mut self, req: &JobRequest) {
        let segs = self.visible(Some(req.project_id.as_str())).await;
        let ids: HashSet<SegmentId> = req.inputs.iter().copied().collect();
        match req.kind {
            JobKind::L0 => {
                let l0 = segs.iter().filter(|s| s.level == Level::L0).count();
                let taken = segs
                    .iter()
                    .filter(|s| s.level == Level::L0 && ids.contains(&s.id))
                    .count();
                self.cov.bump("cut_prefix", u64::from(taken < l0));
            }
            JobKind::L1 => {
                let hour = segs.iter().find(|s| ids.contains(&s.id)).unwrap().partition_hour;
                let in_hour: Vec<&SegmentMeta> = segs
                    .iter()
                    .filter(|s| s.level == Level::L1 && s.partition_hour == hour)
                    .collect();
                let g = groups(&in_hour);
                let taken = g.iter().filter(|g| g.iter().any(|id| ids.contains(id))).count();
                self.cov.bump("b_multi_group", u64::from(taken > 1));
                self.cov.bump("b_suffix", u64::from(taken < g.len()));
            }
        }
    }

    /// begin + run_job + the per-job checks.
    async fn run(&mut self, req: JobRequest, cfg: CompactionConfig) {
        let seed = self.seed;
        self.request_cov(&req).await;
        let before = self.before().await;
        let kind = req.kind;
        let job = self
            .meta
            .begin_compaction(req.clone())
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("seed {seed}: valid request refused: {req:?}"));
        let oracle = self.dotted_oracle(&job).await;
        let fallback = newest_l0_hour(&job);
        let rep = job::run_job(&self.ctx(cfg), job)
            .await
            .unwrap_or_else(|e| panic!("seed {seed}: job failed: {e}"));
        self.record(kind, &rep);
        self.check_outputs(&before, Some(&oracle), fallback).await;
        self.check().await;
    }

    async fn pick_and_run(&mut self, r: &mut Rng, force: bool) {
        let p = PROJECTS[r.below(2) as usize];
        let segs = self.visible(Some(p)).await;
        let trigger = 1 + r.below(6) as usize;
        let cfg = random_cfg(r, trigger);
        let input = pick::PickInput {
            segs: &segs,
            wall_now_ms: forge_model::time::now_ms(),
            data_now_ns: forge_model::time::now_ms() * 1_000_000,
            expired_before_ns: None,
            force,
        };
        if let Some((kind, inputs)) = pick::pick(&input, &cfg) {
            let req = JobRequest {
                project_id: forge_model::testing::pid(p),
                kind,
                inputs,
            };
            if kind == JobKind::L1 {
                let in_hour: Vec<&SegmentMeta> = segs.iter().filter(|s| s.level == Level::L1).collect();
                let hour = segs.iter().find(|s| s.id == req.inputs[0]).unwrap().partition_hour;
                let in_hour: Vec<&SegmentMeta> = in_hour.into_iter().filter(|s| s.partition_hour == hour).collect();
                self.cov
                    .bump("picker_suffix", u64::from(req.inputs.len() < in_hour.len()));
            }
            self.run(req, cfg).await;
        }
    }

    /// Step (c): each invalid request is refused and changes nothing; the corrected one is accepted (then failed).
    async fn invalid(&mut self, r: &mut Rng) {
        let seed = self.seed;
        let p = PROJECTS[r.below(2) as usize];
        let segs = self.visible(Some(p)).await;
        let mut l0: Vec<&SegmentMeta> = segs.iter().filter(|s| s.level == Level::L0).collect();
        l0.sort_by_key(|s| s.min_version);
        let mut hours: BTreeMap<HourBucket, Vec<&SegmentMeta>> = BTreeMap::new();
        for s in segs.iter().filter(|s| s.level == Level::L1) {
            hours.entry(s.partition_hour).or_default().push(s);
        }
        let req = |kind, inputs: Vec<SegmentId>| JobRequest {
            project_id: forge_model::testing::pid(p),
            kind,
            inputs,
        };
        let mut cases: Vec<(&str, JobRequest, JobRequest)> = Vec::new();
        if l0.len() >= 2 {
            cases.push((
                "inv_non_prefix",
                req(JobKind::L0, vec![l0[1].id]),
                req(JobKind::L0, vec![l0[0].id, l0[1].id]),
            ));
        }
        if let (Some(first), Some(runs)) = (l0.first(), hours.values().find(|v| v.len() >= 2)) {
            let mut all = vec![first.id];
            all.extend(runs.iter().map(|s| s.id));
            cases.push((
                "inv_partial_fold",
                req(JobKind::L0, vec![first.id, runs[0].id]),
                req(JobKind::L0, all),
            ));
        }
        if let Some(g) = hours.values().map(|v| groups(v)).find(|g| g.len() >= 2) {
            cases.push((
                "inv_non_suffix",
                req(JobKind::L1, g[0].clone()),
                req(JobKind::L1, g.concat()),
            ));
        }
        for (name, bad, good) in cases {
            let snapshot = self.visible(None).await;
            assert!(
                self.meta.begin_compaction(bad.clone()).await.unwrap().is_none(),
                "seed {seed}: {name} accepted: {bad:?}"
            );
            assert_eq!(self.visible(None).await, snapshot, "seed {seed}: {name} changed state");
            let j = self
                .meta
                .begin_compaction(good.clone())
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("seed {seed}: corrected {name} refused: {good:?}"));
            self.meta.fail_compaction(j.id, "test").await.unwrap();
            self.cov.bump(name, 1);
        }
        // A second kind A while one runs.
        if let Some(first) = l0.first() {
            let a = self
                .meta
                .begin_compaction(req(JobKind::L0, vec![first.id]))
                .await
                .unwrap()
                .unwrap();
            let snapshot = self.visible(None).await;
            for second in [vec![first.id], l0.iter().map(|s| s.id).collect()] {
                assert!(
                    self.meta
                        .begin_compaction(req(JobKind::L0, second))
                        .await
                        .unwrap()
                        .is_none(),
                    "seed {seed}: second kind A accepted"
                );
            }
            assert_eq!(
                self.visible(None).await,
                snapshot,
                "seed {seed}: second kind A changed state"
            );
            self.meta.fail_compaction(a.id, "test").await.unwrap();
            let j = self
                .meta
                .begin_compaction(req(JobKind::L0, vec![first.id]))
                .await
                .unwrap()
                .unwrap();
            self.meta.fail_compaction(j.id, "test").await.unwrap();
            self.cov.bump("inv_second_a", 1);
        }
        self.check().await;
    }

    /// Step (d): GC with zero graces; every blob referenced by a visible row still exists.
    async fn gc(&mut self) {
        let seed = self.seed;
        let o = GcOptions {
            gc_grace: Duration::ZERO,
            orphan_grace: Duration::ZERO,
            sweep_blobs: true,
            dry_run: false,
        };
        let now = forge_model::time::now_ms() + 1;
        let os = self.store.object_store();
        run_gc(
            self.meta.as_ref(),
            os.as_ref(),
            &Clocks::at(now, now),
            &o,
            &BlobGuard::default(),
        )
        .await
        .unwrap();
        self.cov.bump("gc_passes", 1);
        for s in self.visible(None).await {
            for path in blob_refs_in(&scan(self.store.as_ref(), &s).await) {
                assert!(
                    os.head(&Path::from(path.as_str())).await.is_ok(),
                    "seed {seed}: blob {path} of a visible row is gone"
                );
            }
        }
        self.check().await;
    }

    /// Step (e): kind A and kind B begun together, new flushes, then both run concurrently.
    async fn interleave(&mut self, r: &mut Rng, keys: &mut BTreeMap<(usize, u64, u64), KeyInfo>) {
        let seed = self.seed;
        let p = PROJECTS[r.below(2) as usize];
        let segs = self.visible(Some(p)).await;
        let mut l0: Vec<&SegmentMeta> = segs.iter().filter(|s| s.level == Level::L0).collect();
        l0.sort_by_key(|s| s.min_version);
        let mut hours: BTreeMap<HourBucket, Vec<&SegmentMeta>> = BTreeMap::new();
        for s in segs.iter().filter(|s| s.level == Level::L1) {
            hours.entry(s.partition_hour).or_default().push(s);
        }
        if l0.is_empty() || hours.is_empty() {
            return;
        }
        let n = 1 + r.below(l0.len() as u64) as usize;
        let (hour, in_hour) = hours.iter().nth(r.below(hours.len() as u64) as usize).unwrap();
        let g = groups(in_hour);
        let k = 1 + r.below(g.len() as u64) as usize;
        let req_a = JobRequest {
            project_id: forge_model::testing::pid(p),
            kind: JobKind::L0,
            inputs: l0[..n].iter().map(|s| s.id).collect(),
        };
        let req_b = JobRequest {
            project_id: forge_model::testing::pid(p),
            kind: JobKind::L1,
            inputs: g[g.len() - k..].concat(),
        };
        let hour = *hour;
        self.request_cov(&req_a).await;
        self.request_cov(&req_b).await;
        let before = self.before().await;
        let ja = self.meta.begin_compaction(req_a).await.unwrap().expect("kind A");
        let jb = self
            .meta
            .begin_compaction(req_b)
            .await
            .unwrap()
            .expect("kind B beside kind A");
        let fallback = newest_l0_hour(&ja);
        for _ in 0..1 + r.below(3) {
            for _ in 0..2 {
                let v = self.posts.len() as u64 + 1;
                let x = post(r, keys, v);
                self.mem.push(x.clone());
                self.posts.push(x);
            }
            self.flush().await;
        }
        *self.delay.lock().unwrap() = Some((hour, r.chance(50)));
        let (cfg_a, cfg_b) = (random_cfg(r, 4), random_cfg(r, 4));
        let (cxa, cxb) = (self.ctx(cfg_a), self.ctx(cfg_b));
        let (ra, rb) = tokio::join!(job::run_job(&cxa, ja), job::run_job(&cxb, jb));
        *self.delay.lock().unwrap() = None;
        let (ra, rb) = (
            ra.unwrap_or_else(|e| panic!("seed {seed}: kind A failed: {e}")),
            rb.unwrap_or_else(|e| panic!("seed {seed}: kind B failed: {e}")),
        );
        self.record(JobKind::L0, &ra);
        self.record(JobKind::L1, &rb);
        self.cov.bump("interleavings", 1);
        self.cov
            .bump("b_committed_first", u64::from(rb.elapsed_ms < ra.elapsed_ms));
        self.check_outputs(&before, None, fallback).await;
        self.check().await;
    }
}

fn newest_l0_hour(job: &CompactionJob) -> Option<HourBucket> {
    job.inputs
        .iter()
        .filter(|s| s.level == Level::L0)
        .max_by_key(|s| s.min_version)
        .map(|s| s.partition_hour)
}

/// Settings drawn per job (RT #4).
fn random_cfg(r: &mut Rng, l0_trigger: usize) -> CompactionConfig {
    CompactionConfig {
        target_segment: if r.chance(25) {
            bytesize::ByteSize::kib(8)
        } else {
            bytesize::ByteSize::mib(4)
        },
        slice_bytes: if r.chance(50) {
            bytesize::ByteSize::kib(1)
        } else {
            bytesize::ByteSize::mib(128)
        },
        // 1 B: the prefix is one L0 and no kind B fits; 64 KiB: a few groups (a suffix cut); else unbounded.
        max_job_bytes: match r.below(3) {
            0 => bytesize::ByteSize::b(1),
            1 => bytesize::ByteSize::kib(64),
            _ => bytesize::ByteSize::b(u64::MAX / 4),
        },
        l0_trigger,
        ..Default::default()
    }
}

async fn history(seed: u64) -> Cov {
    let mut r = Rng(seed);
    let mut w = World::new(seed).await;
    let mut keys = keys(&mut r);
    let quiet = if r.chance(33) { 20 + r.below(30) } else { u64::MAX };
    let mut v = 0;
    while v < MAX_POSTS {
        v += 1;
        let x = post(&mut r, &mut keys, v);
        w.mem.push(x.clone());
        w.posts.push(x);
        // A phase of 20+ flushes with no jobs.
        if (quiet..quiet.saturating_add(22)).contains(&v) {
            w.flush().await;
            continue;
        }
        if r.chance(12) {
            w.flush().await;
        }
        match r.below(100) {
            0..10 => w.pick_and_run(&mut r, false).await,
            10..20 => w.pick_and_run(&mut r, true).await,
            20..30 => w.invalid(&mut r).await,
            30..35 => w.gc().await,
            35..45 => {
                w.interleave(&mut r, &mut keys).await;
                v = w.posts.len() as u64;
            }
            _ => {}
        }
    }
    w.flush().await;
    w.pick_and_run(&mut r, true).await;
    w.check().await;
    w.cov
}

/// One 1-row L1 is far below the 4 MiB fold arm's limit (target / 2): folds can happen (RT #4).
async fn assert_precondition() {
    let store = memory_store();
    let row = random_row(&mut Rng(1), "p", 1, 1, 1);
    let spec = SegmentSpec {
        id: SegmentId::new(),
        project_id: forge_model::testing::pid("p"),
        level: Level::L1,
        partition_hour: HourBucket(BASE_H),
    };
    let w = store
        .write_segment(spec, Box::pin(futures::stream::iter([Ok(batch_from_rows(&[row]))])))
        .await
        .unwrap();
    assert!(w.meta.byte_size < 2 << 20, "{}", w.meta.byte_size);
}

async fn shard(i: u64) {
    assert_precondition().await;
    let seeds: Vec<u64> = match std::env::var("FORGE_PROP_SEED").ok().and_then(|s| s.parse().ok()) {
        Some(s) => vec![s],
        None => {
            let n: u64 = std::env::var("FORGE_PROP_SEEDS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8);
            (1..=n).collect()
        }
    };
    let t = std::time::Instant::now();
    let mut mine = 0;
    for seed in seeds.into_iter().filter(|s| s % 4 == i) {
        let cov = history(seed).await;
        for (t, c) in TOTALS.iter().zip(cov.0) {
            t.fetch_add(c, Ordering::Relaxed);
        }
        mine += 1;
    }
    SEEDS_RUN.fetch_add(mine, Ordering::Relaxed);
    #[allow(clippy::print_stdout)]
    {
        println!("shard {i}: {mine} seeds in {:?}", t.elapsed());
    }
    if SHARDS_DONE.fetch_add(1, Ordering::AcqRel) + 1 == 4 {
        let totals: Vec<(&str, u64)> = COV
            .iter()
            .copied()
            .zip(TOTALS.iter().map(|t| t.load(Ordering::Relaxed)))
            .collect();
        let n = SEEDS_RUN.load(Ordering::Relaxed);
        #[allow(clippy::print_stdout)]
        {
            println!("compaction_prop: {n} seeds, coverage {totals:?}");
        }
        if n >= 8 {
            for (name, c) in &totals {
                assert!(*c > 0, "coverage {name} = 0 over {n} seeds: {totals:?}");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_prop_shard_0() {
    shard(0).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_prop_shard_1() {
    shard(1).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_prop_shard_2() {
    shard(2).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_prop_shard_3() {
    shard(3).await;
}
