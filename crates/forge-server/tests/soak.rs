//! plans/phase-4 §8: the scaled soak, the single-run substitute for the spec's 24 h x 20k spans/s exit. In process:
//! a data clock running 192x the wall clock (20 wall minutes = 64 data hours), sizes scaled by the same factor,
//! retention on `soak` (1 day) and none on `ctl`. Manual, one run:
//!
//! cargo test -p forge-server --profile quick-release --test soak -- --ignored --nocapture
//!
//! Env: FORGE_SOAK_SECS (default 1200, the least that fits two steady-state windows), FORGE_SOAK_RATE (spans/s of first posts, default 20000).
#![allow(clippy::print_stdout)]
use bytesize::ByteSize;

/// The shipped binary's allocator (forge-cli `mimalloc` feature), so the measured RSS matches what ships.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use forge_ingest::compact::{CompactCtx, L1_MAX_RUNS, compact_project};
use forge_ingest::lifecycle::{GcOptions, run_gc};
use forge_ingest::{Ingest, IngestError};
use forge_meta::Project;
use forge_model::catalog::HourBucket;
use forge_model::config::{ForgeConfig, META_DB};
use forge_model::enums::{RunType, SpanStatus};
use forge_model::ids::ProjectId;
use forge_model::row::SpanRow;
use forge_model::testing::{Rng, agent_parent, files_under, pid, sid, tid};
use forge_model::time::{Clock, Clocks, now_ms, wall_clock};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const SPEED: i64 = 192;
const HOUR_MS: i64 = 3_600_000;
const SPANS: u64 = 100;
const TASKS: u64 = 16;
const MIB: f64 = 1_048_576.0;

fn env_u64(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

// ---------------------------------------------------------------------------------------------------------------
// Data clock: T0 + 192 (wall - wall0), T0 = wall0 - 24 h; can be frozen.
// ---------------------------------------------------------------------------------------------------------------

struct DataClock {
    wall0: i64,
    frozen: AtomicI64,
}

impl DataClock {
    const RUNNING: i64 = i64::MIN;
    fn now(&self) -> i64 {
        match self.frozen.load(Ordering::SeqCst) {
            Self::RUNNING => self.wall0 - 24 * HOUR_MS + SPEED * (now_ms() - self.wall0),
            f => f,
        }
    }
    /// Freezes at a whole second (so the retention cutoff is one too) and returns it.
    fn freeze(&self) -> i64 {
        let v = self.now() / 1000 * 1000;
        self.frozen.store(v, Ordering::SeqCst);
        v
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Log capture: warn/error events by target, and info counters.
// ---------------------------------------------------------------------------------------------------------------

#[derive(Default)]
struct Logs {
    problems: Mutex<Vec<String>>,
    gc_passes: AtomicU64,
}

struct Capture(Arc<Logs>);

struct Msg(String);
impl tracing::field::Visit for Msg {
    fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        let _ = write!(self.0, " {}={v:?}", f.name());
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
    fn on_event(&self, e: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut m = Msg(String::new());
        e.record(&mut m);
        if m.0.contains("message=gc pass done") {
            self.0.gc_passes.fetch_add(1, Ordering::Relaxed);
        }
        if *e.metadata().level() <= tracing::Level::WARN {
            let line = format!("{} {}:{}", e.metadata().level(), e.metadata().target(), m.0);
            self.0.problems.lock().unwrap().push(line);
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Load
// ---------------------------------------------------------------------------------------------------------------

/// Expected merged fields of a sampled trace: span (hex) -> (name, start, end).
type Expected = HashMap<String, (String, i64, Option<i64>)>;

struct Sampled {
    project: &'static str,
    trace: String,
    spans: Expected,
}

#[derive(Default)]
struct Load {
    stop: AtomicBool,
    next_trace: AtomicU64,
    acked_spans: AtomicU64,
    backpressure: AtomicU64,
    other_errors: AtomicU64,
    /// (project is ctl, start second on the data clock) -> acked spans (first posts).
    ledger: Mutex<HashMap<(bool, i64), u64>>,
    sampled: Mutex<Vec<Sampled>>,
}

/// One trace: start posts (agent-shaped, perf.rs style: parent among the previous 8 spans, depth <= 10, one llm
/// span in three with a ~200 B input), and the end patches of every fifth span (end, status OK, a new name, the
/// output; they carry start_time as OTLP posts do).
fn trace_rows(n: u64, project: &ProjectId, start0: i64, text: &str) -> (Vec<SpanRow>, Vec<SpanRow>) {
    let mut r = Rng(n + 1);
    let mut depth = [0u32; SPANS as usize];
    let (mut starts, mut patches) = (Vec::new(), Vec::new());
    for i in 0..SPANS {
        let parent = agent_parent(&mut r, &mut depth, i);
        let llm = i % 3 == 1;
        let start = start0 + i as i64 * 1_000_017;
        let base = SpanRow {
            project_id: Some(project.clone()),
            trace_id: tid(n),
            span_id: sid(i),
            start_time_ns: Some(start),
            ..Default::default()
        };
        starts.push(SpanRow {
            parent_span_id: parent.map(sid),
            name: Some(format!("step-{i}")),
            run_type: Some(if llm { RunType::Llm } else { RunType::Chain }),
            status: Some(SpanStatus::Unset),
            session_id: Some(format!("thread-{}", n % 50)),
            metadata: Some(format!("{{\"user\":\"u{}\",\"step\":{i}}}", i % 17)),
            inputs: llm.then(|| text.to_string()),
            ..base.clone()
        });
        if i % 5 == 0 {
            patches.push(SpanRow {
                end_time_ns: Some(start + 1_000_000 + r.below(1_000_000_000) as i64),
                status: Some(SpanStatus::Ok),
                name: Some(format!("step-{i}-done")),
                total_tokens: llm.then_some(150),
                outputs: llm.then(|| text.to_string()),
                ..base
            });
        }
    }
    (starts, patches)
}

async fn submit(f: &forge_server::Forge, load: &Load, rows: Vec<SpanRow>) -> bool {
    match f.ingest.submit(rows).await {
        Ok(()) => true,
        Err(IngestError::Backpressure) => {
            load.backpressure.fetch_add(1, Ordering::Relaxed);
            false
        }
        Err(_) => {
            load.other_errors.fetch_add(1, Ordering::Relaxed);
            false
        }
    }
}

/// One of TASKS loaders, paced to RATE / TASKS first-post spans per second.
async fn loader(f: Arc<forge_server::Forge>, load: Arc<Load>, clock: Arc<DataClock>, rate: u64, task: u64) {
    let text = serde_json::json!({"messages": [{"role": "user", "content": "lorem ipsum ".repeat(16)}]}).to_string();
    let (soak, ctl) = (pid("soak"), pid("ctl"));
    let period = Duration::from_secs_f64(SPANS as f64 * TASKS as f64 / rate as f64);
    let mut next = tokio::time::Instant::now();
    let mut due: VecDeque<(tokio::time::Instant, Vec<SpanRow>, Option<usize>)> = VecDeque::new();
    let mut rng = Rng(task * 7919 + 1);
    let mut ledger: HashMap<(bool, i64), u64> = HashMap::new();
    while !load.stop.load(Ordering::Relaxed) {
        tokio::time::sleep_until(next).await;
        next += period;
        while due.front().is_some_and(|d| d.0 <= tokio::time::Instant::now()) {
            let (_, rows, sample) = due.pop_front().expect("front");
            let applied: Vec<_> = rows
                .iter()
                .map(|r| (r.span_id.to_string(), r.name.clone(), r.end_time_ns))
                .collect();
            if submit(&f, &load, rows).await
                && let Some(s) = sample
            {
                let mut v = load.sampled.lock().unwrap();
                for (span, name, end) in applied {
                    let e = v[s].spans.get_mut(&span).expect("sampled span");
                    e.0 = name.expect("patch name");
                    e.2 = end;
                }
            }
        }
        let n = load.next_trace.fetch_add(1, Ordering::Relaxed);
        let is_ctl = n % 10 == 9;
        let project = if is_ctl { &ctl } else { &soak };
        let (starts, patches) = trace_rows(n, project, clock.now() * 1_000_000, &text);
        let expected: Expected = starts
            .iter()
            .map(|r| {
                let k = r.span_id.to_string();
                (
                    k,
                    (r.name.clone().expect("name"), r.start_time_ns.expect("start"), None),
                )
            })
            .collect();
        let secs: Vec<i64> = starts
            .iter()
            .map(|r| r.start_time_ns.expect("start").div_euclid(1_000_000_000))
            .collect();
        if !submit(&f, &load, starts).await {
            continue;
        }
        load.acked_spans.fetch_add(SPANS, Ordering::Relaxed);
        for s in secs {
            *ledger.entry((is_ctl, s)).or_default() += 1;
        }
        let sample = matches!(n % 10_000, 0 | 9).then(|| {
            let mut v = load.sampled.lock().unwrap();
            v.push(Sampled {
                project: if is_ctl { "ctl" } else { "soak" },
                trace: tid(n).to_string(),
                spans: expected,
            });
            v.len() - 1
        });
        let at = tokio::time::Instant::now() + Duration::from_millis(1000 + rng.below(4000));
        due.push_back((at, patches, sample));
    }
    let mut l = load.ledger.lock().unwrap();
    for (k, v) in ledger {
        *l.entry(k).or_default() += v;
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Sampler
// ---------------------------------------------------------------------------------------------------------------

fn rss_bytes() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").expect("status");
    let kb: u64 = s
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
        .expect("VmRSS");
    kb * 1024
}

/// (files, bytes) under `dir`, staging files (`#`) excluded.
fn files(dir: &Path) -> (u64, u64) {
    files_under(dir)
        .into_iter()
        .filter(|p| !p.to_string_lossy().contains('#'))
        .filter_map(|p| p.metadata().ok())
        .fold((0, 0), |(n, b), m| (n + 1, b + m.len()))
}

#[derive(Clone, Debug, Default)]
struct Row {
    t: f64,
    rss: u64,
    soak_l0: i64,
    soak_l1: i64,
    ctl_l0: i64,
    ctl_l1: i64,
    compacting: i64,
    deleted: i64,
    deleted_recent: i64,
    max_segs_per_hour: i64,
    objects: u64,
    meta_bytes: u64,
    trace_index_soak: i64,
    trace_index_ctl: i64,
    data_bytes: u64,
    spans_per_s: f64,
    backpressure: u64,
    jobs_done: i64,
    jobs_failed: i64,
    jobs_a: i64,
    jobs_b: i64,
    b_suffix: u64,
    b_whole: u64,
    l0_bytes: u64,
    l1_bytes: u64,
}

const CSV_HEADER: &str = "t_s,rss_mib,soak_l0,soak_l1,ctl_l0,ctl_l1,compacting,deleted,deleted_recent,max_segs_per_hour,\
objects,meta_mib,trace_index_soak,trace_index_ctl,data_mib,spans_per_s,backpressure,jobs_done,jobs_failed,jobs_a,jobs_b,b_suffix,b_whole,\
l0_mib,l1_mib,write_amp";

impl Row {
    fn csv(&self) -> String {
        format!(
            "{:.0},{:.1},{},{},{},{},{},{},{},{},{},{:.2},{},{},{:.1},{:.0},{},{},{},{},{},{},{},{:.1},{:.1},{:.2}",
            self.t,
            self.rss as f64 / MIB,
            self.soak_l0,
            self.soak_l1,
            self.ctl_l0,
            self.ctl_l1,
            self.compacting,
            self.deleted,
            self.deleted_recent,
            self.max_segs_per_hour,
            self.objects,
            self.meta_bytes as f64 / MIB,
            self.trace_index_soak,
            self.trace_index_ctl,
            self.data_bytes as f64 / MIB,
            self.spans_per_s,
            self.backpressure,
            self.jobs_done,
            self.jobs_failed,
            self.jobs_a,
            self.jobs_b,
            self.b_suffix,
            self.b_whole,
            self.l0_bytes as f64 / MIB,
            self.l1_bytes as f64 / MIB,
            self.l1_bytes as f64 / (self.l0_bytes.max(1)) as f64
        )
    }
}

/// Metastore-side state, read with a separate read-only connection.
struct Probe {
    dir: PathBuf,
    cfg: ForgeConfig,
    /// Segment id -> (level, bytes), every segment row ever seen (write amplification).
    seen: HashMap<String, (i64, u64)>,
    /// Kind-B jobs classified: id -> took a suffix (an older group stayed out).
    kind_b: HashMap<i64, bool>,
}

impl Probe {
    fn conn(&self) -> rusqlite::Connection {
        rusqlite::Connection::open_with_flags(self.dir.join(META_DB), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("meta.db")
    }

    /// Fills the metastore fields of `row`; returns violations of the per-hour bound.
    fn sample(&mut self, row: &mut Row, data_now_ms: i64) -> Vec<String> {
        let c = self.conn();
        let q = |sql: &str| -> i64 { c.query_row(sql, [], |r| r.get(0)).expect(sql) };
        let live = |p: &str, level: u8| {
            q(&format!(
                "SELECT count(*) FROM segments WHERE project_id = '{p}' AND level = {level} AND state <> 'deleted'"
            ))
        };
        row.soak_l0 = live("soak", 0);
        row.soak_l1 = live("soak", 1);
        row.ctl_l0 = live("ctl", 0);
        row.ctl_l1 = live("ctl", 1);
        row.compacting = q("SELECT count(*) FROM segments WHERE state = 'compacting'");
        row.deleted = q("SELECT count(*) FROM segments WHERE state = 'deleted'");
        let window = (self.cfg.compaction.gc_grace + self.cfg.compaction.gc_interval).as_millis() as i64;
        row.deleted_recent = q(&format!(
            "SELECT count(*) FROM segments WHERE state = 'deleted' AND deleted_at_ms >= {}",
            now_ms() - window
        ));
        row.trace_index_soak = q("SELECT count(*) FROM trace_index WHERE project_id = 'soak'");
        row.trace_index_ctl = q("SELECT count(*) FROM trace_index WHERE project_id = 'ctl'");
        row.jobs_done = q("SELECT count(*) FROM compaction_jobs WHERE status = 'done'");
        row.jobs_failed = q("SELECT count(*) FROM compaction_jobs WHERE status = 'failed'");
        row.jobs_a = q("SELECT count(*) FROM compaction_jobs WHERE status = 'done' AND kind = 'l0'");
        row.jobs_b = q("SELECT count(*) FROM compaction_jobs WHERE status = 'done' AND kind = 'l1'");
        // Write amplification: every segment row seen (rows stay >= gc_grace after deletion; samples are 10 s).
        let mut st = c.prepare("SELECT id, level, byte_size FROM segments").expect("prepare");
        for x in st
            .query_map([], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?))
            })
            .expect("query")
        {
            let (id, level, bytes) = x.expect("row");
            self.seen.entry(id).or_insert((level, bytes as u64));
        }
        row.l0_bytes = self.seen.values().filter(|v| v.0 == 0).map(|v| v.1).sum();
        row.l1_bytes = self.seen.values().filter(|v| v.0 == 1).map(|v| v.1).sum();
        // Kind B: suffix (an older L1 of the hour existed at begin and was not an input) vs whole hour.
        let mut st = c
            .prepare(
                "SELECT j.id, j.project_id, j.created_at_ms, min(s.partition_hour), min(s.min_version) \
                 FROM compaction_jobs j JOIN compaction_inputs ci ON ci.job_id = j.id \
                 JOIN segments s ON s.id = ci.segment_id WHERE j.kind = 'l1' AND j.status = 'done' GROUP BY j.id",
            )
            .expect("prepare");
        let jobs: Vec<(i64, String, i64, i64, i64)> = st
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)))
            .expect("query")
            .collect::<Result<_, _>>()
            .expect("rows");
        for (id, p, created, hour, min_v) in jobs {
            if self.kind_b.contains_key(&id) {
                continue;
            }
            let older: i64 = c
                .query_row(
                    "SELECT count(*) FROM segments WHERE project_id = ?1 AND level = 1 AND partition_hour = ?2 \
                     AND max_version < ?3 AND created_at_ms <= ?4 AND (deleted_at_ms IS NULL OR deleted_at_ms >= ?4)",
                    rusqlite::params![p, hour, min_v, created],
                    |r| r.get(0),
                )
                .expect("older");
            self.kind_b.insert(id, older > 0);
        }
        row.b_suffix = self.kind_b.values().filter(|s| **s).count() as u64;
        row.b_whole = self.kind_b.values().filter(|s| !**s).count() as u64;
        // Per-hour bound on closed, unexpired hours (both projects; ctl never expires).
        let target = self.cfg.compaction.target_segment.as_u64();
        let mut st = c
            .prepare(
                "SELECT project_id, partition_hour, count(*), sum(byte_size) FROM segments \
                 WHERE level = 1 AND state <> 'deleted' GROUP BY project_id, partition_hour",
            )
            .expect("prepare");
        let hours: Vec<(String, i64, i64, i64)> = st
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .expect("query")
            .collect::<Result<_, _>>()
            .expect("rows");
        let cutoff_ms = data_now_ms - 24 * HOUR_MS;
        let mut bad = Vec::new();
        row.max_segs_per_hour = 0;
        for (p, h, n, bytes) in hours {
            let end_ms = (h + 1) * HOUR_MS;
            let closed = end_ms < data_now_ms - HOUR_MS;
            let expired = p == "soak" && end_ms <= cutoff_ms;
            if !closed || expired {
                continue;
            }
            if p == "soak" {
                row.max_segs_per_hour = row.max_segs_per_hour.max(n);
            }
            let bound = 2 * (bytes as u64).div_ceil(target) as i64 + 2 * (L1_MAX_RUNS as i64 + 1);
            if n > bound {
                bad.push(format!(
                    "t={:.0}s {p} hour {h}: {n} segments > {bound} ({bytes} B)",
                    row.t
                ));
            }
        }
        let db = |s: &str| std::fs::metadata(self.dir.join(format!("{META_DB}{s}"))).map_or(0, |m| m.len());
        row.meta_bytes = db("") + db("-wal");
        let (objects, _) = files(&self.dir.join("data/segments"));
        row.objects = objects;
        row.data_bytes = files(&self.dir).1;
        bad
    }
}

fn median(v: &[f64]) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    let mut v = v.to_vec();
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

/// Values of `f` over rows with `lo <= t < hi`.
fn window(rows: &[(f64, f64)], lo: f64, hi: f64) -> Vec<f64> {
    rows.iter().filter(|(t, _)| *t >= lo && *t < hi).map(|x| x.1).collect()
}

async fn count(f: &forge_server::Forge, sql: &str) -> i64 {
    let b = f.query.sql_batches(sql).await.expect(sql);
    arrow::array::AsArray::as_primitive::<arrow::datatypes::Int64Type>(b[0].column(0)).value(0)
}

// ---------------------------------------------------------------------------------------------------------------
// The soak
// ---------------------------------------------------------------------------------------------------------------

#[ignore = "manual soak (plans/phase-4 §8): run once in quick-release"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn soak_scaled_48_data_hours() {
    let secs = env_u64("FORGE_SOAK_SECS", 1200);
    let rate = env_u64("FORGE_SOAK_RATE", 20_000);
    let baseline_secs = 60;
    let logs = Arc::new(Logs::default());
    tracing_subscriber::registry().with(Capture(logs.clone())).init();

    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().to_path_buf();
    let mut cfg = ForgeConfig::rooted_at(&dir);
    cfg.memtable.flush_interval = Duration::from_secs(5);
    let c = &mut cfg.compaction;
    c.target_segment = ByteSize::kib(1536);
    c.max_job_bytes = ByteSize::mib(16);
    c.slice_bytes = ByteSize::mib(64);
    c.interval = Duration::from_secs(2);
    c.l0_max_age = Duration::from_secs(10);
    c.gc_interval = Duration::from_secs(15);
    c.gc_grace = Duration::from_secs(120);
    // Below the load-time floor (4 x flush_interval + 10 min = 620 s): built in code, and no blobs here.
    c.orphan_grace = Duration::from_secs(600);
    // Retention steady state (review 1 #8): the first data hour expires at 25 data hours (data starts 24 h back),
    // and its deleted rows are purged within gc_grace + gc_interval; plus 10 s of slack.
    let steady = (25 * 3600) as f64 / SPEED as f64 + (c.gc_grace + c.gc_interval).as_secs_f64() + 10.0;
    assert!(
        secs as f64 >= steady + 240.0,
        "FORGE_SOAK_SECS {secs} leaves no two steady-state windows (steady from {steady:.0} s)"
    );

    let f = forge_server::Forge::open(cfg.clone()).await.expect("open");
    for (p, days) in [("soak", Some(1)), ("ctl", None)] {
        f.meta
            .create_project(Project {
                id: pid(p),
                name: p.into(),
                api_key_hash: None,
                retention_days: days,
                created_at_ms: now_ms(),
            })
            .await
            .expect("create project");
    }
    let bg = f.ingest.start_background();
    // The timed run starts after the baseline; its data starts 24 h before the wall clock (T0).
    let clock = Arc::new(DataClock {
        wall0: now_ms() + baseline_secs * 1000,
        frozen: AtomicI64::new(DataClock::RUNNING),
    });
    let data_clock: Clock = {
        let c = clock.clone();
        Arc::new(move || c.now())
    };

    let load = Arc::new(Load::default());
    let tasks: Vec<_> = (0..TASKS)
        .map(|t| tokio::spawn(loader(f.clone(), load.clone(), clock.clone(), rate, t)))
        .collect();

    // RSS every second, from the start of the baseline (t < 0) to the end.
    let started = Instant::now() + Duration::from_secs(baseline_secs as u64);
    // Seconds since the timed run started (negative during the baseline).
    let t_of = move || {
        let now = Instant::now();
        now.saturating_duration_since(started).as_secs_f64() - started.saturating_duration_since(now).as_secs_f64()
    };
    let rss: Arc<Mutex<Vec<(f64, f64)>>> = Arc::default();
    let rss_stop = Arc::new(AtomicBool::new(false));
    let rss_task = {
        let (rss, stop) = (rss.clone(), rss_stop.clone());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            while !stop.load(Ordering::Relaxed) {
                tick.tick().await;
                rss.lock().unwrap().push((t_of(), rss_bytes() as f64));
            }
        })
    };

    // Baseline: load with compaction and GC off (the lifecycle task is not started).
    tokio::time::sleep(Duration::from_secs(baseline_secs as u64)).await;
    let baseline_rss = median(&window(&rss.lock().unwrap(), -(baseline_secs as f64), 0.0));
    println!(
        "baseline (60 s load, no compaction): RSS median {:.1} MiB",
        baseline_rss / MIB
    );
    let lc = forge_server::lifecycle::start_with(f.clone(), data_clock.clone());

    // The timed run: a sample every 10 s.
    let csv_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/soak");
    std::fs::create_dir_all(&csv_dir).expect("csv dir");
    let csv_path = csv_dir.join(format!("{}.csv", now_ms() / 1000));
    let mut csv = vec![CSV_HEADER.to_string()];
    let mut probe = Probe {
        dir: dir.clone(),
        cfg: cfg.clone(),
        seen: HashMap::new(),
        kind_b: HashMap::new(),
    };
    let mut rows: Vec<Row> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    let mut last = (0.0f64, load.acked_spans.load(Ordering::Relaxed));
    println!("{CSV_HEADER}");
    for i in 1..=secs / 10 {
        tokio::time::sleep_until(tokio::time::Instant::from_std(started + Duration::from_secs(i * 10))).await;
        let mut row = Row {
            t: t_of(),
            rss: rss_bytes(),
            backpressure: load.backpressure.load(Ordering::Relaxed),
            ..Default::default()
        };
        let acked = load.acked_spans.load(Ordering::Relaxed);
        row.spans_per_s = (acked - last.1) as f64 / (row.t - last.0);
        last = (row.t, acked);
        let data_now = clock.now();
        failures.extend(tokio::task::block_in_place(|| probe.sample(&mut row, data_now)));
        if row.soak_l0 > 16 || row.ctl_l0 > 16 {
            failures.push(format!(
                "t={:.0}s live L0 soak {} ctl {} > 16",
                row.t, row.soak_l0, row.ctl_l0
            ));
        }
        if row.deleted > row.deleted_recent + 10 {
            failures.push(format!(
                "t={:.0}s deleted {} > {} deleted in the last gc_grace + gc_interval + 10",
                row.t, row.deleted, row.deleted_recent
            ));
        }
        csv.push(row.csv());
        println!("{}", row.csv());
        rows.push(row);
        std::fs::write(&csv_path, csv.join("\n") + "\n").expect("write csv");
    }

    // Ledger with GC quiesced: stop the load, freeze the data clock, stop the lifecycle task, final flush.
    load.stop.store(true, Ordering::SeqCst);
    for t in tasks {
        t.await.expect("loader");
    }
    let frozen = clock.freeze();
    // Failed jobs and warnings while running; the shutdown cancels running jobs (D7: they fail), reported apart.
    let failed_running = rows.last().map_or(0, |r| r.jobs_failed);
    let problems = logs.problems.lock().unwrap().clone();
    lc.shutdown(Duration::from_secs(60)).await;
    bg.shutdown(Duration::from_secs(60))
        .await
        .expect("final flush in time")
        .expect("final flush");
    rss_stop.store(true, Ordering::Relaxed);
    let _ = rss_task.await;
    let clocks = Clocks {
        data: data_clock.clone(),
        wall: wall_clock(),
    };
    let cx = CompactCtx::new(f.meta.clone(), f.segments.clone(), cfg.compaction.clone(), clocks);
    for p in ["soak", "ctl"] {
        loop {
            let s = compact_project(&cx, &pid(p)).await.expect("compact_project");
            if !s.failed.is_empty() {
                failures.push(format!("final compact_project {p}: {:?}", s.failed));
            }
            if !s.capped {
                break;
            }
        }
    }
    let o = GcOptions::from_config(&cfg.compaction);
    let store = f.segments.object_store();
    let gc = run_gc(
        f.meta.as_ref(),
        store.as_ref(),
        &Clocks::at(frozen, now_ms()),
        &o,
        f.ingest.blob_guard(),
    )
    .await
    .expect("final gc");
    println!("final gc: {gc:?}");

    let ledger = load.ledger.lock().unwrap().clone();
    let cutoff_ms = frozen - 24 * HOUR_MS;
    let cutoff_ns = cutoff_ms * 1_000_000;
    let cutoff_hour_ns = HourBucket::from_nanos(cutoff_ns).0 * HourBucket::NANOS;
    let ctl_ledger: u64 = ledger.iter().filter(|(k, _)| k.0).map(|x| x.1).sum();
    let soak_ledger_all: u64 = ledger.iter().filter(|(k, _)| !k.0).map(|x| x.1).sum();
    let soak_ledger: u64 = ledger
        .iter()
        .filter(|(k, _)| !k.0 && k.1 >= cutoff_ms / 1000)
        .map(|x| x.1)
        .sum();
    let ctl_n = count(&f, "SELECT count(*) FROM spans WHERE project_id = 'ctl'").await;
    let soak_n = count(
        &f,
        &format!("SELECT count(*) FROM spans WHERE project_id = 'soak' AND CAST(start_time AS BIGINT) >= {cutoff_ns}"),
    )
    .await;
    let soak_old = count(
        &f,
        &format!(
            "SELECT count(*) FROM spans WHERE project_id = 'soak' AND CAST(start_time AS BIGINT) < {cutoff_hour_ns}"
        ),
    )
    .await;
    let soak_visible = count(&f, "SELECT count(*) FROM spans WHERE project_id = 'soak'").await;
    println!(
        "ledger: ctl {ctl_n} visible / {ctl_ledger} acked; soak >= cutoff {soak_n} visible / {soak_ledger} acked; \
         soak below the cutoff hour {soak_old}; soak visible {soak_visible} of {soak_ledger_all} acked"
    );
    if ctl_n as u64 != ctl_ledger {
        failures.push(format!("ledger: ctl {ctl_n} != {ctl_ledger}"));
    }
    if soak_n as u64 != soak_ledger {
        failures.push(format!("ledger: soak >= cutoff {soak_n} != {soak_ledger}"));
    }
    if soak_old != 0 {
        failures.push(format!("ledger: {soak_old} soak spans below the cutoff hour"));
    }
    // Sampled keys: present with their merged fields, unless their hour expired.
    let sampled = std::mem::take(&mut *load.sampled.lock().unwrap());
    let (mut checked, mut expired) = (0, 0);
    for s in &sampled {
        let b = f
            .query
            .sql_batches(&format!(
                "SELECT encode(span_id, 'hex') AS s, name, CAST(start_time AS BIGINT) AS st, \
                 CAST(end_time AS BIGINT) AS et FROM spans WHERE project_id = '{}' AND encode(trace_id, 'hex') = '{}'",
                s.project, s.trace
            ))
            .await
            .expect("sampled query");
        let mut got: Expected = HashMap::new();
        for b in &b {
            use arrow::array::{Array, AsArray};
            let (sp, name) = (b.column(0).as_string::<i32>(), b.column(1).as_string::<i32>());
            let st = b.column(2).as_primitive::<arrow::datatypes::Int64Type>();
            let et = b.column(3).as_primitive::<arrow::datatypes::Int64Type>();
            for i in 0..b.num_rows() {
                got.insert(
                    sp.value(i).to_string(),
                    (
                        name.value(i).to_string(),
                        st.value(i),
                        et.is_valid(i).then(|| et.value(i)),
                    ),
                );
            }
        }
        let start = s.spans.values().map(|v| v.1).min().expect("spans");
        if s.project == "soak" && start < cutoff_hour_ns && s.spans.values().all(|v| v.1 < cutoff_hour_ns) {
            expired += 1;
            if !got.is_empty() {
                failures.push(format!("sampled trace {} in an expired hour is visible", s.trace));
            }
        } else {
            checked += 1;
            if got != s.spans {
                failures.push(format!("sampled trace {} {}: fields differ", s.project, s.trace));
            }
        }
    }
    println!("sampled traces: {checked} checked, {expired} expired");
    let segs = f
        .meta
        .list_segments(&forge_meta::SegmentFilter::default())
        .await
        .expect("segments");
    let l0_left = segs
        .iter()
        .filter(|s| s.level == forge_model::catalog::Level::L0)
        .count();
    if l0_left != 0 {
        failures.push(format!("{l0_left} L0 left after the final compaction"));
    }

    // Objects and jobs at the end.
    let end = {
        let c = probe.conn();
        let q = |sql: &str| -> i64 { c.query_row(sql, [], |r| r.get(0)).expect(sql) };
        (
            q("SELECT count(*) FROM segments"),
            q("SELECT COALESCE(max(finished_at_ms - created_at_ms), 0) FROM compaction_jobs WHERE status = 'done'"),
            q("SELECT count(*) FROM compaction_jobs WHERE status = 'failed'"),
        )
    };
    let objects = files(&dir.join("data/segments")).0;
    if objects as i64 > 2 * end.0 + 4 {
        failures.push(format!("objects {objects} > 2 x {} segment rows + 4", end.0));
    }
    if end.1 >= cfg.compaction.gc_grace.as_millis() as i64 {
        failures.push(format!("longest job {} ms >= gc_grace", end.1));
    }
    if failed_running != 0 {
        failures.push(format!("{failed_running} failed jobs"));
    }
    let lifecycle_problems: Vec<_> = problems
        .iter()
        .filter(|l| l.contains("lifecycle") || l.contains("compact"))
        .collect();
    if !lifecycle_problems.is_empty() {
        failures.push(format!("lifecycle warn/error events: {lifecycle_problems:?}"));
    }

    // Windowed medians. RSS (scaled to FORGE_SOAK_SECS; 1200 s: t 400-600 s vs the last 200 s). The retention-bound
    // series compare two steady-state windows: [steady, half) vs [half, end] (review 1 #8).
    let s = secs as f64;
    let half = steady + (s - steady) / 2.0;
    let rss_all = rss.lock().unwrap().clone();
    let (mid_lo, mid_hi, last_lo) = (s / 3.0, s / 2.0, s * 5.0 / 6.0);
    let rss_mid = median(&window(&rss_all, mid_lo, mid_hi));
    let rss_last = median(&window(&rss_all, last_lo, s + 60.0));
    let rss_max_first = window(&rss_all, s / 5.0, mid_hi).into_iter().fold(0.0, f64::max);
    let rss_max_second = window(&rss_all, mid_hi, s + 60.0).into_iter().fold(0.0, f64::max);
    let slack = 64.0 * MIB;
    if rss_last > 1.1 * rss_mid + slack {
        failures.push(format!(
            "RSS median last window {:.1} MiB > 1.1 x {:.1} + 64",
            rss_last / MIB,
            rss_mid / MIB
        ));
    }
    if rss_max_second > 1.25 * rss_max_first + slack {
        failures.push(format!(
            "RSS max second half {:.1} MiB > 1.25 x {:.1} + 64",
            rss_max_second / MIB,
            rss_max_first / MIB
        ));
    }
    let mut medians = Vec::new();
    // meta.db is mostly trace_index; `ctl` (no retention) grows by design, so the bound applies to the file size
    // scaled by `soak`'s share of the trace_index rows; `ctl` is reported on its own.
    let soak_share = |r: &Row| r.trace_index_soak as f64 / (r.trace_index_soak + r.trace_index_ctl).max(1) as f64;
    for (name, v, bounded) in [
        (
            "soak live L1",
            rows.iter().map(|r| (r.t, r.soak_l1 as f64)).collect::<Vec<_>>(),
            true,
        ),
        (
            "soak trace_index rows",
            rows.iter().map(|r| (r.t, r.trace_index_soak as f64)).collect(),
            true,
        ),
        (
            "meta.db + -wal bytes x soak share",
            rows.iter()
                .map(|r| (r.t, r.meta_bytes as f64 * soak_share(r)))
                .collect(),
            true,
        ),
        (
            "meta.db + -wal bytes (both projects)",
            rows.iter().map(|r| (r.t, r.meta_bytes as f64)).collect(),
            false,
        ),
        (
            "ctl trace_index rows (no retention: grows)",
            rows.iter().map(|r| (r.t, r.trace_index_ctl as f64)).collect(),
            false,
        ),
    ] {
        let (mid, last) = (median(&window(&v, steady, half)), median(&window(&v, half, s + 60.0)));
        medians.push(format!(
            "{name}: median {mid:.0} (t {steady:.0}-{half:.0} s) -> {last:.0} (t {half:.0}-{s:.0} s)"
        ));
        if bounded && last > 1.1 * mid {
            failures.push(format!("{name}: last-window median {last:.0} > 1.1 x {mid:.0}"));
        }
    }

    // Report.
    let last_row = rows.last().cloned().unwrap_or_default();
    println!("\n===== SOAK SUMMARY ({secs} s at {rate} spans/s; substitute for the spec's 24 h x 20k/s exit)");
    println!("CSV: {}", csv_path.display());
    println!("{CSV_HEADER}");
    for r in rows.iter().filter(|r| (r.t.round() as u64).is_multiple_of(60)) {
        println!("{}", r.csv());
    }
    println!(
        "RSS: baseline {:.1} MiB; median t {mid_lo:.0}-{mid_hi:.0} s {:.1}; median last {:.1}; max t {:.0}-{mid_hi:.0} s \
         {:.1}; max second half {:.1}",
        baseline_rss / MIB,
        rss_mid / MIB,
        rss_last / MIB,
        s / 5.0,
        rss_max_first / MIB,
        rss_max_second / MIB
    );
    for m in &medians {
        println!("{m}");
    }
    println!(
        "max segments per closed soak hour: {}; spans acked {} ({} backpressure, {} other errors); jobs done {} \
         (A {}, B {}: suffix {}, whole hour {}), failed {} (+{} cancelled by the shutdown); gc passes {}; longest job {} ms; peak data dir {:.1} MiB; \
         write amp {:.2} (L1 {:.1} MiB / L0 {:.1} MiB)",
        rows.iter().map(|r| r.max_segs_per_hour).max().unwrap_or(0),
        load.acked_spans.load(Ordering::Relaxed),
        load.backpressure.load(Ordering::Relaxed),
        load.other_errors.load(Ordering::Relaxed),
        last_row.jobs_done,
        last_row.jobs_a,
        last_row.jobs_b,
        last_row.b_suffix,
        last_row.b_whole,
        failed_running,
        end.2 - failed_running,
        logs.gc_passes.load(Ordering::Relaxed),
        end.1,
        rows.iter().map(|r| r.data_bytes).max().unwrap_or(0) as f64 / MIB,
        last_row.l1_bytes as f64 / last_row.l0_bytes.max(1) as f64,
        last_row.l1_bytes as f64 / MIB,
        last_row.l0_bytes as f64 / MIB,
    );
    println!("objects at the end: {objects} for {} segment rows", end.0);
    println!(
        "warn/error events while running: {} total: {:?}",
        problems.len(),
        problems.iter().take(10).collect::<Vec<_>>()
    );
    println!("assertion failures: {}", failures.len());
    for x in &failures {
        println!("  FAIL {x}");
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
