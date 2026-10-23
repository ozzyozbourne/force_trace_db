//! M1 EXIT TEST 2: crash safety. A real `forge serve` under load is killed with SIGKILL at each failpoint (and at
//! random times, and stopped with SIGTERM under load). After a restart: no acked span or field is lost, no span is
//! invented, and no `(key, version)` pair appears twice across visible segments.
#![allow(clippy::print_stderr)]

mod common;

use common::*;
use forge_meta::{Metastore, SqliteMetastore};
use forge_model::row::SpanRow;
use forge_model::testing::{RowKey, files_under, row_key};
use opentelemetry_proto::tonic::collector::trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, status::StatusCode};
use prost::Message;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const CLIENTS: u64 = 4;
const MAX_REQUESTS: u64 = 300;
const SPANS: u64 = 20;
const BASE_NS: u64 = 1_790_000_000_000_000_000;
/// Every case (and every random-SIGKILL round) must ack at least this many load posts.
const MIN_ACKED: usize = CLIENTS as usize * 2;
/// `Post::round` of the single post `restart_and_post` makes.
const POST_RESTART_ROUND: u64 = 99;

// ---------------------------------------------------------------------------------------------------------------
// Load
// ---------------------------------------------------------------------------------------------------------------

fn kv(k: &str, v: any_value::Value) -> KeyValue {
    KeyValue {
        key: k.into(),
        value: Some(AnyValue { value: Some(v) }),
        ..Default::default()
    }
}
fn s(v: impl Into<String>) -> any_value::Value {
    any_value::Value::StringValue(v.into())
}

fn trace_id(round: u64, client: u64, req: u64) -> Vec<u8> {
    let mut t = vec![0xC0 | round as u8, client as u8 + 1];
    t.extend_from_slice(&[0; 10]);
    t.extend_from_slice(&(req as u32 + 1).to_be_bytes());
    t
}
fn span_id(client: u64, req: u64, i: u64) -> Vec<u8> {
    ((client + 1) << 48 | (req + 1) << 16 | (i + 1)).to_be_bytes().to_vec()
}

/// 20 spans of trace (round, client, req): a small tree with LLM spans and ~600 B payloads. `update` re-posts them
/// as end posts: end time, status OK and `forge.test.updated=true`, without the payloads, and with new output
/// tokens on the LLM spans (a column null on the other spans and non-null in both posts, so check O tells
/// "newest non-null wins" from "oldest non-null wins").
fn request(round: u64, client: u64, req: u64, update: bool) -> ExportTraceServiceRequest {
    let spans = (0..SPANS)
        .map(|i| {
            let start = BASE_NS + req * 1_000_000_000 + i * 1_000;
            let mut attributes = vec![kv("forge.test.client", s(format!("{client}")))];
            let llm = i % 4 == 1;
            if update {
                attributes.push(kv("forge.test.updated", any_value::Value::BoolValue(true)));
                if llm {
                    attributes.push(kv(
                        "gen_ai.usage.output_tokens",
                        any_value::Value::IntValue(1_000 + req as i64),
                    ));
                }
            } else {
                if llm {
                    attributes.extend([
                        kv("gen_ai.operation.name", s("chat")),
                        kv("gen_ai.request.model", s("gpt-4o")),
                        kv("gen_ai.usage.input_tokens", any_value::Value::IntValue(100 + i as i64)),
                        kv(
                            "gen_ai.usage.output_tokens",
                            any_value::Value::IntValue(20 + req as i64),
                        ),
                    ]);
                }
                attributes.push(kv("input.value", s(format!("question {req}/{i}: {}", "x".repeat(600)))));
                attributes.push(kv("output.value", s(format!("answer {req}/{i}"))));
            }
            Span {
                trace_id: trace_id(round, client, req),
                span_id: span_id(client, req, i),
                parent_span_id: if i == 0 {
                    vec![]
                } else {
                    span_id(client, req, (i - 1) / 2)
                },
                name: format!("op-{i}"),
                kind: 1,
                start_time_unix_nano: start,
                end_time_unix_nano: if update { start + 1_000_000 } else { 0 },
                attributes,
                status: update.then(|| Status {
                    code: StatusCode::Ok as i32,
                    message: String::new(),
                }),
                ..Default::default()
            }
        })
        .collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[derive(Clone, Debug)]
struct Post {
    project: String,
    req: ExportTraceServiceRequest,
    /// 200 without partial_success. Anything else (error status, transport error, timeout) is in doubt.
    acked: bool,
    round: u64,
}

type Ledger = Arc<Mutex<Vec<Post>>>;

/// One client: sequential OTLP/HTTP protobuf posts; stops at the first non-OK answer or after MAX_REQUESTS.
async fn client(ports: Ports, round: u64, c: u64, ledger: Ledger) {
    let client = http_client();
    let project = format!("p{}", c % 2);
    for r in 0..MAX_REQUESTS {
        let update = r % 4 == 3;
        let req = request(round, c, if update { r - 1 } else { r }, update);
        let body = req.encode_to_vec();
        let idx = {
            let mut l = ledger.lock().unwrap();
            l.push(Post {
                project: project.clone(),
                req,
                acked: false,
                round,
            });
            l.len() - 1
        };
        let resp = post_otlp(&client, &ports, "application/x-protobuf", None, Some(&project), body).await;
        let ok = match resp {
            Ok(r) if r.status() == 200 => match r.bytes().await {
                Ok(b) => ExportTraceServiceResponse::decode(b).is_ok_and(|x| x.partial_success.is_none()),
                Err(_) => false,
            },
            _ => false,
        };
        if !ok {
            return;
        }
        ledger.lock().unwrap()[idx].acked = true;
    }
}

/// Starts CLIENTS clients and returns after `cut` (the clients keep running in the background and stop when the
/// server goes away).
async fn load(ports: Ports, round: u64, ledger: &Ledger, cut: Duration) -> Vec<tokio::task::JoinHandle<()>> {
    let tasks: Vec<_> = (0..CLIENTS)
        .map(|c| tokio::spawn(client(ports, round, c, ledger.clone())))
        .collect();
    tokio::time::sleep(cut).await;
    tasks
}

async fn join(tasks: Vec<tokio::task::JoinHandle<()>>) {
    for t in tasks {
        tokio::time::timeout(Duration::from_secs(30), t)
            .await
            .expect("client stops")
            .expect("client task");
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Server lifecycle
// ---------------------------------------------------------------------------------------------------------------

fn env<'a>(failpoint: Option<&'a str>, interval: &'a str, extra: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
    let mut e = vec![
        ("FORGE_WAL__SEGMENT_BYTES", "256KiB"),
        ("FORGE_MEMTABLE__FLUSH_INTERVAL", interval),
    ];
    if let Some(fp) = failpoint {
        e.push(("FORGE_FAILPOINT", fp));
    }
    e.extend_from_slice(extra);
    e
}

fn assert_killed(st: std::process::ExitStatus, logs: &str, failpoint: Option<&str>) {
    assert_eq!(st.signal(), Some(9), "expected SIGKILL, got {st}\n{logs}");
    if let Some(fp) = failpoint {
        let name = fp.split('@').next().expect("name");
        assert!(logs.contains(&format!("FORGE_FAILPOINT {name} hit")), "{logs}");
    }
}

#[derive(Debug, Clone, Copy)]
struct Replay {
    applied: u64,
    skipped: u64,
    truncated: bool,
}

/// Restart without failpoints, read the replay line, post one `post-restart` span, SIGTERM, expect exit 0.
async fn restart_and_post(dir: &Path, ledger: &Ledger) -> Replay {
    let cfg = write_config(dir, "5s");
    let (mut s, p) = spawn_ready(dir, Some(&cfg), &env(None, "300ms", &[])).await;
    let logs = s.logs();
    let f = |k| log_field(&logs, "wal replay done", k);
    let replay = Replay {
        applied: f("applied").as_u64().expect("applied"),
        skipped: f("skipped").as_u64().expect("skipped"),
        truncated: f("truncated").as_bool().expect("truncated"),
    };
    let mut req = request(9, 9, 0, false);
    req.resource_spans[0].scope_spans[0].spans.truncate(1);
    let r = post_otlp(
        &reqwest::Client::new(),
        &p,
        "application/x-protobuf",
        None,
        Some("post-restart"),
        req.encode_to_vec(),
    )
    .await
    .expect("post-restart post");
    assert_eq!(r.status(), 200, "{}", s.logs());
    ledger.lock().unwrap().push(Post {
        project: "post-restart".into(),
        req,
        acked: true,
        round: POST_RESTART_ROUND,
    });
    s.stop_ok(Duration::from_secs(20)).await;
    replay
}

// ---------------------------------------------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum Orphans {
    Zero,
    AtLeastOne,
    Any,
}

/// Checks A–O (see the table in the plan) against the committed state of `dir`. `ctx` goes into every message.
async fn verify(dir: &Path, ledger: &Ledger, orphans: Orphans, ctx: &str) -> usize {
    let posts = ledger.lock().unwrap().clone();
    let (segs, rows) = visible(dir).await; // E, T

    // D
    for seg in &segs {
        let out = inspect(dir, seg).await;
        assert!(out.contains("contract:  OK"), "{ctx} D: {out}");
    }
    // C
    let mut kv = HashSet::new();
    for r in &rows {
        assert!(
            kv.insert((row_key(r), r.version)),
            "{ctx} C: duplicate {:?} v{}",
            row_key(r),
            r.version
        );
    }

    // Normalized rows per post, then per key: acked posts in order + the in-doubt post (at most one per key).
    // Per key: (post index, that post's rows of the key), in post order.
    let mut by_post: HashMap<RowKey, Vec<(usize, Vec<SpanRow>)>> = HashMap::new();
    for (i, p) in posts.iter().enumerate() {
        for r in normalize(&p.req, Some(&p.project)) {
            let v = by_post.entry(row_key(&r)).or_default();
            match v.last_mut() {
                Some((j, rs)) if *j == i => rs.push(r),
                _ => v.push((i, vec![r])),
            }
        }
    }
    let mut acked: BTreeMap<RowKey, Vec<usize>> = BTreeMap::new();
    let mut doubt: BTreeMap<RowKey, Vec<usize>> = BTreeMap::new();
    for (k, v) in &by_post {
        for (i, _) in v {
            if posts[*i].acked { &mut acked } else { &mut doubt }
                .entry(k.clone())
                .or_default()
                .push(*i);
        }
    }
    let got = merged(&rows);

    // A
    let lost: Vec<_> = acked.keys().filter(|k| !got.contains_key(*k)).collect();
    assert!(
        lost.is_empty(),
        "{ctx} A: {} acked keys lost (rounds {:?}), e.g. {:?}",
        lost.len(),
        lost.iter().map(|k| posts[acked[*k][0]].round).collect::<BTreeSet<_>>(),
        lost.first()
    );
    // B
    let invented: Vec<_> = got
        .keys()
        .filter(|k| !acked.contains_key(*k) && !doubt.contains_key(*k))
        .collect();
    assert!(invented.is_empty(), "{ctx} B: invented {invented:?}");
    // F
    let post_restart: Vec<_> = rows.iter().filter(|r| row_key(r).0 == "post-restart").collect();
    if let Some(pr) = post_restart.iter().map(|r| r.version).max() {
        let others = rows
            .iter()
            .filter(|r| row_key(r).0 != "post-restart")
            .map(|r| r.version)
            .max()
            .unwrap_or(0);
        assert!(pr > others, "{ctx} F: post-restart v{pr} <= v{others}");
    }
    // O: every field.
    for (k, row) in &got {
        let posts_of = |with_doubt: bool| -> Vec<Vec<SpanRow>> {
            by_post[k]
                .iter()
                .filter(|(i, _)| with_doubt || posts[*i].acked)
                .map(|(_, rs)| rs.clone())
                .collect()
        };
        let without = expected(&posts_of(false));
        let with = if doubt.contains_key(k) {
            expected(&posts_of(true))
        } else {
            without.clone()
        };
        assert!(
            without.get(k) == Some(row) || with.get(k) == Some(row),
            "{ctx} O: {k:?}\n got     {row:?}\n acked   {:?}\n +doubt  {:?}",
            without.get(k),
            with.get(k)
        );
    }

    // G: the WAL is fully flushed.
    let meta = SqliteMetastore::open_path(&dir.join("meta.db")).expect("meta");
    let cp = meta
        .wal_checkpoint("node-1")
        .await
        .expect("checkpoint")
        .expect("a checkpoint");
    let wal_dir = dir.join("wal/node-1");
    let st = forge_ingest::wal::replay(&wal_dir, cp.flushed_seq, |s, _| Err(format!("unflushed {s}")))
        .unwrap_or_else(|e| panic!("{ctx} G: {e}"));
    assert_eq!(st.applied, 0, "{ctx} G");
    let wal_files = forge_ingest::wal::list_segments(&wal_dir).expect("list wal").len();
    assert!(wal_files <= 2, "{ctx} G: {wal_files} WAL files left");

    // H: orphan objects (on disk, not visible). `*.vortex#N` staging files are reported, not counted.
    let visible_paths: HashSet<_> = segs.iter().map(|s| dir.join("data").join(&s.path)).collect();
    let (mut on_disk, mut staging) = (Vec::new(), Vec::new());
    for p in files_under(&dir.join("data/segments")) {
        let name = p.to_string_lossy();
        if name.ends_with(".vortex") {
            on_disk.push(p);
        } else if name.contains(".vortex#") {
            staging.push(p);
        }
    }
    let n = on_disk.iter().filter(|p| !visible_paths.contains(*p)).count();
    if !staging.is_empty() {
        eprintln!("{ctx}: {} staging files: {staging:?}", staging.len());
    }
    match orphans {
        Orphans::Zero => assert_eq!(n, 0, "{ctx} H"),
        Orphans::AtLeastOne => assert!(n >= 1, "{ctx} H: expected an orphan"),
        Orphans::Any => {}
    }
    // Not vacuous: if every post were rejected (429/4xx/503), A would check nothing and B/O would accept anything.
    let acked_n = posts.iter().filter(|p| p.acked).count();
    let load_acked = posts
        .iter()
        .filter(|p| p.acked && p.round != POST_RESTART_ROUND)
        .count();
    assert!(load_acked >= MIN_ACKED, "{ctx}: only {load_acked} load posts acked");
    eprintln!(
        "{ctx}: {} posts ({acked_n} acked), {} segments, {} rows, {n} orphans",
        posts.len(),
        segs.len(),
        rows.len()
    );
    got.len()
}

// ---------------------------------------------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------------------------------------------

/// A fresh data dir with its config (`grace`) and an empty ledger.
struct Case {
    tmp: tempfile::TempDir,
    cfg: PathBuf,
    ledger: Ledger,
}

impl Case {
    fn new(grace: &str) -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg = write_config(tmp.path(), grace);
        Self {
            tmp,
            cfg,
            ledger: Ledger::default(),
        }
    }

    fn dir(&self) -> &Path {
        self.tmp.path()
    }

    /// Load until the failpoint SIGKILLs the server.
    async fn kill_under_load(&self, fp: &str, interval: &str) {
        let (mut s, p) = spawn_ready(self.dir(), Some(&self.cfg), &env(Some(fp), interval, &[])).await;
        let tasks = load(p, 0, &self.ledger, Duration::ZERO).await;
        let st = s.wait_exit(Duration::from_secs(20)).await;
        assert_killed(st, &s.logs(), Some(fp));
        join(tasks).await;
    }

    /// 300 ms of load, then SIGTERM: exit 0 with `log` in the logs.
    async fn sigterm_under_load(&self, interval: &str, log: &str) {
        let (mut s, p) = spawn_ready(self.dir(), Some(&self.cfg), &env(None, interval, &[])).await;
        let tasks = load(p, 0, &self.ledger, Duration::from_millis(300)).await;
        let logs = s.stop_ok(Duration::from_secs(20)).await;
        assert!(logs.contains(log), "{logs}");
        join(tasks).await;
    }
}

/// Load until the failpoint SIGKILLs the server, restart, verify.
async fn crash_case(fp: &str, interval: &str, orphans: Orphans) -> Replay {
    let c = Case::new("5s");
    c.kill_under_load(fp, interval).await;
    let replay = restart_and_post(c.dir(), &c.ledger).await;
    let _ = verify(c.dir(), &c.ledger, orphans, &format!("{fp} {replay:?}")).await;
    replay
}

/// `verify`, then `forge sql --data-dir` counts exactly the merged visible keys, in-doubt
/// posts included.
async fn verify_with_sql(dir: &Path, ledger: &Ledger, orphans: Orphans, ctx: &str) {
    let n = verify(dir, ledger, orphans, ctx).await;
    let o = common::forge_output(&[
        "sql",
        "--data-dir",
        dir.to_str().unwrap(),
        "--format",
        "json",
        "SELECT count(*) AS n FROM spans",
    ]);
    assert!(o.status.success(), "{ctx}: {o:?}");
    assert_eq!(common::sql_count(&o), n as u64, "{ctx}: forge sql count");
}

async fn timed<T>(f: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(120), f)
        .await
        .expect("case timed out")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_wal_torn_write() {
    let r = timed(crash_case("wal_torn_write@20", "1h", Orphans::Zero)).await;
    assert!(r.truncated, "{r:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_wal_lose_unsynced() {
    timed(crash_case("wal_lose_unsynced@20", "1h", Orphans::Zero)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_wal_before_fsync() {
    timed(crash_case("wal_before_fsync@20", "1h", Orphans::Zero)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_wal_after_fsync() {
    let r = timed(crash_case("wal_after_fsync@20", "1h", Orphans::Zero)).await;
    assert!(r.applied > 0, "{r:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_flush_before_upload() {
    timed(crash_case("flush_before_upload@1", "300ms", Orphans::Zero)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_upload_before_fsync() {
    timed(crash_case("upload_before_fsync@1", "300ms", Orphans::AtLeastOne)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_upload_lose_unsynced() {
    timed(crash_case("upload_lose_unsynced@1", "300ms", Orphans::Zero)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_upload() {
    timed(crash_case("flush_after_upload@1", "300ms", Orphans::AtLeastOne)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_in_commit_txn() {
    let r = timed(crash_case("meta_commit_in_txn@1", "300ms", Orphans::AtLeastOne)).await;
    assert_eq!(r.skipped, 0, "the first commit rolled back: {r:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_commit() {
    let r = timed(crash_case("flush_after_commit@1", "300ms", Orphans::Zero)).await;
    assert!(r.skipped > 0, "{r:?}");
}

/// Crash, then crash again inside a replay-time flush, then converge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_replay_flush() {
    timed(async {
        let c = Case::new("5s");
        c.kill_under_load("wal_after_fsync@40", "1h").await;

        let fp = "flush_before_upload@1";
        let mut s = ServerProc::spawn(
            c.dir(),
            Some(&c.cfg),
            &env(Some(fp), "1h", &[("FORGE_MEMTABLE__FLUSH_BYTES", "512KiB")]),
        );
        let st = s.wait_exit(Duration::from_secs(20)).await;
        let logs = s.logs();
        assert_killed(st, &logs, Some(fp));
        assert!(!logs.contains("wal replay done"), "died inside replay: {logs}");
        drop(s);

        let replay = restart_and_post(c.dir(), &c.ledger).await;
        let _ = verify(c.dir(), &c.ledger, Orphans::Any, &format!("replay flush {replay:?}")).await;
    })
    .await;
}

/// Three rounds of load + SIGKILL at a random time on one dir; after each round a graceful restart must show
/// every key acked so far (A per round); after the last, A–O over the union.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_random_sigkill() {
    let seed: u64 = std::env::var("FORGE_CRASH_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos() as u64
        });
    eprintln!("crash_random_sigkill: FORGE_CRASH_SEED={seed}");
    let mut rng = forge_model::testing::Rng(seed | 1);
    timed(async {
        let c = Case::new("5s");
        let (dir, cfg) = (c.dir(), Some(c.cfg.as_path()));
        for round in 0..3 {
            let ms = 200 + rng.below(800);
            let (mut s, p) = spawn_ready(dir, cfg, &env(None, "300ms", &[])).await;
            let tasks = load(p, round, &c.ledger, Duration::from_millis(ms)).await;
            // Under a loaded machine the random cut can land before MIN_ACKED acks; extend it (bounded) so the
            // round still exercises a kill mid-load instead of flaking.
            let acked = || {
                let l = c.ledger.lock().unwrap();
                l.iter().filter(|p| p.acked && p.round == round).count()
            };
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while acked() < MIN_ACKED && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            s.signal("KILL");
            let st = s.wait_exit(Duration::from_secs(20)).await;
            assert_killed(st, &s.logs(), None);
            join(tasks).await;
            drop(s);
            let ctx = format!("seed {seed} round {round} (kill after {ms} ms)");
            let round_acked = c
                .ledger
                .lock()
                .unwrap()
                .iter()
                .filter(|p| p.acked && p.round == round)
                .count();
            assert!(
                round_acked >= MIN_ACKED,
                "{ctx}: only {round_acked} posts acked this round"
            );
            // A per round: a graceful start/stop flushes the replayed WAL.
            let (mut s, _) = spawn_ready(dir, cfg, &env(None, "300ms", &[])).await;
            s.stop_ok(Duration::from_secs(20)).await;
            drop(s);
            let _ = verify(dir, &c.ledger, Orphans::Any, &ctx).await;
        }
        let replay = restart_and_post(dir, &c.ledger).await;
        verify_with_sql(dir, &c.ledger, Orphans::Any, &format!("seed {seed} final {replay:?}")).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_stop_under_load() {
    timed(async {
        let c = Case::new("5s");
        c.sigterm_under_load("300ms", "final flush done").await;
        let replay = restart_and_post(c.dir(), &c.ledger).await;
        assert_eq!(replay.applied, 0, "{replay:?}");
        verify_with_sql(c.dir(), &c.ledger, Orphans::Zero, &format!("graceful {replay:?}")).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_flush_timeout_replays() {
    timed(async {
        let c = Case::new("1ms");
        c.sigterm_under_load("1h", "final flush timed out").await;
        let replay = restart_and_post(c.dir(), &c.ledger).await; // rewrites the config with a 5 s grace
        assert!(replay.applied > 0, "{replay:?}");
        let _ = verify(
            c.dir(),
            &c.ledger,
            Orphans::Any,
            &format!("final flush timeout {replay:?}"),
        )
        .await;
    })
    .await;
}
