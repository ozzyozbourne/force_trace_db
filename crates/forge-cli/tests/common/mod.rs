//! Shared harness for the real-binary tests: spawn `forge serve`, read its JSON logs, and read the committed state
//! offline (metastore + segments) after the server has exited.
#![allow(dead_code)]

use forge_ingest::normalize::pricing::Pricing;
use forge_ingest::normalize::{NormalizeConfig, normalize_request};
use forge_meta::{Metastore, SegmentFilter, SqliteMetastore};
use forge_model::catalog::SegmentMeta;
use forge_model::config::StorageConfig;
use forge_model::ids::{ProjectId, SpanId, TraceId};
use forge_model::merge::{Tombstones, merge_versions};
use forge_model::row::{SpanBatchBuilder, SpanRow};
use forge_model::testing::{reference_merge, rows_from_batch};
use forge_storage::inspect::{InspectOptions, inspect_segment};
use forge_storage::{RowSelection, ScanRequest, SegmentStore, VortexSegmentStore, build_object_store};
use futures::TryStreamExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// The `forge` binary with every inherited `FORGE_*` variable removed, so the caller's env never leaks in.
pub fn forge_cmd() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_forge"));
    for (k, _) in std::env::vars().filter(|(k, _)| k.starts_with("FORGE_")) {
        cmd.env_remove(k);
    }
    cmd
}

#[derive(Debug, Clone, Copy)]
pub struct Ports {
    pub ops: SocketAddr,
    pub api: SocketAddr,
    pub otlp_http: SocketAddr,
    pub otlp_grpc: SocketAddr,
}

pub struct ServerProc {
    pub child: Child,
    logs: Arc<Mutex<Vec<String>>>,
    /// Drains stderr; finishes at EOF, i.e. when the child exits.
    reader: Option<std::thread::JoinHandle<()>>,
}

impl ServerProc {
    pub fn spawn(dir: &Path, cfg: Option<&Path>, env: &[(&str, &str)]) -> Self {
        let mut cmd = forge_cmd();
        cmd.arg("serve");
        if let Some(cfg) = cfg {
            cmd.arg("--config").arg(cfg);
        }
        cmd.current_dir(dir)
            .env("FORGE_LOG_FORMAT", "json")
            .env("RUST_LOG", "info");
        cmd.envs(env.iter().copied());
        let mut child = cmd
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn forge");
        let logs = Arc::new(Mutex::new(Vec::new()));
        let (sink, stderr) = (logs.clone(), child.stderr.take().expect("piped stderr"));
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                sink.lock().unwrap_or_else(|e| e.into_inner()).push(line);
            }
        });
        Self {
            child,
            logs,
            reader: Some(reader),
        }
    }

    pub fn lines(&self) -> MutexGuard<'_, Vec<String>> {
        self.logs.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn logs(&self) -> String {
        self.lines().join("\n")
    }

    /// Calls `f` every 50 ms until it returns Some; panics with `what` and the server logs after `within`.
    /// With `alive`, also panics as soon as the server exits.
    pub async fn poll<T>(
        &mut self,
        within: Duration,
        what: &str,
        alive: bool,
        mut f: impl AsyncFnMut(&mut Self) -> Option<T>,
    ) -> T {
        let start = Instant::now();
        loop {
            if let Some(v) = f(self).await {
                return v;
            }
            if alive && let Some(status) = self.child.try_wait().expect("try_wait") {
                panic!("forge exited early: {status}\n--- server stderr ---\n{}", self.logs());
            }
            assert!(start.elapsed() < within, "{what} within {within:?}\n{}", self.logs());
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Bound addresses from the `listening` log events (all configured addrs are port 0).
    pub async fn ports(&mut self) -> Ports {
        // Lines already parsed are skipped on later polls.
        let (mut found, mut seen) = (HashMap::new(), 0);
        self.poll(
            Duration::from_secs(30),
            "no listening lines",
            true,
            async |s: &mut Self| {
                let lines = s.lines();
                for line in &lines[seen..] {
                    let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                        continue;
                    };
                    let f = &v["fields"];
                    if f["message"] == "listening"
                        && let (Some(name), Some(addr)) = (f["name"].as_str(), f["addr"].as_str())
                    {
                        found.insert(name.to_string(), addr.parse::<SocketAddr>().expect("listening addr"));
                    }
                }
                seen = lines.len();
                drop(lines);
                let get = |n: &str| found.get(n).copied();
                Some(Ports {
                    ops: get("ops")?,
                    api: get("api")?,
                    otlp_http: get("otlp-http")?,
                    otlp_grpc: get("otlp-grpc")?,
                })
            },
        )
        .await
    }

    pub async fn wait_healthy(&mut self, ops: SocketAddr) {
        let c = reqwest::Client::new();
        let url = format!("http://{ops}/healthz");
        let body = self
            .poll(
                Duration::from_secs(30),
                "/healthz never 200",
                true,
                async |_: &mut Self| {
                    let r = c.get(&url).send().await.ok().filter(|r| r.status() == 200)?;
                    Some(r.text().await.expect("body"))
                },
            )
            .await;
        assert_eq!(body, "ok");
    }

    pub fn signal(&self, sig: &str) {
        let st = Command::new("kill")
            .args([&format!("-{sig}"), &self.child.id().to_string()])
            .status()
            .expect("run kill");
        assert!(st.success(), "kill -{sig} failed: {st}");
    }

    pub fn sigterm(&self) {
        self.signal("TERM");
    }

    pub async fn wait_exit(&mut self, within: Duration) -> ExitStatus {
        let st = self
            .poll(within, "forge did not exit", false, async |s: &mut Self| {
                s.child.try_wait().expect("try_wait")
            })
            .await;
        if let Some(r) = self.reader.take() {
            tokio::task::spawn_blocking(move || r.join())
                .await
                .expect("join")
                .expect("stderr reader");
        }
        st
    }

    pub async fn wait_for_log(&mut self, needle: &str, within: Duration) {
        let what = format!("log {needle:?} not seen");
        self.poll(within, &what, false, async |s: &mut Self| {
            s.lines().iter().any(|l| l.contains(needle)).then_some(())
        })
        .await;
    }
}

impl Drop for ServerProc {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

pub fn write_config(dir: &Path, grace: &str) -> PathBuf {
    let d = dir.display();
    let s = format!(
        r#"[storage]
url = "file://{d}/data"

[metastore]
url = "sqlite://{d}/meta.db"

[wal]
dir = "{d}/wal"

[cache]
disk_dir = "{d}/cache"

[server]
otlp_grpc_addr = "127.0.0.1:0"
otlp_http_addr = "127.0.0.1:0"
api_addr = "127.0.0.1:0"
admin_addr = "127.0.0.1:0"
shutdown_grace = "{grace}"
"#
    );
    let path = dir.join("forge.toml");
    std::fs::write(&path, s).expect("write config");
    path
}

/// Spawns `forge serve` and waits until all listeners are up and /healthz answers.
pub async fn spawn_ready(dir: &Path, cfg: Option<&Path>, env: &[(&str, &str)]) -> (ServerProc, Ports) {
    let mut s = ServerProc::spawn(dir, cfg, env);
    let p = s.ports().await;
    s.wait_healthy(p.ops).await;
    (s, p)
}

pub async fn start(dir: &Path, grace: &str) -> (ServerProc, Ports) {
    spawn_ready(dir, Some(&write_config(dir, grace)), &[]).await
}

/// First JSON log line whose `fields.message == message`; returns `fields[key]` (caller uses as_u64 / as_bool).
pub fn log_field(logs: &str, message: &str, key: &str) -> serde_json::Value {
    logs.lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .find(|v| v["fields"]["message"] == message)
        .map(|v| v["fields"][key].clone())
        .unwrap_or_else(|| panic!("no {message:?} log line\n{logs}"))
}

// ---------------------------------------------------------------------------------------------------------------
// Offline state
// ---------------------------------------------------------------------------------------------------------------

pub type Key = (String /*project*/, TraceId, SpanId);

pub fn key(r: &SpanRow) -> Key {
    (
        r.project_id.as_ref().expect("project").to_string(),
        r.trace_id,
        r.span_id,
    )
}

fn segment_store(dir: &Path) -> VortexSegmentStore {
    let url = format!("file://{}", dir.join("data").display());
    VortexSegmentStore::new(build_object_store(&StorageConfig { url }).expect("object store"))
}

async fn scan(store: &VortexSegmentStore, seg: &SegmentMeta, rows: RowSelection) -> Vec<SpanRow> {
    let req = ScanRequest {
        rows,
        ..Default::default()
    };
    let batches: Vec<_> = store
        .scan(seg, req)
        .await
        .expect("scan")
        .try_collect()
        .await
        .expect("scan stream");
    batches.iter().flat_map(|b| rows_from_batch(b).expect("rows")).collect()
}

/// Committed state, read offline: every visible segment, fully scanned (raw rows, not merged across segments).
/// Also checks
/// - E: each object exists with `byte_size == segments.byte_size`;
/// - T: for every (project, trace) present, `locate_trace` returns ranges that each scan to rows of exactly that
///   trace, and together cover all of that trace's rows in all segments.
pub async fn visible(dir: &Path) -> (Vec<SegmentMeta>, Vec<SpanRow>) {
    let meta = SqliteMetastore::open_path(&dir.join("meta.db")).expect("open meta.db");
    let store = segment_store(dir);
    let segs = meta
        .list_segments(&SegmentFilter::default())
        .await
        .expect("list_segments");
    let mut rows = Vec::new();
    let mut per_trace: BTreeMap<(String, TraceId), u64> = BTreeMap::new();
    for s in &segs {
        let len = std::fs::metadata(dir.join("data").join(&s.path))
            .unwrap_or_else(|e| panic!("E: segment object {} missing: {e}", s.path))
            .len();
        assert_eq!(len, s.byte_size, "E: {} size", s.path);
        let r = scan(&store, s, RowSelection::All).await;
        assert_eq!(r.len() as u64, s.row_count, "{} row_count", s.path);
        for x in &r {
            *per_trace.entry((key(x).0, x.trace_id)).or_default() += 1;
        }
        rows.extend(r);
    }
    for ((p, t), n) in &per_trace {
        let project = ProjectId::new(p.clone()).expect("project");
        let locs = meta.locate_trace(&project, *t).await.expect("locate_trace");
        let mut covered = 0;
        for l in &locs {
            let r = scan(&store, &l.segment, RowSelection::Range(l.rows())).await;
            assert_eq!(
                r.len() as u64,
                l.row_count,
                "T: range {:?} of {}",
                l.rows(),
                l.segment.path
            );
            assert!(
                r.iter().all(|x| x.trace_id == *t && key(x).0 == *p),
                "T: range {:?} of {} holds other traces",
                l.rows(),
                l.segment.path
            );
            covered += r.len() as u64;
        }
        assert_eq!(covered, *n, "T: trace_index covers {covered} of {n} rows of {p}/{t:?}");
    }
    (segs, rows)
}

fn build(rows: &[SpanRow]) -> arrow::record_batch::RecordBatch {
    let mut b = SpanBatchBuilder::with_capacity(rows.len());
    for r in rows {
        b.push(r).expect("push");
    }
    b.finish().expect("finish")
}

/// Per key: `merge_versions(Drop)` over all rows, version zeroed.
pub fn merged(rows: &[SpanRow]) -> BTreeMap<Key, SpanRow> {
    if rows.is_empty() {
        return BTreeMap::new();
    }
    let m = merge_versions(&[build(rows)], Tombstones::Drop).expect("merge");
    rows_from_batch(&m)
        .expect("rows")
        .into_iter()
        .map(|mut r| {
            r.version = 0;
            (key(&r), r)
        })
        .collect()
}

/// The expected merged rows of posts applied in order: versions stamped in post order, `reference_merge`,
/// tombstones dropped, version zeroed.
pub fn expected(posts: &[Vec<SpanRow>]) -> BTreeMap<Key, SpanRow> {
    let mut all: Vec<SpanRow> = posts.iter().flatten().cloned().collect();
    for (i, r) in all.iter_mut().enumerate() {
        r.version = i as u64 + 1;
    }
    reference_merge(&all)
        .into_iter()
        .filter(|r| !r.deleted)
        .map(|mut r| {
            r.version = 0;
            (key(&r), r)
        })
        .collect()
}

/// `normalize_request` with the server's defaults; every span must be accepted.
pub fn normalize(req: &ExportTraceServiceRequest, header_project: Option<&str>) -> Vec<SpanRow> {
    let cfg = NormalizeConfig {
        default_project: ProjectId::new("default").expect("project"),
        pricing: Arc::new(Pricing::builtin()),
    };
    let header = header_project.map(|p| ProjectId::new(p).expect("project"));
    let n = normalize_request(req.clone(), header.as_ref(), &cfg);
    assert_eq!(n.rejected, 0, "{:?}", n.first_error);
    n.rows
}

/// `inspect segment --verify` output for a committed segment.
pub async fn inspect(dir: &Path, seg: &SegmentMeta) -> String {
    let opts = InspectOptions {
        layout: false,
        verify: true,
    };
    inspect_segment(&dir.join("data").join(&seg.path), &opts)
        .await
        .expect("inspect")
}
