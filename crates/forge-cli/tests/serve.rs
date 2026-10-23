//! M0 exit test: spawn the real `forge serve` binary, talk to all four listeners, SIGTERM it.

mod common;

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use serde_json::json;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
struct Ports {
    ops: SocketAddr,
    api: SocketAddr,
    otlp_http: SocketAddr,
    otlp_grpc: SocketAddr,
}

struct ServerProc {
    child: Child,
    logs: Arc<Mutex<Vec<String>>>,
}

impl ServerProc {
    fn spawn(dir: &Path, cfg: Option<&Path>, env: &[(&str, &str)]) -> Self {
        let mut cmd = common::forge_cmd();
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
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                sink.lock().unwrap_or_else(|e| e.into_inner()).push(line);
            }
        });
        Self { child, logs }
    }

    fn lines(&self) -> MutexGuard<'_, Vec<String>> {
        self.logs.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn logs(&self) -> String {
        self.lines().join("\n")
    }

    /// Calls `f` every 50 ms until it returns Some; panics with `what` and the server logs after `within`.
    /// With `alive`, also panics as soon as the server exits.
    async fn poll<T>(
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
    async fn ports(&mut self) -> Ports {
        self.poll(
            Duration::from_secs(30),
            "no listening lines",
            true,
            async |s: &mut Self| {
                let mut found = HashMap::new();
                for line in s.lines().iter() {
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

    async fn wait_healthy(&mut self, ops: SocketAddr) {
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

    fn sigterm(&self) {
        let st = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("run kill");
        assert!(st.success(), "kill -TERM failed: {st}");
    }

    async fn wait_exit(&mut self, within: Duration) -> ExitStatus {
        let st = self
            .poll(within, "forge did not exit", false, async |s: &mut Self| {
                s.child.try_wait().expect("try_wait")
            })
            .await;
        // Let the stderr reader thread drain the last lines.
        tokio::time::sleep(Duration::from_millis(100)).await;
        st
    }

    async fn wait_for_log(&mut self, needle: &str, within: Duration) {
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

fn write_config(dir: &Path, grace: &str) -> PathBuf {
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
async fn spawn_ready(dir: &Path, cfg: Option<&Path>, env: &[(&str, &str)]) -> (ServerProc, Ports) {
    let mut s = ServerProc::spawn(dir, cfg, env);
    let p = s.ports().await;
    s.wait_healthy(p.ops).await;
    (s, p)
}

async fn start(dir: &Path, grace: &str) -> (ServerProc, Ports) {
    spawn_ready(dir, Some(&write_config(dir, grace)), &[]).await
}

async fn project_list(c: &reqwest::Client, api: SocketAddr) -> String {
    let r = c.get(format!("http://{api}/v1/projects")).send().await.expect("get");
    r.text().await.expect("body")
}

#[tokio::test]
async fn serve_starts_healthz_200_and_sigterm_exits_0() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (mut s, p) = start(tmp.path(), "2s").await;
    {
        let c = reqwest::Client::new();
        let r = c.get(format!("http://{}/readyz", p.ops)).send().await.expect("readyz");
        assert_eq!(r.status(), 200, "{}", s.logs());
        let r = c
            .get(format!("http://{}/metrics", p.ops))
            .send()
            .await
            .expect("metrics");
        assert_eq!(r.status(), 200, "{}", s.logs());

        let mut grpc = TraceServiceClient::connect(format!("http://{}", p.otlp_grpc))
            .await
            .expect("grpc connect");
        let err = grpc
            .export(ExportTraceServiceRequest::default())
            .await
            .expect_err("export must fail in M0");
        assert_eq!(err.code(), tonic::Code::Unimplemented, "{err:?}");

        let r = c
            .post(format!("http://{}/v1/traces", p.otlp_http))
            .send()
            .await
            .expect("otlp http");
        assert_eq!(r.status(), 501, "{}", s.logs());
    }
    assert!(
        s.child.try_wait().expect("try_wait").is_none(),
        "exited before SIGTERM\n{}",
        s.logs()
    );
    s.sigterm();
    let st = s.wait_exit(Duration::from_secs(10)).await;
    let logs = s.logs();
    assert!(st.success(), "{st}\n{logs}");
    assert!(logs.contains("shutdown signal received"), "{logs}");
    assert!(logs.contains("all listeners stopped"), "{logs}");
    assert!(!logs.contains("drain deadline exceeded"), "{logs}");
    assert!(
        !tmp.path().join("meta.db-wal").exists(),
        "metastore not closed cleanly\n{logs}"
    );
}

#[tokio::test]
async fn half_open_connection_hits_drain_deadline_and_still_exits_0() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (mut s, p) = start(tmp.path(), "2s").await;
    let _grpc = std::net::TcpStream::connect(p.otlp_grpc).expect("connect grpc");
    let mut api = std::net::TcpStream::connect(p.api).expect("connect api");
    api.write_all(b"GET /v1/projects HTTP/1.1\r\nHost: x\r\n")
        .expect("partial request");
    tokio::time::sleep(Duration::from_millis(200)).await;
    s.sigterm();
    let st = s.wait_exit(Duration::from_secs(7)).await;
    let logs = s.logs();
    assert!(st.success(), "{st}\n{logs}");
    assert!(logs.contains("drain deadline exceeded"), "{logs}");
}

#[tokio::test]
async fn second_signal_forces_exit() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (mut s, p) = start(tmp.path(), "60s").await;
    let _grpc = std::net::TcpStream::connect(p.otlp_grpc).expect("connect grpc");
    tokio::time::sleep(Duration::from_millis(200)).await;
    s.sigterm();
    s.wait_for_log("shutdown signal received", Duration::from_secs(5)).await;
    s.sigterm();
    let st = s.wait_exit(Duration::from_secs(5)).await;
    assert_eq!(st.code(), Some(130), "{st}\n{}", s.logs());
}

#[tokio::test]
async fn projects_persist_across_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let c = reqwest::Client::new();

    let (mut s, p) = start(tmp.path(), "2s").await;
    let url = format!("http://{}/v1/projects", p.api);
    for (body, want) in [
        (json!({"id": "demo"}), 201),
        (json!({"id": "demo"}), 409),
        (json!({"id": "bad/id"}), 400),
        (json!({"id": "z", "retention_days": 0}), 400),
    ] {
        let r = c.post(&url).json(&body).send().await.expect("post");
        assert_eq!(r.status(), want, "{body}");
    }
    let body = project_list(&c, p.api).await;
    assert!(body.contains("\"demo\""), "{body}");
    s.sigterm();
    assert!(s.wait_exit(Duration::from_secs(10)).await.success(), "{}", s.logs());
    drop(s);

    let (mut s, p) = start(tmp.path(), "2s").await;
    let body = project_list(&c, p.api).await;
    assert!(body.contains("\"demo\""), "{body}");
    s.sigterm();
    assert!(s.wait_exit(Duration::from_secs(10)).await.success(), "{}", s.logs());
}

#[tokio::test]
async fn serve_without_config_uses_relative_defaults() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let env = [
        ("FORGE_SERVER__ADMIN_ADDR", "127.0.0.1:0"),
        ("FORGE_SERVER__API_ADDR", "127.0.0.1:0"),
        ("FORGE_SERVER__OTLP_HTTP_ADDR", "127.0.0.1:0"),
        ("FORGE_SERVER__OTLP_GRPC_ADDR", "127.0.0.1:0"),
        ("FORGE_SERVER__SHUTDOWN_GRACE", "2s"),
    ];
    let (mut s, _) = spawn_ready(tmp.path(), None, &env).await;
    s.sigterm();
    assert!(s.wait_exit(Duration::from_secs(10)).await.success(), "{}", s.logs());
    assert!(tmp.path().join("forge-data/meta.db").is_file(), "{}", s.logs());
    assert!(tmp.path().join("forge-data/data").is_dir(), "{}", s.logs());
}

#[tokio::test]
async fn bad_config_fails_fast() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = tmp.path().join("forge.toml");
    std::fs::write(&cfg, "[cache]\nmemroy = \"1GiB\"\n").expect("write config");
    let mut s = ServerProc::spawn(tmp.path(), Some(&cfg), &[]);
    let st = s.wait_exit(Duration::from_secs(10)).await;
    let logs = s.logs();
    assert!(!st.success(), "{logs}");
    assert!(logs.contains("memroy"), "{logs}");
}
