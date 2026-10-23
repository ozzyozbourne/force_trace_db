//! M0 exit test: spawn the real `forge serve` binary, talk to all four listeners, SIGTERM it.

mod common;

use common::*;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use serde_json::json;
use std::io::Write;
use std::net::SocketAddr;
use std::time::Duration;

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
        grpc.export(ExportTraceServiceRequest::default())
            .await
            .expect("empty export is OK");

        let r = c.post(p.traces_url()).send().await.expect("otlp http");
        assert_eq!(r.status(), 415, "{}", s.logs());
    }
    assert!(
        s.child.try_wait().expect("try_wait").is_none(),
        "exited before SIGTERM\n{}",
        s.logs()
    );
    let logs = s.stop_ok(Duration::from_secs(10)).await;
    assert!(logs.contains("shutdown signal received"), "{logs}");
    assert!(logs.contains("all listeners stopped"), "{logs}");
    assert!(!logs.contains("drain deadline exceeded"), "{logs}");
    assert!(logs.contains("final flush done"), "{logs}");
    assert!(logs.contains("wal replay done"), "{logs}");
    assert!(
        !tmp.path().join("meta.db-wal").exists(),
        "metastore not closed cleanly\n{logs}"
    );
}

#[tokio::test]
async fn half_open_connection_hits_drain_deadline_and_still_exits_0() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (mut s, p) = start(tmp.path(), "500ms").await;
    let _grpc = std::net::TcpStream::connect(p.otlp_grpc).expect("connect grpc");
    let mut api = std::net::TcpStream::connect(p.api).expect("connect api");
    api.write_all(b"GET /v1/projects HTTP/1.1\r\nHost: x\r\n")
        .expect("partial request");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let logs = s.stop_ok(Duration::from_secs(6)).await;
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
    s.stop_ok(Duration::from_secs(10)).await;
    drop(s);

    // Run 2 stops with Ctrl-C (SIGINT).
    let (mut s, p) = start(tmp.path(), "2s").await;
    let body = project_list(&c, p.api).await;
    assert!(body.contains("\"demo\""), "{body}");
    s.signal("INT");
    let st = s.wait_exit(Duration::from_secs(10)).await;
    let logs = s.logs();
    assert!(st.success(), "{st}\n{logs}");
    assert!(logs.contains("shutdown signal received"), "{logs}");
}

#[tokio::test]
async fn signal_during_startup_exits_0() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = write_config(tmp.path(), "2s");
    // Hold an exclusive lock on meta.db so startup blocks in the metastore open, after signal install.
    let lock = rusqlite::Connection::open(tmp.path().join("meta.db")).expect("open meta.db");
    lock.execute_batch("BEGIN EXCLUSIVE").expect("lock meta.db");
    let mut s = ServerProc::spawn(tmp.path(), Some(&cfg), &[]);
    s.wait_for_log("\"starting\"", Duration::from_secs(30)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!s.logs().contains("listening"), "startup was not blocked\n{}", s.logs());
    // Exits while startup is still blocked: the lock is held until after the exit.
    let logs = s.stop_ok(Duration::from_secs(15)).await;
    assert!(logs.contains("shutdown signal received during startup"), "{logs}");
    drop(lock);
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
    let logs = s.stop_ok(Duration::from_secs(10)).await;
    assert!(tmp.path().join("forge-data/meta.db").is_file(), "{logs}");
    assert!(tmp.path().join("forge-data/data").is_dir(), "{logs}");
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
