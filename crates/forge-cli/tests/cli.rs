mod common;

use common::forge_output as forge;

#[test]
fn help_lists_subcommands() {
    let o = forge(&["--help"]);
    assert!(o.status.success(), "{o:?}");
    let s = String::from_utf8_lossy(&o.stdout);
    for c in ["serve", "sql", "inspect", "compact", "gc", "bench"] {
        assert!(s.contains(c), "{c} missing from help:\n{s}");
    }
}

#[test]
fn bench_is_a_stub() {
    let stubs: [&[&str]; 1] = [&["bench", "ingest"]];
    for a in stubs {
        let o = forge(a);
        assert!(!o.status.success(), "{a:?} succeeded");
        let e = String::from_utf8_lossy(&o.stderr);
        assert!(e.contains("not implemented yet"), "{a:?}: {e}");
    }
}

#[test]
fn sql_target_flags_conflict() {
    let o = forge(&["sql", "select 1", "--data-dir", "/d", "--server", "http://x"]);
    assert!(!o.status.success());
    let e = String::from_utf8_lossy(&o.stderr);
    assert!(e.contains("cannot be used with"), "{e}");
}

#[test]
fn sql_offline_needs_existing_data() {
    let o = forge(&["sql", "select 1", "--data-dir", "/nonexistent"]);
    assert!(!o.status.success());
    assert!(String::from_utf8_lossy(&o.stderr).contains("no Forge data"), "{o:?}");
    // Config mode: a missing file:// storage dir fails and is not created.
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("data");
    let o = common::forge_cmd()
        .args(["sql", "select 1"])
        .env("FORGE_STORAGE__URL", format!("file://{}", missing.display()))
        .env(
            "FORGE_METASTORE__URL",
            format!("sqlite://{}/meta.db", tmp.path().display()),
        )
        .output()
        .unwrap();
    assert!(!o.status.success());
    assert!(String::from_utf8_lossy(&o.stderr).contains("no Forge data"), "{o:?}");
    assert!(!missing.exists() && !tmp.path().join("meta.db").exists());
}

#[test]
fn sql_server_errors_and_formats() {
    let o = forge(&["sql", "select 1", "--server", "http://127.0.0.1:1"]);
    assert_eq!(o.status.code(), Some(1), "{o:?}");
    assert!(
        String::from_utf8_lossy(&o.stderr).contains("error: connecting to"),
        "{o:?}"
    );
    let o = forge(&["sql", "select 1", "--format", "csv"]);
    assert!(!o.status.success());
    assert!(
        String::from_utf8_lossy(&o.stderr).contains("invalid value 'csv'"),
        "{o:?}"
    );
}

/// One merged L0 segment of random rows under `dir/data`; returns its path.
async fn write_segment(dir: &std::path::Path) -> std::path::PathBuf {
    use forge_model::catalog::{HourBucket, Level, SegmentId};
    use forge_model::merge::{Tombstones, merge_versions};
    use forge_model::testing::{Rng, batch_from_rows, pid, random_row};
    use forge_storage::{SegmentSpec, SegmentStore};
    use futures::StreamExt;
    let mut r = Rng(5);
    let rows: Vec<_> = (1..=100u64)
        .map(|v| random_row(&mut r, "p", v % 7, v % 13, v))
        .collect();
    let merged = merge_versions(&[batch_from_rows(&rows)], Tombstones::Keep).unwrap();
    let spec = SegmentSpec {
        id: SegmentId::new(),
        project_id: pid("p"),
        level: Level::L0,
        partition_hour: HourBucket(0),
    };
    let w = common::segment_store(dir)
        .write_segment(spec, futures::stream::iter([Ok(merged)]).boxed())
        .await
        .unwrap();
    dir.join("data").join(&w.meta.path)
}

#[tokio::test]
async fn inspect_segment_prints_and_verifies() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_segment(tmp.path()).await;
    let p = path.to_str().unwrap();
    let o = forge(&["inspect", "segment", p, "--verify"]);
    assert!(o.status.success(), "{o:?}");
    let out = String::from_utf8_lossy(&o.stdout);
    assert!(out.contains("rows:") && out.contains("contract:  OK"), "{out}");
    assert!(!out.contains("layout:"), "{out}");
    let o = forge(&["inspect", "segment", p, "--layout"]);
    assert!(o.status.success(), "{o:?}");
    assert!(String::from_utf8_lossy(&o.stdout).contains("layout:"));
}

#[test]
fn inspect_missing_file_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("nope.vortex");
    let o = forge(&["inspect", "segment", path.to_str().unwrap()]);
    assert!(!o.status.success(), "{o:?}");
    let e = String::from_utf8_lossy(&o.stderr);
    assert!(e.contains(&format!("inspecting {}", path.display())), "{e}");
}

/// A data dir with 3 L0 segments of project `p` (flushes 1..=3).
async fn data_dir_with_l0s() -> tempfile::TempDir {
    use forge_meta::{FlushCommit, Metastore, NewSegment, SqliteMetastore};
    use forge_model::catalog::{HourBucket, Level, SegmentId};
    use forge_model::merge::{Tombstones, merge_versions};
    use forge_model::testing::{Rng, batch_from_rows, pid, random_row};
    use forge_storage::{SegmentSpec, SegmentStore};
    let d = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(d.path().join("data")).unwrap();
    let m = SqliteMetastore::open_path(&d.path().join("meta.db")).unwrap();
    let store = common::segment_store(d.path());
    let mut r = Rng(7);
    for f in 1..=3u64 {
        let rows: Vec<_> = (0..5)
            .map(|s| forge_model::row::SpanRow {
                start_time_ns: Some(1_700_000_000_000_000_000 + s as i64),
                deleted: false,
                ..random_row(&mut r, "p", s % 2, s, f * 10 + s)
            })
            .collect();
        let b = merge_versions(&[batch_from_rows(&rows)], Tombstones::Keep).unwrap();
        let spec = SegmentSpec {
            id: SegmentId::new(),
            project_id: pid("p"),
            level: Level::L0,
            partition_hour: HourBucket(472_222),
        };
        let w = store
            .write_segment(spec, Box::pin(futures::stream::iter([Ok(b)])))
            .await
            .unwrap();
        m.commit_flush(FlushCommit {
            node_id: "node-1".into(),
            segments: vec![NewSegment {
                meta: w.meta,
                traces: w.traces,
                blobs: vec![],
            }],
            flushed_wal_seq: f,
            max_version: 100,
        })
        .await
        .unwrap();
    }
    d
}

fn count(dir: &std::path::Path, sql: &str) -> i64 {
    let c = rusqlite::Connection::open(dir.join("meta.db")).unwrap();
    c.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn sql_n(dir: &str) -> u64 {
    common::sql_count(&forge(&[
        "sql",
        "SELECT count(*) AS n FROM spans",
        "--data-dir",
        dir,
        "--format",
        "json",
    ]))
}

/// Every file under `dir`, relative, sorted.
fn ls(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut v: Vec<_> = forge_model::testing::files_under(dir)
        .into_iter()
        .filter(|p| !p.to_string_lossy().contains("meta.db"))
        .collect();
    v.sort();
    v
}

#[tokio::test]
async fn compact_table_json_unknown_and_nothing_to_do() {
    let d = data_dir_with_l0s().await;
    let dir = d.path().to_str().unwrap();
    let n = sql_n(dir);
    let o = forge(&["compact", "--project", "nope", "--data-dir", dir]);
    assert_eq!(o.status.code(), Some(1), "{o:?}");
    assert!(String::from_utf8_lossy(&o.stderr).contains("no project"), "{o:?}");
    let o = forge(&["compact", "--project", "p", "--data-dir", dir]);
    assert!(o.status.success(), "{o:?}");
    let out = String::from_utf8_lossy(&o.stdout);
    assert!(
        out.contains("compacted p l0 job: 3 L0 + 0 L1 -> 1 L1 (hours 2023-11-14/22; 5 rows)"),
        "{out}"
    );
    assert!(out.contains("done: 1 jobs, 0 skipped, 0 failed"), "{out}");
    assert_eq!(
        count(
            d.path(),
            "SELECT count(*) FROM segments WHERE state <> 'deleted' AND level = 0"
        ),
        0
    );
    assert_eq!(
        count(
            d.path(),
            "SELECT count(*) FROM segments WHERE state <> 'deleted' AND level = 1"
        ),
        1
    );
    assert_eq!(sql_n(dir), n);
    let o = forge(&["compact", "--project", "p", "--data-dir", dir]);
    assert!(o.status.success(), "{o:?}");
    assert!(String::from_utf8_lossy(&o.stdout).contains("done: 0 jobs"), "{o:?}");
    // JSON on a fresh data dir.
    let d = data_dir_with_l0s().await;
    let o = forge(&[
        "compact",
        "--project",
        "p",
        "--data-dir",
        d.path().to_str().unwrap(),
        "--format",
        "json",
    ]);
    assert!(o.status.success(), "{o:?}");
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["jobs"].as_array().unwrap().len(), 1, "{v}");
    let j = &v["jobs"][0];
    assert_eq!(
        (j["kind"].as_str(), j["l0_inputs"].as_u64()),
        (Some("l0"), Some(3)),
        "{v}"
    );
    assert_eq!(j["outputs"].as_array().unwrap().len(), 1, "{v}");
}

#[tokio::test]
async fn compact_failed_job_exits_1() {
    let d = data_dir_with_l0s().await;
    let path: String = rusqlite::Connection::open(d.path().join("meta.db"))
        .unwrap()
        .query_row("SELECT path FROM segments LIMIT 1", [], |r| r.get(0))
        .unwrap();
    std::fs::write(d.path().join("data").join(path), b"not a vortex file").unwrap();
    let o = forge(&["compact", "--project", "p", "--data-dir", d.path().to_str().unwrap()]);
    assert_eq!(o.status.code(), Some(1), "{o:?}");
    let out = String::from_utf8_lossy(&o.stdout);
    assert!(out.contains("done: 0 jobs, 0 skipped, 3 failed"), "{out}");
    assert!(
        String::from_utf8_lossy(&o.stderr).contains("compaction job(s) failed"),
        "{o:?}"
    );
    assert_eq!(
        count(d.path(), "SELECT count(*) FROM segments WHERE state <> 'live'"),
        0
    );
}

#[test]
fn usage_errors_exit_2() {
    for a in [
        &["gc", "--data-dir", "/x", "--gc-grace", "bogus"][..],
        &["gc", "--orphan-grace", "1 parsec"],
        &["compact", "--data-dir", "/x"],
    ] {
        assert_eq!(forge(a).status.code(), Some(2), "{a:?}");
    }
}

/// Without --data-dir and with no data at the configured place: exit 1, nothing created (RA m8).
#[test]
fn offline_commands_need_existing_data() {
    for a in [&["compact", "--project", "p"][..], &["gc"]] {
        let tmp = tempfile::tempdir().unwrap();
        let o = common::forge_cmd().args(a).current_dir(tmp.path()).output().unwrap();
        assert_eq!(o.status.code(), Some(1), "{a:?}: {o:?}");
        assert!(String::from_utf8_lossy(&o.stderr).contains("no Forge data"), "{o:?}");
        assert!(
            std::fs::read_dir(tmp.path()).unwrap().next().is_none(),
            "{a:?} created files"
        );
    }
    let o = forge(&["gc", "--data-dir", "/nonexistent"]);
    assert_eq!(o.status.code(), Some(1), "{o:?}");
}

/// D12: compact and gc refuse while a server holds any node's WAL LOCK (exit 3).
#[tokio::test]
async fn refuses_while_a_server_runs() {
    for node in [None, Some("x")] {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = common::write_config(tmp.path(), "2s");
        let env: Vec<(&str, &str)> = node.map(|n| ("FORGE_NODE__ID", n)).into_iter().collect();
        let (mut s, _) = common::spawn_ready(tmp.path(), Some(&cfg), &env).await;
        let dir = tmp.path().to_str().unwrap();
        let lock = format!("wal/{}/LOCK", node.unwrap_or("node-1"));
        for a in [
            &["compact", "--project", "p", "--data-dir", dir][..],
            &["gc", "--data-dir", dir],
        ] {
            let o = forge(a);
            assert_eq!(o.status.code(), Some(3), "{a:?}: {o:?}");
            let e = String::from_utf8_lossy(&o.stderr);
            assert!(
                e.contains(&format!(
                    "a server is running on this data dir ({lock} is held); stop it first"
                )),
                "{e}"
            );
        }
        s.stop_ok(std::time::Duration::from_secs(10)).await;
        let o = forge(&["gc", "--data-dir", dir]);
        assert!(o.status.success(), "after the stop: {o:?}");
    }
}

/// gc --dry-run reports exactly the candidates and changes nothing; the real gc removes exactly those.
#[tokio::test]
async fn gc_dry_run_then_real() {
    use forge_meta::{FlushCommit, Metastore, NewSegment, SqliteMetastore};
    use forge_model::catalog::{HourBucket, Level, SegmentMeta, TraceRange};
    use forge_model::testing::{pid, segment_meta, tid};
    let d = tempfile::tempdir().unwrap();
    let data = d.path().join("data");
    let m = SqliteMetastore::open_path(&d.path().join("meta.db")).unwrap();
    let now_ns = forge_model::time::now_ms() * 1_000_000;
    let day = 86_400_000_000_000i64;
    let mk = |start: i64| {
        let meta = SegmentMeta {
            min_start_ns: Some(start),
            max_start_ns: Some(start),
            start_time_nulls: 0,
            ..segment_meta("p", Level::L0, HourBucket(start.div_euclid(3_600_000_000_000)))
        };
        let f = data.join(&meta.path);
        std::fs::create_dir_all(f.parent().unwrap()).unwrap();
        std::fs::write(f, b"x").unwrap();
        NewSegment {
            meta,
            traces: vec![TraceRange {
                trace_id: tid(1),
                row_start: 0,
                row_count: 1,
            }],
            blobs: vec![],
        }
    };
    let commit = |s: NewSegment, seq| FlushCommit {
        node_id: "node-1".into(),
        segments: vec![s],
        flushed_wal_seq: seq,
        max_version: seq,
    };
    let (x, y, z) = (mk(now_ns - 3 * day), mk(now_ns - 3 * day), mk(now_ns));
    m.commit_flush(commit(x.clone(), 1)).await.unwrap();
    m.set_retention_days(&pid("p"), Some(1)).await.unwrap();
    assert_eq!(m.expire_segments(&pid("p"), now_ns - day, 10).await.unwrap().len(), 1); // x: deleted
    m.commit_flush(commit(y.clone(), 2)).await.unwrap(); // y: expired candidate
    m.commit_flush(commit(z.clone(), 3)).await.unwrap(); // z: kept
    let orphan = data.join("segments/p/L0/2020-01-01/00/01JAAAAAAAAAAAAAAAAAAAAAAA.vortex");
    std::fs::create_dir_all(orphan.parent().unwrap()).unwrap();
    std::fs::write(&orphan, b"x").unwrap();
    drop(m);
    let dir = d.path().to_str().unwrap();
    let args = |dry: bool| {
        let mut a = vec![
            "gc",
            "--data-dir",
            dir,
            "--gc-grace",
            "0s",
            "--orphan-grace",
            "0s",
            "--format",
            "json",
        ];
        if dry {
            a.push("--dry-run");
        }
        a
    };
    let files = ls(&data);
    let rows = count(d.path(), "SELECT count(*) FROM segments");
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let o = forge(&args(true));
    assert!(o.status.success(), "{o:?}");
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    let got = |v: &serde_json::Value| {
        (
            v["expired_segments"].as_u64(),
            v["purged_segments"].as_u64(),
            v["orphan_segment_objects"].as_u64(),
        )
    };
    assert_eq!(got(&v), (Some(1), Some(1), Some(1)), "{v}");
    assert_eq!(ls(&data), files, "dry run changed objects");
    assert_eq!(count(d.path(), "SELECT count(*) FROM segments"), rows);
    assert_eq!(
        count(d.path(), "SELECT count(*) FROM segments WHERE state = 'deleted'"),
        1
    );
    let o = forge(&args(false));
    assert!(o.status.success(), "{o:?}");
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(got(&v), (Some(1), Some(1), Some(1)), "{v}");
    assert!(!data.join(&x.meta.path).exists() && !orphan.exists());
    assert!(data.join(&y.meta.path).exists() && data.join(&z.meta.path).exists());
    assert_eq!(count(d.path(), "SELECT count(*) FROM segments"), 2);
}

/// `--orphan-grace` (flag only: --data-dir ignores FORGE_* env).
#[tokio::test]
async fn orphan_grace_flag() {
    let d = data_dir_with_l0s().await;
    let dir = d.path().to_str().unwrap();
    let orphan = d
        .path()
        .join("data/segments/p/L0/2020-01-01/00/01JAAAAAAAAAAAAAAAAAAAAAAA.vortex");
    std::fs::create_dir_all(orphan.parent().unwrap()).unwrap();
    std::fs::write(&orphan, b"x").unwrap();
    assert!(forge(&["gc", "--data-dir", dir]).status.success());
    assert!(orphan.exists(), "kept with the default 24 h grace");
    let o = common::forge_cmd()
        .args(["gc", "--data-dir", dir])
        .env("FORGE_COMPACTION__ORPHAN_GRACE", "0s")
        .output()
        .unwrap();
    assert!(o.status.success(), "{o:?}");
    assert!(orphan.exists(), "env is ignored with --data-dir");
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(
        forge(&["gc", "--data-dir", dir, "--orphan-grace", "0s"])
            .status
            .success()
    );
    assert!(!orphan.exists(), "removed with --orphan-grace 0s");
}

/// D12: blobs are not swept while a WAL has unflushed records; after a clean start/stop they are.
#[tokio::test]
async fn gc_skips_blobs_with_unflushed_wal() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = common::write_config(tmp.path(), "5s");
    let env = [
        ("FORGE_MEMTABLE__FLUSH_INTERVAL", "1h"),
        ("FORGE_INGEST__BLOB_THRESHOLD", "100B"),
    ];
    let (mut s, p) = common::spawn_ready(tmp.path(), Some(&cfg), &env).await;
    let body = serde_json::json!({"resourceSpans": [{"resource": {"attributes": []}, "scopeSpans": [{"spans": [{
        "traceId": "0102030405060708090a0b0c0d0e0f10", "spanId": "0102030405060708", "name": "x",
        "startTimeUnixNano": "1790000000000000000", "endTimeUnixNano": "1790000000000001000",
        "attributes": [{"key": "big", "value": {"stringValue": "y".repeat(500)}}]}]}]}]});
    let r = common::post_otlp(
        &common::http_client(),
        &p,
        "application/json",
        None,
        Some("p"),
        serde_json::to_vec(&body).unwrap(),
    )
    .await
    .unwrap();
    assert!(r.status().is_success(), "{r:?}");
    s.signal("KILL");
    s.wait_exit(std::time::Duration::from_secs(10)).await;
    let blobs = tmp.path().join("data/blobs/p");
    let referenced: Vec<_> = std::fs::read_dir(&blobs).unwrap().map(|e| e.unwrap().path()).collect();
    assert_eq!(referenced.len(), 1, "the acked span's blob");
    let orphan = blobs.join("0".repeat(64));
    std::fs::write(&orphan, b"x").unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let dir = tmp.path().to_str().unwrap();
    let o = forge(&["gc", "--data-dir", dir, "--orphan-grace", "0s"]);
    assert!(o.status.success(), "{o:?}");
    let out = String::from_utf8_lossy(&o.stdout);
    assert!(
        out.contains("skipped: WAL of node-1 has 1 unflushed records: run forge serve once"),
        "{out}"
    );
    assert!(orphan.exists() && referenced[0].exists());
    let (mut s, _) = common::spawn_ready(tmp.path(), Some(&cfg), &env).await;
    s.stop_ok(std::time::Duration::from_secs(10)).await;
    let o = forge(&["gc", "--data-dir", dir, "--orphan-grace", "0s"]);
    assert!(o.status.success(), "{o:?}");
    assert!(!String::from_utf8_lossy(&o.stdout).contains("skipped"), "{o:?}");
    assert!(!orphan.exists(), "unreferenced blob swept");
    assert!(referenced[0].exists(), "the replayed row's blob is referenced");
}
