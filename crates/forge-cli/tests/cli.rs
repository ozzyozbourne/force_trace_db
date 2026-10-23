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
fn stubs_fail_with_not_implemented() {
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
        let rows: Vec<_> = (0..5).map(|s| random_row(&mut r, "p", s % 2, s, f * 10 + s)).collect();
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

#[tokio::test]
async fn compact_gc() {
    let d = data_dir_with_l0s().await;
    let dir = d.path().to_str().unwrap();
    let o = forge(&["compact", "--project", "nope", "--data-dir", dir]);
    assert_eq!(o.status.code(), Some(1), "{o:?}");
    assert!(String::from_utf8_lossy(&o.stderr).contains("no project"), "{o:?}");
    let o = forge(&["compact", "--project", "p", "--data-dir", dir]);
    assert!(o.status.success(), "{o:?}");
    let out = String::from_utf8_lossy(&o.stdout);
    assert!(out.contains("3 L0 + 0 L1") && out.contains("done: 1 jobs"), "{out}");
    let o = forge(&["compact", "--project", "p", "--data-dir", dir, "--format", "json"]);
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["jobs"].as_array().unwrap().len(), 0, "{v}");
    let o = forge(&[
        "gc",
        "--data-dir",
        dir,
        "--gc-grace",
        "0s",
        "--orphan-grace",
        "0s",
        "--format",
        "json",
    ]);
    assert!(o.status.success(), "{o:?}");
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["purged_segments"], 3, "{v}");
    assert!(v["blobs_skipped"].is_null(), "{v}");
    let o = forge(&["gc", "--data-dir", dir, "--gc-grace", "bogus"]);
    assert_eq!(o.status.code(), Some(2), "{o:?}");
}
