mod common;

use std::process::Output;

fn forge(args: &[&str]) -> Output {
    common::forge_cmd().args(args).output().expect("run forge")
}

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
    let stubs: [&[&str]; 4] = [
        &["sql", "select 1"],
        &["compact", "--project", "p"],
        &["gc"],
        &["bench", "ingest"],
    ];
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

/// One merged L0 segment of random rows under `dir/data`; returns its path.
async fn write_segment(dir: &std::path::Path) -> std::path::PathBuf {
    use forge_model::catalog::{HourBucket, Level, SegmentId};
    use forge_model::merge::{Tombstones, merge_versions};
    use forge_model::row::SpanBatchBuilder;
    use forge_model::testing::{Rng, pid, random_row};
    use forge_storage::{SegmentSpec, SegmentStore, VortexSegmentStore, build_object_store};
    use futures::StreamExt;
    let mut r = Rng(5);
    let mut b = SpanBatchBuilder::with_capacity(100);
    for v in 1..=100u64 {
        b.push(&random_row(&mut r, "p", v % 7, v % 13, v)).unwrap();
    }
    let merged = merge_versions(&[b.finish().unwrap()], Tombstones::Keep).unwrap();
    let url = format!("file://{}", dir.join("data").display());
    let store = VortexSegmentStore::new(build_object_store(&forge_model::config::StorageConfig { url }).unwrap());
    let spec = SegmentSpec {
        id: SegmentId::new(),
        project_id: pid("p"),
        level: Level::L0,
        partition_hour: HourBucket(0),
    };
    let w = store
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
