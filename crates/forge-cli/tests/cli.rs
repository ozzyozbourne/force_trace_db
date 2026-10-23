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
