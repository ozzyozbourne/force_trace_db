use std::process::{Command, Output};

fn forge(args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_forge"));
    for (k, _) in std::env::vars().filter(|(k, _)| k.starts_with("FORGE_")) {
        cmd.env_remove(k);
    }
    cmd.args(args).output().expect("run forge")
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
    let stubs: [&[&str]; 5] = [
        &["sql", "select 1"],
        &["inspect", "segment", "x.vortex"],
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
