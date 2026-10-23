//! Kill switch for crash tests: `FORGE_FAILPOINT=<name>[@<n>]` SIGKILLs the process the n-th time (default 1st)
//! `hit(name)` runs. Every crash is `kill -9`: no destructors, no buffered writes, no core dump.
//! Unset = one LazyLock read per call; always compiled, so the crash tests exercise the real binary.
//!
//! Names: wal_torn_write, wal_lose_unsynced, wal_before_fsync, wal_after_fsync, flush_before_upload,
//! upload_before_fsync, upload_lose_unsynced, flush_after_upload, meta_commit_in_txn, flush_after_commit,
//! flush_after_truncate. Not a kill: flush_merge_panic panics inside the flush's merge task (`fires`).
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU32, Ordering};

struct Armed {
    name: String,
    nth: u32,
    hits: AtomicU32,
}

static ARMED: LazyLock<Option<Armed>> = LazyLock::new(|| {
    let v = std::env::var("FORGE_FAILPOINT").ok()?;
    let (name, nth) = match v.split_once('@') {
        Some((n, k)) => (n.to_string(), k.parse().ok()?),
        None => (v, 1),
    };
    Some(Armed {
        name,
        nth,
        hits: AtomicU32::new(0),
    })
});

/// True exactly once: on the n-th call for the armed `name`. For failpoints that must do something
/// (e.g. a partial write) before calling `kill`.
#[inline]
pub fn fires(name: &str) -> bool {
    ARMED
        .as_ref()
        .is_some_and(|a| a.name == name && a.hits.fetch_add(1, Ordering::Relaxed) + 1 == a.nth)
}

/// SIGKILLs the process on the n-th call for the armed `name`.
#[inline]
pub fn hit(name: &str) {
    if fires(name) {
        kill(name)
    }
}

/// Prints the marker the tests look for, then SIGKILLs this process. Other threads keep running until the
/// signal lands, as they would up to any real crash.
#[allow(clippy::print_stderr)] // the process is about to die; tracing may not flush
pub fn kill(name: &str) -> ! {
    eprintln!("FORGE_FAILPOINT {name} hit; killing");
    let _ = std::process::Command::new("kill")
        .args(["-KILL", &std::process::id().to_string()])
        .status();
    std::process::abort() // only if `kill` is missing
}
