//! Crash-test failpoints (paste target: forge-model/src/failpoint.rs).
//!
//! `FORGE_FAILPOINT=<name>[@<n>]` aborts the process (SIGABRT: no destructors, no buffered writes,
//! the same durability outcome as `kill -9`) the n-th time (default 1st) `hit(<name>)` runs.
//! Unset = one LazyLock read per call; always compiled, so the crash tests exercise the real binary.
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
/// (e.g. a partial write) before aborting.
#[inline]
pub fn fires(name: &str) -> bool {
    ARMED
        .as_ref()
        .is_some_and(|a| a.name == name && a.hits.fetch_add(1, Ordering::Relaxed) + 1 == a.nth)
}

/// Aborts on the n-th call for the armed `name`.
#[inline]
pub fn hit(name: &str) {
    if fires(name) {
        abort(name);
    }
}

#[allow(clippy::print_stderr)] // the process is about to die; tracing may not flush
pub fn abort(name: &str) -> ! {
    // stderr marker so the test can tell "aborted at the failpoint" from a real crash.
    eprintln!("FORGE_FAILPOINT {name} hit; aborting");
    std::process::abort();
}
