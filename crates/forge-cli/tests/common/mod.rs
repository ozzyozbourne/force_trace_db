use std::process::Command;

/// The `forge` binary with every inherited `FORGE_*` variable removed, so the caller's env never leaks in.
pub fn forge_cmd() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_forge"));
    for (k, _) in std::env::vars().filter(|(k, _)| k.starts_with("FORGE_")) {
        cmd.env_remove(k);
    }
    cmd
}
