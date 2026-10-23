use clap::Parser;
#[tokio::main]
async fn main() {
    if let Err(e) = forge_cli::run(forge_cli::Cli::parse()).await {
        use std::io::Write;
        let _ = writeln!(std::io::stderr().lock(), "error: {e:#}");
        // 3: a server holds the data dir (`forge compact`/`forge gc`).
        std::process::exit(if e.is::<forge_cli::ServerRunning>() { 3 } else { 1 });
    }
}
