use clap::Parser;
#[tokio::main]
async fn main() {
    if let Err(e) = forge_cli::run(forge_cli::Cli::parse()).await {
        use std::io::Write;
        let _ = writeln!(std::io::stderr().lock(), "error: {e:#}");
        std::process::exit(1);
    }
}
