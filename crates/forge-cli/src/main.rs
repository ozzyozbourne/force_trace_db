use clap::Parser;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    forge_cli::run(forge_cli::Cli::parse()).await
}
