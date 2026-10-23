//! The `forge` command line: `serve` plus stubs for the later subcommands.
use anyhow::Context;
use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};
use forge_model::config::ForgeConfig;
use forge_server::{Forge, ShutdownSignals};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "forge", version, about = "Forge trace database")]
pub struct Cli {
    #[arg(short, long, global = true, env = "FORGE_CONFIG")]
    pub config: Option<PathBuf>,
    #[arg(long, global = true, env = "FORGE_LOG", default_value = "info")]
    pub log: String,
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Run the server.
    Serve,
    /// Run a SQL query.
    Sql(SqlArgs),
    /// Inspect files.
    #[command(subcommand)]
    Inspect(InspectCmd),
    /// Compact a project.
    Compact {
        #[arg(long)]
        project: String,
    },
    /// Garbage-collect.
    Gc {
        #[arg(long)]
        dry_run: bool,
    },
    /// Benchmarks.
    Bench(BenchArgs),
}

#[derive(Args, Debug)]
#[command(group(ArgGroup::new("target").args(["data_dir", "server"])))]
pub struct SqlArgs {
    pub query: String,
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    #[arg(long)]
    pub server: Option<String>,
    #[arg(long, value_enum, default_value_t = Format::Table)]
    pub format: Format,
}
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum Format {
    Table,
    Json,
    Csv,
}

#[derive(Subcommand, Debug)]
pub enum InspectCmd {
    Segment { path: PathBuf },
}

#[derive(Args, Debug)]
pub struct BenchArgs {
    #[arg(value_enum)]
    pub mode: BenchMode,
    #[arg(long, default_value = "http://127.0.0.1:4317")]
    pub endpoint: String,
}
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum BenchMode {
    Ingest,
    Query,
}

/// `RUST_LOG` wins, else `--log`; always stderr; `FORGE_LOG_FORMAT=json` selects JSON.
pub fn init_tracing(log: &str) {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log));
    let b = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    if std::env::var("FORGE_LOG_FORMAT").is_ok_and(|v| v == "json") {
        b.json().init()
    } else {
        b.init()
    }
}

pub async fn run(cli: Cli) -> anyhow::Result<()> {
    init_tracing(&cli.log);
    match cli.cmd {
        Cmd::Serve => {
            // Before any startup work, so an early SIGTERM is queued instead of killing the process.
            let signals = ShutdownSignals::install().context("installing signal handlers")?;
            let cfg = ForgeConfig::load(cli.config.as_deref()).context("loading config")?;
            Forge::open(cfg).await?.serve(signals).await
        }
        Cmd::Sql(_) => not_yet("sql", "M2"),
        Cmd::Inspect(_) => not_yet("inspect segment", "M1"),
        Cmd::Compact { .. } => not_yet("compact", "M3"),
        Cmd::Gc { .. } => not_yet("gc", "M3"),
        Cmd::Bench(_) => not_yet("bench", "M6"),
    }
}
fn not_yet(cmd: &str, m: &str) -> anyhow::Result<()> {
    anyhow::bail!("`forge {cmd}` is not implemented yet (lands in {m})")
}
