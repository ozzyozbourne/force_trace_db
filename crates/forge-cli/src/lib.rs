//! The `forge` command line: `serve` plus stubs for the later subcommands.
use anyhow::Context;
use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};
use forge_model::config::ForgeConfig;
use forge_model::error::Unimplemented;
use forge_server::{Forge, ShutdownSignals};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "forge", version, about = "Forge trace database")]
pub struct Cli {
    #[arg(short, long, global = true, env = "FORGE_CONFIG")]
    pub config: Option<PathBuf>,
    #[arg(long, global = true, env = "FORGE_LOG", default_value = "info")]
    pub log: String,
    /// `json` selects JSON logs; any other value is human-readable text.
    #[arg(long, global = true, env = "FORGE_LOG_FORMAT", default_value = "text")]
    pub log_format: String,
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
    /// Print a segment's footer, column stats and forge metadata.
    Segment {
        path: PathBuf,
        /// Append the Vortex layout tree.
        #[arg(long)]
        layout: bool,
        /// Scan key columns and re-check the segment contract (sort order, one row per span).
        #[arg(long)]
        verify: bool,
    },
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

/// `RUST_LOG` wins, else `--log`; always stderr; format `json` selects JSON.
pub fn init_tracing(log: &str, format: &str) {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log));
    let b = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    if format == "json" { b.json().init() } else { b.init() }
}

pub async fn run(cli: Cli) -> anyhow::Result<()> {
    init_tracing(&cli.log, &cli.log_format);
    match cli.cmd {
        Cmd::Serve => {
            // Before any startup work, so an early SIGTERM is handled (exit 0) instead of killing the process.
            let mut signals = ShutdownSignals::install().context("installing signal handlers")?;
            tracing::info!("starting");
            let cfg = ForgeConfig::load(cli.config.as_deref()).context("loading config")?;
            let forge = tokio::select! {
                r = Box::pin(Forge::open(cfg)) => r?,
                () = signals.recv() => {
                    // Open may be replaying the WAL or retrying a replay-time flush; exiting is a crash-equivalent
                    // stop (the WAL replays on the next start).
                    tracing::warn!("shutdown signal received during startup; exiting (the WAL replays on the next start)");
                    std::process::exit(0);
                }
            };
            forge.serve(signals).await
        }
        Cmd::Sql(_) => not_yet("sql", Unimplemented("M2")),
        Cmd::Inspect(InspectCmd::Segment { path, layout, verify }) => {
            let path = resolve_segment_path(&path, cli.config.as_deref())?;
            let out = forge_storage::inspect::inspect_segment(
                &path,
                &forge_storage::inspect::InspectOptions { layout, verify },
            )
            .await
            .with_context(|| format!("inspecting {}", path.display()))?;
            use std::io::Write;
            writeln!(std::io::stdout().lock(), "{out}")?;
            if verify && out.contains("contract:  VIOLATED") {
                anyhow::bail!("segment contract violated")
            }
            Ok(())
        }
        Cmd::Compact { .. } => not_yet("compact", Unimplemented("M3")),
        Cmd::Gc { .. } => not_yet("gc", Unimplemented("M3")),
        Cmd::Bench(_) => not_yet("bench", Unimplemented("M6")),
    }
}
fn not_yet(cmd: &str, Unimplemented(m): Unimplemented) -> anyhow::Result<()> {
    anyhow::bail!("`forge {cmd}` is not implemented yet (lands in {m})")
}

/// A missing relative path is retried under the configured file:// storage root.
fn resolve_segment_path(p: &std::path::Path, config: Option<&std::path::Path>) -> anyhow::Result<PathBuf> {
    if p.exists() || p.is_absolute() {
        return Ok(p.to_path_buf());
    }
    let cfg = ForgeConfig::load(config).context("loading config")?;
    match forge_storage::file_url_path(&cfg.storage.url) {
        Ok(root) if root.join(p).exists() => Ok(root.join(p)),
        _ => Ok(p.to_path_buf()),
    }
}
