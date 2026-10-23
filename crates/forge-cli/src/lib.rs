//! The `forge` command line: `serve`, `sql`, `inspect`, plus stubs for the later subcommands.
pub mod table;

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
        Cmd::Sql(a) => sql(a, cli.config.as_deref()).await,
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
const OFFLINE_NOTE: &str = "note: offline mode reads flushed segments only; unflushed rows are invisible";

/// `forge sql`: `--server` POSTs to `/v1/sql`; `--data-dir` or the config's URLs run the query in process over a
/// read-only metastore and no memtable. Both print the same envelope.
async fn sql(a: SqlArgs, config: Option<&std::path::Path>) -> anyhow::Result<()> {
    use std::io::Write;
    let body: Vec<u8> = match &a.server {
        Some(url) => {
            let resp = reqwest::Client::new()
                .post(format!("{}/v1/sql", url.trim_end_matches('/')))
                .json(&serde_json::json!({ "query": a.query }))
                .send()
                .await
                .with_context(|| format!("connecting to {url}"))?;
            let status = resp.status();
            let body = resp.bytes().await.context("reading the response")?;
            if !status.is_success() {
                let msg = serde_json::from_slice::<serde_json::Value>(&body)
                    .ok()
                    .and_then(|v| v["error"].as_str().map(String::from))
                    .unwrap_or_else(|| String::from_utf8_lossy(&body).into_owned());
                anyhow::bail!("{msg} (HTTP {status})");
            }
            body.to_vec()
        }
        None => {
            let (meta_url, storage_url, query_cfg) = match &a.data_dir {
                Some(d) => {
                    if !d.join("meta.db").is_file() || !d.join("data").is_dir() {
                        anyhow::bail!("no Forge data at {}", d.display());
                    }
                    let q = forge_model::config::QueryConfig::default();
                    (
                        format!("sqlite://{}/meta.db", d.display()),
                        format!("file://{}/data", d.display()),
                        q,
                    )
                }
                None => {
                    let cfg = ForgeConfig::load(config).context("loading config")?;
                    // build_object_store creates a missing file:// root: refuse instead.
                    if let Ok(root) = forge_storage::file_url_path(&cfg.storage.url)
                        && !root.is_dir()
                    {
                        anyhow::bail!("no Forge data at {} (storage.url {})", root.display(), cfg.storage.url);
                    }
                    (cfg.metastore.url, cfg.storage.url, cfg.query)
                }
            };
            let meta = forge_meta::open_read_only(&forge_model::config::MetastoreConfig { url: meta_url }).await?;
            let built = forge_storage::build_object_store(&forge_model::config::StorageConfig { url: storage_url })?;
            let store = std::sync::Arc::new(forge_storage::VortexSegmentStore::new(built));
            let engine =
                forge_query::QueryEngine::new(meta, store, std::sync::Arc::new(forge_model::EmptyMemtable), query_cfg);
            writeln!(std::io::stderr().lock(), "{OFFLINE_NOTE}")?;
            let req = forge_query::sql::SqlRequest {
                query: a.query,
                max_rows: None,
                timeout_ms: None,
            };
            use forge_model::error::Classify;
            engine.sql_json(&req).await.map_err(|e| anyhow::anyhow!(e.message()))?
        }
    };
    let env: serde_json::Value = serde_json::from_slice(&body).context("parsing the response")?;
    let out = match a.format {
        Format::Table => table::render_table(&env),
        Format::Json => table::render_json(&env),
    };
    std::io::stdout().lock().write_all(out.as_bytes())?;
    Ok(())
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
