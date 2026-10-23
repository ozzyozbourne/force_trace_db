//! The `forge` command line: `serve`, `sql`, `inspect`, `compact`, `gc`, plus stubs for the later subcommands.
mod table;

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
    /// Compact a project (offline: stop the server first).
    Compact {
        #[arg(long)]
        project: String,
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long, value_enum, default_value_t = Format::Table)]
        format: Format,
    },
    /// Retention, purge, orphan GC (offline: stop the server first).
    Gc {
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long, value_parser = humantime_serde::re::humantime::parse_duration)]
        gc_grace: Option<std::time::Duration>,
        #[arg(long, value_parser = humantime_serde::re::humantime::parse_duration)]
        orphan_grace: Option<std::time::Duration>,
        #[arg(long, value_enum, default_value_t = Format::Table)]
        format: Format,
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
        Cmd::Compact {
            project,
            data_dir,
            format,
        } => {
            compact(
                &Offline::open(data_dir.as_deref(), cli.config.as_deref()).await?,
                &project,
                format,
            )
            .await
        }
        Cmd::Gc {
            dry_run,
            data_dir,
            gc_grace,
            orphan_grace,
            format,
        } => {
            let o = Offline::open(data_dir.as_deref(), cli.config.as_deref()).await?;
            gc(&o, dry_run, gc_grace, orphan_grace, format).await
        }
        Cmd::Bench(_) => not_yet("bench", Unimplemented("M6")),
    }
}
const OFFLINE_NOTE: &str = "note: offline mode reads flushed segments only; unflushed rows are invisible";

/// `forge sql`: `--server` POSTs to `/v1/sql`; `--data-dir` or the config's URLs run the query in process over a
/// read-only metastore and no memtable. Both print the same envelope.
async fn sql(a: SqlArgs, config: Option<&std::path::Path>) -> anyhow::Result<()> {
    use std::io::Write;
    let body = match &a.server {
        Some(url) => remote_sql(url, &a.query).await?,
        None => offline_sql(&a, config).await?,
    };
    let env: serde_json::Value = serde_json::from_slice(&body).context("parsing the response")?;
    let out = match a.format {
        Format::Table => table::render_table(&env),
        Format::Json => table::render_json(&env),
    };
    std::io::stdout().lock().write_all(out.as_bytes())?;
    Ok(())
}

/// The `/v1/sql` envelope from a running server; a non-2xx answer is an error with the server's message.
async fn remote_sql(url: &str, query: &str) -> anyhow::Result<Vec<u8>> {
    let resp = reqwest::Client::new()
        .post(format!("{}/v1/sql", url.trim_end_matches('/')))
        .json(&serde_json::json!({ "query": query }))
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
    Ok(body.to_vec())
}

/// The same envelope, computed in process over the flushed segments of `--data-dir` (or the config's URLs).
async fn offline_sql(a: &SqlArgs, config: Option<&std::path::Path>) -> anyhow::Result<Vec<u8>> {
    use std::io::Write;
    let cfg = match &a.data_dir {
        Some(d) => data_dir_config(d)?,
        None => {
            let cfg = ForgeConfig::load(config).context("loading config")?;
            // build_object_store creates a missing file:// root: refuse instead.
            if let Ok(root) = forge_storage::file_url_path(&cfg.storage.url)
                && !root.is_dir()
            {
                anyhow::bail!("no Forge data at {} (storage.url {})", root.display(), cfg.storage.url);
            }
            cfg
        }
    };
    let meta = forge_meta::open_read_only(&cfg.metastore).await?;
    let store = segment_store(&cfg)?;
    let engine = forge_query::QueryEngine::new(meta, store, std::sync::Arc::new(forge_model::EmptyMemtable), cfg.query);
    writeln!(std::io::stderr().lock(), "{OFFLINE_NOTE}")?;
    let req = forge_query::sql::SqlRequest {
        query: a.query.clone(),
        max_rows: None,
        timeout_ms: None,
    };
    use forge_model::error::Classify;
    engine.sql_json(&req).await.map_err(|e| anyhow::anyhow!(e.message()))
}

/// `--data-dir d`: the config rooted at `d`, which must hold Forge data.
fn data_dir_config(d: &std::path::Path) -> anyhow::Result<ForgeConfig> {
    use forge_model::config::{DATA_DIR, META_DB};
    if !d.join(META_DB).is_file() || !d.join(DATA_DIR).is_dir() {
        anyhow::bail!("no Forge data at {}", d.display());
    }
    Ok(ForgeConfig::rooted_at(d))
}

fn segment_store(cfg: &ForgeConfig) -> anyhow::Result<std::sync::Arc<dyn forge_storage::SegmentStore>> {
    let built = forge_storage::build_object_store(&cfg.storage)?;
    Ok(std::sync::Arc::new(forge_storage::VortexSegmentStore::new(built)))
}

/// `forge compact`/`forge gc` refuse to run next to a server (exit code 3; plans/phase-4 D12).
#[derive(Debug)]
pub struct ServerRunning(pub String);
impl std::fmt::Display for ServerRunning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "a server is running on this data dir ({}/LOCK is held); stop it first",
            self.0
        )
    }
}
impl std::error::Error for ServerRunning {}

/// An offline data dir: every WAL node directory locked (held until drop), metastore read-write, compaction jobs
/// of an earlier process reset.
struct Offline {
    cfg: ForgeConfig,
    meta: std::sync::Arc<dyn forge_meta::Metastore>,
    segments: std::sync::Arc<dyn forge_storage::SegmentStore>,
    /// (node dir name, records past its checkpoint) per locked WAL directory.
    wal: Vec<(String, u64)>,
    _locks: Vec<std::fs::File>,
}

impl Offline {
    async fn open(data_dir: Option<&std::path::Path>, config: Option<&std::path::Path>) -> anyhow::Result<Self> {
        let cfg = match data_dir {
            Some(d) => data_dir_config(d)?,
            None => {
                let cfg = ForgeConfig::load(config).context("loading config")?;
                // build_object_store and the metastore open create missing data: refuse instead.
                let root = forge_storage::file_url_path(&cfg.storage.url).ok();
                let db = cfg
                    .metastore
                    .url
                    .strip_prefix("sqlite://")
                    .map(std::path::PathBuf::from);
                if root.as_ref().is_some_and(|r| !r.is_dir()) || db.as_ref().is_some_and(|d| !d.is_file()) {
                    anyhow::bail!(
                        "no Forge data at {} (storage.url {})",
                        cfg.storage.url,
                        cfg.metastore.url
                    );
                }
                cfg
            }
        };
        // Every node directory (a server may run under another node id) plus our own.
        let mut dirs = std::collections::BTreeSet::from([cfg.wal.dir.join(&cfg.node.id)]);
        if let Ok(rd) = std::fs::read_dir(&cfg.wal.dir) {
            for e in rd {
                let e = e?;
                if e.file_type()?.is_dir() {
                    dirs.insert(e.path());
                }
            }
        }
        let mut locks = Vec::new();
        for d in &dirs {
            match forge_ingest::wal::lock_dir(d) {
                Ok(l) => locks.push(l),
                Err(forge_ingest::wal::WalError::Locked(_)) => {
                    let rel = d
                        .strip_prefix(cfg.wal.dir.parent().unwrap_or(&cfg.wal.dir))
                        .unwrap_or(d);
                    return Err(ServerRunning(rel.display().to_string()).into());
                }
                Err(e) => return Err(e.into()),
            }
        }
        let meta = forge_meta::open(&cfg.metastore).await?;
        let segments = segment_store(&cfg)?;
        let mut wal = Vec::new();
        for d in &dirs {
            let node = d
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let flushed = meta.wal_checkpoint(&node).await?.map_or(0, |c| c.flushed_seq);
            let n = forge_ingest::wal::replay(d, flushed, |_, _| Ok::<_, String>(()))?.applied;
            wal.push((node, n));
        }
        meta.reset_compactions().await?;
        Ok(Self {
            cfg,
            meta,
            segments,
            wal,
            _locks: locks,
        })
    }
}

async fn compact(o: &Offline, project: &str, format: Format) -> anyhow::Result<()> {
    use std::io::Write;
    let p = forge_model::ids::ProjectId::new(project)?;
    if o.meta.get_project(&p).await?.is_none() {
        anyhow::bail!("no project {project:?}");
    }
    let cx = forge_ingest::compact::CompactCtx::new(
        o.meta.clone(),
        o.segments.clone(),
        o.cfg.compaction.clone(),
        forge_model::time::Clocks::wall(),
    );
    let s = forge_ingest::compact::compact_project(&cx, &p).await?;
    let mut out = std::io::stdout().lock();
    match format {
        Format::Json => writeln!(out, "{}", serde_json::to_string(&s)?)?,
        Format::Table => {
            for j in &s.jobs {
                let rows: u64 = j.outputs.iter().map(|o| o.2).sum();
                let hours: std::collections::BTreeSet<String> = j
                    .outputs
                    .iter()
                    .map(|o| {
                        forge_model::catalog::HourBucket(o.0)
                            .path_component()
                            .unwrap_or_else(|_| o.0.to_string())
                    })
                    .collect();
                writeln!(
                    out,
                    "compacted {} {} job: {} L0 + {} L1 -> {} L1 (hours {}; {rows} rows) in {} ms",
                    j.project,
                    j.kind,
                    j.l0_inputs,
                    j.l1_inputs,
                    j.outputs.len(),
                    hours.into_iter().collect::<Vec<_>>().join(", "),
                    j.elapsed_ms
                )?;
            }
            writeln!(
                out,
                "done: {} jobs, {} skipped, {} failed{}",
                s.jobs.len(),
                s.skipped,
                s.failed.len(),
                if s.capped { " (capped: run again)" } else { "" }
            )?;
        }
    }
    if !s.failed.is_empty() {
        anyhow::bail!("{} compaction job(s) failed: {}", s.failed.len(), s.failed.join("; "));
    }
    Ok(())
}

/// `forge gc`: blobs only when no node directory has WAL records past its own checkpoint (lifecycle.md §4).
async fn gc(
    o: &Offline,
    dry_run: bool,
    gc_grace: Option<std::time::Duration>,
    orphan_grace: Option<std::time::Duration>,
    format: Format,
) -> anyhow::Result<()> {
    use std::io::Write;
    let unflushed: Vec<String> = o
        .wal
        .iter()
        .filter(|(_, n)| *n > 0)
        .map(|(node, n)| format!("WAL of {node} has {n} unflushed records"))
        .collect();
    let c = &o.cfg.compaction;
    let opts = forge_ingest::lifecycle::GcOptions {
        gc_grace: gc_grace.unwrap_or(c.gc_grace),
        orphan_grace: orphan_grace.unwrap_or(c.orphan_grace),
        sweep_blobs: unflushed.is_empty(),
        dry_run,
    };
    let now = forge_model::time::now_ms();
    let store = o.segments.object_store();
    // `Offline::open` holds every node's WAL lock: no other writer can put blobs, so an empty guard is exact.
    let clocks = forge_model::time::Clocks::at(now, now);
    let blobs = forge_ingest::blob::BlobGuard::default();
    let mut r = forge_ingest::lifecycle::run_gc(o.meta.as_ref(), store.as_ref(), &clocks, &opts, &blobs).await?;
    if !unflushed.is_empty() {
        r.blobs_skipped = Some(format!("{}: run forge serve once", unflushed.join(", ")));
    }
    let mut out = std::io::stdout().lock();
    match format {
        Format::Json => writeln!(out, "{}", serde_json::to_string(&r)?)?,
        Format::Table => {
            writeln!(out, "expired segments:         {}   (retention)", r.expired_segments)?;
            writeln!(
                out,
                "purged segments:          {}   ({} objects, {} bytes)",
                r.purged_segments, r.deleted_objects, r.freed_bytes
            )?;
            writeln!(out, "orphan segment objects:   {}", r.orphan_segment_objects)?;
            match &r.blobs_skipped {
                Some(why) => writeln!(out, "orphan blobs:             {}   (skipped: {why})", r.orphan_blobs)?,
                None => writeln!(out, "orphan blobs:             {}", r.orphan_blobs)?,
            }
        }
    }
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
