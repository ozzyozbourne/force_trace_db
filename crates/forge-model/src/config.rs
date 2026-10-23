//! `forge.toml` + `FORGE_<SECTION>__<KEY>` environment overrides (master decision 11).
//!
//! Only the spec's sections and keys exist in M0 (plus `node` and `server`); later phases add fields, and
//! `#[serde(default)]` keeps that non-breaking. Default paths are relative to the working directory.

use bytesize::ByteSize;
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ForgeConfig {
    pub node: NodeConfig,
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub metastore: MetastoreConfig,
    pub wal: WalConfig,
    pub memtable: MemtableConfig,
    pub cache: CacheConfig,
    pub compaction: CompactionConfig,
    pub query: QueryConfig,
    pub ingest: IngestConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    /// WAL object prefix and checkpoint owner.
    #[serde(deserialize_with = "string_or_scalar")]
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub otlp_grpc_addr: SocketAddr,
    pub otlp_http_addr: SocketAddr,
    pub api_addr: SocketAddr,
    /// `/healthz`, `/readyz`, `/metrics`.
    pub admin_addr: SocketAddr,
    /// How long open connections may take to drain after the first shutdown signal.
    #[serde(with = "humantime_serde")]
    pub shutdown_grace: Duration,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// `file://<path>` (relative unless the path starts with `/`) or `memory://`; s3/gs/az in M5.
    #[serde(deserialize_with = "string_or_scalar")]
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetastoreConfig {
    /// `sqlite://<path>` or `sqlite::memory:`.
    #[serde(deserialize_with = "string_or_scalar")]
    pub url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalMode {
    Local,
    ObjectStore,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WalConfig {
    pub mode: WalMode,
    pub dir: PathBuf,
    /// Rotate WAL files at this size.
    pub segment_bytes: ByteSize,
    pub group_commit_max_bytes: ByteSize,
    #[serde(with = "humantime_serde")]
    pub group_commit_max_delay: Duration,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestConfig {
    /// Project for spans with no x-forge-project header and no project resource attribute.
    #[serde(deserialize_with = "string_or_scalar")]
    pub default_project: String,
    /// Payload columns longer than this go to blobs/<project>/<sha256>.
    pub blob_threshold: ByteSize,
    /// Wire and decoded body cap for OTLP gRPC and HTTP.
    pub max_request_bytes: ByteSize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MemtableConfig {
    pub flush_bytes: ByteSize,
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
    pub memory_budget: ByteSize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    pub memory: ByteSize,
    pub disk: ByteSize,
    pub disk_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CompactionConfig {
    pub target_segment: ByteSize,
    pub concurrency: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueryConfig {
    pub memory_limit: ByteSize,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
}

/// Accepts str/int/float/bool (figment types env and TOML values) and yields a String.
pub(crate) fn string_or_scalar<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Scalar {
        Str(String),
        U(u64),
        I(i64),
        F(f64),
        B(bool),
    }
    Ok(match Scalar::deserialize(d)? {
        Scalar::Str(s) => s,
        Scalar::U(n) => n.to_string(),
        Scalar::I(n) => n.to_string(),
        Scalar::F(n) => n.to_string(),
        Scalar::B(b) => b.to_string(),
    })
}

/// `scheme://rest` -> `(scheme, rest)`. The shared first step of storage and metastore URL parsing (D9):
/// after the scheme, the rest is a filesystem path for `file://` and `sqlite://`.
pub fn split_scheme(url: &str) -> Option<(&str, &str)> {
    url.split_once("://")
}

impl ForgeConfig {
    /// defaults <- `path` (must exist) | `./forge.toml` if it is a file (cwd only) <- `FORGE_<SECTION>__<KEY>` env.
    pub fn load(path: Option<&Path>) -> Result<Self, figment::Error> {
        use figment::{
            Figment,
            providers::{Env, Format, Serialized, Toml},
        };
        let mut fig = Figment::from(Serialized::defaults(ForgeConfig::default()));
        fig = match path {
            Some(p) => fig.merge(Toml::file_exact(p)),
            None if Path::new("forge.toml").is_file() => fig.merge(Toml::file_exact("forge.toml")),
            None => fig,
        };
        // Only nested keys are config, so FORGE_CONFIG / FORGE_LOG / FORGE_LOG_FORMAT (CLI) are ignored.
        let env = Env::prefixed("FORGE_")
            .filter(|k| k.as_str().contains("__"))
            .split("__");
        // figment type-sniffs env values (`007` -> 7), which loses text for string and path keys, so those
        // keys are merged again with the raw env text.
        let raw: Vec<(String, String)> = env
            .iter()
            .filter(|(k, _)| RAW_ENV_KEYS.contains(&k.as_str()))
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        fig = fig.merge(env);
        for (k, v) in raw {
            fig = fig.merge(Serialized::default(&k, v));
        }
        let cfg: Self = fig.extract()?;
        // node.id names the WAL prefix and the checkpoint owner (M1).
        use crate::ids::ProjectId;
        ProjectId::new(cfg.node.id.clone()).map_err(|e| figment::Error::from(format!("node.id: {e}")))?;
        ProjectId::new(cfg.ingest.default_project.clone())
            .map_err(|e| figment::Error::from(format!("ingest.default_project: {e}")))?;
        if cfg.memtable.flush_bytes > cfg.memtable.memory_budget {
            return Err(figment::Error::from(
                "memtable.flush_bytes must be <= memtable.memory_budget".to_string(),
            ));
        }
        Ok(cfg)
    }
}

/// Config keys whose type is String or PathBuf: env overrides keep their exact text.
const RAW_ENV_KEYS: [&str; 6] = [
    "node.id",
    "storage.url",
    "metastore.url",
    "wal.dir",
    "cache.disk_dir",
    "ingest.default_project",
];

impl Default for NodeConfig {
    fn default() -> Self {
        Self { id: "node-1".into() }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            otlp_grpc_addr: SocketAddr::from(([0, 0, 0, 0], 4317)),
            otlp_http_addr: SocketAddr::from(([0, 0, 0, 0], 4318)),
            api_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
            admin_addr: SocketAddr::from(([0, 0, 0, 0], 9090)),
            shutdown_grace: Duration::from_secs(10),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            url: "file://./forge-data/data".into(),
        }
    }
}

impl Default for MetastoreConfig {
    fn default() -> Self {
        Self {
            url: "sqlite://./forge-data/meta.db".into(),
        }
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            mode: WalMode::Local,
            dir: "./forge-data/wal".into(),
            segment_bytes: ByteSize::mib(64),
            group_commit_max_bytes: ByteSize::mib(1),
            group_commit_max_delay: Duration::from_millis(10),
        }
    }
}

impl Default for MemtableConfig {
    fn default() -> Self {
        Self {
            flush_bytes: ByteSize::mib(64),
            flush_interval: Duration::from_secs(30),
            memory_budget: ByteSize::gib(1),
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            memory: ByteSize::mib(512),
            disk: ByteSize::gib(10),
            disk_dir: "./forge-data/cache".into(),
        }
    }
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            target_segment: ByteSize::mib(256),
            concurrency: 2,
        }
    }
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            memory_limit: ByteSize::gib(2),
            timeout: Duration::from_secs(30),
        }
    }
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            default_project: "default".into(),
            blob_threshold: ByteSize::mib(1),
            max_request_bytes: ByteSize::mib(64),
        }
    }
}
