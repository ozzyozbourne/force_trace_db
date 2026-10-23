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
        fig.merge(
            Env::prefixed("FORGE_")
                .filter(|k| k.as_str().contains("__"))
                .split("__"),
        )
        .extract()
    }
}

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
