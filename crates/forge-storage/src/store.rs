//! Storage URL -> object store (D9). Named `store`, not `object_store`, so it does not shadow the crate.

use crate::{StorageError, StorageResult};
use forge_model::config::StorageConfig;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use std::path::PathBuf;
use std::sync::Arc;

pub struct BuiltStore {
    pub store: Arc<dyn ObjectStore>,
    /// Some(absolute root) for file:// (fsync in M1).
    pub local_root: Option<PathBuf>,
}

/// `file://<path>` -> absolute path + create_dir_all + `LocalFileSystem::new_with_prefix`;
/// `memory://...` -> `InMemory`; s3/gs/az -> `Unimplemented("M5")`; empty `file://` or other -> `Config`.
pub fn build_object_store(cfg: &StorageConfig) -> StorageResult<BuiltStore> {
    let url = cfg.url.as_str();
    if url.starts_with("file://") {
        let dir = file_url_path(url)?;
        std::fs::create_dir_all(&dir)?;
        let store = LocalFileSystem::new_with_prefix(&dir)?;
        return Ok(BuiltStore {
            store: Arc::new(store),
            local_root: Some(dir),
        });
    }
    if url.starts_with("memory://") {
        return Ok(BuiltStore {
            store: Arc::new(InMemory::new()),
            local_root: None,
        });
    }
    if ["s3://", "s3a://", "gs://", "az://", "abfs://", "abfss://", "azure://"]
        .iter()
        .any(|s| url.starts_with(s))
    {
        return Err(StorageError::Unimplemented("M5"));
    }
    Err(StorageError::Config {
        url: url.to_string(),
        msg: "unsupported scheme".into(),
    })
}

/// `file://./x` -> `<cwd>/x`, `file:///x` -> `/x`. Everything after `file://` is a filesystem path
/// (deliberately not RFC 8089 hosts). Pure: does not touch the filesystem.
pub fn file_url_path(url: &str) -> StorageResult<PathBuf> {
    let bad = |msg: &str| StorageError::Config {
        url: url.to_string(),
        msg: msg.to_string(),
    };
    let p = url.strip_prefix("file://").ok_or_else(|| bad("expected file://"))?;
    if p.is_empty() {
        return Err(bad("empty path"));
    }
    Ok(std::path::absolute(p)?)
}
