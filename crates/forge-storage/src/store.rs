//! Storage URL -> object store (D9). Named `store`, not `object_store`, so it does not shadow the crate.

use crate::{StorageError, StorageResult};
use forge_model::config::{StorageConfig, split_scheme};
use forge_model::error::Unimplemented;
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
    match split_scheme(url) {
        Some(("file", _)) => {
            let dir = file_url_path(url)?;
            std::fs::create_dir_all(&dir)?;
            let store = LocalFileSystem::new_with_prefix(&dir)?;
            Ok(BuiltStore {
                store: Arc::new(store),
                local_root: Some(dir),
            })
        }
        Some(("memory", _)) => Ok(BuiltStore {
            store: Arc::new(InMemory::new()),
            local_root: None,
        }),
        Some(("s3" | "s3a" | "gs" | "az" | "abfs" | "abfss" | "azure", _)) => Err(Unimplemented("M5").into()),
        _ => Err(StorageError::Config {
            url: url.to_string(),
            msg: "unsupported scheme".into(),
        }),
    }
}

/// `file://./x` -> `<cwd>/x`, `file:///x` -> `/x`. Everything after `file://` is a filesystem path
/// (deliberately not RFC 8089 hosts). Pure: does not touch the filesystem.
pub fn file_url_path(url: &str) -> StorageResult<PathBuf> {
    let bad = |msg: &str| StorageError::Config {
        url: url.to_string(),
        msg: msg.to_string(),
    };
    match split_scheme(url) {
        Some(("file", "")) => Err(bad("empty path")),
        Some(("file", p)) => Ok(std::path::absolute(p)?),
        _ => Err(bad("expected file://")),
    }
}
