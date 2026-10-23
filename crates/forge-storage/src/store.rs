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
            Ok(BuiltStore {
                store: Arc::new(DurableLocalFs::new(dir.clone())?),
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

// ---- DurableLocalFs (pasted verbatim from scratch/p2_wal/src/durable.rs) ----
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result, UploadPart,
};

#[derive(Debug)]
pub struct DurableLocalFs {
    inner: Arc<LocalFileSystem>,
    root: PathBuf,
}

impl DurableLocalFs {
    /// `root` must exist (build_object_store creates it) and be absolute.
    pub fn new(root: PathBuf) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(LocalFileSystem::new_with_prefix(&root)?),
            root,
        })
    }

    async fn sync(&self, location: &Path) -> Result<()> {
        let file = self.inner.path_to_filesystem(location)?;
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || sync_file_and_parents(&file, &root))
            .await
            .map_err(|e| object_store::Error::JoinError { source: e })?
            .map_err(|e| object_store::Error::Generic {
                store: "DurableLocalFs",
                source: Box::new(e),
            })
    }
}

/// fsync(file), then fsync each ancestor dir up to and including `root` (LocalFileSystem may have
/// created any of them for this put). ~6 fsyncs per segment; fine at one flush per 64 MiB / 30 s.
pub fn sync_file_and_parents(file: &std::path::Path, root: &std::path::Path) -> std::io::Result<()> {
    std::fs::File::open(file)?.sync_all()?;
    let mut dir = file.parent();
    while let Some(d) = dir {
        std::fs::File::open(d)?.sync_all()?;
        if d == root {
            break;
        }
        dir = d.parent();
    }
    Ok(())
}

impl std::fmt::Display for DurableLocalFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DurableLocalFs({})", self.root.display())
    }
}

#[async_trait]
impl ObjectStore for DurableLocalFs {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        let r = self.inner.put_opts(location, payload, opts).await?;
        forge_model::failpoint::hit("upload_before_fsync");
        self.sync(location).await?;
        Ok(r)
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(DurableUpload {
            inner,
            file: self.inner.path_to_filesystem(location)?,
            root: self.root.clone(),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[std::ops::Range<u64>]) -> Result<Vec<bytes::Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        // Deletes need no fsync: a resurrected orphan after power loss is GC'd again.
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await?;
        self.sync(to).await
    }
}

#[derive(Debug)]
struct DurableUpload {
    inner: Box<dyn MultipartUpload>,
    file: PathBuf,
    root: PathBuf,
}

#[async_trait]
impl MultipartUpload for DurableUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }
    async fn complete(&mut self) -> Result<PutResult> {
        let r = self.inner.complete().await?;
        let (file, root) = (self.file.clone(), self.root.clone());
        tokio::task::spawn_blocking(move || sync_file_and_parents(&file, &root))
            .await
            .map_err(|e| object_store::Error::JoinError { source: e })?
            .map_err(|e| object_store::Error::Generic {
                store: "DurableLocalFs",
                source: Box::new(e),
            })?;
        Ok(r)
    }
    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}
