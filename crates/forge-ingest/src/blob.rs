//! Oversized payload offload: `blobs/<project>/<sha256>` + a JSON reference stored in the column.
use bytes::Bytes;
use forge_model::blob::{blob_ref, parse_blob_ref};
use forge_model::catalog::paths::blob_path;
use forge_model::ids::ProjectId;
use forge_model::row::SpanRow;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

/// A reused blob older than this is rewritten (Overwrite, same bytes) to bump last_modified. At most the orphan
/// grace (checked at config load).
const BLOB_REFRESH: chrono::TimeDelta = chrono::TimeDelta::seconds(forge_model::config::BLOB_REFRESH.as_secs() as i64);

/// Replaces every payload column value (`SpanRow::payload_columns_mut`) longer than
/// `threshold` bytes with a blob reference and returns the blobs to write (`put_blobs`), keyed by path.
/// CPU only (SHA-256): call off the async workers.
pub fn plan_blobs(rows: &mut [SpanRow], threshold: usize) -> HashMap<String, Bytes> {
    let mut puts: HashMap<String, Bytes> = HashMap::new();
    for r in rows.iter_mut() {
        let project: ProjectId = r.project_id.clone().expect("normalized rows carry a project");
        for (_, col) in r.payload_columns_mut() {
            let Some(text) = col.as_deref() else { continue };
            if text.len() <= threshold || parse_blob_ref(text).is_some() {
                continue;
            }
            let sha = hex::encode(Sha256::digest(text.as_bytes()));
            let path = blob_path(&project, &sha);
            let size = text.len();
            let bytes = Bytes::from(col.take().expect("checked").into_bytes());
            *col = Some(blob_ref(&path, size));
            puts.entry(path).or_insert(bytes);
        }
    }
    puts
}

/// Blob sweep vs ingest (plans/phase-4 §2.9). `put_blobs` holds the read lock for its whole call; `begin_sweep` takes
/// the write lock once (every earlier put has refreshed its blob, so the listing sees it fresh) and then records
/// the paths of every later put until the sweep ends; the sweep deletes each chunk under the write lock, minus the
/// recorded paths. Nothing is recorded while no sweep runs.
#[derive(Debug, Default)]
pub struct BlobGuard {
    lock: tokio::sync::RwLock<()>,
    /// Some only while a sweep runs (`begin_sweep` .. drop of its `SweepGuard`).
    recent: std::sync::Mutex<Option<HashSet<String>>>,
    /// No `put_blobs` can run during a sweep (see `exclusive`).
    pub(crate) exclusive: bool,
}

/// An active sweep's recording window; dropping it (also on error) stops recording.
pub struct SweepGuard<'a>(&'a BlobGuard);

impl Drop for SweepGuard<'_> {
    fn drop(&mut self) {
        *self.0.recent.lock().unwrap() = None;
    }
}

impl BlobGuard {
    /// For offline callers (`forge gc`) that hold every WAL lock: no ingest runs, so no blob is put or refreshed
    /// during the sweep and its re-check HEAD is skipped.
    pub fn exclusive() -> Self {
        Self {
            exclusive: true,
            ..Self::default()
        }
    }

    /// Sweep start (before listing): waits out in-flight puts, then records later puts until the guard drops.
    pub async fn begin_sweep(&self) -> SweepGuard<'_> {
        let _w = self.lock.write().await;
        *self.recent.lock().unwrap() = Some(HashSet::new());
        SweepGuard(self)
    }

    /// Paths recorded by the active sweep; None when no sweep runs (tests).
    pub fn recorded(&self) -> Option<usize> {
        self.recent.lock().unwrap().as_ref().map(HashSet::len)
    }

    /// Deletes `paths` except those put since `begin_sweep`, under the write lock (no `put_blobs` runs meanwhile).
    /// Returns the paths it deleted.
    pub async fn delete_unless_recent(
        &self,
        store: &dyn ObjectStore,
        paths: Vec<Path>,
    ) -> object_store::Result<Vec<Path>> {
        let _sweep = self.lock.write().await;
        let doomed: Vec<Path> = {
            let recent = self.recent.lock().unwrap();
            let recent = recent.as_ref();
            paths
                .into_iter()
                .filter(|p| !recent.is_some_and(|r| r.contains(p.as_ref())))
                .collect()
        };
        forge_storage::delete_all(store, doomed.clone()).await?;
        Ok(doomed)
    }
}

/// Writes the exact UTF-8 bytes of each planned blob. Content-addressed, so writes are idempotent:
/// `PutMode::Create` + AlreadyExists = success. Returns the number of distinct blobs written (or found).
pub async fn put_blobs(
    puts: HashMap<String, Bytes>,
    store: &dyn ObjectStore,
    guard: &BlobGuard,
) -> object_store::Result<usize> {
    let _sweep = guard.lock.read().await;
    if let Some(r) = guard.recent.lock().unwrap().as_mut() {
        r.extend(puts.keys().cloned());
    }
    let n = puts.len();
    futures::stream::iter(puts)
        .map(|(path, bytes)| async move {
            let path = Path::from(path);
            let create = || PutOptions::from(PutMode::Create);
            match store.put_opts(&path, bytes.clone().into(), create()).await {
                Err(object_store::Error::AlreadyExists { .. }) => {}
                r => return r.map(drop),
            }
            // Reused blob: refresh it when old, so the orphan sweep (last_modified < now - 24 h and no blob_refs
            // row) cannot take it before this row is flushed (lifecycle.md §1.4). Gone since the put (RD #13):
            // write it again.
            match store.head(&path).await {
                Ok(m) if chrono::Utc::now().signed_duration_since(m.last_modified) > BLOB_REFRESH => {
                    store.put(&path, bytes.into()).await.map(drop)
                }
                Ok(_) => Ok(()),
                Err(object_store::Error::NotFound { .. }) => {
                    match store.put_opts(&path, bytes.into(), create()).await {
                        Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(e),
            }
        })
        .buffer_unordered(8)
        .try_collect::<Vec<()>>()
        .await?;
    Ok(n)
}

/// `plan_blobs` then `put_blobs`.
pub async fn offload_blobs(
    rows: &mut [SpanRow],
    store: &dyn ObjectStore,
    threshold: usize,
) -> object_store::Result<usize> {
    put_blobs(plan_blobs(rows, threshold), store, &BlobGuard::default()).await
}
