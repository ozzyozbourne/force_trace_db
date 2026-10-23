//! Lifecycle (M3): retention, purge of deleted segments, orphan GC of `segments/` and `blobs/`.
//!
//! Order everywhere: metastore first, objects after a grace, rows last.
//! - retention marks whole segments `deleted` (one txn per batch); queries stop seeing them at once;
//! - purge deletes the objects of segments deleted more than `gc_grace` ago (in-flight readers are done), then
//!   forgets their rows (trace_index, blob_refs, segments). A crash in between re-runs: NotFound = success;
//! - the orphan sweep deletes objects that no segments row names and blobs that no blob_refs row names, both
//!   older than `orphan_grace` (object last_modified).
use crate::blob::BlobGuard;
use forge_meta::{MetaError, Metastore};
use forge_model::catalog::{Level, SegmentMeta, paths, retention_cutoff_ns};
use forge_model::config::CompactionConfig;
use forge_model::ids::ProjectId;
use forge_model::time::Clocks;
use forge_storage::delete_all;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use std::collections::HashSet;
use std::time::Duration;

/// Rows per purge batch, L1 hours per retention txn.
const BATCH: usize = 1000;
/// Blobs deleted per write-lock hold of the `BlobGuard` (no HEAD under the lock; RA m11).
const BLOB_CHUNK: usize = 64;
/// Concurrent HEADs of the blob sweep's re-check.
const HEAD_CONCURRENCY: usize = 16;

#[derive(Debug, thiserror::Error)]
pub enum GcError {
    #[error(transparent)]
    Meta(#[from] MetaError),
    #[error("object store: {0}")]
    Store(#[from] object_store::Error),
}

#[derive(Clone, Copy, Debug)]
pub struct GcOptions {
    /// Deleted segments' objects are kept this long after `deleted_at` (readers that listed them finish).
    pub gc_grace: Duration,
    /// Objects unknown to the metastore are kept this long after last_modified (uploads not committed yet).
    pub orphan_grace: Duration,
    /// Sweep `blobs/` too. `forge gc` sets it false when a WAL has records past its checkpoint: their blob refs
    /// are not in `blob_refs` yet (lifecycle.md §4). The server always sweeps (its `BlobGuard` covers new puts).
    pub sweep_blobs: bool,
    /// Report only; change nothing.
    pub dry_run: bool,
}

impl GcOptions {
    /// The configured graces; blobs swept; not a dry run.
    pub fn from_config(c: &CompactionConfig) -> Self {
        Self {
            gc_grace: c.gc_grace,
            orphan_grace: c.orphan_grace,
            sweep_blobs: true,
            dry_run: false,
        }
    }
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct GcReport {
    pub expired_segments: usize,
    pub purged_segments: usize,
    pub deleted_objects: usize,
    pub orphan_segment_objects: usize,
    pub orphan_blobs: usize,
    pub freed_bytes: u64,
    pub blobs_skipped: Option<String>,
}

/// Retention (data clock): marks expired segments of every project with `retention_days` deleted
/// (`Metastore::expire_segments`). Returns the count. Dry run: counts the same candidates
/// (`Metastore::expire_candidates`) without marking.
async fn expire(meta: &dyn Metastore, data_now_ms: i64, dry_run: bool) -> Result<usize, GcError> {
    let mut n = 0;
    for p in meta.list_projects().await? {
        let Some(days) = p.retention_days else { continue };
        let cutoff = retention_cutoff_ns(days, data_now_ms.saturating_mul(1_000_000));
        if dry_run {
            n += meta.expire_candidates(&p.id, cutoff, usize::MAX).await?.len();
            continue;
        }
        loop {
            let done = meta.expire_segments(&p.id, cutoff, BATCH).await?;
            n += done.len();
            if !done.is_empty() {
                tracing::info!(project = %p.id, segments = done.len(), days, "retention: segments expired");
            }
            // Fewer than BATCH L1 hours: none is left (the L0 rule already ran in full).
            let hours: HashSet<_> = done
                .iter()
                .filter(|s| s.level == Level::L1)
                .map(|s| s.partition_hour)
                .collect();
            if hours.len() < BATCH {
                break;
            }
        }
    }
    Ok(n)
}

/// Every object of a segment (`.fts` may not exist: NotFound is success).
fn segment_objects(s: &SegmentMeta) -> impl Iterator<Item = Path> {
    paths::segment_objects(&s.path).into_iter().map(Path::from)
}

/// Purge: objects of segments deleted before `now - gc_grace`, then their rows.
async fn purge(
    meta: &dyn Metastore,
    store: &dyn ObjectStore,
    now_ms: i64,
    o: &GcOptions,
    r: &mut GcReport,
) -> Result<(), GcError> {
    let before = now_ms.saturating_sub(o.gc_grace.as_millis() as i64);
    if o.dry_run {
        let (n, bytes) = meta.purge_stats(before).await?;
        r.purged_segments += n;
        r.freed_bytes += bytes;
        return Ok(());
    }
    loop {
        let segs = meta.purge_candidates(before, BATCH).await?;
        r.freed_bytes += segs
            .iter()
            .map(|s| s.byte_size + s.fts_byte_size.unwrap_or(0))
            .sum::<u64>();
        let objects = segs.iter().flat_map(segment_objects).collect();
        r.deleted_objects += delete_all(store, objects).await?;
        if !segs.is_empty() {
            // Review T1: only a batch that deleted objects exercises "forgotten after the object delete".
            forge_model::failpoint::hit("gc_after_object_delete");
        }
        let ids: Vec<_> = segs.iter().map(|s| s.id).collect();
        r.purged_segments += meta.forget_segments(&ids).await?;
        if segs.len() < BATCH {
            return Ok(());
        }
    }
}

/// Project prefixes under `root` (`segments` | `blobs`): `<root>/<project>/`. Names that are not valid project ids
/// are skipped (never deleted).
async fn projects_under(store: &dyn ObjectStore, root: &str) -> Result<Vec<ProjectId>, object_store::Error> {
    let l = store.list_with_delimiter(Some(&Path::from(root))).await?;
    Ok(l.common_prefixes
        .iter()
        .filter_map(|p| p.filename().and_then(|n| ProjectId::new(n).ok()))
        .collect())
}

/// Objects under `prefix` last modified before `cutoff`.
async fn old_objects(
    store: &dyn ObjectStore,
    prefix: &str,
    cutoff: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<ObjectMeta>, object_store::Error> {
    store
        .list(Some(&Path::from(prefix)))
        .try_filter(|m| futures::future::ready(m.last_modified < cutoff))
        .try_collect()
        .await
}

/// Orphan sweep. The listing comes first and the metastore read second: an object committed in between is in
/// the known set; one committed later is younger than `orphan_grace` and was never a candidate.
async fn sweep_orphans(
    meta: &dyn Metastore,
    store: &dyn ObjectStore,
    now_ms: i64,
    o: &GcOptions,
    r: &mut GcReport,
    blobs: &BlobGuard,
) -> Result<(), GcError> {
    let cutoff = chrono::DateTime::from_timestamp_millis(now_ms.saturating_sub(o.orphan_grace.as_millis() as i64))
        .unwrap_or_default();
    for p in projects_under(store, "segments").await? {
        let old = old_objects(store, &format!("segments/{p}"), cutoff).await?;
        if old.is_empty() {
            continue;
        }
        let known: HashSet<String> = meta.segment_paths(&p).await?;
        let orphans: Vec<ObjectMeta> = old
            .into_iter()
            .filter(|m| paths::owning_segment(m.location.as_ref()).is_some_and(|s| !known.contains(&s)))
            .collect();
        r.orphan_segment_objects += orphans.len();
        r.freed_bytes += orphans.iter().map(|m| m.size).sum::<u64>();
        if !orphans.is_empty() {
            tracing::info!(project = %p, objects = orphans.len(), dry_run = o.dry_run, "gc: orphan segment objects");
        }
        if !o.dry_run {
            delete_all(store, orphans.into_iter().map(|m| m.location).collect()).await?;
        }
    }
    if !o.sweep_blobs {
        return Ok(());
    }
    let _recording = blobs.begin_sweep().await;
    for p in projects_under(store, "blobs").await? {
        let old = old_objects(store, &format!("blobs/{p}"), cutoff).await?;
        if old.is_empty() {
            continue;
        }
        let paths: Vec<String> = old.iter().map(|m| m.location.to_string()).collect();
        let refs = meta.referenced_blobs(&p, &paths).await?;
        let unreferenced: Vec<ObjectMeta> = old
            .into_iter()
            .filter(|m| !refs.contains(m.location.as_ref()))
            .collect();
        let doomed: Vec<ObjectMeta> = if blobs.exclusive {
            unreferenced
        } else {
            // Re-check each candidate (no lock held): ingest refreshes an old blob it reuses (`put_blobs`).
            let heads: Vec<_> = unreferenced.iter().map(|m| store.head(&m.location)).collect();
            let heads: Vec<_> = futures::stream::iter(heads).buffered(HEAD_CONCURRENCY).collect().await;
            let mut doomed = Vec::new();
            for (m, h) in unreferenced.into_iter().zip(heads) {
                match h {
                    Ok(h) if h.last_modified < cutoff => doomed.push(m),
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            doomed
        };
        if o.dry_run {
            r.orphan_blobs += doomed.len();
            r.freed_bytes += doomed.iter().map(|m| m.size).sum::<u64>();
            continue;
        }
        // Chunks under the guard's write lock, minus the paths a `put_blobs` touched since the sweep began.
        for chunk in doomed.chunks(BLOB_CHUNK) {
            let paths: Vec<Path> = chunk.iter().map(|m| m.location.clone()).collect();
            let gone: HashSet<Path> = blobs.delete_unless_recent(store, paths).await?.into_iter().collect();
            let deleted: Vec<&ObjectMeta> = chunk.iter().filter(|m| gone.contains(&m.location)).collect();
            r.orphan_blobs += deleted.len();
            r.freed_bytes += deleted.iter().map(|m| m.size).sum::<u64>();
        }
    }
    Ok(())
}

/// One full pass: retention (data clock), purge and orphan sweep (wall clock), job pruning; each clock is read once.
/// `blobs` guards the blob sweep against concurrent `put_blobs` (the server passes its pipeline's guard). Offline
/// callers pass `&BlobGuard::exclusive()`: they hold every WAL lock, so no other writer exists.
pub async fn run_gc(
    meta: &dyn Metastore,
    store: &dyn ObjectStore,
    clocks: &Clocks,
    o: &GcOptions,
    blobs: &BlobGuard,
) -> Result<GcReport, GcError> {
    let (data_now_ms, wall_now_ms) = ((clocks.data)(), (clocks.wall)());
    let mut r = GcReport {
        expired_segments: expire(meta, data_now_ms, o.dry_run).await?,
        ..Default::default()
    };
    purge(meta, store, wall_now_ms, o, &mut r).await?;
    sweep_orphans(meta, store, wall_now_ms, o, &mut r, blobs).await?;
    if !o.dry_run {
        let keep = crate::compact::JOB_RETENTION.as_millis() as i64;
        meta.prune_compaction_jobs(wall_now_ms.saturating_sub(keep)).await?;
    }
    Ok(r)
}
