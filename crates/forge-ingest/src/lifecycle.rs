//! Lifecycle (M3): retention, purge of deleted segments, orphan GC of `segments/` and `blobs/`.
//!
//! Order everywhere: metastore first, objects after a grace, rows last.
//! - retention marks whole segments `deleted` (one txn per batch); queries stop seeing them at once;
//! - purge deletes the objects of segments deleted more than `gc_grace` ago (in-flight readers are done), then
//!   forgets their rows (trace_index, blob_refs, segments). A crash in between re-runs: NotFound = success;
//! - the orphan sweep deletes objects that no segments row names and blobs that no blob_refs row names, both
//!   older than `orphan_grace` (object last_modified).
use forge_meta::{MetaError, Metastore};
use forge_model::catalog::{Level, SegmentMeta, SegmentState, paths};
use forge_model::ids::ProjectId;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use std::collections::HashSet;
use std::time::Duration;

/// Rows per purge batch, L1 hours per retention txn.
pub const BATCH: usize = 1000;
const DAY_NS: i64 = 86_400 * 1_000_000_000;

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
    /// Sweep `blobs/` too. False when a referencing row might be unflushed for longer than `orphan_grace`
    /// (`forge gc` while a server runs, or with WAL records past the checkpoint; lifecycle.md §4).
    pub sweep_blobs: bool,
    /// Report only; change nothing.
    pub dry_run: bool,
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
/// (`Metastore::expire_segments`). Returns the count. Dry run: counts the candidates (`list_segments` +
/// `SegmentMeta::expires`, skipping L1 hours with a compacting segment) without marking.
pub async fn expire(meta: &dyn Metastore, data_now_ms: i64, dry_run: bool) -> Result<usize, GcError> {
    let mut n = 0;
    for p in meta.list_projects().await? {
        let Some(days) = p.retention_days else { continue };
        let cutoff = data_now_ms
            .saturating_mul(1_000_000)
            .saturating_sub(i64::from(days) * DAY_NS);
        if dry_run {
            let f = forge_meta::SegmentFilter {
                project_id: Some(p.id.clone()),
                ..Default::default()
            };
            let segs = meta.list_segments(&f).await?;
            let busy: HashSet<_> = segs
                .iter()
                .filter(|s| s.state == SegmentState::Compacting && s.level == Level::L1)
                .map(|s| s.partition_hour)
                .collect();
            n += segs
                .iter()
                .filter(|s| s.state == SegmentState::Live && s.expires(cutoff))
                .filter(|s| s.level == Level::L0 || !busy.contains(&s.partition_hour))
                .count();
            continue;
        }
        loop {
            let done = meta.expire_segments(&p.id, cutoff, BATCH).await?;
            n += done.len();
            if !done.is_empty() {
                tracing::info!(project = %p.id, segments = done.len(), days, "retention: segments expired");
            }
            if !done.iter().any(|s| s.level == Level::L1) {
                break;
            }
        }
    }
    Ok(n)
}

/// Both objects of a segment (`.fts` may not exist: NotFound is success).
fn segment_objects(s: &SegmentMeta) -> [Path; 2] {
    [Path::from(s.path.as_str()), Path::from(paths::fts_path(&s.path))]
}

/// `delete_stream` over `paths` (S3 batches 1000 per request, Azure 256, local/GCS 10 concurrent single deletes).
/// NotFound is success and does not stop the stream: local/GCS/Azure report a missing object, S3 does not.
/// Returns the number reported deleted.
pub async fn delete_all(store: &dyn ObjectStore, paths: Vec<Path>) -> Result<usize, object_store::Error> {
    if paths.is_empty() {
        return Ok(0);
    }
    let input = futures::stream::iter(paths.into_iter().map(Ok)).boxed();
    let mut out = store.delete_stream(input);
    let mut n = 0;
    while let Some(r) = out.next().await {
        match r {
            Ok(_) => n += 1,
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

/// Purge: objects of segments deleted before `now - gc_grace`, then their rows.
pub async fn purge(
    meta: &dyn Metastore,
    store: &dyn ObjectStore,
    now_ms: i64,
    o: &GcOptions,
    r: &mut GcReport,
) -> Result<(), GcError> {
    let before = now_ms.saturating_sub(o.gc_grace.as_millis() as i64);
    loop {
        let segs = meta.purge_candidates(before, BATCH).await?;
        if o.dry_run {
            r.purged_segments += segs.len();
            r.freed_bytes += segs
                .iter()
                .map(|s| s.byte_size + s.fts_byte_size.unwrap_or(0))
                .sum::<u64>();
            return Ok(());
        }
        let objects = segs.iter().flat_map(segment_objects).collect();
        r.deleted_objects += delete_all(store, objects).await?;
        forge_model::failpoint::hit("gc_after_object_delete");
        let ids: Vec<_> = segs.iter().map(|s| s.id).collect();
        r.purged_segments += meta.forget_segments(&ids).await?;
        r.freed_bytes += segs
            .iter()
            .map(|s| s.byte_size + s.fts_byte_size.unwrap_or(0))
            .sum::<u64>();
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

/// The `.vortex` path a segment object belongs to; None for files that are not segment objects (kept).
fn owning_segment(key: &str) -> Option<String> {
    if key.ends_with(".vortex") {
        Some(key.to_string())
    } else {
        key.strip_suffix(".fts").map(|s| format!("{s}.vortex"))
    }
}

/// Orphan sweep. The listing comes first and the metastore read second: an object committed in between is in
/// the known set; one committed later is younger than `orphan_grace` and was never a candidate.
pub async fn sweep_orphans(
    meta: &dyn Metastore,
    store: &dyn ObjectStore,
    now_ms: i64,
    o: &GcOptions,
    r: &mut GcReport,
    blob_lock: Option<&tokio::sync::RwLock<()>>,
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
            .filter(|m| owning_segment(m.location.as_ref()).is_some_and(|s| !known.contains(&s)))
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
    for p in projects_under(store, "blobs").await? {
        let old = old_objects(store, &format!("blobs/{p}"), cutoff).await?;
        if old.is_empty() {
            continue;
        }
        let refs = meta.blob_refs(&p).await?;
        // put_blobs refreshes under the read lock: no refresh lands between our head and delete.
        let _guard = match blob_lock {
            Some(l) => Some(l.write().await),
            None => None,
        };
        let mut doomed = Vec::new();
        for m in old.into_iter().filter(|m| !refs.contains(m.location.as_ref())) {
            // Re-check right before the delete: ingest refreshes an old blob it reuses (`put_blobs`).
            match store.head(&m.location).await {
                Ok(h) if h.last_modified < cutoff => doomed.push(m),
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        r.orphan_blobs += doomed.len();
        r.freed_bytes += doomed.iter().map(|m| m.size).sum::<u64>();
        if !o.dry_run {
            delete_all(store, doomed.into_iter().map(|m| m.location).collect()).await?;
        }
    }
    Ok(())
}

/// One full pass: retention (data clock), purge and orphan sweep (wall clock), job pruning.
pub async fn run_gc(
    meta: &dyn Metastore,
    store: &dyn ObjectStore,
    data_now_ms: i64,
    wall_now_ms: i64,
    o: &GcOptions,
    blob_lock: Option<&tokio::sync::RwLock<()>>,
) -> Result<GcReport, GcError> {
    let mut r = GcReport {
        expired_segments: expire(meta, data_now_ms, o.dry_run).await?,
        ..Default::default()
    };
    purge(meta, store, wall_now_ms, o, &mut r).await?;
    sweep_orphans(meta, store, wall_now_ms, o, &mut r, blob_lock).await?;
    if !o.dry_run {
        let keep = crate::compact::JOB_RETENTION.as_millis() as i64;
        meta.prune_compaction_jobs(wall_now_ms.saturating_sub(keep)).await?;
    }
    Ok(r)
}
