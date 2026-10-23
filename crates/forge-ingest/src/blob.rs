//! Oversized payload offload: `blobs/<project>/<sha256>` + a JSON reference stored in the column.
use bytes::Bytes;
use forge_model::blob::{blob_ref, parse_blob_ref};
use forge_model::catalog::paths::blob_path;
use forge_model::ids::ProjectId;
use forge_model::row::SpanRow;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// A reused blob older than this is rewritten (Overwrite, same bytes) to bump last_modified. Far below the
/// 24 h orphan grace.
pub const BLOB_REFRESH: chrono::TimeDelta = chrono::TimeDelta::hours(1);

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

/// Writes the exact UTF-8 bytes of each planned blob. Content-addressed, so writes are idempotent:
/// `PutMode::Create` + AlreadyExists = success. Returns the number of distinct blobs written (or found).
pub async fn put_blobs(puts: HashMap<String, Bytes>, store: &dyn ObjectStore) -> object_store::Result<usize> {
    let n = puts.len();
    futures::stream::iter(puts)
        .map(|(path, bytes)| async move {
            let path = Path::from(path);
            let opts = PutOptions::from(PutMode::Create);
            match store.put_opts(&path, bytes.clone().into(), opts).await {
                Ok(_) => Ok(()),
                // Reused blob: refresh it when old, so the orphan sweep (last_modified < now - 24 h and no
                // blob_refs row) cannot take it before this row is flushed (lifecycle.md §1.4).
                Err(object_store::Error::AlreadyExists { .. }) => {
                    let stale = store
                        .head(&path)
                        .await
                        .is_ok_and(|m| chrono::Utc::now().signed_duration_since(m.last_modified) > BLOB_REFRESH);
                    if stale {
                        store.put(&path, bytes.into()).await?;
                    }
                    Ok(())
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
    put_blobs(plan_blobs(rows, threshold), store).await
}
