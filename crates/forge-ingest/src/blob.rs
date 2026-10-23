//! Oversized payload offload: `blobs/<project>/<sha256>` + a JSON reference stored in the column.
use bytes::Bytes;
use forge_model::blob::{blob_ref, parse_blob_ref};
use forge_model::catalog::paths::blob_path;
use forge_model::ids::ProjectId;
use forge_model::row::SpanRow;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, PutMode, PutOptions, path::Path};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Replaces every payload column value (`inputs`, `outputs`, `events`, `attributes`) longer than
/// `threshold` bytes with a blob reference, after writing the exact UTF-8 bytes to the object store.
/// Content-addressed, so writes are idempotent: `PutMode::Create` + AlreadyExists = success.
/// Returns the number of distinct blobs written (or found).
pub async fn offload_blobs(
    rows: &mut [SpanRow],
    store: &dyn ObjectStore,
    threshold: usize,
) -> object_store::Result<usize> {
    let mut puts: HashMap<String, Bytes> = HashMap::new();
    for r in rows.iter_mut() {
        let project: ProjectId = r.project_id.clone().expect("normalized rows carry a project");
        for col in [&mut r.inputs, &mut r.outputs, &mut r.events, &mut r.attributes] {
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
    let n = puts.len();
    futures::stream::iter(puts)
        .map(|(path, bytes)| async move {
            let opts = PutOptions::from(PutMode::Create);
            match store.put_opts(&Path::from(path), bytes.into(), opts).await {
                Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
                Err(e) => Err(e),
            }
        })
        .buffer_unordered(8)
        .try_collect::<Vec<()>>()
        .await?;
    Ok(n)
}
