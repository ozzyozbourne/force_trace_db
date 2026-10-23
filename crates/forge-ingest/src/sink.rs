//! One segment output, shared by flush and compaction: the Vortex writer task plus the row observers (today the
//! blob refs; M4 adds the `.fts` builder here). It knows every object it may create, so cleanup can delete them.
use crate::IngestError;
use arrow::record_batch::RecordBatch;
use forge_meta::NewSegment;
use forge_model::catalog::paths;
use forge_storage::{SegmentSpec, SegmentStore, StorageResult, WrittenSegment};
use futures::future::{AbortHandle, Aborted, abortable};
use futures::{SinkExt, StreamExt};
use std::collections::BTreeSet;
use std::sync::Arc;

pub struct SegmentSink {
    objects: [String; 2],
    tx: futures::channel::mpsc::Sender<RecordBatch>,
    /// The writer, on the blocking pool (the Vortex encode is CPU work). Some until `finish`.
    handle: Option<tokio::task::JoinHandle<Result<StorageResult<WrittenSegment>, Aborted>>>,
    /// Dropping an unfinished sink aborts the writer (nothing is uploaded).
    abort: AbortHandle,
    blobs: BTreeSet<String>,
}

impl SegmentSink {
    /// Starts the writer for `spec`. The rows pushed must follow `SegmentStore::write_segment`'s contract.
    pub fn open(store: Arc<dyn SegmentStore>, spec: SegmentSpec) -> Result<Self, IngestError> {
        let path = paths::segment_path(&spec.project_id, spec.level, spec.partition_hour, spec.id)?;
        let (tx, rx) = futures::channel::mpsc::channel::<RecordBatch>(2);
        let (write, abort) = abortable(async move { store.write_segment(spec, rx.map(Ok).boxed()).await });
        let rt = tokio::runtime::Handle::current();
        let handle = tokio::task::spawn_blocking(move || rt.block_on(write));
        Ok(Self {
            objects: paths::segment_objects(&path),
            tx,
            handle: Some(handle),
            abort,
            blobs: BTreeSet::new(),
        })
    }

    /// Every object this segment can have (`.vortex`, `.fts`), whether or not it was written.
    pub fn objects(&self) -> impl Iterator<Item = object_store::path::Path> + '_ {
        self.objects.iter().map(|p| object_store::path::Path::from(p.as_str()))
    }

    /// Feeds one batch to the writer and the observers. If the writer stopped early, returns its error.
    pub async fn push(&mut self, b: RecordBatch) -> Result<(), IngestError> {
        self.blobs
            .extend(forge_model::blob::blob_refs_in(std::slice::from_ref(&b)));
        if self.tx.send(b).await.is_err() {
            join(self.handle.take().expect("not finished")).await?;
            return Err(IngestError::Internal("segment writer stopped".into()));
        }
        Ok(())
    }

    /// Ends the input; the upload goes on (`finish` waits for it).
    pub fn close(&mut self) {
        self.tx.close_channel();
    }

    /// Ends the input and waits for the upload.
    pub async fn finish(mut self) -> Result<NewSegment, IngestError> {
        self.close();
        let w = join(self.handle.take().expect("not finished")).await?;
        Ok(NewSegment {
            meta: w.meta,
            traces: w.traces,
            blobs: std::mem::take(&mut self.blobs).into_iter().collect(),
        })
    }
}

async fn join(
    h: tokio::task::JoinHandle<Result<StorageResult<WrittenSegment>, Aborted>>,
) -> Result<WrittenSegment, IngestError> {
    let w = h
        .await
        .map_err(|e| IngestError::Internal(format!("segment writer: {e}")))?
        .map_err(|_| IngestError::Internal("segment writer aborted".into()))?;
    Ok(w?)
}

impl Drop for SegmentSink {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.abort.abort();
        }
    }
}
