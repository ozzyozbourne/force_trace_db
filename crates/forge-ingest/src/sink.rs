//! One segment output, shared by flush and compaction: the Vortex writer task plus the row observers (the blob refs
//! and the `.fts` builder). It knows every object it may create, so cleanup can delete them.
use crate::IngestError;
use arrow::record_batch::RecordBatch;
use forge_index::FtsBuilder;
use forge_meta::NewSegment;
use forge_model::catalog::paths;
use forge_storage::{SegmentSpec, SegmentStore, StorageError, StorageResult, WrittenSegment};
use futures::channel::mpsc::Sender;
use futures::future::{AbortHandle, Aborted, abortable};
use futures::{SinkExt, StreamExt};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::JoinHandle;

/// The `.fts` task's outcome: (rows indexed, file bytes) once uploaded.
type FtsOutcome = Result<(u32, u64), FtsError>;

enum FtsError {
    /// The build failed (a bug, or a segment over u32 rows): the segment commits without a sidecar.
    Build(forge_index::IndexError),
    /// The upload failed: the segment fails like a `.vortex` upload (the flush retries it).
    Put(StorageError),
}

pub struct SegmentSink {
    objects: [String; 2],
    tx: Sender<RecordBatch>,
    /// The writer, on the blocking pool (the Vortex encode is CPU work). Some until `finish`.
    handle: Option<JoinHandle<Result<StorageResult<WrittenSegment>, Aborted>>>,
    /// The `.fts` builder + upload, on the blocking pool beside the writer, fed the same batches in the same order
    /// (invariant 2). `fts_tx` is None once the builder stopped early (its error comes out of `finish`).
    fts_tx: Option<Sender<RecordBatch>>,
    fts: Option<JoinHandle<Result<FtsOutcome, Aborted>>>,
    /// Dropping an unfinished sink aborts the writer and the `.fts` task (nothing is uploaded).
    aborts: [AbortHandle; 2],
    blobs: BTreeSet<String>,
    /// (term, row) pairs the `.fts` builder holds; updated after each batch, so it lags `push` by <= 3 batches.
    postings: Arc<AtomicU64>,
}

impl SegmentSink {
    /// Starts the writer for `spec`. The rows pushed must follow `SegmentStore::write_segment`'s contract.
    pub fn open(store: Arc<dyn SegmentStore>, spec: SegmentSpec) -> Result<Self, IngestError> {
        let path = paths::segment_path(&spec.project_id, spec.level, spec.partition_hour, spec.id)?;
        let objects = paths::segment_objects(&path);
        let rt = tokio::runtime::Handle::current();

        let (fts_tx, mut fts_rx) = futures::channel::mpsc::channel::<RecordBatch>(2);
        let (os, fts_path) = (
            store.object_store(),
            object_store::path::Path::from(objects[1].as_str()),
        );
        let postings = Arc::new(AtomicU64::new(0));
        let seen = postings.clone();
        let (build, fts_abort) = abortable(async move {
            let mut b = FtsBuilder::default();
            while let Some(batch) = fts_rx.next().await {
                b.add_batch(&batch).map_err(FtsError::Build)?;
                seen.store(b.postings(), Ordering::Relaxed);
            }
            let (bytes, st) = b.finish().map_err(FtsError::Build)?;
            forge_storage::put_create(os.as_ref(), &fts_path, bytes)
                .await
                .map_err(FtsError::Put)?;
            Ok((st.rows, st.file_bytes))
        });
        let rt2 = rt.clone();
        let fts = tokio::task::spawn_blocking(move || rt2.block_on(build));

        let (tx, rx) = futures::channel::mpsc::channel::<RecordBatch>(2);
        let (write, abort) = abortable(async move { store.write_segment(spec, rx.map(Ok).boxed()).await });
        let handle = tokio::task::spawn_blocking(move || rt.block_on(write));
        Ok(Self {
            objects,
            tx,
            handle: Some(handle),
            fts_tx: Some(fts_tx),
            fts: Some(fts),
            aborts: [abort, fts_abort],
            blobs: BTreeSet::new(),
            postings,
        })
    }

    /// (term, row) pairs in the `.fts` builder so far (D3 posting cap; soft, lags `push`).
    pub fn postings(&self) -> u64 {
        self.postings.load(Ordering::Relaxed)
    }

    /// Every object this segment can have (`.vortex`, `.fts`), whether or not it was written.
    pub fn objects(&self) -> impl Iterator<Item = object_store::path::Path> + '_ {
        self.objects.iter().map(|p| object_store::path::Path::from(p.as_str()))
    }

    /// Feeds one batch to the writer and the observers. If the writer stopped early, returns its error.
    pub async fn push(&mut self, b: RecordBatch) -> Result<(), IngestError> {
        self.blobs
            .extend(forge_model::blob::blob_refs_in(std::slice::from_ref(&b)));
        if let Some(fts) = &mut self.fts_tx
            && fts.send(b.clone()).await.is_err()
        {
            self.fts_tx = None; // the builder failed; `finish` reports it
        }
        if self.tx.send(b).await.is_err() {
            join(self.handle.take().expect("not finished")).await?;
            return Err(IngestError::Internal("segment writer stopped".into()));
        }
        Ok(())
    }

    /// Ends the input; the uploads go on (`finish` waits for them).
    pub fn close(&mut self) {
        self.tx.close_channel();
        self.fts_tx = None;
    }

    /// Ends the input and waits until the writer and the `.fts` task have stopped, ignoring their results (cleanup
    /// after a failure: nothing they started is still being written afterwards). Not an abort: dropping a store
    /// `put` future does not stop a write already handed to a blocking thread.
    pub async fn wait(mut self) {
        self.close();
        if let Some(h) = self.handle.take() {
            let _ = h.await;
        }
        if let Some(h) = self.fts.take() {
            let _ = h.await;
        }
    }

    /// Ends the input and waits for both uploads. `fts_byte_size` is set only once the `.fts` landed; a failed
    /// build commits the segment without a sidecar (search scans it), a failed `.fts` upload fails the segment.
    pub async fn finish(mut self) -> Result<NewSegment, IngestError> {
        self.close();
        let w = join(self.handle.take().expect("not finished")).await;
        let fts = self.fts.take().expect("not finished").await;
        let mut w = w?;
        w.meta.fts_byte_size = match fts {
            Ok(Ok(Ok((rows, bytes)))) if u64::from(rows) == w.meta.row_count => Some(bytes),
            Ok(Ok(Ok((rows, _)))) => {
                tracing::error!(segment = %w.meta.id, rows, expected = w.meta.row_count, ".fts row count differs; segment committed without it");
                None
            }
            Ok(Ok(Err(FtsError::Put(e)))) => return Err(e.into()),
            Ok(Ok(Err(FtsError::Build(e)))) => {
                tracing::error!(segment = %w.meta.id, error = %e, ".fts build failed; segment committed without it");
                None
            }
            Ok(Err(Aborted)) => return Err(IngestError::Internal(".fts task aborted".into())),
            Err(e) => {
                tracing::error!(segment = %w.meta.id, error = %e, ".fts build panicked; segment committed without it");
                None
            }
        };
        Ok(NewSegment {
            meta: w.meta,
            traces: w.traces,
            blobs: std::mem::take(&mut self.blobs).into_iter().collect(),
        })
    }
}

async fn join(h: JoinHandle<Result<StorageResult<WrittenSegment>, Aborted>>) -> Result<WrittenSegment, IngestError> {
    let w = h
        .await
        .map_err(|e| IngestError::Internal(format!("segment writer: {e}")))?
        .map_err(|_| IngestError::Internal("segment writer aborted".into()))?;
    Ok(w?)
}

impl Drop for SegmentSink {
    fn drop(&mut self) {
        if self.handle.is_some() || self.fts.is_some() {
            self.aborts.iter().for_each(AbortHandle::abort);
        }
    }
}
