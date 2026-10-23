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
    /// The task was aborted: the segment fails.
    Aborted,
    /// The task panicked: the segment commits without a sidecar.
    Panic(tokio::task::JoinError),
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
    /// (term, row) pairs the `.fts` builder holds; updated after each batch, so it lags `push` by <= 4 batches
    /// (a channel of 2 + 1 sender slot + the batch in `add_batch`).
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
        let fts = match self.fts.take().expect("not finished").await {
            Ok(Ok(r)) => r,
            Ok(Err(Aborted)) => Err(FtsError::Aborted),
            Err(e) => Err(FtsError::Panic(e)),
        };
        let mut w = w?;
        w.meta.fts_byte_size = match fts {
            Ok((rows, bytes)) if u64::from(rows) == w.meta.row_count => Some(bytes),
            Ok((rows, _)) => {
                tracing::error!(segment = %w.meta.id, rows, expected = w.meta.row_count, ".fts row count differs; segment committed without it");
                None
            }
            Err(FtsError::Put(e)) => return Err(e.into()),
            Err(FtsError::Aborted) => return Err(IngestError::Internal(".fts task aborted".into())),
            Err(FtsError::Build(e)) => {
                tracing::error!(segment = %w.meta.id, error = %e, ".fts build failed; segment committed without it");
                None
            }
            Err(FtsError::Panic(e)) => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use forge_model::catalog::{HourBucket, Level, SegmentId};
    use forge_model::row::SpanRow;
    use forge_model::schema::col;
    use forge_model::testing::{batch_from_rows, pid, sid, sort_by_key, tid};
    use forge_storage::ScanRequest;
    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;

    /// RT #7: three batches through the sink; every row's unique token is indexed at exactly the row position the
    /// `.vortex` scan gives it, and each batch's shared token covers exactly that batch's rows.
    #[tokio::test]
    async fn sink_rows_align_across_batches() {
        let store = forge_storage::testing::memory_store();
        let rows: Vec<SpanRow> = (0..90u64)
            .map(|i| SpanRow {
                project_id: Some(pid("p")),
                trace_id: tid(i / 10),
                span_id: sid(i),
                start_time_ns: (i % 7 != 0).then_some(i as i64),
                inputs: Some(format!("{{\"q\":\"row{i}\"}}")),
                version: 1,
                ..Default::default()
            })
            .collect();
        let all = sort_by_key(&batch_from_rows(&rows));
        let spec = SegmentSpec {
            id: SegmentId::new(),
            project_id: pid("p"),
            level: Level::L0,
            partition_hour: HourBucket(0),
        };
        let mut sink = SegmentSink::open(store.clone(), spec).unwrap();
        for (k, (off, len)) in [(0, 20), (20, 30), (50, 40)].into_iter().enumerate() {
            // Tag each batch's rows with a shared token.
            let b = all.slice(off, len);
            let inputs = b.column(forge_model::schema::col_idx(col::INPUTS)).as_string::<i32>();
            let tagged: arrow::array::StringArray = (0..len)
                .map(|i| Some(inputs.value(i).replace("\"}", &format!(" batch{k}\"}}"))))
                .collect();
            let mut cols = b.columns().to_vec();
            cols[forge_model::schema::col_idx(col::INPUTS)] = Arc::new(tagged);
            sink.push(RecordBatch::try_new(b.schema(), cols).unwrap())
                .await
                .unwrap();
        }
        let seg = sink.finish().await.unwrap().meta;
        let len = seg.fts_byte_size.expect("sidecar");
        let req = ScanRequest {
            projection: Some(vec![col::INPUTS]),
            ..Default::default()
        };
        let scanned: Vec<RecordBatch> = store.scan(&seg, req).await.unwrap().try_collect().await.unwrap();
        let cells: Vec<String> = scanned
            .iter()
            .flat_map(|b| {
                let c = b.column(0).as_string::<i32>();
                (0..c.len()).map(|i| c.value(i).to_string()).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(cells.len(), 90);
        let os = store.object_store();
        let loc = object_store::path::Path::from(paths::fts_path(&seg.path).as_str());
        let bytes = os.get(&loc).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.len() as u64, len);
        let fetch = forge_index::read::bytes_fetch(bytes.clone());
        let f = forge_index::FtsFile::open("t", len, &fetch).await.unwrap().0;
        let an = &mut forge_index::Analyzer::default();
        let lookup = async |an: &mut forge_index::Analyzer, t: &str| {
            let p = forge_index::FtsPredicate::Search {
                column: forge_index::FtsColumn::Inputs,
                query: t.into(),
            };
            let m = forge_index::Matcher::new(an, &p).unwrap();
            f.execute(m.query(), &fetch).await.unwrap().0
        };
        for (pos, c) in cells.iter().enumerate() {
            let tok = c.split(['"', ' ']).find(|w| w.starts_with("row")).unwrap();
            assert_eq!(lookup(an, tok).await, [pos as u32].into_iter().collect(), "{tok}");
        }
        for (k, range) in [(0, 0..20u32), (1, 20..50), (2, 50..90)] {
            assert_eq!(lookup(an, &format!("batch{k}")).await, range.collect(), "batch{k}");
        }
        let nulls: Vec<u32> = (0..90u32)
            .filter(|p| {
                let tok = cells[*p as usize]
                    .split(['"', ' '])
                    .find(|w| w.starts_with("row"))
                    .unwrap();
                tok[3..].parse::<u64>().unwrap() % 7 == 0
            })
            .collect();
        assert_eq!(f.null_starts().iter().collect::<Vec<_>>(), nulls);
    }
}
