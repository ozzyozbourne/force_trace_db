//! The write path: WAL + memtable + flush behind the `Ingest` trait.
use crate::blob::{BlobGuard, plan_blobs, put_blobs};
use crate::flush::{FlushCtx, FlushOptions, FlushReport, flush_frozen};
use crate::memtable::{MemtableSet, accounted_bytes};
use crate::version::VersionAllocator;
use crate::wal::{self, Wal, WalLog};
use crate::{Ingest, IngestError};
use arrow::array::{AsArray, Int64Array};
use arrow::datatypes::Int64Type;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use forge_meta::Metastore;
use forge_model::config::ForgeConfig;
use forge_model::row::{SpanBatchBuilder, SpanRow, validate_batch};
use forge_model::schema::{col, col_idx};
use forge_storage::{SegmentStore, StorageError};
use object_store::ObjectStore;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

type Rows = (RecordBatch, usize); // (batch, accounted bytes)

/// One append: the rows with version 0 and their WAL payload, encoded off the WAL thread. The WAL thread stamps
/// the versions into both (`payload[version_at..]` is the version values buffer).
struct Item {
    rows: Rows,
    payload: Vec<u8>,
    version_at: usize,
    /// The request's bytes in the in-flight count until the rows are in the memtable (dropped after the insert),
    /// or until the item is dropped unqueued.
    reserved: Option<Reservation>,
}

/// Builds and validates the batch (version 0) and encodes its WAL payload. CPU work: call off the async workers.
fn build_item(mut rows: Vec<SpanRow>) -> Result<Item, IngestError> {
    let mut bb = SpanBatchBuilder::with_capacity(rows.len());
    for r in &mut rows {
        r.version = 0; // stamped on the WAL thread
        bb.push(r)?;
    }
    let batch = bb.finish()?;
    validate_batch(&batch)?;
    let bytes = accounted_bytes(&batch);
    let (payload, version_at) = wal::encode_payload(&batch)?;
    Ok(Item {
        rows: (batch, bytes),
        payload,
        version_at,
        reserved: None,
    })
}

/// Accounted bytes of requests admitted by the budget check but not yet in the memtable; released on drop.
struct Reservation {
    inflight: Arc<AtomicUsize>,
    bytes: usize,
}

impl Reservation {
    /// Admits `bytes` iff memtable + OTHER requests' in-flight bytes are within `budget` (this request's own size
    /// is not checked), so memtable + in-flight exceeds the budget by at most one request; a request larger than
    /// the budget still gets in when nothing else is pending.
    ///
    /// The reservation is added BEFORE `unflushed()` is read (both SeqCst, like the memtable's add before the
    /// writer releases a reservation): bytes moving from in-flight to the memtable meanwhile are counted twice,
    /// never zero times.
    fn try_new(
        inflight: &Arc<AtomicUsize>,
        bytes: usize,
        unflushed: impl FnOnce() -> usize,
        budget: usize,
    ) -> Option<Self> {
        let others = inflight.fetch_add(bytes, Ordering::SeqCst);
        let r = Self {
            inflight: inflight.clone(),
            bytes,
        };
        (unflushed() + others <= budget).then_some(r) // dropping `r` releases a refused reservation
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.inflight.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

#[derive(Debug)]
pub struct OpenReport {
    pub replay: wal::ReplayStats,
    pub replay_flushes: usize,
    pub next_version: u64,
}

pub struct Pipeline {
    wal: Wal<Item>,
    mem: Arc<MemtableSet>,
    blobs: Arc<dyn ObjectStore>,
    blob_threshold: usize,
    memory_budget: usize,
    /// See `Reservation`.
    inflight: Arc<AtomicUsize>,
    flush: FlushCtx,
    flush_lock: tokio::sync::Mutex<()>,
    /// Blob sweep vs `put_blobs` (plans/phase-4 §2.9).
    blob_guard: BlobGuard,
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("wal_dir", &self.flush.wal.dir())
            .field("unflushed_bytes", &self.mem.unflushed_bytes())
            .finish_non_exhaustive()
    }
}

impl Pipeline {
    /// The guard the blob sweep uses (`lifecycle::run_gc`).
    pub fn blob_guard(&self) -> &BlobGuard {
        &self.blob_guard
    }

    pub async fn open(
        cfg: &ForgeConfig,
        meta: Arc<dyn Metastore>,
        segments: Arc<dyn SegmentStore>,
    ) -> Result<(Arc<Self>, OpenReport), IngestError> {
        Self::open_with(cfg, meta, segments, FlushOptions::default()).await
    }

    /// `open` with explicit flush knobs (tests: a short backoff, a small merge chunk).
    pub async fn open_with(
        cfg: &ForgeConfig,
        meta: Arc<dyn Metastore>,
        segments: Arc<dyn SegmentStore>,
        flush_opts: FlushOptions,
    ) -> Result<(Arc<Self>, OpenReport), IngestError> {
        let internal = |e: &dyn std::fmt::Display| IngestError::Internal(e.to_string());
        let node_id = cfg.node.id.clone();
        let flush_bytes = cfg.memtable.flush_bytes.as_u64() as usize;
        let log = {
            let (c, n) = (cfg.wal.clone(), node_id.clone());
            Arc::new(
                tokio::task::spawn_blocking(move || WalLog::open(&c, &n))
                    .await
                    .map_err(|e| internal(&e))??,
            )
        };
        let flushed = meta.wal_checkpoint(&node_id).await?.map_or(0, |c| c.flushed_seq);
        let versions = Arc::new(VersionAllocator::new(meta.max_version().await?));
        let mem = Arc::new(MemtableSet::new(flushed, flush_bytes, cfg.memtable.flush_interval));
        let blobs = segments.object_store();
        let flush = FlushCtx {
            node_id: node_id.clone(),
            segments,
            meta,
            mem: mem.clone(),
            wal: log.clone(),
            versions: versions.clone(),
            backoff: flush_opts.backoff,
            merge_chunk_bytes: flush_opts.merge_chunk_bytes,
        };

        let (tx, mut rx) = tokio::sync::mpsc::channel::<(u64, Rows)>(64);
        let rlog = log.clone();
        let reader = tokio::task::spawn_blocking(move || {
            rlog.replay(flushed, |seq, payload| {
                let b = wal::decode_batch(payload).map_err(|e| e.to_string())?;
                validate_batch(&b).map_err(|e| e.to_string())?;
                let len = accounted_bytes(&b);
                tx.blocking_send((seq, (b, len)))
                    .map_err(|_| "replay aborted".to_string())
            })
        });
        let (mut replay_flushes, mut watermark) = (0, flushed);
        while let Some((seq, (b, len))) = rx.recv().await {
            let v = b.column(col_idx(col::VERSION)).as_primitive::<Int64Type>();
            let vmax = arrow::compute::max(v).unwrap_or(0) as u64;
            versions.observe(vmax);
            mem.insert(seq, b, len).map_err(|e| internal(&e))?;
            // Never a frozen memtable during replay (each one is flushed right here), so this is the size trigger.
            if mem.unflushed_bytes() >= flush_bytes {
                let f = mem.freeze().expect("non-empty");
                watermark = flush_frozen(&flush, f).await?.watermark;
                replay_flushes += 1;
            }
        }
        let replay = reader.await.map_err(|e| internal(&e))??;
        // Replayed rows are as old as the crash: flush them on the flusher's first tick.
        mem.mark_due();

        // P2 / invariant 9: versions are reserved here, on the WAL thread, in seq order.
        let v = versions.clone();
        // The payload bytes equal encoding the stamped batch (uncompressed IPC, fixed-width version buffer).
        let encode = move |item: &mut Item, buf: &mut Vec<u8>| {
            let r = v.reserve(item.rows.0.num_rows() as u64);
            let at = buf.len() + item.version_at;
            buf.extend_from_slice(&std::mem::take(&mut item.payload));
            wal::stamp_versions(&mut buf[..], at, r.clone());
            item.rows.0 = with_versions(&item.rows.0, r).unwrap_or_else(|e| {
                // Unreachable: the batch was validated in `submit`.
                tracing::error!(error = %e, "WAL encode failed; aborting");
                std::process::abort()
            });
        };
        let m2 = mem.clone();
        let on_durable = move |items: &mut Vec<(u64, Item)>| {
            for (seq, item) in items.drain(..) {
                let Item {
                    rows: (b, len),
                    reserved,
                    ..
                } = item;
                if let Err(e) = m2.insert(seq, b, len) {
                    tracing::error!(error = %e, "memtable invariant broken; aborting");
                    std::process::abort();
                }
                drop(reserved); // now counted in `unflushed`
            }
        };
        let wal: Wal<Item> = log.start(replay.next_seq, encode, on_durable)?;
        log.truncate(watermark).map_err(wal::WalError::from)?;
        let next_version = versions.high_water() + 1;
        tracing::info!(
            applied = replay.applied,
            skipped = replay.skipped,
            truncated = replay.truncated.is_some(),
            replay_flushes,
            next_seq = replay.next_seq,
            next_version,
            "wal replay done"
        );
        Ok((
            Arc::new(Self {
                wal,
                mem,
                blobs,
                blob_threshold: cfg.ingest.blob_threshold.as_u64() as usize,
                memory_budget: cfg.memtable.memory_budget.as_u64() as usize,
                inflight: Arc::default(),
                flush,
                flush_lock: tokio::sync::Mutex::new(()),
                blob_guard: BlobGuard::with_refresh(cfg.compaction.blob_refresh()),
            }),
            OpenReport {
                replay,
                replay_flushes,
                next_version,
            },
        ))
    }

    /// Freeze (if anything is pending) and flush ONE memtable. Never cancel this future mid-way (invariant 8).
    pub async fn flush_now(&self) -> Result<Option<FlushReport>, IngestError> {
        let _g = self.flush_lock.lock().await;
        let Some(f) = self.mem.freeze() else { return Ok(None) };
        flush_frozen(&self.flush, f).await.map(Some)
    }
    /// Spawns the background flusher. Stop it with `IngestTasks::shutdown`.
    pub fn start_background(self: &Arc<Self>) -> IngestTasks {
        let stop = CancellationToken::new();
        IngestTasks {
            flusher: tokio::spawn(run_flusher(self.clone(), stop.clone())),
            pipeline: self.clone(),
            stop,
        }
    }
    /// Stop accepting appends, drain + fsync the queue, join the writer thread.
    pub async fn close_wal(self: &Arc<Self>) {
        let p = self.clone();
        let _ = tokio::task::spawn_blocking(move || p.wal.shutdown()).await;
    }
    /// Flush the frozen memtable, then the active one.
    pub async fn flush_all(&self) -> Result<(), IngestError> {
        while self.flush_now().await?.is_some() {}
        Ok(())
    }
    /// The query side of the memtable. A snapshot keeps a released memtable's batches alive until the query drops
    /// it, and backpressure does not count them; the query timeout bounds that.
    pub fn memtable_view(&self) -> Arc<dyn forge_model::MemtableView> {
        self.mem.clone()
    }
    pub fn memtable(&self) -> Arc<MemtableSet> {
        self.mem.clone()
    }
}

#[async_trait::async_trait]
impl Ingest for Pipeline {
    async fn submit(&self, rows: Vec<SpanRow>) -> Result<(), IngestError> {
        if rows.is_empty() {
            return Err(IngestError::Invalid("empty batch".into()));
        }
        // Cheap early check; the binding one is the reservation below, once the request's size is known.
        if self.mem.unflushed_bytes() + self.inflight.load(Ordering::Relaxed) > self.memory_budget {
            return Err(IngestError::Backpressure);
        }
        let join = |e: tokio::task::JoinError| IngestError::Internal(e.to_string());
        // Blob refs are planned (hashing) off the workers. Without blobs the item is built in the same task;
        // with blobs the uploads and the build run concurrently. The append waits for both.
        let threshold = self.blob_threshold;
        let (puts, planned) = tokio::task::spawn_blocking(move || -> Result<_, IngestError> {
            let mut rows = rows;
            let puts = plan_blobs(&mut rows, threshold);
            let planned = if puts.is_empty() {
                Ok(build_item(rows)?)
            } else {
                Err(rows)
            };
            Ok((puts, planned))
        })
        .await
        .map_err(join)??;
        let mut item = match planned {
            Ok(item) => item,
            Err(rows) => {
                let put = async {
                    put_blobs(puts, self.blobs.as_ref(), &self.blob_guard)
                        .await
                        .map_err(|e| IngestError::from(StorageError::from(e)))
                };
                let build = async {
                    tokio::task::spawn_blocking(move || build_item(rows))
                        .await
                        .map_err(join)?
                };
                tokio::try_join!(put, build)?.1
            }
        };
        // Bytes between here and the memtable insert count against the budget (they are queued or being written).
        // Rows of a refused request may have uploaded blobs: unreferenced blobs are harmless (content-addressed).
        let reserved = Reservation::try_new(
            &self.inflight,
            item.rows.1,
            || self.mem.unflushed_bytes(),
            self.memory_budget,
        )
        .ok_or(IngestError::Backpressure)?;
        item.reserved = Some(reserved);
        self.wal.append(item).await?;
        Ok(())
    }
}

/// Replaces the version column with `r` in row order.
fn with_versions(b: &RecordBatch, r: Range<u64>) -> Result<RecordBatch, ArrowError> {
    let mut cols = b.columns().to_vec();
    cols[col_idx(col::VERSION)] = Arc::new(Int64Array::from_iter_values(r.map(|v| v as i64)));
    RecordBatch::try_new(b.schema(), cols)
}

/// The pipeline's background tasks (the flusher).
#[derive(Debug)]
pub struct IngestTasks {
    pipeline: Arc<Pipeline>,
    stop: CancellationToken,
    flusher: tokio::task::JoinHandle<()>,
}

impl IngestTasks {
    /// The stop sequence, after the listeners have stopped: close the WAL (queued appends are fsynced and in the
    /// memtable; new appends get `WalError::Closed`), stop the flusher between flushes, then the final flush.
    /// Joining the flusher and the final flush are bounded by `grace`; on a timeout the rows replay from the WAL
    /// on the next start.
    pub async fn shutdown(self, grace: Duration) -> Result<Result<(), IngestError>, tokio::time::error::Elapsed> {
        self.pipeline.close_wal().await;
        self.stop.cancel();
        let final_flush = async {
            let _ = self.flusher.await;
            self.pipeline.flush_all().await
        };
        tokio::time::timeout(grace, final_flush).await
    }
}

/// Background flusher. Cancellation is observed only BETWEEN flushes.
async fn run_flusher(p: Arc<Pipeline>, stop: CancellationToken) {
    let tick = (p.mem.flush_interval() / 4).clamp(Duration::from_millis(50), Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = p.mem.flush_wanted.notified() => {}
            _ = tokio::time::sleep(tick) => {}
        }
        if p.mem.flush_due(Instant::now())
            && let Err(e) = p.flush_now().await
        {
            tracing::error!(error = %e, "flush failed; retrying after backoff");
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = tokio::time::sleep(p.flush.backoff.max) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// L1: the memtable size is read only after this request's bytes are in flight, so a batch moving from
    /// in-flight to the memtable in between is never missed by both.
    #[test]
    fn reservation_counts_itself_before_reading_the_memtable() {
        let inflight = Arc::new(AtomicUsize::new(5));
        let seen = std::cell::Cell::new(0);
        let r = Reservation::try_new(
            &inflight,
            10,
            || {
                seen.set(inflight.load(Ordering::SeqCst));
                0
            },
            100,
        );
        assert_eq!(seen.get(), 15, "own bytes reserved before the memtable read");
        assert!(r.is_some());
        drop(r);
        assert_eq!(inflight.load(Ordering::SeqCst), 5);
        // Admission: memtable + others within the budget, whatever this request's size.
        assert!(Reservation::try_new(&inflight, 1_000, || 95, 100).is_some());
        assert!(Reservation::try_new(&inflight, 1, || 96, 100).is_none());
        assert_eq!(inflight.load(Ordering::SeqCst), 5, "refused reservations are released");
    }
}
