//! The write path: WAL + memtable + flush behind the `Ingest` trait.
use crate::blob::offload_blobs;
use crate::flush::{FlushCtx, FlushReport, flush_frozen};
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
use forge_model::retry::Backoff;
use forge_model::row::{SpanBatchBuilder, SpanRow, validate_batch};
use forge_model::schema::{col, col_idx};
use forge_storage::{SegmentStore, StorageError};
use object_store::ObjectStore;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

type Item = (RecordBatch, usize); // (batch, accounted bytes); version column stamped on the WAL thread

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
    flush_interval: Duration,
    flush: FlushCtx,
    flush_lock: tokio::sync::Mutex<()>,
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
    pub async fn open(
        cfg: &ForgeConfig,
        meta: Arc<dyn Metastore>,
        segments: Arc<dyn SegmentStore>,
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
            backoff: Backoff::default(),
        };

        let (tx, mut rx) = tokio::sync::mpsc::channel::<(u64, RecordBatch, usize)>(64);
        let rlog = log.clone();
        let reader = tokio::task::spawn_blocking(move || {
            rlog.replay(flushed, |seq, payload| {
                let b = wal::decode_batch(payload).map_err(|e| e.to_string())?;
                validate_batch(&b).map_err(|e| e.to_string())?;
                let len = accounted_bytes(&b);
                tx.blocking_send((seq, b, len))
                    .map_err(|_| "replay aborted".to_string())
            })
        });
        let mut replay_flushes = 0;
        while let Some((seq, b, len)) = rx.recv().await {
            let v = b.column(col_idx(col::VERSION)).as_primitive::<Int64Type>();
            let vmax = arrow::compute::max(v).unwrap_or(0) as u64;
            versions.observe(vmax);
            mem.insert(seq, b, len).map_err(|e| internal(&e))?;
            if mem.flush_due(Instant::now()) && mem.unflushed_bytes() >= flush_bytes {
                let f = mem.freeze().expect("non-empty");
                flush_frozen(&flush, f).await.map_err(|e| internal(&e))?;
                replay_flushes += 1;
            }
        }
        let replay = reader.await.map_err(|e| internal(&e))??;

        // P2 / invariant 9: versions are reserved here, on the WAL thread, in seq order.
        let v = versions.clone();
        let encode = move |item: &mut Item| -> Vec<u8> {
            let r = v.reserve(item.0.num_rows() as u64);
            match with_versions(&item.0, r).and_then(|b| {
                let p = wal::encode_batch(&b)?;
                item.0 = b;
                Ok(p)
            }) {
                Ok(p) => p,
                Err(e) => {
                    // Unreachable: the batch was validated in `submit`.
                    tracing::error!(error = %e, "WAL encode failed; aborting");
                    std::process::abort()
                }
            }
        };
        let m2 = mem.clone();
        let on_durable = move |items: &mut Vec<(u64, Item)>| {
            for (seq, (b, len)) in items.drain(..) {
                if let Err(e) = m2.insert(seq, b, len) {
                    tracing::error!(error = %e, "memtable invariant broken; aborting");
                    std::process::abort();
                }
            }
        };
        let wal: Wal<Item> = log.start(replay.next_seq, encode, on_durable)?;
        let watermark = flush.meta.wal_checkpoint(&node_id).await?.map_or(0, |c| c.flushed_seq);
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
                flush_interval: cfg.memtable.flush_interval,
                flush,
                flush_lock: tokio::sync::Mutex::new(()),
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
        flush_frozen(&self.flush, f)
            .await
            .map(Some)
            .map_err(|e| IngestError::Internal(e.to_string()))
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
    pub fn memtable(&self) -> Arc<MemtableSet> {
        self.mem.clone()
    }
}

#[async_trait::async_trait]
impl Ingest for Pipeline {
    async fn submit(&self, mut rows: Vec<SpanRow>) -> Result<(), IngestError> {
        if rows.is_empty() {
            return Err(IngestError::Invalid("empty batch".into()));
        }
        if self.mem.unflushed_bytes() > self.memory_budget {
            return Err(IngestError::Backpressure);
        }
        offload_blobs(&mut rows, self.blobs.as_ref(), self.blob_threshold)
            .await
            .map_err(StorageError::from)?;
        let item = tokio::task::spawn_blocking(move || -> Result<Item, IngestError> {
            let mut bb = SpanBatchBuilder::with_capacity(rows.len());
            for r in &mut rows {
                r.version = 0; // stamped on the WAL thread
                bb.push(r)?;
            }
            let batch = bb.finish()?;
            validate_batch(&batch)?;
            let bytes = accounted_bytes(&batch);
            Ok((batch, bytes))
        })
        .await
        .map_err(|e| IngestError::Internal(e.to_string()))??;
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
    let tick = (p.flush_interval / 4).clamp(Duration::from_millis(50), Duration::from_secs(1));
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
                _ = tokio::time::sleep(Backoff::default().max) => {}
            }
        }
    }
}
