//! `run_job`: lazy trace slices -> scan + context -> merge (Keep) -> start fill -> dotted backfill -> sort -> route to
//! per-(hour, orphan) writers -> commit (plans/phase-4 §2.8, CR §7).
use super::{CompactCtx, JobReport, JobStats, MAX_OPEN_WRITERS};
use crate::IngestError;
use crate::sink::SegmentSink;
use arrow::array::{Array, ArrayRef, AsArray, StringArray, TimestampNanosecondArray, UInt32Array};
use arrow::compute::{concat_batches, take_record_batch};
use arrow::datatypes::{SchemaRef, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use forge_meta::{CompactionCommit, CompactionJob, NewSegment};
use forge_model::catalog::{HourBucket, JobKind, JobStatus, Level, SegmentId, SegmentMeta, TraceRange};
use forge_model::dotted::{Map, Node, resolve_with_known};
use forge_model::failpoint;
use forge_model::ids::{ProjectId, SpanId, TraceId};
use forge_model::merge::{Cancel, Tombstones, merge_versions_projected, sort_physical};
use forge_model::retry::{Backoff, retry_bounded};
use forge_model::row::span_keys;
use forge_model::schema::{TIMEZONE, col, col_idx, projected_schema, spans_schema};
use forge_storage::{RowSelection, ScanRequest, SegmentSpec, SegmentStore, coalesce_ranges};
use futures::{StreamExt, TryStreamExt};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

const CONTEXT_COLUMNS: [&str; 8] = [
    col::PROJECT_ID,
    col::TRACE_ID,
    col::SPAN_ID,
    col::VERSION,
    col::DELETED,
    col::PARENT_SPAN_ID,
    col::DOTTED_ORDER,
    col::START_TIME,
];

/// Decoded / encoded bytes before the first slice is measured (LC §5.1 measured about 13.6).
const INITIAL_RATIO: f64 = 12.0;
/// The commit's retry: transient errors only, 3 attempts 100 ms apart.
const COMMIT_ATTEMPTS: u32 = 3;
const COMMIT_BACKOFF: Backoff = Backoff {
    initial: Duration::from_millis(100),
    max: Duration::from_millis(100),
};

/// Concurrent reads (inputs, context ranges, trace lists); results stay in request order.
const READ_CONCURRENCY: usize = 4;

type Key = (TraceId, SpanId);
/// What the job knows about a key from L1 rows it does not rewrite: (parent, start, dotted_order).
type Context<'a> = Map<Key, Node<'a, Key>>;

/// The job's traces, cut into slices one at a time with the latest measured ratio (RA M3). A slice is whole traces,
/// so every version of a key is in one slice, and one contiguous row range per input.
pub struct Slices {
    traces: Vec<(TraceId, Vec<(usize, TraceRange)>)>,
    /// Encoded bytes per row of each input.
    bytes_per_row: Vec<f64>,
    pos: usize,
}

impl Slices {
    pub fn new(inputs: &[(SegmentMeta, Vec<TraceRange>)]) -> Self {
        let mut by_trace: BTreeMap<TraceId, Vec<(usize, TraceRange)>> = BTreeMap::new();
        for (i, (_, ranges)) in inputs.iter().enumerate() {
            for r in ranges {
                by_trace.entry(r.trace_id).or_default().push((i, *r));
            }
        }
        Self {
            traces: by_trace.into_iter().collect(),
            bytes_per_row: inputs
                .iter()
                .map(|(s, _)| s.byte_size as f64 / s.row_count.max(1) as f64)
                .collect(),
            pos: 0,
        }
    }

    /// The next slice: consecutive traces whose estimated decoded bytes stay within `budget` (at least one trace),
    /// with each input's row range.
    #[allow(clippy::type_complexity)]
    pub fn next(&mut self, budget: u64, ratio: f64) -> Option<(Vec<TraceId>, Vec<(usize, std::ops::Range<u64>)>)> {
        if self.pos >= self.traces.len() {
            return None;
        }
        let (mut traces, mut ranges, mut acc) = (Vec::new(), BTreeMap::<usize, std::ops::Range<u64>>::new(), 0f64);
        while let Some((t, locs)) = self.traces.get(self.pos) {
            let w: f64 = locs
                .iter()
                .map(|(i, r)| r.row_count as f64 * self.bytes_per_row[*i] * ratio)
                .sum();
            if !traces.is_empty() && acc + w > budget as f64 {
                break;
            }
            for (i, r) in locs {
                let e = ranges.entry(*i).or_insert(r.row_start..r.row_start);
                e.end = r.row_start + r.row_count; // traces ascend: the range stays contiguous per input
            }
            traces.push(*t);
            acc += w;
            self.pos += 1;
        }
        Some((traces, ranges.into_iter().collect()))
    }
}

/// One output run: an hour, main or orphan (null start_time) rows (D3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Run {
    hour: HourBucket,
    orphan: bool,
}

/// Routing of one merged, filled row (D3).
fn route(
    key: Key,
    start: Option<i64>,
    end: Option<i64>,
    sticky: &HashMap<Key, HourBucket>,
    trace_l1_hour: Option<HourBucket>,
    fallback: HourBucket,
) -> Run {
    let orphan = start.is_none();
    if let Some(h) = sticky.get(&key) {
        return Run { hour: *h, orphan };
    }
    let hour = start
        .or(end)
        .map(HourBucket::from_nanos)
        .or(trace_l1_hour)
        .unwrap_or(fallback);
    Run { hour, orphan }
}

/// The kind-specific facts of a job, derived once.
struct JobPlan {
    input_ids: BTreeSet<SegmentId>,
    /// D3 fallback: the newest input L0's flush hour (kind B: the hour itself).
    fallback: HourBucket,
    /// Context rows below this version are older than every row of the job's keys (I2 for kind A, the D1c cut for
    /// kind B); only they may fill a null start (CR §2.4, RD #11).
    older_than: u64,
    /// Kind A: the L1 hours folded in.
    folded_hours: u32,
}

impl From<&CompactionJob> for JobPlan {
    fn from(job: &CompactionJob) -> Self {
        let newer_level = if job.kind == JobKind::L0 { Level::L0 } else { Level::L1 };
        let folded: BTreeSet<HourBucket> = job
            .inputs
            .iter()
            .filter(|s| job.kind == JobKind::L0 && s.level == Level::L1)
            .map(|s| s.partition_hour)
            .collect();
        Self {
            input_ids: job.inputs.iter().map(|s| s.id).collect(),
            fallback: job
                .inputs
                .iter()
                .filter(|s| s.level == Level::L0)
                .max_by_key(|s| s.min_version)
                .or(job.inputs.last())
                .map(|s| s.partition_hour)
                .expect("a job has inputs"),
            older_than: job
                .inputs
                .iter()
                .filter(|s| s.level == newer_level)
                .map(|s| s.min_version)
                .min()
                .unwrap_or(0),
            folded_hours: folded.len() as u32,
        }
    }
}

/// One open output.
struct Writer {
    sink: SegmentSink,
    bytes: u64,
    /// Feed tick of the last batch (the least recently fed writer is finished first).
    fed: u64,
}

struct Writers {
    store: Arc<dyn SegmentStore>,
    project: ProjectId,
    open: BTreeMap<Run, Writer>,
    /// Finished outputs, in output order; their uploads go on until `finish`.
    closing: Vec<SegmentSink>,
    /// Every object of every output started (deleted when the job fails before its commit).
    objects: Vec<object_store::path::Path>,
    tick: u64,
    /// D3: rotate an output once its `.fts` builder holds this many postings.
    max_postings: u64,
}

impl Writers {
    fn new(store: Arc<dyn SegmentStore>, project: ProjectId, max_postings: u64) -> Self {
        Self {
            store,
            project,
            open: BTreeMap::new(),
            closing: Vec::new(),
            objects: Vec::new(),
            tick: 0,
            max_postings,
        }
    }

    fn start(&mut self, run: Run) -> Result<Writer, IngestError> {
        let spec = SegmentSpec {
            id: SegmentId::new(),
            project_id: self.project.clone(),
            level: Level::L1,
            partition_hour: run.hour,
        };
        let sink = SegmentSink::open(self.store.clone(), spec)?;
        self.objects.extend(sink.objects());
        Ok(Writer { sink, bytes: 0, fed: 0 })
    }

    /// Sends one routed, sorted batch (whole traces). Rotates at this trace boundary once the output holds
    /// `limit` decoded bytes; finishes the least recently fed writer when `MAX_OPEN_WRITERS` are open. A later
    /// file of the same run holds higher trace ids, so the files stay key-disjoint.
    async fn send(&mut self, run: Run, b: RecordBatch, limit: u64, stats: &mut JobStats) -> Result<(), IngestError> {
        let size = b.get_array_memory_size() as u64;
        if self
            .open
            .get(&run)
            .is_some_and(|w| w.bytes > 0 && (w.bytes + size > limit || w.sink.postings() >= self.max_postings))
        {
            self.close(run);
            stats.rotated += 1;
        }
        if !self.open.contains_key(&run) {
            if self.open.len() >= MAX_OPEN_WRITERS {
                let lru = *self.open.iter().min_by_key(|(_, w)| w.fed).expect("open").0;
                self.close(lru);
                stats.writer_cap += 1;
            }
            let w = self.start(run)?;
            self.open.insert(run, w);
        }
        self.tick += 1;
        let w = self.open.get_mut(&run).expect("present");
        w.fed = self.tick;
        w.bytes += size;
        w.sink.push(b).await
    }

    /// Ends `run`'s output; its upload completes in the background.
    fn close(&mut self, run: Run) {
        let mut w = self.open.remove(&run).expect("present");
        w.sink.close();
        self.closing.push(w.sink);
    }

    /// Ends every output and waits for all uploads; the outputs in order.
    async fn finish(&mut self) -> Result<Vec<NewSegment>, IngestError> {
        for (_, w) in std::mem::take(&mut self.open) {
            self.closing.push(w.sink);
        }
        let mut out = Vec::with_capacity(self.closing.len());
        let mut sinks = std::mem::take(&mut self.closing).into_iter();
        while let Some(sink) = sinks.next() {
            match sink.finish().await {
                Ok(s) => out.push(s),
                Err(e) => {
                    // The rest stay for `abort`, which waits for them before the cleanup (no late upload).
                    self.closing.extend(sinks);
                    return Err(e);
                }
            }
        }
        Ok(out)
    }

    /// Ends every writer and waits for it (`SegmentSink::wait`), so no upload lands after the caller's cleanup
    /// (review 1 #9). Returns every object the job may have created.
    ///
    /// Cost (review 2 R2-5): ending the input lets each writer finish, so a failed or cancelled job encodes and
    /// uploads up to `MAX_OPEN_WRITERS` partial outputs (each up to about `target_segment`) and then deletes them.
    /// A shutdown cancel waits for that too; past the shutdown grace the lifecycle task is aborted, the writers run
    /// on detached and their uploads become orphans for the orphan sweep.
    async fn abort(&mut self) -> Vec<object_store::path::Path> {
        let open = std::mem::take(&mut self.open).into_values().map(|w| w.sink);
        for sink in open.chain(std::mem::take(&mut self.closing)) {
            sink.wait().await;
        }
        std::mem::take(&mut self.objects)
    }
}

/// Runs a begun job to its commit (D7). Before the first commit attempt (`build`) an error fails the job and
/// deletes its uploads (best effort). Once the commit is attempted nothing is deleted: `fail_compaction` reports
/// whether the commit landed (Done = success), and orphan GC removes the uploads of a failed job.
pub async fn run_job(cx: &CompactCtx, job: CompactionJob) -> Result<JobReport, IngestError> {
    failpoint::hit("compact_after_lease");
    let t0 = std::time::Instant::now();
    let mut w = Writers::new(cx.segments.clone(), job.project_id.clone(), cx.max_postings);
    let (mut report, commit) = match build(cx, &job, &mut w).await {
        Ok(x) => x,
        Err(e) => {
            let objects = w.abort().await;
            fail_job(cx, job.id, &e).await;
            let _ = forge_storage::delete_all(cx.segments.object_store().as_ref(), objects).await;
            return Err(e);
        }
    };
    if let Err(e) = commit_job(cx, commit).await {
        if fail_job(cx, job.id, &e).await != Some(JobStatus::Done) {
            return Err(e);
        }
        tracing::warn!(job = job.id, error = %e, "compaction commit reported an error but landed");
    }
    report.elapsed_ms = t0.elapsed().as_millis() as u64;
    Ok(report)
}

/// `fail_compaction`, retried on transient errors (it is idempotent). None if it still fails: the job stays running
/// until the next tick resets it (`reset_dead_jobs`).
async fn fail_job(cx: &CompactCtx, id: i64, e: &IngestError) -> Option<JobStatus> {
    let msg = e.to_string();
    let r = retry_bounded("fail_compaction", COMMIT_BACKOFF, COMMIT_ATTEMPTS, || {
        cx.meta.fail_compaction(id, &msg)
    })
    .await;
    r.inspect_err(|err| tracing::error!(job = id, error = %err, "fail_compaction failed; the job stays running"))
        .ok()
}

/// The commit: the atomic swap, retried on transient errors.
async fn commit_job(cx: &CompactCtx, commit: CompactionCommit) -> Result<(), IngestError> {
    let commit = Arc::new(commit);
    retry_bounded("commit_compaction", COMMIT_BACKOFF, COMMIT_ATTEMPTS, || {
        cx.meta.commit_compaction(commit.clone())
    })
    .await?;
    failpoint::hit("compact_after_swap");
    Ok(())
}

/// Everything before the commit: slices -> outputs uploaded. Returns the report and the commit.
async fn build(
    cx: &CompactCtx,
    job: &CompactionJob,
    w: &mut Writers,
) -> Result<(JobReport, CompactionCommit), IngestError> {
    let cancel = &cx.cancel;
    let plan = JobPlan::from(job);
    let mut stats = JobStats {
        folded_hours: plan.folded_hours,
        ..Default::default()
    };
    let traces = job.inputs.iter().map(|s| cx.meta.segment_traces(s.id));
    let traces: Vec<Vec<TraceRange>> = in_order(traces.collect()).await?;
    let inputs: Vec<(SegmentMeta, Vec<TraceRange>)> = job.inputs.iter().cloned().zip(traces).collect();
    let mut ratio = INITIAL_RATIO;
    let mut slices = Slices::new(&inputs);
    while let Some((traces, ranges)) = slices.next(cx.cfg.slice_bytes.as_u64(), ratio) {
        cancel.check()?;
        stats.slices += 1;
        // 1. Inputs, all columns. `sticky` sources: every L1 row (input or context) with its hour, in read order.
        let scans = ranges.iter().map(|(i, rows)| {
            let req = ScanRequest {
                rows: RowSelection::Range(rows.clone()),
                ..Default::default()
            };
            cx.segments.scan_all(&inputs[*i].0, req)
        });
        let scans: Vec<Vec<RecordBatch>> = in_order(scans.collect()).await?;
        let mut x = SliceInput {
            batches: Vec::new(),
            ctx_all: Vec::new(),
            ctx_old: Vec::new(),
            l1_rows: Vec::new(),
            fallback: plan.fallback,
            max_chunk: cx.merge_chunk_bytes,
        };
        let (mut decoded, mut encoded) = (0u64, 0f64);
        for ((i, rows), bs) in ranges.iter().zip(scans) {
            let seg = &inputs[*i].0;
            for b in bs {
                if seg.level == Level::L1 {
                    x.l1_rows.push((seg.partition_hour, b.clone()));
                }
                decoded += b.get_array_memory_size() as u64;
                x.batches.push(b);
            }
            encoded += (rows.end - rows.start) as f64 * seg.byte_size as f64 / seg.row_count.max(1) as f64;
        }
        if encoded > 0.0 {
            ratio = (decoded as f64 / encoded).max(1.0);
        }
        // 2. Context: the slice traces' non-input L1 locations, from one metastore read.
        let locs = cx
            .meta
            .locate_l1_traces(&job.project_id, &traces, &plan.input_ids)
            .await?;
        let mut per_seg: BTreeMap<SegmentId, (SegmentMeta, Vec<std::ops::Range<u64>>)> = BTreeMap::new();
        for l in locs.into_values().flatten() {
            per_seg
                .entry(l.segment.id)
                .or_insert_with(|| (l.segment.clone(), Vec::new()))
                .1
                .push(l.rows());
        }
        let reads: Vec<(SegmentMeta, std::ops::Range<u64>)> = per_seg
            .into_values()
            .flat_map(|(seg, rs)| coalesce_ranges(rs, 0).into_iter().map(move |r| (seg.clone(), r)))
            .collect();
        let ctx_scans = reads.iter().map(|(seg, r)| {
            let req = ScanRequest {
                projection: Some(CONTEXT_COLUMNS.to_vec()),
                rows: RowSelection::Range(r.clone()),
                ..Default::default()
            };
            cx.segments.scan_all(seg, req)
        });
        let ctx_scans: Vec<Vec<RecordBatch>> = in_order(ctx_scans.collect()).await?;
        for ((seg, _), bs) in reads.iter().zip(ctx_scans) {
            for b in bs {
                x.l1_rows.push((seg.partition_hour, b.clone()));
                if seg.max_version < plan.older_than {
                    x.ctx_old.push(b.clone());
                }
                x.ctx_all.push(b);
            }
        }
        // 3. Merge, fill, backfill, sort and route, on the blocking pool.
        let c2 = cancel.clone();
        let (routed, fills, backfills) = tokio::task::spawn_blocking(move || x.run(&c2))
            .await
            .map_err(|e| IngestError::Internal(format!("compaction merge: {e}")))??;
        stats.fills += fills;
        stats.backfills += backfills;
        // 4. Send, chunk by chunk (each holds whole traces).
        let limit = (cx.cfg.target_segment.as_u64() as f64 * ratio) as u64;
        for (run, b) in routed {
            w.send(run, b, limit, &mut stats).await?;
        }
    }
    let outputs = w.finish().await?;
    stats.orphan_outputs = outputs.iter().filter(|o| o.meta.is_orphan_run()).count() as u32;
    // The last cancellation point: the commit is never interrupted (D7).
    cancel.check()?;
    failpoint::hit("compact_after_upload");
    let l0 = job.inputs.iter().filter(|s| s.level == Level::L0).count();
    let report = JobReport {
        project: job.project_id.to_string(),
        kind: job.kind.as_str(),
        l0_inputs: l0,
        l1_inputs: job.inputs.len() - l0,
        outputs: outputs
            .iter()
            .map(|o| {
                let m = &o.meta;
                (m.partition_hour.0, m.is_orphan_run(), m.row_count, m.byte_size)
            })
            .collect(),
        rows_in: job.inputs.iter().map(|s| s.row_count).sum(),
        elapsed_ms: 0,
        stats,
    };
    let commit = CompactionCommit {
        job_id: job.id,
        outputs,
        max_age_ms: i64::try_from(cx.cfg.commit_max_age().as_millis()).unwrap_or(i64::MAX),
    };
    Ok((report, commit))
}

/// Re-cuts merged chunks so that each holds whole traces (a merge chunk may end inside a trace): a trace's rows
/// can change order once its dotted_orders are filled, so it must be sorted in one batch. One chunk is returned
/// as it is.
fn trace_aligned(chunks: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, IngestError> {
    if chunks.len() <= 1 {
        return Ok(chunks);
    }
    let n = chunks.len();
    let mut out = Vec::with_capacity(n);
    let mut carry: Option<RecordBatch> = None;
    for (i, c) in chunks.into_iter().enumerate() {
        let b = match carry.take() {
            Some(prev) => concat_batches(&c.schema(), [&prev, &c])?,
            None => c,
        };
        if i + 1 == n {
            out.push(b);
            break;
        }
        // The last trace's first row: it may continue in the next chunk.
        let p = b.column(col_idx(col::PROJECT_ID)).as_string::<i32>();
        let t = b.column(col_idx(col::TRACE_ID)).as_binary::<i32>();
        let last = b.num_rows() - 1;
        let mut cut = last;
        while cut > 0 && p.value(cut - 1) == p.value(last) && t.value(cut - 1) == t.value(last) {
            cut -= 1;
        }
        if cut > 0 {
            out.push(b.slice(0, cut));
        }
        carry = Some(b.slice(cut, b.num_rows() - cut));
    }
    Ok(out)
}

/// One slice's rows, moved to the blocking pool.
struct SliceInput {
    /// The inputs' rows, all columns.
    batches: Vec<RecordBatch>,
    /// Context rows (CONTEXT_COLUMNS) of every non-input L1 location, and those older than the job's rows.
    ctx_all: Vec<RecordBatch>,
    ctx_old: Vec<RecordBatch>,
    /// Every L1 row (inputs, then context) with its hour; a later row's hour wins (sticky).
    l1_rows: Vec<(HourBucket, RecordBatch)>,
    fallback: HourBucket,
    max_chunk: usize,
}

impl SliceInput {
    /// Merge (byte-bounded chunks, re-cut at trace boundaries) + fill + backfill + sort, then route (D3): the
    /// output batches in send order, #fills, #backfills.
    #[allow(clippy::type_complexity)]
    fn run(self, cancel: &Cancel) -> Result<(Vec<(Run, RecordBatch)>, u64, u64), IngestError> {
        if failpoint::fires("compact_merge_panic") {
            panic!("FORGE_FAILPOINT compact_merge_panic hit");
        }
        let mut sticky: HashMap<Key, HourBucket> = HashMap::new();
        for (h, b) in &self.l1_rows {
            for k in span_keys(b)? {
                sticky.insert(k, *h);
            }
        }
        let mut trace_hour: HashMap<TraceId, HourBucket> = HashMap::new();
        for (k, h) in &sticky {
            let e = trace_hour.entry(k.0).or_insert(*h);
            *e = (*e).min(*h);
        }
        let merge = |b: &[RecordBatch], schema: &SchemaRef| {
            merge_versions_projected(schema, b, Tombstones::Keep, self.max_chunk, cancel)
        };
        let ctx_schema = projected_schema(&CONTEXT_COLUMNS);
        let all_rows = merge(&self.ctx_all, &ctx_schema)?;
        let all = context(&all_rows)?;
        // `ctx_old` is a subsequence of `ctx_all`: the same length means the same rows (the usual kind A).
        let old_rows = if self.ctx_old.len() == self.ctx_all.len() {
            Vec::new()
        } else {
            merge(&self.ctx_old, &ctx_schema)?
        };
        let old_ctx;
        let old = if self.ctx_old.len() == self.ctx_all.len() {
            &all
        } else {
            old_ctx = context(&old_rows)?;
            &old_ctx
        };
        let merged = merge(&self.batches, &spans_schema())?;
        drop(self.batches);
        let (mut out, mut fills, mut backfills) = (Vec::new(), 0, 0);
        for m in trace_aligned(merged)? {
            cancel.check()?;
            let f = fill(&m, old, &all)?;
            cancel.check()?;
            (fills, backfills) = (fills + f.fills, backfills + f.backfills);
            // Merged rows are in physical order; only a filled dotted_order can move a row.
            let sorted = if f.dotted_filled {
                sort_physical(&f.batch)?
            } else {
                f.batch
            };
            route_chunk(&sorted, &sticky, &trace_hour, self.fallback, &mut out)?;
        }
        Ok((out, fills, backfills))
    }
}

/// Splits one sorted chunk by output run (D3); each part keeps the chunk's row order.
fn route_chunk(
    chunk: &RecordBatch,
    sticky: &HashMap<Key, HourBucket>,
    trace_hour: &HashMap<TraceId, HourBucket>,
    fallback: HourBucket,
    out: &mut Vec<(Run, RecordBatch)>,
) -> Result<(), IngestError> {
    let ks = span_keys(chunk)?;
    let start = chunk
        .column(col_idx(col::START_TIME))
        .as_primitive::<TimestampNanosecondType>();
    let end = chunk
        .column(col_idx(col::END_TIME))
        .as_primitive::<TimestampNanosecondType>();
    let mut groups: BTreeMap<Run, Vec<u32>> = BTreeMap::new();
    for (i, k) in ks.iter().enumerate() {
        let run = route(
            *k,
            start.is_valid(i).then(|| start.value(i)),
            end.is_valid(i).then(|| end.value(i)),
            sticky,
            trace_hour.get(&k.0).copied(),
            fallback,
        );
        groups.entry(run).or_default().push(i as u32);
    }
    for (run, idx) in groups {
        out.push((run, take_record_batch(chunk, &UInt32Array::from(idx))?));
    }
    Ok(())
}

/// Runs `reads`, `READ_CONCURRENCY` at a time; the results in order.
async fn in_order<T, E: Into<IngestError>>(
    reads: Vec<impl std::future::Future<Output = Result<T, E>>>,
) -> Result<Vec<T>, IngestError> {
    futures::stream::iter(reads)
        .buffered(READ_CONCURRENCY)
        .map_err(Into::into)
        .try_collect()
        .await
}

/// Context from merged CONTEXT_COLUMNS rows (borrowing their dotted_order values).
fn context(chunks: &[RecordBatch]) -> Result<Context<'_>, IngestError> {
    let mut out = Context::default();
    for m in chunks {
        let s = m.schema();
        let parent = m.column(s.index_of(col::PARENT_SPAN_ID)?).as_binary::<i32>();
        let dotted = m.column(s.index_of(col::DOTTED_ORDER)?).as_string::<i32>();
        let start = m
            .column(s.index_of(col::START_TIME)?)
            .as_primitive::<TimestampNanosecondType>();
        for (i, k) in span_keys(m)?.into_iter().enumerate() {
            let p = if parent.is_valid(i) {
                Some((k.0, SpanId::from_slice(parent.value(i))?))
            } else {
                None
            };
            let st = start.is_valid(i).then(|| start.value(i));
            out.insert(k, (p, st, dotted.is_valid(i).then(|| dotted.value(i))));
        }
    }
    Ok(out)
}

struct Filled {
    batch: RecordBatch,
    fills: u64,
    backfills: u64,
    /// Some null dotted_order got a value (the rows may need a re-sort).
    dotted_filled: bool,
}

/// Start fill (CR §2.4) and dotted_order (D5): a null start or dotted_order takes the key's value from OLDER
/// context rows (`old`); then the remaining nulls are backfilled by the one walk over the merged rows plus every
/// context node (`all`; an ancestor's existing value is the prefix).
fn fill<'a>(m: &'a RecordBatch, old: &Context<'a>, all: &Context<'a>) -> Result<Filled, IngestError> {
    let start = m
        .column(col_idx(col::START_TIME))
        .as_primitive::<TimestampNanosecondType>();
    let dotted = m.column(col_idx(col::DOTTED_ORDER)).as_string::<i32>();
    if start.null_count() == 0 && dotted.null_count() == 0 {
        return Ok(Filled {
            batch: m.clone(),
            fills: 0,
            backfills: 0,
            dotted_filled: false,
        });
    }
    let ks = span_keys(m)?;
    let parent = m.column(col_idx(col::PARENT_SPAN_ID)).as_binary::<i32>();
    let (mut fills, mut starts, mut dotteds, mut want) = (0u64, Vec::new(), Vec::new(), Vec::new());
    let mut dotted_from_old = false;
    let mut rows: Vec<(Key, Node<'a, Key>)> = Vec::with_capacity(ks.len());
    for (i, k) in ks.iter().enumerate() {
        let o = old.get(k);
        let st = if start.is_valid(i) {
            Some(start.value(i))
        } else {
            let s = o.and_then(|o| o.1);
            fills += u64::from(s.is_some());
            s
        };
        let d = if dotted.is_valid(i) {
            Some(dotted.value(i))
        } else {
            let d = o.and_then(|o| o.2);
            dotted_from_old |= d.is_some();
            d
        };
        let p = if parent.is_valid(i) {
            Some((k.0, SpanId::from_slice(parent.value(i))?))
        } else {
            o.and_then(|o| o.0)
        };
        if d.is_none() {
            want.push(*k);
        }
        rows.push((*k, (p, st, d)));
        starts.push(st);
        dotteds.push(d);
    }
    let resolved = if want.is_empty() {
        Map::default()
    } else {
        let mut nodes = all.clone();
        nodes.extend(rows);
        resolve_with_known(&nodes, want)
    };
    let dotted_filled = dotted_from_old || !resolved.is_empty();
    let new_dotted: StringArray = dotteds
        .into_iter()
        .zip(&ks)
        .map(|(d, k)| d.or_else(|| resolved.get(k).map(String::as_str)))
        .collect();
    let new_start: ArrayRef = Arc::new(TimestampNanosecondArray::from(starts).with_timezone(TIMEZONE));
    let mut cols = m.columns().to_vec();
    cols[col_idx(col::START_TIME)] = new_start;
    cols[col_idx(col::DOTTED_ORDER)] = Arc::new(new_dotted);
    Ok(Filled {
        batch: RecordBatch::try_new(m.schema(), cols)?,
        fills,
        backfills: resolved.len() as u64,
        dotted_filled,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_model::ids::TraceId;
    use forge_model::testing::{batch_from_rows, pid, sid, sort_by_key};

    /// One trace of 10 spans, 100 distinct tokens in `inputs` (10 per span), key-sorted.
    fn trace_batch(t: u64) -> RecordBatch {
        let rows: Vec<_> = (0..10u64)
            .map(|s| {
                let words: Vec<String> = (0..10).map(|w| format!("w{t}x{s}x{w}")).collect();
                forge_model::row::SpanRow {
                    project_id: Some(pid("p")),
                    trace_id: TraceId(u128::from(t).to_be_bytes()),
                    span_id: sid(s),
                    start_time_ns: Some(1_700_000_000_000_000_000 + s as i64),
                    inputs: Some(format!("{{\"text\":\"{}\"}}", words.join(" "))),
                    version: 1,
                    ..Default::default()
                }
            })
            .collect();
        sort_by_key(&batch_from_rows(&rows))
    }

    async fn outputs(max_postings: u64) -> Vec<NewSegment> {
        let mut w = Writers::new(forge_storage::testing::memory_store(), pid("p"), max_postings);
        let run = Run {
            hour: HourBucket(0),
            orphan: false,
        };
        let mut stats = JobStats::default();
        for t in 0..20 {
            w.send(run, trace_batch(t), u64::MAX, &mut stats).await.unwrap();
        }
        w.finish().await.unwrap()
    }

    /// The count lags `push` by up to 4 batches (a channel of 2 + 1 sender slot + the batch in `add_batch`), so only
    /// "more than one output" is asserted, never an exact count; current_thread keeps it deterministic.
    #[tokio::test(flavor = "current_thread")]
    async fn posting_cap_rotates_outputs() {
        let out = outputs(200).await;
        assert!(out.len() > 1, "{} outputs", out.len());
        assert_eq!(out.iter().map(|o| o.meta.row_count).sum::<u64>(), 200);
        for pair in out.windows(2) {
            assert!(pair[0].meta.max_trace_id < pair[1].meta.min_trace_id);
        }
        assert!(out.iter().all(|o| o.meta.fts_byte_size.is_some()));
        assert_eq!(outputs(u64::MAX).await.len(), 1);
    }
}
