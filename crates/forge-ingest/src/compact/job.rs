//! `run_job`: lazy trace slices -> scan + context -> merge (Keep) -> start fill -> dotted backfill -> sort -> route to
//! per-(hour, orphan) writers -> commit (plans/phase-4 §2.8, CR §7).
use super::{CompactCtx, JobReport, JobStats, MAX_OPEN_WRITERS};
use crate::IngestError;
use crate::sink::SegmentSink;
use arrow::array::{Array, ArrayRef, AsArray, StringArray, TimestampNanosecondArray, UInt32Array};
use arrow::compute::{concat_batches, take_record_batch};
use arrow::datatypes::TimestampNanosecondType;
use arrow::record_batch::RecordBatch;
use forge_meta::{CompactionCommit, CompactionJob, NewSegment};
use forge_model::catalog::{HourBucket, JobKind, JobStatus, Level, SegmentId, SegmentMeta, TraceRange};
use forge_model::dotted::{Node, resolve_with_known};
use forge_model::failpoint;
use forge_model::ids::{ProjectId, SpanId, TraceId};
use forge_model::merge::{Tombstones, merge_versions_projected, sort_physical};
use forge_model::retry::{Backoff, retry_bounded};
use forge_model::schema::{TIMEZONE, col, col_idx, projected_schema, spans_schema};
use forge_storage::{RowSelection, ScanRequest, SegmentSpec, SegmentStore};
use futures::TryStreamExt;
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

type Key = (TraceId, SpanId);
/// What the job knows about a key from L1 rows it does not rewrite: (parent, start, dotted_order).
type Context = HashMap<Key, Node<Key>>;

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
    done: Vec<NewSegment>,
    /// Every object of every output started (deleted when the job fails before its commit).
    objects: Vec<object_store::path::Path>,
    rotated: u32,
    capped: u32,
    tick: u64,
}

impl Writers {
    fn new(store: Arc<dyn SegmentStore>, project: ProjectId) -> Self {
        Self {
            store,
            project,
            open: BTreeMap::new(),
            done: Vec::new(),
            objects: Vec::new(),
            rotated: 0,
            capped: 0,
            tick: 0,
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
    async fn send(&mut self, run: Run, b: RecordBatch, limit: u64) -> Result<(), IngestError> {
        let size = b.get_array_memory_size() as u64;
        if self
            .open
            .get(&run)
            .is_some_and(|w| w.bytes > 0 && w.bytes + size > limit)
        {
            let w = self.open.remove(&run).expect("present");
            self.done.push(w.sink.finish().await?);
            self.rotated += 1;
        }
        if !self.open.contains_key(&run) {
            if self.open.len() >= MAX_OPEN_WRITERS {
                let lru = *self.open.iter().min_by_key(|(_, w)| w.fed).expect("open").0;
                let w = self.open.remove(&lru).expect("present");
                self.done.push(w.sink.finish().await?);
                self.capped += 1;
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

    async fn finish(&mut self) -> Result<Vec<NewSegment>, IngestError> {
        for (_, w) in std::mem::take(&mut self.open) {
            self.done.push(w.sink.finish().await?);
        }
        Ok(std::mem::take(&mut self.done))
    }

    /// Stops the open writers (dropping a sink aborts it) and returns every object the job may have created.
    fn abort(&mut self) -> Vec<object_store::path::Path> {
        self.open.clear();
        std::mem::take(&mut self.objects)
    }
}

/// Runs a begun job to its commit (D7). Before the first commit attempt (`build`) an error fails the job and
/// deletes its uploads (best effort). Once the commit is attempted nothing is deleted: `fail_compaction` reports
/// whether the commit landed (Done = success), and orphan GC removes the uploads of a failed job.
pub async fn run_job(cx: &CompactCtx, job: CompactionJob) -> Result<JobReport, IngestError> {
    failpoint::hit("compact_after_lease");
    let t0 = std::time::Instant::now();
    let mut w = Writers::new(cx.segments.clone(), job.project_id.clone());
    let (mut report, commit) = match build(cx, &job, &mut w).await {
        Ok(x) => x,
        Err(e) => {
            let objects = w.abort();
            let _ = cx.meta.fail_compaction(job.id, &e.to_string()).await;
            let _ = forge_storage::delete_all(cx.segments.object_store().as_ref(), objects).await;
            return Err(e);
        }
    };
    if let Err(e) = commit_job(cx, commit).await {
        if !matches!(
            cx.meta.fail_compaction(job.id, &e.to_string()).await,
            Ok(JobStatus::Done)
        ) {
            return Err(e);
        }
        tracing::warn!(job = job.id, error = %e, "compaction commit reported an error but landed");
    }
    report.elapsed_ms = t0.elapsed().as_millis() as u64;
    Ok(report)
}

/// The commit: the atomic swap, retried on transient errors.
async fn commit_job(cx: &CompactCtx, commit: CompactionCommit) -> Result<(), IngestError> {
    retry_bounded("commit_compaction", COMMIT_BACKOFF, COMMIT_ATTEMPTS, || {
        cx.meta.commit_compaction(commit.clone())
    })
    .await?;
    failpoint::hit("compact_after_swap");
    Ok(())
}

async fn scan_all(
    store: &dyn SegmentStore,
    seg: &SegmentMeta,
    req: ScanRequest,
) -> Result<Vec<RecordBatch>, IngestError> {
    Ok(store.scan(seg, req).await?.try_collect().await?)
}

fn keys(b: &RecordBatch) -> Result<Vec<Key>, IngestError> {
    let t = b.column_by_name(col::TRACE_ID).unwrap().as_binary::<i32>();
    let s = b.column_by_name(col::SPAN_ID).unwrap().as_binary::<i32>();
    (0..b.num_rows())
        .map(|i| Ok((TraceId::from_slice(t.value(i))?, SpanId::from_slice(s.value(i))?)))
        .collect()
}

fn coalesce(mut rs: Vec<std::ops::Range<u64>>) -> Vec<std::ops::Range<u64>> {
    rs.sort_by_key(|r| r.start);
    let mut out: Vec<std::ops::Range<u64>> = Vec::new();
    for r in rs {
        match out.last_mut() {
            Some(l) if r.start <= l.end => l.end = l.end.max(r.end),
            _ => out.push(r),
        }
    }
    out
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
    let mut inputs = Vec::with_capacity(job.inputs.len());
    for s in &job.inputs {
        inputs.push((s.clone(), cx.meta.segment_traces(s.id).await?));
    }
    let ctx_schema = projected_schema(&CONTEXT_COLUMNS);
    let max_chunk = cx.merge_chunk_bytes;
    let mut ratio = INITIAL_RATIO;
    let mut slices = Slices::new(&inputs);
    while let Some((traces, ranges)) = slices.next(cx.cfg.slice_bytes.as_u64(), ratio) {
        cancel.check()?;
        stats.slices += 1;
        // 1. Inputs, all columns. sticky: key -> hour from every L1 row (input or context).
        let mut batches = Vec::new();
        let mut sticky: HashMap<Key, HourBucket> = HashMap::new();
        let (mut decoded, mut encoded) = (0u64, 0f64);
        for (i, rows) in &ranges {
            let seg = &inputs[*i].0;
            let req = ScanRequest {
                rows: RowSelection::Range(rows.clone()),
                ..Default::default()
            };
            for b in scan_all(cx.segments.as_ref(), seg, req).await? {
                if seg.level == Level::L1 {
                    for k in keys(&b)? {
                        sticky.insert(k, seg.partition_hour);
                    }
                }
                decoded += b.get_array_memory_size() as u64;
                batches.push(b);
            }
            encoded += (rows.end - rows.start) as f64 * seg.byte_size as f64 / seg.row_count.max(1) as f64;
        }
        if encoded > 0.0 {
            ratio = (decoded as f64 / encoded).max(1.0);
        }
        // 2. Context: the slice traces' non-input L1 locations, from one metastore read.
        let locs = cx.meta.locate_traces(&job.project_id, &traces).await?;
        let mut per_seg: BTreeMap<SegmentId, (SegmentMeta, Vec<std::ops::Range<u64>>)> = BTreeMap::new();
        for l in locs.into_values().flatten() {
            if l.segment.level == Level::L1 && !plan.input_ids.contains(&l.segment.id) {
                per_seg
                    .entry(l.segment.id)
                    .or_insert_with(|| (l.segment.clone(), Vec::new()))
                    .1
                    .push(l.rows());
            }
        }
        let (mut ctx_all, mut ctx_old) = (Vec::new(), Vec::new());
        for (seg, rs) in per_seg.into_values() {
            for r in coalesce(rs) {
                let req = ScanRequest {
                    projection: Some(CONTEXT_COLUMNS.to_vec()),
                    rows: RowSelection::Range(r),
                    ..Default::default()
                };
                for b in scan_all(cx.segments.as_ref(), &seg, req).await? {
                    for k in keys(&b)? {
                        sticky.insert(k, seg.partition_hour);
                    }
                    if seg.max_version < plan.older_than {
                        ctx_old.push(b.clone());
                    }
                    ctx_all.push(b);
                }
            }
        }
        // 3. Merge (byte-bounded chunks, re-cut at trace boundaries) + fill + backfill + sort, on the blocking pool.
        let (c2, s2) = (cancel.clone(), ctx_schema.clone());
        let (chunks, fills, backfills) = tokio::task::spawn_blocking(move || -> Result<_, IngestError> {
            if failpoint::fires("compact_merge_panic") {
                panic!("FORGE_FAILPOINT compact_merge_panic hit");
            }
            let all = context(&merge_versions_projected(
                &s2,
                &ctx_all,
                Tombstones::Keep,
                max_chunk,
                &c2,
            )?)?;
            let old = context(&merge_versions_projected(
                &s2,
                &ctx_old,
                Tombstones::Keep,
                max_chunk,
                &c2,
            )?)?;
            let merged = merge_versions_projected(&spans_schema(), &batches, Tombstones::Keep, max_chunk, &c2)?;
            drop(batches);
            let (mut out, mut fills, mut backfills) = (Vec::new(), 0, 0);
            for m in trace_aligned(merged)? {
                c2.check()?;
                let (filled, f, b) = fill(&m, &old, &all)?;
                c2.check()?;
                out.push(sort_physical(&filled)?);
                (fills, backfills) = (fills + f, backfills + b);
            }
            Ok((out, fills, backfills))
        })
        .await
        .map_err(|e| IngestError::Internal(format!("compaction merge: {e}")))??;
        stats.fills += fills;
        stats.backfills += backfills;
        // 4. Route (D3), chunk by chunk (each holds whole traces).
        let mut trace_hour: HashMap<TraceId, HourBucket> = HashMap::new();
        for (k, h) in &sticky {
            let e = trace_hour.entry(k.0).or_insert(*h);
            *e = (*e).min(*h);
        }
        let limit = (cx.cfg.target_segment.as_u64() as f64 * ratio) as u64;
        for slice in chunks {
            let ks = keys(&slice)?;
            let start = slice
                .column(col_idx(col::START_TIME))
                .as_primitive::<TimestampNanosecondType>();
            let end = slice
                .column(col_idx(col::END_TIME))
                .as_primitive::<TimestampNanosecondType>();
            let mut groups: BTreeMap<Run, Vec<u32>> = BTreeMap::new();
            for (i, k) in ks.iter().enumerate() {
                let run = route(
                    *k,
                    start.is_valid(i).then(|| start.value(i)),
                    end.is_valid(i).then(|| end.value(i)),
                    &sticky,
                    trace_hour.get(&k.0).copied(),
                    plan.fallback,
                );
                groups.entry(run).or_default().push(i as u32);
            }
            for (run, idx) in groups {
                let b = take_record_batch(&slice, &UInt32Array::from(idx))?;
                w.send(run, b, limit).await?;
            }
        }
    }
    let outputs = w.finish().await?;
    stats.rotated = w.rotated;
    stats.writer_cap = w.capped;
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

/// Context from merged CONTEXT_COLUMNS rows.
fn context(chunks: &[RecordBatch]) -> Result<Context, IngestError> {
    let mut out = Context::new();
    for m in chunks {
        let s = m.schema();
        let parent = m.column(s.index_of(col::PARENT_SPAN_ID)?).as_binary::<i32>();
        let dotted = m.column(s.index_of(col::DOTTED_ORDER)?).as_string::<i32>();
        let start = m
            .column(s.index_of(col::START_TIME)?)
            .as_primitive::<TimestampNanosecondType>();
        for (i, k) in keys(m)?.into_iter().enumerate() {
            let p = if parent.is_valid(i) {
                Some((k.0, SpanId::from_slice(parent.value(i))?))
            } else {
                None
            };
            let st = start.is_valid(i).then(|| start.value(i));
            out.insert(k, (p, st, dotted.is_valid(i).then(|| dotted.value(i).to_string())));
        }
    }
    Ok(out)
}

/// Start fill (CR §2.4) and dotted_order (D5): a null start or dotted_order takes the key's value from OLDER
/// context rows (`old`); then the remaining nulls are backfilled by the one walk over the merged rows plus every
/// context node (`all`; an ancestor's existing value is the prefix). Returns the new batch, #fills, #backfills.
fn fill(m: &RecordBatch, old: &Context, all: &Context) -> Result<(RecordBatch, u64, u64), IngestError> {
    let ks = keys(m)?;
    let start = m
        .column(col_idx(col::START_TIME))
        .as_primitive::<TimestampNanosecondType>();
    let parent = m.column(col_idx(col::PARENT_SPAN_ID)).as_binary::<i32>();
    let dotted = m.column(col_idx(col::DOTTED_ORDER)).as_string::<i32>();
    let mut nodes = all.clone();
    let (mut fills, mut starts, mut dotteds, mut want) = (0u64, Vec::new(), Vec::new(), Vec::new());
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
            Some(dotted.value(i).to_string())
        } else {
            o.and_then(|o| o.2.clone())
        };
        let p = if parent.is_valid(i) {
            Some((k.0, SpanId::from_slice(parent.value(i))?))
        } else {
            o.and_then(|o| o.0)
        };
        if d.is_none() {
            want.push(*k);
        }
        nodes.insert(*k, (p, st, d.clone()));
        starts.push(st);
        dotteds.push(d);
    }
    let resolved = resolve_with_known(&nodes, want);
    let new_dotted: StringArray = dotteds
        .into_iter()
        .zip(&ks)
        .map(|(d, k)| d.or_else(|| resolved.get(k).cloned()))
        .collect();
    let new_start: ArrayRef = Arc::new(TimestampNanosecondArray::from(starts).with_timezone(TIMEZONE));
    let mut cols = m.columns().to_vec();
    cols[col_idx(col::START_TIME)] = new_start;
    cols[col_idx(col::DOTTED_ORDER)] = Arc::new(new_dotted);
    Ok((RecordBatch::try_new(m.schema(), cols)?, fills, resolved.len() as u64))
}
