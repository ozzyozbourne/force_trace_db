//! `run_job`: lazy trace slices -> scan + context -> merge (Keep) -> start fill -> dotted backfill -> sort -> route to
//! per-(hour, orphan) writers -> commit (plans/phase-4 §2.8, CR §7).
use super::{CompactCtx, JobReport, JobStats, MAX_OPEN_WRITERS, level_counts};
use crate::IngestError;
use arrow::array::{Array, ArrayRef, AsArray, StringArray, TimestampNanosecondArray, UInt32Array};
use arrow::compute::take_record_batch;
use arrow::datatypes::TimestampNanosecondType;
use arrow::record_batch::RecordBatch;
use forge_meta::{CompactionCommit, CompactionJob, NewSegment};
use forge_model::catalog::{HourBucket, JobKind, JobStatus, Level, SegmentId, SegmentMeta, TraceRange, paths};
use forge_model::dotted::{Node, resolve_with_known};
use forge_model::error::{Classify, ErrorKind};
use forge_model::failpoint;
use forge_model::ids::{ProjectId, SpanId, TraceId};
use forge_model::merge::{Tombstones, merge_projected, sort_physical};
use forge_model::schema::{TIMEZONE, col, col_idx, projected_schema, spans_schema};
use forge_storage::{RowSelection, ScanRequest, SegmentSpec, SegmentStore, StorageResult, WrittenSegment};
use futures::{SinkExt, StreamExt, TryStreamExt};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

pub const CONTEXT_COLUMNS: [&str; 8] = [
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
pub const INITIAL_RATIO: f64 = 12.0;

type Key = (TraceId, SpanId);

/// What the job knows about a key from L1 rows it does not rewrite.
#[derive(Clone, Debug)]
pub struct Known {
    pub start_ns: Option<i64>,
    pub parent: Option<SpanId>,
    pub dotted: Option<String>,
}
pub type Context = HashMap<Key, Known>;

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

/// Routing of one merged, filled row (D3).
pub fn route(
    key: Key,
    start: Option<i64>,
    end: Option<i64>,
    sticky: &HashMap<Key, HourBucket>,
    trace_l1_hour: Option<HourBucket>,
    fallback: HourBucket,
) -> (HourBucket, bool) {
    let orphan = start.is_none();
    if let Some(h) = sticky.get(&key) {
        return (*h, orphan);
    }
    let h = start
        .or(end)
        .map(HourBucket::from_nanos)
        .or(trace_l1_hour)
        .unwrap_or(fallback);
    (h, orphan)
}

/// One open output.
struct Writer {
    id: SegmentId,
    tx: Option<futures::channel::mpsc::Sender<RecordBatch>>,
    handle: tokio::task::JoinHandle<StorageResult<WrittenSegment>>,
    bytes: u64,
    blobs: BTreeSet<String>,
    /// Feed tick of the last batch (the least recently fed writer is finished first).
    fed: u64,
}

struct Writers {
    store: Arc<dyn SegmentStore>,
    project: ProjectId,
    /// Decoded bytes per output before rotation.
    limit: u64,
    open: BTreeMap<(HourBucket, bool), Writer>,
    done: Vec<(NewSegment, bool)>,
    /// Every output id started (for the best-effort delete when the job fails before its commit).
    started: Vec<(HourBucket, SegmentId)>,
    rotated: u32,
    capped: u32,
    tick: u64,
}

impl Writers {
    fn start(&mut self, hour: HourBucket) -> Writer {
        let id = SegmentId::new();
        let (tx, rx) = futures::channel::mpsc::channel::<RecordBatch>(2);
        let spec = SegmentSpec {
            id,
            project_id: self.project.clone(),
            level: Level::L1,
            partition_hour: hour,
        };
        let store = self.store.clone();
        let handle = tokio::spawn(async move { store.write_segment(spec, rx.map(Ok).boxed()).await });
        self.started.push((hour, id));
        Writer {
            id,
            tx: Some(tx),
            handle,
            bytes: 0,
            blobs: BTreeSet::new(),
            fed: 0,
        }
    }

    async fn finish_one(&mut self, mut w: Writer, orphan: bool) -> Result<(), IngestError> {
        drop(w.tx.take());
        let written = w
            .handle
            .await
            .map_err(|e| IngestError::Internal(format!("segment writer: {e}")))??;
        debug_assert_eq!(written.meta.id, w.id);
        self.done.push((
            NewSegment {
                meta: written.meta,
                traces: written.traces,
                blobs: w.blobs.into_iter().collect(),
            },
            orphan,
        ));
        Ok(())
    }

    /// Sends one routed, sorted batch (whole traces of a slice). Rotates at this trace boundary once the output
    /// is full; finishes the least recently fed writer when `MAX_OPEN_WRITERS` are open. A later file of the same
    /// (hour, orphan) holds higher trace ids, so the files stay key-disjoint.
    async fn send(&mut self, hour: HourBucket, orphan: bool, b: RecordBatch) -> Result<(), IngestError> {
        let k = (hour, orphan);
        let size = b.get_array_memory_size() as u64;
        if self
            .open
            .get(&k)
            .is_some_and(|w| w.bytes > 0 && w.bytes + size > self.limit)
        {
            let w = self.open.remove(&k).expect("present");
            self.finish_one(w, orphan).await?;
            self.rotated += 1;
        }
        if !self.open.contains_key(&k) {
            if self.open.len() >= MAX_OPEN_WRITERS {
                let lru = *self.open.iter().min_by_key(|(_, w)| w.fed).expect("open").0;
                let w = self.open.remove(&lru).expect("present");
                self.finish_one(w, lru.1).await?;
                self.capped += 1;
            }
            let w = self.start(hour);
            self.open.insert(k, w);
        }
        self.tick += 1;
        let w = self.open.get_mut(&k).expect("present");
        w.fed = self.tick;
        w.bytes += size;
        w.blobs
            .extend(forge_model::blob::blob_refs_in(std::slice::from_ref(&b)));
        if w.tx.as_mut().expect("open").send(b).await.is_err() {
            // The writer stopped early (an error): surface it.
            let w = self.open.remove(&k).expect("present");
            self.finish_one(w, orphan).await?;
            return Err(IngestError::Internal("segment writer stopped".into()));
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<Vec<(NewSegment, bool)>, IngestError> {
        for ((_, orphan), w) in std::mem::take(&mut self.open) {
            self.finish_one(w, orphan).await?;
        }
        Ok(std::mem::take(&mut self.done))
    }

    fn abort(&self) {
        for w in self.open.values() {
            w.handle.abort();
        }
    }
}

/// Runs a begun job to its commit. Error path (D7): before the first commit attempt the job is failed and its
/// uploads are deleted (best effort); once a commit attempt has started nothing is deleted: `fail_compaction`
/// reports whether the commit landed (Done = success), and orphan GC removes the uploads of a failed job.
pub async fn run_job(cx: &CompactCtx, job: CompactionJob) -> Result<JobReport, IngestError> {
    failpoint::hit("compact_after_lease");
    let t0 = std::time::Instant::now();
    let mut w = Writers {
        store: cx.segments.clone(),
        project: job.project_id.clone(),
        limit: (cx.cfg.target_segment.as_u64() as f64 * INITIAL_RATIO) as u64,
        open: BTreeMap::new(),
        done: Vec::new(),
        started: Vec::new(),
        rotated: 0,
        capped: 0,
        tick: 0,
    };
    let mut report = None;
    let e = match run_inner(cx, &job, &mut w, &mut report).await {
        Ok(()) => return Ok(finish_report(report, t0)),
        Err(e) => e,
    };
    w.abort();
    let status = cx.meta.fail_compaction(job.id, &e.to_string()).await;
    if report.is_some() {
        // A commit attempt started (the report is built right before it).
        if let Ok(JobStatus::Done) = status {
            tracing::warn!(job = job.id, error = %e, "compaction commit reported an error but landed");
            return Ok(finish_report(report, t0));
        }
        return Err(e);
    }
    let paths: Vec<object_store::path::Path> = w
        .started
        .iter()
        .filter_map(|(h, id)| paths::segment_path(&job.project_id, Level::L1, *h, *id).ok())
        .map(object_store::path::Path::from)
        .collect();
    let _ = crate::lifecycle::delete_all(cx.segments.object_store().as_ref(), paths).await;
    Err(e)
}

fn finish_report(report: Option<JobReport>, t0: std::time::Instant) -> JobReport {
    let mut r = report.expect("set before the commit");
    r.elapsed_ms = t0.elapsed().as_millis() as u64;
    r
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

/// Merges adjacent or overlapping row ranges.
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

/// `report` is set right before the first commit attempt.
async fn run_inner(
    cx: &CompactCtx,
    job: &CompactionJob,
    w: &mut Writers,
    report: &mut Option<JobReport>,
) -> Result<(), IngestError> {
    let cancel = &cx.cancel;
    let mut stats = JobStats {
        folded_hours: if job.kind == JobKind::L0 {
            let hours: BTreeSet<HourBucket> = job
                .inputs
                .iter()
                .filter(|s| s.level == Level::L1)
                .map(|s| s.partition_hour)
                .collect();
            hours.len() as u32
        } else {
            0
        },
        ..Default::default()
    };
    let mut inputs = Vec::with_capacity(job.inputs.len());
    for s in &job.inputs {
        inputs.push((s.clone(), cx.meta.segment_traces(s.id).await?));
    }
    let input_ids: BTreeSet<SegmentId> = job.inputs.iter().map(|s| s.id).collect();
    // D3 fallback: the newest input L0's flush hour (kind B: the hour itself).
    let fallback = job
        .inputs
        .iter()
        .filter(|s| s.level == Level::L0)
        .max_by_key(|s| s.min_version)
        .or(job.inputs.last())
        .map(|s| s.partition_hour)
        .expect("a job has inputs");
    // Context rows below this version are older than every row of the job's keys (I2 for kind A, the D1c cut for
    // kind B); only they may fill a null start (CR §2.4, RD #11).
    let newer_level = if job.kind == JobKind::L0 { Level::L0 } else { Level::L1 };
    let older_than = job
        .inputs
        .iter()
        .filter(|s| s.level == newer_level)
        .map(|s| s.min_version)
        .min()
        .unwrap_or(0);
    let ctx_schema = projected_schema(&CONTEXT_COLUMNS);
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
            if l.segment.level == Level::L1 && !input_ids.contains(&l.segment.id) {
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
                    if seg.max_version < older_than {
                        ctx_old.push(b.clone());
                    }
                    ctx_all.push(b);
                }
            }
        }
        // 3. Merge + fill + backfill + sort on the blocking pool.
        let (c2, s2) = (cancel.clone(), ctx_schema.clone());
        let (slice, fills, backfills) = tokio::task::spawn_blocking(move || -> Result<_, IngestError> {
            if failpoint::fires("compact_merge_panic") {
                panic!("FORGE_FAILPOINT compact_merge_panic hit");
            }
            let all = context(&merge_projected(&s2, &ctx_all, Tombstones::Keep, &c2)?)?;
            let old = context(&merge_projected(&s2, &ctx_old, Tombstones::Keep, &c2)?)?;
            let merged = merge_projected(&spans_schema(), &batches, Tombstones::Keep, &c2)?;
            drop(batches);
            c2.check()?;
            let (filled, fills, backfills) = fill(&merged, &old, &all)?;
            c2.check()?;
            Ok((sort_physical(&filled)?, fills, backfills))
        })
        .await
        .map_err(|e| IngestError::Internal(format!("compaction merge: {e}")))??;
        stats.fills += fills;
        stats.backfills += backfills;
        // 4. Route (D3).
        let ks = keys(&slice)?;
        let start = slice
            .column(col_idx(col::START_TIME))
            .as_primitive::<TimestampNanosecondType>();
        let end = slice
            .column(col_idx(col::END_TIME))
            .as_primitive::<TimestampNanosecondType>();
        let mut trace_hour: HashMap<TraceId, HourBucket> = HashMap::new();
        for (k, h) in &sticky {
            let e = trace_hour.entry(k.0).or_insert(*h);
            *e = (*e).min(*h);
        }
        let mut groups: BTreeMap<(HourBucket, bool), Vec<u32>> = BTreeMap::new();
        for (i, k) in ks.iter().enumerate() {
            let g = route(
                *k,
                start.is_valid(i).then(|| start.value(i)),
                end.is_valid(i).then(|| end.value(i)),
                &sticky,
                trace_hour.get(&k.0).copied(),
                fallback,
            );
            groups.entry(g).or_default().push(i as u32);
        }
        w.limit = (cx.cfg.target_segment.as_u64() as f64 * ratio) as u64;
        for ((h, orphan), idx) in groups {
            let b = take_record_batch(&slice, &UInt32Array::from(idx))?;
            w.send(h, orphan, b).await?;
        }
    }
    let outputs = w.finish().await?;
    stats.rotated = w.rotated;
    stats.writer_cap = w.capped;
    stats.orphan_outputs = outputs.iter().filter(|o| o.1).count() as u32;
    // The last cancellation point: the commit is never interrupted (D7).
    cancel.check()?;
    failpoint::hit("compact_after_upload");
    let (l0, l1) = level_counts(&job.inputs);
    *report = Some(JobReport {
        project: job.project_id.to_string(),
        kind: job.kind.as_str(),
        l0_inputs: l0,
        l1_inputs: l1,
        outputs: outputs
            .iter()
            .map(|(o, orphan)| (o.meta.partition_hour.0, *orphan, o.meta.row_count, o.meta.byte_size))
            .collect(),
        rows_in: job.inputs.iter().map(|s| s.row_count).sum(),
        elapsed_ms: 0,
        stats,
    });
    let commit = CompactionCommit {
        job_id: job.id,
        outputs: outputs.into_iter().map(|o| o.0).collect(),
        max_age_ms: i64::try_from((cx.cfg.orphan_grace / 2).as_millis()).unwrap_or(i64::MAX),
    };
    let mut tries = 0;
    loop {
        tries += 1;
        match cx.meta.commit_compaction(commit.clone()).await {
            Ok(()) => break,
            Err(e) if tries < 3 && e.kind() == ErrorKind::Transient => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
    failpoint::hit("compact_after_swap");
    Ok(())
}

/// Context from the merged CONTEXT_COLUMNS rows.
fn context(m: &RecordBatch) -> Result<Context, IngestError> {
    let s = m.schema();
    let parent = m.column(s.index_of(col::PARENT_SPAN_ID)?).as_binary::<i32>();
    let dotted = m.column(s.index_of(col::DOTTED_ORDER)?).as_string::<i32>();
    let start = m
        .column(s.index_of(col::START_TIME)?)
        .as_primitive::<TimestampNanosecondType>();
    let mut out = Context::new();
    for (i, k) in keys(m)?.into_iter().enumerate() {
        out.insert(
            k,
            Known {
                start_ns: start.is_valid(i).then(|| start.value(i)),
                parent: if parent.is_valid(i) {
                    Some(SpanId::from_slice(parent.value(i))?)
                } else {
                    None
                },
                dotted: dotted.is_valid(i).then(|| dotted.value(i).to_string()),
            },
        );
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
    let mut nodes: HashMap<Key, Node<Key>> = all
        .iter()
        .map(|(k, v)| (*k, (v.parent.map(|p| (k.0, p)), v.start_ns, v.dotted.clone())))
        .collect();
    let (mut fills, mut starts, mut dotteds, mut want) = (0u64, Vec::new(), Vec::new(), Vec::new());
    for (i, k) in ks.iter().enumerate() {
        let o = old.get(k);
        let st = if start.is_valid(i) {
            Some(start.value(i))
        } else {
            let s = o.and_then(|o| o.start_ns);
            fills += u64::from(s.is_some());
            s
        };
        let d = if dotted.is_valid(i) {
            Some(dotted.value(i).to_string())
        } else {
            o.and_then(|o| o.dotted.clone())
        };
        let p = if parent.is_valid(i) {
            Some(SpanId::from_slice(parent.value(i))?)
        } else {
            o.and_then(|o| o.parent)
        };
        if d.is_none() {
            want.push(*k);
        }
        nodes.insert(*k, (p.map(|p| (k.0, p)), st, d.clone()));
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
