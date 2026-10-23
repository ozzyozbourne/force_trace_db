//! `run_job`: trace slices -> scan + context -> merge (Keep) -> start fill -> dotted backfill -> sort -> route to
//! per-(hour, orphan) writers -> commit (plan §2.8, CR §7).
use super::{CompactCtx, JobReport, JobStats, level_counts};
use crate::IngestError;
use arrow::array::{Array, ArrayRef, AsArray, StringArray, TimestampNanosecondArray, UInt32Array};
use arrow::compute::take_record_batch;
use arrow::datatypes::TimestampNanosecondType;
use arrow::record_batch::RecordBatch;
use forge_meta::{CompactionCommit, CompactionJob, NewSegment};
use forge_model::catalog::{HourBucket, Level, SegmentId, SegmentMeta, TraceRange, paths};
use forge_model::dotted::resolve_with_known;
use forge_model::error::{Classify, ErrorKind};
use forge_model::failpoint;
use forge_model::ids::{ProjectId, SpanId, TraceId};
use forge_model::merge::{Cancel, Tombstones, merge_projected, sort_physical};
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

type Key = (TraceId, SpanId);

/// What the job knows about a key from L1 rows it does not rewrite.
#[derive(Clone, Debug)]
pub struct Known {
    pub start_ns: Option<i64>,
    pub parent: Option<SpanId>,
    pub dotted: Option<String>,
}
pub type Context = HashMap<Key, Known>;

/// Trace slices (CR §7 verbatim): per slice the traces and each input's contiguous row range.
#[allow(clippy::type_complexity)]
pub fn plan_slices(
    inputs: &[(SegmentMeta, Vec<TraceRange>)],
    budget: u64,
    ratio: f64,
) -> Vec<(Vec<TraceId>, Vec<(usize, std::ops::Range<u64>)>)> {
    let mut by_trace: BTreeMap<TraceId, Vec<(usize, TraceRange)>> = BTreeMap::new();
    for (i, (_, ranges)) in inputs.iter().enumerate() {
        for r in ranges {
            by_trace.entry(r.trace_id).or_default().push((i, *r));
        }
    }
    let bytes_per_row: Vec<f64> = inputs
        .iter()
        .map(|(s, _)| s.byte_size as f64 / s.row_count.max(1) as f64 * ratio)
        .collect();
    let mut out = Vec::new();
    let (mut traces, mut ranges, mut acc) = (Vec::new(), BTreeMap::<usize, std::ops::Range<u64>>::new(), 0f64);
    for (t, locs) in by_trace {
        let w: f64 = locs.iter().map(|(i, r)| r.row_count as f64 * bytes_per_row[*i]).sum();
        if !traces.is_empty() && acc + w > budget as f64 {
            out.push((
                std::mem::take(&mut traces),
                std::mem::take(&mut ranges).into_iter().collect(),
            ));
            acc = 0.0;
        }
        for (i, r) in locs {
            let e = ranges.entry(i).or_insert(r.row_start..r.row_start);
            e.end = r.row_start + r.row_count;
        }
        traces.push(t);
        acc += w;
    }
    if !traces.is_empty() {
        out.push((traces, ranges.into_iter().collect()));
    }
    out
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

/// One open output.
struct Writer {
    id: SegmentId,
    tx: Option<futures::channel::mpsc::Sender<RecordBatch>>,
    handle: tokio::task::JoinHandle<StorageResult<WrittenSegment>>,
    bytes: u64,
    blobs: BTreeSet<String>,
}

struct Writers {
    store: Arc<dyn SegmentStore>,
    project: ProjectId,
    /// Decoded bytes per output before rotation.
    limit: u64,
    open: BTreeMap<(HourBucket, bool), Writer>,
    done: Vec<(NewSegment, bool)>,
    /// Every output id started (for the best-effort delete on failure).
    started: Vec<(HourBucket, SegmentId)>,
    rotated: usize,
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

    /// Sends one routed, sorted batch (whole traces), rotating at a trace boundary.
    async fn send(&mut self, hour: HourBucket, orphan: bool, b: RecordBatch) -> Result<(), IngestError> {
        let size = b.get_array_memory_size() as u64;
        let rotate = self
            .open
            .get(&(hour, orphan))
            .is_some_and(|w| w.bytes > 0 && w.bytes + size > self.limit);
        if rotate {
            let w = self.open.remove(&(hour, orphan)).expect("present");
            self.finish_one(w, orphan).await?;
            self.rotated += 1;
        }
        if !self.open.contains_key(&(hour, orphan)) {
            let w = self.start(hour);
            self.open.insert((hour, orphan), w);
        }
        let w = self.open.get_mut(&(hour, orphan)).expect("present");
        w.bytes += size;
        w.blobs
            .extend(forge_model::blob::blob_refs_in(std::slice::from_ref(&b)));
        if w.tx.as_mut().expect("open").send(b).await.is_err() {
            // The writer stopped early (an error): surface it.
            let w = self.open.remove(&(hour, orphan)).expect("present");
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

pub async fn run_job(cx: &CompactCtx, job: CompactionJob) -> Result<JobReport, IngestError> {
    failpoint::hit("compact_after_lease");
    let t0 = std::time::Instant::now();
    let cancel = Cancel::new();
    let mut w = Writers {
        store: cx.segments.clone(),
        project: job.project_id.clone(),
        limit: cx.cfg.target_segment.as_u64().saturating_mul(4),
        open: BTreeMap::new(),
        done: Vec::new(),
        started: Vec::new(),
        rotated: 0,
    };
    let r = run_inner(cx, &job, &cancel, &mut w, t0).await;
    if let Err(e) = &r {
        w.abort();
        let _ = cx.meta.fail_compaction(job.id, &e.to_string()).await;
        let paths: Vec<object_store::path::Path> = w
            .started
            .iter()
            .filter_map(|(h, id)| paths::segment_path(&job.project_id, Level::L1, *h, *id).ok())
            .map(object_store::path::Path::from)
            .collect();
        let _ = crate::lifecycle::delete_all(cx.segments.object_store().as_ref(), paths).await;
    }
    r
}

async fn run_inner(
    cx: &CompactCtx,
    job: &CompactionJob,
    cancel: &Cancel,
    w: &mut Writers,
    t0: std::time::Instant,
) -> Result<JobReport, IngestError> {
    let mut stats = JobStats::default();
    let mut inputs = Vec::with_capacity(job.inputs.len());
    for s in &job.inputs {
        inputs.push((s.clone(), cx.meta.segment_traces(s.id).await?));
    }
    let input_ids: BTreeSet<SegmentId> = job.inputs.iter().map(|s| s.id).collect();
    let fallback = job
        .inputs
        .iter()
        .filter(|s| s.level == Level::L0)
        .max_by_key(|s| s.min_version)
        .or(job.inputs.last())
        .map(|s| s.partition_hour)
        .expect("a job has inputs");
    let mut ratio = 4.0;
    let ctx_schema = projected_schema(&CONTEXT_COLUMNS);
    let slices = plan_slices(&inputs, cx.cfg.slice_bytes.as_u64(), ratio);
    for (traces, ranges) in slices {
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
        // 2. Context: non-input L1 locations of the slice's traces.
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
        let mut ctx_batches = Vec::new();
        for (seg, mut rs) in per_seg.into_values() {
            rs.sort_by_key(|r| r.start);
            let mut merged: Vec<std::ops::Range<u64>> = Vec::new();
            for r in rs {
                match merged.last_mut() {
                    Some(l) if r.start <= l.end => l.end = l.end.max(r.end),
                    _ => merged.push(r),
                }
            }
            for r in merged {
                let req = ScanRequest {
                    projection: Some(CONTEXT_COLUMNS.to_vec()),
                    rows: RowSelection::Range(r),
                    ..Default::default()
                };
                for b in scan_all(cx.segments.as_ref(), &seg, req).await? {
                    for k in keys(&b)? {
                        sticky.insert(k, seg.partition_hour);
                    }
                    ctx_batches.push(b);
                }
            }
        }
        // 3. Merge + fill + backfill + sort on the blocking pool.
        let (c2, s2) = (cancel.clone(), ctx_schema.clone());
        let (slice, fills, backfills) = tokio::task::spawn_blocking(move || -> Result<_, IngestError> {
            if failpoint::fires("compact_merge_panic") {
                panic!("FORGE_FAILPOINT compact_merge_panic hit");
            }
            let ctx = context(&merge_projected(&s2, &ctx_batches, Tombstones::Keep, &c2)?)?;
            let merged = merge_projected(&spans_schema(), &batches, Tombstones::Keep, &c2)?;
            drop(batches);
            c2.check()?;
            let (filled, fills, backfills) = fill(&merged, &ctx)?;
            c2.check()?;
            Ok((sort_physical(&filled)?, fills, backfills))
        })
        .await
        .map_err(|e| IngestError::Internal(format!("compaction merge: {e}")))??;
        stats.fills += fills as u64;
        stats.backfills += backfills as u64;
        // 4. Route.
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
            let b = take_record_batch(&slice, &UInt32Array::from(idx)).map_err(arrow)?;
            w.send(h, orphan, b).await?;
        }
    }
    let outputs = w.finish().await?;
    stats.rotated = w.rotated as u32;
    stats.orphan_outputs = outputs.iter().filter(|o| o.1).count() as u32;
    let report_outputs = outputs
        .iter()
        .map(|(o, orphan)| (o.meta.partition_hour.0, *orphan, o.meta.row_count, o.meta.byte_size))
        .collect();
    let outputs: Vec<NewSegment> = outputs.into_iter().map(|o| o.0).collect();
    failpoint::hit("compact_after_upload");
    let commit = CompactionCommit {
        job_id: job.id,
        outputs,
        max_age_ms: (cx.cfg.orphan_grace / 2).as_millis() as i64,
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
    let (l0, l1) = level_counts(&job.inputs);
    Ok(JobReport {
        project: job.project_id.to_string(),
        kind: job.kind.as_str(),
        l0_inputs: l0,
        l1_inputs: l1,
        outputs: report_outputs,
        rows_in: job.inputs.iter().map(|s| s.row_count).sum(),
        elapsed_ms: t0.elapsed().as_millis() as u64,
        stats,
    })
}

/// IngestError has no Arrow variant (plan gap): go through ModelError.
fn arrow(e: arrow::error::ArrowError) -> IngestError {
    forge_model::error::ModelError::from(e).into()
}

/// Context from the merged CONTEXT_COLUMNS rows.
fn context(m: &RecordBatch) -> Result<Context, IngestError> {
    let s = m.schema();
    let parent = m
        .column(s.index_of(col::PARENT_SPAN_ID).map_err(arrow)?)
        .as_binary::<i32>();
    let dotted = m
        .column(s.index_of(col::DOTTED_ORDER).map_err(arrow)?)
        .as_string::<i32>();
    let start = m
        .column(s.index_of(col::START_TIME).map_err(arrow)?)
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

/// Start fill (CR §2.4) then dotted backfill (CR §4, D5). Returns the new batch, #fills, #backfills.
fn fill(m: &RecordBatch, ctx: &Context) -> Result<(RecordBatch, usize, usize), IngestError> {
    let ks = keys(m)?;
    let start = m
        .column(col_idx(col::START_TIME))
        .as_primitive::<TimestampNanosecondType>();
    let parent = m.column(col_idx(col::PARENT_SPAN_ID)).as_binary::<i32>();
    let dotted = m.column(col_idx(col::DOTTED_ORDER)).as_string::<i32>();
    let mut fills = 0;
    let starts: Vec<Option<i64>> = (0..m.num_rows())
        .map(|i| {
            if start.is_valid(i) {
                return Some(start.value(i));
            }
            let s = ctx.get(&ks[i]).and_then(|k| k.start_ns);
            fills += usize::from(s.is_some());
            s
        })
        .collect();
    type Node = (Option<SpanId>, Option<i64>, Option<String>);
    let mut nodes: HashMap<Key, Node> = ctx
        .iter()
        .map(|(k, v)| (*k, (v.parent, v.start_ns, v.dotted.clone())))
        .collect();
    let mut want = Vec::new();
    for (i, k) in ks.iter().enumerate() {
        let p = if parent.is_valid(i) {
            Some(SpanId::from_slice(parent.value(i))?)
        } else {
            None
        };
        let d = dotted.is_valid(i).then(|| dotted.value(i).to_string());
        if d.is_none() {
            want.push(*k);
        }
        nodes.insert(*k, (p, starts[i], d));
    }
    let resolved = resolve_with_known(&nodes, want);
    let new_dotted: StringArray = (0..m.num_rows())
        .map(|i| {
            if dotted.is_valid(i) {
                Some(dotted.value(i).to_string())
            } else {
                resolved.get(&ks[i]).cloned()
            }
        })
        .collect();
    let new_start: ArrayRef = Arc::new(TimestampNanosecondArray::from(starts).with_timezone(TIMEZONE));
    let mut cols = m.columns().to_vec();
    cols[col_idx(col::START_TIME)] = new_start;
    cols[col_idx(col::DOTTED_ORDER)] = Arc::new(new_dotted);
    Ok((
        RecordBatch::try_new(m.schema(), cols).map_err(arrow)?,
        fills,
        resolved.len(),
    ))
}
