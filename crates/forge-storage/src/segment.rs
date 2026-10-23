//! Vortex segment files: one file per (project, flush or compaction output).

use crate::{
    BatchStream, BuiltStore, RowSelection, ScanRequest, SegmentSpec, SegmentStore, StorageError, StorageResult,
    WrittenSegment,
};
use arrow::array::{Array, AsArray, StringArray};
use arrow::compute::{max, min};
use arrow::datatypes::{Int64Type, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use forge_model::catalog::{SegmentMeta, SegmentState, TraceRange, paths};
use forge_model::error::ModelError;
use forge_model::ids::TraceId;
use forge_model::row::validate_batch;
use forge_model::schema::PAYLOAD_COLUMNS;
use forge_model::schema::{SPANS_SCHEMA_VERSION, col, projected_schema, spans_schema};
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use vortex::VortexSessionDefault;
use vortex::array::ArrayRef;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::arrow::ArrowSessionExt;
use vortex::compressor::BtrBlocksCompressorBuilder;
use vortex::dtype::DType;
use vortex::dtype::FieldPath;
use vortex::error::VortexResult;
use vortex::expr::{root, select};
use vortex::file::WriteStrategyBuilder;
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::io::session::RuntimeSessionExt;
use vortex::layout::LayoutStrategy;
use vortex::scan::selection::Selection;
use vortex::session::VortexSession;

pub struct VortexSegmentStore {
    store: Arc<dyn ObjectStore>,
    local_root: Option<PathBuf>,
    session: VortexSession,
    /// spans_schema as a Vortex dtype, converted once.
    dtype: DType,
    strategy: Arc<dyn LayoutStrategy>,
    /// D10b: Vortex footers by segment (segments are write-once, so no invalidation).
    footers: moka::future::Cache<forge_model::catalog::SegmentId, vortex::file::Footer>,
}

impl std::fmt::Debug for VortexSegmentStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VortexSegmentStore")
            .field("store", &self.store)
            .field("local_root", &self.local_root)
            .finish_non_exhaustive()
    }
}

impl VortexSegmentStore {
    pub fn new(built: BuiltStore) -> Self {
        let session = VortexSession::default().with_tokio();
        let dtype = session
            .arrow()
            .from_arrow_schema(&spans_schema())
            .expect("the static spans schema converts to a Vortex dtype");
        let strategy = segment_strategy(&session);
        Self {
            store: built.store,
            local_root: built.local_root,
            session,
            dtype,
            strategy,
            footers: moka::future::Cache::new(4096),
        }
    }
    /// Some(absolute root) for file:// stores.
    pub fn local_root(&self) -> Option<&std::path::Path> {
        self.local_root.as_deref()
    }
}

#[derive(Default)]
struct Ingest {
    stats: SegmentStats,
    err: Option<StorageError>,
}

fn lock(m: &Mutex<Ingest>) -> MutexGuard<'_, Ingest> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Default)]
struct SegmentStats {
    rows: u64,
    traces: Vec<TraceRange>,
    /// Sort key of the last row of the previous batch.
    last: Option<OwnedKey>,
    /// Span ids seen in the current trace (rows are trace-contiguous), so a repeated span is caught even
    /// when its dotted_order differs.
    trace_spans: HashSet<Vec<u8>>,
    min_start: Option<i64>,
    max_start: Option<i64>,
    start_nulls: u64,
    min_version: Option<u64>,
    max_version: Option<u64>,
}

/// Physical sort key: (trace_id, dotted_order NULLS LAST, span_id); the project is checked separately.
type Key<'a> = (&'a [u8], (bool, &'a str), &'a [u8]);
type OwnedKey = (Vec<u8>, (bool, String), Vec<u8>);

fn owned(k: Key<'_>) -> OwnedKey {
    (k.0.to_vec(), (k.1.0, k.1.1.to_string()), k.2.to_vec())
}
fn borrowed(k: &OwnedKey) -> Key<'_> {
    (&k.0, (k.1.0, &k.1.1), &k.2)
}

/// The dotted_order part of the sort key: NULLS LAST.
pub(crate) fn nulls_last(d: &StringArray, i: usize) -> (bool, &str) {
    if d.is_null(i) { (true, "") } else { (false, d.value(i)) }
}

/// Extends the running [lo, hi] range with `x`.
pub(crate) fn widen<T: Ord + Copy>(lo: &mut Option<T>, hi: &mut Option<T>, x: T) {
    *lo = Some(lo.map_or(x, |m| m.min(x)));
    *hi = Some(hi.map_or(x, |m| m.max(x)));
}

fn version(v: i64) -> StorageResult<u64> {
    u64::try_from(v).map_err(|_| ModelError::Schema("version must be >= 0".into()).into())
}

impl SegmentStats {
    fn push(&mut self, spec_project: &str, b: &RecordBatch) -> StorageResult<()> {
        let p = b.column_by_name(col::PROJECT_ID).unwrap().as_string::<i32>();
        let t = b.column_by_name(col::TRACE_ID).unwrap().as_binary::<i32>();
        let d = b.column_by_name(col::DOTTED_ORDER).unwrap().as_string::<i32>();
        let sp = b.column_by_name(col::SPAN_ID).unwrap().as_binary::<i32>();
        let s = b
            .column_by_name(col::START_TIME)
            .unwrap()
            .as_primitive::<TimestampNanosecondType>();
        let v = b.column_by_name(col::VERSION).unwrap().as_primitive::<Int64Type>();
        let key = |i: usize| -> Key<'_> { (t.value(i), nulls_last(d, i), sp.value(i)) };
        for i in 0..b.num_rows() {
            let proj = p.value(i);
            if proj != spec_project {
                return Err(StorageError::Unsorted(format!(
                    "row project {proj} != segment project {spec_project}"
                )));
            }
            let cur = key(i);
            let prev = if i > 0 {
                Some(key(i - 1))
            } else {
                self.last.as_ref().map(borrowed)
            };
            let bad = |what: &str| {
                StorageError::Unsorted(format!(
                    "{what} at row {}: trace_id {}, span_id {}, version {}",
                    self.rows + i as u64,
                    hex::encode(cur.0),
                    hex::encode(cur.2),
                    v.value(i)
                ))
            };
            if prev.is_some_and(|prev| prev > cur) {
                return Err(bad("row sorts before the previous row"));
            }
            match self.traces.last_mut() {
                Some(last) if last.trace_id.as_bytes()[..] == *cur.0 => last.row_count += 1,
                _ => {
                    self.trace_spans.clear();
                    self.traces.push(TraceRange {
                        trace_id: TraceId::from_slice(cur.0)?,
                        row_start: self.rows + i as u64,
                        row_count: 1,
                    });
                }
            }
            if !self.trace_spans.insert(cur.2.to_vec()) {
                return Err(bad("span appears twice in its trace (versions must be merged first)"));
            }
        }
        if let Some(i) = b.num_rows().checked_sub(1) {
            self.last = Some(owned(key(i)));
        }
        self.rows += b.num_rows() as u64;
        self.start_nulls += s.null_count() as u64;
        if let (Some(lo), Some(hi)) = (min(s), max(s)) {
            widen(&mut self.min_start, &mut self.max_start, lo);
            widen(&mut self.min_start, &mut self.max_start, hi);
        }
        if let (Some(lo), Some(hi)) = (min(v), max(v)) {
            widen(&mut self.min_version, &mut self.max_version, version(lo)?);
            widen(&mut self.min_version, &mut self.max_version, version(hi)?);
        }
        Ok(())
    }
}

/// The writer's outcome: a stashed input error first, then the writer's own error, then an empty input.
fn outcome(stashed: Option<StorageError>, written: StorageResult<()>, rows: u64) -> StorageResult<()> {
    if let Some(e) = stashed {
        return Err(e);
    }
    written?;
    if rows == 0 {
        return Err(StorageError::EmptySegment);
    }
    Ok(())
}

/// `PutMode::Create` of a segment object (`.vortex`, `.fts`). A retry of a put that landed but was reported as
/// failed counts as written: same key (fresh per segment id), same size.
pub async fn put_create(store: &dyn ObjectStore, location: &Path, buf: Vec<u8>) -> StorageResult<()> {
    let size = buf.len() as u64;
    let create = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    if let Err(e) = store.put_opts(location, PutPayload::from(buf), create).await {
        let landed = matches!(e, object_store::Error::AlreadyExists { .. })
            && store.head(location).await.is_ok_and(|m| m.size == size);
        if !landed {
            return Err(e.into());
        }
    }
    Ok(())
}

#[async_trait]
impl SegmentStore for VortexSegmentStore {
    async fn write_segment(&self, spec: SegmentSpec, rows: BatchStream) -> StorageResult<WrittenSegment> {
        // Batches are validated and measured as the writer pulls them. A failure is stashed so the caller
        // gets the original StorageError rather than its Vortex wrapping.
        let state = Arc::new(Mutex::new(Ingest::default()));
        let schema = spans_schema();
        let dtype = self.dtype.clone();
        let (st, session, project) = (state.clone(), self.session.clone(), spec.project_id.clone());
        let stream = rows.map(move |b| -> VortexResult<ArrayRef> {
            let checked = b.and_then(|b| {
                validate_batch(&b)?;
                lock(&st).stats.push(project.as_str(), &b)?;
                Ok(b)
            });
            match checked {
                Ok(b) => session.arrow().from_arrow_record_batch(b, &schema),
                Err(e) => {
                    let msg = e.to_string();
                    lock(&st).err.get_or_insert(e);
                    Err(vortex::error::vortex_err!("{msg}"))
                }
            }
        });
        let mut buf: Vec<u8> = Vec::new();
        let written = self
            .session
            .write_options()
            .with_strategy(self.strategy.clone())
            .with_metadata_segment(crate::inspect::FORGE_META_KEY, spec_json(&spec).into_bytes())
            .write(&mut buf, ArrayStreamAdapter::new(dtype, stream))
            .await
            .map(drop)
            .map_err(StorageError::from);
        let Ingest { stats, err } = std::mem::take(&mut *lock(&state));
        outcome(err, written, stats.rows)?;
        let path = paths::segment_path(&spec.project_id, spec.level, spec.partition_hour, spec.id)?;
        let byte_size = u64::try_from(buf.len()).unwrap_or(u64::MAX);
        put_create(self.store.as_ref(), &Path::from(path.as_str()), buf).await?;
        let traces = stats.traces;
        let meta = SegmentMeta {
            id: spec.id,
            project_id: spec.project_id,
            level: spec.level,
            partition_hour: spec.partition_hour,
            path,
            row_count: stats.rows,
            byte_size,
            fts_byte_size: None,
            min_start_ns: stats.min_start,
            max_start_ns: stats.max_start,
            start_time_nulls: stats.start_nulls,
            min_trace_id: traces.first().map(|t| t.trace_id).unwrap_or_default(),
            max_trace_id: traces.last().map(|t| t.trace_id).unwrap_or_default(),
            min_version: stats.min_version.unwrap_or(0),
            max_version: stats.max_version.unwrap_or(0),
            schema_version: SPANS_SCHEMA_VERSION,
            created_at_ms: forge_model::time::now_ms(),
            state: SegmentState::Live,
            deleted_at_ms: None,
        };
        Ok(WrittenSegment { meta, traces })
    }

    async fn scan(&self, seg: &SegmentMeta, req: ScanRequest) -> StorageResult<BatchStream> {
        match &req.rows {
            RowSelection::Range(r) if r.start > r.end || r.end > seg.row_count => {
                return Err(StorageError::InvalidInput(format!(
                    "row range {}..{} is invalid for segment {} with {} rows",
                    r.start, r.end, seg.id, seg.row_count
                )));
            }
            RowSelection::Rows(rows) if rows.max().is_some_and(|m| m >= seg.row_count) => {
                return Err(StorageError::InvalidInput(format!(
                    "row id {} is invalid for segment {} with {} rows",
                    rows.max().unwrap_or_default(),
                    seg.id,
                    seg.row_count
                )));
            }
            // Nothing selected: do not even open the file (its footer is ~2 GETs).
            RowSelection::Rows(rows) if rows.is_empty() => return Ok(futures::stream::empty().boxed()),
            _ => {}
        }
        let opts = self.session.open_options().with_file_size(seg.byte_size);
        let cached = self.footers.get(&seg.id).await;
        let opts = match &cached {
            Some(f) => opts.with_footer(f.clone()),
            None => opts,
        };
        let file = opts
            .open_object_store(&self.store, Path::from(seg.path.as_str()))
            .await?;
        if cached.is_none() {
            self.footers.insert(seg.id, file.footer().clone()).await;
        }
        let mut sb = file.scan()?;
        let schema = match req.projection {
            Some(cols) => {
                let schema = projected_schema(&cols);
                sb = sb.with_projection(select(cols, root()).bind(file.dtype())?);
                schema
            }
            None => spans_schema(),
        };
        sb = match req.rows {
            RowSelection::All => sb,
            RowSelection::Range(r) => sb.with_row_range(r),
            // File-absolute row ids; Vortex turns sparse ids into exact split ranges (attempt_split_ranges).
            RowSelection::Rows(rows) => sb.with_selection(Selection::IncludeRoaring(Arc::unwrap_or_clone(rows))),
        };
        if let Some(f) = req.filter {
            sb = sb.with_filter(f.bind(file.dtype())?);
        }
        if let Some(l) = req.limit {
            sb = sb.with_limit(l);
        }
        let s = sb.into_record_batch_stream(schema)?;
        Ok(s.map_err(StorageError::Arrow).boxed())
    }

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }
}

/// Scalar columns: 1024-row blocks (zone maps + chunks), default BtrBlocks (fast decode).
/// Payload columns (inputs, outputs, events, attributes): 64-row chunks, compact preset (Zstd).
fn segment_strategy(session: &VortexSession) -> Arc<dyn LayoutStrategy> {
    let mut b = WriteStrategyBuilder::from_session(session)
        .with_row_block_size(1024)
        .with_data_block_target_bytes(None);
    let payload = WriteStrategyBuilder::from_session(session)
        .with_row_block_size(64)
        .with_data_block_target_bytes(None)
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::from_session(session).with_compact())
        .build();
    for c in PAYLOAD_COLUMNS {
        b = b.with_field_writer(FieldPath::from_name(c), payload.clone());
    }
    b.build()
}

/// The `forge.segment` metadata: every value is from a safe charset, so no serde.
fn spec_json(spec: &SegmentSpec) -> String {
    format!(
        r#"{{"segment_id":"{}","project_id":"{}","level":"{:?}","partition_hour":{},"schema_version":{},"created_at_ms":{}}}"#,
        spec.id,
        spec.project_id,
        spec.level,
        spec.partition_hour.0,
        SPANS_SCHEMA_VERSION,
        forge_model::time::now_ms()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_error_wins_over_empty_input() {
        let vortex_err = || StorageError::Vortex(vortex::error::vortex_err!("writer failed"));
        assert!(matches!(
            outcome(None, Err(vortex_err()), 0),
            Err(StorageError::Vortex(_))
        ));
        assert!(matches!(outcome(None, Ok(()), 0), Err(StorageError::EmptySegment)));
        let stashed = Some(StorageError::Unsorted("x".into()));
        assert!(matches!(
            outcome(stashed, Err(vortex_err()), 0),
            Err(StorageError::Unsorted(_))
        ));
        assert!(outcome(None, Ok(()), 1).is_ok());
    }
}
