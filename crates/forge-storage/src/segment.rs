//! Vortex segment files: one file per (project, flush or compaction output).

use crate::{
    BatchStream, BuiltStore, RowSelection, ScanRequest, SegmentSpec, SegmentStore, StorageError, StorageResult,
    WrittenSegment,
};
use arrow::array::{Array, AsArray};
use arrow::datatypes::{Int64Type, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use forge_model::catalog::{SegmentMeta, SegmentState, TraceRange, paths};
use forge_model::ids::TraceId;
use forge_model::row::validate_batch;
use forge_model::schema::{SPANS_SCHEMA_VERSION, col, projected_schema, spans_schema};
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use object_store::path::Path;
use object_store::{PutMode, PutOptions, PutPayload};
use std::path::PathBuf;
use std::sync::Arc;
use vortex::VortexSessionDefault;
use vortex::array::ArrayRef;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::arrow::ArrowSessionExt;
use vortex::error::VortexResult;
use vortex::expr::{root, select};
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

#[derive(Debug)]
pub struct VortexSegmentStore {
    store: Arc<dyn ObjectStore>,
    local_root: Option<PathBuf>,
    session: VortexSession,
}

impl VortexSegmentStore {
    pub fn new(built: BuiltStore) -> Self {
        Self {
            store: built.store,
            local_root: built.local_root,
            session: VortexSession::default().with_tokio(),
        }
    }
    /// Some(absolute root) for file:// stores.
    pub fn local_root(&self) -> Option<&std::path::Path> {
        self.local_root.as_deref()
    }
}

#[derive(Default)]
struct SegmentStats {
    rows: u64,
    traces: Vec<TraceRange>,
    min_start: Option<i64>,
    max_start: Option<i64>,
    start_nulls: u64,
    min_version: Option<u64>,
    max_version: Option<u64>,
}

/// Extends the running [lo, hi] range with `x`.
fn widen<T: Ord + Copy>(lo: &mut Option<T>, hi: &mut Option<T>, x: T) {
    *lo = Some(lo.map_or(x, |m| m.min(x)));
    *hi = Some(hi.map_or(x, |m| m.max(x)));
}

impl SegmentStats {
    fn push(&mut self, spec_project: &str, b: &RecordBatch) -> StorageResult<()> {
        let p = b.column_by_name(col::PROJECT_ID).unwrap().as_string::<i32>();
        let t = b.column_by_name(col::TRACE_ID).unwrap().as_binary::<i32>();
        let s = b
            .column_by_name(col::START_TIME)
            .unwrap()
            .as_primitive::<TimestampNanosecondType>();
        let v = b.column_by_name(col::VERSION).unwrap().as_primitive::<Int64Type>();
        for i in 0..b.num_rows() {
            let proj = p.value(i);
            if proj != spec_project {
                return Err(StorageError::Unsorted(format!(
                    "row project {proj} != segment project {spec_project}"
                )));
            }
            let tid = TraceId::from_slice(t.value(i)).map_err(|e| StorageError::Unsorted(e.to_string()))?;
            match self.traces.last_mut() {
                Some(last) if last.trace_id == tid => last.row_count += 1,
                Some(last) if last.trace_id > tid => {
                    return Err(StorageError::Unsorted(format!(
                        "trace_id {tid} after {}",
                        last.trace_id
                    )));
                }
                _ => self.traces.push(TraceRange {
                    trace_id: tid,
                    row_start: self.rows,
                    row_count: 1,
                }),
            }
            if s.is_null(i) {
                self.start_nulls += 1;
            } else {
                widen(&mut self.min_start, &mut self.max_start, s.value(i));
            }
            widen(&mut self.min_version, &mut self.max_version, v.value(i) as u64);
            self.rows += 1;
        }
        Ok(())
    }
}

#[async_trait]
impl SegmentStore for VortexSegmentStore {
    async fn write_segment(&self, spec: SegmentSpec, rows: BatchStream) -> StorageResult<WrittenSegment> {
        let batches: Vec<RecordBatch> = rows.try_collect().await?;
        let mut stats = SegmentStats::default();
        for b in &batches {
            validate_batch(b)?;
            stats.push(spec.project_id.as_str(), b)?;
        }
        if stats.rows == 0 {
            return Err(StorageError::EmptySegment);
        }
        let schema = spans_schema();
        let dtype = self.session.arrow().from_arrow_schema(&schema)?;
        let s2 = self.session.clone();
        let sch2 = schema.clone();
        let stream = futures::stream::iter(batches)
            .map(move |b| -> VortexResult<ArrayRef> { s2.arrow().from_arrow_record_batch(b, &sch2) });
        let mut buf: Vec<u8> = Vec::new();
        self.session
            .write_options()
            .write(&mut buf, ArrayStreamAdapter::new(dtype, stream))
            .await?;
        let path = paths::segment_path(&spec.project_id, spec.level, spec.partition_hour, spec.id);
        let byte_size = u64::try_from(buf.len()).unwrap_or(u64::MAX);
        self.store
            .put_opts(
                &path.clone().into(),
                PutPayload::from(buf),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;
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
        let file = self
            .session
            .open_options()
            .with_file_size(seg.byte_size)
            .open_object_store(&self.store, Path::from(seg.path.as_str()))
            .await?;
        let mut sb = file.scan()?;
        let schema = match &req.projection {
            Some(cols) => {
                sb = sb.with_projection(select(cols.clone(), root()).bind(file.dtype())?);
                projected_schema(cols)
            }
            None => spans_schema(),
        };
        sb = match req.rows {
            RowSelection::All => sb,
            RowSelection::Range(r) => sb.with_row_range(r),
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
