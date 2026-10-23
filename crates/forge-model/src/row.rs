use crate::enums::{RunType, SpanStatus};
use crate::error::{ModelError, Result};
use crate::ids::{ProjectId, SpanId, TraceId};
use crate::schema::{TIMEZONE, col, col_idx, spans_schema, tags_item_field};
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use std::sync::{Arc, LazyLock};

/// One upsert of one span, as produced by normalization. `None` = "this upsert does not set the
/// field" (latest non-null wins). JSON columns hold already-serialized JSON text.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SpanRow {
    pub project_id: Option<ProjectId>, // Option only so Default works; required by push()
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub parent_span_id: Option<SpanId>,
    pub dotted_order: Option<String>,
    pub name: Option<String>,
    pub run_type: Option<RunType>,
    pub start_time_ns: Option<i64>,
    pub end_time_ns: Option<i64>,
    pub status: Option<SpanStatus>,
    pub error: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub prompt_tokens: Option<i64>,
    pub completion_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub cost_usd: Option<f64>,
    pub session_id: Option<String>,
    pub tags: Option<Vec<String>>,
    pub metadata: Option<String>,
    pub attributes: Option<String>,
    pub events: Option<String>,
    pub inputs: Option<String>,
    pub outputs: Option<String>,
    pub version: u64,
    pub deleted: bool,
}

impl SpanRow {
    /// The payload columns, in `schema::PAYLOAD_COLUMNS` order.
    pub fn payload_columns_mut(&mut self) -> [(&'static str, &mut Option<String>); 4] {
        [
            (col::INPUTS, &mut self.inputs),
            (col::OUTPUTS, &mut self.outputs),
            (col::EVENTS, &mut self.events),
            (col::ATTRIBUTES, &mut self.attributes),
        ]
    }
}

/// Appends SpanRows into Arrow builders; finish() yields a spans_schema() batch.
pub struct SpanBatchBuilder {
    project_id: StringBuilder,
    trace_id: BinaryBuilder,
    span_id: BinaryBuilder,
    parent_span_id: BinaryBuilder,
    dotted_order: StringBuilder,
    name: StringBuilder,
    run_type: StringBuilder,
    start_time: TimestampNanosecondBuilder,
    end_time: TimestampNanosecondBuilder,
    status: StringBuilder,
    error: StringBuilder,
    model: StringBuilder,
    provider: StringBuilder,
    prompt_tokens: Int64Builder,
    completion_tokens: Int64Builder,
    total_tokens: Int64Builder,
    cost_usd: Float64Builder,
    session_id: StringBuilder,
    tags: ListBuilder<StringBuilder>,
    metadata: StringBuilder,
    attributes: StringBuilder,
    events: StringBuilder,
    inputs: StringBuilder,
    outputs: StringBuilder,
    version: Int64Builder,
    deleted: BooleanBuilder,
}

impl Default for SpanBatchBuilder {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl SpanBatchBuilder {
    pub fn with_capacity(rows: usize) -> Self {
        let s = || StringBuilder::with_capacity(rows, rows * 16);
        let b = |n: usize| BinaryBuilder::with_capacity(rows, rows * n);
        let ts = || TimestampNanosecondBuilder::with_capacity(rows).with_timezone(TIMEZONE);
        Self {
            project_id: s(),
            trace_id: b(TraceId::LEN),
            span_id: b(SpanId::LEN),
            parent_span_id: b(SpanId::LEN),
            dotted_order: s(),
            name: s(),
            run_type: s(),
            start_time: ts(),
            end_time: ts(),
            status: s(),
            error: s(),
            model: s(),
            provider: s(),
            prompt_tokens: Int64Builder::with_capacity(rows),
            completion_tokens: Int64Builder::with_capacity(rows),
            total_tokens: Int64Builder::with_capacity(rows),
            cost_usd: Float64Builder::with_capacity(rows),
            session_id: s(),
            tags: ListBuilder::with_capacity(StringBuilder::with_capacity(rows, rows * 8), rows)
                .with_field(tags_item_field()),
            metadata: s(),
            attributes: s(),
            events: s(),
            inputs: s(),
            outputs: s(),
            version: Int64Builder::with_capacity(rows),
            deleted: BooleanBuilder::with_capacity(rows),
        }
    }
    /// Rows pushed since the last `finish` (every column builder holds one entry per row).
    pub fn len(&self) -> usize {
        self.deleted.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push(&mut self, r: &SpanRow) -> Result<()> {
        let project = r
            .project_id
            .as_ref()
            .ok_or_else(|| ModelError::Schema("project_id missing".into()))?;
        let version = i64::try_from(r.version).map_err(|_| ModelError::Schema("version > i64::MAX".into()))?;
        self.project_id.append_value(project.as_str());
        self.trace_id.append_value(r.trace_id.as_bytes());
        self.span_id.append_value(r.span_id.as_bytes());
        self.parent_span_id
            .append_option(r.parent_span_id.as_ref().map(|p| p.as_bytes()));
        self.dotted_order.append_option(r.dotted_order.as_deref());
        self.name.append_option(r.name.as_deref());
        self.run_type.append_option(r.run_type.map(RunType::as_str));
        self.start_time.append_option(r.start_time_ns);
        self.end_time.append_option(r.end_time_ns);
        self.status.append_option(r.status.map(SpanStatus::as_str));
        self.error.append_option(r.error.as_deref());
        self.model.append_option(r.model.as_deref());
        self.provider.append_option(r.provider.as_deref());
        self.prompt_tokens.append_option(r.prompt_tokens);
        self.completion_tokens.append_option(r.completion_tokens);
        self.total_tokens.append_option(r.total_tokens);
        self.cost_usd.append_option(r.cost_usd);
        self.session_id.append_option(r.session_id.as_deref());
        match &r.tags {
            Some(t) => {
                for x in t {
                    self.tags.values().append_value(x)
                }
                self.tags.append(true)
            }
            None => self.tags.append(false),
        }
        self.metadata.append_option(r.metadata.as_deref());
        self.attributes.append_option(r.attributes.as_deref());
        self.events.append_option(r.events.as_deref());
        self.inputs.append_option(r.inputs.as_deref());
        self.outputs.append_option(r.outputs.as_deref());
        self.version.append_value(version);
        self.deleted.append_value(r.deleted);
        Ok(())
    }

    pub fn finish(&mut self) -> Result<RecordBatch> {
        let cols: Vec<ArrayRef> = vec![
            Arc::new(self.project_id.finish()),
            Arc::new(self.trace_id.finish()),
            Arc::new(self.span_id.finish()),
            Arc::new(self.parent_span_id.finish()),
            Arc::new(self.dotted_order.finish()),
            Arc::new(self.name.finish()),
            Arc::new(self.run_type.finish()),
            Arc::new(self.start_time.finish()),
            Arc::new(self.end_time.finish()),
            Arc::new(self.status.finish()),
            Arc::new(self.error.finish()),
            Arc::new(self.model.finish()),
            Arc::new(self.provider.finish()),
            Arc::new(self.prompt_tokens.finish()),
            Arc::new(self.completion_tokens.finish()),
            Arc::new(self.total_tokens.finish()),
            Arc::new(self.cost_usd.finish()),
            Arc::new(self.session_id.finish()),
            Arc::new(self.tags.finish()),
            Arc::new(self.metadata.finish()),
            Arc::new(self.attributes.finish()),
            Arc::new(self.events.finish()),
            Arc::new(self.inputs.finish()),
            Arc::new(self.outputs.finish()),
            Arc::new(self.version.finish()),
            Arc::new(self.deleted.finish()),
        ];
        Ok(RecordBatch::try_new(spans_schema(), cols)?)
    }
}

/// Checks schema equality and id byte lengths. Call on WAL replay, before segment write, and in tests.
pub fn validate_batch(b: &RecordBatch) -> Result<()> {
    let schema = b.schema();
    if !Arc::ptr_eq(&schema, &spans_schema()) && schema != spans_schema() {
        return Err(ModelError::Schema(format!("schema mismatch: {schema:?}")));
    }
    static ID_COLS: LazyLock<[(&str, usize, usize); 3]> = LazyLock::new(|| {
        [
            (col::TRACE_ID, TraceId::LEN),
            (col::SPAN_ID, SpanId::LEN),
            (col::PARENT_SPAN_ID, SpanId::LEN),
        ]
        .map(|(name, len)| (name, col_idx(name), len))
    });
    for &(name, idx, len) in ID_COLS.iter() {
        let a = b.column(idx).as_binary::<i32>();
        let bad = a
            .value_offsets()
            .windows(2)
            .enumerate()
            .any(|(i, w)| w[1] - w[0] != len as i32 && a.is_valid(i));
        if bad {
            return Err(ModelError::Schema(format!("{name} must be {len} bytes")));
        }
    }
    // Versions are u64 in the model; the i64 column must never hold a negative one.
    let version = b
        .column(col_idx(col::VERSION))
        .as_primitive::<arrow::datatypes::Int64Type>();
    if arrow::compute::min(version).is_some_and(|m| m < 0) {
        return Err(ModelError::Schema("version must be >= 0".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::rows_from_batch;

    fn pid() -> Option<ProjectId> {
        Some(crate::testing::pid("p"))
    }

    #[test]
    fn builds_valid_batches() {
        let full = SpanRow {
            project_id: pid(),
            trace_id: TraceId([1; 16]),
            span_id: SpanId([2; 8]),
            parent_span_id: Some(SpanId([3; 8])),
            dotted_order: Some("d".into()),
            name: Some("n".into()),
            run_type: Some(RunType::Llm),
            start_time_ns: Some(1),
            end_time_ns: Some(2),
            status: Some(SpanStatus::Ok),
            error: Some("e".into()),
            model: Some("m".into()),
            provider: Some("pr".into()),
            prompt_tokens: Some(1),
            completion_tokens: Some(2),
            total_tokens: Some(3),
            cost_usd: Some(0.5),
            session_id: Some("s".into()),
            tags: Some(vec!["a".into(), "b".into()]),
            metadata: Some("{}".into()),
            attributes: Some("{}".into()),
            events: Some("[]".into()),
            inputs: Some("{}".into()),
            outputs: Some("{}".into()),
            version: 1,
            deleted: false,
        };
        let patch = SpanRow {
            project_id: pid(),
            trace_id: TraceId([1; 16]),
            span_id: SpanId([4; 8]),
            version: 2,
            ..Default::default()
        };
        let tomb = SpanRow {
            deleted: true,
            version: 3,
            ..patch.clone()
        };

        let mut b = SpanBatchBuilder::default();
        for r in [&full, &patch, &tomb] {
            b.push(r).unwrap();
        }
        assert_eq!(b.len(), 3);
        let batch = b.finish().unwrap();
        assert!(b.is_empty(), "finish resets len");
        validate_batch(&batch).unwrap();
        assert_eq!(batch.num_rows(), 3);
        for (i, f) in batch.schema().fields().iter().enumerate() {
            let want = if f.is_nullable() { 2 } else { 0 };
            assert_eq!(batch.column(i).null_count(), want, "{}", f.name());
        }
        let deleted = batch.column_by_name(col::DELETED).unwrap().as_boolean();
        assert_eq!(
            deleted.iter().collect::<Vec<_>>(),
            [Some(false), Some(false), Some(true)]
        );
        assert_eq!(rows_from_batch(&batch).unwrap(), vec![full, patch, tomb]);
    }

    #[test]
    fn payload_columns_follow_the_schema_list() {
        let mut r = SpanRow::default();
        let names = r.payload_columns_mut().map(|(n, _)| n);
        assert_eq!(names, crate::schema::PAYLOAD_COLUMNS);
    }

    #[test]
    fn rejects_bad_rows_and_batches() {
        let mut b = SpanBatchBuilder::default();
        assert!(matches!(b.push(&SpanRow::default()), Err(ModelError::Schema(_))));
        b.push(&SpanRow {
            project_id: pid(),
            version: 1,
            ..Default::default()
        })
        .unwrap();
        let good = b.finish().unwrap();
        let mut cols = good.columns().to_vec();
        cols[crate::schema::col_idx(col::TRACE_ID)] = Arc::new(BinaryArray::from_vec(vec![&[0u8; 15][..]]));
        let bad = RecordBatch::try_new(spans_schema(), cols).unwrap();
        assert!(matches!(validate_batch(&bad), Err(ModelError::Schema(_))));
        let mut cols = good.columns().to_vec();
        cols[crate::schema::col_idx(col::VERSION)] = Arc::new(Int64Array::from(vec![-5i64]));
        let negative = RecordBatch::try_new(spans_schema(), cols).unwrap();
        let e = validate_batch(&negative).unwrap_err();
        assert!(e.to_string().contains("version must be >= 0"), "{e}");
    }
}
