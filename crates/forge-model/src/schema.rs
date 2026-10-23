use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use std::sync::{Arc, LazyLock};

/// Column names. Use these everywhere; never string literals.
pub mod col {
    pub const PROJECT_ID: &str = "project_id";
    pub const TRACE_ID: &str = "trace_id";
    pub const SPAN_ID: &str = "span_id";
    pub const PARENT_SPAN_ID: &str = "parent_span_id";
    pub const DOTTED_ORDER: &str = "dotted_order";
    pub const NAME: &str = "name";
    pub const RUN_TYPE: &str = "run_type";
    pub const START_TIME: &str = "start_time";
    pub const END_TIME: &str = "end_time";
    pub const STATUS: &str = "status";
    pub const ERROR: &str = "error";
    pub const MODEL: &str = "model";
    pub const PROVIDER: &str = "provider";
    pub const PROMPT_TOKENS: &str = "prompt_tokens";
    pub const COMPLETION_TOKENS: &str = "completion_tokens";
    pub const TOTAL_TOKENS: &str = "total_tokens";
    pub const COST_USD: &str = "cost_usd";
    pub const SESSION_ID: &str = "session_id";
    pub const TAGS: &str = "tags";
    pub const METADATA: &str = "metadata";
    pub const ATTRIBUTES: &str = "attributes";
    pub const EVENTS: &str = "events";
    pub const INPUTS: &str = "inputs";
    pub const OUTPUTS: &str = "outputs";
    pub const VERSION: &str = "version";
    pub const DELETED: &str = "deleted";
}

/// Bump when the column set/types change; stored per segment (segments.schema_version).
pub const SPANS_SCHEMA_VERSION: u32 = 1;
pub const TIMEZONE: &str = "UTC";

/// Merge key: every version of a span shares these. The only columns that may be
/// filtered below MergeVersionsExec (master decision 5).
pub const KEY_COLUMNS: [&str; 3] = [col::PROJECT_ID, col::TRACE_ID, col::SPAN_ID];
/// Large JSON columns: zstd/compact 64-row chunks in Vortex (decision 13); never read by
/// list/filter plans; candidates for blob offload.
pub const PAYLOAD_COLUMNS: [&str; 4] = [col::INPUTS, col::OUTPUTS, col::EVENTS, col::ATTRIBUTES];
/// Columns indexed in the .fts sidecar; the position is the FTS column id (u8) - never reorder.
pub const FTS_COLUMNS: [&str; 5] = [col::NAME, col::ERROR, col::INPUTS, col::OUTPUTS, col::METADATA];
/// Columns holding JSON text.
pub const JSON_COLUMNS: [&str; 5] = [col::METADATA, col::ATTRIBUTES, col::EVENTS, col::INPUTS, col::OUTPUTS];

pub fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMEZONE.into()))
}

/// The `tags` list item: ("item", Utf8, nullable). Builders must use exactly this field.
pub fn tags_item_field() -> FieldRef {
    Arc::new(Field::new_list_field(DataType::Utf8, true))
}

static SPANS: LazyLock<SchemaRef> = LazyLock::new(|| {
    let ts = timestamp_type();
    // Only the merge key + version + deleted are NOT NULL. Every other column is nullable because
    // a partial upsert (start/end post, PATCH /v1/runs) carries only the fields it changes and
    // "latest non-null wins" fills the rest at merge time.
    Arc::new(Schema::new(vec![
        Field::new(col::PROJECT_ID, DataType::Utf8, false),
        Field::new(col::TRACE_ID, DataType::Binary, false), // 16 bytes (decision 1)
        Field::new(col::SPAN_ID, DataType::Binary, false),  // 8 bytes
        Field::new(col::PARENT_SPAN_ID, DataType::Binary, true), // 8 bytes; null = root (after merge)
        Field::new(col::DOTTED_ORDER, DataType::Utf8, true), // null until parent chain known
        Field::new(col::NAME, DataType::Utf8, true),
        Field::new(col::RUN_TYPE, DataType::Utf8, true), // RunType::as_str (decision 2)
        Field::new(col::START_TIME, ts.clone(), true),
        Field::new(col::END_TIME, ts, true),           // null while open
        Field::new(col::STATUS, DataType::Utf8, true), // SpanStatus::as_str
        Field::new(col::ERROR, DataType::Utf8, true),
        Field::new(col::MODEL, DataType::Utf8, true),
        Field::new(col::PROVIDER, DataType::Utf8, true),
        Field::new(col::PROMPT_TOKENS, DataType::Int64, true),
        Field::new(col::COMPLETION_TOKENS, DataType::Int64, true),
        Field::new(col::TOTAL_TOKENS, DataType::Int64, true),
        Field::new(col::COST_USD, DataType::Float64, true),
        Field::new(col::SESSION_ID, DataType::Utf8, true),
        Field::new(col::TAGS, DataType::List(tags_item_field()), true),
        Field::new(col::METADATA, DataType::Utf8, true), // JSON object text
        Field::new(col::ATTRIBUTES, DataType::Utf8, true), // JSON object text
        Field::new(col::EVENTS, DataType::Utf8, true),   // JSON array text
        Field::new(col::INPUTS, DataType::Utf8, true),   // JSON text or blob ref
        Field::new(col::OUTPUTS, DataType::Utf8, true),  // JSON text or blob ref
        Field::new(col::VERSION, DataType::Int64, false),
        Field::new(col::DELETED, DataType::Boolean, false),
    ]))
});

pub fn spans_schema() -> SchemaRef {
    SPANS.clone()
}
/// Index of a column in spans_schema(); panics on unknown names (programming error).
pub fn col_idx(name: &str) -> usize {
    SPANS.index_of(name).expect("unknown spans column")
}
pub fn projected_schema(cols: &[&str]) -> SchemaRef {
    let idx: Vec<usize> = cols.iter().map(|c| col_idx(c)).collect();
    Arc::new(SPANS.project(&idx).expect("valid projection"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORDER: [&str; 26] = [
        col::PROJECT_ID,
        col::TRACE_ID,
        col::SPAN_ID,
        col::PARENT_SPAN_ID,
        col::DOTTED_ORDER,
        col::NAME,
        col::RUN_TYPE,
        col::START_TIME,
        col::END_TIME,
        col::STATUS,
        col::ERROR,
        col::MODEL,
        col::PROVIDER,
        col::PROMPT_TOKENS,
        col::COMPLETION_TOKENS,
        col::TOTAL_TOKENS,
        col::COST_USD,
        col::SESSION_ID,
        col::TAGS,
        col::METADATA,
        col::ATTRIBUTES,
        col::EVENTS,
        col::INPUTS,
        col::OUTPUTS,
        col::VERSION,
        col::DELETED,
    ];

    #[test]
    fn shape() {
        let s = spans_schema();
        let names: Vec<_> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ORDER);
        let not_null: Vec<_> = s
            .fields()
            .iter()
            .filter(|f| !f.is_nullable())
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            not_null,
            [col::PROJECT_ID, col::TRACE_ID, col::SPAN_ID, col::VERSION, col::DELETED]
        );
        for c in [col::TRACE_ID, col::SPAN_ID, col::PARENT_SPAN_ID] {
            assert_eq!(s.field_with_name(c).unwrap().data_type(), &DataType::Binary);
        }
        for c in [col::START_TIME, col::END_TIME] {
            assert_eq!(s.field_with_name(c).unwrap().data_type(), &timestamp_type());
        }
        assert_eq!(
            s.field_with_name(col::TAGS).unwrap().data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );
    }

    #[test]
    fn column_lists_resolve() {
        for c in KEY_COLUMNS
            .iter()
            .chain(&PAYLOAD_COLUMNS)
            .chain(&FTS_COLUMNS)
            .chain(&JSON_COLUMNS)
        {
            col_idx(c);
        }
        let p = projected_schema(&[col::SPAN_ID, col::TAGS]);
        assert_eq!(p.fields().len(), 2);
        assert_eq!(p.field(0).name(), col::SPAN_ID);
    }
}
