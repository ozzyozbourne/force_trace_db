#![allow(dead_code, unused_imports)]
use forge_model::row::SpanRow;
pub use forge_model::testing::rows_from_batch as rfb;
pub use forge_model::testing::{Rng, batch_from_rows, pid, random_row, reference_merge, sid, tid};
pub fn rows_from_batch(b: &arrow::array::RecordBatch) -> Vec<SpanRow> {
    rfb(b).unwrap()
}

/// A JSON text column as a value (Null when absent).
pub fn j(x: &Option<String>) -> serde_json::Value {
    x.as_deref()
        .map_or(serde_json::Value::Null, |s| serde_json::from_str(s).unwrap())
}
