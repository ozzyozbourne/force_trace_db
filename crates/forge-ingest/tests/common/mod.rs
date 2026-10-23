#![allow(dead_code, unused_imports)]
pub use forge_model::row::rows_from_batch as rfb;
use forge_model::row::{SpanBatchBuilder, SpanRow};
pub use forge_model::testing::{Rng, pid, random_row, reference_merge, sid, tid};

pub fn batch(rows: &[SpanRow]) -> arrow::array::RecordBatch {
    let mut b = SpanBatchBuilder::with_capacity(rows.len());
    for r in rows {
        b.push(r).unwrap();
    }
    b.finish().unwrap()
}
pub fn rows_from_batch(b: &arrow::array::RecordBatch) -> Vec<SpanRow> {
    rfb(b).unwrap()
}
