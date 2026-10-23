//! Derived columns computed after the merge (master decision 15).
use crate::error::Result;
use crate::schema::col;
use arrow::array::{AsArray, Int64Array, RecordBatch};
use arrow::datatypes::Int64Type;
use std::sync::Arc;

/// Effective total: producer total, else prompt + completion if both, else whichever count is present, else null.
pub fn effective_total_tokens(t: &Int64Array, p: &Int64Array, c: &Int64Array) -> Int64Array {
    t.iter()
        .zip(p.iter())
        .zip(c.iter())
        .map(|((t, p), c)| {
            t.or(match (p, c) {
                (Some(a), Some(b)) => Some(a.wrapping_add(b)),
                (a, b) => a.or(b),
            })
        })
        .collect()
}

/// Replaces the total_tokens column (if projected) by the effective one; needs both counts projected.
pub fn with_effective_total_tokens(b: &RecordBatch) -> Result<RecordBatch> {
    let s = b.schema();
    let Ok(ti) = s.index_of(col::TOTAL_TOKENS) else { return Ok(b.clone()) };
    let get = |n: &str| -> Result<&Int64Array> { Ok(b.column(s.index_of(n)?).as_primitive::<Int64Type>()) };
    let e = effective_total_tokens(get(col::TOTAL_TOKENS)?, get(col::PROMPT_TOKENS)?, get(col::COMPLETION_TOKENS)?);
    let mut cols = b.columns().to_vec();
    cols[ti] = Arc::new(e);
    Ok(RecordBatch::try_new(s, cols)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    #[test]
    fn truth_table() {
        let v = [None, Some(10i64)];
        for t in v {
            for p in [None, Some(1i64)] {
                for c in [None, Some(2i64)] {
                    let got = effective_total_tokens(&Int64Array::from(vec![t]), &Int64Array::from(vec![p]), &Int64Array::from(vec![c]));
                    let want = t.or(match (p, c) { (Some(a), Some(b)) => Some(a + b), (a, b) => a.or(b) });
                    assert_eq!(got.iter().next().unwrap(), want);
                    let _ = got.null_count();
                }
            }
        }
    }
}
