//! Version merge + physical sort (paste target: forge-model/src/merge.rs; pure Arrow, no I/O).
//!
//! `merge_versions`: any number of spans_schema batches, any order, any number of versions per
//! `(project_id, trace_id, span_id)` -> ONE row per key where every column is "latest non-null
//! wins" by `version` (`version` and `deleted` come from the newest version), sorted by the physical
//! key `(project_id, trace_id, dotted_order NULLS LAST, span_id)`. That is exactly what
//! `SegmentStore::write_segment` requires (invariant 3).
//!
//! No `concat_batches`: rows are addressed as `(batch, row)` and gathered with `interleave`, so the
//! input is never copied twice and no intermediate Utf8 column can overflow i32 offsets.
use crate::error::Result;
use crate::ids::ProjectId;
use crate::row::validate_batch;
use crate::schema::{col, col_idx, spans_schema};
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch, UInt32Array};
use arrow::compute::{SortOptions, filter_record_batch, interleave, take_record_batch};
use arrow::datatypes::DataType;
use arrow::row::{RowConverter, SortField};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tombstones {
    /// Query time: drop keys whose newest version has deleted=true.
    Drop,
    /// Flush, and compaction jobs that do not hold every visible segment of the affected traces.
    Keep,
}

const ASC_NULLS_LAST: SortOptions = SortOptions {
    descending: false,
    nulls_first: false,
};

pub fn merge_versions(batches: &[RecordBatch], tombstones: Tombstones) -> Result<RecordBatch> {
    let schema = spans_schema();
    let batches: Vec<&RecordBatch> = batches.iter().filter(|b| b.num_rows() > 0).collect();
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    for b in &batches {
        validate_batch(b)?;
    }
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    let (p, t, s, v) = (
        col_idx(col::PROJECT_ID),
        col_idx(col::TRACE_ID),
        col_idx(col::SPAN_ID),
        col_idx(col::VERSION),
    );

    // 1. Merge order: (project, trace, span, version DESC). One RowConverter, rows appended per batch.
    let conv = RowConverter::new(vec![
        SortField::new(DataType::Utf8),
        SortField::new(DataType::Binary),
        SortField::new(DataType::Binary),
        SortField::new_with_options(
            DataType::Int64,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
    ])?;
    let mut rows = conv.empty_rows(total, total * 64);
    let mut loc: Vec<(usize, usize)> = Vec::with_capacity(total);
    for (bi, b) in batches.iter().enumerate() {
        let cols: Vec<ArrayRef> = [p, t, s, v].iter().map(|&i| b.column(i).clone()).collect();
        conv.append(&mut rows, &cols)?;
        loc.extend((0..b.num_rows()).map(|r| (bi, r)));
    }
    let mut order: Vec<u32> = (0..total as u32).collect();
    order.sort_unstable_by(|&a, &b| rows.row(a as usize).cmp(&rows.row(b as usize)));
    drop(rows);

    // 2. Groups = runs of equal (project, trace, span); compared on the typed arrays.
    let keys: Vec<_> = batches
        .iter()
        .map(|b| {
            (
                b.column(p).as_string::<i32>(),
                b.column(t).as_binary::<i32>(),
                b.column(s).as_binary::<i32>(),
            )
        })
        .collect();
    let same_key = |(ba, ra): (usize, usize), (bb, rb): (usize, usize)| {
        let (ka, kb) = (&keys[ba], &keys[bb]);
        ka.2.value(ra) == kb.2.value(rb) && ka.1.value(ra) == kb.1.value(rb) && ka.0.value(ra) == kb.0.value(rb)
    };
    let at = |pos: usize| loc[order[pos] as usize];
    let mut starts: Vec<usize> = Vec::with_capacity(total);
    for pos in 0..total {
        if pos == 0 || !same_key(at(pos - 1), at(pos)) {
            starts.push(pos);
        }
    }
    let groups: Vec<std::ops::Range<usize>> = starts
        .iter()
        .zip(starts.iter().skip(1).copied().chain([total]))
        .map(|(&a, b)| a..b)
        .collect();

    // 3. Per column: for each group, the newest row where the column is non-null (else the newest).
    //    NOT NULL columns (key, version, deleted) always take the newest row.
    let newest: Vec<(usize, usize)> = groups.iter().map(|g| at(g.start)).collect();
    let mut out: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (c, field) in schema.fields().iter().enumerate() {
        let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(c).as_ref()).collect();
        let no_nulls = !field.is_nullable() || arrays.iter().all(|a| a.null_count() == 0);
        let all_singletons = groups.len() == total;
        let idx: Vec<(usize, usize)> = if no_nulls || all_singletons {
            newest.clone()
        } else {
            groups
                .iter()
                .map(|g| {
                    g.clone()
                        .map(at)
                        .find(|&(b, r)| arrays[b].is_valid(r))
                        .unwrap_or_else(|| at(g.start))
                })
                .collect()
        };
        out.push(interleave(&arrays, &idx)?);
    }
    let mut merged = RecordBatch::try_new(schema.clone(), out)?;

    if tombstones == Tombstones::Drop {
        let deleted = merged.column(col_idx(col::DELETED)).as_boolean();
        if deleted.true_count() > 0 {
            let keep: BooleanArray = deleted.iter().map(|d| Some(d != Some(true))).collect();
            merged = filter_record_batch(&merged, &keep)?;
        }
    }
    sort_physical(&merged)
}

/// Sorts by `(project_id, trace_id, dotted_order NULLS LAST, span_id)`, byte-wise (matches the
/// check in forge-storage's `SegmentStats::push`).
pub fn sort_physical(b: &RecordBatch) -> Result<RecordBatch> {
    if b.num_rows() <= 1 {
        return Ok(b.clone());
    }
    let conv = RowConverter::new(vec![
        SortField::new_with_options(DataType::Utf8, ASC_NULLS_LAST),
        SortField::new_with_options(DataType::Binary, ASC_NULLS_LAST),
        SortField::new_with_options(DataType::Utf8, ASC_NULLS_LAST),
        SortField::new_with_options(DataType::Binary, ASC_NULLS_LAST),
    ])?;
    let cols: Vec<ArrayRef> = [col::PROJECT_ID, col::TRACE_ID, col::DOTTED_ORDER, col::SPAN_ID]
        .iter()
        .map(|c| b.column(col_idx(c)).clone())
        .collect();
    let rows = conv.convert_columns(&cols)?;
    let mut idx: Vec<u32> = (0..b.num_rows() as u32).collect();
    idx.sort_unstable_by(|&x, &y| rows.row(x as usize).cmp(&rows.row(y as usize)));
    Ok(take_record_batch(b, &UInt32Array::from(idx))?)
}

/// Zero-copy slices of a project-sorted batch, one per project (one L0 segment each).
pub fn split_by_project(b: &RecordBatch) -> Result<Vec<(ProjectId, RecordBatch)>> {
    let p = b.column(col_idx(col::PROJECT_ID)).as_string::<i32>();
    let mut out = Vec::new();
    let mut start = 0;
    for i in 1..=b.num_rows() {
        if i == b.num_rows() || p.value(i) != p.value(start) {
            out.push((ProjectId::new(p.value(start))?, b.slice(start, i - start)));
            start = i;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::{SpanBatchBuilder, SpanRow, rows_from_batch};
    use crate::testing::{Rng, random_row, reference_merge};
    use proptest::prelude::*;

    fn batch(rows: &[SpanRow]) -> RecordBatch {
        let mut b = SpanBatchBuilder::with_capacity(rows.len());
        for r in rows {
            b.push(r).unwrap();
        }
        b.finish().unwrap()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn merge_matches_reference(seed in 1u64.., n in 1usize..300, chunk in 1usize..40) {
            let mut r = Rng(seed);
            let mut rows: Vec<SpanRow> = (0..n as u64)
                .map(|v| {
                    let p = ["a", "b", "c"][r.below(3) as usize];
                    let (t, s) = (r.below(5), r.below(8));
                    random_row(&mut r, p, t, s, v + 1)
                })
                .collect();
            if n > 2 { let d = rows[0].clone(); rows.push(d); }
            let batches: Vec<_> = rows.chunks(chunk).map(batch).collect();
            let m = merge_versions(&batches, Tombstones::Keep).unwrap();
            prop_assert_eq!(rows_from_batch(&m).unwrap(), reference_merge(&rows));
            let again = merge_versions(std::slice::from_ref(&m), Tombstones::Keep).unwrap();
            prop_assert_eq!(rows_from_batch(&again).unwrap(), rows_from_batch(&m).unwrap());
            for (_, part) in split_by_project(&m).unwrap() { prop_assert!(part.num_rows() > 0); }
        }
    }
}
