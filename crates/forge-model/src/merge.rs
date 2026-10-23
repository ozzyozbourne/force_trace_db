//! Version merge + physical sort (pure Arrow, no I/O).
//!
//! `merge_versions`: any number of spans_schema batches, any order, any number of versions per
//! `(project_id, trace_id, span_id)` -> ONE row per key where every column is "latest non-null
//! wins" by `version` (`version` and `deleted` come from the newest version), sorted by the physical
//! key `(project_id, trace_id, dotted_order NULLS LAST, span_id)`. That is exactly what
//! `SegmentStore::write_segment` requires (invariant 3).
//!
//! Semantics (M2/M3 rely on them):
//! - Per field, the latest non-null value wins by `version`. `version` and `deleted` come from the newest row.
//!   A null `tags` means "not set"; an empty list is a value.
//! - **Associative only for version-disjoint parts** (every version of A older than every version of B):
//!   `merge([merge(A), merge(B)]) == merge(A ∪ B)`. Flushes are version-disjoint and ordered because versions are
//!   assigned in WAL order (invariant 9), so segments + memtable merge exactly; compaction must merge a key's
//!   segments as a contiguous run in flush order.
//! - **Equal versions** (L6): an equal-version row is either an identical replay copy, or the merged superset of
//!   rows that are also present (memtable raw row + segment row during M2's snapshot window). Per-field selection
//!   handles both. Never dedupe whole rows by `(key, version)`.
//! - **Delete is not a barrier** (L2): a re-post after a tombstone is live again, and its null fields are filled
//!   from versions older than the tombstone. Tombstones first appear in M6; revisit then.
//! - Cost is per post: a post computes `cost_usd` only from its own model and tokens. M3/M6 recompute it after the
//!   merge.
//! - **M4** (L8): the FTS builder must skip values that are blob refs (`blob::parse_blob_ref`). Blob-backed
//!   payloads are not searchable in v1.
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
    use crate::row::{SpanBatchBuilder, SpanRow};
    use crate::testing::{Rng, random_row, reference_merge, rows_from_batch};
    use proptest::prelude::*;

    fn batch(rows: &[SpanRow]) -> RecordBatch {
        let mut b = SpanBatchBuilder::with_capacity(rows.len());
        for r in rows {
            b.push(r).unwrap();
        }
        b.finish().unwrap()
    }

    fn row(project: &str, span: u64, version: u64) -> SpanRow {
        SpanRow {
            project_id: Some(crate::testing::pid(project)),
            trace_id: crate::testing::tid(1),
            span_id: crate::testing::sid(span),
            version,
            ..Default::default()
        }
    }

    fn merge(rows: &[SpanRow], t: Tombstones) -> Vec<SpanRow> {
        rows_from_batch(&merge_versions(&[batch(rows)], t).unwrap()).unwrap()
    }

    #[test]
    fn start_end_patch_merge_per_field() {
        let start = SpanRow {
            name: Some("n".into()),
            start_time_ns: Some(1),
            inputs: Some("{}".into()),
            tags: Some(vec!["a".into()]),
            ..row("p", 1, 1)
        };
        let end = SpanRow {
            end_time_ns: Some(2),
            outputs: Some("[1]".into()),
            tags: Some(vec![]), // an empty list is a value
            ..row("p", 1, 2)
        };
        let patch = SpanRow {
            name: Some("renamed".into()),
            ..row("p", 1, 3)
        };
        // Input order does not matter, only versions do.
        let m = merge(&[patch, start, end], Tombstones::Keep);
        assert_eq!(m.len(), 1);
        let r = &m[0];
        assert_eq!(r.name.as_deref(), Some("renamed"));
        assert_eq!((r.start_time_ns, r.end_time_ns), (Some(1), Some(2)));
        assert_eq!((r.inputs.as_deref(), r.outputs.as_deref()), (Some("{}"), Some("[1]")));
        assert_eq!(r.tags, Some(vec![]));
        assert_eq!(r.version, 3);
    }

    #[test]
    fn tombstone_keep_vs_drop() {
        let rows = [
            SpanRow {
                name: Some("x".into()),
                ..row("p", 1, 1)
            },
            SpanRow {
                deleted: true,
                ..row("p", 1, 2)
            },
            row("p", 2, 3),
        ];
        let keep = merge(&rows, Tombstones::Keep);
        assert_eq!(keep.len(), 2);
        assert!(keep.iter().any(|r| r.deleted && r.name.as_deref() == Some("x")));
        let drop = merge(&rows, Tombstones::Drop);
        assert_eq!(drop.len(), 1);
        assert_eq!(drop[0].span_id, crate::testing::sid(2));
    }

    #[test]
    fn delete_then_repost_is_live() {
        // L2: delete is not a barrier. The re-post is live and its nulls are filled from before the tombstone.
        let rows = [
            SpanRow {
                name: Some("v1".into()),
                inputs: Some("{}".into()),
                ..row("p", 1, 1)
            },
            SpanRow {
                deleted: true,
                ..row("p", 1, 2)
            },
            SpanRow {
                end_time_ns: Some(9),
                ..row("p", 1, 3)
            },
        ];
        let m = merge(&rows, Tombstones::Drop);
        assert_eq!(m.len(), 1);
        assert!(!m[0].deleted);
        assert_eq!((m[0].name.as_deref(), m[0].end_time_ns), (Some("v1"), Some(9)));
    }

    /// G1: up to 300 rows over 3 projects x 5 traces x 8 spans, random nulls, ~5 % tombstones, ~2 % duplicates.
    fn g1(seed: u64, n: usize) -> Vec<SpanRow> {
        let mut r = Rng(seed);
        let mut rows: Vec<SpanRow> = Vec::new();
        for v in 0..n as u64 {
            if !rows.is_empty() && r.chance(2) {
                let d = rows[r.below(rows.len() as u64) as usize].clone();
                rows.push(d);
                continue;
            }
            let p = ["a", "b", "c"][r.below(3) as usize];
            let (t, s) = (r.below(5), r.below(8));
            rows.push(random_row(&mut r, p, t, s, v + 1));
        }
        rows
    }

    /// G2: few keys (often one), 2 projects sharing trace/span numbers, 25 % tombstones (delete -> re-post),
    /// 10 % `tags = Some([])`, 5 % duplicates.
    fn g2(seed: u64, n: usize) -> Vec<SpanRow> {
        let mut r = Rng(seed);
        let (traces, spans) = if r.chance(40) {
            (1, 1)
        } else {
            (1 + r.below(2), 1 + r.below(3))
        };
        let mut rows: Vec<SpanRow> = Vec::new();
        for v in 0..n as u64 {
            if !rows.is_empty() && r.chance(5) {
                let d = rows[r.below(rows.len() as u64) as usize].clone();
                rows.push(d);
                continue;
            }
            let p = ["a", "b"][r.below(2) as usize];
            let (t, s) = (r.below(traces), r.below(spans));
            let mut x = random_row(&mut r, p, t, s, v + 1);
            x.deleted = r.chance(25);
            if r.chance(10) {
                x.tags = Some(vec![]);
            }
            rows.push(x);
        }
        rows
    }

    fn generate(g: u8, seed: u64, n: usize) -> Vec<SpanRow> {
        if g == 1 { g1(seed, n) } else { g2(seed, n) }
    }

    fn chunked(rows: &[SpanRow], chunk: usize) -> Vec<RecordBatch> {
        rows.chunks(chunk).map(batch).collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn merge_keep_matches_reference(g in 1u8..=2, seed in 1u64.., n in 1usize..300, chunk in 1usize..40) {
            let rows = generate(g, seed, n);
            let m = merge_versions(&chunked(&rows, chunk), Tombstones::Keep).unwrap();
            prop_assert_eq!(rows_from_batch(&m).unwrap(), reference_merge(&rows));
        }

        #[test]
        fn merge_drop_matches_reference(g in 1u8..=2, seed in 1u64.., n in 1usize..300, chunk in 1usize..40) {
            let rows = generate(g, seed, n);
            let m = merge_versions(&chunked(&rows, chunk), Tombstones::Drop).unwrap();
            let want: Vec<_> = reference_merge(&rows).into_iter().filter(|r| !r.deleted).collect();
            prop_assert_eq!(rows_from_batch(&m).unwrap(), want);
        }

        #[test]
        fn merge_idempotent(g in 1u8..=2, seed in 1u64.., n in 1usize..300, chunk in 1usize..40) {
            let rows = generate(g, seed, n);
            let m = merge_versions(&chunked(&rows, chunk), Tombstones::Keep).unwrap();
            let again = merge_versions(std::slice::from_ref(&m), Tombstones::Keep).unwrap();
            prop_assert_eq!(rows_from_batch(&again).unwrap(), rows_from_batch(&m).unwrap());
        }

        #[test]
        fn merge_associative_for_version_disjoint_splits(
            g in 1u8..=2, seed in 1u64.., n in 1usize..300, chunk in 1usize..40, cut in 0.0f64..=1.0,
        ) {
            let mut rows = generate(g, seed, n);
            rows.sort_by_key(|r| r.version);
            let (a, b) = rows.split_at((rows.len() as f64 * cut) as usize);
            let mut parts = Vec::new();
            for part in [a, b].into_iter().filter(|p| !p.is_empty()) {
                parts.push(merge_versions(&chunked(part, chunk), Tombstones::Keep).unwrap());
            }
            let two_step = merge_versions(&parts, Tombstones::Keep).unwrap();
            let one_step = merge_versions(&chunked(&rows, chunk), Tombstones::Keep).unwrap();
            prop_assert_eq!(rows_from_batch(&two_step).unwrap(), rows_from_batch(&one_step).unwrap());
        }

        #[test]
        fn merge_output_sorted_one_row_per_key(g in 1u8..=2, seed in 1u64.., n in 1usize..300, chunk in 1usize..40) {
            let rows = generate(g, seed, n);
            let m = merge_versions(&chunked(&rows, chunk), Tombstones::Keep).unwrap();
            let out = rows_from_batch(&m).unwrap();
            let k = |r: &SpanRow| (
                r.project_id.as_ref().unwrap().to_string(),
                r.trace_id,
                r.dotted_order.is_none(),
                r.dotted_order.clone().unwrap_or_default(),
                r.span_id,
            );
            prop_assert!(out.windows(2).all(|w| k(&w[0]) < k(&w[1])), "strictly sorted");
            let keys: std::collections::HashSet<_> =
                out.iter().map(|r| (r.project_id.clone(), r.trace_id, r.span_id)).collect();
            prop_assert_eq!(keys.len(), out.len());
            for (_, part) in split_by_project(&m).unwrap() { prop_assert!(part.num_rows() > 0); }
        }
    }
}
