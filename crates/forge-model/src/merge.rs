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
//! - `total_tokens` holds only a producer-sent total, never a derived one (master decision 15), so the per-field
//!   rule stays pure. Readers compute the effective total AFTER the merge with the 4-branch rule: the producer
//!   total if set, else prompt + completion if both are set, else whichever count is set, else null
//!   (`derived::effective_total_tokens`; not a literal `COALESCE(t, p + c)`). Known limit: a stored producer total
//!   from an older post is not updated when a later post changes only a count.
//! - **M4** (L8): the FTS builder must skip values that are blob refs (`blob::parse_blob_ref`). Blob-backed
//!   payloads are not searchable in v1.
//!
//! No `concat_batches`: rows are addressed as `(batch, row)` and gathered with `interleave`, so the
//! input is never copied twice. The output can still exceed i32 Utf8/Binary offsets (2 GiB per column);
//! `merge_versions_chunked` bounds every output batch (the flush uses it).
use crate::error::{ModelError, Result};
use crate::ids::ProjectId;
use crate::row::{validate_projected, validate_projection};
use crate::schema::{col, col_idx, spans_schema};
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, UInt32Array};
use arrow::compute::{SortOptions, interleave, take_record_batch};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::row::{RowConverter, SortField};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Always present in a merge input/output: the key, version, deleted.
pub const MERGE_COLUMNS: [&str; 5] = [col::PROJECT_ID, col::TRACE_ID, col::SPAN_ID, col::VERSION, col::DELETED];

/// Cooperative cancellation for CPU work on the blocking pool (D8). No `Default`: pick `never()` or `new()`
/// explicitly (a `never()` inside an exec would silently disable cancellation).
#[derive(Clone, Debug)]
pub struct Cancel(Option<Arc<CancelState>>);

#[derive(Debug)]
pub struct CancelState {
    cancelled: AtomicBool,
    checks: AtomicU64,
    /// `check()` fails from this call on (test-util `after`); u64::MAX otherwise.
    fail_from: u64,
}

impl Cancel {
    /// `check()` is always Ok; no allocation.
    pub fn never() -> Self {
        Self(None)
    }
    #[allow(clippy::new_without_default)] // deliberate: see the type doc
    pub fn new() -> Self {
        Self::failing_from(u64::MAX)
    }
    fn failing_from(fail_from: u64) -> Self {
        Self(Some(Arc::new(CancelState {
            cancelled: AtomicBool::new(false),
            checks: AtomicU64::new(0),
            fail_from,
        })))
    }
    pub fn cancel(&self) {
        if let Some(s) = &self.0 {
            s.cancelled.store(true, Ordering::Relaxed)
        }
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.as_ref().is_some_and(|s| s.cancelled.load(Ordering::Relaxed))
    }
    /// `Err(Cancelled)` once cancelled.
    pub fn check(&self) -> Result<()> {
        let Some(s) = &self.0 else { return Ok(()) };
        let n = s.checks.fetch_add(1, Ordering::Relaxed);
        if n >= s.fail_from || s.cancelled.load(Ordering::Relaxed) {
            return Err(ModelError::Cancelled);
        }
        Ok(())
    }
    /// `check()` fails from its k-th call on (k = 0: the first call fails). Pins every checkpoint.
    #[cfg(any(test, feature = "test-util"))]
    pub fn after(k: u64) -> Self {
        Self::failing_from(k)
    }
    /// Number of `check()` calls so far.
    #[cfg(any(test, feature = "test-util"))]
    pub fn checks(&self) -> u64 {
        self.0.as_ref().map_or(0, |s| s.checks.load(Ordering::Relaxed))
    }
}
/// Sets the flag when dropped: hold it in the future/stream that a timeout drops.
#[derive(Debug)]
pub struct CancelOnDrop(pub Cancel);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel()
    }
}

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

/// `merge_versions_chunked` as ONE batch: for bounded inputs (tests, queries over small ranges). A column whose
/// merged values exceed 2 GiB fails with an offset overflow; the flush uses the chunked form.
pub fn merge_versions(batches: &[RecordBatch], tombstones: Tombstones) -> Result<RecordBatch> {
    Ok(merge_versions_chunked(batches, tombstones, usize::MAX)?
        .pop()
        .unwrap_or_else(|| RecordBatch::new_empty(spans_schema())))
}

/// The merged rows in physical order, as consecutive batches. A batch ends before the variable-length bytes of
/// its source rows (every version of every key in it, all columns) would pass `max_chunk_bytes`, so each column of
/// each batch holds at most that many bytes, unless one key alone is larger. Empty input or all keys dropped:
/// no batches.
pub fn merge_versions_chunked(
    batches: &[RecordBatch],
    tombstones: Tombstones,
    max_chunk_bytes: usize,
) -> Result<Vec<RecordBatch>> {
    merge_versions_projected(&spans_schema(), batches, tombstones, max_chunk_bytes, &Cancel::never())
}

/// One batch (possibly empty) of `schema`; queries over bounded inputs.
pub fn merge_projected(schema: &SchemaRef, b: &[RecordBatch], t: Tombstones, cancel: &Cancel) -> Result<RecordBatch> {
    Ok(merge_versions_projected(schema, b, t, usize::MAX, cancel)?
        .pop()
        .unwrap_or_else(|| RecordBatch::new_empty(schema.clone())))
}

/// Per-field "latest non-null wins" over ANY projection of spans_schema that contains MERGE_COLUMNS.
pub fn merge_versions_projected(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    tombstones: Tombstones,
    max_chunk_bytes: usize,
    cancel: &Cancel,
) -> Result<Vec<RecordBatch>> {
    validate_projection(schema)?;
    let batches: Vec<&RecordBatch> = batches.iter().filter(|b| b.num_rows() > 0).collect();
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    for b in &batches {
        validate_projected(schema, b)?;
    }
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    let (p, t, s, v) = (
        schema.index_of(col::PROJECT_ID)?,
        schema.index_of(col::TRACE_ID)?,
        schema.index_of(col::SPAN_ID)?,
        schema.index_of(col::VERSION)?,
    );
    let del = schema.index_of(col::DELETED)?;

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
    cancel.check()?;

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
    let mut groups: Vec<std::ops::Range<usize>> = Vec::new();
    let mut start = 0;
    for pos in 1..=total {
        if pos == total || !same_key(at(pos - 1), at(pos)) {
            groups.push(start..pos);
            start = pos;
        }
    }
    cancel.check()?;

    // 3. Per column: for each group, the newest row where the column is non-null (else the newest).
    //    NOT NULL columns (key, version, deleted) always take the newest row.
    let newest: Vec<(usize, usize)> = groups.iter().map(|g| at(g.start)).collect();
    let all_singletons = groups.len() == total;
    let arrays: Vec<Vec<&dyn Array>> = (0..schema.fields().len())
        .map(|c| batches.iter().map(|b| b.column(c).as_ref()).collect())
        .collect();
    let newest_only: Vec<bool> = arrays
        .iter()
        .enumerate()
        .map(|(c, a)| all_singletons || !schema.field(c).is_nullable() || a.iter().all(|a| a.null_count() == 0))
        .collect();
    // The source row of column `c` for group `g`.
    let pick = |c: usize, g: usize| -> (usize, usize) {
        if newest_only[c] {
            return newest[g];
        }
        groups[g]
            .clone()
            .map(at)
            .find(|&(b, r)| arrays[c][b].is_valid(r))
            .unwrap_or(newest[g])
    };

    // 4. The groups to emit: Drop skips keys whose newest row is a tombstone.
    let mut keep: Vec<usize> = (0..groups.len()).collect();
    if tombstones == Tombstones::Drop {
        let deleted: Vec<_> = batches.iter().map(|b| b.column(del).as_boolean()).collect();
        keep.retain(|&g| !deleted[newest[g].0].value(newest[g].1));
    }

    // 5. Physical order of the kept groups, from their merged sort-key columns only (gathered in bounded chunks).
    let weights: Vec<Vec<usize>> = batches.iter().map(|b| row_weights(b)).collect();
    let group_weight = |g: usize| -> usize { groups[g].clone().map(at).map(|(b, r)| weights[b][r]).sum() };
    let chunks = |order: &[usize]| -> Vec<std::ops::Range<usize>> {
        let mut out = Vec::new();
        let (mut start, mut acc) = (0, 0usize);
        for (i, &g) in order.iter().enumerate() {
            let w = group_weight(g);
            if i > start && acc.saturating_add(w) > max_chunk_bytes {
                out.push(start..i);
                (start, acc) = (i, 0);
            }
            acc += w;
        }
        if start < order.len() {
            out.push(start..order.len());
        }
        out
    };
    let gather = |c: usize, order: &[usize]| -> Result<ArrayRef> {
        let idx: Vec<(usize, usize)> = order.iter().map(|&g| pick(c, g)).collect();
        Ok(interleave(&arrays[c], &idx)?)
    };
    let sort_idx: Vec<usize> = SORT_COLUMNS.iter().filter_map(|c| schema.index_of(c).ok()).collect();
    let sort_chunks = chunks(&keep)
        .into_iter()
        .map(|r| sort_idx.iter().map(|&c| gather(c, &keep[r.clone()])).collect())
        .collect::<Result<Vec<Vec<ArrayRef>>>>()?;
    let order: Vec<usize> = physical_order(&sort_chunks)?
        .into_iter()
        .map(|i| keep[i as usize])
        .collect();
    cancel.check()?;

    // 6. One gather per column and chunk, in physical order.
    chunks(&order)
        .into_iter()
        .map(|r| {
            cancel.check()?;
            let out = (0..schema.fields().len())
                .map(|c| gather(c, &order[r.clone()]))
                .collect::<Result<Vec<_>>>()?;
            Ok(RecordBatch::try_new(schema.clone(), out)?)
        })
        .collect()
}

/// Per row: the bytes of its variable-length values over all columns (Utf8/Binary values, list items' values).
/// An upper bound on what the row contributes to any one merged column.
fn row_weights(b: &RecordBatch) -> Vec<usize> {
    let mut w = vec![0usize; b.num_rows()];
    let mut add = |offsets: &[i32]| {
        for (r, o) in offsets.windows(2).enumerate() {
            w[r] += (o[1] - o[0]) as usize;
        }
    };
    for c in b.columns() {
        match c.data_type() {
            DataType::Utf8 => add(c.as_string::<i32>().value_offsets()),
            DataType::Binary => add(c.as_binary::<i32>().value_offsets()),
            DataType::List(_) => {
                let l = c.as_list::<i32>();
                let v = l.values().as_string::<i32>().value_offsets();
                let rows: Vec<i32> = l.value_offsets().iter().map(|&i| v[i as usize]).collect();
                add(&rows);
            }
            _ => {}
        }
    }
    w
}

const SORT_COLUMNS: [&str; 4] = [col::PROJECT_ID, col::TRACE_ID, col::DOTTED_ORDER, col::SPAN_ID];

/// The row order of `SORT_COLUMNS` arrays, given as consecutive chunks: `(project_id, trace_id, dotted_order NULLS
/// LAST, span_id)`, byte-wise. Indexes count rows across the chunks.
fn physical_order(chunks: &[Vec<ArrayRef>]) -> Result<Vec<u32>> {
    let n: usize = chunks.iter().map(|c| c[0].len()).sum();
    let mut idx: Vec<u32> = (0..n as u32).collect();
    if n <= 1 {
        return Ok(idx);
    }
    let conv = RowConverter::new(
        chunks[0]
            .iter()
            .map(|a| SortField::new_with_options(a.data_type().clone(), ASC_NULLS_LAST))
            .collect(),
    )?;
    let mut rows = conv.empty_rows(n, 0);
    for c in chunks {
        conv.append(&mut rows, c)?;
    }
    idx.sort_unstable_by(|&x, &y| rows.row(x as usize).cmp(&rows.row(y as usize)));
    Ok(idx)
}

/// Sorts by `(project_id, trace_id, dotted_order NULLS LAST, span_id)`, byte-wise (matches the
/// check in forge-storage's `SegmentStats::push`).
pub fn sort_physical(b: &RecordBatch) -> Result<RecordBatch> {
    if b.num_rows() <= 1 {
        return Ok(b.clone());
    }
    let cols: Vec<ArrayRef> = SORT_COLUMNS.iter().map(|c| b.column(col_idx(c)).clone()).collect();
    let idx = physical_order(&[cols])?;
    Ok(take_record_batch(b, &UInt32Array::from(idx))?)
}

/// `split_by_project` over consecutive project-sorted batches (`merge_versions_chunked`): per project, its slices
/// of every batch, in order. A project can span batches.
pub fn split_chunks_by_project(chunks: &[RecordBatch]) -> Result<Vec<(ProjectId, Vec<RecordBatch>)>> {
    let mut out: Vec<(ProjectId, Vec<RecordBatch>)> = Vec::new();
    for c in chunks {
        for (p, slice) in split_by_project(c)? {
            match out.last_mut() {
                Some((last, v)) if *last == p => v.push(slice),
                _ => out.push((p, vec![slice])),
            }
        }
    }
    Ok(out)
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
    use crate::row::SpanRow;
    use crate::testing::{Rng, batch_from_rows, physical_key, random_row, reference_merge, rows_from_batch};
    use proptest::prelude::*;

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
        rows_from_batch(&merge_versions(&[batch_from_rows(rows)], t).unwrap()).unwrap()
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
    /// With `pct`% chance, pushes a copy of a random earlier row.
    fn maybe_dup(r: &mut Rng, rows: &mut Vec<SpanRow>, pct: u64) -> bool {
        if rows.is_empty() || !r.chance(pct) {
            return false;
        }
        let d = rows[r.below(rows.len() as u64) as usize].clone();
        rows.push(d);
        true
    }

    fn g1(seed: u64, n: usize) -> Vec<SpanRow> {
        let mut r = Rng(seed);
        let mut rows: Vec<SpanRow> = Vec::new();
        for v in 0..n as u64 {
            if maybe_dup(&mut r, &mut rows, 2) {
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
            if maybe_dup(&mut r, &mut rows, 5) {
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
        rows.chunks(chunk).map(batch_from_rows).collect()
    }

    /// Small `max_chunk_bytes`: several batches, each within the bound (unless one key alone exceeds it), and
    /// together exactly the one-batch merge; the per-project split across batch boundaries matches too.
    #[test]
    fn chunked_merge_bounds_every_batch() {
        for seed in 1..=8u64 {
            let rows = g1(seed, 400);
            for t in [Tombstones::Keep, Tombstones::Drop] {
                let input = chunked(&rows, 1 + seed as usize * 5);
                let one = merge_versions(&input, t).unwrap();
                let max = 2048;
                let chunks = merge_versions_chunked(&input, t, max).unwrap();
                assert!(chunks.len() > 2, "seed {seed}: {} batches", chunks.len());
                for c in &chunks {
                    let w: usize = row_weights(c).iter().sum();
                    assert!(
                        w <= max || c.num_rows() == 1,
                        "seed {seed}: {w} bytes in {} rows",
                        c.num_rows()
                    );
                }
                let all: Vec<SpanRow> = chunks.iter().flat_map(|c| rows_from_batch(c).unwrap()).collect();
                assert_eq!(all, rows_from_batch(&one).unwrap(), "seed {seed} {t:?}");
                let split = split_chunks_by_project(&chunks).unwrap();
                let want = split_by_project(&one).unwrap();
                assert_eq!(split.len(), want.len());
                for ((p, parts), (wp, w)) in split.iter().zip(&want) {
                    let got: Vec<SpanRow> = parts.iter().flat_map(|b| rows_from_batch(b).unwrap()).collect();
                    assert_eq!((p, got), (wp, rows_from_batch(w).unwrap()));
                }
            }
        }
        assert!(merge_versions_chunked(&[], Tombstones::Keep, 1).unwrap().is_empty());
    }

    fn proj(b: &RecordBatch, schema: &SchemaRef) -> RecordBatch {
        let idx: Vec<usize> = schema.fields().iter().map(|f| col_idx(f.name())).collect();
        b.project(&idx).unwrap()
    }

    #[test]
    fn projection_errors() {
        let b = batch_from_rows(&g1(1, 20));
        let no_version = crate::schema::projected_schema(&[col::SPAN_ID, col::PROJECT_ID, col::TRACE_ID, col::DELETED]);
        let r = merge_projected(
            &no_version,
            &[proj(&b, &no_version)],
            Tombstones::Keep,
            &Cancel::never(),
        );
        assert!(matches!(r, Err(ModelError::Schema(_))), "{r:?}");
        // version as Utf8: not a spans_schema field.
        let mut fields: Vec<arrow::datatypes::Field> =
            spans_schema().fields().iter().map(|f| f.as_ref().clone()).collect();
        fields[col_idx(col::VERSION)] = arrow::datatypes::Field::new(col::VERSION, DataType::Utf8, false);
        let bad: SchemaRef = Arc::new(arrow::datatypes::Schema::new(fields));
        let r = merge_projected(&bad, &[], Tombstones::Keep, &Cancel::never());
        assert!(matches!(r, Err(ModelError::Schema(_))), "{r:?}");
    }

    /// Every checkpoint fails the merge when cancellation hits it; one past the last one does not.
    #[test]
    fn cancel_checkpoints() {
        let input = chunked(&g1(3, 200), 7);
        let run = |c: &Cancel| merge_versions_projected(&spans_schema(), &input, Tombstones::Drop, 2048, c);
        let probe = Cancel::after(u64::MAX);
        let want = run(&probe).unwrap();
        let n = probe.checks();
        assert!(n >= 4, "{n} checkpoints");
        for k in 0..n {
            assert!(
                matches!(run(&Cancel::after(k)), Err(ModelError::Cancelled)),
                "checkpoint {k}"
            );
        }
        assert_eq!(run(&Cancel::after(n)).unwrap(), want);
        assert_eq!(run(&Cancel::never()).unwrap(), want);
        let c = Cancel::new();
        assert!(!c.is_cancelled());
        let g = CancelOnDrop(c.clone());
        drop(g);
        assert!(c.is_cancelled() && matches!(run(&c), Err(ModelError::Cancelled)));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        /// Any projection in any column order, any input batch order, plus a merged chunk next to its own raw
        /// rows (the snapshot window: equal versions) == the projected full merge.
        #[test]
        fn merge_projected_matches_full_merge(g in 1u8..=2, seed in 1u64.., n in 1usize..300, chunk in 1usize..40,
                                              mask in 0u32..(1 << 21), drop in proptest::bool::ANY, max in 64usize..100_000,
                                              window in 0usize..300) {
            let rows = generate(g, seed, n);
            let mut r = Rng(seed ^ 0x5DEECE66D);
            let others: Vec<&str> = crate::schema::ALL_COLUMNS.iter().copied().filter(|c| !MERGE_COLUMNS.contains(c)).collect();
            let mut names: Vec<&str> = MERGE_COLUMNS.to_vec();
            names.extend(others.iter().enumerate().filter(|(i, _)| mask & (1 << i) != 0).map(|(_, c)| *c));
            for i in (1..names.len()).rev() {
                names.swap(i, r.below(i as u64 + 1) as usize);
            }
            let schema = crate::schema::projected_schema(&names);
            let t = if drop { Tombstones::Drop } else { Tombstones::Keep };
            let mut input = chunked(&rows, chunk);
            let w = window.min(rows.len());
            if w > 0 {
                input.push(merge_versions(&[batch_from_rows(&rows[..w])], Tombstones::Keep).unwrap());
            }
            for i in (1..input.len()).rev() {
                input.swap(i, r.below(i as u64 + 1) as usize);
            }
            let pin: Vec<RecordBatch> = input.iter().map(|b| proj(b, &schema)).collect();
            let got = merge_versions_projected(&schema, &pin, t, max, &Cancel::never()).unwrap();
            let got = arrow::compute::concat_batches(&schema, &got).unwrap();
            let want = proj(&merge_versions(&chunked(&rows, chunk), t).unwrap(), &schema);
            let sort = |b: &RecordBatch| -> RecordBatch {
                let cols: Vec<ArrayRef> = [col::PROJECT_ID, col::TRACE_ID, col::SPAN_ID].iter().map(|c| b.column(b.schema().index_of(c).unwrap()).clone()).collect();
                let conv = RowConverter::new(cols.iter().map(|a| SortField::new(a.data_type().clone())).collect()).unwrap();
                let r = conv.convert_columns(&cols).unwrap();
                let mut idx: Vec<u32> = (0..b.num_rows() as u32).collect();
                idx.sort_by(|&x, &y| r.row(x as usize).cmp(&r.row(y as usize)));
                take_record_batch(b, &UInt32Array::from(idx)).unwrap()
            };
            if names.contains(&col::DOTTED_ORDER) {
                prop_assert_eq!(got, want);
            } else {
                prop_assert_eq!(sort(&got), sort(&want));
            }
        }
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
            prop_assert!(out.windows(2).all(|w| physical_key(&w[0]) < physical_key(&w[1])), "strictly sorted");
            let keys: std::collections::HashSet<_> =
                out.iter().map(|r| (r.project_id.clone(), r.trace_id, r.span_id)).collect();
            prop_assert_eq!(keys.len(), out.len());
            for (_, part) in split_by_project(&m).unwrap() { prop_assert!(part.num_rows() > 0); }
        }
    }
}
