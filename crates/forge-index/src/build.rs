//! `.fts` builder: (row id, column, text | JSON) -> bytes. Terms are interned once (foldhash), each (term, row) is
//! recorded once as a pair, and `finish` groups the pairs by term with a counting sort (SmithDB pt. 2), so the
//! sort costs O(distinct terms · log) and O(postings).
use crate::analyzer::Analyzer;
use crate::format::{BlockMeta, FST_BLOCK_RAW_BUDGET, Footer, MAGIC, TermValue, encode_postings, put_u32};
use crate::{FtsColumn, IndexError, IndexResult};
use arrow::array::{Array, AsArray};
use arrow::record_batch::RecordBatch;
use forge_model::schema::{col, col_idx};
use std::collections::HashMap;

type TermMap = HashMap<Box<[u8]>, u32, foldhash::fast::RandomState>;
const NO_ROW: u32 = u32::MAX;

/// Sizes of one built file (logged at flush; the bench prints them).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BuildStats {
    pub rows: u32,
    pub terms: u64,
    /// (term, row) pairs.
    pub postings: u64,
    pub inline_terms: u64,
    pub varint_terms: u64,
    pub roaring_terms: u64,
    pub postings_bytes: u64,
    pub dict_bytes: u64,
    pub blocks: u32,
    pub file_bytes: u64,
}

pub struct FtsBuilder {
    an: Analyzer,
    terms: TermMap,
    /// Per term id: the last row recorded (dedupes repeats within a row, and checks the row order).
    last_row: Vec<u32>,
    /// (term id, row), in insertion order.
    pairs: Vec<(u32, u32)>,
    /// Rows seen = the next row id of `add_batch`.
    rows: u32,
    /// Rows of `add_batch` whose start_time is null.
    null_starts: roaring::RoaringBitmap,
    block_budget: usize,
}

impl Default for FtsBuilder {
    fn default() -> Self {
        Self {
            an: Analyzer::default(),
            terms: TermMap::default(),
            last_row: Vec::new(),
            pairs: Vec::new(),
            rows: 0,
            null_starts: roaring::RoaringBitmap::new(),
            block_budget: FST_BLOCK_RAW_BUDGET,
        }
    }
}

impl FtsBuilder {
    /// Raw key bytes per FST block (tests use tiny blocks to get many).
    pub fn with_block_budget(mut self, bytes: usize) -> Self {
        self.block_budget = bytes.max(1);
        self
    }

    /// (term, row) pairs recorded so far: the builder's memory is ~8 B per pair plus the interned keys.
    pub fn postings(&self) -> u64 {
        self.pairs.len() as u64
    }

    /// Rows seen so far (`add_batch` row ids are 0..rows).
    pub fn rows(&self) -> u32 {
        self.rows
    }

    /// Indexes one cell. Per column, `row` must not decrease (rows of different columns may interleave).
    pub fn add(&mut self, row: u32, col: FtsColumn, value: &str) -> IndexResult<()> {
        if row == NO_ROW {
            return Err(IndexError::Build("row id overflow".into()));
        }
        self.rows = self.rows.max(row + 1);
        let Self {
            an,
            terms,
            last_row,
            pairs,
            ..
        } = self;
        let mut out_of_order = false;
        an.for_each_key(col, value, &mut |k| {
            let t = match terms.get(k) {
                Some(&t) => t,
                None => {
                    let t = last_row.len() as u32;
                    terms.insert(k.into(), t);
                    last_row.push(NO_ROW);
                    t
                }
            };
            let last = &mut last_row[t as usize];
            if *last != row {
                out_of_order |= *last != NO_ROW && *last > row;
                *last = row;
                pairs.push((t, row));
            }
        });
        if out_of_order {
            return Err(IndexError::Build(format!(
                "row {row} added after a later row of {}",
                col.name()
            )));
        }
        Ok(())
    }

    /// Indexes every FTS column of a spans_schema batch; its rows get the next row ids. Feed the batches the
    /// segment writer gets, in the same order (invariant 2), and the ids are the `.vortex` row positions.
    pub fn add_batch(&mut self, b: &RecordBatch) -> IndexResult<()> {
        let base = self.rows;
        let n = u32::try_from(b.num_rows())
            .ok()
            .filter(|n| base.checked_add(*n).is_some_and(|e| e < NO_ROW));
        let n = n.ok_or_else(|| IndexError::Build("segment exceeds u32 row ids".into()))?;
        let start = b.column(col_idx(col::START_TIME));
        if start.null_count() > 0 {
            self.null_starts
                .extend((0..n).filter(|i| start.is_null(*i as usize)).map(|i| base + i));
        }
        for col in FtsColumn::ALL {
            let a = b.column(col_idx(col.name())).as_string::<i32>();
            if a.null_count() == a.len() {
                continue;
            }
            for i in 0..n {
                if a.is_valid(i as usize) {
                    self.add(base + i, col, a.value(i as usize))?;
                }
            }
        }
        self.rows = base + n;
        Ok(())
    }

    /// Writes the file.
    pub fn finish(self) -> IndexResult<(Vec<u8>, BuildStats)> {
        let fail = |e: String| IndexError::Build(e);
        let mut st = BuildStats {
            rows: self.rows,
            terms: self.last_row.len() as u64,
            postings: self.pairs.len() as u64,
            ..Default::default()
        };
        // Group the pairs by term (counting sort; rows stay in insertion order, i.e. ascending per term).
        let n_terms = self.last_row.len();
        let mut start = vec![0usize; n_terms + 1];
        for &(t, _) in &self.pairs {
            start[t as usize + 1] += 1;
        }
        for i in 0..n_terms {
            start[i + 1] += start[i];
        }
        let mut fill = start.clone();
        let mut rows = vec![0u32; self.pairs.len()];
        for (t, r) in self.pairs {
            rows[fill[t as usize]] = r;
            fill[t as usize] += 1;
        }
        drop(fill);
        let mut keys: Vec<(Box<[u8]>, u32)> = self.terms.into_iter().collect();
        keys.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        // Postings (df >= 2), in key order; FST values for every term.
        let mut out = Vec::with_capacity(rows.len() * 2 + keys.len() * 16);
        out.extend_from_slice(MAGIC);
        let p0 = out.len() as u64;
        let mut values = Vec::with_capacity(keys.len());
        for (_, t) in &keys {
            let r = &rows[start[*t as usize]..start[*t as usize + 1]];
            let v = if let [row] = r {
                st.inline_terms += 1;
                TermValue::Inline(*row)
            } else {
                let offset = out.len() as u64 - p0;
                if encode_postings(r, &mut out) {
                    st.roaring_terms += 1;
                } else {
                    st.varint_terms += 1;
                }
                TermValue::Postings {
                    offset,
                    len: out.len() as u64 - p0 - offset,
                }
            };
            values.push(v.pack().map_err(fail)?);
        }
        drop(rows);
        let postings = p0..out.len() as u64;

        // Dict: FST blocks of about `block_budget` raw key bytes each.
        let d0 = out.len() as u64;
        let mut blocks = Vec::new();
        let mut i = 0;
        while i < keys.len() {
            let (first, mut raw) = (i, 0);
            let mut b = fst::MapBuilder::memory();
            while i < keys.len() && raw < self.block_budget {
                b.insert(&keys[i].0, values[i]).map_err(|e| fail(e.to_string()))?;
                raw += keys[i].0.len();
                i += 1;
            }
            let bytes = b.into_inner().map_err(|e| fail(e.to_string()))?;
            blocks.push(BlockMeta {
                first_key: keys[first].0.to_vec(),
                last_key: keys[i - 1].0.to_vec(),
                offset: out.len() as u64,
                len: u32::try_from(bytes.len()).map_err(|_| fail("FST block over 4 GiB".into()))?,
                num_terms: (i - first) as u32,
                crc: crc32fast::hash(&bytes),
            });
            out.extend_from_slice(&bytes);
        }
        let dict = d0..out.len() as u64;

        let footer = Footer {
            num_rows: self.rows,
            cols: FtsColumn::ALL.iter().map(|c| *c as u8).collect(),
            postings,
            dict,
            num_terms: keys.len() as u64,
            blocks,
            null_starts: self.null_starts,
        };
        let f0 = out.len();
        footer.encode(&mut out);
        let crc = crc32fast::hash(&out[f0..]);
        let flen = (out.len() - f0) as u32;
        put_u32(&mut out, flen);
        put_u32(&mut out, crc);
        out.extend_from_slice(MAGIC);
        st.postings_bytes = footer.postings.end - footer.postings.start;
        st.dict_bytes = footer.dict.end - footer.dict.start;
        st.blocks = footer.blocks.len() as u32;
        st.file_bytes = out.len() as u64;
        Ok((out, st))
    }
}
