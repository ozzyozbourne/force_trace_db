//! `.fts` reader over a range-fetch closure. `open` costs one tail read (two if the footer is over 64 KiB); a query
//! then costs at most two sequential round trips ("waves"): the missing FST blocks of every leaf, then every
//! posting range of the live leaves. Ranges in a wave are coalesced (1 MiB gap, 16 MiB cap) and fetched by one
//! call, which may issue its GETs concurrently (`ObjectStore::get_ranges`).
use crate::format::{Footer, MAGIC, TRAILER_LEN, TermValue, decode_postings};
use crate::query::{Leaf, Query};
use crate::{IndexError, IndexResult};
use bytes::Bytes;
use fst::{IntoStreamer, Map, Streamer};
use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::Shared;
use roaring::{MultiOps, RoaringBitmap};
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::ops::Range;
use std::sync::Mutex;

/// The speculative tail read: trailer + footer, and the whole dict of a small file.
pub(crate) const TAIL_READ: u64 = 64 << 10;
const COALESCE_GAP: u64 = 1 << 20;
const MAX_GET: u64 = 16 << 20;
/// A prefix leaf expanding to more terms is a query error.
pub const MAX_PREFIX_TERMS: usize = 10_000;

/// I/O of one call: `waves` = sequential round trips, `gets` = coalesced ranges.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Io {
    pub waves: u32,
    pub gets: u32,
    pub bytes: u64,
}

/// One open `.fts` file: the decoded footer plus the FST blocks fetched so far. Cache it per segment.
#[derive(Debug)]
pub struct FtsFile {
    path: String,
    footer: Footer,
    blocks: Mutex<HashMap<usize, Block>>,
}

/// A dict block: decoded, or being fetched by another call (the receiver resolves when that call is done with it,
/// fetched or not).
enum Block {
    Ready(Map<Bytes>),
    Loading(Shared<oneshot::Receiver<()>>),
}

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Ready(_) => "Ready",
            Self::Loading(_) => "Loading",
        })
    }
}

/// The blocks one call is fetching. On drop (done, failed or cancelled) the ones not cached are released, then the
/// calls waiting for them wake (the sender drops after `drop` ran) and fetch them themselves.
struct Claim<'a> {
    file: &'a FtsFile,
    ids: Vec<usize>,
    _done: oneshot::Sender<()>,
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        let mut b = self.file.blocks.lock().expect("poisoned");
        for i in &self.ids {
            if matches!(b.get(i), Some(Block::Loading(_))) {
                b.remove(i);
            }
        }
    }
}

impl FtsFile {
    /// `file_len` = `SegmentMeta::fts_byte_size`. `fetch(ranges)` returns the bytes of each range, in order.
    #[allow(clippy::single_range_in_vec_init)] // `fetch` takes a list of ranges
    pub async fn open<F, Fut>(path: &str, file_len: u64, fetch: &F) -> IndexResult<(Self, Io)>
    where
        F: Fn(Vec<Range<u64>>) -> Fut,
        Fut: Future<Output = IndexResult<Vec<Bytes>>>,
    {
        let corrupt = |msg: String| IndexError::Corrupt {
            path: path.to_string(),
            msg,
        };
        if file_len < MAGIC.len() as u64 + TRAILER_LEN {
            return Err(corrupt(format!("file of {file_len} bytes")));
        }
        let mut io = Io::default();
        let start = file_len.saturating_sub(TAIL_READ);
        let tail = get(path, fetch, vec![start..file_len], &mut io).await?.remove(0);
        let n = tail.len();
        if n as u64 != file_len - start || &tail[n - 4..] != MAGIC {
            return Err(corrupt("bad trailer".into()));
        }
        let flen = u64::from(u32::from_le_bytes(tail[n - 12..n - 8].try_into().expect("4")));
        let crc = u32::from_le_bytes(tail[n - 8..n - 4].try_into().expect("4"));
        let f_start = (file_len - TRAILER_LEN)
            .checked_sub(flen)
            .filter(|s| *s >= MAGIC.len() as u64)
            .ok_or_else(|| corrupt("footer length".into()))?;
        let fbytes = if f_start >= start {
            tail.slice((f_start - start) as usize..n - TRAILER_LEN as usize)
        } else {
            get(path, fetch, vec![f_start..file_len - TRAILER_LEN], &mut io)
                .await?
                .remove(0)
        };
        if crc32fast::hash(&fbytes) != crc {
            return Err(corrupt("footer checksum".into()));
        }
        let footer = Footer::decode(&fbytes, f_start).map_err(corrupt)?;
        let file = Self {
            path: path.to_string(),
            footer,
            blocks: Mutex::default(),
        };
        for (i, b) in file.footer.blocks.iter().enumerate() {
            if b.offset >= start {
                let s = (b.offset - start) as usize;
                file.cache_block(i, tail.slice(s..s + b.len as usize))?;
            }
        }
        Ok((file, io))
    }

    pub fn footer(&self) -> &Footer {
        &self.footer
    }

    /// Rows whose start_time is null (plans/phase-5 D8).
    pub fn null_starts(&self) -> &RoaringBitmap {
        &self.footer.null_starts
    }

    /// Candidate rows (file row ids) of `q`. A term missing from the dictionary ends its AND group before any
    /// postings read; when every group is dead, the second wave never happens.
    pub async fn execute<F, Fut>(&self, q: &Query, fetch: &F) -> IndexResult<(RoaringBitmap, Io)>
    where
        F: Fn(Vec<Range<u64>>) -> Fut,
        Fut: Future<Output = IndexResult<Vec<Bytes>>>,
    {
        let (mut rows, io) = self.execute_many(&[q], fetch).await?;
        Ok((rows.pop().expect("one query"), io))
    }

    /// `execute` of every query of `qs` at once: one wave of FST blocks and one of postings for all of them.
    pub async fn execute_many<F, Fut>(&self, qs: &[&Query], fetch: &F) -> IndexResult<(Vec<RoaringBitmap>, Io)>
    where
        F: Fn(Vec<Range<u64>>) -> Fut,
        Fut: Future<Output = IndexResult<Vec<Bytes>>>,
    {
        let mut io = Io::default();
        let leaves: Vec<&Leaf> = qs.iter().flat_map(|q| q.groups.iter().flatten()).collect();
        // Wave 1: the missing FST blocks of every leaf. A block another call is fetching is waited for, not fetched
        // again; if that call fails, the next round fetches it here.
        let want: BTreeSet<usize> = leaves.iter().flat_map(|l| self.blocks_for(l)).collect();
        loop {
            let (tx, rx) = oneshot::channel();
            let rx = rx.shared();
            let (mut mine, mut others) = (Vec::new(), Vec::new());
            {
                let mut cached = self.blocks.lock().expect("poisoned");
                for &i in &want {
                    match cached.get(&i) {
                        Some(Block::Ready(_)) => {}
                        Some(Block::Loading(w)) => others.push(w.clone()),
                        None => {
                            cached.insert(i, Block::Loading(rx.clone()));
                            mine.push(i);
                        }
                    }
                }
            }
            if mine.is_empty() && others.is_empty() {
                break;
            }
            let claim = Claim {
                file: self,
                ids: mine,
                _done: tx,
            };
            let fetch_mine = async {
                if !claim.ids.is_empty() {
                    let ranges = claim
                        .ids
                        .iter()
                        .map(|&i| {
                            let b = &self.footer.blocks[i];
                            b.offset..b.offset + u64::from(b.len)
                        })
                        .collect();
                    for (i, buf) in claim.ids.iter().zip(wave(&self.path, fetch, ranges, &mut io).await?) {
                        self.cache_block(*i, buf)?;
                    }
                }
                drop(claim); // before waiting for the others: they may wait for these blocks
                Ok::<_, IndexError>(())
            };
            let (r, _) = futures::join!(fetch_mine, futures::future::join_all(others));
            r?;
        }
        let vals = leaves
            .iter()
            .map(|l| self.resolve(l))
            .collect::<IndexResult<Vec<_>>>()?;
        // Prune AND groups with an empty leaf; `live[qi]`: the leaf ranges of query qi's live groups.
        let mut live: Vec<Vec<Range<usize>>> = vec![Vec::new(); qs.len()];
        let mut k = 0;
        for (qi, q) in qs.iter().enumerate() {
            for g in &q.groups {
                let r = k..k + g.len();
                k += g.len();
                if vals[r.clone()].iter().all(|v| !v.is_empty()) {
                    live[qi].push(r);
                }
            }
        }
        // Wave 2: every non-inline posting entry of the live leaves.
        let mut leaf_bm: Vec<RoaringBitmap> = vec![RoaringBitmap::new(); leaves.len()];
        let (mut ranges, mut owners) = (Vec::new(), Vec::new());
        let p0 = self.footer.postings.start;
        for li in live.iter().flatten().flat_map(|r| r.clone()) {
            for &v in &vals[li] {
                match TermValue::unpack(v) {
                    TermValue::Inline(row) => {
                        leaf_bm[li].insert(row);
                    }
                    TermValue::Postings { offset, len } => {
                        let r = p0
                            .checked_add(offset)
                            .and_then(|s| Some(s..s.checked_add(len)?))
                            .filter(|r| r.end <= self.footer.postings.end)
                            .ok_or_else(|| self.corrupt("posting range out of bounds"))?;
                        ranges.push(r);
                        owners.push(li);
                    }
                }
            }
        }
        if !ranges.is_empty() {
            for (li, buf) in owners.into_iter().zip(wave(&self.path, fetch, ranges, &mut io).await?) {
                leaf_bm[li] |= decode_postings(&buf).map_err(|e| self.corrupt(&e))?;
            }
        }
        let rows: Vec<RoaringBitmap> = live
            .into_iter()
            .map(|l| l.into_iter().map(|r| leaf_bm[r].iter().intersection()).union())
            .collect();
        // A row id past the file's rows would make the hit scan fail as a bad request: it is corruption (D4).
        if rows.iter().any(|r| r.max().is_some_and(|m| m >= self.footer.num_rows)) {
            return Err(self.corrupt("posting row id out of range"));
        }
        Ok((rows, io))
    }

    fn corrupt(&self, msg: &str) -> IndexError {
        IndexError::Corrupt {
            path: self.path.clone(),
            msg: msg.to_string(),
        }
    }

    fn cache_block(&self, i: usize, buf: Bytes) -> IndexResult<()> {
        if crc32fast::hash(&buf) != self.footer.blocks[i].crc {
            return Err(self.corrupt(&format!("dict block {i} checksum")));
        }
        let m = Map::new(buf).map_err(|e| self.corrupt(&e.to_string()))?;
        self.blocks.lock().expect("poisoned").insert(i, Block::Ready(m));
        Ok(())
    }

    /// The blocks whose key range can hold `leaf` (none = a miss with zero I/O).
    fn blocks_for(&self, leaf: &Leaf) -> Vec<usize> {
        let bl = &self.footer.blocks;
        match leaf {
            Leaf::Exact(k) => {
                let i = bl.partition_point(|b| b.first_key.as_slice() <= k.as_slice());
                if i > 0 && k.as_slice() <= bl[i - 1].last_key.as_slice() {
                    vec![i - 1]
                } else {
                    vec![]
                }
            }
            Leaf::Prefix(p) => {
                let s = prefix_successor(p);
                (0..bl.len())
                    .filter(|&i| {
                        bl[i].last_key.as_slice() >= p.as_slice()
                            && s.as_ref().is_none_or(|s| bl[i].first_key.as_slice() < s.as_slice())
                    })
                    .collect()
            }
        }
    }

    /// The FST values of `leaf` (its blocks are cached).
    fn resolve(&self, leaf: &Leaf) -> IndexResult<Vec<u64>> {
        let mut vals = Vec::new();
        let cached = self.blocks.lock().expect("poisoned");
        for b in self.blocks_for(leaf) {
            let Some(Block::Ready(m)) = cached.get(&b) else {
                unreachable!("wave 1 fetched every block")
            };
            match leaf {
                Leaf::Exact(k) => vals.extend(m.get(k)),
                Leaf::Prefix(p) => {
                    let s = prefix_successor(p);
                    let rb = m.range().ge(p);
                    let mut st = match &s {
                        Some(s) => rb.lt(s).into_stream(),
                        None => rb.into_stream(),
                    };
                    while let Some((_, v)) = st.next() {
                        vals.push(v);
                        if vals.len() > MAX_PREFIX_TERMS {
                            return Err(crate::matcher::prefix_too_broad());
                        }
                    }
                }
            }
        }
        Ok(vals)
    }
}

/// A `fetch` serving ranges from `file` in memory (tests, tools).
pub fn bytes_fetch(file: Bytes) -> impl Fn(Vec<Range<u64>>) -> std::future::Ready<IndexResult<Vec<Bytes>>> {
    move |rs| {
        std::future::ready(Ok(rs
            .into_iter()
            .map(|r| file.slice(r.start as usize..r.end as usize))
            .collect()))
    }
}

/// The smallest key greater than every key starting with `p` (None: no bound).
fn prefix_successor(p: &[u8]) -> Option<Vec<u8>> {
    let mut s = p.to_vec();
    while let Some(last) = s.pop() {
        if last < 0xff {
            s.push(last + 1);
            return Some(s);
        }
    }
    None
}

async fn get<F, Fut>(path: &str, fetch: &F, ranges: Vec<Range<u64>>, io: &mut Io) -> IndexResult<Vec<Bytes>>
where
    F: Fn(Vec<Range<u64>>) -> Fut,
    Fut: Future<Output = IndexResult<Vec<Bytes>>>,
{
    let want: Vec<u64> = ranges.iter().map(|r| r.end - r.start).collect();
    let bufs = fetch(ranges).await?;
    if bufs.len() != want.len() || bufs.iter().zip(&want).any(|(b, w)| b.len() as u64 != *w) {
        return Err(IndexError::Corrupt {
            path: path.to_string(),
            msg: "short range read".into(),
        });
    }
    io.waves += 1;
    io.gets += want.len() as u32;
    io.bytes += want.iter().sum::<u64>();
    Ok(bufs)
}

/// One round trip: coalesce, fetch, slice each requested range back out (zero-copy).
async fn wave<F, Fut>(path: &str, fetch: &F, ranges: Vec<Range<u64>>, io: &mut Io) -> IndexResult<Vec<Bytes>>
where
    F: Fn(Vec<Range<u64>>) -> Fut,
    Fut: Future<Output = IndexResult<Vec<Bytes>>>,
{
    let mut sorted = ranges.clone();
    sorted.sort_by_key(|r| r.start);
    let mut merged: Vec<Range<u64>> = Vec::new();
    for r in sorted {
        match merged.last_mut() {
            Some(m) if r.start <= m.end + COALESCE_GAP && r.end.max(m.end) - m.start <= MAX_GET => {
                m.end = m.end.max(r.end)
            }
            _ => merged.push(r),
        }
    }
    let bufs = get(path, fetch, merged.clone(), io).await?;
    Ok(ranges
        .iter()
        .map(|r| {
            let j = merged.partition_point(|m| m.start <= r.start) - 1;
            let m = &merged[j];
            bufs[j].slice((r.start - m.start) as usize..(r.end - m.start) as usize)
        })
        .collect())
}
