//! `.fts` v1 byte layout (research/fts.md §3). Little-endian; offsets are absolute file offsets.
//!
//! ```text
//! "FTS1" | POSTINGS (df >= 2 terms, key order) | DICT (FST blocks, ascending disjoint key ranges) | FOOTER
//!        | footer_len u32 | footer_crc32 u32 | "FTS1"
//! ```
//! The dict sits right before the footer, so the 64 KiB tail read of a small L0 file returns the whole dictionary.
use std::ops::Range;

pub const MAGIC: &[u8; 4] = b"FTS1";
pub const VERSION: u16 = 1;
/// footer_len u32 | footer_crc32 u32 | MAGIC.
pub const TRAILER_LEN: u64 = 12;
/// 64K-row groups == the roaring container key (`row >> 16`).
pub const RG_SHIFT: u8 = 16;

/// `[col][01] token`: `search(col, q)`.
pub const KIND_TOKEN: u8 = 0x01;
/// `[col][02] path 0x00 token`: `json_key_search(col, path, text)`.
pub const KIND_PATH_TOKEN: u8 = 0x02;
/// `[col][03] path`: path existence.
pub const KIND_PATH: u8 = 0x03;
pub const PATH_SEP: u8 = 0x00;

/// Raw key bytes per FST block (fts.md §4: 64 KiB gives ~45-60 KB blocks).
pub const FST_BLOCK_RAW_BUDGET: usize = 64 << 10;
/// Posting lists of 2..=16 rows are delta varints; longer ones are roaring per 64K-row group.
pub const SMALL_LIST_MAX: usize = 16;
const ENC_VARINT: u8 = 1;
const ENC_ROARING: u8 = 2;

/// FST value: bit 63 = inline row id (df = 1) in bits 0..32; else bits 0..37 = offset into the postings section,
/// bits 37..63 = byte length.
const INLINE_FLAG: u64 = 1 << 63;
const OFF_BITS: u32 = 37;
const LEN_BITS: u32 = 26;

pub type FmtResult<T> = Result<T, String>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TermValue {
    Inline(u32),
    /// Relative to `Footer::postings.start`.
    Postings {
        offset: u64,
        len: u64,
    },
}

impl TermValue {
    pub fn pack(self) -> FmtResult<u64> {
        match self {
            Self::Inline(row) => Ok(INLINE_FLAG | u64::from(row)),
            Self::Postings { offset, len } if offset < 1 << OFF_BITS && len < 1 << LEN_BITS => {
                Ok(offset | (len << OFF_BITS))
            }
            Self::Postings { offset, len } => Err(format!("posting entry out of range: offset {offset}, len {len}")),
        }
    }
    pub fn unpack(v: u64) -> Self {
        if v & INLINE_FLAG != 0 {
            Self::Inline(v as u32)
        } else {
            Self::Postings {
                offset: v & ((1 << OFF_BITS) - 1),
                len: v >> OFF_BITS,
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub offset: u64,
    pub len: u32,
    pub num_terms: u32,
    /// crc32 of the block bytes (checked when the block is fetched).
    pub crc: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Footer {
    pub num_rows: u32,
    /// FTS column ids (`FtsColumn as u8`) indexed in this file; a missing column falls back to a scan.
    pub cols: Vec<u8>,
    pub postings: Range<u64>,
    pub dict: Range<u64>,
    pub num_terms: u64,
    pub blocks: Vec<BlockMeta>,
}

impl Footer {
    pub fn encode(&self, o: &mut Vec<u8>) {
        put_u16(o, VERSION);
        put_u16(o, 0); // flags: bit0 positions, bit1 zstd-framed postings (both 0 in v1)
        put_u32(o, self.num_rows);
        o.push(RG_SHIFT);
        o.push(self.cols.len() as u8);
        o.extend_from_slice(&self.cols);
        for r in [&self.postings, &self.dict] {
            put_u64(o, r.start);
            put_u64(o, r.end - r.start);
        }
        put_u64(o, self.num_terms);
        put_u32(o, self.blocks.len() as u32);
        for b in &self.blocks {
            for k in [&b.first_key, &b.last_key] {
                put_u16(o, k.len() as u16);
                o.extend_from_slice(k);
            }
            put_u64(o, b.offset);
            put_u32(o, b.len);
            put_u32(o, b.num_terms);
            put_u32(o, b.crc);
        }
    }

    /// `file_len`: every range must lie inside the file, before the footer.
    pub fn decode(b: &[u8], file_len: u64) -> FmtResult<Self> {
        let mut c = Cur::new(b);
        let version = c.u16()?;
        if version != VERSION {
            return Err(format!("unsupported version {version}"));
        }
        let flags = c.u16()?;
        if flags != 0 {
            return Err(format!("unsupported flags {flags:#x}"));
        }
        let num_rows = c.u32()?;
        if c.u8()? != RG_SHIFT {
            return Err("unsupported row-group shift".into());
        }
        let n = c.u8()? as usize;
        let cols = c.bytes(n)?.to_vec();
        let mut range = || -> FmtResult<Range<u64>> {
            let (s, l) = (c.u64()?, c.u64()?);
            let e = s
                .checked_add(l)
                .filter(|e| *e <= file_len)
                .ok_or("section out of bounds")?;
            Ok(s..e)
        };
        let (postings, dict) = (range()?, range()?);
        let num_terms = c.u64()?;
        let n = c.u32()? as usize;
        let mut blocks = Vec::with_capacity(n.min(1 << 16));
        for _ in 0..n {
            let n = c.u16()? as usize;
            let first_key = c.bytes(n)?.to_vec();
            let n = c.u16()? as usize;
            let last_key = c.bytes(n)?.to_vec();
            let (offset, len, num_terms, crc) = (c.u64()?, c.u32()?, c.u32()?, c.u32()?);
            if offset < dict.start || offset + u64::from(len) > dict.end {
                return Err("dict block out of bounds".into());
            }
            blocks.push(BlockMeta {
                first_key,
                last_key,
                offset,
                len,
                num_terms,
                crc,
            });
        }
        if c.p != b.len() {
            return Err("trailing footer bytes".into());
        }
        Ok(Self {
            num_rows,
            cols,
            postings,
            dict,
            num_terms,
            blocks,
        })
    }
}

/// Appends one posting entry for `rows` (sorted, distinct, len >= 2):
/// `enc u8 | doc_freq u32 |` then `doc_freq x varint(delta)` (first delta absolute), or
/// `n_rg u16 | n_rg x (rg u16, byte_len u32) | n_rg x portable roaring (global row ids, optimize()d)`.
pub fn encode_postings(rows: &[u32], o: &mut Vec<u8>) -> bool {
    debug_assert!(rows.len() >= 2 && rows.windows(2).all(|w| w[0] < w[1]));
    if rows.len() <= SMALL_LIST_MAX {
        o.push(ENC_VARINT);
        put_u32(o, rows.len() as u32);
        let mut prev = 0;
        for &r in rows {
            put_varint(o, r - prev);
            prev = r;
        }
        return false;
    }
    o.push(ENC_ROARING);
    put_u32(o, rows.len() as u32);
    let parts: Vec<(u16, Vec<u8>)> = rows
        .chunk_by(|a, b| a >> RG_SHIFT == b >> RG_SHIFT)
        .map(|chunk| {
            let mut bm = roaring::RoaringBitmap::from_sorted_iter(chunk.iter().copied()).expect("sorted");
            bm.optimize(); // run containers for dense terms (system prompts, roles)
            let mut buf = Vec::with_capacity(bm.serialized_size());
            bm.serialize_into(&mut buf).expect("Vec write");
            ((chunk[0] >> RG_SHIFT) as u16, buf)
        })
        .collect();
    put_u16(o, parts.len() as u16);
    for (rg, b) in &parts {
        put_u16(o, *rg);
        put_u32(o, b.len() as u32);
    }
    for (_, b) in &parts {
        o.extend_from_slice(b);
    }
    true
}

/// Decodes one posting entry; `keep(rg)` skips row groups already ruled out (e.g. by another predicate).
pub fn decode_postings(buf: &[u8], keep: &dyn Fn(u16) -> bool) -> FmtResult<roaring::RoaringBitmap> {
    let mut c = Cur::new(buf);
    let enc = c.u8()?;
    let df = c.u32()?;
    let mut bm = roaring::RoaringBitmap::new();
    match enc {
        ENC_VARINT => {
            let mut row = 0u32;
            for i in 0..df {
                let d = c.varint()?;
                row = if i == 0 {
                    d
                } else {
                    row.checked_add(d).ok_or("row id overflow")?
                };
                if keep((row >> RG_SHIFT) as u16) {
                    bm.insert(row);
                }
            }
        }
        ENC_ROARING => {
            let n = c.u16()? as usize;
            let mut dir = Vec::with_capacity(n);
            for _ in 0..n {
                dir.push((c.u16()?, c.u32()? as usize));
            }
            for (rg, len) in dir {
                let part = c.bytes(len)?;
                if keep(rg) {
                    bm |= roaring::RoaringBitmap::deserialize_from(part).map_err(|e| e.to_string())?;
                }
            }
        }
        e => return Err(format!("unknown posting encoding {e}")),
    }
    if c.p != buf.len() {
        return Err("trailing posting bytes".into());
    }
    Ok(bm)
}

pub fn put_u16(o: &mut Vec<u8>, v: u16) {
    o.extend_from_slice(&v.to_le_bytes())
}
pub fn put_u32(o: &mut Vec<u8>, v: u32) {
    o.extend_from_slice(&v.to_le_bytes())
}
pub fn put_u64(o: &mut Vec<u8>, v: u64) {
    o.extend_from_slice(&v.to_le_bytes())
}
fn put_varint(o: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        o.push((v as u8) | 0x80);
        v >>= 7;
    }
    o.push(v as u8);
}

/// Bounds-checked reader: corrupt input is an error, never a panic.
pub struct Cur<'a> {
    b: &'a [u8],
    p: usize,
}
impl<'a> Cur<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Self { b, p: 0 }
    }
    pub fn bytes(&mut self, n: usize) -> FmtResult<&'a [u8]> {
        let s = self.b.get(self.p..self.p.saturating_add(n)).ok_or("truncated")?;
        self.p += n;
        Ok(s)
    }
    fn arr<const N: usize>(&mut self) -> FmtResult<[u8; N]> {
        Ok(self.bytes(N)?.try_into().expect("N bytes"))
    }
    pub fn u8(&mut self) -> FmtResult<u8> {
        Ok(self.bytes(1)?[0])
    }
    pub fn u16(&mut self) -> FmtResult<u16> {
        self.arr().map(u16::from_le_bytes)
    }
    pub fn u32(&mut self) -> FmtResult<u32> {
        self.arr().map(u32::from_le_bytes)
    }
    pub fn u64(&mut self) -> FmtResult<u64> {
        self.arr().map(u64::from_le_bytes)
    }
    fn varint(&mut self) -> FmtResult<u32> {
        let mut v = 0u32;
        for shift in (0..35).step_by(7) {
            let b = self.u8()?;
            v |= u32::from(b & 0x7f).checked_shl(shift).ok_or("varint overflow")?;
            if b < 0x80 {
                return Ok(v);
            }
        }
        Err("varint too long".into())
    }
}
