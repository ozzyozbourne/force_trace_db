//! Local segmented WAL.
//!
//! Layout: `<wal.dir>/<node_id>/<first_seq:020>.wal` + `LOCK`.
//! Segment = `FORGWAL1` magic, then records `len:u32le | crc32:u32le | seq:u64le | payload[len]`,
//! crc32 over `seq || payload`. A record never spans segments. Seqs are contiguous and increasing.
//!
//! One OS thread owns the active segment. Callers send `(item, oneshot)`; the thread has each item append
//! its payload to the group buffer (`encode`, in seq order: Forge stamps versions there) and frames everything queued (up to `group_commit_max_bytes`, or until `group_commit_max_delay` since
//! the first request of the group), does ONE `write_all` + `sync_data`, hands `(seq, item)` to
//! `on_durable` **in seq order** (forge-ingest inserts into the memtable there: invariant 7), and
//! only then acks. A failed write/fsync aborts the process (never retry fsync).
use forge_model::config::{WalConfig, WalMode};
use forge_model::error::Unimplemented;
use forge_model::failpoint;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

pub const MAGIC: &[u8; 8] = b"FORGWAL1";
pub const REC_HDR: usize = 4 + 4 + 8;

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("wal io: {0}")]
    Io(#[from] io::Error),
    /// Bad data that is not a torn tail (inside a sealed segment, bad magic, non-increasing seq).
    #[error("wal corrupt: {} @ {offset}: {msg}", path.display())]
    Corrupt { path: PathBuf, offset: u64, msg: String },
    /// Another process holds `LOCK`.
    #[error("wal dir {} is locked by another process", .0.display())]
    Locked(PathBuf),
    /// Writer stopped (shutdown or died).
    #[error("wal writer is closed")]
    Closed,
    /// The replay callback rejected a record.
    #[error("wal record {seq}: {msg}")]
    Apply { seq: u64, msg: String },
}

#[derive(Clone, Debug)]
pub struct WalOptions {
    /// Node-scoped: `<wal.dir>/<node_id>`.
    pub dir: PathBuf,
    /// Rotate after a segment reaches this size (default 64 MiB).
    pub segment_bytes: u64,
    /// Close a group at this many framed bytes (default 1 MiB).
    pub group_commit_max_bytes: usize,
    /// Close a group this long after its first request even if more are queued (default 10 ms).
    /// The group also closes as soon as the queue is empty (no linger).
    pub group_commit_max_delay: Duration,
    /// Bounded request queue (default 256): a full queue makes `append` wait. Queued bytes are bounded by the
    /// memtable budget (forge-ingest reserves a request's bytes before it is queued), so this only has to keep
    /// the writer busy: a group closes at `group_commit_max_bytes` (1 MiB), which 256 requests of >= 4 KiB fill.
    pub queue: usize,
}

impl WalOptions {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            segment_bytes: 64 << 20,
            group_commit_max_bytes: 1 << 20,
            group_commit_max_delay: Duration::from_millis(10),
            queue: 256,
        }
    }
}

struct Append<T> {
    item: T,
    ack: oneshot::Sender<u64>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct WriterStats {
    pub records: u64,
    pub fsyncs: u64,
}

pub struct Wal<T> {
    tx: std::sync::Mutex<Option<mpsc::Sender<Append<T>>>>,
    thread: std::sync::Mutex<Option<JoinHandle<WriterStats>>>,
}

/// One node's WAL, `<wal.dir>/<node_id>`, holding the directory lock for its lifetime. Its lifecycle is
/// `open` (lock) -> `replay` -> `start` (writer thread) -> `truncate` after each committed flush.
/// Local mode only: `wal.mode = object_store` is M5.
#[derive(Debug)]
pub struct WalLog {
    opts: WalOptions,
    _lock: File,
}

impl WalLog {
    /// Blocking (creates and locks the directory): call from `spawn_blocking`.
    pub fn open(cfg: &WalConfig, node_id: &str) -> Result<Self, crate::IngestError> {
        if cfg.mode == WalMode::ObjectStore {
            return Err(Unimplemented("M5").into());
        }
        let dir = cfg.dir.join(node_id);
        let lock = lock_dir(&dir)?;
        let opts = WalOptions {
            segment_bytes: cfg.segment_bytes.as_u64(),
            group_commit_max_bytes: cfg.group_commit_max_bytes.as_u64() as usize,
            group_commit_max_delay: cfg.group_commit_max_delay,
            ..WalOptions::new(dir)
        };
        Ok(Self { opts, _lock: lock })
    }

    pub fn dir(&self) -> &Path {
        &self.opts.dir
    }

    /// See [`replay`]. Must run before `start`.
    pub fn replay<E: std::fmt::Display>(
        &self,
        flushed_seq: u64,
        apply: impl FnMut(u64, &[u8]) -> Result<(), E>,
    ) -> Result<ReplayStats, WalError> {
        replay(&self.opts.dir, flushed_seq, apply)
    }

    /// See [`Wal::start`].
    pub fn start<T: Send + 'static>(
        &self,
        next_seq: u64,
        encode: impl FnMut(&mut T, &mut Vec<u8>) + Send + 'static,
        on_durable: impl FnMut(&mut Vec<(u64, T)>) + Send + 'static,
    ) -> Result<Wal<T>, WalError> {
        Wal::start(self.opts.clone(), next_seq, encode, on_durable)
    }

    /// See [`truncate`]. Blocking.
    pub fn truncate(&self, flushed_seq: u64) -> io::Result<usize> {
        truncate(&self.opts.dir, flushed_seq)
    }
}

pub fn segment_path(dir: &Path, first_seq: u64) -> PathBuf {
    dir.join(format!("{first_seq:020}.wal"))
}

/// Exclusive advisory lock on `<dir>/LOCK` (std `File::try_lock`, Rust >= 1.89). Held for the
/// process lifetime; released by the kernel on exit, including `kill -9`.
pub fn lock_dir(dir: &Path) -> Result<File, WalError> {
    fs::create_dir_all(dir)?;
    let f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join("LOCK"))?;
    match f.try_lock() {
        Ok(()) => Ok(f),
        Err(fs::TryLockError::WouldBlock) => Err(WalError::Locked(dir.to_path_buf())),
        Err(fs::TryLockError::Error(e)) => Err(e.into()),
    }
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Starts a fresh segment named `first_seq`. A file of that name can only exist if it holds no
/// records (else replay would have set next_seq past it), so overwrite it.
fn new_segment(dir: &Path, first_seq: u64) -> io::Result<File> {
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(segment_path(dir, first_seq))?;
    f.write_all(MAGIC)?;
    f.sync_all()?;
    sync_dir(dir)?;
    Ok(f)
}

/// The record checksum: crc32 over `seq || payload`.
fn record_crc(seq: u64, payload: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(&seq.to_le_bytes());
    h.update(payload);
    h.finalize()
}

/// Appends one record: a header placeholder, the payload (`encode` appends it), then the header backfilled.
fn frame(buf: &mut Vec<u8>, seq: u64, encode: impl FnOnce(&mut Vec<u8>)) {
    let start = buf.len();
    buf.extend_from_slice(&[0; REC_HDR]);
    encode(buf);
    let (hdr, payload) = buf[start..].split_at_mut(REC_HDR);
    hdr[0..4].copy_from_slice(&u32::try_from(payload.len()).expect("record < 4 GiB").to_le_bytes());
    hdr[4..8].copy_from_slice(&record_crc(seq, payload).to_le_bytes());
    hdr[8..16].copy_from_slice(&seq.to_le_bytes());
}

impl<T: Send + 'static> Wal<T> {
    /// The caller holds the directory lock (`lock_dir`, taken before `replay`). `next_seq` comes from
    /// `replay`. `encode` runs on the writer thread, in seq order, and appends the item's payload to
    /// the group buffer (P2: Forge stamps versions there). `on_durable` runs after the group fsync with the group's
    /// records in seq order, before the acks are sent.
    pub fn start(
        opts: WalOptions,
        next_seq: u64,
        encode: impl FnMut(&mut T, &mut Vec<u8>) + Send + 'static,
        on_durable: impl FnMut(&mut Vec<(u64, T)>) + Send + 'static,
    ) -> Result<Self, WalError> {
        assert!(next_seq >= 1, "seq 0 is reserved for 'nothing flushed'");
        let (tx, rx) = mpsc::channel(opts.queue);
        let file = new_segment(&opts.dir, next_seq)?;
        let thread = std::thread::Builder::new()
            .name("forge-wal".into())
            .spawn(move || writer_loop(opts, file, rx, next_seq, encode, on_durable))?;
        Ok(Self {
            tx: std::sync::Mutex::new(Some(tx)),
            thread: std::sync::Mutex::new(Some(thread)),
        })
    }

    /// Resolves after the record is fsynced and `on_durable` ran. Returns its seq.
    /// Cancel-safe in the sense that matters: if the caller is dropped after `send`, the record is
    /// still written and inserted (the client sees an error and retries; versions make that harmless).
    pub async fn append(&self, item: T) -> Result<u64, WalError> {
        let tx = self
            .tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or(WalError::Closed)?;
        let (ack, rx) = oneshot::channel();
        tx.send(Append { item, ack }).await.map_err(|_| WalError::Closed)?;
        rx.await.map_err(|_| WalError::Closed)
    }

    /// Stops accepting, lets the thread drain and fsync everything queued, joins it. Blocking:
    /// call from `spawn_blocking` or after the runtime's request handlers have stopped.
    /// Appends already queued (and senders cloned before this call) still complete.
    pub fn shutdown(&self) -> WriterStats {
        self.stop().map(|r| r.expect("wal thread panicked")).unwrap_or_default()
    }
}

impl<T> Wal<T> {
    /// Drops the sender and joins the thread (None if already stopped).
    fn stop(&self) -> Option<std::thread::Result<WriterStats>> {
        self.tx.lock().unwrap_or_else(|e| e.into_inner()).take();
        let t = self.thread.lock().unwrap_or_else(|e| e.into_inner()).take();
        t.map(JoinHandle::join)
    }
}

impl<T> Drop for Wal<T> {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[allow(clippy::print_stderr)] // abort path: tracing may not flush before abort()
fn writer_loop<T>(
    opts: WalOptions,
    mut file: File,
    mut rx: mpsc::Receiver<Append<T>>,
    mut next_seq: u64,
    mut encode: impl FnMut(&mut T, &mut Vec<u8>),
    mut on_durable: impl FnMut(&mut Vec<(u64, T)>),
) -> WriterStats {
    let mut stats = WriterStats::default();
    let mut seg_len = MAGIC.len() as u64;
    let mut buf = Vec::with_capacity(opts.group_commit_max_bytes + (64 << 10));
    let mut items: Vec<(u64, T)> = Vec::new();
    let mut acks: Vec<oneshot::Sender<u64>> = Vec::new();
    let die = |what: &str, e: io::Error| -> ! {
        // fsyncgate: after a failed write/fsync the page cache state is unknown. Never retry;
        // abort so the process restarts and replays from the last durable state.
        tracing::error!(error = %e, "WAL {what} failed; aborting");
        eprintln!("WAL {what} failed: {e}; aborting");
        std::process::abort();
    };

    while let Some(first) = rx.blocking_recv() {
        let started = Instant::now();
        let group_first = next_seq;
        let mut last_start = 0;
        let mut push = |mut a: Append<T>, buf: &mut Vec<u8>| {
            let seq = next_seq;
            next_seq += 1;
            last_start = buf.len();
            frame(buf, seq, |buf| encode(&mut a.item, buf));
            items.push((seq, a.item));
            acks.push(a.ack);
        };
        push(first, &mut buf);
        // Group commit: drain what is queued now; stop at 1 MiB or 10 ms after the first request.
        while buf.len() < opts.group_commit_max_bytes && started.elapsed() < opts.group_commit_max_delay {
            match rx.try_recv() {
                Ok(a) => push(a, &mut buf),
                Err(_) => break, // empty (or closed): fsync now
            }
        }
        if failpoint::fires("wal_torn_write") {
            // A crash in the middle of the group's write(2): the disk holds a prefix that ends inside the
            // group's last record.
            let cut = last_start + (buf.len() - last_start) / 2;
            let _ = file.write_all(&buf[..cut]).and_then(|()| file.sync_data());
            failpoint::kill("wal_torn_write");
        }
        if let Err(e) = file.write_all(&buf) {
            die("write", e);
        }
        if failpoint::fires("wal_lose_unsynced") {
            // Power-loss model: the bytes written since the last fsync never reach the disk.
            let _ = file.set_len(seg_len);
            let _ = file.sync_all();
            failpoint::kill("wal_lose_unsynced");
        }
        failpoint::hit("wal_before_fsync");
        if let Err(e) = file.sync_data() {
            die("fsync", e);
        }
        failpoint::hit("wal_after_fsync");
        stats.fsyncs += 1;
        stats.records += acks.len() as u64;
        seg_len += buf.len() as u64;
        buf.clear();
        on_durable(&mut items);
        items.clear();
        for (ack, seq) in acks.drain(..).zip(group_first..) {
            let _ = ack.send(seq); // receiver gone = client hung up; the record is durable anyway
        }
        if seg_len >= opts.segment_bytes {
            match new_segment(&opts.dir, next_seq) {
                Ok(f) => file = f,
                Err(e) => die("rotate", e),
            }
            seg_len = MAGIC.len() as u64;
        }
    }
    stats
}

/// Sorted `(first_seq, path)` of `*.wal` files.
pub fn list_segments(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut v = Vec::new();
    for e in fs::read_dir(dir)? {
        let p = e?.path();
        if p.extension().is_some_and(|x| x == "wal")
            && let Some(n) = p
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
        {
            v.push((n, p));
        }
    }
    v.sort();
    Ok(v)
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReplayStats {
    /// Seq for the next append: max(flushed_seq, last seq in the WAL) + 1.
    pub next_seq: u64,
    /// Records passed to the callback (seq > flushed_seq).
    pub applied: u64,
    /// Valid records skipped because seq <= flushed_seq.
    pub skipped: u64,
    /// `(segment, offset)` where a torn tail was cut off.
    pub truncated: Option<(PathBuf, u64)>,
    /// Empty/headerless trailing segments removed (crash during rotation).
    pub removed_empty: usize,
}

/// Replays every record with `seq > flushed_seq`, in seq order. Must run before `Wal::start`, with
/// the dir lock held. A torn record at the end of the LAST segment (short header, length past EOF,
/// CRC mismatch) is cut off with `set_len` + fsync; the same anywhere else is `Corrupt`.
pub fn replay<E: std::fmt::Display>(
    dir: &Path,
    flushed_seq: u64,
    apply: impl FnMut(u64, &[u8]) -> Result<(), E>,
) -> Result<ReplayStats, WalError> {
    fs::create_dir_all(dir)?;
    read(dir, flushed_seq, true, apply)
}

/// Records with `seq > flushed_seq`, read only (offline `forge gc`, review 1 #7): a missing dir counts 0; a torn
/// tail or a headerless last segment ends the count; nothing is created, cut or removed.
pub fn count_unflushed(dir: &Path, flushed_seq: u64) -> Result<u64, WalError> {
    if !dir.is_dir() {
        return Ok(0);
    }
    Ok(read(dir, flushed_seq, false, |_, _| Ok::<_, String>(()))?.applied)
}

/// `replay`'s loop; `repair` = cut a torn tail and remove a headerless last segment.
fn read<E: std::fmt::Display>(
    dir: &Path,
    flushed_seq: u64,
    repair: bool,
    mut apply: impl FnMut(u64, &[u8]) -> Result<(), E>,
) -> Result<ReplayStats, WalError> {
    let segs = list_segments(dir)?;
    let mut st = ReplayStats {
        next_seq: flushed_seq + 1,
        ..Default::default()
    };
    let mut prev_seq: Option<u64> = None;
    for (i, (first_seq, path)) in segs.iter().enumerate() {
        let last = i + 1 == segs.len();
        let corrupt = |offset: u64, msg: String| WalError::Corrupt {
            path: path.clone(),
            offset,
            msg,
        };
        let file_len = fs::metadata(path)?.len();
        let mut r = BufReader::with_capacity(1 << 20, File::open(path)?);
        let mut magic = [0u8; 8];
        if file_len < MAGIC.len() as u64 {
            if last {
                // Crash between create and the magic's fsync: no records can be in it.
                if repair {
                    fs::remove_file(path)?;
                    sync_dir(dir)?;
                    st.removed_empty += 1;
                }
                continue;
            }
            return Err(corrupt(0, "short segment header".into()));
        }
        r.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(corrupt(0, format!("bad magic {magic:?}")));
        }
        let mut off = MAGIC.len() as u64;
        let mut payload = Vec::new();
        while off < file_len {
            let torn = |why: &str| -> Result<(), WalError> {
                if last {
                    Ok(())
                } else {
                    Err(corrupt(off, format!("{why} inside a sealed segment")))
                }
            };
            if file_len - off < REC_HDR as u64 {
                torn("short record header")?;
                break;
            }
            let mut hdr = [0u8; REC_HDR];
            r.read_exact(&mut hdr)?;
            let len = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as u64;
            let crc = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
            let seq = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
            if len > file_len - off - REC_HDR as u64 {
                torn("record length past end of file")?;
                break;
            }
            payload.resize(len as usize, 0);
            r.read_exact(&mut payload)?;
            if record_crc(seq, &payload) != crc {
                torn("crc mismatch")?;
                break;
            }
            // A CRC-valid record with a bad seq is not a torn write: refuse to guess.
            if seq < *first_seq || prev_seq.is_some_and(|p| seq <= p) {
                return Err(corrupt(
                    off,
                    format!("seq {seq} after {prev_seq:?} (segment starts at {first_seq})"),
                ));
            }
            prev_seq = Some(seq);
            if seq > flushed_seq {
                apply(seq, &payload).map_err(|e| WalError::Apply {
                    seq,
                    msg: e.to_string(),
                })?;
                st.applied += 1;
            } else {
                st.skipped += 1;
            }
            st.next_seq = st.next_seq.max(seq + 1);
            off += REC_HDR as u64 + len;
        }
        if off < file_len && repair {
            // Only reachable for the last segment (torn tail).
            let f = OpenOptions::new().write(true).open(path)?;
            f.set_len(off)?;
            f.sync_all()?;
            tracing::warn!(path = %path.display(), offset = off, "WAL torn tail truncated");
            st.truncated = Some((path.clone(), off));
        }
    }
    Ok(st)
}

/// Deletes sealed segments whose every record has `seq <= flushed_seq`. Segment i holds
/// `[first_i, first_{i+1})`; the newest listed segment is never deleted (it may be the active one;
/// if the writer rotated after the listing, it is merely kept until the next call).
pub fn truncate(dir: &Path, flushed_seq: u64) -> io::Result<usize> {
    let segs = list_segments(dir)?;
    let mut n = 0;
    for w in segs.windows(2) {
        if w[1].0 - 1 <= flushed_seq {
            match fs::remove_file(&w[0].1) {
                Ok(()) => n += 1,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
    }
    if n > 0 {
        sync_dir(dir)?;
    }
    Ok(n)
}

// ---- payload codec: Arrow IPC stream of one normalized spans batch ----

pub fn encode_batch(b: &arrow::record_batch::RecordBatch) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut out = Vec::with_capacity(b.get_array_memory_size() + 4096);
    let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut out, &b.schema())?;
    w.write(b)?;
    w.finish()?;
    drop(w);
    Ok(out)
}

/// `encode_batch` + `version_offset` for a validated spans batch. Both can only fail on a Forge bug (e.g. a schema
/// change `version_offset` does not know), so a failure is Internal, never the client's InvalidInput.
pub fn encode_payload(b: &arrow::record_batch::RecordBatch) -> Result<(Vec<u8>, usize), crate::IngestError> {
    let internal = |e: arrow::error::ArrowError| crate::IngestError::Internal(format!("wal encode: {e}"));
    let payload = encode_batch(b).map_err(internal)?;
    let at = version_offset(&payload, b.num_rows()).map_err(internal)?;
    Ok((payload, at))
}

/// Offset in an `encode_batch` payload of the values buffer of the spans `version` column (Int64, no nulls,
/// `rows` values), so the WAL thread can stamp versions into the encoded bytes (uncompressed IPC: the bytes
/// are exactly those of encoding the stamped batch).
pub fn version_offset(payload: &[u8], rows: usize) -> Result<usize, arrow::error::ArrowError> {
    use arrow::datatypes::DataType;
    use forge_model::schema::{col, col_idx, spans_schema};
    let err = |m: String| arrow::error::ArrowError::IpcError(format!("version buffer: {m}"));
    // IPC buffers per field: validity + (offsets + values | values), plus the child's for a list.
    fn buffers(dt: &DataType) -> usize {
        match dt {
            DataType::Utf8 | DataType::Binary => 3,
            DataType::List(f) => 2 + buffers(f.data_type()),
            _ => 2,
        }
    }
    let schema = spans_schema();
    let idx = 1 + schema.fields()[..col_idx(col::VERSION)]
        .iter()
        .map(|f| buffers(f.data_type()))
        .sum::<usize>();
    // Messages: `0xFFFFFFFF | meta_len:i32le | flatbuffer[meta_len] | body`; the schema comes first.
    let mut pos = 0;
    loop {
        let len = payload
            .get(pos + 4..pos + 8)
            .ok_or_else(|| err("no record batch".into()))?;
        let meta_len = i32::from_le_bytes(len.try_into().expect("4 bytes")) as usize;
        let meta = payload
            .get(pos + 8..pos + 8 + meta_len)
            .ok_or_else(|| err("truncated".into()))?;
        let msg = arrow::ipc::root_as_message(meta).map_err(|e| err(e.to_string()))?;
        let body = pos + 8 + meta_len;
        if let Some(rb) = msg.header_as_record_batch() {
            let b = rb
                .buffers()
                .and_then(|b| b.iter().nth(idx))
                .ok_or_else(|| err("missing".into()))?;
            if b.length() as usize != rows * 8 {
                return Err(err(format!("{} bytes for {rows} rows", b.length())));
            }
            return Ok(body + b.offset() as usize);
        }
        pos = body + msg.bodyLength() as usize;
    }
}

/// Writes `versions` (as i64 LE) over the version values buffer that starts at `at`.
pub fn stamp_versions(payload: &mut [u8], at: usize, versions: std::ops::Range<u64>) {
    let n = (versions.end - versions.start) as usize;
    for (dst, v) in payload[at..at + 8 * n].as_chunks_mut::<8>().0.iter_mut().zip(versions) {
        dst.copy_from_slice(&(v as i64).to_le_bytes());
    }
}

/// Exactly one batch, returned with the canonical `spans_schema()` Arc (IPC decoding yields an
/// equal but distinct schema; `validate_batch` accepts both, but downstream `Arc::ptr_eq` fast paths
/// and `concat_batches` with spans_schema() prefer the canonical one).
pub fn decode_batch(bytes: &[u8]) -> Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError> {
    use arrow::error::ArrowError;
    let mut r = arrow::ipc::reader::StreamReader::try_new(io::Cursor::new(bytes), None)?;
    let b = r
        .next()
        .ok_or_else(|| ArrowError::IpcError("empty WAL payload".into()))??;
    if r.next().is_some() {
        return Err(ArrowError::IpcError("WAL payload holds more than one batch".into()));
    }
    arrow::record_batch::RecordBatch::try_new(forge_model::schema::spans_schema(), b.columns().to_vec())
}
