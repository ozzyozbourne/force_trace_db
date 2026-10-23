//! SQLite metastore: one connection behind a mutex, used from `spawn_blocking` (master decision 10).

use crate::{
    FlushCommit, MetaError, MetaResult, Metastore, NewSegment, Project, SegmentFilter, WalCheckpoint, migrations,
};
use async_trait::async_trait;
use forge_model::catalog::{HourBucket, Level, SegmentId, SegmentMeta, SegmentState, TraceLocation};
use forge_model::ids::{ProjectId, TraceId};
use forge_model::time::now_ms;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

/// Settings shared by the read-write and read-only connections: busy timeout, and a statement cache of 64
/// (sqlite.rs has 16 cached statements, rusqlite's default capacity).
fn base_setup(c: &Connection) -> rusqlite::Result<()> {
    c.pragma_update(None, "busy_timeout", 5000)?;
    c.set_prepared_statement_cache_capacity(64);
    Ok(())
}

fn set_pragmas(c: &Connection) -> rusqlite::Result<()> {
    base_setup(c)?;
    // Switching a new file to WAL needs an exclusive lock and SQLite returns BUSY without calling the busy
    // handler when a concurrent opener holds it, so retry for up to the busy timeout.
    // `:memory:` answers "memory"; nothing to assert.
    for attempt in 0.. {
        match c.query_row("PRAGMA journal_mode=WAL", [], |r| r.get::<_, String>(0)) {
            Err(rusqlite::Error::SqliteFailure(f, _))
                if f.code == rusqlite::ErrorCode::DatabaseBusy && attempt < 500 =>
            {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            r => {
                r?;
                break;
            }
        }
    }
    c.pragma_update(None, "synchronous", "FULL")?;
    c.pragma_update(None, "foreign_keys", "ON")?;
    c.pragma_update(None, "cache_size", -16000)?;
    c.pragma_update(None, "temp_store", "MEMORY")?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct SqliteMetastore {
    conn: Arc<Mutex<Connection>>,
    /// `open_read_only` of a read-only dir with an un-checkpointed WAL: the private copy, removed on drop.
    _copy: Option<Arc<TempCopy>>,
}

/// A temp directory holding copies of a database and its WAL; removed when dropped.
#[derive(Debug)]
struct TempCopy(std::path::PathBuf);
impl Drop for TempCopy {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `dir` accepts new files (a probe file is created and removed): permission bits alone miss ownership, ACLs and
/// read-only mounts.
fn dir_writable(dir: &Path) -> bool {
    let probe = dir.join(format!(".forge-probe-{}-{}", std::process::id(), probe_nonce()));
    match std::fs::OpenOptions::new().write(true).create_new(true).open(&probe) {
        Ok(_) => {
            let _ = std::fs::remove_file(&probe);
            true
        }
        Err(_) => false,
    }
}

fn probe_nonce() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos())
}

/// Prefix of `copy_snapshot`'s directories under the system temp dir.
const COPY_PREFIX: &str = "forge-meta-ro-";
/// A copy this old was left by a killed process (the guard removes it on every normal exit); the next copy
/// sweeps it.
const STALE_COPY: std::time::Duration = std::time::Duration::from_secs(3600);
const COPY_ATTEMPTS: u32 = 3;

/// What a writer changes between our two file copies: `meta.db`'s change counter (header bytes 24..28), size and
/// mtime (a checkpoint writes pages), and the WAL salts (header bytes 16..24, new on every WAL restart). An
/// append to the WAL changes none of them, and a torn last frame is dropped by SQLite's checksum.
#[derive(Debug, PartialEq)]
struct Generation {
    counter: Vec<u8>,
    len: u64,
    mtime: Option<std::time::SystemTime>,
    salts: Vec<u8>,
}

fn generation(db: &Path, wal: &Path) -> std::io::Result<Generation> {
    use std::io::{Read, Seek, SeekFrom};
    let header = |p: &Path, at: u64, n: u64| -> std::io::Result<Vec<u8>> {
        let mut f = std::fs::File::open(p)?;
        f.seek(SeekFrom::Start(at))?;
        let mut v = Vec::new();
        f.take(n).read_to_end(&mut v)?;
        Ok(v)
    };
    let m = std::fs::metadata(db)?;
    Ok(Generation {
        counter: header(db, 24, 4)?,
        len: m.len(),
        mtime: m.modified().ok(),
        salts: header(wal, 16, 8)?,
    })
}

#[cfg(test)]
thread_local! {
    /// Test hook: runs between the `meta.db` and the `-wal` copy of each attempt.
    static COPY_HOOK: std::cell::RefCell<Option<Box<dyn FnMut()>>> = const { std::cell::RefCell::new(None) };
}

/// Copies `db` and its non-empty `wal` into a fresh dir under the system temp dir; the guard removes it on drop.
/// A live writer can checkpoint and restart the WAL between the two copies (a silently stale or torn snapshot), so
/// the copy is kept only if `generation` is the same before and after; up to `COPY_ATTEMPTS`, then `Unstable`.
/// There is no size limit: the whole `meta.db` is copied each time. A kill skips the guard and leaves the dir
/// behind; copies older than `STALE_COPY` are swept here first.
fn copy_snapshot(db: &Path, wal: &Path) -> MetaResult<(std::path::PathBuf, Arc<TempCopy>)> {
    let tmp_root = std::env::temp_dir();
    sweep_stale_copies(&tmp_root);
    let tmp = tmp_root.join(format!("{COPY_PREFIX}{}-{}", std::process::id(), probe_nonce()));
    std::fs::create_dir_all(&tmp).map_err(|source| MetaError::CreateDir {
        path: tmp.clone(),
        source,
    })?;
    let guard = Arc::new(TempCopy(tmp.clone()));
    let to = tmp.join(db.file_name().unwrap_or("meta.db".as_ref()));
    let to_wal = sibling(&to, "-wal");
    let err = |from: &Path, to: &Path| {
        let (from, to) = (from.to_path_buf(), to.to_path_buf());
        move |source| MetaError::Copy { from, to, source }
    };
    for _ in 0..COPY_ATTEMPTS {
        let before = generation(db, wal).map_err(err(db, &to))?;
        std::fs::copy(db, &to).map_err(err(db, &to))?;
        #[cfg(test)]
        COPY_HOOK.with(|h| h.borrow_mut().as_mut().map(|f| f()));
        std::fs::copy(wal, &to_wal).map_err(err(wal, &to_wal))?;
        if generation(db, wal).map_err(err(db, &to))? == before {
            return Ok((to, guard));
        }
    }
    Err(MetaError::Unstable {
        path: db.to_path_buf(),
        attempts: COPY_ATTEMPTS,
    })
}

/// Removes `COPY_PREFIX` dirs in `root` last modified more than `STALE_COPY` ago (best effort).
fn sweep_stale_copies(root: &Path) {
    let Ok(entries) = std::fs::read_dir(root) else { return };
    for e in entries.flatten() {
        let stale = e.file_name().to_string_lossy().starts_with(COPY_PREFIX)
            && e.metadata()
                .and_then(|m| m.modified())
                .is_ok_and(|t| t.elapsed().is_ok_and(|age| age > STALE_COPY));
        if stale {
            let _ = std::fs::remove_dir_all(e.path());
        }
    }
}

/// `path` + `suffix` (e.g. `meta.db-wal`).
fn sibling(path: &Path, suffix: &str) -> std::path::PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(suffix);
    s.into()
}

const PROJECT_COLS: &str = "id, name, api_key_hash, retention_days, created_at_ms";
// SQL is built once; every statement goes through prepare_cached.
static INSERT_PROJECT: LazyLock<String> =
    LazyLock::new(|| format!("INSERT INTO projects ({PROJECT_COLS}) VALUES (?1,?2,?3,?4,?5)"));
static INSERT_PROJECT_IF_ABSENT: LazyLock<String> =
    LazyLock::new(|| format!("{} ON CONFLICT(id) DO NOTHING", *INSERT_PROJECT));
static GET_PROJECT: LazyLock<String> = LazyLock::new(|| format!("SELECT {PROJECT_COLS} FROM projects WHERE id = ?1"));
static LIST_PROJECTS: LazyLock<String> = LazyLock::new(|| format!("SELECT {PROJECT_COLS} FROM projects ORDER BY id"));
static INSERT_SEGMENT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "INSERT INTO segments ({SEGMENT_COLS}) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)"
    )
});
static GET_SEGMENT: LazyLock<String> = LazyLock::new(|| format!("SELECT {SEGMENT_COLS} FROM segments WHERE id = ?1"));
static LIST_SEGMENTS_BY_ID: LazyLock<String> = LazyLock::new(|| list_segments_sql("id"));
static LIST_SEGMENTS_NEWEST: LazyLock<String> =
    LazyLock::new(|| list_segments_sql("max_start_ns DESC NULLS FIRST, id DESC"));
static LOCATE_TRACE: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {SEGMENT_COLS}, ti_start, ti_count FROM segments JOIN \
         (SELECT segment_id, row_start AS ti_start, row_count AS ti_count FROM trace_index \
          WHERE project_id = ?1 AND trace_id = ?2) ON id = segment_id \
         WHERE state <> 'deleted' ORDER BY id"
    )
});
/// Opens `path` read-only and checks the schema version (the first read).
fn read_only_conn(path: &Path, immutable: bool) -> MetaResult<Connection> {
    use rusqlite::OpenFlags;
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_URI;
    let c = if immutable {
        let p = path
            .to_string_lossy()
            .replace('%', "%25")
            .replace('?', "%3f")
            .replace('#', "%23");
        Connection::open_with_flags(format!("file:{p}?immutable=1"), flags)?
    } else {
        Connection::open_with_flags(path, flags)?
    };
    base_setup(&c)?;
    let latest = migrations::LATEST;
    let has: bool = c.query_row(
        "SELECT count(*) > 0 FROM sqlite_master WHERE type = 'table' AND name = 'schema_versions'",
        [],
        |r| r.get(0),
    )?;
    let cur = if has { migrations::current_version(&c)? } else { 0 };
    if cur > latest {
        return Err(MetaError::SchemaTooNew {
            db: cur,
            binary: latest,
        });
    }
    if cur < latest {
        return Err(MetaError::Migration {
            version: latest,
            msg: format!("database schema v{cur} is older than v{latest}: run `forge serve` once to migrate"),
        });
    }
    Ok(c)
}

/// Runs the LOCATE_TRACE statement for one trace.
fn locations(
    st: &mut rusqlite::CachedStatement<'_>,
    project: &ProjectId,
    trace: TraceId,
) -> MetaResult<Vec<TraceLocation>> {
    let rows = st.query_map(params![project.as_str(), trace.as_bytes()], |r| {
        Ok((
            segment_from_row(r),
            r.get::<_, i64>("ti_start")?,
            r.get::<_, i64>("ti_count")?,
        ))
    })?;
    let mut out = Vec::new();
    for r in rows {
        let (seg, start, count) = r?;
        out.push(TraceLocation {
            segment: seg?,
            row_start: to_u64("trace_index", start)?,
            row_count: to_u64("trace_index", count)?,
        });
    }
    Ok(out)
}

const INSERT_TRACE: &str =
    "INSERT INTO trace_index (project_id, trace_id, segment_id, row_start, row_count) VALUES (?1,?2,?3,?4,?5)";
const UPSERT_CHECKPOINT: &str = "INSERT INTO wal_checkpoints (node_id, flushed_seq, max_version, updated_at_ms) \
     VALUES (?1, ?2, ?3, ?4) ON CONFLICT(node_id) DO UPDATE SET flushed_seq = MAX(flushed_seq, excluded.flushed_seq), \
     max_version = MAX(max_version, excluded.max_version), updated_at_ms = excluded.updated_at_ms";

fn list_segments_sql(order: &str) -> String {
    format!(
        "SELECT {SEGMENT_COLS} FROM segments \
         WHERE (?1 IS NULL OR project_id = ?1) AND (state <> 'deleted' OR ?2) ORDER BY {order}"
    )
}

const SEGMENT_COLS: &str = "id, project_id, level, partition_hour, path, row_count, byte_size, fts_byte_size, \
    min_start_ns, max_start_ns, start_time_nulls, min_trace_id, max_trace_id, min_version, max_version, \
    schema_version, created_at_ms, state, deleted_at_ms";

/// Only PRIMARY KEY / UNIQUE violations are conflicts; CHECK and NOT NULL stay `Sqlite` (D16).
fn map_err(e: rusqlite::Error, what: &str) -> MetaError {
    match e {
        rusqlite::Error::SqliteFailure(f, _)
            if matches!(
                f.extended_code,
                rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY | rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
            ) =>
        {
            MetaError::Conflict(format!("{what}: {e}"))
        }
        e => MetaError::Sqlite(e),
    }
}

fn corrupt(table: &'static str, msg: impl std::fmt::Display) -> MetaError {
    MetaError::Corrupt {
        table,
        msg: msg.to_string(),
    }
}
fn to_u64(table: &'static str, v: i64) -> MetaResult<u64> {
    u64::try_from(v).map_err(|_| corrupt(table, format!("negative value {v}")))
}

/// Poison-tolerant: a panic in another holder does not make the connection unusable.
fn lock(m: &Mutex<Connection>) -> MutexGuard<'_, Connection> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
fn to_i64(what: &str, v: u64) -> MetaResult<i64> {
    i64::try_from(v).map_err(|_| MetaError::InvalidInput(format!("{what} {v} exceeds i64::MAX")))
}

/// Borrows column `i` as text or blob (no allocation); a wrong type gives the same error as `Row::get`.
fn text<'r>(r: &'r Row<'_>, i: usize) -> MetaResult<&'r str> {
    let v = r.get_ref(i)?;
    v.as_str().map_err(|_| type_err(r, i, v))
}
fn blob<'r>(r: &'r Row<'_>, i: usize) -> MetaResult<&'r [u8]> {
    let v = r.get_ref(i)?;
    v.as_blob().map_err(|_| type_err(r, i, v))
}
fn type_err(r: &Row<'_>, i: usize, v: rusqlite::types::ValueRef<'_>) -> MetaError {
    let name = r.as_ref().column_name(i).map_or_else(|_| String::new(), str::to_string);
    rusqlite::Error::InvalidColumnType(i, name, v.data_type()).into()
}

fn segment_from_row(r: &Row<'_>) -> MetaResult<SegmentMeta> {
    let id = text(r, 0)?;
    let project = text(r, 1)?;
    let level: u8 = r.get(2)?;
    let min_tid = blob(r, 11)?;
    let max_tid = blob(r, 12)?;
    let state = text(r, 17)?;
    let bad = |m: String| corrupt("segments", m);
    let num = |i: usize| to_u64("segments", r.get(i)?);
    Ok(SegmentMeta {
        id: id.parse().map_err(|e| bad(format!("{e}")))?,
        project_id: ProjectId::new(project).map_err(|e| bad(e.to_string()))?,
        level: Level::from_u8(level).ok_or_else(|| bad(format!("level {level}")))?,
        partition_hour: HourBucket(r.get(3)?),
        path: r.get(4)?,
        row_count: num(5)?,
        byte_size: num(6)?,
        fts_byte_size: r.get::<_, Option<i64>>(7)?.map(|v| to_u64("segments", v)).transpose()?,
        min_start_ns: r.get(8)?,
        max_start_ns: r.get(9)?,
        start_time_nulls: num(10)?,
        min_trace_id: TraceId::from_slice(min_tid).map_err(|e| bad(e.to_string()))?,
        max_trace_id: TraceId::from_slice(max_tid).map_err(|e| bad(e.to_string()))?,
        min_version: num(13)?,
        max_version: num(14)?,
        schema_version: r.get(15)?,
        created_at_ms: r.get(16)?,
        state: SegmentState::parse(state).ok_or_else(|| bad(format!("state {state}")))?,
        deleted_at_ms: r.get(18)?,
    })
}

fn project_from_row(r: &Row<'_>) -> MetaResult<Project> {
    Ok(Project {
        id: ProjectId::new(text(r, 0)?).map_err(|e| corrupt("projects", e))?,
        name: r.get(1)?,
        api_key_hash: r.get(2)?,
        retention_days: r.get(3)?,
        created_at_ms: r.get(4)?,
    })
}

/// Trace ranges must cover the segment's rows exactly: contiguous, no overlap, no gap (invariant 2).
fn check_trace_ranges(s: &NewSegment) -> MetaResult<()> {
    let m = &s.meta;
    let mut ranges: Vec<_> = s.traces.iter().collect();
    ranges.sort_by_key(|t| t.row_start);
    let mut end = 0u64;
    for t in ranges {
        let bad = |why: &str| {
            MetaError::InvalidInput(format!(
                "segment {}: trace {} rows {}+{} {why} (segment has {} rows)",
                m.id, t.trace_id, t.row_start, t.row_count, m.row_count
            ))
        };
        if t.row_start < end {
            return Err(bad("overlap another trace range"));
        }
        if t.row_start > end {
            return Err(bad(&format!("leave rows {end}..{} in no trace", t.row_start)));
        }
        end = t.row_start.checked_add(t.row_count).ok_or_else(|| bad("overflow"))?;
        if end > m.row_count {
            return Err(bad("exceed the row count"));
        }
    }
    if end != m.row_count {
        return Err(MetaError::InvalidInput(format!(
            "segment {}: trace ranges cover rows 0..{end} of {}",
            m.id, m.row_count
        )));
    }
    Ok(())
}

/// Inserts one segment, or skips it when the same segment is already committed (a retried commit): same
/// id, path, project, level, partition, row count and byte size. Any other existing segment id is `Conflict`.
fn insert_segment(tx: &Transaction<'_>, s: &NewSegment, now: i64) -> MetaResult<()> {
    let m = &s.meta;
    let id = m.id.to_string();
    check_trace_ranges(s)?;
    let existing = tx
        .prepare_cached(&GET_SEGMENT)?
        .query_row([&id], |r| Ok(segment_from_row(r)))
        .optional()?
        .transpose()?;
    if let Some(old) = existing {
        let ident = |x: &SegmentMeta| {
            (
                x.path.clone(),
                x.project_id.clone(),
                x.level,
                x.partition_hour,
                x.row_count,
                x.byte_size,
            )
        };
        if ident(&old) == ident(m) {
            return Ok(());
        }
        return Err(MetaError::Conflict(format!(
            "segment {id} already exists as {:?}, not {:?} (path, project, level, hour, rows, bytes)",
            ident(&old),
            ident(m)
        )));
    }
    let p = Project::new(m.project_id.clone(), None, None, now)?;
    tx.prepare_cached(&INSERT_PROJECT_IF_ABSENT)?.execute(params![
        p.id.as_str(),
        p.name,
        p.api_key_hash,
        p.retention_days,
        p.created_at_ms
    ])?;
    tx.prepare_cached(&INSERT_SEGMENT)?
        .execute(params![
            id,
            m.project_id.as_str(),
            m.level.as_u8(),
            m.partition_hour.0,
            m.path,
            to_i64("row_count", m.row_count)?,
            to_i64("byte_size", m.byte_size)?,
            m.fts_byte_size.map(|v| to_i64("fts_byte_size", v)).transpose()?,
            m.min_start_ns,
            m.max_start_ns,
            to_i64("start_time_nulls", m.start_time_nulls)?,
            m.min_trace_id.as_bytes(),
            m.max_trace_id.as_bytes(),
            to_i64("min_version", m.min_version)?,
            to_i64("max_version", m.max_version)?,
            m.schema_version,
            m.created_at_ms,
            SegmentState::Live.as_str(),
            Option::<i64>::None,
        ])
        .map_err(|e| map_err(e, &format!("segment {id} ({})", m.path)))?;
    let mut st = tx.prepare_cached(INSERT_TRACE)?;
    for t in &s.traces {
        st.execute(params![
            m.project_id.as_str(),
            t.trace_id.as_bytes(),
            id,
            to_i64("row_start", t.row_start)?,
            to_i64("row_count", t.row_count)?
        ])
        .map_err(|e| map_err(e, &format!("trace {} in segment {id}", t.trace_id)))?;
    }
    Ok(())
}

impl SqliteMetastore {
    /// Opens (creating if needed) and migrates a database file.
    pub fn open_path(path: &Path) -> MetaResult<Self> {
        Self::init(Connection::open(path)?)
    }

    /// Read-only (`forge sql --data-dir`): SQLITE_OPEN_READ_ONLY, busy_timeout only (no journal_mode, no
    /// migrations); the schema version must equal this binary's. Writes fail with SQLITE_READONLY (Internal).
    /// SQLite creates `-shm` beside a WAL database on the first read, so a directory that does not accept files
    /// (a probe, or that first read failing with CANTOPEN / READONLY) needs another way in:
    /// - a non-empty `-wal` (commits not yet checkpointed, e.g. after a crash or a live server): `meta.db` and the
    ///   `-wal` are copied to a private temp dir and opened there, so SQLite replays the WAL (`immutable=1` would
    ///   ignore it); see `copy_snapshot` (a consistency check against a live writer, no size limit);
    /// - otherwise `immutable=1`: the main file is the whole database and nothing can write the directory.
    pub fn open_read_only(path: &Path) -> MetaResult<Self> {
        let dir = path
            .parent()
            .filter(|d| !d.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        if dir_writable(dir) {
            match read_only_conn(path, false) {
                Err(MetaError::Sqlite(rusqlite::Error::SqliteFailure(f, _)))
                    if matches!(f.code, rusqlite::ErrorCode::CannotOpen | rusqlite::ErrorCode::ReadOnly) => {}
                r => return Ok(Self::read_only(r?, None)),
            }
        }
        let wal = sibling(path, "-wal");
        if std::fs::metadata(&wal).is_ok_and(|m| m.len() > 0) {
            let (db, copy) = copy_snapshot(path, &wal)?;
            return Ok(Self::read_only(read_only_conn(&db, false)?, Some(copy)));
        }
        Ok(Self::read_only(read_only_conn(path, true)?, None))
    }

    fn read_only(c: Connection, copy: Option<Arc<TempCopy>>) -> Self {
        Self {
            conn: Arc::new(Mutex::new(c)),
            _copy: copy,
        }
    }

    pub fn open_in_memory() -> MetaResult<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(mut c: Connection) -> MetaResult<Self> {
        set_pragmas(&c)?;
        migrations::migrate(&mut c)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(c)),
            _copy: None,
        })
    }

    /// Test hook: the value of `PRAGMA <name>`.
    #[cfg(any(test, feature = "test-util"))]
    pub fn pragma(&self, name: &str) -> MetaResult<String> {
        use rusqlite::types::Value;
        let c = lock(&self.conn);
        let v: Value = c.query_row(&format!("PRAGMA {name}"), [], |r| r.get(0))?;
        Ok(match v {
            Value::Integer(i) => i.to_string(),
            Value::Text(s) => s,
            other => format!("{other:?}"),
        })
    }

    async fn read<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Connection) -> MetaResult<T> + Send + 'static,
    ) -> MetaResult<T> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || f(&lock(&conn))).await?
    }

    async fn write<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Transaction<'_>) -> MetaResult<T> + Send + 'static,
    ) -> MetaResult<T> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut c = lock(&conn);
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let out = f(&tx)?;
            tx.commit()?;
            Ok(out)
        })
        .await?
    }
}

#[async_trait]
impl Metastore for SqliteMetastore {
    async fn schema_version(&self) -> MetaResult<u32> {
        self.read(|c| Ok(migrations::current_version(c)?)).await
    }

    async fn create_project(&self, p: Project) -> MetaResult<()> {
        self.write(move |tx| {
            tx.prepare_cached(&INSERT_PROJECT)?
                .execute(params![
                    p.id.as_str(),
                    p.name,
                    p.api_key_hash,
                    p.retention_days,
                    p.created_at_ms
                ])
                .map_err(|e| map_err(e, &format!("project {}", p.id)))?;
            Ok(())
        })
        .await
    }

    async fn get_project(&self, id: &ProjectId) -> MetaResult<Option<Project>> {
        let id = id.clone();
        self.read(move |c| {
            c.prepare_cached(&GET_PROJECT)?
                .query_row([id.as_str()], |r| Ok(project_from_row(r)))
                .optional()?
                .transpose()
        })
        .await
    }

    async fn list_projects(&self) -> MetaResult<Vec<Project>> {
        self.read(|c| {
            let mut st = c.prepare_cached(&LIST_PROJECTS)?;
            let rows = st.query_map([], |r| Ok(project_from_row(r)))?;
            rows.map(|r| r?).collect()
        })
        .await
    }

    async fn commit_flush(&self, c: FlushCommit) -> MetaResult<()> {
        let (seq, max_version) = (
            to_i64("flushed_wal_seq", c.flushed_wal_seq)?,
            to_i64("max_version", c.max_version)?,
        );
        let mut ids = std::collections::HashSet::new();
        if let Some(s) = c.segments.iter().find(|s| !ids.insert(s.meta.id)) {
            return Err(MetaError::InvalidInput(format!(
                "segment {} is listed twice in one commit",
                s.meta.id
            )));
        }
        self.write(move |tx| {
            let now = now_ms();
            let stored: Option<i64> = tx
                .prepare_cached("SELECT flushed_seq FROM wal_checkpoints WHERE node_id = ?1")?
                .query_row([&c.node_id], |r| r.get(0))
                .optional()?;
            // Invariant 8: a stale or repeated flush (watermark not advancing) inserts nothing.
            if stored.is_none_or(|s| seq > s) {
                for s in &c.segments {
                    insert_segment(tx, s, now)?;
                }
            }
            tx.prepare_cached(UPSERT_CHECKPOINT)?
                .execute(params![c.node_id, seq, max_version, now])?;
            forge_model::failpoint::hit("meta_commit_in_txn"); // dies before COMMIT: the txn rolls back
            Ok(())
        })
        .await
    }

    async fn wal_checkpoint(&self, node_id: &str) -> MetaResult<Option<WalCheckpoint>> {
        let node = node_id.to_string();
        self.read(move |c| {
            let r: Option<(i64, i64)> = c
                .prepare_cached("SELECT flushed_seq, max_version FROM wal_checkpoints WHERE node_id = ?1")?
                .query_row([node], |r| Ok((r.get(0)?, r.get(1)?)))
                .optional()?;
            r.map(|(s, v)| {
                Ok(WalCheckpoint {
                    flushed_seq: to_u64("wal_checkpoints", s)?,
                    max_version: to_u64("wal_checkpoints", v)?,
                })
            })
            .transpose()
        })
        .await
    }

    async fn max_version(&self) -> MetaResult<u64> {
        self.read(|c| {
            let v: i64 = c
                .prepare_cached(
                    "SELECT MAX(COALESCE((SELECT MAX(max_version) FROM segments), 0), \
                                COALESCE((SELECT MAX(max_version) FROM wal_checkpoints), 0))",
                )?
                .query_row([], |r| r.get(0))?;
            to_u64("segments", v)
        })
        .await
    }

    async fn get_segment(&self, id: SegmentId) -> MetaResult<Option<SegmentMeta>> {
        self.read(move |c| {
            c.prepare_cached(&GET_SEGMENT)?
                .query_row([id.to_string()], |r| Ok(segment_from_row(r)))
                .optional()?
                .transpose()
        })
        .await
    }

    async fn list_segments(&self, f: &SegmentFilter) -> MetaResult<Vec<SegmentMeta>> {
        let f = f.clone();
        self.read(move |c| {
            let sql = if f.newest_first {
                &LIST_SEGMENTS_NEWEST
            } else {
                &LIST_SEGMENTS_BY_ID
            };
            let mut st = c.prepare_cached(sql)?;
            let rows = st.query_map(
                params![f.project_id.as_ref().map(ProjectId::as_str), f.include_deleted],
                |r| Ok(segment_from_row(r)),
            )?;
            let mut out = Vec::new();
            for r in rows {
                if f.limit.is_some_and(|l| out.len() >= l) {
                    break;
                }
                let s = r??;
                if f.levels.as_ref().is_none_or(|l| l.contains(&s.level)) && s.may_overlap_time(f.start_ns, f.end_ns) {
                    out.push(s);
                }
            }
            Ok(out)
        })
        .await
    }

    async fn locate_trace(&self, project: &ProjectId, trace: TraceId) -> MetaResult<Vec<TraceLocation>> {
        let project = project.clone();
        self.read(move |c| locations(&mut c.prepare_cached(&LOCATE_TRACE)?, &project, trace))
            .await
    }

    async fn locate_traces(
        &self,
        project: &ProjectId,
        traces: &[TraceId],
    ) -> MetaResult<HashMap<TraceId, Vec<TraceLocation>>> {
        let project = project.clone();
        let traces = traces.to_vec();
        // One lock and one blocking task; the cached LOCATE_TRACE statement per id (no generated SQL).
        self.read(move |c| {
            let mut st = c.prepare_cached(&LOCATE_TRACE)?;
            let mut out: HashMap<TraceId, Vec<TraceLocation>> = HashMap::new();
            for t in traces {
                let locs = locations(&mut st, &project, t)?;
                if !locs.is_empty() {
                    out.insert(t, locs);
                }
            }
            Ok(out)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A WAL database with 10 checkpointed rows and txn A (10 rows) still in the WAL; the writer stays open.
    fn wal_db(dir: &Path) -> (std::path::PathBuf, std::path::PathBuf, Connection) {
        let db = dir.join("meta.db");
        let w = Connection::open(&db).unwrap();
        w.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(())).unwrap();
        w.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        w.execute_batch("CREATE TABLE t(x)").unwrap();
        let insert10 = || {
            for i in 0..10 {
                w.execute("INSERT INTO t VALUES (?1)", [i]).unwrap();
            }
        };
        insert10();
        w.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
        insert10();
        (db.clone(), sibling(&db, "-wal"), w)
    }

    fn rows(db: &Path) -> i64 {
        Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap()
            .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .unwrap()
    }

    /// Between the two copies the writer checkpoints and commits txn B, restarting the WAL: txn A is in neither
    /// copied file. The first attempt is discarded; the retry sees all 21 rows.
    #[test]
    fn copy_retries_when_the_writer_restarts_the_wal() {
        let d = tempfile::tempdir().unwrap();
        let (db, wal, w) = wal_db(d.path());
        let calls = std::rc::Rc::new(std::cell::Cell::new(0));
        let c = calls.clone();
        COPY_HOOK.with(|h| {
            *h.borrow_mut() = Some(Box::new(move || {
                c.set(c.get() + 1);
                if c.get() == 1 {
                    w.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(())).unwrap();
                    w.execute("INSERT INTO t VALUES (99)", []).unwrap();
                }
            }))
        });
        let (copy, guard) = copy_snapshot(&db, &wal).unwrap();
        COPY_HOOK.with(|h| h.borrow_mut().take());
        assert_eq!(calls.get(), 2, "retried once");
        assert_eq!(rows(&copy), 21);
        let tmp = guard.0.clone();
        drop(guard);
        assert!(!tmp.exists());
    }

    /// A writer that changes the database on every attempt: `Unstable` after 3 attempts, pointing at
    /// `forge sql --server`; the temp dir is removed.
    #[test]
    fn copy_gives_up_on_a_busy_writer() {
        let d = tempfile::tempdir().unwrap();
        let (db, wal, w) = wal_db(d.path());
        let calls = std::rc::Rc::new(std::cell::Cell::new(0));
        let c = calls.clone();
        COPY_HOOK.with(|h| {
            *h.borrow_mut() = Some(Box::new(move || {
                c.set(c.get() + 1);
                w.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(())).unwrap();
                w.execute("INSERT INTO t VALUES (99)", []).unwrap();
            }))
        });
        let before = copies();
        let e = copy_snapshot(&db, &wal).unwrap_err();
        COPY_HOOK.with(|h| h.borrow_mut().take());
        assert_eq!(calls.get(), 3);
        assert!(matches!(e, MetaError::Unstable { attempts: 3, .. }), "{e:?}");
        assert!(e.to_string().contains("forge sql --server"), "{e}");
        assert!(copies().is_subset(&before), "temp dir left behind");
    }

    fn copies() -> std::collections::BTreeSet<std::ffi::OsString> {
        std::fs::read_dir(std::env::temp_dir())
            .unwrap()
            .flatten()
            .map(|e| e.file_name())
            .filter(|n| n.to_string_lossy().starts_with(COPY_PREFIX))
            .collect()
    }

    /// A failed copy names its source and destination.
    #[test]
    fn copy_failure_names_both_paths() {
        let d = tempfile::tempdir().unwrap();
        let db = d.path().join("missing.db");
        let e = copy_snapshot(&db, &sibling(&db, "-wal")).unwrap_err();
        let MetaError::Copy { ref from, ref to, .. } = e else {
            panic!("{e:?}")
        };
        assert_eq!(from, &db);
        assert!(
            to.starts_with(std::env::temp_dir()) && to.ends_with("missing.db"),
            "{to:?}"
        );
        assert!(e.to_string().contains(&*db.to_string_lossy()), "{e}");
    }

    /// A copy dir older than an hour (left by a killed process) is swept by the next copy; a recent one is kept.
    #[test]
    fn copy_sweeps_stale_copies() {
        let tmp = std::env::temp_dir();
        let (stale, fresh) = (
            tmp.join(format!("{COPY_PREFIX}stale-{}", probe_nonce())),
            tmp.join(format!("{COPY_PREFIX}fresh-{}", probe_nonce())),
        );
        for p in [&stale, &fresh] {
            std::fs::create_dir(p).unwrap();
            std::fs::write(p.join("meta.db"), b"x").unwrap();
        }
        let old = std::time::SystemTime::now() - 2 * STALE_COPY;
        std::fs::File::open(&stale).unwrap().set_modified(old).unwrap();
        let d = tempfile::tempdir().unwrap();
        let (db, wal, _w) = wal_db(d.path());
        let _copy = copy_snapshot(&db, &wal).unwrap();
        assert!(!stale.exists());
        assert!(fresh.exists());
        std::fs::remove_dir_all(&fresh).unwrap();
    }
}
