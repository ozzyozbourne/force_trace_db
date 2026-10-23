//! SQLite metastore: one connection behind a mutex, used from `spawn_blocking` (master decision 10).

use crate::{
    FlushCommit, MetaError, MetaResult, Metastore, NewSegment, Project, SegmentFilter, WalCheckpoint, migrations,
};
use async_trait::async_trait;
use forge_model::catalog::{HourBucket, Level, SegmentId, SegmentMeta, SegmentState, TraceLocation};
use forge_model::ids::{ProjectId, TraceId};
use forge_model::time::now_ms;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use std::path::Path;
use std::sync::{Arc, Mutex};

fn set_pragmas(c: &Connection) -> rusqlite::Result<()> {
    c.pragma_update(None, "busy_timeout", 5000)?;
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
}

const PROJECT_COLS: &str = "id, name, api_key_hash, retention_days, created_at_ms";
const SEGMENT_COLS: &str = "id, project_id, level, partition_hour, path, row_count, byte_size, fts_byte_size, \
    min_start_ns, max_start_ns, start_time_nulls, min_trace_id, max_trace_id, min_version, max_version, \
    schema_version, created_at_ms, state, deleted_at_ms";
const SEGMENT_NCOLS: usize = 19;

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

fn corrupt(msg: impl std::fmt::Display) -> MetaError {
    MetaError::Corrupt {
        table: "segments",
        msg: msg.to_string(),
    }
}
fn to_u64(v: i64) -> MetaResult<u64> {
    u64::try_from(v).map_err(|_| corrupt(format!("negative value {v}")))
}
fn to_i64(v: u64) -> MetaResult<i64> {
    i64::try_from(v).map_err(|_| MetaError::Conflict(format!("value {v} exceeds i64")))
}

fn segment_from_row(r: &Row<'_>) -> MetaResult<SegmentMeta> {
    let id: String = r.get(0)?;
    let project: String = r.get(1)?;
    let level: u8 = r.get(2)?;
    let min_tid: Vec<u8> = r.get(11)?;
    let max_tid: Vec<u8> = r.get(12)?;
    let state: String = r.get(17)?;
    Ok(SegmentMeta {
        id: id.parse().map_err(corrupt)?,
        project_id: ProjectId::new(project).map_err(corrupt)?,
        level: Level::from_u8(level).ok_or_else(|| corrupt(format!("level {level}")))?,
        partition_hour: HourBucket(r.get(3)?),
        path: r.get(4)?,
        row_count: to_u64(r.get(5)?)?,
        byte_size: to_u64(r.get(6)?)?,
        fts_byte_size: r.get::<_, Option<i64>>(7)?.map(to_u64).transpose()?,
        min_start_ns: r.get(8)?,
        max_start_ns: r.get(9)?,
        start_time_nulls: to_u64(r.get(10)?)?,
        min_trace_id: TraceId::from_slice(&min_tid).map_err(corrupt)?,
        max_trace_id: TraceId::from_slice(&max_tid).map_err(corrupt)?,
        min_version: to_u64(r.get(13)?)?,
        max_version: to_u64(r.get(14)?)?,
        schema_version: r.get(15)?,
        created_at_ms: r.get(16)?,
        state: SegmentState::parse(&state).ok_or_else(|| corrupt(format!("state {state}")))?,
        deleted_at_ms: r.get(18)?,
    })
}

fn project_from_row(r: &Row<'_>) -> MetaResult<Project> {
    let id: String = r.get(0)?;
    Ok(Project {
        id: ProjectId::new(id).map_err(|e| MetaError::Corrupt {
            table: "projects",
            msg: e.to_string(),
        })?,
        name: r.get(1)?,
        api_key_hash: r.get(2)?,
        retention_days: r.get(3)?,
        created_at_ms: r.get(4)?,
    })
}

fn insert_segment(tx: &Transaction<'_>, s: &NewSegment, now: i64) -> MetaResult<()> {
    let m = &s.meta;
    let id = m.id.to_string();
    let p = Project::new(m.project_id.clone(), None, None, now)?;
    tx.prepare_cached(&format!(
        "INSERT INTO projects ({PROJECT_COLS}) VALUES (?1,?2,?3,?4,?5) ON CONFLICT(id) DO NOTHING"
    ))?
    .execute(params![
        p.id.as_str(),
        p.name,
        p.api_key_hash,
        p.retention_days,
        p.created_at_ms
    ])?;
    tx.prepare_cached(&format!(
        "INSERT INTO segments ({SEGMENT_COLS}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)"
    ))?
    .execute(params![
        id,
        m.project_id.as_str(),
        m.level.as_u8(),
        m.partition_hour.0,
        m.path,
        to_i64(m.row_count)?,
        to_i64(m.byte_size)?,
        m.fts_byte_size.map(to_i64).transpose()?,
        m.min_start_ns,
        m.max_start_ns,
        to_i64(m.start_time_nulls)?,
        m.min_trace_id.as_bytes(),
        m.max_trace_id.as_bytes(),
        to_i64(m.min_version)?,
        to_i64(m.max_version)?,
        m.schema_version,
        m.created_at_ms,
        SegmentState::Live.as_str(),
        Option::<i64>::None,
    ])
    .map_err(|e| map_err(e, &format!("segment {id} ({})", m.path)))?;
    let mut st = tx.prepare_cached(
        "INSERT INTO trace_index (project_id, trace_id, segment_id, row_start, row_count) VALUES (?1,?2,?3,?4,?5)",
    )?;
    for t in &s.traces {
        st.execute(params![
            m.project_id.as_str(),
            t.trace_id.as_bytes(),
            id,
            to_i64(t.row_start)?,
            to_i64(t.row_count)?
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

    pub fn open_in_memory() -> MetaResult<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(mut c: Connection) -> MetaResult<Self> {
        set_pragmas(&c)?;
        migrations::migrate(&mut c)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(c)),
        })
    }

    /// Test hook: the value of `PRAGMA <name>`.
    #[cfg(any(test, feature = "test-util"))]
    pub fn pragma(&self, name: &str) -> MetaResult<String> {
        use rusqlite::types::Value;
        let c = self.conn.lock().unwrap_or_else(|e| e.into_inner());
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
        tokio::task::spawn_blocking(move || f(&conn.lock().unwrap_or_else(|e| e.into_inner()))).await?
    }

    async fn write<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Transaction<'_>) -> MetaResult<T> + Send + 'static,
    ) -> MetaResult<T> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut c = conn.lock().unwrap_or_else(|e| e.into_inner());
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
        self.read(|c| {
            Ok(
                c.query_row("SELECT COALESCE(MAX(version), 0) FROM schema_versions", [], |r| {
                    r.get(0)
                })?,
            )
        })
        .await
    }

    async fn create_project(&self, p: Project) -> MetaResult<()> {
        self.write(move |tx| {
            tx.execute(
                &format!("INSERT INTO projects ({PROJECT_COLS}) VALUES (?1,?2,?3,?4,?5)"),
                params![p.id.as_str(), p.name, p.api_key_hash, p.retention_days, p.created_at_ms],
            )
            .map_err(|e| map_err(e, &format!("project {}", p.id)))?;
            Ok(())
        })
        .await
    }

    async fn get_project(&self, id: &ProjectId) -> MetaResult<Option<Project>> {
        let id = id.clone();
        self.read(move |c| {
            c.query_row(
                &format!("SELECT {PROJECT_COLS} FROM projects WHERE id = ?1"),
                [id.as_str()],
                |r| Ok(project_from_row(r)),
            )
            .optional()?
            .transpose()
        })
        .await
    }

    async fn list_projects(&self) -> MetaResult<Vec<Project>> {
        self.read(|c| {
            let mut st = c.prepare_cached(&format!("SELECT {PROJECT_COLS} FROM projects ORDER BY id"))?;
            let rows = st.query_map([], |r| Ok(project_from_row(r)))?;
            rows.map(|r| r?).collect()
        })
        .await
    }

    async fn commit_flush(&self, c: FlushCommit) -> MetaResult<()> {
        let (seq, max_version) = (to_i64(c.flushed_wal_seq)?, to_i64(c.max_version)?);
        self.write(move |tx| {
            let now = now_ms();
            for s in &c.segments {
                insert_segment(tx, s, now)?;
            }
            tx.execute(
                "INSERT INTO wal_checkpoints (node_id, flushed_seq, max_version, updated_at_ms) VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(node_id) DO UPDATE SET flushed_seq = MAX(flushed_seq, excluded.flushed_seq), \
                 max_version = MAX(max_version, excluded.max_version), updated_at_ms = excluded.updated_at_ms",
                params![c.node_id, seq, max_version, now],
            )?;
            Ok(())
        })
        .await
    }

    async fn wal_checkpoint(&self, node_id: &str) -> MetaResult<Option<WalCheckpoint>> {
        let node = node_id.to_string();
        self.read(move |c| {
            let r: Option<(i64, i64)> = c
                .query_row(
                    "SELECT flushed_seq, max_version FROM wal_checkpoints WHERE node_id = ?1",
                    [node],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            r.map(|(s, v)| {
                Ok(WalCheckpoint {
                    flushed_seq: to_u64(s)?,
                    max_version: to_u64(v)?,
                })
            })
            .transpose()
        })
        .await
    }

    async fn max_version(&self) -> MetaResult<u64> {
        self.read(|c| {
            let v: i64 = c.query_row(
                "SELECT MAX(COALESCE((SELECT MAX(max_version) FROM segments), 0), \
                            COALESCE((SELECT MAX(max_version) FROM wal_checkpoints), 0))",
                [],
                |r| r.get(0),
            )?;
            to_u64(v)
        })
        .await
    }

    async fn get_segment(&self, id: SegmentId) -> MetaResult<Option<SegmentMeta>> {
        self.read(move |c| {
            c.query_row(
                &format!("SELECT {SEGMENT_COLS} FROM segments WHERE id = ?1"),
                [id.to_string()],
                |r| Ok(segment_from_row(r)),
            )
            .optional()?
            .transpose()
        })
        .await
    }

    async fn list_segments(&self, f: &SegmentFilter) -> MetaResult<Vec<SegmentMeta>> {
        let f = f.clone();
        self.read(move |c| {
            let order = if f.newest_first {
                "max_start_ns DESC NULLS FIRST, id DESC"
            } else {
                "id"
            };
            let mut st = c.prepare_cached(&format!(
                "SELECT {SEGMENT_COLS} FROM segments \
                 WHERE (?1 IS NULL OR project_id = ?1) AND (state <> 'deleted' OR ?2) ORDER BY {order}"
            ))?;
            let rows = st.query_map(
                params![f.project_id.as_ref().map(ProjectId::as_str), f.include_deleted],
                |r| Ok(segment_from_row(r)),
            )?;
            let mut out = Vec::new();
            for r in rows {
                let s = r??;
                if f.levels.as_ref().is_none_or(|l| l.contains(&s.level)) && s.may_overlap_time(f.start_ns, f.end_ns) {
                    out.push(s);
                    if f.limit.is_some_and(|l| out.len() >= l) {
                        break;
                    }
                }
            }
            Ok(out)
        })
        .await
    }

    async fn locate_trace(&self, project: &ProjectId, trace: TraceId) -> MetaResult<Vec<TraceLocation>> {
        let project = project.clone();
        self.read(move |c| {
            let cols = SEGMENT_COLS
                .split(", ")
                .map(|c| format!("s.{c}"))
                .collect::<Vec<_>>()
                .join(", ");
            let mut st = c.prepare_cached(&format!(
                "SELECT {cols}, ti.row_start, ti.row_count FROM trace_index ti JOIN segments s ON s.id = ti.segment_id \
                 WHERE ti.project_id = ?1 AND ti.trace_id = ?2 AND s.state <> 'deleted' ORDER BY s.id"
            ))?;
            let rows = st.query_map(params![project.as_str(), trace.as_bytes()], |r| {
                Ok((
                    segment_from_row(r),
                    r.get::<_, i64>(SEGMENT_NCOLS)?,
                    r.get::<_, i64>(SEGMENT_NCOLS + 1)?,
                ))
            })?;
            let mut out = Vec::new();
            for r in rows {
                let (seg, start, count) = r?;
                out.push(TraceLocation {
                    segment: seg?,
                    row_start: to_u64(start)?,
                    row_count: to_u64(count)?,
                });
            }
            Ok(out)
        })
        .await
    }
}
