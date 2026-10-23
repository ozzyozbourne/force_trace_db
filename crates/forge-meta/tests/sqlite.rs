use forge_meta::migrations::{MIGRATIONS, migrate_with};
use forge_meta::*;
use forge_model::catalog::*;
use forge_model::config::MetastoreConfig;
use forge_model::ids::TraceId;
use forge_model::testing::{pid, segment_meta};
use rusqlite::Connection;
use std::path::Path;

fn proj(id: &str, name: &str) -> Project {
    Project {
        id: pid(id),
        name: name.into(),
        api_key_hash: None,
        retention_days: None,
        created_at_ms: 1,
    }
}

/// A segment of project `p` with start times [min, max] (None = all null) and traces 1 and 2.
fn seg(p: &str, level: Level, start: Option<(i64, i64)>, nulls: u64, max_version: u64) -> NewSegment {
    let (t1, t2) = (TraceId([1; 16]), TraceId([2; 16]));
    NewSegment {
        meta: SegmentMeta {
            row_count: 20,
            byte_size: 1000,
            fts_byte_size: Some(10),
            min_start_ns: start.map(|s| s.0),
            max_start_ns: start.map(|s| s.1),
            start_time_nulls: nulls,
            min_trace_id: t1,
            max_trace_id: t2,
            max_version,
            created_at_ms: 5,
            ..segment_meta(p, level, HourBucket(100))
        },
        traces: vec![
            TraceRange {
                trace_id: t1,
                row_start: 0,
                row_count: 10,
            },
            TraceRange {
                trace_id: t2,
                row_start: 10,
                row_count: 10,
            },
        ],
    }
}

fn flush(segments: Vec<NewSegment>, seq: u64, max_version: u64) -> FlushCommit {
    FlushCommit {
        node_id: "n".into(),
        segments,
        flushed_wal_seq: seq,
        max_version,
    }
}

fn db(dir: &Path) -> SqliteMetastore {
    SqliteMetastore::open_path(&dir.join("meta.db")).unwrap()
}

fn has_table(c: &Connection, name: &str) -> bool {
    c.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [name],
        |r| r.get::<_, i64>(0),
    )
    .unwrap()
        == 1
}

fn version_rows(c: &Connection) -> i64 {
    c.query_row("SELECT count(*) FROM schema_versions", [], |r| r.get(0))
        .unwrap()
}

#[tokio::test]
async fn migrations_apply_once() {
    let dir = tempfile::tempdir().unwrap();
    let s = db(dir.path());
    assert_eq!(s.schema_version().await.unwrap(), 1);
    assert_eq!(s.pragma("user_version").unwrap(), "1");
    assert_eq!(s.pragma("journal_mode").unwrap(), "wal");
    assert_eq!(s.pragma("foreign_keys").unwrap(), "1");
    drop(s);
    let s = db(dir.path());
    assert_eq!(s.schema_version().await.unwrap(), 1);
    drop(s);
    assert_eq!(version_rows(&Connection::open(dir.path().join("meta.db")).unwrap()), 1);
}

#[test]
fn failed_migration_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = Connection::open(dir.path().join("m.db")).unwrap();
    let broken = [MIGRATIONS[0], (2, "broken", "CREATE TABLE t2(x); SELEC broken")];
    let e = migrate_with(&mut c, &broken).unwrap_err();
    assert!(matches!(e, MetaError::Migration { version: 2, .. }), "{e:?}");
    assert_eq!(migrate_with(&mut c, MIGRATIONS).unwrap(), 1);
    assert!(!has_table(&c, "t2"));
    assert_eq!(version_rows(&c), 1);
}

#[test]
fn newer_database_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = Connection::open(dir.path().join("m.db")).unwrap();
    assert_eq!(
        migrate_with(&mut c, &[MIGRATIONS[0], (2, "t2", "CREATE TABLE t2(x)")]).unwrap(),
        2
    );
    let e = migrate_with(&mut c, MIGRATIONS).unwrap_err();
    assert!(matches!(e, MetaError::SchemaTooNew { db: 2, binary: 1 }), "{e:?}");
}

#[tokio::test]
async fn concurrent_first_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta.db");
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let p = path.clone();
            std::thread::spawn(move || SqliteMetastore::open_path(&p).map(drop))
        })
        .collect();
    for t in threads {
        t.join().unwrap().unwrap();
    }
    assert_eq!(db(dir.path()).schema_version().await.unwrap(), 1);
    assert_eq!(version_rows(&Connection::open(&path).unwrap()), 1);
}

#[tokio::test]
async fn projects() {
    let s = SqliteMetastore::open_in_memory().unwrap();
    let a = Project {
        api_key_hash: Some([7; 32]),
        retention_days: Some(30),
        ..proj("a", "Alpha")
    };
    s.create_project(a.clone()).await.unwrap();
    assert!(matches!(
        s.create_project(proj("a", "other")).await,
        Err(MetaError::Conflict(_))
    ));
    s.create_project(proj("b", "Alpha")).await.unwrap();
    let bad = Project {
        retention_days: Some(0),
        ..proj("c", "c")
    };
    assert!(matches!(s.create_project(bad).await, Err(MetaError::Sqlite(_))));
    assert_eq!(s.get_project(&pid("a")).await.unwrap(), Some(a.clone()));
    assert_eq!(s.get_project(&pid("zz")).await.unwrap(), None);
    let all = s.list_projects().await.unwrap();
    assert_eq!(all.iter().map(|p| p.id.as_str()).collect::<Vec<_>>(), ["a", "b"]);
}

#[tokio::test]
async fn commit_flush_round_trip() {
    let s = SqliteMetastore::open_in_memory().unwrap();
    let a = seg("p", Level::L0, Some((10, 20)), 0, 7);
    let mut b = seg("q", Level::L0, Some((30, 40)), 0, 9);
    b.meta.row_count = 25;
    b.traces.push(TraceRange {
        trace_id: TraceId([3; 16]),
        row_start: 20,
        row_count: 5,
    });
    s.commit_flush(flush(vec![a.clone(), b.clone()], 50, 8)).await.unwrap();

    let projects: Vec<_> = s
        .list_projects()
        .await
        .unwrap()
        .into_iter()
        .map(|p| (p.id, p.name))
        .collect();
    assert_eq!(projects, [(pid("p"), "p".to_string()), (pid("q"), "q".to_string())]);
    assert_eq!(s.get_segment(a.meta.id).await.unwrap(), Some(a.meta.clone()));
    assert_eq!(s.get_segment(b.meta.id).await.unwrap(), Some(b.meta.clone()));

    let loc = s.locate_trace(&pid("p"), TraceId([2; 16])).await.unwrap();
    assert_eq!(
        loc,
        [TraceLocation {
            segment: a.meta.clone(),
            row_start: 10,
            row_count: 10
        }]
    );
    let loc = s.locate_trace(&pid("q"), TraceId([3; 16])).await.unwrap();
    assert_eq!(loc.len(), 1);
    assert_eq!(loc[0].rows(), 20..25);
    assert!(s.locate_trace(&pid("p"), TraceId([3; 16])).await.unwrap().is_empty());

    // Two segments of one project holding the same trace come back ordered by segment id.
    let c = seg("p", Level::L0, Some((50, 60)), 0, 3);
    s.commit_flush(flush(vec![c.clone()], 60, 3)).await.unwrap();
    let ids: Vec<_> = s
        .locate_trace(&pid("p"), TraceId([1; 16]))
        .await
        .unwrap()
        .iter()
        .map(|l| l.segment.id)
        .collect();
    let mut want = vec![a.meta.id, c.meta.id];
    want.sort();
    assert_eq!(ids, want);

    assert_eq!(
        s.wal_checkpoint("n").await.unwrap(),
        Some(WalCheckpoint {
            flushed_seq: 60,
            max_version: 8
        })
    );
    assert_eq!(s.wal_checkpoint("other").await.unwrap(), None);
    assert_eq!(
        s.max_version().await.unwrap(),
        9,
        "segments' max wins over the checkpoint's 8"
    );
    s.commit_flush(flush(vec![], 10, 12)).await.unwrap();
    assert_eq!(
        s.wal_checkpoint("n").await.unwrap(),
        Some(WalCheckpoint {
            flushed_seq: 60,
            max_version: 12
        })
    );
    assert_eq!(s.max_version().await.unwrap(), 12);
}

#[tokio::test]
async fn auto_create_ignores_names() {
    let s = SqliteMetastore::open_in_memory().unwrap();
    assert_eq!(s.max_version().await.unwrap(), 0);
    s.create_project(proj("a", "b")).await.unwrap();
    s.commit_flush(flush(vec![seg("b", Level::L0, None, 20, 1)], 1, 1))
        .await
        .unwrap();
    assert_eq!(s.get_project(&pid("b")).await.unwrap().unwrap().name, "b");
    assert_eq!(s.get_project(&pid("a")).await.unwrap().unwrap().name, "b");
}

#[tokio::test]
async fn failed_commit_changes_nothing() {
    let s = SqliteMetastore::open_in_memory().unwrap();
    let first = seg("p", Level::L0, Some((1, 2)), 0, 1);
    s.commit_flush(flush(vec![first.clone()], 5, 1)).await.unwrap();

    let s1 = seg("new", Level::L0, Some((1, 2)), 0, 2);
    let mut s2 = seg("new", Level::L0, Some((1, 2)), 0, 2);
    s2.meta.path = s1.meta.path.clone();
    let e = s.commit_flush(flush(vec![s1.clone(), s2], 9, 9)).await.unwrap_err();
    assert!(matches!(e, MetaError::Conflict(_)), "{e:?}");

    assert_eq!(s.list_projects().await.unwrap().len(), 1, "project 'new' rolled back");
    assert_eq!(s.get_segment(s1.meta.id).await.unwrap(), None);
    assert!(s.locate_trace(&pid("new"), TraceId([1; 16])).await.unwrap().is_empty());
    assert_eq!(
        s.wal_checkpoint("n").await.unwrap(),
        Some(WalCheckpoint {
            flushed_seq: 5,
            max_version: 1
        })
    );
    assert_eq!(s.get_segment(first.meta.id).await.unwrap(), Some(first.meta));
}

#[tokio::test]
async fn u64_overflow_is_an_error() {
    let s = SqliteMetastore::open_in_memory().unwrap();
    let e = s.commit_flush(flush(vec![], u64::MAX, 1)).await.unwrap_err();
    assert!(
        matches!(e, MetaError::InvalidInput(ref m) if m.contains("flushed_wal_seq")),
        "{e:?}"
    );
    let mut big = seg("p", Level::L0, None, 20, 1);
    big.meta.max_version = u64::MAX;
    let e = s.commit_flush(flush(vec![big], 1, 1)).await.unwrap_err();
    assert!(
        matches!(e, MetaError::InvalidInput(ref m) if m.contains("max_version")),
        "{e:?}"
    );
    assert_eq!(s.wal_checkpoint("n").await.unwrap(), None);
}

#[tokio::test]
async fn list_segments_filters_and_orders() {
    let s = SqliteMetastore::open_in_memory().unwrap();
    let early = seg("p", Level::L0, Some((100, 200)), 0, 1);
    let late = seg("p", Level::L1, Some((300, 400)), 0, 1);
    let nulls = seg("p", Level::L0, Some((900, 950)), 3, 1);
    let all_null = seg("p", Level::L0, None, 20, 1);
    let other = seg("o", Level::L0, Some((100, 200)), 0, 1);
    let segs = [&early, &late, &nulls, &all_null, &other];
    s.commit_flush(flush(segs.iter().map(|s| (*s).clone()).collect(), 1, 1))
        .await
        .unwrap();
    let ids = |v: Vec<SegmentMeta>| v.into_iter().map(|m| m.id).collect::<Vec<_>>();
    let p = |f: SegmentFilter| SegmentFilter {
        project_id: Some(pid("p")),
        ..f
    };

    let mut by_id = vec![early.meta.id, late.meta.id, nulls.meta.id, all_null.meta.id];
    by_id.sort();
    assert_eq!(ids(s.list_segments(&p(SegmentFilter::default())).await.unwrap()), by_id);
    assert_eq!(s.list_segments(&SegmentFilter::default()).await.unwrap().len(), 5);

    let l1 = p(SegmentFilter {
        levels: Some(vec![Level::L1]),
        ..Default::default()
    });
    assert_eq!(ids(s.list_segments(&l1).await.unwrap()), [late.meta.id]);

    let newest = p(SegmentFilter {
        newest_first: true,
        ..Default::default()
    });
    assert_eq!(
        ids(s.list_segments(&newest).await.unwrap()),
        [all_null.meta.id, nulls.meta.id, late.meta.id, early.meta.id]
    );

    // [200, 300): early has max == start (inside), late has min == end (outside); nulls always included.
    let window = p(SegmentFilter {
        start_ns: Some(200),
        end_ns: Some(300),
        newest_first: true,
        ..Default::default()
    });
    assert_eq!(
        ids(s.list_segments(&window).await.unwrap()),
        [all_null.meta.id, nulls.meta.id, early.meta.id]
    );
    let disjoint = p(SegmentFilter {
        start_ns: Some(500),
        end_ns: Some(600),
        ..Default::default()
    });
    let mut want = vec![nulls.meta.id, all_null.meta.id];
    want.sort();
    assert_eq!(ids(s.list_segments(&disjoint).await.unwrap()), want);

    // limit applies after the Rust-side filters: the L1 segment comes first by time but is filtered out.
    let limited = p(SegmentFilter {
        levels: Some(vec![Level::L0]),
        start_ns: Some(0),
        end_ns: Some(1000),
        newest_first: true,
        limit: Some(2),
        ..Default::default()
    });
    let got = s.list_segments(&limited).await.unwrap();
    assert_eq!(ids(got.clone()), [all_null.meta.id, nulls.meta.id]);
    assert!(
        got.iter()
            .all(|m| m.level == Level::L0 && m.may_overlap_time(Some(0), Some(1000)))
    );
    let limited = SegmentFilter {
        start_ns: Some(150),
        end_ns: Some(250),
        limit: Some(1),
        ..limited
    };
    let got = s.list_segments(&limited).await.unwrap();
    assert_eq!(got.len(), 1);
    assert!(got[0].may_overlap_time(Some(150), Some(250)));
}

#[tokio::test]
async fn urls() {
    let m = open(&MetastoreConfig {
        url: "sqlite::memory:".into(),
    })
    .await
    .unwrap();
    assert_eq!(m.schema_version().await.unwrap(), 1);
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}/sub/meta.db", dir.path().display());
    assert_eq!(
        open(&MetastoreConfig { url })
            .await
            .unwrap()
            .schema_version()
            .await
            .unwrap(),
        1
    );
    assert!(dir.path().join("sub/meta.db").is_file());
    assert!(matches!(
        open(&MetastoreConfig {
            url: "postgres://x".into()
        })
        .await,
        Err(MetaError::Config { ref msg, .. }) if msg == "postgres metastore is not supported yet"
    ));
    assert!(matches!(
        open(&MetastoreConfig { url: "bogus://".into() }).await,
        Err(MetaError::Config { .. })
    ));
}

#[tokio::test]
async fn mkdir_failure_names_the_path() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("file"), b"x").unwrap();
    let url = format!("sqlite://{}/file/x/meta.db", dir.path().display());
    let e = open(&MetastoreConfig { url }).await.err().unwrap();
    assert!(matches!(e, MetaError::CreateDir { .. }), "{e:?}");
    let msg = e.to_string();
    assert!(msg.starts_with("cannot create ") && msg.contains("file/x"), "{msg}");
}

#[test]
fn busy_and_locked_are_transient() {
    use forge_model::error::{Classify, ErrorKind};
    use rusqlite::ffi;
    for code in [ffi::SQLITE_BUSY, ffi::SQLITE_LOCKED] {
        let e = MetaError::Sqlite(rusqlite::Error::SqliteFailure(ffi::Error::new(code), None));
        assert_eq!(e.kind(), ErrorKind::Transient, "{e:?}");
    }
    let e = MetaError::Sqlite(rusqlite::Error::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_CORRUPT),
        None,
    ));
    assert_eq!(e.kind(), ErrorKind::Internal);
}

#[test]
fn upgrade_from_v1_applies_only_v2() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = Connection::open(dir.path().join("m.db")).unwrap();
    assert_eq!(migrate_with(&mut c, MIGRATIONS).unwrap(), 1);
    assert_eq!(
        migrate_with(&mut c, &[MIGRATIONS[0], (2, "t2", "CREATE TABLE t2(x)")]).unwrap(),
        2
    );
    assert_eq!(version_rows(&c), 2);
    assert!(has_table(&c, "t2"));
}

#[tokio::test]
async fn retried_commit_is_idempotent() {
    let s = SqliteMetastore::open_in_memory().unwrap();
    let a = seg("p", Level::L0, Some((1, 2)), 0, 3);
    s.commit_flush(flush(vec![a.clone()], 5, 3)).await.unwrap();
    s.commit_flush(flush(vec![a.clone()], 5, 3)).await.unwrap();
    assert_eq!(
        s.list_segments(&SegmentFilter::default()).await.unwrap(),
        std::slice::from_ref(&a.meta)
    );
    assert_eq!(s.locate_trace(&pid("p"), TraceId([1; 16])).await.unwrap().len(), 1);
    assert_eq!(
        s.wal_checkpoint("n").await.unwrap(),
        Some(WalCheckpoint {
            flushed_seq: 5,
            max_version: 3
        })
    );
    // Same id, different path: a real conflict.
    let mut other = a.clone();
    other.meta.path.push_str(".other");
    let e = s.commit_flush(flush(vec![other], 6, 3)).await.unwrap_err();
    assert!(matches!(e, MetaError::Conflict(_)), "{e:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_retried_after_cancellation_succeeds() {
    let s = SqliteMetastore::open_in_memory().unwrap();
    let a = seg("p", Level::L0, Some((1, 2)), 0, 3);
    // Poll the commit once (it starts on the blocking pool), then drop it: the txn may still land.
    tokio::select! {
        biased;
        _ = s.commit_flush(flush(vec![a.clone()], 5, 3)) => {}
        _ = std::future::ready(()) => {}
    }
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    s.commit_flush(flush(vec![a.clone()], 5, 3)).await.unwrap();
    assert_eq!(s.get_segment(a.meta.id).await.unwrap(), Some(a.meta));
}

#[tokio::test]
async fn bad_trace_ranges_are_invalid_input() {
    let s = SqliteMetastore::open_in_memory().unwrap();
    let mut past_end = seg("p", Level::L0, None, 20, 1);
    past_end.traces[1].row_start = 15; // 15..25 of 20 rows
    let mut overlap = seg("p", Level::L0, None, 20, 1);
    overlap.traces[1].row_start = 5; // 0..10 and 5..15
    let mut overflow = seg("p", Level::L0, None, 20, 1);
    overflow.traces[1].row_start = u64::MAX;
    for bad in [past_end, overlap, overflow] {
        let e = s.commit_flush(flush(vec![bad], 1, 1)).await.unwrap_err();
        assert!(matches!(e, MetaError::InvalidInput(_)), "{e:?}");
    }
    assert!(s.list_segments(&SegmentFilter::default()).await.unwrap().is_empty());
}

#[tokio::test]
async fn limit_is_applied_after_the_rust_filters() {
    let s = SqliteMetastore::open_in_memory().unwrap();
    // Newest-first puts the all-null L1 segment first in SQL order; the levels filter drops it.
    let l1_null = seg("p", Level::L1, None, 20, 1);
    let early = seg("p", Level::L0, Some((100, 200)), 0, 1);
    s.commit_flush(flush(vec![l1_null, early.clone()], 1, 1)).await.unwrap();
    let f = SegmentFilter {
        levels: Some(vec![Level::L0]),
        newest_first: true,
        limit: Some(1),
        ..Default::default()
    };
    assert_eq!(s.list_segments(&f).await.unwrap(), [early.meta]);
    let none = SegmentFilter {
        limit: Some(0),
        ..Default::default()
    };
    assert!(s.list_segments(&none).await.unwrap().is_empty());
}
