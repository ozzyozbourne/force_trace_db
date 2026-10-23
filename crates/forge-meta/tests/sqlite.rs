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
/// `a` is the only visible segment and trace [1; 16] has one location.
async fn assert_only(s: &SqliteMetastore, a: &NewSegment) {
    assert_eq!(
        s.list_segments(&SegmentFilter::default()).await.unwrap(),
        std::slice::from_ref(&a.meta)
    );
    assert_eq!(s.locate_trace(&pid("p"), TraceId([1; 16])).await.unwrap().len(), 1);
}

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
    assert_only(&s, &a).await;
    assert_eq!(
        s.wal_checkpoint("n").await.unwrap(),
        Some(WalCheckpoint {
            flushed_seq: 5,
            max_version: 3
        })
    );
    // Same id with a different path or different stored metadata: a real conflict, and nothing changes.
    let mut path = a.clone();
    path.meta.path.push_str(".other");
    let mut rows = a.clone();
    rows.meta.row_count = 40;
    rows.traces[1].row_count = 30;
    let mut bytes = a.clone();
    bytes.meta.byte_size += 1;
    let mut level = a.clone();
    level.meta.level = Level::L1;
    let mut hour = a.clone();
    hour.meta.partition_hour = HourBucket(101);
    for other in [path, rows, bytes, level, hour] {
        let e = s.commit_flush(flush(vec![other], 6, 3)).await.unwrap_err();
        assert!(matches!(e, MetaError::Conflict(_)), "{e:?}");
    }
    assert_eq!(s.get_segment(a.meta.id).await.unwrap(), Some(a.meta.clone()));
    assert_eq!(s.wal_checkpoint("n").await.unwrap().unwrap().flushed_seq, 5);

    // The same segment twice in one commit is a flusher bug, not a retry.
    let b = seg("p", Level::L0, Some((1, 2)), 0, 3);
    let e = s
        .commit_flush(flush(vec![b.clone(), b.clone()], 7, 3))
        .await
        .unwrap_err();
    assert!(
        matches!(e, MetaError::InvalidInput(ref m) if m.contains("listed twice")),
        "{e:?}"
    );
    assert_eq!(s.get_segment(b.meta.id).await.unwrap(), None);
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
    let mut gap = seg("p", Level::L0, None, 20, 1);
    (gap.traces[1].row_start, gap.traces[1].row_count) = (12, 8); // rows 10..12 in no trace
    let mut short = seg("p", Level::L0, None, 20, 1);
    short.traces[1].row_count = 5; // rows 15..20 in no trace
    let mut late_start = seg("p", Level::L0, None, 20, 1);
    late_start.traces.remove(0); // rows 0..10 in no trace
    for bad in [past_end, overlap, overflow, gap, short, late_start] {
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

#[tokio::test(flavor = "multi_thread")]
async fn stale_watermark_commit_inserts_nothing() {
    let s = SqliteMetastore::open_in_memory().unwrap();
    let a = seg("p", Level::L0, Some((1, 2)), 0, 3);
    s.commit_flush(flush(vec![a.clone()], 5, 3)).await.unwrap();
    // New segments at a watermark that does not advance: Ok, but invisible (a re-flush of committed rows).
    for (b, seq) in [
        (seg("p", Level::L0, Some((1, 2)), 0, 3), 5),
        (seg("p", Level::L0, Some((1, 2)), 0, 3), 4),
    ] {
        s.commit_flush(flush(vec![b.clone()], seq, 3)).await.unwrap();
        assert_eq!(s.get_segment(b.meta.id).await.unwrap(), None, "seq {seq}");
    }
    assert_only(&s, &a).await;
    assert_eq!(s.wal_checkpoint("n").await.unwrap().unwrap().flushed_seq, 5);
    // The checkpoint upsert still runs: max_version rises, flushed_seq never moves back.
    s.commit_flush(flush(vec![], 3, 99)).await.unwrap();
    assert_eq!(
        s.wal_checkpoint("n").await.unwrap(),
        Some(WalCheckpoint {
            flushed_seq: 5,
            max_version: 99
        })
    );
    assert_eq!(s.max_version().await.unwrap(), 99);
}

fn files(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    v.sort();
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn read_only_open() {
    let d = tempfile::tempdir().unwrap();
    let url = |p: &Path| MetastoreConfig {
        url: format!("sqlite://{}", p.display()),
    };
    // A missing file: Config, and nothing is created.
    let missing = d.path().join("nope.db");
    let e = open_read_only(&url(&missing)).await.unwrap_err();
    assert!(matches!(e, MetaError::Config { .. }), "{e}");
    assert!(!missing.exists());
    // Committed data is readable; writes fail (Internal).
    let path = d.path().join("meta.db");
    let w = db(d.path());
    w.commit_flush(flush(vec![seg("p", Level::L0, Some((1, 2)), 0, 3)], 1, 3))
        .await
        .unwrap();
    let r = open_read_only(&url(&path)).await.unwrap();
    assert_eq!(r.list_segments(&SegmentFilter::default()).await.unwrap().len(), 1);
    let e = r.create_project(proj("x", "x")).await.unwrap_err();
    assert!(e.to_string().contains("readonly"), "{e}");
    assert_eq!(
        forge_model::error::Classify::kind(&e),
        forge_model::error::ErrorKind::Internal
    );
    // Another connection holds an uncommitted BEGIN IMMEDIATE: the open works and reads committed data.
    let wc = Connection::open(&path).unwrap();
    wc.execute_batch("BEGIN IMMEDIATE; INSERT INTO projects (id, name, created_at_ms) VALUES ('zz','zz',1);")
        .unwrap();
    let r2 = open_read_only(&url(&path)).await.unwrap();
    assert_eq!(r2.list_segments(&SegmentFilter::default()).await.unwrap().len(), 1);
    assert!(r2.get_project(&pid("zz")).await.unwrap().is_none());
    wc.execute_batch("ROLLBACK").unwrap();
    drop((wc, r, r2, w));
    // After a clean close.
    let r3 = open_read_only(&url(&path)).await.unwrap();
    assert_eq!(r3.list_segments(&SegmentFilter::default()).await.unwrap().len(), 1);
    drop(r3);
    // An older schema.
    let old = d.path().join("old.db");
    Connection::open(&old)
        .unwrap()
        .execute_batch(
            "CREATE TABLE schema_versions (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at INTEGER NOT NULL)",
        )
        .unwrap();
    let e = open_read_only(&url(&old)).await.unwrap_err();
    assert!(e.to_string().contains("run `forge serve` once"), "{e}");
}

/// A closed database in a 0o555 directory opens with `immutable=1`: readable, and no `-wal` / `-shm` appear.
#[tokio::test(flavor = "multi_thread")]
async fn read_only_dir_open() {
    use std::os::unix::fs::PermissionsExt;
    let d = tempfile::tempdir().unwrap();
    let w = db(d.path());
    w.commit_flush(flush(vec![seg("p", Level::L0, Some((1, 2)), 0, 3)], 1, 3))
        .await
        .unwrap();
    drop(w);
    let ro = d.path().join("ro");
    std::fs::create_dir(&ro).unwrap();
    std::fs::copy(d.path().join("meta.db"), ro.join("meta.db")).unwrap();
    std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o555)).unwrap();
    let cfg = MetastoreConfig {
        url: format!("sqlite://{}", ro.join("meta.db").display()),
    };
    let r = open_read_only(&cfg).await.unwrap();
    assert_eq!(r.list_segments(&SegmentFilter::default()).await.unwrap().len(), 1);
    assert_eq!(r.locate_trace(&pid("p"), TraceId([1; 16])).await.unwrap().len(), 1);
    drop(r);
    assert_eq!(files(&ro), ["meta.db"]);
    std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// 1,200 traces over 3 flushes: the batched call equals `locate_trace` per id; missing ids are absent; deleted
/// segments are excluded; other projects are invisible.
#[tokio::test(flavor = "multi_thread")]
async fn locate_traces_matches_locate_trace() {
    let d = tempfile::tempdir().unwrap();
    let w = db(d.path());
    let tid = |k: u64, i: u8| {
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&k.to_be_bytes());
        b[8] = i;
        TraceId(b)
    };
    let mut all = Vec::new();
    for i in 0..3u8 {
        let mut segs = Vec::new();
        for p in ["p", "q"] {
            let mut s = seg(p, Level::L0, Some((1, 2)), 0, 3);
            s.meta.row_count = 440;
            // Traces 0..440 of this flush (i) plus, in the p segment, every 10th trace of flush 0 again.
            s.traces = (0..440u64)
                .map(|k| TraceRange {
                    trace_id: if p == "p" && k % 10 == 0 { tid(k, 0) } else { tid(k, i) },
                    row_start: k,
                    row_count: 1,
                })
                .collect();
            s.traces.sort_by_key(|t| t.trace_id);
            for (k, t) in s.traces.iter_mut().enumerate() {
                t.row_start = k as u64;
            }
            segs.push(s);
        }
        all.extend(segs.iter().filter(|s| s.meta.project_id == pid("p")).cloned());
        w.commit_flush(flush(segs, i as u64 + 1, 3)).await.unwrap();
    }
    // Flush 1's p segment is deleted.
    Connection::open(d.path().join("meta.db"))
        .unwrap()
        .execute(
            "UPDATE segments SET state = 'deleted', deleted_at_ms = 1 WHERE path = ?1",
            [all[1].meta.path.as_str()],
        )
        .unwrap();
    let mut ids: Vec<TraceId> = all.iter().flat_map(|s| s.traces.iter().map(|t| t.trace_id)).collect();
    ids.sort();
    ids.dedup();
    ids.push(TraceId([0xee; 16]));
    let m = w.locate_traces(&pid("p"), &ids).await.unwrap();
    let mut found = 0;
    for t in &ids {
        let one = w.locate_trace(&pid("p"), *t).await.unwrap();
        assert!(
            one.iter().all(|l| l.segment.id != all[1].meta.id),
            "deleted segment listed"
        );
        assert!(
            one.iter().all(|l| l.segment.project_id == pid("p")),
            "other project listed"
        );
        assert_eq!(
            m.contains_key(t),
            !one.is_empty(),
            "{t}: traces with no rows are absent"
        );
        found += m.get(t).map_or(0, |v| v.len());
        assert_eq!(m.get(t).cloned().unwrap_or_default(), one, "{t}");
    }
    assert!(ids.len() >= 1_200, "{} traces", ids.len());
    assert_eq!(found, 880, "flush 0 and flush 2 segments of p");
    assert!(!m.contains_key(&TraceId([0xee; 16])));
}
