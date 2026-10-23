//! plans/phase-4 §6.5: begin rules (D1), swap (D6), fail/reset, job-age guard (D15), atomicity.
use forge_meta::*;
use forge_model::catalog::*;
use forge_model::ids::TraceId;
use forge_model::testing::{pid, segment_meta};
use rusqlite::Connection;
use std::sync::Arc;

fn seg(level: Level, hour: i64, v: (u64, u64), trace: u8) -> NewSegment {
    let t = TraceId([trace; 16]);
    NewSegment {
        meta: SegmentMeta {
            row_count: 2,
            min_version: v.0,
            max_version: v.1,
            min_trace_id: t,
            max_trace_id: t,
            ..segment_meta("p", level, HourBucket(hour))
        },
        traces: vec![TraceRange {
            trace_id: t,
            row_start: 0,
            row_count: 2,
        }],
        blobs: vec![format!("blobs/p/{trace}")],
    }
}

struct Db {
    _dir: tempfile::TempDir,
    m: SqliteMetastore,
    raw: Connection,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta.db");
    let m = SqliteMetastore::open_path(&path).unwrap();
    let raw = Connection::open(&path).unwrap();
    Db { _dir: dir, m, raw }
}

impl Db {
    async fn flush(&self, segs: &[&NewSegment], seq: u64) {
        self.m
            .commit_flush(FlushCommit {
                node_id: "n".into(),
                segments: segs.iter().map(|s| (*s).clone()).collect(),
                flushed_wal_seq: seq,
                max_version: 1000,
            })
            .await
            .unwrap();
    }
    async fn begin(&self, kind: JobKind, inputs: &[&NewSegment]) -> Option<CompactionJob> {
        self.m
            .begin_compaction(JobRequest {
                project_id: pid("p"),
                kind,
                inputs: inputs.iter().map(|s| s.meta.id).collect(),
            })
            .await
            .unwrap()
    }
    async fn state(&self, s: &NewSegment) -> SegmentState {
        self.m.get_segment(s.meta.id).await.unwrap().unwrap().state
    }
    fn count(&self, sql: &str) -> i64 {
        self.raw.query_row(sql, [], |r| r.get(0)).unwrap()
    }
    /// Everything a rejected begin must leave unchanged.
    fn snapshot(&self) -> (i64, i64, i64) {
        (
            self.count("SELECT count(*) FROM compaction_jobs"),
            self.count("SELECT count(*) FROM compaction_inputs"),
            self.count("SELECT count(*) FROM segments WHERE state = 'compacting'"),
        )
    }
}

fn commit(job: &CompactionJob, outputs: Vec<NewSegment>) -> Arc<CompactionCommit> {
    Arc::new(CompactionCommit {
        job_id: job.id,
        outputs,
        max_age_ms: 3_600_000,
    })
}

#[tokio::test]
async fn begin_rules_kind_a() {
    let d = db();
    let (a, b, c) = (
        seg(Level::L0, 1, (10, 11), 1),
        seg(Level::L0, 1, (12, 13), 2),
        seg(Level::L0, 1, (14, 15), 3),
    );
    let (r1, r2) = (seg(Level::L1, 7, (1, 2), 4), seg(Level::L1, 7, (3, 4), 5));
    d.flush(&[&r1, &r2], 1).await;
    d.flush(&[&a], 2).await;
    d.flush(&[&b], 3).await;
    d.flush(&[&c], 4).await;
    let before = d.snapshot();
    // Not a prefix; partial fold; kind B over L0; inputs of another project; empty kind A.
    assert!(d.begin(JobKind::L0, &[&b]).await.is_none(), "non-prefix");
    assert!(d.begin(JobKind::L0, &[&a, &c]).await.is_none(), "prefix with a gap");
    assert!(d.begin(JobKind::L0, &[&a, &r1]).await.is_none(), "partial fold");
    assert!(d.begin(JobKind::L1, &[&a]).await.is_none(), "kind B over L0");
    assert!(d.begin(JobKind::L0, &[&r1, &r2]).await.is_none(), "kind A without L0");
    let mut q = seg(Level::L0, 1, (16, 17), 9);
    q.meta.project_id = pid("q");
    d.flush(&[&q], 5).await;
    assert!(d.begin(JobKind::L0, &[&a, &q]).await.is_none(), "other project");
    assert_eq!(d.snapshot(), before, "every rejection writes nothing");
    // The prefix + a whole fold.
    let j = d.begin(JobKind::L0, &[&a, &b, &r1, &r2]).await.unwrap();
    assert_eq!(j.kind, JobKind::L0);
    assert!(j.inputs.iter().all(|s| s.state == SegmentState::Compacting));
    assert!(j.inputs.windows(2).all(|w| w[0].min_version <= w[1].min_version));
    // A second kind A: c is not a prefix while a/b are visible (D1a rejects before the unique index).
    assert!(d.begin(JobKind::L0, &[&c]).await.is_none());
    // Inputs must be live.
    assert!(d.begin(JobKind::L1, &[&r1, &r2]).await.is_none(), "inputs compacting");
}

#[tokio::test]
async fn begin_rules_kind_b_suffix() {
    let d = db();
    // Hour 7: three groups by version; g2 holds a main and an orphan segment of one job (overlapping versions).
    let g1 = seg(Level::L1, 7, (1, 5), 1);
    let g2m = seg(Level::L1, 7, (6, 9), 2);
    let g2o = seg(Level::L1, 7, (7, 8), 3);
    let g3 = seg(Level::L1, 7, (10, 12), 4);
    let other_hour = seg(Level::L1, 8, (13, 14), 5);
    d.flush(&[&g1, &g2m, &g2o, &g3, &other_hour], 1).await;
    let before = d.snapshot();
    assert!(
        d.begin(JobKind::L1, &[&g1, &g2m, &g2o]).await.is_none(),
        "skips the newest"
    );
    assert!(
        d.begin(JobKind::L1, &[&g1, &g3]).await.is_none(),
        "skips a middle group"
    );
    assert!(d.begin(JobKind::L1, &[&g2m, &g3]).await.is_none(), "splits a group");
    assert!(d.begin(JobKind::L1, &[&g3, &other_hour]).await.is_none(), "two hours");
    assert_eq!(d.snapshot(), before);
    // The newest suffix of whole groups.
    let j = d.begin(JobKind::L1, &[&g2m, &g2o, &g3]).await.unwrap();
    assert_eq!(j.inputs.len(), 3);
    // A second kind B on the older rest is rejected (the running job's segments are newer, D1c).
    assert!(d.begin(JobKind::L1, &[&g1]).await.is_none());
    // Kind B beside kind A on the same project.
    let a = seg(Level::L0, 7, (20, 21), 6);
    d.flush(&[&a], 2).await;
    assert!(d.begin(JobKind::L0, &[&a]).await.is_some(), "kind A beside kind B");
    // Whole hour = the special case of the suffix.
    let d2 = db();
    d2.flush(&[&g1, &g2m, &g2o, &g3], 1).await;
    assert!(d2.begin(JobKind::L1, &[&g1, &g2m, &g2o, &g3]).await.is_some());
}

/// The partial unique index is a backstop (D1a rejects a second kind A first): reach it with a raw INSERT.
#[tokio::test]
async fn one_running_l0_job_index() {
    let d = db();
    let a = seg(Level::L0, 1, (1, 2), 1);
    d.flush(&[&a], 1).await;
    d.begin(JobKind::L0, &[&a]).await.unwrap();
    let e = d
        .raw
        .execute(
            "INSERT INTO compaction_jobs (project_id, kind, status, created_at_ms) VALUES ('p', 'l0', 'running', 1)",
            [],
        )
        .unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
    d.raw
        .execute(
            "INSERT INTO compaction_jobs (project_id, kind, status, created_at_ms) VALUES ('p', 'l1', 'running', 1)",
            [],
        )
        .unwrap();
}

#[tokio::test]
async fn swap_retry_fail_reset_prune() {
    let d = db();
    let (a, b) = (seg(Level::L0, 1, (10, 11), 1), seg(Level::L0, 1, (12, 13), 2));
    let r1 = seg(Level::L1, 1, (1, 2), 1);
    d.flush(&[&r1], 1).await;
    d.flush(&[&a], 2).await;
    d.flush(&[&b], 3).await;
    let j = d.begin(JobKind::L0, &[&a, &r1]).await.unwrap();
    let o = seg(Level::L1, 1, (2, 11), 1);
    let c = commit(&j, vec![o.clone()]);
    d.m.commit_compaction(c.clone()).await.unwrap();
    d.m.commit_compaction(c.clone()).await.unwrap(); // retry after done: Ok
    assert_eq!(d.state(&a).await, SegmentState::Deleted);
    assert_eq!(d.state(&r1).await, SegmentState::Deleted);
    assert_eq!(d.state(&b).await, SegmentState::Live);
    // D6: the inputs' trace_index rows remain, locate ignores them.
    assert_eq!(d.m.segment_traces(a.meta.id).await.unwrap().len(), 1);
    let loc = d.m.locate_trace(&pid("p"), TraceId([1; 16])).await.unwrap();
    assert_eq!(loc.len(), 1);
    assert_eq!(loc[0].segment.id, o.meta.id);
    assert_eq!(
        d.count(&format!(
            "SELECT count(*) FROM blob_refs WHERE segment_id = '{}'",
            o.meta.id
        )),
        1
    );
    assert_eq!(d.m.fail_compaction(j.id, "late").await.unwrap(), JobStatus::Done);
    assert_eq!(d.state(&o).await, SegmentState::Live, "fail after done changes nothing");

    // Failed job: inputs live, commit -> Conflict.
    let j2 = d.begin(JobKind::L0, &[&b]).await.unwrap();
    assert_eq!(d.m.fail_compaction(j2.id, "boom").await.unwrap(), JobStatus::Failed);
    assert_eq!(d.state(&b).await, SegmentState::Live);
    let e = d.m.commit_compaction(commit(&j2, vec![])).await.unwrap_err();
    assert!(matches!(e, MetaError::Conflict(_)), "{e:?}");

    // Job-age guard (D15): Conflict, nothing written.
    let j3 = d.begin(JobKind::L0, &[&b]).await.unwrap();
    let o3 = seg(Level::L1, 1, (12, 13), 2);
    let e =
        d.m.commit_compaction(Arc::new(CompactionCommit {
            job_id: j3.id,
            outputs: vec![o3.clone()],
            max_age_ms: -1,
        }))
        .await
        .unwrap_err();
    assert!(matches!(&e, MetaError::Conflict(m) if m.contains("max age")), "{e:?}");
    assert!(d.m.get_segment(o3.meta.id).await.unwrap().is_none());
    assert_eq!(d.state(&b).await, SegmentState::Compacting);

    // reset: every running job failed, inputs live.
    assert_eq!(d.m.reset_compactions().await.unwrap(), 1);
    assert_eq!(d.state(&b).await, SegmentState::Live);
    assert_eq!(
        d.count("SELECT count(*) FROM compaction_jobs WHERE status = 'running'"),
        0
    );
    assert_eq!(d.m.reset_compactions().await.unwrap(), 0);

    // prune: done/failed only, by finished_at.
    let j4 = d.begin(JobKind::L0, &[&b]).await.unwrap();
    assert_eq!(d.m.prune_compaction_jobs(0).await.unwrap(), 0);
    assert_eq!(d.m.prune_compaction_jobs(i64::MAX).await.unwrap(), 3);
    assert_eq!(d.count("SELECT count(*) FROM compaction_jobs"), 1);
    assert_eq!(
        d.count(&format!("SELECT count(*) FROM compaction_jobs WHERE id = {}", j4.id)),
        1
    );
}

/// RT #6: outputs[1] fails inside insert_segment (a duplicate SegmentId) after the inputs' UPDATE: nothing lands.
#[tokio::test]
async fn swap_is_atomic() {
    let d = db();
    let (a, b) = (seg(Level::L0, 1, (10, 11), 1), seg(Level::L0, 1, (12, 13), 2));
    d.flush(&[&a], 1).await;
    d.flush(&[&b], 2).await;
    let j = d.begin(JobKind::L0, &[&a]).await.unwrap();
    let o0 = seg(Level::L1, 1, (10, 11), 1);
    let mut dup = seg(Level::L1, 1, (10, 11), 7);
    dup.meta.id = b.meta.id; // already exists
    assert!(d.m.commit_compaction(commit(&j, vec![o0.clone(), dup])).await.is_err());
    assert_eq!(d.state(&a).await, SegmentState::Compacting);
    assert_eq!(
        d.count("SELECT count(*) FROM compaction_jobs WHERE status = 'running'"),
        1
    );
    assert!(d.m.get_segment(o0.meta.id).await.unwrap().is_none());
    for t in ["trace_index", "blob_refs"] {
        assert_eq!(
            d.count(&format!("SELECT count(*) FROM {t} WHERE segment_id = '{}'", o0.meta.id)),
            0,
            "{t}"
        );
    }
}

/// RA m5: the input aggregates over zero inputs are Corrupt, not a column-type error.
#[tokio::test]
async fn commit_without_inputs_is_corrupt() {
    let d = db();
    let a = seg(Level::L0, 1, (10, 11), 1);
    d.flush(&[&a], 1).await;
    let j = d.begin(JobKind::L0, &[&a]).await.unwrap();
    d.raw.execute("DELETE FROM compaction_inputs", []).unwrap();
    let e = d.m.commit_compaction(commit(&j, vec![])).await.unwrap_err();
    assert!(matches!(e, MetaError::Corrupt { .. }), "{e:?}");
}

#[tokio::test]
async fn segment_traces_by_row_start() {
    let d = db();
    let mut s = seg(Level::L0, 1, (1, 2), 1);
    s.meta.row_count = 6;
    s.traces = [(9u8, 0u64), (3, 2), (5, 4)]
        .iter()
        .map(|&(t, start)| TraceRange {
            trace_id: TraceId([t; 16]),
            row_start: start,
            row_count: 2,
        })
        .collect();
    s.meta.min_trace_id = TraceId([3; 16]);
    s.meta.max_trace_id = TraceId([9; 16]);
    d.flush(&[&s], 1).await;
    let got: Vec<u64> =
        d.m.segment_traces(s.meta.id)
            .await
            .unwrap()
            .iter()
            .map(|r| r.row_start)
            .collect();
    assert_eq!(got, vec![0, 2, 4]);
}
