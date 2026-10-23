//! plans/phase-4 §6.5: focused compaction cases (B1 regression, slices, rotation and the writer cap, placement,
//! fill, sticky, backfill, the upload-failure path). The commit-hook cases live in forge-query (HookedMetastore).
use forge_ingest::compact::{CompactCtx, compact_project, job};
use forge_ingest::testing::write_flush;
use forge_meta::{JobRequest, Metastore, SegmentFilter, SqliteMetastore};
use forge_model::blob::{blob_ref, blob_refs_in};
use forge_model::catalog::{HourBucket, JobKind, Level, SegmentId, SegmentMeta, SegmentState};
use forge_model::config::CompactionConfig;
use forge_model::dotted::dotted_segment;
use forge_model::merge::{Tombstones, merge_versions};
use forge_model::row::SpanRow;
use forge_model::testing::{batch_from_rows, pid, rows_from_batch, sid, tid};
use forge_model::time::Clocks;
use forge_storage::testing::{HookedSegmentStore, memory_store};
use forge_storage::{ScanRequest, SegmentStore, StorageError};
use futures::TryStreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const H: i64 = HourBucket::NANOS;
const H0: i64 = 1_700_000_000 / 3600;

fn at(hour: i64, off: i64) -> i64 {
    (H0 + hour) * H + off
}

fn row(trace: u64, span: u64, v: u64) -> SpanRow {
    SpanRow {
        project_id: Some(pid("p")),
        trace_id: tid(trace),
        span_id: sid(span),
        version: v,
        name: Some(format!("n{v}")),
        ..Default::default()
    }
}

struct W {
    meta: Arc<dyn Metastore>,
    store: Arc<dyn SegmentStore>,
    seq: u64,
}

impl W {
    fn new() -> Self {
        Self {
            meta: Arc::new(SqliteMetastore::open_in_memory().unwrap()),
            store: memory_store(),
            seq: 0,
        }
    }
    fn cx(&self, cfg: CompactionConfig) -> CompactCtx {
        CompactCtx::new(self.meta.clone(), self.store.clone(), cfg, Clocks::wall())
    }
    async fn flush(&mut self, rows: &[SpanRow]) -> SegmentId {
        self.seq += 1;
        let b = batch_from_rows(rows);
        let segs = write_flush(
            self.meta.as_ref(),
            self.store.clone(),
            &b,
            HourBucket(H0 + 9),
            "n",
            1 << 30,
            self.seq,
            1_000_000,
        )
        .await;
        segs[0].id
    }
    async fn segs(&self) -> Vec<SegmentMeta> {
        self.meta.list_segments(&SegmentFilter::default()).await.unwrap()
    }
    async fn l1(&self) -> Vec<SegmentMeta> {
        self.segs().await.into_iter().filter(|s| s.level == Level::L1).collect()
    }
    async fn rows(&self, s: &SegmentMeta) -> Vec<SpanRow> {
        let b = self.store.scan_all(s, ScanRequest::default()).await.unwrap();
        b.iter().flat_map(|b| rows_from_batch(b).unwrap()).collect()
    }
    async fn begin(&self, kind: JobKind, inputs: Vec<SegmentId>) -> forge_meta::CompactionJob {
        self.meta
            .begin_compaction(JobRequest {
                project_id: pid("p"),
                kind,
                inputs,
            })
            .await
            .unwrap()
            .expect("valid request")
    }
    /// One kind-A job over every L0 (no folds).
    async fn compact_l0(&self, cfg: CompactionConfig) -> forge_ingest::compact::JobReport {
        let ids = self
            .segs()
            .await
            .into_iter()
            .filter(|s| s.level == Level::L0)
            .map(|s| s.id)
            .collect();
        let j = self.begin(JobKind::L0, ids).await;
        job::run_job(&self.cx(cfg), j).await.unwrap()
    }
    /// Every visible row, merged.
    async fn read(&self) -> Vec<SpanRow> {
        let mut all = Vec::new();
        for s in self.segs().await {
            all.extend(self.rows(&s).await);
        }
        rows_from_batch(&merge_versions(&[batch_from_rows(&all)], Tombstones::Keep).unwrap()).unwrap()
    }
}

/// RA B1: one job's main + orphan run (and rotated files) never trigger kind B; `forge compact` settles.
#[tokio::test]
async fn b1_regression_one_job_then_none() {
    let mut w = W::new();
    let start = SpanRow {
        start_time_ns: Some(at(0, 1)),
        ..row(1, 1, 1)
    };
    let patch = SpanRow {
        end_time_ns: Some(at(0, 5)),
        ..row(1, 2, 2)
    };
    w.flush(&[start, patch]).await;
    let cx = w.cx(CompactionConfig::default());
    let s = compact_project(&cx, &pid("p")).await.unwrap();
    assert_eq!(s.jobs.len(), 1, "{s:?}");
    assert_eq!(s.jobs[0].outputs.len(), 2, "main + orphan: {s:?}");
    let s = compact_project(&cx, &pid("p")).await.unwrap();
    assert!(s.jobs.is_empty() && !s.capped, "{s:?}");
    // Rotated outputs of one job do not trigger either.
    let mut w = W::new();
    let rows: Vec<SpanRow> = (0..40)
        .map(|t| SpanRow {
            start_time_ns: Some(at(0, t as i64)),
            inputs: Some("x".repeat(2000)),
            ..row(t, 1, t + 1)
        })
        .collect();
    w.flush(&rows).await;
    let cx = w.cx(CompactionConfig {
        target_segment: bytesize::ByteSize::kib(8),
        slice_bytes: bytesize::ByteSize::kib(1),
        ..Default::default()
    });
    let s = compact_project(&cx, &pid("p")).await.unwrap();
    assert_eq!(s.jobs.len(), 1, "{s:?}");
    assert!(s.jobs[0].stats.rotated > 0, "{s:?}");
}

/// Many slices give the same rows as one slice; rotation and the writer cap give key-disjoint files of one hour.
#[tokio::test]
async fn slices_rotation_and_writer_cap() {
    let rows: Vec<SpanRow> = (0..60u64)
        .flat_map(|t| {
            (0..3u64).map(move |s| SpanRow {
                // 6 hours: more (hour, orphan) outputs than MAX_OPEN_WRITERS, fed in trace order.
                start_time_ns: (s != 2).then(|| at((t % 6) as i64, t as i64)),
                end_time_ns: Some(at((t % 6) as i64, 9)),
                inputs: Some(format!("payload {t} {s} {}", "y".repeat(300))),
                ..row(t, s, t * 3 + s + 1)
            })
        })
        .collect();
    let mut out = Vec::new();
    for (slice, target) in [(128 << 20, 256 << 20), (1u64, 8 << 10)] {
        let mut w = W::new();
        w.flush(&rows).await;
        let rep = w
            .compact_l0(CompactionConfig {
                slice_bytes: bytesize::ByteSize::b(slice),
                target_segment: bytesize::ByteSize::b(target),
                ..Default::default()
            })
            .await;
        let l1 = w.l1().await;
        // Per (hour, orphan): files have pairwise disjoint trace ranges.
        let mut by: std::collections::BTreeMap<(HourBucket, bool), Vec<&SegmentMeta>> = Default::default();
        for s in &l1 {
            assert!(s.start_time_nulls == 0 || s.start_time_nulls == s.row_count);
            by.entry((s.partition_hour, s.start_time_nulls > 0))
                .or_default()
                .push(s);
        }
        for v in by.values_mut() {
            v.sort_by_key(|s| s.min_trace_id);
            assert!(v.windows(2).all(|p| p[0].max_trace_id < p[1].min_trace_id), "{v:?}");
        }
        if slice == 1 {
            assert!(rep.stats.slices > 1 && rep.stats.writer_cap > 0, "{:?}", rep.stats);
        } else {
            assert_eq!((rep.stats.slices, rep.stats.rotated), (1, 0), "{:?}", rep.stats);
        }
        let mut r = w.read().await;
        r.sort_by_key(|x| (x.trace_id, x.span_id));
        out.push(r);
    }
    assert_eq!(out[0], out[1]);
}

/// The lazy re-plan: the measured ratio changes the slice size.
#[test]
fn slices_follow_the_ratio() {
    let seg = SegmentMeta {
        row_count: 100,
        byte_size: 1000,
        ..forge_model::testing::segment_meta("p", Level::L0, HourBucket(H0))
    };
    let ranges: Vec<_> = (0..100)
        .map(|t| forge_model::catalog::TraceRange {
            trace_id: tid(t),
            row_start: t,
            row_count: 1,
        })
        .collect();
    let mut s = job::Slices::new(&[(seg, ranges)]);
    let a = s.next(100, 1.0).unwrap(); // 10 B/row
    let b = s.next(100, 10.0).unwrap(); // 100 B/row
    assert_eq!((a.0.len(), b.0.len()), (10, 1));
    let mut n = 2;
    while s.next(100, 1.0).is_some() {
        n += 1;
    }
    assert_eq!(n, 2 + 9);
}

/// Placement (D3), the start fill from older rows only, sticky across a start edit, backfill via context.
#[tokio::test]
async fn placement_fill_sticky_backfill() {
    let mut w = W::new();
    // k1: null start, end in hour 2 -> orphan run of hour 2. k2: start in hour 1. parent P (hour 1) of child C.
    w.flush(&[
        SpanRow {
            end_time_ns: Some(at(2, 7)),
            ..row(1, 1, 1)
        },
        SpanRow {
            start_time_ns: Some(at(1, 3)),
            ..row(1, 2, 2)
        },
        SpanRow {
            start_time_ns: Some(at(1, 4)),
            ..row(2, 10, 3)
        },
    ])
    .await;
    w.compact_l0(CompactionConfig::default()).await;
    let l1 = w.l1().await;
    let orphan = l1.iter().find(|s| s.start_time_nulls > 0).unwrap();
    assert_eq!(orphan.partition_hour, HourBucket(H0 + 2));
    let p_dotted = dotted_segment(at(1, 4), sid(10));
    // k1 gets a start in hour 0 (stays in hour 2: sticky); k2 a patch with no start (filled from the older L1);
    // child C of P with no dotted (backfilled through the context ancestor P).
    w.flush(&[
        SpanRow {
            start_time_ns: Some(at(0, 1)),
            ..row(1, 1, 4)
        },
        SpanRow {
            end_time_ns: Some(at(1, 9)),
            ..row(1, 2, 5)
        },
        SpanRow {
            start_time_ns: Some(at(1, 5)),
            parent_span_id: Some(sid(10)),
            ..row(2, 11, 6)
        },
    ])
    .await;
    let rep = w.compact_l0(CompactionConfig::default()).await;
    assert_eq!(rep.stats.fills, 1, "{rep:?}");
    for s in w.l1().await {
        for r in w.rows(&s).await {
            if r.version < 4 {
                continue;
            }
            match r.span_id {
                x if x == sid(1) => assert_eq!(s.partition_hour, HourBucket(H0 + 2), "sticky across a start edit"),
                x if x == sid(2) => {
                    assert_eq!(r.start_time_ns, Some(at(1, 3)), "filled from the older L1 row");
                    assert_eq!((s.partition_hour, s.start_time_nulls), (HourBucket(H0 + 1), 0));
                }
                _ => assert_eq!(
                    r.dotted_order,
                    Some(format!("{p_dotted}.{}", dotted_segment(at(1, 5), sid(11)))),
                    "backfill through the context ancestor"
                ),
            }
        }
    }
}

/// RD #11: a newer context row (a concurrent kind-A run of the hour) is never a fill source.
#[tokio::test]
async fn newer_context_is_not_a_fill_source() {
    let mut w = W::new();
    w.flush(&[SpanRow {
        end_time_ns: Some(at(0, 7)),
        ..row(1, 1, 1)
    }])
    .await;
    w.compact_l0(CompactionConfig::default()).await; // orphan run of hour 0
    let orphan = w.l1().await.remove(0);
    let l0 = w
        .flush(&[SpanRow {
            start_time_ns: Some(at(0, 3)),
            ..row(1, 1, 2)
        }])
        .await;
    let jb = w.begin(JobKind::L1, vec![orphan.id]).await;
    let ja = w.begin(JobKind::L0, vec![l0]).await; // cannot fold hour 0 (compacting): a newer run of hour 0
    job::run_job(&w.cx(CompactionConfig::default()), ja).await.unwrap();
    let rb = job::run_job(&w.cx(CompactionConfig::default()), jb).await.unwrap();
    assert_eq!(rb.stats.fills, 0, "{rb:?}");
    let l1 = w.l1().await;
    assert_eq!(l1.len(), 2);
    let b_out = l1.iter().find(|s| s.max_version == 1).unwrap();
    assert_eq!(b_out.start_time_nulls, b_out.row_count, "still an orphan");
    let read = w.read().await;
    assert_eq!(read[0].start_time_ns, Some(at(0, 3)));
}

/// An upload error before the commit: job failed, inputs live, the uploaded outputs deleted (D7). Output blobs
/// are the blobs of the output rows.
#[tokio::test]
async fn upload_failure_deletes_uploads() {
    let mut w = W::new();
    let rows: Vec<SpanRow> = (0..20)
        .map(|t| SpanRow {
            start_time_ns: Some(at(0, t as i64)),
            outputs: Some(blob_ref(&format!("blobs/p/b{}", t % 3), 1)),
            inputs: Some("z".repeat(3000)),
            ..row(t, 1, t + 1)
        })
        .collect();
    let l0 = w.flush(&rows).await;
    let cfg = CompactionConfig {
        target_segment: bytesize::ByteSize::kib(8),
        slice_bytes: bytesize::ByteSize::kib(1),
        ..Default::default()
    };
    // The third write_segment fails: two outputs are uploaded first.
    let writes = Arc::new(AtomicUsize::new(0));
    let n = writes.clone();
    let store = Arc::new(HookedSegmentStore::new(
        w.store.clone(),
        Box::new(move |_| {
            let r = if n.fetch_add(1, Ordering::SeqCst) + 1 == 3 {
                Err(StorageError::InvalidInput("injected upload failure".into()))
            } else {
                Ok(())
            };
            Box::pin(async move { r })
        }),
    ));
    let cx = CompactCtx {
        segments: store,
        ..w.cx(cfg.clone())
    };
    let j = w.begin(JobKind::L0, vec![l0]).await;
    assert!(job::run_job(&cx, j).await.is_err());
    assert!(writes.load(Ordering::SeqCst) >= 3);
    let segs = w.segs().await;
    assert!(
        segs.iter()
            .all(|s| s.state == SegmentState::Live && s.level == Level::L0)
    );
    let os = w.store.object_store();
    let l1_objects: Vec<_> = os
        .list(Some(&object_store::path::Path::from("segments/p/L1")))
        .try_collect()
        .await
        .unwrap();
    assert!(l1_objects.is_empty(), "uploads left: {l1_objects:?}");
    // A later job succeeds; each output's blob_refs are its rows' blobs.
    let rep = w.compact_l0(cfg).await;
    assert!(rep.outputs.len() > 1);
    for s in w.l1().await {
        let b = w.store.scan_all(&s, ScanRequest::default()).await.unwrap();
        let mut refs: Vec<String> = w.meta.blob_refs(&pid("p")).await.unwrap().into_iter().collect();
        refs.sort();
        for path in blob_refs_in(&b) {
            assert!(refs.contains(&path), "{path}");
        }
    }
}

/// The job's merge is byte-bounded and re-cut at trace boundaries: a 1-byte bound (every merged batch as small as
/// possible) writes the same outputs, row for row, as the default bound.
#[tokio::test]
async fn tiny_merge_chunks_write_the_same_outputs() {
    let starts: Vec<SpanRow> = (0..30)
        .flat_map(|t| {
            (0..4).map(move |s| SpanRow {
                start_time_ns: Some(at(t as i64 % 3, s as i64)),
                parent_span_id: (s > 0).then(|| sid(t * 10)),
                ..row(t, t * 10 + s, t * 4 + s + 1)
            })
        })
        .collect();
    // Patches: newer versions of some spans, and spans whose start never arrives (orphan rows).
    let patches: Vec<SpanRow> = (0..30)
        .map(|t| SpanRow {
            end_time_ns: Some(at(t as i64 % 3, 9)),
            ..row(t, t * 10 + (t % 5), 1000 + t)
        })
        .collect();
    let cfg = CompactionConfig {
        slice_bytes: bytesize::ByteSize::kib(2),
        ..Default::default()
    };
    let mut outputs = Vec::new();
    for chunk_bytes in [None, Some(1)] {
        let mut w = W::new();
        let a = w.flush(&starts).await;
        let b = w.flush(&patches).await;
        let mut cx = w.cx(cfg.clone());
        if let Some(n) = chunk_bytes {
            cx.merge_chunk_bytes = n;
        }
        let j = w.begin(JobKind::L0, vec![a, b]).await;
        let rep = job::run_job(&cx, j).await.unwrap();
        assert!(rep.stats.slices > 1 && rep.stats.orphan_outputs > 0, "{rep:?}");
        let mut l1 = w.l1().await;
        l1.sort_by_key(|s| (s.partition_hour, s.is_orphan_run(), s.min_trace_id));
        let mut out = Vec::new();
        for s in &l1 {
            out.push((s.partition_hour, s.is_orphan_run(), w.rows(s).await));
        }
        outputs.push(out);
    }
    assert_eq!(outputs[0], outputs[1]);
}
