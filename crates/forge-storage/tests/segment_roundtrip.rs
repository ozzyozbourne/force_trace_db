use arrow::array::{AsArray, Int64Array};
use arrow::compute::{concat_batches, max, min};
use arrow::datatypes::SchemaRef;
use arrow::datatypes::TimestampNanosecondType;
use arrow::record_batch::RecordBatch;
use forge_model::catalog::{HourBucket, Level, SegmentId, paths};
use forge_model::config::StorageConfig;
use forge_model::dotted::dotted_order;
use forge_model::error::{Classify, ErrorKind, ModelError};
use forge_model::ids::{SpanId, TraceId};
use forge_model::row::SpanRow;
use forge_model::schema::{col, col_idx, projected_schema, spans_schema};
use forge_model::testing::{batch_from_rows, pid};
use forge_storage::*;
use futures::{TryStreamExt, stream};
use object_store::ObjectStoreExt;
use std::sync::Arc;

fn trace(t: usize) -> TraceId {
    TraceId((t as u128 + 1).to_be_bytes())
}

/// `traces` x `spans_per` rows of project `p`, sorted by (trace_id, dotted_order); versions 1..=n.
fn rows(traces: usize, spans_per: usize) -> Vec<SpanRow> {
    let mut out = Vec::with_capacity(traces * spans_per);
    for t in 0..traces {
        let t0 = 1_700_000_000_000_000_000 + t as i64 * 1_000_000;
        let root = SpanId([1; 8]);
        let root_dotted = dotted_order(None, t0, root);
        for s in 0..spans_per {
            let n = out.len();
            let start = t0 + s as i64 * 1_000;
            let (span, dotted) = if s == 0 {
                (root, root_dotted.clone())
            } else {
                let sp = SpanId((s as u64 + 1).to_be_bytes());
                (sp, dotted_order(Some(&root_dotted), start, sp))
            };
            out.push(SpanRow {
                project_id: Some(pid("p")),
                trace_id: trace(t),
                span_id: span,
                parent_span_id: (s > 0).then_some(root),
                dotted_order: Some(dotted),
                name: match n % 3 {
                    0 => None,
                    1 => Some(String::new()),
                    _ => Some(format!("späñ-{n} 🚀")),
                },
                start_time_ns: (n % 7 != 3).then_some(start),
                tags: match n % 3 {
                    0 => None,
                    1 => Some(vec![]),
                    _ => Some(vec![String::new(), "ü".into()]),
                },
                inputs: Some(format!("{{\"q\":\"日本 {n}\"}}")),
                version: n as u64 + 1,
                ..Default::default()
            });
        }
    }
    out
}

fn input(batches: Vec<RecordBatch>) -> BatchStream {
    Box::pin(stream::iter(batches.into_iter().map(Ok)))
}

fn spec(p: &str) -> SegmentSpec {
    SegmentSpec {
        id: SegmentId::new(),
        project_id: pid(p),
        level: Level::L0,
        partition_hour: HourBucket(493_000),
    }
}

fn store(url: &str) -> VortexSegmentStore {
    VortexSegmentStore::new(build_object_store(&StorageConfig { url: url.into() }).unwrap())
}

async fn collect(s: BatchStream, schema: SchemaRef) -> RecordBatch {
    let v: Vec<_> = s.try_collect().await.unwrap();
    concat_batches(&schema, &v).unwrap()
}

async fn scan_range(
    st: &VortexSegmentStore,
    meta: &forge_model::catalog::SegmentMeta,
    r: std::ops::Range<u64>,
) -> RecordBatch {
    let req = ScanRequest {
        rows: RowSelection::Range(r),
        ..Default::default()
    };
    collect(st.scan(meta, req).await.unwrap(), spans_schema()).await
}

#[tokio::test]
async fn memory_round_trip_multi_batch() {
    let st = store("memory://");
    let all = batch_from_rows(&rows(10, 10));
    let sp = spec("p");
    let parts = vec![all.slice(0, 15), all.slice(15, 40), all.slice(55, 45)];
    let w = st.write_segment(sp.clone(), input(parts)).await.unwrap();

    let m = &w.meta;
    assert_eq!(m.row_count, 100);
    assert_eq!(
        m.start_time_nulls,
        all.column_by_name(col::START_TIME).unwrap().null_count() as u64
    );
    assert!(m.start_time_nulls > 0);
    // The extremes sit in different batches: a per-batch (not per-segment) min/max would miss them.
    let starts = all
        .column_by_name(col::START_TIME)
        .unwrap()
        .as_primitive::<TimestampNanosecondType>();
    assert_eq!((m.min_start_ns, m.max_start_ns), (min(starts), max(starts)));
    assert!(m.min_start_ns.is_some() && m.min_start_ns < m.max_start_ns);
    assert_eq!((m.min_trace_id, m.max_trace_id), (trace(0), trace(9)));
    assert_eq!((m.min_version, m.max_version), (1, 100));
    assert_eq!(
        m.path,
        paths::segment_path(&sp.project_id, sp.level, sp.partition_hour, sp.id).unwrap()
    );
    assert_eq!(m.fts_byte_size, None);
    let head = st.object_store().head(&m.path.as_str().into()).await.unwrap();
    assert_eq!(head.size, m.byte_size);
    assert_eq!(w.traces.len(), 10);
    for (i, t) in w.traces.iter().enumerate() {
        assert_eq!((t.trace_id, t.row_start, t.row_count), (trace(i), i as u64 * 10, 10));
    }

    assert_eq!(
        collect(st.scan(m, ScanRequest::default()).await.unwrap(), spans_schema()).await,
        all
    );

    let cols = vec![col::SPAN_ID, col::TAGS];
    let req = ScanRequest {
        projection: Some(cols.clone()),
        rows: RowSelection::Range(20..30),
        ..Default::default()
    };
    let got = collect(st.scan(m, req).await.unwrap(), projected_schema(&cols)).await;
    let idx: Vec<usize> = cols.iter().map(|c| col_idx(c)).collect();
    assert_eq!(got, all.slice(20, 10).project(&idx).unwrap());
}

#[tokio::test]
async fn multi_chunk_ranges() {
    let st = store("memory://");
    let all = batch_from_rows(&rows(2_000, 10));
    let w = st.write_segment(spec("p"), input(vec![all.clone()])).await.unwrap();
    assert_eq!(w.meta.row_count, 20_000);
    let parts: Vec<_> = st
        .scan(&w.meta, ScanRequest::default())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(
        parts.len() > 1,
        "expected more than one Vortex split, got {}",
        parts.len()
    );
    assert_eq!(scan_range(&st, &w.meta, 8190..8200).await, all.slice(8190, 10));
    // Straddles the first split (row 16384).
    assert_eq!(scan_range(&st, &w.meta, 16_380..16_390).await, all.slice(16_380, 10));
    assert_eq!(scan_range(&st, &w.meta, 0..20_000).await, all);
    assert_eq!(scan_range(&st, &w.meta, 19_995..20_000).await, all.slice(19_995, 5));
}

#[tokio::test]
async fn file_store_writes_under_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let st = store(&format!("file://{}/data", tmp.path().display()));
    let all = batch_from_rows(&rows(3, 4));
    let w = st.write_segment(spec("p"), input(vec![all.clone()])).await.unwrap();
    assert!(w.meta.path.starts_with("segments/p/L0/"), "{}", w.meta.path);
    let on_disk = tmp.path().join("data").join(&w.meta.path);
    assert_eq!(std::fs::metadata(&on_disk).unwrap().len(), w.meta.byte_size);
    assert_eq!(st.local_root(), Some(tmp.path().join("data").as_path()));
    assert_eq!(
        collect(st.scan(&w.meta, ScanRequest::default()).await.unwrap(), spans_schema()).await,
        all
    );
}

#[test]
fn file_url_paths() {
    assert_eq!(
        file_url_path("file://./x").unwrap(),
        std::env::current_dir().unwrap().join("x")
    );
    assert_eq!(file_url_path("file:///x").unwrap(), std::path::Path::new("/x"));
}

#[tokio::test]
async fn write_invariants() {
    let st = store("memory://");
    let r = rows(10, 3);
    let all = batch_from_rows(&r);

    let mut reversed = r.clone();
    reversed.reverse();
    let e = st
        .write_segment(spec("p"), input(vec![batch_from_rows(&reversed)]))
        .await
        .unwrap_err();
    assert!(matches!(e, StorageError::Unsorted(_)), "{e:?}");

    let e = st
        .write_segment(spec("p"), input(vec![all.slice(15, 15), all.slice(0, 15)]))
        .await
        .unwrap_err();
    assert!(matches!(e, StorageError::Unsorted(_)), "across batches: {e:?}");

    let e = st.write_segment(spec("q"), input(vec![all.clone()])).await.unwrap_err();
    assert!(matches!(e, StorageError::Unsorted(_)), "{e:?}");

    let e = st.write_segment(spec("p"), input(vec![])).await.unwrap_err();
    assert!(matches!(e, StorageError::EmptySegment), "{e:?}");

    let sp = spec("p");
    st.write_segment(sp.clone(), input(vec![all.clone()])).await.unwrap();
    let e = st.write_segment(sp, input(vec![all.slice(0, 3)])).await.unwrap_err();
    assert!(
        matches!(e, StorageError::ObjectStore(object_store::Error::AlreadyExists { .. })),
        "same id, different content: {e:?}"
    );
}

#[tokio::test]
async fn retry_after_a_put_that_landed_succeeds() {
    let st = store("memory://");
    let all = batch_from_rows(&rows(4, 3));
    let sp = spec("p");
    let first = st.write_segment(sp.clone(), input(vec![all.clone()])).await.unwrap();
    // The caller saw an error (e.g. a timeout) and retries the same spec with the same rows.
    let again = st.write_segment(sp, input(vec![all.clone()])).await.unwrap();
    assert_eq!(
        (again.meta.path, again.meta.byte_size),
        (first.meta.path.clone(), first.meta.byte_size)
    );
    assert_eq!(
        collect(
            st.scan(&first.meta, ScanRequest::default()).await.unwrap(),
            spans_schema()
        )
        .await,
        all
    );
}

fn with_version(b: &RecordBatch, versions: Vec<i64>) -> RecordBatch {
    let mut cols = b.columns().to_vec();
    cols[col_idx(col::VERSION)] = Arc::new(Int64Array::from(versions));
    RecordBatch::try_new(spans_schema(), cols).unwrap()
}

#[tokio::test]
async fn negative_versions_are_rejected() {
    let st = store("memory://");
    let b = with_version(&batch_from_rows(&rows(1, 2)), vec![-1, 3]);
    let e = st.write_segment(spec("p"), input(vec![b])).await.unwrap_err();
    assert!(
        matches!(&e, StorageError::Model(ModelError::Schema(m)) if m.contains("version")),
        "{e:?}"
    );
}

async fn write(st: &VortexSegmentStore, rows: &[SpanRow]) -> StorageResult<WrittenSegment> {
    st.write_segment(spec("p"), input(vec![batch_from_rows(rows)])).await
}

#[tokio::test]
async fn full_sort_key_is_enforced() {
    let st = store("memory://");
    let base = rows(1, 3);

    let mut desc = base.clone();
    desc.reverse(); // one trace, dotted_order descending
    let e = write(&st, &desc).await.unwrap_err();
    assert!(matches!(e, StorageError::Unsorted(_)), "{e:?}");
    assert!(
        e.to_string()
            .contains("dotted_order NULLS LAST, span_id) within one project, with one merged row per span"),
        "{e}"
    );

    let mut dup = base.clone();
    dup.push(base[2].clone()); // the exact same row twice
    let e = write(&st, &dup).await.unwrap_err();
    assert!(e.to_string().contains("span appears twice"), "{e}");

    // A segment holds one merged row per span (invariant 3): two versions of one span are rejected, whether
    // adjacent or apart (dotted_order changed between versions, so they do not sort together).
    let mut v2 = base[2].clone();
    v2.version = 99;
    let mut adjacent = base.clone();
    adjacent.push(v2);
    let e = write(&st, &adjacent).await.unwrap_err();
    assert!(e.to_string().contains("span appears twice"), "adjacent: {e}");
    let mut moved = base[0].clone();
    moved.version = 100;
    moved.dotted_order = None;
    let mut apart = base.clone();
    apart.push(moved);
    let e = write(&st, &apart).await.unwrap_err();
    assert!(e.to_string().contains("span appears twice"), "apart: {e}");

    // Same span key, two different spans, sorted by span_id when dotted_order is equal.
    let mut sibling = base[2].clone();
    sibling.span_id = SpanId([0xff; 8]);
    let mut by_span = base.clone();
    by_span.push(sibling.clone());
    assert!(write(&st, &by_span).await.is_ok());

    // NULLS LAST: a null dotted_order may follow a non-null one, not the reverse.
    let mut null_dotted = sibling.clone();
    null_dotted.dotted_order = None;
    let mut nulls_last = base.clone();
    nulls_last.push(null_dotted.clone());
    assert!(write(&st, &nulls_last).await.is_ok());
    let mut nulls_first = base.clone();
    nulls_first.insert(0, null_dotted);
    assert!(matches!(write(&st, &nulls_first).await, Err(StorageError::Unsorted(_))));

    // The check holds across batch boundaries.
    let b = batch_from_rows(&dup);
    let e = st
        .write_segment(spec("p"), input(vec![b.slice(0, 3), b.slice(3, 1)]))
        .await
        .unwrap_err();
    assert!(e.to_string().contains("span appears twice"), "across batches: {e}");
    let b = batch_from_rows(&desc);
    let e = st
        .write_segment(spec("p"), input(vec![b.slice(0, 1), b.slice(1, 2)]))
        .await
        .unwrap_err();
    assert!(e.to_string().contains("sorts before"), "across batches: {e}");
}

#[tokio::test]
async fn scan_rejects_bad_row_ranges() {
    let st = store("memory://");
    let w = st
        .write_segment(spec("p"), input(vec![batch_from_rows(&rows(10, 10))]))
        .await
        .unwrap();
    #[allow(clippy::reversed_empty_ranges)]
    for r in [95..200u64, 150..160, 10..5] {
        let req = ScanRequest {
            rows: RowSelection::Range(r.clone()),
            ..Default::default()
        };
        match st.scan(&w.meta, req).await {
            Err(e) => assert_eq!(e.kind(), ErrorKind::InvalidInput, "{r:?}: {e:?}"),
            Ok(_) => panic!("{r:?} accepted"),
        }
    }
    assert_eq!(scan_range(&st, &w.meta, 100..100).await.num_rows(), 0);
    assert_eq!(scan_range(&st, &w.meta, 90..100).await.num_rows(), 10);
}

#[tokio::test]
async fn out_of_range_hour_is_an_error_not_a_panic() {
    let st = store("memory://");
    let sp = SegmentSpec {
        partition_hour: HourBucket(i64::MAX / 3600),
        ..spec("p")
    };
    let e = st
        .write_segment(sp, input(vec![batch_from_rows(&rows(1, 1))]))
        .await
        .unwrap_err();
    assert!(matches!(e, StorageError::Model(ModelError::HourOutOfRange(_))), "{e:?}");
}

#[test]
fn urls() {
    let u = |s: &str| build_object_store(&StorageConfig { url: s.into() });
    assert!(matches!(
        u("s3://b/p"),
        Err(StorageError::Unimplemented(forge_model::error::Unimplemented("M5")))
    ));
    assert!(matches!(u("ftp://x"), Err(StorageError::Config { .. })));
    assert!(matches!(u("file://"), Err(StorageError::Config { .. })));
    assert!(u("memory://").unwrap().local_root.is_none());
}

#[test]
fn transient_errors() {
    use object_store::Error as E;
    use std::io::{Error, ErrorKind};
    let os = |e: E| StorageError::ObjectStore(e);
    assert!(
        os(E::Generic {
            store: "s",
            source: "boom".into()
        })
        .is_transient()
    );
    let path = || "p".to_string();
    let source = || -> Box<dyn std::error::Error + Send + Sync> { "x".into() };
    for e in [
        E::NotFound {
            path: path(),
            source: source(),
        },
        E::PermissionDenied {
            path: path(),
            source: source(),
        },
        E::AlreadyExists {
            path: path(),
            source: source(),
        },
    ] {
        assert!(!os(e).is_transient());
    }
    assert!(StorageError::Io(Error::from(ErrorKind::TimedOut)).is_transient());
    assert!(!StorageError::Io(Error::from(ErrorKind::PermissionDenied)).is_transient());
    assert!(!StorageError::EmptySegment.is_transient());
}

/// Plan §5.4: random multi-version rows over 3 projects -> the flush's merge (bounded batches) and per-project
/// split -> the real segment writer. The merge equals the reference; each slice is accepted (strict order, one row
/// per span), scans back unchanged, and its trace ranges tile it exactly, each range holding only its trace.
#[tokio::test]
async fn random_merge_passes_write_segment() {
    use forge_model::merge::{Tombstones, merge_versions_chunked, split_chunks_by_project};
    use forge_model::testing::{Rng, random_row, reference_merge, rows_from_batch};
    let st = store("memory://");
    for seed in 1..=12u64 {
        let mut r = Rng(seed * 7919 + 1);
        let mut rows = Vec::new();
        let mut version = 0;
        for _ in 0..(200 + r.below(1500)) {
            version += 1 + r.below(3);
            let project = ["alpha", "beta", "gamma"][r.below(3) as usize];
            let trace = r.below(25);
            let span = trace * 100 + r.below(12);
            rows.push(random_row(&mut r, project, trace, span, version));
        }
        // Memtable = unsorted batches in arrival order.
        for i in (1..rows.len()).rev() {
            rows.swap(i, r.below(i as u64 + 1) as usize);
        }
        let mut batches = Vec::new();
        let mut i = 0;
        while i < rows.len() {
            let n = (1 + r.below(40) as usize).min(rows.len() - i);
            batches.push(batch_from_rows(&rows[i..i + n]));
            i += n;
        }
        let merged = merge_versions_chunked(&batches, Tombstones::Keep, 8 << 10).unwrap();
        let all: Vec<SpanRow> = merged.iter().flat_map(|b| rows_from_batch(b).unwrap()).collect();
        assert_eq!(all, reference_merge(&rows), "seed {seed}");

        let parts = split_chunks_by_project(&merged).unwrap();
        assert_eq!(parts.len(), 3, "seed {seed}");
        for (p, slices) in parts {
            let want = concat_batches(&spans_schema(), &slices).unwrap();
            let w = st
                .write_segment(spec(p.as_str()), input(slices))
                .await
                .unwrap_or_else(|e| panic!("seed {seed} {p}: {e}"));
            assert_eq!(w.meta.row_count as usize, want.num_rows());
            let got = collect(st.scan(&w.meta, ScanRequest::default()).await.unwrap(), spans_schema()).await;
            assert_eq!(got, want, "seed {seed} {p}: scan");
            let want_rows = rows_from_batch(&want).unwrap();
            let mut next = 0;
            for t in &w.traces {
                assert_eq!(t.row_start, next, "seed {seed} {p}: ranges tile the slice");
                let range = t.row_start as usize..(t.row_start + t.row_count) as usize;
                assert!(
                    want_rows[range].iter().all(|x| x.trace_id == t.trace_id),
                    "seed {seed} {p}: range of {:?} holds other traces",
                    t.trace_id
                );
                next += t.row_count;
            }
            assert_eq!(next, w.meta.row_count, "seed {seed} {p}: ranges cover the slice");
        }
    }
}
