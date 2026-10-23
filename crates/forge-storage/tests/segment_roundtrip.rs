use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use forge_model::catalog::{HourBucket, Level, SegmentId, paths};
use forge_model::config::StorageConfig;
use forge_model::dotted::dotted_order;
use forge_model::ids::{SpanId, TraceId};
use forge_model::row::{SpanBatchBuilder, SpanRow};
use forge_model::schema::{col, col_idx, projected_schema, spans_schema};
use forge_model::testing::pid;
use forge_storage::*;
use futures::{TryStreamExt, stream};
use object_store::ObjectStoreExt;

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

fn to_batch(rows: &[SpanRow]) -> RecordBatch {
    let mut b = SpanBatchBuilder::with_capacity(rows.len());
    for r in rows {
        b.push(r).unwrap();
    }
    b.finish().unwrap()
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
    let all = to_batch(&rows(10, 10));
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
    assert_eq!((m.min_trace_id, m.max_trace_id), (trace(0), trace(9)));
    assert_eq!((m.min_version, m.max_version), (1, 100));
    assert_eq!(
        m.path,
        paths::segment_path(&sp.project_id, sp.level, sp.partition_hour, sp.id)
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
    let all = to_batch(&rows(2_000, 10));
    let w = st.write_segment(spec("p"), input(vec![all.clone()])).await.unwrap();
    assert_eq!(w.meta.row_count, 20_000);
    assert_eq!(scan_range(&st, &w.meta, 8190..8200).await, all.slice(8190, 10));
    assert_eq!(scan_range(&st, &w.meta, 19_995..20_000).await, all.slice(19_995, 5));
}

#[tokio::test]
async fn file_store_writes_under_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let st = store(&format!("file://{}/data", tmp.path().display()));
    let all = to_batch(&rows(3, 4));
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
    let all = to_batch(&r);

    let mut reversed = r.clone();
    reversed.reverse();
    let e = st
        .write_segment(spec("p"), input(vec![to_batch(&reversed)]))
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
    let e = st.write_segment(sp, input(vec![all])).await.unwrap_err();
    assert!(
        matches!(e, StorageError::ObjectStore(object_store::Error::AlreadyExists { .. })),
        "{e:?}"
    );
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
