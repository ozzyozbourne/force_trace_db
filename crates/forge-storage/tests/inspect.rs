use forge_model::catalog::{HourBucket, Level, SegmentId};
use forge_model::merge::{Tombstones, merge_versions, sort_physical, split_by_project};
use forge_model::schema::spans_schema;
use forge_model::testing::{Rng, batch_from_rows, pid, random_row, reference_merge, rows_from_batch};
use forge_storage::inspect::{InspectOptions, inspect_segment};
use forge_storage::*;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tuned_strategy_and_inspect() {
    let mut r = Rng(11);
    let mut rows = Vec::new();
    for v in 1..=6000u64 {
        let t = r.below(60);
        let s = t * 64 + r.below(40);
        let mut row = random_row(&mut r, "alpha", t, s, v);
        if let Some(i) = row.inputs.as_mut() {
            i.push_str(&format!("{:>1000}", format!("\"pad{v}\"")));
        }
        rows.push(row);
    }
    let merged = merge_versions(&[batch_from_rows(&rows)], Tombstones::Keep).unwrap();
    assert_eq!(rows_from_batch(&merged).unwrap(), reference_merge(&rows));
    let (_, slice) = split_by_project(&merged).unwrap().remove(0);
    let dir = tempfile::tempdir().unwrap();
    let built = build_object_store(&forge_model::config::StorageConfig {
        url: format!("file://{}", dir.path().display()),
    })
    .unwrap();
    let st = VortexSegmentStore::new(built);
    let spec = SegmentSpec {
        id: SegmentId::new(),
        project_id: pid("alpha"),
        level: Level::L0,
        partition_hour: HourBucket(491234),
    };
    let chunks: Vec<_> = (0..slice.num_rows())
        .step_by(8192)
        .map(|o| Ok(slice.slice(o, 8192.min(slice.num_rows() - o))))
        .collect();
    let w = st
        .write_segment(spec, futures::stream::iter(chunks).boxed())
        .await
        .unwrap();
    let back: Vec<_> = st
        .scan(&w.meta, ScanRequest::default())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let back = arrow::compute::concat_batches(&spans_schema(), &back).unwrap();
    assert_eq!(rows_from_batch(&back).unwrap(), rows_from_batch(&slice).unwrap());
    let path = dir.path().join(&w.meta.path);
    let out = inspect_segment(
        &path,
        &InspectOptions {
            layout: false,
            verify: true,
        },
    )
    .await
    .unwrap();
    assert!(out.contains("contract:  OK"), "{out}");
    assert!(out.contains("\"segment_id\""), "{out}");
    assert!(out.contains("rows:"), "{out}");
    let lay = inspect_segment(
        &path,
        &InspectOptions {
            layout: true,
            verify: false,
        },
    )
    .await
    .unwrap();
    assert!(lay.contains("layout:"));
    let e = inspect_segment(
        &dir.path().join("nope.vortex"),
        &InspectOptions {
            layout: false,
            verify: false,
        },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(e, StorageError::Io(ref io) if io.kind() == std::io::ErrorKind::NotFound),
        "{e}"
    );
}

#[tokio::test]
async fn sorting_without_merging_is_rejected() {
    let mut r = Rng(42);
    let rows: Vec<_> = (0..50u64)
        .map(|v| random_row(&mut r, "p", v % 3, v % 5, v + 1))
        .collect();
    let unmerged = sort_physical(&batch_from_rows(&rows)).unwrap();
    let st = VortexSegmentStore::new(BuiltStore {
        store: Arc::new(object_store::memory::InMemory::new()),
        local_root: None,
    });
    let spec = SegmentSpec {
        id: SegmentId::new(),
        project_id: pid("p"),
        level: Level::L0,
        partition_hour: HourBucket(0),
    };
    let e = st
        .write_segment(spec, futures::stream::iter([Ok(unmerged)]).boxed())
        .await
        .unwrap_err();
    assert!(matches!(e, StorageError::Unsorted(_)), "{e}");
}

#[tokio::test]
async fn durable_local_fs_put_get_copy_multipart() {
    let dir = tempfile::tempdir().unwrap();
    let s = DurableLocalFs::new(dir.path().to_path_buf()).unwrap();
    let p = object_store::path::Path::from("a/b/c.bin");
    s.put(&p, b"hello".to_vec().into()).await.unwrap();
    assert_eq!(s.get(&p).await.unwrap().bytes().await.unwrap().as_ref(), b"hello");
    let opts = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    let e = s.put_opts(&p, b"x".to_vec().into(), opts.clone()).await.unwrap_err();
    assert!(matches!(e, object_store::Error::AlreadyExists { .. }), "{e}");
    let q = object_store::path::Path::from("x/y.bin");
    s.copy(&p, &q).await.unwrap();
    let mut up = s
        .put_multipart(&object_store::path::Path::from("m/n/o.bin"))
        .await
        .unwrap();
    up.put_part(vec![1u8; 10].into()).await.unwrap();
    up.complete().await.unwrap();
    assert_eq!(std::fs::read(dir.path().join("m/n/o.bin")).unwrap(), vec![1u8; 10]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inspect_rejects_a_non_spans_vortex_file() {
    use vortex::VortexSessionDefault;
    use vortex::array::stream::ArrayStreamAdapter;
    use vortex::arrow::ArrowSessionExt;
    use vortex::file::WriteOptionsSessionExt;
    use vortex::io::session::RuntimeSessionExt;
    let session = vortex::session::VortexSession::default().with_tokio();
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
        "x",
        arrow::datatypes::DataType::Int64,
        false,
    )]));
    let b = arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow::array::Int64Array::from(vec![1i64, 2, 3]))],
    )
    .unwrap();
    let dtype = session.arrow().from_arrow_schema(&schema).unwrap();
    let array = session.arrow().from_arrow_record_batch(b, &schema).unwrap();
    let mut buf = Vec::new();
    session
        .write_options()
        .write(
            &mut buf,
            ArrayStreamAdapter::new(dtype, futures::stream::iter([Ok(array)])),
        )
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("other.vortex");
    std::fs::write(&path, buf).unwrap();
    let e = inspect_segment(
        &path,
        &InspectOptions {
            layout: false,
            verify: true,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(e, StorageError::InvalidInput(_)), "{e}");
}

/// `AlreadyExists` on a Create put means another writer's object is acked as ours (content-addressed blobs), so the
/// existing file is fsynced first. Seen through a file that cannot be opened: the error surfaces instead of the
/// bare `AlreadyExists`.
#[tokio::test]
async fn durable_local_fs_already_exists_syncs_the_existing_object() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let s = DurableLocalFs::new(dir.path().to_path_buf()).unwrap();
    let p = object_store::path::Path::from("blobs/p/abc");
    let create = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    s.put_opts(&p, b"v".to_vec().into(), create.clone()).await.unwrap();
    let e = s.put_opts(&p, b"v".to_vec().into(), create.clone()).await.unwrap_err();
    assert!(matches!(e, object_store::Error::AlreadyExists { .. }), "{e}");
    let file = dir.path().join("blobs/p/abc");
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o000)).unwrap();
    let e = s.put_opts(&p, b"v".to_vec().into(), create).await.unwrap_err();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(
        !matches!(e, object_store::Error::AlreadyExists { .. }),
        "AlreadyExists returned without syncing the existing object"
    );
}
