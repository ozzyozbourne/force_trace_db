//! Test wrappers around a `SegmentStore` (feature `test-util`).
use crate::{BatchStream, ScanRequest, SegmentSpec, SegmentStore, StorageResult, VortexSegmentStore, WrittenSegment};
use async_trait::async_trait;
use forge_model::catalog::SegmentMeta;
use forge_model::config::StorageConfig;
use futures::future::BoxFuture;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A Vortex segment store over `memory://`.
pub fn memory_store() -> Arc<dyn SegmentStore> {
    let built = crate::build_object_store(&StorageConfig {
        url: "memory://".into(),
    })
    .expect("memory store");
    Arc::new(VortexSegmentStore::new(built))
}

/// Runs before each `write_segment`; an error is returned instead of writing.
pub type WriteHook = Box<dyn Fn(&SegmentSpec) -> BoxFuture<'static, StorageResult<()>> + Send + Sync>;

/// Delegates to `inner`; `before_write` runs before each `write_segment` (inject failures or delays).
pub struct HookedSegmentStore {
    pub inner: Arc<dyn SegmentStore>,
    before_write: WriteHook,
}

impl HookedSegmentStore {
    pub fn new(inner: Arc<dyn SegmentStore>, before_write: WriteHook) -> Self {
        Self { inner, before_write }
    }
}

impl std::fmt::Debug for HookedSegmentStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HookedSegmentStore")
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait]
impl SegmentStore for HookedSegmentStore {
    async fn write_segment(&self, spec: SegmentSpec, rows: BatchStream) -> StorageResult<WrittenSegment> {
        (self.before_write)(&spec).await?;
        self.inner.write_segment(spec, rows).await
    }
    async fn scan(&self, seg: &SegmentMeta, req: ScanRequest) -> StorageResult<BatchStream> {
        self.inner.scan(seg, req).await
    }
    fn object_store(&self) -> Arc<dyn object_store::ObjectStore> {
        self.inner.object_store()
    }
}

/// Delegates to `inner`, records every scan's projection, and optionally sleeps before each scan.
#[derive(Debug)]
pub struct RecordingSegmentStore {
    inner: Arc<dyn SegmentStore>,
    delay: Duration,
    projections: Mutex<Vec<Vec<&'static str>>>,
}

impl RecordingSegmentStore {
    pub fn new(inner: Arc<dyn SegmentStore>) -> Self {
        Self {
            inner,
            delay: Duration::ZERO,
            projections: Mutex::default(),
        }
    }
    pub fn slow(inner: Arc<dyn SegmentStore>, delay: Duration) -> Self {
        Self {
            delay,
            ..Self::new(inner)
        }
    }
    /// The projections scanned since the last call (None = all columns).
    pub fn take_projections(&self) -> Vec<Vec<&'static str>> {
        std::mem::take(&mut self.projections.lock().unwrap())
    }
}

#[async_trait]
impl SegmentStore for RecordingSegmentStore {
    async fn write_segment(&self, spec: SegmentSpec, rows: BatchStream) -> StorageResult<WrittenSegment> {
        self.inner.write_segment(spec, rows).await
    }
    async fn scan(&self, seg: &SegmentMeta, req: ScanRequest) -> StorageResult<BatchStream> {
        let cols = req
            .projection
            .clone()
            .unwrap_or_else(|| forge_model::schema::ALL_COLUMNS.to_vec());
        self.projections.lock().unwrap().push(cols);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        self.inner.scan(seg, req).await
    }
    fn object_store(&self) -> Arc<dyn object_store::ObjectStore> {
        self.inner.object_store()
    }
}

/// Models S3: every read request (`get_opts`, which `head` and `get_range` go through, and `get_ranges`) sleeps
/// `delay` first; `get_ranges` counts as one request wave (its ranges go in parallel). Writes are not delayed.
/// The delay can change at run time (0 while loading data).
#[derive(Debug)]
pub struct LatencyStore {
    inner: Arc<dyn object_store::ObjectStore>,
    delay_us: std::sync::atomic::AtomicU64,
    /// Read requests so far.
    pub gets: std::sync::atomic::AtomicU64,
}

mod latency {
    use super::LatencyStore;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
        PutOptions, PutPayload, PutResult, Result,
    };
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    impl LatencyStore {
        pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                delay_us: 0.into(),
                gets: 0.into(),
            }
        }
        pub fn set_delay(&self, d: Duration) {
            self.delay_us
                .store(u64::try_from(d.as_micros()).unwrap_or(u64::MAX), Ordering::Relaxed);
        }
        async fn read(&self) {
            self.gets.fetch_add(1, Ordering::Relaxed);
            let us = self.delay_us.load(Ordering::Relaxed);
            if us > 0 {
                tokio::time::sleep(Duration::from_micros(us)).await;
            }
        }
    }

    impl std::fmt::Display for LatencyStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "LatencyStore({})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for LatencyStore {
        async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            self.read().await;
            self.inner.get_opts(location, options).await
        }
        /// One wave: the default would re-enter `get_range` (and the delay) per coalesced range.
        async fn get_ranges(&self, location: &Path, ranges: &[std::ops::Range<u64>]) -> Result<Vec<bytes::Bytes>> {
            self.read().await;
            self.inner.get_ranges(location, ranges).await
        }
        fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }
}
