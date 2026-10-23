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
