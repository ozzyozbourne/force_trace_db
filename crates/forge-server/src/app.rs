use crate::listeners::{ShutdownSignals, serve_axum, serve_grpc};
use crate::tasks::Tasks;
use crate::{api_router, ops_router, otlp};
use forge_model::config::ForgeConfig;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::server::TcpIncoming;

pub struct Forge {
    pub config: ForgeConfig,
    pub meta: Arc<dyn forge_meta::Metastore>,
    /// Built at startup: validates the storage URL and creates the local directory.
    pub segments: Arc<dyn forge_storage::SegmentStore>,
    pub ingest: Arc<forge_ingest::Pipeline>,
    pub ready: AtomicBool,
    pub shutdown: CancellationToken,
}

pub type AppState = Arc<Forge>;

impl Forge {
    /// Opens (and migrates) the metastore, then builds the object store and segment store.
    pub async fn open(config: ForgeConfig) -> anyhow::Result<Arc<Self>> {
        let meta = forge_meta::open(&config.metastore).await?;
        // Directory creation is blocking work, like the metastore open.
        let storage = config.storage.clone();
        let built = tokio::task::spawn_blocking(move || forge_storage::build_object_store(&storage)).await??;
        let segments: Arc<dyn forge_storage::SegmentStore> = Arc::new(forge_storage::VortexSegmentStore::new(built));
        let (ingest, _report) = forge_ingest::Pipeline::open(&config, meta.clone(), segments.clone()).await?;
        Ok(Arc::new(Self {
            config,
            meta,
            segments,
            ingest,
            ready: AtomicBool::new(false),
            shutdown: CancellationToken::new(),
        }))
    }

    /// Binds all four listeners, sets ready, and runs until the first signal (or a listener error).
    /// Draining is bounded by `server.shutdown_grace`; a second signal exits the process with 130.
    pub async fn serve(self: Arc<Self>, mut signals: ShutdownSignals) -> anyhow::Result<()> {
        let s = &self.config.server;
        let (ops_l, api_l, http_l) = (
            TcpListener::bind(s.admin_addr).await?,
            TcpListener::bind(s.api_addr).await?,
            TcpListener::bind(s.otlp_http_addr).await?,
        );
        let grpc_inc = TcpIncoming::bind(s.otlp_grpc_addr)?;
        let max = self.config.ingest.max_request_bytes.as_u64() as usize;
        let receiver = otlp::OtlpReceiver::new(self.ingest.clone(), &self.config.ingest)?;
        let (listeners, background) = (Tasks::new(self.shutdown.clone()), Tasks::new(self.shutdown.clone()));
        let stop_flusher = CancellationToken::new();
        {
            let (p, t) = (self.ingest.clone(), stop_flusher.clone());
            background.spawn("flusher", async move {
                forge_ingest::run_flusher(p, t).await;
                Ok(())
            });
        }
        listeners.spawn(
            "ops",
            serve_axum("ops", ops_l, ops_router(self.clone()), self.shutdown.clone()),
        );
        listeners.spawn(
            "api",
            serve_axum("api", api_l, api_router(self.clone()), self.shutdown.clone()),
        );
        listeners.spawn(
            "otlp-http",
            serve_axum(
                "otlp-http",
                http_l,
                otlp::http_router(receiver.clone(), max),
                self.shutdown.clone(),
            ),
        );
        listeners.spawn(
            "otlp-grpc",
            serve_grpc(grpc_inc, otlp::grpc_service(receiver, max), self.shutdown.clone()),
        );
        self.ready.store(true, Ordering::Release);

        let tok = self.shutdown.clone();
        tokio::spawn(async move {
            signals.recv().await;
            tracing::info!("shutdown signal received; draining");
            tok.cancel();
            signals.recv().await;
            tracing::warn!("second signal; exiting now");
            std::process::exit(130);
        });

        self.shutdown.cancelled().await;
        let grace = s.shutdown_grace;
        if tokio::time::timeout(grace, listeners.wait()).await.is_err() {
            tracing::warn!(?grace, "drain deadline exceeded; dropping open connections");
        }
        tracing::info!("all listeners stopped");
        self.ingest.close_wal().await;
        stop_flusher.cancel();
        match tokio::time::timeout(grace, async {
            background.wait().await;
            self.ingest.flush_all().await
        })
        .await
        {
            Ok(Ok(())) => tracing::info!("final flush done"),
            Ok(Err(e)) => tracing::warn!(error = %e, "final flush failed; rows replay from the WAL on start"),
            Err(_) => tracing::warn!(?grace, "final flush timed out; rows replay from the WAL on start"),
        }
        listeners
            .take_error()
            .or_else(|| background.take_error())
            .map_or(Ok(()), Err)
    }
}
