use crate::listeners::{ShutdownSignals, serve_axum, serve_grpc};
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
        Ok(Arc::new(Self {
            config,
            meta,
            segments: Arc::new(forge_storage::VortexSegmentStore::new(built)),
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

        // Any listener failure stops the others; join! (not try_join!) lets every listener drain.
        let guard = |r: anyhow::Result<()>| {
            if r.is_err() {
                self.shutdown.cancel();
            }
            r
        };
        let all = async {
            let (a, b, c, d) = tokio::join!(
                async { guard(serve_axum("ops", ops_l, ops_router(self.clone()), self.shutdown.clone()).await) },
                async { guard(serve_axum("api", api_l, api_router(self.clone()), self.shutdown.clone()).await) },
                async { guard(serve_axum("otlp-http", http_l, otlp::http_router(), self.shutdown.clone()).await) },
                async { guard(serve_grpc(grpc_inc, self.shutdown.clone()).await) },
            );
            a.and(b).and(c).and(d)
        };
        // Half-open connections (no h2 preface, partial request line) would block the drain forever.
        let deadline = async {
            self.shutdown.cancelled().await;
            tokio::time::sleep(s.shutdown_grace).await;
        };
        let res = tokio::select! {
            r = Box::pin(all) => r,
            () = deadline => {
                tracing::warn!(grace = ?s.shutdown_grace, "drain deadline exceeded; dropping open connections");
                Ok(())
            }
        };
        tracing::info!("all listeners stopped");
        res
    }
}
