use axum::Router;
use tokio::net::TcpListener;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tonic::transport::server::TcpIncoming;

/// SIGTERM + SIGINT streams. Created before `Forge::open`, so a signal during startup is queued, not fatal.
pub struct ShutdownSignals {
    term: Signal,
    int: Signal,
}

impl ShutdownSignals {
    pub fn install() -> std::io::Result<Self> {
        Ok(Self {
            term: signal(SignalKind::terminate())?,
            int: signal(SignalKind::interrupt())?,
        })
    }

    /// Resolves on the next SIGTERM or SIGINT.
    pub async fn recv(&mut self) {
        tokio::select! {
            _ = self.term.recv() => {}
            _ = self.int.recv() => {}
        }
    }
}

pub async fn serve_axum(name: &'static str, l: TcpListener, app: Router, tok: CancellationToken) -> anyhow::Result<()> {
    tracing::info!(name, addr = %l.local_addr()?, "listening");
    axum::serve(l, app)
        .with_graceful_shutdown(tok.cancelled_owned())
        .await?;
    tracing::info!(name, "stopped");
    Ok(())
}

pub async fn serve_grpc(
    inc: TcpIncoming,
    svc: opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer<
        crate::otlp::OtlpReceiver,
    >,
    tok: CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!(name = "otlp-grpc", addr = %inc.local_addr()?, "listening");
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve_with_incoming_shutdown(inc, tok.cancelled_owned())
        .await?;
    tracing::info!(name = "otlp-grpc", "stopped");
    Ok(())
}
