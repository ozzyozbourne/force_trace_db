//! A group of tasks that start together and drain together. Any task error cancels `on_error`
//! (= Forge.shutdown), so one failing listener stops the process in the defined order.
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Clone)]
pub(crate) struct Tasks {
    tracker: TaskTracker,
    on_error: CancellationToken,
    first_err: Arc<Mutex<Option<anyhow::Error>>>,
}
impl Tasks {
    pub fn new(on_error: CancellationToken) -> Self {
        Self {
            tracker: TaskTracker::new(),
            on_error,
            first_err: Arc::default(),
        }
    }
    pub fn spawn(&self, name: &'static str, fut: impl Future<Output = anyhow::Result<()>> + Send + 'static) {
        let (tok, err) = (self.on_error.clone(), self.first_err.clone());
        self.tracker.spawn(async move {
            if let Err(e) = fut.await {
                tracing::error!(name, error = %e, "task failed; shutting down");
                err.lock().unwrap_or_else(|p| p.into_inner()).get_or_insert(e);
                tok.cancel();
            }
        });
    }
    /// Close + wait for all tasks.
    pub async fn wait(&self) {
        self.tracker.close();
        self.tracker.wait().await
    }
    pub fn take_error(&self) -> Option<anyhow::Error> {
        self.first_err.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}
