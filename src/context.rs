use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Clone)]
pub(crate) struct Context {
    shutdown: Arc<AtomicBool>,
    notifier: broadcast::Sender<()>,
}

impl Context {
    pub(crate) fn new(notifier: broadcast::Sender<()>) -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            notifier,
        }
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    pub(crate) async fn cancel(&mut self) {
        if self.is_shutdown() {
            return;
        }
        let _ = self.notifier.send(());
    }

    pub(crate) async fn done(&mut self) {
        if self.is_shutdown() {
            return;
        }

        let _ = self.notifier.subscribe().recv().await;

        self.shutdown.store(true, Ordering::SeqCst);
    }
}
