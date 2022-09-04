use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Clone)]
pub(crate) struct Context {
    shutdown: Arc<AtomicBool>,
    notifier: broadcast::Sender<()>,
}

impl Context {
    pub fn new(notifier: broadcast::Sender<()>) -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            notifier,
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    pub async fn cancel(&mut self) {
        if self.is_shutdown() {
            return;
        }
        let _ = self.notifier.send(());
    }

    pub async fn done(&mut self) {
        if self.is_shutdown() {
            return;
        }

        let _ = self.notifier.subscribe().recv().await;

        self.shutdown.store(true, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Mutex;
    use tokio::task;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test() {
        let (tx, _) = broadcast::channel(1);
        let mut context = Context::new(tx);
        let count = Arc::new(Mutex::new(0));
        let task = {
            let count = count.clone();
            async move {
                loop {
                    let mut r_count = count.lock().await;
                    *r_count += 1;
                }
            }
        };
        task::spawn({
            let mut context = context.clone();
            async move {
                sleep(Duration::from_secs(1)).await;
                context.cancel().await;
            }
        });
        tokio::select! {
            _ = task => {},
            _ = context.done() => {}
        };
        let count = count.lock().await;
        assert!(*count > 0);
        assert_eq!(context.is_shutdown(), true);
    }
}
