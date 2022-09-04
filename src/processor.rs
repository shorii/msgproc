use crate::context::Context;
use crate::kafka::message::Message;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

/// [crate::msgproc::MsgProc]でProcessorを扱う際の一般的な表現
#[async_trait]
pub trait IProcessor: 'static + Send + Sync {
    async fn execute(&mut self, msg: Message) -> Result<(), &'static str>;
}

pub(crate) struct DefaultProcessor;

#[async_trait]
impl IProcessor for DefaultProcessor {
    async fn execute(&mut self, _msg: Message) -> Result<(), &'static str> {
        // noop
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct Processor {
    proc: Arc<Mutex<Box<dyn IProcessor>>>,
    context: Context,
    _shutdown_complete_tx: mpsc::Sender<()>,
}

impl Processor {
    pub fn new(
        proc: Arc<Mutex<Box<dyn IProcessor>>>,
        context: Context,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
            proc,
            context,
            _shutdown_complete_tx: shutdown_complete_tx,
        }
    }

    pub async fn run(&mut self, msg: Message) -> Result<(), &'static str> {
        let mut processor = self.proc.lock().await;
        tokio::select! {
            res = processor.execute(msg) => { res }
            _ = self.context.done() => { Err("Already canceled.") }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::message::testing::create_message;
    use tokio::sync::broadcast;
    use tokio::task;
    use tokio::time::{sleep, Duration};

    mod processor {
        use super::*;
        pub struct HeavyComputingProcessor;

        #[async_trait]
        impl IProcessor for HeavyComputingProcessor {
            async fn execute(&mut self, _msg: Message) -> Result<(), &'static str> {
                loop {
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }

        #[tokio::test]
        async fn test_context() {
            let (tx, _) = broadcast::channel(1);
            let context = Context::new(tx);
            let (shudown_complete_tx, _) = mpsc::channel(1);
            let mut processor = Processor {
                context: context.clone(),
                proc: Arc::new(Mutex::new(Box::new(HeavyComputingProcessor))),
                _shutdown_complete_tx: shudown_complete_tx,
            };
            task::spawn({
                let mut context = context.clone();
                async move {
                    sleep(Duration::from_secs(1)).await;
                    context.cancel().await;
                }
            });
            assert!(processor
                .run(create_message("topic", 0, 0, "payload"))
                .await
                .is_err());
        }

        #[tokio::test]
        async fn test_shutdown_complete_tx() {
            let (tx, _) = broadcast::channel(1);
            let context = Context::new(tx);
            let (shudown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
            let processor = Processor {
                context: context.clone(),
                proc: Arc::new(Mutex::new(Box::new(HeavyComputingProcessor))),
                _shutdown_complete_tx: shudown_complete_tx,
            };
            drop(processor);
            assert!(shutdown_complete_rx.recv().await.is_none());
        }
    }
}
