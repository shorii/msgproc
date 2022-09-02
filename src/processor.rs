use crate::context::Context;
use crate::kafka::message::Message;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

#[async_trait]
pub trait IProcessor: 'static + Send + Sync {
    async fn execute(&self, msg: Message) -> Result<(), &'static str>;
}

pub struct DefaultProcessor;

#[async_trait]
impl IProcessor for DefaultProcessor {
    async fn execute(&self, _msg: Message) -> Result<(), &'static str> {
        // noop
        Ok(())
    }
}

#[derive(Clone)]
pub struct Processor {
    proc: Arc<Box<dyn IProcessor>>,
    context: Context,
    _shutdown_complete_tx: mpsc::Sender<()>,
}

impl Processor {
    pub fn new(
        proc: Arc<Box<dyn IProcessor>>,
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
        tokio::select! {
            res = self.proc.execute(msg) => { res }
            _ = self.context.done() => { Err("Already canceled.") }
        }
    }
}

#[async_trait]
pub trait IProcessorMut: 'static + Send + Sync {
    async fn execute(&mut self, msg: Message) -> Result<(), &'static str>;
}

pub struct DefaultProcessorMut;

#[async_trait]
impl IProcessorMut for DefaultProcessorMut {
    async fn execute(&mut self, _msg: Message) -> Result<(), &'static str> {
        // noop
        Ok(())
    }
}

#[derive(Clone)]
pub struct ProcessorMut {
    proc: Arc<Mutex<Box<dyn IProcessorMut>>>,
    context: Context,
    _shutdown_complete_tx: mpsc::Sender<()>,
}

impl ProcessorMut {
    pub fn new(
        proc: Arc<Mutex<Box<dyn IProcessorMut>>>,
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
        let mut processor_mut = self.proc.lock().await;
        tokio::select! {
            res = processor_mut.execute(msg) => { res }
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
            async fn execute(&self, _msg: Message) -> Result<(), &'static str> {
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
                proc: Arc::new(Box::new(HeavyComputingProcessor)),
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
                proc: Arc::new(Box::new(HeavyComputingProcessor)),
                _shutdown_complete_tx: shudown_complete_tx,
            };
            drop(processor);
            assert!(shutdown_complete_rx.recv().await.is_none());
        }
    }

    mod processor_mut {
        use super::*;
        pub struct HeavyComputingProcessorMut;

        #[async_trait]
        impl IProcessorMut for HeavyComputingProcessorMut {
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
            let mut processor_mut = ProcessorMut {
                context: context.clone(),
                proc: Arc::new(Mutex::new(Box::new(HeavyComputingProcessorMut))),
                _shutdown_complete_tx: shudown_complete_tx,
            };
            task::spawn({
                let mut context = context.clone();
                async move {
                    sleep(Duration::from_secs(1)).await;
                    context.cancel().await;
                }
            });
            assert!(processor_mut
                .run(create_message("topic", 0, 0, "payload"))
                .await
                .is_err());
        }

        #[tokio::test]
        async fn test_shutdown_complete_tx() {
            let (tx, _) = broadcast::channel(1);
            let context = Context::new(tx);
            let (shudown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
            let processor = ProcessorMut {
                context: context.clone(),
                proc: Arc::new(Mutex::new(Box::new(HeavyComputingProcessorMut))),
                _shutdown_complete_tx: shudown_complete_tx,
            };
            drop(processor);
            assert!(shutdown_complete_rx.recv().await.is_none());
        }
    }
}
