use crate::consumer::OwnedMessage;
use crate::context::Context;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;

#[async_trait]
pub trait IProcessor: 'static + Send + Sync {
    async fn execute(&self, msg: OwnedMessage) -> Result<(), &'static str>;
}

pub struct DefaultProcessor;

#[async_trait]
impl IProcessor for DefaultProcessor {
    async fn execute(&self, _msg: OwnedMessage) -> Result<(), &'static str> {
        // noop
        Ok(())
    }
}

pub(crate) struct Processor {
    proc: Arc<Box<dyn IProcessor>>,
    context: Context,
    _shutdown_complete_tx: mpsc::Sender<()>,
}

impl Processor {
    pub(crate) fn new(
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

    pub(crate) async fn run(&mut self, msg: OwnedMessage) -> Result<(), &'static str> {
        tokio::select! {
            res = self.proc.execute(msg) => { res }
            _ = self.context.done() => { Err("Already canceled.") }
        }
    }
}
