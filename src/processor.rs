use crate::context::Context;
use crate::kafka::consumer::OwnedMessage;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

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

    pub async fn run(&mut self, msg: OwnedMessage) -> Result<(), &'static str> {
        tokio::select! {
            res = self.proc.execute(msg) => { res }
            _ = self.context.done() => { Err("Already canceled.") }
        }
    }
}

#[async_trait]
pub trait IProcessorMut: 'static + Send + Sync {
    async fn execute(&mut self, msg: OwnedMessage) -> Result<(), &'static str>;
}

pub struct DefaultProcessorMut;

#[async_trait]
impl IProcessorMut for DefaultProcessorMut {
    async fn execute(&mut self, _msg: OwnedMessage) -> Result<(), &'static str> {
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

    pub async fn run(&mut self, msg: OwnedMessage) -> Result<(), &'static str> {
        let mut processor_mut = self.proc.lock().await;
        tokio::select! {
            res = processor_mut.execute(msg) => { res }
            _ = self.context.done() => { Err("Already canceled.") }
        }
    }
}
