use crate::consumer::Consumer;
use crate::context::Context;
use crate::kafka::config::StreamConsumerConfig;
use crate::kafka::consumer::IStreamConsumer;
use crate::options::AnyOptions;
use crate::processor::{DefaultProcessor, IProcessor};
use log::{error, info};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Mutex};

pub struct MsgProcConfig {
    consumer_config: Option<StreamConsumerConfig>,
    topics: Vec<String>,
    processor: Arc<Mutex<Box<dyn IProcessor>>>,
    context: Context,
    shutdown_complete_rx: Option<mpsc::Receiver<()>>,
    shutdown_complete_tx: Option<mpsc::Sender<()>>,
    options: AnyOptions,
}

impl MsgProcConfig {
    const PROCESSOR_BUFFER_SIZE: &'static str = "processor_buffer_size";

    pub fn new() -> Self {
        Self::default()
    }

    pub fn set<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.consumer_config
            .as_mut()
            .unwrap()
            .set(key.into(), value.into());
        self
    }

    pub fn topics(&mut self, topics: &[&str]) -> &mut Self {
        self.topics = topics.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        self
    }

    pub fn consumer_buffer_size(&mut self, consumer_buffer_size: usize) -> &mut Self {
        self.consumer_config
            .as_mut()
            .unwrap()
            .set_buffer_size(consumer_buffer_size);
        self
    }

    pub fn processor_buffer_size(&mut self, processor_buffer_size: usize) -> &mut Self {
        self.options
            .set(Self::PROCESSOR_BUFFER_SIZE, processor_buffer_size);
        self
    }

    pub fn processor(&mut self, processor: impl IProcessor) -> &mut Self {
        self.processor = Arc::new(Mutex::new(Box::new(processor)));
        self
    }

    pub fn create(&mut self) -> MsgProc {
        let buffer_size = self.options.get(Self::PROCESSOR_BUFFER_SIZE);
        MsgProc {
            consumer: Arc::new(Box::new(
                self.consumer_config.clone().unwrap().create().unwrap(),
            )),
            context: self.context.clone(),
            shutdown_complete_rx: self.shutdown_complete_rx.take(),
            shutdown_complete_tx: self.shutdown_complete_tx.take(),
            processor: self.processor.clone(),
            buffer_size,
        }
    }
}

impl Default for MsgProcConfig {
    fn default() -> Self {
        let mut options = AnyOptions::new();
        options.set::<usize>(Self::PROCESSOR_BUFFER_SIZE, 64);
        let (notify_shutdown, _) = broadcast::channel(1);
        let context = Context::new(notify_shutdown.clone());
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
        let processor: Arc<Mutex<Box<dyn IProcessor>>> =
            Arc::new(Mutex::new(Box::new(DefaultProcessor)));
        Self {
            consumer_config: Some(StreamConsumerConfig::new()),
            topics: vec![],
            processor,
            context,
            shutdown_complete_rx: Some(shutdown_complete_rx),
            shutdown_complete_tx: Some(shutdown_complete_tx),
            options,
        }
    }
}

pub struct MsgProc {
    consumer: Arc<Box<dyn IStreamConsumer>>,
    context: Context,
    shutdown_complete_rx: Option<mpsc::Receiver<()>>,
    shutdown_complete_tx: Option<mpsc::Sender<()>>,
    processor: Arc<Mutex<Box<dyn IProcessor>>>,
    buffer_size: usize,
}

impl MsgProc {
    pub async fn run(self, shutdown: impl Future + Send) {
        let mut consumer = Consumer::new(
            self.consumer,
            self.context,
            self.shutdown_complete_rx.unwrap(),
            self.shutdown_complete_tx.unwrap(),
            self.processor,
            self.buffer_size,
        );
        info!("Start processing.");
        tokio::select! {
            _ = consumer.run_main_consume() => {
                error!("Error occurred.");
            }
            _ = shutdown => {
                info!("Catch shutdown event. Shutdowning...");
            }
        }
        consumer.wait().await;
    }
}
