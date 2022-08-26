use crate::consumer::{Consumer, ConsumerBase, ConsumerMut, IConsumer};
use crate::context::Context;
use crate::kafka::config::StreamConsumerConfig;
use crate::options::AnyOptions;
use crate::processor::{
    DefaultProcessor, DefaultProcessorMut, IProcessor, IProcessorMut, Processor, ProcessorMut,
};
use async_trait::async_trait;
use log::{error, info};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Mutex, Semaphore};

macro_rules! create_base_consumer {
    ($config: expr) => {
        ConsumerBase::new(
            $config.consumer_config.clone().unwrap().create().unwrap(),
            $config.context.clone(),
            $config.shutdown_complete_rx.take().unwrap(),
            $config.shutdown_complete_tx.take().unwrap(),
        )
    };
}

macro_rules! run_imsgproc {
    ($consumer: expr, $shutdown: expr) => {
        info!("Start processing.");
        tokio::select! {
            _ = $consumer.run_main_consume() => {
                error!("Error occurred.");
            }
            _ = $shutdown => {
                info!("Catch shutdown event. Shutdowning...");
            }
        }
        $consumer.wait().await;
    };
}

#[async_trait]
pub trait IMsgProc {
    type TConsumer: IConsumer;

    async fn run(self, shutdown: impl Future + Send);
}
pub struct MsgProcConfig<M>
where
    M: IMsgProc,
{
    consumer_config: Option<StreamConsumerConfig>,
    topics: Vec<String>,
    processor: <M::TConsumer as IConsumer>::TProcessor,
    context: Context,
    shutdown_complete_rx: Option<mpsc::Receiver<()>>,
    shutdown_complete_tx: Option<mpsc::Sender<()>>,
    options: AnyOptions,
}

impl<M> MsgProcConfig<M>
where
    M: IMsgProc,
{
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
}

impl Default for MsgProcConfig<MsgProc> {
    fn default() -> Self {
        let mut options = AnyOptions::new();
        options.set::<usize>(Self::LIMIT, 8);
        let (notify_shutdown, _) = broadcast::channel(1);
        let context = Context::new(notify_shutdown.clone());
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
        let processor = Processor::new(
            Arc::new(Box::new(DefaultProcessor)),
            context.clone(),
            shutdown_complete_tx.clone(),
        );
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

impl Default for MsgProcConfig<MsgProcMut> {
    fn default() -> Self {
        let mut options = AnyOptions::new();
        options.set::<usize>(Self::BUFFER_SIZE, 64);
        let (notify_shutdown, _) = broadcast::channel(1);
        let context = Context::new(notify_shutdown.clone());
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
        let processor = ProcessorMut::new(
            Arc::new(Mutex::new(Box::new(DefaultProcessorMut))),
            context.clone(),
            shutdown_complete_tx.clone(),
        );
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

impl MsgProcConfig<MsgProc> {
    const LIMIT: &'static str = "limit";

    pub fn new() -> Self {
        Self::default()
    }

    pub fn limit(&mut self, limit: usize) -> &mut Self {
        self.options.set(Self::LIMIT, limit);
        self
    }

    pub fn processor(&mut self, processor: impl IProcessor) -> &mut Self {
        let processor = Processor::new(
            Arc::new(Box::new(processor)),
            self.context.clone(),
            self.shutdown_complete_tx.clone().unwrap(),
        );
        self.processor = processor;
        self
    }

    pub fn create(&mut self) -> MsgProc {
        let base = create_base_consumer!(self);
        let limit = self.options.get(Self::LIMIT);
        let semaphore = Arc::new(Semaphore::new(limit));
        MsgProc {
            base,
            processor: self.processor.clone(),
            semaphore,
        }
    }
}

impl MsgProcConfig<MsgProcMut> {
    const BUFFER_SIZE: &'static str = "buffer_size";

    pub fn new() -> Self {
        Self::default()
    }

    pub fn processor_mut(&mut self, processor_mut: impl IProcessorMut) -> &mut Self {
        let processor_mut = ProcessorMut::new(
            Arc::new(Mutex::new(Box::new(processor_mut))),
            self.context.clone(),
            self.shutdown_complete_tx.clone().unwrap(),
        );
        self.processor = processor_mut;
        self
    }

    pub fn create(&mut self) -> MsgProcMut {
        let base = create_base_consumer!(self);
        let buffer_size = self.options.get(Self::BUFFER_SIZE);
        MsgProcMut {
            base,
            processor_mut: self.processor.clone(),
            buffer_size,
        }
    }
}

pub struct MsgProc {
    base: ConsumerBase,
    processor: Processor,
    semaphore: Arc<Semaphore>,
}

#[async_trait]
impl IMsgProc for MsgProc {
    type TConsumer = Consumer;

    async fn run(self, shutdown: impl Future + Send) {
        let mut consumer = Consumer::new(self.base, self.processor, self.semaphore);
        run_imsgproc!(consumer, shutdown);
    }
}

pub struct MsgProcMut {
    base: ConsumerBase,
    processor_mut: ProcessorMut,
    buffer_size: usize,
}

#[async_trait]
impl IMsgProc for MsgProcMut {
    type TConsumer = ConsumerMut;

    async fn run(self, shutdown: impl Future + Send) {
        let mut consumer_mut = ConsumerMut::new(self.base, self.processor_mut, self.buffer_size);
        run_imsgproc!(consumer_mut, shutdown);
    }
}
