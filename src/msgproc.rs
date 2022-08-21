use crate::consumer::{IStreamConsumer, Message, StreamConsumerConfig};
use crate::context::Context;
use crate::processor::{DefaultProcessor, IProcessor, Processor};
use log::{debug, error, info};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::task;
use tokio_stream::StreamExt;

pub struct MsgProcConfig {
    consumer_config: Option<StreamConsumerConfig>,
    topics: Vec<String>,
    processor: Option<Box<dyn IProcessor>>,
    limit: usize,
}

impl Default for MsgProcConfig {
    fn default() -> Self {
        Self {
            consumer_config: Some(StreamConsumerConfig::new()),
            topics: vec![],
            processor: Some(Box::new(DefaultProcessor)),
            limit: 8,
        }
    }
}

impl MsgProcConfig {
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

    pub fn processor(&mut self, processor: impl IProcessor) -> &mut Self {
        self.processor = Some(Box::new(processor));
        self
    }

    pub fn limit(&mut self, limit: usize) -> &mut Self {
        self.limit = limit;
        self
    }

    pub fn create(&mut self) -> Result<MsgProc, &'static str> {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
        let mut consumer_config = self.consumer_config.take().unwrap();
        let stream_consumer = consumer_config.create().unwrap();
        stream_consumer
            .subscribe(&self.topics.iter().map(|x| x.as_str()).collect::<Vec<_>>())
            .unwrap();
        Ok(MsgProc {
            consumer: Arc::new(Box::new(stream_consumer)),
            processor: Arc::new(self.processor.take().unwrap()),
            semaphore: Arc::new(Semaphore::new(self.limit)),
            context: Context::new(notify_shutdown.clone()),
            shutdown_complete_rx: Some(shutdown_complete_rx),
            shutdown_complete_tx: Some(shutdown_complete_tx),
        })
    }
}

pub struct MsgProc {
    consumer: Arc<Box<dyn IStreamConsumer>>,
    processor: Arc<Box<dyn IProcessor>>,
    semaphore: Arc<Semaphore>,
    context: Context,
    shutdown_complete_rx: Option<mpsc::Receiver<()>>,
    shutdown_complete_tx: Option<mpsc::Sender<()>>,
}

impl MsgProc {
    async fn run_main_task(&mut self) {
        let mut stream = self.consumer.stream();
        loop {
            match stream.next().await {
                Some(Ok(msg)) => {
                    debug!("Read message from stream.");

                    let owned_msg = msg.detach();
                    let permit = self.semaphore.clone().acquire_owned().await.unwrap();
                    let mut processor = Processor::new(
                        self.processor.clone(),
                        self.context.clone(),
                        self.shutdown_complete_tx.clone().unwrap(),
                    );
                    task::spawn({
                        let consumer = self.consumer.clone();
                        let topic = msg.topic().to_string();
                        let partition = msg.partition();
                        let offset = msg.offset();
                        let mut context = self.context.clone();
                        async move {
                            if let Err(e) = processor.run(owned_msg).await {
                                error!("Failed to run processor.({})", e);
                                context.cancel().await;
                                drop(permit);
                                return;
                            }

                            if consumer.store_offset(&topic, partition, offset).is_err() {
                                error!(
                                    "Failed to store offset.(topic={}, partition={}, offset={}",
                                    topic, partition, offset
                                );
                                context.cancel().await;
                                drop(permit);
                                return;
                            }
                            drop(permit);
                        }
                    });
                }
                Some(Err(_)) => {
                    error!("KafkaError occurred.");
                    break;
                }
                None => {
                    debug!("Message does not exist in broker.");
                    continue;
                }
            }
        }
    }

    pub async fn run(&mut self, shutdown: impl Future) {
        info!("Start processing.");
        tokio::select! {
            _ = self.run_main_task() => {
                error!("Error occurred.");
            }
            _ = shutdown => {
                info!("Catch shutdown event. Shutdowning...");
            }
        }

        let shutdown_complete_tx = self.shutdown_complete_tx.take().unwrap();
        drop(shutdown_complete_tx);

        let mut shutdown_complete_rx = self.shutdown_complete_rx.take().unwrap();
        let _ = shutdown_complete_rx.recv().await;
    }
}
