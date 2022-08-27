use crate::context::Context;
use crate::kafka::consumer::{IStreamConsumer, Message, OwnedMessage};
use crate::processor::{Processor, ProcessorMut};
use async_trait::async_trait;
use log::{debug, error};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{mpsc, Semaphore};
use tokio::task;
use tokio_stream::StreamExt;

#[derive(Error, Debug)]
#[error("ConsumeError occurred.")]
pub struct ConsumeError {
    msg: String,
    #[source]
    source: anyhow::Error,
}

macro_rules! raise_consume_error {
    ($msg: expr, $source: expr) => {
        return Err(ConsumeError {
            msg: $msg.to_string(),
            source: $source.into(),
        });
    };
}

macro_rules! consume_loop {
    (($stream: expr, $context: expr) => $proc: expr) => {
        loop {
            if $context.is_shutdown() {
                break;
            }
            match $stream.next().await {
                Some(Ok(msg)) => {
                    debug!("Read message from stream.");
                    let owned_msg = msg.detach();
                    if $proc(owned_msg).await.is_err() {
                        error!("Processor Error occurred.");
                        break;
                    }
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
    };
}

pub struct ConsumerBase {
    consumer: Arc<Box<dyn IStreamConsumer>>,
    context: Context,
    shutdown_complete_rx: Option<mpsc::Receiver<()>>,
    shutdown_complete_tx: Option<mpsc::Sender<()>>,
}

impl ConsumerBase {
    pub(crate) fn new(
        consumer: impl IStreamConsumer,
        context: Context,
        shutdown_complete_rx: mpsc::Receiver<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
            consumer: Arc::new(Box::new(consumer)),
            context,
            shutdown_complete_rx: Some(shutdown_complete_rx),
            shutdown_complete_tx: Some(shutdown_complete_tx),
        }
    }
}

#[async_trait]
pub trait IConsumer {
    type TProcessor;
    async fn run_main_consume(&mut self);
    async fn wait(&mut self);
}

pub struct Consumer {
    base: ConsumerBase,
    processor: Processor,
    semaphore: Arc<Semaphore>,
}

impl Consumer {
    pub fn new(base: ConsumerBase, processor: Processor, semaphore: Arc<Semaphore>) -> Self {
        Self {
            base,
            processor,
            semaphore,
        }
    }
}

#[async_trait]
impl IConsumer for Consumer {
    type TProcessor = Processor;

    async fn run_main_consume(&mut self) {
        let mut stream = self.base.consumer.stream();
        consume_loop!((stream, self.base.context) => |msg: OwnedMessage| async {
            let permit = match self.semaphore.clone().acquire_owned().await {
                Ok(p) => { p },
                Err(e) => {
                    raise_consume_error!("Failed to acquire permit.", e);
                }
            };
            let mut processor = self.processor.clone();
            task::spawn({
                let consumer = self.base.consumer.clone();
                let topic = msg.topic().to_string();
                let partition = msg.partition();
                let offset = msg.offset();
                let mut context = self.base.context.clone();
                async move {
                    if let Err(e) = processor.run(msg).await {
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
            Ok(())
        });
    }

    async fn wait(&mut self) {
        let shutdown_complete_tx = self.base.shutdown_complete_tx.take().unwrap();
        drop(shutdown_complete_tx);

        let mut shutdown_complete_rx = self.base.shutdown_complete_rx.take().unwrap();
        let _ = shutdown_complete_rx.recv().await;
    }
}

pub struct ConsumerMut {
    base: ConsumerBase,
    processor_mut: ProcessorMut,
    buffer_size: usize,
}

impl ConsumerMut {
    pub fn new(base: ConsumerBase, processor_mut: ProcessorMut, buffer_size: usize) -> Self {
        Self {
            base,
            processor_mut,
            buffer_size,
        }
    }
}

#[async_trait]
impl IConsumer for ConsumerMut {
    type TProcessor = ProcessorMut;

    async fn run_main_consume(&mut self) {
        let (msg_tx, mut msg_rx) = mpsc::channel::<OwnedMessage>(self.buffer_size);
        task::spawn({
            let mut context = self.base.context.clone();
            let mut processor_mut = self.processor_mut.clone();
            let consumer = self.base.consumer.clone();
            async move {
                while let Some(msg) = msg_rx.recv().await {
                    if let Err(e) = processor_mut.run(msg.clone()).await {
                        error!("Failed to run processor.({})", e);
                        context.cancel().await;
                        break;
                    }
                    let topic = msg.topic();
                    let partition = msg.partition();
                    let offset = msg.offset();
                    if consumer.store_offset(topic, partition, offset).is_err() {
                        error!(
                            "Failed to store offset.(topic={}, partition={}, offset={}",
                            topic, partition, offset
                        );
                        context.cancel().await;
                        break;
                    }
                }
            }
        });

        let mut stream = self.base.consumer.stream();
        consume_loop!((stream, self.base.context) => |owned_msg| async {
            if let Err(e) = msg_tx.send(owned_msg).await {
                raise_consume_error!("Failed to execute ProcessorMut.", e);
            }
            Ok(())
        });
    }

    async fn wait(&mut self) {
        let shutdown_complete_tx = self.base.shutdown_complete_tx.take().unwrap();
        drop(shutdown_complete_tx);

        let mut shutdown_complete_rx = self.base.shutdown_complete_rx.take().unwrap();
        let _ = shutdown_complete_rx.recv().await;
    }
}
