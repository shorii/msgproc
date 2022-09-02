use crate::context::Context;
use crate::kafka::consumer::IStreamConsumer;
use crate::kafka::message::Message;
use crate::processor::{IProcessor, IProcessorMut, Processor, ProcessorMut};
use async_trait::async_trait;
use log::{debug, error};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task;
use tokio_stream::StreamExt;

#[derive(Error, Debug)]
#[error("ConsumeError occurred.")]
pub struct ConsumeError {
    msg: String,
    #[source]
    source: Option<anyhow::Error>,
}

macro_rules! raise_consume_error {
    ($msg: expr $(, $source: expr)?) => {
        {
            #[allow(unused_mut)]
            #[allow(unused_assignments)]
            let mut source = None;
            $( source = Some($source.into()); )?
            return Err(ConsumeError {
                msg: $msg.to_string(),
                source,
            });
        }
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
                    if $proc(msg).await.is_err() {
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
        consumer: Arc<Box<dyn IStreamConsumer>>,
        context: Context,
        shutdown_complete_rx: mpsc::Receiver<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
            consumer,
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
    processor: Arc<Box<dyn IProcessor>>,
    semaphore: Arc<Semaphore>,
}

impl Consumer {
    pub fn new(
        base: ConsumerBase,
        processor: Arc<Box<dyn IProcessor>>,
        semaphore: Arc<Semaphore>,
    ) -> Self {
        Self {
            base,
            processor,
            semaphore,
        }
    }
}

#[async_trait]
impl IConsumer for Consumer {
    type TProcessor = Arc<Box<dyn IProcessor>>;

    async fn run_main_consume(&mut self) {
        let mut stream = self.base.consumer.stream();
        consume_loop!((stream, self.base.context) => |msg: Message| async {
            let permit = match self.semaphore.clone().acquire_owned().await {
                Ok(p) => { p },
                Err(e) => {
                    raise_consume_error!("Failed to acquire permit.", e);
                }
            };
            let shutdown_complete_tx = match self.base.shutdown_complete_tx.clone() {
                Some(tx) => tx,
                None => raise_consume_error!("Main process is shutdowning. Break consuming loop.")
            };
            let processor = self.processor.clone();
            task::spawn({
                let consumer = self.base.consumer.clone();
                let topic = msg.topic().to_string();
                let partition = msg.partition();
                let offset = msg.offset();
                let mut context = self.base.context.clone();
                let mut processor = Processor::new(
                    processor,
                    context.clone(),
                    shutdown_complete_tx,
                );
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
    processor_mut: Arc<Mutex<Box<dyn IProcessorMut>>>,
    buffer_size: usize,
}

impl ConsumerMut {
    pub fn new(
        base: ConsumerBase,
        processor_mut: Arc<Mutex<Box<dyn IProcessorMut>>>,
        buffer_size: usize,
    ) -> Self {
        Self {
            base,
            processor_mut,
            buffer_size,
        }
    }
}

#[async_trait]
impl IConsumer for ConsumerMut {
    type TProcessor = Arc<Mutex<Box<dyn IProcessorMut>>>;

    async fn run_main_consume(&mut self) {
        let (msg_tx, mut msg_rx) = mpsc::channel::<Message>(self.buffer_size);
        task::spawn({
            let mut context = self.base.context.clone();
            let processor_mut = self.processor_mut.clone();
            let consumer = self.base.consumer.clone();
            let shutdown_complete_tx = self.base.shutdown_complete_tx.clone().unwrap();
            async move {
                // When ConsumeError occurrs, `msg_tx` is dropped and `msg_rx` returns `None`.
                while let Some(msg) = msg_rx.recv().await {
                    let mut processor_mut = ProcessorMut::new(
                        processor_mut.clone(),
                        context.clone(),
                        shutdown_complete_tx.clone(),
                    );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::Context;
    use crate::kafka::consumer::testing::*;
    use tokio::sync::broadcast;

    mod consumer {
        use super::*;
        use crate::kafka::message::testing::create_message;
        use crate::processor::IProcessor;
        use rdkafka::error::KafkaResult;

        const SEMAPHORE_LIMIT: usize = 16;

        fn setup(
            messages: &[KafkaResult<Message>],
            offset_context: OffsetContext,
            processor: impl IProcessor,
        ) -> Consumer {
            let consumer = MockStreamConsumer::new(messages, offset_context);
            let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
            let (notify_shutdown, _) = broadcast::channel(1);
            let context = Context::new(notify_shutdown);
            let base = ConsumerBase::new(
                Arc::new(Box::new(consumer)),
                context.clone(),
                shutdown_complete_rx,
                shutdown_complete_tx.clone(),
            );
            Consumer::new(
                base,
                Arc::new(Box::new(processor)),
                Arc::new(Semaphore::new(SEMAPHORE_LIMIT)),
            )
        }

        mod test_normal {
            use super::*;

            struct NoopProcessor;

            #[async_trait]
            impl IProcessor for NoopProcessor {
                async fn execute(&self, _msg: Message) -> Result<(), &'static str> {
                    Ok(())
                }
            }

            #[tokio::test]
            async fn test() {
                let topic1 = "topic1";
                let topic1_partition1 = 0;
                let topic1_partition2 = 1;

                let topic2 = "topic2";
                let topic2_partition1 = 0;

                let messages = vec![
                    // topic2 and topic2_partition1: 4
                    // topic1 and topic1_partition2: 5
                    // topic1 and topic1_partition1: 6
                    Ok(create_message(topic2, topic2_partition1, 0, "test")),
                    Ok(create_message(topic2, topic2_partition1, 1, "test")),
                    Ok(create_message(topic1, topic1_partition2, 0, "test")),
                    Ok(create_message(topic1, topic1_partition1, 0, "test")),
                    Ok(create_message(topic1, topic1_partition1, 1, "test")),
                    Ok(create_message(topic1, topic1_partition1, 2, "test")),
                    Ok(create_message(topic2, topic2_partition1, 2, "test")),
                    Ok(create_message(topic1, topic1_partition2, 1, "test")),
                    Ok(create_message(topic1, topic1_partition1, 3, "test")),
                    Ok(create_message(topic1, topic1_partition2, 2, "test")),
                    Ok(create_message(topic2, topic2_partition1, 3, "test")),
                    Ok(create_message(topic1, topic1_partition2, 3, "test")),
                    Ok(create_message(topic1, topic1_partition1, 4, "test")),
                    Ok(create_message(topic1, topic1_partition1, 5, "test")),
                    Ok(create_message(topic1, topic1_partition2, 4, "test")),
                ];
                let offset_context = OffsetContext::new();
                let mut consumer = setup(&messages, offset_context.clone(), NoopProcessor);
                consumer.run_main_consume().await;
                consumer.wait().await;
                assert_eq!(
                    offset_context.get(OffsetContextKey::new(topic1, topic1_partition1)),
                    5
                );
                assert_eq!(
                    offset_context.get(OffsetContextKey::new(topic1, topic1_partition2)),
                    4
                );
                assert_eq!(
                    offset_context.get(OffsetContextKey::new(topic2, topic2_partition1)),
                    3
                );
            }
        }

        mod test_processor_raises_error {
            use super::*;

            struct ErrorProcessor {
                error_topic: String,
                error_partition: i32,
                error_offset: i64,
            }

            #[async_trait]
            impl IProcessor for ErrorProcessor {
                async fn execute(&self, msg: Message) -> Result<(), &'static str> {
                    if msg.topic() == &self.error_topic
                        && msg.partition() == self.error_partition
                        && msg.offset() == self.error_offset
                    {
                        return Err("Processor raises error.");
                    }
                    Ok(())
                }
            }

            #[tokio::test]
            async fn test() {
                let topic1 = "topic1";
                let topic1_partition1 = 0;
                let topic1_partition2 = 1;

                let topic2 = "topic2";
                let topic2_partition1 = 0;

                let messages = vec![
                    // topic2 and topic2_partition1: 4
                    // topic1 and topic1_partition2: 5
                    // topic1 and topic1_partition1: 6
                    Ok(create_message(topic2, topic2_partition1, 0, "test")),
                    Ok(create_message(topic2, topic2_partition1, 1, "test")),
                    Ok(create_message(topic1, topic1_partition2, 0, "test")),
                    Ok(create_message(topic1, topic1_partition1, 0, "test")),
                    Ok(create_message(topic1, topic1_partition1, 1, "test")),
                    Ok(create_message(topic1, topic1_partition1, 2, "test")),
                    Ok(create_message(topic2, topic2_partition1, 2, "test")),
                    Ok(create_message(topic1, topic1_partition2, 1, "test")),
                    Ok(create_message(topic1, topic1_partition1, 3, "test")),
                    Ok(create_message(topic1, topic1_partition2, 2, "test")),
                    Ok(create_message(topic2, topic2_partition1, 3, "test")),
                    Ok(create_message(topic1, topic1_partition2, 3, "test")),
                    Ok(create_message(topic1, topic1_partition1, 4, "test")),
                    Ok(create_message(topic1, topic1_partition1, 5, "test")),
                    Ok(create_message(topic1, topic1_partition2, 4, "test")),
                ];
                let offset_context = OffsetContext::new();
                let processor = ErrorProcessor {
                    error_topic: topic1.to_string(),
                    error_partition: topic1_partition1,
                    error_offset: 4,
                };
                let mut consumer = setup(&messages, offset_context.clone(), processor);
                consumer.run_main_consume().await;
                consumer.wait().await;
                assert_eq!(
                    offset_context.get(OffsetContextKey::new(topic1, topic1_partition1)),
                    3,
                );
                assert_eq!(
                    offset_context.get(OffsetContextKey::new(topic1, topic1_partition2)),
                    3
                );
                assert_eq!(
                    offset_context.get(OffsetContextKey::new(topic2, topic2_partition1)),
                    3
                );
            }
        }
        mod test_kafka_raises_error {}
    }
}
