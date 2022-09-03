use crate::context::Context;
use crate::kafka::consumer::IStreamConsumer;
use crate::kafka::message::Message;
use crate::processor::{IProcessor, Processor};
use log::{debug, error};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
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

pub(crate) struct Consumer {
    consumer: Arc<Box<dyn IStreamConsumer>>,
    context: Context,
    shutdown_complete_rx: Option<mpsc::Receiver<()>>,
    shutdown_complete_tx: Option<mpsc::Sender<()>>,
    processor: Arc<Mutex<Box<dyn IProcessor>>>,
    buffer_size: usize,
}

impl Consumer {
    pub fn new(
        consumer: Arc<Box<dyn IStreamConsumer>>,
        context: Context,
        shutdown_complete_rx: mpsc::Receiver<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
        processor: Arc<Mutex<Box<dyn IProcessor>>>,
        buffer_size: usize,
    ) -> Self {
        Self {
            consumer,
            context,
            shutdown_complete_rx: Some(shutdown_complete_rx),
            shutdown_complete_tx: Some(shutdown_complete_tx),
            processor,
            buffer_size,
        }
    }

    pub async fn run_main_consume(&mut self) {
        let (msg_tx, mut msg_rx) = mpsc::channel::<Message>(self.buffer_size);
        task::spawn({
            let mut context = self.context.clone();
            let processor = self.processor.clone();
            let consumer = self.consumer.clone();
            let shutdown_complete_tx = self.shutdown_complete_tx.clone().unwrap();
            async move {
                // When ConsumeError occurrs, `msg_tx` is dropped and `msg_rx` returns `None`.
                while let Some(msg) = msg_rx.recv().await {
                    let mut processor = Processor::new(
                        processor.clone(),
                        context.clone(),
                        shutdown_complete_tx.clone(),
                    );
                    if let Err(e) = processor.run(msg.clone()).await {
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

        let mut stream = self.consumer.stream();
        consume_loop!((stream, self.context) => |owned_msg| async {
            if let Err(e) = msg_tx.send(owned_msg).await {
                raise_consume_error!("Failed to execute ProcessorMut.", e);
            }
            Ok(())
        });
    }

    pub async fn wait(&mut self) {
        let shutdown_complete_tx = self.shutdown_complete_tx.take().unwrap();
        drop(shutdown_complete_tx);

        let mut shutdown_complete_rx = self.shutdown_complete_rx.take().unwrap();
        let _ = shutdown_complete_rx.recv().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::Context;
    use crate::kafka::consumer::testing::*;
    use crate::kafka::message::testing::create_message;
    use crate::processor::IProcessor;
    use async_trait::async_trait;
    use rdkafka::error::KafkaResult;
    use tokio::sync::broadcast;

    const BUFFER_LIMIT: usize = 16;

    fn create_test_messages() -> Vec<KafkaResult<Message>> {
        vec![
            // topic = "topic2", partition = 0: 4
            // topic = "topic1", partition = 1: 5
            // topic = "topic1", partition = 0: 6
            Ok(create_message("topic2", 0, 0, "test")),
            Ok(create_message("topic2", 0, 1, "test")),
            Ok(create_message("topic1", 1, 0, "test")),
            Ok(create_message("topic1", 0, 0, "test")),
            Ok(create_message("topic1", 0, 1, "test")),
            Ok(create_message("topic1", 0, 2, "test")),
            Ok(create_message("topic2", 0, 2, "test")),
            Ok(create_message("topic1", 1, 1, "test")),
            Ok(create_message("topic1", 0, 3, "test")),
            Ok(create_message("topic1", 1, 2, "test")),
            Ok(create_message("topic2", 0, 3, "test")),
            Ok(create_message("topic1", 1, 3, "test")),
            Ok(create_message("topic1", 0, 4, "test")),
            Ok(create_message("topic1", 0, 5, "test")),
            Ok(create_message("topic1", 1, 4, "test")),
        ]
    }

    fn setup(
        messages: &[KafkaResult<Message>],
        offset_context: OffsetContext,
        processor: impl IProcessor,
    ) -> Consumer {
        let consumer = MockStreamConsumer::new(messages, offset_context);
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
        let (notify_shutdown, _) = broadcast::channel(1);
        let context = Context::new(notify_shutdown);
        Consumer::new(
            Arc::new(Box::new(consumer)),
            context.clone(),
            shutdown_complete_rx,
            shutdown_complete_tx.clone(),
            Arc::new(Mutex::new(Box::new(processor))),
            BUFFER_LIMIT,
        )
    }

    mod test_normal {
        use super::*;

        struct NoopProcessor;

        #[async_trait]
        impl IProcessor for NoopProcessor {
            async fn execute(&mut self, _msg: Message) -> Result<(), &'static str> {
                Ok(())
            }
        }

        #[tokio::test]
        async fn test() {
            let offset_context = OffsetContext::new();
            let test_messages = create_test_messages();
            let mut consumer = setup(&test_messages, offset_context.clone(), NoopProcessor);
            consumer.run_main_consume().await;
            consumer.wait().await;
            assert_eq!(offset_context.get(OffsetContextKey::new("topic1", 0)), 5);
            assert_eq!(offset_context.get(OffsetContextKey::new("topic1", 1)), 4);
            assert_eq!(offset_context.get(OffsetContextKey::new("topic2", 0)), 3);
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
            async fn execute(&mut self, msg: Message) -> Result<(), &'static str> {
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
            let messages = create_test_messages();
            let offset_context = OffsetContext::new();
            let processor = ErrorProcessor {
                error_topic: "topic1".to_string(),
                error_partition: 0,
                error_offset: 4,
            };
            let mut consumer = setup(&messages, offset_context.clone(), processor);
            consumer.run_main_consume().await;
            consumer.wait().await;
            assert_eq!(offset_context.get(OffsetContextKey::new("topic1", 0)), 3);
            assert_eq!(offset_context.get(OffsetContextKey::new("topic1", 1)), 3);
            assert_eq!(offset_context.get(OffsetContextKey::new("topic2", 0)), 3);
        }
    }
    mod test_kafka_raises_error {
        use rdkafka::error::{KafkaError, RDKafkaErrorCode};

        use super::*;

        struct NoopProcessor;

        #[async_trait]
        impl IProcessor for NoopProcessor {
            async fn execute(&mut self, _msg: Message) -> Result<(), &'static str> {
                Ok(())
            }
        }

        #[tokio::test]
        async fn test() {
            let mut messages = create_test_messages();
            messages.insert(
                7,
                Err(KafkaError::MessageConsumption(RDKafkaErrorCode::Fail)),
            );
            let offset_context = OffsetContext::new();
            let mut consumer = setup(&messages, offset_context.clone(), NoopProcessor);
            consumer.run_main_consume().await;
            consumer.wait().await;
            assert_eq!(offset_context.get(OffsetContextKey::new("topic1", 0)), 2);
            assert_eq!(offset_context.get(OffsetContextKey::new("topic1", 1)), 0);
            assert_eq!(offset_context.get(OffsetContextKey::new("topic2", 0)), 2);
        }
    }
}
