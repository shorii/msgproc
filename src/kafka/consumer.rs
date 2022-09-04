use crate::kafka::message::Message;
use async_trait::async_trait;
use rdkafka::consumer::stream_consumer::StreamConsumer as BaseStreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaResult;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio_stream::Stream;

pub(crate) struct MessageStream {
    receiver: mpsc::Receiver<KafkaResult<Message>>,
}

impl MessageStream {
    fn new(receiver: mpsc::Receiver<KafkaResult<Message>>) -> Self {
        Self { receiver }
    }
}

impl Stream for MessageStream {
    type Item = KafkaResult<Message>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}

#[async_trait]
pub(crate) trait IStreamConsumer: 'static + Send + Sync {
    fn stream(&self) -> MessageStream;
    fn subscribe(&self, topics: &[&str]) -> KafkaResult<()>;
    fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> KafkaResult<()>;
}

pub(crate) struct StreamConsumer {
    base_consumer: Arc<BaseStreamConsumer>,
    buffer_size: usize,
}

impl StreamConsumer {
    pub(crate) fn new(base_consumer: BaseStreamConsumer, buffer_size: usize) -> Self {
        Self {
            base_consumer: Arc::new(base_consumer),
            buffer_size,
        }
    }
}

#[async_trait]
impl IStreamConsumer for StreamConsumer {
    fn stream(&self) -> MessageStream {
        let (tx, rx) = mpsc::channel::<KafkaResult<Message>>(self.buffer_size);
        tokio::spawn({
            let base_consumer = self.base_consumer.clone();
            async move {
                loop {
                    let message = match base_consumer.recv().await {
                        Ok(msg) => Ok(Message::new(msg.detach())),
                        Err(e) => Err(e),
                    };
                    let _ = tx.send(message).await;
                }
            }
        });
        MessageStream::new(rx)
    }

    fn subscribe(&self, topics: &[&str]) -> KafkaResult<()> {
        self.base_consumer.subscribe(topics)
    }

    fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> KafkaResult<()> {
        self.base_consumer.store_offset(topic, partition, offset)
    }
}

#[cfg(test)]
pub mod testing {
    use rdkafka::error::KafkaError;

    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(PartialEq, Eq, Hash, Clone)]
    pub struct OffsetContextKey {
        topic: String,
        partition: i32,
    }

    impl OffsetContextKey {
        pub fn new(topic: &str, partition: i32) -> Self {
            Self {
                topic: topic.to_string(),
                partition,
            }
        }
    }

    #[derive(Clone)]
    pub struct OffsetContext(Arc<Mutex<HashMap<OffsetContextKey, i64>>>);

    impl OffsetContext {
        pub fn new() -> Self {
            Self(Arc::new(Mutex::new(HashMap::new())))
        }

        fn set(&self, key: OffsetContextKey, value: i64) {
            let mut context = self.0.lock().unwrap();
            context.insert(key, value);
        }

        pub fn get(&self, key: OffsetContextKey) -> i64 {
            *self.0.lock().unwrap().get(&key).unwrap()
        }
    }

    #[derive(Clone)]
    pub struct MockStreamConsumer {
        topics: Arc<Mutex<Vec<String>>>,
        offset: OffsetContext,
        messages: Vec<KafkaResult<Message>>,
    }

    impl MockStreamConsumer {
        pub fn new(messages: &[KafkaResult<Message>], offset: OffsetContext) -> Self {
            Self {
                topics: Arc::new(Mutex::new(vec![])),
                offset,
                messages: messages.to_vec(),
            }
        }

        pub fn get_offset(&self) -> OffsetContext {
            self.offset.clone()
        }
    }

    const MOCK_BUFFER_SIZE: usize = 16;

    impl IStreamConsumer for MockStreamConsumer {
        fn stream(&self) -> MessageStream {
            let (tx, rx) = mpsc::channel::<KafkaResult<Message>>(MOCK_BUFFER_SIZE);
            tokio::spawn({
                let mut messages = self.messages.clone().into_iter();
                async move {
                    while let Some(message) = messages.next() {
                        let _ = tx.send(message).await;
                    }
                    // NOTE: End of stream
                    let _ = tx.send(Err(KafkaError::PartitionEOF(0))).await;
                }
            });
            MessageStream::new(rx)
        }

        fn subscribe(&self, topics: &[&str]) -> KafkaResult<()> {
            let mut tp = self.topics.lock().unwrap();
            *tp = topics.iter().map(|x| x.to_string()).collect::<Vec<_>>();
            Ok(())
        }

        fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> KafkaResult<()> {
            self.offset
                .set(OffsetContextKey::new(topic, partition), offset);
            Ok(())
        }
    }
}
