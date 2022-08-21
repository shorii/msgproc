use async_trait::async_trait;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer as BaseStreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::MessageStream;
use rdkafka::error::KafkaResult;
use rdkafka::message::BorrowedMessage;
use rdkafka::ClientConfig;

pub use rdkafka::message::{Message, OwnedMessage};

pub(crate) struct StreamConsumerConfig {
    base_config: ClientConfig,
}

impl StreamConsumerConfig {
    pub(crate) fn new() -> StreamConsumerConfig {
        StreamConsumerConfig {
            base_config: ClientConfig::new(),
        }
    }

    pub(crate) fn set<K, V>(&mut self, key: K, value: V) -> &mut StreamConsumerConfig
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.base_config.set(key.into(), value.into());
        self
    }

    pub(crate) fn create(&mut self) -> KafkaResult<StreamConsumer> {
        self.base_config.set("enable.auto.commit", "true");
        self.base_config.set("enable.auto.offset.store", "false");
        StreamConsumer::from_config(&self.base_config)
    }
}

#[async_trait]
pub(crate) trait IStreamConsumer: 'static + Send + Sync {
    fn stream(&self) -> MessageStream<'_>;
    async fn recv(&self) -> KafkaResult<BorrowedMessage>;
    fn subscribe(&self, topics: &[&str]) -> KafkaResult<()>;
    fn store_offset(&self, topics: &str, partition: i32, offset: i64) -> KafkaResult<()>;
}

pub(crate) struct StreamConsumer {
    base_consumer: BaseStreamConsumer,
}

#[async_trait]
impl IStreamConsumer for StreamConsumer {
    fn stream(&self) -> MessageStream<'_> {
        self.base_consumer.stream()
    }

    async fn recv(&self) -> KafkaResult<BorrowedMessage> {
        self.base_consumer.recv().await
    }

    fn subscribe(&self, topics: &[&str]) -> KafkaResult<()> {
        self.base_consumer.subscribe(topics)
    }

    fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> KafkaResult<()> {
        self.base_consumer.store_offset(topic, partition, offset)
    }
}

impl FromClientConfig for StreamConsumer {
    fn from_config(config: &rdkafka::ClientConfig) -> KafkaResult<Self> {
        let base_consumer = BaseStreamConsumer::from_config(config)?;
        Ok(StreamConsumer { base_consumer })
    }
}
