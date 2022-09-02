use crate::kafka::consumer::StreamConsumer;
use rdkafka::error::KafkaResult;
use rdkafka::ClientConfig;
use rdkafka::{
    config::FromClientConfig, consumer::stream_consumer::StreamConsumer as BaseStreamConsumer,
};

pub use rdkafka::message::{Message, OwnedMessage};

const DEFAULT_BUFFER_SIZE: usize = 16;

#[derive(Clone, Debug)]
pub(crate) struct StreamConsumerConfig {
    base_config: ClientConfig,
    buffer_size: usize,
}

impl Default for StreamConsumerConfig {
    fn default() -> Self {
        Self {
            base_config: ClientConfig::new(),
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }
}

impl StreamConsumerConfig {
    pub(crate) fn new() -> StreamConsumerConfig {
        Self {
            ..Default::default()
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

    pub(crate) fn set_buffer_size(&mut self, buffer_size: usize) -> &mut StreamConsumerConfig {
        self.buffer_size = buffer_size;
        self
    }

    pub(crate) fn create(&mut self) -> KafkaResult<StreamConsumer> {
        self.base_config.set("enable.auto.commit", "true");
        self.base_config.set("enable.auto.offset.store", "false");
        let base_consumer = BaseStreamConsumer::from_config(&self.base_config)?;
        Ok(StreamConsumer::new(base_consumer, self.buffer_size))
    }
}
