use super::consumer::StreamConsumer;
use rdkafka::config::FromClientConfig;
use rdkafka::error::KafkaResult;
use rdkafka::ClientConfig;

pub use rdkafka::message::{Message, OwnedMessage};

#[derive(Clone, Debug)]
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
