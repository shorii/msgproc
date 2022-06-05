use anyhow::Result;
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{CommitMode, Consumer as RdKafkaConsumer};
use rdkafka::message::OwnedMessage;
use rdkafka::topic_partition_list::Offset;
use rdkafka::TopicPartitionList;
use std::time::Duration;

pub trait IConsumer: Send + Sync {
    fn get_topics(&self, timeout: Duration) -> Result<Vec<String>>;
    fn consume(&self, timeout: Duration) -> Option<Result<OwnedMessage>>;
    fn subscribe(&self, topics: &[&str]) -> Result<()>;
    fn unsubscribe(&self);
    fn seek(&self, topic: &str, partition: i32, offset: Offset, timeout: Duration) -> Result<()>;
    fn commit(&self, topic: &str, partition: i32) -> Result<()>;
    fn pause(&self, topic: &str, partition: i32) -> Result<()>;
    fn resume(&self, topic: &str, partition: i32) -> Result<()>;
}

impl IConsumer for BaseConsumer {
    fn get_topics(&self, timeout: Duration) -> Result<Vec<String>> {
        let metadata = self.fetch_metadata(None, timeout);
        match metadata {
            Ok(md) => Ok(md
                .topics()
                .iter()
                .map(|t| String::from(t.name()))
                .collect::<Vec<_>>()),
            Err(e) => Err(e.into()),
        }
    }

    fn consume(&self, timeout: Duration) -> Option<Result<OwnedMessage>> {
        match BaseConsumer::poll(self, timeout) {
            Some(Ok(msg)) => Some(Ok(msg.detach())),
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }

    fn subscribe(&self, topics: &[&str]) -> Result<()> {
        RdKafkaConsumer::subscribe(self, topics).map_err(|e| e.into())
    }

    fn unsubscribe(&self) {
        RdKafkaConsumer::unsubscribe(self)
    }

    fn seek(&self, topic: &str, partition: i32, offset: Offset, timeout: Duration) -> Result<()> {
        RdKafkaConsumer::seek(self, topic, partition, offset, timeout).map_err(|e| e.into())
    }

    fn commit(&self, topic: &str, partition: i32) -> Result<()> {
        let partitions = {
            let mut p = TopicPartitionList::new();
            p.add_partition(topic, partition);
            p
        };
        RdKafkaConsumer::commit(self, &partitions, CommitMode::Sync).map_err(|e| e.into())
    }

    fn pause(&self, topic: &str, partition: i32) -> Result<()> {
        let partitions = {
            let mut p = TopicPartitionList::new();
            p.add_partition(topic, partition);
            p
        };
        RdKafkaConsumer::pause(self, &partitions).map_err(|e| e.into())
    }

    fn resume(&self, topic: &str, partition: i32) -> Result<()> {
        let partitions = {
            let mut p = TopicPartitionList::new();
            p.add_partition(topic, partition);
            p
        };
        RdKafkaConsumer::resume(self, &partitions).map_err(|e| e.into())
    }
}