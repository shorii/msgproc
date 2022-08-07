use anyhow::Result;
use rdkafka::message::OwnedMessage;
use rdkafka::topic_partition_list::Offset;
use std::time::Duration;

/// [crate::msgproc::MsgProc]でConsumerを扱う際の一般的な表現。
pub trait IConsumer: Send + Sync {
    /// 存在するトピックの一覧を取得する。
    fn get_topics(&self, timeout: Duration) -> Result<Vec<String>>;

    /// subscribe中のトピックからメッセージを消費する。
    fn consume(&self, timeout: Duration) -> Option<Result<OwnedMessage>>;

    /// トピックの一覧を指定し、subscribeを開始する。
    fn subscribe(&self, topics: &[&str]) -> Result<()>;

    /// subscribeを中止する。
    fn unsubscribe(&self);

    /// Consumerのオフセットを指定した箇所まで戻す。
    fn seek(&self, topic: &str, partition: i32, offset: Offset, timeout: Duration) -> Result<()>;

    /// Consumerのオフセットをコミットする。
    fn commit(&self, topic: &str, partition: i32, offset: i64) -> Result<()>;

    /// consumeを一時的に停止する。
    fn pause(&self, topic: &str, partition: i32) -> Result<()>;

    /// consumeを再開する。
    fn resume(&self, topic: &str, partition: i32) -> Result<()>;
}

#[cfg(not(test))]
pub mod ext {
    use super::*;
    pub use rdkafka::consumer::base_consumer::BaseConsumer;
    use rdkafka::consumer::{CommitMode, Consumer as RdKafkaConsumer};
    use rdkafka::TopicPartitionList;

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

        fn seek(
            &self,
            topic: &str,
            partition: i32,
            offset: Offset,
            timeout: Duration,
        ) -> Result<()> {
            RdKafkaConsumer::seek(self, topic, partition, offset, timeout).map_err(|e| e.into())
        }

        fn commit(&self, topic: &str, partition: i32, offset: i64) -> Result<()> {
            let partitions = {
                let mut p = TopicPartitionList::new();
                p.add_partition_offset(topic, partition, Offset::Offset(offset + 1))
                    .unwrap();
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
}

#[cfg(test)]
pub mod ext {
    use super::*;
    use crate::kafka::alias::{Partition, Topic};
    use anyhow::bail;
    use rdkafka::config::FromClientConfig;
    use rdkafka::error::KafkaResult;
    use rdkafka::message::Message;
    use rdkafka::ClientConfig;
    use std::sync::RwLock;

    pub struct BaseConsumer {
        topics: RwLock<Vec<Topic>>,
        messages: Vec<OwnedMessage>,
        message_offset: RwLock<usize>,
        seeks: RwLock<Vec<(Topic, Partition, Offset, Duration)>>,
        commits: RwLock<Vec<(Topic, Partition)>>,
        pauses: RwLock<Vec<(Topic, Partition)>>,
        resumes: RwLock<Vec<(Topic, Partition)>>,
    }

    impl BaseConsumer {
        pub fn add_message(&mut self, topic: &str, message: OwnedMessage) -> Result<()> {
            if message.topic() != topic {
                bail!("inconsistent");
            }
            let mut topics = self.topics.write().unwrap();
            if !topics.contains(&topic.to_string()) {
                topics.push(topic.to_string());
            }
            self.messages.push(message);
            Ok(())
        }
    }

    impl FromClientConfig for BaseConsumer {
        fn from_config(_: &ClientConfig) -> KafkaResult<Self> {
            Ok(BaseConsumer {
                topics: RwLock::new(vec![]),
                messages: vec![],
                message_offset: RwLock::new(0),
                seeks: RwLock::new(vec![]),
                commits: RwLock::new(vec![]),
                pauses: RwLock::new(vec![]),
                resumes: RwLock::new(vec![]),
            })
        }
    }

    impl IConsumer for BaseConsumer {
        fn get_topics(&self, _timeout: Duration) -> Result<Vec<String>> {
            Ok(self.topics.read().unwrap().to_vec())
        }
        fn consume(&self, _timeout: Duration) -> Option<Result<OwnedMessage>> {
            let mut message_offset = self.message_offset.write().unwrap();
            if *message_offset >= self.messages.len() {
                None
            } else {
                let current_offset = *message_offset;
                *message_offset += 1;
                Some(Ok(self.messages[current_offset].clone()))
            }
        }
        fn subscribe(&self, topics: &[&str]) -> Result<()> {
            let mut topics = topics
                .to_vec()
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<String>>();
            let mut current_topics = self.topics.write().unwrap();
            current_topics.append(&mut topics);
            Ok(())
        }
        fn unsubscribe(&self) {
            let mut current_topics = self.topics.write().unwrap();
            *current_topics = vec![];
        }

        fn seek(
            &self,
            topic: &str,
            partition: i32,
            offset: Offset,
            timeout: Duration,
        ) -> Result<()> {
            let mut current_seeks = self.seeks.write().unwrap();
            current_seeks.push((topic.to_string(), partition, offset, timeout));
            Ok(())
        }

        fn commit(&self, topic: &str, partition: i32, _offset: i64) -> Result<()> {
            let mut current_commits = self.commits.write().unwrap();
            current_commits.push((topic.to_string(), partition));
            Ok(())
        }

        fn pause(&self, topic: &str, partition: i32) -> Result<()> {
            let mut current_pauses = self.pauses.write().unwrap();
            current_pauses.push((topic.to_string(), partition));
            Ok(())
        }

        fn resume(&self, topic: &str, partition: i32) -> Result<()> {
            let mut current_resumes = self.resumes.write().unwrap();
            current_resumes.push((topic.to_string(), partition));
            Ok(())
        }
    }
}

pub use ext::BaseConsumer;
