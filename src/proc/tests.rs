use crate::policy::DefaultJobPolicy;
use anyhow::{bail, Result};
use rdkafka::message::OwnedMessage;
use rdkafka::message::Timestamp;
use rdkafka::Offset;
use std::sync::RwLock;

use super::*;

type Topic = String;
type Partition = i32;

struct MockConsumer {
    topics: RwLock<Vec<Topic>>,
    messages: Vec<OwnedMessage>,
    message_offset: RwLock<usize>,
    seeks: RwLock<Vec<(Topic, Partition, Offset, Duration)>>,
    commits: RwLock<Vec<(Topic, Partition)>>,
    pauses: RwLock<Vec<(Topic, Partition)>>,
    resumes: RwLock<Vec<(Topic, Partition)>>,
}

impl MockConsumer {
    fn new() -> Self {
        Self {
            topics: RwLock::new(vec![]),
            messages: vec![],
            message_offset: RwLock::new(0),
            seeks: RwLock::new(vec![]),
            commits: RwLock::new(vec![]),
            pauses: RwLock::new(vec![]),
            resumes: RwLock::new(vec![]),
        }
    }

    fn add_message(&mut self, topic: &str, message: OwnedMessage) -> Result<()> {
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

impl IConsumer for MockConsumer {
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

    fn seek(&self, topic: &str, partition: i32, offset: Offset, timeout: Duration) -> Result<()> {
        let mut current_seeks = self.seeks.write().unwrap();
        current_seeks.push((topic.to_string(), partition, offset, timeout));
        Ok(())
    }

    fn commit(&self, topic: &str, partition: i32) -> Result<()> {
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

struct MsgStore {
    msgs: Vec<OwnedMessage>,
}

impl Actor for MsgStore {
    type Context = Context<Self>;
}

impl Handler<HandleMsg> for MsgStore {
    type Result = ();
    fn handle(&mut self, msg: HandleMsg, _ctx: &mut Self::Context) -> Self::Result {
        let HandleMsg { proc: _proc, msg } = msg;
        self.msgs.push(msg);
        ()
    }
}

#[derive(Message)]
#[rtype(result = "GetStoredMsgResponse")]
struct GetStoredMsg;

#[derive(MessageResponse)]
struct GetStoredMsgResponse(Vec<OwnedMessage>);

impl Handler<GetStoredMsg> for MsgStore {
    type Result = GetStoredMsgResponse;
    fn handle(&mut self, _msg: GetStoredMsg, _ctx: &mut Self::Context) -> Self::Result {
        GetStoredMsgResponse(self.msgs.clone())
    }
}

fn create_message(topic: &str, partition: i32, offset: i64, payload: &str) -> OwnedMessage {
    let payload: Vec<u8> = payload.as_bytes().to_vec();
    OwnedMessage::new(
        Some(payload),
        None,
        topic.to_string(),
        Timestamp::CreateTime(0),
        partition,
        offset,
        None,
    )
}

fn assert_message(message: OwnedMessage, topic: &str, partition: i32, offset: i64, payload: &str) {
    assert_eq!(message.topic(), topic);
    assert_eq!(message.partition(), partition);
    assert_eq!(message.offset(), offset);
    assert_eq!(message.payload().unwrap(), payload.as_bytes());
}

#[actix::test]
async fn test() {
    let mut consumer = MockConsumer::new();
    consumer
        .add_message("topic1", create_message("topic1", 0, 0, "message1"))
        .unwrap();
    consumer
        .add_message("topic2", create_message("topic2", 0, 0, "message2"))
        .unwrap();
    consumer
        .add_message("topic1", create_message("topic1", 0, 1, "message3"))
        .unwrap();
    consumer
        .add_message("topic2", create_message("topic2", 0, 1, "message4"))
        .unwrap();

    let msgproc = Proc::new(
        vec![],
        Box::new(consumer),
        Box::new(DefaultJobPolicy::new(Duration::from_secs(1), 5)),
        Box::new(DefaultJobPolicy::new(Duration::from_secs(1), 5)),
        Duration::from_secs(5),
    );
    let msgproc_addr = msgproc.start();
    let msgstore1 = MsgStore { msgs: vec![] }.start();
    let msgstore2 = MsgStore { msgs: vec![] }.start();
    let _res = msgproc_addr
        .send(MsgHandler(msgstore1.clone().recipient()))
        .await;
    let _res = msgproc_addr
        .send(MsgHandler(msgstore2.clone().recipient()))
        .await;
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    let res1 = msgstore1.send(GetStoredMsg).await.unwrap();
    assert_message(res1.0[0].clone(), "topic1", 0, 0, "message1");
    assert_message(res1.0[1].clone(), "topic2", 0, 0, "message2");
    assert_message(res1.0[2].clone(), "topic1", 0, 1, "message3");
    assert_message(res1.0[3].clone(), "topic2", 0, 1, "message4");

    let res2 = msgstore2.send(GetStoredMsg).await.unwrap();
    assert_message(res2.0[0].clone(), "topic1", 0, 0, "message1");
    assert_message(res2.0[1].clone(), "topic2", 0, 0, "message2");
    assert_message(res2.0[2].clone(), "topic1", 0, 1, "message3");
    assert_message(res2.0[3].clone(), "topic2", 0, 1, "message4");
}
