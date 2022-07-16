use actix::prelude::*;
use log::{error, warn};
use rdkafka::message::Message;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use uuid::Uuid;

use crate::consumer::IConsumer;
use crate::msg::{InnerMsg, Msg, MsgProcResult, MsgProcessor, MsgProcessorDescriptor};
use crate::policy::IJobPolicy;

struct InnerMsgProcessor {
    id: Uuid,
    proc: Recipient<MsgProcResult>,
    processor: Recipient<Msg>,
}

macro_rules! key {
    ($msg: expr) => {
        TopicManagementKey {
            topic: $msg.topic().to_string(),
            partition: $msg.partition(),
        }
    };
}

#[derive(Eq, PartialEq, Hash)]
struct TopicManagementKey {
    topic: String,
    partition: i32,
}

struct TopicManagementContext {
    notified: HashSet<Uuid>,
    done: HashSet<Uuid>,
}

impl TopicManagementContext {
    fn new() -> Self {
        Self {
            notified: HashSet::<Uuid>::new(),
            done: HashSet::<Uuid>::new(),
        }
    }

    fn notify(&mut self, id: Uuid) {
        self.notified.insert(id);
    }

    fn done(&mut self, id: Uuid) -> bool {
        self.done.insert(id);
        self.notified == self.done
    }
}

macro_rules! kafka_error {
    ($addr: ident, $e: expr) => {
        $addr.do_send($crate::msg::MsgProcResult(Err(
            $crate::error::MsgProcError::KafkaError($e.to_string()),
        )));
    };
}

/// kafkaから消費したメッセージを処理する[Actor]
///
/// 定期的にメッセージを[IConsumer::consume]し、あらかじめ登録したprocessorにメッセージを配送する。
/// [IConsumer::subscribe]するトピックは正規表現で指定できる。
/// kafkaのトピックは定期的に取得していて、[MsgProc]が開始された後にkafkaにトピックが作成された場合であっても[IConsumer::subscribe]することができる。
/// また、逆にトピックがkafkaから削除された場合には[IConsumer::unsubscribe]される。
pub struct MsgProc {
    topic_patterns: Vec<Regex>,
    topics: Vec<String>,
    topic_management: HashMap<TopicManagementKey, TopicManagementContext>,
    consumer: Box<dyn IConsumer>,
    processors: Vec<InnerMsgProcessor>,
    update_topics_policy: Box<dyn IJobPolicy>,
    consume_message_policy: Box<dyn IJobPolicy>,
    timeout: Duration,
}

impl MsgProc {
    /// 新たに[MsgProc]を生成する。
    ///
    /// * `topic_patterns` - [IConsumer::subscribe]するトピックの名前に合致する正規表現の[Vec]
    /// * `consumer` - kafkaからメッセージを[IConsumer::consume]する[IConsumer]
    /// * `update_topics_policy` - トピックを更新する際の間隔やリトライを決める[IJobPolicy]
    /// * `consume_message_policy` - [IConsumer::consume]する際のの間隔やリトライを決める[IJobPolicy]
    /// * `timeout` - kafkaの操作時におけるタイムアウト値
    pub fn new(
        topic_patterns: Vec<Regex>,
        consumer: Box<dyn IConsumer>,
        update_topics_policy: Box<dyn IJobPolicy>,
        consume_message_policy: Box<dyn IJobPolicy>,
        timeout: Duration,
    ) -> Self {
        Self {
            topic_patterns,
            topics: vec![],
            topic_management: HashMap::new(),
            consumer,
            processors: vec![],
            update_topics_policy,
            consume_message_policy,
            timeout,
        }
    }

    fn notify(&mut self, msg: InnerMsg) {
        for processor in self.processors.iter() {
            let InnerMsgProcessor {
                id,
                processor,
                proc,
            } = processor;
            processor.do_send(Msg::new(proc.clone(), msg.0.clone(), *id));
            let context = self.topic_management.get_mut(&key!(msg.0)).unwrap();
            context.notify(*id);
        }
    }

    fn update_topics(&mut self, ctx: &mut Context<MsgProc>) {
        match self.consumer.get_topics(self.timeout) {
            Ok(topics) => {
                let matched_topics = topics
                    .into_iter()
                    .filter(|t| {
                        for regexp in self.topic_patterns.iter() {
                            if regexp.is_match(t) {
                                return true;
                            }
                        }
                        false
                    })
                    .collect::<Vec<_>>();
                self.consumer.unsubscribe();
                if self
                    .consumer
                    .subscribe(
                        &matched_topics
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>(),
                    )
                    .is_ok()
                {
                    self.topics = matched_topics;
                    self.update_topics_policy.reset();
                    return;
                }
                if self.update_topics_policy.violate() {
                    ctx.stop();
                    return;
                }
                self.update_topics_policy.update();
            }
            Err(_) => {
                if self.update_topics_policy.violate() {
                    ctx.stop();
                    return;
                }
                self.update_topics_policy.update();
            }
        };
    }

    fn consume_message(&mut self, ctx: &mut Context<MsgProc>) {
        let message = self.consumer.consume(Duration::from_secs(5));
        let addr = ctx.address();
        match message {
            Some(Ok(msg)) => {
                addr.do_send(InnerMsg(msg.clone()));
                self.consume_message_policy.reset();
                let topic = msg.topic();
                let partition = msg.partition();
                let offset = msg.offset();
                if self.consumer.pause(topic, partition).is_err() {
                    kafka_error!(
                        addr,
                        format!(
                            "Failed to pause topic.(topic: {}, partition: {}, offset: {})",
                            topic, partition, offset
                        )
                    );
                }
                self.topic_management
                    .insert(key!(msg), TopicManagementContext::new());
            }
            Some(Err(_)) => {
                warn!("KafkaError occurred. Offset is not incremented.");
                if self.consume_message_policy.violate() {
                    kafka_error!(addr, "Retry limitation reached.");
                }
                self.consume_message_policy.update();
            }
            None => {}
        }
    }
}

impl Actor for MsgProc {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(self.update_topics_policy.interval(), MsgProc::update_topics);
        ctx.run_interval(
            self.consume_message_policy.interval(),
            MsgProc::consume_message,
        );
    }
}

impl Handler<InnerMsg> for MsgProc {
    type Result = ();

    fn handle(&mut self, msg: InnerMsg, _ctx: &mut Self::Context) -> Self::Result {
        self.notify(msg);
    }
}

impl Handler<MsgProcessor> for MsgProc {
    type Result = ();

    fn handle(&mut self, msg: MsgProcessor, ctx: &mut Self::Context) -> Self::Result {
        self.processors.push(InnerMsgProcessor {
            id: Uuid::new_v4(),
            proc: ctx.address().recipient(),
            processor: msg.0,
        });
    }
}

impl Handler<MsgProcResult> for MsgProc {
    type Result = ();

    fn handle(&mut self, msg: MsgProcResult, ctx: &mut Self::Context) -> Self::Result {
        match msg.0 {
            Ok(done_handler) => {
                let MsgProcessorDescriptor {
                    message,
                    processor_id: handler_id,
                } = done_handler;
                let context = self.topic_management.get_mut(&key!(message));
                if let Some(context) = context {
                    if context.done(handler_id) {
                        let topic = message.topic();
                        let partition = message.partition();
                        let offset = message.offset();
                        self.consumer.commit(topic, partition, offset).unwrap();
                        self.consumer.resume(topic, partition).unwrap();
                        return;
                    } else {
                        return;
                    }
                }
                ctx.stop();
            }
            Err(e) => {
                error!("{}", e);
                ctx.stop();
            }
        }
    }
}

#[cfg(test)]
mod tests;
