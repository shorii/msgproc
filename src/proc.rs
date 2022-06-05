use actix::prelude::*;
use log::error;
use rdkafka::message::Message;
use regex::Regex;
use std::time::Duration;

use crate::consumer::IConsumer;
use crate::msg::{HandleMsg, Msg, MsgHandleResult, MsgHandler};
use crate::policy::IJobPolicy;

struct RegisteredMsgHandler {
    proc: Recipient<MsgHandleResult>,
    handler: Recipient<HandleMsg>,
}

struct Proc {
    topic_patterns: Vec<Regex>,
    topics: Vec<String>,
    consumer: Box<dyn IConsumer>,
    handlers: Vec<RegisteredMsgHandler>,
    update_topics_policy: Box<dyn IJobPolicy>,
    consume_message_policy: Box<dyn IJobPolicy>,
    timeout: Duration,
}

impl Proc {
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
            consumer,
            handlers: vec![],
            update_topics_policy,
            consume_message_policy,
            timeout: timeout,
        }
    }

    fn notify(&self, msg: Msg) {
        for handler in self.handlers.iter() {
            let RegisteredMsgHandler { handler, proc } = handler;
            handler.do_send(HandleMsg {
                proc: proc.clone(),
                msg: msg.0.clone(),
            });
        }
    }

    fn update_topics(&mut self, ctx: &mut Context<Proc>) {
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
                if let Ok(_) = self.consumer.subscribe(
                    &matched_topics
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>(),
                ) {
                    self.topics = matched_topics;
                    self.update_topics_policy.reset();
                    return;
                }
                if self.update_topics_policy.check() {
                    ctx.stop();
                    return;
                }
                self.update_topics_policy.update();
            }
            Err(_) => {
                if self.update_topics_policy.check() {
                    ctx.stop();
                    return;
                }
                self.update_topics_policy.update();
            }
        };
    }

    fn consume_message(&mut self, ctx: &mut Context<Proc>) {
        let message = self.consumer.consume(Duration::from_secs(5));
        match message {
            Some(Ok(msg)) => {
                ctx.address().do_send(Msg(msg.clone()));
                self.consume_message_policy.reset();
                let topic = msg.topic();
                let partition = msg.partition();
                let offset = msg.offset();
                let commit = self.consumer.commit(&topic, partition);
                if commit.is_err() {
                    error!(
                        "KafkaError occurred. Failed to commit offset.(topic: {}, partition: {}, offset: {})",
                        topic, partition, offset
                    );
                    if self.consume_message_policy.check() {
                        ctx.stop();
                        return;
                    }
                    self.consume_message_policy.update();
                }
            }
            Some(Err(_)) => {
                error!("KafkaError occurred. Offset is not incremented.");
                if self.consume_message_policy.check() {
                    ctx.stop();
                    return;
                }
                self.consume_message_policy.update();
            }
            None => {}
        }
    }
}

impl Actor for Proc {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(self.update_topics_policy.interval(), Proc::update_topics);
        ctx.run_interval(
            self.consume_message_policy.interval(),
            Proc::consume_message,
        );
    }
}

impl Handler<Msg> for Proc {
    type Result = ();

    fn handle(&mut self, msg: Msg, _ctx: &mut Self::Context) -> Self::Result {
        self.notify(msg);
    }
}

impl Handler<MsgHandler> for Proc {
    type Result = ();

    fn handle(&mut self, msg: MsgHandler, ctx: &mut Self::Context) -> Self::Result {
        self.handlers.push(RegisteredMsgHandler {
            proc: ctx.address().recipient(),
            handler: msg.0,
        });
    }
}

impl Handler<MsgHandleResult> for Proc {
    type Result = ();

    fn handle(&mut self, msg: MsgHandleResult, ctx: &mut Self::Context) -> Self::Result {
        if msg.0.is_err() {
            error!("Failed to handle msg.");
            ctx.stop();
        }
    }
}

#[cfg(test)]
mod tests;
