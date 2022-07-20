use crate::internal::msg::{consume, process};
use crate::kafka::consumer::IConsumer;
use crate::kafka::key::Topic;
use actix::prelude::*;
use crossbeam::channel;
use log::{error, info};
use rdkafka::consumer::BaseConsumer;
use rdkafka::message::{Message, OwnedMessage};
use rdkafka::ClientConfig;
use std::collections::HashMap;
use std::thread::{self, JoinHandle};
use std::time::Duration;

enum Signal {
    END,
    COMMIT(OwnedMessage),
}

pub struct TopicThread {
    thread: JoinHandle<()>,
    signal_bus: channel::Sender<Signal>,
}

pub struct ConsumeActor {
    config: HashMap<String, String>,
    topics: Vec<Topic>,
    recipient: Recipient<process::NotifyRequest>,
    active_topic_threads: HashMap<Topic, TopicThread>,
}

impl ConsumeActor {
    pub fn new(
        config: HashMap<String, String>,
        topics: Vec<String>,
        recipient: Recipient<process::NotifyRequest>,
    ) -> Self {
        Self {
            config,
            topics,
            recipient: recipient,
            active_topic_threads: HashMap::new(),
        }
    }

    fn create_consumer(&self) -> BaseConsumer {
        let mut client_config = ClientConfig::new();
        for (key, value) in self.config.iter() {
            client_config.set(key, value);
        }
        client_config.set("enable.auto.commit", "false");
        client_config.set("auto.offset.reset", "earliest");
        client_config.create::<BaseConsumer>().unwrap()
    }

    fn spawn_consume(
        &mut self,
        ctx: &mut Context<ConsumeActor>,
        topic: &str,
        signal_bus: channel::Receiver<Signal>,
    ) -> JoinHandle<()> {
        let topic = topic.to_string();
        let consume_thread = {
            let remove = {
                let recipient = ctx.address().recipient();
                move |topic: &str| {
                    recipient.do_send(consume::RemoveRequest(topic.to_string()));
                }
            };
            let consumer = self.create_consumer();
            consumer.subscribe(&[&topic]).unwrap();
            let recipient = self.recipient.clone();
            thread::spawn(move || loop {
                match signal_bus.try_recv() {
                    Ok(Signal::END) => {
                        break;
                    }
                    Ok(Signal::COMMIT(message)) => {
                        let topic = message.topic();
                        let partition = message.partition();
                        let offset = message.offset();
                        if consumer.commit(topic, partition, offset).is_err() {
                            remove(&topic);
                            break;
                        }
                        if consumer.resume(topic, partition).is_err() {
                            remove(&topic);
                            break;
                        }
                    }
                    Err(_) => {
                        // channel is empty. continue to consume.
                    }
                }
                match consumer.consume(Duration::from_secs(5)) {
                    Some(Ok(msg)) => {
                        recipient.do_send(process::NotifyRequest(msg.clone()));
                    }
                    Some(Err(e)) => {
                        info!("KafkaError occurred (Error: {})", e);
                        remove(&topic);
                        break;
                    }
                    None => {
                        // topic is empty. continue to consume.
                    }
                }
            })
        };
        consume_thread
    }
}

impl Actor for ConsumeActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        for topic in self.topics.clone().into_iter() {
            let (s, r) = channel::bounded::<Signal>(0);
            let thread = self.spawn_consume(ctx, &topic, r);
            self.active_topic_threads.insert(
                topic,
                TopicThread {
                    thread,
                    signal_bus: s,
                },
            );
        }
        ctx.run_interval(Duration::from_secs(60), |actor, ctx| {
            // TODO respawn TopicThread
        });
    }
}

impl Handler<consume::AddRequest> for ConsumeActor {
    type Result = ();
    fn handle(&mut self, msg: consume::AddRequest, ctx: &mut Self::Context) -> Self::Result {
        let topic = msg.0;
        if self.topics.contains(&topic) {
            return;
        }
        let (s, r) = channel::bounded::<Signal>(0);
        let thread = self.spawn_consume(ctx, &topic, r);
        self.active_topic_threads.insert(
            topic,
            TopicThread {
                thread,
                signal_bus: s,
            },
        );
    }
}

impl Handler<consume::CommitRequest> for ConsumeActor {
    type Result = ();
    fn handle(&mut self, msg: consume::CommitRequest, _ctx: &mut Self::Context) -> Self::Result {
        let topic = msg.0.topic();
        match self.active_topic_threads.get(topic) {
            Some(att) => {
                if att.signal_bus.send(Signal::COMMIT(msg.0.clone())).is_err() {
                    let partition = msg.0.partition();
                    let offset = msg.0.offset();
                    // Ignore. Because of `At least once`.
                    info!(
                        "Failed to commit (topic: {}, partition: {}, offset: {})",
                        topic, partition, offset
                    );
                };
            }
            None => {
                // Ignore. Because of `At least once`.
                info!("Topic thread is not activated (topic: {})", topic);
            }
        };
    }
}

impl Handler<consume::RemoveRequest> for ConsumeActor {
    type Result = ();
    fn handle(&mut self, msg: consume::RemoveRequest, ctx: &mut Self::Context) -> Self::Result {
        let consume::RemoveRequest(topic) = msg;
        if let Some(att) = self.active_topic_threads.remove(&topic) {
            info!(
                "Topic thread is removed from `active_topic_threads` (topic: {})",
                topic
            );
            // When `send` returns Err, thread has already been finished.
            if let Ok(_) = att.signal_bus.send(Signal::END) {
                if att.thread.join().is_err() {
                    error!("Panic occurred and shutdown ConsumeActor");
                    let active_topic_threads = std::mem::take(&mut self.active_topic_threads);
                    for (key, att) in active_topic_threads.into_iter() {
                        if att.signal_bus.send(Signal::END).is_err() {
                            error!("Topic thread is not activated (topic: {})", key);
                        }
                        if att.thread.join().is_err() {
                            error!(
                                "Failed to shutdown topic thread gracefully (topic: {})",
                                key
                            );
                        }
                    }
                    ctx.stop();
                }
            }
        }
    }
}
