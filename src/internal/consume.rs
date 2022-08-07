use crate::internal::msg::{consume, process};
use crate::internal::utils::RecipientExt;
use crate::kafka::alias::Topic;
use crate::kafka::consumer::{BaseConsumer, IConsumer};
use actix::prelude::*;
use crossbeam::channel;
use log::{error, info};
use rdkafka::message::{Message, OwnedMessage};
use rdkafka::ClientConfig;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::thread::{self, JoinHandle};
use std::time::Duration;

enum ConsumeSignal {
    End,
    Commit(OwnedMessage),
}

pub struct TopicThread {
    thread: JoinHandle<()>,
    signal_bus: channel::Sender<ConsumeSignal>,
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
            recipient,
            active_topic_threads: HashMap::new(),
        }
    }

    fn create_consumer(&self, topic: &str) -> BaseConsumer {
        let mut client_config = ClientConfig::new();
        for (key, value) in self.config.iter() {
            client_config.set(key, value);
        }
        client_config.set("group.id", topic);
        client_config.set("enable.auto.commit", "false");
        client_config.set("auto.offset.reset", "earliest");
        client_config.create::<BaseConsumer>().unwrap()
    }

    fn spawn_consume(
        &mut self,
        ctx: &mut Context<ConsumeActor>,
        topic: &str,
        signal_bus: channel::Receiver<ConsumeSignal>,
    ) -> JoinHandle<()> {
        let topic = topic.to_string();
        let remove = {
            let recipient = ctx.address().recipient();
            move |topic: &str| {
                recipient
                    .send_safety(consume::RemoveRequest(topic.to_string()))
                    .unwrap();
            }
        };
        let consumer = self.create_consumer(&topic);
        consumer.subscribe(&[&topic]).unwrap();
        let recipient = self.recipient.clone();
        thread::spawn(move || loop {
            match signal_bus.try_recv() {
                Ok(ConsumeSignal::End) => {
                    break;
                }
                Ok(ConsumeSignal::Commit(message)) => {
                    let partition = message.partition();
                    let offset = message.offset();
                    if consumer.commit(&topic, partition, offset).is_err() {
                        remove(&topic);
                        break;
                    }
                    if consumer.resume(&topic, partition).is_err() {
                        remove(&topic);
                        break;
                    }
                }
                Err(_) => {
                    // channel is empty. continue to consume.
                }
            }
            match consumer.consume(Duration::from_secs(0)) {
                Some(Ok(msg)) => {
                    let partition = msg.partition();
                    if consumer.pause(&topic, partition).is_err() {
                        remove(&topic);
                        break;
                    }
                    let result = recipient.send_safety(process::NotifyRequest(msg.clone()));
                    if result.is_err() {
                        remove(&topic);
                        break;
                    }
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
    }
}

impl Actor for ConsumeActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        for topic in self.topics.clone().into_iter() {
            let (s, r) = channel::bounded::<ConsumeSignal>(0);
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
            let registered_topic_set: HashSet<String> =
                HashSet::from_iter(actor.topics.iter().cloned());
            let activated_topic_set: HashSet<String> =
                HashSet::from_iter(actor.active_topic_threads.iter().map(|x| x.0).cloned());
            for topic in registered_topic_set.difference(&activated_topic_set) {
                let (s, r) = channel::bounded::<ConsumeSignal>(0);
                let thread = actor.spawn_consume(ctx, topic, r);
                actor.active_topic_threads.insert(
                    topic.to_string(),
                    TopicThread {
                        thread,
                        signal_bus: s,
                    },
                );
            }
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
        let (s, r) = channel::bounded::<ConsumeSignal>(0);
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
                if att
                    .signal_bus
                    .send(ConsumeSignal::Commit(msg.0.clone()))
                    .is_err()
                {
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
            if att.signal_bus.send(ConsumeSignal::End).is_ok() && att.thread.join().is_err() {
                error!("Panic occurred and shutdown ConsumeActor");
                let active_topic_threads = std::mem::take(&mut self.active_topic_threads);
                for (key, att) in active_topic_threads.into_iter() {
                    if att.signal_bus.send(ConsumeSignal::End).is_err() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{self, AssertUnwindSafe};

    struct NoopActor;

    impl Actor for NoopActor {
        type Context = Context<Self>;
    }

    impl Handler<process::NotifyRequest> for NoopActor {
        type Result = ();
        fn handle(
            &mut self,
            _msg: process::NotifyRequest,
            _ctx: &mut Self::Context,
        ) -> Self::Result {
            // noop
        }
    }

    fn run_test<F>(topics: Vec<String>, timeout: Duration, test_fn: F) -> thread::Result<()>
    where
        F: FnOnce(Addr<ConsumeActor>) -> () + Send + 'static,
    {
        let test_fn = |addr| panic::catch_unwind(AssertUnwindSafe(|| test_fn(addr)));
        let system_runner = System::new();
        let process_arbiter = Arbiter::new();
        let process_addr = Actor::start_in_arbiter(
            &process_arbiter.handle(),
            move |_: &mut Context<NoopActor>| NoopActor,
        );

        let consume_arbiter = Arbiter::new();
        let consume_addr = Actor::start_in_arbiter(&consume_arbiter.handle(), {
            let recipient = process_addr.clone().recipient();
            move |_: &mut Context<ConsumeActor>| {
                ConsumeActor::new(HashMap::new(), topics, recipient)
            }
        });
        let handle = {
            let system = System::current();
            thread::spawn(move || {
                loop {
                    let consume_addr = consume_addr.clone();
                    let result = test_fn(consume_addr);
                    if result.is_err() {
                        // TODO timeout process. continue
                        break;
                    }
                }
                system.stop();
            })
        };
        system_runner.run();
        handle.join()
    }

    #[test]
    fn test_add_request() {}
}
