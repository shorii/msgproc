use crate::internal::msg::{consume, process};
use crate::kafka::consumer::IConsumer;
use crate::kafka::key::{key, TopicManagementKey};
use actix::prelude::*;
use crossbeam::channel;
use rdkafka::message::Message;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

enum Signal {
    END,
    COMMIT(TopicManagementKey),
}

pub struct TopicOffsetManagementContext {
    offset: Option<i64>,
}

pub struct ConsumeActor {
    threads: Vec<JoinHandle<()>>,
    offsets: Arc<RwLock<HashMap<TopicManagementKey, TopicOffsetManagementContext>>>,
    consumer: Arc<Box<dyn IConsumer>>,
    recipient: Option<Recipient<process::NotifyRequest>>,
    parallels: usize,
    signal_bus: Option<channel::Sender<Signal>>,
    stopping: AtomicBool,
}

impl ConsumeActor {
    pub fn new(consumer: Box<dyn IConsumer>, parallels: usize) -> Self {
        Self {
            threads: vec![],
            offsets: Arc::new(RwLock::new(HashMap::new())),
            consumer: Arc::new(consumer),
            recipient: None,
            parallels,
            signal_bus: None,
            stopping: AtomicBool::new(false),
        }
    }

    fn spawn_consume(
        &mut self,
        ctx: &mut Context<ConsumeActor>,
        signal_bus: channel::Receiver<Signal>,
    ) -> JoinHandle<()> {
        let consume_thread = {
            let stop = {
                let sender = self.signal_bus.as_ref().unwrap().clone();
                let recipient = ctx.address().recipient();
                move || {
                    sender
                        .send(Signal::END)
                        .expect("Failed to send Signal::END");
                    recipient.do_send(consume::StopRequest);
                }
            };
            let offsets = Arc::clone(&self.offsets);
            let consumer = Arc::clone(&self.consumer);
            let recipient = self.recipient.clone();
            thread::spawn(move || loop {
                if recipient.is_none() {
                    continue;
                }
                let recipient = recipient.as_ref().unwrap();
                match signal_bus.try_recv() {
                    Ok(Signal::END) => {
                        break;
                    }
                    Ok(Signal::COMMIT(key)) => {
                        let acquired = offsets.write();
                        if acquired.is_err() {
                            stop();
                            break;
                        }
                        let context = acquired.as_ref().unwrap().get(&key);
                        if context.is_none() {
                            stop();
                            break;
                        }
                        let TopicOffsetManagementContext { mut offset } = context.unwrap();
                        match offset.take() {
                            Some(offset) => {
                                let topic = key.topic;
                                let partition = key.partition;
                                if consumer.commit(&topic, partition, offset).is_err() {
                                    stop();
                                    break;
                                }
                                if consumer.resume(&topic, partition).is_err() {
                                    stop();
                                    break;
                                }
                            }
                            None => {
                                // committed by other thread. continue to consume.
                            }
                        }
                    }
                    Err(_) => {
                        // channel is empty. continue to consume.
                    }
                }
                match consumer.consume(Duration::from_secs(5)) {
                    Some(Ok(msg)) => {
                        recipient.do_send(process::NotifyRequest(msg.clone()));
                        let topic = msg.topic();
                        let partition = msg.partition();
                        let offset = msg.offset();
                        if consumer.pause(topic, partition).is_err() {
                            // TODO log error message
                            stop();
                            break;
                        }
                        let acquired = offsets.write();
                        if acquired.is_err() {
                            stop();
                            break;
                        }
                        let mut context = acquired.unwrap();
                        context.insert(
                            key!(msg),
                            TopicOffsetManagementContext {
                                offset: Some(offset),
                            },
                        );
                    }
                    Some(Err(_)) => {
                        // TODO log error message
                        stop();
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
        let (s, r) = channel::bounded::<Signal>(0);
        self.signal_bus = Some(s.clone());
        for _ in 0..self.parallels {
            let thread = self.spawn_consume(ctx, r.clone());
            self.threads.push(thread);
        }
    }
}

impl Handler<consume::AddRequest> for ConsumeActor {
    type Result = ();
    fn handle(&mut self, msg: consume::AddRequest, _ctx: &mut Self::Context) -> Self::Result {
        let mut offsets = self.offsets.write().unwrap();
        offsets.insert(msg.0, TopicOffsetManagementContext { offset: None });
    }
}

impl Handler<consume::CommitRequest> for ConsumeActor {
    type Result = ();
    fn handle(&mut self, msg: consume::CommitRequest, _ctx: &mut Self::Context) -> Self::Result {
        self.signal_bus
            .as_ref()
            .unwrap()
            .send(Signal::COMMIT(msg.0))
            .expect("Failed to send Signal::Commit");
    }
}

impl Handler<consume::StopRequest> for ConsumeActor {
    type Result = ();
    fn handle(&mut self, _msg: consume::StopRequest, ctx: &mut Self::Context) -> Self::Result {
        let stopping = self.stopping.get_mut();
        if *stopping {
            return;
        }
        *stopping = true;
        let threads = std::mem::take(&mut self.threads);
        let mut deadline = 5;
        loop {
            let mut finished = true;
            for thread in &threads {
                if thread.is_finished() {
                    continue;
                }
                finished = false;
            }
            if finished {
                break;
            }
            thread::sleep(Duration::from_secs(1));
            deadline -= 1;
            if deadline >= 0 {
                break;
            }
        }
        ctx.stop();
    }
}

impl Handler<consume::SetupRequest> for ConsumeActor {
    type Result = ();
    fn handle(&mut self, msg: consume::SetupRequest, _ctx: &mut Self::Context) -> Self::Result {
        self.recipient = Some(msg.0);
    }
}
