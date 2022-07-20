use actix::prelude::*;
use rdkafka::message::Message;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use uuid::Uuid;

use crate::internal::msg::{consume, process};
use crate::kafka::key::{key, TopicManagementKey};
use crate::msg::Msg;
use crate::msgproc::IMsgProcessor;

struct Processor {
    id: Uuid,
    proc: Recipient<process::DoneRequest>,
    processor: Arc<Mutex<Box<dyn IMsgProcessor>>>,
}

struct TopicManagementProcessContext {
    processor_ids: HashSet<Uuid>,
    notified: HashSet<Uuid>,
    done: HashSet<Uuid>,
    activated: HashMap<Uuid, JoinHandle<()>>,
}

impl TopicManagementProcessContext {
    fn new(processor_ids: &[Uuid]) -> Self {
        Self {
            processor_ids: processor_ids.iter().cloned().collect(),
            notified: HashSet::<Uuid>::new(),
            done: HashSet::<Uuid>::new(),
            activated: HashMap::new(),
        }
    }

    fn notify(&mut self, processor_id: Uuid, activated: JoinHandle<()>) {
        self.notified.insert(processor_id);
        self.activated.insert(processor_id, activated);
    }

    fn done(&mut self, processor_id: Uuid) -> bool {
        self.done.insert(processor_id);
        if self.notified == self.done && self.done == self.processor_ids {
            for id in self.done.iter() {
                let handle = self.activated.get(id).expect("activated must be set");
                if !handle.is_finished() {
                    panic!("activated must be terminated");
                }
            }
            return true;
        }
        false
    }
}

pub struct ProcessActor {
    contexts: HashMap<TopicManagementKey, TopicManagementProcessContext>,
    processors: Vec<Processor>,
    commit_recipient: Option<Recipient<consume::CommitRequest>>,
    stop_recipient: Option<Recipient<consume::RemoveRequest>>,
}

impl ProcessActor {
    pub fn new() -> Self {
        Self {
            contexts: HashMap::new(),
            processors: vec![],
            commit_recipient: None,
            stop_recipient: None,
        }
    }
}

impl Actor for ProcessActor {
    type Context = Context<Self>;
}

impl Handler<process::NotifyRequest> for ProcessActor {
    type Result = ();
    fn handle(&mut self, msg: process::NotifyRequest, _ctx: &mut Self::Context) -> Self::Result {
        let processor_ids = self.processors.iter().map(|p| p.id).collect::<Vec<_>>();
        let mut context = TopicManagementProcessContext::new(&processor_ids);
        for processor in &self.processors {
            let Processor {
                id,
                processor,
                proc,
            } = processor;

            let activated = {
                let id = *id;
                let processor = Arc::clone(processor);
                let proc = proc.clone();
                let owned_message = msg.0.clone();
                thread::spawn(move || {
                    let mut p = processor.lock().unwrap();
                    let msg = Msg::new(proc, owned_message, id);
                    p.process(msg);
                })
            };
            context.notify(*id, activated);
        }
        self.contexts.insert(key!(msg.0), context);
    }
}

impl Handler<process::AddRequest> for ProcessActor {
    type Result = ();
    fn handle(&mut self, msg: process::AddRequest, ctx: &mut Self::Context) -> Self::Result {
        self.processors.push(Processor {
            id: Uuid::new_v4(),
            proc: ctx.address().recipient(),
            processor: msg.0,
        });
    }
}

impl Handler<process::DoneRequest> for ProcessActor {
    type Result = ();
    fn handle(&mut self, msg: process::DoneRequest, _ctx: &mut Self::Context) -> Self::Result {
        match msg.0 {
            Ok(descriptor) => {
                let process::ProcessDescriptor {
                    message,
                    processor_id,
                } = descriptor;
                let context = self.contexts.get_mut(&key!(message));
                if let Some(context) = context {
                    if context.done(processor_id) {
                        self.commit_recipient
                            .as_ref()
                            .unwrap()
                            .do_send(consume::CommitRequest(message));
                    }
                }
            }
            Err(topic) => {
                self.stop_recipient
                    .as_ref()
                    .unwrap()
                    .do_send(consume::RemoveRequest(topic));
            }
        };
    }
}

impl Handler<process::SetupRequest> for ProcessActor {
    type Result = ();
    fn handle(&mut self, msg: process::SetupRequest, _ctx: &mut Self::Context) -> Self::Result {
        self.commit_recipient = Some(msg.commit_recipient);
        self.stop_recipient = Some(msg.stop_recipient);
    }
}
