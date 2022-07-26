use actix::prelude::*;
use rdkafka::message::{Message, OwnedMessage};
use std::collections::{HashMap, HashSet};
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use uuid::Uuid;

use crate::internal::msg::{consume, process, ProcessorId};
use crate::internal::utils::RecipientExt;
use crate::kafka::alias::{Partition, Topic};
use crate::msg::Msg;
use crate::msgproc::IMsgProcessor;

struct Processor {
    id: ProcessorId,
    proc: Recipient<process::DoneRequest>,
    processor: Arc<Mutex<Box<dyn IMsgProcessor>>>,
}

struct TopicManagementProcessContext {
    processor_ids: HashSet<ProcessorId>,
    notified: HashSet<ProcessorId>,
    done: HashSet<ProcessorId>,
    activated: HashMap<ProcessorId, JoinHandle<()>>,
}

impl TopicManagementProcessContext {
    fn new(processor_ids: &[ProcessorId]) -> Self {
        Self {
            processor_ids: HashSet::from_iter(processor_ids.to_vec()),
            notified: HashSet::<ProcessorId>::new(),
            done: HashSet::<ProcessorId>::new(),
            activated: HashMap::new(),
        }
    }

    fn notify(&mut self, processor_id: ProcessorId, activated: JoinHandle<()>) {
        self.notified.insert(processor_id);
        self.activated.insert(processor_id, activated);
    }

    fn done(&mut self, processor_id: ProcessorId) -> bool {
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

#[derive(Eq, PartialEq, Hash)]
struct TopicManagementProcessContextKey {
    pub topic: Topic,
    pub partition: Partition,
}

impl TopicManagementProcessContextKey {
    fn new(msg: OwnedMessage) -> Self {
        let topic = msg.topic().to_string();
        let partition = msg.partition();
        Self { topic, partition }
    }
}

pub struct ProcessActor {
    contexts: HashMap<TopicManagementProcessContextKey, TopicManagementProcessContext>,
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
                    let mut msg = Msg::new(proc, owned_message, id);
                    let result = panic::catch_unwind(AssertUnwindSafe(|| {
                        let mut p = processor.lock().unwrap();
                        p.process(&mut msg);
                    }));
                    if result.is_err() {
                        msg.mark_as_panic("Failed to process message");
                    }
                })
            };
            context.notify(*id, activated);
        }
        self.contexts
            .insert(TopicManagementProcessContextKey::new(msg.0), context);
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
            process::ProcessStatus::Success(descriptor) => {
                let process::ProcessDescriptor {
                    message,
                    processor_id,
                } = descriptor;
                let key = TopicManagementProcessContextKey::new(message.clone());
                let context = self.contexts.get_mut(&key);
                if let Some(context) = context {
                    if context.done(processor_id) {
                        let result = self
                            .commit_recipient
                            .as_ref()
                            .unwrap()
                            .send_safety(consume::CommitRequest(message));
                        if result.is_err() {
                            System::current().stop();
                        }
                    }
                }
            }
            process::ProcessStatus::Error(topic) => {
                let result = self
                    .stop_recipient
                    .as_ref()
                    .unwrap()
                    .send_safety(consume::RemoveRequest(topic));
                if result.is_err() {
                    System::current().stop();
                }
            }
            process::ProcessStatus::Panic => System::current().stop(),
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
