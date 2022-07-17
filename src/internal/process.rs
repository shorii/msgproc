use actix::prelude::*;
use rdkafka::message::Message;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::internal::msg::{consume, process};
use crate::kafka::key::{key, TopicManagementKey};
use crate::msg::Msg;

struct Processor {
    id: Uuid,
    proc: Recipient<process::DoneRequest>,
    processor: Recipient<Msg>,
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

pub struct ProcessActor {
    contexts: HashMap<TopicManagementKey, TopicManagementContext>,
    processors: Vec<Processor>,
    commit_recipient: Option<Recipient<consume::CommitRequest>>,
    stop_recipient: Option<Recipient<consume::StopRequest>>,
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
        for processor in &self.processors {
            let Processor {
                id,
                processor,
                proc,
            } = processor;
            processor.do_send(Msg::new(proc.clone(), msg.0.clone(), *id));
            let mut context = TopicManagementContext::new();
            context.notify(*id);
            self.contexts.insert(key!(msg.0), context);
        }
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
                            .do_send(consume::CommitRequest(key!(message)));
                    }
                }
            }
            Err(_) => {
                self.stop_recipient
                    .as_ref()
                    .unwrap()
                    .do_send(consume::StopRequest);
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
