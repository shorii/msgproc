use actix::prelude::*;
use rdkafka::message::Message;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::actors::msg::{consume, process};
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

struct ProcessActor {
    contexts: HashMap<TopicManagementKey, TopicManagementContext>,
    processors: Vec<Processor>,
    commit_recipient: Recipient<consume::CommitRequest>,
    stop_recipient: Recipient<consume::StopRequest>,
}

impl ProcessActor {
    pub fn new(
        commit_recipient: Recipient<consume::CommitRequest>,
        stop_recipient: Recipient<consume::StopRequest>,
    ) -> Self {
        Self {
            contexts: HashMap::new(),
            processors: vec![],
            commit_recipient,
            stop_recipient,
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
                            .do_send(consume::CommitRequest(key!(message)));
                    }
                }
            }
            Err(_) => {
                self.stop_recipient.do_send(consume::StopRequest);
            }
        };
    }
}
