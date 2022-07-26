use crate::internal::consume::ConsumeActor;
use crate::internal::msg::process;
use crate::internal::process::ProcessActor;
use crate::kafka::alias::Topic;
use crate::msg::Msg;
use actix::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub trait IMsgProcessor: Send + Sync + 'static {
    fn process(&mut self, msg: &mut Msg);
}

pub struct MsgProcBuilder {
    config: Option<HashMap<String, String>>,
    topics: Vec<Topic>,
    processors: Vec<Box<dyn IMsgProcessor>>,
}

impl Default for MsgProcBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MsgProcBuilder {
    pub fn new() -> Self {
        Self {
            config: None,
            topics: vec![],
            processors: vec![],
        }
    }

    pub fn config(&mut self, config: HashMap<String, String>) -> &mut Self {
        self.config = Some(config);
        self
    }

    pub fn topics<T, U>(&mut self, topics: T) -> &mut Self
    where
        T: Into<Vec<U>>,
        U: Into<String>,
    {
        let into_vec: Vec<U> = topics.into();
        self.topics = into_vec.into_iter().map(|f| f.into()).collect::<Vec<_>>();
        self
    }

    pub fn processor(&mut self, processor: Box<dyn IMsgProcessor>) -> &mut Self {
        self.processors.push(processor);
        self
    }

    pub fn build(self) -> MsgProc {
        MsgProc::new(self.config.unwrap(), self.topics, self.processors)
    }
}

pub struct MsgProc {
    _p_addr: Addr<ProcessActor>,
    _c_addr: Addr<ConsumeActor>,
    system_runner: SystemRunner,
}

impl MsgProc {
    fn new(
        config: HashMap<String, String>,
        topics: Vec<String>,
        processors: Vec<Box<dyn IMsgProcessor>>,
    ) -> Self {
        let system_runner = System::new();
        let process_arbiter = Arbiter::new();
        let process_addr = Actor::start_in_arbiter(
            &process_arbiter.handle(),
            move |_: &mut Context<ProcessActor>| ProcessActor::new(),
        );

        let consume_arbiter = Arbiter::new();
        let consume_addr = Actor::start_in_arbiter(&consume_arbiter.handle(), {
            let recipient = process_addr.clone().recipient();
            move |_: &mut Context<ConsumeActor>| ConsumeActor::new(config, topics, recipient)
        });

        for processor in processors {
            process_addr.do_send(process::AddRequest(Arc::new(Mutex::new(processor))));
        }

        process_addr.do_send(process::SetupRequest {
            commit_recipient: consume_addr.clone().recipient(),
            stop_recipient: consume_addr.clone().recipient(),
        });

        MsgProc {
            _p_addr: process_addr,
            _c_addr: consume_addr,
            system_runner,
        }
    }

    pub fn run(self) -> std::io::Result<()> {
        self.system_runner.run()
    }
}
