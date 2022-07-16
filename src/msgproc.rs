use crate::internal::consume::ConsumeActor;
use crate::internal::msg::{consume, process};
use crate::internal::process::ProcessActor;
use crate::kafka::consumer::IConsumer;
use crate::msg::Msg;
use actix::prelude::*;
use num_cpus;
use rdkafka::consumer::BaseConsumer;
use rdkafka::ClientConfig;
use std::collections::HashMap;

pub struct MsgProcBuilder {
    consumer: Option<Box<dyn IConsumer>>,
    parallels: usize,
    processors: Vec<Recipient<Msg>>,
}

impl MsgProcBuilder {
    pub fn new() -> Self {
        Self {
            consumer: None,
            parallels: 1,
            processors: vec![],
        }
    }

    pub fn consumer_config<K, V>(&mut self, config: HashMap<K, V>) -> &mut Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        let mut client_config = ClientConfig::new();
        for (key, value) in config.into_iter() {
            client_config.set::<K, V>(key, value);
        }
        client_config.set("enable.auto.commit", "false");
        self.consumer = Some(Box::new(client_config.create::<BaseConsumer>().unwrap()));

        self
    }

    pub fn parallels(&mut self, parallels: usize) -> &mut Self {
        let p_count = core::cmp::max(parallels, self.parallels);
        self.parallels = p_count;
        self
    }

    pub fn processor(&mut self, processor: Recipient<Msg>) -> &mut Self {
        self.processors.push(processor);
        self
    }
}

pub struct MsgProc {
    _p_addr: Addr<ProcessActor>,
    _c_addr: Addr<ConsumeActor>,
}

impl MsgProc {
    pub fn invoke(
        consumer: Box<dyn IConsumer>,
        parallels: usize,
        processors: &[Recipient<Msg>],
    ) -> Self {
        let p_count = core::cmp::min(parallels, num_cpus::get());
        let consume_arbiter = Arbiter::new();
        let consume_addr = Actor::start_in_arbiter(
            &consume_arbiter.handle(),
            move |_: &mut Context<ConsumeActor>| ConsumeActor::new(consumer, p_count),
        );

        let process_arbiter = Arbiter::new();
        let process_addr = Actor::start_in_arbiter(
            &process_arbiter.handle(),
            move |_: &mut Context<ProcessActor>| ProcessActor::new(),
        );
        for processor in processors {
            process_addr.do_send(process::AddRequest(processor.clone()))
        }

        process_addr.do_send(process::SetupRequest {
            commit_recipient: consume_addr.clone().recipient(),
            stop_recipient: consume_addr.clone().recipient(),
        });

        consume_addr.do_send(consume::SetupRequest(process_addr.clone().recipient()));

        MsgProc {
            _p_addr: process_addr,
            _c_addr: consume_addr,
        }
    }
}
