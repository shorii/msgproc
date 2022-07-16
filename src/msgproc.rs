use crate::internal::consume::ConsumeActor;
use crate::internal::msg::process;
use crate::internal::process::ProcessActor;
use crate::kafka::consumer::IConsumer;
use crate::msg::Msg;
use actix::prelude::*;
use num_cpus;
use rdkafka::consumer::BaseConsumer;
use rdkafka::ClientConfig;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub struct MsgProcBuilder {
    consumer: Option<Arc<Box<dyn IConsumer>>>,
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

    pub fn consumer_config<K, V>(&mut self, config: HashMap<K, V>, topics: &[Regex]) -> &mut Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        let mut client_config = ClientConfig::new();
        for (key, value) in config.into_iter() {
            client_config.set::<K, V>(key, value);
        }
        client_config.set("enable.auto.commit", "false");
        let consumer = client_config.create::<BaseConsumer>().unwrap();
        let o_topics = consumer.get_topics(Duration::from_secs(5)).unwrap();
        let f_topics = o_topics
            .iter()
            .filter(|s| {
                for reg in topics.iter() {
                    if reg.is_match(s) {
                        return true;
                    }
                }
                false
            })
            .map(|s| s.as_str())
            .collect::<Vec<_>>();
        consumer.subscribe(&f_topics).unwrap();
        self.consumer = Some(Arc::new(Box::new(consumer)));
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

    pub fn build(&mut self) -> MsgProc {
        MsgProc::invoke(
            Arc::clone(&self.consumer.as_ref().unwrap()),
            self.parallels,
            &self.processors,
        )
    }
}

pub struct MsgProc {
    _p_addr: Addr<ProcessActor>,
    _c_addr: Addr<ConsumeActor>,
}

impl MsgProc {
    pub fn invoke(
        consumer: Arc<Box<dyn IConsumer>>,
        parallels: usize,
        processors: &[Recipient<Msg>],
    ) -> Self {
        let process_arbiter = Arbiter::new();
        let process_addr = Actor::start_in_arbiter(
            &process_arbiter.handle(),
            move |_: &mut Context<ProcessActor>| ProcessActor::new(),
        );

        let p_count = core::cmp::min(parallels, num_cpus::get());
        let consume_arbiter = Arbiter::new();
        let consume_addr = Actor::start_in_arbiter(&consume_arbiter.handle(), {
            let recipient = process_addr.clone().recipient();
            move |_: &mut Context<ConsumeActor>| ConsumeActor::new(consumer, p_count, recipient)
        });

        for processor in processors {
            process_addr.do_send(process::AddRequest(processor.clone()))
        }

        process_addr.do_send(process::SetupRequest {
            commit_recipient: consume_addr.clone().recipient(),
            stop_recipient: consume_addr.clone().recipient(),
        });

        MsgProc {
            _p_addr: process_addr,
            _c_addr: consume_addr,
        }
    }
}
