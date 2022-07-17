use actix::Recipient;
use msgproc::msg::Msg;
use msgproc::msgproc::{MsgProc, MsgProcBuilder};
use rdkafka::producer::BaseProducer;
use rdkafka::ClientConfig;
use regex::Regex;
use std::collections::HashMap;

pub fn create_producer() -> BaseProducer {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .unwrap()
}

pub fn create_msgproc_builder(processors: &[Recipient<Msg>], topics: &[Regex]) -> MsgProc {
    let mut config = HashMap::new();
    config.insert("group.id", "test");
    config.insert("bootstrap.servers", "localhost:9092");
    config.insert("session.timeout.ms", "6000");
    config.insert("auto.offset.reset", "earliest");
    let mut builder = MsgProcBuilder::new();
    builder.consumer_config(config, topics).parallels(2);
    for processor in processors {
        builder.processor(processor.clone());
    }
    builder.build()
}
