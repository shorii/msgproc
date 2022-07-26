use msgproc::msgproc::{IMsgProcessor, MsgProc, MsgProcBuilder};
use rdkafka::producer::BaseProducer;
use rdkafka::ClientConfig;
use std::collections::HashMap;

pub fn create_producer() -> BaseProducer {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .unwrap()
}

pub fn create_msgproc_builder(processors: Vec<Box<dyn IMsgProcessor>>, topics: &[&str]) -> MsgProc {
    let mut config = HashMap::new();
    config.insert("group.id".to_string(), "test".to_string());
    config.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    config.insert("session.timeout.ms".to_string(), "6000".to_string());
    config.insert("auto.offset.reset".to_string(), "earliest".to_string());
    let mut builder = MsgProcBuilder::new();
    builder.config(config).topics(topics);
    for processor in processors.into_iter() {
        builder.processor(processor);
    }
    builder.build()
}
