use msgproc::consumer::IConsumer;
use msgproc::msgproc::MsgProc;
use msgproc::policy::DefaultJobPolicy;
use rdkafka::consumer::BaseConsumer;
use rdkafka::producer::BaseProducer;
use rdkafka::ClientConfig;
use regex::Regex;
use std::time::Duration;

pub fn create_producer() -> BaseProducer {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .unwrap()
}

pub fn create_message_processor(testcase: &str) -> MsgProc {
    let topic_patterns = vec![Regex::new(testcase).unwrap()];
    let consumer: Box<BaseConsumer> = Box::new(
        ClientConfig::new()
            .set("group.id", "mygroup")
            .set("bootstrap.servers", "localhost:9092")
            .set("enable.auto.commit", "false")
            .set("session.timeout.ms", "6000")
            .create()
            .unwrap(),
    );
    consumer.subscribe(&["integration_test1"]).unwrap();
    let update_topic_policy = Box::new(DefaultJobPolicy::new(Duration::from_secs(3600), 5));
    let consume_message_policy = Box::new(DefaultJobPolicy::new(Duration::from_secs(0), 5));
    let timeout = Duration::from_secs(5);
    MsgProc::new(
        topic_patterns,
        consumer,
        update_topic_policy,
        consume_message_policy,
        timeout,
    )
}
