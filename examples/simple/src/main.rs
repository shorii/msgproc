use msgproc::msg::Msg;
use msgproc::msgproc::{IMsgProcessor, MsgProcBuilder};
use rdkafka::message::Message;
use rdkafka::producer::{BaseProducer, BaseRecord};
use rdkafka::ClientConfig;
use std::collections::HashMap;
use std::time::Duration;

struct Processor1;

impl IMsgProcessor for Processor1 {
    fn process(&mut self, msg: &mut Msg) {
        let message = msg.get_owned_message();
        let body = message.payload_view::<str>().unwrap().unwrap();
        println!(
            "Processor1: (body: {}, topic: {}, partition: {})",
            body,
            message.topic(),
            message.partition()
        );
    }
}

struct Processor2;

impl IMsgProcessor for Processor2 {
    fn process(&mut self, msg: &mut Msg) {
        let message = msg.get_owned_message();
        let body = message.payload_view::<str>().unwrap().unwrap();
        println!(
            "Processor2: (body: {}, topic: {}, partition: {})",
            body,
            message.topic(),
            message.partition()
        );
    }
}

fn setup(topics: &[&str]) {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .unwrap();
    for topic in topics {
        for n in 0..5 {
            producer
                .send(BaseRecord::<str, str>::to(topic).payload(format!("message: {}", n).as_str()))
                .unwrap();
        }
    }
    for _ in 0..10 {
        producer.poll(Duration::from_millis(100));
    }
}

fn main() {
    let topics = ["topic_foo", "topic_bar"];
    setup(&topics);

    let mut config = HashMap::new();
    config.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    config.insert("session.timeout.ms".to_string(), "6000".to_string());

    let builder = MsgProcBuilder::new();
    let msgproc = builder
        .config(config)
        .topics(topics)
        .processor(Box::new(Processor1))
        .processor(Box::new(Processor2))
        .build();
    msgproc.run().unwrap();
}
