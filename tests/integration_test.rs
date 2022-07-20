use common::{create_msgproc_builder, create_producer};
use msgproc::msg::Msg;
use msgproc::msgproc::IMsgProcessor;
use rdkafka::message::Message;
use rdkafka::producer::BaseRecord;
use std::time::Duration;

mod common;

struct Processor1;

impl IMsgProcessor for Processor1 {
    fn process(&mut self, msg: Msg) {
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
    fn process(&mut self, msg: Msg) {
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

fn setup(test_name: &str) {
    let producer = &create_producer();
    for n in 0..5 {
        producer
            .send(
                BaseRecord::<str, str>::to(test_name)
                    .payload(format!("test_message: {}", n).as_str()),
            )
            .unwrap();
    }
    for _ in 0..10 {
        producer.poll(Duration::from_millis(100));
    }
}

#[test]
fn integration_test() {
    let test_name1 = "integration_test1";
    let test_name2 = "integration_test2";
    setup(test_name1);
    setup(test_name2);
    let msgproc = create_msgproc_builder(
        vec![Box::new(Processor1), Box::new(Processor2)],
        &[test_name1, test_name2],
    );
    msgproc.run().unwrap();
}
