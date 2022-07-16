use actix::prelude::*;
use common::{create_msgproc_builder, create_producer};
use msgproc::msg::Msg;
use rdkafka::message::Message;
use rdkafka::producer::BaseRecord;
use regex::Regex;
use std::time::Duration;

mod common;

struct Processor {
    msgs: Vec<String>,
}

impl Actor for Processor {
    type Context = Context<Self>;
}

impl Handler<Msg> for Processor {
    type Result = ();

    fn handle(&mut self, msg: Msg, _ctx: &mut Self::Context) -> Self::Result {
        let owned_message = msg.get_owned_message();
        let payload = owned_message.payload_view::<str>().unwrap().unwrap();
        println!("{}", payload.to_string());
        self.msgs.push(payload.to_string());
    }
}

#[derive(Message)]
#[rtype(result = "GetStoredMsgResponse")]
struct GetStoredMsg;

#[derive(MessageResponse)]
struct GetStoredMsgResponse(Vec<String>);

impl Handler<GetStoredMsg> for Processor {
    type Result = GetStoredMsgResponse;
    fn handle(&mut self, _msg: GetStoredMsg, _ctx: &mut Self::Context) -> Self::Result {
        GetStoredMsgResponse(self.msgs.clone())
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

#[actix::test]
async fn integration_test() {
    let test_name = "integration_test";
    setup(test_name);
    let arbiter = Arbiter::new();
    let addr = Actor::start_in_arbiter(&arbiter.handle(), |_| Processor { msgs: vec![] });
    let _msgproc = create_msgproc_builder(
        &[addr.clone().recipient()],
        &[Regex::new(&format!("^{}$", test_name)).unwrap()],
    );
    tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;
    let response = addr.send(GetStoredMsg).await.unwrap();
    let msgs = response.0;
    assert_eq!(msgs.len(), 5);
    assert_eq!(msgs[0], "test_message: 0");
    assert_eq!(msgs[1], "test_message: 1");
    assert_eq!(msgs[2], "test_message: 2");
    assert_eq!(msgs[3], "test_message: 3");
    assert_eq!(msgs[4], "test_message: 4");
}
