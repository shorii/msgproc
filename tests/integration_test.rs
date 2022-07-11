use actix::prelude::*;
use msgproc::msg::Msg;
use msgproc::msg::MsgProcessor;
use msgproc::msgproc::MsgProc;
use rdkafka::message::Message;
use rdkafka::producer::BaseRecord;
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
    let producer = &common::create_producer();
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
    let test_name = "integration_test1";

    setup(test_name);

    let msgproc: MsgProc = common::create_message_processor(test_name);
    let addr = msgproc.start();

    let processor_addr = Processor { msgs: vec![] }.start();

    addr.send(MsgProcessor(processor_addr.clone().recipient()))
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(120)).await;

    let response = processor_addr.send(GetStoredMsg).await.unwrap();
    let msgs = response.0;
    assert_eq!(msgs.len(), 5);
    assert_eq!(msgs[0], "test_message: 0");
    assert_eq!(msgs[1], "test_message: 1");
    assert_eq!(msgs[2], "test_message: 2");
    assert_eq!(msgs[3], "test_message: 3");
    assert_eq!(msgs[4], "test_message: 4");
}
