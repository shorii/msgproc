use async_trait::async_trait;
use env_logger;
use msgproc::prelude::*;
use tokio::signal;

struct Processor;

#[async_trait]
impl IProcessor for Processor {
    async fn execute(&self, msg: OwnedMessage) -> Result<(), &'static str> {
        println!("{:?}", msg);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let mut msgproc = MsgProcConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "group")
        .set("session.timeout.ms", "6000")
        .set("max.poll.interval.ms", "6000")
        .topics(&["sample_topic"])
        .processor(Processor)
        .limit(64)
        .create()
        .unwrap();
    msgproc.run(signal::ctrl_c()).await;
}
