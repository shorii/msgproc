//! kafkaのConsumerのトピックごとのオフセットなどをユーザ定義の
//! 処理の結果に応じて管理するライブラリ。
//!
//! ### Features
//!
//! - トピックごとにオフセットを処理の結果に応じて管理
//! - トピックを正規表現によって指定可能で、[msgproc::MsgProc]動作中にkafkaに存在するトピックが変化した場合自動的に更新
//! - ポリシーによるkafkaとのやり取り失敗時の挙動の制御、メッセージの消費速度、トピック更新速度の制御
//!
//! ### Examples
//!
//! ```no_run
//! use msgproc::policy::DefaultJobPolicy;
//! use msgproc::msgproc::MsgProc;
//! use msgproc::msg::{Msg, MsgProcessor};
//! use rdkafka::consumer::BaseConsumer;
//! use rdkafka::ClientConfig;
//! use std::time::Duration;
//! use regex::Regex;
//! use actix::prelude::*;
//!
//! struct PrintProcessor;
//!
//! fn something_went_wrong() -> bool {
//!     true
//! }
//!
//! impl Actor for PrintProcessor {
//!     type Context = Context<Self>;
//! }
//!
//! impl Handler<Msg> for PrintProcessor {
//!     type Result = ();
//!     fn handle(&mut self, msg: Msg, ctx: &mut Self::Context) -> Self::Result {
//!         let owned_message = msg.get_owned_message();
//!         if something_went_wrong() {
//!             msg.mark_as_error("Something went wrong!")
//!         } else {
//!             println!("{:?}", owned_message);
//!         }
//!     }
//! }
//!
//! #[actix::main]
//! async fn main() {
//!     let topic_patterns = vec![
//!         Regex::new("^sample-.*").unwrap()
//!     ];
//!     let consumer: Box<BaseConsumer> = Box::new(
//!         ClientConfig::new()
//!             .set("group.id", "group1")
//!             .set("bootstrap.servers", "localhost:9092")
//!             .set("enable.auto.commit", "false")
//!             .set("session.timeout.ms", "6000")
//!             .create().unwrap()
//!     );
//!     let update_topic_policy = Box::new(
//!         DefaultJobPolicy::new(Duration::from_secs(3600), 5)
//!     );
//!     let consume_message_policy = Box::new(
//!         DefaultJobPolicy::new(Duration::from_secs(0), 5)
//!     );
//!     let timeout = Duration::from_secs(5);
//!     let msgproc = MsgProc::new(
//!         topic_patterns,
//!         consumer,
//!         update_topic_policy,
//!         consume_message_policy,
//!         timeout,
//!     );
//!     let msgproc_addr = msgproc.start();
//!     msgproc_addr
//!         .send(MsgProcessor(PrintProcessor.start().recipient()))
//!         .await
//!         .unwrap();
//! }
//! ```

mod actors;
mod error;
mod kafka;
pub mod msg;
pub mod policy;
