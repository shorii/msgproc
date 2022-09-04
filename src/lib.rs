//! kafkaからメッセージを処理し、オフセットを更新するライブラリ。
//!
//! ### Features
//!
//! - [processor::IProcessor]を実装することでメッセージの処理部分を組み込むことができる
//!
//! ### Examples
//!
//! ```no_run
//! use async_trait::async_trait;
//! use env_logger;
//! use msgproc::prelude::*;
//! use tokio::signal;
//!
//! struct Processor;
//!
//! #[async_trait]
//! impl IProcessor for Processor {
//!     async fn execute(&mut self, msg: message::Message) -> Result<(), &'static str> {
//!         println!("{:?}", msg);
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     env_logger::init();
//!     let msgproc = MsgProcConfig::new()
//!         .set("bootstrap.servers", "localhost:9092")
//!         .set("group.id", "group")
//!         .set("session.timeout.ms", "6000")
//!         .set("max.poll.interval.ms", "6000")
//!         .topics(&["sample_topic"])
//!         .processor(Processor)
//!         .create();
//!     msgproc.run(signal::ctrl_c()).await;
//! }
//! ```
mod consumer;
mod context;
mod kafka;
mod msgproc;
mod options;
mod processor;

pub mod prelude {
    pub use super::kafka::*;
    pub use super::msgproc::*;
    pub use super::processor::*;
}
