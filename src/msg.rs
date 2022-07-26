use crate::internal::msg::process;
use crate::internal::msg::ProcessorId;
use actix::prelude::*;
use rdkafka::message::Message;
use rdkafka::message::OwnedMessage;
use std::cell::RefCell;

/// ユーザ定義の[Handler]で処理するデータ型
///
/// kafkaから[crate::consumer::IConsumer::consume]されたデータと[crate::msgproc::MsgProc]を制御するためのデータが定義されており、データの入れ物としてだけでなく
/// ユーザ定義の[Handler]から[crate::msgproc::MsgProc]への結果返却をする役割も担っている。
/// [Drop]トレイトが実装されており、[Handler]の[Handler::handle]関数のスコープから外れた際に[crate::msgproc::MsgProc]へ結果を返却するようになっている。
#[derive(Message)]
#[rtype(result = "()")]
pub struct Msg {
    proc: Recipient<process::DoneRequest>,
    msg: OwnedMessage,
    processor_id: ProcessorId,
    error_msg: RefCell<Option<String>>,
    panic_msg: RefCell<Option<String>>,
}

impl Msg {
    pub(crate) fn new(
        proc: Recipient<process::DoneRequest>,
        msg: OwnedMessage,
        processor_id: ProcessorId,
    ) -> Self {
        Self {
            proc,
            msg,
            processor_id,
            error_msg: RefCell::new(None),
            panic_msg: RefCell::new(None),
        }
    }

    /// [Msg]にエラーメッセージを設定する。
    ///
    /// ユーザ定義の[Handler]の[Handler::handle]関数で最終的に[Msg]が[Drop::drop]される際に、エラーを表す結果が[crate::msgproc::MsgProc]に送信され、[crate::msgproc::MsgProc]全体が停止される。
    ///
    /// * `error_msg` - 返却するエラーに設定するメッセージ
    pub fn mark_as_error(&self, error_msg: &str) {
        let mut e = self.error_msg.borrow_mut();
        *e = Some(error_msg.to_string());
    }

    pub(crate) fn mark_as_panic(&self, panic_msg: &str) {
        let mut p = self.panic_msg.borrow_mut();
        *p = Some(panic_msg.to_string());
    }

    /// kafkaから[crate::consumer::IConsumer::consume]されたメッセージを取得する。
    pub fn get_owned_message(&self) -> OwnedMessage {
        self.msg.clone()
    }
}

impl Drop for Msg {
    fn drop(&mut self) {
        if let Some(_panic_msg) = self.panic_msg.borrow().clone() {
            self.proc.do_send(process::DoneRequest::panic());
            return;
        }
        match self.error_msg.borrow().clone() {
            Some(_error_msg) => {
                // TODO log error message
                self.proc
                    .do_send(process::DoneRequest::error(self.msg.topic()));
            }
            None => {
                self.proc
                    .do_send(process::DoneRequest::success(process::ProcessDescriptor {
                        message: self.msg.clone(),
                        processor_id: self.processor_id,
                    }));
            }
        }
    }
}
