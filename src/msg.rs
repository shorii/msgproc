use crate::error::MsgProcError;
use actix::prelude::*;
use rdkafka::message::OwnedMessage;
use std::cell::RefCell;
use uuid::Uuid;

/// ユーザ定義の[Handler]で処理するデータ型
///
/// kafkaから[crate::consumer::IConsumer::consume]されたデータと[crate::proc::MsgProc]を制御するためのデータが定義されており、データの入れ物としてだけでなく
/// ユーザ定義の[Handler]から[crate::proc::MsgProc]への結果返却をする役割も担っている。
/// [Drop]トレイトが実装されており、[Handler]の[Handler::handle]関数のスコープから外れた際に[crate::proc::MsgProc]へ結果を返却するようになっている。
#[derive(Message)]
#[rtype(result = "()")]
pub struct Msg {
    proc: Recipient<MsgProcResult>,
    msg: OwnedMessage,
    handler_id: Uuid,
    error_msg: RefCell<Option<String>>,
}

impl Msg {
    pub(crate) fn new(proc: Recipient<MsgProcResult>, msg: OwnedMessage, handler_id: Uuid) -> Self {
        Self {
            proc,
            msg,
            handler_id,
            error_msg: RefCell::new(None),
        }
    }

    /// [Msg]にエラーメッセージを設定する。
    ///
    /// ユーザ定義の[Handler]の[Handler::handle]関数で最終的に[Msg]が[Drop::drop]される際に、エラーを表す結果が[crate::proc::MsgProc]に送信され、[crate::proc::MsgProc]全体が停止される。
    ///
    /// * `error_msg` - 返却するエラーに設定するメッセージ
    pub fn mark_as_error(&self, error_msg: String) {
        let mut e = self.error_msg.borrow_mut();
        *e = Some(error_msg);
    }

    /// kafkaから[crate::consumer::IConsumer::consume]されたメッセージを取得する。
    pub fn get_owned_message(&self) -> OwnedMessage {
        self.msg.clone()
    }
}

impl Drop for Msg {
    fn drop(&mut self) {
        match self.error_msg.borrow().clone() {
            Some(error_msg) => {
                self.proc
                    .do_send(MsgProcResult(Err(MsgProcError::HandleError(error_msg))));
            }
            None => {
                self.proc.do_send(MsgProcResult(Ok(MsgProcessorDescriptor {
                    message: self.msg.clone(),
                    processor_id: self.handler_id,
                })));
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct InnerMsg(pub OwnedMessage);

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct MsgProcessor(pub Recipient<Msg>);

pub(crate) struct MsgProcessorDescriptor {
    pub message: OwnedMessage,
    pub processor_id: Uuid,
}

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct MsgProcResult(pub Result<MsgProcessorDescriptor, MsgProcError>);
