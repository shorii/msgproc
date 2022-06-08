/// Procに対してKafkaErrorを送る。
///
/// Kafkaの操作に関して何らかのエラーが発生した場合に用いる。
///
/// * `$addr` - `actix::Addr<Proc>`
/// * `$e` - エラーメッセージ
#[macro_export]
macro_rules! kafka_error {
    ($addr: ident, $e: expr) => {
        $addr.do_send($crate::msg::MsgProcResult(Err(
            $crate::error::MsgProcError::KafkaError($e.to_string()),
        )));
    };
}

/// Procに対してHandleErrorを送る。
///
/// ユーザ定義のメッセージ処理途中で何らかのエラーが発生した場合に用いる。
///
/// # Example
///
/// ```
/// # use actix::prelude::*;
/// # use msgproc::msg::Msg;
/// # use msgproc::handle_error;
/// # use rdkafka::message::OwnedMessage;
///
/// # struct UserDefinedActor;
///
/// # fn something_went_wrong(_msg: OwnedMessage) -> bool {
/// #     True
/// # }
///
/// # impl Actor for UserDefinedActor {
/// #     type Context = Context<Self>;
/// # }
///
/// # impl Handler<Msg> for UserDefinedActor {
/// #     type Result = ();
///
/// #     fn handle(&mut self, msg: Msg, ctx: &mut Self::Context) -> Self::Result {
/// #         let Msg{ msg, proc } = msg;
/// #         if something_went_wrong(msg) {
/// #             handle_error!(proc, "Something went wrong!");
/// #         }
/// #     }
/// # }
/// ```
///
/// * `$addr` - `actix::Addr<Proc>`
/// * `$e` - エラーメッセージ
#[macro_export]
macro_rules! handle_error {
    ($addr: ident, $e: expr) => {
        $addr.do_send($crate::msg::MsgProcResult(Err(
            $crate::error::MsgProcError::HandleError($e.to_string()),
        )));
    };
}
