use crate::error::MsgHandleError;
use actix::prelude::*;
use rdkafka::message::OwnedMessage;

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct Msg(pub OwnedMessage);

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct HandleMsg {
    pub proc: Recipient<MsgHandleResult>,
    pub msg: OwnedMessage,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct MsgHandler(pub Recipient<HandleMsg>);

#[derive(Message)]
#[rtype(result = "()")]
pub struct MsgHandleResult(pub Result<(), MsgHandleError>);
