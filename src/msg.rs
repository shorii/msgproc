use crate::error::MsgHandleError;
use actix::prelude::*;
use rdkafka::message::OwnedMessage;

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct InnerMsg(pub OwnedMessage);

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct Msg {
    pub proc: Recipient<MsgHandleResult>,
    pub msg: OwnedMessage,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct MsgHandler(pub Recipient<Msg>);

#[derive(Message)]
#[rtype(result = "()")]
pub struct MsgHandleResult(pub Result<OwnedMessage, MsgHandleError>);
