use crate::error::MsgProcError;
use actix::prelude::*;
use rdkafka::message::OwnedMessage;

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct InnerMsg(pub OwnedMessage);

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct Msg {
    pub proc: Recipient<MsgProcResult>,
    pub msg: OwnedMessage,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct MsgHandler(pub Recipient<Msg>);

#[derive(Message)]
#[rtype(result = "()")]
pub struct MsgProcResult(pub Result<OwnedMessage, MsgProcError>);
