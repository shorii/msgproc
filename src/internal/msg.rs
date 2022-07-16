use crate::error::MsgProcError;
use crate::kafka::key::TopicManagementKey;
use crate::msg::Msg;
use actix::prelude::*;
use rdkafka::message::OwnedMessage;
use uuid::Uuid;

pub mod consume {
    use super::*;

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct AddRequest(pub TopicManagementKey);

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct CommitRequest(pub TopicManagementKey);

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct StopRequest;

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct SetupRequest(pub Recipient<process::NotifyRequest>);
}

pub mod process {
    use super::*;

    pub struct ProcessDescriptor {
        pub message: OwnedMessage,
        pub processor_id: Uuid,
    }

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct NotifyRequest(pub OwnedMessage);

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct AddRequest(pub Recipient<Msg>);

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct DoneRequest(pub Result<ProcessDescriptor, MsgProcError>);

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct SetupRequest {
        pub commit_recipient: Recipient<consume::CommitRequest>,
        pub stop_recipient: Recipient<consume::StopRequest>,
    }
}
