use crate::kafka::key::Topic;
use actix::prelude::*;
use rdkafka::message::OwnedMessage;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

pub mod consume {
    use super::*;

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct AddRequest(pub Topic);

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct CommitRequest(pub OwnedMessage);

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct RemoveRequest(pub Topic);
}

pub mod process {
    use crate::msgproc::IMsgProcessor;

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
    pub struct AddRequest(pub Arc<Mutex<Box<dyn IMsgProcessor>>>);

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct DoneRequest(pub Result<ProcessDescriptor, String>);

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct SetupRequest {
        pub commit_recipient: Recipient<consume::CommitRequest>,
        pub stop_recipient: Recipient<consume::RemoveRequest>,
    }
}
