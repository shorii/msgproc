use crate::kafka::alias::Topic;
use actix::prelude::*;
use rdkafka::message::OwnedMessage;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

pub type ProcessorId = Uuid;

pub mod consume {
    use super::*;

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct AddRequest(pub Topic);

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct CommitRequest(pub OwnedMessage);

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct RemoveRequest(pub Topic);
}

pub mod process {
    use crate::msgproc::IMsgProcessor;

    use super::*;

    #[derive(Clone)]
    pub struct ProcessDescriptor {
        pub message: OwnedMessage,
        pub processor_id: ProcessorId,
    }

    #[derive(Clone)]
    pub enum ProcessStatus {
        Panic,
        Error(Topic),
        Success(ProcessDescriptor),
    }

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct NotifyRequest(pub OwnedMessage);

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct AddRequest(pub Arc<Mutex<Box<dyn IMsgProcessor>>>);

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct DoneRequest(pub ProcessStatus);

    impl DoneRequest {
        pub fn success(descriptor: ProcessDescriptor) -> Self {
            DoneRequest(ProcessStatus::Success(descriptor))
        }

        pub fn error(topic: &str) -> Self {
            DoneRequest(ProcessStatus::Error(topic.to_string()))
        }

        pub fn panic() -> Self {
            DoneRequest(ProcessStatus::Panic)
        }
    }

    #[derive(Message, Clone)]
    #[rtype(result = "()")]
    pub struct SetupRequest {
        pub commit_recipient: Recipient<consume::CommitRequest>,
        pub stop_recipient: Recipient<consume::RemoveRequest>,
    }
}
