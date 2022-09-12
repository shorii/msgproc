use rdkafka::message::{FromBytes, Message as IMessage, OwnedMessage};

/// ユーザ定義の[crate::processor::IProcessor]のImplementorで処理するデータ型
#[derive(Clone, Debug)]
pub struct Message {
    base: OwnedMessage,
}

impl Message {
    pub fn new(base: OwnedMessage) -> Self {
        Self { base }
    }

    pub fn topic(&self) -> &str {
        self.base.topic()
    }

    pub fn partition(&self) -> i32 {
        self.base.partition()
    }

    pub fn offset(&self) -> i64 {
        self.base.offset()
    }

    pub fn payload<P: ?Sized + FromBytes>(&self) -> Option<Result<&P, P::Error>> {
        self.base.payload_view::<P>()
    }
}

#[cfg(test)]
pub mod testing {
    use super::*;
    use bytes::Bytes;
    use rdkafka::Timestamp;

    pub fn create_message<P>(topic: &str, partition: i32, offset: i64, payload: P) -> Message
    where
        P: Into<Bytes>,
    {
        let payload: Bytes = payload.into();
        let message = OwnedMessage::new(
            Some(payload.to_vec()),
            None,
            topic.to_string(),
            Timestamp::CreateTime(0),
            partition,
            offset,
            None,
        );
        Message::new(message)
    }
}
