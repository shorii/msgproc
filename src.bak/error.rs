use thiserror::Error;

#[derive(Error, Debug)]
pub enum MsgProcError {
    #[error("HandleError: {0}")]
    HandleError(String),
    #[error("KafkaError: {0}")]
    KafkaError(String),
}
