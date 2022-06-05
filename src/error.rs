use thiserror::Error;

#[derive(Error, Debug)]
#[error("Failed to handle Msg")]
pub struct MsgHandleError;
