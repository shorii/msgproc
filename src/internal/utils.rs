use actix::prelude::*;
use std::thread::sleep;
use std::time::Duration;

pub trait RecipientExt<M>
where
    M: Message + Send + Clone,
    M::Result: Send,
{
    fn send_safety(&self, msg: M) -> Result<(), SendError<M>>;
}

impl<M> RecipientExt<M> for Recipient<M>
where
    M: Message + Send + Clone,
    M::Result: Send,
{
    fn send_safety(&self, msg: M) -> Result<(), SendError<M>> {
        loop {
            match self.try_send(msg.clone()) {
                Ok(_) => return Ok(()),
                Err(e) => match e {
                    SendError::Full(_) => {
                        sleep(Duration::from_secs(1));
                        continue;
                    }
                    SendError::Closed(_) => {
                        return Err(e);
                    }
                },
            }
        }
    }
}
