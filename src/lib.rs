mod consumer;
mod context;
pub mod kafka;
pub mod msgproc;
mod options;
pub mod processor;

pub mod prelude {
    pub use super::kafka::*;
    pub use super::msgproc::*;
    pub use super::processor::*;
}
