mod consumer;
mod context;
mod kafka;
mod msgproc;
mod options;
mod processor;

pub mod prelude {
    pub use super::kafka::*;
    pub use super::msgproc::*;
    pub use super::processor::*;
}
