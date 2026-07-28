use std::error::Error;
use std::fmt;

use crate::TopicPartition;

/// Product-neutral write-admission hook.
///
/// The open-source runtime does not install a fence. Private or embedded
/// deployments can provide one to reject stale writer epochs before the
/// engine reserves or mutates durable state.
pub trait WriteFence: Send + Sync + fmt::Debug {
    fn check_write(
        &self,
        topic_partition: TopicPartition,
        writer_epoch: u64,
    ) -> Result<(), WriteFenceError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteFenceError {
    message: String,
}

impl WriteFenceError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for WriteFenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for WriteFenceError {}
