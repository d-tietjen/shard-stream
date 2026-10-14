use std::error::Error;
use std::fmt;

use crate::TopicPartition;

/// Product-neutral write-admission hook.
///
/// The open-source runtime does not install a fence. Private or embedded
/// deployments can provide one to reject stale writer epochs before the
/// engine reserves or mutates durable state.
/// An accepted epoch identifies each new reservation and its persisted
/// placement; an exact retry retains its original receipt and placement.
///
/// This check returns no ownership guard. It does not synchronize a later
/// authority change with coordinator reservation, a queued physical append,
/// or WAL mutation. Hosts requiring that boundary need a separate admitted
/// physical-write contract; checking an epoch alone cannot establish it.
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
