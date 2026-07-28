use std::error::Error;
use std::fmt;

use shard_stream_core::TopicPartition;

use crate::LogicalTopicBinding;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicAccess {
    Append,
    Fetch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicRouteErrorCode {
    InvalidInput,
    NotFound,
    WritePaused,
    SdkUpgradeRequired,
    StaleGeneration,
    Unavailable,
    Corrupt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicRouteError {
    pub code: TopicRouteErrorCode,
    pub message: String,
    pub current: Option<Box<LogicalTopicBinding>>,
}

impl TopicRouteError {
    #[must_use]
    pub fn new(code: TopicRouteErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            current: None,
        }
    }

    #[must_use]
    pub fn with_current(mut self, current: LogicalTopicBinding) -> Self {
        self.current = Some(Box::new(current));
        self
    }
}

impl fmt::Display for TopicRouteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for TopicRouteError {}

/// Optional stable-topic resolver installed by an extended distribution.
///
/// Returning `Ok(None)` means the logical topic is also the physical topic.
/// The public RF1 server uses that identity behavior; private generation-aware
/// runtimes can resolve hidden immutable generations through this boundary.
pub trait LogicalTopicResolver: Send + Sync + fmt::Debug {
    fn resolve_legacy(
        &self,
        topic_partition: TopicPartition,
        access: TopicAccess,
    ) -> Result<Option<TopicPartition>, TopicRouteError>;
}
