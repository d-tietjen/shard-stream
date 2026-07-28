//! Transport-independent request, response, and error contracts.

mod logical;
mod replication;
mod routing;
mod wire;

use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;

use bytes::Bytes;
use shard_stream_core::{BatchId, LogicalOffset, LogicalPartitionId, Placement, RecordId, TopicId};

pub use logical::{
    ActiveActiveCursorPosition, HaProfileRef, LogicalCursorError, LogicalCursorPosition,
    LogicalTopicBinding, LogicalTopicCursor, TopicCompatibility, TopicHaMode,
};
pub use replication::ReplicaAppend;
pub use routing::{LogicalTopicResolver, TopicAccess, TopicRouteError, TopicRouteErrorCode};
pub use wire::{
    EncodedFetchResponse, NATIVE_APPEND_RESPONSE_CONTENT_TYPE, NativeFetchBatch, WireError,
    decode_append_response, decode_fetch_batches, decode_fetch_batches_owned,
    encode_append_response, encode_fetch_batches, encode_fetch_batches_segmented,
};

pub const NATIVE_PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    Leader,
    Quorum,
    AllReplicas,
    Object,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchMode {
    Ordered,
    Replicated,
    AsAvailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProducerIdentity {
    pub producer_id: u128,
    pub epoch: u32,
    pub first_sequence: u64,
    /// Stable caller-supplied identity for this append.
    ///
    /// Kafka-compatible producers leave this unset. Recoverable native
    /// producer sessions use it to reject a retry that reuses a sequence for
    /// a different logical event.
    pub event_id: Option<RecordId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AtomicGroupIdentity {
    pub transaction_id: u128,
    pub chunk_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRequest {
    pub request_id: u128,
    pub topic_id: TopicId,
    pub partition_id: LogicalPartitionId,
    pub record_count: u32,
    pub payload: Bytes,
    pub durability: Durability,
    pub producer: Option<ProducerIdentity>,
    /// Partition-scoped native atomic group. Group chunks are durable but
    /// hidden from ordinary fetch until a commit marker is synced.
    pub atomic_group: Option<AtomicGroupIdentity>,
    /// Fencing token required when the engine has a write fence installed.
    pub leader_epoch: Option<u64>,
    /// Opaque context forwarded only to an injected durability extension.
    pub extension_context: Option<Bytes>,
}

impl AppendRequest {
    pub fn validate(&self, max_batch_bytes: usize) -> Result<NonZeroU32, ApiError> {
        let record_count = NonZeroU32::new(self.record_count).ok_or_else(|| {
            ApiError::new(
                ErrorCode::InvalidRequest,
                false,
                "record_count must be nonzero",
            )
        })?;
        if self.payload.len() > max_batch_bytes {
            return Err(ApiError::new(
                ErrorCode::BatchTooLarge,
                false,
                "append payload exceeds the configured batch limit",
            ));
        }
        Ok(record_count)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AppendResponse {
    pub request_id: u128,
    pub batch_id: BatchId,
    /// Stable native identifier for the first record in this batch.
    pub record_id: RecordId,
    pub first_offset: LogicalOffset,
    pub last_offset: LogicalOffset,
    /// Exclusive highest contiguous offset visible to ordered consumers.
    pub visible_through: LogicalOffset,
    pub placement: Placement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchRequest {
    pub request_id: u128,
    pub topic_id: TopicId,
    pub partition_id: LogicalPartitionId,
    pub start_offset: LogicalOffset,
    pub max_bytes: u32,
    pub mode: FetchMode,
}

impl FetchRequest {
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.max_bytes == 0 {
            return Err(ApiError::new(
                ErrorCode::InvalidRequest,
                false,
                "max_bytes must be nonzero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    InvalidRequest,
    BatchTooLarge,
    AdmissionLimited,
    NotCoordinator,
    StaleEpoch,
    DurabilityUnavailable,
    OffsetOutOfRange,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub code: ErrorCode,
    pub retryable: bool,
    pub message: &'static str,
    pub retry_after_ms: Option<u64>,
}

impl ApiError {
    #[must_use]
    pub const fn new(code: ErrorCode, retryable: bool, message: &'static str) -> Self {
        Self {
            code,
            retryable,
            message,
            retry_after_ms: None,
        }
    }

    #[must_use]
    pub const fn with_retry_after(mut self, retry_after_ms: u64) -> Self {
        self.retry_after_ms = Some(retry_after_ms);
        self
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.code, self.message)
    }
}

impl Error for ApiError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(record_count: u32, payload_bytes: usize) -> AppendRequest {
        AppendRequest {
            request_id: 1,
            topic_id: TopicId::new(2),
            partition_id: LogicalPartitionId::new(3),
            record_count,
            payload: Bytes::from(vec![0; payload_bytes]),
            durability: Durability::Quorum,
            producer: None,
            atomic_group: None,
            leader_epoch: None,
            extension_context: None,
        }
    }

    #[test]
    fn append_validation_rejects_zero_records_and_oversized_payloads() {
        assert_eq!(
            request(0, 1).validate(10).expect_err("zero records").code,
            ErrorCode::InvalidRequest
        );
        assert_eq!(
            request(1, 11).validate(10).expect_err("oversized").code,
            ErrorCode::BatchTooLarge
        );
        assert_eq!(request(2, 10).validate(10).expect("valid").get(), 2);
    }

    #[test]
    fn fetch_requires_a_nonzero_byte_budget() {
        let request = FetchRequest {
            request_id: 1,
            topic_id: TopicId::new(2),
            partition_id: LogicalPartitionId::new(3),
            start_offset: LogicalOffset::new(0),
            max_bytes: 0,
            mode: FetchMode::Ordered,
        };
        assert_eq!(
            request.validate().expect_err("zero bytes").code,
            ErrorCode::InvalidRequest
        );
    }
}
