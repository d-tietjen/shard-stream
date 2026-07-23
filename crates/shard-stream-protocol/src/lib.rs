//! Transport-independent request, response, and error contracts.

mod wire;

use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;

use shard_stream_core::{BatchId, LogicalOffset, LogicalPartitionId, Placement, TopicId};

pub use wire::{NativeFetchBatch, WireError, decode_fetch_batches, encode_fetch_batches};

pub const NATIVE_PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    Leader,
    Quorum,
    Object,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchMode {
    Ordered,
    AsAvailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerIdentity {
    pub producer_id: u128,
    pub epoch: u32,
    pub first_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRequest {
    pub request_id: u128,
    pub topic_id: TopicId,
    pub partition_id: LogicalPartitionId,
    pub record_count: u32,
    pub payload: Vec<u8>,
    pub durability: Durability,
    pub producer: Option<ProducerIdentity>,
    /// Fencing token required when the engine has a write fence installed.
    pub leader_epoch: Option<u64>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResponse {
    pub request_id: u128,
    pub batch_id: BatchId,
    pub first_offset: LogicalOffset,
    pub last_offset: LogicalOffset,
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
            payload: vec![0; payload_bytes],
            durability: Durability::Quorum,
            producer: None,
            leader_epoch: None,
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
