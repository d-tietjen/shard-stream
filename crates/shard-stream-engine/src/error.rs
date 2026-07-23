use std::error::Error;
use std::fmt;

use shard_stream_core::{LogicalPartitionId, SequencerError, ShardId, TopicId};
use shard_stream_storage::StorageError;

pub type EngineResult<T> = Result<T, EngineError>;

#[derive(Debug)]
pub enum EngineError {
    InvalidConfig(String),
    TopicAlreadyExists(TopicId),
    UnknownPartition {
        topic_id: TopicId,
        partition_id: LogicalPartitionId,
    },
    UnknownShard(ShardId),
    AdmissionLimited {
        shard_id: ShardId,
        reason: &'static str,
    },
    WorkerStopped(ShardId),
    UnsupportedDurability(&'static str),
    DurabilityUnavailable {
        required_replicas: u32,
        durable_replicas: u32,
    },
    StaleProducerEpoch {
        producer_id: u128,
        current: u32,
        received: u32,
    },
    ProducerSequenceOutOfOrder {
        producer_id: u128,
        expected: u64,
        received: u64,
    },
    ProducerRequiresNewEpoch(u128),
    DuplicateSequenceMismatch(u128),
    CorruptState(String),
    Sequencer(SequencerError),
    Storage(StorageError),
}

impl fmt::Display for EngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "invalid engine config: {message}"),
            Self::TopicAlreadyExists(topic_id) => {
                write!(formatter, "topic {topic_id} already exists")
            }
            Self::UnknownPartition {
                topic_id,
                partition_id,
            } => write!(
                formatter,
                "unknown topic/partition {topic_id}/{partition_id}"
            ),
            Self::UnknownShard(shard_id) => write!(formatter, "unknown shard {shard_id}"),
            Self::AdmissionLimited { shard_id, reason } => {
                write!(formatter, "shard {shard_id} admission limited: {reason}")
            }
            Self::WorkerStopped(shard_id) => write!(formatter, "shard {shard_id} worker stopped"),
            Self::UnsupportedDurability(reason) => {
                write!(formatter, "unsupported durability: {reason}")
            }
            Self::DurabilityUnavailable {
                required_replicas,
                durable_replicas,
            } => write!(
                formatter,
                "durability unavailable: required {required_replicas} replicas, reached {durable_replicas}"
            ),
            Self::StaleProducerEpoch {
                producer_id,
                current,
                received,
            } => write!(
                formatter,
                "producer {producer_id} epoch {received} is stale; current epoch is {current}"
            ),
            Self::ProducerSequenceOutOfOrder {
                producer_id,
                expected,
                received,
            } => write!(
                formatter,
                "producer {producer_id} sequence out of order: expected {expected}, received {received}"
            ),
            Self::ProducerRequiresNewEpoch(producer_id) => write!(
                formatter,
                "producer {producer_id} must acquire a new epoch after a failed append"
            ),
            Self::DuplicateSequenceMismatch(producer_id) => write!(
                formatter,
                "producer {producer_id} retried a sequence with different content"
            ),
            Self::CorruptState(message) => write!(formatter, "corrupt engine state: {message}"),
            Self::Sequencer(error) => error.fmt(formatter),
            Self::Storage(error) => error.fmt(formatter),
        }
    }
}

impl Error for EngineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sequencer(error) => Some(error),
            Self::Storage(error) => Some(error),
            _ => None,
        }
    }
}

impl From<SequencerError> for EngineError {
    fn from(error: SequencerError) -> Self {
        Self::Sequencer(error)
    }
}

impl From<StorageError> for EngineError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}
