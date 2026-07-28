use std::path::PathBuf;
use std::time::Duration;

use shard_stream_core::{ShardId, TopicId};

use crate::{EngineError, EngineResult};

pub(crate) const COMMAND_OVERHEAD_BYTES: usize = 256;

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub object_store_dir: Option<PathBuf>,
    /// Number of physical worker threads that own storage and I/O.
    pub shard_count: u32,
    /// Stable logical append lanes mapped onto physical workers.
    pub virtual_lane_count: u32,
    /// Maximum durability copies available to the engine. Values above one
    /// require an injected [`crate::ReplicationTransport`].
    pub replication_factor: u32,
    /// Minimum durable copies required by quorum aliases. The public server
    /// fixes this to one.
    pub min_in_sync_replicas: u32,
    pub queue_slots_per_shard: usize,
    pub queue_bytes_per_shard: usize,
    pub target_pack_bytes: u64,
    pub max_pack_age: Duration,
    pub max_batch_bytes: usize,
    pub max_fetch_bytes: usize,
    pub append_linger: Duration,
}

impl EngineConfig {
    pub fn validate(&self) -> EngineResult<()> {
        if self.shard_count == 0 {
            return Err(EngineError::InvalidConfig(
                "shard_count must be nonzero".into(),
            ));
        }
        if self.virtual_lane_count == 0 {
            return Err(EngineError::InvalidConfig(
                "virtual_lane_count must be nonzero".into(),
            ));
        }
        if self.replication_factor == 0 {
            return Err(EngineError::InvalidConfig(
                "replication_factor must be nonzero".into(),
            ));
        }
        if self.replication_factor > 64 {
            return Err(EngineError::InvalidConfig(
                "replication_factor cannot exceed 64".into(),
            ));
        }
        if self.min_in_sync_replicas == 0 || self.min_in_sync_replicas > self.replication_factor {
            return Err(EngineError::InvalidConfig(
                "min_in_sync_replicas must be between one and replication_factor".into(),
            ));
        }
        if self.queue_slots_per_shard == 0 || self.queue_bytes_per_shard == 0 {
            return Err(EngineError::InvalidConfig(
                "shard queue slot and byte limits must be nonzero".into(),
            ));
        }
        if self.target_pack_bytes == 0
            || self.max_pack_age.is_zero()
            || self.max_batch_bytes == 0
            || self.max_fetch_bytes == 0
        {
            return Err(EngineError::InvalidConfig(
                "pack byte/age and request byte limits must be nonzero".into(),
            ));
        }
        if self.append_linger > Duration::from_secs(60) {
            return Err(EngineError::InvalidConfig(
                "append_linger cannot exceed 60 seconds".into(),
            ));
        }
        if self
            .max_batch_bytes
            .checked_add(COMMAND_OVERHEAD_BYTES)
            .is_none_or(|bytes| bytes > self.queue_bytes_per_shard)
        {
            return Err(EngineError::InvalidConfig(
                "max_batch_bytes plus command overhead cannot exceed queue_bytes_per_shard".into(),
            ));
        }
        if self
            .max_fetch_bytes
            .checked_add(COMMAND_OVERHEAD_BYTES)
            .is_none_or(|bytes| bytes > self.queue_bytes_per_shard)
        {
            return Err(EngineError::InvalidConfig(
                "max_fetch_bytes plus command overhead cannot exceed queue_bytes_per_shard".into(),
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn shard_ids(&self) -> Vec<ShardId> {
        (0..self.shard_count).map(ShardId::new).collect()
    }

    #[must_use]
    pub fn virtual_lane_ids(&self) -> Vec<ShardId> {
        (0..self.virtual_lane_count).map(ShardId::new).collect()
    }

    #[must_use]
    pub const fn physical_shard_for_lane(&self, lane: ShardId) -> ShardId {
        ShardId::new(lane.get() % self.shard_count)
    }
}

#[derive(Debug, Clone)]
pub struct TopicConfig {
    pub topic_id: TopicId,
    pub partitions: u32,
    pub shards: Option<Vec<ShardId>>,
}

/// Replication durability persisted with every physical topic generation.
///
/// `EngineConfig` describes the maximum capacity available to this engine.
/// This policy is the authoritative per-partition value used for append
/// acknowledgements and replicated watermarks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionReplicationPolicy {
    pub policy_generation: u64,
    pub replication_factor: u32,
    pub min_in_sync_replicas: u32,
}

impl PartitionReplicationPolicy {
    pub(crate) fn validate(self, maximum_replication_factor: u32) -> EngineResult<()> {
        if self.policy_generation == 0 {
            return Err(EngineError::InvalidConfig(
                "policy_generation must be nonzero".into(),
            ));
        }
        if self.replication_factor == 0 || self.replication_factor > 64 {
            return Err(EngineError::InvalidConfig(
                "topic replication_factor must be between one and 64".into(),
            ));
        }
        if self.replication_factor > maximum_replication_factor {
            return Err(EngineError::InvalidConfig(format!(
                "topic replication_factor {} exceeds engine capacity {maximum_replication_factor}",
                self.replication_factor
            )));
        }
        if self.min_in_sync_replicas == 0 || self.min_in_sync_replicas > self.replication_factor {
            return Err(EngineError::InvalidConfig(
                "topic min_in_sync_replicas must be between one and replication_factor".into(),
            ));
        }
        Ok(())
    }
}

impl EngineConfig {
    #[must_use]
    pub const fn default_replication_policy(&self) -> PartitionReplicationPolicy {
        PartitionReplicationPolicy {
            policy_generation: 1,
            replication_factor: self.replication_factor,
            min_in_sync_replicas: self.min_in_sync_replicas,
        }
    }
}
