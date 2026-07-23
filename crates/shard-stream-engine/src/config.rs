use std::path::PathBuf;

use shard_stream_core::{ShardId, TopicId};

use crate::{EngineError, EngineResult};

pub(crate) const COMMAND_OVERHEAD_BYTES: usize = 256;

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub object_store_dir: Option<PathBuf>,
    pub shard_count: u32,
    pub replication_factor: u32,
    pub min_in_sync_replicas: u32,
    pub queue_slots_per_shard: usize,
    pub queue_bytes_per_shard: usize,
    pub target_pack_bytes: u64,
    pub max_batch_bytes: usize,
    pub max_fetch_bytes: usize,
}

impl EngineConfig {
    pub fn validate(&self) -> EngineResult<()> {
        if self.shard_count == 0 {
            return Err(EngineError::InvalidConfig(
                "shard_count must be nonzero".into(),
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
        if self.target_pack_bytes == 0 || self.max_batch_bytes == 0 || self.max_fetch_bytes == 0 {
            return Err(EngineError::InvalidConfig(
                "pack and request byte limits must be nonzero".into(),
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
}

#[derive(Debug, Clone)]
pub struct TopicConfig {
    pub topic_id: TopicId,
    pub partitions: u32,
    pub shards: Option<Vec<ShardId>>,
}
