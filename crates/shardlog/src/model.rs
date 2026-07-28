use std::num::NonZeroU32;

macro_rules! identifier {
    ($name:ident, $inner:ty) => {
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            serde::Serialize,
            serde::Deserialize,
        )]
        pub struct $name($inner);

        impl $name {
            #[must_use]
            pub const fn new(value: $inner) -> Self {
                Self(value)
            }

            #[must_use]
            pub const fn get(self) -> $inner {
                self.0
            }
        }

        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self::new(value)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

identifier!(TopicId, u128);
identifier!(LogicalPartitionId, u32);
identifier!(VirtualLaneId, u32);
identifier!(RingEpoch, u64);
identifier!(LeaderEpoch, u64);
identifier!(BatchId, u128);
identifier!(LogicalOffset, u64);
identifier!(PlacementSequence, u64);
identifier!(RecordId, u128);

impl RecordId {
    /// Derives a stable, process-independent record identifier for a batch.
    #[must_use]
    pub fn for_batch(
        topic_id: TopicId,
        partition_id: LogicalPartitionId,
        batch_id: BatchId,
    ) -> Self {
        let mut canonical = [0u8; 36];
        canonical[..16].copy_from_slice(&topic_id.get().to_le_bytes());
        canonical[16..20].copy_from_slice(&partition_id.get().to_le_bytes());
        canonical[20..].copy_from_slice(&batch_id.get().to_le_bytes());
        Self::new(xxhash_rust::xxh3::xxh3_128(&canonical))
    }

    /// Derives the stable identity of a logical producer event.
    #[must_use]
    pub fn for_producer_event(
        logical_topic_id: TopicId,
        partition_id: LogicalPartitionId,
        producer_id: u128,
        producer_epoch: u32,
        first_sequence: u64,
    ) -> Self {
        let mut canonical = [0u8; 56];
        canonical[..16].copy_from_slice(&logical_topic_id.get().to_le_bytes());
        canonical[16..20].copy_from_slice(&partition_id.get().to_le_bytes());
        canonical[20..36].copy_from_slice(&producer_id.to_le_bytes());
        canonical[36..40].copy_from_slice(&producer_epoch.to_le_bytes());
        canonical[40..48].copy_from_slice(&first_sequence.to_le_bytes());
        canonical[48..].copy_from_slice(b"SSEVENT1");
        Self::new(xxhash_rust::xxh3::xxh3_128(&canonical))
    }
}

pub type ShardId = VirtualLaneId;
pub type StreamOffset = LogicalOffset;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct TopicPartition {
    pub topic_id: TopicId,
    pub partition_id: LogicalPartitionId,
}

impl TopicPartition {
    #[must_use]
    pub const fn new(topic_id: TopicId, partition_id: LogicalPartitionId) -> Self {
        Self {
            topic_id,
            partition_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Placement {
    pub virtual_lane_id: VirtualLaneId,
    pub ring_epoch: RingEpoch,
    pub leader_epoch: LeaderEpoch,
    pub sequence: PlacementSequence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reservation {
    pub topic_id: TopicId,
    pub partition_id: LogicalPartitionId,
    pub batch_id: BatchId,
    pub first_offset: LogicalOffset,
    pub last_offset: LogicalOffset,
    pub record_count: NonZeroU32,
    pub placement: Placement,
}
