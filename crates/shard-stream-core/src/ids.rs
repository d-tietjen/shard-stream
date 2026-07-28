pub use shardlog::{
    BatchId, LeaderEpoch, LogicalOffset, LogicalPartitionId, PlacementSequence, RecordId,
    RingEpoch, ShardId, StreamOffset, TopicId, TopicPartition, VirtualLaneId,
};

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

identifier!(BrokerId, u32);
// An origin may contain multiple local brokers, so its stable identity is
// intentionally distinct from BrokerId.
identifier!(OriginId, u32);
identifier!(PhysicalShardId, u32);
identifier!(AssignmentEpoch, u64);
