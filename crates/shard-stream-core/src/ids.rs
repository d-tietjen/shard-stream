macro_rules! identifier {
    ($name:ident, $inner:ty) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
identifier!(ShardId, u32);
identifier!(RingEpoch, u64);
identifier!(BatchId, u128);
identifier!(LogicalOffset, u128);
identifier!(PlacementSequence, u128);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
