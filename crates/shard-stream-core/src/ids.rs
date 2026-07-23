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
    };
}

identifier!(TopicId, u128);
identifier!(LogicalPartitionId, u32);
identifier!(ShardId, u32);
identifier!(RingEpoch, u64);
identifier!(BatchId, u128);
identifier!(LogicalOffset, u128);
identifier!(PlacementSequence, u128);
