//! Embedded crash-recoverable append log with durable extent packs.

#![warn(missing_docs)]

mod api;
#[allow(missing_docs)]
mod codec;
#[allow(missing_docs)]
mod error;
#[allow(missing_docs)]
mod extent;
#[allow(missing_docs)]
mod journal;
#[allow(missing_docs)]
mod model;
#[allow(missing_docs)]
mod tier;

pub use api::{
    ShardLog, ShardLogAppendReceipt, ShardLogCompactionReceipt, ShardLogConfig, ShardLogRecord,
    ShardLogResult,
};
/// Error returned by ShardLog storage and recovery operations.
pub use error::StorageError as ShardLogError;

#[doc(hidden)]
pub use error::{StorageError, StorageResult};
#[doc(hidden)]
pub use extent::{
    ExtentCatalogEntry, GarbageCollectionStats, PreparedExtent, SealedPack, ShardStore,
    ShardStoreConfig, StoredBatch, StoredBatchMetadata,
};
#[doc(hidden)]
pub use journal::{
    AtomicGroupSequence, CoordinatorJournal, DIGEST_XXH3_128, JournalEvent, ProducerSequence,
    RecoveredJournal,
};
#[doc(hidden)]
pub use model::{
    BatchId, LeaderEpoch, LogicalOffset, LogicalPartitionId, Placement, PlacementSequence,
    RecordId, Reservation, RingEpoch, ShardId, StreamOffset, TopicId, TopicPartition,
    VirtualLaneId,
};
#[doc(hidden)]
pub use tier::{LocalObjectTier, TierExtent, TierManifest, TierObject};
