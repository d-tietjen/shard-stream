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
mod recovery_codec;
mod recovery_payload;
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
#[cfg(unix)]
pub use extent::ShardRecoveryGuard;
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
#[cfg(unix)]
pub use tier::TierRecoveryGuard;
#[doc(hidden)]
pub use tier::{
    LocalObjectTier, TierExtent, TierManifest, TierObject, decode_recovery_tier_manifest,
    parse_recovery_tier_current,
};

pub use recovery_codec::{
    RECOVERY_EXTENT_HEADER_BYTES, RECOVERY_EXTENT_PREFIX_BYTES, RECOVERY_JOURNAL_PREFIX_BYTES,
    RecoveryExtentHeader, RecoveryExtentTail, RecoveryExtentTailUnknown, inspect_recovery_extent,
    inspect_recovery_extent_tail, inspect_recovery_journal, recovery_extent_frame_bytes,
    recovery_journal_fixed_inspection_cost, recovery_journal_frame_bytes,
};

pub use recovery_payload::{
    BorrowedRecoveryExtent, RecoveryExtentInspectionCost, RecoveryExtentInspectionError,
    RecoveryExtentInspectionLimits, inspect_recovery_extent_payload,
    recovery_extent_inspection_cost,
};
