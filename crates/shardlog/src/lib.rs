//! Embedded crash-recoverable append log with durable extent packs.

#![warn(missing_docs)]

mod api;
#[allow(missing_docs)]
mod codec;
mod durability;
#[allow(missing_docs)]
mod error;
#[allow(missing_docs)]
mod extent;
mod fetch_limits;
#[allow(missing_docs)]
mod journal;
#[allow(missing_docs)]
mod model;
mod recovery_codec;
mod recovery_payload;
#[allow(missing_docs)]
mod tier;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub use api::{
    OpenedPinnedShardLog, PinnedShardLogOpenChecks, PinnedShardLogOpenError,
    PinnedShardLogOpenFailure, PinnedShardLogOpenObservation, pinned_shard_log_open_cost,
};
pub use api::{
    ShardLog, ShardLogAppendReceipt, ShardLogCompactionReceipt, ShardLogConfig, ShardLogRecord,
    ShardLogResult,
};
pub use durability::{
    RetainedStorageDurabilitySetupError, StorageDurabilityBudget, StorageDurabilityCost,
    StorageDurabilityFailure, StorageDurabilityLimits, StorageDurabilityOperation,
    StorageDurabilityOutcome, storage_durability_cost,
};
/// Error returned by ShardLog storage and recovery operations.
pub use error::StorageError as ShardLogError;

#[doc(hidden)]
pub use error::{StorageError, StorageResult};
#[cfg(unix)]
pub use extent::ShardRecoveryGuard;
#[cfg(unix)]
pub use extent::{
    AcquiredShardRecoveryGuard, RetainedShardGuardAcquireError, ShardGuardAcquireCost,
    ShardGuardAcquireFailure, ShardGuardAcquireLimits, ShardGuardAcquireObservation,
    shard_guard_acquire_cost,
};
#[doc(hidden)]
pub use extent::{
    ExtentCatalogEntry, GarbageCollectionStats, PreparedExtent, SealedPack, ShardStore,
    ShardStoreConfig, StoredBatch, StoredBatchMetadata,
};
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use extent::{
    OpenedPinnedShardStore, PinnedStoreDirectoryEntry, PinnedStoreOpenChecks, PinnedStoreOpenCost,
    PinnedStoreOpenError, PinnedStoreOpenFailure, PinnedStoreOpenLimits, PinnedStoreOpenOutcome,
    PinnedStoreOpenPhase, PinnedStoreOpenUsage, pinned_store_open_cost,
};
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use extent::{
    PinnedShardAppendBudget, PinnedShardAppendChecks, PinnedShardAppendCost,
    PinnedShardAppendError, PinnedShardAppendFailure, PinnedShardAppendInput,
    PinnedShardAppendLimits, PinnedShardAppendPhase, PinnedShardAppendReport,
    PinnedShardAppendUsage, pinned_shard_append_cost,
};
#[cfg(unix)]
pub use extent::{
    PinnedShardDirectory, PinnedShardDirectoryCost, PinnedShardDirectoryError,
    PinnedShardDirectoryFailure, PinnedShardDirectoryLimits, PinnedShardDirectoryObservation,
    ShardDirectoryCheckFailure, ShardDirectoryChecks, pinned_shard_directory_cost,
};
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use extent::{
    PinnedShardGcChecks, PinnedShardGcCost, PinnedShardGcError, PinnedShardGcFailure,
    PinnedShardGcLimits, PinnedShardGcPack, PinnedShardGcPhase, PinnedShardGcReport,
    PinnedShardGcUsage, pinned_shard_gc_cost,
};
#[cfg(unix)]
pub use extent::{
    ShardGuardValidation, ShardGuardValidationCost, ShardGuardValidationError,
    ShardGuardValidationFailure, ShardGuardValidationLimits, shard_guard_validation_cost,
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

pub use tier::{
    RecoveryTierPathCost, RecoveryTierPathError, RecoveryTierPathLimits,
    recovery_tier_manifest_path_matches, recovery_tier_object_path_matches,
    recovery_tier_path_inspection_cost,
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

pub use fetch_limits::{
    StorageFetchBudget, StorageFetchCost, StorageFetchFailure, StorageFetchLimits,
    StorageFetchPlanError, StorageFetchReadError, StorageFetchUsage, storage_fetch_cost,
};
