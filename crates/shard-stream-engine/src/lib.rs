//! Durable shard-striped append and ordered fetch engine.

mod config;
mod coordinator;
mod durable_apply;
mod durable_sink;
mod engine;
mod engine_handle;
mod error;
mod query_lineage;
#[cfg(unix)]
mod recovery_archived;
#[cfg(unix)]
mod recovery_census;
#[cfg(unix)]
mod recovery_json;
mod worker;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub use recovery_census::{NativePrimaryOutcomes, NativePrimaryRecovery};

// The actual neutral storage policies used by bounded native recovery.
// Reexport the same types so composing hosts do not select a second version.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use shardlog::{
    PinnedShardDirectoryLimits, PinnedShardGcChecks, PinnedShardGcLimits, PinnedStoreOpenChecks,
    PinnedStoreOpenLimits, ShardDirectoryCheckFailure, ShardDirectoryChecks,
    ShardGuardAcquireLimits,
};

#[cfg(unix)]
pub use recovery_census::{
    NativeRecoveryCatalog, NativeRecoveryCensus, NativeRecoveryError, NativeRecoveryLimits,
    NativeRecoveryPartition, NativeRecoveryUsage, OwnedRecoveryStorage, RecoveryExtentObservation,
    RecoveryExtentTailObservation, RecoverySourceFact, RecoverySourceKind,
    RecoveryTierManifestObservation,
};

pub use config::{EngineConfig, PartitionReplicationPolicy, TopicConfig};
pub use durable_apply::{DurableApplyConfig, DurableApplyQueue};
pub use durable_sink::{
    DurableAppend, DurableAppendDelivery, DurableAppendSink, DurableAppendSinkFactory,
    DurableAtomicDecision, DurableAtomicGroup, DurableSinkApply, DurableSinkAtomicVisibility,
    DurableSinkCheckpoint, DurableSinkConfig, DurableSinkLagPolicy, DurableSinkOptions,
    DurableSinkRecoveryMode, DurableSinkStats, NativeResumeSinkOutcome,
};
#[cfg(unix)]
pub use engine::{OwnedEngineOpenError, OwnedRecoveredEngine};

pub use engine::{
    AtomicGroupInspection, AtomicGroupState, FetchedBatch, NativeFetchCost, NativeFetchFailure,
    NativeFetchLimits, NativeFetchResult, NativeResumeCallbackError, NativeResumeCallbackResult,
    NativeResumeCoordinatorOutcome, NativeResumeCost, NativeResumeFailure, NativeResumeLimits,
    NativeResumeObservation, NativeResumeOutcome, NativeResumeRefusal, NativeResumeReport,
    NativeResumeSinkCacheOperation, NativeResumeUsage, NativeResumeWork, PartitionEpochs,
    PartitionWatermarks, ProducerProgress, ReplicationProgress, ReplicationStats,
    ReplicationTicket, ReplicationTransport, RetainedNativeFetchError, RetainedNativeResumeError,
    RetentionGuard, StreamEngine, native_fetch_cost, native_resume_cost,
};
pub use engine_handle::{EngineHandle, WeakEngineHandle};
pub use error::{EngineError, EngineResult};
pub use query_lineage::EngineQueryLineage;
pub use shard_stream_protocol::ReplicaAppend;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use worker::{NativeGcOutcome, NativeResumeGcPolicy};
// The original native enum, also borrowed by the pre-open census.
pub use shardlog::{
    JournalEvent, RecoveryExtentHeader, RecoveryExtentTail, RecoveryExtentTailUnknown,
};

// Ordinary native byte views/costs for hosts retaining the actual source owner.
pub use shardlog::{
    BorrowedRecoveryExtent, RECOVERY_EXTENT_PREFIX_BYTES, RecoveryExtentInspectionCost,
    RecoveryExtentInspectionError, RecoveryExtentInspectionLimits, inspect_recovery_extent_payload,
    recovery_extent_frame_bytes, recovery_extent_inspection_cost,
};

// Shared native path construction for ordinary archived tier source joins.
pub use shardlog::{
    RecoveryTierPathCost, RecoveryTierPathError, RecoveryTierPathLimits,
    recovery_tier_manifest_path_matches, recovery_tier_object_path_matches,
    recovery_tier_path_inspection_cost,
};

#[cfg(unix)]
pub use recovery_archived::{
    ArchivedNativeExtent, ArchivedNativeJournalEvent, ArchivedNativeTail, NativeArchivedSourceCost,
    NativeArchivedSourceError, NativeArchivedSourceLimits, UnverifiedNativeArchivedSource,
    inspect_archived_native_source, native_archived_source_inspection_cost,
};

/// Actual cumulative native fetch debit facts, without resource/serving admission.
pub use shardlog::StorageFetchUsage as NativeFetchUsage;
