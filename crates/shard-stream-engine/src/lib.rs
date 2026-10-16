//! Durable shard-striped append and ordered fetch engine.

mod config;
mod coordinator;
mod durable_apply;
mod durable_sink;
mod engine;
mod error;
#[cfg(unix)]
mod recovery_census;
mod worker;

#[cfg(unix)]
pub use recovery_census::{
    NativeRecoveryCensus, NativeRecoveryError, NativeRecoveryLimits, NativeRecoveryUsage,
    OwnedRecoveryStorage, RecoveryExtentObservation, RecoveryExtentTailObservation,
    RecoverySourceFact, RecoverySourceKind, RecoveryTierManifestObservation,
};

pub use config::{EngineConfig, PartitionReplicationPolicy, TopicConfig};
pub use durable_apply::{DurableApplyConfig, DurableApplyQueue};
pub use durable_sink::{
    DurableAppend, DurableAppendDelivery, DurableAppendSink, DurableAppendSinkFactory,
    DurableAtomicDecision, DurableAtomicGroup, DurableSinkApply, DurableSinkAtomicVisibility,
    DurableSinkCheckpoint, DurableSinkConfig, DurableSinkLagPolicy, DurableSinkOptions,
    DurableSinkRecoveryMode, DurableSinkStats,
};
#[cfg(unix)]
pub use engine::{OwnedEngineOpenError, OwnedRecoveredEngine};

pub use engine::{
    AtomicGroupInspection, AtomicGroupState, FetchedBatch, PartitionEpochs, PartitionWatermarks,
    ProducerProgress, ReplicationProgress, ReplicationStats, ReplicationTicket,
    ReplicationTransport, RetentionGuard, StreamEngine,
};
pub use error::{EngineError, EngineResult};
pub use shard_stream_protocol::ReplicaAppend;
// The original native enum, also borrowed by the pre-open census.
pub use shardlog::{
    JournalEvent, RecoveryExtentHeader, RecoveryExtentTail, RecoveryExtentTailUnknown,
};
