//! Durable shard-striped append and ordered fetch engine.

mod config;
mod coordinator;
mod durable_apply;
mod durable_sink;
mod engine;
mod error;
mod worker;

pub use config::{EngineConfig, PartitionReplicationPolicy, TopicConfig};
pub use durable_apply::{DurableApplyConfig, DurableApplyQueue};
pub use durable_sink::{
    DurableAppend, DurableAppendDelivery, DurableAppendSink, DurableAppendSinkFactory,
    DurableAtomicDecision, DurableAtomicGroup, DurableSinkApply, DurableSinkAtomicVisibility,
    DurableSinkCheckpoint, DurableSinkConfig, DurableSinkLagPolicy, DurableSinkOptions,
    DurableSinkRecoveryMode, DurableSinkStats,
};
pub use engine::{
    AtomicGroupInspection, AtomicGroupState, FetchedBatch, PartitionEpochs, PartitionWatermarks,
    ProducerProgress, ReplicationProgress, ReplicationStats, ReplicationTicket,
    ReplicationTransport, RetentionGuard, StreamEngine,
};
pub use error::{EngineError, EngineResult};
pub use shard_stream_protocol::ReplicaAppend;
