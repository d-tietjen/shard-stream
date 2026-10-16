use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::num::NonZeroU32;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::mpsc::{Receiver, channel, sync_channel};
use std::sync::{Arc, Weak};

use bytes::Bytes;
use shard_stream_core::{
    BatchId, DirectoryOwnership, LeaderEpoch, LogicalOffset, LogicalPartitionId,
    PendingWriteAdmission, Placement, RecordId, ReplicaSourceIdentity, Reservation, RingEpoch,
    ShardId, TopicPartition, WriteFence,
};
use shard_stream_protocol::{
    AppendRequest, AppendResponse, AtomicGroupIdentity, Durability, FetchMode, FetchRequest,
    ProducerIdentity, ReplicaAppend,
};
use shardlog::{
    AtomicGroupSequence, CoordinatorJournal, DIGEST_XXH3_128, ProducerSequence, StoredBatch,
    StoredBatchMetadata,
};

use crate::coordinator::{
    AppendIntent, CommittedBatchSet, CompleteAppend, ControlJournal, CoordinatorPool,
    CoordinatorRouter, PrepareOutcome, PrepareReplicaOutcome, PreparedReplica, ReplicaIntent,
    ReserveOutcome, VisibilityMode, recover_partition_states, retention_hints,
};
use crate::durable_sink::DurableSinkDispatcher;
use crate::worker::{FetchCompletion, ProposedAppendOutcome, ShardHandle};
use crate::{
    DurableAppend, DurableAppendDelivery, DurableAtomicDecision, DurableAtomicGroup,
    DurableSinkCheckpoint, DurableSinkConfig, DurableSinkRecoveryMode, DurableSinkStats,
    EngineConfig, EngineError, EngineResult, PartitionReplicationPolicy, TopicConfig,
};

#[cfg(unix)]
#[path = "owned_engine.rs"]
mod owned;
#[cfg(unix)]
pub use owned::{OwnedEngineOpenError, OwnedRecoveredEngine};

const MAX_FETCH_PLAN_BATCHES_PER_SHARD: usize = 4_096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedBatch {
    pub batch_id: BatchId,
    /// Caller-supplied native event identity, when the append was produced by
    /// a recoverable producer session.
    pub event_id: Option<RecordId>,
    /// Native atomic-group identity for auditing and administrative replay.
    /// Ordinary fetch only returns this after the group is committed.
    pub atomic_group: Option<AtomicGroupIdentity>,
    pub first_offset: LogicalOffset,
    pub last_offset: LogicalOffset,
    pub record_count: NonZeroU32,
    pub payload: Bytes,
    pub placement: Placement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionWatermarks {
    pub log_start: LogicalOffset,
    pub allocated_end: LogicalOffset,
    pub contiguous_log_end: LogicalOffset,
    pub replicated_high_watermark: LogicalOffset,
    pub last_stable_offset: LogicalOffset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerProgress {
    pub epoch: u32,
    pub next_sequence: u64,
    pub requires_new_epoch: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionEpochs {
    pub ring_epoch: RingEpoch,
    pub leader_epoch: shard_stream_core::LeaderEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtomicGroupState {
    Unresolved,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AtomicGroupInspection {
    pub transaction_id: u128,
    pub state: AtomicGroupState,
    pub chunk_count: u64,
    pub record_count: u64,
    pub payload_bytes: u64,
    pub first_offset: LogicalOffset,
    pub last_offset: LogicalOffset,
}

pub struct StreamEngine {
    config: EngineConfig,
    shards: Vec<ShardHandle>,
    recovered_partitions: Vec<TopicPartition>,
    _replication_progress: Arc<CoordinatorRouter>,
    coordinators: CoordinatorPool,
    journal: ControlJournal,
    write_fence: Option<Arc<dyn WriteFence>>,
    requires_physical_admission: bool,
    replication: Option<Arc<dyn ReplicationTransport>>,
    retention_guard: Option<Arc<dyn RetentionGuard>>,
    durable_sink: Option<DurableSinkDispatcher>,
    // Keep directory custody through destruction of every engine-owned field.
    // Detached sink workers also retain this exact owner independently.
    directory_ownership: Option<Arc<DirectoryOwnership>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplicationStats {
    pub pending_jobs: u64,
    pub pending_bytes: u64,
    pub max_pending_bytes: u64,
    pub admission_waits: u64,
    pub admission_wait_nanos: u64,
    pub completed_async_jobs: u64,
    pub completed_sync_jobs: u64,
    pub retry_attempts: u64,
    pub failed_jobs: u64,
}

/// Completion for replication admitted before the leader append finishes.
///
/// Keeping this as a blocking ticket lets local durability and follower
/// durability progress concurrently without coupling the engine to an async
/// runtime or a concrete transport.
pub struct ReplicationTicket {
    receiver: Receiver<EngineResult<u32>>,
}

impl ReplicationTicket {
    #[must_use]
    pub fn pending(receiver: Receiver<EngineResult<u32>>) -> Self {
        Self { receiver }
    }

    #[must_use]
    pub fn completed(result: EngineResult<u32>) -> Self {
        let (sender, receiver) = sync_channel(1);
        let _ = sender.send(result);
        Self { receiver }
    }

    pub fn wait(self) -> EngineResult<u32> {
        self.receiver
            .recv()
            .map_err(|_| EngineError::CorruptState("remote replication response lost".into()))?
    }
}

#[derive(Clone)]
pub struct ReplicationProgress {
    router: Weak<CoordinatorRouter>,
}

impl fmt::Debug for ReplicationProgress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplicationProgress")
            .finish_non_exhaustive()
    }
}

impl ReplicationProgress {
    /// Records the total durable replica count, including the leader, after a
    /// background replication job finishes.
    pub fn record_durable_replicas(&self, reservation: Reservation, durable_replicas: u32) {
        if let Some(router) = self.router.upgrade() {
            router.replica_count_durable(reservation, durable_replicas);
        }
    }
}

pub(crate) enum ReplicaAppendPlan {
    Duplicate(AppendResponse),
    Prepared {
        prepared: Box<PreparedReplica>,
        admission: Arc<dyn PendingWriteAdmission>,
    },
    Reserved {
        response: AppendResponse,
        producer: Option<ProducerSequence>,
        atomic_group: Option<AtomicGroupSequence>,
        admission: Option<Arc<dyn PendingWriteAdmission>>,
    },
}

pub trait ReplicationTransport: Send + Sync + std::fmt::Debug {
    /// Installs the partition-routed completion sink used by asynchronous
    /// follower replication. Called exactly once while the engine opens.
    fn install_progress(&self, progress: ReplicationProgress) -> EngineResult<()>;

    /// Enqueues a batch for every assigned follower and waits for at least
    /// `required_followers` durable acknowledgements. A value of zero must
    /// enqueue bounded background replication and return without waiting for
    /// network or follower fsync.
    fn replicate(&self, batch: ReplicaAppend, required_followers: u32) -> EngineResult<u32>;

    /// Starts replication and returns a completion that may be waited after
    /// the leader's local append. Transports with an admission queue should
    /// override this so local and remote durability overlap.
    fn begin_replicate(
        &self,
        batch: ReplicaAppend,
        required_followers: u32,
    ) -> EngineResult<ReplicationTicket> {
        Ok(ReplicationTicket::completed(
            self.replicate(batch, required_followers),
        ))
    }

    fn stats(&self) -> ReplicationStats {
        ReplicationStats::default()
    }
}

pub trait RetentionGuard: Send + Sync + std::fmt::Debug {
    fn validate_advance(
        &self,
        topic_partition: TopicPartition,
        proposed_log_start: LogicalOffset,
    ) -> EngineResult<()>;
}

impl StreamEngine {
    /// Maximum payload budget accepted by one fetch request.
    #[must_use]
    pub const fn maximum_fetch_bytes(&self) -> usize {
        self.config.max_fetch_bytes
    }

    /// Maximum payload budget accepted by one append request.
    #[must_use]
    pub const fn maximum_batch_bytes(&self) -> usize {
        self.config.max_batch_bytes
    }

    /// Check that this exact fence is installed for guarded replica admission.
    ///
    /// This uses the physical-admission mode captured when the engine opened
    /// and the installed fence's allocation identity, without calling the fence.
    /// It proves installation mechanics only, not current TLS identity, issuer
    /// authorization, lease validity, or permission to write.
    #[must_use]
    pub fn has_guarded_replica_fence(&self, fence: &Arc<dyn WriteFence>) -> bool {
        self.requires_physical_admission
            && self
                .write_fence
                .as_ref()
                .is_some_and(|installed| Arc::ptr_eq(installed, fence))
    }

    /// Checks exact directory-owner installation without filesystem I/O.
    ///
    /// This proves retained resource custody only, not process identity,
    /// authenticated recovery state, lease validity or permission to write.
    #[must_use]
    pub fn has_directory_ownership(&self, owner: &Arc<DirectoryOwnership>) -> bool {
        self.directory_ownership
            .as_ref()
            .is_some_and(|installed| Arc::ptr_eq(installed, owner))
    }

    pub fn open(config: EngineConfig) -> EngineResult<Self> {
        Self::open_inner(None, config, None, None, None, None, false)
    }

    pub fn open_with_fence(
        config: EngineConfig,
        write_fence: Arc<dyn WriteFence>,
    ) -> EngineResult<Self> {
        Self::open_inner(None, config, Some(write_fence), None, None, None, false)
    }

    pub fn open_with_replication(
        config: EngineConfig,
        replication: Arc<dyn ReplicationTransport>,
    ) -> EngineResult<Self> {
        Self::open_inner(None, config, None, Some(replication), None, None, false)
    }

    /// Opens an engine with independently optional fencing and replication.
    ///
    /// Embedded deployments normally need both: the fence rejects writes from
    /// stale leadership epochs while the transport provides RF2/RF3
    /// durability. The narrower constructors remain convenient for tests and
    /// development deployments.
    pub fn open_with_components(
        config: EngineConfig,
        write_fence: Option<Arc<dyn WriteFence>>,
        replication: Option<Arc<dyn ReplicationTransport>>,
    ) -> EngineResult<Self> {
        Self::open_inner(None, config, write_fence, replication, None, None, false)
    }

    pub fn open_with_all_components(
        config: EngineConfig,
        write_fence: Option<Arc<dyn WriteFence>>,
        replication: Option<Arc<dyn ReplicationTransport>>,
        retention_guard: Option<Arc<dyn RetentionGuard>>,
    ) -> EngineResult<Self> {
        Self::open_inner(
            None,
            config,
            write_fence,
            replication,
            retention_guard,
            None,
            false,
        )
    }

    /// Opens an RF1 engine with a cluster-shared durable append sink.
    ///
    /// Recovered RF1 partitions are replayed automatically. RF>1 composition
    /// uses `open_with_all_components_and_durable_sink` and activates only the
    /// partitions currently led by this broker through `resume_durable_sink`.
    pub fn open_with_durable_sink(
        config: EngineConfig,
        durable_sink: DurableSinkConfig,
    ) -> EngineResult<Self> {
        Self::open_inner(None, config, None, None, None, Some(durable_sink), true)
    }

    pub fn open_with_all_components_and_durable_sink(
        config: EngineConfig,
        write_fence: Option<Arc<dyn WriteFence>>,
        replication: Option<Arc<dyn ReplicationTransport>>,
        retention_guard: Option<Arc<dyn RetentionGuard>>,
        durable_sink: Option<DurableSinkConfig>,
    ) -> EngineResult<Self> {
        Self::open_inner(
            None,
            config,
            write_fence,
            replication,
            retention_guard,
            durable_sink,
            false,
        )
    }

    /// Opens storage while retaining exclusive custody acquired before host recovery.
    ///
    /// The owner must name this exact directory and unchanged lock file. It is
    /// retained through escaped engine `Arc`s and through physically running
    /// sink callbacks even when bounded dispatcher destruction detaches them.
    /// Other constructors keep their existing unowned behavior. This does not
    /// authenticate incarnation, assignment, lease or checkpoint history.
    /// Recovered sinks use the same explicit `resume_durable_sink` contract as
    /// `open_with_all_components_and_durable_sink`.
    pub fn open_with_owned_components(
        config: EngineConfig,
        directory_ownership: Arc<DirectoryOwnership>,
        write_fence: Option<Arc<dyn WriteFence>>,
        replication: Option<Arc<dyn ReplicationTransport>>,
        retention_guard: Option<Arc<dyn RetentionGuard>>,
        durable_sink: Option<DurableSinkConfig>,
    ) -> EngineResult<Self> {
        Self::open_inner(
            Some(directory_ownership),
            config,
            write_fence,
            replication,
            retention_guard,
            durable_sink,
            false,
        )
    }

    fn open_inner(
        // Parameters drop in reverse order, after locals. Keep custody last
        // even if validation fails before components move into local state.
        directory_ownership: Option<Arc<DirectoryOwnership>>,
        mut config: EngineConfig,
        write_fence: Option<Arc<dyn WriteFence>>,
        replication: Option<Arc<dyn ReplicationTransport>>,
        retention_guard: Option<Arc<dyn RetentionGuard>>,
        durable_sink: Option<DurableSinkConfig>,
        auto_resume_durable_sink: bool,
    ) -> EngineResult<Self> {
        config.validate()?;
        if let Some(owner) = &directory_ownership {
            owner
                .validate_directory(&config.data_dir)
                .map_err(|error| {
                    EngineError::InvalidConfig(format!(
                        "directory ownership validation failed: {error}"
                    ))
                })?;
            // Resolve relative paths once, while their opened identities agree.
            config.data_dir =
                std::fs::canonicalize(&config.data_dir).map_err(shardlog::StorageError::from)?;
            owner
                .validate_directory(&config.data_dir)
                .map_err(|error| {
                    EngineError::InvalidConfig(format!(
                        "directory ownership validation failed: {error}"
                    ))
                })?;
        }
        let requires_physical_admission = write_fence
            .as_ref()
            .is_some_and(|fence| fence.requires_physical_admission());
        if requires_physical_admission {
            for bytes in [config.max_batch_bytes, config.max_fetch_bytes] {
                if crate::worker::append_command_charge(bytes, true)
                    .is_none_or(|charge| charge > config.queue_bytes_per_shard)
                {
                    return Err(EngineError::InvalidConfig(
                        "guarded request ceilings plus admission metadata exceed shard capacity"
                            .into(),
                    ));
                }
            }
        }
        if config.replication_factor > 1 && replication.is_none() {
            return Err(EngineError::InvalidConfig(
                "RF>1 requires an injected replication transport; the public engine only provides RF1 storage"
                    .into(),
            ));
        }
        std::fs::create_dir_all(&config.data_dir).map_err(shardlog::StorageError::from)?;
        let durable_sink = durable_sink
            .map(|sink| {
                DurableSinkDispatcher::open_with_ownership(
                    sink,
                    &config.shard_ids(),
                    config.queue_bytes_per_shard,
                    config.max_batch_bytes,
                    directory_ownership.as_ref().map(Arc::clone),
                )
            })
            .transpose()?;
        let auto_recover_sink =
            auto_resume_durable_sink && config.replication_factor == 1 && durable_sink.is_some();

        let (journal, recovered) =
            CoordinatorJournal::open(config.data_dir.join("coordinator.ssj"))?;
        let retained_log_starts = retention_hints(&recovered.events);
        let mut shards = Vec::with_capacity(config.shard_count as usize);
        let mut catalog = HashMap::new();
        for shard_id in config.shard_ids() {
            let (handle, entries) = match &directory_ownership {
                Some(owner) => {
                    ShardHandle::spawn_with_ownership(&config, shard_id, Some(Arc::clone(owner)))?
                }
                None => ShardHandle::spawn(&config, shard_id)?,
            };
            for entry in entries {
                let topic_partition =
                    TopicPartition::new(entry.reservation.topic_id, entry.reservation.partition_id);
                if retained_log_starts
                    .get(&topic_partition)
                    .is_some_and(|log_start| entry.reservation.last_offset < *log_start)
                {
                    continue;
                }
                let key = (topic_partition, entry.reservation.batch_id);
                if catalog.insert(key, entry).is_some() {
                    return Err(EngineError::CorruptState(format!(
                        "batch {} appears in more than one extent",
                        key.1
                    )));
                }
            }
            shards.push(handle);
        }

        let recovered_states = recover_partition_states(&config, &recovered.events, &catalog, 1)?;
        let mut recovered_partitions = recovered_states.keys().copied().collect::<Vec<_>>();
        recovered_partitions.sort_unstable();
        let journal = ControlJournal::spawn(journal)?;
        let coordinators =
            CoordinatorPool::spawn(config.shard_count as usize, recovered_states, &journal)?;
        let replica_progress = Arc::new(coordinators.router());
        if let Some(replication) = &replication {
            replication.install_progress(ReplicationProgress {
                router: Arc::downgrade(&replica_progress),
            })?;
        }
        let engine = Self {
            config,
            shards,
            recovered_partitions,
            _replication_progress: replica_progress,
            coordinators,
            journal,
            write_fence,
            requires_physical_admission,
            replication,
            retention_guard,
            durable_sink,
            directory_ownership,
        };
        if auto_recover_sink {
            engine.resume_durable_sink(&engine.recovered_partitions)?;
        }
        for (topic_partition, log_start) in retained_log_starts {
            if log_start.get() > 0 {
                engine.collect_garbage(topic_partition, log_start)?;
            }
        }
        Ok(engine)
    }

    pub fn create_topic(&self, topic: TopicConfig) -> EngineResult<()> {
        self.create_topic_with_policy(topic, self.config.default_replication_policy())
    }

    /// Creates a topic whose logical partitions each have one stable owner
    /// lane selected round-robin from the configured shard ring.
    ///
    /// This mode is intended for stateful partition consumers that keep
    /// mutable thread-local state alongside the shard-stream owner. Unlike a
    /// regular topic, successive appends to one partition never rotate across
    /// lanes.
    pub fn create_partition_affine_topic(&self, topic: TopicConfig) -> EngineResult<()> {
        if topic.partitions == 0 {
            return Err(EngineError::InvalidConfig(
                "topic partitions must be nonzero".into(),
            ));
        }
        let replication_policy = self.config.default_replication_policy();
        replication_policy.validate(self.config.replication_factor)?;
        let shards = topic
            .shards
            .clone()
            .unwrap_or_else(|| self.config.virtual_lane_ids());
        validate_ring(&self.config, &shards)?;
        self.coordinators
            .create_partition_affine_topic(topic, shards, replication_policy)
    }

    pub fn create_topic_with_policy(
        &self,
        topic: TopicConfig,
        replication_policy: PartitionReplicationPolicy,
    ) -> EngineResult<()> {
        if topic.partitions == 0 {
            return Err(EngineError::InvalidConfig(
                "topic partitions must be nonzero".into(),
            ));
        }
        replication_policy.validate(self.config.replication_factor)?;
        let shards = topic
            .shards
            .clone()
            .unwrap_or_else(|| self.config.virtual_lane_ids());
        validate_ring(&self.config, &shards)?;
        self.coordinators
            .create_topic(topic, shards, replication_policy)
    }

    /// Applies a same-data-generation durability policy update.
    ///
    /// The operation is replay-safe across partitions: a retry accepts
    /// partitions already at the exact target policy, while any conflicting
    /// generation fails before new mutations are attempted.
    pub fn update_topic_replication_policy(
        &self,
        topic_id: shard_stream_core::TopicId,
        expected_policy_generation: u64,
        replication_policy: PartitionReplicationPolicy,
    ) -> EngineResult<()> {
        replication_policy.validate(self.config.replication_factor)?;
        if replication_policy.policy_generation <= expected_policy_generation {
            return Err(EngineError::InvalidConfig(
                "replication policy generation must advance".into(),
            ));
        }
        let partitions = self.topic_partitions(topic_id);
        if partitions.is_empty() {
            return Err(EngineError::UnknownPartition {
                topic_id,
                partition_id: LogicalPartitionId::new(0),
            });
        }
        for partition_id in &partitions {
            let topic_partition = TopicPartition::new(topic_id, *partition_id);
            let current = self.coordinators.replication_policy(topic_partition)?;
            if current != replication_policy
                && current.policy_generation != expected_policy_generation
            {
                return Err(EngineError::Fenced(format!(
                    "partition {} policy generation is {}, expected {expected_policy_generation}",
                    partition_id.get(),
                    current.policy_generation
                )));
            }
        }
        for partition_id in partitions {
            self.coordinators.update_replication_policy(
                TopicPartition::new(topic_id, partition_id),
                expected_policy_generation,
                replication_policy,
            )?;
        }
        Ok(())
    }

    pub fn reconfigure_partition(
        &self,
        topic_partition: TopicPartition,
        ring_epoch: RingEpoch,
        shards: Vec<ShardId>,
    ) -> EngineResult<()> {
        validate_ring(&self.config, &shards)?;
        self.coordinators
            .reconfigure(topic_partition, ring_epoch, shards)
    }

    pub fn append(&self, request: AppendRequest) -> EngineResult<AppendResponse> {
        self.append_inner(request, None)
    }

    /// Appends durably while forwarding bounded process-local bytes to the
    /// configured durable sink.
    ///
    /// `sink_context` is never authoritative: replay receives only the stored
    /// append payload. Its bytes count against durable-sink backpressure.
    pub fn append_with_durable_sink_context(
        &self,
        request: AppendRequest,
        sink_context: Bytes,
    ) -> EngineResult<AppendResponse> {
        self.append_inner(request, Some(sink_context))
    }

    fn append_inner(
        &self,
        request: AppendRequest,
        sink_context: Option<Bytes>,
    ) -> EngineResult<AppendResponse> {
        if request.durability == Durability::Object && self.config.object_store_dir.is_none() {
            return Err(EngineError::UnsupportedDurability(
                "object acknowledgement requires object_store_dir",
            ));
        }
        let record_count = request
            .validate(self.config.max_batch_bytes)
            .map_err(|error| {
                EngineError::InvalidConfig(format!("invalid append request: {error}"))
            })?;
        let topic_partition = TopicPartition::new(request.topic_id, request.partition_id);
        let mut prepared_guarded = None;
        let mut guarded_input = None;
        let (admitted_leader_epoch, mut admission) = if let Some(write_fence) = &self.write_fence {
            let leader_epoch = request.leader_epoch.ok_or_else(|| {
                EngineError::Fenced("leader_epoch is required by the write fence".into())
            })?;
            if self.requires_physical_admission {
                // Read-only current authority can return an immutable receipt;
                // it never grants permission for a new physical operation.
                write_fence
                    .check_write(topic_partition, leader_epoch)
                    .map_err(|error| EngineError::Fenced(error.to_string()))?;
                let digest = if request.producer.is_some() || request.atomic_group.is_some() {
                    xxhash_rust::xxh3::xxh3_128(&request.payload).to_le_bytes()
                } else {
                    [0; 16]
                };
                let bytes = u64::try_from(request.payload.len())
                    .map_err(|_| EngineError::InvalidConfig("append payload exceeds u64".into()))?;
                match self._replication_progress.prepare(AppendIntent {
                    topic_partition,
                    request_id: request.request_id,
                    producer: request.producer,
                    atomic_group: request.atomic_group,
                    record_count,
                    payload_bytes: bytes,
                    payload_digest: digest,
                    durability: request.durability,
                    admitted_leader_epoch: Some(LeaderEpoch::new(leader_epoch)),
                })? {
                    PrepareOutcome::Duplicate(outcome) => {
                        return guarded_cached_response(&request, outcome);
                    }
                    PrepareOutcome::New(prepared) => prepared_guarded = Some(*prepared),
                }
                guarded_input = Some((digest, bytes));
            }
            let admission = write_fence
                .prepare_write(topic_partition, leader_epoch)
                .map_err(|error| EngineError::Fenced(error.to_string()))?;
            (Some(LeaderEpoch::new(leader_epoch)), admission)
        } else {
            (None, None)
        };
        self.validate_admission_contract(admission.as_ref())?;
        let (mut sink_charge, sink_payload) = if let Some(durable_sink) = &self.durable_sink {
            durable_sink.validate_append(&request.payload, record_count)?;
            (
                durable_sink.reserve(
                    request
                        .payload
                        .len()
                        .checked_add(sink_context.as_ref().map_or(0, Bytes::len))
                        .ok_or_else(|| {
                            EngineError::InvalidConfig(
                                "append plus sink context exceeds usize".into(),
                            )
                        })?,
                )?,
                Some(request.payload.clone()),
            )
        } else {
            (None, None)
        };
        let payload_digest = if let Some((digest, _)) = guarded_input {
            digest
        } else if request.producer.is_some() || request.atomic_group.is_some() {
            xxhash_rust::xxh3::xxh3_128(&request.payload).to_le_bytes()
        } else {
            [0; 16]
        };
        let payload_bytes = match guarded_input {
            Some((_, bytes)) => bytes,
            None => u64::try_from(request.payload.len())
                .map_err(|_| EngineError::InvalidConfig("append payload exceeds u64".into()))?,
        };
        if admission.is_some()
            && crate::worker::append_command_charge(request.payload.len(), true)
                .is_none_or(|charge| charge > self.config.queue_bytes_per_shard)
        {
            return Err(EngineError::InvalidConfig(
                "append plus physical admission metadata exceeds shard capacity".into(),
            ));
        }
        validate_pending_admission(admission.as_ref())?;
        let guarded = admission.is_some();
        let mut guarded_result = None;
        let reserve_outcome = if let Some(admission) = admission.take() {
            match prepared_guarded {
                Some(prepared) => {
                    let ticket = self
                        .shard_for_lane(prepared.proposal.placement().virtual_lane_id)?
                        .begin_proposed_append(
                            prepared,
                            request.payload.clone(),
                            request.durability == Durability::Object,
                            admission,
                            self._replication_progress.clone(),
                        )?;
                    match ticket.wait() {
                        ProposedAppendOutcome::Unallocated(error)
                        | ProposedAppendOutcome::Unknown(error) => return Err(error),
                        ProposedAppendOutcome::Duplicate(outcome) => outcome,
                        ProposedAppendOutcome::Committed {
                            reservation,
                            producer,
                            replication_policy,
                            result,
                        } => {
                            guarded_result = Some(result);
                            ReserveOutcome::Reserved {
                                reservation,
                                producer,
                                replication_policy,
                            }
                        }
                    }
                }
                None => {
                    return Err(EngineError::CorruptState(
                        "physical ticket lacks its pure preparation".into(),
                    ));
                }
            }
        } else {
            self.coordinators.reserve(
                topic_partition,
                request.request_id,
                request.producer,
                request.atomic_group,
                record_count,
                payload_bytes,
                payload_digest,
                request.durability,
                admitted_leader_epoch,
            )?
        };
        let replication_policy = match reserve_outcome {
            ReserveOutcome::Duplicate {
                replication_policy, ..
            }
            | ReserveOutcome::Reserved {
                replication_policy, ..
            } => replication_policy,
        };
        let required_replicas = match request.durability {
            Durability::Quorum => replication_policy.min_in_sync_replicas,
            Durability::AllReplicas => replication_policy.replication_factor,
            Durability::Leader | Durability::Object => 1,
        };
        let (reservation, producer) = match reserve_outcome {
            ReserveOutcome::Duplicate {
                response,
                durable_replicas,
                object_durable,
                ..
            } => {
                if request.durability == Durability::Object && !object_durable {
                    return Err(EngineError::ObjectDurabilityUnavailable);
                }
                let async_followers_pending = self.replication.is_some()
                    && matches!(request.durability, Durability::Leader | Durability::Object)
                    && durable_replicas < replication_policy.replication_factor;
                if durable_replicas >= required_replicas && (guarded || !async_followers_pending) {
                    return Ok(response);
                }
                if guarded {
                    return Err(EngineError::Fenced(
                        "historical receipt replication requires separate authenticated replay authority".into(),
                    ));
                }
                let Some(replication) = &self.replication else {
                    return Err(EngineError::DurabilityUnavailable {
                        required_replicas,
                        durable_replicas,
                    });
                };
                let reservation = Reservation {
                    topic_id: request.topic_id,
                    partition_id: request.partition_id,
                    batch_id: response.batch_id,
                    first_offset: response.first_offset,
                    last_offset: response.last_offset,
                    record_count,
                    placement: response.placement,
                };
                let remote = replication.replicate(
                    ReplicaAppend {
                        reservation,
                        producer: request.producer,
                        atomic_group: request.atomic_group,
                        extension_context: request.extension_context,
                        payload_digest,
                        payload: request.payload,
                    },
                    if async_followers_pending {
                        0
                    } else {
                        required_replicas.saturating_sub(1)
                    },
                )?;
                let reached = remote.saturating_add(1);
                self.coordinators
                    .record_durable_replicas(topic_partition, reservation, reached)?;
                if reached < required_replicas {
                    return Err(EngineError::DurabilityUnavailable {
                        required_replicas,
                        durable_replicas: reached,
                    });
                }
                return Ok(response);
            }
            ReserveOutcome::Reserved {
                reservation,
                producer,
                ..
            } => (reservation, producer),
        };

        let required_followers = required_replicas.saturating_sub(1);
        let mut replicated_batch = self.replication.as_ref().map(|_| ReplicaAppend {
            reservation,
            producer: request.producer,
            atomic_group: request.atomic_group,
            extension_context: request.extension_context,
            payload_digest,
            payload: request.payload.clone(),
        });
        let mut replication_ticket = None;
        let mut replication_start_error = None;
        if !guarded
            && required_followers > 0
            && let (Some(replication), Some(batch)) = (&self.replication, replicated_batch.take())
        {
            match replication.begin_replicate(batch, required_followers) {
                Ok(ticket) => replication_ticket = Some(ticket),
                Err(error) => replication_start_error = Some(error),
            }
        }
        let append_result = if let Some(result) = guarded_result {
            result
        } else {
            self.shard_for_lane(reservation.placement.virtual_lane_id)?
                .append(
                    reservation,
                    producer,
                    request.atomic_group.map(|group| AtomicGroupSequence {
                        transaction_id: group.transaction_id,
                        chunk_sequence: group.chunk_sequence,
                        payload_digest,
                    }),
                    request.payload,
                    request.durability == Durability::Object,
                    admission,
                )
        };
        let response = append_response(request.request_id, reservation);
        match append_result {
            Ok(mut durability) => {
                if let (Some(durable_sink), Some(payload)) = (&self.durable_sink, sink_payload) {
                    durable_sink.enqueue(
                        durable_append(
                            &self.config,
                            reservation,
                            request.producer.and_then(|producer| producer.event_id),
                            request.atomic_group,
                            payload,
                            sink_context,
                        ),
                        sink_charge.take(),
                    );
                }
                let mut replication_error = replication_start_error;
                if let Some(ticket) = replication_ticket {
                    match ticket.wait() {
                        Ok(remote) => {
                            durability.durable_replicas =
                                durability.durable_replicas.saturating_add(remote);
                        }
                        Err(error) => replication_error = Some(error),
                    }
                } else if let (Some(replication), Some(batch)) =
                    (&self.replication, replicated_batch)
                {
                    // Guarded local work has settled and released its physical
                    // authority before remote admission/ACK waits. A follower
                    // must independently prove its own physical permission.
                    match replication.replicate(batch, if guarded { required_followers } else { 0 })
                    {
                        Ok(remote) => {
                            durability.durable_replicas =
                                durability.durable_replicas.saturating_add(remote);
                        }
                        Err(error) => replication_error = Some(error),
                    }
                }
                let response = self.coordinators.complete(
                    topic_partition,
                    CompleteAppend {
                        reservation,
                        producer: request.producer,
                        payload_digest,
                        durable_replicas: durability.durable_replicas,
                        object_durable: durability.object_durable,
                        response,
                    },
                )?;
                if let Some(error) = replication_error {
                    return Err(error);
                }
                if durability.durable_replicas < required_replicas {
                    return Err(EngineError::DurabilityUnavailable {
                        required_replicas,
                        durable_replicas: durability.durable_replicas,
                    });
                }
                if request.durability == Durability::Object && !durability.object_durable {
                    return Err(EngineError::ObjectDurabilityUnavailable);
                }
                Ok(response)
            }
            Err(failure) => {
                self.coordinators.fail(
                    topic_partition,
                    reservation,
                    request.producer,
                    failure.definitely_not_written,
                )?;
                Err(failure.error)
            }
        }
    }

    /// Installs a leader-reserved batch on a follower. The reservation is
    /// idempotent and must be the exact next batch for this partition.
    pub fn append_replica(&self, batch: ReplicaAppend) -> EngineResult<AppendResponse> {
        let plan = self.plan_replica_append(&batch)?;
        self.execute_replica_append(batch, plan)
    }

    pub(crate) fn plan_replica_append(
        &self,
        batch: &ReplicaAppend,
    ) -> EngineResult<ReplicaAppendPlan> {
        self.plan_replica_append_from_peer(batch, None)
    }

    /// Plans exact replica input with optional trusted transport context.
    ///
    /// The installed fence owns role/lineage validation; the context's presence
    /// alone is not authentication. Pending permission survives the physical
    /// queue. Guarded bound duplicates check current read-only replica authority
    /// and exact durable identity before acquiring any new ticket or guard.
    pub(crate) fn plan_replica_append_from_peer(
        &self,
        batch: &ReplicaAppend,
        source: Option<&ReplicaSourceIdentity>,
    ) -> EngineResult<ReplicaAppendPlan> {
        let topic_partition =
            TopicPartition::new(batch.reservation.topic_id, batch.reservation.partition_id);
        if self.requires_physical_admission
            && let Some((write_fence, source)) = self.write_fence.as_ref().zip(source)
        {
            let epoch = batch.reservation.placement.leader_epoch.get();
            write_fence
                .check_replica_write(topic_partition, epoch, source)
                .map_err(|error| EngineError::Fenced(error.to_string()))?;
            let payload_bytes = u64::try_from(batch.payload.len())
                .map_err(|_| EngineError::InvalidConfig("replica payload exceeds u64".into()))?;
            if batch.payload.len() > self.config.max_batch_bytes
                || crate::worker::append_command_charge(batch.payload.len(), true)
                    .is_none_or(|charge| charge > self.config.queue_bytes_per_shard)
            {
                return Err(EngineError::InvalidConfig(
                    "replica append plus physical admission metadata exceeds shard capacity".into(),
                ));
            }
            let prepared = self._replication_progress.prepare_replica(ReplicaIntent {
                reservation: batch.reservation,
                producer: batch.producer,
                atomic_group: batch.atomic_group,
                payload_bytes,
                payload_digest: batch.payload_digest,
            })?;
            match prepared {
                PrepareReplicaOutcome::Duplicate(ReserveOutcome::Duplicate {
                    response, ..
                }) => {
                    // The pure RPC may wait. Recheck current authority before
                    // returning history; this grants no new physical permission.
                    write_fence
                        .check_replica_write(topic_partition, epoch, source)
                        .map_err(|error| EngineError::Fenced(error.to_string()))?;
                    return Ok(ReplicaAppendPlan::Duplicate(response));
                }
                PrepareReplicaOutcome::Duplicate(ReserveOutcome::Reserved { .. }) => {
                    return Err(EngineError::CorruptState(
                        "replica duplicate returned new allocation".into(),
                    ));
                }
                PrepareReplicaOutcome::New(prepared) => {
                    let admission = write_fence
                        .prepare_replica_write(topic_partition, epoch, source)
                        .map_err(|error| EngineError::Fenced(error.to_string()))?;
                    self.validate_admission_contract(admission.as_ref())?;
                    validate_pending_admission(admission.as_ref())?;
                    let admission = admission.ok_or_else(|| {
                        EngineError::Fenced("guarded replica has no physical ticket".into())
                    })?;
                    return Ok(ReplicaAppendPlan::Prepared {
                        prepared: Box::new(prepared),
                        admission,
                    });
                }
            }
        }
        let admission = self.write_fence.as_ref().map_or(Ok(None), |write_fence| {
            let epoch = batch.reservation.placement.leader_epoch.get();
            match source {
                Some(source) => write_fence.prepare_replica_write(topic_partition, epoch, source),
                None => write_fence.prepare_unbound_replica_write(topic_partition, epoch),
            }
            .map_err(|error| EngineError::Fenced(error.to_string()))
        })?;
        self.validate_admission_contract(admission.as_ref())?;
        let producer = batch.producer.map(|producer| ProducerSequence {
            producer_id: producer.producer_id,
            epoch: producer.epoch,
            first_sequence: producer.first_sequence,
            event_id: producer.event_id,
            digest_algorithm: DIGEST_XXH3_128,
            payload_digest: batch.payload_digest,
        });
        let atomic_group = batch.atomic_group.map(|group| AtomicGroupSequence {
            transaction_id: group.transaction_id,
            chunk_sequence: group.chunk_sequence,
            payload_digest: batch.payload_digest,
        });
        let payload_bytes = u64::try_from(batch.payload.len())
            .map_err(|_| EngineError::InvalidConfig("replica payload exceeds u64".into()))?;
        if admission.is_some()
            && (batch.payload.len() > self.config.max_batch_bytes
                || crate::worker::append_command_charge(batch.payload.len(), true)
                    .is_none_or(|charge| charge > self.config.queue_bytes_per_shard))
        {
            return Err(EngineError::InvalidConfig(
                "replica append plus physical admission metadata exceeds shard capacity".into(),
            ));
        }
        validate_pending_admission(admission.as_ref())?;
        if let Some(admission) = admission {
            // The actual immutable remote coordinates remain unallocated until
            // the charged receiver shard owns a physical guard and commits.
            return match self._replication_progress.prepare_replica(ReplicaIntent {
                reservation: batch.reservation,
                producer: batch.producer,
                atomic_group: batch.atomic_group,
                payload_bytes,
                payload_digest: batch.payload_digest,
            })? {
                PrepareReplicaOutcome::New(prepared) => Ok(ReplicaAppendPlan::Prepared {
                    prepared: Box::new(prepared),
                    admission,
                }),
                PrepareReplicaOutcome::Duplicate(ReserveOutcome::Duplicate {
                    response, ..
                }) => Ok(ReplicaAppendPlan::Duplicate(response)),
                PrepareReplicaOutcome::Duplicate(ReserveOutcome::Reserved { .. }) => Err(
                    EngineError::CorruptState("replica duplicate returned new allocation".into()),
                ),
            };
        }
        self.coordinators
            .observe_leader_epoch(topic_partition, batch.reservation.placement.leader_epoch)?;
        let response = match self.coordinators.reserve_replica(
            topic_partition,
            batch.reservation,
            batch.producer,
            batch.atomic_group,
            payload_bytes,
            batch.payload_digest,
        )? {
            ReserveOutcome::Duplicate { response, .. } => {
                return Ok(ReplicaAppendPlan::Duplicate(response));
            }
            ReserveOutcome::Reserved { .. } => append_response(0, batch.reservation),
        };
        Ok(ReplicaAppendPlan::Reserved {
            response,
            producer,
            atomic_group,
            admission: None,
        })
    }

    fn validate_admission_contract(
        &self,
        admission: Option<&Arc<dyn PendingWriteAdmission>>,
    ) -> EngineResult<()> {
        if admission.is_some() != self.requires_physical_admission {
            return Err(EngineError::Fenced(
                "physical admission ticket disagrees with the installed fence contract".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn execute_replica_append(
        &self,
        batch: ReplicaAppend,
        plan: ReplicaAppendPlan,
    ) -> EngineResult<AppendResponse> {
        self.execute_replica_append_group(vec![(batch, plan)])
            .into_iter()
            .next()
            .expect("single replica append returns one result")
    }

    pub(crate) fn execute_replica_append_group(
        &self,
        appends: Vec<(ReplicaAppend, ReplicaAppendPlan)>,
    ) -> Vec<EngineResult<AppendResponse>> {
        enum Pending {
            Legacy {
                batch: ReplicaAppend,
                response: AppendResponse,
                receiver:
                    Receiver<Result<crate::worker::AppendDurability, crate::worker::AppendFailure>>,
            },
            Prepared {
                batch: ReplicaAppend,
                ticket: crate::worker::ProposedAppendTicket,
            },
        }
        let mut results = (0..appends.len())
            .map(|_| None)
            .collect::<Vec<Option<EngineResult<AppendResponse>>>>();
        let mut pending = Vec::with_capacity(appends.len());
        for (index, (batch, plan)) in appends.into_iter().enumerate() {
            match plan {
                ReplicaAppendPlan::Duplicate(response) => results[index] = Some(Ok(response)),
                ReplicaAppendPlan::Prepared {
                    prepared,
                    admission,
                } => {
                    let result = self
                        .shard_for_lane(batch.reservation.placement.virtual_lane_id)
                        .and_then(|shard| {
                            shard.begin_prepared_replica(
                                *prepared,
                                batch.payload.clone(),
                                admission,
                                Arc::clone(&self._replication_progress),
                            )
                        });
                    match result {
                        Ok(ticket) => pending.push((index, Pending::Prepared { batch, ticket })),
                        Err(error) => results[index] = Some(Err(error)),
                    }
                }
                ReplicaAppendPlan::Reserved {
                    response,
                    producer,
                    atomic_group,
                    admission,
                } => {
                    let shard =
                        match self.shard_for_lane(batch.reservation.placement.virtual_lane_id) {
                            Ok(shard) => shard,
                            Err(error) => {
                                results[index] = Some(Err(error));
                                continue;
                            }
                        };
                    let (completion, receiver) = sync_channel(1);
                    match shard.begin_append(
                        batch.reservation,
                        producer,
                        atomic_group,
                        batch.payload.clone(),
                        false,
                        admission,
                        completion,
                    ) {
                        Ok(()) => pending.push((
                            index,
                            Pending::Legacy {
                                batch,
                                response,
                                receiver,
                            },
                        )),
                        Err(error) => {
                            results[index] =
                                Some(self.finish_replica_append(&batch, response, Err(error)));
                        }
                    }
                }
            }
        }
        for (index, pending) in pending {
            results[index] = Some(match pending {
                Pending::Legacy {
                    batch,
                    response,
                    receiver,
                } => {
                    let append_result = match receiver.recv() {
                        Ok(result) => result,
                        Err(_) => Err(crate::worker::AppendFailure::ambiguous(
                            EngineError::WorkerStopped(batch.reservation.placement.virtual_lane_id),
                        )),
                    };
                    self.finish_replica_append(&batch, response, append_result)
                }
                Pending::Prepared { batch, ticket } => match ticket.wait() {
                    ProposedAppendOutcome::Unallocated(error)
                    | ProposedAppendOutcome::Unknown(error) => Err(error),
                    ProposedAppendOutcome::Duplicate(ReserveOutcome::Duplicate {
                        response,
                        ..
                    }) => Ok(response),
                    ProposedAppendOutcome::Duplicate(ReserveOutcome::Reserved { .. }) => {
                        Err(EngineError::CorruptState(
                            "replica duplicate returned new allocation".into(),
                        ))
                    }
                    ProposedAppendOutcome::Committed {
                        reservation,
                        result,
                        ..
                    } => {
                        if reservation != batch.reservation {
                            Err(EngineError::CorruptState(
                                "guarded replica committed different coordinates".into(),
                            ))
                        } else {
                            self.finish_replica_append(
                                &batch,
                                append_response(0, reservation),
                                result,
                            )
                        }
                    }
                },
            });
        }
        results
            .into_iter()
            .map(|result| result.expect("every replica append receives a result"))
            .collect()
    }

    fn finish_replica_append(
        &self,
        batch: &ReplicaAppend,
        response: AppendResponse,
        append_result: Result<crate::worker::AppendDurability, crate::worker::AppendFailure>,
    ) -> EngineResult<AppendResponse> {
        let topic_partition =
            TopicPartition::new(batch.reservation.topic_id, batch.reservation.partition_id);
        match append_result {
            Ok(durability) => self.coordinators.complete(
                topic_partition,
                CompleteAppend {
                    reservation: batch.reservation,
                    producer: batch.producer,
                    payload_digest: batch.payload_digest,
                    durable_replicas: durability.durable_replicas,
                    object_durable: durability.object_durable,
                    response,
                },
            ),
            Err(failure) => {
                self.coordinators.fail(
                    topic_partition,
                    batch.reservation,
                    batch.producer,
                    failure.definitely_not_written,
                )?;
                Err(failure.error)
            }
        }
    }

    pub(crate) fn cancel_replica_append(&self, batch: &ReplicaAppend) -> EngineResult<()> {
        self.coordinators.fail(
            TopicPartition::new(batch.reservation.topic_id, batch.reservation.partition_id),
            batch.reservation,
            batch.producer,
            true,
        )
    }

    pub(crate) fn replica_append_worker_count(&self) -> usize {
        self.shards.len()
    }

    pub(crate) fn replica_append_worker_index(&self, lane_id: ShardId) -> usize {
        self.config.physical_shard_for_lane(lane_id).get() as usize
    }

    pub fn fetch(&self, request: FetchRequest) -> EngineResult<Vec<FetchedBatch>> {
        self.fetch_stored(request).map(|batches| {
            batches
                .into_iter()
                .map(|batch| FetchedBatch {
                    batch_id: batch.reservation.batch_id,
                    event_id: batch.producer.and_then(|producer| producer.event_id),
                    atomic_group: batch.atomic_group.map(|group| AtomicGroupIdentity {
                        transaction_id: group.transaction_id,
                        chunk_sequence: group.chunk_sequence,
                    }),
                    first_offset: batch.reservation.first_offset,
                    last_offset: batch.reservation.last_offset,
                    record_count: batch.reservation.record_count,
                    payload: batch.payload,
                    placement: batch.reservation.placement,
                })
                .collect()
        })
    }

    fn fetch_stored(&self, request: FetchRequest) -> EngineResult<Vec<StoredBatch>> {
        self.fetch_stored_internal(request, false)
    }

    fn fetch_stored_internal(
        &self,
        request: FetchRequest,
        include_unresolved: bool,
    ) -> EngineResult<Vec<StoredBatch>> {
        request.validate().map_err(|error| {
            EngineError::InvalidConfig(format!("invalid fetch request: {error}"))
        })?;
        let max_bytes = usize::try_from(request.max_bytes)
            .map_err(|_| EngineError::InvalidConfig("max_bytes exceeds usize".into()))?;
        if max_bytes > self.config.max_fetch_bytes {
            return Err(EngineError::InvalidConfig(format!(
                "fetch max_bytes {max_bytes} exceeds configured maximum {}",
                self.config.max_fetch_bytes
            )));
        }
        let topic_partition = TopicPartition::new(request.topic_id, request.partition_id);
        let bounds = self
            .coordinators
            .fetch_snapshot(topic_partition, request.start_offset)?;
        let ordered_end = if include_unresolved {
            bounds.physical_end
        } else {
            match request.mode {
                FetchMode::Replicated => bounds.replicated_end,
                FetchMode::Ordered | FetchMode::AsAvailable => bounds.ordered_end,
            }
        };

        let (completion, completions) = channel();
        for (lane, shard) in self.shards.iter().enumerate() {
            shard.begin_fetch_plan(
                lane,
                topic_partition,
                request.start_offset,
                max_bytes,
                MAX_FETCH_PLAN_BATCHES_PER_SHARD,
                completion.clone(),
            )?;
        }
        let mut shard_plans = (0..self.shards.len())
            .map(|_| Vec::new())
            .collect::<Vec<_>>();
        let mut planned_lanes = vec![false; self.shards.len()];
        for _ in 0..self.shards.len() {
            let received = completions.recv().map_err(|_| {
                EngineError::CorruptState("fetch plan completion channel disconnected".into())
            })?;
            let FetchCompletion::Plan { lane, result } = received else {
                return Err(EngineError::CorruptState(
                    "received a fetch read completion during planning".into(),
                ));
            };
            let Some(planned) = shard_plans.get_mut(lane) else {
                return Err(EngineError::CorruptState(
                    "fetch plan completion has an invalid lane".into(),
                ));
            };
            let Some(seen) = planned_lanes.get_mut(lane) else {
                return Err(EngineError::CorruptState(
                    "fetch plan completion has an invalid lane".into(),
                ));
            };
            if *seen {
                return Err(EngineError::CorruptState(
                    "fetch plan completed twice for one lane".into(),
                ));
            }
            *seen = true;
            *planned = result?;
        }

        let candidates = shard_plans
            .iter()
            .flatten()
            .map(|metadata| metadata.reservation().batch_id)
            .collect();
        let committed = Some(self.coordinators.committed_candidates(
            topic_partition,
            candidates,
            include_unresolved,
        )?);
        let selection = select_fetch_plan(
            &shard_plans,
            request.mode,
            ordered_end,
            committed.as_ref(),
            max_bytes,
        )?;

        let mut pending_reads = 0usize;
        for (lane, (shard, planned)) in self.shards.iter().zip(selection.by_shard).enumerate() {
            if !planned.is_empty() {
                shard.begin_fetch_read(lane, planned, completion.clone())?;
                pending_reads += 1;
            }
        }
        let mut shard_batches = (0..self.shards.len())
            .map(|_| VecDeque::new())
            .collect::<Vec<_>>();
        let mut read_lanes = vec![false; self.shards.len()];
        for _ in 0..pending_reads {
            let received = completions.recv().map_err(|_| {
                EngineError::CorruptState("fetch read completion channel disconnected".into())
            })?;
            let FetchCompletion::Read { lane, result } = received else {
                return Err(EngineError::CorruptState(
                    "received a fetch plan completion during reading".into(),
                ));
            };
            let Some(batches) = shard_batches.get_mut(lane) else {
                return Err(EngineError::CorruptState(
                    "fetch read completion has an invalid lane".into(),
                ));
            };
            let Some(seen) = read_lanes.get_mut(lane) else {
                return Err(EngineError::CorruptState(
                    "fetch read completion has an invalid lane".into(),
                ));
            };
            if *seen {
                return Err(EngineError::CorruptState(
                    "fetch read completed twice for one lane".into(),
                ));
            }
            *seen = true;
            *batches = VecDeque::from(result?);
        }
        materialize_stored_batches(shard_batches, &selection.merge_order)
    }

    /// Returns partitions that had local WAL extents during engine recovery.
    /// The server uses this list to schedule post-listener follower catch-up
    /// only for partitions led by this broker.
    #[must_use]
    pub fn recovered_partitions(&self) -> Vec<TopicPartition> {
        self.recovered_partitions.clone()
    }

    /// Replays retained WAL data for partitions currently led by this broker.
    ///
    /// Live follower application is intentionally silent. A promoted follower
    /// calls this method with its new leader assignments so any event missed by
    /// the previous leader is recovered through the cluster-shared checkpoint.
    pub fn resume_durable_sink(&self, partitions: &[TopicPartition]) -> EngineResult<()> {
        let Some(durable_sink) = &self.durable_sink else {
            return Ok(());
        };
        let max_bytes = u32::try_from(self.config.max_fetch_bytes).map_err(|_| {
            EngineError::InvalidConfig("max_fetch_bytes exceeds the sink recovery limit".into())
        })?;
        for &topic_partition in partitions {
            let mut checkpoint = durable_sink.checkpoint(topic_partition)?;
            let mut recovered_decisions = HashMap::new();
            let watermarks = self.watermarks(topic_partition)?;
            if checkpoint.next_offset < watermarks.log_start {
                return Err(EngineError::DurableSinkCheckpoint(format!(
                    "checkpoint offset {} for {}/{} is behind retained log start {}",
                    checkpoint.next_offset,
                    topic_partition.topic_id,
                    topic_partition.partition_id,
                    watermarks.log_start
                )));
            }
            if checkpoint.next_offset > watermarks.contiguous_log_end {
                return Err(EngineError::DurableSinkCheckpoint(format!(
                    "checkpoint offset {} for {}/{} is ahead of durable log end {}",
                    checkpoint.next_offset,
                    topic_partition.topic_id,
                    topic_partition.partition_id,
                    watermarks.contiguous_log_end
                )));
            }
            let target_end = watermarks.contiguous_log_end;
            while checkpoint.next_offset < target_end {
                let batches = self.fetch_stored_internal(
                    FetchRequest {
                        request_id: 0,
                        topic_id: topic_partition.topic_id,
                        partition_id: topic_partition.partition_id,
                        start_offset: checkpoint.next_offset,
                        max_bytes,
                        mode: FetchMode::Ordered,
                    },
                    true,
                )?;
                if batches.is_empty() {
                    return Err(EngineError::CorruptState(format!(
                        "durable sink recovery found no batch at offset {} for {}/{}",
                        checkpoint.next_offset,
                        topic_partition.topic_id,
                        topic_partition.partition_id
                    )));
                }
                for batch in batches {
                    if batch.reservation.first_offset >= target_end {
                        break;
                    }
                    let next = checkpoint.after(batch.reservation)?;
                    let recovered_group = batch.atomic_group.map(|group| DurableAtomicGroup {
                        topic_partition,
                        transaction_id: group.transaction_id,
                    });
                    durable_sink.enqueue_replay(durable_append_from_stored(&self.config, batch))?;
                    if let Some(group) = recovered_group {
                        let inspection =
                            self.inspect_atomic_group(topic_partition, group.transaction_id)?;
                        if inspection.state != AtomicGroupState::Unresolved {
                            recovered_decisions.insert(
                                group,
                                DurableAtomicDecision {
                                    group,
                                    outcome: inspection.state,
                                },
                            );
                        }
                    }
                    checkpoint = next;
                }
            }
            for decision in recovered_decisions.values().copied() {
                durable_sink.enqueue_replay_decision(decision)?;
            }
            durable_sink.clear_dirty(topic_partition);
            if durable_sink.recovery_mode() == DurableSinkRecoveryMode::Blocking {
                durable_sink.wait_for(checkpoint)?;
                for group in recovered_decisions.keys().copied() {
                    durable_sink.wait_for_decision(group)?;
                }
            }
            let log_start = self.watermarks(topic_partition)?.log_start;
            self.collect_garbage(topic_partition, log_start)?;
        }

        let mut pending_decisions = Vec::new();
        for group in durable_sink.pending_atomic_groups()? {
            if !partitions.contains(&group.topic_partition) {
                continue;
            }
            let inspection =
                self.inspect_atomic_group(group.topic_partition, group.transaction_id)?;
            if inspection.state != AtomicGroupState::Unresolved {
                let decision = DurableAtomicDecision {
                    group,
                    outcome: inspection.state,
                };
                durable_sink.enqueue_replay_decision(decision)?;
                pending_decisions.push(group);
            }
        }
        if durable_sink.recovery_mode() == DurableSinkRecoveryMode::Blocking {
            for group in pending_decisions {
                durable_sink.wait_for_decision(group)?;
            }
        }
        Ok(())
    }

    /// Replays recovered local WAL batches through the asynchronous replication
    /// transport. Recovery starts with leader-only durability, so a crash does
    /// not turn a local extent into a false RF3 watermark. The transport's
    /// normal idempotent follower append and contiguous progress path then
    /// rebuild remote durability.
    pub fn resume_recovered_replication(&self, partitions: &[TopicPartition]) -> EngineResult<()> {
        let Some(replication) = &self.replication else {
            return Ok(());
        };
        let max_bytes = u32::try_from(self.config.max_fetch_bytes).map_err(|_| {
            EngineError::InvalidConfig("max_fetch_bytes exceeds the recovery fetch limit".into())
        })?;
        for &topic_partition in partitions {
            let watermarks = self.watermarks(topic_partition)?;
            let mut offset = watermarks.log_start;
            let end = watermarks.contiguous_log_end;
            while offset < end {
                let batches = self.fetch_stored_internal(
                    FetchRequest {
                        request_id: 0,
                        topic_id: topic_partition.topic_id,
                        partition_id: topic_partition.partition_id,
                        start_offset: offset,
                        max_bytes,
                        mode: FetchMode::Ordered,
                    },
                    true,
                )?;
                if batches.is_empty() {
                    return Err(EngineError::CorruptState(format!(
                        "recovery replication found no batch at offset {} for {}/{}",
                        offset.get(),
                        topic_partition.topic_id,
                        topic_partition.partition_id
                    )));
                }
                for batch in batches {
                    let producer = batch.producer.map(|producer| ProducerIdentity {
                        producer_id: producer.producer_id,
                        epoch: producer.epoch,
                        first_sequence: producer.first_sequence,
                        event_id: producer.event_id,
                    });
                    let payload_digest = batch
                        .producer
                        .map_or([0; 16], |producer| producer.payload_digest);
                    offset = LogicalOffset::new(
                        batch
                            .reservation
                            .last_offset
                            .get()
                            .checked_add(1)
                            .ok_or_else(|| {
                                EngineError::CorruptState(
                                    "recovery replication offset space exhausted".into(),
                                )
                            })?,
                    );
                    replication.replicate(
                        ReplicaAppend {
                            reservation: batch.reservation,
                            producer,
                            atomic_group: batch.atomic_group.map(|group| AtomicGroupIdentity {
                                transaction_id: group.transaction_id,
                                chunk_sequence: group.chunk_sequence,
                            }),
                            extension_context: None,
                            payload_digest,
                            payload: batch.payload,
                        },
                        0,
                    )?;
                }
            }
        }
        Ok(())
    }

    pub fn watermarks(&self, topic_partition: TopicPartition) -> EngineResult<PartitionWatermarks> {
        self.coordinators.watermarks(topic_partition)
    }

    pub fn partition_replication_policy(
        &self,
        topic_partition: TopicPartition,
    ) -> EngineResult<PartitionReplicationPolicy> {
        self.coordinators.replication_policy(topic_partition)
    }

    /// Returns the producer cursor reconstructed from retained durable
    /// extents. Native producer sessions use this to repair metadata after a
    /// crash between log append and metadata advancement.
    pub fn producer_progress(
        &self,
        topic_partition: TopicPartition,
        producer_id: u128,
    ) -> EngineResult<Option<ProducerProgress>> {
        self.coordinators
            .producer_progress(topic_partition, producer_id)
    }

    /// Looks up the original durable receipt for an idempotent logical event.
    ///
    /// HA routers use this against a retained source generation before
    /// admitting an ambiguous retry to a newly activated generation.
    pub fn idempotent_receipt(
        &self,
        topic_partition: TopicPartition,
        producer: ProducerIdentity,
    ) -> EngineResult<Option<AppendResponse>> {
        self.coordinators
            .idempotent_receipt(topic_partition, producer)
    }

    pub fn partition_epochs(
        &self,
        topic_partition: TopicPartition,
    ) -> EngineResult<PartitionEpochs> {
        self.coordinators.partition_epochs(topic_partition)
    }

    /// Administrative byte accounting for retention pins. This measures
    /// application payload currently visible from `start_offset`.
    pub fn retained_payload_bytes(
        &self,
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
    ) -> EngineResult<u64> {
        let watermarks = self.watermarks(topic_partition)?;
        if start_offset < watermarks.log_start {
            return Err(EngineError::OffsetOutOfRange {
                requested: start_offset,
                log_start: watermarks.log_start,
            });
        }
        let mut offset = start_offset;
        let mut bytes = 0u64;
        let max_bytes = u32::try_from(self.config.max_fetch_bytes)
            .map_err(|_| EngineError::InvalidConfig("max_fetch_bytes exceeds u32".into()))?;
        while offset < watermarks.last_stable_offset {
            let batches = self.fetch(FetchRequest {
                request_id: 0,
                topic_id: topic_partition.topic_id,
                partition_id: topic_partition.partition_id,
                start_offset: offset,
                max_bytes,
                mode: FetchMode::Ordered,
            })?;
            if batches.is_empty() {
                break;
            }
            for batch in batches {
                bytes = bytes
                    .checked_add(u64::try_from(batch.payload.len()).map_err(|_| {
                        EngineError::InvalidConfig("retained payload exceeds u64".into())
                    })?)
                    .ok_or_else(|| {
                        EngineError::InvalidConfig("retained payload byte count overflow".into())
                    })?;
                offset = LogicalOffset::new(batch.last_offset.get().checked_add(1).ok_or_else(
                    || EngineError::InvalidConfig("retained offset exhausted".into()),
                )?);
            }
        }
        Ok(bytes)
    }

    /// Returns the durable engine view of a partition-scoped atomic group.
    pub fn inspect_atomic_group(
        &self,
        topic_partition: TopicPartition,
        transaction_id: u128,
    ) -> EngineResult<AtomicGroupInspection> {
        self.coordinators
            .inspect_atomic_group(topic_partition, transaction_id)
    }

    /// Syncs a commit marker and makes every chunk in the group visible
    /// atomically through the partition's last-stable offset.
    pub fn commit_atomic_group(
        &self,
        topic_partition: TopicPartition,
        transaction_id: u128,
    ) -> EngineResult<AtomicGroupInspection> {
        let mut sink_charge = self
            .durable_sink
            .as_ref()
            .map(DurableSinkDispatcher::reserve_decision)
            .transpose()?
            .flatten();
        let inspection = self.coordinators.finalize_atomic_group(
            topic_partition,
            transaction_id,
            AtomicGroupState::Committed,
        )?;
        if let Some(durable_sink) = &self.durable_sink {
            durable_sink.enqueue_decision(
                DurableAtomicDecision {
                    group: DurableAtomicGroup {
                        topic_partition,
                        transaction_id,
                    },
                    outcome: AtomicGroupState::Committed,
                },
                sink_charge.take(),
            );
        }
        Ok(inspection)
    }

    /// Syncs an abort marker. Aborted chunks consume offsets but are never
    /// returned by ordinary fetch.
    pub fn abort_atomic_group(
        &self,
        topic_partition: TopicPartition,
        transaction_id: u128,
    ) -> EngineResult<AtomicGroupInspection> {
        let mut sink_charge = self
            .durable_sink
            .as_ref()
            .map(DurableSinkDispatcher::reserve_decision)
            .transpose()?
            .flatten();
        let inspection = self.coordinators.finalize_atomic_group(
            topic_partition,
            transaction_id,
            AtomicGroupState::Aborted,
        )?;
        if let Some(durable_sink) = &self.durable_sink {
            durable_sink.enqueue_decision(
                DurableAtomicDecision {
                    group: DurableAtomicGroup {
                        topic_partition,
                        transaction_id,
                    },
                    outcome: AtomicGroupState::Aborted,
                },
                sink_charge.take(),
            );
        }
        Ok(inspection)
    }

    /// Waits without blocking the partition coordinator until `through` is
    /// contiguous under the selected ordered visibility contract.
    pub fn wait_until_visible(
        &self,
        topic_partition: TopicPartition,
        through: LogicalOffset,
        mode: FetchMode,
    ) -> EngineResult<()> {
        let mode = match mode {
            FetchMode::Ordered => VisibilityMode::Ordered,
            FetchMode::Replicated => VisibilityMode::Replicated,
            FetchMode::AsAvailable => {
                return Err(EngineError::InvalidConfig(
                    "as-available fetch mode has no contiguous visibility watermark".into(),
                ));
            }
        };
        self.coordinators
            .wait_until_visible(topic_partition, through, mode)
    }

    /// Returns the exact next leader-reserved batch a follower can admit.
    ///
    /// Replica transports use this cursor to bound out-of-order buffering
    /// without weakening the sequencer's fenced, gap-free append contract.
    pub fn replica_next_batch(&self, topic_partition: TopicPartition) -> EngineResult<BatchId> {
        self.coordinators.replica_next_batch(topic_partition)
    }

    #[must_use]
    pub fn topic_partitions(
        &self,
        topic_id: shard_stream_core::TopicId,
    ) -> Vec<LogicalPartitionId> {
        self.coordinators.topic_partitions(topic_id)
    }

    /// Returns every partition configured in this local engine.
    ///
    /// Health, retention, and administrative callers use this bounded
    /// coordinator snapshot rather than maintaining a second catalog.
    #[must_use]
    pub fn all_partitions(&self) -> Vec<TopicPartition> {
        self.coordinators.all_partitions()
    }

    pub fn truncate_partition(
        &self,
        topic_partition: TopicPartition,
        log_start: LogicalOffset,
    ) -> EngineResult<PartitionWatermarks> {
        if let Some(durable_sink) = &self.durable_sink {
            let checkpoint = durable_sink.checkpoint(topic_partition)?;
            if log_start > checkpoint.next_offset {
                return Err(EngineError::RetentionPinned {
                    requested: log_start,
                    pinned_at: checkpoint.next_offset,
                });
            }
        }
        if let Some(guard) = &self.retention_guard {
            guard.validate_advance(topic_partition, log_start)?;
        }
        let updated = self.coordinators.truncate(topic_partition, log_start)?;
        self.collect_garbage(topic_partition, log_start)?;
        Ok(updated)
    }

    pub fn sync(&self) -> EngineResult<()> {
        // Queue one barrier on every physical shard before waiting. Each
        // shard coalesces all writes admitted before its barrier into one
        // shard-local sync operation, while independent shard fsyncs run in
        // parallel instead of serializing engine-wide durability latency.
        let barriers = self
            .shards
            .iter()
            .map(|shard| Ok((shard.shard_id, shard.begin_sync()?)))
            .collect::<EngineResult<Vec<_>>>()?;
        let mut first_error = None;
        for (shard_id, barrier) in barriers {
            let result = barrier
                .recv()
                .map_err(|_| EngineError::WorkerStopped(shard_id))
                .and_then(|result| result.map_err(EngineError::from));
            if first_error.is_none() {
                first_error = result.err();
            }
        }
        if let Err(error) = self.journal.sync()
            && first_error.is_none()
        {
            first_error = Some(error);
        }
        first_error.map_or(Ok(()), Err)
    }

    #[must_use]
    pub fn replication_stats(&self) -> ReplicationStats {
        self.replication
            .as_ref()
            .map_or_else(ReplicationStats::default, |replication| replication.stats())
    }

    #[must_use]
    pub fn durable_sink_stats(&self) -> DurableSinkStats {
        self.durable_sink
            .as_ref()
            .map_or_else(DurableSinkStats::default, DurableSinkDispatcher::stats)
    }

    pub fn durable_sink_checkpoint(
        &self,
        topic_partition: TopicPartition,
    ) -> EngineResult<Option<DurableSinkCheckpoint>> {
        self.durable_sink
            .as_ref()
            .map(|sink| sink.checkpoint(topic_partition))
            .transpose()
    }

    /// Blocks until the configured durable sink reaches `target`.
    ///
    /// Unlike repeatedly loading [`Self::durable_sink_checkpoint`], this waits
    /// on the sink's progress notification and consumes no polling interval.
    pub fn wait_for_durable_sink_checkpoint(
        &self,
        target: DurableSinkCheckpoint,
    ) -> EngineResult<()> {
        self.durable_sink
            .as_ref()
            .ok_or_else(|| {
                EngineError::DurableSinkUnavailable("engine has no configured durable sink".into())
            })?
            .wait_for(target)
    }

    fn shard_for_lane(&self, lane_id: ShardId) -> EngineResult<&ShardHandle> {
        let shard_id = self.config.physical_shard_for_lane(lane_id);
        self.shards
            .get(shard_id.get() as usize)
            .filter(|shard| shard.shard_id == shard_id)
            .ok_or(EngineError::UnknownShard(shard_id))
    }

    fn collect_garbage(
        &self,
        topic_partition: TopicPartition,
        mut log_start: LogicalOffset,
    ) -> EngineResult<()> {
        if let Some(durable_sink) = &self.durable_sink {
            log_start = log_start.min(durable_sink.checkpoint(topic_partition)?.next_offset);
        }
        for shard in &self.shards {
            shard.garbage_collect(topic_partition, log_start)?;
        }
        Ok(())
    }
}

fn durable_append(
    config: &EngineConfig,
    reservation: Reservation,
    producer_event_id: Option<RecordId>,
    atomic_group: Option<AtomicGroupIdentity>,
    payload: Bytes,
    transient_context: Option<Bytes>,
) -> DurableAppend {
    DurableAppend {
        event_id: RecordId::for_batch(
            reservation.topic_id,
            reservation.partition_id,
            reservation.batch_id,
        ),
        physical_shard_id: config.physical_shard_for_lane(reservation.placement.virtual_lane_id),
        reservation,
        producer_event_id,
        atomic_group,
        delivery: DurableAppendDelivery::Publish,
        payload,
        transient_context,
    }
}

fn durable_append_from_stored(config: &EngineConfig, batch: StoredBatch) -> DurableAppend {
    durable_append(
        config,
        batch.reservation,
        batch.producer.and_then(|producer| producer.event_id),
        batch.atomic_group.map(|group| AtomicGroupIdentity {
            transaction_id: group.transaction_id,
            chunk_sequence: group.chunk_sequence,
        }),
        batch.payload,
        None,
    )
}

fn materialize_stored_batches(
    mut lanes: Vec<VecDeque<StoredBatch>>,
    merge_order: &[usize],
) -> EngineResult<Vec<StoredBatch>> {
    let mut result = Vec::with_capacity(merge_order.len());
    for &lane in merge_order {
        let batch = lanes
            .get_mut(lane)
            .and_then(VecDeque::pop_front)
            .ok_or_else(|| {
                EngineError::CorruptState("fetch materialization did not match its plan".into())
            })?;
        result.push(batch);
    }
    Ok(result)
}

struct FetchSelection {
    by_shard: Vec<Vec<StoredBatchMetadata>>,
    merge_order: Vec<usize>,
}

fn select_fetch_plan(
    shard_plans: &[Vec<StoredBatchMetadata>],
    mode: FetchMode,
    ordered_end: LogicalOffset,
    committed: Option<&CommittedBatchSet>,
    max_bytes: usize,
) -> EngineResult<FetchSelection> {
    let mut positions = vec![0usize; shard_plans.len()];
    let mut ready = BinaryHeap::with_capacity(shard_plans.len());
    let mut selected = shard_plans.iter().map(|_| Vec::new()).collect::<Vec<_>>();
    let mut merge_order = Vec::new();
    for (lane, planned) in shard_plans.iter().enumerate() {
        if let Some(metadata) = planned.first() {
            ready.push(Reverse((metadata.reservation().first_offset, lane)));
        }
    }

    let mut used = 0usize;
    let mut selected_count = 0usize;
    while let Some(Reverse((_, lane))) = ready.pop() {
        let metadata = shard_plans[lane][positions[lane]];
        positions[lane] += 1;
        if let Some(next) = shard_plans[lane].get(positions[lane]) {
            ready.push(Reverse((next.reservation().first_offset, lane)));
        }

        if matches!(mode, FetchMode::Ordered | FetchMode::Replicated)
            && metadata.reservation().last_offset >= ordered_end
        {
            break;
        }
        let visible = committed
            .expect("fetch planning has a committed snapshot")
            .contains(metadata.reservation().batch_id);
        if !visible {
            if mode == FetchMode::AsAvailable {
                continue;
            }
            if metadata.reservation().first_offset >= ordered_end {
                break;
            }
            continue;
        }

        let next = used.checked_add(metadata.payload_len()).ok_or_else(|| {
            EngineError::InvalidConfig("selected fetch byte count overflow".into())
        })?;
        if selected_count > 0 && next > max_bytes {
            break;
        }
        used = next;
        selected[lane].push(metadata);
        merge_order.push(lane);
        selected_count += 1;
    }
    Ok(FetchSelection {
        by_shard: selected,
        merge_order,
    })
}

fn guarded_cached_response(
    request: &AppendRequest,
    outcome: ReserveOutcome,
) -> EngineResult<AppendResponse> {
    let ReserveOutcome::Duplicate {
        response,
        durable_replicas,
        object_durable,
        replication_policy,
    } = outcome
    else {
        return Err(EngineError::CorruptState(
            "cached response is not an existing receipt".into(),
        ));
    };
    if request.durability == Durability::Object && !object_durable {
        return Err(EngineError::ObjectDurabilityUnavailable);
    }
    let required = match request.durability {
        Durability::Quorum => replication_policy.min_in_sync_replicas,
        Durability::AllReplicas => replication_policy.replication_factor,
        Durability::Leader | Durability::Object => 1,
    };
    if durable_replicas < required {
        return Err(EngineError::Fenced(
            "historical receipt replication requires separate authenticated replay authority"
                .into(),
        ));
    }
    Ok(response)
}

fn validate_pending_admission(
    admission: Option<&Arc<dyn PendingWriteAdmission>>,
) -> EngineResult<()> {
    if let Some(admission) = admission {
        catch_unwind(AssertUnwindSafe(|| admission.validate_before_reserve()))
            .map_err(|_| EngineError::Fenced("pre-reservation write admission panicked".into()))?
            .map_err(|error| EngineError::Fenced(error.to_string()))?;
    }
    Ok(())
}

fn append_response(request_id: u128, reservation: Reservation) -> AppendResponse {
    AppendResponse {
        request_id,
        batch_id: reservation.batch_id,
        record_id: RecordId::for_batch(
            reservation.topic_id,
            reservation.partition_id,
            reservation.batch_id,
        ),
        first_offset: reservation.first_offset,
        last_offset: reservation.last_offset,
        visible_through: reservation.first_offset,
        placement: reservation.placement,
    }
}

fn validate_ring(config: &EngineConfig, shards: &[ShardId]) -> EngineResult<()> {
    if shards.is_empty() {
        return Err(EngineError::InvalidConfig(
            "placement ring cannot be empty".into(),
        ));
    }
    let mut unique = HashSet::with_capacity(shards.len());
    for shard in shards {
        if shard.get() >= config.virtual_lane_count {
            return Err(EngineError::UnknownShard(*shard));
        }
        if !unique.insert(*shard) {
            return Err(EngineError::InvalidConfig(format!(
                "duplicate shard {shard} in placement ring"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use shard_stream_core::{
        LeaderEpoch, PlacementSequence, SequencerError, TopicId, VirtualLaneId,
    };
    use shard_stream_protocol::{Durability, FetchMode, ProducerIdentity};
    use shardlog::{JournalEvent, ShardStore};

    use crate::{
        DurableAppendSink, DurableAppendSinkFactory, DurableSinkApply, DurableSinkOptions,
    };

    #[path = "replica_proposal_tests.rs"]
    mod replica_proposal_tests;

    #[cfg(unix)]
    #[path = "directory_ownership_tests.rs"]
    mod directory_ownership_tests;

    struct DirectReplication {
        follower: Arc<StreamEngine>,
        fail_first: AtomicBool,
        progress: OnceLock<ReplicationProgress>,
    }

    #[derive(Debug)]
    struct OutOfOrderReplication {
        first_waiting: AtomicBool,
        release_first: AtomicBool,
    }

    #[derive(Debug)]
    struct DeferredReplication {
        progress: OnceLock<ReplicationProgress>,
        release: Arc<AtomicBool>,
    }

    #[derive(Debug)]
    struct NoopReplication;

    impl std::fmt::Debug for DirectReplication {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("DirectReplication")
                .field("fail_first", &self.fail_first.load(Ordering::Relaxed))
                .finish_non_exhaustive()
        }
    }

    impl ReplicationTransport for DirectReplication {
        fn install_progress(&self, progress: ReplicationProgress) -> EngineResult<()> {
            self.progress.set(progress).map_err(|_| {
                EngineError::InvalidConfig("direct replication progress installed twice".into())
            })
        }

        fn replicate(&self, batch: ReplicaAppend, required_followers: u32) -> EngineResult<u32> {
            if self.fail_first.swap(false, Ordering::AcqRel) {
                return Err(EngineError::DurabilityUnavailable {
                    required_replicas: required_followers.saturating_add(1),
                    durable_replicas: 1,
                });
            }
            let reservation = batch.reservation;
            self.follower.append_replica(batch)?;
            if required_followers == 0
                && let Some(progress) = self.progress.get()
            {
                progress.record_durable_replicas(reservation, 2);
                return Ok(0);
            }
            Ok(1)
        }
    }

    impl ReplicationTransport for OutOfOrderReplication {
        fn install_progress(&self, _progress: ReplicationProgress) -> EngineResult<()> {
            Ok(())
        }

        fn replicate(&self, batch: ReplicaAppend, required_followers: u32) -> EngineResult<u32> {
            assert_eq!(required_followers, 1);
            if batch.reservation.batch_id == BatchId::new(0) {
                self.first_waiting.store(true, Ordering::Release);
                while !self.release_first.load(Ordering::Acquire) {
                    std::thread::yield_now();
                }
            }
            Ok(1)
        }
    }

    impl ReplicationTransport for DeferredReplication {
        fn install_progress(&self, progress: ReplicationProgress) -> EngineResult<()> {
            self.progress.set(progress).map_err(|_| {
                EngineError::InvalidConfig("deferred replication progress installed twice".into())
            })
        }

        fn replicate(&self, batch: ReplicaAppend, required_followers: u32) -> EngineResult<u32> {
            assert_eq!(required_followers, 0);
            let progress = self
                .progress
                .get()
                .expect("replication progress installed")
                .clone();
            let release = Arc::clone(&self.release);
            std::thread::spawn(move || {
                while !release.load(Ordering::Acquire) {
                    std::thread::yield_now();
                }
                progress.record_durable_replicas(batch.reservation, 3);
            });
            Ok(0)
        }
    }

    impl ReplicationTransport for NoopReplication {
        fn install_progress(&self, _progress: ReplicationProgress) -> EngineResult<()> {
            Ok(())
        }

        fn replicate(&self, _batch: ReplicaAppend, _required_followers: u32) -> EngineResult<u32> {
            Ok(0)
        }
    }

    use super::*;

    #[derive(Debug, Default)]
    struct RecordingSinkState {
        checkpoints: HashMap<TopicPartition, DurableSinkCheckpoint>,
        appends: Vec<DurableAppend>,
        staged: HashSet<DurableAtomicGroup>,
        decisions: Vec<DurableAtomicDecision>,
    }

    #[derive(Debug, Default)]
    struct RecordingSinkControl {
        state: Mutex<RecordingSinkState>,
        validation_calls: AtomicUsize,
        failures_remaining: AtomicUsize,
        stale_conflicts_remaining: AtomicUsize,
        advancing_conflict_once: AtomicBool,
        panic_once: AtomicBool,
        block: AtomicBool,
        entered: AtomicBool,
        failed_partition: Mutex<Option<TopicPartition>>,
    }

    #[derive(Debug)]
    struct RecordingSink {
        control: Arc<RecordingSinkControl>,
    }

    #[derive(Debug)]
    struct RecordingSinkFactory {
        control: Arc<RecordingSinkControl>,
    }

    impl DurableAppendSink for RecordingSink {
        fn apply(
            &self,
            expected: DurableSinkCheckpoint,
            appends: &[DurableAppend],
            next: DurableSinkCheckpoint,
        ) -> EngineResult<DurableSinkApply> {
            self.control.entered.store(true, Ordering::Release);
            while self.control.block.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            if self.control.panic_once.swap(false, Ordering::AcqRel) {
                panic!("recording sink injected panic");
            }
            if self
                .control
                .failures_remaining
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(EngineError::DurableSinkUnavailable(
                    "recording sink injected failure".into(),
                ));
            }
            if self
                .control
                .failed_partition
                .lock()
                .map_err(|_| {
                    EngineError::CorruptState("recording sink failure lock poisoned".into())
                })?
                .is_some_and(|partition| partition == expected.topic_partition)
            {
                return Err(EngineError::DurableSinkUnavailable(
                    "recording sink partition failure".into(),
                ));
            }
            let mut state =
                self.control.state.lock().map_err(|_| {
                    EngineError::CorruptState("recording sink lock poisoned".into())
                })?;
            let actual = state
                .checkpoints
                .get(&expected.topic_partition)
                .copied()
                .unwrap_or_else(|| DurableSinkCheckpoint::initial(expected.topic_partition));
            if actual != expected {
                return Ok(DurableSinkApply::CheckpointConflict(actual));
            }
            if self
                .control
                .stale_conflicts_remaining
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Ok(DurableSinkApply::CheckpointConflict(actual));
            }
            if self
                .control
                .advancing_conflict_once
                .swap(false, Ordering::AcqRel)
            {
                state.appends.extend_from_slice(appends);
                state.checkpoints.insert(next.topic_partition, next);
                return Ok(DurableSinkApply::CheckpointConflict(next));
            }
            for append in appends {
                if append.delivery == DurableAppendDelivery::Stage
                    && let Some(group) = append.atomic_group
                {
                    state.staged.insert(DurableAtomicGroup {
                        topic_partition: append.topic_partition(),
                        transaction_id: group.transaction_id,
                    });
                }
                state.appends.push(append.clone());
            }
            state.checkpoints.insert(next.topic_partition, next);
            Ok(DurableSinkApply::Applied)
        }
    }

    impl DurableAppendSinkFactory for RecordingSinkFactory {
        fn validate_append(&self, payload: &[u8], _record_count: NonZeroU32) -> EngineResult<()> {
            self.control
                .validation_calls
                .fetch_add(1, Ordering::Relaxed);
            if payload == b"reject" {
                return Err(EngineError::InvalidConfig(
                    "recording sink rejected append".into(),
                ));
            }
            Ok(())
        }

        fn load_checkpoint(
            &self,
            topic_partition: TopicPartition,
        ) -> EngineResult<Option<DurableSinkCheckpoint>> {
            Ok(self
                .control
                .state
                .lock()
                .map_err(|_| EngineError::CorruptState("recording sink lock poisoned".into()))?
                .checkpoints
                .get(&topic_partition)
                .copied())
        }

        fn open_shard(&self, _shard_id: ShardId) -> EngineResult<Arc<dyn DurableAppendSink>> {
            Ok(Arc::new(RecordingSink {
                control: Arc::clone(&self.control),
            }))
        }

        fn pending_atomic_groups(&self) -> EngineResult<Vec<DurableAtomicGroup>> {
            Ok(self
                .control
                .state
                .lock()
                .map_err(|_| EngineError::CorruptState("recording sink lock poisoned".into()))?
                .staged
                .iter()
                .copied()
                .collect())
        }

        fn supports_committed_only(&self) -> bool {
            true
        }

        fn finalize_atomic_group(&self, decision: DurableAtomicDecision) -> EngineResult<()> {
            let mut state =
                self.control.state.lock().map_err(|_| {
                    EngineError::CorruptState("recording sink lock poisoned".into())
                })?;
            state.staged.remove(&decision.group);
            if !state.decisions.contains(&decision) {
                state.decisions.push(decision);
            }
            Ok(())
        }
    }

    fn recording_sink_config(
        control: &Arc<RecordingSinkControl>,
        options: DurableSinkOptions,
    ) -> DurableSinkConfig {
        DurableSinkConfig::new(Arc::new(RecordingSinkFactory {
            control: Arc::clone(control),
        }))
        .with_options(options)
    }

    fn wait_for_test(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(3);
        while !condition() {
            assert!(Instant::now() < deadline, "condition did not become true");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    #[derive(Debug)]
    struct ExactEpochFence;

    impl shard_stream_core::WriteFence for ExactEpochFence {
        fn check_write(
            &self,
            _topic_partition: TopicPartition,
            leader_epoch: u64,
        ) -> Result<(), shard_stream_core::WriteFenceError> {
            if leader_epoch == 7 {
                Ok(())
            } else {
                Err(shard_stream_core::WriteFenceError::new(format!(
                    "expected epoch 7, received epoch {leader_epoch}"
                )))
            }
        }
    }

    // This local admission fixture tests engine coordinates, not a distributed
    // authority or synchronization between a lease transition and physical I/O.
    #[derive(Debug)]
    struct FixtureEpochFence(AtomicU64);

    impl WriteFence for FixtureEpochFence {
        fn check_write(
            &self,
            _topic_partition: TopicPartition,
            leader_epoch: u64,
        ) -> Result<(), shard_stream_core::WriteFenceError> {
            let expected = self.0.load(Ordering::Acquire);
            if expected == leader_epoch {
                Ok(())
            } else {
                Err(shard_stream_core::WriteFenceError::new(format!(
                    "expected fixture epoch {expected}, received {leader_epoch}"
                )))
            }
        }
    }

    #[derive(Debug, Default)]
    struct FixturePendingAdmission {
        refuse_before_reserve: AtomicBool,
        panic_before_reserve: AtomicBool,
        refuse_physical: AtomicBool,
        active: Arc<AtomicUsize>,
        physical_admissions: AtomicUsize,
        expected_ring: AtomicU64,
        proposal_pause: Mutex<Option<Arc<ProposalPause>>>,
    }

    #[derive(Debug)]
    struct FixtureSettlementGuard(Arc<AtomicUsize>);

    impl shard_stream_core::PhysicalWriteGuard for FixtureSettlementGuard {}

    impl Drop for FixtureSettlementGuard {
        fn drop(&mut self) {
            assert_eq!(self.0.fetch_sub(1, Ordering::AcqRel), 1);
        }
    }

    #[derive(Debug, Default)]
    struct ProposalPause {
        entered: AtomicBool,
        released: Mutex<bool>,
        changed: std::sync::Condvar,
    }

    struct ReleaseProposal(Arc<ProposalPause>);

    impl Drop for ReleaseProposal {
        fn drop(&mut self) {
            *self.0.released.lock().expect("proposal release") = true;
            self.0.changed.notify_all();
        }
    }

    impl FixturePendingAdmission {
        fn acquire_guard(
            &self,
        ) -> Result<
            Box<dyn shard_stream_core::PhysicalWriteGuard>,
            shard_stream_core::WriteFenceError,
        > {
            self.active
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                    (active == 0).then_some(1)
                })
                .map_err(|_| {
                    shard_stream_core::WriteFenceError::new("bounded fixture guard capacity")
                })?;
            self.physical_admissions.fetch_add(1, Ordering::Relaxed);
            Ok(Box::new(FixtureSettlementGuard(Arc::clone(&self.active))))
        }
    }

    impl PendingWriteAdmission for FixturePendingAdmission {
        fn validate_before_reserve(&self) -> Result<(), shard_stream_core::WriteFenceError> {
            if self.panic_before_reserve.swap(false, Ordering::AcqRel) {
                panic!("bounded pre-reservation fixture panic");
            }
            if self.refuse_before_reserve.load(Ordering::Acquire) {
                return Err(shard_stream_core::WriteFenceError::new(
                    "bounded fixture early refusal",
                ));
            }
            Ok(())
        }

        fn try_admit_proposal(
            &self,
            proposal: &shard_stream_core::ReservationProposal,
        ) -> Result<
            Box<dyn shard_stream_core::PhysicalWriteGuard>,
            shard_stream_core::WriteFenceError,
        > {
            if proposal.topic_partition()
                != TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0))
                || proposal.placement().leader_epoch != LeaderEpoch::new(7)
                || proposal.placement().ring_epoch
                    != RingEpoch::new(self.expected_ring.load(Ordering::Acquire).max(1))
                || self.refuse_physical.load(Ordering::Acquire)
            {
                return Err(shard_stream_core::WriteFenceError::new(
                    "bounded fixture proposal refusal",
                ));
            }
            let guard = self.acquire_guard()?;
            // A finite test barrier on the real worker; it is not a production
            // authority, metadata transport or a bounded-callback qualification.
            let pause = self.proposal_pause.lock().expect("proposal pause").clone();
            if let Some(pause) = pause {
                pause.entered.store(true, Ordering::Release);
                let mut released = pause.released.lock().expect("proposal released");
                while !*released {
                    released = pause.changed.wait(released).expect("proposal barrier");
                }
            }
            Ok(guard)
        }

        fn try_admit_physical(
            &self,
            reservation: &Reservation,
        ) -> Result<
            Box<dyn shard_stream_core::PhysicalWriteGuard>,
            shard_stream_core::WriteFenceError,
        > {
            if reservation.topic_id != TopicId::new(1)
                || reservation.partition_id != LogicalPartitionId::new(0)
                || reservation.placement.leader_epoch != LeaderEpoch::new(7)
                || reservation.placement.ring_epoch != RingEpoch::new(1)
                || self.refuse_physical.load(Ordering::Acquire)
            {
                return Err(shard_stream_core::WriteFenceError::new(
                    "bounded fixture physical refusal",
                ));
            }
            self.acquire_guard()
        }
    }

    #[derive(Debug, Default)]
    struct TicketBudgetFence {
        used: Arc<AtomicUsize>,
        prepares: AtomicUsize,
    }

    #[derive(Debug)]
    struct TestTicketCharge(Arc<AtomicUsize>);

    impl Drop for TestTicketCharge {
        fn drop(&mut self) {
            assert_eq!(self.0.fetch_sub(1, Ordering::AcqRel), 1);
        }
    }

    #[derive(Debug)]
    struct BudgetTicket {
        charge: Arc<TestTicketCharge>,
        admitted: AtomicBool,
    }

    #[derive(Debug)]
    struct BudgetGuard {
        _charge: Arc<TestTicketCharge>,
    }
    impl shard_stream_core::PhysicalWriteGuard for BudgetGuard {}

    impl PendingWriteAdmission for BudgetTicket {
        fn validate_before_reserve(&self) -> Result<(), shard_stream_core::WriteFenceError> {
            if self.admitted.load(Ordering::Acquire) {
                return Err(shard_stream_core::WriteFenceError::new(
                    "fixture ticket is single-use",
                ));
            }
            Ok(())
        }
        fn try_admit_proposal(
            &self,
            proposal: &shard_stream_core::ReservationProposal,
        ) -> Result<
            Box<dyn shard_stream_core::PhysicalWriteGuard>,
            shard_stream_core::WriteFenceError,
        > {
            ExactEpochFence.check_write(
                proposal.topic_partition(),
                proposal.placement().leader_epoch.get(),
            )?;
            if proposal.placement().ring_epoch != RingEpoch::new(1) {
                return Err(shard_stream_core::WriteFenceError::new(
                    "fixture ring changed",
                ));
            }
            self.admitted
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| {
                    shard_stream_core::WriteFenceError::new("fixture ticket is single-use")
                })?;
            Ok(Box::new(BudgetGuard {
                _charge: self.charge.clone(),
            }))
        }
        fn try_admit_physical(
            &self,
            _reservation: &Reservation,
        ) -> Result<
            Box<dyn shard_stream_core::PhysicalWriteGuard>,
            shard_stream_core::WriteFenceError,
        > {
            Err(shard_stream_core::WriteFenceError::new(
                "fixture incoming replica authority is unavailable",
            ))
        }
    }

    impl WriteFence for TicketBudgetFence {
        fn check_write(
            &self,
            partition: TopicPartition,
            epoch: u64,
        ) -> Result<(), shard_stream_core::WriteFenceError> {
            ExactEpochFence.check_write(partition, epoch)
        }
        fn requires_physical_admission(&self) -> bool {
            true
        }
        fn prepare_write(
            &self,
            partition: TopicPartition,
            epoch: u64,
        ) -> Result<Option<Arc<dyn PendingWriteAdmission>>, shard_stream_core::WriteFenceError>
        {
            self.check_write(partition, epoch)?;
            self.prepares.fetch_add(1, Ordering::Relaxed);
            self.used
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| {
                    shard_stream_core::WriteFenceError::new("fixture ticket capacity is occupied")
                })?;
            Ok(Some(Arc::new(BudgetTicket {
                charge: Arc::new(TestTicketCharge(self.used.clone())),
                admitted: AtomicBool::new(false),
            })))
        }
    }

    #[derive(Debug)]
    struct FixturePendingFence {
        admission: Arc<FixturePendingAdmission>,
        declared: bool,
    }

    impl WriteFence for FixturePendingFence {
        fn check_write(
            &self,
            partition: TopicPartition,
            epoch: u64,
        ) -> Result<(), shard_stream_core::WriteFenceError> {
            ExactEpochFence.check_write(partition, epoch)
        }

        fn requires_physical_admission(&self) -> bool {
            self.declared
        }

        fn prepare_write(
            &self,
            partition: TopicPartition,
            epoch: u64,
        ) -> Result<Option<Arc<dyn PendingWriteAdmission>>, shard_stream_core::WriteFenceError>
        {
            self.check_write(partition, epoch)?;
            Ok(Some(self.admission.clone()))
        }
    }

    #[derive(Debug)]
    struct FixtureReplicaContextFence {
        source: ReplicaSourceIdentity,
        pending: FixturePendingFence,
    }

    impl WriteFence for FixtureReplicaContextFence {
        fn check_write(
            &self,
            _partition: TopicPartition,
            _epoch: u64,
        ) -> Result<(), shard_stream_core::WriteFenceError> {
            Err(shard_stream_core::WriteFenceError::new(
                "fixture follower has no local leader permission",
            ))
        }

        fn requires_physical_admission(&self) -> bool {
            true
        }

        fn check_replica_write(
            &self,
            partition: TopicPartition,
            epoch: u64,
            source: &ReplicaSourceIdentity,
        ) -> Result<(), shard_stream_core::WriteFenceError> {
            if source != &self.source {
                return Err(shard_stream_core::WriteFenceError::new(
                    "fixture context mismatch",
                ));
            }
            ExactEpochFence.check_write(partition, epoch)
        }

        fn prepare_replica_write(
            &self,
            partition: TopicPartition,
            epoch: u64,
            source: &ReplicaSourceIdentity,
        ) -> Result<Option<Arc<dyn PendingWriteAdmission>>, shard_stream_core::WriteFenceError>
        {
            self.check_replica_write(partition, epoch, source)?;
            self.pending.prepare_write(partition, epoch)
        }
    }

    #[derive(Debug)]
    struct FixtureUnboundReplicaFence(FixturePendingFence);

    impl WriteFence for FixtureUnboundReplicaFence {
        fn check_write(
            &self,
            partition: TopicPartition,
            epoch: u64,
        ) -> Result<(), shard_stream_core::WriteFenceError> {
            self.0.check_write(partition, epoch)
        }

        fn requires_physical_admission(&self) -> bool {
            true
        }

        fn prepare_write(
            &self,
            partition: TopicPartition,
            epoch: u64,
        ) -> Result<Option<Arc<dyn PendingWriteAdmission>>, shard_stream_core::WriteFenceError>
        {
            self.0.prepare_write(partition, epoch)
        }

        fn prepare_unbound_replica_write(
            &self,
            _partition: TopicPartition,
            _epoch: u64,
        ) -> Result<Option<Arc<dyn PendingWriteAdmission>>, shard_stream_core::WriteFenceError>
        {
            Err(shard_stream_core::WriteFenceError::new(
                "fixture replica requires authenticated sender context",
            ))
        }
    }

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "shard-stream-engine-{name}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("temp dir");
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn config(path: &std::path::Path) -> EngineConfig {
        EngineConfig {
            data_dir: path.to_path_buf(),
            object_store_dir: None,
            shard_count: 3,
            virtual_lane_count: 3,
            replication_factor: 1,
            min_in_sync_replicas: 1,
            queue_slots_per_shard: 64,
            queue_bytes_per_shard: 2 * 1024 * 1024,
            target_pack_bytes: 1024,
            max_pack_age: std::time::Duration::from_secs(1),
            max_batch_bytes: 64 * 1024,
            max_fetch_bytes: 1024 * 1024,
            append_linger: std::time::Duration::from_millis(1),
        }
    }

    fn append_request(
        request_id: u128,
        payload: &[u8],
        producer: Option<ProducerIdentity>,
    ) -> AppendRequest {
        AppendRequest {
            request_id,
            topic_id: TopicId::new(1),
            partition_id: LogicalPartitionId::new(0),
            record_count: 1,
            payload: bytes::Bytes::copy_from_slice(payload),
            durability: Durability::Leader,
            producer,
            atomic_group: None,
            leader_epoch: None,
            extension_context: None,
        }
    }

    fn fetch_request(start_offset: u64) -> FetchRequest {
        FetchRequest {
            request_id: 1,
            topic_id: TopicId::new(1),
            partition_id: LogicalPartitionId::new(0),
            start_offset: LogicalOffset::new(start_offset),
            max_bytes: 1024,
            mode: FetchMode::Ordered,
        }
    }

    fn atomic_request(
        request_id: u128,
        transaction_id: u128,
        chunk_sequence: u64,
        payload: &'static [u8],
    ) -> AppendRequest {
        let mut request = append_request(request_id, payload, None);
        request.atomic_group = Some(AtomicGroupIdentity {
            transaction_id,
            chunk_sequence,
        });
        request
    }

    #[test]
    fn partition_affine_topics_keep_each_partition_on_one_distributed_lane() {
        let temp = TempDir::new("partition-affine-topic");
        let engine = StreamEngine::open(config(&temp.0)).expect("open engine");
        engine
            .create_partition_affine_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 6,
                shards: None,
            })
            .expect("create partition-affine topic");

        for partition_id in 0..6 {
            for append_index in 0..2 {
                let mut request = append_request(
                    u128::from(partition_id * 2 + append_index + 1),
                    b"record",
                    None,
                );
                request.partition_id = LogicalPartitionId::new(partition_id);
                let appended = engine.append(request).expect("append");
                assert_eq!(
                    appended.placement.virtual_lane_id,
                    ShardId::new(partition_id % 3)
                );
            }
        }
        drop(engine);

        let recovered = StreamEngine::open(config(&temp.0)).expect("recover engine");
        recovered
            .create_partition_affine_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 6,
                shards: None,
            })
            .expect("idempotently recover partition-affine topic");
        let mut request = append_request(100, b"recovered", None);
        request.partition_id = LogicalPartitionId::new(4);
        let appended = recovered.append(request).expect("append after recovery");
        assert_eq!(appended.placement.virtual_lane_id, ShardId::new(1));
    }

    #[test]
    fn durable_sink_validates_orders_maps_physical_shards_and_retries_failures() {
        let temp = TempDir::new("durable-sink-live");
        let control = Arc::new(RecordingSinkControl {
            failures_remaining: AtomicUsize::new(1),
            panic_once: AtomicBool::new(true),
            ..RecordingSinkControl::default()
        });
        let mut engine_config = config(&temp.0);
        engine_config.shard_count = 1;
        engine_config.virtual_lane_count = 4;
        let engine = StreamEngine::open_with_durable_sink(
            engine_config,
            recording_sink_config(&control, DurableSinkOptions::default()),
        )
        .expect("open with sink");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("create topic");

        engine
            .append(append_request(1, b"first", None))
            .expect("sink failure cannot reverse durable append");
        engine
            .append(append_request(2, b"second", None))
            .expect("second append");
        assert!(matches!(
            engine.append(append_request(3, b"reject", None)),
            Err(EngineError::InvalidConfig(_))
        ));

        wait_for_test(|| {
            control
                .state
                .lock()
                .expect("state")
                .checkpoints
                .values()
                .any(|checkpoint| checkpoint.next_offset == LogicalOffset::new(2))
        });
        let state = control.state.lock().expect("state");
        assert_eq!(
            state
                .appends
                .iter()
                .map(|append| append.payload.as_ref())
                .collect::<Vec<_>>(),
            vec![b"first".as_slice(), b"second".as_slice()]
        );
        assert!(
            state
                .appends
                .iter()
                .all(|append| append.physical_shard_id == ShardId::new(0))
        );
        assert_eq!(
            state
                .appends
                .iter()
                .map(|append| append.reservation.placement.sequence.get())
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        drop(state);
        let stats = engine.durable_sink_stats();
        assert!(stats.failed_attempts >= 2);
        assert!(stats.retry_attempts >= 1);
        assert_eq!(engine.fetch(fetch_request(0)).expect("fetch").len(), 2);
    }

    #[test]
    fn transient_sink_context_is_forwarded_bounded_and_not_authoritative() {
        let temp = TempDir::new("durable-sink-transient-context");
        let control = Arc::new(RecordingSinkControl {
            block: AtomicBool::new(true),
            ..RecordingSinkControl::default()
        });
        let engine = StreamEngine::open_with_durable_sink(
            config(&temp.0),
            recording_sink_config(&control, DurableSinkOptions::default()),
        )
        .expect("open with sink");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("create topic");

        let payload = Bytes::from_static(b"stored");
        let context = Bytes::from_static(b"already-decoded");
        engine
            .append_with_durable_sink_context(append_request(1, &payload, None), context.clone())
            .expect("authoritative append is durable");
        wait_for_test(|| control.entered.load(Ordering::Acquire));
        assert_eq!(
            engine.durable_sink_stats().pending_bytes,
            (payload.len() + context.len()) as u64
        );

        control.block.store(false, Ordering::Release);
        wait_for_test(|| engine.durable_sink_stats().pending_items == 0);
        let state = control.state.lock().expect("state");
        assert_eq!(state.appends.len(), 1);
        assert_eq!(state.appends[0].payload, payload);
        assert_eq!(
            state.appends[0].transient_context.as_deref(),
            Some(context.as_ref())
        );
        drop(state);

        let fetched = engine
            .fetch(fetch_request(0))
            .expect("stored payload fetches");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].payload, payload);
    }

    #[test]
    fn durable_sink_replays_wal_written_before_sink_installation() {
        let temp = TempDir::new("durable-sink-replay");
        let engine_config = config(&temp.0);
        {
            let engine = StreamEngine::open(engine_config.clone()).expect("open without sink");
            engine
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(1),
                    partitions: 1,
                    shards: None,
                })
                .expect("create topic");
            engine
                .append(append_request(1, b"before-install", None))
                .expect("append");
        }

        let control = Arc::new(RecordingSinkControl::default());
        let engine = StreamEngine::open_with_durable_sink(
            engine_config,
            recording_sink_config(&control, DurableSinkOptions::default()),
        )
        .expect("reopen with sink");
        let checkpoint = engine
            .durable_sink_checkpoint(TopicPartition::new(
                TopicId::new(1),
                LogicalPartitionId::new(0),
            ))
            .expect("checkpoint")
            .expect("sink installed");
        assert_eq!(checkpoint.next_offset, LogicalOffset::new(1));
        let state = control.state.lock().expect("state");
        assert_eq!(state.appends.len(), 1);
        assert_eq!(state.appends[0].payload.as_ref(), b"before-install");
        assert!(state.appends[0].transient_context.is_none());
    }

    #[test]
    fn promoted_replica_replays_missing_cluster_sink_event() {
        let temp = TempDir::new("durable-sink-promotion");
        let follower_path = temp.0.join("follower");
        let leader_path = temp.0.join("leader");
        let mut follower_config = config(&follower_path);
        follower_config.replication_factor = 1;
        let follower = Arc::new(StreamEngine::open(follower_config.clone()).expect("follower"));
        follower
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("follower topic");

        let mut leader_config = config(&leader_path);
        leader_config.replication_factor = 2;
        leader_config.min_in_sync_replicas = 2;
        let transport: Arc<dyn ReplicationTransport> = Arc::new(DirectReplication {
            follower: Arc::clone(&follower),
            fail_first: AtomicBool::new(false),
            progress: OnceLock::new(),
        });
        {
            let leader =
                StreamEngine::open_with_replication(leader_config, transport).expect("leader");
            leader
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(1),
                    partitions: 1,
                    shards: None,
                })
                .expect("leader topic");
            let mut request = append_request(1, b"survives-failover", None);
            request.durability = Durability::AllReplicas;
            leader.append(request).expect("replicated append");
        }
        drop(follower);

        let control = Arc::new(RecordingSinkControl::default());
        let promoted = StreamEngine::open_with_durable_sink(
            follower_config,
            recording_sink_config(&control, DurableSinkOptions::default()),
        )
        .expect("promoted follower");
        assert_eq!(promoted.durable_sink_stats().pending_items, 0);
        let state = control.state.lock().expect("state");
        assert_eq!(state.appends.len(), 1);
        assert_eq!(state.appends[0].payload.as_ref(), b"survives-failover");
    }

    #[test]
    fn committed_only_sink_stages_then_finalizes_atomic_groups() {
        let temp = TempDir::new("durable-sink-atomic");
        let control = Arc::new(RecordingSinkControl::default());
        let options = DurableSinkOptions {
            atomic_visibility: crate::DurableSinkAtomicVisibility::CommittedOnly,
            ..DurableSinkOptions::default()
        };
        let engine = StreamEngine::open_with_durable_sink(
            config(&temp.0),
            recording_sink_config(&control, options),
        )
        .expect("open");
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("topic");
        engine
            .append(atomic_request(1, 91, 0, b"atomic"))
            .expect("atomic append");
        wait_for_test(|| !control.state.lock().expect("state").staged.is_empty());
        assert_eq!(
            control.state.lock().expect("state").appends[0].delivery,
            DurableAppendDelivery::Stage
        );
        engine
            .commit_atomic_group(partition, 91)
            .expect("commit group");
        wait_for_test(|| {
            control
                .state
                .lock()
                .expect("state")
                .decisions
                .iter()
                .any(|decision| decision.outcome == AtomicGroupState::Committed)
        });
        assert!(control.state.lock().expect("state").staged.is_empty());
    }

    #[test]
    fn durable_sink_checkpoint_pins_retention_until_delivery() {
        let temp = TempDir::new("durable-sink-retention");
        let control = Arc::new(RecordingSinkControl {
            block: AtomicBool::new(true),
            ..RecordingSinkControl::default()
        });
        let options = DurableSinkOptions {
            recovery_timeout: Duration::from_millis(100),
            ..DurableSinkOptions::default()
        };
        let engine = StreamEngine::open_with_durable_sink(
            config(&temp.0),
            recording_sink_config(&control, options),
        )
        .expect("open");
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("topic");
        engine
            .append(append_request(1, b"pinned", None))
            .expect("append");
        wait_for_test(|| control.entered.load(Ordering::Acquire));
        assert!(matches!(
            engine.truncate_partition(partition, LogicalOffset::new(1)),
            Err(EngineError::RetentionPinned { .. })
        ));
        control.block.store(false, Ordering::Release);
        wait_for_test(|| {
            control
                .state
                .lock()
                .expect("state")
                .checkpoints
                .get(&partition)
                .is_some_and(|checkpoint| checkpoint.next_offset == LogicalOffset::new(1))
        });
        engine
            .truncate_partition(partition, LogicalOffset::new(1))
            .expect("retention advances after checkpoint");
    }

    #[test]
    fn durable_sink_retries_nonadvancing_conflicts_and_accepts_external_commit() {
        let temp = TempDir::new("durable-sink-conflicts");
        let control = Arc::new(RecordingSinkControl {
            stale_conflicts_remaining: AtomicUsize::new(1),
            advancing_conflict_once: AtomicBool::new(true),
            ..RecordingSinkControl::default()
        });
        let engine = StreamEngine::open_with_durable_sink(
            config(&temp.0),
            recording_sink_config(&control, DurableSinkOptions::default()),
        )
        .expect("open");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("topic");
        engine
            .append(append_request(1, b"one-logical-event", None))
            .expect("append");

        wait_for_test(|| {
            control
                .state
                .lock()
                .expect("state")
                .checkpoints
                .values()
                .any(|checkpoint| checkpoint.next_offset == LogicalOffset::new(1))
        });
        assert_eq!(control.state.lock().expect("state").appends.len(), 1);
        let stats = engine.durable_sink_stats();
        assert_eq!(stats.checkpoint_conflicts, 1);
        assert!(stats.failed_attempts >= 1);
    }

    #[test]
    fn durable_sink_retry_backoff_does_not_stall_healthy_partitions() {
        let temp = TempDir::new("durable-sink-retry-isolation");
        let failed_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let healthy_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(1));
        let control = Arc::new(RecordingSinkControl {
            failed_partition: Mutex::new(Some(failed_partition)),
            ..RecordingSinkControl::default()
        });
        let options = DurableSinkOptions {
            worker_count: 1,
            ..DurableSinkOptions::default()
        };
        let engine = StreamEngine::open_with_durable_sink(
            config(&temp.0),
            recording_sink_config(&control, options),
        )
        .expect("open");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 2,
                shards: None,
            })
            .expect("topic");
        engine
            .append(append_request(1, b"permanently-failing", None))
            .expect("durable failed-partition append");
        wait_for_test(|| engine.durable_sink_stats().retry_attempts >= 6);

        let mut healthy = append_request(2, b"healthy", None);
        healthy.partition_id = LogicalPartitionId::new(1);
        let started = Instant::now();
        engine.append(healthy).expect("durable healthy append");
        wait_for_test(|| {
            control
                .state
                .lock()
                .expect("state")
                .checkpoints
                .get(&healthy_partition)
                .is_some_and(|checkpoint| checkpoint.next_offset == LogicalOffset::new(1))
        });
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "failed partition retry backoff stalled a healthy partition"
        );
    }

    #[test]
    fn availability_mode_drops_only_notification_and_rescans_retained_wal() {
        let temp = TempDir::new("durable-sink-availability");
        let control = Arc::new(RecordingSinkControl {
            block: AtomicBool::new(true),
            ..RecordingSinkControl::default()
        });
        let options = DurableSinkOptions {
            lag_policy: crate::DurableSinkLagPolicy::Availability,
            max_pending_items: 1,
            ..DurableSinkOptions::default()
        };
        let engine = StreamEngine::open_with_durable_sink(
            config(&temp.0),
            recording_sink_config(&control, options),
        )
        .expect("open");
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("topic");
        engine
            .append(append_request(1, b"queued", None))
            .expect("first append");
        wait_for_test(|| control.entered.load(Ordering::Acquire));
        engine
            .append(append_request(2, b"rescan", None))
            .expect("availability append");
        assert_eq!(engine.durable_sink_stats().dirty_partitions, 1);
        assert_eq!(engine.durable_sink_stats().dropped_notifications, 1);

        control.block.store(false, Ordering::Release);
        engine
            .resume_durable_sink(&[partition])
            .expect("rescan retained WAL");
        let state = control.state.lock().expect("state");
        assert_eq!(
            state
                .appends
                .iter()
                .map(|append| append.payload.as_ref())
                .collect::<Vec<_>>(),
            vec![b"queued".as_slice(), b"rescan".as_slice()]
        );
        drop(state);
        assert_eq!(engine.durable_sink_stats().dirty_partitions, 0);
    }

    #[test]
    fn backpressure_happens_before_reserving_an_offset() {
        let temp = TempDir::new("durable-sink-backpressure");
        let control = Arc::new(RecordingSinkControl {
            block: AtomicBool::new(true),
            ..RecordingSinkControl::default()
        });
        let options = DurableSinkOptions {
            max_pending_items: 1,
            ..DurableSinkOptions::default()
        };
        let engine = Arc::new(
            StreamEngine::open_with_durable_sink(
                config(&temp.0),
                recording_sink_config(&control, options),
            )
            .expect("open"),
        );
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("topic");
        engine
            .append(append_request(1, b"blocked-sink", None))
            .expect("first append");
        wait_for_test(|| control.entered.load(Ordering::Acquire));

        let second_engine = Arc::clone(&engine);
        let second =
            std::thread::spawn(move || second_engine.append(append_request(2, b"later", None)));
        std::thread::sleep(Duration::from_millis(30));
        assert_eq!(
            engine
                .watermarks(partition)
                .expect("watermarks")
                .allocated_end,
            LogicalOffset::new(1)
        );
        control.block.store(false, Ordering::Release);
        second
            .join()
            .expect("append thread")
            .expect("second append");
        wait_for_test(|| {
            control
                .state
                .lock()
                .expect("state")
                .checkpoints
                .get(&partition)
                .is_some_and(|checkpoint| checkpoint.next_offset == LogicalOffset::new(2))
        });
    }

    #[test]
    fn durable_sink_shutdown_is_bounded_when_callback_hangs() {
        let temp = TempDir::new("durable-sink-shutdown");
        let control = Arc::new(RecordingSinkControl {
            block: AtomicBool::new(true),
            ..RecordingSinkControl::default()
        });
        let engine = StreamEngine::open_with_durable_sink(
            config(&temp.0),
            recording_sink_config(&control, DurableSinkOptions::default()),
        )
        .expect("open");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("topic");
        engine
            .append(append_request(1, b"unfinished", None))
            .expect("append");
        wait_for_test(|| control.entered.load(Ordering::Acquire));

        let started = Instant::now();
        drop(engine);
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "engine destruction waited for sink callback"
        );
        control.block.store(false, Ordering::Release);
    }

    #[test]
    fn durable_sink_sequences_concurrent_cross_shard_completions() {
        let temp = TempDir::new("durable-sink-cross-shard-order");
        let control = Arc::new(RecordingSinkControl::default());
        let engine = Arc::new(
            StreamEngine::open_with_durable_sink(
                config(&temp.0),
                recording_sink_config(&control, DurableSinkOptions::default()),
            )
            .expect("open"),
        );
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("topic");

        std::thread::scope(|scope| {
            for worker in 0..8_u128 {
                let engine = Arc::clone(&engine);
                scope.spawn(move || {
                    for sequence in 0..16_u128 {
                        let request_id = worker * 16 + sequence;
                        engine
                            .append(append_request(request_id, &request_id.to_le_bytes(), None))
                            .expect("concurrent append");
                    }
                });
            }
        });
        engine
            .resume_durable_sink(&[partition])
            .expect("wait for ordered sink");

        let state = control.state.lock().expect("state");
        assert_eq!(state.appends.len(), 128);
        assert_eq!(
            state
                .appends
                .iter()
                .map(|append| append.reservation.placement.sequence.get())
                .collect::<Vec<_>>(),
            (0..128).collect::<Vec<_>>()
        );
        assert_eq!(
            state
                .appends
                .iter()
                .map(|append| append.event_id)
                .collect::<HashSet<_>>()
                .len(),
            128
        );
    }

    #[test]
    fn appends_round_robin_and_fetches_in_logical_order() {
        let temp = TempDir::new("round-robin");
        let engine = StreamEngine::open(config(&temp.0)).expect("open");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("create topic");

        let responses = ["zero", "one", "two", "three"].map(|payload| {
            engine
                .append(append_request(1, payload.as_bytes(), None))
                .expect("append")
        });
        assert_eq!(
            responses.map(|response| response.placement.virtual_lane_id.get()),
            [0, 1, 2, 0]
        );
        let fetched = engine.fetch(fetch_request(0)).expect("fetch");
        assert_eq!(
            fetched
                .iter()
                .map(|batch| batch.payload.as_ref())
                .collect::<Vec<_>>(),
            vec![
                b"zero".as_slice(),
                b"one".as_slice(),
                b"two".as_slice(),
                b"three".as_slice()
            ]
        );
        assert_eq!(
            engine
                .watermarks(TopicPartition::new(
                    TopicId::new(1),
                    LogicalPartitionId::new(0)
                ))
                .expect("watermarks")
                .contiguous_log_end,
            LogicalOffset::new(4)
        );
    }

    #[test]
    fn rf_above_one_requires_an_extension_transport() {
        let temp = TempDir::new("rf-extension-required");
        let mut engine_config = config(&temp.0);
        engine_config.replication_factor = 2;
        engine_config.min_in_sync_replicas = 2;
        assert!(matches!(
            StreamEngine::open(engine_config),
            Err(EngineError::InvalidConfig(message))
                if message.contains("requires an injected replication transport")
        ));
    }

    #[test]
    fn atomic_group_holds_last_stable_offset_until_commit() {
        let temp = TempDir::new("atomic-commit");
        let mut engine_config = config(&temp.0);
        engine_config.replication_factor = 1;
        engine_config.min_in_sync_replicas = 1;
        let engine = StreamEngine::open(engine_config).expect("open");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("create topic");
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));

        engine
            .append(append_request(1, b"before", None))
            .expect("ordinary append");
        let first = engine
            .append(atomic_request(2, 77, 0, b"group-0"))
            .expect("first chunk");
        let retry = engine
            .append(atomic_request(3, 77, 0, b"group-0"))
            .expect("idempotent chunk retry");
        assert_eq!(retry.batch_id, first.batch_id);
        assert!(matches!(
            engine.append(atomic_request(4, 77, 0, b"changed")),
            Err(EngineError::AtomicGroupChunkMismatch(77))
        ));
        engine
            .append(atomic_request(5, 77, 1, b"group-1"))
            .expect("second chunk");
        engine
            .append(append_request(6, b"after", None))
            .expect("later ordinary append");

        let watermarks = engine.watermarks(partition).expect("watermarks");
        assert_eq!(watermarks.contiguous_log_end, LogicalOffset::new(4));
        assert_eq!(watermarks.last_stable_offset, LogicalOffset::new(1));
        assert_eq!(
            engine
                .fetch(fetch_request(0))
                .expect("stable fetch")
                .iter()
                .map(|batch| batch.payload.as_ref())
                .collect::<Vec<_>>(),
            vec![b"before".as_slice()]
        );

        let committed = engine.commit_atomic_group(partition, 77).expect("commit");
        assert_eq!(committed.state, AtomicGroupState::Committed);
        assert_eq!(committed.chunk_count, 2);
        assert_eq!(
            engine
                .fetch(fetch_request(0))
                .expect("committed fetch")
                .iter()
                .map(|batch| batch.payload.as_ref())
                .collect::<Vec<_>>(),
            vec![
                b"before".as_slice(),
                b"group-0".as_slice(),
                b"group-1".as_slice(),
                b"after".as_slice()
            ]
        );
        assert_eq!(
            engine
                .commit_atomic_group(partition, 77)
                .expect("idempotent commit"),
            committed
        );
    }

    #[test]
    fn atomic_group_commit_waits_for_minimum_isr_durability() {
        let temp = TempDir::new("atomic-commit-min-isr");
        let release = Arc::new(AtomicBool::new(false));
        let transport: Arc<dyn ReplicationTransport> = Arc::new(DeferredReplication {
            progress: OnceLock::new(),
            release: Arc::clone(&release),
        });
        let mut engine_config = config(&temp.0);
        engine_config.replication_factor = 3;
        engine_config.min_in_sync_replicas = 2;
        let engine = StreamEngine::open_with_replication(engine_config, transport).expect("open");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("create topic");
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));

        engine
            .append(atomic_request(1, 78, 0, b"group"))
            .expect("leader-durable chunk");
        assert!(matches!(
            engine.commit_atomic_group(partition, 78),
            Err(EngineError::DurabilityUnavailable {
                required_replicas: 2,
                durable_replicas: 1,
            })
        ));

        release.store(true, Ordering::Release);
        let deadline = Instant::now() + Duration::from_secs(5);
        while engine
            .watermarks(partition)
            .expect("watermarks")
            .replicated_high_watermark
            < LogicalOffset::new(1)
        {
            assert!(
                Instant::now() < deadline,
                "replication did not catch up before the test deadline"
            );
            std::thread::yield_now();
        }
        assert_eq!(
            engine
                .commit_atomic_group(partition, 78)
                .expect("quorum-safe commit")
                .state,
            AtomicGroupState::Committed
        );
    }

    #[test]
    fn unresolved_group_survives_restart_and_abort_unblocks_visibility() {
        let temp = TempDir::new("atomic-recovery");
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        {
            let engine = StreamEngine::open(config(&temp.0)).expect("open");
            engine
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(1),
                    partitions: 1,
                    shards: None,
                })
                .expect("create topic");
            engine
                .append(atomic_request(1, 88, 0, b"ambiguous"))
                .expect("group chunk");
            engine
                .append(append_request(2, b"visible-after-abort", None))
                .expect("later append");
            assert!(
                engine
                    .fetch(fetch_request(0))
                    .expect("blocked fetch")
                    .is_empty()
            );
            assert!(matches!(
                engine.truncate_partition(partition, LogicalOffset::new(1)),
                Err(EngineError::InvalidConfig(_))
            ));
        }

        let engine = StreamEngine::open(config(&temp.0)).expect("reopen");
        let recovered = engine.inspect_atomic_group(partition, 88).expect("inspect");
        assert_eq!(recovered.state, AtomicGroupState::Unresolved);
        assert_eq!(
            engine
                .watermarks(partition)
                .expect("watermarks")
                .last_stable_offset,
            LogicalOffset::new(0)
        );
        engine.abort_atomic_group(partition, 88).expect("abort");
        let fetched = engine.fetch(fetch_request(0)).expect("fetch after abort");
        assert_eq!(fetched.len(), 1);
        assert_eq!(
            fetched[0].payload,
            Bytes::from_static(b"visible-after-abort")
        );
        drop(engine);

        let recovered = StreamEngine::open(config(&temp.0)).expect("reopen aborted");
        assert_eq!(
            recovered
                .inspect_atomic_group(partition, 88)
                .expect("inspect aborted")
                .state,
            AtomicGroupState::Aborted
        );
        assert_eq!(
            recovered.fetch(fetch_request(0)).expect("recovered fetch")[0].payload,
            Bytes::from_static(b"visible-after-abort")
        );
    }

    #[test]
    fn fetch_materializes_only_the_globally_selected_batches() {
        let temp = TempDir::new("global-fetch-budget");
        let mut engine_config = config(&temp.0);
        engine_config.shard_count = 8;
        engine_config.replication_factor = 1;
        engine_config.min_in_sync_replicas = 1;
        engine_config.target_pack_bytes = 8 * 1024 * 1024;
        let engine = StreamEngine::open(engine_config).expect("open");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("create topic");

        let payload = vec![7u8; 32 * 1024];
        for request_id in 0..64u128 {
            engine
                .append(append_request(request_id, &payload, None))
                .expect("append");
        }

        let fetched = engine
            .fetch(FetchRequest {
                request_id: 1,
                topic_id: TopicId::new(1),
                partition_id: LogicalPartitionId::new(0),
                start_offset: LogicalOffset::new(0),
                max_bytes: 1024 * 1024,
                mode: FetchMode::Ordered,
            })
            .expect("first fetch");
        assert_eq!(fetched.len(), 32);
        assert_eq!(
            fetched
                .iter()
                .map(|batch| batch.batch_id.get())
                .collect::<Vec<_>>(),
            (0..32u128).collect::<Vec<_>>()
        );

        let fetched = engine
            .fetch(FetchRequest {
                request_id: 2,
                topic_id: TopicId::new(1),
                partition_id: LogicalPartitionId::new(0),
                start_offset: LogicalOffset::new(32),
                max_bytes: 1024 * 1024,
                mode: FetchMode::Ordered,
            })
            .expect("second fetch");
        assert_eq!(fetched.len(), 32);
        assert_eq!(fetched[0].batch_id, BatchId::new(32));
        assert_eq!(fetched[31].batch_id, BatchId::new(63));
    }

    #[test]
    fn zero_length_batch_does_not_make_a_later_oversized_batch_free() {
        let temp = TempDir::new("zero-length-budget");
        let mut engine_config = config(&temp.0);
        engine_config.shard_count = 1;
        engine_config.replication_factor = 1;
        engine_config.min_in_sync_replicas = 1;
        let engine = StreamEngine::open(engine_config).expect("open");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("create topic");
        engine
            .append(append_request(0, b"", None))
            .expect("append empty batch");
        engine
            .append(append_request(1, &vec![1; 2 * 1024], None))
            .expect("append oversized-for-fetch batch");

        let fetched = engine.fetch(fetch_request(0)).expect("fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].batch_id, BatchId::new(0));
    }

    #[test]
    fn installed_write_fence_requires_and_validates_leader_epoch() {
        let temp = TempDir::new("write-fence");
        let engine = StreamEngine::open_with_fence(config(&temp.0), Arc::new(ExactEpochFence))
            .expect("open");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("create topic");

        let mut request = append_request(1, b"fenced", None);
        assert!(matches!(
            engine.append(request.clone()),
            Err(EngineError::Fenced(_))
        ));
        request.leader_epoch = Some(6);
        assert!(matches!(
            engine.append(request.clone()),
            Err(EngineError::Fenced(_))
        ));
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        assert_eq!(
            engine
                .watermarks(partition)
                .expect("refused frontier")
                .allocated_end,
            LogicalOffset::new(0)
        );
        assert!(
            engine
                .fetch(fetch_request(0))
                .expect("refused WAL")
                .is_empty()
        );
        request.leader_epoch = Some(7);
        let receipt = engine.append(request).expect("current epoch");
        assert_eq!(receipt.placement.leader_epoch, LeaderEpoch::new(7));
    }

    #[test]
    fn admitted_fenced_epoch_reaches_real_wal_sink_and_replica_coordinates() {
        let leader_directory = TempDir::new("admitted-epoch-leader");
        let follower_directory = TempDir::new("admitted-epoch-follower");
        let follower = Arc::new(
            StreamEngine::open(config(&follower_directory.0)).expect("real follower engine"),
        );
        let topic = TopicConfig {
            topic_id: TopicId::new(1),
            partitions: 1,
            shards: None,
        };
        follower
            .create_topic(topic.clone())
            .expect("follower topic");
        let control = Arc::new(RecordingSinkControl::default());
        let mut leader_config = config(&leader_directory.0);
        leader_config.replication_factor = 2;
        leader_config.min_in_sync_replicas = 2;
        let leader = StreamEngine::open_with_all_components_and_durable_sink(
            leader_config,
            Some(Arc::new(ExactEpochFence)),
            Some(Arc::new(DirectReplication {
                follower: Arc::clone(&follower),
                fail_first: AtomicBool::new(false),
                progress: OnceLock::new(),
            })),
            None,
            Some(recording_sink_config(
                &control,
                DurableSinkOptions::default(),
            )),
        )
        .expect("real fenced leader and sink");
        leader.create_topic(topic).expect("leader topic");
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let mut request = append_request(1, b"admitted epoch reaches physical storage", None);
        request.leader_epoch = Some(7);
        request.durability = Durability::Quorum;
        let receipt = leader.append(request).expect("genuine engine append");
        assert_eq!(receipt.placement.leader_epoch, LeaderEpoch::new(7));
        leader
            .wait_for_durable_sink_checkpoint(DurableSinkCheckpoint {
                topic_partition: partition,
                next_placement_sequence: PlacementSequence::new(1),
                next_offset: LogicalOffset::new(1),
            })
            .expect("actual sink applied the reserved placement");
        for engine in [&leader, follower.as_ref()] {
            let wal = engine
                .fetch(fetch_request(0))
                .expect("checksummed WAL readback");
            assert_eq!(wal.len(), 1);
            assert_eq!(wal[0].placement, receipt.placement);
            assert_eq!(wal[0].first_offset, receipt.first_offset);
            assert_eq!(wal[0].last_offset, receipt.last_offset);
            assert_eq!(
                wal[0].payload,
                Bytes::from_static(b"admitted epoch reaches physical storage")
            );
        }
        let applied = control.state.lock().expect("actual recorded sink state");
        assert_eq!(applied.appends.len(), 1);
        assert_eq!(applied.appends[0].reservation.placement, receipt.placement);
        // This real second-WAL transport does not qualify an external host's
        // authenticated replica admission or an authority-transition guard.
    }

    #[test]
    fn admitted_epoch_rotation_preserves_exact_producer_retry_and_recovered_wal() {
        let directory = TempDir::new("admitted-epoch-recovery");
        let fence = Arc::new(FixtureEpochFence(AtomicU64::new(7)));
        let first_producer = ProducerIdentity {
            producer_id: 55,
            epoch: 0,
            first_sequence: 0,
            event_id: Some(RecordId::for_producer_event(
                TopicId::new(1),
                LogicalPartitionId::new(0),
                55,
                0,
                0,
            )),
        };
        let mut first_request = append_request(1, b"first exact identity", Some(first_producer));
        first_request.leader_epoch = Some(7);
        let first_receipt;
        let second_receipt;
        {
            let engine = StreamEngine::open_with_fence(config(&directory.0), fence.clone())
                .expect("real fenced engine");
            engine
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(1),
                    partitions: 1,
                    shards: None,
                })
                .expect("fresh topic without seeded epochs");
            first_receipt = engine
                .append(first_request.clone())
                .expect("first physical append");
            fence.0.store(8, Ordering::Release);
            first_request.leader_epoch = Some(8);
            assert_eq!(
                engine
                    .append(first_request.clone())
                    .expect("exact current-admission retry"),
                first_receipt
            );
            let mut second = append_request(
                2,
                b"second physical identity",
                Some(ProducerIdentity {
                    first_sequence: 1,
                    event_id: Some(RecordId::for_producer_event(
                        TopicId::new(1),
                        LogicalPartitionId::new(0),
                        55,
                        0,
                        1,
                    )),
                    ..first_producer
                }),
            );
            second.leader_epoch = Some(8);
            second_receipt = engine.append(second).expect("new epoch physical append");
            assert_eq!(first_receipt.placement.leader_epoch, LeaderEpoch::new(7));
            assert_eq!(second_receipt.placement.leader_epoch, LeaderEpoch::new(8));
        }
        let engine = StreamEngine::open_with_fence(config(&directory.0), fence.clone())
            .expect("reopen actual persisted epochs and producer receipts");
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let before = engine.watermarks(partition).expect("recovered frontiers");
        let recovered = engine.append(first_request).expect("recovered exact retry");
        // Visibility is a conservative live watermark, not persisted batch
        // identity. Recovery must preserve every durable receipt coordinate.
        assert_eq!(
            (
                recovered.request_id,
                recovered.batch_id,
                recovered.record_id,
                recovered.first_offset,
                recovered.last_offset,
                recovered.placement,
            ),
            (
                first_receipt.request_id,
                first_receipt.batch_id,
                first_receipt.record_id,
                first_receipt.first_offset,
                first_receipt.last_offset,
                first_receipt.placement,
            )
        );
        assert!(recovered.visible_through <= before.last_stable_offset);
        assert_eq!(
            engine.watermarks(partition).expect("retry frontiers"),
            before
        );
        let wal = engine
            .fetch(fetch_request(0))
            .expect("recovered checksummed WAL");
        assert_eq!(wal.len(), 2);
        assert_eq!(wal[0].placement, first_receipt.placement);
        assert_eq!(wal[1].placement, second_receipt.placement);
        // Even an installed admission hook that regresses cannot stamp a new
        // physical reservation below the epoch recovered from the real WAL.
        fence.0.store(7, Ordering::Release);
        fence
            .check_write(partition, 7)
            .expect("hook accepts the regressed fixture token");
        let mut stale = append_request(3, b"must not regress recovered placement", None);
        stale.leader_epoch = Some(7);
        assert!(matches!(
            engine.append(stale),
            Err(EngineError::Sequencer(SequencerError::StaleLeaderEpoch { current, proposed }))
                if current == LeaderEpoch::new(8) && proposed == LeaderEpoch::new(7)
        ));
        assert_eq!(
            engine.watermarks(partition).expect("refused frontiers"),
            before
        );
        assert_eq!(engine.fetch(fetch_request(0)).expect("unchanged WAL"), wal);
        fence.0.store(9, Ordering::Release);
        let mut next = append_request(4, b"later physical progress", None);
        next.leader_epoch = Some(9);
        let progressed = engine
            .append(next)
            .expect("newer admitted epoch after refusal");
        assert_eq!(progressed.first_offset, LogicalOffset::new(2));
        assert_eq!(progressed.placement.leader_epoch, LeaderEpoch::new(9));
        let recovered = engine
            .fetch(fetch_request(0))
            .expect("later checksummed WAL");
        assert_eq!(recovered.len(), 3);
        assert_eq!(&recovered[..2], wal.as_slice());
    }

    #[test]
    fn admitted_epoch_rotation_retains_exact_atomic_chunk_receipt() {
        let directory = TempDir::new("admitted-epoch-atomic-retry");
        let fence = Arc::new(FixtureEpochFence(AtomicU64::new(7)));
        let engine = StreamEngine::open_with_fence(config(&directory.0), fence.clone())
            .expect("real atomic fenced engine");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("fresh atomic topic");
        let mut first = atomic_request(1, 99, 0, b"first atomic chunk");
        first.leader_epoch = Some(7);
        let first_receipt = engine.append(first.clone()).expect("actual first chunk");
        fence.0.store(8, Ordering::Release);
        let mut second = atomic_request(2, 99, 1, b"second atomic chunk");
        second.leader_epoch = Some(8);
        let second_receipt = engine.append(second).expect("actual second chunk");
        first.leader_epoch = Some(8);
        assert_eq!(
            engine
                .append(first)
                .expect("old atomic identity under current admission"),
            first_receipt
        );
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        engine
            .commit_atomic_group(partition, 99)
            .expect("commit real atomic chunks");
        let wal = engine
            .fetch(fetch_request(0))
            .expect("committed actual WAL");
        assert_eq!(wal.len(), 2);
        assert_eq!(wal[0].placement, first_receipt.placement);
        assert_eq!(wal[1].placement, second_receipt.placement);
        assert_eq!(first_receipt.placement.leader_epoch, LeaderEpoch::new(7));
        assert_eq!(second_receipt.placement.leader_epoch, LeaderEpoch::new(8));
    }

    #[test]
    fn unfenced_supplied_epoch_keeps_existing_placement_behavior() {
        let directory = TempDir::new("unfenced-epoch-compatibility");
        let engine = StreamEngine::open(config(&directory.0)).expect("ordinary unfenced engine");
        let uninstalled: Arc<dyn WriteFence> = Arc::new(FixturePendingFence {
            admission: Arc::new(FixturePendingAdmission::default()),
            declared: true,
        });
        assert!(
            !engine.has_guarded_replica_fence(&uninstalled),
            "an unfenced engine cannot establish another instance's guarded installation"
        );
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("ordinary topic");
        let mut supplied = append_request(1, b"untrusted supplied epoch", None);
        supplied.leader_epoch = Some(91);
        let first = engine
            .append(supplied)
            .expect("existing unfenced supplied-token behavior");
        let second = engine
            .append(append_request(2, b"no epoch", None))
            .expect("unfenced None behavior");
        assert_eq!(first.placement.leader_epoch, LeaderEpoch::new(0));
        assert_eq!(second.placement.leader_epoch, LeaderEpoch::new(0));
        let wal = engine.fetch(fetch_request(0)).expect("ordinary actual WAL");
        assert_eq!(wal.len(), 2);
        assert_eq!(wal[0].placement, first.placement);
        assert_eq!(wal[1].placement, second.placement);
    }

    #[test]
    fn guarded_exact_capacity_is_validated_before_storage_and_preserves_legacy_bound() {
        let directory = TempDir::new("guarded-capacity");
        let ceiling = 64 * 1024;
        let mut limits = config(&directory.0.join("invalid"));
        limits.max_batch_bytes = ceiling;
        limits.max_fetch_bytes = ceiling;
        limits.queue_bytes_per_shard = ceiling + crate::config::COMMAND_OVERHEAD_BYTES;
        let admission = Arc::new(FixturePendingAdmission::default());
        let fence = Arc::new(FixturePendingFence {
            admission,
            declared: true,
        });
        assert!(matches!(
            StreamEngine::open_with_fence(limits.clone(), fence.clone()),
            Err(EngineError::InvalidConfig(_))
        ));
        assert!(
            !limits.data_dir.exists(),
            "incompatible strong config must not open storage"
        );
        limits.data_dir = directory.0.join("guarded");
        limits.queue_bytes_per_shard += crate::worker::ADMISSION_METADATA_BYTES;
        let engine =
            StreamEngine::open_with_fence(limits.clone(), fence).expect("exact strong bound");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("actual guarded topic");
        assert_eq!(engine.maximum_batch_bytes(), ceiling);
        let mut request = append_request(1, &vec![8; ceiling], None);
        request.leader_epoch = Some(7);
        let receipt = engine
            .append(request)
            .expect("advertised maximum fits actual physical charge");
        assert_eq!(receipt.placement.leader_epoch, LeaderEpoch::new(7));
        let mut next = append_request(2, b"later fitting progress", None);
        next.leader_epoch = Some(7);
        assert_eq!(
            engine
                .append(next)
                .expect("later guarded progress")
                .first_offset,
            LogicalOffset::new(1)
        );
        drop(engine);
        limits.data_dir = directory.0.join("legacy");
        limits.queue_bytes_per_shard -= crate::worker::ADMISSION_METADATA_BYTES;
        let legacy = StreamEngine::open_with_fence(limits, Arc::new(ExactEpochFence))
            .expect("legacy None admission retains its exact original bound");
        legacy
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("actual legacy topic");
        let mut request = append_request(1, &vec![9; ceiling], None);
        request.leader_epoch = Some(7);
        legacy
            .append(request)
            .expect("legacy maximum remains accepted");
    }

    #[test]
    fn guarded_early_and_queued_physical_refusal_leave_no_allocation_or_abort_gap() {
        let directory = TempDir::new("guarded-refusal-lineage");
        let admission = Arc::new(FixturePendingAdmission::default());
        let fence = Arc::new(FixturePendingFence {
            admission: admission.clone(),
            declared: true,
        });
        let engine = StreamEngine::open_with_fence(config(&directory.0), fence.clone())
            .expect("real guarded engine");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("actual topic");
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let mut refused = append_request(1, b"refused", None);
        refused.leader_epoch = Some(7);
        let before = engine
            .watermarks(partition)
            .expect("actual initial frontiers");
        admission
            .refuse_before_reserve
            .store(true, Ordering::Release);
        assert!(matches!(
            engine.append(refused.clone()),
            Err(EngineError::Fenced(_))
        ));
        admission
            .refuse_before_reserve
            .store(false, Ordering::Release);
        admission
            .panic_before_reserve
            .store(true, Ordering::Release);
        assert!(matches!(
            engine.append(refused.clone()),
            Err(EngineError::Fenced(_))
        ));
        assert_eq!(
            engine
                .watermarks(partition)
                .expect("early refused frontiers"),
            before
        );
        assert!(
            engine
                .fetch(fetch_request(0))
                .expect("early refused WAL")
                .is_empty()
        );
        admission.refuse_physical.store(true, Ordering::Release);
        assert!(matches!(
            engine.append(refused),
            Err(EngineError::Fenced(_))
        ));
        assert_eq!(
            engine
                .watermarks(partition)
                .expect("queued refused frontiers"),
            before
        );
        assert!(
            engine
                .fetch(fetch_request(0))
                .expect("no unauthorized data WAL")
                .is_empty()
        );
        admission.refuse_physical.store(false, Ordering::Release);
        let mut next = append_request(2, b"later real WAL after refusal", None);
        next.leader_epoch = Some(7);
        let receipt = engine.append(next).expect("actual later physical progress");
        assert_eq!(receipt.first_offset, LogicalOffset::new(0));
        let wal = engine
            .fetch(fetch_request(0))
            .expect("actual data excludes refused slot");
        assert_eq!(wal.len(), 1);
        assert_eq!(wal[0].first_offset, LogicalOffset::new(0));
        drop(engine);
        let reopened = StreamEngine::open_with_fence(config(&directory.0), fence)
            .expect("recover only genuinely committed data");
        let recovered = reopened
            .fetch(fetch_request(0))
            .expect("recovered actual data");
        assert_eq!(recovered, wal);
        assert_eq!(
            reopened
                .watermarks(partition)
                .expect("recovered allocated frontier")
                .allocated_end,
            LogicalOffset::new(1)
        );
        assert_eq!(admission.active.load(Ordering::Acquire), 0);
        // No external authority is installed: this tests genuine engine/WAL
        // sequencing rather than controller leases or a replica credential.
    }

    fn proposed_test_request(
        engine: &StreamEngine,
        request: &AppendRequest,
    ) -> crate::coordinator::PreparedAppend {
        let record_count = request
            .validate(engine.maximum_batch_bytes())
            .expect("actual request validation");
        let payload_digest = if request.producer.is_some() || request.atomic_group.is_some() {
            xxhash_rust::xxh3::xxh3_128(&request.payload).to_le_bytes()
        } else {
            [0; 16]
        };
        let intent = AppendIntent {
            topic_partition: TopicPartition::new(request.topic_id, request.partition_id),
            request_id: request.request_id,
            producer: request.producer,
            atomic_group: request.atomic_group,
            record_count,
            payload_bytes: request.payload.len() as u64,
            payload_digest,
            durability: request.durability,
            admitted_leader_epoch: request.leader_epoch.map(LeaderEpoch::new),
        };
        match engine
            ._replication_progress
            .prepare(intent)
            .expect("pure genuine preparation")
        {
            PrepareOutcome::New(prepared) => *prepared,
            PrepareOutcome::Duplicate(_) => panic!("new fixture input must not be a receipt"),
        }
    }

    #[test]
    fn proposal_refusal_preserves_producer_and_atomic_retry_and_later_sink_progress() {
        for producer in [
            None,
            Some(ProducerIdentity {
                producer_id: 17,
                epoch: 1,
                first_sequence: 0,
                event_id: None,
            }),
        ] {
            let directory = TempDir::new("proposal-refusal-index");
            let admission = Arc::new(FixturePendingAdmission::default());
            let fence = Arc::new(FixturePendingFence {
                admission: admission.clone(),
                declared: true,
            });
            let control = Arc::new(RecordingSinkControl::default());
            let engine = StreamEngine::open_with_all_components_and_durable_sink(
                config(&directory.0),
                Some(fence),
                None,
                None,
                Some(recording_sink_config(
                    &control,
                    DurableSinkOptions::default(),
                )),
            )
            .expect("real engine and checkpointed sink");
            engine
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(1),
                    partitions: 1,
                    shards: None,
                })
                .expect("topic");
            let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
            let mut request = append_request(1, b"indexed after a refused proposal", producer);
            request.leader_epoch = Some(7);
            request.atomic_group = Some(AtomicGroupIdentity {
                transaction_id: 91,
                chunk_sequence: 0,
            });
            admission.refuse_physical.store(true, Ordering::Release);
            assert!(matches!(
                engine.append(request.clone()),
                Err(EngineError::Fenced(_))
            ));
            assert_eq!(
                engine
                    .watermarks(partition)
                    .expect("refused allocation")
                    .allocated_end,
                LogicalOffset::new(0)
            );
            assert!(
                engine
                    .fetch(fetch_request(0))
                    .expect("refused data WAL")
                    .is_empty()
            );
            assert!(matches!(
                engine.inspect_atomic_group(partition, 91),
                Err(EngineError::UnknownAtomicGroup(91))
            ));
            if let Some(producer) = producer {
                assert!(
                    engine
                        .producer_progress(partition, producer.producer_id)
                        .expect("unpoisoned producer")
                        .is_none()
                );
            }
            admission.refuse_physical.store(false, Ordering::Release);
            let receipt = engine
                .append(request.clone())
                .expect("same input is still eligible");
            assert_eq!(receipt.first_offset, LogicalOffset::new(0));
            assert_eq!(admission.active.load(Ordering::Acquire), 0);
            let admissions = admission.physical_admissions.load(Ordering::Acquire);
            assert_eq!(
                engine.append(request).expect("exact existing receipt"),
                receipt
            );
            assert_eq!(
                admission.physical_admissions.load(Ordering::Acquire),
                admissions
            );
            engine
                .commit_atomic_group(partition, 91)
                .expect("actual atomic decision");
            wait_for_test(|| {
                engine
                    .durable_sink_checkpoint(partition)
                    .expect("real sink checkpoint")
                    .is_some_and(|checkpoint| checkpoint.next_offset == LogicalOffset::new(1))
            });
            let state = control.state.lock().expect("actual sink state");
            assert_eq!(state.appends.len(), 1);
            assert_eq!(
                state.appends[0].reservation.first_offset,
                LogicalOffset::new(0)
            );
            assert_eq!(
                state.appends[0].payload,
                Bytes::from_static(b"indexed after a refused proposal")
            );
            drop(state);
            let wal = engine
                .fetch(fetch_request(0))
                .expect("visible committed data");
            assert_eq!(wal.len(), 1);
            drop(engine);
            let reopened = StreamEngine::open(config(&directory.0))
                .expect("genuine recovery without aborted gap");
            assert_eq!(
                reopened
                    .fetch(fetch_request(0))
                    .expect("recovered exact data"),
                wal
            );
        }
    }

    #[test]
    fn engine_cached_receipt_needs_no_ticket_or_sink_charge_under_escaped_guard_capacity() {
        // A bounded neutral fence qualifies engine ownership, not a real lease/HA authority.
        let directory = TempDir::new("cached-engine-receipt-capacity");
        let fence = Arc::new(TicketBudgetFence::default());
        let control = Arc::new(RecordingSinkControl::default());
        let engine = StreamEngine::open_with_all_components_and_durable_sink(
            config(&directory.0),
            Some(fence.clone()),
            None,
            None,
            Some(recording_sink_config(
                &control,
                DurableSinkOptions::default(),
            )),
        )
        .expect("real guarded engine and sink");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("topic");
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let mut request = append_request(
            1,
            b"immutable durable receipt",
            Some(ProducerIdentity {
                producer_id: 31,
                epoch: 1,
                first_sequence: 0,
                event_id: None,
            }),
        );
        request.leader_epoch = Some(7);
        let receipt = engine.append(request.clone()).expect("actual initial WAL");
        wait_for_test(|| engine.durable_sink_stats().pending_items == 0);
        let before = engine
            .watermarks(partition)
            .expect("actual committed frontier");
        let before_wal = engine
            .fetch(fetch_request(0))
            .expect("actual committed WAL");
        let mut next = append_request(
            2,
            b"new write after capacity recovery",
            Some(ProducerIdentity {
                producer_id: 31,
                epoch: 1,
                first_sequence: 1,
                event_id: None,
            }),
        );
        next.leader_epoch = Some(7);
        let prepared = proposed_test_request(&engine, &next);
        let ticket = fence
            .prepare_write(partition, 7)
            .expect("actual bounded ticket")
            .expect("ticket");
        let guard = ticket
            .try_admit_proposal(&prepared.proposal)
            .expect("owned physical permission");
        drop(ticket);
        assert_eq!(fence.used.load(Ordering::Acquire), 1);
        let prepares = fence.prepares.load(Ordering::Acquire);
        let validations = control.validation_calls.load(Ordering::Acquire);
        assert_eq!(
            engine
                .append(request.clone())
                .expect("cached ACK under occupied ticket capacity"),
            receipt
        );
        assert_eq!(fence.prepares.load(Ordering::Acquire), prepares);
        assert_eq!(
            control.validation_calls.load(Ordering::Acquire),
            validations
        );
        assert_eq!(fence.used.load(Ordering::Acquire), 1);
        assert_eq!(
            engine
                .watermarks(partition)
                .expect("cached path does not allocate"),
            before
        );
        assert_eq!(
            engine
                .fetch(fetch_request(0))
                .expect("cached path has no new WAL"),
            before_wal
        );
        let mut wrong_payload = request.clone();
        wrong_payload.payload = Bytes::from_static(b"different bytes at the old producer sequence");
        assert!(matches!(
            engine.append(wrong_payload),
            Err(EngineError::DuplicateSequenceMismatch(31))
        ));
        let mut wrong_count = request.clone();
        wrong_count.record_count = 2;
        assert!(matches!(
            engine.append(wrong_count),
            Err(EngineError::DuplicateSequenceMismatch(31))
        ));
        request.leader_epoch = Some(6);
        assert!(matches!(
            engine.append(request),
            Err(EngineError::Fenced(_))
        ));
        assert_eq!(fence.prepares.load(Ordering::Acquire), prepares);
        assert!(matches!(
            engine.append(next.clone()),
            Err(EngineError::Fenced(_))
        ));
        assert_eq!(
            engine
                .watermarks(partition)
                .expect("new input refusal has no allocation"),
            before
        );
        drop(guard);
        assert_eq!(fence.used.load(Ordering::Acquire), 0);
        assert_eq!(
            engine
                .append(next)
                .expect("actual same-producer progress after final owner Drop")
                .first_offset,
            LogicalOffset::new(1)
        );
    }

    #[test]
    fn stale_queued_proposal_conflict_has_no_new_allocation_or_producer_poison() {
        let directory = TempDir::new("proposal-compare-commit");
        let admission = Arc::new(FixturePendingAdmission::default());
        let engine = StreamEngine::open_with_fence(
            config(&directory.0),
            Arc::new(FixturePendingFence {
                admission: admission.clone(),
                declared: true,
            }),
        )
        .expect("guarded engine");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("topic");
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let mut stale = append_request(
            2,
            b"second producer",
            Some(ProducerIdentity {
                producer_id: 22,
                epoch: 1,
                first_sequence: 0,
                event_id: None,
            }),
        );
        stale.leader_epoch = Some(7);
        let prepared = proposed_test_request(&engine, &stale);
        let mut first = append_request(1, b"first actual record", None);
        first.leader_epoch = Some(7);
        engine
            .append(first)
            .expect("a genuine competing commit wins");
        let before = engine
            .watermarks(partition)
            .expect("before stale queued proposal");
        let ticket = engine
            .shard_for_lane(prepared.proposal.placement().virtual_lane_id)
            .expect("real lane")
            .begin_proposed_append(
                prepared,
                stale.payload.clone(),
                false,
                admission,
                engine._replication_progress.clone(),
            )
            .expect("charged actual queue");
        assert!(matches!(
            ticket.wait(),
            ProposedAppendOutcome::Unallocated(EngineError::Sequencer(
                shard_stream_core::SequencerError::ReservationProposalChanged
            ))
        ));
        assert_eq!(
            engine
                .watermarks(partition)
                .expect("conflict changes no allocation"),
            before
        );
        assert!(
            engine
                .producer_progress(partition, 22)
                .expect("producer was not advanced or failed")
                .is_none()
        );
        assert_eq!(
            engine
                .fetch(fetch_request(0))
                .expect("actual WAL retains winner only")
                .len(),
            1
        );
        assert_eq!(
            engine
                .append(stale)
                .expect("fresh same-producer progress")
                .first_offset,
            LogicalOffset::new(1)
        );
    }

    #[test]
    fn queued_proposal_cancellation_prevents_late_allocation_and_preserves_same_input() {
        let directory = TempDir::new("proposal-cancellation");
        let pause = Arc::new(ProposalPause::default());
        let admission = Arc::new(FixturePendingAdmission::default());
        *admission
            .proposal_pause
            .lock()
            .expect("install finite test barrier") = Some(pause.clone());
        let engine = StreamEngine::open_with_fence(
            config(&directory.0),
            Arc::new(FixturePendingFence {
                admission: admission.clone(),
                declared: true,
            }),
        )
        .expect("guarded engine");
        // Declared after the engine so unwind releases the worker before engine Drop.
        let release = ReleaseProposal(pause.clone());
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("topic");
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let mut request = append_request(
            1,
            b"cancelled producer can still progress",
            Some(ProducerIdentity {
                producer_id: 29,
                epoch: 1,
                first_sequence: 0,
                event_id: None,
            }),
        );
        request.leader_epoch = Some(7);
        let prepared = proposed_test_request(&engine, &request);
        let ticket = engine
            .shard_for_lane(prepared.proposal.placement().virtual_lane_id)
            .expect("real lane")
            .begin_proposed_append(
                prepared,
                request.payload.clone(),
                false,
                admission.clone(),
                engine._replication_progress.clone(),
            )
            .expect("actual charged proposal");
        wait_for_test(|| pause.entered.load(Ordering::Acquire));
        assert_eq!(admission.active.load(Ordering::Acquire), 1);
        drop(ticket); // Actual caller-drop cancellation while the worker owns its guard.
        assert_eq!(
            engine
                .watermarks(partition)
                .expect("not yet allocated")
                .allocated_end,
            LogicalOffset::new(0)
        );
        drop(release);
        assert!(
            engine
                .fetch(fetch_request(0))
                .expect("physical barrier and queue settle")
                .is_empty()
        );
        assert_eq!(
            engine
                .watermarks(partition)
                .expect("cancelled commit cannot run later")
                .allocated_end,
            LogicalOffset::new(0)
        );
        assert!(
            engine
                .producer_progress(partition, 29)
                .expect("cancel does not poison producer")
                .is_none()
        );
        assert_eq!(admission.active.load(Ordering::Acquire), 0);
        assert_eq!(
            engine
                .append(request)
                .expect("same-ID actual later progress")
                .first_offset,
            LogicalOffset::new(0)
        );
    }

    #[test]
    fn undeclared_physical_ticket_refuses_before_reservation() {
        let directory = TempDir::new("undeclared-guard");
        let fence = Arc::new(FixturePendingFence {
            admission: Arc::new(FixturePendingAdmission::default()),
            declared: false,
        });
        let engine =
            StreamEngine::open_with_fence(config(&directory.0), fence).expect("legacy declaration");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("actual topic");
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let mut request =
            append_request(1, b"ticket cannot bypass declared capacity contract", None);
        request.leader_epoch = Some(7);
        assert!(matches!(
            engine.append(request),
            Err(EngineError::Fenced(_))
        ));
        assert_eq!(
            engine
                .watermarks(partition)
                .expect("unchanged allocation")
                .allocated_end,
            LogicalOffset::new(0)
        );
        assert!(
            engine
                .fetch(fetch_request(0))
                .expect("unchanged WAL")
                .is_empty()
        );
    }

    #[test]
    fn unbound_replica_cannot_reuse_current_local_leader_permission() {
        let directory = TempDir::new("unbound-current-leader");
        let leader = StreamEngine::open_with_fence(
            config(&directory.0.join("source")),
            Arc::new(ExactEpochFence),
        )
        .expect("real source leader");
        leader
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("source topic");
        let mut request = append_request(1, b"actual reserved source payload", None);
        request.leader_epoch = Some(7);
        let receipt = leader.append(request).expect("actual source reservation");
        let batch = ReplicaAppend {
            reservation: Reservation {
                topic_id: TopicId::new(1),
                partition_id: LogicalPartitionId::new(0),
                batch_id: receipt.batch_id,
                first_offset: receipt.first_offset,
                last_offset: receipt.last_offset,
                record_count: NonZeroU32::new(1).expect("actual record count"),
                placement: receipt.placement,
            },
            producer: None,
            atomic_group: None,
            extension_context: None,
            payload_digest: [0; 16],
            payload: Bytes::from_static(b"actual reserved source payload"),
        };
        let pending = Arc::new(FixturePendingAdmission::default());
        let engine = Arc::new(
            StreamEngine::open_with_fence(
                config(&directory.0.join("local-leader")),
                Arc::new(FixtureUnboundReplicaFence(FixturePendingFence {
                    admission: pending.clone(),
                    declared: true,
                })),
            )
            .expect("actual guarded local leader"),
        );
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("local topic");
        let queue = crate::DurableApplyQueue::spawn(
            engine.clone(),
            crate::DurableApplyConfig {
                worker_count: 1,
                queue_slots_per_worker: 4,
                max_pending_per_partition: 4,
                max_gap_batches: 4,
                max_pending_bytes_per_partition: 1024,
            },
        )
        .expect("actual bounded replica queue");
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let before = engine.watermarks(partition).expect("initial frontiers");
        for _ in 0..2 {
            let error = queue
                .append(batch.clone())
                .expect_err("replica needs its own admission path");
            assert!(matches!(error, EngineError::Fenced(ref reason)
                if reason.contains("authenticated sender context")));
        }
        let unverified_context = ReplicaSourceIdentity {
            broker_id: shard_stream_core::BrokerId::new(9),
            incarnation: 3,
            credential_fingerprint: [5; 32],
        };
        let error = queue
            .append_from_peer(batch, unverified_context)
            .expect_err("default context handling cannot authenticate supplied fields");
        assert!(matches!(error, EngineError::Fenced(ref reason)
            if reason.contains("read-only replica authority is not implemented")));
        assert_eq!(
            engine.watermarks(partition).expect("refused frontiers"),
            before
        );
        assert!(
            engine
                .fetch(fetch_request(0))
                .expect("refused WAL")
                .is_empty()
        );
        assert_eq!(pending.physical_admissions.load(Ordering::Relaxed), 0);
        assert_eq!(pending.active.load(Ordering::Acquire), 0);

        let mut local = append_request(2, b"actual authorized local leader", None);
        local.leader_epoch = Some(7);
        let accepted = engine
            .append(local)
            .expect("local leader permission preserved");
        assert_eq!(accepted.first_offset, LogicalOffset::new(0));
        let mut later = append_request(3, b"continued local leader progress", None);
        later.leader_epoch = Some(7);
        let continued = engine.append(later).expect("later fitting append");
        assert_eq!(continued.first_offset, LogicalOffset::new(1));
        let fetched = engine.fetch(fetch_request(0)).expect("actual leader WAL");
        assert_eq!(fetched.len(), 2);
        assert_eq!(
            fetched[0].payload,
            Bytes::from_static(b"actual authorized local leader")
        );
        assert_eq!(fetched[0].placement, accepted.placement);
        assert_eq!(pending.physical_admissions.load(Ordering::Relaxed), 2);
        assert_eq!(pending.active.load(Ordering::Acquire), 0);
        // The fixed fence qualifies role-specific engine routing, not a real
        // TLS credential, controller lease or distributed authority.
    }

    #[test]
    fn trusted_replica_context_reaches_real_queue_without_granting_local_leader_permission() {
        let directory = TempDir::new("guarded-replica-context");
        let leader = StreamEngine::open_with_fence(
            config(&directory.0.join("leader")),
            Arc::new(ExactEpochFence),
        )
        .expect("real leader");
        leader
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("leader topic");
        let mut request = append_request(1, b"actual leader payload", None);
        request.leader_epoch = Some(7);
        let receipt = leader.append(request).expect("genuine leader reservation");
        let source = ReplicaSourceIdentity {
            broker_id: shard_stream_core::BrokerId::new(9),
            incarnation: 3,
            credential_fingerprint: [5; 32],
        };
        let pending = Arc::new(FixturePendingAdmission::default());
        let follower = Arc::new(
            StreamEngine::open_with_fence(
                config(&directory.0.join("follower")),
                Arc::new(FixtureReplicaContextFence {
                    source,
                    pending: FixturePendingFence {
                        admission: pending.clone(),
                        declared: true,
                    },
                }),
            )
            .expect("real guarded follower"),
        );
        follower
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("follower topic");
        let queue =
            crate::DurableApplyQueue::spawn(follower.clone(), crate::DurableApplyConfig::default())
                .expect("actual follower queue");
        let batch = ReplicaAppend {
            reservation: Reservation {
                topic_id: TopicId::new(1),
                partition_id: LogicalPartitionId::new(0),
                batch_id: receipt.batch_id,
                first_offset: receipt.first_offset,
                last_offset: receipt.last_offset,
                record_count: NonZeroU32::new(1).expect("actual record count"),
                placement: receipt.placement,
            },
            producer: None,
            atomic_group: None,
            extension_context: None,
            payload_digest: [0; 16],
            payload: Bytes::from_static(b"actual leader payload"),
        };
        assert!(matches!(
            queue.append(batch.clone()),
            Err(EngineError::Fenced(_))
        ));
        let mut wrong_source = source;
        wrong_source.incarnation += 1;
        assert!(matches!(
            queue.append_from_peer(batch.clone(), wrong_source),
            Err(EngineError::Fenced(_))
        ));
        assert_eq!(
            follower
                .watermarks(TopicPartition::new(
                    TopicId::new(1),
                    LogicalPartitionId::new(0)
                ))
                .expect("refused follower"),
            PartitionWatermarks {
                log_start: LogicalOffset::new(0),
                allocated_end: LogicalOffset::new(0),
                contiguous_log_end: LogicalOffset::new(0),
                replicated_high_watermark: LogicalOffset::new(0),
                last_stable_offset: LogicalOffset::new(0),
            }
        );
        let applied = queue
            .append_from_peer(batch.clone(), source)
            .expect("actual guarded replica");
        assert_eq!(applied.placement, receipt.placement);
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let before = follower
            .watermarks(partition)
            .expect("actual applied frontiers");
        let wal_before = follower
            .fetch(fetch_request(0))
            .expect("actual applied WAL");
        let cached = queue
            .append_from_peer(batch, source)
            .expect("cached replica");
        assert_eq!(
            (
                cached.request_id,
                cached.batch_id,
                cached.record_id,
                cached.first_offset,
                cached.last_offset,
                cached.placement,
            ),
            (
                applied.request_id,
                applied.batch_id,
                applied.record_id,
                applied.first_offset,
                applied.last_offset,
                applied.placement,
            )
        );
        assert!(cached.visible_through <= before.last_stable_offset);
        assert_eq!(
            follower.watermarks(partition).expect("cached frontiers"),
            before
        );
        assert_eq!(pending.physical_admissions.load(Ordering::Relaxed), 1);
        assert_eq!(pending.active.load(Ordering::Acquire), 0);
        let fetched = follower
            .fetch(fetch_request(0))
            .expect("actual replica WAL");
        assert_eq!(fetched, wal_before);
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].placement, receipt.placement);
        assert_eq!(
            fetched[0].payload,
            Bytes::from_static(b"actual leader payload")
        );
        // The fixed context verifies plumbing only; it is not a TLS identity,
        // current remote lease or distributed ownership qualification.
    }

    #[test]
    fn restart_recovers_extents_sequencer_and_deduplication() {
        let temp = TempDir::new("restart");
        let event_id =
            RecordId::for_producer_event(TopicId::new(1), LogicalPartitionId::new(0), 55, 0, 0);
        let producer = ProducerIdentity {
            producer_id: 55,
            epoch: 0,
            first_sequence: 0,
            event_id: Some(event_id),
        };
        let first;
        {
            let engine = StreamEngine::open(config(&temp.0)).expect("open");
            engine
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(1),
                    partitions: 1,
                    shards: None,
                })
                .expect("create");
            first = engine
                .append(append_request(100, b"durable", Some(producer)))
                .expect("append");
            engine.sync().expect("sync");
        }

        let engine = StreamEngine::open(config(&temp.0)).expect("reopen");
        let duplicate = engine
            .append(append_request(200, b"durable", Some(producer)))
            .expect("deduplicated append");
        assert_eq!(duplicate.batch_id, first.batch_id);
        assert_eq!(duplicate.first_offset, first.first_offset);
        assert_eq!(
            engine
                .idempotent_receipt(
                    TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0)),
                    producer,
                )
                .expect("receipt lookup")
                .expect("original receipt")
                .batch_id,
            first.batch_id
        );

        let next = engine
            .append(append_request(
                201,
                b"next",
                Some(ProducerIdentity {
                    first_sequence: 1,
                    ..producer
                }),
            ))
            .expect("next append");
        assert_eq!(next.first_offset, LogicalOffset::new(1));
        assert_eq!(engine.fetch(fetch_request(0)).expect("fetch").len(), 2);
    }

    #[test]
    fn missing_interior_extent_recovers_as_abort_range() {
        let temp = TempDir::new("abort-recovery");
        let engine_config = config(&temp.0);
        fs::create_dir_all(&engine_config.data_dir).expect("data dir");
        let (mut journal, _) =
            CoordinatorJournal::open(engine_config.data_dir.join("coordinator.ssj"))
                .expect("journal");
        let topic_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        journal
            .append(
                &JournalEvent::Configure {
                    topic_partition,
                    ring_epoch: RingEpoch::new(1),
                    shards: engine_config.shard_ids(),
                },
                true,
            )
            .expect("configure");
        let reservation = Reservation {
            topic_id: TopicId::new(1),
            partition_id: LogicalPartitionId::new(0),
            batch_id: BatchId::new(1),
            first_offset: LogicalOffset::new(2),
            last_offset: LogicalOffset::new(2),
            record_count: NonZeroU32::new(1).expect("nonzero"),
            placement: shard_stream_core::Placement {
                virtual_lane_id: ShardId::new(1),
                ring_epoch: RingEpoch::new(1),
                leader_epoch: shard_stream_core::LeaderEpoch::new(0),
                sequence: PlacementSequence::new(1),
            },
        };
        drop(journal);
        let mut shard = ShardStore::open(shardlog::ShardStoreConfig {
            directory: engine_config.data_dir.join("shards/shard-1"),
            shard_id: ShardId::new(1),
            target_pack_bytes: 1024,
            max_batch_bytes: 64 * 1024,
        })
        .expect("open shard");
        shard
            .append(reservation, None, b"after-gap", true)
            .expect("append later extent");
        drop(shard);

        let engine = StreamEngine::open(engine_config).expect("recover");
        assert_eq!(
            engine
                .watermarks(topic_partition)
                .expect("recovered watermarks")
                .contiguous_log_end,
            LogicalOffset::new(3)
        );
        let appended = engine
            .append(append_request(1, b"next", None))
            .expect("append");
        assert_eq!(appended.first_offset, LogicalOffset::new(3));
        assert_eq!(
            engine
                .watermarks(topic_partition)
                .expect("watermarks")
                .contiguous_log_end,
            LogicalOffset::new(4)
        );
    }

    #[test]
    fn duplicate_sequence_with_different_payload_is_rejected() {
        let temp = TempDir::new("dedup-mismatch");
        let engine = StreamEngine::open(config(&temp.0)).expect("open");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("create");
        let producer = ProducerIdentity {
            producer_id: 9,
            epoch: 0,
            first_sequence: 0,
            event_id: None,
        };
        engine
            .append(append_request(1, b"first", Some(producer)))
            .expect("first");
        assert!(matches!(
            engine.append(append_request(2, b"different", Some(producer))),
            Err(EngineError::DuplicateSequenceMismatch(9))
        ));
    }

    #[test]
    fn rf1_quorum_alias_advances_local_durable_watermark() {
        let temp = TempDir::new("quorum");
        let engine = StreamEngine::open(config(&temp.0)).expect("open");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("create");
        let mut request = append_request(1, b"replicated", None);
        request.durability = Durability::Quorum;
        let response = engine.append(request).expect("quorum append");
        assert_eq!(response.first_offset, LogicalOffset::new(0));

        let topic_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let watermarks = engine.watermarks(topic_partition).expect("watermarks");
        assert_eq!(watermarks.contiguous_log_end, LogicalOffset::new(1));
        assert_eq!(watermarks.replicated_high_watermark, LogicalOffset::new(1));
        engine.sync().expect("local durable sync");
    }

    #[test]
    fn replicated_watermark_waits_for_contiguous_out_of_order_completions() {
        let temp = TempDir::new("out-of-order-replication-watermark");
        let replication = Arc::new(OutOfOrderReplication {
            first_waiting: AtomicBool::new(false),
            release_first: AtomicBool::new(false),
        });
        let mut engine_config = config(&temp.0);
        engine_config.replication_factor = 2;
        engine_config.min_in_sync_replicas = 2;
        let engine = Arc::new(
            StreamEngine::open_with_replication(
                engine_config,
                replication.clone() as Arc<dyn ReplicationTransport>,
            )
            .expect("open"),
        );
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("create");
        let first = {
            let engine = Arc::clone(&engine);
            std::thread::spawn(move || {
                let mut request = append_request(1, b"first", None);
                request.durability = Durability::AllReplicas;
                engine.append(request)
            })
        };
        let deadline = Instant::now() + Duration::from_secs(2);
        while !replication.first_waiting.load(Ordering::Acquire) {
            assert!(
                Instant::now() < deadline,
                "first batch did not enter replication"
            );
            std::thread::yield_now();
        }
        let mut second = append_request(2, b"second", None);
        second.durability = Durability::AllReplicas;
        let second = engine.append(second).expect("second append");

        let topic_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let stalled = engine.watermarks(topic_partition).expect("watermarks");
        assert_eq!(stalled.replicated_high_watermark, LogicalOffset::new(0));
        let waiter_started = Arc::new(AtomicBool::new(false));
        let waiter_finished = Arc::new(AtomicBool::new(false));
        let visibility_waiter = {
            let engine = Arc::clone(&engine);
            let waiter_started = Arc::clone(&waiter_started);
            let waiter_finished = Arc::clone(&waiter_finished);
            std::thread::spawn(move || {
                waiter_started.store(true, Ordering::Release);
                engine
                    .wait_until_visible(
                        topic_partition,
                        LogicalOffset::new(second.last_offset.get() + 1),
                        FetchMode::Replicated,
                    )
                    .expect("visibility wait");
                waiter_finished.store(true, Ordering::Release);
            })
        };
        while !waiter_started.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        std::thread::sleep(Duration::from_millis(10));
        assert!(!waiter_finished.load(Ordering::Acquire));
        replication.release_first.store(true, Ordering::Release);
        first.join().expect("first join").expect("first append");
        visibility_waiter.join().expect("visibility waiter");
        assert!(waiter_finished.load(Ordering::Acquire));
        let advanced = engine.watermarks(topic_partition).expect("watermarks");
        assert_eq!(advanced.replicated_high_watermark, LogicalOffset::new(2));
        assert_eq!(advanced.contiguous_log_end, LogicalOffset::new(2));
    }

    #[test]
    fn follower_reservation_is_retryable_only_after_the_inflight_attempt_aborts() {
        let temp = TempDir::new("replica-reservation-retry");
        let mut engine_config = config(&temp.0);
        engine_config.replication_factor = 1;
        engine_config.min_in_sync_replicas = 1;
        let engine = StreamEngine::open(engine_config).expect("open");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("create");
        let reservation = Reservation {
            topic_id: TopicId::new(1),
            partition_id: LogicalPartitionId::new(0),
            batch_id: BatchId::new(0),
            first_offset: LogicalOffset::new(0),
            last_offset: LogicalOffset::new(0),
            record_count: NonZeroU32::new(1).expect("count"),
            placement: Placement {
                virtual_lane_id: VirtualLaneId::new(0),
                ring_epoch: RingEpoch::new(1),
                leader_epoch: LeaderEpoch::new(0),
                sequence: PlacementSequence::new(0),
            },
        };
        let batch = ReplicaAppend {
            reservation,
            producer: None,
            atomic_group: None,
            extension_context: None,
            payload_digest: [7; 16],
            payload: Bytes::from_static(b"retryable"),
        };

        assert!(matches!(
            engine.plan_replica_append(&batch),
            Ok(ReplicaAppendPlan::Reserved { .. })
        ));
        assert!(matches!(
            engine.plan_replica_append(&batch),
            Err(EngineError::ReplicaInFlight(batch_id)) if batch_id == BatchId::new(0)
        ));

        engine
            .cancel_replica_append(&batch)
            .expect("abort reservation");
        let retry = engine.plan_replica_append(&batch).expect("retry plan");
        assert_eq!(
            engine
                .watermarks(TopicPartition::new(
                    TopicId::new(1),
                    LogicalPartitionId::new(0)
                ))
                .expect("pending retry watermarks")
                .contiguous_log_end,
            LogicalOffset::new(0)
        );
        let response = engine
            .execute_replica_append(batch.clone(), retry)
            .expect("retry append");
        assert_eq!(response.batch_id, BatchId::new(0));
        assert!(matches!(
            engine.plan_replica_append(&batch),
            Ok(ReplicaAppendPlan::Duplicate(response))
                if response.batch_id == BatchId::new(0)
        ));
    }

    #[test]
    fn leader_ack_replication_progress_advances_after_replica_drain() {
        let temp = TempDir::new("leader-replication-progress");
        let engine = StreamEngine::open(config(&temp.0)).expect("open");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("create");
        engine
            .append(append_request(1, b"replicate-in-background", None))
            .expect("leader append");
        engine.sync().expect("replica drain");

        let topic_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let watermarks = engine.watermarks(topic_partition).expect("watermarks");
        assert_eq!(watermarks.contiguous_log_end, LogicalOffset::new(1));
        assert_eq!(watermarks.replicated_high_watermark, LogicalOffset::new(1));
    }

    #[test]
    fn leader_ack_returns_before_distributed_replication_and_tracks_later_completion() {
        let temp = TempDir::new("async-distributed-replication");
        let release = Arc::new(AtomicBool::new(false));
        let transport = Arc::new(DeferredReplication {
            progress: OnceLock::new(),
            release: Arc::clone(&release),
        });
        let mut engine_config = config(&temp.0);
        engine_config.replication_factor = 3;
        engine_config.min_in_sync_replicas = 2;
        let engine = StreamEngine::open_with_replication(
            engine_config,
            transport as Arc<dyn ReplicationTransport>,
        )
        .expect("open");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("create");

        engine
            .append(append_request(1, b"ack-before-followers", None))
            .expect("leader acknowledgement");
        let topic_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        assert_eq!(
            engine
                .watermarks(topic_partition)
                .expect("leader-only watermarks")
                .replicated_high_watermark,
            LogicalOffset::new(0)
        );

        release.store(true, Ordering::Release);
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let replicated = engine
                .watermarks(topic_partition)
                .expect("replicated watermarks")
                .replicated_high_watermark;
            if replicated == LogicalOffset::new(1) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "asynchronous replication completion was not applied"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn object_ack_seals_pack_publishes_manifest_and_survives_restart() {
        let temp = TempDir::new("object-durable");
        let mut engine_config = config(&temp.0.join("data"));
        engine_config.object_store_dir = Some(temp.0.join("objects"));
        let producer = ProducerIdentity {
            producer_id: 77,
            epoch: 0,
            first_sequence: 0,
            event_id: None,
        };
        let response;
        {
            let engine = StreamEngine::open(engine_config.clone()).expect("open");
            engine
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(1),
                    partitions: 1,
                    shards: None,
                })
                .expect("create");
            let mut request = append_request(1, b"object-durable", Some(producer));
            request.durability = Durability::Object;
            response = engine.append(request).expect("object append");
        }

        let tier = shardlog::LocalObjectTier::open(
            engine_config
                .object_store_dir
                .as_ref()
                .expect("object store"),
            ShardId::new(0),
        )
        .expect("open tier");
        assert_eq!(tier.manifest().generation, 1);
        assert_eq!(tier.manifest().objects.len(), 1);
        assert!(tier.manifest().objects[0].bytes > 0);

        let engine = StreamEngine::open(engine_config).expect("reopen");
        let mut retry = append_request(2, b"object-durable", Some(producer));
        retry.durability = Durability::Object;
        let duplicate = engine.append(retry).expect("deduplicated object append");
        assert_eq!(duplicate.batch_id, response.batch_id);
        assert_eq!(engine.fetch(fetch_request(0)).expect("fetch").len(), 1);
    }

    #[test]
    fn retention_log_start_is_monotonic_batch_aligned_and_restart_durable() {
        let temp = TempDir::new("retention");
        let topic_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        {
            let engine = StreamEngine::open(config(&temp.0)).expect("open");
            engine
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(1),
                    partitions: 1,
                    shards: None,
                })
                .expect("create");
            for request_id in 1..=3 {
                engine
                    .append(append_request(request_id, &[request_id as u8], None))
                    .expect("append");
            }
            let watermarks = engine
                .truncate_partition(topic_partition, LogicalOffset::new(2))
                .expect("truncate");
            assert_eq!(watermarks.log_start, LogicalOffset::new(2));
            assert!(matches!(
                engine.fetch(fetch_request(0)),
                Err(EngineError::OffsetOutOfRange { .. })
            ));
            assert_eq!(
                engine.fetch(fetch_request(2)).expect("retained fetch")[0].payload,
                vec![3]
            );
            assert!(
                engine
                    .truncate_partition(topic_partition, LogicalOffset::new(1))
                    .is_err()
            );
        }

        let engine = StreamEngine::open(config(&temp.0)).expect("reopen");
        assert_eq!(
            engine
                .watermarks(topic_partition)
                .expect("watermarks")
                .log_start,
            LogicalOffset::new(2)
        );
        assert!(matches!(
            engine.fetch(fetch_request(1)),
            Err(EngineError::OffsetOutOfRange { .. })
        ));
    }

    #[test]
    fn dense_batch_index_crosses_segments_and_restarts_after_prefix_retention() {
        let temp = TempDir::new("dense-index-segments");
        let topic_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let mut engine_config = config(&temp.0);
        engine_config.shard_count = 1;
        engine_config.replication_factor = 1;
        engine_config.min_in_sync_replicas = 1;
        engine_config.target_pack_bytes = 8 * 1024 * 1024;

        {
            let engine = StreamEngine::open(engine_config.clone()).expect("open");
            engine
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(1),
                    partitions: 1,
                    shards: None,
                })
                .expect("create");
            for request_id in 0..1_030u128 {
                engine
                    .append(append_request(request_id, &[request_id as u8], None))
                    .expect("append across dense segments");
            }

            let fetched = engine.fetch(fetch_request(1_024)).expect("tail fetch");
            assert_eq!(fetched.len(), 6);
            assert_eq!(fetched[0].batch_id, BatchId::new(1_024));
            assert_eq!(fetched[5].batch_id, BatchId::new(1_029));

            let watermarks = engine
                .truncate_partition(topic_partition, LogicalOffset::new(1_024))
                .expect("truncate at segment boundary");
            assert_eq!(watermarks.log_start, LogicalOffset::new(1_024));
            assert_eq!(watermarks.contiguous_log_end, LogicalOffset::new(1_030));
        }

        let engine = StreamEngine::open(engine_config).expect("reopen");
        assert!(matches!(
            engine.fetch(fetch_request(1_023)),
            Err(EngineError::OffsetOutOfRange { .. })
        ));
        let fetched = engine.fetch(fetch_request(1_024)).expect("retained fetch");
        assert_eq!(fetched.len(), 6);
        assert_eq!(fetched[0].batch_id, BatchId::new(1_024));
    }

    #[test]
    fn remote_all_replicas_retry_preserves_the_reservation_and_populates_the_follower() {
        let temp = TempDir::new("remote-replication");
        let mut follower_config = config(&temp.0.join("follower"));
        follower_config.replication_factor = 1;
        follower_config.min_in_sync_replicas = 1;
        let follower = Arc::new(StreamEngine::open(follower_config).expect("follower"));
        follower
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("follower topic");

        let mut leader_config = config(&temp.0.join("leader"));
        leader_config.replication_factor = 2;
        leader_config.min_in_sync_replicas = 2;
        let transport: Arc<dyn ReplicationTransport> = Arc::new(DirectReplication {
            follower: Arc::clone(&follower),
            fail_first: AtomicBool::new(true),
            progress: OnceLock::new(),
        });
        let leader = StreamEngine::open_with_replication(leader_config, transport).expect("leader");
        leader
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("leader topic");

        let producer = ProducerIdentity {
            producer_id: 99,
            epoch: 0,
            first_sequence: 0,
            event_id: None,
        };
        let mut request = append_request(1, b"replicated", Some(producer));
        request.durability = Durability::AllReplicas;
        assert!(matches!(
            leader.append(request.clone()),
            Err(EngineError::DurabilityUnavailable { .. })
        ));
        let topic_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let before_retry = leader.watermarks(topic_partition).expect("watermarks");
        assert_eq!(before_retry.contiguous_log_end, LogicalOffset::new(1));
        assert_eq!(
            before_retry.replicated_high_watermark,
            LogicalOffset::new(0)
        );
        let mut replicated_fetch = fetch_request(0);
        replicated_fetch.mode = FetchMode::Replicated;
        assert!(
            leader
                .fetch(replicated_fetch)
                .expect("replicated fetch")
                .is_empty()
        );
        let receipt = leader.append(request).expect("retry reaches follower");
        assert_eq!(receipt.first_offset, LogicalOffset::new(0));
        assert_eq!(receipt.visible_through, LogicalOffset::new(1));
        assert_eq!(
            leader
                .watermarks(topic_partition)
                .expect("watermarks")
                .replicated_high_watermark,
            LogicalOffset::new(1)
        );
        let fetched = follower.fetch(fetch_request(0)).expect("follower fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].payload, Bytes::from_static(b"replicated"));
    }

    #[test]
    fn async_idempotent_retry_reenqueues_missing_follower_replication() {
        let temp = TempDir::new("async-replication-retry");
        let mut follower_config = config(&temp.0.join("follower"));
        follower_config.replication_factor = 1;
        follower_config.min_in_sync_replicas = 1;
        let follower = Arc::new(StreamEngine::open(follower_config).expect("follower"));
        follower
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("follower topic");

        let mut leader_config = config(&temp.0.join("leader"));
        leader_config.replication_factor = 2;
        leader_config.min_in_sync_replicas = 2;
        let transport: Arc<dyn ReplicationTransport> = Arc::new(DirectReplication {
            follower: Arc::clone(&follower),
            fail_first: AtomicBool::new(true),
            progress: OnceLock::new(),
        });
        let leader = StreamEngine::open_with_replication(leader_config, transport).expect("leader");
        leader
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("leader topic");

        let request = append_request(
            1,
            b"async-retry",
            Some(ProducerIdentity {
                producer_id: 100,
                epoch: 0,
                first_sequence: 0,
                event_id: None,
            }),
        );
        assert!(matches!(
            leader.append(request.clone()),
            Err(EngineError::DurabilityUnavailable { .. })
        ));
        let receipt = leader.append(request).expect("retry enqueues replication");
        assert_eq!(receipt.first_offset, LogicalOffset::new(0));
        assert_eq!(
            follower.fetch(fetch_request(0)).expect("follower fetch")[0].payload,
            Bytes::from_static(b"async-retry")
        );
    }

    #[test]
    fn restart_replays_recovered_wal_tail_before_advancing_replica_watermark() {
        let temp = TempDir::new("async-replication-recovery");
        let leader_path = temp.0.join("leader");
        let follower_path = temp.0.join("follower");
        let mut follower_config = config(&follower_path);
        follower_config.replication_factor = 1;
        follower_config.min_in_sync_replicas = 1;
        let follower = Arc::new(StreamEngine::open(follower_config).expect("follower open"));
        follower
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("follower topic");

        let mut initial_config = config(&leader_path);
        initial_config.replication_factor = 2;
        initial_config.min_in_sync_replicas = 2;
        {
            let leader = StreamEngine::open_with_replication(
                initial_config.clone(),
                Arc::new(NoopReplication),
            )
            .expect("initial leader");
            leader
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(1),
                    partitions: 1,
                    shards: None,
                })
                .expect("leader topic");
            leader
                .append(append_request(1, b"recovered-tail", None))
                .expect("local leader append");
        }

        let transport: Arc<dyn ReplicationTransport> = Arc::new(DirectReplication {
            follower: Arc::clone(&follower),
            fail_first: AtomicBool::new(false),
            progress: OnceLock::new(),
        });
        let leader = StreamEngine::open_with_replication(initial_config, transport)
            .expect("reopen leader with replication");
        let topic_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let recovered = leader.recovered_partitions();
        assert_eq!(recovered, vec![topic_partition]);
        let before_replay = leader
            .watermarks(topic_partition)
            .expect("recovered watermarks");
        assert_eq!(before_replay.contiguous_log_end, LogicalOffset::new(1));
        assert_eq!(
            before_replay.replicated_high_watermark,
            LogicalOffset::new(0)
        );

        leader
            .resume_recovered_replication(&recovered)
            .expect("replay recovered WAL");
        assert_eq!(
            leader
                .watermarks(topic_partition)
                .expect("replayed watermarks")
                .replicated_high_watermark,
            LogicalOffset::new(1)
        );
        assert_eq!(
            follower
                .fetch(fetch_request(0))
                .expect("follower recovery fetch")[0]
                .payload,
            Bytes::from_static(b"recovered-tail")
        );
    }

    #[test]
    fn restart_replication_replays_unresolved_atomic_tail_before_commit() {
        let temp = TempDir::new("atomic-replication-recovery");
        let leader_path = temp.0.join("leader");
        let follower_path = temp.0.join("follower");
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let mut follower_config = config(&follower_path);
        follower_config.replication_factor = 1;
        follower_config.min_in_sync_replicas = 1;
        let follower = Arc::new(StreamEngine::open(follower_config).expect("follower open"));
        follower
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("follower topic");

        let mut leader_config = config(&leader_path);
        leader_config.replication_factor = 2;
        leader_config.min_in_sync_replicas = 2;
        {
            let leader = StreamEngine::open_with_replication(
                leader_config.clone(),
                Arc::new(NoopReplication),
            )
            .expect("initial leader");
            leader
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(1),
                    partitions: 1,
                    shards: None,
                })
                .expect("leader topic");
            leader
                .append(atomic_request(1, 999, 0, b"unresolved-tail"))
                .expect("atomic append");
        }

        let transport: Arc<dyn ReplicationTransport> = Arc::new(DirectReplication {
            follower: Arc::clone(&follower),
            fail_first: AtomicBool::new(false),
            progress: OnceLock::new(),
        });
        let leader =
            StreamEngine::open_with_replication(leader_config, transport).expect("reopen leader");
        leader
            .resume_recovered_replication(&leader.recovered_partitions())
            .expect("replay unresolved tail");
        assert_eq!(
            follower
                .inspect_atomic_group(partition, 999)
                .expect("follower group")
                .state,
            AtomicGroupState::Unresolved
        );
        assert!(follower.fetch(fetch_request(0)).expect("hidden").is_empty());

        leader
            .commit_atomic_group(partition, 999)
            .expect("quorum-safe leader commit");
        follower
            .commit_atomic_group(partition, 999)
            .expect("apply replicated metadata decision");
        assert_eq!(
            follower.fetch(fetch_request(0)).expect("visible")[0].payload,
            Bytes::from_static(b"unresolved-tail")
        );
    }

    #[test]
    fn retention_reclaims_only_fully_expired_sealed_packs() {
        let temp = TempDir::new("retention-gc");
        let mut engine_config = config(&temp.0.join("data"));
        engine_config.object_store_dir = Some(temp.0.join("objects"));
        engine_config.shard_count = 1;
        engine_config.target_pack_bytes = 1;
        let topic_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        {
            let engine = StreamEngine::open(engine_config.clone()).expect("open");
            engine
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(1),
                    partitions: 1,
                    shards: None,
                })
                .expect("create");
            for request_id in 1..=3 {
                let mut request = append_request(request_id, &[request_id as u8], None);
                request.durability = Durability::Object;
                engine.append(request).expect("object append");
            }
            engine
                .truncate_partition(topic_partition, LogicalOffset::new(2))
                .expect("truncate");
        }

        let primary = temp.0.join("data/shards/shard-0");
        assert!(!primary.join("pack-00000000000000000000.sse").exists());
        assert!(!primary.join("pack-00000000000000000001.sse").exists());
        assert!(primary.join("pack-00000000000000000002.sse").exists());

        let engine = StreamEngine::open(engine_config).expect("reopen after GC");
        assert_eq!(
            engine.fetch(fetch_request(2)).expect("retained fetch")[0].payload,
            vec![3]
        );
    }

    #[test]
    fn recovery_applies_log_start_before_validating_a_fully_reclaimed_prefix() {
        let temp = TempDir::new("retention-empty-prefix-recovery");
        let mut engine_config = config(&temp.0);
        engine_config.shard_count = 1;
        engine_config.target_pack_bytes = 1;
        let topic_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        {
            let engine = StreamEngine::open(engine_config.clone()).expect("open");
            engine
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(1),
                    partitions: 1,
                    shards: None,
                })
                .expect("create");
            for request_id in 1..=2 {
                engine
                    .append(append_request(request_id, &[request_id as u8], None))
                    .expect("append");
            }
            engine.sync().expect("sync");
        }
        {
            // Reopen first so both tiny source packs are immutable and the
            // retention pass can physically remove the complete prefix.
            let engine = StreamEngine::open(engine_config.clone()).expect("reopen before truncate");
            let watermarks = engine
                .truncate_partition(topic_partition, LogicalOffset::new(2))
                .expect("truncate complete prefix");
            assert_eq!(watermarks.log_start, LogicalOffset::new(2));
            assert_eq!(watermarks.last_stable_offset, LogicalOffset::new(2));
        }

        let engine = StreamEngine::open(engine_config).expect("reopen reclaimed prefix");
        let watermarks = engine.watermarks(topic_partition).expect("watermarks");
        assert_eq!(watermarks.log_start, LogicalOffset::new(2));
        assert_eq!(watermarks.last_stable_offset, LogicalOffset::new(2));
        assert!(
            engine
                .fetch(fetch_request(2))
                .expect("empty tail")
                .is_empty()
        );
        let response = engine
            .append(append_request(3, b"after-retention", None))
            .expect("append resumes after retained offset");
        assert_eq!(response.first_offset, LogicalOffset::new(2));
    }

    #[test]
    fn concurrent_appends_recover_without_per_append_journal_events() {
        let temp = TempDir::new("mpsc-concurrency");
        let engine_config = config(&temp.0);
        let engine = Arc::new(StreamEngine::open(engine_config.clone()).expect("open"));
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("create");

        std::thread::scope(|scope| {
            for producer in 0..8u128 {
                let engine = Arc::clone(&engine);
                scope.spawn(move || {
                    for sequence in 0..32u128 {
                        let request_id = producer * 32 + sequence;
                        engine
                            .append(append_request(request_id, &[request_id as u8], None))
                            .expect("concurrent append");
                    }
                });
            }
        });
        let topic_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let watermarks = engine.watermarks(topic_partition).expect("watermarks");
        assert_eq!(watermarks.allocated_end, LogicalOffset::new(256));
        assert_eq!(watermarks.contiguous_log_end, LogicalOffset::new(256));
        let fetched = engine.fetch(fetch_request(0)).expect("live fetch");
        assert_eq!(fetched.len(), 256);
        assert!(
            fetched
                .iter()
                .enumerate()
                .all(|(offset, batch)| batch.first_offset == LogicalOffset::new(offset as u64))
        );
        drop(engine);

        let (_, recovered_journal) =
            CoordinatorJournal::open(engine_config.data_dir.join("coordinator.ssj"))
                .expect("journal");
        assert_eq!(recovered_journal.events.len(), 1);
        assert!(matches!(
            recovered_journal.events[0],
            JournalEvent::CreatePartition { .. }
        ));

        let engine = StreamEngine::open(engine_config).expect("reopen");
        let recovered = engine.watermarks(topic_partition).expect("recovered");
        assert_eq!(recovered.allocated_end, LogicalOffset::new(256));
        assert_eq!(recovered.contiguous_log_end, LogicalOffset::new(256));
    }

    #[test]
    fn partition_replication_policy_survives_restart() {
        let temp = TempDir::new("partition-policy-restart");
        let engine_config = config(&temp.0);
        let topic_partition = TopicPartition::new(TopicId::new(41), LogicalPartitionId::new(0));
        let policy = PartitionReplicationPolicy {
            policy_generation: 7,
            replication_factor: 1,
            min_in_sync_replicas: 1,
        };
        {
            let engine = StreamEngine::open(engine_config.clone()).expect("open");
            engine
                .create_topic_with_policy(
                    TopicConfig {
                        topic_id: topic_partition.topic_id,
                        partitions: 1,
                        shards: None,
                    },
                    policy,
                )
                .expect("create with policy");
            assert_eq!(
                engine
                    .partition_replication_policy(topic_partition)
                    .expect("policy"),
                policy
            );
            let mut request = append_request(1, b"policy", None);
            request.topic_id = topic_partition.topic_id;
            request.durability = Durability::AllReplicas;
            engine.append(request).expect("RF1 all-replicas append");
            engine.sync().expect("sync");
        }

        let reopened = StreamEngine::open(engine_config).expect("reopen");
        assert_eq!(
            reopened
                .partition_replication_policy(topic_partition)
                .expect("recovered policy"),
            policy
        );
        assert_eq!(
            reopened
                .watermarks(topic_partition)
                .expect("watermarks")
                .replicated_high_watermark,
            LogicalOffset::new(1)
        );
    }

    #[test]
    fn topic_policy_update_is_generation_checked_idempotent_and_restart_durable() {
        let temp = TempDir::new("partition-policy-update");
        let mut engine_config = config(&temp.0);
        engine_config.replication_factor = 3;
        engine_config.min_in_sync_replicas = 1;
        let topic_id = TopicId::new(42);
        let target = PartitionReplicationPolicy {
            policy_generation: 2,
            replication_factor: 2,
            min_in_sync_replicas: 2,
        };
        {
            let engine = StreamEngine::open_with_replication(
                engine_config.clone(),
                Arc::new(NoopReplication),
            )
            .expect("open");
            engine
                .create_topic(TopicConfig {
                    topic_id,
                    partitions: 2,
                    shards: None,
                })
                .expect("create");
            engine
                .update_topic_replication_policy(topic_id, 1, target)
                .expect("update");
            engine
                .update_topic_replication_policy(topic_id, 1, target)
                .expect("idempotent update");
            let conflict = PartitionReplicationPolicy {
                policy_generation: 3,
                replication_factor: 3,
                min_in_sync_replicas: 2,
            };
            assert!(matches!(
                engine.update_topic_replication_policy(topic_id, 1, conflict),
                Err(EngineError::Fenced(_))
            ));
            engine.sync().expect("sync");
        }

        let reopened =
            StreamEngine::open_with_replication(engine_config, Arc::new(NoopReplication))
                .expect("reopen");
        for partition_id in 0..2 {
            assert_eq!(
                reopened
                    .partition_replication_policy(TopicPartition::new(
                        topic_id,
                        LogicalPartitionId::new(partition_id),
                    ))
                    .expect("policy"),
                target
            );
        }
    }
}
