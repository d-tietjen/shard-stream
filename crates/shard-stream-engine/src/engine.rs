use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::num::NonZeroU32;
use std::sync::mpsc::{Receiver, channel, sync_channel};
use std::sync::{Arc, Weak};

use bytes::Bytes;
use shard_stream_core::{
    BatchId, LogicalOffset, LogicalPartitionId, Placement, RecordId, Reservation, RingEpoch,
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
    CommittedBatchSet, CompleteAppend, ControlJournal, CoordinatorPool, CoordinatorRouter,
    ReserveOutcome, VisibilityMode, recover_partition_states, retention_hints,
};
use crate::worker::{FetchCompletion, ShardHandle};
use crate::{EngineConfig, EngineError, EngineResult, PartitionReplicationPolicy, TopicConfig};

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
    replication: Option<Arc<dyn ReplicationTransport>>,
    retention_guard: Option<Arc<dyn RetentionGuard>>,
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
    Reserved {
        response: AppendResponse,
        producer: Option<ProducerSequence>,
        atomic_group: Option<AtomicGroupSequence>,
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

    pub fn open(config: EngineConfig) -> EngineResult<Self> {
        Self::open_inner(config, None, None, None)
    }

    pub fn open_with_fence(
        config: EngineConfig,
        write_fence: Arc<dyn WriteFence>,
    ) -> EngineResult<Self> {
        Self::open_inner(config, Some(write_fence), None, None)
    }

    pub fn open_with_replication(
        config: EngineConfig,
        replication: Arc<dyn ReplicationTransport>,
    ) -> EngineResult<Self> {
        Self::open_inner(config, None, Some(replication), None)
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
        Self::open_inner(config, write_fence, replication, None)
    }

    pub fn open_with_all_components(
        config: EngineConfig,
        write_fence: Option<Arc<dyn WriteFence>>,
        replication: Option<Arc<dyn ReplicationTransport>>,
        retention_guard: Option<Arc<dyn RetentionGuard>>,
    ) -> EngineResult<Self> {
        Self::open_inner(config, write_fence, replication, retention_guard)
    }

    fn open_inner(
        config: EngineConfig,
        write_fence: Option<Arc<dyn WriteFence>>,
        replication: Option<Arc<dyn ReplicationTransport>>,
        retention_guard: Option<Arc<dyn RetentionGuard>>,
    ) -> EngineResult<Self> {
        config.validate()?;
        if config.replication_factor > 1 && replication.is_none() {
            return Err(EngineError::InvalidConfig(
                "RF>1 requires an injected replication transport; the public engine only provides RF1 storage"
                    .into(),
            ));
        }
        std::fs::create_dir_all(&config.data_dir).map_err(shardlog::StorageError::from)?;

        let (journal, recovered) =
            CoordinatorJournal::open(config.data_dir.join("coordinator.ssj"))?;
        let retained_log_starts = retention_hints(&recovered.events);
        let mut shards = Vec::with_capacity(config.shard_count as usize);
        let mut catalog = HashMap::new();
        for shard_id in config.shard_ids() {
            let (handle, entries) = ShardHandle::spawn(&config, shard_id)?;
            for entry in entries {
                let key = (
                    TopicPartition::new(entry.reservation.topic_id, entry.reservation.partition_id),
                    entry.reservation.batch_id,
                );
                if catalog.insert(key, entry).is_some() {
                    return Err(EngineError::CorruptState(format!(
                        "batch {} appears in more than one extent",
                        key.1
                    )));
                }
            }
            shards.push(handle);
        }

        let mut recovered_partitions = catalog
            .keys()
            .map(|(topic_partition, _)| *topic_partition)
            .collect::<Vec<_>>();
        recovered_partitions.sort_unstable();
        recovered_partitions.dedup();
        let recovered_states = recover_partition_states(&config, &recovered.events, &catalog, 1)?;
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
            replication,
            retention_guard,
        };
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
        if let Some(write_fence) = &self.write_fence {
            let leader_epoch = request.leader_epoch.ok_or_else(|| {
                EngineError::Fenced("leader_epoch is required by the write fence".into())
            })?;
            write_fence
                .check_write(topic_partition, leader_epoch)
                .map_err(|error| EngineError::Fenced(error.to_string()))?;
        }
        let payload_digest = if request.producer.is_some() || request.atomic_group.is_some() {
            xxhash_rust::xxh3::xxh3_128(&request.payload).to_le_bytes()
        } else {
            [0; 16]
        };
        let payload_bytes = u64::try_from(request.payload.len())
            .map_err(|_| EngineError::InvalidConfig("append payload exceeds u64".into()))?;
        let reserve_outcome = self.coordinators.reserve(
            topic_partition,
            request.request_id,
            request.producer,
            request.atomic_group,
            record_count,
            payload_bytes,
            payload_digest,
            request.durability,
        )?;
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
                if durable_replicas >= required_replicas && !async_followers_pending {
                    return Ok(response);
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
        if required_followers > 0
            && let (Some(replication), Some(batch)) = (&self.replication, replicated_batch.take())
        {
            match replication.begin_replicate(batch, required_followers) {
                Ok(ticket) => replication_ticket = Some(ticket),
                Err(error) => replication_start_error = Some(error),
            }
        }
        let append_result = self
            .shard_for_lane(reservation.placement.virtual_lane_id)?
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
            );
        let response = append_response(request.request_id, reservation);
        match append_result {
            Ok(mut durability) => {
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
                    match replication.replicate(batch, 0) {
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
        let topic_partition =
            TopicPartition::new(batch.reservation.topic_id, batch.reservation.partition_id);
        if let Some(write_fence) = &self.write_fence {
            write_fence
                .check_write(
                    topic_partition,
                    batch.reservation.placement.leader_epoch.get(),
                )
                .map_err(|error| EngineError::Fenced(error.to_string()))?;
        }
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
        })
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
        let mut results = (0..appends.len())
            .map(|_| None)
            .collect::<Vec<Option<EngineResult<AppendResponse>>>>();
        let mut pending = Vec::with_capacity(appends.len());
        for (index, (batch, plan)) in appends.into_iter().enumerate() {
            match plan {
                ReplicaAppendPlan::Duplicate(response) => results[index] = Some(Ok(response)),
                ReplicaAppendPlan::Reserved {
                    response,
                    producer,
                    atomic_group,
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
                        completion,
                    ) {
                        Ok(()) => pending.push((index, batch, response, receiver)),
                        Err(error) => {
                            results[index] =
                                Some(self.finish_replica_append(&batch, response, Err(error)));
                        }
                    }
                }
            }
        }
        for (index, batch, response, receiver) in pending {
            let append_result = receiver
                .recv()
                .map_err(|_| {
                    crate::worker::AppendFailure::ambiguous(EngineError::WorkerStopped(
                        batch.reservation.placement.virtual_lane_id,
                    ))
                })
                .and_then(|result| {
                    result.map_err(|error| crate::worker::AppendFailure::ambiguous(error.into()))
                });
            results[index] = Some(self.finish_replica_append(&batch, response, append_result));
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
        self.coordinators.finalize_atomic_group(
            topic_partition,
            transaction_id,
            AtomicGroupState::Committed,
        )
    }

    /// Syncs an abort marker. Aborted chunks consume offsets but are never
    /// returned by ordinary fetch.
    pub fn abort_atomic_group(
        &self,
        topic_partition: TopicPartition,
        transaction_id: u128,
    ) -> EngineResult<AtomicGroupInspection> {
        self.coordinators.finalize_atomic_group(
            topic_partition,
            transaction_id,
            AtomicGroupState::Aborted,
        )
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
        log_start: LogicalOffset,
    ) -> EngineResult<()> {
        for shard in &self.shards {
            shard.garbage_collect(topic_partition, log_start)?;
        }
        Ok(())
    }
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
    use std::fs;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, OnceLock};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use shard_stream_core::{LeaderEpoch, PlacementSequence, TopicId, VirtualLaneId};
    use shard_stream_protocol::{Durability, FetchMode, ProducerIdentity};
    use shardlog::{JournalEvent, ShardStore};

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
        request.leader_epoch = Some(7);
        engine.append(request).expect("current epoch");
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
