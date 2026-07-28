use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::num::NonZeroU32;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::{self, JoinHandle};

use ordered_segment_map::DenseOrderedMap;
use shard_stream_core::{
    BatchId, LeaderEpoch, LogicalOffset, LogicalPartitionId, Placement, PlacementSequence,
    RecordId, Reservation, RingEpoch, SequencerState, ShardId, TopicId, TopicPartition,
    TopicSequencer,
};
use shard_stream_protocol::{AppendResponse, AtomicGroupIdentity, Durability, ProducerIdentity};
use shardlog::{
    CoordinatorJournal, DIGEST_XXH3_128, ExtentCatalogEntry, JournalEvent, ProducerSequence,
    StorageError,
};

use crate::{
    AtomicGroupInspection, AtomicGroupState, EngineConfig, EngineError, EngineResult,
    PartitionEpochs, PartitionReplicationPolicy, PartitionWatermarks, ProducerProgress,
    TopicConfig,
};

const COORDINATOR_QUEUE_SLOTS: usize = 16_384;
const CONTROL_QUEUE_SLOTS: usize = 1_024;
const MAX_RECOVERED_GAP_BATCHES: u128 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchStatus {
    Pending,
    Committed,
    Unresolved,
    Aborted,
}

#[derive(Debug, Clone)]
struct BatchRuntime {
    reservation: Reservation,
    status: BatchStatus,
    durable_replicas: u32,
    replica_acks: u64,
    producer: Option<ProducerIdentity>,
    atomic_group: Option<AtomicGroupIdentity>,
    payload_digest: Option<[u8; 16]>,
    payload_bytes: u64,
    object_durable: bool,
}

#[derive(Debug, Clone)]
struct AtomicGroupRuntime {
    state: AtomicGroupState,
    batch_ids: Vec<u128>,
    next_sequence: u64,
    payload_bytes: u64,
    record_count: u64,
}

#[derive(Debug, Clone, Copy)]
struct ProducerCursor {
    epoch: u32,
    next_sequence: u64,
    failed: bool,
}

#[derive(Debug, Clone, Copy)]
struct DedupRecord {
    digest_algorithm: u8,
    payload_digest: [u8; 16],
    event_id: Option<RecordId>,
    record_count: u32,
    durable_replicas: u32,
    object_durable: bool,
    response: AppendResponse,
}

#[derive(Debug)]
pub(crate) struct CommittedBatchSet {
    first_batch_id: u128,
    len: usize,
    words: Vec<u64>,
}

impl CommittedBatchSet {
    fn new(first_batch_id: u128, len: usize) -> Self {
        Self {
            first_batch_id,
            len,
            words: vec![0; len.div_ceil(u64::BITS as usize)],
        }
    }

    fn insert(&mut self, batch_id: BatchId) {
        let Some(relative) = batch_id.get().checked_sub(self.first_batch_id) else {
            return;
        };
        let Ok(relative) = usize::try_from(relative) else {
            return;
        };
        if relative >= self.len {
            return;
        }
        self.words[relative / u64::BITS as usize] |= 1u64 << (relative % u64::BITS as usize);
    }

    pub(crate) fn contains(&self, batch_id: BatchId) -> bool {
        let Some(relative) = batch_id.get().checked_sub(self.first_batch_id) else {
            return false;
        };
        let Ok(relative) = usize::try_from(relative) else {
            return false;
        };
        relative < self.len
            && self.words[relative / u64::BITS as usize] & (1u64 << (relative % u64::BITS as usize))
                != 0
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FetchBounds {
    pub(crate) physical_end: LogicalOffset,
    pub(crate) ordered_end: LogicalOffset,
    pub(crate) replicated_end: LogicalOffset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VisibilityMode {
    Ordered,
    Replicated,
}

#[derive(Debug)]
pub(crate) struct PartitionState {
    sequencer: TopicSequencer,
    log_start: LogicalOffset,
    batches: DenseOrderedMap<BatchRuntime>,
    replication_policy: PartitionReplicationPolicy,
    next_contiguous_batch: u128,
    contiguous_log_end: LogicalOffset,
    next_stable_batch: u128,
    last_stable_offset: LogicalOffset,
    next_replicated_batch: u128,
    replicated_high_watermark: LogicalOffset,
    atomic_groups: HashMap<u128, AtomicGroupRuntime>,
    finalized_groups: HashMap<u128, AtomicGroupState>,
    producers: HashMap<u128, ProducerCursor>,
    dedup: HashMap<(u128, u32, u64), DedupRecord>,
    ordered_waiters: BTreeMap<u64, Vec<SyncSender<EngineResult<()>>>>,
    replicated_waiters: BTreeMap<u64, Vec<SyncSender<EngineResult<()>>>>,
}

impl PartitionState {
    fn empty(sequencer: TopicSequencer, replication_policy: PartitionReplicationPolicy) -> Self {
        Self {
            sequencer,
            log_start: LogicalOffset::new(0),
            batches: DenseOrderedMap::new(0),
            replication_policy,
            next_contiguous_batch: 0,
            contiguous_log_end: LogicalOffset::new(0),
            next_stable_batch: 0,
            last_stable_offset: LogicalOffset::new(0),
            next_replicated_batch: 0,
            replicated_high_watermark: LogicalOffset::new(0),
            atomic_groups: HashMap::new(),
            finalized_groups: HashMap::new(),
            producers: HashMap::new(),
            dedup: HashMap::new(),
            ordered_waiters: BTreeMap::new(),
            replicated_waiters: BTreeMap::new(),
        }
    }

    fn watermarks(&self) -> PartitionWatermarks {
        PartitionWatermarks {
            log_start: self.log_start,
            allocated_end: self.sequencer.next_offset(),
            contiguous_log_end: self.contiguous_log_end,
            replicated_high_watermark: self.replicated_high_watermark,
            last_stable_offset: self.last_stable_offset,
        }
    }

    fn fetch_bounds(&self, start_offset: LogicalOffset) -> EngineResult<FetchBounds> {
        if start_offset < self.log_start {
            return Err(EngineError::OffsetOutOfRange {
                requested: start_offset,
                log_start: self.log_start,
            });
        }
        Ok(FetchBounds {
            physical_end: self.contiguous_log_end,
            ordered_end: self.last_stable_offset,
            replicated_end: self.replicated_high_watermark.min(self.last_stable_offset),
        })
    }

    fn committed_candidates(
        &self,
        candidates: &[BatchId],
        include_unresolved: bool,
    ) -> EngineResult<CommittedBatchSet> {
        let Some(first) = candidates.iter().map(|batch_id| batch_id.get()).min() else {
            return Ok(CommittedBatchSet::new(0, 0));
        };
        let last = candidates
            .iter()
            .map(|batch_id| batch_id.get())
            .max()
            .expect("nonempty candidates have a maximum");
        let len = last
            .checked_sub(first)
            .and_then(|span| span.checked_add(1))
            .and_then(|span| usize::try_from(span).ok())
            .ok_or_else(|| {
                EngineError::InvalidConfig("fetch candidate window exceeds usize".into())
            })?;
        let mut committed = CommittedBatchSet::new(first, len);
        for batch_id in candidates {
            if self.batches.get(batch_id.get()).is_some_and(|batch| {
                batch.status == BatchStatus::Committed
                    || (include_unresolved && batch.status == BatchStatus::Unresolved)
            }) {
                committed.insert(*batch_id);
            }
        }
        Ok(committed)
    }

    fn advance_cached_watermarks(&mut self) {
        while let Some(batch) = self.batches.get(self.next_contiguous_batch) {
            if batch.reservation.first_offset != self.contiguous_log_end
                || batch.status == BatchStatus::Pending
            {
                break;
            }
            self.contiguous_log_end = next_offset(batch.reservation);
            self.next_contiguous_batch = self
                .next_contiguous_batch
                .checked_add(1)
                .expect("sequencer never exhausts the batch ID space");
        }

        while let Some(batch) = self.batches.get(self.next_stable_batch) {
            if batch.reservation.first_offset != self.last_stable_offset
                || matches!(batch.status, BatchStatus::Pending | BatchStatus::Unresolved)
            {
                break;
            }
            self.last_stable_offset = next_offset(batch.reservation);
            self.next_stable_batch = self
                .next_stable_batch
                .checked_add(1)
                .expect("sequencer never exhausts the batch ID space");
        }

        while let Some(batch) = self.batches.get(self.next_replicated_batch) {
            if batch.reservation.first_offset != self.replicated_high_watermark
                || batch.status == BatchStatus::Pending
                || (batch.status != BatchStatus::Aborted
                    && batch.durable_replicas < self.replication_policy.min_in_sync_replicas)
            {
                break;
            }
            self.replicated_high_watermark = next_offset(batch.reservation);
            self.next_replicated_batch = self
                .next_replicated_batch
                .checked_add(1)
                .expect("sequencer never exhausts the batch ID space");
        }
        Self::notify_waiters(&mut self.ordered_waiters, self.last_stable_offset);
        Self::notify_waiters(
            &mut self.replicated_waiters,
            self.replicated_high_watermark.min(self.last_stable_offset),
        );
    }

    fn wait_until_visible(
        &mut self,
        through: LogicalOffset,
        mode: VisibilityMode,
        response: SyncSender<EngineResult<()>>,
    ) {
        let (visible, waiters) = match mode {
            VisibilityMode::Ordered => (self.last_stable_offset, &mut self.ordered_waiters),
            VisibilityMode::Replicated => (
                self.replicated_high_watermark.min(self.last_stable_offset),
                &mut self.replicated_waiters,
            ),
        };
        if visible >= through {
            let _ = response.send(Ok(()));
        } else {
            waiters.entry(through.get()).or_default().push(response);
        }
    }

    fn notify_waiters(
        waiters: &mut BTreeMap<u64, Vec<SyncSender<EngineResult<()>>>>,
        visible: LogicalOffset,
    ) {
        let ready = waiters
            .range(..=visible.get())
            .map(|(offset, _)| *offset)
            .collect::<Vec<_>>();
        for offset in ready {
            if let Some(responses) = waiters.remove(&offset) {
                for response in responses {
                    let _ = response.send(Ok(()));
                }
            }
        }
    }

    fn reset_cached_watermarks_after_truncate(&mut self) {
        self.next_contiguous_batch = self.batches.first_key();
        self.contiguous_log_end = self.log_start;
        self.next_stable_batch = self.batches.first_key();
        self.last_stable_offset = self.log_start;
        self.next_replicated_batch = self.batches.first_key();
        self.replicated_high_watermark = self.log_start;
        self.advance_cached_watermarks();
    }

    fn compute_replicated_high_watermark(&self, min_in_sync_replicas: u32) -> LogicalOffset {
        let first_visible = self.first_batch_with_last_at_or_after(self.log_start);
        let mut replicated_end = self.log_start;
        for batch in self.batches.values_from(first_visible) {
            if batch.reservation.first_offset != replicated_end
                || batch.status == BatchStatus::Pending
                || (batch.status != BatchStatus::Aborted
                    && batch.durable_replicas < min_in_sync_replicas)
            {
                break;
            }
            replicated_end = next_offset(batch.reservation);
        }
        replicated_end
    }

    fn first_batch_with_last_at_or_after(&self, offset: LogicalOffset) -> u128 {
        self.batches
            .binary_search_by(|_, batch| {
                if batch.reservation.last_offset < offset {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            })
            .unwrap_or_else(|key| key)
    }

    fn batch_key_at_first_offset(&self, offset: LogicalOffset) -> Option<u128> {
        self.batches
            .binary_search_by(|_, batch| batch.reservation.first_offset.cmp(&offset))
            .ok()
    }
}

fn next_offset(reservation: Reservation) -> LogicalOffset {
    LogicalOffset::new(
        reservation
            .last_offset
            .get()
            .checked_add(1)
            .expect("sequencer never allocates an unrepresentable end offset"),
    )
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ReserveOutcome {
    Duplicate {
        response: AppendResponse,
        durable_replicas: u32,
        object_durable: bool,
        replication_policy: PartitionReplicationPolicy,
    },
    Reserved {
        reservation: Reservation,
        producer: Option<ProducerSequence>,
        replication_policy: PartitionReplicationPolicy,
    },
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CompleteAppend {
    pub reservation: Reservation,
    pub producer: Option<ProducerIdentity>,
    pub payload_digest: [u8; 16],
    pub durable_replicas: u32,
    pub object_durable: bool,
    pub response: AppendResponse,
}

enum ControlCommand {
    Append {
        event: JournalEvent,
        response: SyncSender<Result<(), String>>,
    },
    Sync {
        response: SyncSender<Result<(), String>>,
    },
}

#[derive(Clone)]
struct ControlJournalHandle {
    sender: SyncSender<ControlCommand>,
}

impl ControlJournalHandle {
    fn append(&self, event: JournalEvent) -> EngineResult<()> {
        let (response, receiver) = sync_channel(1);
        self.sender
            .send(ControlCommand::Append { event, response })
            .map_err(|_| coordinator_stopped("control journal"))?;
        receive_control(receiver)
    }

    fn sync(&self) -> EngineResult<()> {
        let (response, receiver) = sync_channel(1);
        self.sender
            .send(ControlCommand::Sync { response })
            .map_err(|_| coordinator_stopped("control journal"))?;
        receive_control(receiver)
    }
}

pub(crate) struct ControlJournal {
    handle: Option<ControlJournalHandle>,
    thread: Option<JoinHandle<()>>,
}

impl ControlJournal {
    pub(crate) fn spawn(mut journal: CoordinatorJournal) -> EngineResult<Self> {
        let (sender, receiver) = sync_channel(CONTROL_QUEUE_SLOTS);
        let thread = thread::Builder::new()
            .name("shard-stream-coordinator-journal".into())
            .spawn(move || {
                while let Ok(command) = receiver.recv() {
                    match command {
                        ControlCommand::Append { event, response } => {
                            let result = journal
                                .append(&event, true)
                                .map_err(|error| error.to_string());
                            let _ = response.send(result);
                        }
                        ControlCommand::Sync { response } => {
                            let result = journal.sync().map_err(|error| error.to_string());
                            let _ = response.send(result);
                        }
                    }
                }
            })
            .map_err(|error| {
                EngineError::InvalidConfig(format!(
                    "failed to spawn control journal worker: {error}"
                ))
            })?;
        Ok(Self {
            handle: Some(ControlJournalHandle { sender }),
            thread: Some(thread),
        })
    }

    fn handle(&self) -> ControlJournalHandle {
        self.handle
            .as_ref()
            .expect("control journal is active")
            .clone()
    }

    pub(crate) fn sync(&self) -> EngineResult<()> {
        self.handle().sync()
    }
}

impl Drop for ControlJournal {
    fn drop(&mut self) {
        self.handle.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

enum CoordinatorCommand {
    Create {
        topic_partition: TopicPartition,
        shards: Vec<ShardId>,
        replication_policy: PartitionReplicationPolicy,
        response: SyncSender<EngineResult<()>>,
    },
    Reserve {
        topic_partition: TopicPartition,
        request_id: u128,
        producer: Option<ProducerIdentity>,
        atomic_group: Option<AtomicGroupIdentity>,
        record_count: NonZeroU32,
        payload_bytes: u64,
        payload_digest: [u8; 16],
        durability: Durability,
        response: SyncSender<EngineResult<ReserveOutcome>>,
    },
    ReserveReplica {
        topic_partition: TopicPartition,
        reservation: Reservation,
        producer: Option<ProducerIdentity>,
        atomic_group: Option<AtomicGroupIdentity>,
        payload_bytes: u64,
        payload_digest: [u8; 16],
        response: SyncSender<EngineResult<ReserveOutcome>>,
    },
    ObserveLeaderEpoch {
        topic_partition: TopicPartition,
        leader_epoch: LeaderEpoch,
        response: SyncSender<EngineResult<()>>,
    },
    Complete {
        topic_partition: TopicPartition,
        completion: CompleteAppend,
        response: SyncSender<EngineResult<AppendResponse>>,
    },
    ReplicaCountDurable {
        topic_partition: TopicPartition,
        reservation: Reservation,
        durable_replicas: u32,
        response: SyncSender<EngineResult<()>>,
    },
    ReplicaCountDurableAsync {
        topic_partition: TopicPartition,
        reservation: Reservation,
        durable_replicas: u32,
    },
    Fail {
        topic_partition: TopicPartition,
        reservation: Reservation,
        producer: Option<ProducerIdentity>,
        definitely_not_written: bool,
        response: SyncSender<EngineResult<()>>,
    },
    FetchSnapshot {
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
        response: SyncSender<EngineResult<FetchBounds>>,
    },
    CommittedCandidates {
        topic_partition: TopicPartition,
        candidates: Vec<BatchId>,
        include_unresolved: bool,
        response: SyncSender<EngineResult<CommittedBatchSet>>,
    },
    Watermarks {
        topic_partition: TopicPartition,
        response: SyncSender<EngineResult<PartitionWatermarks>>,
    },
    ReplicationPolicy {
        topic_partition: TopicPartition,
        response: SyncSender<EngineResult<PartitionReplicationPolicy>>,
    },
    ProducerProgress {
        topic_partition: TopicPartition,
        producer_id: u128,
        response: SyncSender<EngineResult<Option<ProducerProgress>>>,
    },
    IdempotentReceipt {
        topic_partition: TopicPartition,
        producer: ProducerIdentity,
        response: SyncSender<EngineResult<Option<AppendResponse>>>,
    },
    PartitionEpochs {
        topic_partition: TopicPartition,
        response: SyncSender<EngineResult<PartitionEpochs>>,
    },
    InspectAtomicGroup {
        topic_partition: TopicPartition,
        transaction_id: u128,
        response: SyncSender<EngineResult<AtomicGroupInspection>>,
    },
    FinalizeAtomicGroup {
        topic_partition: TopicPartition,
        transaction_id: u128,
        outcome: AtomicGroupState,
        response: SyncSender<EngineResult<AtomicGroupInspection>>,
    },
    WaitUntilVisible {
        topic_partition: TopicPartition,
        through: LogicalOffset,
        mode: VisibilityMode,
        response: SyncSender<EngineResult<()>>,
    },
    ReplicaNextBatch {
        topic_partition: TopicPartition,
        response: SyncSender<EngineResult<BatchId>>,
    },
    Reconfigure {
        topic_partition: TopicPartition,
        ring_epoch: RingEpoch,
        shards: Vec<ShardId>,
        response: SyncSender<EngineResult<()>>,
    },
    UpdateReplicationPolicy {
        topic_partition: TopicPartition,
        expected_policy_generation: u64,
        replication_policy: PartitionReplicationPolicy,
        response: SyncSender<EngineResult<()>>,
    },
    Truncate {
        topic_partition: TopicPartition,
        log_start: LogicalOffset,
        response: SyncSender<EngineResult<PartitionWatermarks>>,
    },
    TopicPartitions {
        topic_id: TopicId,
        response: SyncSender<Vec<LogicalPartitionId>>,
    },
    AllPartitions {
        response: SyncSender<Vec<TopicPartition>>,
    },
}

struct CoordinatorWorker {
    sender: Option<SyncSender<CoordinatorCommand>>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for CoordinatorWorker {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub(crate) struct CoordinatorPool {
    workers: Vec<CoordinatorWorker>,
}

#[derive(Clone)]
pub(crate) struct CoordinatorRouter {
    senders: Vec<SyncSender<CoordinatorCommand>>,
}

impl CoordinatorRouter {
    pub(crate) fn replica_count_durable(&self, reservation: Reservation, durable_replicas: u32) {
        let topic_partition = TopicPartition::new(reservation.topic_id, reservation.partition_id);
        let index = coordinator_index(topic_partition, self.senders.len());
        let _ = self.senders[index].send(CoordinatorCommand::ReplicaCountDurableAsync {
            topic_partition,
            reservation,
            durable_replicas,
        });
    }
}

impl CoordinatorPool {
    pub(crate) fn spawn(
        worker_count: usize,
        mut recovered: HashMap<TopicPartition, PartitionState>,
        journal: &ControlJournal,
    ) -> EngineResult<Self> {
        let worker_count = worker_count.max(1);
        let mut worker_states = (0..worker_count)
            .map(|_| HashMap::new())
            .collect::<Vec<_>>();
        for (topic_partition, state) in recovered.drain() {
            let index = coordinator_index(topic_partition, worker_count);
            worker_states[index].insert(topic_partition, state);
        }

        let mut workers = Vec::with_capacity(worker_count);
        for (index, states) in worker_states.into_iter().enumerate() {
            let (sender, receiver) = sync_channel(COORDINATOR_QUEUE_SLOTS);
            let control = journal.handle();
            let thread = thread::Builder::new()
                .name(format!("shard-stream-coordinator-{index}"))
                .spawn(move || run_coordinator(states, receiver, control))
                .map_err(|error| {
                    EngineError::InvalidConfig(format!(
                        "failed to spawn partition coordinator: {error}"
                    ))
                })?;
            workers.push(CoordinatorWorker {
                sender: Some(sender),
                thread: Some(thread),
            });
        }
        Ok(Self { workers })
    }

    pub(crate) fn create_topic(
        &self,
        topic: TopicConfig,
        shards: Vec<ShardId>,
        replication_policy: PartitionReplicationPolicy,
    ) -> EngineResult<()> {
        for partition_id in 0..topic.partitions {
            let topic_partition =
                TopicPartition::new(topic.topic_id, LogicalPartitionId::new(partition_id));
            let (response, receiver) = sync_channel(1);
            self.send(
                topic_partition,
                CoordinatorCommand::Create {
                    topic_partition,
                    shards: shards.clone(),
                    replication_policy,
                    response,
                },
            )?;
            receive(receiver)?;
        }
        Ok(())
    }

    pub(crate) fn router(&self) -> CoordinatorRouter {
        CoordinatorRouter {
            senders: self
                .workers
                .iter()
                .map(|worker| {
                    worker
                        .sender
                        .as_ref()
                        .expect("coordinator pool is active")
                        .clone()
                })
                .collect(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reserve(
        &self,
        topic_partition: TopicPartition,
        request_id: u128,
        producer: Option<ProducerIdentity>,
        atomic_group: Option<AtomicGroupIdentity>,
        record_count: NonZeroU32,
        payload_bytes: u64,
        payload_digest: [u8; 16],
        durability: Durability,
    ) -> EngineResult<ReserveOutcome> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::Reserve {
                topic_partition,
                request_id,
                producer,
                atomic_group,
                record_count,
                payload_bytes,
                payload_digest,
                durability,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn reserve_replica(
        &self,
        topic_partition: TopicPartition,
        reservation: Reservation,
        producer: Option<ProducerIdentity>,
        atomic_group: Option<AtomicGroupIdentity>,
        payload_bytes: u64,
        payload_digest: [u8; 16],
    ) -> EngineResult<ReserveOutcome> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::ReserveReplica {
                topic_partition,
                reservation,
                producer,
                atomic_group,
                payload_bytes,
                payload_digest,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn observe_leader_epoch(
        &self,
        topic_partition: TopicPartition,
        leader_epoch: LeaderEpoch,
    ) -> EngineResult<()> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::ObserveLeaderEpoch {
                topic_partition,
                leader_epoch,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn record_durable_replicas(
        &self,
        topic_partition: TopicPartition,
        reservation: Reservation,
        durable_replicas: u32,
    ) -> EngineResult<()> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::ReplicaCountDurable {
                topic_partition,
                reservation,
                durable_replicas,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn complete(
        &self,
        topic_partition: TopicPartition,
        completion: CompleteAppend,
    ) -> EngineResult<AppendResponse> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::Complete {
                topic_partition,
                completion,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn fail(
        &self,
        topic_partition: TopicPartition,
        reservation: Reservation,
        producer: Option<ProducerIdentity>,
        definitely_not_written: bool,
    ) -> EngineResult<()> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::Fail {
                topic_partition,
                reservation,
                producer,
                definitely_not_written,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn fetch_snapshot(
        &self,
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
    ) -> EngineResult<FetchBounds> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::FetchSnapshot {
                topic_partition,
                start_offset,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn committed_candidates(
        &self,
        topic_partition: TopicPartition,
        candidates: Vec<BatchId>,
        include_unresolved: bool,
    ) -> EngineResult<CommittedBatchSet> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::CommittedCandidates {
                topic_partition,
                candidates,
                include_unresolved,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn watermarks(
        &self,
        topic_partition: TopicPartition,
    ) -> EngineResult<PartitionWatermarks> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::Watermarks {
                topic_partition,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn replication_policy(
        &self,
        topic_partition: TopicPartition,
    ) -> EngineResult<PartitionReplicationPolicy> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::ReplicationPolicy {
                topic_partition,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn producer_progress(
        &self,
        topic_partition: TopicPartition,
        producer_id: u128,
    ) -> EngineResult<Option<ProducerProgress>> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::ProducerProgress {
                topic_partition,
                producer_id,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn idempotent_receipt(
        &self,
        topic_partition: TopicPartition,
        producer: ProducerIdentity,
    ) -> EngineResult<Option<AppendResponse>> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::IdempotentReceipt {
                topic_partition,
                producer,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn partition_epochs(
        &self,
        topic_partition: TopicPartition,
    ) -> EngineResult<PartitionEpochs> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::PartitionEpochs {
                topic_partition,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn inspect_atomic_group(
        &self,
        topic_partition: TopicPartition,
        transaction_id: u128,
    ) -> EngineResult<AtomicGroupInspection> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::InspectAtomicGroup {
                topic_partition,
                transaction_id,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn finalize_atomic_group(
        &self,
        topic_partition: TopicPartition,
        transaction_id: u128,
        outcome: AtomicGroupState,
    ) -> EngineResult<AtomicGroupInspection> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::FinalizeAtomicGroup {
                topic_partition,
                transaction_id,
                outcome,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn wait_until_visible(
        &self,
        topic_partition: TopicPartition,
        through: LogicalOffset,
        mode: VisibilityMode,
    ) -> EngineResult<()> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::WaitUntilVisible {
                topic_partition,
                through,
                mode,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn replica_next_batch(
        &self,
        topic_partition: TopicPartition,
    ) -> EngineResult<BatchId> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::ReplicaNextBatch {
                topic_partition,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn reconfigure(
        &self,
        topic_partition: TopicPartition,
        ring_epoch: RingEpoch,
        shards: Vec<ShardId>,
    ) -> EngineResult<()> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::Reconfigure {
                topic_partition,
                ring_epoch,
                shards,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn update_replication_policy(
        &self,
        topic_partition: TopicPartition,
        expected_policy_generation: u64,
        replication_policy: PartitionReplicationPolicy,
    ) -> EngineResult<()> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::UpdateReplicationPolicy {
                topic_partition,
                expected_policy_generation,
                replication_policy,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn truncate(
        &self,
        topic_partition: TopicPartition,
        log_start: LogicalOffset,
    ) -> EngineResult<PartitionWatermarks> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::Truncate {
                topic_partition,
                log_start,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn topic_partitions(&self, topic_id: TopicId) -> Vec<LogicalPartitionId> {
        let mut partitions = Vec::new();
        for worker in &self.workers {
            let (response, receiver) = sync_channel(1);
            let Some(sender) = &worker.sender else {
                continue;
            };
            if sender
                .send(CoordinatorCommand::TopicPartitions { topic_id, response })
                .is_ok()
                && let Ok(mut worker_partitions) = receiver.recv()
            {
                partitions.append(&mut worker_partitions);
            }
        }
        partitions.sort_unstable();
        partitions
    }

    pub(crate) fn all_partitions(&self) -> Vec<TopicPartition> {
        let mut partitions = Vec::new();
        for worker in &self.workers {
            let (response, receiver) = sync_channel(1);
            let Some(sender) = &worker.sender else {
                continue;
            };
            if sender
                .send(CoordinatorCommand::AllPartitions { response })
                .is_ok()
                && let Ok(mut worker_partitions) = receiver.recv()
            {
                partitions.append(&mut worker_partitions);
            }
        }
        partitions.sort_unstable();
        partitions
    }

    fn send(
        &self,
        topic_partition: TopicPartition,
        command: CoordinatorCommand,
    ) -> EngineResult<()> {
        let index = coordinator_index(topic_partition, self.workers.len());
        self.workers[index]
            .sender
            .as_ref()
            .ok_or_else(|| coordinator_stopped("partition coordinator"))?
            .send(command)
            .map_err(|_| coordinator_stopped("partition coordinator"))
    }
}

fn run_coordinator(
    mut states: HashMap<TopicPartition, PartitionState>,
    receiver: Receiver<CoordinatorCommand>,
    journal: ControlJournalHandle,
) {
    while let Ok(command) = receiver.recv() {
        match command {
            CoordinatorCommand::Create {
                topic_partition,
                shards,
                replication_policy,
                response,
            } => {
                let result = create_partition(
                    &mut states,
                    &journal,
                    topic_partition,
                    shards,
                    replication_policy,
                );
                let _ = response.send(result);
            }
            CoordinatorCommand::ReplicaCountDurable {
                topic_partition,
                reservation,
                durable_replicas,
                response,
            } => {
                let result = state_mut(&mut states, topic_partition).and_then(|state| {
                    set_durable_replica_count(state, reservation, durable_replicas)
                });
                let _ = response.send(result);
            }
            CoordinatorCommand::ReplicaCountDurableAsync {
                topic_partition,
                reservation,
                durable_replicas,
            } => {
                let _ = state_mut(&mut states, topic_partition).and_then(|state| {
                    set_durable_replica_count(state, reservation, durable_replicas)
                });
            }
            CoordinatorCommand::Reserve {
                topic_partition,
                request_id,
                producer,
                atomic_group,
                record_count,
                payload_bytes,
                payload_digest,
                durability,
                response,
            } => {
                let result = state_mut(&mut states, topic_partition).and_then(|state| {
                    reserve(
                        state,
                        request_id,
                        producer,
                        atomic_group,
                        record_count,
                        payload_bytes,
                        payload_digest,
                        durability,
                    )
                });
                let _ = response.send(result);
            }
            CoordinatorCommand::ReserveReplica {
                topic_partition,
                reservation,
                producer,
                atomic_group,
                payload_bytes,
                payload_digest,
                response,
            } => {
                let result = state_mut(&mut states, topic_partition).and_then(|state| {
                    reserve_replica(
                        state,
                        reservation,
                        producer,
                        atomic_group,
                        payload_bytes,
                        payload_digest,
                    )
                });
                let _ = response.send(result);
            }
            CoordinatorCommand::ObserveLeaderEpoch {
                topic_partition,
                leader_epoch,
                response,
            } => {
                let result = state_mut(&mut states, topic_partition).and_then(|state| {
                    state.sequencer.observe_leader_epoch(leader_epoch)?;
                    Ok(())
                });
                let _ = response.send(result);
            }
            CoordinatorCommand::Complete {
                topic_partition,
                completion,
                response,
            } => {
                let result = state_mut(&mut states, topic_partition)
                    .and_then(|state| complete(state, completion));
                let _ = response.send(result);
            }
            CoordinatorCommand::Fail {
                topic_partition,
                reservation,
                producer,
                definitely_not_written,
                response,
            } => {
                let result = state_mut(&mut states, topic_partition)
                    .and_then(|state| fail(state, reservation, producer, definitely_not_written));
                let _ = response.send(result);
            }
            CoordinatorCommand::FetchSnapshot {
                topic_partition,
                start_offset,
                response,
            } => {
                let result = state(&states, topic_partition)
                    .and_then(|state| state.fetch_bounds(start_offset));
                let _ = response.send(result);
            }
            CoordinatorCommand::CommittedCandidates {
                topic_partition,
                candidates,
                include_unresolved,
                response,
            } => {
                let result = state(&states, topic_partition)
                    .and_then(|state| state.committed_candidates(&candidates, include_unresolved));
                let _ = response.send(result);
            }
            CoordinatorCommand::Watermarks {
                topic_partition,
                response,
            } => {
                let result = state(&states, topic_partition).map(PartitionState::watermarks);
                let _ = response.send(result);
            }
            CoordinatorCommand::ReplicationPolicy {
                topic_partition,
                response,
            } => {
                let result = state(&states, topic_partition).map(|state| state.replication_policy);
                let _ = response.send(result);
            }
            CoordinatorCommand::ProducerProgress {
                topic_partition,
                producer_id,
                response,
            } => {
                let result = state(&states, topic_partition).map(|state| {
                    state
                        .producers
                        .get(&producer_id)
                        .map(|cursor| ProducerProgress {
                            epoch: cursor.epoch,
                            next_sequence: cursor.next_sequence,
                            requires_new_epoch: cursor.failed,
                        })
                });
                let _ = response.send(result);
            }
            CoordinatorCommand::IdempotentReceipt {
                topic_partition,
                producer,
                response,
            } => {
                let result = state(&states, topic_partition)
                    .and_then(|state| idempotent_receipt(state, producer));
                let _ = response.send(result);
            }
            CoordinatorCommand::PartitionEpochs {
                topic_partition,
                response,
            } => {
                let result = state(&states, topic_partition).map(|state| {
                    let sequencer = state.sequencer.state();
                    PartitionEpochs {
                        ring_epoch: sequencer.ring_epoch,
                        leader_epoch: sequencer.leader_epoch,
                    }
                });
                let _ = response.send(result);
            }
            CoordinatorCommand::InspectAtomicGroup {
                topic_partition,
                transaction_id,
                response,
            } => {
                let result = state(&states, topic_partition)
                    .and_then(|state| inspect_atomic_group(state, transaction_id));
                let _ = response.send(result);
            }
            CoordinatorCommand::FinalizeAtomicGroup {
                topic_partition,
                transaction_id,
                outcome,
                response,
            } => {
                let result = state_mut(&mut states, topic_partition).and_then(|state| {
                    finalize_atomic_group(state, &journal, topic_partition, transaction_id, outcome)
                });
                let _ = response.send(result);
            }
            CoordinatorCommand::WaitUntilVisible {
                topic_partition,
                through,
                mode,
                response,
            } => match state_mut(&mut states, topic_partition) {
                Ok(state) => state.wait_until_visible(through, mode, response),
                Err(error) => {
                    let _ = response.send(Err(error));
                }
            },
            CoordinatorCommand::ReplicaNextBatch {
                topic_partition,
                response,
            } => {
                let result = states
                    .get(&topic_partition)
                    .map(|state| BatchId::new(state.sequencer.state().next_batch_id))
                    .ok_or(EngineError::UnknownPartition {
                        topic_id: topic_partition.topic_id,
                        partition_id: topic_partition.partition_id,
                    });
                let _ = response.send(result);
            }
            CoordinatorCommand::Reconfigure {
                topic_partition,
                ring_epoch,
                shards,
                response,
            } => {
                let result = state_mut(&mut states, topic_partition).and_then(|state| {
                    let mut candidate = TopicSequencer::restore(state.sequencer.state())?;
                    candidate.reconfigure(ring_epoch, shards.clone())?;
                    journal.append(JournalEvent::Configure {
                        topic_partition,
                        ring_epoch,
                        shards,
                    })?;
                    state.sequencer = candidate;
                    Ok(())
                });
                let _ = response.send(result);
            }
            CoordinatorCommand::UpdateReplicationPolicy {
                topic_partition,
                expected_policy_generation,
                replication_policy,
                response,
            } => {
                let result = state_mut(&mut states, topic_partition).and_then(|state| {
                    update_replication_policy(
                        state,
                        &journal,
                        topic_partition,
                        expected_policy_generation,
                        replication_policy,
                    )
                });
                let _ = response.send(result);
            }
            CoordinatorCommand::Truncate {
                topic_partition,
                log_start,
                response,
            } => {
                let result = state_mut(&mut states, topic_partition)
                    .and_then(|state| truncate(state, &journal, topic_partition, log_start));
                let _ = response.send(result);
            }
            CoordinatorCommand::TopicPartitions { topic_id, response } => {
                let partitions = states
                    .keys()
                    .filter(|topic_partition| topic_partition.topic_id == topic_id)
                    .map(|topic_partition| topic_partition.partition_id)
                    .collect();
                let _ = response.send(partitions);
            }
            CoordinatorCommand::AllPartitions { response } => {
                let partitions = states.keys().copied().collect();
                let _ = response.send(partitions);
            }
        }
    }
}

fn create_partition(
    states: &mut HashMap<TopicPartition, PartitionState>,
    journal: &ControlJournalHandle,
    topic_partition: TopicPartition,
    shards: Vec<ShardId>,
    replication_policy: PartitionReplicationPolicy,
) -> EngineResult<()> {
    if let Some(existing) = states.get(&topic_partition) {
        let existing = existing.sequencer.state();
        if existing.ring_epoch == RingEpoch::new(1)
            && existing.virtual_lanes == shards
            && states
                .get(&topic_partition)
                .is_some_and(|state| state.replication_policy == replication_policy)
        {
            return Ok(());
        }
        return Err(EngineError::TopicAlreadyExists(topic_partition.topic_id));
    }
    let sequencer = TopicSequencer::new(
        topic_partition.topic_id,
        topic_partition.partition_id,
        RingEpoch::new(1),
        shards.clone(),
    )?;
    journal.append(JournalEvent::CreatePartition {
        topic_partition,
        ring_epoch: RingEpoch::new(1),
        shards,
        policy_generation: replication_policy.policy_generation,
        replication_factor: replication_policy.replication_factor,
        min_in_sync_replicas: replication_policy.min_in_sync_replicas,
    })?;
    states.insert(
        topic_partition,
        PartitionState::empty(sequencer, replication_policy),
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn reserve(
    state: &mut PartitionState,
    request_id: u128,
    producer: Option<ProducerIdentity>,
    atomic_group: Option<AtomicGroupIdentity>,
    record_count: NonZeroU32,
    payload_bytes: u64,
    payload_digest: [u8; 16],
    durability: Durability,
) -> EngineResult<ReserveOutcome> {
    if let Some((response, durable_replicas, object_durable)) = check_producer(
        state,
        request_id,
        producer,
        record_count,
        payload_digest,
        durability,
    )? {
        return Ok(ReserveOutcome::Duplicate {
            response,
            durable_replicas,
            object_durable,
            replication_policy: state.replication_policy,
        });
    }
    if let Some(group) = atomic_group
        && let Some(duplicate) = check_atomic_group(
            state,
            request_id,
            group,
            record_count,
            payload_bytes,
            payload_digest,
        )?
    {
        return Ok(duplicate);
    }
    let previous = state.sequencer.state();
    let reservation = state.sequencer.reserve(record_count)?;
    if let Some(identity) = producer
        && let Err(error) = advance_producer(state, identity, record_count)
    {
        state.sequencer =
            TopicSequencer::restore(previous).expect("previous sequencer state was valid");
        return Err(error);
    }
    let producer_sequence = producer.map(|identity| ProducerSequence {
        producer_id: identity.producer_id,
        epoch: identity.epoch,
        first_sequence: identity.first_sequence,
        event_id: identity.event_id,
        digest_algorithm: DIGEST_XXH3_128,
        payload_digest,
    });
    append_batch_runtime(
        state,
        BatchRuntime {
            reservation,
            status: BatchStatus::Pending,
            durable_replicas: 0,
            replica_acks: 0,
            producer,
            atomic_group,
            payload_digest: Some(payload_digest),
            payload_bytes,
            object_durable: false,
        },
    )?;
    if let Some(group) = atomic_group {
        register_atomic_chunk(state, group, reservation, payload_bytes)?;
    }
    Ok(ReserveOutcome::Reserved {
        reservation,
        producer: producer_sequence,
        replication_policy: state.replication_policy,
    })
}

fn complete(
    state: &mut PartitionState,
    mut completion: CompleteAppend,
) -> EngineResult<AppendResponse> {
    let status = state
        .batches
        .get(completion.reservation.batch_id.get())
        .and_then(|batch| batch.atomic_group)
        .map_or(BatchStatus::Committed, |_| BatchStatus::Unresolved);
    let durable_replicas = mark_batch(
        state,
        completion.reservation,
        status,
        completion.durable_replicas,
    )?;
    if let Some(batch) = state.batches.get_mut(completion.reservation.batch_id.get()) {
        batch.object_durable = completion.object_durable;
    }
    completion.response.visible_through = state.last_stable_offset;
    if let Some(producer) = completion.producer {
        state.dedup.insert(
            (
                producer.producer_id,
                producer.epoch,
                producer.first_sequence,
            ),
            DedupRecord {
                digest_algorithm: DIGEST_XXH3_128,
                payload_digest: completion.payload_digest,
                event_id: producer.event_id,
                record_count: completion.reservation.record_count.get(),
                durable_replicas,
                object_durable: completion.object_durable,
                response: completion.response,
            },
        );
    }
    Ok(completion.response)
}

fn check_atomic_group(
    state: &PartitionState,
    request_id: u128,
    identity: AtomicGroupIdentity,
    record_count: NonZeroU32,
    payload_bytes: u64,
    payload_digest: [u8; 16],
) -> EngineResult<Option<ReserveOutcome>> {
    if state
        .finalized_groups
        .contains_key(&identity.transaction_id)
    {
        return Err(EngineError::AtomicGroupFinalized(identity.transaction_id));
    }
    let Some(group) = state.atomic_groups.get(&identity.transaction_id) else {
        if identity.chunk_sequence == 0 {
            return Ok(None);
        }
        return Err(EngineError::AtomicGroupSequenceOutOfOrder {
            transaction_id: identity.transaction_id,
            expected: 0,
            received: identity.chunk_sequence,
        });
    };
    if group.state != AtomicGroupState::Unresolved {
        return Err(EngineError::AtomicGroupFinalized(identity.transaction_id));
    }
    if identity.chunk_sequence > group.next_sequence {
        return Err(EngineError::AtomicGroupSequenceOutOfOrder {
            transaction_id: identity.transaction_id,
            expected: group.next_sequence,
            received: identity.chunk_sequence,
        });
    }
    if identity.chunk_sequence == group.next_sequence {
        return Ok(None);
    }
    let batch_id = *group
        .batch_ids
        .get(identity.chunk_sequence as usize)
        .ok_or_else(|| {
            EngineError::CorruptState(format!(
                "atomic group {} is missing chunk {}",
                identity.transaction_id, identity.chunk_sequence
            ))
        })?;
    let batch = state.batches.get(batch_id).ok_or_else(|| {
        EngineError::CorruptState(format!(
            "atomic group {} references missing batch {batch_id}",
            identity.transaction_id
        ))
    })?;
    if batch.atomic_group != Some(identity)
        || batch.reservation.record_count != record_count
        || batch.payload_bytes != payload_bytes
        || batch.payload_digest != Some(payload_digest)
    {
        return Err(EngineError::AtomicGroupChunkMismatch(
            identity.transaction_id,
        ));
    }
    match batch.status {
        BatchStatus::Pending => Err(EngineError::ReplicaInFlight(batch.reservation.batch_id)),
        BatchStatus::Committed | BatchStatus::Unresolved => Ok(Some(ReserveOutcome::Duplicate {
            response: append_response(request_id, batch.reservation),
            durable_replicas: batch.durable_replicas,
            object_durable: batch.object_durable,
            replication_policy: state.replication_policy,
        })),
        BatchStatus::Aborted => Err(EngineError::CorruptState(format!(
            "atomic group {} retains aborted batch {}",
            identity.transaction_id, batch.reservation.batch_id
        ))),
    }
}

fn register_atomic_chunk(
    state: &mut PartitionState,
    identity: AtomicGroupIdentity,
    reservation: Reservation,
    payload_bytes: u64,
) -> EngineResult<()> {
    let group = state
        .atomic_groups
        .entry(identity.transaction_id)
        .or_insert_with(|| AtomicGroupRuntime {
            state: AtomicGroupState::Unresolved,
            batch_ids: Vec::new(),
            next_sequence: 0,
            payload_bytes: 0,
            record_count: 0,
        });
    if group.state != AtomicGroupState::Unresolved {
        return Err(EngineError::AtomicGroupFinalized(identity.transaction_id));
    }
    if identity.chunk_sequence != group.next_sequence {
        return Err(EngineError::AtomicGroupSequenceOutOfOrder {
            transaction_id: identity.transaction_id,
            expected: group.next_sequence,
            received: identity.chunk_sequence,
        });
    }
    group.payload_bytes = group
        .payload_bytes
        .checked_add(payload_bytes)
        .ok_or_else(|| EngineError::InvalidConfig("atomic group byte count overflow".into()))?;
    group.record_count = group
        .record_count
        .checked_add(u64::from(reservation.record_count.get()))
        .ok_or_else(|| EngineError::InvalidConfig("atomic group record count overflow".into()))?;
    group.batch_ids.push(reservation.batch_id.get());
    group.next_sequence = group
        .next_sequence
        .checked_add(1)
        .ok_or_else(|| EngineError::InvalidConfig("atomic group sequence exhausted".into()))?;
    Ok(())
}

fn reserve_replica(
    state: &mut PartitionState,
    reservation: Reservation,
    producer: Option<ProducerIdentity>,
    atomic_group: Option<AtomicGroupIdentity>,
    payload_bytes: u64,
    payload_digest: [u8; 16],
) -> EngineResult<ReserveOutcome> {
    if let Some(existing) = state.batches.get_mut(reservation.batch_id.get()) {
        let identity_matches = existing.reservation == reservation
            && existing.producer == producer
            && existing.atomic_group == atomic_group
            && existing.payload_bytes == payload_bytes
            && existing
                .payload_digest
                .is_none_or(|existing| existing == payload_digest);
        if !identity_matches {
            return Err(EngineError::CorruptState(format!(
                "replica reservation conflicts with batch {}",
                reservation.batch_id
            )));
        }
        match existing.status {
            BatchStatus::Committed | BatchStatus::Unresolved => {
                return Ok(ReserveOutcome::Duplicate {
                    response: append_response(0, reservation),
                    durable_replicas: existing.durable_replicas,
                    object_durable: existing.object_durable,
                    replication_policy: state.replication_policy,
                });
            }
            BatchStatus::Pending => {
                return Err(EngineError::ReplicaInFlight(reservation.batch_id));
            }
            BatchStatus::Aborted => {
                existing.status = BatchStatus::Pending;
                existing.producer = producer;
                existing.atomic_group = atomic_group;
                existing.payload_digest = Some(payload_digest);
                existing.payload_bytes = payload_bytes;
            }
        }
        if let Some(producer) = producer
            && let Some(cursor) = state.producers.get_mut(&producer.producer_id)
            && cursor.epoch == producer.epoch
        {
            cursor.failed = false;
        }
        if let Some(group) = atomic_group {
            register_atomic_chunk(state, group, reservation, payload_bytes)?;
        }
        state.reset_cached_watermarks_after_truncate();
        return Ok(ReserveOutcome::Reserved {
            reservation,
            producer: producer.map(|identity| ProducerSequence {
                producer_id: identity.producer_id,
                epoch: identity.epoch,
                first_sequence: identity.first_sequence,
                event_id: identity.event_id,
                digest_algorithm: DIGEST_XXH3_128,
                payload_digest,
            }),
            replication_policy: state.replication_policy,
        });
    }
    if let Some(group) = atomic_group
        && check_atomic_group(
            state,
            0,
            group,
            reservation.record_count,
            payload_bytes,
            payload_digest,
        )?
        .is_some()
    {
        return Err(EngineError::Fenced(format!(
            "replica reservation {} reuses atomic group {} chunk {} with a different batch",
            reservation.batch_id, group.transaction_id, group.chunk_sequence
        )));
    }
    let previous = state.sequencer.state();
    let expected = state.sequencer.reserve(reservation.record_count)?;
    if expected != reservation {
        state.sequencer =
            TopicSequencer::restore(previous).expect("previous follower sequencer state was valid");
        return Err(EngineError::Fenced(format!(
            "replica reservation {} is not the exact next fenced placement",
            reservation.batch_id
        )));
    }
    if let Some(identity) = producer
        && let Err(error) = advance_producer(state, identity, reservation.record_count)
    {
        state.sequencer =
            TopicSequencer::restore(previous).expect("previous follower sequencer state was valid");
        return Err(error);
    }
    append_batch_runtime(
        state,
        BatchRuntime {
            reservation,
            status: BatchStatus::Pending,
            durable_replicas: 0,
            replica_acks: 0,
            producer,
            atomic_group,
            payload_digest: Some(payload_digest),
            payload_bytes,
            object_durable: false,
        },
    )?;
    if let Some(group) = atomic_group {
        register_atomic_chunk(state, group, reservation, payload_bytes)?;
    }
    Ok(ReserveOutcome::Reserved {
        reservation,
        producer: producer.map(|identity| ProducerSequence {
            producer_id: identity.producer_id,
            epoch: identity.epoch,
            first_sequence: identity.first_sequence,
            event_id: identity.event_id,
            digest_algorithm: DIGEST_XXH3_128,
            payload_digest,
        }),
        replication_policy: state.replication_policy,
    })
}

fn set_durable_replica_count(
    state: &mut PartitionState,
    reservation: Reservation,
    durable_replicas: u32,
) -> EngineResult<()> {
    let batch = state
        .batches
        .get_mut(reservation.batch_id.get())
        .ok_or_else(|| {
            EngineError::CorruptState(format!(
                "replica progress references missing batch {}",
                reservation.batch_id
            ))
        })?;
    if batch.reservation != reservation
        || !matches!(
            batch.status,
            BatchStatus::Committed | BatchStatus::Unresolved
        )
    {
        return Err(EngineError::CorruptState(format!(
            "invalid replica count for batch {}",
            reservation.batch_id
        )));
    }
    batch.durable_replicas = batch.durable_replicas.max(durable_replicas);
    state.advance_cached_watermarks();
    for record in state.dedup.values_mut() {
        if record.response.batch_id == reservation.batch_id {
            record.durable_replicas = record.durable_replicas.max(durable_replicas);
        }
    }
    Ok(())
}

fn fail(
    state: &mut PartitionState,
    reservation: Reservation,
    producer: Option<ProducerIdentity>,
    definitely_not_written: bool,
) -> EngineResult<()> {
    if let Some(producer) = producer
        && let Some(cursor) = state.producers.get_mut(&producer.producer_id)
    {
        cursor.failed = true;
    }
    if definitely_not_written {
        mark_batch(state, reservation, BatchStatus::Aborted, 0)?;
        rollback_atomic_chunk(state, reservation)?;
    }
    Ok(())
}

fn rollback_atomic_chunk(state: &mut PartitionState, reservation: Reservation) -> EngineResult<()> {
    let Some(identity) = state
        .batches
        .get(reservation.batch_id.get())
        .and_then(|batch| batch.atomic_group)
    else {
        return Ok(());
    };
    let payload_bytes = state
        .batches
        .get(reservation.batch_id.get())
        .map_or(0, |batch| batch.payload_bytes);
    let group = state
        .atomic_groups
        .get_mut(&identity.transaction_id)
        .ok_or(EngineError::UnknownAtomicGroup(identity.transaction_id))?;
    if group.batch_ids.last().copied() != Some(reservation.batch_id.get())
        || group.next_sequence != identity.chunk_sequence.saturating_add(1)
    {
        return Err(EngineError::CorruptState(format!(
            "cannot roll back non-tail chunk {} from atomic group {}",
            identity.chunk_sequence, identity.transaction_id
        )));
    }
    group.batch_ids.pop();
    group.next_sequence = identity.chunk_sequence;
    group.payload_bytes = group
        .payload_bytes
        .checked_sub(payload_bytes)
        .ok_or_else(|| EngineError::CorruptState("atomic group byte count underflow".into()))?;
    group.record_count = group
        .record_count
        .checked_sub(u64::from(reservation.record_count.get()))
        .ok_or_else(|| EngineError::CorruptState("atomic group record count underflow".into()))?;
    if group.batch_ids.is_empty() {
        state.atomic_groups.remove(&identity.transaction_id);
    }
    Ok(())
}

fn inspect_atomic_group(
    state: &PartitionState,
    transaction_id: u128,
) -> EngineResult<AtomicGroupInspection> {
    let group = state
        .atomic_groups
        .get(&transaction_id)
        .ok_or(EngineError::UnknownAtomicGroup(transaction_id))?;
    let first = group
        .batch_ids
        .first()
        .and_then(|batch_id| state.batches.get(*batch_id))
        .ok_or_else(|| {
            EngineError::CorruptState(format!("atomic group {transaction_id} has no chunks"))
        })?;
    let last = group
        .batch_ids
        .last()
        .and_then(|batch_id| state.batches.get(*batch_id))
        .ok_or_else(|| {
            EngineError::CorruptState(format!("atomic group {transaction_id} has no chunks"))
        })?;
    Ok(AtomicGroupInspection {
        transaction_id,
        state: group.state,
        chunk_count: group.next_sequence,
        record_count: group.record_count,
        payload_bytes: group.payload_bytes,
        first_offset: first.reservation.first_offset,
        last_offset: last.reservation.last_offset,
    })
}

fn finalize_atomic_group(
    state: &mut PartitionState,
    journal: &ControlJournalHandle,
    topic_partition: TopicPartition,
    transaction_id: u128,
    outcome: AtomicGroupState,
) -> EngineResult<AtomicGroupInspection> {
    if outcome == AtomicGroupState::Unresolved {
        return Err(EngineError::InvalidConfig(
            "an atomic group can only commit or abort".into(),
        ));
    }
    let group = state
        .atomic_groups
        .get(&transaction_id)
        .ok_or(EngineError::UnknownAtomicGroup(transaction_id))?;
    if group.state == outcome {
        return inspect_atomic_group(state, transaction_id);
    }
    if group.state != AtomicGroupState::Unresolved {
        return Err(EngineError::AtomicGroupFinalized(transaction_id));
    }
    for batch_id in &group.batch_ids {
        let batch = state.batches.get(*batch_id).ok_or_else(|| {
            EngineError::CorruptState(format!(
                "atomic group {transaction_id} references missing batch {batch_id}"
            ))
        })?;
        if batch.status != BatchStatus::Unresolved {
            return Err(EngineError::ReplicaInFlight(batch.reservation.batch_id));
        }
        if outcome == AtomicGroupState::Committed
            && batch.durable_replicas < state.replication_policy.min_in_sync_replicas
        {
            return Err(EngineError::DurabilityUnavailable {
                required_replicas: state.replication_policy.min_in_sync_replicas,
                durable_replicas: batch.durable_replicas,
            });
        }
    }
    let marker = match outcome {
        AtomicGroupState::Committed => JournalEvent::GroupCommit {
            topic_partition,
            transaction_id,
        },
        AtomicGroupState::Aborted => JournalEvent::GroupAbort {
            topic_partition,
            transaction_id,
        },
        AtomicGroupState::Unresolved => unreachable!(),
    };
    journal.append(marker)?;
    let batch_ids = group.batch_ids.clone();
    for batch_id in batch_ids {
        let batch = state
            .batches
            .get_mut(batch_id)
            .expect("validated atomic group batch exists");
        batch.status = match outcome {
            AtomicGroupState::Committed => BatchStatus::Committed,
            AtomicGroupState::Aborted => BatchStatus::Aborted,
            AtomicGroupState::Unresolved => unreachable!(),
        };
    }
    state
        .atomic_groups
        .get_mut(&transaction_id)
        .expect("validated atomic group exists")
        .state = outcome;
    state.finalized_groups.insert(transaction_id, outcome);
    state.advance_cached_watermarks();
    inspect_atomic_group(state, transaction_id)
}

fn update_replication_policy(
    state: &mut PartitionState,
    journal: &ControlJournalHandle,
    topic_partition: TopicPartition,
    expected_policy_generation: u64,
    replication_policy: PartitionReplicationPolicy,
) -> EngineResult<()> {
    if state.replication_policy == replication_policy {
        return Ok(());
    }
    if state.replication_policy.policy_generation != expected_policy_generation {
        return Err(EngineError::Fenced(format!(
            "replication policy generation changed from expected {expected_policy_generation} to {}",
            state.replication_policy.policy_generation
        )));
    }
    if replication_policy.policy_generation <= expected_policy_generation {
        return Err(EngineError::InvalidConfig(
            "replication policy generation must advance".into(),
        ));
    }
    let target_replicated = state
        .compute_replicated_high_watermark(replication_policy.min_in_sync_replicas)
        .min(state.last_stable_offset);
    if target_replicated < state.last_stable_offset {
        return Err(EngineError::DurabilityUnavailable {
            required_replicas: replication_policy.min_in_sync_replicas,
            durable_replicas: state
                .batches
                .values_from(state.first_batch_with_last_at_or_after(target_replicated))
                .find(|batch| {
                    batch.status != BatchStatus::Aborted
                        && batch.durable_replicas < replication_policy.min_in_sync_replicas
                })
                .map_or(0, |batch| batch.durable_replicas),
        });
    }
    journal.append(JournalEvent::UpdateReplicationPolicy {
        topic_partition,
        expected_policy_generation,
        policy_generation: replication_policy.policy_generation,
        replication_factor: replication_policy.replication_factor,
        min_in_sync_replicas: replication_policy.min_in_sync_replicas,
    })?;
    state.replication_policy = replication_policy;
    state.reset_cached_watermarks_after_truncate();
    Ok(())
}

fn truncate(
    state: &mut PartitionState,
    journal: &ControlJournalHandle,
    topic_partition: TopicPartition,
    log_start: LogicalOffset,
) -> EngineResult<PartitionWatermarks> {
    let watermarks = state.watermarks();
    if log_start < state.log_start {
        return Err(EngineError::InvalidConfig(format!(
            "log start cannot move backward from {} to {log_start}",
            state.log_start
        )));
    }
    if log_start > watermarks.last_stable_offset {
        return Err(EngineError::InvalidConfig(format!(
            "log start {log_start} exceeds last stable offset {}",
            watermarks.last_stable_offset
        )));
    }
    let first_retained = if log_start == watermarks.last_stable_offset {
        state.batches.next_key()
    } else {
        state.batch_key_at_first_offset(log_start).ok_or_else(|| {
            EngineError::InvalidConfig("log start must align to a batch boundary".into())
        })?
    };
    if let Some(first) = state.batches.get(first_retained)
        && let Some(group) = first.atomic_group
        && group.chunk_sequence != 0
    {
        return Err(EngineError::InvalidConfig(
            "retention cannot split an atomic group".into(),
        ));
    }
    if log_start == state.log_start {
        return Ok(watermarks);
    }
    journal.append(JournalEvent::Truncate {
        topic_partition,
        log_start,
    })?;
    state.log_start = log_start;
    state.batches.truncate_before(first_retained);
    state.atomic_groups.retain(|_, group| {
        group
            .batch_ids
            .last()
            .is_some_and(|batch_id| *batch_id >= first_retained)
    });
    state.reset_cached_watermarks_after_truncate();
    Ok(state.watermarks())
}

fn check_producer(
    state: &PartitionState,
    request_id: u128,
    producer: Option<ProducerIdentity>,
    record_count: NonZeroU32,
    payload_digest: [u8; 16],
    durability: Durability,
) -> EngineResult<Option<(AppendResponse, u32, bool)>> {
    let Some(producer) = producer else {
        return Ok(None);
    };
    if let Some(cursor) = state.producers.get(&producer.producer_id) {
        if producer.epoch < cursor.epoch {
            return Err(EngineError::StaleProducerEpoch {
                producer_id: producer.producer_id,
                current: cursor.epoch,
                received: producer.epoch,
            });
        }
        if producer.epoch == cursor.epoch {
            if let Some(record) = state.dedup.get(&(
                producer.producer_id,
                producer.epoch,
                producer.first_sequence,
            )) {
                let digest_matches = record.digest_algorithm == DIGEST_XXH3_128
                    && record.payload_digest == payload_digest
                    && record.event_id == producer.event_id;
                if !digest_matches || record.record_count != record_count.get() {
                    return Err(EngineError::DuplicateSequenceMismatch(producer.producer_id));
                }
                let _ = durability;
                return Ok(Some((
                    AppendResponse {
                        request_id,
                        ..record.response
                    },
                    record.durable_replicas,
                    record.object_durable,
                )));
            }
            if cursor.failed {
                return Err(EngineError::ProducerRequiresNewEpoch(producer.producer_id));
            }
            if producer.first_sequence != cursor.next_sequence {
                return Err(EngineError::ProducerSequenceOutOfOrder {
                    producer_id: producer.producer_id,
                    expected: cursor.next_sequence,
                    received: producer.first_sequence,
                });
            }
            return Ok(None);
        }
        if producer.first_sequence != cursor.next_sequence {
            return Err(EngineError::ProducerSequenceOutOfOrder {
                producer_id: producer.producer_id,
                expected: cursor.next_sequence,
                received: producer.first_sequence,
            });
        }
        return Ok(None);
    }
    if producer.first_sequence != 0 {
        return Err(EngineError::ProducerSequenceOutOfOrder {
            producer_id: producer.producer_id,
            expected: 0,
            received: producer.first_sequence,
        });
    }
    Ok(None)
}

fn idempotent_receipt(
    state: &PartitionState,
    producer: ProducerIdentity,
) -> EngineResult<Option<AppendResponse>> {
    let Some(record) = state.dedup.get(&(
        producer.producer_id,
        producer.epoch,
        producer.first_sequence,
    )) else {
        return Ok(None);
    };
    if record.event_id != producer.event_id {
        return Err(EngineError::DuplicateSequenceMismatch(producer.producer_id));
    }
    Ok(Some(record.response))
}

fn advance_producer(
    state: &mut PartitionState,
    producer: ProducerIdentity,
    record_count: NonZeroU32,
) -> EngineResult<()> {
    let next_sequence = producer
        .first_sequence
        .checked_add(u64::from(record_count.get()))
        .ok_or_else(|| EngineError::InvalidConfig("producer sequence overflow".into()))?;
    match state.producers.get_mut(&producer.producer_id) {
        Some(cursor) if producer.epoch > cursor.epoch => {
            *cursor = ProducerCursor {
                epoch: producer.epoch,
                next_sequence,
                failed: false,
            };
        }
        Some(cursor) => cursor.next_sequence = next_sequence,
        None => {
            state.producers.insert(
                producer.producer_id,
                ProducerCursor {
                    epoch: producer.epoch,
                    next_sequence,
                    failed: false,
                },
            );
        }
    }
    Ok(())
}

fn append_batch_runtime(state: &mut PartitionState, batch: BatchRuntime) -> EngineResult<()> {
    let batch_id = batch.reservation.batch_id;
    state.batches.push(batch_id.get(), batch).map_err(|error| {
        EngineError::CorruptState(format!(
            "batch {batch_id} violates dense index ordering: {error}"
        ))
    })?;
    state.advance_cached_watermarks();
    Ok(())
}

fn mark_batch(
    state: &mut PartitionState,
    reservation: Reservation,
    status: BatchStatus,
    durable_replicas: u32,
) -> EngineResult<u32> {
    let durable_replicas = {
        let batch = state
            .batches
            .get_mut(reservation.batch_id.get())
            .ok_or_else(|| {
                EngineError::CorruptState(format!(
                    "batch {} is missing from runtime state",
                    reservation.batch_id
                ))
            })?;
        if batch.reservation != reservation || batch.status != BatchStatus::Pending {
            return Err(EngineError::CorruptState(format!(
                "invalid lifecycle transition for batch {}",
                reservation.batch_id
            )));
        }
        batch.status = status;
        if matches!(status, BatchStatus::Committed | BatchStatus::Unresolved) {
            batch.replica_acks |= 1;
            batch.durable_replicas = batch
                .durable_replicas
                .max(durable_replicas)
                .max(batch.replica_acks.count_ones());
        } else {
            batch.durable_replicas = 0;
            batch.replica_acks = 0;
        }
        batch.durable_replicas
    };
    state.advance_cached_watermarks();
    Ok(durable_replicas)
}

pub(crate) fn retention_hints(events: &[JournalEvent]) -> HashMap<TopicPartition, LogicalOffset> {
    let mut hints = HashMap::new();
    for event in events {
        if let JournalEvent::Truncate {
            topic_partition,
            log_start,
        } = event
        {
            hints
                .entry(*topic_partition)
                .and_modify(|current: &mut LogicalOffset| *current = (*current).max(*log_start))
                .or_insert(*log_start);
        }
    }
    hints
}

fn record_recovered_group_outcome(
    rings: &HashMap<TopicPartition, BTreeMap<RingEpoch, Vec<ShardId>>>,
    outcomes: &mut HashMap<(TopicPartition, u128), AtomicGroupState>,
    topic_partition: TopicPartition,
    transaction_id: u128,
    outcome: AtomicGroupState,
) -> EngineResult<()> {
    if !rings.contains_key(&topic_partition) {
        return Err(EngineError::CorruptState(format!(
            "atomic group marker for unconfigured partition {}/{}",
            topic_partition.topic_id, topic_partition.partition_id
        )));
    }
    match outcomes.insert((topic_partition, transaction_id), outcome) {
        Some(previous) if previous != outcome => Err(EngineError::CorruptState(format!(
            "atomic group {transaction_id} has conflicting commit and abort markers"
        ))),
        _ => Ok(()),
    }
}

pub(crate) fn recover_partition_states(
    config: &EngineConfig,
    events: &[JournalEvent],
    catalog: &HashMap<(TopicPartition, BatchId), ExtentCatalogEntry>,
    recovered_durable_replicas: u32,
) -> EngineResult<HashMap<TopicPartition, PartitionState>> {
    if recovered_durable_replicas == 0 || recovered_durable_replicas > config.replication_factor {
        return Err(EngineError::InvalidConfig(
            "recovered durable replica count must be within the configured replication factor"
                .into(),
        ));
    }
    let mut rings: HashMap<TopicPartition, BTreeMap<RingEpoch, Vec<ShardId>>> = HashMap::new();
    let mut replication_policies = HashMap::new();
    let mut log_starts = HashMap::new();
    let mut group_outcomes = HashMap::new();
    for event in events {
        match event {
            JournalEvent::Configure {
                topic_partition,
                ring_epoch,
                shards,
            } => {
                validate_ring(config, shards)?;
                let history = rings.entry(*topic_partition).or_default();
                if history
                    .last_key_value()
                    .is_some_and(|(current, _)| ring_epoch <= current)
                {
                    return Err(EngineError::CorruptState(format!(
                        "non-increasing ring epoch for {}/{}",
                        topic_partition.topic_id, topic_partition.partition_id
                    )));
                }
                history.insert(*ring_epoch, shards.clone());
                replication_policies
                    .entry(*topic_partition)
                    .or_insert_with(|| config.default_replication_policy());
            }
            JournalEvent::CreatePartition {
                topic_partition,
                ring_epoch,
                shards,
                policy_generation,
                replication_factor,
                min_in_sync_replicas,
            } => {
                validate_ring(config, shards)?;
                if rings.contains_key(topic_partition) {
                    return Err(EngineError::CorruptState(format!(
                        "duplicate partition creation for {}/{}",
                        topic_partition.topic_id, topic_partition.partition_id
                    )));
                }
                let policy = PartitionReplicationPolicy {
                    policy_generation: *policy_generation,
                    replication_factor: *replication_factor,
                    min_in_sync_replicas: *min_in_sync_replicas,
                };
                policy.validate(config.replication_factor)?;
                rings
                    .entry(*topic_partition)
                    .or_default()
                    .insert(*ring_epoch, shards.clone());
                replication_policies.insert(*topic_partition, policy);
            }
            JournalEvent::UpdateReplicationPolicy {
                topic_partition,
                expected_policy_generation,
                policy_generation,
                replication_factor,
                min_in_sync_replicas,
            } => {
                if !rings.contains_key(topic_partition) {
                    return Err(EngineError::CorruptState(format!(
                        "replication policy update for unconfigured partition {}/{}",
                        topic_partition.topic_id, topic_partition.partition_id
                    )));
                }
                let policy = PartitionReplicationPolicy {
                    policy_generation: *policy_generation,
                    replication_factor: *replication_factor,
                    min_in_sync_replicas: *min_in_sync_replicas,
                };
                policy.validate(config.replication_factor)?;
                let current = replication_policies
                    .get(topic_partition)
                    .copied()
                    .unwrap_or_else(|| config.default_replication_policy());
                if current != policy {
                    if current.policy_generation != *expected_policy_generation {
                        return Err(EngineError::CorruptState(format!(
                            "replication policy generation mismatch for {}/{}: expected {}, found {}",
                            topic_partition.topic_id,
                            topic_partition.partition_id,
                            expected_policy_generation,
                            current.policy_generation
                        )));
                    }
                    replication_policies.insert(*topic_partition, policy);
                }
            }
            JournalEvent::Truncate {
                topic_partition,
                log_start,
            } => {
                if !rings.contains_key(topic_partition) {
                    return Err(EngineError::CorruptState(format!(
                        "retention event for unconfigured partition {}/{}",
                        topic_partition.topic_id, topic_partition.partition_id
                    )));
                }
                let previous = log_starts
                    .get(topic_partition)
                    .copied()
                    .unwrap_or(LogicalOffset::new(0));
                if *log_start < previous {
                    return Err(EngineError::CorruptState(format!(
                        "log start moved backward for {}/{}",
                        topic_partition.topic_id, topic_partition.partition_id
                    )));
                }
                log_starts.insert(*topic_partition, *log_start);
            }
            JournalEvent::GroupCommit {
                topic_partition,
                transaction_id,
            } => {
                record_recovered_group_outcome(
                    &rings,
                    &mut group_outcomes,
                    *topic_partition,
                    *transaction_id,
                    AtomicGroupState::Committed,
                )?;
            }
            JournalEvent::GroupAbort {
                topic_partition,
                transaction_id,
            } => {
                record_recovered_group_outcome(
                    &rings,
                    &mut group_outcomes,
                    *topic_partition,
                    *transaction_id,
                    AtomicGroupState::Aborted,
                )?;
            }
        }
    }

    let mut by_partition: HashMap<TopicPartition, Vec<ExtentCatalogEntry>> = HashMap::new();
    for (&(topic_partition, _), entry) in catalog {
        if !rings.contains_key(&topic_partition) {
            return Err(EngineError::CorruptState(format!(
                "extent for unconfigured partition {}/{}",
                topic_partition.topic_id, topic_partition.partition_id
            )));
        }
        by_partition
            .entry(topic_partition)
            .or_default()
            .push(*entry);
    }

    let mut result = HashMap::new();
    for (topic_partition, history) in rings {
        let (&ring_epoch, shards) = history
            .last_key_value()
            .expect("configured partition has a ring");
        let mut entries = by_partition.remove(&topic_partition).unwrap_or_default();
        entries.sort_by_key(|entry| entry.reservation.first_offset);
        let replication_policy = replication_policies
            .remove(&topic_partition)
            .unwrap_or_else(|| config.default_replication_policy());
        replication_policy.validate(config.replication_factor)?;
        let partition_recovered_durable_replicas =
            recovered_durable_replicas.min(replication_policy.replication_factor);
        let mut state = PartitionState::empty(
            TopicSequencer::new(
                topic_partition.topic_id,
                topic_partition.partition_id,
                ring_epoch,
                shards.clone(),
            )?,
            replication_policy,
        );
        let mut next_batch_id = 0u128;
        let mut next_offset = 0u64;
        let mut next_placement_sequence = 0u64;

        for entry in entries {
            let reservation = entry.reservation;
            let atomic_group = entry.atomic_group.map(|group| AtomicGroupIdentity {
                transaction_id: group.transaction_id,
                chunk_sequence: group.chunk_sequence,
            });
            let status = atomic_group.map_or(BatchStatus::Committed, |group| match group_outcomes
                .get(&(topic_partition, group.transaction_id))
            {
                Some(AtomicGroupState::Committed) => BatchStatus::Committed,
                Some(AtomicGroupState::Aborted) => BatchStatus::Aborted,
                Some(AtomicGroupState::Unresolved) | None => BatchStatus::Unresolved,
            });
            validate_recovered_extent(&history, topic_partition, reservation)?;
            if reservation.first_offset.get() < next_offset
                || reservation.batch_id.get() < next_batch_id
                || reservation.placement.sequence.get() < next_placement_sequence
            {
                return Err(EngineError::CorruptState(format!(
                    "extent overlap or sequence regression at batch {}",
                    reservation.batch_id
                )));
            }
            let missing_batches = reservation.batch_id.get() - next_batch_id;
            let missing_sequences = reservation.placement.sequence.get() - next_placement_sequence;
            let missing_records = reservation.first_offset.get() - next_offset;
            if missing_batches != u128::from(missing_sequences) {
                return Err(EngineError::CorruptState(format!(
                    "batch and placement gaps disagree before batch {}",
                    reservation.batch_id
                )));
            }
            append_recovered_aborts(
                &mut state,
                &history,
                topic_partition,
                next_batch_id,
                next_offset,
                next_placement_sequence,
                missing_batches,
                missing_records,
                reservation.placement.ring_epoch,
            )?;
            append_batch_runtime(
                &mut state,
                BatchRuntime {
                    reservation,
                    status,
                    durable_replicas: if status == BatchStatus::Aborted {
                        0
                    } else {
                        partition_recovered_durable_replicas
                    },
                    replica_acks: if status == BatchStatus::Aborted {
                        0
                    } else {
                        replica_mask(partition_recovered_durable_replicas)
                    },
                    producer: entry.producer.map(|producer| ProducerIdentity {
                        producer_id: producer.producer_id,
                        epoch: producer.epoch,
                        first_sequence: producer.first_sequence,
                        event_id: producer.event_id,
                    }),
                    atomic_group,
                    payload_digest: entry
                        .producer
                        .map(|producer| producer.payload_digest)
                        .or_else(|| entry.atomic_group.map(|group| group.payload_digest)),
                    payload_bytes: entry.payload_bytes,
                    object_durable: entry.object_durable,
                },
            )?;
            if let Some(group) = atomic_group {
                register_atomic_chunk(&mut state, group, reservation, entry.payload_bytes)?;
            }
            restore_producer(
                &mut state,
                reservation,
                entry.producer,
                partition_recovered_durable_replicas,
                entry.object_durable,
            )?;
            next_offset = reservation
                .last_offset
                .get()
                .checked_add(1)
                .ok_or_else(|| EngineError::CorruptState("offset space exhausted".into()))?;
            next_batch_id = reservation
                .batch_id
                .get()
                .checked_add(1)
                .ok_or_else(|| EngineError::CorruptState("batch ID space exhausted".into()))?;
            next_placement_sequence = reservation
                .placement
                .sequence
                .get()
                .checked_add(1)
                .ok_or_else(|| EngineError::CorruptState("placement sequence exhausted".into()))?;
        }
        for (&transaction_id, group) in &mut state.atomic_groups {
            group.state = group_outcomes
                .get(&(topic_partition, transaction_id))
                .copied()
                .unwrap_or(AtomicGroupState::Unresolved);
        }
        state.finalized_groups.extend(
            group_outcomes
                .iter()
                .filter(|((partition, _), _)| *partition == topic_partition)
                .map(|((_, transaction_id), outcome)| (*transaction_id, *outcome)),
        );
        state.sequencer = TopicSequencer::restore(SequencerState {
            topic_id: topic_partition.topic_id,
            partition_id: topic_partition.partition_id,
            ring_epoch,
            leader_epoch: entries_leader_epoch(&state),
            virtual_lanes: shards.clone(),
            next_batch_id,
            next_offset,
            next_placement_sequence,
        })?;
        let recovered_log_start = log_starts
            .get(&topic_partition)
            .copied()
            .unwrap_or(LogicalOffset::new(0));
        let contiguous_end = state.watermarks().contiguous_log_end;
        let first_retained = if recovered_log_start == contiguous_end {
            Some(state.batches.next_key())
        } else {
            state.batch_key_at_first_offset(recovered_log_start)
        };
        if recovered_log_start > contiguous_end || first_retained.is_none() {
            return Err(EngineError::CorruptState(format!(
                "invalid recovered log start {recovered_log_start} for {}/{}",
                topic_partition.topic_id, topic_partition.partition_id
            )));
        }
        state.log_start = recovered_log_start;
        state
            .batches
            .truncate_before(first_retained.expect("validated retained batch key"));
        state.reset_cached_watermarks_after_truncate();
        result.insert(topic_partition, state);
    }
    Ok(result)
}

fn validate_recovered_extent(
    rings: &BTreeMap<RingEpoch, Vec<ShardId>>,
    topic_partition: TopicPartition,
    reservation: Reservation,
) -> EngineResult<()> {
    if reservation.topic_id != topic_partition.topic_id
        || reservation.partition_id != topic_partition.partition_id
        || reservation.batch_id.get() != u128::from(reservation.placement.sequence.get())
    {
        return Err(EngineError::CorruptState(format!(
            "extent identity is inconsistent at batch {}",
            reservation.batch_id
        )));
    }
    let shards = rings
        .get(&reservation.placement.ring_epoch)
        .ok_or_else(|| {
            EngineError::CorruptState(format!(
                "batch {} references unknown ring epoch {}",
                reservation.batch_id, reservation.placement.ring_epoch
            ))
        })?;
    let index = reservation.placement.sequence.get() as usize % shards.len();
    if shards[index] != reservation.placement.virtual_lane_id {
        return Err(EngineError::CorruptState(format!(
            "batch {} violates round-robin placement",
            reservation.batch_id
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_recovered_aborts(
    state: &mut PartitionState,
    rings: &BTreeMap<RingEpoch, Vec<ShardId>>,
    topic_partition: TopicPartition,
    first_batch_id: u128,
    first_offset: u64,
    first_sequence: u64,
    missing_batches: u128,
    missing_records: u64,
    ring_epoch: RingEpoch,
) -> EngineResult<()> {
    if missing_batches == 0 {
        if missing_records == 0 {
            return Ok(());
        }
        return Err(EngineError::CorruptState(
            "logical offset gap has no missing reservations".into(),
        ));
    }
    let missing_batches_u64 = u64::try_from(missing_batches)
        .map_err(|_| EngineError::CorruptState("recovered batch gap exceeds u64".into()))?;
    if missing_batches > MAX_RECOVERED_GAP_BATCHES
        || missing_records < missing_batches_u64
        || missing_records > missing_batches_u64.saturating_mul(u64::from(u32::MAX))
    {
        return Err(EngineError::CorruptState(
            "recovered reservation gap exceeds safety bounds".into(),
        ));
    }
    let shards = rings.get(&ring_epoch).ok_or_else(|| {
        EngineError::CorruptState("recovered gap references an unknown ring".into())
    })?;
    let base = missing_records / missing_batches_u64;
    let remainder = missing_records % missing_batches_u64;
    let mut offset = first_offset;
    for index in 0..missing_batches_u64 {
        let count = base + u64::from(index < remainder);
        let count_u32 = u32::try_from(count)
            .map_err(|_| EngineError::CorruptState("recovered abort is too large".into()))?;
        let record_count = NonZeroU32::new(count_u32)
            .ok_or_else(|| EngineError::CorruptState("zero recovered abort".into()))?;
        let batch_id = first_batch_id + u128::from(index);
        let sequence = first_sequence + index;
        let shard_index = sequence as usize % shards.len();
        let last_offset = offset
            .checked_add(count - 1)
            .ok_or_else(|| EngineError::CorruptState("abort offset overflow".into()))?;
        let reservation = Reservation {
            topic_id: topic_partition.topic_id,
            partition_id: topic_partition.partition_id,
            batch_id: BatchId::new(batch_id),
            first_offset: LogicalOffset::new(offset),
            last_offset: LogicalOffset::new(last_offset),
            record_count,
            placement: Placement {
                virtual_lane_id: shards[shard_index],
                ring_epoch,
                leader_epoch: state.sequencer.state().leader_epoch,
                sequence: PlacementSequence::new(sequence),
            },
        };
        append_batch_runtime(
            state,
            BatchRuntime {
                reservation,
                status: BatchStatus::Aborted,
                durable_replicas: 0,
                replica_acks: 0,
                producer: None,
                atomic_group: None,
                payload_digest: None,
                payload_bytes: 0,
                object_durable: false,
            },
        )?;
        offset = last_offset + 1;
    }
    Ok(())
}

fn entries_leader_epoch(state: &PartitionState) -> shard_stream_core::LeaderEpoch {
    state
        .batches
        .values_from(state.batches.first_key())
        .last()
        .map_or(shard_stream_core::LeaderEpoch::new(0), |batch| {
            batch.reservation.placement.leader_epoch
        })
}

const fn replica_mask(replication_factor: u32) -> u64 {
    if replication_factor >= 64 {
        u64::MAX
    } else {
        (1u64 << replication_factor) - 1
    }
}

fn restore_producer(
    state: &mut PartitionState,
    reservation: Reservation,
    producer: Option<ProducerSequence>,
    durable_replicas: u32,
    object_durable: bool,
) -> EngineResult<()> {
    let Some(producer) = producer else {
        return Ok(());
    };
    let response = append_response(0, reservation);
    state.dedup.insert(
        (
            producer.producer_id,
            producer.epoch,
            producer.first_sequence,
        ),
        DedupRecord {
            digest_algorithm: producer.digest_algorithm,
            payload_digest: producer.payload_digest,
            event_id: producer.event_id,
            record_count: reservation.record_count.get(),
            durable_replicas,
            object_durable,
            response,
        },
    );
    let next_sequence = producer
        .first_sequence
        .checked_add(u64::from(reservation.record_count.get()))
        .ok_or_else(|| EngineError::CorruptState("producer sequence overflow".into()))?;
    let cursor = state
        .producers
        .entry(producer.producer_id)
        .or_insert(ProducerCursor {
            epoch: producer.epoch,
            next_sequence,
            failed: false,
        });
    if producer.epoch > cursor.epoch {
        *cursor = ProducerCursor {
            epoch: producer.epoch,
            next_sequence,
            failed: false,
        };
    } else if producer.epoch == cursor.epoch {
        cursor.next_sequence = cursor.next_sequence.max(next_sequence);
    }
    Ok(())
}

fn state(
    states: &HashMap<TopicPartition, PartitionState>,
    topic_partition: TopicPartition,
) -> EngineResult<&PartitionState> {
    states
        .get(&topic_partition)
        .ok_or(EngineError::UnknownPartition {
            topic_id: topic_partition.topic_id,
            partition_id: topic_partition.partition_id,
        })
}

fn state_mut(
    states: &mut HashMap<TopicPartition, PartitionState>,
    topic_partition: TopicPartition,
) -> EngineResult<&mut PartitionState> {
    states
        .get_mut(&topic_partition)
        .ok_or(EngineError::UnknownPartition {
            topic_id: topic_partition.topic_id,
            partition_id: topic_partition.partition_id,
        })
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

fn coordinator_index(topic_partition: TopicPartition, worker_count: usize) -> usize {
    let topic = topic_partition.topic_id.get();
    let folded =
        (topic as u64) ^ ((topic >> 64) as u64) ^ u64::from(topic_partition.partition_id.get());
    splitmix64(folded) as usize % worker_count
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
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

fn receive<T>(receiver: Receiver<EngineResult<T>>) -> EngineResult<T> {
    receiver
        .recv()
        .map_err(|_| coordinator_stopped("partition coordinator"))?
}

fn receive_control(receiver: Receiver<Result<(), String>>) -> EngineResult<()> {
    receiver
        .recv()
        .map_err(|_| coordinator_stopped("control journal"))?
        .map_err(|message| {
            EngineError::Storage(StorageError::Io(io::Error::other(format!(
                "control journal failed: {message}"
            ))))
        })
}

fn coordinator_stopped(component: &str) -> EngineError {
    EngineError::CorruptState(format!("{component} stopped"))
}
