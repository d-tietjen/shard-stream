use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::num::NonZeroU32;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::{self, JoinHandle};

use ordered_segment_map::DenseOrderedMap;
use shard_stream_core::{
    BatchId, LogicalOffset, LogicalPartitionId, Placement, PlacementSequence, Reservation,
    RingEpoch, SequencerState, ShardId, TopicId, TopicPartition, TopicSequencer,
};
use shard_stream_protocol::{AppendResponse, Durability, ProducerIdentity};
use shard_stream_storage::{
    CoordinatorJournal, DIGEST_XXH3_128, ExtentCatalogEntry, JournalEvent, ProducerSequence,
    StorageError,
};

use crate::{EngineConfig, EngineError, EngineResult, PartitionWatermarks, TopicConfig};

const COORDINATOR_QUEUE_SLOTS: usize = 16_384;
const CONTROL_QUEUE_SLOTS: usize = 1_024;
const MAX_RECOVERED_GAP_BATCHES: u128 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchStatus {
    Pending,
    Committed,
    Aborted,
}

#[derive(Debug, Clone)]
struct BatchRuntime {
    reservation: Reservation,
    status: BatchStatus,
    durable_replicas: u32,
    replica_acks: u64,
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
    pub(crate) ordered_end: LogicalOffset,
}

#[derive(Debug)]
pub(crate) struct PartitionState {
    sequencer: TopicSequencer,
    log_start: LogicalOffset,
    batches: DenseOrderedMap<BatchRuntime>,
    min_in_sync_replicas: u32,
    next_contiguous_batch: u128,
    contiguous_log_end: LogicalOffset,
    next_replicated_batch: u128,
    replicated_high_watermark: LogicalOffset,
    producers: HashMap<u128, ProducerCursor>,
    dedup: HashMap<(u128, u32, u64), DedupRecord>,
}

impl PartitionState {
    fn empty(sequencer: TopicSequencer, min_in_sync_replicas: u32) -> Self {
        Self {
            sequencer,
            log_start: LogicalOffset::new(0),
            batches: DenseOrderedMap::new(0),
            min_in_sync_replicas,
            next_contiguous_batch: 0,
            contiguous_log_end: LogicalOffset::new(0),
            next_replicated_batch: 0,
            replicated_high_watermark: LogicalOffset::new(0),
            producers: HashMap::new(),
            dedup: HashMap::new(),
        }
    }

    fn watermarks(&self, min_in_sync_replicas: u32) -> PartitionWatermarks {
        let replicated_high_watermark = if min_in_sync_replicas == self.min_in_sync_replicas {
            self.replicated_high_watermark
        } else {
            self.compute_replicated_high_watermark(min_in_sync_replicas)
        };
        PartitionWatermarks {
            log_start: self.log_start,
            allocated_end: self.sequencer.next_offset(),
            contiguous_log_end: self.contiguous_log_end,
            replicated_high_watermark,
            last_stable_offset: self.contiguous_log_end,
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
            ordered_end: self.contiguous_log_end,
        })
    }

    fn committed_candidates(&self, candidates: &[BatchId]) -> EngineResult<CommittedBatchSet> {
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
            if self
                .batches
                .get(batch_id.get())
                .is_some_and(|batch| batch.status == BatchStatus::Committed)
            {
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

        while let Some(batch) = self.batches.get(self.next_replicated_batch) {
            if batch.reservation.first_offset != self.replicated_high_watermark
                || batch.status == BatchStatus::Pending
                || (batch.status == BatchStatus::Committed
                    && batch.durable_replicas < self.min_in_sync_replicas)
            {
                break;
            }
            self.replicated_high_watermark = next_offset(batch.reservation);
            self.next_replicated_batch = self
                .next_replicated_batch
                .checked_add(1)
                .expect("sequencer never exhausts the batch ID space");
        }
    }

    fn reset_cached_watermarks_after_truncate(&mut self) {
        self.next_contiguous_batch = self.batches.first_key();
        self.contiguous_log_end = self.log_start;
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
                || (batch.status == BatchStatus::Committed
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
    Duplicate(AppendResponse),
    Reserved {
        reservation: Reservation,
        producer: Option<ProducerSequence>,
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
            .name("shard-stream-control-journal".into())
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
        min_in_sync_replicas: u32,
        response: SyncSender<EngineResult<()>>,
    },
    Reserve {
        topic_partition: TopicPartition,
        request_id: u128,
        producer: Option<ProducerIdentity>,
        record_count: NonZeroU32,
        payload_digest: [u8; 16],
        durability: Durability,
        min_in_sync_replicas: u32,
        response: SyncSender<EngineResult<ReserveOutcome>>,
    },
    Complete {
        topic_partition: TopicPartition,
        completion: CompleteAppend,
        response: SyncSender<EngineResult<()>>,
    },
    ReplicaDurable {
        topic_partition: TopicPartition,
        reservation: Reservation,
        replica_index: u32,
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
        response: SyncSender<EngineResult<CommittedBatchSet>>,
    },
    Watermarks {
        topic_partition: TopicPartition,
        min_in_sync_replicas: u32,
        response: SyncSender<EngineResult<PartitionWatermarks>>,
    },
    Reconfigure {
        topic_partition: TopicPartition,
        ring_epoch: RingEpoch,
        shards: Vec<ShardId>,
        response: SyncSender<EngineResult<()>>,
    },
    Truncate {
        topic_partition: TopicPartition,
        log_start: LogicalOffset,
        min_in_sync_replicas: u32,
        response: SyncSender<EngineResult<PartitionWatermarks>>,
    },
    TopicPartitions {
        topic_id: TopicId,
        response: SyncSender<Vec<LogicalPartitionId>>,
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
    pub(crate) fn replica_durable(&self, reservation: Reservation, replica_index: u32) {
        let topic_partition = TopicPartition::new(reservation.topic_id, reservation.partition_id);
        let index = coordinator_index(topic_partition, self.senders.len());
        let _ = self.senders[index].send(CoordinatorCommand::ReplicaDurable {
            topic_partition,
            reservation,
            replica_index,
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
        min_in_sync_replicas: u32,
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
                    min_in_sync_replicas,
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
        record_count: NonZeroU32,
        payload_digest: [u8; 16],
        durability: Durability,
        min_in_sync_replicas: u32,
    ) -> EngineResult<ReserveOutcome> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::Reserve {
                topic_partition,
                request_id,
                producer,
                record_count,
                payload_digest,
                durability,
                min_in_sync_replicas,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn complete(
        &self,
        topic_partition: TopicPartition,
        completion: CompleteAppend,
    ) -> EngineResult<()> {
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
    ) -> EngineResult<CommittedBatchSet> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::CommittedCandidates {
                topic_partition,
                candidates,
                response,
            },
        )?;
        receive(receiver)
    }

    pub(crate) fn watermarks(
        &self,
        topic_partition: TopicPartition,
        min_in_sync_replicas: u32,
    ) -> EngineResult<PartitionWatermarks> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::Watermarks {
                topic_partition,
                min_in_sync_replicas,
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

    pub(crate) fn truncate(
        &self,
        topic_partition: TopicPartition,
        log_start: LogicalOffset,
        min_in_sync_replicas: u32,
    ) -> EngineResult<PartitionWatermarks> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            CoordinatorCommand::Truncate {
                topic_partition,
                log_start,
                min_in_sync_replicas,
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
                min_in_sync_replicas,
                response,
            } => {
                let result = create_partition(
                    &mut states,
                    &journal,
                    topic_partition,
                    shards,
                    min_in_sync_replicas,
                );
                let _ = response.send(result);
            }
            CoordinatorCommand::ReplicaDurable {
                topic_partition,
                reservation,
                replica_index,
            } => {
                let _ = state_mut(&mut states, topic_partition)
                    .and_then(|state| replica_durable(state, reservation, replica_index));
            }
            CoordinatorCommand::Reserve {
                topic_partition,
                request_id,
                producer,
                record_count,
                payload_digest,
                durability,
                min_in_sync_replicas,
                response,
            } => {
                let result = state_mut(&mut states, topic_partition).and_then(|state| {
                    reserve(
                        state,
                        request_id,
                        producer,
                        record_count,
                        payload_digest,
                        durability,
                        min_in_sync_replicas,
                    )
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
                response,
            } => {
                let result = state(&states, topic_partition)
                    .and_then(|state| state.committed_candidates(&candidates));
                let _ = response.send(result);
            }
            CoordinatorCommand::Watermarks {
                topic_partition,
                min_in_sync_replicas,
                response,
            } => {
                let result = state(&states, topic_partition)
                    .map(|state| state.watermarks(min_in_sync_replicas));
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
            CoordinatorCommand::Truncate {
                topic_partition,
                log_start,
                min_in_sync_replicas,
                response,
            } => {
                let result = state_mut(&mut states, topic_partition).and_then(|state| {
                    truncate(
                        state,
                        &journal,
                        topic_partition,
                        log_start,
                        min_in_sync_replicas,
                    )
                });
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
        }
    }
}

fn create_partition(
    states: &mut HashMap<TopicPartition, PartitionState>,
    journal: &ControlJournalHandle,
    topic_partition: TopicPartition,
    shards: Vec<ShardId>,
    min_in_sync_replicas: u32,
) -> EngineResult<()> {
    if let Some(existing) = states.get(&topic_partition) {
        let existing = existing.sequencer.state();
        if existing.ring_epoch == RingEpoch::new(1) && existing.shards == shards {
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
    journal.append(JournalEvent::Configure {
        topic_partition,
        ring_epoch: RingEpoch::new(1),
        shards,
    })?;
    states.insert(
        topic_partition,
        PartitionState::empty(sequencer, min_in_sync_replicas),
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn reserve(
    state: &mut PartitionState,
    request_id: u128,
    producer: Option<ProducerIdentity>,
    record_count: NonZeroU32,
    payload_digest: [u8; 16],
    durability: Durability,
    min_in_sync_replicas: u32,
) -> EngineResult<ReserveOutcome> {
    if let Some(response) = check_producer(
        state,
        request_id,
        producer,
        record_count,
        payload_digest,
        durability,
        min_in_sync_replicas,
    )? {
        return Ok(ReserveOutcome::Duplicate(response));
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
    let producer = producer.map(|identity| ProducerSequence {
        producer_id: identity.producer_id,
        epoch: identity.epoch,
        first_sequence: identity.first_sequence,
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
        },
    )?;
    Ok(ReserveOutcome::Reserved {
        reservation,
        producer,
    })
}

fn complete(state: &mut PartitionState, completion: CompleteAppend) -> EngineResult<()> {
    let durable_replicas = mark_batch(
        state,
        completion.reservation,
        BatchStatus::Committed,
        completion.durable_replicas,
    )?;
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
                record_count: completion.reservation.record_count.get(),
                durable_replicas,
                object_durable: completion.object_durable,
                response: completion.response,
            },
        );
    }
    Ok(())
}

fn replica_durable(
    state: &mut PartitionState,
    reservation: Reservation,
    replica_index: u32,
) -> EngineResult<()> {
    if replica_index >= 64 {
        return Err(EngineError::CorruptState(
            "replica index exceeds progress bitmap".into(),
        ));
    }
    let durable_replicas = {
        let batch = state
            .batches
            .get_mut(reservation.batch_id.get())
            .ok_or_else(|| {
                EngineError::CorruptState(format!(
                    "replica progress references missing batch {}",
                    reservation.batch_id
                ))
            })?;
        if batch.reservation != reservation || batch.status == BatchStatus::Aborted {
            return Err(EngineError::CorruptState(format!(
                "invalid replica progress for batch {}",
                reservation.batch_id
            )));
        }
        batch.replica_acks |= 1u64 << replica_index;
        batch.durable_replicas = batch.durable_replicas.max(batch.replica_acks.count_ones());
        batch.durable_replicas
    };
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
    }
    Ok(())
}

fn truncate(
    state: &mut PartitionState,
    journal: &ControlJournalHandle,
    topic_partition: TopicPartition,
    log_start: LogicalOffset,
    min_in_sync_replicas: u32,
) -> EngineResult<PartitionWatermarks> {
    let watermarks = state.watermarks(min_in_sync_replicas);
    if log_start < state.log_start {
        return Err(EngineError::InvalidConfig(format!(
            "log start cannot move backward from {} to {log_start}",
            state.log_start
        )));
    }
    if log_start > watermarks.contiguous_log_end {
        return Err(EngineError::InvalidConfig(format!(
            "log start {log_start} exceeds contiguous log end {}",
            watermarks.contiguous_log_end
        )));
    }
    let first_retained = if log_start == watermarks.contiguous_log_end {
        state.batches.next_key()
    } else {
        state.batch_key_at_first_offset(log_start).ok_or_else(|| {
            EngineError::InvalidConfig("log start must align to a batch boundary".into())
        })?
    };
    if log_start == state.log_start {
        return Ok(watermarks);
    }
    journal.append(JournalEvent::Truncate {
        topic_partition,
        log_start,
    })?;
    state.log_start = log_start;
    state.batches.truncate_before(first_retained);
    state.reset_cached_watermarks_after_truncate();
    Ok(state.watermarks(min_in_sync_replicas))
}

fn check_producer(
    state: &PartitionState,
    request_id: u128,
    producer: Option<ProducerIdentity>,
    record_count: NonZeroU32,
    payload_digest: [u8; 16],
    durability: Durability,
    min_in_sync_replicas: u32,
) -> EngineResult<Option<AppendResponse>> {
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
                    && record.payload_digest == payload_digest;
                if !digest_matches || record.record_count != record_count.get() {
                    return Err(EngineError::DuplicateSequenceMismatch(producer.producer_id));
                }
                if durability == Durability::Quorum
                    && record.durable_replicas < min_in_sync_replicas
                {
                    return Err(EngineError::DurabilityUnavailable {
                        required_replicas: min_in_sync_replicas,
                        durable_replicas: record.durable_replicas,
                    });
                }
                if durability == Durability::Object && !record.object_durable {
                    return Err(EngineError::ObjectDurabilityUnavailable);
                }
                return Ok(Some(AppendResponse {
                    request_id,
                    ..record.response
                }));
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
        if status == BatchStatus::Committed {
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

pub(crate) fn recover_partition_states(
    config: &EngineConfig,
    events: &[JournalEvent],
    catalog: &HashMap<(TopicPartition, BatchId), ExtentCatalogEntry>,
) -> EngineResult<HashMap<TopicPartition, PartitionState>> {
    let mut rings: HashMap<TopicPartition, BTreeMap<RingEpoch, Vec<ShardId>>> = HashMap::new();
    let mut log_starts = HashMap::new();
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
        let mut state = PartitionState::empty(
            TopicSequencer::new(
                topic_partition.topic_id,
                topic_partition.partition_id,
                ring_epoch,
                shards.clone(),
            )?,
            config.min_in_sync_replicas,
        );
        let mut next_batch_id = 0u128;
        let mut next_offset = 0u128;
        let mut next_placement_sequence = 0u128;

        for entry in entries {
            let reservation = entry.reservation;
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
            if missing_batches != missing_sequences {
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
                    status: BatchStatus::Committed,
                    durable_replicas: config.replication_factor,
                    replica_acks: replica_mask(config.replication_factor),
                },
            )?;
            restore_producer(
                &mut state,
                reservation,
                entry.producer,
                config.replication_factor,
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

        state.sequencer = TopicSequencer::restore(SequencerState {
            topic_id: topic_partition.topic_id,
            partition_id: topic_partition.partition_id,
            ring_epoch,
            shards: shards.clone(),
            next_batch_id,
            next_offset,
            next_placement_sequence,
        })?;
        let recovered_log_start = log_starts
            .get(&topic_partition)
            .copied()
            .unwrap_or(LogicalOffset::new(0));
        let contiguous_end = state
            .watermarks(config.min_in_sync_replicas)
            .contiguous_log_end;
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
        || reservation.batch_id.get() != reservation.placement.sequence.get()
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
    let index = (reservation.placement.sequence.get() % shards.len() as u128) as usize;
    if shards[index] != reservation.placement.shard_id {
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
    first_offset: u128,
    first_sequence: u128,
    missing_batches: u128,
    missing_records: u128,
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
    if missing_batches > MAX_RECOVERED_GAP_BATCHES
        || missing_records < missing_batches
        || missing_records > missing_batches.saturating_mul(u128::from(u32::MAX))
    {
        return Err(EngineError::CorruptState(
            "recovered reservation gap exceeds safety bounds".into(),
        ));
    }
    let shards = rings.get(&ring_epoch).ok_or_else(|| {
        EngineError::CorruptState("recovered gap references an unknown ring".into())
    })?;
    let base = missing_records / missing_batches;
    let remainder = missing_records % missing_batches;
    let mut offset = first_offset;
    for index in 0..missing_batches {
        let count = base + u128::from(index < remainder);
        let count_u32 = u32::try_from(count)
            .map_err(|_| EngineError::CorruptState("recovered abort is too large".into()))?;
        let record_count = NonZeroU32::new(count_u32)
            .ok_or_else(|| EngineError::CorruptState("zero recovered abort".into()))?;
        let batch_id = first_batch_id + index;
        let sequence = first_sequence + index;
        let shard_index = (sequence % shards.len() as u128) as usize;
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
                shard_id: shards[shard_index],
                ring_epoch,
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
            },
        )?;
        offset = last_offset + 1;
    }
    Ok(())
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
        if shard.get() >= config.shard_count {
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
        first_offset: reservation.first_offset,
        last_offset: reservation.last_offset,
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
