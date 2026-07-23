use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, RwLock};

use shard_stream_core::{
    BatchId, LogicalOffset, LogicalPartitionId, Placement, Reservation, RingEpoch, SequencerState,
    ShardId, TopicPartition, TopicSequencer,
};
use shard_stream_protocol::{
    AppendRequest, AppendResponse, Durability, FetchMode, FetchRequest, ProducerIdentity,
};
use shard_stream_storage::{
    CoordinatorJournal, DIGEST_BLAKE3, ExtentCatalogEntry, JournalEvent, ProducerSequence,
    StoredBatch,
};

use crate::worker::ShardHandle;
use crate::{EngineConfig, EngineError, EngineResult, TopicConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedBatch {
    pub batch_id: BatchId,
    pub first_offset: LogicalOffset,
    pub last_offset: LogicalOffset,
    pub record_count: NonZeroU32,
    pub payload: Vec<u8>,
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
    payload_digest: [u8; 32],
    record_count: u32,
    durable_replicas: u32,
    object_durable: bool,
    response: AppendResponse,
}

type RecoveredBatch = (
    Reservation,
    Option<ProducerSequence>,
    BatchStatus,
    u32,
    bool,
);
type RecoveredStatus = (BatchStatus, u32, bool);

#[derive(Debug)]
struct PartitionState {
    sequencer: TopicSequencer,
    log_start: LogicalOffset,
    batches: BTreeMap<LogicalOffset, BatchRuntime>,
    producers: HashMap<u128, ProducerCursor>,
    dedup: HashMap<(u128, u32, u64), DedupRecord>,
}

impl PartitionState {
    fn empty(sequencer: TopicSequencer) -> Self {
        Self {
            sequencer,
            log_start: LogicalOffset::new(0),
            batches: BTreeMap::new(),
            producers: HashMap::new(),
            dedup: HashMap::new(),
        }
    }

    fn watermarks(&self, min_in_sync_replicas: u32) -> PartitionWatermarks {
        let mut contiguous_end = 0u128;
        for (first_offset, batch) in &self.batches {
            if first_offset.get() != contiguous_end || batch.status == BatchStatus::Pending {
                break;
            }
            contiguous_end = batch
                .reservation
                .last_offset
                .get()
                .checked_add(1)
                .expect("sequencer never allocates an unrepresentable end offset");
        }
        let mut replicated_end = 0u128;
        for (first_offset, batch) in &self.batches {
            if first_offset.get() != replicated_end || batch.status == BatchStatus::Pending {
                break;
            }
            if batch.status == BatchStatus::Committed
                && batch.durable_replicas < min_in_sync_replicas
            {
                break;
            }
            replicated_end = batch
                .reservation
                .last_offset
                .get()
                .checked_add(1)
                .expect("sequencer never allocates an unrepresentable end offset");
        }
        let contiguous = LogicalOffset::new(contiguous_end);
        PartitionWatermarks {
            log_start: self.log_start,
            allocated_end: self.sequencer.next_offset(),
            contiguous_log_end: contiguous,
            replicated_high_watermark: LogicalOffset::new(replicated_end),
            last_stable_offset: contiguous,
        }
    }

    fn committed_offsets(&self, mode: FetchMode) -> HashSet<BatchId> {
        let ordered_end = self.watermarks(1).contiguous_log_end;
        self.batches
            .values()
            .filter(|batch| {
                batch.status == BatchStatus::Committed
                    && batch.reservation.last_offset >= self.log_start
                    && (mode == FetchMode::AsAvailable
                        || batch.reservation.last_offset < ordered_end)
            })
            .map(|batch| batch.reservation.batch_id)
            .collect()
    }
}

pub struct StreamEngine {
    config: EngineConfig,
    shards: Vec<ShardHandle>,
    partitions: RwLock<HashMap<TopicPartition, Arc<Mutex<PartitionState>>>>,
    journal: Mutex<CoordinatorJournal>,
}

impl StreamEngine {
    pub fn open(config: EngineConfig) -> EngineResult<Self> {
        config.validate()?;
        std::fs::create_dir_all(&config.data_dir)
            .map_err(shard_stream_storage::StorageError::from)?;

        let (mut journal, recovered) =
            CoordinatorJournal::open(config.data_dir.join("coordinator.ssj"))?;
        let retained_log_starts = retention_hints(&recovered.events);
        let mut shards = Vec::with_capacity(config.shard_count as usize);
        let mut catalog = HashMap::new();
        for shard_id in config.shard_ids() {
            let (handle, entries) = ShardHandle::spawn(&config, shard_id, &retained_log_starts)?;
            for entry in entries {
                let key = batch_key(&entry.reservation);
                if catalog.insert(key, entry).is_some() {
                    return Err(EngineError::CorruptState(format!(
                        "batch {} appears in more than one extent",
                        key.1
                    )));
                }
            }
            shards.push(handle);
        }

        let partitions = recover_partitions(&config, &mut journal, &recovered.events, &catalog)?;
        let engine = Self {
            config,
            shards,
            partitions: RwLock::new(partitions),
            journal: Mutex::new(journal),
        };
        for (topic_partition, log_start) in retained_log_starts {
            if log_start.get() > 0 {
                engine.collect_garbage(topic_partition, log_start)?;
            }
        }
        Ok(engine)
    }

    pub fn create_topic(&self, topic: TopicConfig) -> EngineResult<()> {
        if topic.partitions == 0 {
            return Err(EngineError::InvalidConfig(
                "topic partitions must be nonzero".into(),
            ));
        }
        let shards = topic.shards.unwrap_or_else(|| self.config.shard_ids());
        validate_ring(&self.config, &shards)?;

        let mut partitions = write_lock(&self.partitions);
        if partitions.keys().any(|topic_partition| {
            topic_partition.topic_id == topic.topic_id
                && topic_partition.partition_id.get() >= topic.partitions
        }) {
            return Err(EngineError::TopicAlreadyExists(topic.topic_id));
        }

        let mut journal = mutex_lock(&self.journal);
        for partition_id in 0..topic.partitions {
            let topic_partition =
                TopicPartition::new(topic.topic_id, LogicalPartitionId::new(partition_id));
            if let Some(existing) = partitions.get(&topic_partition) {
                let existing = mutex_lock(existing).sequencer.state();
                if existing.ring_epoch == RingEpoch::new(1) && existing.shards != shards {
                    return Err(EngineError::TopicAlreadyExists(topic.topic_id));
                }
                continue;
            }
            journal.append(
                &JournalEvent::Configure {
                    topic_partition,
                    ring_epoch: RingEpoch::new(1),
                    shards: shards.clone(),
                },
                true,
            )?;
            let sequencer = TopicSequencer::new(
                topic.topic_id,
                topic_partition.partition_id,
                RingEpoch::new(1),
                shards.clone(),
            )?;
            partitions.insert(
                topic_partition,
                Arc::new(Mutex::new(PartitionState::empty(sequencer))),
            );
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
        let partition = self.partition(topic_partition)?;
        let mut state = mutex_lock(&partition);
        let previous = state.sequencer.state();
        state.sequencer.reconfigure(ring_epoch, shards.clone())?;
        if let Err(error) = mutex_lock(&self.journal).append(
            &JournalEvent::Configure {
                topic_partition,
                ring_epoch,
                shards,
            },
            true,
        ) {
            state.sequencer =
                TopicSequencer::restore(previous).expect("previous sequencer state was valid");
            return Err(error.into());
        }
        Ok(())
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
        let partition = self.partition(topic_partition)?;
        let payload_digest = *blake3::hash(&request.payload).as_bytes();

        let reservation;
        {
            let mut state = mutex_lock(&partition);
            if let Some(response) = check_producer(
                &state,
                request.request_id,
                request.producer,
                record_count,
                payload_digest,
                request.durability,
                self.config.min_in_sync_replicas,
            )? {
                return Ok(response);
            }

            let previous_sequencer = state.sequencer.state();
            reservation = state.sequencer.reserve(record_count)?;
            let producer = request.producer.map(|identity| ProducerSequence {
                producer_id: identity.producer_id,
                epoch: identity.epoch,
                first_sequence: identity.first_sequence,
                digest_algorithm: DIGEST_BLAKE3,
                payload_digest,
            });
            if let Err(error) = mutex_lock(&self.journal).append(
                &JournalEvent::Reserve {
                    reservation,
                    producer,
                },
                true,
            ) {
                state.sequencer = TopicSequencer::restore(previous_sequencer)
                    .expect("previous sequencer state was valid");
                return Err(error.into());
            }
            if let Some(identity) = request.producer {
                advance_producer(&mut state, identity, record_count)?;
            }
            state.batches.insert(
                reservation.first_offset,
                BatchRuntime {
                    reservation,
                    status: BatchStatus::Pending,
                    durable_replicas: 0,
                },
            );
        }

        let append_result = self.shard(reservation.placement.shard_id)?.append(
            reservation,
            request.payload,
            true,
            request.durability == Durability::Object,
        );
        let response = append_response(request.request_id, reservation);
        let mut state = mutex_lock(&partition);
        match append_result {
            Ok(durability) => {
                let journal_result = mutex_lock(&self.journal).append(
                    &JournalEvent::Commit {
                        topic_partition,
                        batch_id: reservation.batch_id,
                        durable_replicas: durability.durable_replicas,
                        object_durable: durability.object_durable,
                    },
                    true,
                );
                mark_batch(
                    &mut state,
                    reservation,
                    BatchStatus::Committed,
                    durability.durable_replicas,
                )?;
                if let Some(producer) = request.producer {
                    state.dedup.insert(
                        (
                            producer.producer_id,
                            producer.epoch,
                            producer.first_sequence,
                        ),
                        DedupRecord {
                            digest_algorithm: DIGEST_BLAKE3,
                            payload_digest,
                            record_count: record_count.get(),
                            durable_replicas: durability.durable_replicas,
                            object_durable: durability.object_durable,
                            response,
                        },
                    );
                }
                journal_result?;
                if request.durability == Durability::Quorum
                    && durability.durable_replicas < self.config.min_in_sync_replicas
                {
                    return Err(EngineError::DurabilityUnavailable {
                        required_replicas: self.config.min_in_sync_replicas,
                        durable_replicas: durability.durable_replicas,
                    });
                }
                if request.durability == Durability::Object && !durability.object_durable {
                    return Err(EngineError::ObjectDurabilityUnavailable);
                }
                Ok(response)
            }
            Err(failure) => {
                if let Some(producer) = request.producer
                    && let Some(cursor) = state.producers.get_mut(&producer.producer_id)
                {
                    cursor.failed = true;
                }
                if failure.definitely_not_written {
                    mutex_lock(&self.journal).append(
                        &JournalEvent::Abort {
                            topic_partition,
                            batch_id: reservation.batch_id,
                        },
                        true,
                    )?;
                    mark_batch(&mut state, reservation, BatchStatus::Aborted, 0)?;
                }
                // A storage or response-channel failure can occur after the
                // extent reached disk. Leave that reservation pending so
                // recovery can resolve it from the canonical extent instead
                // of writing a contradictory Abort record.
                Err(failure.error)
            }
        }
    }

    pub fn fetch(&self, request: FetchRequest) -> EngineResult<Vec<FetchedBatch>> {
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
        let partition = self.partition(topic_partition)?;
        let committed = {
            let state = mutex_lock(&partition);
            if request.start_offset < state.log_start {
                return Err(EngineError::OffsetOutOfRange {
                    requested: request.start_offset,
                    log_start: state.log_start,
                });
            }
            state.committed_offsets(request.mode)
        };

        let mut receivers = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            receivers.push((
                shard.shard_id,
                shard.begin_fetch(topic_partition, request.start_offset, max_bytes)?,
            ));
        }
        let mut batches = Vec::new();
        for (shard_id, receiver) in receivers {
            let shard_batches = receiver
                .recv()
                .map_err(|_| EngineError::WorkerStopped(shard_id))??;
            batches.extend(shard_batches);
        }
        batches.retain(|batch| committed.contains(&batch.reservation.batch_id));
        batches.sort_by_key(|batch| batch.reservation.first_offset);
        Ok(limit_fetched_batches(batches, max_bytes))
    }

    pub fn watermarks(&self, topic_partition: TopicPartition) -> EngineResult<PartitionWatermarks> {
        let partition = self.partition(topic_partition)?;
        let watermarks = mutex_lock(&partition).watermarks(self.config.min_in_sync_replicas);
        Ok(watermarks)
    }

    pub fn truncate_partition(
        &self,
        topic_partition: TopicPartition,
        log_start: LogicalOffset,
    ) -> EngineResult<PartitionWatermarks> {
        let partition = self.partition(topic_partition)?;
        let mut state = mutex_lock(&partition);
        let watermarks = state.watermarks(self.config.min_in_sync_replicas);
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
        if log_start != watermarks.contiguous_log_end
            && log_start.get() != 0
            && !state.batches.contains_key(&log_start)
        {
            return Err(EngineError::InvalidConfig(
                "log start must align to a batch boundary".into(),
            ));
        }
        if log_start == state.log_start {
            return Ok(watermarks);
        }
        mutex_lock(&self.journal).append(
            &JournalEvent::Truncate {
                topic_partition,
                log_start,
            },
            true,
        )?;
        state.log_start = log_start;
        let updated = state.watermarks(self.config.min_in_sync_replicas);
        drop(state);
        self.collect_garbage(topic_partition, log_start)?;
        Ok(updated)
    }

    pub fn sync(&self) -> EngineResult<()> {
        for shard in &self.shards {
            shard.sync()?;
        }
        mutex_lock(&self.journal).sync()?;
        Ok(())
    }

    fn partition(
        &self,
        topic_partition: TopicPartition,
    ) -> EngineResult<Arc<Mutex<PartitionState>>> {
        read_lock(&self.partitions)
            .get(&topic_partition)
            .cloned()
            .ok_or(EngineError::UnknownPartition {
                topic_id: topic_partition.topic_id,
                partition_id: topic_partition.partition_id,
            })
    }

    fn shard(&self, shard_id: ShardId) -> EngineResult<&ShardHandle> {
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

fn retention_hints(events: &[JournalEvent]) -> HashMap<TopicPartition, LogicalOffset> {
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

fn recover_partitions(
    config: &EngineConfig,
    journal: &mut CoordinatorJournal,
    events: &[JournalEvent],
    catalog: &HashMap<(TopicPartition, BatchId), ExtentCatalogEntry>,
) -> EngineResult<HashMap<TopicPartition, Arc<Mutex<PartitionState>>>> {
    let mut configurations: HashMap<TopicPartition, (RingEpoch, Vec<ShardId>)> = HashMap::new();
    let mut log_starts: HashMap<TopicPartition, LogicalOffset> = HashMap::new();
    let mut reservations: HashMap<
        (TopicPartition, BatchId),
        (Reservation, Option<ProducerSequence>),
    > = HashMap::new();
    let mut statuses: HashMap<(TopicPartition, BatchId), RecoveredStatus> = HashMap::new();

    for event in events {
        match event {
            JournalEvent::Configure {
                topic_partition,
                ring_epoch,
                shards,
            } => {
                validate_ring(config, shards)?;
                if let Some((current, _)) = configurations.get(topic_partition)
                    && ring_epoch <= current
                {
                    return Err(EngineError::CorruptState(format!(
                        "non-increasing ring epoch for {}/{}",
                        topic_partition.topic_id, topic_partition.partition_id
                    )));
                }
                configurations.insert(*topic_partition, (*ring_epoch, shards.clone()));
            }
            JournalEvent::Reserve {
                reservation,
                producer,
            } => {
                let key = batch_key(reservation);
                if reservations
                    .insert(key, (*reservation, *producer))
                    .is_some()
                {
                    return Err(EngineError::CorruptState(format!(
                        "duplicate reservation for batch {}",
                        reservation.batch_id
                    )));
                }
                statuses.insert(key, (BatchStatus::Pending, 0, false));
            }
            JournalEvent::Commit {
                topic_partition,
                batch_id,
                durable_replicas,
                object_durable,
            } => {
                set_recovered_status(
                    &reservations,
                    &mut statuses,
                    (*topic_partition, *batch_id),
                    BatchStatus::Committed,
                    *durable_replicas,
                    *object_durable,
                )?;
            }
            JournalEvent::Abort {
                topic_partition,
                batch_id,
            } => {
                set_recovered_status(
                    &reservations,
                    &mut statuses,
                    (*topic_partition, *batch_id),
                    BatchStatus::Aborted,
                    0,
                    false,
                )?;
            }
            JournalEvent::Truncate {
                topic_partition,
                log_start,
            } => {
                if !configurations.contains_key(topic_partition) {
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

    for (key, entry) in catalog {
        let Some((reservation, _)) = reservations.get(key) else {
            return Err(EngineError::CorruptState(format!(
                "extent batch {} has no durable reservation",
                key.1
            )));
        };
        if *reservation != entry.reservation {
            return Err(EngineError::CorruptState(format!(
                "extent metadata differs from reservation for batch {}",
                key.1
            )));
        }
    }

    for (key, (reservation, _)) in &reservations {
        let has_extent = catalog.contains_key(key);
        match (statuses.get(key).copied(), has_extent) {
            (Some((BatchStatus::Pending, _, _)), true) => {
                journal.append(
                    &JournalEvent::Commit {
                        topic_partition: key.0,
                        batch_id: key.1,
                        durable_replicas: config.replication_factor,
                        object_durable: false,
                    },
                    true,
                )?;
                statuses.insert(
                    *key,
                    (BatchStatus::Committed, config.replication_factor, false),
                );
            }
            (Some((BatchStatus::Pending, _, _)), false) => {
                journal.append(
                    &JournalEvent::Abort {
                        topic_partition: key.0,
                        batch_id: key.1,
                    },
                    true,
                )?;
                statuses.insert(*key, (BatchStatus::Aborted, 0, false));
            }
            (Some((BatchStatus::Committed, _, _)), false)
                if log_starts
                    .get(&key.0)
                    .is_some_and(|log_start| reservation.last_offset < *log_start) => {}
            (Some((BatchStatus::Committed, _, _)), false) => {
                return Err(EngineError::CorruptState(format!(
                    "committed batch {} is missing its extent",
                    key.1
                )));
            }
            (Some((BatchStatus::Aborted, _, _)), true) => {
                return Err(EngineError::CorruptState(format!(
                    "aborted batch {} has a canonical extent",
                    key.1
                )));
            }
            (Some((BatchStatus::Committed | BatchStatus::Aborted, _, _)), _) => {}
            (None, _) => {
                return Err(EngineError::CorruptState(format!(
                    "batch {} has no lifecycle state",
                    reservation.batch_id
                )));
            }
        }
    }

    let mut by_partition: HashMap<TopicPartition, Vec<RecoveredBatch>> = HashMap::new();
    for (key, (reservation, producer)) in reservations {
        by_partition.entry(key.0).or_default().push((
            reservation,
            producer,
            statuses[&key].0,
            statuses[&key].1,
            statuses[&key].2,
        ));
    }

    let mut result = HashMap::new();
    for (topic_partition, (ring_epoch, shards)) in configurations {
        let mut recovered_batches = by_partition.remove(&topic_partition).unwrap_or_default();
        recovered_batches.sort_by_key(|(reservation, _, _, _, _)| reservation.first_offset);
        let mut next_batch_id = 0u128;
        let mut next_offset = 0u128;
        let mut next_placement_sequence = 0u128;
        for (reservation, _, _, _, _) in &recovered_batches {
            if reservation.first_offset.get() != next_offset {
                return Err(EngineError::CorruptState(format!(
                    "reservation offset gap or overlap at batch {}",
                    reservation.batch_id
                )));
            }
            next_offset = reservation
                .last_offset
                .get()
                .checked_add(1)
                .ok_or_else(|| EngineError::CorruptState("offset space exhausted".into()))?;
            next_batch_id =
                next_batch_id.max(
                    reservation.batch_id.get().checked_add(1).ok_or_else(|| {
                        EngineError::CorruptState("batch ID space exhausted".into())
                    })?,
                );
            next_placement_sequence = next_placement_sequence.max(
                reservation
                    .placement
                    .sequence
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| {
                        EngineError::CorruptState("placement sequence exhausted".into())
                    })?,
            );
            if reservation.placement.ring_epoch > ring_epoch {
                return Err(EngineError::CorruptState(format!(
                    "batch {} uses a future ring epoch",
                    reservation.batch_id
                )));
            }
        }
        let sequencer = TopicSequencer::restore(SequencerState {
            topic_id: topic_partition.topic_id,
            partition_id: topic_partition.partition_id,
            ring_epoch,
            shards,
            next_batch_id,
            next_offset,
            next_placement_sequence,
        })?;
        let mut state = PartitionState::empty(sequencer);
        for (reservation, producer, status, durable_replicas, object_durable) in recovered_batches {
            let durable_replicas = if status == BatchStatus::Committed {
                durable_replicas.max(config.replication_factor)
            } else {
                0
            };
            state.batches.insert(
                reservation.first_offset,
                BatchRuntime {
                    reservation,
                    status,
                    durable_replicas,
                },
            );
            if status == BatchStatus::Committed
                && let Some(producer) = producer
            {
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
                    .ok_or_else(|| {
                        EngineError::CorruptState("producer sequence overflow".into())
                    })?;
                let cursor =
                    state
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
            }
        }
        let recovered_log_start = log_starts
            .get(&topic_partition)
            .copied()
            .unwrap_or(LogicalOffset::new(0));
        let contiguous_end = state
            .watermarks(config.min_in_sync_replicas)
            .contiguous_log_end;
        if recovered_log_start > contiguous_end
            || (recovered_log_start != contiguous_end
                && recovered_log_start.get() != 0
                && !state.batches.contains_key(&recovered_log_start))
        {
            return Err(EngineError::CorruptState(format!(
                "invalid recovered log start {recovered_log_start} for {}/{}",
                topic_partition.topic_id, topic_partition.partition_id
            )));
        }
        state.log_start = recovered_log_start;
        result.insert(topic_partition, Arc::new(Mutex::new(state)));
    }
    if let Some((topic_partition, _)) = by_partition.into_iter().next() {
        return Err(EngineError::CorruptState(format!(
            "partition {}/{} has reservations but no configuration",
            topic_partition.topic_id, topic_partition.partition_id
        )));
    }
    Ok(result)
}

fn check_producer(
    state: &PartitionState,
    request_id: u128,
    producer: Option<ProducerIdentity>,
    record_count: NonZeroU32,
    payload_digest: [u8; 32],
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
                let digest_matches = record.digest_algorithm == DIGEST_BLAKE3
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
        Some(cursor) => {
            cursor.next_sequence = next_sequence;
        }
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

fn limit_fetched_batches(batches: Vec<StoredBatch>, max_bytes: usize) -> Vec<FetchedBatch> {
    let mut result = Vec::new();
    let mut used = 0usize;
    for batch in batches {
        let next = used.saturating_add(batch.payload.len());
        if !result.is_empty() && next > max_bytes {
            break;
        }
        used = next;
        result.push(FetchedBatch {
            batch_id: batch.reservation.batch_id,
            first_offset: batch.reservation.first_offset,
            last_offset: batch.reservation.last_offset,
            record_count: batch.reservation.record_count,
            payload: batch.payload,
            placement: batch.reservation.placement,
        });
    }
    result
}

fn mark_batch(
    state: &mut PartitionState,
    reservation: Reservation,
    status: BatchStatus,
    durable_replicas: u32,
) -> EngineResult<()> {
    let batch = state
        .batches
        .get_mut(&reservation.first_offset)
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
    batch.durable_replicas = durable_replicas;
    Ok(())
}

fn set_recovered_status(
    reservations: &HashMap<(TopicPartition, BatchId), (Reservation, Option<ProducerSequence>)>,
    statuses: &mut HashMap<(TopicPartition, BatchId), RecoveredStatus>,
    key: (TopicPartition, BatchId),
    status: BatchStatus,
    durable_replicas: u32,
    object_durable: bool,
) -> EngineResult<()> {
    if !reservations.contains_key(&key) {
        return Err(EngineError::CorruptState(format!(
            "lifecycle event for batch {} precedes its reservation",
            key.1
        )));
    }
    if statuses.insert(key, (status, durable_replicas, object_durable))
        != Some((BatchStatus::Pending, 0, false))
    {
        return Err(EngineError::CorruptState(format!(
            "duplicate lifecycle completion for batch {}",
            key.1
        )));
    }
    Ok(())
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

fn batch_key(reservation: &Reservation) -> (TopicPartition, BatchId) {
    (
        TopicPartition::new(reservation.topic_id, reservation.partition_id),
        reservation.batch_id,
    )
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

fn mutex_lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use shard_stream_core::{PlacementSequence, TopicId};
    use shard_stream_storage::ShardStore;

    use super::*;

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
            replication_factor: 3,
            min_in_sync_replicas: 2,
            queue_slots_per_shard: 64,
            queue_bytes_per_shard: 2 * 1024 * 1024,
            target_pack_bytes: 1024,
            max_batch_bytes: 64 * 1024,
            max_fetch_bytes: 1024 * 1024,
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
            payload: payload.to_vec(),
            durability: Durability::Leader,
            producer,
        }
    }

    fn fetch_request(start_offset: u128) -> FetchRequest {
        FetchRequest {
            request_id: 1,
            topic_id: TopicId::new(1),
            partition_id: LogicalPartitionId::new(0),
            start_offset: LogicalOffset::new(start_offset),
            max_bytes: 1024,
            mode: FetchMode::Ordered,
        }
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
            responses.map(|response| response.placement.shard_id.get()),
            [0, 1, 2, 0]
        );
        let fetched = engine.fetch(fetch_request(0)).expect("fetch");
        assert_eq!(
            fetched
                .iter()
                .map(|batch| batch.payload.as_slice())
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
    fn restart_recovers_extents_sequencer_and_deduplication() {
        let temp = TempDir::new("restart");
        let producer = ProducerIdentity {
            producer_id: 55,
            epoch: 0,
            first_sequence: 0,
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
    fn unresolved_reservation_recovers_as_abort_range() {
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
            batch_id: BatchId::new(0),
            first_offset: LogicalOffset::new(0),
            last_offset: LogicalOffset::new(1),
            record_count: NonZeroU32::new(2).expect("nonzero"),
            placement: shard_stream_core::Placement {
                shard_id: ShardId::new(0),
                ring_epoch: RingEpoch::new(1),
                sequence: PlacementSequence::new(0),
            },
        };
        journal
            .append(
                &JournalEvent::Reserve {
                    reservation,
                    producer: None,
                },
                true,
            )
            .expect("reserve");
        drop(journal);

        let engine = StreamEngine::open(engine_config).expect("recover");
        let appended = engine
            .append(append_request(1, b"after-gap", None))
            .expect("append");
        assert_eq!(appended.first_offset, LogicalOffset::new(2));
        assert_eq!(
            engine
                .watermarks(topic_partition)
                .expect("watermarks")
                .contiguous_log_end,
            LogicalOffset::new(3)
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
    fn quorum_append_advances_replicated_high_watermark() {
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
        for replica_index in 1..3 {
            let path = temp
                .0
                .join("replicas")
                .join(format!("replica-{replica_index}"))
                .join("shard-0")
                .join("pack-00000000000000000000.sse");
            assert!(path.exists(), "missing local replica pack {path:?}");
        }
    }

    #[test]
    fn increasing_replication_factor_catches_up_existing_extents() {
        let temp = TempDir::new("replica-catch-up");
        let mut single_copy = config(&temp.0);
        single_copy.replication_factor = 1;
        single_copy.min_in_sync_replicas = 1;
        {
            let engine = StreamEngine::open(single_copy).expect("open single-copy engine");
            engine
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(1),
                    partitions: 1,
                    shards: None,
                })
                .expect("create");
            engine
                .append(append_request(1, b"before-replication", None))
                .expect("append");
        }

        let engine = StreamEngine::open(config(&temp.0)).expect("open replicated engine");
        let topic_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        assert_eq!(
            engine
                .watermarks(topic_partition)
                .expect("watermarks")
                .replicated_high_watermark,
            LogicalOffset::new(1)
        );
        assert_eq!(
            engine.fetch(fetch_request(0)).expect("fetch")[0].payload,
            b"before-replication"
        );

        for replica_index in 1..3 {
            let replica = ShardStore::open(shard_stream_storage::ShardStoreConfig {
                directory: temp
                    .0
                    .join("replicas")
                    .join(format!("replica-{replica_index}"))
                    .join("shard-0"),
                shard_id: ShardId::new(0),
                target_pack_bytes: 1024,
                max_batch_bytes: 64 * 1024,
            })
            .expect("open replica");
            assert_eq!(replica.catalog().len(), 1);
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

        let tier = shard_stream_storage::LocalObjectTier::open(
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
}
