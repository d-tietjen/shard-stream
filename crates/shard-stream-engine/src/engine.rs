use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::mpsc::channel;

use bytes::Bytes;
use shard_stream_control::WriteFence;
use shard_stream_core::{
    BatchId, LogicalOffset, LogicalPartitionId, Placement, Reservation, RingEpoch, ShardId,
    TopicPartition,
};
use shard_stream_protocol::{AppendRequest, AppendResponse, Durability, FetchMode, FetchRequest};
use shard_stream_storage::{CoordinatorJournal, StoredBatch, StoredBatchMetadata};

use crate::coordinator::{
    CommittedBatchSet, CompleteAppend, ControlJournal, CoordinatorPool, ReserveOutcome,
    recover_partition_states, retention_hints,
};
use crate::worker::{FetchCompletion, ShardHandle};
use crate::{EngineConfig, EngineError, EngineResult, TopicConfig};

const MAX_FETCH_PLAN_BATCHES_PER_SHARD: usize = 4_096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedBatch {
    pub batch_id: BatchId,
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

pub struct StreamEngine {
    config: EngineConfig,
    shards: Vec<ShardHandle>,
    coordinators: CoordinatorPool,
    journal: ControlJournal,
    write_fence: Option<Arc<dyn WriteFence>>,
}

impl StreamEngine {
    pub fn open(config: EngineConfig) -> EngineResult<Self> {
        Self::open_inner(config, None)
    }

    pub fn open_with_fence(
        config: EngineConfig,
        write_fence: Arc<dyn WriteFence>,
    ) -> EngineResult<Self> {
        Self::open_inner(config, Some(write_fence))
    }

    fn open_inner(
        config: EngineConfig,
        write_fence: Option<Arc<dyn WriteFence>>,
    ) -> EngineResult<Self> {
        config.validate()?;
        std::fs::create_dir_all(&config.data_dir)
            .map_err(shard_stream_storage::StorageError::from)?;

        let (journal, recovered) =
            CoordinatorJournal::open(config.data_dir.join("coordinator.ssj"))?;
        let retained_log_starts = retention_hints(&recovered.events);
        let mut shards = Vec::with_capacity(config.shard_count as usize);
        let mut catalog = HashMap::new();
        for shard_id in config.shard_ids() {
            let (handle, entries) = ShardHandle::spawn(&config, shard_id, &retained_log_starts)?;
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

        let recovered_states = recover_partition_states(&config, &recovered.events, &catalog)?;
        let journal = ControlJournal::spawn(journal)?;
        let coordinators =
            CoordinatorPool::spawn(config.shard_count as usize, recovered_states, &journal)?;
        let replica_progress = coordinators.router();
        for shard in &shards {
            shard.install_replica_progress(replica_progress.clone())?;
        }
        let engine = Self {
            config,
            shards,
            coordinators,
            journal,
            write_fence,
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
        let shards = topic
            .shards
            .clone()
            .unwrap_or_else(|| self.config.shard_ids());
        validate_ring(&self.config, &shards)?;
        self.coordinators
            .create_topic(topic, shards, self.config.min_in_sync_replicas)
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
        let payload_digest = request.producer.map_or([0; 16], |_| {
            xxhash_rust::xxh3::xxh3_128(&request.payload).to_le_bytes()
        });
        let (reservation, producer) = match self.coordinators.reserve(
            topic_partition,
            request.request_id,
            request.producer,
            record_count,
            payload_digest,
            request.durability,
            self.config.min_in_sync_replicas,
        )? {
            ReserveOutcome::Duplicate(response) => return Ok(response),
            ReserveOutcome::Reserved {
                reservation,
                producer,
            } => (reservation, producer),
        };

        let required_replicas = if request.durability == Durability::Quorum {
            self.config.min_in_sync_replicas
        } else {
            1
        };
        let append_result = self.shard(reservation.placement.shard_id)?.append(
            reservation,
            producer,
            request.payload,
            required_replicas,
            request.durability == Durability::Object,
        );
        let response = append_response(request.request_id, reservation);
        match append_result {
            Ok(durability) => {
                self.coordinators.complete(
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
        let bounds = self
            .coordinators
            .fetch_snapshot(topic_partition, request.start_offset)?;

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

        let committed = if request.mode == FetchMode::AsAvailable {
            let candidates = shard_plans
                .iter()
                .flatten()
                .map(|metadata| metadata.reservation().batch_id)
                .collect();
            Some(
                self.coordinators
                    .committed_candidates(topic_partition, candidates)?,
            )
        } else {
            None
        };
        let selection = select_fetch_plan(
            &shard_plans,
            request.mode,
            bounds.ordered_end,
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
        materialize_fetched_batches(shard_batches, &selection.merge_order)
    }

    pub fn watermarks(&self, topic_partition: TopicPartition) -> EngineResult<PartitionWatermarks> {
        self.coordinators
            .watermarks(topic_partition, self.config.min_in_sync_replicas)
    }

    #[must_use]
    pub fn topic_partitions(
        &self,
        topic_id: shard_stream_core::TopicId,
    ) -> Vec<LogicalPartitionId> {
        self.coordinators.topic_partitions(topic_id)
    }

    pub fn truncate_partition(
        &self,
        topic_partition: TopicPartition,
        log_start: LogicalOffset,
    ) -> EngineResult<PartitionWatermarks> {
        let updated = self.coordinators.truncate(
            topic_partition,
            log_start,
            self.config.min_in_sync_replicas,
        )?;
        self.collect_garbage(topic_partition, log_start)?;
        Ok(updated)
    }

    pub fn sync(&self) -> EngineResult<()> {
        for shard in &self.shards {
            shard.sync()?;
        }
        self.journal.sync()?;
        Ok(())
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

fn materialize_fetched_batches(
    mut lanes: Vec<VecDeque<StoredBatch>>,
    merge_order: &[usize],
) -> EngineResult<Vec<FetchedBatch>> {
    let mut result = Vec::with_capacity(merge_order.len());
    for &lane in merge_order {
        let batch = lanes
            .get_mut(lane)
            .and_then(VecDeque::pop_front)
            .ok_or_else(|| {
                EngineError::CorruptState("fetch materialization did not match its plan".into())
            })?;
        result.push(FetchedBatch {
            batch_id: batch.reservation.batch_id,
            first_offset: batch.reservation.first_offset,
            last_offset: batch.reservation.last_offset,
            record_count: batch.reservation.record_count,
            payload: batch.payload,
            placement: batch.reservation.placement,
        });
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

        let visible = match mode {
            FetchMode::Ordered => metadata.reservation().last_offset < ordered_end,
            FetchMode::AsAvailable => committed
                .expect("as-available planning has a committed snapshot")
                .contains(metadata.reservation().batch_id),
        };
        if !visible {
            if mode == FetchMode::Ordered {
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
        first_offset: reservation.first_offset,
        last_offset: reservation.last_offset,
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use shard_stream_core::{PlacementSequence, TopicId};
    use shard_stream_protocol::{FetchMode, ProducerIdentity};
    use shard_stream_storage::{JournalEvent, ShardStore};

    use super::*;

    struct ExactEpochFence;

    impl shard_stream_control::WriteFence for ExactEpochFence {
        fn check_write(
            &self,
            _topic_partition: TopicPartition,
            leader_epoch: u64,
        ) -> shard_stream_control::ControlResult<()> {
            if leader_epoch == 7 {
                Ok(())
            } else {
                Err(shard_stream_control::ControlError::Fenced {
                    expected_epoch: 7,
                    received_epoch: leader_epoch,
                })
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
            replication_factor: 3,
            min_in_sync_replicas: 2,
            queue_slots_per_shard: 64,
            queue_bytes_per_shard: 2 * 1024 * 1024,
            target_pack_bytes: 1024,
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
            leader_epoch: None,
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
                shard_id: ShardId::new(1),
                ring_epoch: RingEpoch::new(1),
                sequence: PlacementSequence::new(1),
            },
        };
        drop(journal);
        let mut shard = ShardStore::open(shard_stream_storage::ShardStoreConfig {
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
        engine.sync().expect("replica drain");
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
            engine.fetch(fetch_request(0)).expect("fetch")[0]
                .payload
                .as_ref(),
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
                .all(|(offset, batch)| batch.first_offset == LogicalOffset::new(offset as u128))
        );
        drop(engine);

        let (_, recovered_journal) =
            CoordinatorJournal::open(engine_config.data_dir.join("coordinator.ssj"))
                .expect("journal");
        assert_eq!(recovered_journal.events.len(), 1);
        assert!(matches!(
            recovered_journal.events[0],
            JournalEvent::Configure { .. }
        ));

        let engine = StreamEngine::open(engine_config).expect("reopen");
        let recovered = engine.watermarks(topic_partition).expect("recovered");
        assert_eq!(recovered.allocated_end, LogicalOffset::new(256));
        assert_eq!(recovered.contiguous_log_end, LogicalOffset::new(256));
    }
}
