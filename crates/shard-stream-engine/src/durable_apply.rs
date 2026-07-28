use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use shard_stream_core::{BatchId, TopicPartition};
use shard_stream_protocol::{AppendResponse, ReplicaAppend};

use crate::engine::ReplicaAppendPlan;
use crate::{EngineError, EngineResult, StreamEngine};

const MAX_REPLICA_APPEND_GROUP: usize = 8;
const REPLICA_APPEND_GROUP_LINGER: Duration = Duration::from_micros(200);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableApplyConfig {
    pub worker_count: usize,
    pub queue_slots_per_worker: usize,
    pub max_pending_per_partition: usize,
    pub max_gap_batches: u128,
    pub max_pending_bytes_per_partition: usize,
}

impl Default for DurableApplyConfig {
    fn default() -> Self {
        Self {
            worker_count: 4,
            queue_slots_per_worker: 1_024,
            max_pending_per_partition: 256,
            max_gap_batches: 256,
            max_pending_bytes_per_partition: 256 * 1024 * 1024,
        }
    }
}

impl DurableApplyConfig {
    fn validate(self) -> EngineResult<Self> {
        if self.worker_count == 0
            || self.queue_slots_per_worker == 0
            || self.max_pending_per_partition == 0
            || self.max_gap_batches == 0
            || self.max_pending_bytes_per_partition == 0
        {
            return Err(EngineError::InvalidConfig(
                "durable apply worker, queue, pending, and gap bounds must be nonzero".into(),
            ));
        }
        Ok(self)
    }
}

struct AdmissionCommand {
    batch: ReplicaAppend,
    response: SyncSender<EngineResult<AppendResponse>>,
}

struct PendingBatch {
    batch: ReplicaAppend,
    waiters: Vec<SyncSender<EngineResult<AppendResponse>>>,
}

struct DurableApplyWork {
    batch: ReplicaAppend,
    plan: ReplicaAppendPlan,
    waiters: Vec<SyncSender<EngineResult<AppendResponse>>>,
}

struct PartitionAdmission {
    next_batch_id: u128,
    pending: BTreeMap<u128, PendingBatch>,
    pending_bytes: usize,
}

struct AdmissionWorker {
    sender: Option<SyncSender<AdmissionCommand>>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for AdmissionWorker {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct AppendWorker {
    sender: Option<SyncSender<DurableApplyWork>>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for AppendWorker {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Bounded, actor-sharded durable application for externally reserved batches.
///
/// Each partition is owned by exactly one worker. Batches may arrive in any
/// lane completion order. The admission actor reserves them in contiguous
/// batch-ID order, then bounded physical-lane workers persist them concurrently.
///
/// This queue owns no network, membership, or failover behavior. Extended
/// distributions may place it behind a transport implementation.
pub struct DurableApplyQueue {
    // Admission workers must stop before append workers so none can dispatch
    // into a lane queue while that queue is being torn down.
    admission_workers: Vec<AdmissionWorker>,
    append_workers: Vec<AppendWorker>,
}

impl fmt::Debug for DurableApplyQueue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableApplyQueue")
            .field("admission_workers", &self.admission_workers.len())
            .field("append_workers", &self.append_workers.len())
            .finish()
    }
}

impl DurableApplyQueue {
    pub fn spawn(engine: Arc<StreamEngine>, config: DurableApplyConfig) -> EngineResult<Self> {
        let config = config.validate()?;
        let append_worker_count = engine.replica_append_worker_count();
        let mut append_workers = Vec::with_capacity(append_worker_count);
        let mut append_senders = Vec::with_capacity(append_worker_count);
        for index in 0..append_worker_count {
            let (sender, receiver) =
                sync_channel::<DurableApplyWork>(config.queue_slots_per_worker);
            let engine = Arc::clone(&engine);
            let thread = thread::Builder::new()
                .name(format!("shard-stream-replica-append-{index}"))
                .spawn(move || {
                    while let Ok(work) = receiver.recv() {
                        let mut works = vec![work];
                        let deadline = Instant::now() + REPLICA_APPEND_GROUP_LINGER;
                        while works.len() < MAX_REPLICA_APPEND_GROUP {
                            match receiver
                                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                            {
                                Ok(work) => works.push(work),
                                Err(_) => break,
                            }
                        }
                        let mut waiters = Vec::with_capacity(works.len());
                        let appends = works
                            .into_iter()
                            .map(|work| {
                                waiters.push(work.waiters);
                                (work.batch, work.plan)
                            })
                            .collect::<Vec<_>>();
                        let results = engine.execute_replica_append_group(appends);
                        for (waiters, result) in waiters.into_iter().zip(results) {
                            respond_all(waiters, result);
                        }
                    }
                })
                .map_err(|error| {
                    EngineError::InvalidConfig(format!(
                        "failed to spawn durable apply worker: {error}"
                    ))
                })?;
            append_senders.push(sender.clone());
            append_workers.push(AppendWorker {
                sender: Some(sender),
                thread: Some(thread),
            });
        }

        let mut admission_workers = Vec::with_capacity(config.worker_count);
        for index in 0..config.worker_count {
            let (sender, receiver) = sync_channel(config.queue_slots_per_worker);
            let engine = Arc::clone(&engine);
            let append_senders = append_senders.clone();
            let thread = thread::Builder::new()
                .name(format!("shard-stream-replica-admission-{index}"))
                .spawn(move || {
                    let mut partitions = HashMap::new();
                    while let Ok(command) = receiver.recv() {
                        admit(&engine, &append_senders, config, &mut partitions, command);
                    }
                })
                .map_err(|error| {
                    EngineError::InvalidConfig(format!(
                        "failed to spawn durable apply worker: {error}"
                    ))
                })?;
            admission_workers.push(AdmissionWorker {
                sender: Some(sender),
                thread: Some(thread),
            });
        }
        Ok(Self {
            admission_workers,
            append_workers,
        })
    }

    pub fn append(&self, batch: ReplicaAppend) -> EngineResult<AppendResponse> {
        let topic_partition =
            TopicPartition::new(batch.reservation.topic_id, batch.reservation.partition_id);
        let index = partition_worker(topic_partition, self.admission_workers.len());
        let sender = self.admission_workers[index]
            .sender
            .as_ref()
            .ok_or_else(|| EngineError::CorruptState("durable apply worker stopped".into()))?;
        let (response, receiver) = sync_channel(1);
        sender
            .send(AdmissionCommand { batch, response })
            .map_err(|_| EngineError::CorruptState("durable apply worker stopped".into()))?;
        receiver
            .recv()
            .map_err(|_| EngineError::CorruptState("durable apply response lost".into()))?
    }
}

fn admit(
    engine: &StreamEngine,
    append_senders: &[SyncSender<DurableApplyWork>],
    config: DurableApplyConfig,
    partitions: &mut HashMap<TopicPartition, PartitionAdmission>,
    command: AdmissionCommand,
) {
    let topic_partition = TopicPartition::new(
        command.batch.reservation.topic_id,
        command.batch.reservation.partition_id,
    );
    let state = match partitions.entry(topic_partition) {
        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::hash_map::Entry::Vacant(entry) => {
            let next = match engine.replica_next_batch(topic_partition) {
                Ok(next) => next.get(),
                Err(error) => {
                    let _ = command.response.send(Err(error));
                    return;
                }
            };
            entry.insert(PartitionAdmission {
                next_batch_id: next,
                pending: BTreeMap::new(),
                pending_bytes: 0,
            })
        }
    };
    let received = command.batch.reservation.batch_id.get();
    if received < state.next_batch_id {
        reserve_and_dispatch(
            engine,
            append_senders,
            command.batch,
            vec![command.response],
        );
        return;
    }
    if received > state.next_batch_id {
        let gap = received - state.next_batch_id;
        let received_bytes = command.batch.payload.len();
        if gap > config.max_gap_batches
            || (!state.pending.contains_key(&received)
                && state.pending.len() >= config.max_pending_per_partition)
        {
            let _ = command
                .response
                .send(Err(EngineError::ReplicaReorderLimited {
                    expected: BatchId::new(state.next_batch_id),
                    received: BatchId::new(received),
                    max_gap_batches: config.max_gap_batches,
                }));
            return;
        }
        if !state.pending.contains_key(&received)
            && received_bytes
                > config
                    .max_pending_bytes_per_partition
                    .saturating_sub(state.pending_bytes)
        {
            let _ = command
                .response
                .send(Err(EngineError::ReplicaBufferLimited {
                    pending_bytes: state.pending_bytes,
                    received_bytes,
                    max_pending_bytes: config.max_pending_bytes_per_partition,
                }));
            return;
        }
        match state.pending.entry(received) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                state.pending_bytes = state.pending_bytes.saturating_add(received_bytes);
                entry.insert(PendingBatch {
                    batch: command.batch,
                    waiters: vec![command.response],
                });
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if entry.get().batch == command.batch {
                    entry.get_mut().waiters.push(command.response);
                } else {
                    let _ = command.response.send(Err(EngineError::CorruptState(format!(
                        "conflicting buffered replica batch {}",
                        command.batch.reservation.batch_id
                    ))));
                }
            }
        }
        return;
    }

    if reserve_and_dispatch(
        engine,
        append_senders,
        command.batch,
        vec![command.response],
    ) {
        state.next_batch_id = state.next_batch_id.saturating_add(1);
        drain_contiguous(engine, append_senders, state);
    }
}

fn drain_contiguous(
    engine: &StreamEngine,
    append_senders: &[SyncSender<DurableApplyWork>],
    state: &mut PartitionAdmission,
) {
    while let Some(pending) = state.pending.remove(&state.next_batch_id) {
        state.pending_bytes = state
            .pending_bytes
            .saturating_sub(pending.batch.payload.len());
        if reserve_and_dispatch(engine, append_senders, pending.batch, pending.waiters) {
            state.next_batch_id = state.next_batch_id.saturating_add(1);
        } else {
            break;
        }
    }
}

fn reserve_and_dispatch(
    engine: &StreamEngine,
    append_senders: &[SyncSender<DurableApplyWork>],
    batch: ReplicaAppend,
    waiters: Vec<SyncSender<EngineResult<AppendResponse>>>,
) -> bool {
    let plan = match engine.plan_replica_append(&batch) {
        Ok(ReplicaAppendPlan::Duplicate(response)) => {
            respond_all(waiters, Ok(response));
            return true;
        }
        Ok(plan @ ReplicaAppendPlan::Reserved { .. }) => plan,
        Err(error) => {
            respond_all(waiters, Err(error));
            return false;
        }
    };
    let worker_index =
        engine.replica_append_worker_index(batch.reservation.placement.virtual_lane_id);
    let Some(sender) = append_senders.get(worker_index) else {
        let _ = engine.cancel_replica_append(&batch);
        respond_all(
            waiters,
            Err(EngineError::CorruptState(format!(
                "durable apply worker {worker_index} is missing"
            ))),
        );
        return false;
    };
    if let Err(error) = sender.send(DurableApplyWork {
        batch,
        plan,
        waiters,
    }) {
        let work = error.0;
        let _ = engine.cancel_replica_append(&work.batch);
        respond_all(
            work.waiters,
            Err(EngineError::CorruptState(format!(
                "durable apply worker {worker_index} stopped"
            ))),
        );
        return false;
    }
    true
}

fn respond_all(
    waiters: Vec<SyncSender<EngineResult<AppendResponse>>>,
    result: EngineResult<AppendResponse>,
) {
    match result {
        Ok(response) => {
            for waiter in waiters {
                let _ = waiter.send(Ok(response));
            }
        }
        Err(error) => {
            let message = error.to_string();
            let mut waiters = waiters.into_iter();
            if let Some(waiter) = waiters.next() {
                let _ = waiter.send(Err(error));
            }
            for waiter in waiters {
                let _ = waiter.send(Err(EngineError::CorruptState(message.clone())));
            }
        }
    }
}

fn partition_worker(topic_partition: TopicPartition, worker_count: usize) -> usize {
    let mut key = [0u8; 20];
    key[..16].copy_from_slice(&topic_partition.topic_id.get().to_le_bytes());
    key[16..].copy_from_slice(&topic_partition.partition_id.get().to_le_bytes());
    xxhash_rust::xxh3::xxh3_64(&key) as usize % worker_count
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::num::NonZeroU32;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use bytes::Bytes;
    use shard_stream_core::{
        LogicalOffset, LogicalPartitionId, Placement, PlacementSequence, RingEpoch, TopicId,
        VirtualLaneId,
    };

    use super::*;
    use crate::{EngineConfig, TopicConfig};

    static NEXT_TEMP_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let sequence = NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "shard-stream-replica-admission-{}-{nonce}-{sequence}",
                std::process::id(),
            ));
            fs::create_dir_all(&path).expect("temp directory");
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn engine(directory: &std::path::Path) -> Arc<StreamEngine> {
        let engine = Arc::new(
            StreamEngine::open(EngineConfig {
                data_dir: directory.to_path_buf(),
                object_store_dir: None,
                shard_count: 2,
                virtual_lane_count: 2,
                replication_factor: 1,
                min_in_sync_replicas: 1,
                queue_slots_per_shard: 64,
                queue_bytes_per_shard: 2 * 1024 * 1024,
                target_pack_bytes: 1024 * 1024,
                max_pack_age: Duration::from_secs(1),
                max_batch_bytes: 1024 * 1024,
                max_fetch_bytes: 1024 * 1024,
                append_linger: Duration::from_millis(1),
            })
            .expect("engine"),
        );
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(7),
                partitions: 1,
                shards: None,
            })
            .expect("topic");
        engine
    }

    fn batch(batch_id: u128) -> ReplicaAppend {
        let offset = u64::try_from(batch_id).expect("test offset");
        ReplicaAppend {
            reservation: shard_stream_core::Reservation {
                topic_id: TopicId::new(7),
                partition_id: LogicalPartitionId::new(0),
                batch_id: BatchId::new(batch_id),
                first_offset: LogicalOffset::new(offset),
                last_offset: LogicalOffset::new(offset),
                record_count: NonZeroU32::new(1).expect("count"),
                placement: Placement {
                    virtual_lane_id: VirtualLaneId::new(offset as u32 % 2),
                    ring_epoch: RingEpoch::new(1),
                    leader_epoch: shard_stream_core::LeaderEpoch::new(0),
                    sequence: PlacementSequence::new(offset),
                },
            },
            producer: None,
            atomic_group: None,
            extension_context: None,
            payload_digest: [0; 16],
            payload: Bytes::from(vec![batch_id as u8]),
        }
    }

    #[test]
    fn buffers_out_of_order_batches_until_the_gap_is_durable() {
        let directory = TempDir::new();
        let engine = engine(&directory.0);
        let admission = Arc::new(
            DurableApplyQueue::spawn(
                Arc::clone(&engine),
                DurableApplyConfig {
                    worker_count: 1,
                    ..DurableApplyConfig::default()
                },
            )
            .expect("admission"),
        );
        let delayed = {
            let admission = Arc::clone(&admission);
            thread::spawn(move || admission.append(batch(1)))
        };
        thread::sleep(Duration::from_millis(10));
        admission.append(batch(0)).expect("first batch");
        delayed.join().expect("join").expect("second batch");

        let watermarks = engine
            .watermarks(TopicPartition::new(
                TopicId::new(7),
                LogicalPartitionId::new(0),
            ))
            .expect("watermarks");
        assert_eq!(watermarks.contiguous_log_end, LogicalOffset::new(2));
        assert_eq!(
            engine
                .replica_next_batch(TopicPartition::new(
                    TopicId::new(7),
                    LogicalPartitionId::new(0)
                ))
                .expect("next"),
            BatchId::new(2)
        );
    }

    #[test]
    fn rejects_batches_beyond_the_bounded_reorder_window() {
        let directory = TempDir::new();
        let engine = engine(&directory.0);
        let admission = DurableApplyQueue::spawn(
            engine,
            DurableApplyConfig {
                worker_count: 1,
                max_gap_batches: 2,
                ..DurableApplyConfig::default()
            },
        )
        .expect("admission");
        assert!(matches!(
            admission.append(batch(3)),
            Err(EngineError::ReplicaReorderLimited {
                expected,
                received,
                max_gap_batches: 2,
            }) if expected == BatchId::new(0) && received == BatchId::new(3)
        ));
    }

    #[test]
    fn bounds_out_of_order_payload_bytes_per_partition() {
        let directory = TempDir::new();
        let engine = engine(&directory.0);
        let admission = Arc::new(
            DurableApplyQueue::spawn(
                engine,
                DurableApplyConfig {
                    worker_count: 1,
                    max_pending_bytes_per_partition: 1,
                    ..DurableApplyConfig::default()
                },
            )
            .expect("admission"),
        );
        let delayed = {
            let admission = Arc::clone(&admission);
            thread::spawn(move || admission.append(batch(3)))
        };
        thread::sleep(Duration::from_millis(10));
        assert!(matches!(
            admission.append(batch(2)),
            Err(EngineError::ReplicaBufferLimited {
                pending_bytes: 1,
                received_bytes: 1,
                max_pending_bytes: 1,
            })
        ));
        admission.append(batch(0)).expect("batch zero");
        admission.append(batch(1)).expect("batch one");
        admission.append(batch(2)).expect("batch two retry");
        delayed.join().expect("join").expect("batch three");
    }
}
