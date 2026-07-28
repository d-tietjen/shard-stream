use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, SyncSender, TryRecvError, sync_channel};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bytes::Bytes;
use shard_stream_core::{
    ByteBoundedSender, LogicalOffset, Reservation, ShardId, TopicPartition, TrySendError,
    byte_bounded_channel,
};
use shardlog::{
    AtomicGroupSequence, ExtentCatalogEntry, LocalObjectTier, ProducerSequence, ShardStore,
    ShardStoreConfig, StorageError, StorageResult, StoredBatch, StoredBatchMetadata, TierExtent,
};

use crate::config::COMMAND_OVERHEAD_BYTES;
use crate::{EngineConfig, EngineError, EngineResult};

const MAX_APPEND_GROUP: usize = 128;
const TARGET_APPEND_GROUP: usize = 8;
const MIN_APPEND_GROUP_LINGER: Duration = Duration::from_micros(200);
const APPEND_GROUP_IDLE_RESET: Duration = Duration::from_millis(4);
const SYNC_QUEUE_SLOTS: usize = 8;
const MAX_SYNC_BATCH: usize = 8;

struct AppendGroupTuner {
    linger: Duration,
    min_linger: Duration,
    max_linger: Duration,
    last_group_completed: Option<Instant>,
}

impl AppendGroupTuner {
    fn new(max_linger: Duration) -> Self {
        let min_linger = MIN_APPEND_GROUP_LINGER.min(max_linger);
        Self {
            linger: min_linger,
            min_linger,
            max_linger,
            last_group_completed: None,
        }
    }

    fn begin_group(&mut self, now: Instant) -> Duration {
        if self
            .last_group_completed
            .is_none_or(|completed| now.duration_since(completed) >= APPEND_GROUP_IDLE_RESET)
        {
            self.linger = self.min_linger;
        }
        self.linger
    }

    fn complete_group(&mut self, now: Instant, group_len: usize) {
        self.last_group_completed = Some(now);
        if group_len < TARGET_APPEND_GROUP {
            self.linger = self.linger.saturating_mul(2).min(self.max_linger);
        } else if group_len == MAX_APPEND_GROUP {
            self.linger = (self.linger / 2).max(self.min_linger);
        }
    }
}

pub(crate) struct AppendFailure {
    pub(crate) error: EngineError,
    pub(crate) definitely_not_written: bool,
}

impl AppendFailure {
    fn rejected(error: EngineError) -> Self {
        Self {
            error,
            definitely_not_written: true,
        }
    }

    pub(crate) fn ambiguous(error: EngineError) -> Self {
        Self {
            error,
            definitely_not_written: false,
        }
    }
}

struct AppendCommand {
    reservation: Reservation,
    producer: Option<ProducerSequence>,
    atomic_group: Option<AtomicGroupSequence>,
    payload: Bytes,
    object_durable: bool,
    response: SyncSender<StorageResult<AppendDurability>>,
}

struct SyncRequest {
    file: File,
    commands: Vec<AppendCommand>,
    outcomes: Vec<StorageResult<AppendDurability>>,
    completion: Option<SyncSender<StorageResult<()>>>,
}

struct AppendGroupOutput {
    outcomes: Vec<StorageResult<AppendDurability>>,
    sync_file: Option<File>,
}

pub(crate) enum FetchCompletion {
    Plan {
        lane: usize,
        result: StorageResult<Vec<StoredBatchMetadata>>,
    },
    Read {
        lane: usize,
        result: StorageResult<Vec<StoredBatch>>,
    },
}

// Append is the hot-path variant. Keeping it inline avoids one allocation per
// batch; queue memory is already bounded by slots and charged bytes.
#[allow(clippy::large_enum_variant)]
enum ShardCommand {
    Append(AppendCommand),
    PlanFetch {
        lane: usize,
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
        max_bytes: usize,
        max_batches: usize,
        completion: Sender<FetchCompletion>,
    },
    ReadFetch {
        lane: usize,
        planned: Vec<StoredBatchMetadata>,
        completion: Sender<FetchCompletion>,
    },
    Sync {
        response: SyncSender<StorageResult<()>>,
    },
    GarbageCollect {
        topic_partition: TopicPartition,
        log_start: LogicalOffset,
        response: SyncSender<StorageResult<()>>,
    },
}

pub(crate) struct ShardHandle {
    pub(crate) shard_id: ShardId,
    sender: Option<ByteBoundedSender<ShardCommand>>,
    thread: Option<JoinHandle<()>>,
    sync_sender: Option<SyncSender<SyncRequest>>,
    sync_thread: Option<JoinHandle<()>>,
}

impl ShardHandle {
    pub(crate) fn spawn(
        config: &EngineConfig,
        shard_id: ShardId,
    ) -> EngineResult<(Self, Vec<ExtentCatalogEntry>)> {
        let directory = shard_directory(&config.data_dir, shard_id);
        let primary = ShardStore::open(ShardStoreConfig {
            directory,
            shard_id,
            target_pack_bytes: config.target_pack_bytes,
            max_batch_bytes: config.max_batch_bytes,
        })?;
        let mut tier = config
            .object_store_dir
            .as_ref()
            .map(|directory| LocalObjectTier::open(directory, shard_id))
            .transpose()?;
        if let Some(tier) = &mut tier {
            offload_sealed_packs(&primary, tier)?;
        }
        let mut catalog = primary.catalog();
        if let Some(tier) = &tier {
            let durable_packs = tier
                .manifest()
                .objects
                .iter()
                .map(|object| object.pack_sequence)
                .collect::<std::collections::HashSet<_>>();
            for entry in &mut catalog {
                entry.object_durable = durable_packs.contains(&entry.pack_sequence);
            }
        }
        let stores = LaneStores { primary, tier };
        let (sync_sender, sync_receiver) = sync_channel(SYNC_QUEUE_SLOTS);
        let sync_thread = thread::Builder::new()
            .name(format!("shard-stream-sync-{shard_id}"))
            .spawn(move || run_sync_worker(sync_receiver))
            .map_err(|error| {
                EngineError::InvalidConfig(format!("failed to spawn sync worker: {error}"))
            })?;
        let (sender, receiver) =
            byte_bounded_channel(config.queue_slots_per_shard, config.queue_bytes_per_shard)
                .map_err(|error| EngineError::InvalidConfig(error.to_string()))?;
        let append_linger = config.append_linger;
        let max_pack_age = config.max_pack_age;
        let append_sync_sender = sync_sender.clone();
        let thread = thread::Builder::new()
            .name(format!("shard-stream-lane-{shard_id}"))
            .spawn(move || {
                run_worker(
                    stores,
                    receiver,
                    append_linger,
                    max_pack_age,
                    append_sync_sender,
                )
            })
            .map_err(|error| {
                EngineError::InvalidConfig(format!("failed to spawn shard worker: {error}"))
            })?;
        Ok((
            Self {
                shard_id,
                sender: Some(sender),
                thread: Some(thread),
                sync_sender: Some(sync_sender),
                sync_thread: Some(sync_thread),
            },
            catalog,
        ))
    }

    pub(crate) fn append(
        &self,
        reservation: Reservation,
        producer: Option<ProducerSequence>,
        atomic_group: Option<AtomicGroupSequence>,
        payload: Bytes,
        object_durable: bool,
    ) -> Result<AppendDurability, AppendFailure> {
        let (response, receiver) = sync_channel(1);
        self.begin_append(
            reservation,
            producer,
            atomic_group,
            payload,
            object_durable,
            response,
        )?;
        receiver
            .recv()
            .map_err(|_| AppendFailure::ambiguous(EngineError::WorkerStopped(self.shard_id)))?
            .map_err(|error| AppendFailure::ambiguous(error.into()))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn begin_append(
        &self,
        reservation: Reservation,
        producer: Option<ProducerSequence>,
        atomic_group: Option<AtomicGroupSequence>,
        payload: Bytes,
        object_durable: bool,
        response: SyncSender<StorageResult<AppendDurability>>,
    ) -> Result<(), AppendFailure> {
        let charged_bytes = payload
            .len()
            .checked_add(COMMAND_OVERHEAD_BYTES)
            .ok_or_else(|| {
                AppendFailure::rejected(EngineError::AdmissionLimited {
                    shard_id: self.shard_id,
                    reason: "append byte charge overflow",
                })
            })?;
        self.try_send(
            ShardCommand::Append(AppendCommand {
                reservation,
                producer,
                atomic_group,
                payload,
                object_durable,
                response,
            }),
            charged_bytes,
        )
        .map_err(AppendFailure::rejected)
    }

    pub(crate) fn begin_fetch_plan(
        &self,
        lane: usize,
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
        max_bytes: usize,
        max_batches: usize,
        completion: Sender<FetchCompletion>,
    ) -> EngineResult<()> {
        let charged_bytes =
            max_bytes
                .checked_add(COMMAND_OVERHEAD_BYTES)
                .ok_or(EngineError::AdmissionLimited {
                    shard_id: self.shard_id,
                    reason: "fetch byte charge overflow",
                })?;
        self.try_send(
            ShardCommand::PlanFetch {
                lane,
                topic_partition,
                start_offset,
                max_bytes,
                max_batches,
                completion,
            },
            charged_bytes,
        )
    }

    pub(crate) fn begin_fetch_read(
        &self,
        lane: usize,
        planned: Vec<StoredBatchMetadata>,
        completion: Sender<FetchCompletion>,
    ) -> EngineResult<()> {
        let charged_bytes = planned
            .iter()
            .try_fold(COMMAND_OVERHEAD_BYTES, |total, metadata| {
                total.checked_add(metadata.payload_len())
            });
        let charged_bytes = charged_bytes.ok_or(EngineError::AdmissionLimited {
            shard_id: self.shard_id,
            reason: "fetch read byte charge overflow",
        })?;
        self.try_send(
            ShardCommand::ReadFetch {
                lane,
                planned,
                completion,
            },
            charged_bytes,
        )
    }

    pub(crate) fn begin_sync(&self) -> EngineResult<Receiver<StorageResult<()>>> {
        let (response, receiver) = sync_channel(1);
        self.try_send(ShardCommand::Sync { response }, COMMAND_OVERHEAD_BYTES)?;
        Ok(receiver)
    }

    pub(crate) fn garbage_collect(
        &self,
        topic_partition: TopicPartition,
        log_start: LogicalOffset,
    ) -> EngineResult<()> {
        let (response, receiver) = sync_channel(1);
        self.try_send(
            ShardCommand::GarbageCollect {
                topic_partition,
                log_start,
                response,
            },
            COMMAND_OVERHEAD_BYTES,
        )?;
        receiver
            .recv()
            .map_err(|_| EngineError::WorkerStopped(self.shard_id))??;
        Ok(())
    }

    fn try_send(&self, command: ShardCommand, bytes: usize) -> EngineResult<()> {
        let Some(sender) = &self.sender else {
            return Err(EngineError::WorkerStopped(self.shard_id));
        };
        sender
            .try_send(command, bytes)
            .map_err(|error| match error {
                TrySendError::TooLarge { .. } => EngineError::AdmissionLimited {
                    shard_id: self.shard_id,
                    reason: "command exceeds shard byte budget",
                },
                TrySendError::ByteBudget { .. } => EngineError::AdmissionLimited {
                    shard_id: self.shard_id,
                    reason: "shard byte budget exhausted",
                },
                TrySendError::Full(_) => EngineError::AdmissionLimited {
                    shard_id: self.shard_id,
                    reason: "shard slot budget exhausted",
                },
                TrySendError::Disconnected(_) => EngineError::WorkerStopped(self.shard_id),
            })
    }
}

impl Drop for ShardHandle {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.sync_sender.take();
        if let Some(thread) = self.sync_thread.take() {
            let _ = thread.join();
        }
    }
}

fn run_sync_worker(receiver: Receiver<SyncRequest>) {
    while let Ok(first) = receiver.recv() {
        let mut requests = vec![first];
        while requests.len() < MAX_SYNC_BATCH {
            match receiver.try_recv() {
                Ok(request) => {
                    let is_barrier = request.commands.is_empty();
                    requests.push(request);
                    if is_barrier {
                        break;
                    }
                }
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }

        // The append worker submits fences in write order. Pack rotation
        // synchronizes the previous active file before switching files, so a
        // sync of the newest descriptor covers every request in this batch
        // without sharing a lock or mutable ShardStore state.
        let result = requests
            .last()
            .expect("sync batch is nonempty")
            .file
            .sync_data()
            .map_err(StorageError::from);
        for request in requests {
            if request.commands.is_empty() {
                if let Some(completion) = request.completion {
                    let _ =
                        completion.send(result.as_ref().map(|_| ()).map_err(clone_storage_error));
                }
            } else {
                finish_append_sync(
                    request.commands,
                    request.outcomes,
                    result.as_ref().map(|_| ()).map_err(clone_storage_error),
                );
            }
        }
    }
}

fn run_worker(
    mut stores: LaneStores,
    receiver: shard_stream_core::ByteBoundedReceiver<ShardCommand>,
    append_linger: Duration,
    max_pack_age: Duration,
    sync_sender: SyncSender<SyncRequest>,
) {
    let mut deferred = None;
    let mut append_group_tuner = AppendGroupTuner::new(append_linger);
    loop {
        let command = match deferred.take() {
            Some(command) => command,
            None => match receiver.recv_timeout(max_pack_age.min(Duration::from_millis(100))) {
                Ok(command) => command.into_inner(),
                Err(RecvTimeoutError::Timeout) => {
                    let _ = stores.seal_aged_pack(max_pack_age);
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            },
        };
        match command {
            ShardCommand::Append(first) => {
                let started = Instant::now();
                let deadline = started + append_group_tuner.begin_group(started);
                let mut appends = Vec::with_capacity(TARGET_APPEND_GROUP);
                appends.push(first);
                while appends.len() < MAX_APPEND_GROUP {
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    match receiver.recv_timeout(deadline.saturating_duration_since(now)) {
                        Ok(command) => match command.into_inner() {
                            ShardCommand::Append(command) => appends.push(command),
                            command => {
                                deferred = Some(command);
                                break;
                            }
                        },
                        Err(_) => break,
                    }
                }
                let AppendGroupOutput {
                    outcomes,
                    sync_file,
                } = stores.append_group(&appends);
                append_group_tuner.complete_group(Instant::now(), appends.len());
                if let Some(file) = sync_file {
                    if let Err(request) = enqueue_sync(&sync_sender, file, appends, outcomes) {
                        send_append_results(
                            request.commands,
                            fail_durable_outcomes(request.outcomes, &sync_worker_stopped_error()),
                        );
                    }
                } else {
                    send_append_results(appends, outcomes);
                }
            }
            ShardCommand::PlanFetch {
                lane,
                topic_partition,
                start_offset,
                max_bytes,
                max_batches,
                completion,
            } => {
                let _ = completion.send(FetchCompletion::Plan {
                    lane,
                    result: stores.primary.plan_fetch(
                        topic_partition,
                        start_offset,
                        max_bytes,
                        max_batches,
                    ),
                });
            }
            ShardCommand::ReadFetch {
                lane,
                planned,
                completion,
            } => {
                let _ = completion.send(FetchCompletion::Read {
                    lane,
                    result: stores.primary.read_fetch_plan(&planned),
                });
            }
            ShardCommand::Sync { response } => {
                let _ = response.send(stores.sync(&sync_sender));
            }
            ShardCommand::GarbageCollect {
                topic_partition,
                log_start,
                response,
            } => {
                let _ = response.send(stores.garbage_collect(topic_partition, log_start));
            }
        }
    }
}

fn enqueue_sync(
    sync_sender: &SyncSender<SyncRequest>,
    file: File,
    commands: Vec<AppendCommand>,
    outcomes: Vec<StorageResult<AppendDurability>>,
) -> Result<(), SyncRequest> {
    sync_sender
        .send(SyncRequest {
            file,
            commands,
            outcomes,
            completion: None,
        })
        .map_err(|error| error.0)
}

fn finish_append_sync(
    commands: Vec<AppendCommand>,
    outcomes: Vec<StorageResult<AppendDurability>>,
    sync_result: StorageResult<()>,
) {
    let outcomes = match sync_result {
        Ok(()) => outcomes,
        Err(error) => fail_durable_outcomes(outcomes, &error),
    };
    send_append_results(commands, outcomes);
}

fn fail_durable_outcomes(
    outcomes: Vec<StorageResult<AppendDurability>>,
    error: &StorageError,
) -> Vec<StorageResult<AppendDurability>> {
    outcomes
        .into_iter()
        .map(|outcome| match outcome {
            Ok(_) => Err(clone_storage_error(error)),
            Err(error) => Err(error),
        })
        .collect()
}

fn send_append_results(
    commands: Vec<AppendCommand>,
    outcomes: Vec<StorageResult<AppendDurability>>,
) {
    for (command, result) in commands.into_iter().zip(outcomes) {
        let _ = command.response.send(result);
    }
}

fn sync_worker_stopped_error() -> StorageError {
    StorageError::Io(io::Error::new(
        io::ErrorKind::BrokenPipe,
        "shard sync worker stopped",
    ))
}

fn shard_directory(data_dir: &std::path::Path, shard_id: ShardId) -> PathBuf {
    data_dir.join("shards").join(format!("shard-{shard_id}"))
}

struct LaneStores {
    primary: ShardStore,
    tier: Option<LocalObjectTier>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AppendDurability {
    pub(crate) durable_replicas: u32,
    pub(crate) object_durable: bool,
}

impl LaneStores {
    fn seal_aged_pack(&mut self, max_pack_age: Duration) -> StorageResult<()> {
        if self.primary.seal_active_if_older(max_pack_age)?.is_some()
            && let Some(tier) = &mut self.tier
        {
            offload_sealed_packs(&self.primary, tier)?;
        }
        Ok(())
    }

    fn append_group(&mut self, appends: &[AppendCommand]) -> AppendGroupOutput {
        let mut outcomes = (0..appends.len())
            .map(|_| {
                Ok(AppendDurability {
                    durable_replicas: 0,
                    object_durable: false,
                })
            })
            .collect::<Vec<_>>();
        let mut primary_written = vec![false; appends.len()];
        let mut sync_file = None;
        let mut prepared_extents = Vec::with_capacity(appends.len());
        for (index, append) in appends.iter().enumerate() {
            match self.primary.prepare_append_atomic(
                append.reservation,
                append.producer,
                append.atomic_group,
                append.payload.clone(),
            ) {
                Ok(prepared) => {
                    prepared_extents.push(Some(prepared));
                }
                Err(error) => {
                    outcomes[index] = Err(clone_storage_error(&error));
                    prepared_extents.push(None);
                }
            }
        }
        let valid_prepared = prepared_extents.iter().flatten().collect::<Vec<_>>();
        if !valid_prepared.is_empty() {
            if let Err(error) = self.primary.append_prepared_group(&valid_prepared, false) {
                for (index, prepared) in prepared_extents.iter().enumerate() {
                    if prepared.is_some() {
                        outcomes[index] = Err(clone_storage_error(&error));
                    }
                }
                return AppendGroupOutput {
                    outcomes,
                    sync_file,
                };
            }
            for (index, prepared) in prepared_extents.iter().enumerate() {
                primary_written[index] = prepared.is_some();
            }
            let requires_object = appends
                .iter()
                .enumerate()
                .any(|(index, append)| primary_written[index] && append.object_durable);
            if !requires_object {
                match self.primary.active_file_clone() {
                    Ok(file) => {
                        sync_file = Some(file);
                        for (index, written) in primary_written.iter().enumerate() {
                            if *written {
                                outcomes[index] = Ok(AppendDurability {
                                    durable_replicas: 1,
                                    object_durable: false,
                                });
                            }
                        }
                    }
                    Err(error) => {
                        for (index, written) in primary_written.iter().enumerate() {
                            if *written {
                                outcomes[index] = Err(clone_storage_error(&error));
                            }
                        }
                        return AppendGroupOutput {
                            outcomes,
                            sync_file,
                        };
                    }
                }
            } else {
                match self.primary.sync() {
                    Ok(()) => {
                        for (index, written) in primary_written.iter().enumerate() {
                            if *written {
                                outcomes[index] = Ok(AppendDurability {
                                    durable_replicas: 1,
                                    object_durable: false,
                                });
                            }
                        }
                    }
                    Err(error) => {
                        for (index, written) in primary_written.iter().enumerate() {
                            if *written {
                                outcomes[index] = Err(clone_storage_error(&error));
                            }
                        }
                        return AppendGroupOutput {
                            outcomes,
                            sync_file,
                        };
                    }
                }
            }
        }

        if let Some(tier) = &mut self.tier {
            let requires_object = appends
                .iter()
                .enumerate()
                .any(|(index, append)| primary_written[index] && append.object_durable);
            let object_durable = if requires_object {
                self.primary
                    .seal_active()
                    .and_then(|_| offload_sealed_packs(&self.primary, tier))
                    .is_ok()
            } else {
                let _ = offload_sealed_packs(&self.primary, tier);
                false
            };
            if object_durable {
                for (index, written) in primary_written.iter().enumerate() {
                    if *written && let Ok(durability) = &mut outcomes[index] {
                        durability.object_durable = true;
                    }
                }
            }
        }
        AppendGroupOutput {
            outcomes,
            sync_file,
        }
    }

    fn sync(&mut self, sync_sender: &SyncSender<SyncRequest>) -> StorageResult<()> {
        let primary_file = self.primary.active_file_clone()?;
        let (primary_response, primary_receiver) = sync_channel(1);
        sync_sender
            .send(SyncRequest {
                file: primary_file,
                commands: Vec::new(),
                outcomes: Vec::new(),
                completion: Some(primary_response),
            })
            .map_err(|_| {
                StorageError::Io(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "shard sync worker stopped",
                ))
            })?;
        primary_receiver
            .recv()
            .map_err(|_| sync_worker_stopped_error())??;
        Ok(())
    }

    fn garbage_collect(
        &mut self,
        topic_partition: TopicPartition,
        log_start: LogicalOffset,
    ) -> StorageResult<()> {
        let log_starts = HashMap::from([(topic_partition, log_start)]);
        self.primary.garbage_collect(&log_starts)?;
        Ok(())
    }
}

fn offload_sealed_packs(store: &ShardStore, tier: &mut LocalObjectTier) -> StorageResult<()> {
    for pack in store.sealed_packs()? {
        let extents = store
            .catalog()
            .into_iter()
            .filter(|entry| entry.pack_sequence == pack.sequence)
            .map(|entry| TierExtent::from_reservation(entry.reservation))
            .collect();
        tier.offload_pack(pack.sequence, &pack.path, extents)?;
    }
    Ok(())
}

fn clone_storage_error(error: &shardlog::StorageError) -> shardlog::StorageError {
    shardlog::StorageError::Io(io::Error::other(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_group_linger_grows_for_sparse_groups_and_resets_after_idle() {
        let max_linger = Duration::from_millis(1);
        let mut tuner = AppendGroupTuner::new(max_linger);
        let started = Instant::now();
        assert_eq!(tuner.begin_group(started), MIN_APPEND_GROUP_LINGER);

        tuner.complete_group(started, 1);
        assert_eq!(
            tuner.begin_group(started + Duration::from_micros(1)),
            Duration::from_micros(400)
        );
        tuner.complete_group(started + Duration::from_micros(1), 2);
        assert_eq!(
            tuner.begin_group(started + Duration::from_micros(2)),
            Duration::from_micros(800)
        );
        tuner.complete_group(started + Duration::from_micros(2), 4);
        assert_eq!(
            tuner.begin_group(started + Duration::from_micros(3)),
            max_linger
        );
        assert_eq!(
            tuner.begin_group(started + APPEND_GROUP_IDLE_RESET + Duration::from_micros(2)),
            MIN_APPEND_GROUP_LINGER
        );
    }

    #[test]
    fn zero_append_linger_disables_group_waiting() {
        let mut tuner = AppendGroupTuner::new(Duration::ZERO);
        let now = Instant::now();
        assert_eq!(tuner.begin_group(now), Duration::ZERO);
        tuner.complete_group(now, 1);
        assert_eq!(tuner.begin_group(now), Duration::ZERO);
    }

    #[test]
    fn saturated_append_groups_reduce_linger() {
        let max_linger = Duration::from_millis(1);
        let mut tuner = AppendGroupTuner {
            linger: max_linger,
            min_linger: MIN_APPEND_GROUP_LINGER,
            max_linger,
            last_group_completed: None,
        };
        let now = Instant::now();
        tuner.complete_group(now, MAX_APPEND_GROUP);
        assert_eq!(
            tuner.begin_group(now + Duration::from_micros(1)),
            Duration::from_micros(500)
        );
    }
}
