use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, SyncSender, sync_channel};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bytes::Bytes;
use shard_stream_core::{
    ByteBoundedSender, LogicalOffset, Reservation, ShardId, TopicPartition, TrySendError,
    byte_bounded_channel,
};
use shard_stream_storage::{
    ExtentCatalogEntry, LocalObjectTier, PreparedExtent, ProducerSequence, ShardStore,
    ShardStoreConfig, StorageResult, StoredBatch, StoredBatchMetadata,
};

use crate::config::COMMAND_OVERHEAD_BYTES;
use crate::coordinator::CoordinatorRouter;
use crate::{EngineConfig, EngineError, EngineResult};

const MAX_APPEND_GROUP: usize = 128;
const TARGET_APPEND_GROUP: usize = 8;
const MIN_APPEND_GROUP_LINGER: Duration = Duration::from_micros(200);
const APPEND_GROUP_IDLE_RESET: Duration = Duration::from_millis(4);

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

    fn ambiguous(error: EngineError) -> Self {
        Self {
            error,
            definitely_not_written: false,
        }
    }
}

struct AppendCommand {
    reservation: Reservation,
    producer: Option<ProducerSequence>,
    payload: Bytes,
    required_replicas: u32,
    object_durable: bool,
    response: SyncSender<StorageResult<AppendDurability>>,
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
    InstallReplicaProgress {
        router: CoordinatorRouter,
        response: SyncSender<EngineResult<()>>,
    },
}

pub(crate) struct ShardHandle {
    pub(crate) shard_id: ShardId,
    sender: Option<ByteBoundedSender<ShardCommand>>,
    thread: Option<JoinHandle<()>>,
}

impl ShardHandle {
    pub(crate) fn spawn(
        config: &EngineConfig,
        shard_id: ShardId,
        retained_log_starts: &HashMap<TopicPartition, LogicalOffset>,
    ) -> EngineResult<(Self, Vec<ExtentCatalogEntry>)> {
        let directory = shard_directory(&config.data_dir, shard_id);
        let mut primary = ShardStore::open(ShardStoreConfig {
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
        let mut replicas = Vec::with_capacity(config.replication_factor.saturating_sub(1) as usize);
        for replica_index in 1..config.replication_factor {
            let mut replica = ShardStore::open(ShardStoreConfig {
                directory: replica_directory(&config.data_dir, replica_index, shard_id),
                shard_id,
                target_pack_bytes: config.target_pack_bytes,
                max_batch_bytes: config.max_batch_bytes,
            })?;
            catch_up_replica(&mut primary, &mut replica, &catalog, retained_log_starts)?;
            replicas.push(Some(ReplicaHandle::spawn(
                replica,
                shard_id,
                replica_index,
            )?));
        }
        let stores = LaneStores {
            primary,
            replicas,
            tier,
        };
        let (sender, receiver) =
            byte_bounded_channel(config.queue_slots_per_shard, config.queue_bytes_per_shard)
                .map_err(|error| EngineError::InvalidConfig(error.to_string()))?;
        let append_linger = config.append_linger;
        let thread = thread::Builder::new()
            .name(format!("shard-stream-lane-{shard_id}"))
            .spawn(move || run_worker(stores, receiver, append_linger))
            .map_err(|error| {
                EngineError::InvalidConfig(format!("failed to spawn shard worker: {error}"))
            })?;
        Ok((
            Self {
                shard_id,
                sender: Some(sender),
                thread: Some(thread),
            },
            catalog,
        ))
    }

    pub(crate) fn append(
        &self,
        reservation: Reservation,
        producer: Option<ProducerSequence>,
        payload: Bytes,
        required_replicas: u32,
        object_durable: bool,
    ) -> Result<AppendDurability, AppendFailure> {
        let (response, receiver) = sync_channel(1);
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
                payload,
                required_replicas,
                object_durable,
                response,
            }),
            charged_bytes,
        )
        .map_err(AppendFailure::rejected)?;
        receiver
            .recv()
            .map_err(|_| AppendFailure::ambiguous(EngineError::WorkerStopped(self.shard_id)))?
            .map_err(|error| AppendFailure::ambiguous(error.into()))
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

    pub(crate) fn sync(&self) -> EngineResult<()> {
        let (response, receiver) = sync_channel(1);
        self.try_send(ShardCommand::Sync { response }, COMMAND_OVERHEAD_BYTES)?;
        receiver
            .recv()
            .map_err(|_| EngineError::WorkerStopped(self.shard_id))??;
        Ok(())
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

    pub(crate) fn install_replica_progress(&self, router: CoordinatorRouter) -> EngineResult<()> {
        let (response, receiver) = sync_channel(1);
        self.try_send(
            ShardCommand::InstallReplicaProgress { router, response },
            COMMAND_OVERHEAD_BYTES,
        )?;
        receiver
            .recv()
            .map_err(|_| EngineError::WorkerStopped(self.shard_id))?
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
    }
}

fn run_worker(
    mut stores: LaneStores,
    receiver: shard_stream_core::ByteBoundedReceiver<ShardCommand>,
    append_linger: Duration,
) {
    let mut deferred = None;
    let mut append_group_tuner = AppendGroupTuner::new(append_linger);
    loop {
        let command = match deferred.take() {
            Some(command) => command,
            None => match receiver.recv() {
                Ok(command) => command.into_inner(),
                Err(_) => break,
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
                let results = stores.append_group(&appends);
                append_group_tuner.complete_group(Instant::now(), appends.len());
                for (command, result) in appends.into_iter().zip(results) {
                    let _ = command.response.send(result);
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
                let _ = response.send(stores.sync());
            }
            ShardCommand::GarbageCollect {
                topic_partition,
                log_start,
                response,
            } => {
                let _ = response.send(stores.garbage_collect(topic_partition, log_start));
            }
            ShardCommand::InstallReplicaProgress { router, response } => {
                let _ = response.send(stores.install_replica_progress(router));
            }
        }
    }
}

fn shard_directory(data_dir: &std::path::Path, shard_id: ShardId) -> PathBuf {
    data_dir.join("shards").join(format!("shard-{shard_id}"))
}

fn replica_directory(data_dir: &std::path::Path, replica_index: u32, shard_id: ShardId) -> PathBuf {
    data_dir
        .join("replicas")
        .join(format!("replica-{replica_index}"))
        .join(format!("shard-{shard_id}"))
}

#[derive(Clone)]
struct ReplicaAppend {
    prepared: Arc<PreparedExtent>,
}

enum ReplicaCommand {
    AppendGroup {
        appends: Vec<ReplicaAppend>,
        replica_index: u32,
        response: SyncSender<(u32, StorageResult<()>)>,
    },
    Sync {
        response: SyncSender<StorageResult<()>>,
    },
    InstallProgress {
        router: CoordinatorRouter,
        response: SyncSender<()>,
    },
    GarbageCollect {
        topic_partition: TopicPartition,
        log_start: LogicalOffset,
        response: SyncSender<StorageResult<()>>,
    },
}

struct ReplicaHandle {
    shard_id: ShardId,
    sender: Option<SyncSender<ReplicaCommand>>,
    thread: Option<JoinHandle<()>>,
}

impl ReplicaHandle {
    fn spawn(mut store: ShardStore, shard_id: ShardId, replica_index: u32) -> EngineResult<Self> {
        let (sender, receiver) = sync_channel(8);
        let thread = thread::Builder::new()
            .name(format!(
                "shard-stream-replica-{replica_index}-shard-{shard_id}"
            ))
            .spawn(move || {
                let mut progress = None::<CoordinatorRouter>;
                while let Ok(command) = receiver.recv() {
                    match command {
                        ReplicaCommand::AppendGroup {
                            appends,
                            replica_index,
                            response,
                        } => {
                            let result = {
                                let prepared = appends
                                    .iter()
                                    .map(|append| append.prepared.as_ref())
                                    .collect::<Vec<_>>();
                                store.append_prepared_group(&prepared, true)
                            };
                            if result.is_ok()
                                && let Some(progress) = &progress
                            {
                                for append in &appends {
                                    progress.replica_durable(
                                        append.prepared.reservation(),
                                        replica_index,
                                    );
                                }
                            }
                            let failed = result.is_err();
                            let _ = response.send((replica_index, result));
                            if failed {
                                break;
                            }
                        }
                        ReplicaCommand::Sync { response } => {
                            let _ = response.send(store.sync());
                        }
                        ReplicaCommand::GarbageCollect {
                            topic_partition,
                            log_start,
                            response,
                        } => {
                            let starts = HashMap::from([(topic_partition, log_start)]);
                            let _ = response.send(store.garbage_collect(&starts).map(|_| ()));
                        }
                        ReplicaCommand::InstallProgress { router, response } => {
                            progress = Some(router);
                            let _ = response.send(());
                        }
                    }
                }
            })
            .map_err(|error| {
                EngineError::InvalidConfig(format!("failed to spawn replica worker: {error}"))
            })?;
        Ok(Self {
            shard_id,
            sender: Some(sender),
            thread: Some(thread),
        })
    }

    fn begin_append_group(
        &self,
        appends: Vec<ReplicaAppend>,
        replica_index: u32,
        response: SyncSender<(u32, StorageResult<()>)>,
    ) -> EngineResult<()> {
        self.send(ReplicaCommand::AppendGroup {
            appends,
            replica_index,
            response,
        })
    }

    fn begin_sync(&self) -> EngineResult<Receiver<StorageResult<()>>> {
        let (response, receiver) = sync_channel(1);
        self.send(ReplicaCommand::Sync { response })?;
        Ok(receiver)
    }

    fn begin_garbage_collect(
        &self,
        topic_partition: TopicPartition,
        log_start: LogicalOffset,
    ) -> EngineResult<Receiver<StorageResult<()>>> {
        let (response, receiver) = sync_channel(1);
        self.send(ReplicaCommand::GarbageCollect {
            topic_partition,
            log_start,
            response,
        })?;
        Ok(receiver)
    }

    fn install_progress(&self, router: CoordinatorRouter) -> EngineResult<()> {
        let (response, receiver) = sync_channel(1);
        self.send(ReplicaCommand::InstallProgress { router, response })?;
        receiver
            .recv()
            .map_err(|_| EngineError::WorkerStopped(self.shard_id))
    }

    fn send(&self, command: ReplicaCommand) -> EngineResult<()> {
        self.sender
            .as_ref()
            .ok_or(EngineError::WorkerStopped(self.shard_id))?
            .send(command)
            .map_err(|_| EngineError::WorkerStopped(self.shard_id))
    }
}

impl Drop for ReplicaHandle {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct LaneStores {
    primary: ShardStore,
    replicas: Vec<Option<ReplicaHandle>>,
    tier: Option<LocalObjectTier>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AppendDurability {
    pub(crate) durable_replicas: u32,
    pub(crate) object_durable: bool,
}

impl LaneStores {
    fn install_replica_progress(&self, router: CoordinatorRouter) -> EngineResult<()> {
        for replica in self.replicas.iter().flatten() {
            replica.install_progress(router.clone())?;
        }
        Ok(())
    }

    fn append_group(&mut self, appends: &[AppendCommand]) -> Vec<StorageResult<AppendDurability>> {
        let mut outcomes = (0..appends.len())
            .map(|_| {
                Ok(AppendDurability {
                    durable_replicas: 0,
                    object_durable: false,
                })
            })
            .collect::<Vec<_>>();
        let mut primary_written = vec![false; appends.len()];
        let mut prepared_extents = Vec::with_capacity(appends.len());
        for (index, append) in appends.iter().enumerate() {
            match self.primary.prepare_append(
                append.reservation,
                append.producer,
                append.payload.clone(),
            ) {
                Ok(prepared) => {
                    prepared_extents.push(Some(Arc::new(prepared)));
                }
                Err(error) => {
                    outcomes[index] = Err(clone_storage_error(&error));
                    prepared_extents.push(None);
                }
            }
        }
        let valid_prepared = prepared_extents
            .iter()
            .flatten()
            .map(Arc::as_ref)
            .collect::<Vec<_>>();
        if !valid_prepared.is_empty() {
            if let Err(error) = self.primary.append_prepared_group(&valid_prepared, false) {
                for (index, prepared) in prepared_extents.iter().enumerate() {
                    if prepared.is_some() {
                        outcomes[index] = Err(clone_storage_error(&error));
                    }
                }
                return outcomes;
            }
            for (index, prepared) in prepared_extents.iter().enumerate() {
                primary_written[index] = prepared.is_some();
            }
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
                    return outcomes;
                }
            }
        }

        let replica_appends = appends
            .iter()
            .enumerate()
            .filter(|(index, _)| primary_written[*index])
            .map(|(index, _)| ReplicaAppend {
                prepared: Arc::clone(
                    prepared_extents[index]
                        .as_ref()
                        .expect("written primary has a prepared extent"),
                ),
            })
            .collect::<Vec<_>>();
        let (replica_response, replica_receiver) = sync_channel(self.replicas.len().max(1));
        let mut submitted_replicas = 0usize;
        for index in 0..self.replicas.len() {
            let Some(replica) = self.replicas[index].as_ref() else {
                continue;
            };
            let replica_index = u32::try_from(index)
                .expect("replica vector fits u32")
                .saturating_add(1);
            if replica
                .begin_append_group(
                    replica_appends.clone(),
                    replica_index,
                    replica_response.clone(),
                )
                .is_ok()
            {
                submitted_replicas += 1;
            } else {
                self.replicas[index] = None;
            }
        }
        drop(replica_response);

        let required_replicas = appends
            .iter()
            .enumerate()
            .filter(|(index, _)| primary_written[*index])
            .map(|(_, append)| append.required_replicas)
            .max()
            .unwrap_or(1);
        let required_followers = required_replicas.saturating_sub(1);
        let mut durable_followers = 0u32;
        let mut completed_replicas = 0usize;
        while durable_followers < required_followers && completed_replicas < submitted_replicas {
            let Ok((replica_index, result)) = replica_receiver.recv() else {
                break;
            };
            completed_replicas += 1;
            if result.is_ok() {
                durable_followers = durable_followers.saturating_add(1);
                for (batch_index, written) in primary_written.iter().enumerate() {
                    if *written && let Ok(durability) = &mut outcomes[batch_index] {
                        durability.durable_replicas = durability.durable_replicas.saturating_add(1);
                    }
                }
            } else {
                let index = replica_index.saturating_sub(1) as usize;
                if index < self.replicas.len() {
                    self.replicas[index] = None;
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
        outcomes
    }

    fn sync(&mut self) -> StorageResult<()> {
        let mut receivers = Vec::new();
        for (index, replica) in self.replicas.iter().enumerate() {
            let Some(replica) = replica else {
                continue;
            };
            receivers.push((index, replica.begin_sync().ok()));
        }
        self.primary.sync()?;
        for (index, receiver) in receivers {
            if !receiver.is_some_and(|receiver| matches!(receiver.recv(), Ok(Ok(())))) {
                self.replicas[index] = None;
            }
        }
        Ok(())
    }

    fn garbage_collect(
        &mut self,
        topic_partition: TopicPartition,
        log_start: LogicalOffset,
    ) -> StorageResult<()> {
        let log_starts = HashMap::from([(topic_partition, log_start)]);
        self.primary.garbage_collect(&log_starts)?;
        let mut receivers = Vec::new();
        for (index, replica) in self.replicas.iter().enumerate() {
            let Some(replica) = replica else {
                continue;
            };
            receivers.push((
                index,
                replica
                    .begin_garbage_collect(topic_partition, log_start)
                    .ok(),
            ));
        }
        for (index, receiver) in receivers {
            if !receiver.is_some_and(|receiver| matches!(receiver.recv(), Ok(Ok(())))) {
                self.replicas[index] = None;
            }
        }
        Ok(())
    }
}

fn offload_sealed_packs(store: &ShardStore, tier: &mut LocalObjectTier) -> StorageResult<()> {
    for pack in store.sealed_packs()? {
        tier.offload_pack(pack.sequence, &pack.path)?;
    }
    Ok(())
}

fn clone_storage_error(
    error: &shard_stream_storage::StorageError,
) -> shard_stream_storage::StorageError {
    shard_stream_storage::StorageError::Io(io::Error::other(error.to_string()))
}

fn catch_up_replica(
    primary: &mut ShardStore,
    replica: &mut ShardStore,
    primary_catalog: &[ExtentCatalogEntry],
    retained_log_starts: &HashMap<TopicPartition, LogicalOffset>,
) -> StorageResult<()> {
    let primary_batches = primary_catalog
        .iter()
        .map(|entry| {
            (
                (
                    TopicPartition::new(entry.reservation.topic_id, entry.reservation.partition_id),
                    entry.reservation.batch_id,
                ),
                entry.reservation,
            )
        })
        .collect::<std::collections::HashMap<_, _>>();
    for entry in replica.catalog() {
        let key = (
            TopicPartition::new(entry.reservation.topic_id, entry.reservation.partition_id),
            entry.reservation.batch_id,
        );
        if primary_batches.get(&key) != Some(&entry.reservation)
            && retained_log_starts
                .get(&key.0)
                .is_none_or(|log_start| entry.reservation.last_offset >= *log_start)
        {
            return Err(shard_stream_storage::StorageError::InvalidInput(
                "replica contains an extent absent or different on the primary".into(),
            ));
        }
    }
    for entry in primary_catalog {
        if replica.contains(&entry.reservation) {
            replica.read_batch(&entry.reservation)?;
            continue;
        }
        let batch = primary.read_batch(&entry.reservation)?.ok_or_else(|| {
            shard_stream_storage::StorageError::InvalidInput(
                "primary catalog entry is unreadable".into(),
            )
        })?;
        replica.append(entry.reservation, entry.producer, &batch.payload, true)?;
    }
    Ok(())
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
