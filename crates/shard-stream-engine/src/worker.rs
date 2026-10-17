use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, SyncSender, TryRecvError, sync_channel};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bytes::Bytes;
use shard_stream_core::{
    Budgeted, ByteBoundedSender, LogicalOffset, PendingWriteAdmission, PhysicalWriteGuard,
    Reservation, ShardId, TopicPartition, TrySendError, byte_bounded_channel,
};
use shardlog::{
    AtomicGroupSequence, ExtentCatalogEntry, LocalObjectTier, ProducerSequence, ShardStore,
    ShardStoreConfig, StorageError, StorageResult, StoredBatch, StoredBatchMetadata, TierExtent,
};

use crate::config::COMMAND_OVERHEAD_BYTES;
use crate::{EngineConfig, EngineError, EngineResult};

#[path = "worker_primary.rs"]
mod primary;
pub(crate) use primary::PrimaryStore;
#[cfg(unix)]
#[path = "worker_start.rs"]
mod start;
#[cfg(unix)]
pub(crate) use start::RecoveredStartError;

const MAX_APPEND_GROUP: usize = 128;
const TARGET_APPEND_GROUP: usize = 8;
const MIN_APPEND_GROUP_LINGER: Duration = Duration::from_micros(200);
const APPEND_GROUP_IDLE_RESET: Duration = Duration::from_millis(4);
const SYNC_QUEUE_SLOTS: usize = 8;
const MAX_SYNC_BATCH: usize = 8;
/// Fixed command fields retained with a guarded append through settlement.
pub(crate) const ADMISSION_METADATA_BYTES: usize =
    std::mem::size_of::<(
        Option<Arc<dyn PendingWriteAdmission>>,
        Option<Box<dyn PhysicalWriteGuard>>,
        Option<Budgeted<()>>,
    )>() + std::mem::size_of::<proposal::PreparedInput>()
        + std::mem::size_of::<crate::coordinator::CommitControl>()
        + std::mem::size_of::<Arc<crate::coordinator::CoordinatorRouter>>();

/// Concrete fixed native channel/value capacity before recovered worker startup.
#[cfg(unix)]
pub(crate) fn recovery_worker_bytes(config: &EngineConfig) -> Option<usize> {
    config
        .queue_slots_per_shard
        .checked_mul(std::mem::size_of::<ShardCommand>())?
        .checked_add(SYNC_QUEUE_SLOTS.checked_mul(std::mem::size_of::<SyncRequest>())?)?
        .checked_add(std::mem::size_of::<ShardHandle>() + std::mem::size_of::<LaneStores>())
}

/// Actual extra input-slot, holder, readiness and fixed stack ceilings for the
/// new owned route; ordinary recovered workers keep their existing path.
#[cfg(unix)]
pub(crate) fn recovery_owned_worker_extra_bytes() -> Option<usize> {
    start::retained_start_bytes()?
        .checked_add(2usize.checked_mul(std::mem::size_of::<primary::DirectoryRetention>())?)
}

/// Computes the declared command charge without claiming a process RAM bound.
pub(crate) fn append_command_charge(payload_bytes: usize, guarded: bool) -> Option<usize> {
    payload_bytes
        .checked_add(COMMAND_OVERHEAD_BYTES)?
        .checked_add(if guarded { ADMISSION_METADATA_BYTES } else { 0 })
}

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
#[path = "worker_owned_tests.rs"]
mod owned_store_tests;
#[cfg(test)]
#[path = "worker_physical_admission_tests.rs"]
mod physical_admission_tests;

#[path = "worker_fetch.rs"]
mod bounded_fetch;
#[cfg(test)]
pub(crate) use bounded_fetch::observe as observe_bounded_fetch;
pub(crate) use bounded_fetch::{
    BoundedFetchCompletion, DispatchError as BoundedFetchDispatchError,
    PlanInput as BoundedFetchPlanInput, bounded_fetch_command_bytes,
};

#[cfg(any(target_os = "linux", target_os = "android"))]
#[path = "worker_gc.rs"]
mod gc;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) use gc::COMMAND_DIAGNOSTIC_BYTES as OWNED_GC_COMMAND_DIAGNOSTIC_BYTES;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) use gc::COMMAND_ENTRIES as OWNED_GC_COMMAND_ENTRIES;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) use gc::GcTicket;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) use gc::command_cost as owned_gc_command_cost;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use gc::{NativeGcOutcome, NativeResumeGcPolicy};

mod proposal;
pub(crate) use proposal::{ProposedAppendOutcome, ProposedAppendTicket};

enum AppendReply {
    Legacy(SyncSender<Result<AppendDurability, AppendFailure>>),
    Proposed {
        replication_policy: crate::PartitionReplicationPolicy,
        response: SyncSender<ProposedAppendOutcome>,
    },
}

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

#[derive(Debug)]
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
    admission: Option<Arc<dyn PendingWriteAdmission>>,
    _physical_guard: Option<Box<dyn PhysicalWriteGuard>>,
    _byte_charge: Option<Budgeted<()>>,
    response: AppendReply,
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
    #[cfg(any(target_os = "linux", target_os = "android"))]
    OwnedGc(gc::Command),
    BoundedFetch(bounded_fetch::Command),
    Append(AppendCommand),
    ProposedAppend(Box<proposal::ProposedAppendCommand>),
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
    #[cfg(unix)]
    GarbageCollectUntil {
        topic_partition: TopicPartition,
        log_start: LogicalOffset,
        expiry: Instant,
        response: SyncSender<StorageResult<()>>,
    },
}

pub(crate) struct ShardHandle {
    pub(crate) shard_id: ShardId,
    sender: Option<ByteBoundedSender<ShardCommand>>,
    thread: Option<JoinHandle<()>>,
    sync_sender: Option<SyncSender<SyncRequest>>,
    sync_thread: Option<JoinHandle<()>>,
    #[cfg(unix)]
    recovered_input: Option<start::InputSlot>,
}

impl ShardHandle {
    pub(crate) fn spawn(
        config: &EngineConfig,
        shard_id: ShardId,
    ) -> EngineResult<(Self, Vec<ExtentCatalogEntry>)> {
        Self::spawn_with_ownership(config, shard_id, None)
    }

    pub(crate) fn spawn_with_ownership(
        config: &EngineConfig,
        shard_id: ShardId,
        directory_ownership: Option<Arc<shard_stream_core::DirectoryOwnership>>,
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
        let stores = LaneStores {
            primary: PrimaryStore::Ordinary(primary),
            tier,
        };
        let (sync_sender, sync_receiver) = sync_channel(SYNC_QUEUE_SLOTS);
        let sync_ownership = directory_ownership.as_ref().map(Arc::clone);
        let sync_thread = thread::Builder::new()
            .name(format!("shard-stream-sync-{shard_id}"))
            .spawn(move || {
                let ownership = sync_ownership;
                run_sync_worker(sync_receiver);
                drop(ownership);
            })
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
                let ownership = directory_ownership;
                run_worker(
                    stores,
                    receiver,
                    append_linger,
                    max_pack_age,
                    append_sync_sender,
                );
                drop(ownership);
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
                #[cfg(unix)]
                recovered_input: None,
            },
            catalog,
        ))
    }

    /// Native worker continuation with already-open, same-lease stores.
    #[cfg(unix)]
    pub(crate) fn spawn_recovered(
        config: &EngineConfig,
        shard_id: ShardId,
        primary: PrimaryStore,
        mut tier: Option<LocalObjectTier>,
        directory_ownership: Arc<shard_stream_core::DirectoryOwnership>,
        expiry: Instant,
    ) -> EngineResult<(Self, Vec<ExtentCatalogEntry>)> {
        recovered_worker_clock(expiry)?;
        if let Some(tier) = &mut tier {
            offload_sealed_packs_until(
                primary.native().ok_or_else(primary::unavailable)?,
                tier,
                expiry,
            )?;
        }
        recovered_worker_clock(expiry)?;
        let mut catalog = primary
            .native()
            .ok_or_else(primary::unavailable)?
            .catalog_until(expiry)?;
        if let Some(tier) = &tier {
            let mut durable_packs = std::collections::HashSet::new();
            for object in &tier.manifest().objects {
                recovered_worker_clock(expiry)?;
                durable_packs.insert(object.pack_sequence);
            }
            for entry in &mut catalog {
                recovered_worker_clock(expiry)?;
                entry.object_durable = durable_packs.contains(&entry.pack_sequence);
            }
        }
        recovered_worker_clock(expiry)?;
        let sync_directory = primary.retain_directory();
        let lane_directory = primary.retain_directory();
        let stores = LaneStores { primary, tier };
        let (sync_sender, sync_receiver) = sync_channel(SYNC_QUEUE_SLOTS);
        // The incremental real handle joins any already-spawned worker on
        // every later failure; no half-open continuation detaches a worker.
        let mut result = Self {
            shard_id,
            sender: None,
            thread: None,
            sync_sender: Some(sync_sender),
            sync_thread: None,
            recovered_input: None,
        };
        let sync_ownership = Arc::clone(&directory_ownership);
        recovered_worker_clock(expiry)?;
        result.sync_thread = Some(
            thread::Builder::new()
                .name(format!("shard-stream-sync-{shard_id}"))
                .spawn(move || {
                    let directory = sync_directory;
                    let ownership = sync_ownership;
                    run_sync_worker(sync_receiver);
                    drop(ownership);
                    drop(directory);
                })
                .map_err(|e| {
                    EngineError::InvalidConfig(format!("failed to spawn sync worker: {e}"))
                })?,
        );
        #[cfg(test)]
        recovered_open_test_step("sync-spawned");
        recovered_worker_clock(expiry)?;
        let (sender, receiver) =
            byte_bounded_channel(config.queue_slots_per_shard, config.queue_bytes_per_shard)
                .map_err(|e| EngineError::InvalidConfig(e.to_string()))?;
        let append_linger = config.append_linger;
        let max_pack_age = config.max_pack_age;
        let append_sync_sender = result
            .sync_sender
            .as_ref()
            .ok_or(EngineError::WorkerStopped(shard_id))?
            .clone();
        recovered_worker_clock(expiry)?;
        result.thread = Some(
            thread::Builder::new()
                .name(format!("shard-stream-lane-{shard_id}"))
                .spawn(move || {
                    let directory = lane_directory;
                    let ownership = directory_ownership;
                    run_worker(
                        stores,
                        receiver,
                        append_linger,
                        max_pack_age,
                        append_sync_sender,
                    );
                    drop(ownership);
                    drop(directory);
                })
                .map_err(|e| {
                    EngineError::InvalidConfig(format!("failed to spawn shard worker: {e}"))
                })?,
        );
        result.sender = Some(sender);
        #[cfg(test)]
        recovered_open_test_step("lane-spawned");
        recovered_worker_clock(expiry)?;
        Ok((result, catalog))
    }

    pub(crate) fn append(
        &self,
        reservation: Reservation,
        producer: Option<ProducerSequence>,
        atomic_group: Option<AtomicGroupSequence>,
        payload: Bytes,
        object_durable: bool,
        admission: Option<Arc<dyn PendingWriteAdmission>>,
    ) -> Result<AppendDurability, AppendFailure> {
        let (response, receiver) = sync_channel(1);
        self.begin_append(
            reservation,
            producer,
            atomic_group,
            payload,
            object_durable,
            admission,
            response,
        )?;
        receiver
            .recv()
            .map_err(|_| AppendFailure::ambiguous(EngineError::WorkerStopped(self.shard_id)))?
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn begin_append(
        &self,
        reservation: Reservation,
        producer: Option<ProducerSequence>,
        atomic_group: Option<AtomicGroupSequence>,
        payload: Bytes,
        object_durable: bool,
        admission: Option<Arc<dyn PendingWriteAdmission>>,
        response: SyncSender<Result<AppendDurability, AppendFailure>>,
    ) -> Result<(), AppendFailure> {
        let charged_bytes =
            append_command_charge(payload.len(), admission.is_some()).ok_or_else(|| {
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
                admission,
                _physical_guard: None,
                _byte_charge: None,
                response: AppendReply::Legacy(response),
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

    #[cfg(unix)]
    pub(crate) fn garbage_collect_until(
        &self,
        topic_partition: TopicPartition,
        log_start: LogicalOffset,
        expiry: Instant,
    ) -> EngineResult<()> {
        recovered_worker_clock(expiry)?;
        let (response, receiver) = sync_channel(1);
        recovered_worker_clock(expiry)?;
        self.try_send(
            ShardCommand::GarbageCollectUntil {
                topic_partition,
                log_start,
                expiry,
                response,
            },
            COMMAND_OVERHEAD_BYTES,
        )?;
        receiver
            .recv()
            .map_err(|_| EngineError::WorkerStopped(self.shard_id))??;
        recovered_worker_clock(expiry)?;
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
        #[cfg(unix)]
        if let Some(input) = self.recovered_input.take() {
            // The lane has physically joined. If it never entered, dispose its
            // actual retained inputs before disconnecting/joining sync; opaque
            // directory clones in both physical closures still retain custody.
            let actual = input
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            drop(actual);
            drop(input);
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
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        owned_store_tests::before_sync(&requests.last().expect("actual nonempty sync batch").file);
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
                Ok(command) => retain_physical_charge(command),
                Err(RecvTimeoutError::Timeout) => {
                    let _ = stores.seal_aged_pack(max_pack_age);
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            },
        };
        let Some(command) = proposal::resolve(command) else {
            continue;
        };
        match command {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ShardCommand::OwnedGc(command) => gc::execute(&mut stores, command),
            ShardCommand::BoundedFetch(command) => bounded_fetch::execute(&mut stores, command),
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
                        Ok(command) => match proposal::resolve(retain_physical_charge(command)) {
                            Some(ShardCommand::Append(command)) => appends.push(command),
                            Some(command) => {
                                deferred = Some(command);
                                break;
                            }
                            None => continue,
                        },
                        Err(_) => break,
                    }
                }
                let mut admitted = Vec::with_capacity(appends.len());
                for mut command in appends {
                    match admit_physical(&mut command) {
                        Ok(()) => admitted.push(command),
                        Err(error) => {
                            send_append_result(command, Err(error));
                        }
                    }
                }
                if admitted.is_empty() {
                    continue;
                }
                let appends = admitted;
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
                    result: stores
                        .primary
                        .native()
                        .ok_or_else(primary::unavailable)
                        .and_then(|primary| {
                            primary.plan_fetch(
                                topic_partition,
                                start_offset,
                                max_bytes,
                                max_batches,
                            )
                        }),
                });
            }
            ShardCommand::ReadFetch {
                lane,
                planned,
                completion,
            } => {
                let _ = completion.send(FetchCompletion::Read {
                    lane,
                    result: stores
                        .primary
                        .native()
                        .ok_or_else(primary::unavailable)
                        .and_then(|primary| primary.read_fetch_plan(&planned)),
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
            #[cfg(unix)]
            ShardCommand::GarbageCollectUntil {
                topic_partition,
                log_start,
                expiry,
                response,
            } => {
                #[cfg(test)]
                recovered_gc_queued_test_step(
                    stores
                        .primary
                        .native()
                        .and_then(|primary| primary.active_path().parent()),
                );
                // Queue residence cannot create a new clock. No native work
                // begins until the actual worker checks the original Instant.
                let result = (|| {
                    if Instant::now() >= expiry {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "native recovery original expiry elapsed",
                        )
                        .into());
                    }
                    let starts = HashMap::from([(topic_partition, log_start)]);
                    stores
                        .primary
                        .native_mut()
                        .ok_or_else(primary::unavailable)?
                        .garbage_collect_until(&starts, expiry)?;
                    Ok(())
                })();
                let _ = response.send(result);
            }
            ShardCommand::ProposedAppend(_) => {
                unreachable!("proposals are resolved before dispatch")
            }
        }
    }
}

fn retain_physical_charge(command: Budgeted<ShardCommand>) -> ShardCommand {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    if matches!(&*command, ShardCommand::OwnedGc(_)) {
        let (mut command, charge) = command.into_parts();
        if let ShardCommand::OwnedGc(command) = &mut command {
            gc::retain_charge(command, charge);
        }
        return command;
    }
    if !matches!(&*command, ShardCommand::Append(append) if append.admission.is_some())
        && !matches!(&*command, ShardCommand::ProposedAppend(_))
        && !matches!(&*command, ShardCommand::BoundedFetch(_))
    {
        return command.into_inner();
    }
    let (command, charge) = command.into_parts();
    match command {
        ShardCommand::BoundedFetch(mut command) => {
            command.byte_charge = Some(charge);
            ShardCommand::BoundedFetch(command)
        }
        ShardCommand::Append(mut command) => {
            if command.admission.is_some() {
                command._byte_charge = Some(charge);
            }
            ShardCommand::Append(command)
        }
        ShardCommand::ProposedAppend(mut command) => {
            command.byte_charge = Some(charge);
            ShardCommand::ProposedAppend(command)
        }
        command => command,
    }
}

fn admit_physical(command: &mut AppendCommand) -> Result<(), AppendFailure> {
    if command._physical_guard.is_some() {
        return Ok(());
    }
    command._physical_guard = command
        .admission
        .as_ref()
        .map_or(Ok(None), |admission| {
            catch_unwind(AssertUnwindSafe(|| {
                admission.try_admit_physical(&command.reservation)
            }))
            .map_err(|_| "physical write admission panicked".to_owned())?
            .map(Some)
            .map_err(|error| error.to_string())
        })
        .map_err(|error| AppendFailure::rejected(EngineError::Fenced(error)))?;
    Ok(())
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
        send_append_result(
            command,
            result.map_err(|error| AppendFailure::ambiguous(error.into())),
        );
    }
}

fn send_append_result(command: AppendCommand, result: Result<AppendDurability, AppendFailure>) {
    let reservation = command.reservation;
    let producer = command.producer;
    let AppendCommand {
        response,
        admission,
        payload,
        _physical_guard: guard,
        _byte_charge: charge,
        ..
    } = command;
    // Physical work has settled or was refused before mutation. Release owned
    // input, authority and capacity before the caller observes this result.
    drop(guard);
    drop(payload);
    drop(admission);
    drop(charge);
    match response {
        AppendReply::Legacy(response) => {
            let _ = response.send(result);
        }
        AppendReply::Proposed {
            replication_policy,
            response,
        } => {
            let _ = response.send(ProposedAppendOutcome::Committed {
                reservation,
                producer,
                replication_policy,
                result,
            });
        }
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
    primary: PrimaryStore,
    tier: Option<LocalObjectTier>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AppendDurability {
    pub(crate) durable_replicas: u32,
    pub(crate) object_durable: bool,
}

impl LaneStores {
    fn borrowed(&mut self) -> StorageResult<BorrowedLaneStores<'_>> {
        Ok(BorrowedLaneStores {
            primary: self.primary.native_mut().ok_or_else(primary::unavailable)?,
            tier: &mut self.tier,
        })
    }
    fn seal_aged_pack(&mut self, age: Duration) -> StorageResult<()> {
        // Owned idle work has no bounded seal/rotation producer here. Keep the
        // ordinary algorithm exclusively on the ordinary variant.
        if self.primary.is_owned() {
            return Ok(());
        }
        self.borrowed()?.seal_aged_pack(age)
    }
    fn append_group(&mut self, appends: &[AppendCommand]) -> AppendGroupOutput {
        match self.borrowed() {
            Ok(mut stores) => stores.append_group(appends),
            Err(error) => AppendGroupOutput {
                outcomes: appends
                    .iter()
                    .map(|_| Err(clone_storage_error(&error)))
                    .collect(),
                sync_file: None,
            },
        }
    }
    fn sync(&mut self, sender: &SyncSender<SyncRequest>) -> StorageResult<()> {
        self.borrowed()?.sync(sender)
    }
    fn garbage_collect(&mut self, tp: TopicPartition, start: LogicalOffset) -> StorageResult<()> {
        self.borrowed()?.garbage_collect(tp, start)
    }
}

struct BorrowedLaneStores<'a> {
    primary: &'a mut ShardStore,
    tier: &'a mut Option<LocalObjectTier>,
}

impl BorrowedLaneStores<'_> {
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

#[cfg(unix)]
fn recovered_worker_clock(expiry: Instant) -> EngineResult<()> {
    if Instant::now() >= expiry {
        return Err(EngineError::InvalidConfig(
            "native recovery original expiry elapsed".into(),
        ));
    }
    Ok(())
}

// Native policy is identical; only the genuine owned opening clock bounds
// independent catalog/offload steps. Ordinary workers use the original helper.
#[cfg(unix)]
fn offload_sealed_packs_until(
    store: &ShardStore,
    tier: &mut LocalObjectTier,
    expiry: Instant,
) -> EngineResult<()> {
    recovered_worker_clock(expiry)?;
    let packs = store.sealed_packs_until(expiry)?;
    for pack in packs {
        recovered_worker_clock(expiry)?;
        let mut extents = Vec::new();
        for entry in store.catalog_until(expiry)? {
            recovered_worker_clock(expiry)?;
            if entry.pack_sequence == pack.sequence {
                extents.push(TierExtent::from_reservation(entry.reservation));
            }
        }
        recovered_worker_clock(expiry)?;
        tier.offload_pack_until(pack.sequence, &pack.path, extents, expiry)?;
        #[cfg(test)]
        recovered_open_test_step("pack-complete");
        recovered_worker_clock(expiry)?;
    }
    Ok(())
}

#[cfg(all(unix, test))]
thread_local! {
    pub(crate) static RECOVERED_OPEN_STEP: std::cell::RefCell<Option<Box<dyn FnMut(&str)>>> =
        const { std::cell::RefCell::new(None) };
}
#[cfg(all(unix, test))]
fn recovered_open_test_step(step: &str) {
    RECOVERED_OPEN_STEP.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook(step);
        }
    });
}

#[cfg(all(unix, test))]
type RecoveryGcBarrier = std::sync::Arc<(std::sync::Mutex<(bool, bool)>, std::sync::Condvar)>;
#[cfg(all(unix, test))]
pub(crate) static RECOVERY_GC_QUEUE_BARRIER: std::sync::Mutex<
    Option<(std::path::PathBuf, RecoveryGcBarrier)>,
> = std::sync::Mutex::new(None);
#[cfg(all(unix, test))]
fn recovered_gc_queued_test_step(directory: Option<&std::path::Path>) {
    let barrier = RECOVERY_GC_QUEUE_BARRIER
        .lock()
        .expect("test hook")
        .as_ref()
        .filter(|(path, _)| Some(path.as_path()) == directory)
        .map(|(_, barrier)| std::sync::Arc::clone(barrier));
    if let Some(barrier) = barrier {
        let (lock, changed) = &*barrier;
        let mut state = lock.lock().expect("actual worker barrier");
        state.0 = true;
        changed.notify_all();
        while !state.1 {
            state = changed.wait(state).expect("release actual worker");
        }
    }
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
