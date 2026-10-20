//! Retain the actual whole primary until real recovered lane entry and joins.

use super::*;
use std::any::Any;
use std::sync::Mutex;
use std::sync::mpsc::{RecvTimeoutError, TryRecvError};

const STACK_BYTES: usize = 2 * 1024 * 1024;

pub(super) struct LaneInput {
    stores: LaneStores,
    receiver: shard_stream_core::ByteBoundedReceiver<ShardCommand>,
    append_linger: Duration,
    max_pack_age: Duration,
    sync_sender: SyncSender<SyncRequest>,
}
pub(super) type InputSlot = Arc<Mutex<Option<LaneInput>>>;

// The actual native channel result, with both endpoints or its original error.
type LaneChannelResult = Result<
    (
        ByteBoundedSender<ShardCommand>,
        shard_stream_core::ByteBoundedReceiver<ShardCommand>,
    ),
    shard_stream_core::ChannelConfigError,
>;

struct Start {
    primary: Option<PrimaryStore>,
    tier: Option<LocalObjectTier>,
    catalog_result: Option<StorageResult<Vec<ExtentCatalogEntry>>>,
    catalog: Vec<ExtentCatalogEntry>,
    sync_spawn: Option<io::Result<JoinHandle<()>>>,
    lane_spawn: Option<io::Result<JoinHandle<()>>>,
    channel_result: Option<LaneChannelResult>,
    sync_ready: Option<Result<(), RecvTimeoutError>>,
    lane_ready: Option<Result<(), RecvTimeoutError>>,
    panic_payload: Option<Box<dyn Any + Send>>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    policy: Option<NativeResumeGcPolicy>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    budget_error: Option<shard_stream_core::NativeDirectoryFailure>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    checkpoint_error: Option<shardlog::ShardDirectoryCheckFailure>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    deny_panic: Option<Box<dyn Any + Send>>,
    // Joining actual workers precedes every remaining original owner/holder.
    handle: ShardHandle,
    lane_directory: Option<primary::DirectoryRetention>,
    sync_directory: Option<primary::DirectoryRetention>,
    ownership: Arc<shard_stream_core::DirectoryOwnership>,
}

/// Actual retained start refusal; no input, worker or bare store extraction.
pub(crate) struct RecoveredStartError {
    error: EngineError,
    start: Option<Box<Start>>,
}
impl RecoveredStartError {
    pub(crate) fn error(&self) -> &EngineError {
        &self.error
    }
    pub(crate) fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.start.as_ref().and_then(|v| v.panic_payload.as_deref())
    }
    pub(crate) fn sync_spawn_error(&self) -> Option<&io::Error> {
        self.start.as_ref()?.sync_spawn.as_ref()?.as_ref().err()
    }
    pub(crate) fn channel_error(&self) -> Option<&shard_stream_core::ChannelConfigError> {
        self.start.as_ref()?.channel_result.as_ref()?.as_ref().err()
    }
    pub(crate) fn lane_spawn_error(&self) -> Option<&io::Error> {
        self.start.as_ref()?.lane_spawn.as_ref()?.as_ref().err()
    }
    #[cfg(test)]
    pub(crate) fn lane_spawned(&self) -> bool {
        self.start
            .as_ref()
            .is_some_and(|start| start.handle.thread.is_some())
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(crate) fn budget_error(&self) -> Option<&shard_stream_core::NativeDirectoryFailure> {
        self.start.as_ref()?.budget_error.as_ref()
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(crate) fn checkpoint_error(&self) -> Option<shardlog::ShardDirectoryCheckFailure> {
        self.start.as_ref()?.checkpoint_error
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(crate) fn deny_panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.start.as_ref()?.deny_panic.as_deref()
    }
}
impl std::fmt::Debug for RecoveredStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveredStartError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for RecoveredStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}
impl std::error::Error for RecoveredStartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(error) = self.budget_error() {
            return Some(error);
        }
        if let Some(error) = self.channel_error() {
            return Some(error);
        }
        self.sync_spawn_error()
            .or_else(|| self.lane_spawn_error())
            .map(|error| error as &dyn std::error::Error)
    }
}

fn wait_ready(receiver: &Receiver<()>, expiry: Instant) -> Result<(), RecvTimeoutError> {
    match receiver.try_recv() {
        Ok(()) => Ok(()),
        Err(TryRecvError::Disconnected) => Err(RecvTimeoutError::Disconnected),
        Err(TryRecvError::Empty) => {
            receiver.recv_timeout(expiry.saturating_duration_since(Instant::now()))
        }
    }
}
impl Start {
    fn check(&mut self, expiry: Instant) -> EngineResult<()> {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(policy) = &self.policy {
            if let Err(error) = policy
                .budget
                .cap_expiry(expiry)
                .and_then(|()| policy.budget.check())
            {
                self.budget_error = Some(error);
                return Err(EngineError::InvalidConfig(
                    "same native worker-start budget refused".into(),
                ));
            }
            let checked = policy.checks.check();
            if let Err(error) = checked {
                self.checkpoint_error = Some(error);
            }
            if let Err(error) = policy.budget.check() {
                self.budget_error = Some(error);
                return Err(EngineError::InvalidConfig(
                    "same native worker-start budget refused after checkpoint".into(),
                ));
            }
            if checked.is_err() {
                return Err(EngineError::InvalidConfig(
                    "actual worker-start composing checkpoint refused".into(),
                ));
            }
        }
        recovered_worker_clock(expiry)
    }
    fn readiness_expiry(&self, expiry: Instant) -> Instant {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(policy) = &self.policy {
            return expiry.min(policy.budget.absolute_expiry());
        }
        expiry
    }
    fn deny(&mut self) {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(policy) = &self.policy {
            policy.budget.deny();
            self.deny_panic = catch_unwind(AssertUnwindSafe(|| policy.checks.deny())).err();
        }
    }
    fn run(&mut self, config: &EngineConfig, expiry: Instant) -> EngineResult<()> {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if self.policy.is_none() {
            return Err(EngineError::InvalidConfig(
                "actual owned primary requires its same startup work/checks".into(),
            ));
        }
        self.check(expiry)?;
        let common = self.readiness_expiry(expiry);
        if let Some(tier) = &mut self.tier {
            let primary = self
                .primary
                .as_ref()
                .and_then(PrimaryStore::native)
                .ok_or_else(primary::unavailable)?;
            offload_sealed_packs_until(primary, tier, common)?;
        }
        self.check(expiry)?;
        self.catalog_result = Some(
            self.primary
                .as_ref()
                .and_then(PrimaryStore::native)
                .ok_or_else(primary::unavailable)?
                .catalog_until(self.readiness_expiry(expiry)),
        );
        self.check(expiry)?;
        if !matches!(self.catalog_result, Some(Ok(_))) {
            return Err(EngineError::CorruptState(
                "actual recovered catalog refused; result retained".into(),
            ));
        }
        let Some(Ok(catalog)) = self.catalog_result.take() else {
            return Err(EngineError::WorkerStopped(self.handle.shard_id));
        };
        self.catalog = catalog;
        // Tier/catalog remain ordinary native algorithms; their true bounded
        // producers are separate from this physical ownership handoff.
        if self.tier.is_some() {
            let mut durable_packs = std::collections::HashSet::new();
            let count = self
                .tier
                .as_ref()
                .ok_or(EngineError::WorkerStopped(self.handle.shard_id))?
                .manifest()
                .objects
                .len();
            for index in 0..count {
                self.check(expiry)?;
                let sequence = self
                    .tier
                    .as_ref()
                    .ok_or(EngineError::WorkerStopped(self.handle.shard_id))?
                    .manifest()
                    .objects[index]
                    .pack_sequence;
                durable_packs.insert(sequence);
            }
            for index in 0..self.catalog.len() {
                self.check(expiry)?;
                self.catalog[index].object_durable =
                    durable_packs.contains(&self.catalog[index].pack_sequence);
            }
        }
        self.check(expiry)?;
        let (sync_sender, sync_receiver) = sync_channel(SYNC_QUEUE_SLOTS);
        self.handle.sync_sender = Some(sync_sender);
        let (ready, ready_receiver) = sync_channel(1);
        let ownership = Arc::clone(&self.ownership);
        let directory = self.sync_directory.take();
        let name = format!("shard-stream-sync-{}", self.handle.shard_id);
        self.check(expiry)?;
        self.sync_spawn = Some(
            thread::Builder::new()
                .name(name)
                .stack_size(STACK_BYTES)
                .spawn(move || {
                    let directory = directory;
                    let ownership = ownership;
                    let _ = ready.send(());
                    run_sync_worker(sync_receiver);
                    drop(ownership);
                    drop(directory);
                }),
        );
        // Store the real JoinHandle immediately, including late success, before
        // any checkpoint or test callback. Failure leaves the real IO error.
        if matches!(self.sync_spawn, Some(Ok(_)))
            && let Some(Ok(thread)) = self.sync_spawn.take()
        {
            self.handle.sync_thread = Some(thread);
        }
        #[cfg(test)]
        recovered_open_test_step("sync-spawned");
        self.check(expiry)?;
        if self.handle.sync_thread.is_none() {
            return Err(EngineError::InvalidConfig(
                "actual recovered sync spawn refused".into(),
            ));
        }
        self.sync_ready = Some(wait_ready(&ready_receiver, self.readiness_expiry(expiry)));
        self.check(expiry)?;
        if !matches!(self.sync_ready, Some(Ok(()))) {
            return Err(EngineError::WorkerStopped(self.handle.shard_id));
        }
        self.channel_result = Some(byte_bounded_channel(
            config.queue_slots_per_shard,
            config.queue_bytes_per_shard,
        ));
        self.check(expiry)?;
        if !matches!(self.channel_result, Some(Ok(_))) {
            return Err(EngineError::InvalidConfig(
                "actual recovered lane channel refused".into(),
            ));
        }
        let Some(Ok((sender, receiver))) = self.channel_result.take() else {
            return Err(EngineError::WorkerStopped(self.handle.shard_id));
        };
        self.handle.sender = Some(sender);
        self.handle.recovered_input = Some(Arc::new(Mutex::new(None)));
        self.check(expiry)?;
        // Allocate/validate the slot before moving the sole actual Core store.
        // All following input moves are infallible and enter the retained slot
        // before the next callback/clock or actual OS thread attempt.
        let append_sync_sender = self
            .handle
            .sync_sender
            .as_ref()
            .ok_or(EngineError::WorkerStopped(self.handle.shard_id))?
            .clone();
        let slot = self
            .handle
            .recovered_input
            .as_mut()
            .ok_or(EngineError::WorkerStopped(self.handle.shard_id))?;
        let slot = Arc::get_mut(slot).ok_or(EngineError::WorkerStopped(self.handle.shard_id))?;
        let slot = slot
            .get_mut()
            .map_err(|_| EngineError::WorkerStopped(self.handle.shard_id))?;
        let primary = self.primary.take().ok_or_else(primary::unavailable)?;
        *slot = Some(LaneInput {
            stores: LaneStores {
                primary,
                tier: self.tier.take(),
            },
            receiver,
            append_linger: config.append_linger,
            max_pack_age: config.max_pack_age,
            sync_sender: append_sync_sender,
        });
        self.check(expiry)?;
        let slot = Arc::clone(
            self.handle
                .recovered_input
                .as_ref()
                .ok_or(EngineError::WorkerStopped(self.handle.shard_id))?,
        );
        let (ready, ready_receiver) = sync_channel(1);
        let directory = self.lane_directory.take();
        let ownership = Arc::clone(&self.ownership);
        let name = format!("shard-stream-lane-{}", self.handle.shard_id);
        self.check(expiry)?;
        self.lane_spawn = Some(
            thread::Builder::new()
                .name(name)
                .stack_size(STACK_BYTES)
                .spawn(move || {
                    let directory = directory;
                    let ownership = ownership;
                    let input = {
                        let locked = slot.lock();
                        match locked {
                            Ok(mut input) => input.take(),
                            Err(_) => None,
                        }
                    };
                    if let Some(input) = input {
                        let _ = ready.send(());
                        run_worker(
                            input.stores,
                            input.receiver,
                            input.append_linger,
                            input.max_pack_age,
                            input.sync_sender,
                        );
                    }
                    drop(slot);
                    drop(ownership);
                    drop(directory);
                }),
        );
        if matches!(self.lane_spawn, Some(Ok(_)))
            && let Some(Ok(thread)) = self.lane_spawn.take()
        {
            self.handle.thread = Some(thread);
        }
        #[cfg(test)]
        recovered_open_test_step("lane-spawned");
        self.check(expiry)?;
        if self.handle.thread.is_none() {
            return Err(EngineError::InvalidConfig(
                "actual recovered lane spawn refused".into(),
            ));
        }
        self.lane_ready = Some(wait_ready(&ready_receiver, self.readiness_expiry(expiry)));
        self.check(expiry)?;
        if !matches!(self.lane_ready, Some(Ok(()))) {
            return Err(EngineError::WorkerStopped(self.handle.shard_id));
        }
        Ok(())
    }
}

pub(super) fn retained_start_bytes() -> Option<usize> {
    [
        std::mem::size_of::<Start>(),
        std::mem::size_of::<RecoveredStartError>(),
        std::mem::size_of::<Mutex<Option<LaneInput>>>(),
        std::mem::size_of::<[usize; 2]>(),
        std::mem::size_of::<Option<InputSlot>>(),
        std::mem::size_of::<(SyncSender<()>, Receiver<()>)>() * 2,
        std::mem::size_of::<String>() * 2,
        std::mem::size_of::<Box<dyn Any + Send>>() * 2,
        STACK_BYTES * 2,
        512,
    ]
    .into_iter()
    .try_fold(0usize, usize::checked_add)
}
impl ShardHandle {
    #[cfg(unix)]
    pub(crate) fn spawn_recovered_primary(
        config: &EngineConfig,
        shard_id: ShardId,
        primary: PrimaryStore,
        tier: Option<LocalObjectTier>,
        ownership: Arc<shard_stream_core::DirectoryOwnership>,
        expiry: Instant,
        #[cfg(any(target_os = "linux", target_os = "android"))] policy: Option<
            NativeResumeGcPolicy,
        >,
    ) -> Result<(Self, Vec<ExtentCatalogEntry>), RecoveredStartError> {
        if !primary.is_owned() {
            return Self::spawn_recovered(config, shard_id, primary, tier, ownership, expiry)
                .map_err(|error| RecoveredStartError { error, start: None });
        }
        let lane_directory = Some(primary.retain_directory());
        let sync_directory = Some(primary.retain_directory());
        let mut start = Box::new(Start {
            primary: Some(primary),
            tier,
            catalog_result: None,
            catalog: Vec::new(),
            sync_spawn: None,
            lane_spawn: None,
            channel_result: None,
            sync_ready: None,
            lane_ready: None,
            panic_payload: None,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            policy,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            budget_error: None,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            checkpoint_error: None,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            deny_panic: None,
            handle: Self {
                shard_id,
                sender: None,
                thread: None,
                sync_sender: None,
                sync_thread: None,
                recovered_input: None,
            },
            lane_directory,
            sync_directory,
            ownership,
        });
        let result = catch_unwind(AssertUnwindSafe(|| start.run(config, expiry)));
        let result = match result {
            Ok(result) => result,
            Err(payload) => {
                start.panic_payload = Some(payload);
                Err(EngineError::InvalidConfig(
                    "actual recovered worker start unwound".into(),
                ))
            }
        };
        if let Err(error) = result {
            start.deny();
            return Err(RecoveredStartError {
                error,
                start: Some(start),
            });
        }
        let Start {
            handle, catalog, ..
        } = *start;
        Ok((handle, catalog))
    }
}
