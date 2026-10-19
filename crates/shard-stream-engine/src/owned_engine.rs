//! Consuming same-lease native construction; this is storage custody, not serving authority.

use super::*;
use crate::recovery_census::{NativeRecoveryUsage, OwnedEngineCustody, OwnedRecoveryStorage};
use std::mem::size_of;
use std::time::Instant;

struct Opening {
    engine: JoiningEngine,
    shards: Vec<ShardHandle>,
    worker_start_error: Option<crate::worker::RecoveredStartError>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    gc_outcomes: Vec<crate::worker::NativeGcOutcome>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    gc_pending: Option<crate::worker::GcTicket>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    gc_allocation_error: Option<std::collections::TryReserveError>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    gc_receive_error: Option<std::sync::mpsc::RecvError>,
    journal: Option<ControlJournal>,
    sink: JoiningSink,
    write_fence: Option<Arc<dyn WriteFence>>,
    replication: Option<Arc<dyn ReplicationTransport>>,
    retention_guard: Option<Arc<dyn RetentionGuard>>,
    sink_config: Option<DurableSinkConfig>,
    // Last through errors, unwind, failed spawning and every real worker join.
    storage: OwnedRecoveryStorage,
}
// These fields complete physical sink cleanup before the surrounding owner's
// ordinary field destruction. Opening itself has no Drop, so its final storage
// can move by value without unsafe code or an artificial replacement owner.
struct JoiningEngine(Option<StreamEngine>);
impl std::ops::Deref for JoiningEngine {
    type Target = Option<StreamEngine>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for JoiningEngine {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Drop for JoiningEngine {
    fn drop(&mut self) {
        if let Some(engine) = &mut self.0
            && let Some(sink) = &mut engine.durable_sink
        {
            sink.join_owned_recovery();
        }
    }
}
struct JoiningSink(Option<DurableSinkDispatcher>);
impl std::ops::Deref for JoiningSink {
    type Target = Option<DurableSinkDispatcher>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for JoiningSink {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Drop for JoiningSink {
    fn drop(&mut self) {
        if let Some(sink) = &mut self.0 {
            sink.join_owned_recovery();
        }
    }
}

/// Normal native engine retaining the actual pre-open leases and source evidence.
///
/// No serving/currentness or WriteFence is manufactured by construction. The
/// caller must durably preserve and authenticate all required lineage/floors
/// before mutable storage open, and perform its own complete current rejoin.
/// Only borrowing the engine is allowed: its workers cannot escape this owner.
pub struct OwnedRecoveredEngine {
    engine: StreamEngine,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    _gc_outcomes: std::sync::Mutex<Vec<crate::worker::NativeGcOutcome>>,
    custody: OwnedEngineCustody,
}
impl OwnedRecoveredEngine {
    /// Normal native catalog/fetch/append API, with the actual supplied components.
    pub fn engine(&self) -> &StreamEngine {
        &self.engine
    }
    /// Original census clock, never renewed by construction or worker creation.
    pub fn absolute_expiry(&self) -> Instant {
        self.custody.absolute_expiry()
    }
    /// Cumulative native recovery reservations, not a process RSS bound.
    pub fn usage(&self) -> NativeRecoveryUsage {
        self.custody.usage()
    }
    /// All original native observations remain borrowed, including repaired tails.
    pub fn prior_sources(&self) -> impl Iterator<Item = &crate::RecoverySourceFact> {
        self.custody.facts()
    }
    /// Actual root allocation kept through worker and resource destruction.
    pub fn directory_ownership(&self) -> &Arc<DirectoryOwnership> {
        self.custody.owner()
    }
}
impl Drop for OwnedRecoveredEngine {
    fn drop(&mut self) {
        if let Some(sink) = &mut self.engine.durable_sink {
            sink.join_owned_recovery();
        }
    }
}
impl fmt::Debug for OwnedRecoveredEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedRecoveredEngine")
            .finish_non_exhaustive()
    }
}

/// Refusal retaining actual native stores/workers/leases until physical cleanup.
enum FailedCustody {
    Opening(Box<Opening>),
    // Native setup and snapshot retirement definitively completed, but E
    // elapsed during their physical tail. No engine access can escape refusal.
    Completed(Box<OwnedRecoveredEngine>),
}
/// Opaque refusal retaining actual native workers, stores and original custody.
///
/// Both partially opened state and definitely completed-but-late state remain
/// private through physical cleanup. No engine or retry handle can be extracted.
pub struct OwnedEngineOpenError {
    error: EngineError,
    custody: FailedCustody,
}
impl OwnedEngineOpenError {
    /// Native refusal; no token or storage handle can be extracted for retry.
    pub fn error(&self) -> &EngineError {
        &self.error
    }
    /// Actual retained worker-start error, independently of the public wrapper.
    pub fn worker_start_error(&self) -> Option<&EngineError> {
        match &self.custody {
            FailedCustody::Opening(opening) => opening
                .worker_start_error
                .as_ref()
                .map(|error| error.error()),
            FailedCustody::Completed(_) => None,
        }
    }
    /// Actual worker-start unwind, retained with its real partial workers/store.
    pub fn worker_start_panic_payload(&self) -> Option<&(dyn std::any::Any + Send)> {
        match &self.custody {
            FailedCustody::Opening(opening) => opening
                .worker_start_error
                .as_ref()
                .and_then(|error| error.panic_payload()),
            FailedCustody::Completed(_) => None,
        }
    }
    /// Actual SAME-directory-budget refusal retained at worker startup.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn worker_start_budget_error(&self) -> Option<&shard_stream_core::NativeDirectoryFailure> {
        match &self.custody {
            FailedCustody::Opening(opening) => opening
                .worker_start_error
                .as_ref()
                .and_then(|error| error.budget_error()),
            FailedCustody::Completed(_) => None,
        }
    }
    /// Actual composing checkpoint refusal, separate from a later budget error.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn worker_start_checkpoint_error(&self) -> Option<shardlog::ShardDirectoryCheckFailure> {
        match &self.custody {
            FailedCustody::Opening(opening) => opening
                .worker_start_error
                .as_ref()
                .and_then(|error| error.checkpoint_error()),
            FailedCustody::Completed(_) => None,
        }
    }
    /// Actual captured-health denial unwind, independently of startup unwinds.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn worker_start_deny_panic_payload(&self) -> Option<&(dyn std::any::Any + Send)> {
        match &self.custody {
            FailedCustody::Opening(opening) => opening
                .worker_start_error
                .as_ref()
                .and_then(|error| error.deny_panic_payload()),
            FailedCustody::Completed(_) => None,
        }
    }
    /// Actual bootstrap collection completions/refusals retained before post-E.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn gc_outcomes(&self) -> &[crate::NativeGcOutcome] {
        match &self.custody {
            FailedCustody::Opening(opening) => &opening.gc_outcomes,
            FailedCustody::Completed(_) => &[],
        }
    }
    /// Actual complete-output reservation refusal before a GC command enters.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn gc_allocation_error(&self) -> Option<&std::collections::TryReserveError> {
        match &self.custody {
            FailedCustody::Opening(opening) => opening.gc_allocation_error.as_ref(),
            FailedCustody::Completed(_) => None,
        }
    }
    /// Actual completion-channel disconnection, never modeled as successful GC.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn gc_receive_error(&self) -> Option<&std::sync::mpsc::RecvError> {
        match &self.custody {
            FailedCustody::Opening(opening) => opening.gc_receive_error.as_ref(),
            FailedCustody::Completed(_) => None,
        }
    }
    /// Work already admitted under the unchanged original clock.
    pub fn usage(&self) -> NativeRecoveryUsage {
        match &self.custody {
            FailedCustody::Opening(value) => value.storage.usage(),
            FailedCustody::Completed(value) => value.usage(),
        }
    }
    /// Original operation expiry, retained even after physical completion is late.
    pub fn absolute_expiry(&self) -> Instant {
        match &self.custody {
            FailedCustody::Opening(value) => value.storage.absolute_expiry(),
            FailedCustody::Completed(value) => value.absolute_expiry(),
        }
    }
}
impl fmt::Display for OwnedEngineOpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}
impl fmt::Debug for OwnedEngineOpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedEngineOpenError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
impl std::error::Error for OwnedEngineOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.custody {
            FailedCustody::Opening(opening) => opening
                .worker_start_error
                .as_ref()
                .map(|error| error as &dyn std::error::Error),
            FailedCustody::Completed(_) => None,
        }
    }
}

impl StreamEngine {
    /// Consumes genuine restored native stores without reopening/relocking them.
    ///
    /// Inputs must come from `read_for_engine(...).restore_storage()`. The
    /// actual configuration, original Instant, anchored coordinator descriptor,
    /// native shard/tier leases and old raw facts are carried internally. This
    /// does not resume sinks or grant permission; external component callbacks
    /// retain their existing contracts and are checked for late completion.
    pub fn open_recovered_storage(
        storage: OwnedRecoveryStorage,
        write_fence: Option<Arc<dyn WriteFence>>,
        replication: Option<Arc<dyn ReplicationTransport>>,
        retention_guard: Option<Arc<dyn RetentionGuard>>,
        durable_sink: Option<DurableSinkConfig>,
    ) -> Result<OwnedRecoveredEngine, OwnedEngineOpenError> {
        let mut opening = Opening {
            engine: JoiningEngine(None),
            shards: Vec::new(),
            worker_start_error: None,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            gc_outcomes: Vec::new(),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            gc_pending: None,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            gc_allocation_error: None,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            gc_receive_error: None,
            journal: None,
            sink: JoiningSink(None),
            write_fence,
            replication,
            retention_guard,
            sink_config: durable_sink,
            storage,
        };
        let result = opening.construct();
        if let Err(error) = result {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            if opening.worker_start_error.is_none()
                && opening
                    .gc_outcomes
                    .last()
                    .is_none_or(|outcome| outcome.accepted())
            {
                opening.storage.recovery_deny_primary();
            }
            #[cfg(not(any(target_os = "linux", target_os = "android")))]
            opening.storage.recovery_deny_primary();
            return Err(OwnedEngineOpenError {
                error,
                custody: FailedCustody::Opening(Box::new(opening)),
            });
        }
        let Some(engine) = opening.engine.take() else {
            return Err(OwnedEngineOpenError {
                error: EngineError::CorruptState("owned engine construction incomplete".into()),
                custody: FailedCustody::Opening(Box::new(opening)),
            });
        };
        // All fallible work is complete; the actual final storage moves by value.
        let recovered = opening.finish(engine);
        if Instant::now() >= recovered.absolute_expiry() {
            return Err(OwnedEngineOpenError {
                error: EngineError::InvalidConfig("native recovery original expiry elapsed".into()),
                custody: FailedCustody::Completed(Box::new(recovered)),
            });
        }
        Ok(recovered)
    }
}

// The test fixture prices the same complete fixed opening/error owners. Keep
// this test-only getter tied to the exact construct framing expression.
#[cfg(test)]
pub(crate) fn recovery_opening_framing_bytes_for_test() -> usize {
    size_of::<Opening>() + size_of::<OwnedRecoveredEngine>() + size_of::<OwnedEngineOpenError>()
}

impl Opening {
    fn finish(self, engine: StreamEngine) -> OwnedRecoveredEngine {
        OwnedRecoveredEngine {
            engine,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            _gc_outcomes: std::sync::Mutex::new(self.gc_outcomes),
            custody: self.storage.finish_engine_custody(),
        }
    }

    fn construct(&mut self) -> EngineResult<()> {
        self.storage.recovery_clock()?;
        self.storage.admit_engine()?;
        let extra = size_of::<Self>()
            + size_of::<OwnedRecoveredEngine>()
            + size_of::<OwnedEngineOpenError>();
        self.storage.recovery_charge(extra, extra as u64)?;
        let config = self.storage.recovery_config().clone();
        config.validate()?;
        self.storage
            .recovery_owner()
            .validate_directory(&config.data_dir)
            .map_err(|e| EngineError::InvalidConfig(e.to_string()))?;
        let requires_physical_admission = self
            .write_fence
            .as_ref()
            .is_some_and(|f| f.requires_physical_admission());
        self.storage.recovery_clock()?;
        if requires_physical_admission {
            for bytes in [config.max_batch_bytes, config.max_fetch_bytes] {
                if crate::worker::append_command_charge(bytes, true)
                    .is_none_or(|n| n > config.queue_bytes_per_shard)
                {
                    return Err(EngineError::InvalidConfig(
                        "guarded request ceilings plus admission metadata exceed shard capacity"
                            .into(),
                    ));
                }
            }
        }
        if config.replication_factor > 1 && self.replication.is_none() {
            return Err(EngineError::InvalidConfig("RF>1 requires an injected replication transport; the public engine only provides RF1 storage".into()));
        }
        if let Some(sink) = self.sink_config.as_ref() {
            let bytes = crate::durable_sink::recovery_dispatcher_bytes(
                sink,
                config.shard_count as usize,
                config.queue_bytes_per_shard,
            )
            .ok_or_else(|| {
                EngineError::InvalidConfig("native sink opening allowance overflow".into())
            })?;
            self.storage.recovery_charge(
                bytes,
                u64::try_from(bytes).map_err(|_| {
                    EngineError::InvalidConfig("native sink opening allowance overflow".into())
                })?,
            )?;
        }
        if let Some(sink) = self.sink_config.take() {
            self.storage.recovery_clock()?;
            self.sink.0 = Some(DurableSinkDispatcher::open_with_recovery_ownership(
                sink,
                &config.shard_ids(),
                config.queue_bytes_per_shard,
                config.max_batch_bytes,
                Arc::clone(self.storage.recovery_owner()),
                self.storage.absolute_expiry(),
            )?);
            self.storage.recovery_clock()?;
        }
        let (journal, recovered) = self.storage.recovery_open_journal()?;
        let retained_log_starts = crate::coordinator::retention_hints_until(
            &recovered.events,
            self.storage.absolute_expiry(),
        )?;
        let mut catalog = HashMap::new();
        self.shards
            .try_reserve_exact(config.shard_count as usize)
            .map_err(|_| EngineError::InvalidConfig("native worker reservation failed".into()))?;
        for number in 0..config.shard_count {
            self.storage.recovery_clock()?;
            let shard = ShardId::new(number);
            #[cfg(any(target_os = "linux", target_os = "android"))]
            let startup_policy = self.storage.recovery_gc_policy()?;
            let (primary, tier) = self.storage.recovery_take_shard(shard)?;
            let observed = ShardHandle::spawn_recovered_primary(
                &config,
                shard,
                primary,
                tier,
                Arc::clone(self.storage.recovery_owner()),
                self.storage.absolute_expiry(),
                #[cfg(any(target_os = "linux", target_os = "android"))]
                startup_policy,
            );
            let (handle, entries) = match observed {
                Ok(value) => value,
                Err(error) => {
                    self.worker_start_error = Some(error);
                    return Err(EngineError::InvalidConfig(
                        "actual retained recovered worker start refused".into(),
                    ));
                }
            };
            // Keep the live handle before fallible catalog duplicate checking.
            self.shards.push(handle);
            for entry in entries {
                self.storage.recovery_clock()?;
                let tp =
                    TopicPartition::new(entry.reservation.topic_id, entry.reservation.partition_id);
                if retained_log_starts
                    .get(&tp)
                    .is_some_and(|start| entry.reservation.last_offset < *start)
                {
                    continue;
                }
                let key = (tp, entry.reservation.batch_id);
                if catalog.insert(key, entry).is_some() {
                    return Err(EngineError::CorruptState(format!(
                        "batch {} appears in more than one extent",
                        key.1
                    )));
                }
            }
        }
        let visits = catalog
            .len()
            .checked_add(1)
            .and_then(|n| {
                n.checked_mul(
                    catalog
                        .len()
                        .checked_add(recovered.events.len())?
                        .checked_add(1)?,
                )
            })
            .and_then(|n| {
                n.checked_mul(
                    size_of::<shardlog::ExtentCatalogEntry>() + size_of::<shardlog::JournalEvent>(),
                )
            })
            .ok_or_else(|| EngineError::InvalidConfig("native recovery sizing overflow".into()))?;
        self.storage.recovery_charge(
            0,
            u64::try_from(visits).map_err(|_| {
                EngineError::InvalidConfig("native recovery sizing overflow".into())
            })?,
        )?;
        let slots = crate::coordinator::recovery_batch_slots(
            &recovered.events,
            &catalog,
            self.storage.absolute_expiry(),
        )?;
        let partitions = recovered
            .events
            .len()
            .checked_add(catalog.len())
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| EngineError::InvalidConfig("native recovery value overflow".into()))?;
        let bytes = crate::coordinator::recovery_coordinator_bytes(
            config.shard_count as usize,
            partitions,
            slots,
        )
        .ok_or_else(|| EngineError::InvalidConfig("native recovery value overflow".into()))?;
        self.storage.recovery_charge(
            bytes,
            u64::try_from(bytes)
                .ok()
                .and_then(|n| n.checked_mul(8))
                .ok_or_else(|| {
                    EngineError::InvalidConfig("native recovery work overflow".into())
                })?,
        )?;
        self.storage.recovery_clock()?;
        let recovered_states = crate::coordinator::recover_partition_states_until(
            &config,
            &recovered.events,
            &catalog,
            1,
            self.storage.absolute_expiry(),
        )?;
        self.storage.recovery_clock()?;
        let mut recovered_partitions = recovered_states.keys().copied().collect::<Vec<_>>();
        recovered_partitions.sort_unstable();
        self.storage.recovery_clock()?;
        self.journal = Some(ControlJournal::spawn_until(
            journal,
            self.storage.absolute_expiry(),
        )?);
        self.storage.recovery_clock()?;
        let coordinators = CoordinatorPool::spawn_until(
            config.shard_count as usize,
            recovered_states,
            self.journal
                .as_ref()
                .ok_or_else(|| EngineError::CorruptState("owned journal missing".into()))?,
            self.storage.absolute_expiry(),
        )?;
        self.storage.recovery_clock()?;
        let replica_progress = Arc::new(coordinators.router());
        if let Some(replication) = &self.replication {
            self.storage.recovery_clock()?;
            replication.install_progress(ReplicationProgress {
                router: Arc::downgrade(&replica_progress),
            })?;
            self.storage.recovery_clock()?;
        }
        self.engine.0 = Some(StreamEngine {
            config,
            shards: std::mem::take(&mut self.shards),
            recovered_partitions,
            _replication_progress: replica_progress,
            coordinators,
            journal: self
                .journal
                .take()
                .ok_or_else(|| EngineError::CorruptState("owned journal missing".into()))?,
            write_fence: self.write_fence.take(),
            requires_physical_admission,
            replication: self.replication.take(),
            retention_guard: self.retention_guard.take(),
            durable_sink: self.sink.take(),
            directory_ownership: Some(Arc::clone(self.storage.recovery_owner())),
        });
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let owned_gc = self.storage.recovery_gc_policy()?;
        for (tp, start) in retained_log_starts {
            self.storage.recovery_clock()?;
            if start.get() > 0 {
                let engine = self
                    .engine
                    .as_ref()
                    .ok_or_else(|| EngineError::CorruptState("owned engine missing".into()))?;
                let mut log_start = start;
                if let Some(sink) = &engine.durable_sink {
                    self.storage.recovery_clock()?;
                    log_start = log_start.min(sink.checkpoint(tp)?.next_offset);
                    self.storage.recovery_clock()?;
                }
                for shard in &engine.shards {
                    self.storage.recovery_clock()?;
                    #[cfg(any(target_os = "linux", target_os = "android"))]
                    if let Some(policy) = &owned_gc {
                        let fixed = crate::worker::owned_gc_command_cost()
                            .and_then(|n| {
                                n.checked_add(std::mem::size_of::<crate::NativeGcOutcome>())
                            })
                            .ok_or_else(|| {
                                EngineError::InvalidConfig(
                                    "native GC output sizing overflow".into(),
                                )
                            })?;
                        self.storage.recovery_charge(
                            fixed,
                            u64::try_from(fixed).map_err(|_| {
                                EngineError::InvalidConfig(
                                    "native GC output sizing overflow".into(),
                                )
                            })?,
                        )?;
                        self.gc_allocation_error = self.gc_outcomes.try_reserve_exact(1).err();
                        self.storage.recovery_clock()?;
                        if self.gc_allocation_error.is_some() {
                            return Err(EngineError::InvalidConfig(
                                "native GC output reservation refused".into(),
                            ));
                        }
                        match shard.begin_owned_gc(
                            tp,
                            log_start,
                            policy,
                            self.storage.absolute_expiry(),
                        ) {
                            Ok(ticket) => self.gc_pending = Some(ticket),
                            Err(outcome) => {
                                self.gc_outcomes.push(outcome);
                                return Err(EngineError::InvalidConfig(
                                    "actual native GC command refused before entry".into(),
                                ));
                            }
                        }
                        let observed = self
                            .gc_pending
                            .as_mut()
                            .ok_or(EngineError::WorkerStopped(shard.shard_id))?
                            .settle();
                        self.gc_pending.take();
                        match observed {
                            Ok(outcome) => self.gc_outcomes.push(outcome),
                            Err(error) => {
                                self.gc_receive_error = Some(error);
                                return Err(EngineError::WorkerStopped(shard.shard_id));
                            }
                        }
                        self.storage.recovery_clock()?;
                        if !self
                            .gc_outcomes
                            .last()
                            .is_some_and(|outcome| outcome.accepted())
                        {
                            return Err(EngineError::InvalidConfig(
                                "actual consuming Core GC refused".into(),
                            ));
                        }
                        continue;
                    }
                    shard.garbage_collect_until(tp, log_start, self.storage.absolute_expiry())?;
                    self.storage.recovery_clock()?;
                }
            }
        }
        self.storage.recovery_clock()
    }
}
