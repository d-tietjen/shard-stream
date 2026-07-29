use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::num::NonZeroU32;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bytes::Bytes;
use shard_stream_core::{
    LogicalOffset, PlacementSequence, RecordId, Reservation, ShardId, TopicPartition,
};
use shard_stream_protocol::AtomicGroupIdentity;

use crate::{AtomicGroupState, EngineError, EngineResult};

const RETRY_MIN: Duration = Duration::from_millis(10);
const RETRY_MAX: Duration = Duration::from_secs(1);

/// Cluster-shared progress for one logical partition.
///
/// `next_placement_sequence` and `next_offset` identify the same next batch.
/// Sink implementations must persist both values with the data transaction so
/// recovery can detect a stale, skipped, or mismatched checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DurableSinkCheckpoint {
    pub topic_partition: TopicPartition,
    pub next_placement_sequence: PlacementSequence,
    pub next_offset: LogicalOffset,
}

impl DurableSinkCheckpoint {
    #[must_use]
    pub const fn initial(topic_partition: TopicPartition) -> Self {
        Self {
            topic_partition,
            next_placement_sequence: PlacementSequence::new(0),
            next_offset: LogicalOffset::new(0),
        }
    }

    pub(crate) fn after(self, reservation: Reservation) -> EngineResult<Self> {
        if self.topic_partition
            != TopicPartition::new(reservation.topic_id, reservation.partition_id)
            || self.next_placement_sequence != reservation.placement.sequence
            || self.next_offset != reservation.first_offset
        {
            return Err(EngineError::DurableSinkCheckpoint(format!(
                "append {} at sequence {} and offset {} does not follow checkpoint sequence {} and offset {}",
                reservation.batch_id,
                reservation.placement.sequence,
                reservation.first_offset,
                self.next_placement_sequence,
                self.next_offset
            )));
        }
        Ok(Self {
            topic_partition: self.topic_partition,
            next_placement_sequence: PlacementSequence::new(
                self.next_placement_sequence
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| {
                        EngineError::DurableSinkCheckpoint(
                            "durable sink placement sequence exhausted".into(),
                        )
                    })?,
            ),
            next_offset: LogicalOffset::new(
                reservation
                    .last_offset
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| {
                        EngineError::DurableSinkCheckpoint(
                            "durable sink logical offset exhausted".into(),
                        )
                    })?,
            ),
        })
    }
}

/// How an append should become visible in a configured sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableAppendDelivery {
    /// Publish the append as soon as the sink transaction commits.
    Publish,
    /// Durably stage the append until an atomic-group decision arrives.
    Stage,
}

/// One leader-owned append that is already durable in the local WAL.
#[derive(Debug, Clone)]
pub struct DurableAppend {
    pub event_id: RecordId,
    pub physical_shard_id: ShardId,
    pub reservation: Reservation,
    pub producer_event_id: Option<RecordId>,
    pub atomic_group: Option<AtomicGroupIdentity>,
    pub delivery: DurableAppendDelivery,
    pub payload: Bytes,
}

impl DurableAppend {
    #[must_use]
    pub const fn topic_partition(&self) -> TopicPartition {
        TopicPartition::new(self.reservation.topic_id, self.reservation.partition_id)
    }
}

/// Stable identity for one partition-scoped atomic group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DurableAtomicGroup {
    pub topic_partition: TopicPartition,
    pub transaction_id: u128,
}

/// A durable coordinator decision for a staged atomic group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableAtomicDecision {
    pub group: DurableAtomicGroup,
    pub outcome: AtomicGroupState,
}

/// Result of an atomic sink/checkpoint transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableSinkApply {
    /// The append data and `next` checkpoint committed atomically.
    Applied,
    /// Another owner advanced the cluster checkpoint first.
    CheckpointConflict(DurableSinkCheckpoint),
}

/// Sink instance associated with one physical shard.
///
/// Every instance produced by one factory must share the same transactional
/// checkpoint namespace. `apply` must compare `expected`, atomically persist
/// all append effects and `next`, and be idempotent by `DurableAppend::event_id`.
pub trait DurableAppendSink: Send + Sync + fmt::Debug {
    fn apply(
        &self,
        expected: DurableSinkCheckpoint,
        appends: &[DurableAppend],
        next: DurableSinkCheckpoint,
    ) -> EngineResult<DurableSinkApply>;
}

/// Constructs shard-local sink handles backed by one cluster-shared sink.
pub trait DurableAppendSinkFactory: Send + Sync + fmt::Debug {
    /// Runs before leader reservation so malformed data cannot consume offsets.
    fn validate_append(&self, payload: &[u8], record_count: NonZeroU32) -> EngineResult<()>;

    /// Loads the last atomically committed checkpoint for a partition.
    fn load_checkpoint(
        &self,
        topic_partition: TopicPartition,
    ) -> EngineResult<Option<DurableSinkCheckpoint>>;

    /// Opens a handle used to apply events physically owned by `shard_id`.
    fn open_shard(&self, shard_id: ShardId) -> EngineResult<Arc<dyn DurableAppendSink>>;

    /// Declares support for durable staging and idempotent commit/abort decisions.
    fn supports_committed_only(&self) -> bool {
        false
    }

    /// Returns unresolved groups durably staged by committed-only mode.
    fn pending_atomic_groups(&self) -> EngineResult<Vec<DurableAtomicGroup>> {
        Ok(Vec::new())
    }

    /// Atomically publishes or discards a previously staged group.
    fn finalize_atomic_group(&self, _decision: DurableAtomicDecision) -> EngineResult<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DurableSinkRecoveryMode {
    #[default]
    Blocking,
    Background,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DurableSinkLagPolicy {
    #[default]
    Backpressure,
    Availability,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DurableSinkAtomicVisibility {
    #[default]
    AllDurable,
    CommittedOnly,
}

/// Delivery limits and recovery policy for a durable sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableSinkOptions {
    pub recovery_mode: DurableSinkRecoveryMode,
    pub lag_policy: DurableSinkLagPolicy,
    pub atomic_visibility: DurableSinkAtomicVisibility,
    /// Partition-striped callback workers. Increase this for slow independent sinks.
    pub worker_count: usize,
    pub max_pending_items: usize,
    /// `None` inherits `EngineConfig::queue_bytes_per_shard`.
    pub max_pending_bytes: Option<usize>,
    /// Bounds blocking checkpoint loads and startup catch-up.
    pub recovery_timeout: Duration,
}

impl Default for DurableSinkOptions {
    fn default() -> Self {
        Self {
            recovery_mode: DurableSinkRecoveryMode::Blocking,
            lag_policy: DurableSinkLagPolicy::Backpressure,
            atomic_visibility: DurableSinkAtomicVisibility::AllDurable,
            worker_count: 1,
            max_pending_items: 4_096,
            max_pending_bytes: None,
            recovery_timeout: Duration::from_secs(30),
        }
    }
}

/// Factory and policy passed to a sink-enabled engine constructor.
#[derive(Clone)]
pub struct DurableSinkConfig {
    pub factory: Arc<dyn DurableAppendSinkFactory>,
    pub options: DurableSinkOptions,
}

impl DurableSinkConfig {
    #[must_use]
    pub fn new(factory: Arc<dyn DurableAppendSinkFactory>) -> Self {
        Self {
            factory,
            options: DurableSinkOptions::default(),
        }
    }

    #[must_use]
    pub const fn with_options(mut self, options: DurableSinkOptions) -> Self {
        self.options = options;
        self
    }
}

impl fmt::Debug for DurableSinkConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableSinkConfig")
            .field("factory", &self.factory)
            .field("options", &self.options)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DurableSinkStats {
    pub pending_items: u64,
    pub pending_bytes: u64,
    /// Milliseconds since the most recent checkpoint while work is pending.
    pub checkpoint_age_ms: u64,
    pub applied_appends: u64,
    pub checkpoint_conflicts: u64,
    pub retry_attempts: u64,
    pub failed_attempts: u64,
    pub dropped_notifications: u64,
    pub dirty_partitions: u64,
}

#[derive(Debug)]
struct Budget {
    pending_items: AtomicUsize,
    pending_bytes: AtomicUsize,
    stopped: AtomicBool,
    wait_lock: Mutex<()>,
    changed: Condvar,
    max_items: usize,
    max_bytes: usize,
}

impl Budget {
    fn reserve(
        self: &Arc<Self>,
        bytes: usize,
        policy: DurableSinkLagPolicy,
    ) -> EngineResult<Option<BudgetCharge>> {
        if self.try_reserve(bytes, false) {
            return Ok(Some(BudgetCharge {
                budget: Arc::clone(self),
                bytes,
            }));
        }
        if policy == DurableSinkLagPolicy::Availability {
            return Ok(None);
        }
        let mut wait = self.wait_lock.lock().map_err(|_| {
            EngineError::DurableSinkUnavailable("delivery budget wait lock poisoned".into())
        })?;
        loop {
            if self.stopped.load(Ordering::Acquire) {
                return Err(EngineError::DurableSinkUnavailable(
                    "delivery worker stopped".into(),
                ));
            }
            if self.try_reserve(bytes, true) {
                return Ok(Some(BudgetCharge {
                    budget: Arc::clone(self),
                    bytes,
                }));
            }
            wait = self.changed.wait(wait).map_err(|_| {
                EngineError::DurableSinkUnavailable("delivery budget wait lock poisoned".into())
            })?;
        }
    }

    fn try_reserve(&self, bytes: usize, wait_lock_held: bool) -> bool {
        if self.stopped.load(Ordering::Acquire) {
            return false;
        }
        if self
            .pending_items
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                (pending < self.max_items).then_some(pending + 1)
            })
            .is_err()
        {
            return false;
        }
        if self
            .pending_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                pending
                    .checked_add(bytes)
                    .filter(|&next| next <= self.max_bytes)
            })
            .is_ok()
        {
            true
        } else {
            if wait_lock_held {
                self.pending_items.fetch_sub(1, Ordering::AcqRel);
                self.changed.notify_all();
            } else {
                let _wait = self
                    .wait_lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                self.pending_items.fetch_sub(1, Ordering::AcqRel);
                self.changed.notify_all();
            }
            false
        }
    }

    fn release(&self, bytes: usize) {
        let _wait = self
            .wait_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.pending_items.fetch_sub(1, Ordering::AcqRel);
        self.pending_bytes.fetch_sub(bytes, Ordering::AcqRel);
        self.changed.notify_all();
    }

    fn stop(&self) {
        let _wait = self
            .wait_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.stopped.store(true, Ordering::Release);
        self.changed.notify_all();
    }

    fn snapshot(&self) -> (u64, u64) {
        (
            self.pending_items.load(Ordering::Relaxed) as u64,
            self.pending_bytes.load(Ordering::Relaxed) as u64,
        )
    }
}

#[derive(Debug)]
pub(crate) struct BudgetCharge {
    budget: Arc<Budget>,
    bytes: usize,
}

impl Drop for BudgetCharge {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

#[derive(Debug)]
struct QueuedAppend {
    append: DurableAppend,
    _charge: BudgetCharge,
}

struct QueuedDecision {
    decision: DurableAtomicDecision,
    _charge: BudgetCharge,
}

enum Command {
    Append(QueuedAppend),
    Decision(QueuedDecision),
    LoadCheckpoint {
        topic_partition: TopicPartition,
        response: std::sync::mpsc::SyncSender<EngineResult<DurableSinkCheckpoint>>,
    },
    PendingGroups {
        response: std::sync::mpsc::SyncSender<EngineResult<Vec<DurableAtomicGroup>>>,
    },
    Shutdown,
}

struct PartitionDelivery {
    checkpoint: DurableSinkCheckpoint,
    pending: BTreeMap<u64, QueuedAppend>,
}

impl fmt::Debug for PartitionDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PartitionDelivery")
            .field("checkpoint", &self.checkpoint)
            .field("pending", &self.pending.len())
            .finish()
    }
}

#[derive(Default)]
struct StatsState {
    applied_appends: AtomicU64,
    checkpoint_conflicts: AtomicU64,
    retry_attempts: AtomicU64,
    failed_attempts: AtomicU64,
    dropped_notifications: AtomicU64,
}

struct SharedProgress {
    checkpoints: Mutex<HashMap<TopicPartition, DurableSinkCheckpoint>>,
    changed: Condvar,
    finalized_groups: Mutex<HashSet<DurableAtomicGroup>>,
    decision_changed: Condvar,
    dirty: Mutex<HashSet<TopicPartition>>,
    stopped: AtomicBool,
    started_at: Instant,
    last_checkpoint_micros: AtomicU64,
    stats: StatsState,
}

impl SharedProgress {
    fn checkpoint(&self, topic_partition: TopicPartition) -> Option<DurableSinkCheckpoint> {
        self.checkpoints
            .lock()
            .ok()
            .and_then(|checkpoints| checkpoints.get(&topic_partition).copied())
    }

    fn record_checkpoint(&self, checkpoint: DurableSinkCheckpoint) {
        if let Ok(mut checkpoints) = self.checkpoints.lock() {
            checkpoints.insert(checkpoint.topic_partition, checkpoint);
            self.last_checkpoint_micros
                .store(elapsed_micros(self.started_at), Ordering::Release);
            self.changed.notify_all();
        }
    }

    fn mark_dirty(&self, topic_partition: TopicPartition) {
        self.stats
            .dropped_notifications
            .fetch_add(1, Ordering::Relaxed);
        if let Ok(mut dirty) = self.dirty.lock() {
            dirty.insert(topic_partition);
        }
    }

    fn clear_dirty(&self, topic_partition: TopicPartition) {
        if let Ok(mut dirty) = self.dirty.lock() {
            dirty.remove(&topic_partition);
        }
    }

    fn record_decision(&self, group: DurableAtomicGroup) {
        if let Ok(mut finalized) = self.finalized_groups.lock() {
            finalized.insert(group);
            self.decision_changed.notify_all();
        }
    }
}

pub(crate) struct DurableSinkDispatcher {
    factory: Arc<dyn DurableAppendSinkFactory>,
    options: DurableSinkOptions,
    senders: Vec<Sender<Command>>,
    budget: Arc<Budget>,
    shared: Arc<SharedProgress>,
    workers: Vec<JoinHandle<()>>,
}

impl fmt::Debug for DurableSinkDispatcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableSinkDispatcher")
            .field("options", &self.options)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl DurableSinkDispatcher {
    pub(crate) fn open(
        config: DurableSinkConfig,
        shard_ids: &[ShardId],
        inherited_max_bytes: usize,
        maximum_event_bytes: usize,
    ) -> EngineResult<Self> {
        let DurableSinkConfig { factory, options } = config;
        if options.max_pending_items == 0 {
            return Err(EngineError::InvalidConfig(
                "durable sink max_pending_items must be nonzero".into(),
            ));
        }
        if options.worker_count == 0 {
            return Err(EngineError::InvalidConfig(
                "durable sink worker_count must be nonzero".into(),
            ));
        }
        if options.worker_count > 256 {
            return Err(EngineError::InvalidConfig(
                "durable sink worker_count cannot exceed 256".into(),
            ));
        }
        if options.recovery_timeout.is_zero() {
            return Err(EngineError::InvalidConfig(
                "durable sink recovery_timeout must be nonzero".into(),
            ));
        }
        let max_bytes = options.max_pending_bytes.unwrap_or(inherited_max_bytes);
        if max_bytes == 0 {
            return Err(EngineError::InvalidConfig(
                "durable sink max_pending_bytes must be nonzero".into(),
            ));
        }
        if max_bytes < maximum_event_bytes {
            return Err(EngineError::InvalidConfig(format!(
                "durable sink max_pending_bytes {max_bytes} is below max_batch_bytes {maximum_event_bytes}"
            )));
        }
        if options.atomic_visibility == DurableSinkAtomicVisibility::CommittedOnly
            && !invoke_factory(|| Ok(factory.supports_committed_only()))?
        {
            return Err(EngineError::InvalidConfig(
                "committed-only durable sinks must support durable atomic staging and decisions"
                    .into(),
            ));
        }
        let sinks = shard_ids
            .iter()
            .map(|&shard_id| {
                invoke_factory(|| factory.open_shard(shard_id)).map(|sink| (shard_id, sink))
            })
            .collect::<EngineResult<HashMap<_, _>>>()?;
        let budget = Arc::new(Budget {
            pending_items: AtomicUsize::new(0),
            pending_bytes: AtomicUsize::new(0),
            stopped: AtomicBool::new(false),
            wait_lock: Mutex::new(()),
            changed: Condvar::new(),
            max_items: options.max_pending_items,
            max_bytes,
        });
        let shared = Arc::new(SharedProgress {
            checkpoints: Mutex::new(HashMap::new()),
            changed: Condvar::new(),
            finalized_groups: Mutex::new(HashSet::new()),
            decision_changed: Condvar::new(),
            dirty: Mutex::new(HashSet::new()),
            stopped: AtomicBool::new(false),
            started_at: Instant::now(),
            last_checkpoint_micros: AtomicU64::new(0),
            stats: StatsState::default(),
        });
        let sinks = Arc::new(sinks);
        let mut senders = Vec::with_capacity(options.worker_count);
        let mut workers = Vec::with_capacity(options.worker_count);
        for worker_index in 0..options.worker_count {
            let (sender, receiver) = channel();
            let worker_factory = Arc::clone(&factory);
            let worker_sinks = Arc::clone(&sinks);
            let worker_shared = Arc::clone(&shared);
            let worker = thread::Builder::new()
                .name(format!("shard-stream-durable-sink-{worker_index}"))
                .spawn(move || {
                    run_dispatcher(
                        receiver,
                        worker_factory,
                        worker_sinks,
                        options.atomic_visibility,
                        worker_shared,
                    );
                })
                .map_err(|error| {
                    EngineError::InvalidConfig(format!(
                        "failed to spawn durable sink worker: {error}"
                    ))
                })?;
            senders.push(sender);
            workers.push(worker);
        }
        Ok(Self {
            factory,
            options,
            senders,
            budget,
            shared,
            workers,
        })
    }

    pub(crate) fn validate_append(
        &self,
        payload: &[u8],
        record_count: NonZeroU32,
    ) -> EngineResult<()> {
        invoke_factory(|| self.factory.validate_append(payload, record_count))
    }

    pub(crate) fn reserve(&self, bytes: usize) -> EngineResult<Option<BudgetCharge>> {
        self.budget.reserve(bytes, self.options.lag_policy)
    }

    pub(crate) fn enqueue(&self, append: DurableAppend, charge: Option<BudgetCharge>) {
        let topic_partition = append.topic_partition();
        let Some(charge) = charge else {
            self.shared.mark_dirty(topic_partition);
            return;
        };
        if self
            .sender_for(topic_partition)
            .send(Command::Append(QueuedAppend {
                append,
                _charge: charge,
            }))
            .is_err()
        {
            self.shared.mark_dirty(topic_partition);
        }
    }

    pub(crate) fn enqueue_replay(&self, append: DurableAppend) -> EngineResult<()> {
        let bytes = append.payload.len();
        let charge = self
            .budget
            .reserve(bytes, DurableSinkLagPolicy::Backpressure)?
            .expect("blocking reserve returns a charge");
        self.sender_for(append.topic_partition())
            .send(Command::Append(QueuedAppend {
                append,
                _charge: charge,
            }))
            .map_err(|_| EngineError::DurableSinkUnavailable("delivery worker stopped".into()))
    }

    pub(crate) fn reserve_decision(&self) -> EngineResult<Option<BudgetCharge>> {
        if self.options.atomic_visibility == DurableSinkAtomicVisibility::CommittedOnly {
            self.budget.reserve(0, self.options.lag_policy)
        } else {
            Ok(None)
        }
    }

    pub(crate) fn enqueue_decision(
        &self,
        decision: DurableAtomicDecision,
        charge: Option<BudgetCharge>,
    ) {
        if self.options.atomic_visibility != DurableSinkAtomicVisibility::CommittedOnly {
            return;
        }
        let Some(charge) = charge else {
            self.shared.mark_dirty(decision.group.topic_partition);
            return;
        };
        if self
            .sender_for(decision.group.topic_partition)
            .send(Command::Decision(QueuedDecision {
                decision,
                _charge: charge,
            }))
            .is_err()
        {
            self.shared.mark_dirty(decision.group.topic_partition);
        }
    }

    pub(crate) fn enqueue_replay_decision(
        &self,
        decision: DurableAtomicDecision,
    ) -> EngineResult<()> {
        if self.options.atomic_visibility != DurableSinkAtomicVisibility::CommittedOnly {
            return Ok(());
        }
        let charge = self
            .budget
            .reserve(0, DurableSinkLagPolicy::Backpressure)?
            .expect("blocking reserve returns a charge");
        self.sender_for(decision.group.topic_partition)
            .send(Command::Decision(QueuedDecision {
                decision,
                _charge: charge,
            }))
            .map_err(|_| EngineError::DurableSinkUnavailable("delivery worker stopped".into()))
    }

    pub(crate) fn checkpoint(
        &self,
        topic_partition: TopicPartition,
    ) -> EngineResult<DurableSinkCheckpoint> {
        if let Some(checkpoint) = self.shared.checkpoint(topic_partition) {
            return Ok(checkpoint);
        }
        let (response, receiver) = sync_channel(1);
        self.sender_for(topic_partition)
            .send(Command::LoadCheckpoint {
                topic_partition,
                response,
            })
            .map_err(|_| EngineError::DurableSinkUnavailable("delivery worker stopped".into()))?;
        receiver
            .recv_timeout(self.options.recovery_timeout)
            .map_err(|_| {
                EngineError::DurableSinkUnavailable(
                    "checkpoint load exceeded the configured recovery timeout".into(),
                )
            })?
    }

    pub(crate) fn pending_atomic_groups(&self) -> EngineResult<Vec<DurableAtomicGroup>> {
        if self.options.atomic_visibility != DurableSinkAtomicVisibility::CommittedOnly {
            return Ok(Vec::new());
        }
        let (response, receiver) = sync_channel(1);
        self.senders[0]
            .send(Command::PendingGroups { response })
            .map_err(|_| EngineError::DurableSinkUnavailable("delivery worker stopped".into()))?;
        receiver
            .recv_timeout(self.options.recovery_timeout)
            .map_err(|_| {
                EngineError::DurableSinkUnavailable(
                    "pending group load exceeded the configured recovery timeout".into(),
                )
            })?
    }

    pub(crate) fn wait_for(&self, target: DurableSinkCheckpoint) -> EngineResult<()> {
        let mut checkpoints =
            self.shared.checkpoints.lock().map_err(|_| {
                EngineError::DurableSinkUnavailable("checkpoint lock poisoned".into())
            })?;
        let deadline = Instant::now() + self.options.recovery_timeout;
        loop {
            if checkpoints
                .get(&target.topic_partition)
                .is_some_and(|checkpoint| {
                    checkpoint.next_placement_sequence >= target.next_placement_sequence
                        && checkpoint.next_offset >= target.next_offset
                })
            {
                return Ok(());
            }
            if self.shared.stopped.load(Ordering::Acquire) {
                return Err(EngineError::DurableSinkUnavailable(
                    "delivery worker stopped before reaching checkpoint".into(),
                ));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(EngineError::DurableSinkUnavailable(
                    "sink catch-up exceeded the configured recovery timeout".into(),
                ));
            }
            let (next, timed_out) = self
                .shared
                .changed
                .wait_timeout(checkpoints, remaining)
                .map_err(|_| {
                    EngineError::DurableSinkUnavailable("checkpoint lock poisoned".into())
                })?;
            checkpoints = next;
            if timed_out.timed_out() {
                return Err(EngineError::DurableSinkUnavailable(
                    "sink catch-up exceeded the configured recovery timeout".into(),
                ));
            }
        }
    }

    pub(crate) fn wait_for_decision(&self, group: DurableAtomicGroup) -> EngineResult<()> {
        let mut finalized = self.shared.finalized_groups.lock().map_err(|_| {
            EngineError::DurableSinkUnavailable("atomic decision lock poisoned".into())
        })?;
        let deadline = Instant::now() + self.options.recovery_timeout;
        loop {
            if finalized.contains(&group) {
                return Ok(());
            }
            if self.shared.stopped.load(Ordering::Acquire) {
                return Err(EngineError::DurableSinkUnavailable(
                    "delivery worker stopped before applying atomic decision".into(),
                ));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(EngineError::DurableSinkUnavailable(
                    "atomic decision exceeded the configured recovery timeout".into(),
                ));
            }
            let (next, timed_out) = self
                .shared
                .decision_changed
                .wait_timeout(finalized, remaining)
                .map_err(|_| {
                    EngineError::DurableSinkUnavailable("atomic decision lock poisoned".into())
                })?;
            finalized = next;
            if timed_out.timed_out() {
                return Err(EngineError::DurableSinkUnavailable(
                    "atomic decision exceeded the configured recovery timeout".into(),
                ));
            }
        }
    }

    pub(crate) fn clear_dirty(&self, topic_partition: TopicPartition) {
        self.shared.clear_dirty(topic_partition);
    }

    pub(crate) const fn recovery_mode(&self) -> DurableSinkRecoveryMode {
        self.options.recovery_mode
    }

    pub(crate) fn stats(&self) -> DurableSinkStats {
        let (pending_items, pending_bytes) = self.budget.snapshot();
        let dirty_partitions = self
            .shared
            .dirty
            .lock()
            .map_or(0, |dirty| dirty.len() as u64);
        let checkpoint_age_ms = if pending_items > 0 || dirty_partitions > 0 {
            elapsed_micros(self.shared.started_at)
                .saturating_sub(self.shared.last_checkpoint_micros.load(Ordering::Acquire))
                / 1_000
        } else {
            0
        };
        DurableSinkStats {
            pending_items,
            pending_bytes,
            checkpoint_age_ms,
            applied_appends: self.shared.stats.applied_appends.load(Ordering::Relaxed),
            checkpoint_conflicts: self
                .shared
                .stats
                .checkpoint_conflicts
                .load(Ordering::Relaxed),
            retry_attempts: self.shared.stats.retry_attempts.load(Ordering::Relaxed),
            failed_attempts: self.shared.stats.failed_attempts.load(Ordering::Relaxed),
            dropped_notifications: self
                .shared
                .stats
                .dropped_notifications
                .load(Ordering::Relaxed),
            dirty_partitions,
        }
    }

    fn sender_for(&self, topic_partition: TopicPartition) -> &Sender<Command> {
        let topic = topic_partition.topic_id.get();
        let folded_topic = (topic as u64) ^ ((topic >> 64) as u64);
        let partition = u64::from(topic_partition.partition_id.get());
        let hash = folded_topic ^ partition.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        &self.senders[hash as usize % self.senders.len()]
    }
}

impl Drop for DurableSinkDispatcher {
    fn drop(&mut self) {
        self.budget.stop();
        self.shared.stopped.store(true, Ordering::Release);
        self.shared.changed.notify_all();
        self.shared.decision_changed.notify_all();
        for sender in &self.senders {
            let _ = sender.send(Command::Shutdown);
        }
        // A third-party callback may be permanently blocked. Detaching keeps
        // engine destruction bounded; WAL replay recovers uncheckpointed work.
        self.workers.clear();
    }
}

fn run_dispatcher(
    receiver: Receiver<Command>,
    factory: Arc<dyn DurableAppendSinkFactory>,
    sinks: Arc<HashMap<ShardId, Arc<dyn DurableAppendSink>>>,
    atomic_visibility: DurableSinkAtomicVisibility,
    shared: Arc<SharedProgress>,
) {
    let mut partitions: HashMap<TopicPartition, PartitionDelivery> = HashMap::new();
    let mut decisions = VecDeque::new();
    let mut retry_delay = RETRY_MIN;
    let mut next_retry = Instant::now();
    loop {
        let timeout = next_retry.saturating_duration_since(Instant::now());
        match receiver.recv_timeout(timeout) {
            Ok(Command::Append(mut queued)) => {
                if atomic_visibility == DurableSinkAtomicVisibility::CommittedOnly
                    && queued.append.atomic_group.is_some()
                {
                    queued.append.delivery = DurableAppendDelivery::Stage;
                }
                let topic_partition = queued.append.topic_partition();
                match ensure_partition(&factory, &mut partitions, topic_partition, &shared) {
                    Ok(partition) => {
                        let sequence = queued.append.reservation.placement.sequence.get();
                        if sequence < partition.checkpoint.next_placement_sequence.get() {
                            continue;
                        }
                        match partition.pending.entry(sequence) {
                            std::collections::btree_map::Entry::Vacant(entry) => {
                                entry.insert(queued);
                            }
                            std::collections::btree_map::Entry::Occupied(entry) => {
                                if entry.get().append.event_id != queued.append.event_id {
                                    shared.stats.failed_attempts.fetch_add(1, Ordering::Relaxed);
                                    shared.mark_dirty(topic_partition);
                                }
                            }
                        }
                    }
                    Err(_) => {
                        shared.stats.failed_attempts.fetch_add(1, Ordering::Relaxed);
                        shared.mark_dirty(topic_partition);
                    }
                }
            }
            Ok(Command::Decision(decision)) => decisions.push_back(decision),
            Ok(Command::LoadCheckpoint {
                topic_partition,
                response,
            }) => {
                let result = ensure_partition(&factory, &mut partitions, topic_partition, &shared)
                    .map(|partition| partition.checkpoint);
                let _ = response.send(result);
            }
            Ok(Command::PendingGroups { response }) => {
                let result = invoke_factory(|| factory.pending_atomic_groups());
                let _ = response.send(result);
            }
            Ok(Command::Shutdown) | Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {}
        }

        if Instant::now() < next_retry {
            continue;
        }
        let mut failed = process_decisions(&factory, &mut decisions, &shared);
        failed |= process_ready(&sinks, &mut partitions, &shared);
        if failed {
            shared.stats.retry_attempts.fetch_add(1, Ordering::Relaxed);
            next_retry = Instant::now() + retry_delay;
            retry_delay = retry_delay.saturating_mul(2).min(RETRY_MAX);
        } else {
            retry_delay = RETRY_MIN;
            next_retry = Instant::now() + RETRY_MIN;
        }
    }
    shared.stopped.store(true, Ordering::Release);
    shared.changed.notify_all();
    shared.decision_changed.notify_all();
}

fn ensure_partition<'a>(
    factory: &Arc<dyn DurableAppendSinkFactory>,
    partitions: &'a mut HashMap<TopicPartition, PartitionDelivery>,
    topic_partition: TopicPartition,
    shared: &SharedProgress,
) -> EngineResult<&'a mut PartitionDelivery> {
    match partitions.entry(topic_partition) {
        std::collections::hash_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
        std::collections::hash_map::Entry::Vacant(entry) => {
            let checkpoint = invoke_factory(|| factory.load_checkpoint(topic_partition))?
                .unwrap_or_else(|| DurableSinkCheckpoint::initial(topic_partition));
            validate_checkpoint(topic_partition, checkpoint)?;
            shared.record_checkpoint(checkpoint);
            Ok(entry.insert(PartitionDelivery {
                checkpoint,
                pending: BTreeMap::new(),
            }))
        }
    }
}

fn process_ready(
    sinks: &HashMap<ShardId, Arc<dyn DurableAppendSink>>,
    partitions: &mut HashMap<TopicPartition, PartitionDelivery>,
    shared: &SharedProgress,
) -> bool {
    let mut failed = false;
    for partition in partitions.values_mut() {
        loop {
            let sequence = partition.checkpoint.next_placement_sequence.get();
            let Some(queued) = partition.pending.remove(&sequence) else {
                break;
            };
            let next = match partition.checkpoint.after(queued.append.reservation) {
                Ok(next) => next,
                Err(_) => {
                    shared.stats.failed_attempts.fetch_add(1, Ordering::Relaxed);
                    failed = true;
                    partition.pending.insert(sequence, queued);
                    break;
                }
            };
            let Some(sink) = sinks.get(&queued.append.physical_shard_id) else {
                shared.stats.failed_attempts.fetch_add(1, Ordering::Relaxed);
                failed = true;
                partition.pending.insert(sequence, queued);
                break;
            };
            let result = catch_unwind(AssertUnwindSafe(|| {
                sink.apply(
                    partition.checkpoint,
                    std::slice::from_ref(&queued.append),
                    next,
                )
            }));
            match result {
                Ok(Ok(DurableSinkApply::Applied)) => {
                    partition.checkpoint = next;
                    shared.record_checkpoint(next);
                    shared.stats.applied_appends.fetch_add(1, Ordering::Relaxed);
                }
                Ok(Ok(DurableSinkApply::CheckpointConflict(actual))) => {
                    if validate_checkpoint(partition.checkpoint.topic_partition, actual).is_err()
                        || actual.next_placement_sequence
                            < partition.checkpoint.next_placement_sequence
                        || actual.next_offset < partition.checkpoint.next_offset
                        || (actual.next_placement_sequence < next.next_placement_sequence
                            || actual.next_offset < next.next_offset)
                    {
                        shared.stats.failed_attempts.fetch_add(1, Ordering::Relaxed);
                        failed = true;
                        partition.pending.insert(sequence, queued);
                        break;
                    }
                    partition.checkpoint = actual;
                    partition.pending.retain(|&pending_sequence, _| {
                        pending_sequence >= actual.next_placement_sequence.get()
                    });
                    shared.record_checkpoint(actual);
                    shared
                        .stats
                        .checkpoint_conflicts
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(Err(_)) | Err(_) => {
                    shared.stats.failed_attempts.fetch_add(1, Ordering::Relaxed);
                    failed = true;
                    partition.pending.insert(sequence, queued);
                    break;
                }
            }
        }
    }
    failed
}

fn process_decisions(
    factory: &Arc<dyn DurableAppendSinkFactory>,
    decisions: &mut VecDeque<QueuedDecision>,
    shared: &SharedProgress,
) -> bool {
    let mut failed = false;
    let count = decisions.len();
    for _ in 0..count {
        let Some(decision) = decisions.pop_front() else {
            break;
        };
        match invoke_factory(|| factory.finalize_atomic_group(decision.decision)) {
            Ok(()) => shared.record_decision(decision.decision.group),
            Err(_) => {
                shared.stats.failed_attempts.fetch_add(1, Ordering::Relaxed);
                decisions.push_back(decision);
                failed = true;
            }
        }
    }
    failed
}

fn invoke_factory<T>(operation: impl FnOnce() -> EngineResult<T>) -> EngineResult<T> {
    catch_unwind(AssertUnwindSafe(operation))
        .map_err(|_| EngineError::DurableSinkUnavailable("durable sink callback panicked".into()))?
}

fn validate_checkpoint(
    topic_partition: TopicPartition,
    checkpoint: DurableSinkCheckpoint,
) -> EngineResult<()> {
    if checkpoint.topic_partition != topic_partition {
        return Err(EngineError::DurableSinkCheckpoint(format!(
            "checkpoint belongs to {}/{}, expected {}/{}",
            checkpoint.topic_partition.topic_id,
            checkpoint.topic_partition.partition_id,
            topic_partition.topic_id,
            topic_partition.partition_id
        )));
    }
    Ok(())
}

fn elapsed_micros(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_micros()).unwrap_or(u64::MAX)
}
