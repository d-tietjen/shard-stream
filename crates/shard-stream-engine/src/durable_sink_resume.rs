//! Actual bounded callback commands on the existing native sink workers.

use super::*;
use crate::engine::native_resume::{NativeResumeObservation, NativeResumeRefusal};
use std::mem::size_of;
use std::sync::mpsc::SyncSender;

#[derive(Debug)]
pub(crate) enum Value {
    Checkpoint(Option<DurableSinkCheckpoint>),
    Pending(Vec<DurableAtomicGroup>),
    Applied(DurableSinkApply),
    Finalized,
}

/// Actual callback result and the separate native cache-publication result.
/// Neither is a currentness or serving witness. A callback effect can succeed
/// while later native cache publication refuses; no rollback is claimed.
#[derive(Debug)]
pub struct NativeResumeSinkOutcome {
    provider_entered: bool,
    pub(crate) callback: NativeResumeObservation<Value>,
    pub(crate) progress: Option<NativeResumeObservation<()>>,
}
impl NativeResumeSinkOutcome {
    /// Whether this command actually entered its strict provider callback.
    /// AllDurable no-op decision/pending paths report false, just as the
    /// ordinary native dispatcher skips those callbacks in that mode.
    pub fn callback_entered(&self) -> bool {
        self.provider_entered
    }
    /// Actual checkpoint result, including legitimate first-use absence.
    pub fn checkpoint(&self) -> Option<Option<DurableSinkCheckpoint>> {
        match &self.callback.result {
            Ok(Value::Checkpoint(value)) => Some(*value),
            _ => None,
        }
    }
    /// Every actual pending group returned by the bounded provider.
    pub fn pending_groups(&self) -> Option<&[DurableAtomicGroup]> {
        match &self.callback.result {
            Ok(Value::Pending(groups)) => Some(groups),
            _ => None,
        }
    }
    /// Actual transactional apply result, even when native publication is late.
    pub fn apply_result(&self) -> Option<DurableSinkApply> {
        match &self.callback.result {
            Ok(Value::Applied(value)) => Some(*value),
            _ => None,
        }
    }
    /// Selected native decision path succeeded, independently of E.
    /// Check callback_entered() to distinguish an AllDurable no-op.
    pub fn finalized(&self) -> bool {
        matches!(self.callback.result, Ok(Value::Finalized))
    }
    /// Original typed provider/queue/panic refusal.
    pub fn callback_error(&self) -> Option<&NativeResumeCallbackError> {
        self.callback.result.as_ref().err()
    }
    /// Same work refused after the actual provider result was retained.
    pub fn callback_refusal(&self) -> Option<NativeResumeRefusal> {
        self.callback.refusal
    }
    /// Actual bounded native cache publication, when that phase was entered.
    pub fn progress(&self) -> Option<&NativeResumeObservation<()>> {
        self.progress.as_ref()
    }
    pub(crate) fn accepted(&self) -> bool {
        self.callback.result.is_ok()
            && self.callback.refusal.is_none()
            && self
                .progress
                .as_ref()
                .is_none_or(|p| p.result.is_ok() && p.refusal.is_none())
    }
    fn failed(error: NativeResumeCallbackError, work: &NativeResumeWork) -> Self {
        Self {
            provider_entered: false,
            callback: NativeResumeObservation::completed(Err(error), work),
            progress: None,
        }
    }
}

enum Action {
    Checkpoint(TopicPartition),
    Pending,
    Apply {
        expected: DurableSinkCheckpoint,
        append: DurableAppend,
        next: DurableSinkCheckpoint,
    },
    Decision(DurableAtomicDecision),
}
struct Reply(Option<SyncSender<NativeResumeSinkOutcome>>);
impl Reply {
    fn complete(mut self, outcome: NativeResumeSinkOutcome) {
        if let Some(reply) = self.0.take() {
            let _ = reply.send(outcome);
        }
    }
}
impl Drop for Reply {
    fn drop(&mut self) {
        if let Some(reply) = self.0.take() {
            let _ = reply.send(NativeResumeSinkOutcome {
                provider_entered: false,
                callback: NativeResumeObservation {
                    result: Err(NativeResumeCallbackError::worker_stopped(
                        "native bounded sink command",
                    )),
                    refusal: None,
                },
                progress: None,
            });
        }
    }
}

struct EnteredJob {
    receiver: Option<Receiver<NativeResumeSinkOutcome>>,
    work: NativeResumeWork,
}
impl EnteredJob {
    fn receive(mut self) -> NativeResumeSinkOutcome {
        let observed = match self.receiver.as_ref() {
            Some(receiver) => receiver.recv(),
            None => {
                return NativeResumeSinkOutcome::failed(
                    NativeResumeCallbackError::invalid_state(
                        "bounded sink completion was already consumed",
                    ),
                    &self.work,
                );
            }
        };
        self.receiver.take();
        match observed {
            Ok(outcome) => outcome,
            Err(_) => NativeResumeSinkOutcome::failed(
                NativeResumeCallbackError::worker_stopped("native bounded sink"),
                &self.work,
            ),
        }
    }
}
impl Drop for EnteredJob {
    fn drop(&mut self) {
        if let Some(receiver) = self.receiver.as_ref() {
            // An unwinding owning caller still physically drains this actual
            // entered command while the outside EngineHandle remains retained.
            self.work.deny();
            let _ = receiver.recv();
        }
    }
}

pub(super) struct BoundedCommand {
    action: Action,
    work: NativeResumeWork,
    charge: BudgetCharge,
    // Last: real inputs, callback handles and lag charge settle before reply.
    reply: Reply,
}

impl DurableSinkDispatcher {
    pub(crate) fn resume_checkpoint(
        &self,
        partition: TopicPartition,
        work: &NativeResumeWork,
    ) -> NativeResumeSinkOutcome {
        self.resume_command(partition, Action::Checkpoint(partition), 0, work)
    }
    pub(crate) fn resume_pending(&self, work: &NativeResumeWork) -> NativeResumeSinkOutcome {
        if self.options.atomic_visibility != DurableSinkAtomicVisibility::CommittedOnly {
            return NativeResumeSinkOutcome {
                provider_entered: false,
                callback: NativeResumeObservation::completed(Ok(Value::Pending(Vec::new())), work),
                progress: None,
            };
        }
        // Same actual first dispatcher lane as ordinary pending_atomic_groups.
        self.resume_command_on(&self.senders[0], Action::Pending, 0, work)
    }
    pub(crate) fn resume_apply(
        &self,
        expected: DurableSinkCheckpoint,
        mut append: DurableAppend,
        next: DurableSinkCheckpoint,
        work: &NativeResumeWork,
    ) -> NativeResumeSinkOutcome {
        if self.options.atomic_visibility == DurableSinkAtomicVisibility::CommittedOnly
            && append.atomic_group.is_some()
        {
            append.delivery = DurableAppendDelivery::Stage;
        }
        let bytes = append.payload.len();
        self.resume_command(
            append.topic_partition(),
            Action::Apply {
                expected,
                append,
                next,
            },
            bytes,
            work,
        )
    }
    pub(crate) fn resume_decision(
        &self,
        decision: DurableAtomicDecision,
        work: &NativeResumeWork,
    ) -> NativeResumeSinkOutcome {
        if self.options.atomic_visibility != DurableSinkAtomicVisibility::CommittedOnly {
            return NativeResumeSinkOutcome {
                provider_entered: false,
                callback: NativeResumeObservation::completed(Ok(Value::Finalized), work),
                progress: None,
            };
        }
        self.resume_command(
            decision.group.topic_partition,
            Action::Decision(decision),
            0,
            work,
        )
    }
    fn resume_command(
        &self,
        partition: TopicPartition,
        action: Action,
        bytes: usize,
        work: &NativeResumeWork,
    ) -> NativeResumeSinkOutcome {
        self.resume_command_on(self.sender_for(partition), action, bytes, work)
    }
    fn resume_command_on(
        &self,
        queue: &Sender<Command>,
        action: Action,
        bytes: usize,
        work: &NativeResumeWork,
    ) -> NativeResumeSinkOutcome {
        let values = size_of::<BoundedCommand>()
            + size_of::<EnteredJob>()
            + size_of::<NativeResumeSinkOutcome>()
            + size_of::<(
                SyncSender<NativeResumeSinkOutcome>,
                Receiver<NativeResumeSinkOutcome>,
            )>();
        // Shared validate_checkpoint formats only two bounded native identities.
        let diagnostic = "checkpoint belongs to /, expected /".len()
            + 2 * (u128::MAX.ilog10() as usize + 1 + u32::MAX.ilog10() as usize + 1);
        if let Err(error) = work.reserve_command(values + diagnostic, diagnostic, 6, 0) {
            return NativeResumeSinkOutcome::failed(
                NativeResumeCallbackError::refused(error),
                work,
            );
        }
        if let Err(error) = work.check() {
            return NativeResumeSinkOutcome::failed(
                NativeResumeCallbackError::refused(error),
                work,
            );
        }
        // The existing native lag slots bound actual queued commands globally.
        // Strict replay refuses capacity; it never waits on the old Condvar or
        // invokes the ordinary unbounded callback as a fallback.
        if !self.budget.try_reserve(bytes, false) {
            return NativeResumeSinkOutcome::failed(
                NativeResumeCallbackError::queue_full("native bounded sink"),
                work,
            );
        }
        let charge = BudgetCharge {
            budget: Arc::clone(&self.budget),
            bytes,
        };
        if let Err(error) = work.check() {
            return NativeResumeSinkOutcome::failed(
                NativeResumeCallbackError::refused(error),
                work,
            );
        }
        let (reply, receiver) = sync_channel(1);
        if let Err(error) = work.check() {
            return NativeResumeSinkOutcome::failed(
                NativeResumeCallbackError::refused(error),
                work,
            );
        }
        let sent = queue.send(Command::BoundedResume(BoundedCommand {
            action,
            work: work.clone(),
            charge,
            reply: Reply(Some(reply)),
        }));
        if sent.is_err() {
            return NativeResumeSinkOutcome::failed(
                NativeResumeCallbackError::worker_stopped("native bounded sink"),
                work,
            );
        }
        // This actual entered job is drained even after original E. The worker
        // checks the SAME work before its callback, and replies only after its
        // real inputs/charges have settled. No relative receive timeout.
        EnteredJob {
            receiver: Some(receiver),
            work: work.clone(),
        }
        .receive()
    }
}

fn refused(error: NativeResumeRefusal) -> NativeResumeCallbackError {
    NativeResumeCallbackError::refused(error)
}
fn add(a: usize, b: usize) -> NativeResumeCallbackResult<usize> {
    a.checked_add(b)
        .ok_or_else(|| refused(NativeResumeRefusal::Capacity))
}
fn mul(a: usize, b: usize) -> NativeResumeCallbackResult<usize> {
    a.checked_mul(b)
        .ok_or_else(|| refused(NativeResumeRefusal::Capacity))
}

// Logical native element requests, not an allocator/hash-table/RSS claim. Pay
// the entire existing element population plus the added slot before a possible
// rehash. The same cumulative entry/byte/work ceilings bound this request.
fn reserve_map<K: Eq + std::hash::Hash, V>(
    map: &mut HashMap<K, V>,
    work: &NativeResumeWork,
) -> NativeResumeCallbackResult<()> {
    let count = add(map.len(), 1)?;
    let bytes = mul(count, size_of::<(K, V)>())?;
    work.reserve_values(bytes, 0, count, bytes)
        .map_err(refused)?;
    work.check().map_err(refused)?;
    let observed = map
        .try_reserve(1)
        .map_err(NativeResumeCallbackError::from_allocation);
    let after = work.check();
    match (observed, after) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(refused(error)),
        (Ok(()), Ok(())) => Ok(()),
    }
}
fn publish_checkpoint(
    partitions: &mut HashMap<TopicPartition, PartitionDelivery>,
    shared: &SharedProgress,
    checkpoint: DurableSinkCheckpoint,
    work: &NativeResumeWork,
) -> NativeResumeCallbackResult<()> {
    work.check().map_err(refused)?;
    work.reserve_table_probe::<(TopicPartition, PartitionDelivery)>(
        partitions.capacity(),
        "sink.partitions.get",
    )
    .map_err(refused)?;
    if let Some(previous) = partitions.get(&checkpoint.topic_partition) {
        if checkpoint.next_placement_sequence < previous.checkpoint.next_placement_sequence
            || checkpoint.next_offset < previous.checkpoint.next_offset
        {
            return Err(NativeResumeCallbackError::invalid_state(
                "native sink checkpoint would regress partition cache",
            ));
        }
    } else {
        reserve_map(partitions, work)?;
    }
    work.check().map_err(refused)?;
    work.reserve_table_probe::<(TopicPartition, PartitionDelivery)>(
        partitions.capacity(),
        "sink.partitions.entry",
    )
    .map_err(refused)?;
    let partition = partitions
        .entry(checkpoint.topic_partition)
        .or_insert_with(|| PartitionDelivery {
            checkpoint,
            pending: BTreeMap::new(),
            retry_at: None,
            retry_delay: RETRY_MIN,
        });
    partition.checkpoint = checkpoint;
    partition.retry_at = None;
    partition.retry_delay = RETRY_MIN;
    work.check().map_err(refused)?;
    loop {
        work.reserve_values(0, 0, 1, 1).map_err(refused)?;
        let stale = partition
            .pending
            .first_key_value()
            .is_some_and(|(&sequence, _)| sequence < checkpoint.next_placement_sequence.get());
        if !stale {
            break;
        }
        // Pop only the ordered stale prefix; no unbounded retain scan or new
        // callback enters after a late preceding cleanup.
        drop(partition.pending.pop_first());
        work.check().map_err(refused)?;
    }
    work.check().map_err(refused)?;
    let mut checkpoints = shared
        .checkpoints
        .lock()
        .map_err(|_| refused(NativeResumeRefusal::Poisoned))?;
    work.check().map_err(refused)?;
    work.reserve_table_probe::<(TopicPartition, DurableSinkCheckpoint)>(
        checkpoints.capacity(),
        "sink.checkpoints.get",
    )
    .map_err(refused)?;
    if let Some(previous) = checkpoints.get(&checkpoint.topic_partition) {
        if checkpoint.next_placement_sequence < previous.next_placement_sequence
            || checkpoint.next_offset < previous.next_offset
        {
            return Err(NativeResumeCallbackError::invalid_state(
                "native sink checkpoint would regress shared cache",
            ));
        }
    } else {
        reserve_map(&mut checkpoints, work)?;
    }
    work.check().map_err(refused)?;
    work.reserve_table_probe::<(TopicPartition, DurableSinkCheckpoint)>(
        checkpoints.capacity(),
        "sink.checkpoints.insert",
    )
    .map_err(refused)?;
    checkpoints.insert(checkpoint.topic_partition, checkpoint);
    shared
        .last_checkpoint_micros
        .store(elapsed_micros(shared.started_at), Ordering::Release);
    shared.changed.notify_all();
    work.check().map_err(refused)
}
fn publish_decision(
    shared: &SharedProgress,
    group: DurableAtomicGroup,
    work: &NativeResumeWork,
) -> NativeResumeCallbackResult<()> {
    work.check().map_err(refused)?;
    let mut groups = shared
        .finalized_groups
        .lock()
        .map_err(|_| refused(NativeResumeRefusal::Poisoned))?;
    work.check().map_err(refused)?;
    work.reserve_table_probe::<DurableAtomicGroup>(groups.capacity(), "sink.groups.contains")
        .map_err(refused)?;
    if !groups.contains(&group) {
        let count = add(groups.len(), 1)?;
        let bytes = mul(count, size_of::<DurableAtomicGroup>())?;
        work.reserve_values(bytes, 0, count, bytes)
            .map_err(refused)?;
        work.check().map_err(refused)?;
        let observed = groups
            .try_reserve(1)
            .map_err(NativeResumeCallbackError::from_allocation);
        let after = work.check();
        match (observed, after) {
            (Err(error), _) => return Err(error),
            (Ok(()), Err(error)) => return Err(refused(error)),
            (Ok(()), Ok(())) => {}
        }
    }
    work.check().map_err(refused)?;
    work.reserve_table_probe::<DurableAtomicGroup>(groups.capacity(), "sink.groups.insert")
        .map_err(refused)?;
    groups.insert(group);
    shared.decision_changed.notify_all();
    work.check().map_err(refused)
}

pub(super) fn execute(
    factory: &Arc<dyn DurableAppendSinkFactory>,
    sinks: &HashMap<ShardId, Arc<dyn DurableAppendSink>>,
    partitions: &mut HashMap<TopicPartition, PartitionDelivery>,
    shared: &SharedProgress,
    command: BoundedCommand,
) {
    let mut provider_entered = false;
    let result = (|| {
        command.work.check().map_err(refused)?;
        command.work.reserve_callback().map_err(refused)?;
        command.work.check().map_err(refused)?;
        match &command.action {
            Action::Checkpoint(partition) => {
                command.work.check().map_err(refused)?;
                provider_entered = true;
                factory
                    .load_checkpoint_until(*partition, &command.work)
                    .map(Value::Checkpoint)
            }
            Action::Pending => {
                command.work.check().map_err(refused)?;
                provider_entered = true;
                factory
                    .pending_atomic_groups_until(&command.work)
                    .map(Value::Pending)
            }
            Action::Apply {
                expected,
                append,
                next,
            } => {
                command
                    .work
                    .reserve_table_probe::<(ShardId, Arc<dyn DurableAppendSink>)>(
                        sinks.capacity(),
                        "sink.sinks.get",
                    )
                    .map_err(refused)?;
                let sink = sinks.get(&append.physical_shard_id).ok_or_else(|| {
                    NativeResumeCallbackError::invalid_state("native sink shard absent")
                })?;
                command.work.check().map_err(refused)?;
                provider_entered = true;
                sink.apply_until(
                    *expected,
                    std::slice::from_ref(append),
                    *next,
                    &command.work,
                )
                .map(Value::Applied)
            }
            Action::Decision(decision) => {
                command.work.check().map_err(refused)?;
                provider_entered = true;
                factory
                    .finalize_atomic_group_until(*decision, &command.work)
                    .map(|()| Value::Finalized)
            }
        }
    });
    let result = match catch_unwind(AssertUnwindSafe(result)) {
        Ok(result) => result,
        Err(payload) => Err(NativeResumeCallbackError::from_panic(payload)),
    };
    // Preserve the actual callback result BEFORE the post-E observation or any
    // cache operation. A real committed effect is never erased by a late clock.
    let mut outcome = NativeResumeSinkOutcome {
        provider_entered,
        callback: NativeResumeObservation::completed(result, &command.work),
        progress: None,
    };
    if outcome.callback.result.is_ok() && outcome.callback.refusal.is_none() {
        let progress = catch_unwind(AssertUnwindSafe(|| {
            match (&command.action, &outcome.callback.result) {
                (Action::Checkpoint(partition), Ok(Value::Checkpoint(value))) => {
                    let checkpoint =
                        value.unwrap_or_else(|| DurableSinkCheckpoint::initial(*partition));
                    validate_checkpoint(*partition, checkpoint)
                        .map_err(NativeResumeCallbackError::from_engine)?;
                    publish_checkpoint(partitions, shared, checkpoint, &command.work)
                }
                (Action::Apply { expected, next, .. }, Ok(Value::Applied(value))) => {
                    let checkpoint = match value {
                        DurableSinkApply::Applied => *next,
                        DurableSinkApply::CheckpointConflict(actual) => {
                            if checkpoint_conflict_invalid(*expected, *next, *actual) {
                                return Err(NativeResumeCallbackError::invalid_state(
                                    "native sink checkpoint conflict did not advance",
                                ));
                            }
                            *actual
                        }
                    };
                    publish_checkpoint(partitions, shared, checkpoint, &command.work)?;
                    match value {
                        DurableSinkApply::Applied => {
                            shared.stats.applied_appends.fetch_add(1, Ordering::Relaxed);
                        }
                        DurableSinkApply::CheckpointConflict(_) => {
                            shared
                                .stats
                                .checkpoint_conflicts
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Ok(())
                }
                (Action::Decision(decision), Ok(Value::Finalized)) => {
                    publish_decision(shared, decision.group, &command.work)
                }
                (Action::Pending, Ok(Value::Pending(_))) => return Ok(()),
                _ => Err(NativeResumeCallbackError::invalid_state(
                    "native sink callback result mismatch",
                )),
            }
        }));
        let progress = match progress {
            Ok(result) => result,
            Err(payload) => Err(NativeResumeCallbackError::from_panic(payload)),
        };
        outcome.progress = Some(NativeResumeObservation::completed(progress, &command.work));
    }
    let BoundedCommand {
        action,
        work,
        charge,
        reply,
    } = command;
    drop(action);
    drop(charge);
    drop(work);
    reply.complete(outcome);
}
