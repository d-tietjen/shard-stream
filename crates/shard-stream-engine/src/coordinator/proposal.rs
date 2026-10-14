//! Pure append preparation and serialized commit of its exact coordinates.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use shard_stream_core::ReservationProposal;

use super::{
    AtomicGroupIdentity, AtomicGroupRuntime, AtomicGroupState, BatchRuntime, BatchStatus,
    DIGEST_XXH3_128, Durability, EngineError, EngineResult, LeaderEpoch, NonZeroU32,
    PartitionReplicationPolicy, PartitionState, ProducerCursor, ProducerIdentity, ProducerSequence,
    ReserveOutcome, TopicPartition, check_atomic_group, check_producer,
};

pub(super) const RPC_TIMEOUT: Duration = Duration::from_secs(1);
const PENDING: u8 = 0;
const CANCELLED: u8 = 1;
const COMMITTING: u8 = 2;

/// One queued operation's cancellation arbitration; it owns no reservation.
#[derive(Debug, Default)]
pub(crate) struct CommitControl(AtomicU8);

impl CommitControl {
    /// Cancellation succeeds only while no commit has acquired ownership.
    pub(crate) fn cancel(&self) -> bool {
        self.0
            .compare_exchange(PENDING, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            || self.0.load(Ordering::Acquire) == CANCELLED
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire) == CANCELLED
    }

    fn begin_commit(&self) -> EngineResult<()> {
        self.0
            .compare_exchange(PENDING, COMMITTING, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| {
                EngineError::Fenced(
                    "reservation proposal was cancelled or already committed".into(),
                )
            })
    }
}

/// Allocation knowledge from one actual coordinator response or cancellation.
pub(crate) enum CommitOutcome {
    Settled(ReserveOutcome),
    /// An actual returned precommit refusal or successful Pending cancellation.
    Unallocated(EngineError),
    /// Commit won, but its reply disappeared before coordinates were confirmed.
    Unknown(EngineError),
}

fn returned_commit(result: EngineResult<ReserveOutcome>) -> CommitOutcome {
    match result {
        Ok(outcome) => CommitOutcome::Settled(outcome),
        // commit() returns errors only before sequencer/producer/group mutation.
        Err(error) => CommitOutcome::Unallocated(error),
    }
}

fn missing_commit(control: &CommitControl) -> CommitOutcome {
    let error = super::coordinator_stopped("reservation commit");
    if control.cancel() {
        CommitOutcome::Unallocated(error)
    } else {
        CommitOutcome::Unknown(error)
    }
}

pub(super) fn await_commit(
    receiver: Receiver<EngineResult<ReserveOutcome>>,
    control: &CommitControl,
) -> CommitOutcome {
    match receiver.recv_timeout(RPC_TIMEOUT) {
        Ok(result) => returned_commit(result),
        Err(RecvTimeoutError::Disconnected) => missing_commit(control),
        Err(RecvTimeoutError::Timeout) if control.cancel() => CommitOutcome::Unallocated(
            EngineError::Fenced("queued reservation commit was cancelled before allocation".into()),
        ),
        Err(RecvTimeoutError::Timeout) => {
            // Commit owns arbitration. The physical worker retains permission
            // and capacity until the response arrives or its sender is destroyed;
            // a caller deadline cannot detach a still-running allocation.
            match receiver.recv() {
                Ok(result) => returned_commit(result),
                Err(_) => missing_commit(control),
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AppendIntent {
    pub(crate) topic_partition: TopicPartition,
    pub(crate) request_id: u128,
    pub(crate) producer: Option<ProducerIdentity>,
    pub(crate) atomic_group: Option<AtomicGroupIdentity>,
    pub(crate) record_count: NonZeroU32,
    pub(crate) payload_bytes: u64,
    pub(crate) payload_digest: [u8; 16],
    pub(crate) durability: Durability,
    pub(crate) admitted_leader_epoch: Option<LeaderEpoch>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AtomicSnapshot {
    state: AtomicGroupState,
    next_sequence: u64,
    payload_bytes: u64,
    record_count: u64,
    chunk_count: usize,
    last_batch: Option<u128>,
}

impl AtomicSnapshot {
    fn from_group(group: &AtomicGroupRuntime) -> Self {
        Self {
            state: group.state,
            next_sequence: group.next_sequence,
            payload_bytes: group.payload_bytes,
            record_count: group.record_count,
            chunk_count: group.batch_ids.len(),
            last_batch: group.batch_ids.last().copied(),
        }
    }
}

/// Fixed-size input and relevant state, without a mutable partition copy.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PreparedAppend {
    pub(crate) intent: AppendIntent,
    pub(crate) proposal: ReservationProposal,
    replication_policy: PartitionReplicationPolicy,
    producer_before: Option<ProducerCursor>,
    producer_after: Option<ProducerCursor>,
    atomic_before: Option<AtomicSnapshot>,
    atomic_after: Option<AtomicSnapshot>,
    dense_next_key: u128,
    dense_len: usize,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum PrepareOutcome {
    Duplicate(ReserveOutcome),
    New(PreparedAppend),
}

fn duplicate(state: &PartitionState, intent: AppendIntent) -> EngineResult<Option<ReserveOutcome>> {
    if let Some((response, durable_replicas, object_durable)) = check_producer(
        state,
        intent.request_id,
        intent.producer,
        intent.record_count,
        intent.payload_digest,
        intent.durability,
    )? {
        return Ok(Some(ReserveOutcome::Duplicate {
            response,
            durable_replicas,
            object_durable,
            replication_policy: state.replication_policy,
        }));
    }
    match intent.atomic_group {
        Some(identity) => check_atomic_group(
            state,
            intent.request_id,
            identity,
            intent.record_count,
            intent.payload_bytes,
            intent.payload_digest,
        ),
        None => Ok(None),
    }
}

pub(super) fn prepare(
    state: &PartitionState,
    intent: AppendIntent,
) -> EngineResult<PrepareOutcome> {
    if let Some(outcome) = duplicate(state, intent)? {
        return Ok(PrepareOutcome::Duplicate(outcome));
    }
    let proposal = state
        .sequencer
        .propose_reservation(intent.record_count, intent.admitted_leader_epoch)?;
    if proposal.batch_id().get() != state.batches.next_key() {
        return Err(EngineError::CorruptState(
            "sequencer and dense batch index disagree".into(),
        ));
    }
    state
        .batches
        .len()
        .checked_add(1)
        .ok_or_else(|| EngineError::InvalidConfig("dense batch capacity exhausted".into()))?;
    let producer_before = intent
        .producer
        .and_then(|p| state.producers.get(&p.producer_id).copied());
    let producer_after = intent
        .producer
        .map(|identity| {
            let next_sequence = identity
                .first_sequence
                .checked_add(u64::from(intent.record_count.get()))
                .ok_or_else(|| EngineError::InvalidConfig("producer sequence overflow".into()))?;
            Ok::<_, EngineError>(ProducerCursor {
                epoch: identity.epoch,
                next_sequence,
                failed: false,
            })
        })
        .transpose()?;
    let atomic_before = intent
        .atomic_group
        .and_then(|a| state.atomic_groups.get(&a.transaction_id))
        .map(AtomicSnapshot::from_group);
    let atomic_after = intent
        .atomic_group
        .map(|_| {
            let before = atomic_before.unwrap_or(AtomicSnapshot {
                state: AtomicGroupState::Unresolved,
                next_sequence: 0,
                payload_bytes: 0,
                record_count: 0,
                chunk_count: 0,
                last_batch: None,
            });
            Ok::<_, EngineError>(AtomicSnapshot {
                state: AtomicGroupState::Unresolved,
                next_sequence: before.next_sequence.checked_add(1).ok_or_else(|| {
                    EngineError::InvalidConfig("atomic group sequence exhausted".into())
                })?,
                payload_bytes: before
                    .payload_bytes
                    .checked_add(intent.payload_bytes)
                    .ok_or_else(|| {
                        EngineError::InvalidConfig("atomic group byte count overflow".into())
                    })?,
                record_count: before
                    .record_count
                    .checked_add(u64::from(intent.record_count.get()))
                    .ok_or_else(|| {
                        EngineError::InvalidConfig("atomic group record count overflow".into())
                    })?,
                chunk_count: before.chunk_count.checked_add(1).ok_or_else(|| {
                    EngineError::InvalidConfig("atomic group chunk capacity exhausted".into())
                })?,
                last_batch: Some(proposal.batch_id().get()),
            })
        })
        .transpose()?;
    Ok(PrepareOutcome::New(PreparedAppend {
        intent,
        proposal,
        replication_policy: state.replication_policy,
        producer_before,
        producer_after,
        atomic_before,
        atomic_after,
        dense_next_key: state.batches.next_key(),
        dense_len: state.batches.len(),
    }))
}

pub(super) fn commit(
    state: &mut PartitionState,
    prepared: PreparedAppend,
    control: Option<&CommitControl>,
) -> EngineResult<ReserveOutcome> {
    // A concurrent completion can turn this exact input into an existing receipt.
    // No proposal epoch or coordinates may replace its historical placement.
    if let Some(outcome) = duplicate(state, prepared.intent)? {
        return Ok(outcome);
    }
    let PrepareOutcome::New(current) = prepare(state, prepared.intent)? else {
        return Err(EngineError::CorruptState(
            "duplicate changed within serialized commit".into(),
        ));
    };
    if current.proposal != prepared.proposal
        || current.replication_policy != prepared.replication_policy
        || current.producer_before != prepared.producer_before
        || current.producer_after != prepared.producer_after
        || current.atomic_before != prepared.atomic_before
        || current.atomic_after != prepared.atomic_after
        || current.dense_next_key != prepared.dense_next_key
        || current.dense_len != prepared.dense_len
    {
        return Err(shard_stream_core::SequencerError::ReservationProposalChanged.into());
    }
    if let Some(control) = control {
        control.begin_commit()?;
    }
    let reservation = state.sequencer.commit_reservation(prepared.proposal)?;
    // Every logical failure below was checked against this same serialized
    // state before commit; no producer/group updates happen on a refusal.
    state
        .batches
        .push(
            reservation.batch_id.get(),
            BatchRuntime {
                reservation,
                status: BatchStatus::Pending,
                durable_replicas: 0,
                replica_acks: 0,
                producer: prepared.intent.producer,
                atomic_group: prepared.intent.atomic_group,
                payload_digest: Some(prepared.intent.payload_digest),
                payload_bytes: prepared.intent.payload_bytes,
                object_durable: false,
            },
        )
        .expect("dense insertion was validated before reservation commit");
    if let (Some(identity), Some(cursor)) = (prepared.intent.producer, prepared.producer_after) {
        state.producers.insert(identity.producer_id, cursor);
    }
    if let (Some(identity), Some(after)) = (prepared.intent.atomic_group, prepared.atomic_after) {
        let group = state
            .atomic_groups
            .entry(identity.transaction_id)
            .or_insert_with(|| AtomicGroupRuntime {
                state: AtomicGroupState::Unresolved,
                batch_ids: Vec::new(),
                next_sequence: 0,
                payload_bytes: 0,
                record_count: 0,
            });
        group.batch_ids.push(reservation.batch_id.get());
        group.next_sequence = after.next_sequence;
        group.payload_bytes = after.payload_bytes;
        group.record_count = after.record_count;
    }
    Ok(ReserveOutcome::Reserved {
        reservation,
        producer: prepared.intent.producer.map(|identity| ProducerSequence {
            producer_id: identity.producer_id,
            epoch: identity.epoch,
            first_sequence: identity.first_sequence,
            event_id: identity.event_id,
            digest_algorithm: DIGEST_XXH3_128,
            payload_digest: prepared.intent.payload_digest,
        }),
        replication_policy: prepared.replication_policy,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc::{RecvTimeoutError, SyncSender, sync_channel};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use bytes::Bytes;
    use shard_stream_core::{
        LogicalOffset, LogicalPartitionId, PendingWriteAdmission, PhysicalWriteGuard, Reservation,
        RingEpoch, ShardId, TopicId, TopicSequencer, WriteFenceError,
    };
    use shardlog::{ShardStore, ShardStoreConfig};

    use super::*;
    use crate::EngineConfig;
    use crate::coordinator::{CoordinatorCommand, CoordinatorRouter};
    use crate::worker::{ProposedAppendOutcome, ShardHandle, append_command_charge};

    fn prepared_fixture() -> (PartitionState, PreparedAppend) {
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let state = PartitionState::empty(
            TopicSequencer::new(
                partition.topic_id,
                partition.partition_id,
                RingEpoch::new(1),
                vec![ShardId::new(0)],
            )
            .expect("genuine sequencer"),
            PartitionReplicationPolicy {
                policy_generation: 1,
                replication_factor: 1,
                min_in_sync_replicas: 1,
            },
        );
        // Preparation must leave both producer and atomic-group state unchanged.
        let intent = AppendIntent {
            topic_partition: partition,
            request_id: 1,
            producer: Some(ProducerIdentity {
                producer_id: 22,
                epoch: 1,
                first_sequence: 0,
                event_id: None,
            }),
            atomic_group: Some(AtomicGroupIdentity {
                transaction_id: 33,
                chunk_sequence: 0,
            }),
            record_count: NonZeroU32::new(1).expect("one record"),
            payload_bytes: 2,
            payload_digest: [7; 16],
            durability: Durability::Leader,
            admitted_leader_epoch: Some(LeaderEpoch::new(7)),
        };
        let PrepareOutcome::New(prepared) = prepare(&state, intent).expect("pure preparation")
        else {
            panic!("new input has no duplicate receipt");
        };
        assert_eq!(state.sequencer.next_offset(), LogicalOffset::new(0));
        assert!(state.producers.is_empty());
        assert!(state.atomic_groups.is_empty());
        (state, prepared)
    }

    #[test]
    fn cancelled_pending_commit_cannot_allocate_after_its_response_deadline() {
        let (mut state, prepared) = prepared_fixture();
        let control = CommitControl::default();
        let (sender, receiver) = sync_channel(1);
        assert!(matches!(
            await_commit(receiver, &control),
            CommitOutcome::Unallocated(EngineError::Fenced(_))
        ));
        assert!(control.is_cancelled());
        assert!(matches!(
            commit(&mut state, prepared, Some(&control)),
            Err(EngineError::Fenced(_))
        ));
        assert_eq!(state.sequencer.next_offset(), LogicalOffset::new(0));
        assert!(state.producers.is_empty());
        assert!(state.atomic_groups.is_empty());
        assert_eq!(state.batches.len(), 0);
        drop(sender);
    }

    #[test]
    fn lost_response_after_actual_coordinator_commit_is_unknown() {
        let (mut state, prepared) = prepared_fixture();
        let control = CommitControl::default();
        let (sender, receiver) = sync_channel(1);
        assert!(matches!(
            commit(&mut state, prepared, Some(&control)),
            Ok(ReserveOutcome::Reserved { .. })
        ));
        drop(sender); // Lose a genuine committed result, not an invented state flag.
        assert!(matches!(
            await_commit(receiver, &control),
            CommitOutcome::Unknown(EngineError::CorruptState(_))
        ));
        assert!(!control.cancel());
        assert_eq!(state.sequencer.next_offset(), LogicalOffset::new(1));
        assert_eq!(state.producers[&22].next_sequence, 1);
        assert_eq!(state.atomic_groups[&33].batch_ids.len(), 1);
        assert_eq!(
            state.batches.get(0).expect("allocated batch").status,
            BatchStatus::Pending
        );
    }

    #[derive(Debug, Default)]
    struct FixtureAdmission(Arc<AtomicUsize>);

    #[derive(Debug)]
    struct FixtureGuard(Arc<AtomicUsize>);
    impl PhysicalWriteGuard for FixtureGuard {}
    impl Drop for FixtureGuard {
        fn drop(&mut self) {
            assert_eq!(self.0.fetch_sub(1, Ordering::AcqRel), 1);
        }
    }

    impl PendingWriteAdmission for FixtureAdmission {
        fn validate_before_reserve(&self) -> Result<(), WriteFenceError> {
            Ok(())
        }
        fn try_admit_proposal(
            &self,
            proposal: &ReservationProposal,
        ) -> Result<Box<dyn PhysicalWriteGuard>, WriteFenceError> {
            assert_eq!(proposal.placement().leader_epoch, LeaderEpoch::new(7));
            self.0
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| WriteFenceError::new("fixture physical capacity is occupied"))?;
            Ok(Box::new(FixtureGuard(self.0.clone())))
        }
        fn try_admit_physical(
            &self,
            _reservation: &Reservation,
        ) -> Result<Box<dyn PhysicalWriteGuard>, WriteFenceError> {
            Err(WriteFenceError::new(
                "fixture grants proposal permission only",
            ))
        }
    }

    struct FixtureDirectory(PathBuf);
    impl Drop for FixtureDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Release the withheld reply before joining either real request owner.
    struct ReleaseReply {
        release: Option<SyncSender<()>>,
        coordinator: Option<JoinHandle<PartitionState>>,
        caller: Option<JoinHandle<()>>,
    }
    impl ReleaseReply {
        fn finish(mut self) -> PartitionState {
            let _ = self.release.take().expect("release sender").send(());
            let state = self
                .coordinator
                .take()
                .expect("coordinator")
                .join()
                .expect("commit owner");
            self.caller
                .take()
                .expect("caller")
                .join()
                .expect("physical reply owner");
            state
        }
    }
    impl Drop for ReleaseReply {
        fn drop(&mut self) {
            if let Some(release) = self.release.take() {
                let _ = release.send(());
            }
            if let Some(coordinator) = self.coordinator.take() {
                let _ = coordinator.join();
            }
            if let Some(caller) = self.caller.take() {
                let _ = caller.join();
            }
        }
    }

    #[test]
    fn committing_timeout_keeps_real_worker_guard_and_charge_until_lost_reply_settles() {
        let directory = FixtureDirectory(std::env::temp_dir().join(format!(
            "shard-stream-commit-result-loss-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).expect("fixture clock").as_nanos(),
        )));
        let config = EngineConfig {
            data_dir: directory.0.clone(),
            object_store_dir: None,
            shard_count: 1,
            virtual_lane_count: 1,
            replication_factor: 1,
            min_in_sync_replicas: 1,
            queue_slots_per_shard: 1,
            queue_bytes_per_shard: append_command_charge(2, true).expect("actual command charge"),
            target_pack_bytes: 64 * 1024,
            max_pack_age: Duration::from_secs(60),
            max_batch_bytes: 2,
            max_fetch_bytes: 2,
            append_linger: Duration::ZERO,
        };
        config.validate().expect("valid actual shard configuration");
        let (shard, _) = ShardHandle::spawn(&config, ShardId::new(0)).expect("real WAL worker");
        let (mut state, prepared) = prepared_fixture();
        let (commands, receiver) = sync_channel(1);
        let router = Arc::new(CoordinatorRouter {
            senders: vec![commands],
        });
        let (started, entered) = sync_channel(1);
        let (release, released) = sync_channel(1);
        let coordinator = thread::spawn(move || {
            let CoordinatorCommand::CommitPrepared {
                prepared,
                control,
                response,
            } = receiver.recv().expect("actual queued commit request")
            else {
                panic!("fixture receives exactly one commit");
            };
            assert!(matches!(
                commit(&mut state, prepared, Some(&control)),
                Ok(ReserveOutcome::Reserved { .. })
            ));
            started.send(()).expect("genuine allocation observed");
            released.recv().expect("finite reply-loss barrier");
            drop(response); // Physical owner must classify the missing COMMITTING reply as Unknown.
            state
        });
        // This guard is after the shard: unwind stops the coordinator before worker Drop.
        let mut release_reply = ReleaseReply {
            release: Some(release),
            coordinator: Some(coordinator),
            caller: None,
        };
        let admission = Arc::new(FixtureAdmission::default());
        let ticket = shard
            .begin_proposed_append(
                prepared,
                Bytes::from_static(b"ab"),
                false,
                admission.clone(),
                router.clone(),
            )
            .expect("actual charged physical queue");
        let (outcome_sender, outcome_receiver) = sync_channel(1);
        release_reply.caller = Some(thread::spawn(move || {
            let _ = outcome_sender.send(ticket.wait());
        }));
        entered
            .recv_timeout(Duration::from_secs(5))
            .expect("actual commit won");
        assert!(matches!(
            outcome_receiver.recv_timeout(RPC_TIMEOUT + Duration::from_millis(100)),
            Err(RecvTimeoutError::Timeout)
        ));
        assert_eq!(admission.0.load(Ordering::Acquire), 1);
        assert!(matches!(
            shard.begin_proposed_append(
                prepared,
                Bytes::from_static(b"ab"),
                false,
                admission.clone(),
                router.clone(),
            ),
            Err(EngineError::AdmissionLimited {
                reason: "shard byte budget exhausted",
                ..
            })
        ));
        let state = release_reply.finish();
        assert!(matches!(
            outcome_receiver.recv().expect("actual worker reply"),
            ProposedAppendOutcome::Unknown(EngineError::CorruptState(_))
        ));
        assert_eq!(admission.0.load(Ordering::Acquire), 0);
        assert_eq!(state.sequencer.next_offset(), LogicalOffset::new(1));
        assert_eq!(
            state.batches.get(0).expect("no fabricated abort").status,
            BatchStatus::Pending
        );
        assert_eq!(state.producers[&22].next_sequence, 1);
        assert_eq!(state.atomic_groups[&33].batch_ids.len(), 1);
        // Capacity is released only after the missing reply settled. A genuinely
        // stopped coordinator refuses new work without inventing another allocation.
        let ticket = shard
            .begin_proposed_append(
                prepared,
                Bytes::from_static(b"ab"),
                false,
                admission.clone(),
                router,
            )
            .expect("physical capacity recovered");
        assert!(matches!(
            ticket.wait(),
            ProposedAppendOutcome::Unallocated(_)
        ));
        assert_eq!(admission.0.load(Ordering::Acquire), 0);
        drop(shard);
        let wal = ShardStore::open(ShardStoreConfig {
            directory: directory.0.join("shards").join("shard-0"),
            shard_id: ShardId::new(0),
            target_pack_bytes: config.target_pack_bytes,
            max_batch_bytes: config.max_batch_bytes,
        })
        .expect("actual WAL reopen");
        assert!(wal.catalog().is_empty());
    }
}
