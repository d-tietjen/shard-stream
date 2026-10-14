//! Pure verification and guarded commit of an immutable remote reservation.

use super::proposal::{self, AppendIntent, CommitControl, PrepareOutcome, PreparedAppend};
use super::{
    AtomicGroupIdentity, BatchStatus, Durability, EngineError, EngineResult, PartitionState,
    ProducerIdentity, Reservation, ReserveOutcome, TopicPartition, append_response,
};

/// The exact received coordinates and identities, without receiver mutation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReplicaIntent {
    pub(crate) reservation: Reservation,
    pub(crate) producer: Option<ProducerIdentity>,
    pub(crate) atomic_group: Option<AtomicGroupIdentity>,
    pub(crate) payload_bytes: u64,
    pub(crate) payload_digest: [u8; 16],
}

impl ReplicaIntent {
    pub(crate) fn topic_partition(self) -> TopicPartition {
        TopicPartition::new(self.reservation.topic_id, self.reservation.partition_id)
    }
}

/// Fixed-size snapshots of the prospective receiver state and remote input.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PreparedReplica {
    reservation: Reservation,
    prepared: PreparedAppend,
}

impl PreparedReplica {
    pub(crate) fn reservation(self) -> Reservation {
        self.reservation
    }

    pub(crate) fn intent(self) -> ReplicaIntent {
        ReplicaIntent {
            reservation: self.reservation,
            producer: self.prepared.intent.producer,
            atomic_group: self.prepared.intent.atomic_group,
            payload_bytes: self.prepared.intent.payload_bytes,
            payload_digest: self.prepared.intent.payload_digest,
        }
    }
}

// Fixed snapshots stay inline in the bounded coordinator response; an extra
// heap owner is unnecessary for this internal preparation result.
#[allow(clippy::large_enum_variant)]
pub(crate) enum PrepareReplicaOutcome {
    Duplicate(ReserveOutcome),
    New(PreparedReplica),
}

pub(super) fn prepare(
    state: &PartitionState,
    intent: ReplicaIntent,
) -> EngineResult<PrepareReplicaOutcome> {
    let reservation = intent.reservation;
    if let Some(existing) = state.batches.get(reservation.batch_id.get()) {
        if existing.reservation != reservation
            || existing.producer != intent.producer
            || existing.atomic_group != intent.atomic_group
            || existing.payload_bytes != intent.payload_bytes
            || existing
                .payload_digest
                .is_some_and(|digest| digest != intent.payload_digest)
        {
            return Err(EngineError::CorruptState(format!(
                "replica reservation conflicts with batch {}",
                reservation.batch_id
            )));
        }
        return match existing.status {
            BatchStatus::Committed | BatchStatus::Unresolved => Ok(
                PrepareReplicaOutcome::Duplicate(ReserveOutcome::Duplicate {
                    response: append_response(0, reservation),
                    durable_replicas: existing.durable_replicas,
                    object_durable: existing.object_durable,
                    replication_policy: state.replication_policy,
                }),
            ),
            BatchStatus::Pending => Err(EngineError::ReplicaInFlight(reservation.batch_id)),
            // A current new-write ticket supplies no authenticated terminal or
            // historical replay lineage for reviving a previously aborted batch.
            BatchStatus::Aborted => Err(EngineError::Fenced(
                "guarded aborted replica requires authenticated replay lineage".into(),
            )),
        };
    }
    let append = AppendIntent {
        topic_partition: intent.topic_partition(),
        request_id: 0,
        producer: intent.producer,
        atomic_group: intent.atomic_group,
        record_count: reservation.record_count,
        payload_bytes: intent.payload_bytes,
        payload_digest: intent.payload_digest,
        durability: Durability::Leader,
        admitted_leader_epoch: Some(reservation.placement.leader_epoch),
    };
    let PrepareOutcome::New(prepared) = proposal::prepare(state, append)? else {
        return Err(EngineError::Fenced(
            "remote reservation reuses another producer or atomic receipt".into(),
        ));
    };
    if !prepared.proposal.matches_reservation(&reservation) {
        return Err(EngineError::Fenced(format!(
            "replica reservation {} is not the exact next fenced placement",
            reservation.batch_id
        )));
    }
    Ok(PrepareReplicaOutcome::New(PreparedReplica {
        reservation,
        prepared,
    }))
}

pub(super) fn commit(
    state: &mut PartitionState,
    prepared: PreparedReplica,
    control: &CommitControl,
) -> EngineResult<ReserveOutcome> {
    // This check is pure and serialized with the following compare/commit.
    // It also permits an exact concurrent completion to return its immutable ACK.
    match prepare(state, prepared.intent())? {
        PrepareReplicaOutcome::Duplicate(outcome) => Ok(outcome),
        PrepareReplicaOutcome::New(_) => proposal::commit(state, prepared.prepared, Some(control)),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::sync::mpsc::sync_channel;

    use shard_stream_core::{
        LeaderEpoch, LogicalPartitionId, RingEpoch, ShardId, TopicId, TopicSequencer,
    };

    use super::*;
    use crate::PartitionReplicationPolicy;
    use crate::coordinator::proposal::{CommitOutcome, await_commit};

    fn fixture() -> (PartitionState, ReplicaIntent) {
        let sequencer = || {
            TopicSequencer::new(
                TopicId::new(1),
                LogicalPartitionId::new(0),
                RingEpoch::new(1),
                vec![ShardId::new(0)],
            )
            .expect("actual sequencer")
        };
        let mut source = sequencer();
        source
            .observe_leader_epoch(LeaderEpoch::new(7))
            .expect("source admitted epoch");
        let reservation = source
            .reserve(NonZeroU32::new(2).expect("two records"))
            .expect("genuine remote reservation");
        let receiver = PartitionState::empty(
            sequencer(),
            PartitionReplicationPolicy {
                policy_generation: 1,
                replication_factor: 1,
                min_in_sync_replicas: 1,
            },
        );
        (
            receiver,
            ReplicaIntent {
                reservation,
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
                payload_bytes: 2,
                payload_digest: [7; 16],
            },
        )
    }

    #[test]
    fn replica_pending_cancellation_preserves_every_receiver_coordinate() {
        let (mut receiver, intent) = fixture();
        let before = receiver.sequencer.state();
        let PrepareReplicaOutcome::New(prepared) =
            prepare(&receiver, intent).expect("pure remote preparation")
        else {
            panic!("fresh remote input is not a duplicate");
        };
        assert_eq!(receiver.sequencer.state(), before);
        let control = CommitControl::default();
        assert!(control.cancel());
        assert!(matches!(
            commit(&mut receiver, prepared, &control),
            Err(EngineError::Fenced(_))
        ));
        assert_eq!(receiver.sequencer.state(), before);
        assert!(receiver.producers.is_empty());
        assert!(receiver.atomic_groups.is_empty());
        assert_eq!(receiver.batches.len(), 0);
    }

    #[test]
    fn replica_lost_response_after_real_commit_is_unknown_without_abort() {
        let (mut receiver, intent) = fixture();
        let PrepareReplicaOutcome::New(prepared) =
            prepare(&receiver, intent).expect("pure remote preparation")
        else {
            panic!("fresh remote input is not a duplicate");
        };
        let control = CommitControl::default();
        let (sender, response) = sync_channel(1);
        assert!(
            matches!(commit(&mut receiver, prepared, &control), Ok(ReserveOutcome::Reserved { reservation, .. }) if reservation == intent.reservation)
        );
        // Lose a genuine completed coordinator result, rather than fabricating
        // an arbitration flag or treating a disappeared reply as unallocated.
        drop(sender);
        assert!(matches!(
            await_commit(response, &control),
            CommitOutcome::Unknown(EngineError::CorruptState(_))
        ));
        assert!(!control.cancel());
        assert_eq!(receiver.sequencer.next_offset().get(), 2);
        assert_eq!(receiver.sequencer.state().leader_epoch, LeaderEpoch::new(7));
        assert_eq!(receiver.producers[&22].next_sequence, 2);
        assert_eq!(receiver.atomic_groups[&33].batch_ids.len(), 1);
        assert_eq!(
            receiver
                .batches
                .get(0)
                .expect("actual committed reservation")
                .status,
            BatchStatus::Pending
        );
    }

    #[test]
    fn replica_malformed_remote_coordinates_never_observe_the_remote_epoch() {
        let (receiver, mut intent) = fixture();
        let before = receiver.sequencer.state();
        intent.reservation.placement.sequence = shard_stream_core::PlacementSequence::new(9);
        assert!(matches!(
            prepare(&receiver, intent),
            Err(EngineError::Fenced(_))
        ));
        assert_eq!(receiver.sequencer.state(), before);
        assert!(receiver.producers.is_empty());
        assert!(receiver.atomic_groups.is_empty());
        assert_eq!(receiver.batches.len(), 0);
    }
}
