use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;

use crate::{
    BatchId, LeaderEpoch, LogicalOffset, LogicalPartitionId, PlacementSequence, RingEpoch, TopicId,
    VirtualLaneId,
};

pub use shardlog::{Placement, Reservation};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequencerError {
    EmptyRing,
    DuplicateLane(VirtualLaneId),
    StaleRingEpoch {
        current: RingEpoch,
        proposed: RingEpoch,
    },
    StaleLeaderEpoch {
        current: LeaderEpoch,
        proposed: LeaderEpoch,
    },
    OffsetSpaceExhausted,
    BatchIdSpaceExhausted,
    PlacementSequenceSpaceExhausted,
    ReservationProposalChanged,
}

impl fmt::Display for SequencerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyRing => formatter.write_str("placement ring cannot be empty"),
            Self::DuplicateLane(lane) => {
                write!(
                    formatter,
                    "placement ring contains duplicate virtual lane {lane:?}"
                )
            }
            Self::StaleRingEpoch { current, proposed } => write!(
                formatter,
                "ring epoch must increase: current={current:?}, proposed={proposed:?}"
            ),
            Self::StaleLeaderEpoch { current, proposed } => write!(
                formatter,
                "leader epoch cannot move backward: current={current:?}, proposed={proposed:?}"
            ),
            Self::OffsetSpaceExhausted => formatter.write_str("logical offset space exhausted"),
            Self::BatchIdSpaceExhausted => formatter.write_str("batch ID space exhausted"),
            Self::PlacementSequenceSpaceExhausted => {
                formatter.write_str("placement sequence space exhausted")
            }
            Self::ReservationProposalChanged => {
                formatter.write_str("sequencer state changed before reservation commit")
            }
        }
    }
}

impl Error for SequencerError {}

#[derive(Debug)]
pub struct TopicSequencer {
    topic_id: TopicId,
    partition_id: LogicalPartitionId,
    ring_epoch: RingEpoch,
    leader_epoch: LeaderEpoch,
    virtual_lanes: Vec<VirtualLaneId>,
    next_batch_id: u128,
    next_offset: u64,
    next_placement_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequencerState {
    pub topic_id: TopicId,
    pub partition_id: LogicalPartitionId,
    pub ring_epoch: RingEpoch,
    pub leader_epoch: LeaderEpoch,
    pub virtual_lanes: Vec<VirtualLaneId>,
    pub next_batch_id: u128,
    pub next_offset: u64,
    pub next_placement_sequence: u64,
}

/// Fixed-size coordinates for a possible reservation, without allocated offsets.
///
/// Creating or dropping this value does not change sequencing or leader epoch.
/// It grants no write authority and cannot be supplied to WAL or replication
/// APIs as a reservation. A composing host must obtain any required physical
/// authority and commit against the same sequencer before using the reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReservationProposal {
    reservation: Reservation,
    previous_leader_epoch: LeaderEpoch,
}

impl ReservationProposal {
    /// Exact topic and logical partition whose sequencer produced this proposal.
    #[must_use]
    pub const fn topic_partition(&self) -> crate::TopicPartition {
        crate::TopicPartition::new(self.reservation.topic_id, self.reservation.partition_id)
    }

    /// Batch identifier that commit would allocate.
    #[must_use]
    pub const fn batch_id(&self) -> BatchId {
        self.reservation.batch_id
    }

    /// First record offset that commit would allocate.
    #[must_use]
    pub const fn first_offset(&self) -> LogicalOffset {
        self.reservation.first_offset
    }

    /// Last record offset that commit would allocate.
    #[must_use]
    pub const fn last_offset(&self) -> LogicalOffset {
        self.reservation.last_offset
    }

    /// Validated nonzero record count for this possible batch.
    #[must_use]
    pub const fn record_count(&self) -> NonZeroU32 {
        self.reservation.record_count
    }

    /// Prospective lane, ring, leader epoch and placement sequence.
    #[must_use]
    pub const fn placement(&self) -> Placement {
        self.reservation.placement
    }

    /// Tests exact coordinates after a successful sequencer commit.
    #[must_use]
    pub fn matches_reservation(&self, reservation: &Reservation) -> bool {
        self.reservation == *reservation
    }
}

impl TopicSequencer {
    pub fn new(
        topic_id: TopicId,
        partition_id: LogicalPartitionId,
        ring_epoch: RingEpoch,
        virtual_lanes: Vec<VirtualLaneId>,
    ) -> Result<Self, SequencerError> {
        validate_virtual_lanes(&virtual_lanes)?;
        Ok(Self {
            topic_id,
            partition_id,
            ring_epoch,
            leader_epoch: LeaderEpoch::new(0),
            virtual_lanes,
            next_batch_id: 0,
            next_offset: 0,
            next_placement_sequence: 0,
        })
    }

    pub fn restore(state: SequencerState) -> Result<Self, SequencerError> {
        validate_virtual_lanes(&state.virtual_lanes)?;
        Ok(Self {
            topic_id: state.topic_id,
            partition_id: state.partition_id,
            ring_epoch: state.ring_epoch,
            leader_epoch: state.leader_epoch,
            virtual_lanes: state.virtual_lanes,
            next_batch_id: state.next_batch_id,
            next_offset: state.next_offset,
            next_placement_sequence: state.next_placement_sequence,
        })
    }

    pub fn reserve(&mut self, record_count: NonZeroU32) -> Result<Reservation, SequencerError> {
        let proposal = self.propose_reservation(record_count, None)?;
        self.commit_reservation(proposal)
    }

    /// Computes bounded next coordinates without allocating or changing epoch.
    ///
    /// An admitted epoch is prospective until commit. This method is pure;
    /// authority checks, producer identity and atomic-group bookkeeping remain
    /// the coordinator's responsibility.
    pub fn propose_reservation(
        &self,
        record_count: NonZeroU32,
        admitted_leader_epoch: Option<LeaderEpoch>,
    ) -> Result<ReservationProposal, SequencerError> {
        let leader_epoch = admitted_leader_epoch.unwrap_or(self.leader_epoch);
        if leader_epoch < self.leader_epoch {
            return Err(SequencerError::StaleLeaderEpoch {
                current: self.leader_epoch,
                proposed: leader_epoch,
            });
        }
        let record_count_u64 = u64::from(record_count.get());
        let last_offset = self
            .next_offset
            .checked_add(record_count_u64 - 1)
            .ok_or(SequencerError::OffsetSpaceExhausted)?;
        self.next_offset
            .checked_add(record_count_u64)
            .ok_or(SequencerError::OffsetSpaceExhausted)?;
        self.next_batch_id
            .checked_add(1)
            .ok_or(SequencerError::BatchIdSpaceExhausted)?;
        self.next_placement_sequence
            .checked_add(1)
            .ok_or(SequencerError::PlacementSequenceSpaceExhausted)?;

        let lane_index = self.next_placement_sequence as usize % self.virtual_lanes.len();
        let reservation = Reservation {
            topic_id: self.topic_id,
            partition_id: self.partition_id,
            batch_id: BatchId::new(self.next_batch_id),
            first_offset: LogicalOffset::new(self.next_offset),
            last_offset: LogicalOffset::new(last_offset),
            record_count,
            placement: Placement {
                virtual_lane_id: self.virtual_lanes[lane_index],
                ring_epoch: self.ring_epoch,
                leader_epoch,
                sequence: PlacementSequence::new(self.next_placement_sequence),
            },
        };

        Ok(ReservationProposal {
            reservation,
            previous_leader_epoch: self.leader_epoch,
        })
    }

    /// Allocates only if all prospective coordinates and prior epoch still match.
    ///
    /// The caller serializes this operation with other sequencer mutations and
    /// retains any required physical authority across commit and WAL settlement.
    /// Conflicts change no state and grant no reservation or write permission.
    pub fn commit_reservation(
        &mut self,
        proposal: ReservationProposal,
    ) -> Result<Reservation, SequencerError> {
        let current = self.propose_reservation(
            proposal.record_count(),
            Some(proposal.placement().leader_epoch),
        );
        if current.as_ref() != Ok(&proposal) {
            return Err(SequencerError::ReservationProposalChanged);
        }
        let next_offset = self
            .next_offset
            .checked_add(u64::from(proposal.record_count().get()))
            .ok_or(SequencerError::OffsetSpaceExhausted)?;
        let next_batch_id = self
            .next_batch_id
            .checked_add(1)
            .ok_or(SequencerError::BatchIdSpaceExhausted)?;
        let next_placement_sequence = self
            .next_placement_sequence
            .checked_add(1)
            .ok_or(SequencerError::PlacementSequenceSpaceExhausted)?;
        self.leader_epoch = proposal.placement().leader_epoch;
        self.next_offset = next_offset;
        self.next_batch_id = next_batch_id;
        self.next_placement_sequence = next_placement_sequence;
        Ok(proposal.reservation)
    }

    pub fn reconfigure(
        &mut self,
        ring_epoch: RingEpoch,
        virtual_lanes: Vec<VirtualLaneId>,
    ) -> Result<(), SequencerError> {
        if ring_epoch <= self.ring_epoch {
            return Err(SequencerError::StaleRingEpoch {
                current: self.ring_epoch,
                proposed: ring_epoch,
            });
        }
        validate_virtual_lanes(&virtual_lanes)?;
        self.ring_epoch = ring_epoch;
        self.virtual_lanes = virtual_lanes;
        Ok(())
    }

    pub fn observe_leader_epoch(
        &mut self,
        leader_epoch: LeaderEpoch,
    ) -> Result<(), SequencerError> {
        if leader_epoch < self.leader_epoch {
            return Err(SequencerError::StaleLeaderEpoch {
                current: self.leader_epoch,
                proposed: leader_epoch,
            });
        }
        self.leader_epoch = leader_epoch;
        Ok(())
    }

    #[must_use]
    pub const fn next_offset(&self) -> LogicalOffset {
        LogicalOffset::new(self.next_offset)
    }

    #[must_use]
    pub const fn ring_epoch(&self) -> RingEpoch {
        self.ring_epoch
    }

    #[must_use]
    pub fn state(&self) -> SequencerState {
        SequencerState {
            topic_id: self.topic_id,
            partition_id: self.partition_id,
            ring_epoch: self.ring_epoch,
            leader_epoch: self.leader_epoch,
            virtual_lanes: self.virtual_lanes.clone(),
            next_batch_id: self.next_batch_id,
            next_offset: self.next_offset,
            next_placement_sequence: self.next_placement_sequence,
        }
    }

    #[cfg(test)]
    fn set_state(&mut self, next_batch_id: u128, next_offset: u64, next_placement_sequence: u64) {
        self.next_batch_id = next_batch_id;
        self.next_offset = next_offset;
        self.next_placement_sequence = next_placement_sequence;
    }
}

fn validate_virtual_lanes(virtual_lanes: &[VirtualLaneId]) -> Result<(), SequencerError> {
    if virtual_lanes.is_empty() {
        return Err(SequencerError::EmptyRing);
    }
    let mut unique = HashSet::with_capacity(virtual_lanes.len());
    for lane in virtual_lanes {
        if !unique.insert(*lane) {
            return Err(SequencerError::DuplicateLane(*lane));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sequencer(lanes: &[u32]) -> TopicSequencer {
        TopicSequencer::new(
            TopicId::new(7),
            LogicalPartitionId::new(3),
            RingEpoch::new(1),
            lanes.iter().copied().map(VirtualLaneId::new).collect(),
        )
        .expect("valid sequencer")
    }

    #[test]
    fn proposed_epoch_and_offsets_change_only_on_matching_commit() {
        let mut sequencer = sequencer(&[10, 11]);
        let initial = sequencer.state();
        let count = NonZeroU32::new(3).expect("nonzero count");
        let proposal = sequencer
            .propose_reservation(count, Some(LeaderEpoch::new(7)))
            .expect("prospective current epoch");
        assert_eq!(sequencer.state(), initial);
        assert_eq!(
            sequencer
                .propose_reservation(count, Some(LeaderEpoch::new(7)))
                .expect("same coordinates without allocation"),
            proposal
        );
        assert_eq!(
            proposal.topic_partition(),
            crate::TopicPartition::new(initial.topic_id, initial.partition_id)
        );
        assert_eq!(proposal.batch_id(), BatchId::new(0));
        assert_eq!(proposal.first_offset(), LogicalOffset::new(0));
        assert_eq!(proposal.last_offset(), LogicalOffset::new(2));
        assert_eq!(proposal.record_count(), count);
        assert_eq!(proposal.placement().leader_epoch, LeaderEpoch::new(7));
        let reservation = sequencer
            .commit_reservation(proposal)
            .expect("matching commit");
        assert!(proposal.matches_reservation(&reservation));
        let committed = sequencer.state();
        assert_eq!(committed.next_offset, 3);
        assert_eq!(committed.next_batch_id, 1);
        assert_eq!(committed.next_placement_sequence, 1);
        assert_eq!(committed.leader_epoch, LeaderEpoch::new(7));
        assert_eq!(
            sequencer.commit_reservation(proposal),
            Err(SequencerError::ReservationProposalChanged)
        );
        assert_eq!(sequencer.state(), committed);
        let next = sequencer
            .reserve(NonZeroU32::MIN)
            .expect("legacy reservation after commit");
        assert_eq!(next.first_offset, LogicalOffset::new(3));
        assert_eq!(next.placement.virtual_lane_id, VirtualLaneId::new(11));
        assert_eq!(next.placement.leader_epoch, LeaderEpoch::new(7));
    }

    #[test]
    fn proposals_refuse_allocation_ring_and_epoch_conflicts() {
        for conflict in 0..3 {
            let mut sequencer = sequencer(&[1, 2]);
            let proposal = sequencer
                .propose_reservation(NonZeroU32::MIN, Some(LeaderEpoch::new(4)))
                .expect("proposal before independent mutation");
            match conflict {
                0 => {
                    sequencer
                        .reserve(NonZeroU32::MIN)
                        .expect("competing reservation");
                }
                1 => {
                    sequencer
                        .reconfigure(RingEpoch::new(2), vec![VirtualLaneId::new(3)])
                        .expect("changed ring");
                }
                _ => {
                    sequencer
                        .observe_leader_epoch(LeaderEpoch::new(1))
                        .expect("changed prior epoch");
                }
            }
            let changed = sequencer.state();
            assert_eq!(
                sequencer.commit_reservation(proposal),
                Err(SequencerError::ReservationProposalChanged)
            );
            assert_eq!(sequencer.state(), changed);
            let fresh = sequencer
                .propose_reservation(NonZeroU32::MIN, Some(LeaderEpoch::new(4)))
                .expect("fresh proposal after conflict");
            let actual = sequencer.commit_reservation(fresh).expect("later progress");
            assert!(fresh.matches_reservation(&actual));
        }
    }

    #[test]
    fn rejected_proposals_preserve_epoch_and_all_checked_counters() {
        let mut sequencer = sequencer(&[1]);
        sequencer
            .observe_leader_epoch(LeaderEpoch::new(5))
            .expect("existing epoch");
        let current = sequencer.state();
        assert_eq!(
            sequencer.propose_reservation(NonZeroU32::MIN, Some(LeaderEpoch::new(4))),
            Err(SequencerError::StaleLeaderEpoch {
                current: LeaderEpoch::new(5),
                proposed: LeaderEpoch::new(4)
            })
        );
        assert_eq!(sequencer.state(), current);
        for (batch, offset, placement, expected) in [
            (4, u64::MAX, 9, SequencerError::OffsetSpaceExhausted),
            (u128::MAX, 0, 9, SequencerError::BatchIdSpaceExhausted),
            (
                4,
                0,
                u64::MAX,
                SequencerError::PlacementSequenceSpaceExhausted,
            ),
        ] {
            sequencer.set_state(batch, offset, placement);
            let before = sequencer.state();
            assert_eq!(
                sequencer.propose_reservation(NonZeroU32::MIN, Some(LeaderEpoch::new(6))),
                Err(expected)
            );
            assert_eq!(sequencer.state(), before);
            assert_eq!(sequencer.reserve(NonZeroU32::MIN), Err(expected));
            assert_eq!(sequencer.state(), before);
        }
    }

    #[test]
    fn proposals_from_another_partition_cannot_allocate_here() {
        let other = TopicSequencer::new(
            TopicId::new(99),
            LogicalPartitionId::new(0),
            RingEpoch::new(1),
            vec![VirtualLaneId::new(1)],
        )
        .expect("other sequencer");
        let proposal = other
            .propose_reservation(NonZeroU32::MIN, None)
            .expect("other proposal");
        let mut sequencer = sequencer(&[1]);
        let initial = sequencer.state();
        assert_eq!(
            sequencer.commit_reservation(proposal),
            Err(SequencerError::ReservationProposalChanged)
        );
        assert_eq!(sequencer.state(), initial);
    }

    #[test]
    fn placement_advances_once_per_batch_not_per_record() {
        let mut sequencer = sequencer(&[10, 11, 12]);
        let counts = [5, 1, 9, 2];
        let reservations = counts.map(|count| {
            sequencer
                .reserve(NonZeroU32::new(count).expect("nonzero"))
                .expect("reservation")
        });

        assert_eq!(
            reservations.map(|reservation| reservation.placement.virtual_lane_id.get()),
            [10, 11, 12, 10]
        );
        assert_eq!(
            reservations.map(|reservation| reservation.placement.sequence.get()),
            [0, 1, 2, 3]
        );
        assert_eq!(
            reservations.map(|reservation| (
                reservation.first_offset.get(),
                reservation.last_offset.get()
            )),
            [(0, 4), (5, 5), (6, 14), (15, 16)]
        );
    }

    #[test]
    fn ring_change_preserves_global_placement_sequence() {
        let mut sequencer = sequencer(&[1, 2]);
        let first = sequencer
            .reserve(NonZeroU32::MIN)
            .expect("first reservation");
        assert_eq!(first.placement.sequence.get(), 0);

        sequencer
            .reconfigure(
                RingEpoch::new(2),
                vec![
                    VirtualLaneId::new(7),
                    VirtualLaneId::new(8),
                    VirtualLaneId::new(9),
                ],
            )
            .expect("new epoch");
        let second = sequencer
            .reserve(NonZeroU32::MIN)
            .expect("second reservation");

        assert_eq!(second.placement.sequence.get(), 1);
        assert_eq!(second.placement.virtual_lane_id, VirtualLaneId::new(8));
        assert_eq!(second.placement.ring_epoch, RingEpoch::new(2));
    }

    #[test]
    fn rejects_empty_duplicate_and_stale_rings() {
        assert_eq!(
            TopicSequencer::new(
                TopicId::new(1),
                LogicalPartitionId::new(0),
                RingEpoch::new(1),
                Vec::new()
            )
            .expect_err("empty ring"),
            SequencerError::EmptyRing
        );
        assert_eq!(
            TopicSequencer::new(
                TopicId::new(1),
                LogicalPartitionId::new(0),
                RingEpoch::new(1),
                vec![VirtualLaneId::new(2), VirtualLaneId::new(2)]
            )
            .expect_err("duplicate"),
            SequencerError::DuplicateLane(VirtualLaneId::new(2))
        );

        let mut sequencer = sequencer(&[1]);
        assert!(matches!(
            sequencer.reconfigure(RingEpoch::new(1), vec![VirtualLaneId::new(2)]),
            Err(SequencerError::StaleRingEpoch { .. })
        ));
    }

    #[test]
    fn failed_checked_reservation_does_not_advance_state() {
        let mut sequencer = sequencer(&[1]);
        sequencer.set_state(4, u64::MAX, 9);

        assert_eq!(
            sequencer
                .reserve(NonZeroU32::MIN)
                .expect_err("offset overflow"),
            SequencerError::OffsetSpaceExhausted
        );
        assert_eq!(sequencer.next_offset(), LogicalOffset::new(u64::MAX));
        assert_eq!(sequencer.next_batch_id, 4);
        assert_eq!(sequencer.next_placement_sequence, 9);
    }
}
