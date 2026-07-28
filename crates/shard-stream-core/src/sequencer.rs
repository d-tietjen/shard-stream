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
        let record_count_u64 = u64::from(record_count.get());
        let last_offset = self
            .next_offset
            .checked_add(record_count_u64 - 1)
            .ok_or(SequencerError::OffsetSpaceExhausted)?;
        let next_offset = self
            .next_offset
            .checked_add(record_count_u64)
            .ok_or(SequencerError::OffsetSpaceExhausted)?;
        let next_batch_id = self
            .next_batch_id
            .checked_add(1)
            .ok_or(SequencerError::BatchIdSpaceExhausted)?;
        let next_placement_sequence = self
            .next_placement_sequence
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
                leader_epoch: self.leader_epoch,
                sequence: PlacementSequence::new(self.next_placement_sequence),
            },
        };

        self.next_offset = next_offset;
        self.next_batch_id = next_batch_id;
        self.next_placement_sequence = next_placement_sequence;
        Ok(reservation)
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
