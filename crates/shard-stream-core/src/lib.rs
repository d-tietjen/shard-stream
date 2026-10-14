//! Stream-native identifiers, sequencing, placement, and bounded execution.

mod channel;
mod directory_ownership;
mod fencing;
mod ids;
mod sequencer;
mod topology;

pub use channel::{
    Budgeted, ByteBoundedReceiver, ByteBoundedSender, ChannelConfigError, ChannelStats,
    TrySendError, byte_bounded_channel,
};
pub use directory_ownership::{DirectoryOwnership, DirectoryOwnershipError};
pub use fencing::{
    PendingWriteAdmission, PhysicalWriteGuard, ReplicaSourceIdentity, WriteFence, WriteFenceError,
};
pub use ids::{
    AssignmentEpoch, BatchId, BrokerId, LeaderEpoch, LogicalOffset, LogicalPartitionId, OriginId,
    PhysicalShardId, PlacementSequence, RecordId, RingEpoch, ShardId, StreamOffset, TopicId,
    TopicPartition, VirtualLaneId,
};
pub use sequencer::{
    Placement, Reservation, ReservationProposal, SequencerError, SequencerState, TopicSequencer,
};
pub use topology::{
    AssignmentProvider, BrokerDescriptor, ExtentDescriptor, PartitionAssignment, ReplicaProgress,
    SingleNodeAssignment,
};
