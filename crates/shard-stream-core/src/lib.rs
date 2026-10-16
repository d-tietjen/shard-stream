//! Stream-native identifiers, sequencing, placement, and bounded execution.

mod channel;
mod directory_ownership;
mod fencing;
mod ids;
#[cfg(unix)]
mod native_directory_budget;
#[cfg(unix)]
mod owned_directory_handle;
mod sequencer;
mod topology;

pub use channel::{
    Budgeted, ByteBoundedReceiver, ByteBoundedSender, ChannelConfigError, ChannelStats,
    TrySendError, byte_bounded_channel,
};
pub use directory_ownership::{
    DirectoryOwnership, DirectoryOwnershipError, DirectoryOwnershipUntilError,
    DirectoryValidationCost,
};
pub use fencing::{
    PendingWriteAdmission, PhysicalWriteGuard, ReplicaSourceIdentity, WriteFence, WriteFenceError,
};
pub use ids::{
    AssignmentEpoch, BatchId, BrokerId, LeaderEpoch, LogicalOffset, LogicalPartitionId, OriginId,
    PhysicalShardId, PlacementSequence, RecordId, RingEpoch, ShardId, StreamOffset, TopicId,
    TopicPartition, VirtualLaneId,
};
#[cfg(unix)]
pub use owned_directory_handle::{
    NativeDirectoryProgress, OwnedDirectoryEntries, OwnedDirectoryHandle, OwnedEntryKind,
    OwnedFileAccess, OwnedFileHandle, RetainedDirectoryError,
};
#[cfg(unix)]
pub use owned_directory_handle::{
    NativeFileIdentity, NativeFileReplacement, NativeFileReplacementCost,
    NativeFileReplacementFailure, NativeFileReplacementLimits, NativeFileReplacementOutcome,
    NativeFileReplacementPhase, RetainedNativeFileReplacementError, native_file_replacement_cost,
};
#[cfg(unix)]
pub use owned_directory_handle::{
    OwnedShardDirectory, OwnedShardDirectoryCost, OwnedShardDirectoryFailure,
    RetainedOwnedShardDirectoryError, owned_shard_directory_cost,
};
pub use sequencer::{
    Placement, Reservation, ReservationProposal, SequencerError, SequencerState, TopicSequencer,
};
pub use topology::{
    AssignmentProvider, BrokerDescriptor, ExtentDescriptor, PartitionAssignment, ReplicaProgress,
    SingleNodeAssignment,
};

#[cfg(unix)]
pub use native_directory_budget::{
    NativeDirectoryBudget, NativeDirectoryCost, NativeDirectoryFailure, NativeDirectoryLimits,
    NativeDirectoryUsage, native_directory_cost,
};
