use std::error::Error;
use std::fmt;
use std::sync::Arc;

use crate::{BrokerId, Reservation, ReservationProposal, TopicPartition};

/// Trusted transport context associated with an authenticated replica sender.
///
/// A host constructs this from its verified connection identity, never from
/// replica payload fields. The fingerprint convention and its binding to
/// finalized membership are the host's responsibility. These fields alone
/// establish neither authentication nor current leader authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaSourceIdentity {
    /// Broker matched to the transport's verified credential.
    pub broker_id: BrokerId,
    /// Exact credential-bound broker incarnation.
    pub incarnation: u64,
    /// Fingerprint of the verified credential using the host's fixed convention.
    pub credential_fingerprint: [u8; 32],
}

/// Pending authority for one admitted write, without physical write permission.
///
/// Implementations join the original admission to the same transition state at
/// physical mutation. Its methods must be bounded and nonblocking, using
/// already verified local state rather than metadata or network I/O.
pub trait PendingWriteAdmission: Send + Sync + fmt::Debug {
    /// Refuses authority already revoked during preflight or capacity waits.
    ///
    /// This check precedes a new reservation. It is not a settlement guard and
    /// cannot by itself synchronize later revocation with physical mutation.
    fn validate_before_reserve(&self) -> Result<(), WriteFenceError>;

    /// Obtains physical authority for exact prospective coordinates before any
    /// offset, producer cursor or atomic-group state is allocated.
    ///
    /// The worker has already charged and queued its input. Commit must compare
    /// this same proposal and retain the returned guard through local WAL/sync
    /// settlement. The proposal is not an allocated reservation. Implementations
    /// must validate its genuine topic, ring and epoch with the same transition
    /// state as their reservation admission; no metadata I/O is permitted.
    /// Existing tickets fail closed until their host implements this boundary.
    fn try_admit_proposal(
        &self,
        _proposal: &ReservationProposal,
    ) -> Result<Box<dyn PhysicalWriteGuard>, WriteFenceError> {
        Err(WriteFenceError::new(
            "physical reservation-proposal admission is not implemented",
        ))
    }

    /// Joins the exact reservation to current authority at physical admission.
    ///
    /// The returned guard must prevent conflicting effective ownership from
    /// publishing until physical settlement. Implementations validate the
    /// actual partition, epoch and ring, current role, revocation and genuine
    /// lease deadline. A queued ticket carries no permission before this call.
    fn try_admit_physical(
        &self,
        reservation: &Reservation,
    ) -> Result<Box<dyn PhysicalWriteGuard>, WriteFenceError>;
}

/// Owned authority retained until an admitted physical operation settles.
///
/// Drop releases only bounded local ownership and wakes pending control work;
/// it must not perform I/O or synchronously wait for a transition. This is a
/// local admission boundary, not a guarantee that fsync finishes before lease
/// expiry or that distributed finality has remained unchanged.
pub trait PhysicalWriteGuard: Send + fmt::Debug {}

/// Product-neutral write-admission hook.
///
/// The open-source runtime does not install a fence. Private or embedded
/// deployments can provide one to reject stale writer epochs before the
/// engine reserves or mutates durable state.
/// An accepted epoch identifies each new reservation and its persisted
/// placement; an exact retry retains its original receipt and placement.
///
/// The default preparation methods retain check-only behavior and return no
/// ticket. Hosts requiring physical transition synchronization must override
/// them with the genuine pending-admission contract; the default check alone
/// cannot establish that stronger boundary.
pub trait WriteFence: Send + Sync + fmt::Debug {
    fn check_write(
        &self,
        topic_partition: TopicPartition,
        writer_epoch: u64,
    ) -> Result<(), WriteFenceError>;

    /// Checks current replica authority without acquiring a physical ticket.
    ///
    /// Guarded bound replicas use this before exact duplicate lookup and again
    /// before returning an immutable cached receipt. Implementations verify the
    /// trusted source's current remote-leader role using bounded local state,
    /// without ticket capacity, metadata I/O or local-leader permission. A
    /// successful check cannot authorize a new physical operation. The default
    /// refuses until a strong host implements its actual replica authority.
    fn check_replica_write(
        &self,
        _topic_partition: TopicPartition,
        _writer_epoch: u64,
        _source: &ReplicaSourceIdentity,
    ) -> Result<(), WriteFenceError> {
        Err(WriteFenceError::new(
            "read-only replica authority is not implemented",
        ))
    }

    /// Declares an immutable installation requirement for physical tickets.
    ///
    /// This is not current readiness. A host returning true must prepare a
    /// genuine ticket or refuse each operation; a check-only host returns false.
    /// The engine validates the fixed command overhead before opening storage.
    fn requires_physical_admission(&self) -> bool {
        false
    }

    /// Prepares a local-leader append for later physical admission.
    ///
    /// The default preserves existing check-only implementations.
    fn prepare_write(
        &self,
        topic_partition: TopicPartition,
        writer_epoch: u64,
    ) -> Result<Option<Arc<dyn PendingWriteAdmission>>, WriteFenceError> {
        self.check_write(topic_partition, writer_epoch)?;
        Ok(None)
    }

    /// Prepares a replica append without authenticated sender context.
    ///
    /// The default preserves existing implementations by using their write
    /// preparation. A host requiring verified replica identity must refuse
    /// this path explicitly, including when it is the current local leader.
    /// Local-leader permission alone must not authenticate an incoming replica.
    fn prepare_unbound_replica_write(
        &self,
        topic_partition: TopicPartition,
        writer_epoch: u64,
    ) -> Result<Option<Arc<dyn PendingWriteAdmission>>, WriteFenceError> {
        self.prepare_write(topic_partition, writer_epoch)
    }

    /// Prepares an authenticated replica append with trusted sender context.
    ///
    /// A host override verifies current remote-leader authority without granting
    /// a follower a local writable-leader lease. The default preserves the
    /// existing unbound preparation and does not authenticate the supplied
    /// context. Refusing unbound replicas therefore also refuses the default
    /// handling of an unverified context.
    fn prepare_replica_write(
        &self,
        topic_partition: TopicPartition,
        writer_epoch: u64,
        _source: &ReplicaSourceIdentity,
    ) -> Result<Option<Arc<dyn PendingWriteAdmission>>, WriteFenceError> {
        self.prepare_unbound_replica_write(topic_partition, writer_epoch)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteFenceError {
    message: String,
}

impl WriteFenceError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for WriteFenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for WriteFenceError {}
