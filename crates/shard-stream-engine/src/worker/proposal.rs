//! Charged proposal queueing, physical permission and exact coordinator commit.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use bytes::Bytes;
use shard_stream_core::{
    Budgeted, PendingWriteAdmission, PhysicalWriteGuard, Reservation, ShardId,
};
use shardlog::{AtomicGroupSequence, ProducerSequence};

use super::{
    AppendCommand, AppendDurability, AppendFailure, AppendReply, ShardCommand, ShardHandle,
    append_command_charge,
};
use crate::PartitionReplicationPolicy;
use crate::coordinator::{
    CommitControl, CommitOutcome, CoordinatorRouter, PreparedAppend, PreparedReplica,
    ReserveOutcome,
};
use crate::{EngineError, EngineResult};

pub(crate) enum ProposedAppendOutcome {
    /// No sequencing, producer or atomic-group mutation occurred.
    Unallocated(EngineError),
    /// Caller disappeared or a worker stopped without an authoritative result.
    Unknown(EngineError),
    Duplicate(ReserveOutcome),
    Committed {
        reservation: Reservation,
        producer: Option<ProducerSequence>,
        replication_policy: PartitionReplicationPolicy,
        result: Result<AppendDurability, AppendFailure>,
    },
}

pub(crate) struct ProposedAppendTicket {
    receiver: Option<Receiver<ProposedAppendOutcome>>,
    control: Arc<CommitControl>,
    shard_id: ShardId,
}

impl ProposedAppendTicket {
    pub(crate) fn wait(mut self) -> ProposedAppendOutcome {
        self.receiver
            .take()
            .expect("proposal result receiver is present")
            .recv()
            .unwrap_or_else(|_| {
                ProposedAppendOutcome::Unknown(EngineError::WorkerStopped(self.shard_id))
            })
    }
}

impl Drop for ProposedAppendTicket {
    fn drop(&mut self) {
        // A queued operation can be abandoned without detaching future allocation
        // from physical ownership. Once commit wins, only its worker settles it.
        self.control.cancel();
    }
}

#[derive(Clone, Copy)]
pub(super) enum PreparedInput {
    Leader(PreparedAppend),
    Replica(PreparedReplica),
}

impl PreparedInput {
    fn atomic_group(self) -> Option<AtomicGroupSequence> {
        let (identity, payload_digest) = match self {
            Self::Leader(prepared) => {
                (prepared.intent.atomic_group, prepared.intent.payload_digest)
            }
            Self::Replica(prepared) => {
                let intent = prepared.intent();
                (intent.atomic_group, intent.payload_digest)
            }
        };
        identity.map(|identity| AtomicGroupSequence {
            transaction_id: identity.transaction_id,
            chunk_sequence: identity.chunk_sequence,
            payload_digest,
        })
    }
}

pub(super) struct ProposedAppendCommand {
    prepared: PreparedInput,
    payload: Bytes,
    object_durable: bool,
    admission: Arc<dyn PendingWriteAdmission>,
    control: Arc<CommitControl>,
    coordinator: Arc<CoordinatorRouter>,
    pub(super) byte_charge: Option<Budgeted<()>>,
    response: SyncSender<ProposedAppendOutcome>,
}

impl ShardHandle {
    pub(crate) fn begin_proposed_append(
        &self,
        prepared: PreparedAppend,
        payload: Bytes,
        object_durable: bool,
        admission: Arc<dyn PendingWriteAdmission>,
        coordinator: Arc<CoordinatorRouter>,
    ) -> EngineResult<ProposedAppendTicket> {
        self.begin_prepared_append(
            PreparedInput::Leader(prepared),
            payload,
            object_durable,
            admission,
            coordinator,
        )
    }

    pub(crate) fn begin_prepared_replica(
        &self,
        prepared: PreparedReplica,
        payload: Bytes,
        admission: Arc<dyn PendingWriteAdmission>,
        coordinator: Arc<CoordinatorRouter>,
    ) -> EngineResult<ProposedAppendTicket> {
        self.begin_prepared_append(
            PreparedInput::Replica(prepared),
            payload,
            false,
            admission,
            coordinator,
        )
    }

    fn begin_prepared_append(
        &self,
        prepared: PreparedInput,
        payload: Bytes,
        object_durable: bool,
        admission: Arc<dyn PendingWriteAdmission>,
        coordinator: Arc<CoordinatorRouter>,
    ) -> EngineResult<ProposedAppendTicket> {
        let charged_bytes =
            append_command_charge(payload.len(), true).ok_or(EngineError::AdmissionLimited {
                shard_id: self.shard_id,
                reason: "proposal byte charge overflow",
            })?;
        let control = Arc::new(CommitControl::default());
        let (response, receiver) = sync_channel(1);
        self.try_send(
            ShardCommand::ProposedAppend(Box::new(ProposedAppendCommand {
                prepared,
                payload,
                object_durable,
                admission,
                control: control.clone(),
                coordinator,
                byte_charge: None,
                response,
            })),
            charged_bytes,
        )?;
        Ok(ProposedAppendTicket {
            receiver: Some(receiver),
            control,
            shard_id: self.shard_id,
        })
    }
}

fn reply_without_wal(
    command: Box<ProposedAppendCommand>,
    outcome: ProposedAppendOutcome,
    guard: Option<Box<dyn PhysicalWriteGuard>>,
) {
    let ProposedAppendCommand {
        payload,
        admission,
        control,
        byte_charge,
        response,
        ..
    } = *command;
    if matches!(&outcome, ProposedAppendOutcome::Unallocated(_)) {
        control.cancel();
    }
    drop(guard);
    drop(payload);
    drop(admission);
    drop(byte_charge);
    let _ = response.send(outcome);
}

fn reply_unallocated(
    command: Box<ProposedAppendCommand>,
    error: EngineError,
    guard: Option<Box<dyn PhysicalWriteGuard>>,
) {
    reply_without_wal(command, ProposedAppendOutcome::Unallocated(error), guard);
}

pub(super) fn resolve(command: ShardCommand) -> Option<ShardCommand> {
    let ShardCommand::ProposedAppend(command) = command else {
        return Some(command);
    };
    if command.control.is_cancelled() {
        reply_unallocated(
            command,
            EngineError::Fenced("queued proposal was cancelled before physical admission".into()),
            None,
        );
        return None;
    }
    let guard = match catch_unwind(AssertUnwindSafe(|| match &command.prepared {
        PreparedInput::Leader(prepared) => command.admission.try_admit_proposal(&prepared.proposal),
        PreparedInput::Replica(prepared) => command
            .admission
            .try_admit_physical(&prepared.reservation()),
    })) {
        Ok(Ok(guard)) => guard,
        Ok(Err(error)) => {
            reply_unallocated(command, EngineError::Fenced(error.to_string()), None);
            return None;
        }
        Err(_) => {
            reply_unallocated(
                command,
                EngineError::Fenced("physical proposal admission panicked".into()),
                None,
            );
            return None;
        }
    };
    let committed = match command.prepared {
        PreparedInput::Leader(prepared) => command
            .coordinator
            .commit_prepared(prepared, command.control.clone()),
        PreparedInput::Replica(prepared) => command
            .coordinator
            .commit_prepared_replica(prepared, command.control.clone()),
    };
    match committed {
        CommitOutcome::Unallocated(error) => {
            reply_unallocated(command, error, Some(guard));
            None
        }
        CommitOutcome::Unknown(error) => {
            // The coordinator has settled/stopped without confirming coordinates.
            // Do not manufacture an abort, index delivery or no-allocation proof.
            reply_without_wal(command, ProposedAppendOutcome::Unknown(error), Some(guard));
            None
        }
        CommitOutcome::Settled(duplicate @ ReserveOutcome::Duplicate { .. }) => {
            let ProposedAppendCommand {
                payload,
                admission,
                byte_charge,
                response,
                ..
            } = *command;
            drop(guard);
            drop(payload);
            drop(admission);
            drop(byte_charge);
            let _ = response.send(ProposedAppendOutcome::Duplicate(duplicate));
            None
        }
        CommitOutcome::Settled(ReserveOutcome::Reserved {
            reservation,
            producer,
            replication_policy,
        }) => {
            let ProposedAppendCommand {
                prepared,
                payload,
                object_durable,
                admission,
                byte_charge,
                response,
                ..
            } = *command;
            Some(ShardCommand::Append(AppendCommand {
                reservation,
                producer,
                atomic_group: prepared.atomic_group(),
                payload,
                object_durable,
                admission: Some(admission),
                _physical_guard: Some(guard),
                _byte_charge: byte_charge,
                response: AppendReply::Proposed {
                    replication_policy,
                    response,
                },
            }))
        }
    }
}
