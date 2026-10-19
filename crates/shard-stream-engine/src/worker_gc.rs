//! Consuming same-pin GC on the actual lane, with physical ticket settlement.

use super::*;
use shard_stream_core::{
    NativeDirectoryBudget, NativeDirectoryFailure, OwnedPinnedShardGcComplete,
    OwnedPinnedShardGcReport, RetainedOwnedPinnedShardGcError,
};
use shardlog::{PinnedShardGcChecks, PinnedShardGcLimits, PinnedShardGcReport};
use std::any::Any;
use std::collections::TryReserveError;
use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::sync::mpsc::RecvError;

/// Ordinary finite GC policy; its checks must use the actual composing health.
///
/// This has no authority constructor or Session. The composing host supplies
/// the SAME native directory work and actual captured-health check/deny object.
#[derive(Clone)]
pub struct NativeResumeGcPolicy {
    /// SAME original/min-only cumulative directory work across primary phases.
    pub budget: Arc<NativeDirectoryBudget>,
    /// Actual native planning/index/path/IO/outcome ceilings.
    pub limits: PinnedShardGcLimits,
    /// Actual composing work/health denial, required before native settlement.
    pub checks: Arc<dyn PinnedShardGcChecks + Send + Sync>,
}

/// Actual result of one physically completed lane collection command.
///
/// A successful native report survives a later caller/checkpoint refusal.
/// Native refusal retains the whole actual Core store and original root.
pub struct NativeGcOutcome {
    error: Option<EngineError>,
    budget_error: Option<NativeDirectoryFailure>,
    panic_payload: Option<Box<dyn Any + Send>>,
    deny_panic_payload: Option<Box<dyn Any + Send>>,
    allocation_error: Option<TryReserveError>,
    floors: Option<HashMap<TopicPartition, LogicalOffset>>,
    policy: Option<NativeResumeGcPolicy>,
    // Last: all work/checks/input/result diagnostics settle before same custody.
    native: Option<Result<OwnedPinnedShardGcReport, RetainedOwnedPinnedShardGcError>>,
}
impl NativeGcOutcome {
    fn new() -> Self {
        Self {
            native: None,
            error: None,
            budget_error: None,
            panic_payload: None,
            deny_panic_payload: None,
            allocation_error: None,
            floors: None,
            policy: None,
        }
    }
    /// Actual successful or refused native/Core report, without owner transfer.
    pub fn native_report(&self) -> Option<&PinnedShardGcReport> {
        match self.native.as_ref()? {
            Ok(report) => Some(report.report()),
            Err(error) => error.native_report(),
        }
    }
    /// Actual Core refusal retaining the consumed store/pin/root.
    pub fn native_error(&self) -> Option<&RetainedOwnedPinnedShardGcError> {
        self.native.as_ref().and_then(|v| v.as_ref().err())
    }
    /// Actual pre-entry queue/lane or checkpoint refusal.
    pub fn error(&self) -> Option<&EngineError> {
        self.error.as_ref()
    }
    /// Actual SAME-budget result observed after native completion.
    pub fn budget_error(&self) -> Option<&NativeDirectoryFailure> {
        self.budget_error.as_ref()
    }
    /// Actual bounded map allocation refusal.
    pub fn allocation_error(&self) -> Option<&TryReserveError> {
        self.allocation_error.as_ref()
    }
    /// Actual command/checkpoint unwind; nested native payloads remain native.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.panic_payload.as_deref()
    }
    /// Actual mandatory-denial unwind retained independently.
    pub fn deny_panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.deny_panic_payload.as_deref()
    }
    pub(crate) fn accepted(&self) -> bool {
        matches!(self.native, Some(Ok(_)))
            && self.error.is_none()
            && self.budget_error.is_none()
            && self.panic_payload.is_none()
            && self.deny_panic_payload.is_none()
    }
    fn deny(&mut self, policy: &NativeResumeGcPolicy) {
        policy.budget.deny();
        // Core attempted its supplied denial while still holding the actual
        // pin guard. Never repeat a callback that may already have unwound.
        if matches!(self.native, Some(Err(_))) {
            return;
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| policy.checks.deny())) {
            self.deny_panic_payload = Some(payload);
        }
    }
    fn retain_command_input(&mut self, mut command: Command) {
        command.reply.disarm();
        self.floors = Some(command.floors);
    }
}
impl fmt::Debug for NativeGcOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeGcOutcome")
            .field("accepted", &self.accepted())
            .field("error", &self.error)
            .field("budget_error", &self.budget_error)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for NativeGcOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("actual native GC command refused")
    }
}
impl Error for NativeGcOutcome {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(error) = self.native_error() {
            return Some(error);
        }
        if let Some(error) = self.budget_error.as_ref() {
            return Some(error);
        }
        if let Some(error) = self.allocation_error.as_ref() {
            return Some(error);
        }
        self.error.as_ref().map(|v| v as &dyn Error)
    }
}

struct Reply {
    shard: ShardId,
    response: Option<SyncSender<NativeGcOutcome>>,
}
impl Reply {
    fn complete(mut self, outcome: NativeGcOutcome) {
        if let Some(sender) = self.response.take() {
            let _ = sender.send(outcome);
        }
    }
    fn disarm(&mut self) {
        self.response.take();
    }
}
impl Drop for Reply {
    fn drop(&mut self) {
        if let Some(sender) = self.response.take() {
            let mut outcome = NativeGcOutcome::new();
            outcome.error = Some(EngineError::WorkerStopped(self.shard));
            let _ = sender.send(outcome);
        }
    }
}

pub(super) struct Command {
    floors: HashMap<TopicPartition, LogicalOffset>,
    policy: NativeResumeGcPolicy,
    expiry: Instant,
    byte_charge: Option<Budgeted<()>>,
    // Last: inputs and actual channel charge drop before fallback completion.
    reply: Reply,
}

/// A stored actual receiver is physically drained even on caller unwind/drop.
pub(crate) struct GcTicket {
    receiver: Option<Receiver<NativeGcOutcome>>,
}
impl GcTicket {
    pub(crate) fn settle(&mut self) -> Result<NativeGcOutcome, RecvError> {
        let Some(receiver) = self.receiver.as_ref() else {
            return Err(RecvError);
        };
        let observed = receiver.recv();
        self.receiver.take();
        observed
    }
}
impl Drop for GcTicket {
    fn drop(&mut self) {
        if let Some(receiver) = self.receiver.as_ref() {
            let observed = receiver.recv();
            drop(observed);
        }
    }
}

const PRIMARY_KIND_DIAGNOSTIC: &str =
    "bounded consuming GC requires the actual owned primary store";
const COMMAND_ARITHMETIC_DIAGNOSTIC: &str = "native GC command arithmetic refused";
// At most one of these mutually exclusive bridge-owned String buffers is
// produced by a command. Its headers are already in the typed outcome values.
pub(crate) const COMMAND_DIAGNOSTIC_BYTES: usize =
    if PRIMARY_KIND_DIAGNOSTIC.len() > COMMAND_ARITHMETIC_DIAGNOSTIC.len() {
        PRIMARY_KIND_DIAGNOSTIC.len()
    } else {
        COMMAND_ARITHMETIC_DIAGNOSTIC.len()
    };

/// Source-visible queue/map/channel/typed result ceiling, separate from Core IO.
pub(crate) fn command_cost() -> Option<usize> {
    [
        size_of::<Command>(),
        size_of::<GcTicket>(),
        size_of::<NativeGcOutcome>(),
        size_of::<Result<NativeGcOutcome, RecvError>>(),
        size_of::<OwnedPinnedShardGcReport>(),
        size_of::<Reply>(),
        size_of::<NativeResumeGcPolicy>(),
        size_of::<NativeResumeGcPolicy>(),
        size_of::<(TopicPartition, LogicalOffset)>().checked_mul(4)?,
        size_of::<SyncSender<NativeGcOutcome>>(),
        size_of::<Receiver<NativeGcOutcome>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
        COMMAND_DIAGNOSTIC_BYTES,
    ]
    .into_iter()
    .try_fold(0usize, usize::checked_add)
}
// Thirteen fixed value/result requests, one owned diagnostic buffer, four
// floor-map slots and bounded policy/Arc-clone/handoff entry fees. Library/kernel
// allocator overhead is outside this declared logical source domain.
pub(crate) const COMMAND_ENTRIES: usize = 25;
impl ShardHandle {
    #[allow(
        clippy::result_large_err,
        reason = "return the actual retained store and result custody without a refusal allocation"
    )]
    pub(crate) fn begin_owned_gc(
        &self,
        tp: TopicPartition,
        start: LogicalOffset,
        policy: &NativeResumeGcPolicy,
        expiry: Instant,
    ) -> Result<GcTicket, NativeGcOutcome> {
        let mut outcome = NativeGcOutcome::new();
        let admitted = (|| {
            policy.budget.cap_expiry(expiry)?;
            let bytes = command_cost().ok_or(NativeDirectoryFailure::Capacity)?;
            policy.budget.reserve(
                0,
                0,
                bytes,
                COMMAND_DIAGNOSTIC_BYTES,
                COMMAND_ENTRIES,
                bytes,
            )?;
            policy.budget.check()
        })();
        if let Err(error) = admitted {
            outcome.budget_error = Some(error);
            outcome.deny(policy);
            return Err(outcome);
        }
        outcome.policy = Some(policy.clone());
        outcome.floors = Some(HashMap::new());
        let Some(floors) = outcome.floors.as_mut() else {
            return Err(outcome);
        };
        outcome.allocation_error = floors.try_reserve(1).err();
        if outcome.allocation_error.is_some() {
            outcome.deny(policy);
            return Err(outcome);
        }
        floors.insert(tp, start);
        if let Err(error) = policy.budget.check() {
            outcome.budget_error = Some(error);
            outcome.deny(policy);
            return Err(outcome);
        }
        let Some(floors) = outcome.floors.take() else {
            outcome.deny(policy);
            return Err(outcome);
        };
        let (response, receiver) = sync_channel(1);
        let command = Command {
            floors,
            policy: policy.clone(),
            expiry,
            byte_charge: None,
            reply: Reply {
                shard: self.shard_id,
                response: Some(response),
            },
        };
        if let Err(error) = policy.budget.check() {
            outcome.budget_error = Some(error);
            outcome.retain_command_input(command);
            outcome.deny(policy);
            return Err(outcome);
        }
        let bytes = command_cost().and_then(|n| n.checked_add(COMMAND_OVERHEAD_BYTES));
        let Some(bytes) = bytes else {
            outcome.error = Some(EngineError::InvalidConfig(
                COMMAND_ARITHMETIC_DIAGNOSTIC.into(),
            ));
            outcome.retain_command_input(command);
            outcome.deny(policy);
            return Err(outcome);
        };
        let Some(sender) = self.sender.as_ref() else {
            outcome.error = Some(EngineError::WorkerStopped(self.shard_id));
            outcome.retain_command_input(command);
            outcome.deny(policy);
            return Err(outcome);
        };
        match sender.try_send(ShardCommand::OwnedGc(command), bytes) {
            Ok(()) => Ok(GcTicket {
                receiver: Some(receiver),
            }),
            Err(error) => {
                let (command, error) = match error {
                    TrySendError::TooLarge { item: command, .. } => (
                        command,
                        EngineError::AdmissionLimited {
                            shard_id: self.shard_id,
                            reason: "native GC command exceeds byte queue",
                        },
                    ),
                    TrySendError::ByteBudget { item: command, .. } => (
                        command,
                        EngineError::AdmissionLimited {
                            shard_id: self.shard_id,
                            reason: "native GC byte queue exhausted",
                        },
                    ),
                    TrySendError::Full(command) => (
                        command,
                        EngineError::AdmissionLimited {
                            shard_id: self.shard_id,
                            reason: "native GC slot queue exhausted",
                        },
                    ),
                    TrySendError::Disconnected(command) => {
                        (command, EngineError::WorkerStopped(self.shard_id))
                    }
                };
                if let ShardCommand::OwnedGc(command) = command {
                    outcome.retain_command_input(command);
                }
                outcome.error = Some(error);
                outcome.deny(policy);
                Err(outcome)
            }
        }
    }
}

pub(super) fn retain_charge(command: &mut Command, charge: Budgeted<()>) {
    command.byte_charge = Some(charge);
}

pub(super) fn execute(stores: &mut LaneStores, command: Command) {
    #[cfg(test)]
    owned_store_tests::before_gc(
        stores
            .primary
            .native()
            .and_then(|primary| primary.active_path().parent()),
    );
    let mut outcome = NativeGcOutcome::new();
    outcome.policy = Some(command.policy.clone());
    // Actual Core completion/error remains outside the command unwind boundary.
    let mut native: Option<Result<OwnedPinnedShardGcComplete, RetainedOwnedPinnedShardGcError>> =
        None;
    let observed = catch_unwind(AssertUnwindSafe(|| {
        command
            .policy
            .budget
            .cap_expiry(command.expiry)
            .map_err(|error| {
                outcome.budget_error = Some(error);
            })?;
        command.policy.budget.check().map_err(|error| {
            outcome.budget_error = Some(error);
        })?;
        let primary = std::mem::replace(&mut stores.primary, PrimaryStore::Vacant);
        let PrimaryStore::Owned(store) = primary else {
            stores.primary = primary;
            outcome.error = Some(EngineError::InvalidConfig(PRIMARY_KIND_DIAGNOSTIC.into()));
            return Err(());
        };
        native = Some(store.garbage_collect_until(
            &command.floors,
            command.policy.limits,
            &command.policy.budget,
            command.policy.checks.as_ref(),
        ));
        Ok(())
    }));
    match observed {
        Ok(_) => {}
        Err(payload) => outcome.panic_payload = Some(payload),
    }
    if let Some(observed) = native {
        outcome.native = Some(match observed {
            Ok(complete) => {
                let (store, report) = complete.into_store_and_report();
                stores.primary = PrimaryStore::Owned(store);
                Ok(report)
            }
            Err(error) => Err(error),
        });
    }
    if let Err(error) = command.policy.budget.check() {
        outcome.budget_error = Some(error);
    }
    if !outcome.accepted() {
        outcome.deny(&command.policy);
    } else {
        outcome.policy.take();
    }
    let Command {
        floors,
        policy,
        expiry: _,
        byte_charge,
        reply,
    } = command;
    drop(floors);
    drop(byte_charge);
    drop(policy);
    reply.complete(outcome);
}
