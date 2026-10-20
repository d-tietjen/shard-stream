//! Consuming canonical mutation while the original Core directory drops last.
//!
//! The native write/GC result stays whole across both fresh parent validations.
//! No File, native owner, arbitrary panic value, counter or callback escapes.

use super::{
    Control, OwnedPinnedShardLog, OwnedPinnedShardLogFailure, OwnedPinnedShardLogValidation,
    Scope as ConstructorContext, Validation, ValidationChecks, Work,
};
use crate::{
    CheckedDirectoryValidationChecks, NativeDirectoryBudget, NativeDirectoryFailure,
    NativeDirectoryLimits, NativeDirectoryUsage, OwnedDirectoryHandle, OwnedShardDirectory,
    checked_directory_validation_cost, native_directory_cost,
};
use bytes::Bytes;
use shardlog::{
    OpenedPinnedShardLog, PinnedShardAppendBudget, PinnedShardAppendLimits, PinnedShardAppendUsage,
    PinnedShardGcLimits, PinnedShardLogGcChecks, PinnedShardLogGcComplete, PinnedShardLogGcError,
    PinnedShardLogGcObservation, PinnedShardLogWriteChecks, PinnedShardLogWriteComplete,
    PinnedShardLogWriteError, PinnedShardLogWriteLimits, PinnedShardLogWriteObservation,
    ShardDirectoryCheckFailure, ShardDirectoryChecks, ShardLogAppendReceipt,
    ShardLogCompactionReceipt, StorageFetchBudget, StorageFetchCost, StorageFetchFailure,
    StorageFetchLimits, StorageFetchUsage, pinned_shard_append_cost, pinned_shard_log_gc_cost,
    pinned_shard_log_write_cost, storage_fetch_cost,
};
use std::{
    any::Any,
    cell::{Cell, RefCell},
    error::Error,
    fmt,
    mem::size_of,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{MutexGuard, TryLockError, TryLockResult},
    time::Instant,
};

type PinBorrow<'a> = Option<TryLockResult<MutexGuard<'a, OwnedShardDirectory>>>;
type Step<T> = Result<T, OwnedPinnedShardLogMutationFailure>;

/// Per-mutation Core framing and separate incremental native framing facts.
///
/// Prepay Core, shared fetch and cumulative append policies once separately.
/// Add this framing for every actual call; the producers still spend those
/// SAME counters. These facts neither admit work nor bound allocator/RSS use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnedPinnedShardLogMutationCost {
    /// Independent Core bridge values and two retained validation frames.
    pub declared_bytes: usize,
    /// Bridge-owned string bytes; real validation owns its own path fees.
    pub string_bytes: usize,
    /// Independent bridge, parent-retain and validation slots.
    pub entries: usize,
    /// Bridge and validation work, including entry work exactly once.
    pub work_bytes: usize,
    /// Two checked validation frames already included above.
    pub validation_framing_bytes: usize,
    /// Two checked validation frame slot counts already included above.
    pub validation_framing_entries: usize,
    /// Two actual parent retains; no shared directory clone is made.
    pub parent_retention_bytes: usize,
    /// This bridge plus two fresh checked parent validations.
    pub core_operations: usize,
    /// Native producer framing, excluding the shared fetch and append policies.
    pub native_framing: StorageFetchCost,
}

/// Terminal bridge category; the original inputs/results remain privately owned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnedPinnedShardLogMutationFailure {
    /// Same cumulative Core counters or current minimum expiry refused.
    Core,
    /// Same cumulative fetch counters refused.
    Native,
    /// The supplied cumulative append budget was already refused.
    Append,
    /// Actual captured work/health refused.
    Checkpoint,
    /// The actual fresh prevalidation refused.
    PreValidation,
    /// Immediate access to the SAME native pin was busy or poisoned.
    PinBorrow,
    /// The real canonical write, seal or compaction producer refused.
    Mutation,
    /// Fresh postvalidation refused after the actual native result returned.
    PostValidation,
    /// The actual bridge or composing callback unwound.
    Unwind,
    /// An internal whole-owner transition could not complete.
    Invariant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    FixedAdmission,
    Split,
    PreRetain,
    PreValidation,
    PinBorrowed,
    Native,
    PostRetain,
    PostValidation,
    Usage,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BorrowFailure {
    WouldBlock,
    Poisoned,
}
#[derive(Clone, Copy)]
enum Kind {
    Append,
    Seal,
    Compact(PinnedShardGcLimits),
}
struct Request {
    kind: Kind,
    sequence: u64,
    records: Option<Vec<Bytes>>,
    original_capacity: usize,
    durable: bool,
    write: PinnedShardLogWriteLimits,
}
#[allow(
    clippy::large_enum_variant,
    reason = "whole actual outcomes use prepaid stack custody without an unpaid heap allocation"
)]
enum Native {
    Write(Result<PinnedShardLogWriteComplete, PinnedShardLogWriteError>),
    Gc(Result<PinnedShardLogGcComplete, PinnedShardLogGcError>),
}
#[allow(
    clippy::large_enum_variant,
    reason = "whole actual completion retains native ownership without an unpaid heap allocation"
)]
enum CompletedNative {
    Write(PinnedShardLogWriteComplete),
    Gc(PinnedShardLogGcComplete),
}
impl Native {
    fn is_ok(&self) -> bool {
        match self {
            Self::Write(v) => v.is_ok(),
            Self::Gc(v) => v.is_ok(),
        }
    }
    fn observation(&self) -> OwnedPinnedShardLogMutationObservation<'_> {
        let actual = match self {
            Self::Write(Ok(v)) => Observation::Write(v.observation()),
            Self::Write(Err(v)) => Observation::Write(v.observation()),
            Self::Gc(Ok(v)) => Observation::Gc(v.observation()),
            Self::Gc(Err(v)) => Observation::Gc(v.observation()),
        };
        OwnedPinnedShardLogMutationObservation { actual }
    }
}
impl CompletedNative {
    fn observation(&self) -> OwnedPinnedShardLogMutationObservation<'_> {
        let actual = match self {
            Self::Write(v) => Observation::Write(v.observation()),
            Self::Gc(v) => Observation::Gc(v.observation()),
        };
        OwnedPinnedShardLogMutationObservation { actual }
    }
    fn into_log(self) -> OpenedPinnedShardLog {
        match self {
            Self::Write(v) => v.into_log(),
            Self::Gc(v) => v.into_log(),
        }
    }
}

#[derive(Default)]
struct AppendDenial {
    attempted: Cell<bool>,
    returned: Cell<bool>,
    actual: RefCell<Option<Result<(), Box<dyn Any + Send>>>>,
}
impl AppendDenial {
    fn deny(&self, append: &mut PinnedShardAppendBudget) {
        if !self.attempted.replace(true) {
            let actual = catch_unwind(AssertUnwindSafe(|| {
                append.deny();
                self.returned.set(true);
            }));
            *self.actual.borrow_mut() = Some(actual);
        }
    }
}
fn checkpoint(work: &Work<'_>) -> Step<()> {
    work.check().map_err(|v| match v {
        OwnedPinnedShardLogFailure::Core => OwnedPinnedShardLogMutationFailure::Core,
        OwnedPinnedShardLogFailure::Native => OwnedPinnedShardLogMutationFailure::Native,
        OwnedPinnedShardLogFailure::Checkpoint => OwnedPinnedShardLogMutationFailure::Checkpoint,
        _ => OwnedPinnedShardLogMutationFailure::Invariant,
    })
}
struct ParentChecks<'a, 'b, 'c> {
    work: &'a Work<'b>,
    append: RefCell<&'c mut PinnedShardAppendBudget>,
    denial: &'a AppendDenial,
}
impl ShardDirectoryChecks for ParentChecks<'_, '_, '_> {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        checkpoint(self.work).map_err(|_| ShardDirectoryCheckFailure::Refused)
    }
}
impl CheckedDirectoryValidationChecks for ParentChecks<'_, '_, '_> {
    fn deny(&self) {
        // Release the actual append borrow before calling captured health.
        {
            self.denial.deny(&mut self.append.borrow_mut());
        }
        ValidationChecks(self.work).deny();
    }
}
struct NativeChecks<'a, 'b>(&'a Work<'b>);
impl ShardDirectoryChecks for NativeChecks<'_, '_> {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        checkpoint(self.0).map_err(|_| ShardDirectoryCheckFailure::Refused)
    }
}
impl NativeChecks<'_, '_> {
    fn deny_captured(&self) {
        // The canonical producer owns the exclusive append borrow. Its actual
        // write settlement and tracked GC perform their own append denial.
        // Shared fetch/tracking may also call here; do not invent an append
        // observation or alias that exclusive borrow in those callbacks.
        self.0.control.borrow_mut().native_deny_attempted = true;
        self.0.deny_core();
        self.0.deny_outer();
    }
}
impl PinnedShardLogWriteChecks for NativeChecks<'_, '_> {
    fn deny(&self) {
        self.deny_captured();
    }
}
impl PinnedShardLogGcChecks for NativeChecks<'_, '_> {
    fn deny(&self) {
        self.deny_captured();
    }
}

#[derive(Default)]
struct Effects {
    cap: Option<Result<(), NativeDirectoryFailure>>,
    cost: Option<Result<OwnedPinnedShardLogMutationCost, NativeDirectoryFailure>>,
    fee: Option<Result<(), NativeDirectoryFailure>>,
    original_append_expiry: Option<Instant>,
    minimum_expiry: Option<Instant>,
    append_usage: Option<PinnedShardAppendUsage>,
    append_refused: Option<bool>,
    core_usage: Option<Result<NativeDirectoryUsage, NativeDirectoryFailure>>,
    native_usage: Option<Result<StorageFetchUsage, StorageFetchFailure>>,
    entered: Option<Phase>,
    pin_borrowed: bool,
    borrow_failure: Option<BorrowFailure>,
    operation_panic: Option<Box<dyn Any + Send>>,
    control: Option<Control>,
    append_denial: Option<AppendDenial>,
}
struct Operation {
    effects: Effects,
    request: Request,
    pre_validation: Option<Validation>,
    post_validation: Option<Validation>,
    pending_handle: Option<OwnedDirectoryHandle>,
    native: Option<Native>,
    remaining_native: Option<OpenedPinnedShardLog>,
}
struct Scope {
    operation: Operation,
    early: Option<OwnedPinnedShardLog>,
    // LAST: original constructor reports, then SAME native pin and Core parent.
    context: Option<ConstructorContext>,
}
impl Scope {
    fn new(input: OwnedPinnedShardLog, request: Request) -> Self {
        Self {
            operation: Operation {
                effects: Effects::default(),
                request,
                pre_validation: None,
                post_validation: None,
                pending_handle: None,
                native: None,
                remaining_native: None,
            },
            early: Some(input),
            context: None,
        }
    }
    fn prepare(&mut self, append: &mut PinnedShardAppendBudget, work: &Work<'_>) -> Step<()> {
        let effects = &mut self.operation.effects;
        effects.original_append_expiry = Some(append.absolute_expiry());
        effects.cap = Some(
            work.core
                .cap_expiry(append.absolute_expiry().min(work.native.absolute_expiry())),
        );
        effects.minimum_expiry = Some(work.core.absolute_expiry());
        append.cap_expiry(work.core.absolute_expiry());
        if matches!(effects.cap, Some(Err(_))) {
            return Err(OwnedPinnedShardLogMutationFailure::Core);
        }
        checkpoint(work)?;
        if append.is_refused() {
            return Err(OwnedPinnedShardLogMutationFailure::Append);
        }
        let request = &self.operation.request;
        effects.cost = Some(mutation_cost(
            work.core.limits(),
            work.native.limits(),
            append.limits(),
            request.write,
            match request.kind {
                Kind::Compact(gc) => Some(gc),
                _ => None,
            },
        ));
        if matches!(effects.cost, Some(Err(_))) {
            return Err(OwnedPinnedShardLogMutationFailure::Core);
        }
        let (bytes, entries, cpu) =
            fixed_fee().ok_or(OwnedPinnedShardLogMutationFailure::Invariant)?;
        self.operation.charge(work, 1, bytes, entries, cpu)?;
        self.operation.effects.entered = Some(Phase::FixedAdmission);
        completed(Phase::FixedAdmission);
        checkpoint(work)?;
        self.context = Some(
            self.early
                .take()
                .ok_or(OwnedPinnedShardLogMutationFailure::Invariant)?
                .scope,
        );
        if matches!(self.operation.request.kind, Kind::Append)
            && self.operation.request.records.is_none()
        {
            return Err(OwnedPinnedShardLogMutationFailure::Invariant);
        }
        let actual = self
            .context
            .as_mut()
            .ok_or(OwnedPinnedShardLogMutationFailure::Invariant)?
            .native
            .take();
        match actual {
            Some(Ok(log)) => self.operation.remaining_native = Some(log),
            actual => {
                self.context
                    .as_mut()
                    .ok_or(OwnedPinnedShardLogMutationFailure::Invariant)?
                    .native = actual;
                return Err(OwnedPinnedShardLogMutationFailure::Invariant);
            }
        }
        self.operation.effects.entered = Some(Phase::Split);
        completed(Phase::Split);
        checkpoint(work)
    }
}
impl Operation {
    fn charge(
        &mut self,
        work: &Work<'_>,
        operations: usize,
        bytes: usize,
        entries: usize,
        cpu: usize,
    ) -> Step<()> {
        self.effects.fee = Some(work.core.reserve(operations, 0, bytes, 0, entries, cpu));
        if matches!(self.effects.fee, Some(Err(_))) {
            Err(OwnedPinnedShardLogMutationFailure::Core)
        } else {
            Ok(())
        }
    }
    fn retain_parent(
        &mut self,
        parent: &OwnedDirectoryHandle,
        work: &Work<'_>,
        phase: Phase,
    ) -> Step<()> {
        checkpoint(work)?;
        let (bytes, entries, cpu) =
            retain_fee().ok_or(OwnedPinnedShardLogMutationFailure::Invariant)?;
        self.charge(work, 0, bytes, entries, cpu)?;
        checkpoint(work)?;
        self.effects.entered = Some(phase);
        self.pending_handle = Some(parent.retain());
        completed(phase);
        checkpoint(work)
    }
    fn validate(
        &mut self,
        append: &mut PinnedShardAppendBudget,
        work: &Work<'_>,
        denial: &AppendDenial,
        before: bool,
    ) -> Step<()> {
        checkpoint(work)?;
        let input = self
            .pending_handle
            .take()
            .ok_or(OwnedPinnedShardLogMutationFailure::Invariant)?;
        let phase = if before {
            Phase::PreValidation
        } else {
            Phase::PostValidation
        };
        self.effects.entered = Some(phase);
        let actual = input.validate_for_storage_fetch_until(
            work.core,
            work.native,
            &ParentChecks {
                work,
                append: RefCell::new(append),
                denial,
            },
        );
        if before {
            self.pre_validation = Some(actual);
        } else {
            self.post_validation = Some(actual);
        }
        completed(phase);
        checkpoint(work)?;
        let actual = if before {
            self.pre_validation.as_ref()
        } else {
            self.post_validation.as_ref()
        };
        if matches!(actual, Some(Ok(_))) {
            Ok(())
        } else if before {
            Err(OwnedPinnedShardLogMutationFailure::PreValidation)
        } else {
            Err(OwnedPinnedShardLogMutationFailure::PostValidation)
        }
    }
    fn run<'a>(
        &mut self,
        directory: &'a super::OwnedShardStoreDirectory,
        borrowed: &mut PinBorrow<'a>,
        append: &mut PinnedShardAppendBudget,
        work: &Work<'_>,
        denial: &AppendDenial,
    ) -> Step<()> {
        self.retain_parent(&directory.inner.parent, work, Phase::PreRetain)?;
        self.validate(append, work, denial, true)?;
        checkpoint(work)?;
        self.effects.entered = Some(Phase::PinBorrowed);
        *borrowed = Some(directory.inner.native.try_lock());
        match borrowed.as_ref() {
            Some(Ok(_)) => self.effects.pin_borrowed = true,
            Some(Err(TryLockError::WouldBlock)) => {
                self.effects.borrow_failure = Some(BorrowFailure::WouldBlock)
            }
            Some(Err(TryLockError::Poisoned(_))) => {
                self.effects.borrow_failure = Some(BorrowFailure::Poisoned)
            }
            None => return Err(OwnedPinnedShardLogMutationFailure::Invariant),
        }
        completed(Phase::PinBorrowed);
        checkpoint(work)?;
        let pin = match borrowed.as_ref() {
            Some(Ok(pin)) => pin,
            Some(Err(_)) => return Err(OwnedPinnedShardLogMutationFailure::PinBorrow),
            None => return Err(OwnedPinnedShardLogMutationFailure::Invariant),
        };
        let actual = self
            .remaining_native
            .take()
            .ok_or(OwnedPinnedShardLogMutationFailure::Invariant)?;
        self.effects.entered = Some(Phase::Native);
        self.native = Some(match self.request.kind {
            Kind::Append => Native::Write(
                actual.append_group_until(
                    &pin.native,
                    self.request.sequence,
                    self.request
                        .records
                        .take()
                        .ok_or(OwnedPinnedShardLogMutationFailure::Invariant)?,
                    self.request.durable,
                    append,
                    work.native,
                    self.request.write,
                    &NativeChecks(work),
                ),
            ),
            Kind::Seal => Native::Write(actual.seal_until(
                &pin.native,
                append,
                work.native,
                self.request.write,
                &NativeChecks(work),
            )),
            Kind::Compact(gc) => Native::Gc(actual.compact_before_until(
                &pin.native,
                self.request.sequence,
                append,
                work.native,
                self.request.write,
                gc,
                &NativeChecks(work),
            )),
        });
        // The actual whole native Result is retained before hooks and post-E.
        self.effects.append_refused = Some(append.is_refused());
        self.effects.append_usage = Some(append.usage());
        completed(Phase::Native);
        checkpoint(work)?;
        if !self.native.as_ref().is_some_and(Native::is_ok) {
            return Err(OwnedPinnedShardLogMutationFailure::Mutation);
        }
        self.retain_parent(&directory.inner.parent, work, Phase::PostRetain)?;
        self.validate(append, work, denial, false)?;
        self.effects.core_usage = Some(work.core.usage());
        self.effects.native_usage = Some(work.native.usage());
        self.effects.entered = Some(Phase::Usage);
        completed(Phase::Usage);
        checkpoint(work)?;
        if matches!(self.effects.core_usage, Some(Err(_))) {
            return Err(OwnedPinnedShardLogMutationFailure::Core);
        }
        if matches!(self.effects.native_usage, Some(Err(_))) {
            return Err(OwnedPinnedShardLogMutationFailure::Native);
        }
        Ok(())
    }
}

enum Observation<'a> {
    Write(PinnedShardLogWriteObservation<'a>),
    Gc(PinnedShardLogGcObservation<'a>),
}
/// Safe scalar/data facade over the retained genuine mutation result.
/// No native wrapper, File, Any/panic payload or from-parts construction is exposed.
pub struct OwnedPinnedShardLogMutationObservation<'a> {
    actual: Observation<'a>,
}
impl OwnedPinnedShardLogMutationObservation<'_> {
    /// Actual complete native append receipt, copied without rebuilding it.
    /// A later Core refusal can still retain a native-complete receipt here.
    pub fn append_receipt(&self) -> Option<ShardLogAppendReceipt> {
        match &self.actual {
            Observation::Write(v) => v.receipt().copied(),
            _ => None,
        }
    }
    /// Actual native-complete compaction receipt, including after late Core refusal.
    pub fn compaction_receipt(&self) -> Option<ShardLogCompactionReceipt> {
        match &self.actual {
            Observation::Gc(v) => v.receipt().copied(),
            _ => None,
        }
    }
    /// Number of actual retired descriptor transfers on a write/seal.
    pub fn transferred_files(&self) -> Option<usize> {
        match &self.actual {
            Observation::Write(v) => Some(v.transferred_files()),
            _ => None,
        }
    }
    /// Actual GC quarantine count; no File or close witness is exposed.
    pub fn quarantined_files(&self) -> Option<usize> {
        match &self.actual {
            Observation::Gc(v) => Some(v.quarantined_files()),
            _ => None,
        }
    }
    /// Count of original physical write Results, including partial native failure.
    pub fn write_result_count(&self) -> Option<usize> {
        match &self.actual {
            Observation::Write(v) => Some(v.write_results().count()),
            _ => None,
        }
    }
    /// Actual independent GC work/denial panic occupancy; no Any escapes.
    pub fn gc_panic_observations(&self) -> Option<[bool; 3]> {
        match &self.actual {
            Observation::Gc(v) => {
                let denial = v.denial_panics();
                Some([v.panic_payload().is_some(), denial[0], denial[1]])
            }
            _ => None,
        }
    }
}

/// Successful whole mutation, with operation state before the original context.
pub struct OwnedPinnedShardLogMutationComplete {
    operation: Operation,
    native: CompletedNative,
    context: ConstructorContext,
}
impl OwnedPinnedShardLogMutationComplete {
    /// Actual native facts; these establish neither current authority nor serving.
    pub fn observation(&self) -> OwnedPinnedShardLogMutationObservation<'_> {
        self.native.observation()
    }
    /// Actual cumulative append usage retained at this operation's return.
    pub fn append_usage(&self) -> Option<PinnedShardAppendUsage> {
        self.operation.effects.append_usage
    }
    /// Actual saved append expiry and the minimum Core expiry at entry.
    pub fn entry_deadlines(&self) -> Option<(Instant, Instant)> {
        Some((
            self.operation.effects.original_append_expiry?,
            self.operation.effects.minimum_expiry?,
        ))
    }
    /// Actual fresh pre-mutation validation, independent of constructor history.
    pub fn pre_validation(&self) -> Option<OwnedPinnedShardLogValidation<'_>> {
        self.operation
            .pre_validation
            .as_ref()
            .map(|actual| OwnedPinnedShardLogValidation { actual })
    }
    /// Actual fresh post-mutation validation.
    pub fn post_validation(&self) -> Option<OwnedPinnedShardLogValidation<'_>> {
        self.operation
            .post_validation
            .as_ref()
            .map(|actual| OwnedPinnedShardLogValidation { actual })
    }
    /// Destroy settled mutation state before restoring the SAME native log.
    /// No new constructor, IO, callback, counter or deadline is created.
    pub fn into_log(self) -> OwnedPinnedShardLog {
        let Self {
            operation,
            native,
            mut context,
        } = self;
        drop(operation);
        context.native = Some(Ok(native.into_log()));
        OwnedPinnedShardLog { scope: context }
    }
}
/// Terminal refusal retaining uncertain effects and the actual original root LAST.
/// No retry, native/whole-log, raw error, File or Any extractor is provided.
pub struct RetainedOwnedPinnedShardLogMutationError {
    failure: OwnedPinnedShardLogMutationFailure,
    scope: Scope,
}
impl RetainedOwnedPinnedShardLogMutationError {
    /// Actual bridge terminal category.
    pub fn failure(&self) -> OwnedPinnedShardLogMutationFailure {
        self.failure
    }
    /// Actual cumulative append usage, including refused entered operations.
    pub fn append_usage(&self) -> Option<PinnedShardAppendUsage> {
        self.scope.operation.effects.append_usage
    }
    /// Actual saved append expiry and the minimum Core expiry at entry.
    pub fn entry_deadlines(&self) -> Option<(Instant, Instant)> {
        let effects = &self.scope.operation.effects;
        Some((effects.original_append_expiry?, effects.minimum_expiry?))
    }
    /// Actual sticky append refusal observed before returning this carrier.
    pub fn append_refused(&self) -> Option<bool> {
        self.scope.operation.effects.append_refused
    }
    /// Actual native return; None means no native result was returned.
    pub fn native_returned_success(&self) -> Option<bool> {
        self.scope.operation.native.as_ref().map(Native::is_ok)
    }
    /// Actual native diagnostic data, including a native Ok followed by Core refusal.
    pub fn observation(&self) -> Option<OwnedPinnedShardLogMutationObservation<'_>> {
        self.scope
            .operation
            .native
            .as_ref()
            .map(Native::observation)
    }
    /// Actual fresh prevalidation retained before any later refusal.
    pub fn pre_validation(&self) -> Option<OwnedPinnedShardLogValidation<'_>> {
        self.scope
            .operation
            .pre_validation
            .as_ref()
            .map(|actual| OwnedPinnedShardLogValidation { actual })
    }
    /// Actual fresh postvalidation, including a refusal or late returned result.
    pub fn post_validation(&self) -> Option<OwnedPinnedShardLogValidation<'_>> {
        self.scope
            .operation
            .post_validation
            .as_ref()
            .map(|actual| OwnedPinnedShardLogValidation { actual })
    }
    /// Actual bridge operation/Core/fetch/health/append denial panic occupancy.
    pub fn has_panic_payloads(&self) -> [bool; 5] {
        let e = &self.scope.operation.effects;
        let c = e.control.as_ref();
        [
            e.operation_panic.is_some(),
            c.is_some_and(|v| v.core_deny_panic.is_some()),
            c.is_some_and(|v| v.native_deny_panic.is_some()),
            c.is_some_and(|v| v.outer_deny_panic.is_some()),
            e.append_denial
                .as_ref()
                .is_some_and(|v| v.actual.borrow().as_ref().is_some_and(Result::is_err)),
        ]
    }
    /// Actual retained mutable-append denial attempt/normal return facts.
    pub fn append_denial(&self) -> (bool, bool) {
        self.scope
            .operation
            .effects
            .append_denial
            .as_ref()
            .map_or((false, false), |v| (v.attempted.get(), v.returned.get()))
    }
    /// Actual outer-health attempt/normal return facts; an unwind is not retried.
    pub fn outer_denial(&self) -> (bool, bool) {
        self.scope
            .operation
            .effects
            .control
            .as_ref()
            .map_or((false, false), |v| {
                (v.outer_deny_attempted, v.outer_deny_returned)
            })
    }
    /// Actual immediate pin borrow observations without a guard or poison carrier.
    pub fn pin_borrow_observations(&self) -> (bool, bool, bool) {
        let e = &self.scope.operation.effects;
        (
            e.pin_borrowed,
            e.borrow_failure == Some(BorrowFailure::WouldBlock),
            e.borrow_failure == Some(BorrowFailure::Poisoned),
        )
    }
    /// Actual original input Vec capacity, including unused caller slots.
    pub fn original_input_capacity(&self) -> usize {
        self.scope.operation.request.original_capacity
    }
}
impl fmt::Debug for OwnedPinnedShardLogMutationComplete {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedPinnedShardLogMutationComplete")
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for RetainedOwnedPinnedShardLogMutationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedOwnedPinnedShardLogMutationError")
            .field("failure", &self.failure)
            .field("native_ok", &self.native_returned_success())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedOwnedPinnedShardLogMutationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Core canonical mutation refused: {:?}", self.failure)
    }
}
impl Error for RetainedOwnedPinnedShardLogMutationError {}

fn retain_fee() -> Option<(usize, usize, usize)> {
    let bytes =
        size_of::<OwnedDirectoryHandle>().checked_add(size_of::<Option<OwnedDirectoryHandle>>())?;
    Some((bytes, 2, bytes.checked_add(1)?))
}
fn fixed_fee() -> Option<(usize, usize, usize)> {
    let values = [
        size_of::<Scope>(),
        size_of::<Operation>(),
        size_of::<Effects>(),
        size_of::<Request>(),
        size_of::<Kind>(),
        size_of::<Control>(),
        size_of::<RefCell<Control>>(),
        size_of::<Work<'static>>(),
        size_of::<ValidationChecks<'static, 'static>>(),
        size_of::<ParentChecks<'static, 'static, 'static>>(),
        size_of::<NativeChecks<'static, 'static>>(),
        size_of::<AppendDenial>(),
        size_of::<RefCell<&'static mut PinnedShardAppendBudget>>(),
        size_of::<&'static mut PinnedShardAppendBudget>(),
        size_of::<OwnedPinnedShardLogMutationComplete>(),
        size_of::<RetainedOwnedPinnedShardLogMutationError>(),
        size_of::<OwnedPinnedShardLogMutationCost>(),
        size_of::<OwnedPinnedShardLogMutationFailure>(),
        size_of::<OwnedPinnedShardLogMutationObservation<'static>>(),
        size_of::<Observation<'static>>(),
        size_of::<OwnedPinnedShardLogValidation<'static>>(),
        size_of::<Vec<Bytes>>(),
        size_of::<Option<Vec<Bytes>>>(),
        size_of::<u64>(),
        size_of::<usize>(),
        size_of::<bool>(),
        size_of::<PinnedShardLogWriteLimits>(),
        size_of::<PinnedShardGcLimits>(),
        size_of::<Option<(CompletedNative, ConstructorContext)>>(),
        size_of::<(CompletedNative, ConstructorContext)>(),
        size_of::<Option<OwnedPinnedShardLog>>(),
        size_of::<Option<ConstructorContext>>(),
        size_of::<Option<Native>>(),
        size_of::<Native>(),
        size_of::<Option<OpenedPinnedShardLog>>(),
        size_of::<CompletedNative>(),
        size_of::<Result<PinnedShardLogWriteComplete, PinnedShardLogWriteError>>(),
        size_of::<Result<PinnedShardLogGcComplete, PinnedShardLogGcError>>(),
        size_of::<Option<Validation>>(),
        size_of::<Validation>(),
        size_of::<Option<Validation>>(),
        size_of::<Validation>(),
        size_of::<PinBorrow<'static>>(),
        size_of::<TryLockResult<MutexGuard<'static, OwnedShardDirectory>>>(),
        size_of::<BorrowFailure>(),
        size_of::<Option<BorrowFailure>>(),
        size_of::<Option<Result<(), NativeDirectoryFailure>>>(),
        size_of::<Result<(), NativeDirectoryFailure>>(),
        size_of::<Option<Result<OwnedPinnedShardLogMutationCost, NativeDirectoryFailure>>>(),
        size_of::<Result<OwnedPinnedShardLogMutationCost, NativeDirectoryFailure>>(),
        size_of::<Option<Result<(), NativeDirectoryFailure>>>(),
        size_of::<Result<(), NativeDirectoryFailure>>(),
        size_of::<Option<Result<NativeDirectoryUsage, NativeDirectoryFailure>>>(),
        size_of::<Result<NativeDirectoryUsage, NativeDirectoryFailure>>(),
        size_of::<Option<Result<StorageFetchUsage, StorageFetchFailure>>>(),
        size_of::<Result<StorageFetchUsage, StorageFetchFailure>>(),
        size_of::<Option<Instant>>(),
        size_of::<Option<Instant>>(),
        size_of::<Option<PinnedShardAppendUsage>>(),
        size_of::<PinnedShardAppendUsage>(),
        size_of::<Option<bool>>(),
        size_of::<Option<Phase>>(),
        size_of::<Phase>(),
        size_of::<Option<Control>>(),
        size_of::<Option<AppendDenial>>(),
        size_of::<Cell<bool>>(),
        size_of::<Cell<bool>>(),
        size_of::<bool>(),
        size_of::<bool>(),
        size_of::<RefCell<Option<Result<(), Box<dyn Any + Send>>>>>(),
        size_of::<Option<Result<(), Box<dyn Any + Send>>>>(),
        size_of::<Result<(), Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<std::cell::RefMut<'static, &'static mut PinnedShardAppendBudget>>(),
        size_of::<std::cell::RefMut<'static, Option<Result<(), Box<dyn Any + Send>>>>>(),
        size_of::<bool>(),
        size_of::<bool>(),
        size_of::<bool>(),
        size_of::<bool>(),
        size_of::<bool>(),
        size_of::<Option<Result<(), NativeDirectoryFailure>>>(),
        size_of::<Option<Result<(), NativeDirectoryFailure>>>(),
        size_of::<Result<(), NativeDirectoryFailure>>(),
        size_of::<Result<(), NativeDirectoryFailure>>(),
        size_of::<Option<Result<(), StorageFetchFailure>>>(),
        size_of::<Option<Result<(), StorageFetchFailure>>>(),
        size_of::<Result<(), StorageFetchFailure>>(),
        size_of::<Result<(), StorageFetchFailure>>(),
        size_of::<Option<Result<(), ShardDirectoryCheckFailure>>>(),
        size_of::<Result<(), ShardDirectoryCheckFailure>>(),
        size_of::<bool>(), // actual retained append-refused value
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Option<OwnedPinnedShardLogMutationFailure>>(),
        size_of::<Result<Step<()>, Box<dyn Any + Send>>>(),
        size_of::<Result<Step<()>, Box<dyn Any + Send>>>(),
        size_of::<Step<()>>(),
        size_of::<Result<(), Box<dyn Any + Send>>>(),
        size_of::<Result<(), Box<dyn Any + Send>>>(),
        size_of::<Result<(), Box<dyn Any + Send>>>(),
    ];
    let bytes = values.into_iter().try_fold(0usize, usize::checked_add)?;
    Some((bytes, values.len(), bytes))
}
fn mutation_cost(
    core_limits: NativeDirectoryLimits,
    fetch: StorageFetchLimits,
    append: PinnedShardAppendLimits,
    write: PinnedShardLogWriteLimits,
    gc: Option<PinnedShardGcLimits>,
) -> Result<OwnedPinnedShardLogMutationCost, NativeDirectoryFailure> {
    let core = native_directory_cost(core_limits)?;
    let validation = checked_directory_validation_cost(core_limits)?;
    let twice = |a: usize, b: usize| {
        a.checked_sub(b)
            .and_then(|n| n.checked_mul(2))
            .ok_or(NativeDirectoryFailure::Capacity)
    };
    let validation_bytes = twice(validation.declared_bytes, core.declared_bytes)?;
    let validation_entries = twice(validation.entries, core.entries)?;
    let validation_work = twice(validation.work_bytes, core.work_bytes)?;
    let (bytes, entries, work) = fixed_fee().ok_or(NativeDirectoryFailure::Capacity)?;
    let (retain, retain_entries, retain_work) =
        retain_fee().ok_or(NativeDirectoryFailure::Capacity)?;
    let global = storage_fetch_cost(fetch).map_err(|_| NativeDirectoryFailure::Capacity)?;
    let append_cost = pinned_shard_append_cost(append).ok_or(NativeDirectoryFailure::Capacity)?;
    let native = match gc {
        Some(gc) => pinned_shard_log_gc_cost(fetch, append, write, gc),
        None => pinned_shard_log_write_cost(fetch, append, write),
    }
    .ok_or(NativeDirectoryFailure::Capacity)?;
    let difference = |a: usize, b: usize, c: usize| {
        a.checked_sub(b)
            .and_then(|n| n.checked_sub(c))
            .ok_or(NativeDirectoryFailure::Capacity)
    };
    let parent_retention_bytes = retain
        .checked_mul(2)
        .ok_or(NativeDirectoryFailure::Capacity)?;
    Ok(OwnedPinnedShardLogMutationCost {
        declared_bytes: bytes
            .checked_add(parent_retention_bytes)
            .and_then(|n| n.checked_add(validation_bytes))
            .ok_or(NativeDirectoryFailure::Capacity)?,
        string_bytes: 0,
        entries: entries
            .checked_add(
                retain_entries
                    .checked_mul(2)
                    .ok_or(NativeDirectoryFailure::Capacity)?,
            )
            .and_then(|n| n.checked_add(validation_entries))
            .ok_or(NativeDirectoryFailure::Capacity)?,
        work_bytes: retain_work
            .checked_add(retain_entries)
            .and_then(|n| n.checked_mul(2))
            .and_then(|n| n.checked_add(work))
            .and_then(|n| n.checked_add(entries))
            .and_then(|n| n.checked_add(validation_work))
            .ok_or(NativeDirectoryFailure::Capacity)?,
        validation_framing_bytes: validation_bytes,
        validation_framing_entries: validation_entries,
        parent_retention_bytes,
        core_operations: 3,
        native_framing: StorageFetchCost {
            declared_bytes: difference(
                native.declared_bytes,
                global.declared_bytes,
                append_cost.declared_bytes,
            )?,
            entries: difference(native.entries, global.entries, append_cost.entries)?,
            string_bytes: difference(
                native.string_bytes,
                global.string_bytes,
                append_cost.string_bytes,
            )?,
            work_bytes: difference(native.work_bytes, global.work_bytes, append_cost.work_bytes)?,
        },
    })
}
/// Plain per-append/seal bridge facts, excluding the shared policy ceilings.
pub fn owned_pinned_shard_log_write_cost(
    core: NativeDirectoryLimits,
    fetch: StorageFetchLimits,
    append: PinnedShardAppendLimits,
    write: PinnedShardLogWriteLimits,
) -> Result<OwnedPinnedShardLogMutationCost, NativeDirectoryFailure> {
    mutation_cost(core, fetch, append, write, None)
}
/// Plain per-compaction bridge facts; native framing includes its genuine seal.
pub fn owned_pinned_shard_log_gc_cost(
    core: NativeDirectoryLimits,
    fetch: StorageFetchLimits,
    append: PinnedShardAppendLimits,
    write: PinnedShardLogWriteLimits,
    gc: PinnedShardGcLimits,
) -> Result<OwnedPinnedShardLogMutationCost, NativeDirectoryFailure> {
    mutation_cost(core, fetch, append, write, Some(gc))
}

impl OwnedPinnedShardLog {
    /// Consume this exact log through native canonical append and fresh validations.
    /// Actual input Vec capacity is preserved; no payload repacking occurs.
    ///
    /// # Errors
    /// Retains all actual effects/input and denies the same append/Core/fetch
    /// counters and captured health. No new authority, policy or retry is made.
    #[allow(
        clippy::too_many_arguments,
        clippy::result_large_err,
        reason = "explicit original budgets and whole early custody"
    )]
    pub fn append_group_until(
        self,
        first_sequence: u64,
        records: Vec<Bytes>,
        durable: bool,
        append: &mut PinnedShardAppendBudget,
        native: &StorageFetchBudget,
        core: &NativeDirectoryBudget,
        write: PinnedShardLogWriteLimits,
        checks: &dyn CheckedDirectoryValidationChecks,
    ) -> Result<OwnedPinnedShardLogMutationComplete, RetainedOwnedPinnedShardLogMutationError> {
        let original_capacity = records.capacity();
        mutation_entry(
            self,
            Request {
                kind: Kind::Append,
                sequence: first_sequence,
                records: Some(records),
                original_capacity,
                durable,
                write,
            },
            append,
            native,
            core,
            checks,
        )
    }
    /// Consume this exact log through the genuine bounded seal/descriptor transfer.
    ///
    /// # Errors
    /// Returns terminal whole custody on native, validation, budget or health refusal.
    #[allow(
        clippy::result_large_err,
        reason = "whole early custody cannot use unpaid heap allocation"
    )]
    pub fn seal_until(
        self,
        append: &mut PinnedShardAppendBudget,
        native: &StorageFetchBudget,
        core: &NativeDirectoryBudget,
        write: PinnedShardLogWriteLimits,
        checks: &dyn CheckedDirectoryValidationChecks,
    ) -> Result<OwnedPinnedShardLogMutationComplete, RetainedOwnedPinnedShardLogMutationError> {
        mutation_entry(
            self,
            Request {
                kind: Kind::Seal,
                sequence: 0,
                records: None,
                original_capacity: 0,
                durable: false,
                write,
            },
            append,
            native,
            core,
            checks,
        )
    }
    /// Seal and compact through the actual tracked collector on this SAME log.
    ///
    /// # Errors
    /// Keeps all real quarantine/partial effects and same physical root on refusal.
    #[allow(
        clippy::too_many_arguments,
        clippy::result_large_err,
        reason = "explicit original budgets and whole early custody"
    )]
    pub fn compact_before_until(
        self,
        sequence: u64,
        append: &mut PinnedShardAppendBudget,
        native: &StorageFetchBudget,
        core: &NativeDirectoryBudget,
        write: PinnedShardLogWriteLimits,
        gc: PinnedShardGcLimits,
        checks: &dyn CheckedDirectoryValidationChecks,
    ) -> Result<OwnedPinnedShardLogMutationComplete, RetainedOwnedPinnedShardLogMutationError> {
        mutation_entry(
            self,
            Request {
                kind: Kind::Compact(gc),
                sequence,
                records: None,
                original_capacity: 0,
                durable: false,
                write,
            },
            append,
            native,
            core,
            checks,
        )
    }
}
#[allow(
    clippy::result_large_err,
    reason = "retain actual early/native owners without unpaid allocation"
)]
fn mutation_entry(
    input: OwnedPinnedShardLog,
    request: Request,
    append: &mut PinnedShardAppendBudget,
    native: &StorageFetchBudget,
    core: &NativeDirectoryBudget,
    checks: &dyn CheckedDirectoryValidationChecks,
) -> Result<OwnedPinnedShardLogMutationComplete, RetainedOwnedPinnedShardLogMutationError> {
    let mut scope = Scope::new(input, request);
    let control = RefCell::new(Control::default());
    let append_denial = AppendDenial::default();
    let work = Work {
        core,
        native,
        outer: checks,
        control: &control,
    };
    let prepared = catch_unwind(AssertUnwindSafe(|| scope.prepare(append, &work)));
    let mut failure = match prepared {
        Ok(Ok(())) => None,
        Ok(Err(v)) => Some(v),
        Err(p) => {
            scope.operation.effects.operation_panic = Some(p);
            Some(OwnedPinnedShardLogMutationFailure::Unwind)
        }
    };
    if failure.is_none() {
        let Scope {
            operation, context, ..
        } = &mut scope;
        match context
            .as_ref()
            .and_then(|context| context.directory.as_ref())
        {
            Some(directory) => {
                // Typed borrow and SAME context remain outside the catch.
                let mut borrowed: PinBorrow<'_> = None;
                let actual = catch_unwind(AssertUnwindSafe(|| {
                    operation.run(directory, &mut borrowed, append, &work, &append_denial)
                }));
                failure = match actual {
                    Ok(Ok(())) => None,
                    Ok(Err(v)) => Some(v),
                    Err(p) => {
                        operation.effects.operation_panic = Some(p);
                        Some(OwnedPinnedShardLogMutationFailure::Unwind)
                    }
                };
                if failure.is_some() {
                    append_denial.deny(append);
                    work.deny_bridge();
                }
                drop(borrowed);
            }
            None => failure = Some(OwnedPinnedShardLogMutationFailure::Invariant),
        }
    }
    let mut successful = None;
    if failure.is_none() {
        match (scope.operation.native.take(), scope.context.take()) {
            (Some(Native::Write(Ok(v))), Some(context)) => {
                successful = Some((CompletedNative::Write(v), context))
            }
            (Some(Native::Gc(Ok(v))), Some(context)) => {
                successful = Some((CompletedNative::Gc(v), context))
            }
            (actual, context) => {
                scope.operation.native = actual;
                scope.context = context;
                failure = Some(OwnedPinnedShardLogMutationFailure::Invariant);
            }
        }
    }
    if failure.is_some() {
        append_denial.deny(append);
        work.deny_bridge();
    }
    scope.operation.effects.append_usage = Some(append.usage());
    scope.operation.effects.append_refused = Some(append.is_refused());
    scope.operation.effects.control = Some(control.into_inner());
    scope.operation.effects.append_denial = Some(append_denial);
    if let Some((native, context)) = successful {
        return Ok(OwnedPinnedShardLogMutationComplete {
            operation: scope.operation,
            native,
            context,
        });
    }
    Err(RetainedOwnedPinnedShardLogMutationError {
        failure: failure.unwrap_or(OwnedPinnedShardLogMutationFailure::Invariant),
        scope,
    })
}

#[cfg(test)]
#[path = "owned_pinned_shard_write_tests.rs"]
mod tests;
#[cfg(test)]
fn completed(phase: Phase) {
    tests::completed(phase);
}
#[cfg(not(test))]
fn completed(_: Phase) {}
