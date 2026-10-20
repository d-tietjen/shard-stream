//! Consume the genuine Core canonical log through one real bounded native read.
//!
//! Constructor validation is retained history, never permission for this read.
//! Each call consumes two fresh prepaid parent handles through the actual
//! checked validator, uses the same counters/current minimum E and keeps the
//! same original constructor context and physical directory last.

use super::{
    Control, OwnedPinnedShardLog, OwnedPinnedShardLogFailure, OwnedPinnedShardLogValidation,
    Scope as ConstructorContext, Validation, ValidationChecks, Work,
};
use crate::{
    CheckedDirectoryValidationChecks, NativeDirectoryBudget, NativeDirectoryFailure,
    NativeDirectoryLimits, NativeDirectoryUsage, OwnedDirectoryHandle, OwnedShardDirectory,
    checked_directory_validation_cost, native_directory_cost,
};
use shardlog::{
    OpenedPinnedShardLog, PinnedShardFetchChecks, PinnedShardFetchFailure,
    PinnedShardFetchObservation, PinnedShardFetchOutcome, PinnedShardLogReadComplete,
    PinnedShardLogReadError, PinnedShardLogReadFailure, ShardDirectoryCheckFailure,
    ShardDirectoryChecks, ShardLogRecord, StorageFetchBudget, StorageFetchFailure,
    StorageFetchLimits, StorageFetchUsage, pinned_shard_log_open_cost, pinned_shard_log_read_cost,
};
use std::{
    any::Any,
    cell::RefCell,
    collections::TryReserveError,
    error::Error,
    fmt,
    mem::size_of,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{MutexGuard, TryLockError, TryLockResult},
    time::Instant,
};

type Native = Result<PinnedShardLogReadComplete, PinnedShardLogReadError>;
type PinBorrow<'a> = Option<TryLockResult<MutexGuard<'a, OwnedShardDirectory>>>;
type Step<T> = Result<T, OwnedPinnedShardReadFailure>;

/// Per-read bridge/Core framing and separate incremental native read framing.
///
/// This contains no configured global native or Core ceiling. Prepay each
/// global policy once, the constructor frame once, and these retained per-read
/// frames for every actual initial read. The same actual producers still debit
/// their supplied counters at entry; this getter creates or admits no work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnedPinnedShardReadCost {
    /// Bridge values, two real parent retains and two validation frames.
    pub declared_bytes: usize,
    /// No bridge-owned string allocation; validation pays its actual paths.
    pub string_bytes: usize,
    /// Independently retained bridge/validation/parent slots.
    pub entries: usize,
    /// Bridge work and actual validation framing, with entry fees once.
    pub work_bytes: usize,
    /// Two validation frame values, already included above.
    pub validation_framing_bytes: usize,
    /// Two validation frame entry counts, already included above.
    pub validation_framing_entries: usize,
    /// Two independently prepaid real parent retain headers/Arc increments.
    pub parent_retention_bytes: usize,
    /// One bridge plus two genuine validation operations on the same Core state.
    pub core_operations: usize,
    /// Native read/fetch framing beyond the one global+constructor ceiling.
    pub native_framing_declared_bytes: usize,
    /// Native per-read diagnostics already included in native declared bytes.
    pub native_framing_string_bytes: usize,
    /// Native per-read retained slots beyond the one constructor frame.
    pub native_framing_entries: usize,
    /// Native read/fetch framing work beyond the one constructor frame.
    pub native_framing_work_bytes: usize,
}

/// Terminal category; actual owning results and inputs stay privately retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnedPinnedShardReadFailure {
    /// The same Core counters/current minimum E refused.
    Core,
    /// The same native counters/original E refused.
    Native,
    /// Actual captured work/health check refused.
    Checkpoint,
    /// The real per-read prevalidation refused.
    PreValidation,
    /// Immediate borrowing of the same actual native pin refused.
    PinBorrow,
    /// The genuine consuming native read returned its complete owning error.
    Read,
    /// Postvalidation refused while the actual native read result stays owned.
    PostValidation,
    /// An actual bridge/checkpoint/observer unwound.
    Unwind,
    /// A private whole-input transition was unavailable.
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
struct Request {
    start: u64,
    max_bytes: usize,
    max_records: usize,
}

fn checkpoint(work: &Work<'_>) -> Step<()> {
    work.check().map_err(|error| match error {
        OwnedPinnedShardLogFailure::Core => OwnedPinnedShardReadFailure::Core,
        OwnedPinnedShardLogFailure::Native => OwnedPinnedShardReadFailure::Native,
        OwnedPinnedShardLogFailure::Checkpoint => OwnedPinnedShardReadFailure::Checkpoint,
        _ => OwnedPinnedShardReadFailure::Invariant,
    })
}
struct ReadChecks<'a, 'b>(&'a Work<'b>);
impl ShardDirectoryChecks for ReadChecks<'_, '_> {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        checkpoint(self.0).map_err(|_| ShardDirectoryCheckFailure::Refused)
    }
}
impl PinnedShardFetchChecks for ReadChecks<'_, '_> {
    fn deny(&self) {
        // The real read producer attempted native denial before calling us.
        // Its payload remains in that actual native carrier. Core denial MUST
        // precede the first actual outer-health attempt, including on unwind.
        self.0.control.borrow_mut().native_deny_attempted = true;
        self.0.deny_core();
        self.0.deny_outer();
    }
}

#[derive(Default)]
struct Effects {
    cap: Option<Result<(), NativeDirectoryFailure>>,
    cost: Option<Result<OwnedPinnedShardReadCost, NativeDirectoryFailure>>,
    fee: Option<Result<(), NativeDirectoryFailure>>,
    original_native_expiry: Option<Instant>,
    entry_core_minimum: Option<Instant>,
    core_usage: Option<Result<NativeDirectoryUsage, NativeDirectoryFailure>>,
    native_usage: Option<Result<StorageFetchUsage, StorageFetchFailure>>,
    entered: Option<Phase>,
    pin_borrowed: bool,
    borrow_failure: Option<BorrowFailure>,
    operation_panic: Option<Box<dyn Any + Send>>,
    control: Option<Control>,
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
    // Whole early input before any cost/check/catch/Arc increment.
    early: Option<OwnedPinnedShardLog>,
    // LAST after split: all original constructor results, then SAME directory.
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
    fn prepare(&mut self, work: &Work<'_>) -> Step<()> {
        self.operation.effects.original_native_expiry = Some(work.native.absolute_expiry());
        self.operation.effects.cap = Some(work.core.cap_expiry(work.native.absolute_expiry()));
        self.operation.effects.entry_core_minimum = Some(work.core.absolute_expiry());
        if matches!(self.operation.effects.cap, Some(Err(_))) {
            return Err(OwnedPinnedShardReadFailure::Core);
        }
        checkpoint(work)?;
        self.operation.effects.cost = Some(owned_pinned_shard_read_cost(
            work.core.limits(),
            work.native.limits(),
        ));
        if matches!(self.operation.effects.cost, Some(Err(_))) {
            return Err(OwnedPinnedShardReadFailure::Core);
        }
        let (bytes, entries, cpu) = fixed_fee().ok_or(OwnedPinnedShardReadFailure::Invariant)?;
        self.operation.charge(work, 1, bytes, entries, cpu)?;
        self.operation.effects.entered = Some(Phase::FixedAdmission);
        completed(Phase::FixedAdmission);
        checkpoint(work)?;
        let input = self
            .early
            .take()
            .ok_or(OwnedPinnedShardReadFailure::Invariant)?;
        self.context = Some(input.scope);
        let actual = self
            .context
            .as_mut()
            .ok_or(OwnedPinnedShardReadFailure::Invariant)?
            .native
            .take();
        match actual {
            Some(Ok(log)) => self.operation.remaining_native = Some(log),
            actual => {
                self.context
                    .as_mut()
                    .ok_or(OwnedPinnedShardReadFailure::Invariant)?
                    .native = actual;
                return Err(OwnedPinnedShardReadFailure::Invariant);
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
            Err(OwnedPinnedShardReadFailure::Core)
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
        let (bytes, entries, cpu) = retain_fee().ok_or(OwnedPinnedShardReadFailure::Invariant)?;
        self.charge(work, 0, bytes, entries, cpu)?;
        checkpoint(work)?;
        self.effects.entered = Some(phase);
        self.pending_handle = Some(parent.retain());
        completed(phase);
        checkpoint(work)
    }
    fn validate(&mut self, work: &Work<'_>, before: bool) -> Step<()> {
        checkpoint(work)?;
        let input = self
            .pending_handle
            .take()
            .ok_or(OwnedPinnedShardReadFailure::Invariant)?;
        let phase = if before {
            Phase::PreValidation
        } else {
            Phase::PostValidation
        };
        self.effects.entered = Some(phase);
        let actual =
            input.validate_for_storage_fetch_until(work.core, work.native, &ValidationChecks(work));
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
        } else {
            Err(if before {
                OwnedPinnedShardReadFailure::PreValidation
            } else {
                OwnedPinnedShardReadFailure::PostValidation
            })
        }
    }
    fn run<'a>(
        &mut self,
        directory: &'a super::OwnedShardStoreDirectory,
        borrowed: &mut PinBorrow<'a>,
        work: &Work<'_>,
    ) -> Step<()> {
        self.retain_parent(&directory.inner.parent, work, Phase::PreRetain)?;
        self.validate(work, true)?;
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
            None => return Err(OwnedPinnedShardReadFailure::Invariant),
        }
        completed(Phase::PinBorrowed);
        checkpoint(work)?;
        let pin = match borrowed.as_ref() {
            Some(Ok(pin)) => pin,
            Some(Err(_)) => return Err(OwnedPinnedShardReadFailure::PinBorrow),
            None => return Err(OwnedPinnedShardReadFailure::Invariant),
        };
        let actual = self
            .remaining_native
            .take()
            .ok_or(OwnedPinnedShardReadFailure::Invariant)?;
        self.effects.entered = Some(Phase::Native);
        self.native = Some(actual.read_from_until(
            &pin.native,
            self.request.start,
            self.request.max_bytes,
            self.request.max_records,
            work.native,
            &ReadChecks(work),
        ));
        // WHOLE native Ok/Err is already in this outside-catch operation.
        completed(Phase::Native);
        checkpoint(work)?;
        if !matches!(self.native, Some(Ok(_))) {
            return Err(OwnedPinnedShardReadFailure::Read);
        }
        self.retain_parent(&directory.inner.parent, work, Phase::PostRetain)?;
        self.validate(work, false)?;
        self.effects.core_usage = Some(work.core.usage());
        self.effects.native_usage = Some(work.native.usage());
        self.effects.entered = Some(Phase::Usage);
        completed(Phase::Usage);
        checkpoint(work)?;
        if matches!(self.effects.core_usage, Some(Err(_))) {
            return Err(OwnedPinnedShardReadFailure::Core);
        }
        if matches!(self.effects.native_usage, Some(Err(_))) {
            return Err(OwnedPinnedShardReadFailure::Native);
        }
        Ok(())
    }
    fn records(&self) -> &[ShardLogRecord] {
        match self.native.as_ref() {
            Some(Ok(v)) => v.records(),
            Some(Err(v)) => v.records(),
            None => &[],
        }
    }
    fn observation(&self) -> Option<OwnedPinnedShardReadObservation<'_>> {
        let actual = match self.native.as_ref()? {
            Ok(v) => v.observation(),
            Err(v) => v.observation(),
        }?;
        Some(OwnedPinnedShardReadObservation { actual })
    }
}

/// Safe actual physical-read facts without native owning/error/Any extraction.
pub struct OwnedPinnedShardReadObservation<'a> {
    actual: &'a PinnedShardFetchObservation,
}
impl OwnedPinnedShardReadObservation<'_> {
    /// Original typed stat/pread Results, with no File or movable descriptor.
    pub fn outcomes(&self) -> &[PinnedShardFetchOutcome] {
        self.actual.outcomes()
    }
    /// Actual retained range length and filled prefix; no unselected bytes escape.
    pub fn partial_buffer_lengths(&self, range: usize) -> Option<(usize, usize)> {
        self.actual.partial_buffer_lengths(range)
    }
    /// Exact actual bounded native basename, including accepted zero prefixes.
    pub fn pack_name(&self, range: usize) -> Option<&std::ffi::CStr> {
        self.actual.pack_name(range)
    }
    /// Actual retained allocation error, without a native owning-error borrow.
    pub fn allocation_error(&self) -> Option<&TryReserveError> {
        self.actual.allocation_error()
    }
    /// Actual terminal physical-read category.
    pub fn failure(&self) -> Option<&PinnedShardFetchFailure> {
        self.actual.failure()
    }
    /// Actual original cumulative native usage Result.
    pub fn usage(&self) -> Option<&Result<StorageFetchUsage, StorageFetchFailure>> {
        self.actual.usage()
    }
    /// Actual independent native operation/budget/composing panic occupancy.
    pub fn has_panic_payloads(&self) -> [bool; 3] {
        self.actual.panic_payloads().map(|v| v.is_some())
    }
}

/// Successful genuine read retaining read reports before original constructor context.
/// No budget, Session, callback, native wrapper or descriptor is exposed/stored.
pub struct OwnedPinnedShardReadComplete {
    operation: Operation,
    native: PinnedShardLogReadComplete,
    // LAST: original constructor reports and SAME shared physical directory.
    context: ConstructorContext,
}
impl OwnedPinnedShardReadComplete {
    /// Actual canonical output in native selection order; no reconstructed rows.
    pub fn records(&self) -> &[ShardLogRecord] {
        self.native.records()
    }
    /// Actual physical-read observation through the noncontent scalar facade.
    pub fn observation(&self) -> Option<OwnedPinnedShardReadObservation<'_>> {
        self.native
            .observation()
            .map(|actual| OwnedPinnedShardReadObservation { actual })
    }
    /// This read's actual fresh prevalidation, independent of constructor history.
    pub fn pre_validation(&self) -> Option<OwnedPinnedShardLogValidation<'_>> {
        self.operation
            .pre_validation
            .as_ref()
            .map(|actual| OwnedPinnedShardLogValidation { actual })
    }
    /// This read's actual fresh postvalidation.
    pub fn post_validation(&self) -> Option<OwnedPinnedShardLogValidation<'_>> {
        self.operation
            .post_validation
            .as_ref()
            .map(|actual| OwnedPinnedShardLogValidation { actual })
    }
    /// Original constructor prevalidation retained unchanged with this context.
    pub fn constructor_pre_validation(&self) -> Option<OwnedPinnedShardLogValidation<'_>> {
        self.context
            .pre_validation
            .as_ref()
            .map(|actual| OwnedPinnedShardLogValidation { actual })
    }
    /// Original constructor postvalidation retained unchanged with this context.
    pub fn constructor_post_validation(&self) -> Option<OwnedPinnedShardLogValidation<'_>> {
        self.context
            .post_validation
            .as_ref()
            .map(|actual| OwnedPinnedShardLogValidation { actual })
    }
    /// Destroy settled read state, then restore the actual returned native log.
    ///
    /// This is a whole-owner restoration only: no physical IO, callback, native
    /// from-parts constructor, counter reset or fallible operation is entered.
    pub fn into_log(self) -> OwnedPinnedShardLog {
        let Self {
            operation,
            native,
            mut context,
        } = self;
        drop(operation);
        let actual_log = native.into_log();
        context.native = Some(Ok(actual_log));
        OwnedPinnedShardLog { scope: context }
    }
}

/// Refusal retains original input or actual completed/uncertain read plus context.
/// No retry, whole-log/native/FD/Any/error-carrier extractor is available.
pub struct RetainedOwnedPinnedShardReadError {
    failure: OwnedPinnedShardReadFailure,
    scope: Scope,
}
impl RetainedOwnedPinnedShardReadError {
    /// Actual bridge terminal category.
    pub fn failure(&self) -> OwnedPinnedShardReadFailure {
        self.failure
    }
    /// Actual native return; None means no native read result was returned.
    pub fn native_returned_success(&self) -> Option<bool> {
        self.scope.operation.native.as_ref().map(Result::is_ok)
    }
    /// Actual complete canonical prefix retained before refusal.
    pub fn records(&self) -> &[ShardLogRecord] {
        self.scope.operation.records()
    }
    /// Actual physical read facts, including late native success.
    pub fn observation(&self) -> Option<OwnedPinnedShardReadObservation<'_>> {
        self.scope.operation.observation()
    }
    /// Actual read-local prevalidation retained before hooks/postchecks.
    pub fn pre_validation(&self) -> Option<OwnedPinnedShardLogValidation<'_>> {
        self.scope
            .operation
            .pre_validation
            .as_ref()
            .map(|actual| OwnedPinnedShardLogValidation { actual })
    }
    /// Actual read-local postvalidation, including late/error results.
    pub fn post_validation(&self) -> Option<OwnedPinnedShardLogValidation<'_>> {
        self.scope
            .operation
            .post_validation
            .as_ref()
            .map(|actual| OwnedPinnedShardLogValidation { actual })
    }
    /// Actual native canonical read failure category, never the owning error.
    pub fn native_failure(&self) -> Option<&PinnedShardLogReadFailure> {
        self.scope
            .operation
            .native
            .as_ref()?
            .as_ref()
            .err()?
            .failure()
    }
    /// Actual native operation/budget/composing denial panic occupancy.
    pub fn native_panic_observations(&self) -> Option<[bool; 3]> {
        Some(
            self.scope
                .operation
                .native
                .as_ref()?
                .as_ref()
                .err()?
                .panic_payloads()
                .map(|v| v.is_some()),
        )
    }
    /// Actual bridge operation/Core/native/outer denial panic occupancy.
    pub fn has_panic_payloads(&self) -> [bool; 4] {
        let effects = &self.scope.operation.effects;
        let control = effects.control.as_ref();
        [
            effects.operation_panic.is_some(),
            control.is_some_and(|v| v.core_deny_panic.is_some()),
            control.is_some_and(|v| v.native_deny_panic.is_some()),
            control.is_some_and(|v| v.outer_deny_panic.is_some()),
        ]
    }
    /// Actual immediate private borrow observations; no guard/error escapes.
    pub fn pin_borrow_observations(&self) -> (bool, bool, bool) {
        let effects = &self.scope.operation.effects;
        (
            effects.pin_borrowed,
            effects.borrow_failure == Some(BorrowFailure::WouldBlock),
            effects.borrow_failure == Some(BorrowFailure::Poisoned),
        )
    }
    /// Actual outer-health attempted/returned facts; an unwind is not retried.
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
}
impl fmt::Debug for OwnedPinnedShardReadComplete {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedPinnedShardReadComplete")
            .field("records", &self.records().len())
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for RetainedOwnedPinnedShardReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedOwnedPinnedShardReadError")
            .field("failure", &self.failure)
            .field("native_returned_success", &self.native_returned_success())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedOwnedPinnedShardReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Core bounded canonical read refused: {:?}", self.failure)
    }
}
impl Error for RetainedOwnedPinnedShardReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Forward only actual non-owning Core failures. No native owning error,
        // validator carrier or arbitrary panic value is reachable by downcast.
        let effects = &self.scope.operation.effects;
        if let Some(Err(v)) = effects.cap.as_ref() {
            return Some(v);
        }
        if let Some(Err(v)) = effects.cost.as_ref() {
            return Some(v);
        }
        if let Some(Err(v)) = effects.fee.as_ref() {
            return Some(v);
        }
        None
    }
}

fn retain_fee() -> Option<(usize, usize, usize)> {
    let bytes =
        size_of::<OwnedDirectoryHandle>().checked_add(size_of::<Option<OwnedDirectoryHandle>>())?;
    Some((bytes, 2, bytes.checked_add(1)?)) // reserve adds entry work itself.
}
fn fixed_fee() -> Option<(usize, usize, usize)> {
    let values = [
        size_of::<Scope>(),
        size_of::<Operation>(),
        size_of::<Effects>(),
        size_of::<Request>(),
        size_of::<Control>(),
        size_of::<RefCell<Control>>(),
        size_of::<Work<'static>>(),
        size_of::<ValidationChecks<'static, 'static>>(),
        size_of::<ReadChecks<'static, 'static>>(),
        size_of::<OwnedPinnedShardReadComplete>(),
        size_of::<RetainedOwnedPinnedShardReadError>(),
        size_of::<OwnedPinnedShardReadCost>(),
        size_of::<OwnedPinnedShardReadFailure>(),
        size_of::<OwnedPinnedShardReadObservation<'static>>(),
        size_of::<OwnedPinnedShardLogValidation<'static>>(),
        size_of::<u64>(),
        size_of::<usize>(),
        size_of::<usize>(), // actual request scalars
        size_of::<Option<(PinnedShardLogReadComplete, ConstructorContext)>>(),
        size_of::<(PinnedShardLogReadComplete, ConstructorContext)>(), // settled success transfer
        size_of::<Option<OwnedPinnedShardLog>>(),
        size_of::<Option<ConstructorContext>>(),
        size_of::<Option<Native>>(),
        size_of::<Native>(),
        size_of::<Option<OpenedPinnedShardLog>>(),
        size_of::<PinnedShardLogReadComplete>(),
        size_of::<PinnedShardLogReadError>(),
        size_of::<Option<Validation>>(),
        size_of::<Validation>(),
        size_of::<Option<Validation>>(),
        size_of::<Validation>(),
        size_of::<PinBorrow<'static>>(),
        size_of::<TryLockResult<MutexGuard<'static, OwnedShardDirectory>>>(),
        size_of::<BorrowFailure>(),
        size_of::<Option<BorrowFailure>>(),
        size_of::<Option<Result<(), NativeDirectoryFailure>>>(),
        size_of::<Option<Result<(), NativeDirectoryFailure>>>(),
        size_of::<Option<Result<OwnedPinnedShardReadCost, NativeDirectoryFailure>>>(),
        size_of::<Result<OwnedPinnedShardReadCost, NativeDirectoryFailure>>(),
        size_of::<Option<Result<(), NativeDirectoryFailure>>>(),
        size_of::<Option<Result<(), NativeDirectoryFailure>>>(),
        size_of::<Option<Result<(), StorageFetchFailure>>>(),
        size_of::<Option<Result<(), StorageFetchFailure>>>(),
        size_of::<Option<Result<(), ShardDirectoryCheckFailure>>>(),
        size_of::<Option<Result<NativeDirectoryUsage, NativeDirectoryFailure>>>(),
        size_of::<Option<Result<StorageFetchUsage, StorageFetchFailure>>>(),
        size_of::<Option<Instant>>(),
        size_of::<Option<Instant>>(),
        size_of::<Option<Phase>>(),
        size_of::<Option<Control>>(),
        // Independent retained flags/results, beyond their enclosing headers.
        size_of::<bool>(), // Control.core_deny_attempted
        size_of::<bool>(), // Control.native_deny_attempted
        size_of::<bool>(), // Control.outer_deny_attempted
        size_of::<bool>(), // Control.outer_deny_returned
        size_of::<bool>(), // Effects.pin_borrowed
        size_of::<Result<(), NativeDirectoryFailure>>(), // actual min cap
        size_of::<Result<(), NativeDirectoryFailure>>(), // actual entry/retain fee
        size_of::<Result<(), NativeDirectoryFailure>>(), // actual first Core check
        size_of::<Result<(), NativeDirectoryFailure>>(), // actual post Core check
        size_of::<Result<(), StorageFetchFailure>>(), // actual first native check
        size_of::<Result<(), StorageFetchFailure>>(), // actual post native check
        size_of::<Result<(), ShardDirectoryCheckFailure>>(), // actual outer result
        size_of::<Result<NativeDirectoryUsage, NativeDirectoryFailure>>(),
        size_of::<Result<StorageFetchUsage, StorageFetchFailure>>(),
        size_of::<Phase>(),
        size_of::<Option<OwnedPinnedShardReadFailure>>(),
        size_of::<Result<Step<()>, Box<dyn Any + Send>>>(), // second, entered-read catch
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Result<Step<()>, Box<dyn Any + Send>>>(),
        size_of::<Step<()>>(),
        size_of::<Result<(), Box<dyn Any + Send>>>(),
        size_of::<Result<(), Box<dyn Any + Send>>>(),
        size_of::<Result<(), Box<dyn Any + Send>>>(),
        size_of::<[bool; 4]>(),
        size_of::<[usize; 6]>(),
    ];
    let bytes = values.into_iter().try_fold(0usize, usize::checked_add)?;
    Some((bytes, values.len(), bytes)) // reserve adds entries exactly once.
}
/// Plain source-derived per-read cost with no configured global ceiling repeated.
pub fn owned_pinned_shard_read_cost(
    core_limits: NativeDirectoryLimits,
    native_limits: StorageFetchLimits,
) -> Result<OwnedPinnedShardReadCost, NativeDirectoryFailure> {
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
    let (fixed, entries, work) = fixed_fee().ok_or(NativeDirectoryFailure::Capacity)?;
    let (retain, retained_entries, retained_work) =
        retain_fee().ok_or(NativeDirectoryFailure::Capacity)?;
    let open =
        pinned_shard_log_open_cost(native_limits).map_err(|_| NativeDirectoryFailure::Capacity)?;
    let read =
        pinned_shard_log_read_cost(native_limits).map_err(|_| NativeDirectoryFailure::Capacity)?;
    let difference = |a: usize, b: usize| a.checked_sub(b).ok_or(NativeDirectoryFailure::Capacity);
    let parent_retention_bytes = retain
        .checked_mul(2)
        .ok_or(NativeDirectoryFailure::Capacity)?;
    Ok(OwnedPinnedShardReadCost {
        declared_bytes: fixed
            .checked_add(parent_retention_bytes)
            .and_then(|n| n.checked_add(validation_bytes))
            .ok_or(NativeDirectoryFailure::Capacity)?,
        string_bytes: 0,
        entries: retained_entries
            .checked_mul(2)
            .and_then(|n| n.checked_add(entries))
            .and_then(|n| n.checked_add(validation_entries))
            .ok_or(NativeDirectoryFailure::Capacity)?,
        work_bytes: retained_work
            .checked_add(retained_entries)
            .and_then(|n| n.checked_mul(2))
            .and_then(|n| n.checked_add(work))
            .and_then(|n| n.checked_add(entries))
            .and_then(|n| n.checked_add(validation_work))
            .ok_or(NativeDirectoryFailure::Capacity)?,
        validation_framing_bytes: validation_bytes,
        validation_framing_entries: validation_entries,
        parent_retention_bytes,
        core_operations: 3,
        native_framing_declared_bytes: difference(read.declared_bytes, open.declared_bytes)?,
        native_framing_string_bytes: difference(read.string_bytes, open.string_bytes)?,
        native_framing_entries: difference(read.entries, open.entries)?,
        native_framing_work_bytes: difference(read.work_bytes, open.work_bytes)?,
    })
}

impl OwnedPinnedShardLog {
    /// Consume this actual Core log through one bounded, descriptor-pinned read.
    ///
    /// Every read independently validates its real parent before and after,
    /// retaining both full results and the actual consuming native Result.
    /// Constructor reports remain original history. Both supplied budgets and
    /// captured health are checked through the real nested producers. Counters,
    /// deadlines, policies and Sessions are never reconstructed or stored.
    ///
    /// # Errors
    /// Denies both SAME counters and attempts captured health once, retaining
    /// independent panic values before releasing the actual typed pin borrow.
    /// Late native Ok remains owned on Core/postvalidation/checkpoint refusal.
    #[allow(
        clippy::result_large_err,
        reason = "retain whole early input and actual entered results without unpaid heap allocation"
    )]
    pub fn read_from_until(
        self,
        start_sequence: u64,
        max_bytes: usize,
        max_records: usize,
        native: &StorageFetchBudget,
        core: &NativeDirectoryBudget,
        checks: &dyn CheckedDirectoryValidationChecks,
    ) -> Result<OwnedPinnedShardReadComplete, RetainedOwnedPinnedShardReadError> {
        let mut scope = Scope::new(
            self,
            Request {
                start: start_sequence,
                max_bytes,
                max_records,
            },
        );
        let control = RefCell::new(Control::default());
        let work = Work {
            core,
            native,
            outer: checks,
            control: &control,
        };
        let prepared = catch_unwind(AssertUnwindSafe(|| scope.prepare(&work)));
        let mut failure = match prepared {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(payload) => {
                scope.operation.effects.operation_panic = Some(payload);
                Some(OwnedPinnedShardReadFailure::Unwind)
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
                    // Actual typed Result/guard is OUTSIDE catch; context and
                    // SAME directory stay alive until denial/settlement/unlock.
                    let mut borrowed: PinBorrow<'_> = None;
                    let actual = catch_unwind(AssertUnwindSafe(|| {
                        operation.run(directory, &mut borrowed, &work)
                    }));
                    failure = match actual {
                        Ok(Ok(())) => None,
                        Ok(Err(error)) => Some(error),
                        Err(payload) => {
                            operation.effects.operation_panic = Some(payload);
                            Some(OwnedPinnedShardReadFailure::Unwind)
                        }
                    };
                    if failure.is_some() {
                        work.deny_bridge();
                    }
                    drop(borrowed);
                }
                None => failure = Some(OwnedPinnedShardReadFailure::Invariant),
            }
        }
        let mut successful = None;
        if failure.is_none() {
            match (scope.operation.native.take(), scope.context.take()) {
                (Some(Ok(native)), Some(context)) => successful = Some((native, context)),
                (actual, context) => {
                    scope.operation.native = actual;
                    scope.context = context;
                    failure = Some(OwnedPinnedShardReadFailure::Invariant);
                }
            }
        }
        // The final invariant arm uses the original borrowed attempted-once
        // state too; no replacement control/counter/deadline is manufactured.
        if failure.is_some() {
            work.deny_bridge();
        }
        scope.operation.effects.control = Some(control.into_inner());
        if let Some((native, context)) = successful {
            return Ok(OwnedPinnedShardReadComplete {
                operation: scope.operation,
                native,
                context,
            });
        }
        Err(RetainedOwnedPinnedShardReadError {
            failure: failure.unwrap_or(OwnedPinnedShardReadFailure::Invariant),
            scope,
        })
    }
}

#[cfg(test)]
#[path = "owned_pinned_shard_read_tests.rs"]
mod tests;
#[cfg(test)]
fn completed(phase: Phase) {
    tests::completed(phase);
}
#[cfg(not(test))]
fn completed(_: Phase) {}
