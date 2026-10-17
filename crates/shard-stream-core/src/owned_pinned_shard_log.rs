//! Consume the actual Core store into its native canonical log without new budgets.
//!
//! Both real parent validations and the native conversion keep their complete
//! typed results. The existing shared directory moves last; no descriptor,
//! native owning wrapper, panic payload or uncertain input can be extracted.

use super::{OwnedPinnedShardStore, OwnedShardStoreDirectory};
use crate::{
    CheckedDirectoryValidationChecks, CheckedDirectoryValidationFailure, NativeDirectoryBudget,
    NativeDirectoryFailure, NativeDirectoryLimits, NativeDirectoryUsage, OwnedDirectoryHandle,
    RetainedCheckedDirectoryValidation, RetainedCheckedDirectoryValidationError,
    checked_directory_validation_cost, native_directory_cost,
};
use shardlog::{
    OpenedPinnedShardLog, OpenedPinnedShardStore, PinnedShardLogOpenChecks,
    PinnedShardLogOpenError, PinnedShardLogOpenFailure, PinnedShardLogOpenObservation, Reservation,
    ShardDirectoryCheckFailure, ShardDirectoryChecks, ShardLogResult, StorageFetchBudget,
    StorageFetchFailure, StorageFetchUsage,
};
use std::{
    any::Any,
    cell::RefCell,
    collections::TryReserveError,
    error::Error,
    fmt,
    mem::size_of,
    panic::{AssertUnwindSafe, catch_unwind},
    time::Instant,
};

type Validation =
    Result<RetainedCheckedDirectoryValidation, RetainedCheckedDirectoryValidationError>;
type Native = Result<OpenedPinnedShardLog, PinnedShardLogOpenError>;
type Step<T> = Result<T, OwnedPinnedShardLogFailure>;

/// Plain per-conversion bridge and retained validation framing; no admission.
///
/// Add this once for each actual conversion to one shared Core policy ceiling
/// and one shared native constructor ceiling. Neither configured maximum is
/// repeated here, and this getter creates or reserves no budget. Actual calls
/// continue spending both supplied counters without renewal or refund.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnedPinnedShardLogCost {
    /// Independently named bridge values, two retains and two validation frames.
    pub declared_bytes: usize,
    /// This bridge allocates no strings; existing validation pays its buffers.
    pub string_bytes: usize,
    /// Named bridge, two handle-retain and two validation-framing slots.
    pub entries: usize,
    /// Source-visible bridge work plus both real validation framing costs.
    pub work_bytes: usize,
    /// Per-conversion validation framing already included above; not two policies.
    pub validation_framing_bytes: usize,
    /// Per-conversion validation framing slots already included above.
    pub validation_framing_entries: usize,
    /// Two actual parent handle values; no existing shared directory Arc clone.
    pub parent_retention_bytes: usize,
    /// One bridge operation and two actual consuming validation operations.
    pub core_operations: usize,
}

/// Actual terminal category; every entered richer outcome stays privately held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnedPinnedShardLogFailure {
    /// Same cumulative Core counters or current minimum E refused.
    Core,
    /// Same original native counters or E refused.
    Native,
    /// Captured actual work/health refused.
    Checkpoint,
    /// The actual pre-conversion checked validation refused.
    PreValidation,
    /// The native canonical conversion returned its owning refusal.
    Conversion,
    /// Actual post-conversion checked validation refused after native completion.
    PostValidation,
    /// A bridge callback or entered producer unwound; payload stays owned.
    Unwind,
    /// A private whole-owner state transition was unavailable.
    Invariant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    FixedAdmission,
    Split,
    PreRetain,
    PreValidation,
    Native,
    PostRetain,
    PostValidation,
    Usage,
}

#[derive(Default)]
struct Control {
    core: [Option<Result<(), NativeDirectoryFailure>>; 2],
    native: [Option<Result<(), StorageFetchFailure>>; 2],
    outer: Option<Result<(), ShardDirectoryCheckFailure>>,
    core_deny_attempted: bool,
    native_deny_attempted: bool,
    outer_deny_attempted: bool,
    outer_deny_returned: bool,
    core_deny_panic: Option<Box<dyn Any + Send>>,
    native_deny_panic: Option<Box<dyn Any + Send>>,
    outer_deny_panic: Option<Box<dyn Any + Send>>,
}
struct Work<'a> {
    core: &'a NativeDirectoryBudget,
    native: &'a StorageFetchBudget,
    outer: &'a dyn CheckedDirectoryValidationChecks,
    control: &'a RefCell<Control>,
}
impl Work<'_> {
    fn check(&self) -> Step<()> {
        self.control.borrow_mut().core[0] = Some(self.core.check());
        if matches!(self.control.borrow().core[0], Some(Err(_))) {
            return Err(OwnedPinnedShardLogFailure::Core);
        }
        self.control.borrow_mut().native[0] = Some(self.native.check());
        if matches!(self.control.borrow().native[0], Some(Err(_))) {
            return Err(OwnedPinnedShardLogFailure::Native);
        }
        // No RefCell borrow is held across the real composing callback.
        let actual = self.outer.check();
        self.control.borrow_mut().outer = Some(actual);
        self.control.borrow_mut().core[1] = Some(self.core.check());
        self.control.borrow_mut().native[1] = Some(self.native.check());
        if matches!(self.control.borrow().core[1], Some(Err(_))) {
            return Err(OwnedPinnedShardLogFailure::Core);
        }
        if matches!(self.control.borrow().native[1], Some(Err(_))) {
            return Err(OwnedPinnedShardLogFailure::Native);
        }
        if matches!(self.control.borrow().outer, Some(Err(_))) {
            return Err(OwnedPinnedShardLogFailure::Checkpoint);
        }
        Ok(())
    }
    fn deny_core(&self) {
        if self.control.borrow().core_deny_attempted {
            return;
        }
        self.control.borrow_mut().core_deny_attempted = true;
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.core.deny())) {
            self.control.borrow_mut().core_deny_panic = Some(payload);
        }
    }
    fn deny_native(&self) {
        if self.control.borrow().native_deny_attempted {
            return;
        }
        self.control.borrow_mut().native_deny_attempted = true;
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.native.deny())) {
            self.control.borrow_mut().native_deny_panic = Some(payload);
        }
    }
    fn deny_outer(&self) {
        if self.control.borrow().outer_deny_attempted {
            return;
        }
        // An unwound actual outer attempt is terminal and is never retried.
        self.control.borrow_mut().outer_deny_attempted = true;
        self.outer.deny();
        self.control.borrow_mut().outer_deny_returned = true;
    }
    fn deny_bridge(&self) {
        self.deny_core();
        self.deny_native();
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.deny_outer())) {
            self.control.borrow_mut().outer_deny_panic = Some(payload);
        }
    }
}
struct ValidationChecks<'a, 'b>(&'a Work<'b>);
struct NativeChecks<'a, 'b>(&'a Work<'b>);
impl ShardDirectoryChecks for ValidationChecks<'_, '_> {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        self.0
            .check()
            .map_err(|_| ShardDirectoryCheckFailure::Refused)
    }
}
impl ShardDirectoryChecks for NativeChecks<'_, '_> {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        self.0
            .check()
            .map_err(|_| ShardDirectoryCheckFailure::Refused)
    }
}
impl CheckedDirectoryValidationChecks for ValidationChecks<'_, '_> {
    fn deny(&self) {
        // This producer invokes us after independently attempting BOTH budget
        // denials. Their actual panic/results remain in its validation carrier.
        {
            let mut c = self.0.control.borrow_mut();
            c.core_deny_attempted = true;
            c.native_deny_attempted = true;
        }
        self.0.deny_outer();
    }
}
impl PinnedShardLogOpenChecks for NativeChecks<'_, '_> {
    fn deny(&self) {
        // Native conversion has attempted only its native denial. Ensure the
        // SAME Core denial before the first actual outer-health attempt.
        self.0.control.borrow_mut().native_deny_attempted = true;
        self.0.deny_core();
        self.0.deny_outer();
    }
}

#[derive(Default)]
struct Effects {
    cap: Option<Result<(), NativeDirectoryFailure>>,
    cost: Option<Result<OwnedPinnedShardLogCost, NativeDirectoryFailure>>,
    fee: Option<Result<(), NativeDirectoryFailure>>,
    original_native_expiry: Option<Instant>,
    entry_core_minimum: Option<Instant>,
    entered: Option<Phase>,
    core_usage: Option<Result<NativeDirectoryUsage, NativeDirectoryFailure>>,
    native_usage: Option<Result<StorageFetchUsage, StorageFetchFailure>>,
    operation_panic: Option<Box<dyn Any + Send>>,
    control: Option<Control>,
}
struct Scope {
    effects: Effects,
    pre_validation: Option<Validation>,
    post_validation: Option<Validation>,
    pending_handle: Option<OwnedDirectoryHandle>,
    native: Option<Native>,
    remaining_native: Option<OpenedPinnedShardStore>,
    // Original whole input owns its directory last before the actual split.
    early: Option<OwnedPinnedShardStore>,
    // LAST after the split: same original Arc, native pin, and Core root.
    directory: Option<OwnedShardStoreDirectory>,
}
impl Scope {
    fn new(input: OwnedPinnedShardStore) -> Self {
        Self {
            effects: Effects::default(),
            pre_validation: None,
            post_validation: None,
            pending_handle: None,
            native: None,
            remaining_native: None,
            early: Some(input),
            directory: None,
        }
    }
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
            return Err(OwnedPinnedShardLogFailure::Core);
        }
        Ok(())
    }
    fn retain_parent(&mut self, work: &Work<'_>, phase: Phase) -> Step<()> {
        work.check()?;
        let (bytes, entries, cpu) = retain_fee().ok_or(OwnedPinnedShardLogFailure::Invariant)?;
        self.charge(work, 0, bytes, entries, cpu)?;
        work.check()?;
        self.effects.entered = Some(phase);
        let directory = self
            .directory
            .as_ref()
            .ok_or(OwnedPinnedShardLogFailure::Invariant)?;
        self.pending_handle = Some(directory.inner.parent.retain());
        completed(phase);
        work.check()
    }
    fn validate(&mut self, work: &Work<'_>, before: bool) -> Step<()> {
        work.check()?;
        let input = self
            .pending_handle
            .take()
            .ok_or(OwnedPinnedShardLogFailure::Invariant)?;
        self.effects.entered = Some(if before {
            Phase::PreValidation
        } else {
            Phase::PostValidation
        });
        let actual =
            input.validate_for_storage_fetch_until(work.core, work.native, &ValidationChecks(work));
        if before {
            self.pre_validation = Some(actual);
        } else {
            self.post_validation = Some(actual);
        }
        completed(if before {
            Phase::PreValidation
        } else {
            Phase::PostValidation
        });
        work.check()?;
        let result = if before {
            self.pre_validation.as_ref()
        } else {
            self.post_validation.as_ref()
        };
        if matches!(result, Some(Ok(_))) {
            Ok(())
        } else {
            Err(if before {
                OwnedPinnedShardLogFailure::PreValidation
            } else {
                OwnedPinnedShardLogFailure::PostValidation
            })
        }
    }
    fn run(&mut self, work: &Work<'_>) -> Step<()> {
        self.effects.original_native_expiry = Some(work.native.absolute_expiry());
        self.effects.cap = Some(work.core.cap_expiry(work.native.absolute_expiry()));
        self.effects.entry_core_minimum = Some(work.core.absolute_expiry());
        if matches!(self.effects.cap, Some(Err(_))) {
            return Err(OwnedPinnedShardLogFailure::Core);
        }
        work.check()?;
        self.effects.cost = Some(owned_pinned_shard_log_cost(work.core.limits()));
        if matches!(self.effects.cost, Some(Err(_))) {
            return Err(OwnedPinnedShardLogFailure::Core);
        }
        let (bytes, entries, cpu) = fixed_fee().ok_or(OwnedPinnedShardLogFailure::Invariant)?;
        self.charge(work, 1, bytes, entries, cpu)?;
        self.effects.entered = Some(Phase::FixedAdmission);
        completed(Phase::FixedAdmission);
        work.check()?;
        let OwnedPinnedShardStore { opened, directory } = self
            .early
            .take()
            .ok_or(OwnedPinnedShardLogFailure::Invariant)?;
        self.remaining_native = Some(opened);
        self.directory = Some(directory);
        self.effects.entered = Some(Phase::Split);
        completed(Phase::Split);
        work.check()?;
        self.retain_parent(work, Phase::PreRetain)?;
        self.validate(work, true)?;
        work.check()?;
        let input = self
            .remaining_native
            .take()
            .ok_or(OwnedPinnedShardLogFailure::Invariant)?;
        self.effects.entered = Some(Phase::Native);
        self.native = Some(input.into_shard_log_until(work.native, &NativeChecks(work)));
        // The WHOLE actual native Result is already in this outside-catch scope.
        completed(Phase::Native);
        work.check()?;
        if !matches!(self.native, Some(Ok(_))) {
            return Err(OwnedPinnedShardLogFailure::Conversion);
        }
        self.retain_parent(work, Phase::PostRetain)?;
        self.validate(work, false)?;
        work.check()?;
        self.effects.core_usage = Some(work.core.usage());
        self.effects.native_usage = Some(work.native.usage());
        self.effects.entered = Some(Phase::Usage);
        completed(Phase::Usage);
        work.check()?;
        if matches!(self.effects.core_usage, Some(Err(_))) {
            return Err(OwnedPinnedShardLogFailure::Core);
        }
        if matches!(self.effects.native_usage, Some(Err(_))) {
            return Err(OwnedPinnedShardLogFailure::Native);
        }
        Ok(())
    }
    fn native_observation(&self) -> Option<&PinnedShardLogOpenObservation> {
        match self.native.as_ref()? {
            Ok(log) => Some(log.observation()),
            Err(error) => Some(error.observation()),
        }
    }
}

/// Canonical Core log retaining both validations and the actual native result.
///
/// No budget, Session, callback or serving permission is stored. This first
/// bridge supplies bounded conversion only; bounded log operations need their
/// own genuine producers. There is no native/store/descriptor extractor.
pub struct OwnedPinnedShardLog {
    scope: Scope,
}
/// Refusal retaining the actual input or native outcome and both validations.
///
/// No owner-bearing error, handle, native result, panic payload or retry can be
/// extracted, including through Error::source. Success and refusal keep the
/// same shared directory last until every actual retained outcome is dropped.
pub struct RetainedOwnedPinnedShardLogError {
    failure: OwnedPinnedShardLogFailure,
    scope: Scope,
}

/// Safe borrowed facts from one actual pre/post consuming validation.
pub struct OwnedPinnedShardLogValidation<'a> {
    actual: &'a Validation,
}
impl OwnedPinnedShardLogValidation<'_> {
    /// Whether that actual checked validation returned success.
    pub fn returned_success(&self) -> bool {
        self.actual.is_ok()
    }
    /// Actual validation refusal category, if present.
    pub fn failure(&self) -> Option<CheckedDirectoryValidationFailure> {
        self.actual.as_ref().err().map(|e| e.failure())
    }
    /// Actual retained result-buffer capacity without an owning-carrier borrow.
    pub fn retained_capacity(&self) -> usize {
        match self.actual {
            Ok(v) => v.retained_capacity(),
            Err(v) => v.retained_capacity(),
        }
    }
    /// Actual retained Core usage Result, without budget extraction.
    pub fn core_usage(&self) -> Option<&Result<NativeDirectoryUsage, NativeDirectoryFailure>> {
        match self.actual {
            Ok(v) => v.core_usage(),
            Err(v) => v.core_usage(),
        }
    }
    /// Actual retained shared native usage Result, without counter renewal.
    pub fn native_usage(&self) -> Option<&Result<StorageFetchUsage, StorageFetchFailure>> {
        match self.actual {
            Ok(v) => v.native_usage(),
            Err(v) => v.native_usage(),
        }
    }
    /// Original native E and the actual Core entry cap, not a deadline grant.
    pub fn entry_deadlines(&self) -> Option<(Instant, Instant)> {
        match self.actual {
            Ok(v) => v.entry_deadlines(),
            Err(v) => v.entry_deadlines(),
        }
    }
    /// Actual validation operation/Core/native/outer panic-slot occupancy.
    pub fn panic_observations(&self) -> [bool; 4] {
        match self.actual {
            Ok(_) => [false; 4],
            Err(v) => v.panic_observations(),
        }
    }
}

/// Safe native constructor facts; the actual owning native result stays private.
pub struct OwnedPinnedShardLogObservation<'a> {
    actual: &'a PinnedShardLogOpenObservation,
}
impl OwnedPinnedShardLogObservation<'_> {
    /// Actual original conversion expiry retained as data.
    pub fn absolute_expiry(&self) -> Instant {
        self.actual.absolute_expiry()
    }
    /// Actual catalog length, capacity and prepaid scan work.
    pub fn catalog_shape(&self) -> Option<(usize, usize, usize)> {
        self.actual.catalog_shape()
    }
    /// Actual validated sequence prefix; no owning result or file is exposed.
    pub fn retained_sequences(&self) -> &[u64] {
        self.actual.retained_sequences()
    }
    /// Actual sequence allocation capacity, independently of length.
    pub fn sequence_capacity(&self) -> usize {
        self.actual.sequence_capacity()
    }
    /// Actual last catalog reservation, if entered.
    pub fn retained_reservation(&self) -> Option<Reservation> {
        self.actual.retained_reservation()
    }
    /// Actual canonical bounds Result without any owner-bearing wrapper.
    pub fn bounds_result(&self) -> Option<&ShardLogResult<Option<(u64, u64)>>> {
        self.actual.bounds_result()
    }
    /// Actual allocation error, if present.
    pub fn allocation_error(&self) -> Option<&TryReserveError> {
        self.actual.allocation_error()
    }
    /// Actual shared native usage, without returning the budget.
    pub fn usage(&self) -> Option<&Result<StorageFetchUsage, StorageFetchFailure>> {
        self.actual.usage()
    }
    /// Actual native scalar refusal; no native owning error can escape.
    pub fn failure(&self) -> Option<&PinnedShardLogOpenFailure> {
        self.actual.failure()
    }
    /// Which actual native panic slots are occupied, without Any/downcast access.
    pub fn has_panic_payloads(&self) -> [bool; 3] {
        self.actual.panic_payloads().map(|v| v.is_some())
    }
}
impl OwnedPinnedShardLog {
    /// Actual native half-open canonical bounds, not reconstructed observations.
    pub fn bounds(&self) -> Option<(u64, u64)> {
        match self.scope.native.as_ref() {
            Some(Ok(v)) => v.bounds(),
            _ => None,
        }
    }
    /// Safe actual pre-conversion validation facts.
    pub fn pre_validation(&self) -> Option<OwnedPinnedShardLogValidation<'_>> {
        self.scope
            .pre_validation
            .as_ref()
            .map(|actual| OwnedPinnedShardLogValidation { actual })
    }
    /// Safe actual post-conversion validation facts.
    pub fn post_validation(&self) -> Option<OwnedPinnedShardLogValidation<'_>> {
        self.scope
            .post_validation
            .as_ref()
            .map(|actual| OwnedPinnedShardLogValidation { actual })
    }
    /// Safe actual native conversion observations, with no native wrapper borrow.
    pub fn observation(&self) -> Option<OwnedPinnedShardLogObservation<'_>> {
        self.scope
            .native_observation()
            .map(|actual| OwnedPinnedShardLogObservation { actual })
    }
}
impl RetainedOwnedPinnedShardLogError {
    /// Actual terminal bridge category.
    pub fn failure(&self) -> OwnedPinnedShardLogFailure {
        self.failure
    }
    /// Actual native producer returned Ok or Err; None means it never returned.
    pub fn native_returned_success(&self) -> Option<bool> {
        self.scope.native.as_ref().map(Result::is_ok)
    }
    /// Safe actual pre-conversion validation facts.
    pub fn pre_validation(&self) -> Option<OwnedPinnedShardLogValidation<'_>> {
        self.scope
            .pre_validation
            .as_ref()
            .map(|actual| OwnedPinnedShardLogValidation { actual })
    }
    /// Safe actual post-conversion validation facts.
    pub fn post_validation(&self) -> Option<OwnedPinnedShardLogValidation<'_>> {
        self.scope
            .post_validation
            .as_ref()
            .map(|actual| OwnedPinnedShardLogValidation { actual })
    }
    /// Safe actual native prefix/results, including a late successful conversion.
    pub fn observation(&self) -> Option<OwnedPinnedShardLogObservation<'_>> {
        self.scope
            .native_observation()
            .map(|actual| OwnedPinnedShardLogObservation { actual })
    }
    /// Actual bridge operation/Core-deny/native-deny/outer-deny panic occupancy.
    pub fn has_panic_payloads(&self) -> [bool; 4] {
        let c = self.scope.effects.control.as_ref();
        [
            self.scope.effects.operation_panic.is_some(),
            c.is_some_and(|v| v.core_deny_panic.is_some()),
            c.is_some_and(|v| v.native_deny_panic.is_some()),
            c.is_some_and(|v| v.outer_deny_panic.is_some()),
        ]
    }
    /// Actual outer-health denial attempt/normal return; a panic is not retried.
    pub fn outer_denial(&self) -> (bool, bool) {
        self.scope
            .effects
            .control
            .as_ref()
            .map_or((false, false), |v| {
                (v.outer_deny_attempted, v.outer_deny_returned)
            })
    }
}
impl fmt::Debug for OwnedPinnedShardLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedPinnedShardLog")
            .field("bounds", &self.bounds())
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for RetainedOwnedPinnedShardLogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedOwnedPinnedShardLogError")
            .field("failure", &self.failure)
            .field("native_returned_success", &self.native_returned_success())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedOwnedPinnedShardLogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Core canonical log conversion refused: {:?}",
            self.failure
        )
    }
}
impl Error for RetainedOwnedPinnedShardLogError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Only actual non-owning Core budget failures are forwarded. Never
        // return native/validation owning errors or arbitrary panic values.
        if let Some(Err(error)) = self.scope.effects.cap.as_ref() {
            return Some(error);
        }
        if let Some(Err(error)) = self.scope.effects.fee.as_ref() {
            return Some(error);
        }
        if let Some(Err(error)) = self.scope.effects.cost.as_ref() {
            return Some(error);
        }
        None
    }
}

fn retain_fee() -> Option<(usize, usize, usize)> {
    let bytes =
        size_of::<OwnedDirectoryHandle>().checked_add(size_of::<Option<OwnedDirectoryHandle>>())?;
    let entries = 2usize;
    // reserve() itself adds the entry work fee; this argument pays bytes and
    // the real Arc increment once, without hiding a second entry charge.
    Some((bytes, entries, bytes.checked_add(1)?))
}
fn fixed_fee() -> Option<(usize, usize, usize)> {
    // Independently retained logical values/headers/panics are named even when
    // an enclosing carrier physically contains them. No native fee is replaced.
    let values = [
        size_of::<Scope>(),
        size_of::<Effects>(),
        size_of::<Control>(),
        size_of::<RefCell<Control>>(),
        size_of::<Work<'static>>(),
        size_of::<ValidationChecks<'static, 'static>>(),
        size_of::<NativeChecks<'static, 'static>>(),
        size_of::<OwnedPinnedShardLog>(),
        size_of::<RetainedOwnedPinnedShardLogError>(),
        size_of::<OwnedPinnedShardLogCost>(),
        size_of::<OwnedPinnedShardLogFailure>(),
        size_of::<OwnedPinnedShardLogValidation<'static>>(),
        size_of::<OwnedPinnedShardLogObservation<'static>>(),
        size_of::<Option<OwnedPinnedShardStore>>(),
        size_of::<Option<OpenedPinnedShardStore>>(),
        size_of::<Option<OwnedShardStoreDirectory>>(),
        size_of::<Option<Native>>(),
        size_of::<Native>(),
        size_of::<OpenedPinnedShardLog>(),
        size_of::<PinnedShardLogOpenError>(),
        size_of::<Option<Validation>>(),
        size_of::<Validation>(), // actual pre-validation retention
        size_of::<Option<Validation>>(),
        size_of::<Validation>(), // actual post-validation retention
        size_of::<Option<Result<OwnedPinnedShardLogCost, NativeDirectoryFailure>>>(),
        size_of::<Result<OwnedPinnedShardLogCost, NativeDirectoryFailure>>(),
        size_of::<Option<Result<(), NativeDirectoryFailure>>>(), // minimum cap
        size_of::<Option<Result<(), NativeDirectoryFailure>>>(), // fixed/retain admission
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
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(), // operation
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(), // Core denial
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(), // native denial
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(), // outer denial
        size_of::<Result<Step<()>, Box<dyn Any + Send>>>(),
        size_of::<Step<()>>(),
        size_of::<Result<(), Box<dyn Any + Send>>>(),
        size_of::<Result<(), Box<dyn Any + Send>>>(),
        size_of::<Result<(), Box<dyn Any + Send>>>(),
        size_of::<[bool; 4]>(),
        size_of::<[usize; 6]>(),
    ];
    let bytes = values.into_iter().try_fold(0usize, usize::checked_add)?;
    let entries = values.len();
    // NativeDirectoryBudget::reserve adds the per-entry work fee itself.
    Some((bytes, entries, bytes))
}
/// Derive this bridge's plain per-conversion cost, without creating work.
///
/// The supplied Core global policy and the shared native constructor ceiling
/// are prepaid once separately. The difference between the checked validator
/// and Core global getter is included TWICE for two simultaneously retained
/// validation frames. Each actual validator still spends its own fixed47 and
/// real traversal costs against the SAME Core/native counters at entry.
pub fn owned_pinned_shard_log_cost(
    limits: NativeDirectoryLimits,
) -> Result<OwnedPinnedShardLogCost, NativeDirectoryFailure> {
    let core = native_directory_cost(limits)?;
    let validation = checked_directory_validation_cost(limits)?;
    let validation_bytes = validation
        .declared_bytes
        .checked_sub(core.declared_bytes)
        .and_then(|n| n.checked_mul(2))
        .ok_or(NativeDirectoryFailure::Capacity)?;
    let validation_entries = validation
        .entries
        .checked_sub(core.entries)
        .and_then(|n| n.checked_mul(2))
        .ok_or(NativeDirectoryFailure::Capacity)?;
    let validation_work = validation
        .work_bytes
        .checked_sub(core.work_bytes)
        .and_then(|n| n.checked_mul(2))
        .ok_or(NativeDirectoryFailure::Capacity)?;
    let (fixed, entries, work) = fixed_fee().ok_or(NativeDirectoryFailure::Capacity)?;
    let (retain, retain_entries, retain_work) =
        retain_fee().ok_or(NativeDirectoryFailure::Capacity)?;
    let parent_retention_bytes = retain
        .checked_mul(2)
        .ok_or(NativeDirectoryFailure::Capacity)?;
    Ok(OwnedPinnedShardLogCost {
        declared_bytes: fixed
            .checked_add(parent_retention_bytes)
            .and_then(|n| n.checked_add(validation_bytes))
            .ok_or(NativeDirectoryFailure::Capacity)?,
        string_bytes: 0,
        entries: retain_entries
            .checked_mul(2)
            .and_then(|n| n.checked_add(entries))
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
    })
}
impl OwnedPinnedShardStore {
    /// Consume this actual opened Core store with retained pre/post validation.
    ///
    /// Callers prepay the one Core policy, one shared native constructor
    /// ceiling and this per-conversion bridge cost while their original Session
    /// is ACTIVE. No budget is created/reset here. Three actual Core operations
    /// enter on success: this bridge and two consuming validations. The same
    /// Core minimum is capped to native E before every physical path; all late
    /// native/validation outcomes remain privately owned before callbacks.
    #[allow(
        clippy::result_large_err,
        reason = "whole early input and entered owning results cannot move into an unpaid heap allocation"
    )]
    pub fn into_shard_log_until(
        self,
        native: &StorageFetchBudget,
        core: &NativeDirectoryBudget,
        checks: &dyn CheckedDirectoryValidationChecks,
    ) -> Result<OwnedPinnedShardLog, RetainedOwnedPinnedShardLogError> {
        let mut scope = Scope::new(self);
        let control = RefCell::new(Control::default());
        let work = Work {
            core,
            native,
            outer: checks,
            control: &control,
        };
        let observed = catch_unwind(AssertUnwindSafe(|| scope.run(&work)));
        let failure = match observed {
            Ok(Ok(())) => None,
            Ok(Err(failure)) => Some(failure),
            Err(payload) => {
                scope.effects.operation_panic = Some(payload);
                Some(OwnedPinnedShardLogFailure::Unwind)
            }
        };
        if failure.is_some() {
            work.deny_bridge();
        }
        scope.effects.control = Some(control.into_inner());
        match failure {
            None => Ok(OwnedPinnedShardLog { scope }),
            Some(failure) => Err(RetainedOwnedPinnedShardLogError { failure, scope }),
        }
    }
}

#[cfg(test)]
#[path = "owned_pinned_shard_log_tests.rs"]
mod tests;
#[cfg(test)]
fn completed(phase: Phase) {
    tests::completed(phase);
}
#[cfg(not(test))]
fn completed(_: Phase) {}
