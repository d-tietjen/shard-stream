//! Consuming validation with retained native results and borrowed composing work.

use super::*;
use crate::{
    NativeDirectoryBudget, NativeDirectoryFailure, NativeDirectoryLimits, NativeDirectoryUsage,
    native_directory_cost,
};
use rustix::{fd::OwnedFd, fs::Stat};
use shardlog::{
    ShardDirectoryCheckFailure, ShardDirectoryChecks, StorageFetchBudget, StorageFetchFailure,
    StorageFetchUsage,
};
use std::{
    any::Any,
    collections::TryReserveError,
    error::Error,
    ffi::{CStr, CString, NulError},
    fmt,
    mem::size_of,
    panic::{AssertUnwindSafe, catch_unwind},
    time::Instant,
};

/// Borrowed checks of the actual composing work and captured health.
///
/// Implementations inspect their original counters and health, and deny that
/// same work. A caller sharing this adapter across several producers records
/// an actual denial attempt before calling health, so an unwind is not retried.
/// This trait is neither an ownership constructor nor serving permission.
pub trait CheckedDirectoryValidationChecks: ShardDirectoryChecks {
    /// Deny the same real composing work and captured health.
    fn deny(&self);
}

/// The entered physical, conversion or identity phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckedDirectoryValidationPhase {
    /// Actual retained-root pathname metadata.
    RootPathMetadata,
    /// Actual ancestor pathname metadata.
    AncestorMetadata,
    /// Open of the real filesystem root.
    RootOpened,
    /// Bounded conversion of one genuine component to a native C string.
    ComponentName,
    /// Actual no-follow open of that component.
    ComponentOpened,
    /// Actual resolved-root descriptor stat.
    ResolvedRootStat,
    /// Actual original root descriptor stat.
    RetainedRootStat,
    /// Actual named lock stat without following a symlink.
    NamedLockStat,
    /// Actual original lock descriptor stat.
    RetainedLockStat,
    /// Root handle pathname metadata after original-owner validation.
    HandlePathMetadata,
    /// Actual root handle descriptor metadata.
    HandleDescriptorMetadata,
    /// Actual child handle metadata.
    ChildMetadata,
    /// Actual child descriptor stat.
    ChildDescriptorStat,
    /// Actual named child stat without following a symlink.
    NamedChildStat,
    /// Shared original pure identity policy.
    Identity,
}

/// Terminal category; actual raw results and refusals stay separately retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckedDirectoryValidationFailure {
    /// Same cumulative Core budget or minimum E refused.
    Core,
    /// Same supplied native fetch budget refused.
    Native,
    /// Actual composing checks refused.
    Checkpoint,
    /// Actual native filesystem operation or identity predicate refused.
    Ownership,
    /// Actual bounded record allocation refused.
    Allocation,
    /// Actual component-to-C-string conversion refused.
    Name,
    /// An entered producer or callback unwound.
    Unwind,
    /// A private retained-state transition was unavailable.
    Invariant,
}

/// Plain setup plus one cumulative Core policy ceiling, excluding native setup.
///
/// The caller prepays its supplied native fetch ceiling once, independently.
/// Actual validation spends the SAME existing Core budget incrementally. These
/// logical facts exclude allocator/kernel internals and measured process RSS.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckedDirectoryValidationCost {
    /// Core configured values plus independently named validation values.
    pub declared_bytes: usize,
    /// Configured path/name subset already included in declared bytes.
    pub string_bytes: usize,
    /// Core configured and fixed validation value/retention slots.
    pub entries: usize,
    /// Core configured and fixed validation work, including entry fees.
    pub work_bytes: usize,
}

pub(crate) type Step<T> = std::result::Result<T, CheckedDirectoryValidationFailure>;
pub(crate) enum Value {
    Metadata(io::Result<std::fs::Metadata>),
    Stat(std::result::Result<Stat, rustix::io::Errno>),
    Open(std::result::Result<OwnedFd, rustix::io::Errno>),
    Name(std::result::Result<CString, NulError>),
    Identity(Result<()>),
}
struct Outcome {
    phase: CheckedDirectoryValidationPhase,
    ordinal: usize,
    value: Value,
}

/// Safe borrowed observation of an actual retained phase result.
///
/// No native descriptor, owning result or input handle can be extracted.
pub struct CheckedDirectoryValidationObservation<'a> {
    outcome: &'a Outcome,
}
impl CheckedDirectoryValidationObservation<'_> {
    /// Actual phase represented by this result.
    pub fn phase(&self) -> CheckedDirectoryValidationPhase {
        self.outcome.phase
    }
    /// Actual ordinal in the retained operation trace.
    pub fn ordinal(&self) -> usize {
        self.outcome.ordinal
    }
    /// Whether that actual producer returned success, independently of readiness.
    pub fn returned_success(&self) -> bool {
        match &self.outcome.value {
            Value::Metadata(r) => r.is_ok(),
            Value::Stat(r) => r.is_ok(),
            Value::Open(r) => r.is_ok(),
            Value::Name(r) => r.is_ok(),
            Value::Identity(r) => r.is_ok(),
        }
    }
    /// Actual standard I/O error, without an owner-bearing wrapper.
    pub fn io_error(&self) -> Option<&io::Error> {
        match &self.outcome.value {
            Value::Metadata(Err(e)) | Value::Identity(Err(DirectoryOwnershipError::Io(e))) => {
                Some(e)
            }
            _ => None,
        }
    }
    /// Actual native errno returned by a real stat/open.
    pub fn native_errno(&self) -> Option<rustix::io::Errno> {
        match &self.outcome.value {
            Value::Stat(Err(e)) | Value::Open(Err(e)) => Some(*e),
            _ => None,
        }
    }
    /// Actual metadata, containing no descriptor or owner extraction.
    pub fn metadata(&self) -> Option<&std::fs::Metadata> {
        match &self.outcome.value {
            Value::Metadata(Ok(v)) => Some(v),
            _ => None,
        }
    }
    /// Actual native stat value, containing no descriptor.
    pub fn native_stat(&self) -> Option<&Stat> {
        match &self.outcome.value {
            Value::Stat(Ok(v)) => Some(v),
            _ => None,
        }
    }
}

pub(crate) struct Work<'a> {
    pub(crate) core: &'a NativeDirectoryBudget,
    pub(crate) native: &'a StorageFetchBudget,
    checks: &'a dyn CheckedDirectoryValidationChecks,
}
#[derive(Default)]
struct Checks {
    core: [Option<std::result::Result<(), NativeDirectoryFailure>>; 2],
    native: [Option<std::result::Result<(), StorageFetchFailure>>; 2],
    outer: Option<std::result::Result<(), ShardDirectoryCheckFailure>>,
}
pub(crate) struct Effects {
    values: Vec<Outcome>,
    pending: Option<Outcome>,
    allocation: Option<std::result::Result<(), TryReserveError>>,
    cap: Option<std::result::Result<(), NativeDirectoryFailure>>,
    fee: Option<std::result::Result<(), NativeDirectoryFailure>>,
    rule: Option<std::result::Result<(), NativeDirectoryFailure>>,
    checks: Checks,
    entered: Option<CheckedDirectoryValidationPhase>,
    original_native_expiry: Option<Instant>,
    core_minimum: Option<Instant>,
    core_usage: Option<std::result::Result<NativeDirectoryUsage, NativeDirectoryFailure>>,
    native_usage: Option<std::result::Result<StorageFetchUsage, StorageFetchFailure>>,
    operation_panic: Option<Box<dyn Any + Send>>,
    core_deny_panic: Option<Box<dyn Any + Send>>,
    native_deny_panic: Option<Box<dyn Any + Send>>,
    outer_deny_panic: Option<Box<dyn Any + Send>>,
    outer_deny_attempted: bool,
    outer_deny_returned: bool,
}
impl Effects {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            pending: None,
            allocation: None,
            cap: None,
            fee: None,
            rule: None,
            checks: Checks::default(),
            entered: None,
            original_native_expiry: None,
            core_minimum: None,
            core_usage: None,
            native_usage: None,
            operation_panic: None,
            core_deny_panic: None,
            native_deny_panic: None,
            outer_deny_panic: None,
            outer_deny_attempted: false,
            outer_deny_returned: false,
        }
    }
    pub(crate) fn checkpoint(&mut self, work: &Work<'_>) -> Step<()> {
        // A prior failed checkpoint is terminal. Only settled Ok(()) values can
        // be replaced; an actual refusal stays in its original typed slot.
        self.checks.core[0] = Some(work.core.check());
        if matches!(self.checks.core[0], Some(Err(_))) {
            return Err(CheckedDirectoryValidationFailure::Core);
        }
        self.checks.native[0] = Some(work.native.check());
        if matches!(self.checks.native[0], Some(Err(_))) {
            return Err(CheckedDirectoryValidationFailure::Native);
        }
        self.checks.outer = Some(work.checks.check());
        self.checks.core[1] = Some(work.core.check());
        self.checks.native[1] = Some(work.native.check());
        if matches!(self.checks.core[1], Some(Err(_))) {
            return Err(CheckedDirectoryValidationFailure::Core);
        }
        if matches!(self.checks.native[1], Some(Err(_))) {
            return Err(CheckedDirectoryValidationFailure::Native);
        }
        if matches!(self.checks.outer, Some(Err(_))) {
            return Err(CheckedDirectoryValidationFailure::Checkpoint);
        }
        Ok(())
    }
    pub(crate) fn charge(
        &mut self,
        work: &Work<'_>,
        calls: usize,
        bytes: usize,
        strings: usize,
        entries: usize,
        work_bytes: usize,
    ) -> Step<()> {
        self.fee = Some(
            work.core
                .reserve(0, calls, bytes, strings, entries, work_bytes),
        );
        if matches!(self.fee, Some(Err(_))) {
            return Err(CheckedDirectoryValidationFailure::Core);
        }
        Ok(())
    }
    pub(crate) fn scan(&mut self, work: &Work<'_>, bytes: usize, passes: usize) -> Step<()> {
        self.checkpoint(work)?;
        if bytes > work.core.limits().max_path_bytes {
            return self.capacity();
        }
        let Some(fee) = bytes.checked_add(1).and_then(|n| n.checked_mul(passes)) else {
            return self.capacity();
        };
        self.charge(work, 0, 0, 0, 0, fee)?;
        self.checkpoint(work)
    }
    pub(crate) fn capacity<T>(&mut self) -> Step<T> {
        self.rule = Some(Err(NativeDirectoryFailure::Capacity));
        Err(CheckedDirectoryValidationFailure::Core)
    }
    pub(crate) fn prepare(
        &mut self,
        work: &Work<'_>,
        phase: CheckedDirectoryValidationPhase,
        calls: usize,
        typed_bytes: usize,
        strings: usize,
    ) -> Step<()> {
        self.checkpoint(work)?;
        let Some(bytes) = size_of::<Outcome>()
            .checked_add(size_of::<Value>())
            .and_then(|n| n.checked_add(typed_bytes))
            .and_then(|n| n.checked_add(strings))
        else {
            return self.capacity();
        };
        // Four bounded passes for each native pathname/name buffer cover its
        // source-visible NUL scan, copy and native conversion work. The string
        // subset remains included once in declared bytes.
        let Some(work_bytes) = strings.checked_mul(3).and_then(|n| bytes.checked_add(n)) else {
            return self.capacity();
        };
        self.charge(work, calls, bytes, strings, 4, work_bytes)?;
        if self.values.len() == self.values.capacity() {
            let before = self.values.capacity();
            let delta = size_of::<Outcome>();
            let Some(movement) = before.checked_mul(delta).and_then(|n| n.checked_add(delta))
            else {
                return self.capacity();
            };
            self.charge(work, 0, delta, 0, 1, movement)?;
            self.checkpoint(work)?;
            self.entered = Some(phase);
            self.allocation = Some(self.values.try_reserve_exact(1));
            completed(phase, true);
            self.checkpoint(work)?;
            if matches!(self.allocation, Some(Err(_))) {
                return Err(CheckedDirectoryValidationFailure::Allocation);
            }
            let excess = self
                .values
                .capacity()
                .checked_sub(before)
                .and_then(|n| n.checked_sub(1))
                .ok_or(CheckedDirectoryValidationFailure::Invariant)?;
            let Some(extra) = excess.checked_mul(delta) else {
                return self.capacity();
            };
            self.charge(work, 0, extra, 0, excess, extra)?;
        }
        self.checkpoint(work)?;
        self.entered = Some(phase);
        Ok(())
    }
    pub(crate) fn prepare_typed<T, E>(
        &mut self,
        work: &Work<'_>,
        phase: CheckedDirectoryValidationPhase,
        calls: usize,
        strings: usize,
    ) -> Step<()> {
        // The actual returned value and its Result header are independent
        // logical fees; neither the enclosing enum nor Vec capacity replaces them.
        let Some(bytes) = size_of::<T>()
            .checked_add(size_of::<E>())
            .and_then(|n| n.checked_add(size_of::<std::result::Result<T, E>>()))
        else {
            return self.capacity();
        };
        self.prepare(work, phase, calls, bytes, strings)
    }
    pub(crate) fn completed(
        &mut self,
        work: &Work<'_>,
        phase: CheckedDirectoryValidationPhase,
        value: Value,
    ) -> Step<usize> {
        let ordinal = self.values.len();
        self.pending = Some(Outcome {
            phase,
            ordinal,
            value,
        });
        if self.values.len() >= self.values.capacity() {
            return Err(CheckedDirectoryValidationFailure::Invariant);
        }
        if let Some(outcome) = self.pending.take() {
            self.values.push(outcome);
        } else {
            return Err(CheckedDirectoryValidationFailure::Invariant);
        }
        completed(phase, false);
        self.checkpoint(work)?;
        let failed = match &self.values[ordinal].value {
            Value::Metadata(r) => r.is_err(),
            Value::Stat(r) => r.is_err(),
            Value::Open(r) => r.is_err(),
            Value::Name(r) => r.is_err(),
            Value::Identity(r) => r.is_err(),
        };
        if failed {
            return Err(if matches!(&self.values[ordinal].value, Value::Name(_)) {
                CheckedDirectoryValidationFailure::Name
            } else {
                CheckedDirectoryValidationFailure::Ownership
            });
        }
        Ok(ordinal)
    }
    pub(crate) fn optional_metadata_completed(
        &mut self,
        work: &Work<'_>,
        phase: CheckedDirectoryValidationPhase,
        value: io::Result<std::fs::Metadata>,
    ) -> Step<Option<usize>> {
        let missing = matches!(&value, Err(e) if e.kind() == io::ErrorKind::NotFound);
        let result = self.completed(work, phase, Value::Metadata(value));
        if missing && matches!(result, Err(CheckedDirectoryValidationFailure::Ownership)) {
            Ok(None)
        } else {
            result.map(Some)
        }
    }
    pub(crate) fn metadata(&self, index: usize) -> Step<&std::fs::Metadata> {
        match self.values.get(index).map(|v| &v.value) {
            Some(Value::Metadata(Ok(v))) => Ok(v),
            _ => Err(CheckedDirectoryValidationFailure::Invariant),
        }
    }
    pub(crate) fn stat(&self, index: usize) -> Step<&Stat> {
        match self.values.get(index).map(|v| &v.value) {
            Some(Value::Stat(Ok(v))) => Ok(v),
            _ => Err(CheckedDirectoryValidationFailure::Invariant),
        }
    }
    pub(crate) fn fd(&self, index: usize) -> Step<&OwnedFd> {
        match self.values.get(index).map(|v| &v.value) {
            Some(Value::Open(Ok(v))) => Ok(v),
            _ => Err(CheckedDirectoryValidationFailure::Invariant),
        }
    }
    pub(crate) fn name(&self, index: usize) -> Step<&CStr> {
        match self.values.get(index).map(|v| &v.value) {
            Some(Value::Name(Ok(v))) => Ok(v.as_c_str()),
            _ => Err(CheckedDirectoryValidationFailure::Invariant),
        }
    }
    pub(crate) fn identity(&mut self, work: &Work<'_>, result: Result<()>) -> Step<()> {
        // The pure predicate is already paid by its source-visible inputs and
        // its caller prepares this result slot BEFORE evaluating it.
        self.completed(
            work,
            CheckedDirectoryValidationPhase::Identity,
            Value::Identity(result),
        )
        .map(|_| ())
    }
    fn deny(&mut self, work: &Work<'_>) {
        if let Err(p) = catch_unwind(AssertUnwindSafe(|| work.core.deny())) {
            self.core_deny_panic = Some(p);
        }
        if let Err(p) = catch_unwind(AssertUnwindSafe(|| work.native.deny())) {
            self.native_deny_panic = Some(p);
        }
        if !self.outer_deny_attempted {
            self.outer_deny_attempted = true;
            match catch_unwind(AssertUnwindSafe(|| work.checks.deny())) {
                Ok(()) => self.outer_deny_returned = true,
                Err(p) => self.outer_deny_panic = Some(p),
            }
        }
    }
    fn usage(&mut self, work: &Work<'_>) {
        self.core_usage = Some(work.core.usage());
        self.native_usage = Some(work.native.usage());
    }
}
struct Scope {
    effects: Effects,
    // LAST: all exact native results, real walk FDs and panic payloads settle
    // before the whole original handle and its physical owner can drop.
    input: OwnedDirectoryHandle,
}

/// Completed validation retaining its actual input and all native phase results.
///
/// This is an ordinary observation, not a serving or completion witness. No
/// Session, borrowed checks or budget is retained for future operations.
pub struct RetainedCheckedDirectoryValidation {
    scope: Scope,
}
/// Refusal retaining the whole original handle and all actual entered results.
///
/// No handle, descriptor, native owning result or retry can be extracted.
pub struct RetainedCheckedDirectoryValidationError {
    failure: CheckedDirectoryValidationFailure,
    scope: Scope,
}

impl RetainedCheckedDirectoryValidation {
    /// Safe actual native phase observations, with no allocation or FD escape.
    pub fn observations(&self) -> impl Iterator<Item = CheckedDirectoryValidationObservation<'_>> {
        self.scope
            .effects
            .values
            .iter()
            .map(|outcome| CheckedDirectoryValidationObservation { outcome })
    }
    /// Exact retained result-buffer capacity, independently of its length.
    pub fn retained_capacity(&self) -> usize {
        self.scope.effects.values.capacity()
    }
    /// Actual cumulative Core debit retained at completion.
    pub fn core_usage(
        &self,
    ) -> Option<&std::result::Result<NativeDirectoryUsage, NativeDirectoryFailure>> {
        self.scope.effects.core_usage.as_ref()
    }
    /// Actual supplied native-counter debit, without creating or resetting it.
    pub fn native_usage(
        &self,
    ) -> Option<&std::result::Result<StorageFetchUsage, StorageFetchFailure>> {
        self.scope.effects.native_usage.as_ref()
    }
    /// Original supplied native E and Core minimum sampled after its cap.
    pub fn entry_deadlines(&self) -> Option<(Instant, Instant)> {
        self.scope
            .effects
            .original_native_expiry
            .zip(self.scope.effects.core_minimum)
    }
    /// Return only the WHOLE original handle after successful validation.
    pub fn into_handle(self) -> OwnedDirectoryHandle {
        let Scope { input, .. } = self.scope;
        input
    }
}
impl RetainedCheckedDirectoryValidationError {
    /// Terminal category without erasing its original typed cause.
    pub fn failure(&self) -> CheckedDirectoryValidationFailure {
        self.failure
    }
    /// Safe actual native phase observations, with no owner/FD extraction.
    pub fn observations(&self) -> impl Iterator<Item = CheckedDirectoryValidationObservation<'_>> {
        self.scope
            .effects
            .values
            .iter()
            .map(|outcome| CheckedDirectoryValidationObservation { outcome })
    }
    /// Exact retained result-buffer capacity, including a refused excess.
    pub fn retained_capacity(&self) -> usize {
        self.scope.effects.values.capacity()
    }
    /// Actual cumulative Core debit retained at completion.
    pub fn core_usage(
        &self,
    ) -> Option<&std::result::Result<NativeDirectoryUsage, NativeDirectoryFailure>> {
        self.scope.effects.core_usage.as_ref()
    }
    /// Actual supplied native-counter debit, without creating or resetting it.
    pub fn native_usage(
        &self,
    ) -> Option<&std::result::Result<StorageFetchUsage, StorageFetchFailure>> {
        self.scope.effects.native_usage.as_ref()
    }
    /// Original supplied native E and Core minimum sampled after its cap.
    pub fn entry_deadlines(&self) -> Option<(Instant, Instant)> {
        self.scope
            .effects
            .original_native_expiry
            .zip(self.scope.effects.core_minimum)
    }
    /// Actual entered phase, which may not have produced a result before panic.
    pub fn entered_phase(&self) -> Option<CheckedDirectoryValidationPhase> {
        self.scope.effects.entered
    }
    /// Actual first refusal from the SAME Core counters or cap.
    pub fn core_error(&self) -> Option<&NativeDirectoryFailure> {
        let e = &self.scope.effects;
        e.cap
            .as_ref()
            .and_then(|r| r.as_ref().err())
            .or_else(|| e.fee.as_ref().and_then(|r| r.as_ref().err()))
            .or_else(|| e.rule.as_ref().and_then(|r| r.as_ref().err()))
            .or_else(|| {
                e.checks
                    .core
                    .iter()
                    .filter_map(|r| r.as_ref().and_then(|r| r.as_ref().err()))
                    .next()
            })
    }
    /// Actual first refusal from the SAME native fetch counters.
    pub fn native_error(&self) -> Option<&StorageFetchFailure> {
        self.scope
            .effects
            .checks
            .native
            .iter()
            .filter_map(|r| r.as_ref().and_then(|r| r.as_ref().err()))
            .next()
    }
    /// Actual composing checkpoint result when it refused.
    pub fn checkpoint_error(&self) -> Option<ShardDirectoryCheckFailure> {
        self.scope
            .effects
            .checks
            .outer
            .as_ref()
            .and_then(|r| r.as_ref().err())
            .copied()
    }
    /// Whether actual operation or independent denial callbacks unwound.
    pub fn panic_observations(&self) -> [bool; 4] {
        let e = &self.scope.effects;
        [
            e.operation_panic.is_some(),
            e.core_deny_panic.is_some(),
            e.native_deny_panic.is_some(),
            e.outer_deny_panic.is_some(),
        ]
    }
    /// Whether the actual composing denial callback was attempted and returned.
    pub fn composing_denial(&self) -> (bool, bool) {
        (
            self.scope.effects.outer_deny_attempted,
            self.scope.effects.outer_deny_returned,
        )
    }
}
impl fmt::Debug for RetainedCheckedDirectoryValidation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedCheckedDirectoryValidation")
            .field("results", &self.scope.effects.values.len())
            .field("capacity", &self.retained_capacity())
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for RetainedCheckedDirectoryValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedCheckedDirectoryValidationError")
            .field("failure", &self.failure)
            .field("results", &self.scope.effects.values.len())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedCheckedDirectoryValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "retained checked directory validation refused: {:?}",
            self.failure
        )
    }
}
impl Error for RetainedCheckedDirectoryValidationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(e) = self.core_error() {
            return Some(e);
        }
        if let Some(e) = self.native_error() {
            return Some(e);
        }
        if let Some(Err(e)) = &self.scope.effects.allocation {
            return Some(e);
        }
        for r in &self.scope.effects.values {
            match &r.value {
                Value::Metadata(Err(e)) => return Some(e),
                Value::Stat(Err(e)) | Value::Open(Err(e)) => return Some(e),
                Value::Name(Err(e)) => return Some(e),
                Value::Identity(Err(e)) => return Some(e),
                _ => {}
            }
        }
        None
    }
}

fn fixed_fee() -> Option<(usize, usize)> {
    // Each independent result, panic payload header and bookkeeping value has
    // its own slot even where the enclosing carrier also includes that field.
    // These are source-visible logical fees, not allocator/RSS measurements.
    let values = [
        size_of::<Scope>(),
        size_of::<Effects>(),
        size_of::<Checks>(),
        size_of::<Work<'static>>(),
        size_of::<RetainedCheckedDirectoryValidation>(),
        size_of::<RetainedCheckedDirectoryValidationError>(),
        size_of::<CheckedDirectoryValidationCost>(),
        size_of::<CheckedDirectoryValidationFailure>(),
        size_of::<OwnedDirectoryHandle>(),
        size_of::<Vec<Outcome>>(),
        size_of::<Option<Outcome>>(),
        size_of::<Outcome>(),
        size_of::<Value>(),
        size_of::<Option<std::result::Result<(), TryReserveError>>>(),
        size_of::<std::result::Result<(), TryReserveError>>(),
        size_of::<TryReserveError>(),
        size_of::<Option<std::result::Result<(), NativeDirectoryFailure>>>(), // minimum cap
        size_of::<Option<std::result::Result<(), NativeDirectoryFailure>>>(), // cumulative fee
        size_of::<Option<std::result::Result<(), NativeDirectoryFailure>>>(), // original pure rule
        size_of::<Option<std::result::Result<(), NativeDirectoryFailure>>>(), // Core before callback
        size_of::<Option<std::result::Result<(), NativeDirectoryFailure>>>(), // Core after callback
        size_of::<Option<std::result::Result<(), StorageFetchFailure>>>(), // native before callback
        size_of::<Option<std::result::Result<(), StorageFetchFailure>>>(), // native after callback
        size_of::<Option<std::result::Result<(), ShardDirectoryCheckFailure>>>(),
        size_of::<Option<Instant>>(),
        size_of::<Option<Instant>>(),
        size_of::<Option<CheckedDirectoryValidationPhase>>(),
        size_of::<Option<std::result::Result<NativeDirectoryUsage, NativeDirectoryFailure>>>(),
        size_of::<Option<std::result::Result<StorageFetchUsage, StorageFetchFailure>>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(), // operation
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(), // Core denial
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(), // native denial
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(), // composing denial
        size_of::<std::result::Result<Step<()>, Box<dyn Any + Send>>>(),
        size_of::<std::result::Result<(), Box<dyn Any + Send>>>(), // Core denial catch
        size_of::<std::result::Result<(), Box<dyn Any + Send>>>(), // native denial catch
        size_of::<std::result::Result<(), Box<dyn Any + Send>>>(), // composing denial catch
        size_of::<Step<()>>(),
        size_of::<
            std::result::Result<
                RetainedCheckedDirectoryValidation,
                RetainedCheckedDirectoryValidationError,
            >,
        >(),
        size_of::<bool>(),
        size_of::<bool>(),       // denial attempted/returned
        size_of::<[usize; 6]>(), // ordinal, capacity, delta, excess, bytes, entries
        size_of::<[usize; 2]>(), // checked fixed-byte/entry sum bookkeeping
    ];
    Some((
        values.into_iter().try_fold(0usize, usize::checked_add)?,
        values.len(),
    ))
}
/// Derive plain configured Core and fixed retention costs without I/O.
///
/// This does not construct or reserve either budget. Native fetch setup is paid
/// once separately. Incremental actual result/FD/string capacity consumes the
/// supplied Core policy and cannot be reset between recursive phases or calls.
pub fn checked_directory_validation_cost(
    limits: NativeDirectoryLimits,
) -> std::result::Result<CheckedDirectoryValidationCost, NativeDirectoryFailure> {
    let core = native_directory_cost(limits)?;
    let (bytes, entries) = fixed_fee().ok_or(NativeDirectoryFailure::Capacity)?;
    Ok(CheckedDirectoryValidationCost {
        declared_bytes: core
            .declared_bytes
            .checked_add(bytes)
            .ok_or(NativeDirectoryFailure::Capacity)?,
        string_bytes: core.string_bytes,
        entries: core
            .entries
            .checked_add(entries)
            .ok_or(NativeDirectoryFailure::Capacity)?,
        work_bytes: core
            .work_bytes
            .checked_add(bytes)
            .and_then(|n| n.checked_add(entries))
            .ok_or(NativeDirectoryFailure::Capacity)?,
    })
}

fn validate_handle(
    handle: &OwnedDirectoryHandle,
    depth: usize,
    effects: &mut Effects,
    work: &Work<'_>,
) -> Step<()> {
    effects.checkpoint(work)?;
    effects.rule = Some(super::operation_until::checked_depth_path(
        handle, work.core, depth,
    ));
    if matches!(effects.rule, Some(Err(_))) {
        return Err(CheckedDirectoryValidationFailure::Core);
    }
    let frame = size_of::<(&OwnedDirectoryHandle, usize, Option<&OwnedDirectoryHandle>)>();
    effects.charge(work, 0, frame, 0, 3, frame)?;
    if let Some((parent, name)) = &handle.inner.parent {
        let Some(next_depth) = depth.checked_add(1) else {
            return effects.capacity();
        };
        validate_handle(parent, next_depth, effects, work)?;
        if name.as_bytes().len() > work.core.limits().max_component_bytes {
            return effects.capacity();
        }
        effects.scan(work, name.as_bytes().len(), 4)?;
        effects.prepare_typed::<(), DirectoryOwnershipError>(
            work,
            CheckedDirectoryValidationPhase::Identity,
            0,
            0,
        )?;
        effects.identity(work, component(name))?;
        let Some(n) = name.as_bytes().len().checked_add(1) else {
            return effects.capacity();
        };
        effects.prepare_typed::<CString, NulError>(
            work,
            CheckedDirectoryValidationPhase::ComponentName,
            0,
            n,
        )?;
        let name_index = effects.completed(
            work,
            CheckedDirectoryValidationPhase::ComponentName,
            Value::Name(CString::new(name.as_bytes())),
        )?;
        effects.prepare_typed::<std::fs::Metadata, io::Error>(
            work,
            CheckedDirectoryValidationPhase::ChildMetadata,
            1,
            0,
        )?;
        let actual = effects.completed(
            work,
            CheckedDirectoryValidationPhase::ChildMetadata,
            Value::Metadata(handle.inner.file.metadata()),
        )?;
        effects.prepare_typed::<Stat, rustix::io::Errno>(
            work,
            CheckedDirectoryValidationPhase::ChildDescriptorStat,
            1,
            0,
        )?;
        let opened = effects.completed(
            work,
            CheckedDirectoryValidationPhase::ChildDescriptorStat,
            Value::Stat(fstat(&handle.inner.file)),
        )?;
        effects.prepare_typed::<Stat, rustix::io::Errno>(
            work,
            CheckedDirectoryValidationPhase::NamedChildStat,
            1,
            0,
        )?;
        let raw = statat(
            &parent.inner.file,
            effects.name(name_index)?,
            AtFlags::SYMLINK_NOFOLLOW,
        );
        let named = effects.completed(
            work,
            CheckedDirectoryValidationPhase::NamedChildStat,
            Value::Stat(raw),
        )?;
        effects.prepare_typed::<(), DirectoryOwnershipError>(
            work,
            CheckedDirectoryValidationPhase::Identity,
            0,
            0,
        )?;
        effects.identity(
            work,
            validate_entry_identity(
                effects.metadata(actual)?,
                effects.stat(opened)?,
                effects.stat(named)?,
                true,
            ),
        )?;
    } else {
        handle
            .inner
            .owner
            .validate_retained_checked(&handle.inner.path, effects, work)?;
        let Some(path_buffer) = handle
            .inner
            .path
            .as_os_str()
            .as_bytes()
            .len()
            .checked_add(1)
        else {
            return effects.capacity();
        };
        effects.prepare_typed::<std::fs::Metadata, io::Error>(
            work,
            CheckedDirectoryValidationPhase::HandlePathMetadata,
            1,
            path_buffer,
        )?;
        let named = effects.completed(
            work,
            CheckedDirectoryValidationPhase::HandlePathMetadata,
            Value::Metadata(std::fs::symlink_metadata(&handle.inner.path)),
        )?;
        effects.prepare_typed::<std::fs::Metadata, io::Error>(
            work,
            CheckedDirectoryValidationPhase::HandleDescriptorMetadata,
            1,
            0,
        )?;
        let actual = effects.completed(
            work,
            CheckedDirectoryValidationPhase::HandleDescriptorMetadata,
            Value::Metadata(handle.inner.file.metadata()),
        )?;
        effects.prepare_typed::<(), DirectoryOwnershipError>(
            work,
            CheckedDirectoryValidationPhase::Identity,
            0,
            0,
        )?;
        let n = effects.metadata(named)?;
        let a = effects.metadata(actual)?;
        effects.identity(work, validate_root_handle_identity(n, a))?;
    }
    effects.checkpoint(work)
}

#[allow(
    clippy::result_large_err,
    reason = "early refusal retains the actual whole input without an unpaid heap allocation"
)]
impl OwnedDirectoryHandle {
    /// Consume this actual handle for retained validation of both real budgets.
    ///
    /// Both counters and mandatory composing checks run before every entered
    /// allocation, conversion and native phase. The Core minimum is first
    /// capped to the supplied native original E, without replacing either
    /// counter. An admitted synchronous call can finish late; its actual Result
    /// and all transient walk FDs remain in the outside-catch carrier first.
    /// All refusals deny both budgets and attempt composing denial before any
    /// physical input or raw result drops. No budget/check/Session is retained
    /// for future operations, no FD or claimed-path factory is exposed, and
    /// successful validation supplies no serving or currentness authority.
    ///
    /// Callers prepay the plain setup ceiling and each actual input handle
    /// retention BEFORE cloning it. This consumes that WHOLE input on every
    /// refusal. Later operations need their own actual admitted work.
    pub fn validate_for_storage_fetch_until(
        self,
        core: &NativeDirectoryBudget,
        native: &StorageFetchBudget,
        checks: &dyn CheckedDirectoryValidationChecks,
    ) -> std::result::Result<
        RetainedCheckedDirectoryValidation,
        RetainedCheckedDirectoryValidationError,
    > {
        let mut scope = Scope {
            effects: Effects::new(),
            input: self,
        };
        let work = Work {
            core,
            native,
            checks,
        };
        let observed = catch_unwind(AssertUnwindSafe(|| -> Step<()> {
            scope.effects.original_native_expiry = Some(native.absolute_expiry());
            scope.effects.cap = Some(core.cap_expiry(native.absolute_expiry()));
            scope.effects.core_minimum = Some(core.absolute_expiry());
            if matches!(scope.effects.cap, Some(Err(_))) {
                return Err(CheckedDirectoryValidationFailure::Core);
            }
            scope.effects.checkpoint(&work)?;
            let Some((bytes, entries)) = fixed_fee() else {
                return scope.effects.capacity();
            };
            scope.effects.fee = Some(core.reserve(1, 0, bytes, 0, entries, bytes));
            if matches!(scope.effects.fee, Some(Err(_))) {
                return Err(CheckedDirectoryValidationFailure::Core);
            }
            validate_handle(&scope.input, 0, &mut scope.effects, &work)?;
            scope.effects.usage(&work);
            scope.effects.checkpoint(&work)
        }));
        let result = match observed {
            Ok(r) => r,
            Err(p) => {
                scope.effects.operation_panic = Some(p);
                Err(CheckedDirectoryValidationFailure::Unwind)
            }
        };
        match result {
            Ok(()) => Ok(RetainedCheckedDirectoryValidation { scope }),
            Err(failure) => {
                scope.effects.deny(&work);
                scope.effects.usage(&work);
                Err(RetainedCheckedDirectoryValidationError { failure, scope })
            }
        }
    }
}

fn completed(phase: CheckedDirectoryValidationPhase, allocation: bool) {
    #[cfg(test)]
    tests::completed(phase, allocation);
    #[cfg(not(test))]
    let _ = (phase, allocation);
}
#[cfg(test)]
#[path = "owned_directory_validation_checked_tests.rs"]
mod tests;
