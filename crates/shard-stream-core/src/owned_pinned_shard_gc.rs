//! Consume the actual Core store through native GC and all retained outcomes.

use super::{OwnedPinnedShardStore, OwnedShardStoreDirectory};
use crate::{
    DirectoryOwnership, LogicalOffset, NativeDirectoryBudget, NativeDirectoryFailure,
    OwnedDirectoryHandle, OwnedShardDirectory, RetainedDirectoryError, TopicPartition,
};
use shardlog::{
    PinnedShardGcChecks, PinnedShardGcError, PinnedShardGcLimits, PinnedShardGcReport,
    ShardDirectoryCheckFailure, ShardDirectoryChecks, pinned_shard_gc_cost,
};
use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, MutexGuard, TryLockError, TryLockResult};

type PinBorrow<'a> = Option<TryLockResult<MutexGuard<'a, OwnedShardDirectory>>>;

/// Plain logical native GC and fixed consuming-Core carrier ceilings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnedPinnedShardGcCost {
    /// Native requested allocations and fixed Core value/carrier fees.
    pub declared_bytes: usize,
    /// Native path/name subset already included in declared bytes.
    pub string_bytes: usize,
    /// Fixed Core and configured native allocation/value/outcome slots.
    pub entries: usize,
    /// Native configured work plus fixed Core work, excluding Core validation.
    pub work_bytes: usize,
    /// Native identity/collection calls; Core validation pays its own calls.
    pub native_call_slots: usize,
}

/// Actual consuming Core GC refusal; the whole actual store remains retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnedPinnedShardGcFailure {
    /// SAME cumulative counters or original/minimum E refused.
    Budget,
    /// Actual recursive Core parent/root validation refused.
    Directory,
    /// The composing operation's actual checkpoint refused.
    Checkpoint,
    /// Immediate borrowing of the SAME private pin refused or was poisoned.
    PinBorrow,
    /// The actual native collector refused; its report remains retained.
    Native,
    /// An actual bridge/checkpoint unwound; its original payload remains held.
    Unwind,
    /// An internal carrier transition refused without rebuilding any input.
    Invariant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BorrowFailure {
    WouldBlock,
    Poisoned,
}

struct Effects {
    native: Option<Result<PinnedShardGcReport, PinnedShardGcError>>,
    directory_error: Option<RetainedDirectoryError>,
    budget_error: Option<NativeDirectoryFailure>,
    checkpoint_error: Option<ShardDirectoryCheckFailure>,
    panic_payload: Option<Box<dyn Any + Send>>,
    deny_panic_payload: Option<Box<dyn Any + Send>>,
    denial_completed: bool,
    pin_borrowed: bool,
    borrow_failure: Option<BorrowFailure>,
}
impl Effects {
    fn new() -> Self {
        Self {
            native: None,
            directory_error: None,
            budget_error: None,
            checkpoint_error: None,
            panic_payload: None,
            deny_panic_payload: None,
            denial_completed: false,
            pin_borrowed: false,
            borrow_failure: None,
        }
    }
    fn check(&self, checks: &Checks<'_>) -> Result<(), OwnedPinnedShardGcFailure> {
        checks.check().map_err(|_| {
            if checks.budget_error.borrow().is_some() {
                OwnedPinnedShardGcFailure::Budget
            } else {
                OwnedPinnedShardGcFailure::Checkpoint
            }
        })
    }
    fn validate(
        &mut self,
        parent: &OwnedDirectoryHandle,
        budget: &NativeDirectoryBudget,
    ) -> Result<(), OwnedPinnedShardGcFailure> {
        match parent.validate_until(budget) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.directory_error = Some(error);
                Err(OwnedPinnedShardGcFailure::Directory)
            }
        }
    }
}

struct Checks<'a> {
    budget: &'a NativeDirectoryBudget,
    outer: &'a dyn PinnedShardGcChecks,
    budget_error: RefCell<Option<NativeDirectoryFailure>>,
    checkpoint_error: Cell<Option<ShardDirectoryCheckFailure>>,
    denial_attempted: Cell<bool>,
    denial_completed: Cell<bool>,
}
impl Checks<'_> {
    fn check_budget(&self) -> Result<(), ShardDirectoryCheckFailure> {
        match self.budget.check() {
            Ok(()) => Ok(()),
            Err(error) => {
                let mut retained = self.budget_error.borrow_mut();
                if retained.is_none() {
                    *retained = Some(error);
                }
                Err(ShardDirectoryCheckFailure::Refused)
            }
        }
    }
}
impl ShardDirectoryChecks for Checks<'_> {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        self.check_budget()?;
        let observed = self.outer.check();
        if let Err(error) = observed
            && self.checkpoint_error.get().is_none()
        {
            self.checkpoint_error.set(Some(error));
        }
        self.check_budget()?;
        observed
    }
}
impl PinnedShardGcChecks for Checks<'_> {
    fn deny(&self) {
        self.budget.deny();
        if !self.denial_attempted.replace(true) {
            self.outer.deny();
            self.denial_completed.set(true);
        }
    }
}

/// Actual completed native collection, with the whole Core store retained last.
pub struct OwnedPinnedShardGcComplete {
    report: PinnedShardGcReport,
    store: OwnedPinnedShardStore,
}
/// Actual native report retaining its SAME physical directory and Core parent.
///
/// A worker can restore its whole store while returning this actual report.
/// Every original native outcome drops before the shared pin and parent; no
/// native report, descriptor or permission can be extracted from this carrier.
pub struct OwnedPinnedShardGcReport {
    report: PinnedShardGcReport,
    directory: OwnedShardStoreDirectory,
}
impl OwnedPinnedShardGcReport {
    /// Borrow the original native report, without copying its typed outcomes.
    pub fn report(&self) -> &PinnedShardGcReport {
        &self.report
    }
    /// SAME original Core root allocation retained through this report's drop.
    pub fn owner(&self) -> &Arc<DirectoryOwnership> {
        self.directory.owner()
    }
}
impl fmt::Debug for OwnedPinnedShardGcReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedPinnedShardGcReport")
            .field("report", &self.report)
            .field("directory", &self.directory)
            .finish_non_exhaustive()
    }
}
impl OwnedPinnedShardGcComplete {
    /// Actual native collection/index/physical outcomes, without copying them.
    pub fn report(&self) -> &PinnedShardGcReport {
        &self.report
    }
    /// Borrow the whole genuine Core owner after physical collection settled.
    pub fn store(&self) -> &OwnedPinnedShardStore {
        &self.store
    }
    /// Return the whole Core store owner, dropping the native report first.
    ///
    /// This returns no bare store, pin, descriptor or serving permission.
    pub fn into_store(self) -> OwnedPinnedShardStore {
        let Self { report, store } = self;
        drop(report);
        store
    }
    /// Return the whole store and its actual ownership-retaining native report.
    ///
    /// This performs one Arc increment prepaid by the original GC cost. It
    /// creates no bare store/pin/report extractor or new physical operation.
    /// Dropping either returned value first still keeps the original parent
    /// until all native outcomes belonging to the other value are destroyed.
    pub fn into_store_and_report(self) -> (OwnedPinnedShardStore, OwnedPinnedShardGcReport) {
        let Self { report, store } = self;
        let directory = store.retain_directory();
        (store, OwnedPinnedShardGcReport { report, directory })
    }
}
impl fmt::Debug for OwnedPinnedShardGcComplete {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedPinnedShardGcComplete")
            .field("report", &self.report)
            .finish_non_exhaustive()
    }
}

/// Actual refusal retaining all native outcomes and the SAME whole Core store.
///
/// There is no extraction/retry constructor. The store, partial indexes, native
/// directory, acquired lease and original Core parent remain through cleanup.
pub struct RetainedOwnedPinnedShardGcError {
    failure: OwnedPinnedShardGcFailure,
    effects: Effects,
    // Last: native outcomes/errors/panics settle before actual store/root custody.
    store: OwnedPinnedShardStore,
}
impl RetainedOwnedPinnedShardGcError {
    /// Actual terminal Core category.
    pub fn failure(&self) -> OwnedPinnedShardGcFailure {
        self.failure
    }
    /// Actual first SAME-budget refusal, including a tighter original E.
    pub fn budget_error(&self) -> Option<&NativeDirectoryFailure> {
        self.effects.budget_error.as_ref()
    }
    /// Actual recursive Core parent/root refusal and retained outcomes.
    pub fn directory_error(&self) -> Option<&RetainedDirectoryError> {
        self.effects.directory_error.as_ref()
    }
    /// Actual first returned composing checkpoint refusal.
    pub fn checkpoint_error(&self) -> Option<ShardDirectoryCheckFailure> {
        self.effects.checkpoint_error
    }
    /// Actual native collector refusal and its full partial-effect report.
    pub fn native_error(&self) -> Option<&PinnedShardGcError> {
        self.effects.native.as_ref()?.as_ref().err()
    }
    /// Actual native report on either native success or native refusal.
    pub fn native_report(&self) -> Option<&PinnedShardGcReport> {
        match self.effects.native.as_ref()? {
            Ok(report) => Some(report),
            Err(error) => Some(error.report()),
        }
    }
    /// Actual bridge panic; native panics remain in the native report.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.effects.panic_payload.as_deref()
    }
    /// Actual bridge denial panic; native denial panics remain in its report.
    pub fn deny_panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.effects.deny_panic_payload.as_deref()
    }
    /// Whether mandatory composing denial actually returned normally.
    pub fn denial_completed(&self) -> bool {
        self.effects.denial_completed
    }
    /// Whether the actual private pin borrow succeeded before post-call checks.
    pub fn pin_borrowed(&self) -> bool {
        self.effects.pin_borrowed
    }
    /// The actual immediate pin borrow returned WouldBlock.
    pub fn pin_would_block(&self) -> bool {
        self.effects.borrow_failure == Some(BorrowFailure::WouldBlock)
    }
    /// The actual private pin mutex returned its poisoned result.
    pub fn pin_poisoned(&self) -> bool {
        self.effects.borrow_failure == Some(BorrowFailure::Poisoned)
    }
    /// SAME original Core root retained with the actual store and native pin.
    pub fn owner(&self) -> &Arc<DirectoryOwnership> {
        self.store.owner()
    }
}
impl fmt::Debug for RetainedOwnedPinnedShardGcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedOwnedPinnedShardGcError")
            .field("failure", &self.failure)
            .field("native_report", &self.native_report())
            .field("pin_borrowed", &self.pin_borrowed())
            .field("denial_completed", &self.denial_completed())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedOwnedPinnedShardGcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "owned pinned GC refused: {:?}", self.failure)
    }
}
impl Error for RetainedOwnedPinnedShardGcError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(error) = self.native_error() {
            return Some(error);
        }
        if let Some(error) = self.directory_error() {
            return Some(error);
        }
        self.budget_error().map(|error| error as &dyn Error)
    }
}

/// Derive logical fixed-Core and genuine native GC ceilings before entry.
///
/// Existing store/pin/floor allocations are retained actual inputs, not copied.
/// Recursive Core validation charges its own phases on the SAME budget. This
/// includes the actual typed pin-borrow slot, independent bridge panic slots
/// and one optional report transfer retaining the same physical directory;
/// allocator/library/kernel/RSS and performance remain separate measured gates.
pub fn owned_pinned_shard_gc_cost(limits: PinnedShardGcLimits) -> Option<OwnedPinnedShardGcCost> {
    let native = pinned_shard_gc_cost(limits)?;
    let values = [
        size_of::<Effects>(),
        size_of::<OwnedPinnedShardGcComplete>(),
        size_of::<OwnedPinnedShardGcReport>(),
        size_of::<OwnedShardStoreDirectory>(),
        size_of::<RetainedOwnedPinnedShardGcError>(),
        size_of::<OwnedPinnedShardGcCost>(),
        size_of::<Checks<'static>>(),
        size_of::<PinBorrow<'static>>(),
        size_of::<Result<PinnedShardGcReport, PinnedShardGcError>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
    ];
    let fixed = values.into_iter().try_fold(0usize, usize::checked_add)?;
    Some(OwnedPinnedShardGcCost {
        declared_bytes: native.declared_bytes.checked_add(fixed)?,
        string_bytes: native.string_bytes,
        entries: native.entries.checked_add(values.len())?,
        work_bytes: native
            .work_bytes
            .checked_add(fixed)?
            .checked_add(values.len())?
            .checked_add(1)?, // One optional SAME-directory Arc increment.
        native_call_slots: native.native_call_slots,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    ParentValidated,
    PinBorrowed,
    NativeCompleted,
    FinalParentValidated,
}
fn completed(phase: Phase) {
    #[cfg(test)]
    tests::completed(phase);
    #[cfg(not(test))]
    let _ = phase;
}

#[allow(
    clippy::result_large_err,
    reason = "retain the actual whole Core store without allocating on pre-entry or late refusal"
)]
impl OwnedPinnedShardStore {
    /// Consume the actual store through bounded GC using its SAME private pin.
    ///
    /// Success returns a completed native report and this whole Core owner.
    /// Refusal retains the actual store, partial native/index outcomes, distinct
    /// panic payloads and original Core parent. There is no borrowed-store-plus-
    /// claimed-parent pairing. The pin borrow result remains outside catch and
    /// stays held through denial and native settlement before it is released.
    /// `checks.deny()` must latch the SAME actual work and captured health; no
    /// callback, Session or startup budget is retained in either output.
    ///
    /// # Errors
    /// Every refusal retains this whole actual owner, denies SAME counters and
    /// attempts mandatory composing denial before releasing the pin borrow.
    pub fn garbage_collect_until(
        self,
        log_starts: &HashMap<TopicPartition, LogicalOffset>,
        limits: PinnedShardGcLimits,
        budget: &NativeDirectoryBudget,
        checks: &dyn PinnedShardGcChecks,
    ) -> Result<OwnedPinnedShardGcComplete, RetainedOwnedPinnedShardGcError> {
        // Outside every catch: the actual store is never a closure-local value.
        let mut store = self;
        let mut effects = Effects::new();
        let admitted = (|| {
            budget.check()?;
            if limits.max_path_bytes > budget.limits().max_path_bytes {
                return Err(NativeDirectoryFailure::Capacity);
            }
            let cost =
                owned_pinned_shard_gc_cost(limits).ok_or(NativeDirectoryFailure::Capacity)?;
            budget.charge(
                1,
                cost.native_call_slots,
                cost.declared_bytes,
                cost.string_bytes,
                cost.entries,
                cost.work_bytes,
            )?;
            budget.check()
        })();
        if let Err(error) = admitted {
            effects.budget_error = Some(error);
            budget.deny();
            let denied = catch_unwind(AssertUnwindSafe(|| checks.deny()));
            effects.denial_completed = denied.is_ok();
            effects.deny_panic_payload = denied.err();
            return Err(RetainedOwnedPinnedShardGcError {
                failure: OwnedPinnedShardGcFailure::Budget,
                effects,
                store,
            });
        }
        let checks = Checks {
            budget,
            outer: checks,
            budget_error: RefCell::new(None),
            checkpoint_error: Cell::new(None),
            denial_attempted: Cell::new(false),
            denial_completed: Cell::new(false),
        };
        let observed = {
            let shared = &store.directory.inner;
            // The actual typed Result/guard survives an entered callback unwind.
            let mut borrowed: PinBorrow<'_> = None;
            let observed = catch_unwind(AssertUnwindSafe(|| {
                effects.check(&checks)?;
                effects.validate(&shared.parent, budget)?;
                completed(Phase::ParentValidated);
                effects.check(&checks)?;
                borrowed = Some(shared.native.try_lock());
                match borrowed.as_ref() {
                    Some(Ok(_)) => effects.pin_borrowed = true,
                    Some(Err(TryLockError::WouldBlock)) => {
                        effects.borrow_failure = Some(BorrowFailure::WouldBlock)
                    }
                    Some(Err(TryLockError::Poisoned(_))) => {
                        effects.borrow_failure = Some(BorrowFailure::Poisoned)
                    }
                    None => return Err(OwnedPinnedShardGcFailure::Invariant),
                }
                completed(Phase::PinBorrowed);
                effects.check(&checks)?;
                let pin = match borrowed.as_ref() {
                    Some(Ok(pin)) => pin,
                    Some(Err(_)) => return Err(OwnedPinnedShardGcFailure::PinBorrow),
                    None => return Err(OwnedPinnedShardGcFailure::Invariant),
                };
                effects.native = Some(store.opened.store_mut().garbage_collect_pinned_until(
                    &pin.native,
                    log_starts,
                    limits,
                    budget.absolute_expiry(),
                    &checks,
                ));
                completed(Phase::NativeCompleted);
                let checkpoint = effects.check(&checks);
                if matches!(effects.native.as_ref(), Some(Err(_))) {
                    return Err(OwnedPinnedShardGcFailure::Native);
                }
                checkpoint?;
                if !matches!(effects.native.as_ref(), Some(Ok(_))) {
                    return Err(OwnedPinnedShardGcFailure::Invariant);
                }
                effects.validate(&shared.parent, budget)?;
                completed(Phase::FinalParentValidated);
                effects.check(&checks)
            }));
            let observed = match observed {
                Ok(result) => result,
                Err(payload) => {
                    effects.panic_payload = Some(payload);
                    Err(OwnedPinnedShardGcFailure::Unwind)
                }
            };
            let observed = observed.and({
                if matches!(effects.native.as_ref(), Some(Ok(_))) {
                    Ok(())
                } else {
                    Err(OwnedPinnedShardGcFailure::Invariant)
                }
            });
            if observed.is_err()
                && let Err(payload) = catch_unwind(AssertUnwindSafe(|| checks.deny()))
            {
                effects.deny_panic_payload = Some(payload);
            }
            // SAME denial and already-prepaid native settlement precede unlock.
            // All native results and the whole owner remain outside this frame.
            drop(borrowed);
            observed
        };
        let failure = match observed {
            Ok(()) => match effects.native.take() {
                Some(Ok(report)) => return Ok(OwnedPinnedShardGcComplete { report, store }),
                native => {
                    effects.native = native;
                    OwnedPinnedShardGcFailure::Invariant
                }
            },
            Err(failure) => failure,
        };
        // The final invariant arm cannot be produced by the successful path,
        // but still denies before any actual input/outcome is destroyed.
        if !checks.denial_attempted.get() {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| checks.deny())) {
                effects.deny_panic_payload = Some(payload);
            }
            effects.denial_completed = checks.denial_completed.get();
        }
        effects.budget_error = checks.budget_error.into_inner();
        effects.checkpoint_error = checks.checkpoint_error.get();
        effects.denial_completed = checks.denial_completed.get();
        Err(RetainedOwnedPinnedShardGcError {
            failure,
            effects,
            store,
        })
    }
}

#[cfg(test)]
#[path = "owned_pinned_shard_gc_tests.rs"]
mod tests;
