//! Consume the actual Core/native directory into its actual opened store.
//!
//! Opaque directory clones retain the SAME pin, acquired lease and Core parent
//! for physical jobs. They expose no native descriptor or authority constructor.

use super::{OwnedDirectoryHandle, OwnedShardDirectory};
use crate::{
    DirectoryOwnership, NativeDirectoryBudget, NativeDirectoryFailure, RetainedDirectoryError,
};
use shardlog::{
    OpenedPinnedShardStore, PinnedStoreOpenChecks, PinnedStoreOpenError, PinnedStoreOpenLimits,
    PinnedStoreOpenOutcome, PinnedStoreOpenUsage, ShardDirectoryCheckFailure, ShardDirectoryChecks,
    ShardStore, ShardStoreConfig, pinned_store_open_cost,
};
use std::any::Any;
use std::cell::{Cell, RefCell};
use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[path = "owned_pinned_shard_append.rs"]
mod append;
pub use append::{
    OwnedPinnedShardAppendComplete, OwnedPinnedShardAppendCost, OwnedPinnedShardAppendFailure,
    OwnedPinnedShardAppendLimits, OwnedPinnedShardAppendObservation, OwnedPinnedShardAppendReport,
    RetainedOwnedPinnedShardAppendError, owned_pinned_shard_append_cost,
};

#[path = "owned_pinned_shard_gc.rs"]
mod gc;
pub use gc::{
    OwnedPinnedShardGcComplete, OwnedPinnedShardGcCost, OwnedPinnedShardGcFailure,
    OwnedPinnedShardGcReport, RetainedOwnedPinnedShardGcError, owned_pinned_shard_gc_cost,
};

/// Plain logical bridge and native open ceilings; no admission or RSS claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnedPinnedShardStoreCost {
    /// Native setup/dynamic ceilings, carrier values and the actual Arc request.
    pub declared_bytes: usize,
    /// Input/path/name subset already included in declared bytes.
    pub string_bytes: usize,
    /// Bridge/native values, outcomes and configured allocation slots.
    pub entries: usize,
    /// Native configured work and fixed bridge work, excluding Core validation.
    pub work_bytes: usize,
    /// Native open/repair call ceiling; Core validation charges its own calls.
    pub native_call_slots: usize,
}

/// Actual consuming bridge refusal, with richer failures kept in custody.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnedPinnedShardStoreFailure {
    /// SAME cumulative Core counters or original/minimum E refused.
    Budget,
    /// Actual recursive Core parent/root validation refused.
    Directory,
    /// The actual composing operation's checkpoint refused.
    Checkpoint,
    /// The genuine native open/repair producer refused.
    Native,
    /// A bridge producer/checkpoint unwound; its actual payload is retained.
    Unwind,
    /// An internal carrier transition refused without reconstructing inputs.
    Invariant,
}

struct SharedDirectory {
    // Actual observations can own Send-only panic payloads. Keep them behind
    // synchronization rather than asserting Sync or rebuilding a reduced pin.
    native: Mutex<OwnedShardDirectory>,
    // SAME original handle allocation retained last; no descriptor clone.
    parent: OwnedDirectoryHandle,
}

/// Strong opaque custody of the exact directory consumed by the native store.
///
/// Every clone retains the SAME actual pin, acquired lock and original Core
/// parent. A physical worker must keep its clone outside its request/file/result
/// frames until those frames and entered work are destroyed. Cloning performs
/// one Arc increment; callers prepay each job's value/entry/work before cloning.
/// This contains no Session, startup budget or serving permission.
#[derive(Clone)]
pub struct OwnedShardStoreDirectory {
    inner: Arc<SharedDirectory>,
}
impl OwnedShardStoreDirectory {
    /// Diagnostic path from the SAME original Core directory.
    pub fn directory(&self) -> &Path {
        self.inner.parent.path()
    }
    /// SAME original physical root allocation, without native extraction.
    pub fn owner(&self) -> &Arc<DirectoryOwnership> {
        self.inner.parent.owner()
    }
}
impl fmt::Debug for OwnedShardStoreDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedShardStoreDirectory")
            .field("directory", &self.directory())
            .finish_non_exhaustive()
    }
}

/// Actual opened native store, retaining its SAME physical directory last.
///
/// The open operation is bounded. Later ordinary borrowed store methods keep
/// their existing behavior and require separate strict producers when composed
/// into a bounded host. No store/pin/descriptor extraction is provided.
pub struct OwnedPinnedShardStore {
    opened: OpenedPinnedShardStore,
    // Last: native store/files/outcomes drop before the pin and Core parent.
    directory: OwnedShardStoreDirectory,
}
impl OwnedPinnedShardStore {
    /// Borrow the actual opened store, without rebuilding it from observations.
    pub fn store(&self) -> &ShardStore {
        self.opened.store()
    }
    /// Borrow the actual mutable store; this does not bound later operations.
    pub fn store_mut(&mut self) -> &mut ShardStore {
        self.opened.store_mut()
    }
    /// Retain the SAME physical directory for an actual entered child/job.
    ///
    /// The child must prepay this fixed-size value/entry/Arc work before entry
    /// and drop it only after its actual files, results and physical work.
    pub fn retain_directory(&self) -> OwnedShardStoreDirectory {
        self.directory.clone()
    }
    /// Original actual native open usage, not a new live-operation budget.
    pub fn open_usage(&self) -> PinnedStoreOpenUsage {
        self.opened.usage()
    }
    /// Original native open deadline retained as diagnostic data only.
    pub fn open_expiry(&self) -> Instant {
        self.opened.absolute_expiry()
    }
    /// Borrow the native open outcomes already retained with this store.
    pub fn open_outcomes(&self) -> &[PinnedStoreOpenOutcome] {
        self.opened.outcomes()
    }
    /// SAME original physical root allocation.
    pub fn owner(&self) -> &Arc<DirectoryOwnership> {
        self.directory.owner()
    }
}
impl fmt::Debug for OwnedPinnedShardStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedPinnedShardStore")
            .field("opened", &self.opened)
            .field("directory", &self.directory)
            .finish_non_exhaustive()
    }
}

struct Checks<'a> {
    budget: &'a NativeDirectoryBudget,
    outer: &'a dyn PinnedStoreOpenChecks,
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
                let mut saved = self.budget_error.borrow_mut();
                if saved.is_none() {
                    *saved = Some(error);
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
        if let Err(error) = observed {
            if self.checkpoint_error.get().is_none() {
                self.checkpoint_error.set(Some(error));
            }
        }
        // Keep the actual composing result before checking the SAME minimum E.
        self.check_budget()?;
        observed
    }
}
impl PinnedStoreOpenChecks for Checks<'_> {
    fn deny(&self) {
        self.budget.deny();
        if !self.denial_attempted.replace(true) {
            self.outer.deny();
            self.denial_completed.set(true);
        }
    }
}

struct Scope {
    native: Option<Result<OpenedPinnedShardStore, PinnedStoreOpenError>>,
    config: Option<ShardStoreConfig>,
    directory_error: Option<RetainedDirectoryError>,
    budget_error: Option<NativeDirectoryFailure>,
    checkpoint_error: Option<ShardDirectoryCheckFailure>,
    panic_payload: Option<Box<dyn Any + Send>>,
    deny_panic_payload: Option<Box<dyn Any + Send>>,
    denial_completed: bool,
    // Last: all actual results/configuration settle before the original parent.
    directory: OwnedShardStoreDirectory,
}
enum Custody {
    Early {
        config: ShardStoreConfig,
        directory: OwnedShardDirectory,
    },
    Entered(Box<Scope>),
}

/// Actual open refusal retaining its input or completed native result and root.
pub struct RetainedOwnedPinnedShardStoreError {
    failure: OwnedPinnedShardStoreFailure,
    early_budget_error: Option<NativeDirectoryFailure>,
    early_deny_panic_payload: Option<Box<dyn Any + Send>>,
    early_denial_completed: bool,
    // Last: retains actual native partials/store before the pin and Core parent.
    custody: Custody,
}
impl RetainedOwnedPinnedShardStoreError {
    /// Actual bridge terminal category; native details remain separately held.
    pub fn failure(&self) -> OwnedPinnedShardStoreFailure {
        self.failure
    }
    fn scope(&self) -> Option<&Scope> {
        match &self.custody {
            Custody::Entered(scope) => Some(scope),
            Custody::Early { .. } => None,
        }
    }
    /// Actual input still held before native entry, without cloning its path.
    pub fn input_config(&self) -> Option<&ShardStoreConfig> {
        match &self.custody {
            Custody::Early { config, .. } => Some(config),
            Custody::Entered(scope) => scope.config.as_ref(),
        }
    }
    /// Actual first SAME-budget refusal, including a later tightened E.
    pub fn budget_error(&self) -> Option<&NativeDirectoryFailure> {
        self.early_budget_error
            .as_ref()
            .or_else(|| self.scope().and_then(|scope| scope.budget_error.as_ref()))
    }
    /// Actual recursive Core parent/root refusal and its retained outcomes.
    pub fn directory_error(&self) -> Option<&RetainedDirectoryError> {
        self.scope()
            .and_then(|scope| scope.directory_error.as_ref())
    }
    /// Actual first composing checkpoint refusal, if one returned.
    pub fn checkpoint_error(&self) -> Option<ShardDirectoryCheckFailure> {
        self.scope().and_then(|scope| scope.checkpoint_error)
    }
    /// Actual native open refusal and its partial descriptors/indexes/repairs.
    pub fn native_error(&self) -> Option<&PinnedStoreOpenError> {
        self.scope()?.native.as_ref()?.as_ref().err()
    }
    /// Whether native open succeeded before a later bridge refusal.
    pub fn retains_opened_store(&self) -> bool {
        self.scope()
            .is_some_and(|scope| matches!(scope.native, Some(Ok(_))))
    }
    /// Actual native usage on either native success or native refusal.
    pub fn native_usage(&self) -> Option<PinnedStoreOpenUsage> {
        match self.scope()?.native.as_ref()? {
            Ok(opened) => Some(opened.usage()),
            Err(error) => Some(error.usage()),
        }
    }
    /// Native outcomes on either success or refusal, without copying them.
    pub fn native_outcomes(&self) -> Option<&[PinnedStoreOpenOutcome]> {
        match self.scope()?.native.as_ref()? {
            Ok(opened) => Some(opened.outcomes()),
            Err(error) => Some(error.outcomes()),
        }
    }
    /// Actual bridge panic; native producer panics remain in `native_error()`.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.scope()
            .and_then(|scope| scope.panic_payload.as_deref())
    }
    /// Actual bridge denial panic; native denial panics remain in native error.
    pub fn deny_panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.early_deny_panic_payload.as_deref().or_else(|| {
            self.scope()
                .and_then(|scope| scope.deny_panic_payload.as_deref())
        })
    }
    /// Whether the actual mandatory composing denial returned normally.
    pub fn denial_completed(&self) -> bool {
        self.scope()
            .map_or(self.early_denial_completed, |scope| scope.denial_completed)
    }
    /// SAME original physical root held after every partial/error outcome.
    pub fn owner(&self) -> &Arc<DirectoryOwnership> {
        match &self.custody {
            Custody::Early { directory, .. } => directory.owner(),
            Custody::Entered(scope) => scope.directory.owner(),
        }
    }
}
impl fmt::Debug for RetainedOwnedPinnedShardStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedOwnedPinnedShardStoreError")
            .field("failure", &self.failure)
            .field("budget_error", &self.budget_error())
            .field("retains_opened_store", &self.retains_opened_store())
            .field("native_usage", &self.native_usage())
            .field("denial_completed", &self.denial_completed())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedOwnedPinnedShardStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "owned pinned shard store refused: {:?}", self.failure)
    }
}
impl Error for RetainedOwnedPinnedShardStoreError {
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

/// Derive checked logical costs before consuming the actual native directory.
///
/// Recursive Core validation spends its own actual phases on the SAME budget.
/// The actual input owner/configuration are moved; no replacement native input
/// is constructed. The two Arc counters and consumed directory heap value are
/// included independently of the bridge's fixed result/carrier values.
pub fn owned_pinned_shard_store_cost(
    config: &ShardStoreConfig,
    limits: PinnedStoreOpenLimits,
) -> Option<OwnedPinnedShardStoreCost> {
    let native = pinned_store_open_cost(config, limits)?;
    let values = [
        size_of::<Scope>(),
        size_of::<OwnedPinnedShardStore>(),
        size_of::<RetainedOwnedPinnedShardStoreError>(),
        size_of::<OwnedPinnedShardStoreCost>(),
        size_of::<Checks<'static>>(),
        size_of::<SharedDirectory>(),
        size_of::<[usize; 2]>(),
        size_of::<Result<OpenedPinnedShardStore, PinnedStoreOpenError>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
    ];
    let fixed = values.into_iter().try_fold(0usize, usize::checked_add)?;
    Some(OwnedPinnedShardStoreCost {
        declared_bytes: native.declared_bytes.checked_add(fixed)?,
        string_bytes: native.string_bytes,
        entries: native.entries.checked_add(values.len())?,
        work_bytes: native
            .work_bytes
            .checked_add(fixed)?
            .checked_add(values.len())?,
        native_call_slots: limits.max_io_calls,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    DirectoryShared,
    ParentValidated,
    NativeCompleted,
    FinalParentValidated,
}
fn completed(phase: Phase) {
    #[cfg(test)]
    tests::completed(phase);
    #[cfg(not(test))]
    let _ = phase;
}
impl Scope {
    fn check(&self, checks: &Checks<'_>) -> Result<(), OwnedPinnedShardStoreFailure> {
        checks.check().map_err(|_| {
            if checks.budget_error.borrow().is_some() {
                OwnedPinnedShardStoreFailure::Budget
            } else {
                OwnedPinnedShardStoreFailure::Checkpoint
            }
        })
    }
    fn validate(
        &mut self,
        budget: &NativeDirectoryBudget,
    ) -> Result<(), OwnedPinnedShardStoreFailure> {
        match self.directory.inner.parent.validate_until(budget) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.directory_error = Some(error);
                Err(OwnedPinnedShardStoreFailure::Directory)
            }
        }
    }
    fn run(
        &mut self,
        limits: PinnedStoreOpenLimits,
        checks: &Checks<'_>,
    ) -> Result<(), OwnedPinnedShardStoreFailure> {
        completed(Phase::DirectoryShared);
        self.check(checks)?;
        self.validate(checks.budget)?;
        completed(Phase::ParentValidated);
        self.check(checks)?;
        // No clone has escaped during this consuming open. Borrow the actual
        // input directly without locking, copying its observations or guessing
        // an owner on poison. The full input stays in the outside carrier.
        let shared = Arc::get_mut(&mut self.directory.inner)
            .ok_or(OwnedPinnedShardStoreFailure::Invariant)?;
        let directory = shared
            .native
            .get_mut()
            .map_err(|_| OwnedPinnedShardStoreFailure::Invariant)?;
        let config = self
            .config
            .take()
            .ok_or(OwnedPinnedShardStoreFailure::Invariant)?;
        // The actual result is in the outside scope before callbacks/post-E.
        self.native = Some(ShardStore::open_pinned_until(
            config,
            &directory.native,
            limits,
            checks.budget.absolute_expiry(),
            checks,
        ));
        completed(Phase::NativeCompleted);
        self.check(checks)?;
        if !matches!(self.native, Some(Ok(_))) {
            return Err(OwnedPinnedShardStoreFailure::Native);
        }
        self.validate(checks.budget)?;
        completed(Phase::FinalParentValidated);
        self.check(checks)
    }
}

#[allow(
    clippy::result_large_err,
    reason = "retain the actual consumed directory and configuration without allocating on pre-entry refusal"
)]
impl OwnedShardDirectory {
    /// Consume this genuine Core/native directory into its opened native store.
    ///
    /// Original cumulative Core fees/minimum E and mandatory composing checks
    /// cover actual Core validation and native discovery/read/decode/repair.
    /// `checks.deny()` must latch the SAME actual composing work and captured
    /// health before entered native repair settlement. Actual partial/native
    /// results and independent panic payloads remain outside unwind boundaries.
    /// Success retains no Session, callback or permanently expired budget.
    ///
    /// # Errors
    /// Retains this actual input directory/configuration before entry, or the
    /// actual partial/opened native store and original root after entry. Every
    /// refusal denies the SAME budget and attempts mandatory composing denial.
    pub fn open_store_until(
        self,
        config: ShardStoreConfig,
        limits: PinnedStoreOpenLimits,
        budget: &NativeDirectoryBudget,
        checks: &dyn PinnedStoreOpenChecks,
    ) -> Result<OwnedPinnedShardStore, RetainedOwnedPinnedShardStoreError> {
        let admitted = (|| {
            budget.check()?;
            if limits.max_path_bytes > budget.limits().max_path_bytes {
                return Err(NativeDirectoryFailure::Capacity);
            }
            let cost = owned_pinned_shard_store_cost(&config, limits)
                .ok_or(NativeDirectoryFailure::Capacity)?;
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
            budget.deny();
            let denial = catch_unwind(AssertUnwindSafe(|| checks.deny()));
            let denial_completed = denial.is_ok();
            return Err(RetainedOwnedPinnedShardStoreError {
                failure: OwnedPinnedShardStoreFailure::Budget,
                early_budget_error: Some(error),
                early_deny_panic_payload: denial.err(),
                early_denial_completed: denial_completed,
                custody: Custody::Early {
                    config,
                    directory: self,
                },
            });
        }
        let parent = self.parent.retain();
        let mut scope = Box::new(Scope {
            native: None,
            config: Some(config),
            directory_error: None,
            budget_error: None,
            checkpoint_error: None,
            panic_payload: None,
            deny_panic_payload: None,
            denial_completed: false,
            directory: OwnedShardStoreDirectory {
                inner: Arc::new(SharedDirectory {
                    native: Mutex::new(self),
                    parent,
                }),
            },
        });
        let checks = Checks {
            budget,
            outer: checks,
            budget_error: RefCell::new(None),
            checkpoint_error: Cell::new(None),
            denial_attempted: Cell::new(false),
            denial_completed: Cell::new(false),
        };
        let observed = catch_unwind(AssertUnwindSafe(|| scope.run(limits, &checks)));
        let observed = match observed {
            Ok(result) => result,
            Err(payload) => {
                scope.panic_payload = Some(payload);
                Err(OwnedPinnedShardStoreFailure::Unwind)
            }
        };
        let observed = match observed {
            Ok(()) => match scope.native.take() {
                Some(Ok(opened)) => {
                    let Scope { directory, .. } = *scope;
                    return Ok(OwnedPinnedShardStore { opened, directory });
                }
                native => {
                    scope.native = native;
                    OwnedPinnedShardStoreFailure::Invariant
                }
            },
            Err(failure) => failure,
        };
        // Native failure may already have attempted denial before settlement.
        // Never repeat an actual denial callback that unwound inside native.
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| checks.deny())) {
            scope.deny_panic_payload = Some(payload);
        }
        scope.denial_completed = checks.denial_completed.get();
        scope.budget_error = checks.budget_error.into_inner();
        scope.checkpoint_error = checks.checkpoint_error.get();
        Err(RetainedOwnedPinnedShardStoreError {
            failure: observed,
            early_budget_error: None,
            early_deny_panic_payload: None,
            early_denial_completed: false,
            custody: Custody::Entered(scope),
        })
    }
}

#[cfg(test)]
#[path = "owned_pinned_shard_store_tests.rs"]
mod tests;
