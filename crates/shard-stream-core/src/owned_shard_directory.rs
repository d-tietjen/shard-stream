//! Native shard-directory continuation from an actual retained Core directory.
//!
//! The bridge derives the actual descriptor itself and retains its SAME parent
//! and root. Native checks spend the original budget's min-only deadline; no
//! startup budget or borrowed Session is stored in the successful owner.

use super::*;
use crate::{NativeDirectoryBudget, NativeDirectoryFailure};
use shardlog::{
    AcquiredShardRecoveryGuard, PinnedShardDirectory, PinnedShardDirectoryError,
    PinnedShardDirectoryLimits, PinnedShardDirectoryObservation, ShardDirectoryCheckFailure,
    ShardDirectoryChecks, pinned_shard_directory_cost,
};
use std::any::Any;
use std::cell::RefCell;
use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::panic::{AssertUnwindSafe, catch_unwind};

#[cfg(any(target_os = "linux", target_os = "android"))]
#[path = "owned_pinned_shard_store.rs"]
mod store;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use store::{
    OwnedPinnedShardGcComplete, OwnedPinnedShardGcCost, OwnedPinnedShardGcFailure,
    OwnedPinnedShardGcReport, RetainedOwnedPinnedShardGcError, owned_pinned_shard_gc_cost,
};
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use store::{
    OwnedPinnedShardStore, OwnedPinnedShardStoreCost, OwnedPinnedShardStoreFailure,
    OwnedShardStoreDirectory, RetainedOwnedPinnedShardStoreError, owned_pinned_shard_store_cost,
};

/// Plain source-visible bridge/native values before actual continuation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnedShardDirectoryCost {
    /// Fixed carrier/checkpoint/native values and native conversion ceilings.
    pub declared_bytes: usize,
    /// Native path subset already included in declared bytes.
    pub string_bytes: usize,
    /// Fixed bridge values and all native observation/outcome slots.
    pub entries: usize,
    /// Source setup/native path/stat work, excluding recursive Core validation.
    pub work_bytes: usize,
    /// Actual descriptor clone plus four native metadata/stat phase slots.
    pub native_call_slots: usize,
}

/// Actual continuation refusal, with richer native failures held in custody.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnedShardDirectoryFailure {
    /// The SAME cumulative budget or minimum original E refused.
    Budget,
    /// Actual recursive Core parent/root validation refused.
    Directory,
    /// The actual retained directory descriptor clone failed.
    Clone,
    /// The actual native descriptor/name/acquired-lock producer refused.
    Native,
    /// A bridge phase unwound; its actual payload/custody is retained.
    Unwind,
    /// A private carrier invariant refused without reconstructing any input.
    Invariant,
}

struct Checks<'a> {
    budget: &'a NativeDirectoryBudget,
    failure: RefCell<Option<NativeDirectoryFailure>>,
}
impl ShardDirectoryChecks for Checks<'_> {
    fn check(&self) -> std::result::Result<(), ShardDirectoryCheckFailure> {
        match self.budget.check() {
            Ok(()) => Ok(()),
            Err(failure) => {
                *self.failure.borrow_mut() = Some(failure);
                Err(ShardDirectoryCheckFailure::Refused)
            }
        }
    }
}

struct Scope {
    native: Option<std::result::Result<PinnedShardDirectory, PinnedShardDirectoryError>>,
    directory_file: Option<File>,
    guard: Option<AcquiredShardRecoveryGuard>,
    directory_error: Option<RetainedDirectoryError>,
    clone_error: Option<io::Error>,
    budget_error: Option<NativeDirectoryFailure>,
    panic_payload: Option<Box<dyn Any + Send>>,
    // Last: all native descriptors/results settle before original root custody.
    parent: OwnedDirectoryHandle,
}

/// Actual native directory/lease retaining the SAME original Core parent/root.
///
/// There is no constructor accepting an arbitrary FD with a claimed Core owner,
/// native descriptor/guard extraction, fresh startup clock or serving grant.
/// This owner qualifies physical directory custody only; native store opening,
/// repair, appending and garbage collection need their actual bounded producers.
pub struct OwnedShardDirectory {
    native: PinnedShardDirectory,
    parent: OwnedDirectoryHandle,
}
impl fmt::Debug for OwnedShardDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedShardDirectory")
            .field("native", &self.native)
            .finish_non_exhaustive()
    }
}
impl OwnedShardDirectory {
    /// Same native constructor observations, with no descriptor extraction.
    pub fn observation(&self) -> &PinnedShardDirectoryObservation {
        self.native.observation()
    }
    /// Diagnostic original Core parent path; actual I/O retains its descriptor.
    pub fn directory(&self) -> &Path {
        self.parent.path()
    }
    /// SAME original physical root owner, without reconstruction or admission.
    pub fn owner(&self) -> &Arc<DirectoryOwnership> {
        self.parent.owner()
    }
}

enum Custody {
    Early {
        guard: AcquiredShardRecoveryGuard,
        parent: OwnedDirectoryHandle,
    },
    Entered(Box<Scope>),
}
/// Actual refusal retaining its acquired native guard and SAME Core root.
pub struct RetainedOwnedShardDirectoryError {
    failure: OwnedShardDirectoryFailure,
    early_budget_error: Option<NativeDirectoryFailure>,
    custody: Custody,
}
impl RetainedOwnedShardDirectoryError {
    /// Actual terminal bridge refusal.
    pub fn failure(&self) -> OwnedShardDirectoryFailure {
        self.failure
    }
    fn scope(&self) -> Option<&Scope> {
        match &self.custody {
            Custody::Entered(scope) => Some(scope),
            _ => None,
        }
    }
    /// Original SAME-budget failure, including a tighter native checkpoint.
    pub fn budget_error(&self) -> Option<&NativeDirectoryFailure> {
        self.early_budget_error
            .as_ref()
            .or_else(|| self.scope().and_then(|scope| scope.budget_error.as_ref()))
    }
    /// Actual recursive Core root/parent validation refusal and partial outcomes.
    pub fn directory_error(&self) -> Option<&RetainedDirectoryError> {
        self.scope()
            .and_then(|scope| scope.directory_error.as_ref())
    }
    /// Actual native producer refusal and its original metadata/errors/panic.
    pub fn native_error(&self) -> Option<&PinnedShardDirectoryError> {
        self.scope()
            .and_then(|scope| scope.native.as_ref())
            .and_then(|native| native.as_ref().err())
    }
    /// Actual native observation on either native success or native refusal.
    pub fn native_observation(&self) -> Option<&PinnedShardDirectoryObservation> {
        match self.scope()?.native.as_ref()? {
            Ok(native) => Some(native.observation()),
            Err(error) => Some(error.observation()),
        }
    }
    /// Original actual descriptor-clone failure, if any.
    pub fn clone_error(&self) -> Option<&io::Error> {
        self.scope().and_then(|scope| scope.clone_error.as_ref())
    }
    /// Actual bridge unwind; native unwinds remain in `native_error()`.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.scope()
            .and_then(|scope| scope.panic_payload.as_deref())
    }
    /// Whether the actual input guard remains directly held before native entry.
    pub fn retains_input_guard(&self) -> bool {
        match &self.custody {
            Custody::Early { guard, .. } => {
                let _ = guard.observation();
                true
            }
            Custody::Entered(scope) => scope.guard.is_some(),
        }
    }
    /// Actual cloned directory descriptor is retained before native entry.
    pub fn retains_directory_clone(&self) -> bool {
        self.scope()
            .is_some_and(|scope| scope.directory_file.is_some())
    }
    /// SAME actual Core root, held until all native custody physically drops.
    pub fn owner(&self) -> &Arc<DirectoryOwnership> {
        match &self.custody {
            Custody::Early { parent, .. } => parent.owner(),
            Custody::Entered(scope) => scope.parent.owner(),
        }
    }
}
impl fmt::Debug for RetainedOwnedShardDirectoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedOwnedShardDirectoryError")
            .field("failure", &self.failure)
            .field("budget_error", &self.budget_error())
            .field("native_observation", &self.native_observation())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedOwnedShardDirectoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "owned shard directory refused: {:?}", self.failure)
    }
}
impl Error for RetainedOwnedShardDirectoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(error) = self.native_error() {
            return Some(error);
        }
        if let Some(error) = self.directory_error() {
            return Some(error);
        }
        if let Some(error) = self.clone_error() {
            return Some(error);
        }
        self.budget_error().map(|error| error as &dyn Error)
    }
}

/// Derive actual bridge/native logical fees before accepting the acquired guard.
///
/// Recursive Core validation charges its own actual phases to the SAME budget.
/// Existing acquired-guard allocations are retained, not copied or normalized.
pub fn owned_shard_directory_cost(
    limits: PinnedShardDirectoryLimits,
) -> Option<OwnedShardDirectoryCost> {
    let native = pinned_shard_directory_cost(limits)?;
    let values = [
        size_of::<Scope>(),
        size_of::<OwnedShardDirectory>(),
        size_of::<RetainedOwnedShardDirectoryError>(),
        size_of::<Checks<'static>>(),
        size_of::<io::Result<File>>(),
        size_of::<OwnedShardDirectoryCost>(),
        // Independent of a native panic already retained inside `native`.
        size_of::<Box<dyn Any + Send>>(),
    ];
    let fixed = values.into_iter().try_fold(0usize, usize::checked_add)?;
    let declared_bytes = native.declared_bytes.checked_add(fixed)?;
    Some(OwnedShardDirectoryCost {
        declared_bytes,
        string_bytes: native.string_bytes,
        entries: native.entries.checked_add(values.len())?,
        work_bytes: native.work_bytes.checked_add(fixed)?.checked_add(1)?,
        native_call_slots: native.native_call_slots.checked_add(1)?,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    ParentValidated,
    DescriptorCloned,
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
    fn check(
        &mut self,
        budget: &NativeDirectoryBudget,
    ) -> std::result::Result<(), OwnedShardDirectoryFailure> {
        match budget.check() {
            Ok(()) => Ok(()),
            Err(error) => {
                self.budget_error = Some(error);
                Err(OwnedShardDirectoryFailure::Budget)
            }
        }
    }
    fn validate(
        &mut self,
        budget: &NativeDirectoryBudget,
    ) -> std::result::Result<(), OwnedShardDirectoryFailure> {
        match self.parent.validate_until(budget) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.directory_error = Some(error);
                Err(OwnedShardDirectoryFailure::Directory)
            }
        }
    }
    fn run(
        &mut self,
        limits: PinnedShardDirectoryLimits,
        budget: &NativeDirectoryBudget,
    ) -> std::result::Result<(), OwnedShardDirectoryFailure> {
        self.check(budget)?;
        self.validate(budget)?;
        completed(Phase::ParentValidated);
        self.check(budget)?;
        let observed = self.parent.inner.file.try_clone();
        match observed {
            Ok(file) => self.directory_file = Some(file),
            Err(error) => self.clone_error = Some(error),
        }
        completed(Phase::DescriptorCloned);
        self.check(budget)?;
        if self.clone_error.is_some() {
            return Err(OwnedShardDirectoryFailure::Clone);
        }
        let (file, guard) = match (self.directory_file.take(), self.guard.take()) {
            (Some(file), Some(guard)) => (file, guard),
            (file, guard) => {
                self.directory_file = file;
                self.guard = guard;
                return Err(OwnedShardDirectoryFailure::Invariant);
            }
        };
        let checks = Checks {
            budget,
            failure: RefCell::new(None),
        };
        let observed = PinnedShardDirectory::from_descriptor_until(
            file,
            guard,
            self.parent.path(),
            limits,
            budget.absolute_expiry(),
            &checks,
        );
        self.native = Some(observed);
        self.budget_error = checks.failure.into_inner();
        completed(Phase::NativeCompleted);
        self.check(budget)?;
        if !matches!(self.native.as_ref(), Some(Ok(_))) {
            return Err(OwnedShardDirectoryFailure::Native);
        }
        self.validate(budget)?;
        completed(Phase::FinalParentValidated);
        self.check(budget)
    }
}

#[allow(
    clippy::result_large_err,
    reason = "retain the actual already-acquired input guard and original root without allocating on early refusal"
)]
impl OwnedDirectoryHandle {
    /// Continue an acquired native shard lease from this actual directory FD.
    ///
    /// This derives the descriptor itself; no arbitrary FD/claimed-owner pair
    /// is accepted. All Core/native phases share the SAME budget/minimum E.
    /// Actual clone/native results are kept before post-E checks; late/error or
    /// unwind denies that budget first and retains the actual guard/root/FDs.
    /// Success contains no Session or permanently expired startup work budget.
    ///
    /// # Errors
    /// Returns original guard/root custody even for pre-entry refusal, and all
    /// actual partial descriptor/native/error/unwind outcomes after entry.
    pub fn pin_shard_directory_until(
        &self,
        guard: AcquiredShardRecoveryGuard,
        limits: PinnedShardDirectoryLimits,
        budget: &NativeDirectoryBudget,
    ) -> std::result::Result<OwnedShardDirectory, RetainedOwnedShardDirectoryError> {
        let admitted = (|| {
            budget.check()?;
            if limits.max_path_bytes > budget.limits().max_path_bytes {
                return Err(NativeDirectoryFailure::Capacity);
            }
            let cost =
                owned_shard_directory_cost(limits).ok_or(NativeDirectoryFailure::Capacity)?;
            budget.charge(
                1,
                cost.native_call_slots,
                cost.declared_bytes,
                cost.string_bytes,
                cost.entries,
                cost.work_bytes,
            )?;
            budget.check()?;
            Ok(())
        })();
        if let Err(error) = admitted {
            budget.deny();
            return Err(RetainedOwnedShardDirectoryError {
                failure: OwnedShardDirectoryFailure::Budget,
                early_budget_error: Some(error),
                custody: Custody::Early {
                    guard,
                    parent: self.retain(),
                },
            });
        }
        let mut scope = Box::new(Scope {
            native: None,
            directory_file: None,
            guard: Some(guard),
            directory_error: None,
            clone_error: None,
            budget_error: None,
            panic_payload: None,
            parent: self.retain(),
        });
        let observed = catch_unwind(AssertUnwindSafe(|| scope.run(limits, budget)));
        let observed = match observed {
            Ok(observed) => observed,
            Err(payload) => {
                budget.deny();
                scope.panic_payload = Some(payload);
                Err(OwnedShardDirectoryFailure::Unwind)
            }
        };
        match observed {
            Ok(()) => match scope.native.take() {
                Some(Ok(native)) => {
                    let Scope { parent, .. } = *scope;
                    Ok(OwnedShardDirectory { native, parent })
                }
                native => {
                    scope.native = native;
                    budget.deny();
                    Err(RetainedOwnedShardDirectoryError {
                        failure: OwnedShardDirectoryFailure::Invariant,
                        early_budget_error: None,
                        custody: Custody::Entered(scope),
                    })
                }
            },
            Err(failure) => {
                budget.deny();
                Err(RetainedOwnedShardDirectoryError {
                    failure,
                    early_budget_error: None,
                    custody: Custody::Entered(scope),
                })
            }
        }
    }
}

#[cfg(test)]
#[path = "owned_shard_directory_tests.rs"]
mod tests;
