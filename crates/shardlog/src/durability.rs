//! Original-expiry counters and actual native flush/sync outcomes.
//!
//! These ordinary values supply no authority or durability by themselves. The
//! composing owner retains this budget together with the actual storage handle
//! through synchronous completion and every refusal or unwind.

use std::{collections::TryReserveError, error::Error, fmt, io, mem::size_of, time::Instant};

/// Cumulative native durability-call allowance for one original deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageDurabilityLimits {
    /// Maximum reserved flush/sync call slots, including refused late entry.
    pub max_io_calls: usize,
}

/// Logical source costs; allocator, kernel, device and RSS overhead are excluded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageDurabilityCost {
    /// Fixed budget/error values and the complete requested outcome vector.
    pub declared_bytes: usize,
    /// Fixed values plus reserved native call slots.
    pub entries: usize,
    /// Declared source bytes plus the native-call allowance.
    pub work_bytes: usize,
}

/// Returns checked source costs before allocating counters or entering storage.
pub fn storage_durability_cost(limits: StorageDurabilityLimits) -> Option<StorageDurabilityCost> {
    let slots = limits
        .max_io_calls
        .checked_mul(size_of::<StorageDurabilityOutcome>())?;
    let declared_bytes = size_of::<StorageDurabilityBudget>()
        .checked_add(size_of::<RetainedStorageDurabilitySetupError>())?
        .checked_add(slots)?;
    Some(StorageDurabilityCost {
        declared_bytes,
        entries: limits.max_io_calls.checked_add(2)?,
        work_bytes: declared_bytes.checked_add(limits.max_io_calls)?,
    })
}

/// The actual standard file operation entered by a native storage producer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageDurabilityOperation {
    /// `File::flush` on the real active file, as in the ordinary native path.
    Flush,
    /// `File::sync_data` on that same actual active file.
    SyncData,
}

/// A prepaid native call slot and its original typed outcome.
#[derive(Debug)]
pub struct StorageDurabilityOutcome {
    operation: StorageDurabilityOperation,
    entered: bool,
    result: Option<io::Result<()>>,
}

impl StorageDurabilityOutcome {
    /// Actual native operation associated with this reserved slot.
    pub fn operation(&self) -> StorageDurabilityOperation {
        self.operation
    }
    /// Whether the synchronous native call was entered after its last E check.
    pub fn entered(&self) -> bool {
        self.entered
    }
    /// Original typed result saved before the post-call E observation.
    pub fn result(&self) -> Option<&io::Result<()>> {
        self.result.as_ref()
    }
}

/// A refusal; actual completed native I/O results remain in the same budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageDurabilityFailure {
    /// The original absolute deadline elapsed, possibly after a completed call.
    Expired,
    /// The next native call would exceed its cumulative allowance.
    Capacity,
    /// A source-size or slot calculation overflowed.
    Arithmetic,
    /// Outcome-vector allocation failed; the setup error retains its real cause.
    Allocation,
    /// This budget previously refused or was explicitly denied.
    Denied,
    /// Actual native I/O failed; its original error remains at this outcome index.
    Io {
        /// Index into this same budget's actual native outcome vector.
        outcome_index: usize,
    },
}

impl fmt::Display for StorageDurabilityFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "native durability call refused: {self:?}")
    }
}
impl Error for StorageDurabilityFailure {}

/// Same-deadline cumulative counters and actual synchronous native outcomes.
///
/// Slots never refund or recycle. This owns no file or serving permission.
/// Keep it with the same actual storage handle, including when a call refuses.
#[derive(Debug)]
pub struct StorageDurabilityBudget {
    expiry: Instant,
    limits: StorageDurabilityLimits,
    outcomes: Vec<StorageDurabilityOutcome>,
    denied: bool,
    allocation_error: Option<TryReserveError>,
}

/// Failed construction retaining the actual reservation and allocator outcome.
#[derive(Debug)]
pub struct RetainedStorageDurabilitySetupError {
    failure: StorageDurabilityFailure,
    budget: StorageDurabilityBudget,
}

impl RetainedStorageDurabilitySetupError {
    /// Actual terminal construction refusal.
    pub fn failure(&self) -> StorageDurabilityFailure {
        self.failure
    }
    /// Actual source buffers and typed allocation error retained by construction.
    pub fn budget(&self) -> &StorageDurabilityBudget {
        &self.budget
    }
}
impl fmt::Display for RetainedStorageDurabilitySetupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.failure, f)
    }
}
impl Error for RetainedStorageDurabilitySetupError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.budget
            .allocation_error
            .as_ref()
            .map(|e| e as &dyn Error)
    }
}

impl StorageDurabilityBudget {
    /// Allocates the finite outcome slots after the composing owner precharges.
    ///
    /// A late or failed allocation remains in the returned error. No filesystem
    /// operation is entered and no original deadline is refreshed here.
    #[allow(
        clippy::result_large_err,
        reason = "retain reservation without allocating on refusal"
    )]
    pub fn new(
        limits: StorageDurabilityLimits,
        original_expiry: Instant,
    ) -> Result<Self, RetainedStorageDurabilitySetupError> {
        let mut budget = Self {
            expiry: original_expiry,
            limits,
            outcomes: Vec::new(),
            denied: false,
            allocation_error: None,
        };
        let result = (|| {
            budget.check()?;
            storage_durability_cost(limits).ok_or(StorageDurabilityFailure::Arithmetic)?;
            budget.check()?;
            budget.allocation_error = budget.outcomes.try_reserve_exact(limits.max_io_calls).err();
            budget.check()?;
            if budget.allocation_error.is_some() {
                return Err(StorageDurabilityFailure::Allocation);
            }
            Ok(())
        })();
        match result {
            Ok(()) => Ok(budget),
            Err(failure) => {
                budget.denied = true;
                Err(RetainedStorageDurabilitySetupError { failure, budget })
            }
        }
    }

    /// Original caller-supplied absolute expiry.
    pub fn absolute_expiry(&self) -> Instant {
        self.expiry
    }
    /// Original finite native-call allowance.
    pub fn limits(&self) -> StorageDurabilityLimits {
        self.limits
    }
    /// Actual reserved/entered/completed slots, including late/error outcomes.
    pub fn outcomes(&self) -> &[StorageDurabilityOutcome] {
        &self.outcomes
    }
    /// Actual allocator refusal, even if the subsequent clock reported expiry.
    pub fn allocation_error(&self) -> Option<&TryReserveError> {
        self.allocation_error.as_ref()
    }
    /// Deny subsequent native entry without discarding completed outcomes.
    pub fn deny(&mut self) {
        self.denied = true;
    }

    fn clock(&mut self) -> Result<(), StorageDurabilityFailure> {
        if Instant::now() >= self.expiry {
            self.denied = true;
            return Err(StorageDurabilityFailure::Expired);
        }
        Ok(())
    }

    fn check(&mut self) -> Result<(), StorageDurabilityFailure> {
        self.clock()?;
        if self.denied {
            return Err(StorageDurabilityFailure::Denied);
        }
        Ok(())
    }

    pub(crate) fn execute(
        &mut self,
        operation: StorageDurabilityOperation,
        call: impl FnOnce() -> io::Result<()>,
        completed: &mut impl FnMut(StorageDurabilityOperation),
    ) -> Result<(), StorageDurabilityFailure> {
        self.check()?;
        if self.outcomes.len() == self.limits.max_io_calls {
            self.denied = true;
            return Err(StorageDurabilityFailure::Capacity);
        }
        let slot = self.outcomes.len();
        self.outcomes.push(StorageDurabilityOutcome {
            operation,
            entered: false,
            result: None,
        });
        self.check()?;
        // Refuse re-entry even if the native call or its completion observer
        // unwinds. Only definitely settled, timely success clears this latch.
        self.denied = true;
        self.outcomes[slot].entered = true;
        self.outcomes[slot].result = Some(call());
        completed(operation);
        self.clock()?;
        if matches!(self.outcomes[slot].result, Some(Err(_))) {
            return Err(StorageDurabilityFailure::Io {
                outcome_index: slot,
            });
        }
        self.denied = false;
        Ok(())
    }
}
