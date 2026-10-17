//! Finite ordinary native directory work under one original absolute deadline.

use crate::{
    DirectoryOwnershipError, DirectoryOwnershipUntilError, OwnedDirectoryHandle, OwnedFileHandle,
    RetainedDirectoryError,
};
use std::{
    error::Error,
    fmt,
    mem::size_of,
    sync::{Arc, Mutex},
    time::Instant,
};

/// Cumulative finite allowances for genuine retained native directory handles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeDirectoryLimits {
    /// Maximum actual retained parent depth, with the root at depth zero.
    pub max_depth: usize,
    /// Maximum bytes in one literal native component before any conversion.
    pub max_component_bytes: usize,
    /// Maximum diagnostic/root pathname bytes before native validation.
    pub max_path_bytes: usize,
    /// Cumulative entered public native operations, including validations.
    pub max_operations: usize,
    /// Cumulative prepaid explicit physical-call ceilings, including validation.
    pub max_calls: usize,
    /// Cumulative source-visible native allocation/value slots.
    pub max_entries: usize,
    /// Cumulative source-visible native allocation requests, including paths.
    pub max_declared_bytes: usize,
    /// Cumulative explicit native work and entry fees.
    pub max_work_bytes: usize,
}

/// Actual cumulative prepaid logical fees, never reset between child handles.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NativeDirectoryUsage {
    /// Entered public operations.
    pub operations: usize,
    /// Prepaid native-call ceilings; late refused entry can leave an unused fee.
    pub calls: usize,
    /// Prepaid source-visible logical allocation ceilings.
    pub declared_bytes: usize,
    /// Path/name subset already included in declared bytes.
    pub string_bytes: usize,
    /// Prepaid source-visible value/allocation slots.
    pub entries: usize,
    /// Native path/stat/local-value work including entry fees.
    pub work_bytes: usize,
}

/// Plain source-visible setup and cumulative configured work ceilings.
///
/// These facts grant no admission or custody. Allocator/library/kernel internals
/// and whole-process RSS are separate measured gates; path/name buffers,
/// explicit native Stat values, retained errors/handles and configured slots
/// are included in this logical accounting domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeDirectoryCost {
    /// Setup values plus the cumulative native allocation ceiling.
    pub declared_bytes: usize,
    /// Setup slots plus the cumulative native entry ceiling.
    pub entries: usize,
    /// Cumulative native path/name subset of declared bytes.
    pub string_bytes: usize,
    /// Setup plus cumulative source-visible native work ceiling.
    pub work_bytes: usize,
}

/// A genuine bounded native refusal, preserving any completed native error.
#[derive(Debug)]
pub enum NativeDirectoryFailure {
    /// Original E elapsed; an actual completed ownership/I/O error is retained.
    Expired(Option<DirectoryOwnershipError>),
    /// Checked finite admission refused before the next phase.
    Capacity,
    /// Shared ordinary counters were poisoned.
    Poisoned,
    /// A prior sibling refused; no further phase enters.
    Denied,
    /// An actual native identity or filesystem phase refused within E.
    Ownership(DirectoryOwnershipError),
}
impl fmt::Display for NativeDirectoryFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Expired(_) => f.write_str("native directory original deadline elapsed"),
            Self::Capacity => f.write_str("native directory capacity exceeded"),
            Self::Poisoned => f.write_str("native directory counters poisoned"),
            Self::Denied => f.write_str("native directory work refused"),
            Self::Ownership(error) => fmt::Display::fmt(error, f),
        }
    }
}
impl Error for NativeDirectoryFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Expired(Some(error)) | Self::Ownership(error) => Some(error),
            _ => None,
        }
    }
}
impl From<DirectoryOwnershipError> for NativeDirectoryFailure {
    fn from(error: DirectoryOwnershipError) -> Self {
        Self::Ownership(error)
    }
}
impl From<DirectoryOwnershipUntilError> for NativeDirectoryFailure {
    fn from(error: DirectoryOwnershipUntilError) -> Self {
        match error {
            DirectoryOwnershipUntilError::Expired => Self::Expired(None),
            DirectoryOwnershipUntilError::Capacity => Self::Capacity,
            DirectoryOwnershipUntilError::Ownership(error) => Self::Ownership(error),
        }
    }
}

struct State {
    expiry: Instant,
    usage: NativeDirectoryUsage,
    denied: bool,
}
struct Inner {
    limits: NativeDirectoryLimits,
    original_expiry: Instant,
    state: Mutex<State>,
}

/// Ordinary shared counters; this contains no native owner or serving proof.
///
/// Every clone spends the SAME actual counters and original/min-only E. The
/// caller must prepay the plain ceiling and retain its real native handles.
#[derive(Clone)]
pub struct NativeDirectoryBudget {
    inner: Arc<Inner>,
}
impl fmt::Debug for NativeDirectoryBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeDirectoryBudget")
            .field("limits", &self.inner.limits)
            .field("original_expiry", &self.inner.original_expiry)
            .finish_non_exhaustive()
    }
}
fn sum(a: usize, b: usize) -> Result<usize, NativeDirectoryFailure> {
    a.checked_add(b).ok_or(NativeDirectoryFailure::Capacity)
}

/// Derive checked plain costs before constructing native directory counters.
pub fn native_directory_cost(
    limits: NativeDirectoryLimits,
) -> Result<NativeDirectoryCost, NativeDirectoryFailure> {
    if limits.max_component_bytes == 0
        || limits.max_path_bytes == 0
        || limits.max_operations == 0
        || limits.max_calls == 0
    {
        return Err(NativeDirectoryFailure::Capacity);
    }
    let fixed_values = [
        size_of::<Inner>(),
        size_of::<NativeDirectoryBudget>(),
        size_of::<NativeDirectoryUsage>(),
        size_of::<NativeDirectoryCost>(),
        size_of::<OwnedDirectoryHandle>(),
        size_of::<OwnedFileHandle>(),
        size_of::<RetainedDirectoryError>(),
        crate::owned_directory_handle::operation_until::retained_scope_bytes(),
    ];
    let fixed = fixed_values
        .into_iter()
        .try_fold(size_of::<[usize; 2]>(), sum)?;
    let entries = sum(sum(fixed_values.len(), 2)?, limits.max_entries)?;
    let declared_bytes = sum(fixed, limits.max_declared_bytes)?;
    Ok(NativeDirectoryCost {
        declared_bytes,
        entries,
        string_bytes: limits.max_declared_bytes,
        work_bytes: sum(sum(declared_bytes, entries)?, limits.max_work_bytes)?,
    })
}
impl NativeDirectoryBudget {
    /// Construct finite ordinary counters with the caller's original E.
    pub fn new(
        limits: NativeDirectoryLimits,
        original_expiry: Instant,
    ) -> Result<Self, NativeDirectoryFailure> {
        if Instant::now() >= original_expiry {
            return Err(NativeDirectoryFailure::Expired(None));
        }
        let cost = native_directory_cost(limits);
        if Instant::now() >= original_expiry {
            return Err(NativeDirectoryFailure::Expired(None));
        }
        cost?;
        let budget = Self {
            inner: Arc::new(Inner {
                limits,
                original_expiry,
                state: Mutex::new(State {
                    expiry: original_expiry,
                    usage: NativeDirectoryUsage::default(),
                    denied: false,
                }),
            }),
        };
        budget.check()?;
        Ok(budget)
    }
    /// The actual shared minimum of original E and all later caps.
    pub fn absolute_expiry(&self) -> Instant {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .expiry
    }
    /// Tighten the SAME actual deadline, retaining all limits/fees/refusal state.
    ///
    /// The minimum is stored even when this returns expired, denied or poison;
    /// a later caller cannot revive this owner by supplying a larger deadline.
    pub fn cap_expiry(&self, earlier: Instant) -> Result<(), NativeDirectoryFailure> {
        let (mut state, poisoned) = match self.inner.state.lock() {
            Ok(state) => (state, false),
            Err(poison) => (poison.into_inner(), true),
        };
        state.expiry = state.expiry.min(earlier);
        if poisoned {
            state.denied = true;
            return Err(NativeDirectoryFailure::Poisoned);
        }
        if Instant::now() >= state.expiry {
            return Err(NativeDirectoryFailure::Expired(None));
        }
        if state.denied {
            return Err(NativeDirectoryFailure::Denied);
        }
        Ok(())
    }
    /// The unchanged shared finite limits.
    pub fn limits(&self) -> NativeDirectoryLimits {
        self.inner.limits
    }
    /// Copy the actual cumulative prepaid fees, including late refused entry.
    pub fn usage(&self) -> Result<NativeDirectoryUsage, NativeDirectoryFailure> {
        self.inner
            .state
            .lock()
            .map(|state| state.usage)
            .map_err(|_| NativeDirectoryFailure::Poisoned)
    }
    /// Check original E and the shared refusal state before a new phase.
    pub fn check(&self) -> Result<(), NativeDirectoryFailure> {
        self.charge(0, 0, 0, 0, 0, 0)
    }
    /// Prepay a composing producer's actual costs against these SAME counters.
    ///
    /// String bytes are a subset of declared bytes. Each entry also adds one
    /// work-byte fee, in addition to `work`. Reservations are cumulative and
    /// cannot be refunded; capacity or arithmetic refusal denies later phases.
    /// This checks the original/minimum deadline and grants no native custody
    /// or serving permission. The caller retains its real inputs and outcomes.
    pub fn reserve(
        &self,
        operations: usize,
        calls: usize,
        declared: usize,
        strings: usize,
        entries: usize,
        work: usize,
    ) -> Result<(), NativeDirectoryFailure> {
        self.charge(operations, calls, declared, strings, entries, work)
    }
    /// Refuse every later phase using these counters, including sibling clones.
    ///
    /// Composing producers call this before their captured-health denial and
    /// physical settlement. It preserves all paid costs and the minimum E.
    /// Denial grants no native ownership or permission and cannot be reversed.
    pub fn deny(&self) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .denied = true;
    }
    pub(crate) fn charge(
        &self,
        operations: usize,
        calls: usize,
        declared: usize,
        strings: usize,
        entries: usize,
        work: usize,
    ) -> Result<(), NativeDirectoryFailure> {
        if Instant::now() >= self.inner.original_expiry {
            return Err(NativeDirectoryFailure::Expired(None));
        }
        let state = self.inner.state.lock();
        let expiry = match &state {
            Ok(state) => state.expiry,
            Err(poison) => poison.get_ref().expiry,
        };
        if Instant::now() >= expiry {
            return Err(NativeDirectoryFailure::Expired(None));
        }
        let mut state = state.map_err(|_| NativeDirectoryFailure::Poisoned)?;
        if state.denied {
            return Err(NativeDirectoryFailure::Denied);
        }
        let next = (|| {
            let usage = NativeDirectoryUsage {
                operations: sum(state.usage.operations, operations)?,
                calls: sum(state.usage.calls, calls)?,
                declared_bytes: sum(state.usage.declared_bytes, declared)?,
                string_bytes: sum(state.usage.string_bytes, strings)?,
                entries: sum(state.usage.entries, entries)?,
                work_bytes: sum(state.usage.work_bytes, sum(work, entries)?)?,
            };
            let limits = self.inner.limits;
            if usage.operations > limits.max_operations
                || usage.calls > limits.max_calls
                || usage.declared_bytes > limits.max_declared_bytes
                || usage.string_bytes > usage.declared_bytes
                || usage.entries > limits.max_entries
                || usage.work_bytes > limits.max_work_bytes
            {
                return Err(NativeDirectoryFailure::Capacity);
            }
            Ok(usage)
        })();
        match next {
            Ok(usage) => {
                state.usage = usage;
                Ok(())
            }
            Err(error) => {
                state.denied = true;
                Err(error)
            }
        }
    }
    pub(crate) fn completed<T>(
        &self,
        observed: Result<T, DirectoryOwnershipError>,
    ) -> Result<T, NativeDirectoryFailure> {
        if Instant::now() >= self.absolute_expiry() {
            self.deny();
            return Err(NativeDirectoryFailure::Expired(observed.err()));
        }
        match observed {
            Ok(value) => {
                self.check()?;
                Ok(value)
            }
            Err(error) => {
                self.deny();
                Err(NativeDirectoryFailure::Ownership(error))
            }
        }
    }
    pub(crate) fn completed_optional<T>(
        &self,
        observed: Result<T, DirectoryOwnershipError>,
    ) -> Result<Option<T>, NativeDirectoryFailure> {
        if Instant::now() >= self.absolute_expiry() {
            self.deny();
            return Err(NativeDirectoryFailure::Expired(observed.err()));
        }
        match observed {
            Err(DirectoryOwnershipError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                match self.check() {
                    Ok(()) => Ok(None),
                    Err(NativeDirectoryFailure::Expired(_)) => Err(
                        NativeDirectoryFailure::Expired(Some(DirectoryOwnershipError::Io(error))),
                    ),
                    Err(failure) => Err(failure),
                }
            }
            observed => self.completed(observed).map(Some),
        }
    }
    pub(crate) fn finish<T>(
        &self,
        observed: Result<T, NativeDirectoryFailure>,
    ) -> Result<T, NativeDirectoryFailure> {
        let observed = if Instant::now() >= self.absolute_expiry() {
            Err(match observed {
                Err(NativeDirectoryFailure::Ownership(error))
                | Err(NativeDirectoryFailure::Expired(Some(error))) => {
                    NativeDirectoryFailure::Expired(Some(error))
                }
                _ => NativeDirectoryFailure::Expired(None),
            })
        } else {
            observed.and_then(|value| {
                self.check()?;
                Ok(value)
            })
        };
        if observed.is_err() {
            self.deny();
        }
        observed
    }
}
