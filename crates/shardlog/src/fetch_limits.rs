//! Shared ordinary resource counters for bounded native fetch work.
//!
//! These counters grant no storage/currentness/serving authority. A composing
//! owner pays their plain source-visible ceiling before constructing one, and
//! retains the actual storage/engine while every entered physical job settles.

use crate::StorageError;
#[cfg(unix)]
use crate::{ShardDirectoryCheckFailure, ShardDirectoryChecks};
#[cfg(unix)]
use std::{
    any::Any,
    panic::{AssertUnwindSafe, catch_unwind},
};
use std::{
    error::Error,
    fmt,
    mem::size_of,
    sync::{Arc, Mutex},
    time::Instant,
};

/// Finite native fetch allowances shared by every entered shard job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageFetchLimits {
    /// Maximum metadata entries in any one native shard plan.
    pub max_plan_batches: usize,
    /// Cumulative explicitly allocated metadata/local-value slots.
    pub max_entries: usize,
    /// Maximum bytes in an actual native pack pathname before opening it.
    pub max_path_bytes: usize,
    /// Cumulative prepaid read_at attempts, including short/error/EOF attempts.
    pub max_read_attempts: usize,
    /// Cumulative requested read_at bytes, including retries and coalesced gaps.
    pub max_read_bytes: usize,
    /// Cumulative requested native allocation bytes, including output buffers.
    pub max_declared_bytes: usize,
    /// Cumulative explicit native work bytes and entries.
    pub max_work_bytes: usize,
}

/// Actual cumulative native fetch debit; no counter is reset between jobs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageFetchUsage {
    /// Requested native allocation bytes.
    pub declared_bytes: usize,
    /// Requested metadata/local-value slots.
    pub entries: usize,
    /// Prepaid read_at attempts; a late entry check may retain an unused debit.
    pub read_attempts: usize,
    /// Prepaid requested read_at bytes; retries and late refused entry count.
    pub read_bytes: usize,
    /// Debited native CPU/metadata/path/read work.
    pub work_bytes: usize,
}

/// Plain source-visible setup plus configured allocation/work ceilings.
///
/// String bytes are a subset of declared bytes. These facts exclude allocator,
/// kernel and RSS overhead and grant no admission. A native engine composes
/// them with its actual coordinator/worker and output-container costs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageFetchCost {
    /// Native budget structures and configured allocation ceiling.
    pub declared_bytes: usize,
    /// Native fixed slots plus configured cumulative slot ceiling.
    pub entries: usize,
    /// Cumulative path-conversion subset of the declared native ceiling.
    pub string_bytes: usize,
    /// Source-visible setup and configured native work ceiling.
    pub work_bytes: usize,
}

/// A deadline, resource, poison, prior refusal or actual native I/O refusal.
#[derive(Debug)]
pub enum StorageFetchFailure {
    /// Original absolute E elapsed after an entered phase; preserve any error.
    Expired(Option<StorageError>),
    /// A checked finite allowance refused before the next native phase.
    Capacity,
    /// Shared counters were poisoned; no further native phase enters.
    Poisoned,
    /// Another shard refused; only already-entered work may settle.
    Denied,
    /// The actual entered native phase refused within E.
    Storage(StorageError),
    /// A retained read reached an inconsistent internal owner transition.
    ReadState,
    /// The actual bounded native read frame unwound after retaining its owners.
    NativeReadUnwind {
        /// Original source/native unwind payload, never reconstructed.
        payload: Box<dyn std::any::Any + Send>,
        /// Actual native error already returned before the unwind, if any.
        storage_error: Option<StorageError>,
        /// Original fetch expiry elapsed after this actual outcome.
        late: bool,
    },
    /// SAME composing checkpoint refused after retaining an actual native error.
    #[cfg(unix)]
    Checkpoint {
        /// The exact ordinary composing checkpoint category.
        failure: ShardDirectoryCheckFailure,
        /// Independently completed actual native error, when present.
        storage_error: Option<StorageError>,
        /// The original fetch expiry elapsed after this actual outcome.
        late: bool,
    },
    /// SAME composing checkpoint unwound, retaining the original payload.
    #[cfg(unix)]
    CheckpointUnwind {
        /// Original callback unwind value, without formatting or replacement.
        payload: Box<dyn Any + Send>,
        /// Independently completed actual native error, when present.
        storage_error: Option<StorageError>,
        /// The original fetch expiry elapsed after this actual outcome.
        late: bool,
    },
}
impl fmt::Display for StorageFetchFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Expired(_) => "native fetch original deadline elapsed",
            Self::Capacity => "native fetch capacity exceeded",
            Self::Poisoned => "native fetch counters poisoned",
            Self::Denied => "native fetch work refused",
            Self::Storage(_) => "native fetch storage phase refused",
            Self::ReadState => "native fetch read owner transition refused",
            Self::NativeReadUnwind { .. } => "native fetch read frame unwound",
            #[cfg(unix)]
            Self::Checkpoint { .. } => "native fetch composing checkpoint refused",
            #[cfg(unix)]
            Self::CheckpointUnwind { .. } => "native fetch composing checkpoint unwound",
        })
    }
}
impl Error for StorageFetchFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Expired(Some(error)) | Self::Storage(error) => Some(error),
            Self::NativeReadUnwind {
                storage_error: Some(error),
                ..
            } => Some(error),
            #[cfg(unix)]
            Self::Checkpoint {
                storage_error: Some(error),
                ..
            }
            | Self::CheckpointUnwind {
                storage_error: Some(error),
                ..
            } => Some(error),
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
struct State {
    usage: StorageFetchUsage,
    denied: bool,
}
struct Inner {
    limits: StorageFetchLimits,
    expiry: Instant,
    state: Mutex<State>,
    #[cfg(unix)]
    checks: Option<Arc<dyn ShardDirectoryChecks + Send + Sync>>,
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Inner");
        debug
            .field("limits", &self.limits)
            .field("expiry", &self.expiry)
            .field("state", &self.state);
        #[cfg(unix)]
        if self.checks.is_some() {
            debug.field("checked", &true);
        }
        debug.finish()
    }
}

#[cfg(unix)]
enum CheckFailure {
    Refused(ShardDirectoryCheckFailure),
    Unwind(Box<dyn Any + Send>),
}
#[cfg(unix)]
impl CheckFailure {
    fn retain(self, storage_error: Option<StorageError>) -> StorageFetchFailure {
        match self {
            Self::Refused(failure) => StorageFetchFailure::Checkpoint {
                failure,
                storage_error,
                late: false,
            },
            Self::Unwind(payload) => StorageFetchFailure::CheckpointUnwind {
                payload,
                storage_error,
                late: false,
            },
        }
    }
}
#[cfg(unix)]
fn observe_checks(checks: &(dyn ShardDirectoryChecks + Send + Sync)) -> Result<(), CheckFailure> {
    match catch_unwind(AssertUnwindSafe(|| checks.check())) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(CheckFailure::Refused(error)),
        Err(payload) => Err(CheckFailure::Unwind(payload)),
    }
}

/// Shared ordinary counters and the same original absolute deadline.
///
/// Clones refer to the same actual counters. This owns no engine/storage and
/// proves no caller admission, source completeness, process or serving state.
#[derive(Clone, Debug)]
pub struct StorageFetchBudget {
    inner: Arc<Inner>,
}

fn checked(a: usize, b: usize) -> Result<usize, StorageFetchFailure> {
    a.checked_add(b).ok_or(StorageFetchFailure::Capacity)
}

/// Derive checked plain costs before allocating the shared native counters.
///
/// Allocation ceilings are cumulative requested native sizes, not measured
/// allocator capacities or private library storage. Actual path conversions
/// and all output buffer/metadata allocations are charged before entry.
pub fn storage_fetch_cost(
    limits: StorageFetchLimits,
) -> Result<StorageFetchCost, StorageFetchFailure> {
    if limits.max_plan_batches == 0 || limits.max_entries == 0 || limits.max_path_bytes == 0 {
        return Err(StorageFetchFailure::Capacity);
    }
    let fixed = checked(
        checked(size_of::<Inner>(), size_of::<StorageFetchBudget>())?,
        checked(
            checked(size_of::<[usize; 2]>(), size_of::<StorageFetchCost>())?,
            size_of::<StorageFetchUsage>(),
        )?,
    )?;
    #[cfg(unix)]
    let fixed = checked(fixed, size_of::<CheckFailure>())?;
    let (read_framing, read_entries) = read_custody_cost()?;
    let fixed = checked(fixed, read_framing)?;
    let string_bytes = limits.max_declared_bytes;
    let declared_bytes = checked(fixed, limits.max_declared_bytes)?;
    // The actual Inner, budget handle, cost and usage values plus the two
    // source-visible Arc counters are separate fixed logical slots.
    let fixed_values = [
        size_of::<Inner>(),
        size_of::<StorageFetchBudget>(),
        size_of::<StorageFetchCost>(),
        size_of::<StorageFetchUsage>(),
    ];
    let entries = checked(limits.max_entries, checked(fixed_values.len(), 2)?)?;
    #[cfg(unix)]
    let entries = checked(entries, 1)?;
    let entries = checked(entries, read_entries)?;
    Ok(StorageFetchCost {
        declared_bytes,
        entries,
        string_bytes,
        work_bytes: checked(checked(declared_bytes, entries)?, limits.max_work_bytes)?,
    })
}

impl StorageFetchBudget {
    /// Construct finite ordinary counters after the composing owner precharges.
    ///
    /// This does not perform I/O, open storage or produce serving permission.
    pub fn new(
        limits: StorageFetchLimits,
        original_expiry: Instant,
    ) -> Result<Self, StorageFetchFailure> {
        Self::new_inner(
            limits,
            original_expiry,
            #[cfg(unix)]
            None,
        )
    }
    /// Construct the SAME finite native budget with a deny-only composing check.
    ///
    /// The caller must prepay actual setup, retain richer checkpoint failures,
    /// and keep original owners until every entered job physically settles.
    /// Checks run without the fetch counter lock and cannot renew limits or E.
    ///
    /// # Errors
    /// Retains a typed checkpoint refusal or original unwind payload, as well as
    /// ordinary setup/deadline failures. This does not grant serving authority.
    #[cfg(unix)]
    pub fn new_with_checks(
        limits: StorageFetchLimits,
        original_expiry: Instant,
        checks: Arc<dyn ShardDirectoryChecks + Send + Sync>,
    ) -> Result<Self, StorageFetchFailure> {
        Self::new_inner(limits, original_expiry, Some(checks))
    }
    fn new_inner(
        limits: StorageFetchLimits,
        original_expiry: Instant,
        #[cfg(unix)] checks: Option<Arc<dyn ShardDirectoryChecks + Send + Sync>>,
    ) -> Result<Self, StorageFetchFailure> {
        if Instant::now() >= original_expiry {
            return Err(StorageFetchFailure::Expired(None));
        }
        let measured = storage_fetch_cost(limits);
        if Instant::now() >= original_expiry {
            return Err(StorageFetchFailure::Expired(None));
        }
        measured?;
        #[cfg(unix)]
        if let Some(checks) = &checks {
            observe_checks(checks.as_ref()).map_err(|failure| {
                let failure = failure.retain(None);
                if Instant::now() >= original_expiry {
                    failure.late()
                } else {
                    failure
                }
            })?;
            if Instant::now() >= original_expiry {
                return Err(StorageFetchFailure::Expired(None));
            }
        }
        let budget = Self {
            inner: Arc::new(Inner {
                limits,
                expiry: original_expiry,
                state: Mutex::new(State::default()),
                #[cfg(unix)]
                checks,
            }),
        };
        budget.check()?;
        Ok(budget)
    }
    /// The unchanged caller-supplied absolute deadline.
    pub fn absolute_expiry(&self) -> Instant {
        self.inner.expiry
    }
    /// The unchanged finite limits shared by all jobs.
    pub fn limits(&self) -> StorageFetchLimits {
        self.inner.limits
    }
    /// Copy actual cumulative charged counters, including paid refused entry.
    pub fn usage(&self) -> Result<StorageFetchUsage, StorageFetchFailure> {
        self.inner
            .state
            .lock()
            .map(|state| state.usage)
            .map_err(|_| StorageFetchFailure::Poisoned)
    }
    /// Refuse any later phase while already-entered calls physically settle.
    pub fn deny(&self) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .denied = true;
    }
    /// Check E and the same actual shared refusal state before a new phase.
    pub fn check(&self) -> Result<(), StorageFetchFailure> {
        self.charge(0, 0, 0, 0, 0)
    }
    /// Spend ordinary metadata/allocation/work counters before a native phase.
    ///
    /// This can only consume the same finite budget, never refund or renew it;
    /// it supplies no resource admission or serving authority to its caller.
    pub fn charge_values(
        &self,
        declared_bytes: usize,
        entries: usize,
        work_bytes: usize,
    ) -> Result<(), StorageFetchFailure> {
        self.charge(declared_bytes, entries, 0, 0, work_bytes)
    }
    pub(crate) fn charge(
        &self,
        declared: usize,
        entries: usize,
        attempts: usize,
        read_bytes: usize,
        work: usize,
    ) -> Result<(), StorageFetchFailure> {
        #[cfg(unix)]
        if let Some(checks) = &self.inner.checks {
            self.charge_plain(0, 0, 0, 0, 0)?;
            self.checked_observation(checks.as_ref())?;
            self.charge_plain(declared, entries, attempts, read_bytes, work)?;
            self.checked_observation(checks.as_ref())?;
            return self.charge_plain(0, 0, 0, 0, 0);
        }
        self.charge_plain(declared, entries, attempts, read_bytes, work)
    }
    #[cfg(unix)]
    fn checked_observation(
        &self,
        checks: &(dyn ShardDirectoryChecks + Send + Sync),
    ) -> Result<(), StorageFetchFailure> {
        match observe_checks(checks) {
            Ok(()) => Ok(()),
            Err(failure) => {
                self.deny();
                let failure = failure.retain(None);
                Err(if Instant::now() >= self.inner.expiry {
                    failure.late()
                } else {
                    failure
                })
            }
        }
    }
    fn charge_plain(
        &self,
        declared: usize,
        entries: usize,
        attempts: usize,
        read_bytes: usize,
        work: usize,
    ) -> Result<(), StorageFetchFailure> {
        if Instant::now() >= self.inner.expiry {
            return Err(StorageFetchFailure::Expired(None));
        }
        let observed = self.inner.state.lock();
        // The mutex may have waited behind a real entered sibling phase, even
        // when its completed result is a poison refusal.
        if Instant::now() >= self.inner.expiry {
            return Err(StorageFetchFailure::Expired(None));
        }
        let mut state = observed.map_err(|_| StorageFetchFailure::Poisoned)?;
        if state.denied {
            return Err(StorageFetchFailure::Denied);
        }
        let next = (|| {
            let usage = StorageFetchUsage {
                declared_bytes: checked(state.usage.declared_bytes, declared)?,
                entries: checked(state.usage.entries, entries)?,
                read_attempts: checked(state.usage.read_attempts, attempts)?,
                read_bytes: checked(state.usage.read_bytes, read_bytes)?,
                work_bytes: checked(state.usage.work_bytes, checked(work, entries)?)?,
            };
            let limits = self.inner.limits;
            if usage.declared_bytes > limits.max_declared_bytes
                || usage.entries > limits.max_entries
                || usage.read_attempts > limits.max_read_attempts
                || usage.read_bytes > limits.max_read_bytes
                || usage.work_bytes > limits.max_work_bytes
            {
                return Err(StorageFetchFailure::Capacity);
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
    // Called only after the actual successful native value is already retained
    // outside the read frame. This is a post-success check, not a native Result.
    pub(crate) fn completed_success(&self) -> Result<(), StorageFetchFailure> {
        #[cfg(unix)]
        if let Some(checks) = &self.inner.checks {
            if Instant::now() >= self.inner.expiry {
                self.deny();
                return Err(StorageFetchFailure::Expired(None));
            }
            self.charge_plain(0, 0, 0, 0, 0)?;
            if let Err(failure) = observe_checks(checks.as_ref()) {
                self.deny();
                let failure = failure.retain(None);
                return Err(if Instant::now() >= self.inner.expiry {
                    failure.late()
                } else {
                    failure
                });
            }
            return self.charge_plain(0, 0, 0, 0, 0);
        }
        if Instant::now() >= self.inner.expiry {
            self.deny();
            return Err(StorageFetchFailure::Expired(None));
        }
        self.check()
    }
    // The real returned error moves exactly once into this terminal cause.
    // Native-error precedence over prior sibling denial matches completed().
    pub(crate) fn completed_error(&self, error: StorageError) -> StorageFetchFailure {
        #[cfg(unix)]
        if let Some(checks) = &self.inner.checks {
            if Instant::now() >= self.inner.expiry {
                self.deny();
                return StorageFetchFailure::Expired(Some(error));
            }
            if self.charge_plain(0, 0, 0, 0, 0).is_err() {
                self.deny();
                return StorageFetchFailure::Storage(error);
            }
            if let Err(failure) = observe_checks(checks.as_ref()) {
                self.deny();
                let failure = failure.retain(Some(error));
                return if Instant::now() >= self.inner.expiry {
                    failure.late()
                } else {
                    failure
                };
            }
            let after = self.charge_plain(0, 0, 0, 0, 0);
            self.deny();
            return if matches!(after, Err(StorageFetchFailure::Expired(_))) {
                StorageFetchFailure::Expired(Some(error))
            } else {
                StorageFetchFailure::Storage(error)
            };
        }
        if Instant::now() >= self.inner.expiry {
            self.deny();
            StorageFetchFailure::Expired(Some(error))
        } else {
            self.deny();
            StorageFetchFailure::Storage(error)
        }
    }
    pub(crate) fn completed<T>(
        &self,
        result: Result<T, StorageError>,
    ) -> Result<T, StorageFetchFailure> {
        #[cfg(unix)]
        if let Some(checks) = &self.inner.checks {
            if Instant::now() >= self.inner.expiry {
                self.deny();
                return Err(StorageFetchFailure::Expired(result.err()));
            }
            if let Err(failure) = self.charge_plain(0, 0, 0, 0, 0) {
                // Preserve existing native-error precedence over sibling denial.
                return match result {
                    Err(error) => {
                        self.deny();
                        Err(StorageFetchFailure::Storage(error))
                    }
                    Ok(_) => Err(failure),
                };
            }
            if let Err(failure) = observe_checks(checks.as_ref()) {
                self.deny();
                let failure = failure.retain(result.err());
                return Err(if Instant::now() >= self.inner.expiry {
                    failure.late()
                } else {
                    failure
                });
            }
            let after = self.charge_plain(0, 0, 0, 0, 0);
            return match result {
                Ok(value) => {
                    after?;
                    Ok(value)
                }
                Err(error) => {
                    self.deny();
                    if matches!(after, Err(StorageFetchFailure::Expired(_))) {
                        Err(StorageFetchFailure::Expired(Some(error)))
                    } else {
                        Err(StorageFetchFailure::Storage(error))
                    }
                }
            };
        }
        self.completed_plain(result)
    }
    fn completed_plain<T>(
        &self,
        result: Result<T, StorageError>,
    ) -> Result<T, StorageFetchFailure> {
        if Instant::now() >= self.inner.expiry {
            self.deny();
            return Err(StorageFetchFailure::Expired(result.err()));
        }
        match result {
            Ok(value) => {
                self.check()?;
                Ok(value)
            }
            Err(error) => {
                self.deny();
                Err(StorageFetchFailure::Storage(error))
            }
        }
    }
}

impl From<StorageError> for StorageFetchFailure {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}
impl StorageFetchFailure {
    pub(crate) fn into_storage_error(self) -> StorageError {
        match self {
            Self::Storage(error) => error,
            error => StorageError::LimitExceeded(error.to_string()),
        }
    }
    pub(crate) fn late(self) -> Self {
        match self {
            Self::Storage(error) => Self::Expired(Some(error)),
            Self::Expired(error) => Self::Expired(error),
            Self::NativeReadUnwind {
                payload,
                storage_error,
                ..
            } => Self::NativeReadUnwind {
                payload,
                storage_error,
                late: true,
            },
            #[cfg(unix)]
            Self::Checkpoint {
                failure,
                storage_error,
                ..
            } => Self::Checkpoint {
                failure,
                storage_error,
                late: true,
            },
            #[cfg(unix)]
            Self::CheckpointUnwind {
                payload,
                storage_error,
                ..
            } => Self::CheckpointUnwind {
                payload,
                storage_error,
                late: true,
            },
            _ => Self::Expired(None),
        }
    }
}

/// A refused native plan retaining every already-materialized metadata entry.
#[derive(Debug)]
pub struct StorageFetchPlanError {
    pub(crate) failure: StorageFetchFailure,
    pub(crate) planned: Vec<crate::StoredBatchMetadata>,
}
impl StorageFetchPlanError {
    /// Actual native deadline/resource/I/O refusal.
    pub fn failure(&self) -> &StorageFetchFailure {
        &self.failure
    }
    /// Every metadata entry materialized before refusal.
    pub fn planned(&self) -> &[crate::StoredBatchMetadata] {
        &self.planned
    }
}
impl fmt::Display for StorageFetchPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.failure, f)
    }
}
impl Error for StorageFetchPlanError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.failure)
    }
}

// One current range only. Actual buffer/result children precede File owners
// on drop; enclosing engine fetch Scope already drops this before its engine.
#[derive(Default)]
pub(crate) struct ReadCustody {
    pub(crate) pack_sequence: Option<u64>,
    pub(crate) range_start: u64,
    pub(crate) next_offset: u64,
    pub(crate) range_len: usize,
    pub(crate) confirmed_bytes: usize,
    pub(crate) read_entered: bool,
    pub(crate) read_returned: bool,
    pub(crate) read_error_returned: bool,
    pub(crate) open_error_returned: bool,
    pub(crate) raw: Option<crate::extent::ReadBufferOwner>,
    pub(crate) promoted: Option<bytes::Bytes>,
    pub(crate) read_result: Option<Result<usize, StorageError>>,
    pub(crate) reader: Option<std::fs::File>,
    pub(crate) open_result: Option<Result<std::fs::File, StorageError>>,
}
// Physical coalescing can include unselected records between requested batches.
// Keep that complete owner for custody without formatting its bytes or File.
impl fmt::Debug for ReadCustody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadCustody")
            .field("pack_sequence", &self.pack_sequence)
            .field("range_start", &self.range_start)
            .field("next_offset", &self.next_offset)
            .field("range_len", &self.range_len)
            .field("confirmed_bytes", &self.confirmed_bytes)
            .field("read_entered", &self.read_entered)
            .field("read_returned", &self.read_returned)
            .field("read_error_returned", &self.read_error_returned)
            .field("open_error_returned", &self.open_error_returned)
            .field("has_raw_buffer", &self.raw.is_some())
            .field("has_promoted_buffer", &self.promoted.is_some())
            .field("has_read_result", &self.read_result.is_some())
            .field("has_native_reader", &self.has_reader())
            .field("has_open_result", &self.open_result.is_some())
            .finish()
    }
}
impl ReadCustody {
    pub(crate) fn take_native_error(&mut self) -> Option<StorageError> {
        if self.read_result.as_ref().is_some_and(Result::is_err)
            && let Some(Err(error)) = self.read_result.take()
        {
            self.read_error_returned = true;
            return Some(error);
        }
        if self.open_result.as_ref().is_some_and(Result::is_err)
            && let Some(Err(error)) = self.open_result.take()
        {
            self.open_error_returned = true;
            return Some(error);
        }
        None
    }
    fn buffer_len(&self) -> Option<usize> {
        self.raw
            .as_ref()
            .map(|raw| raw.as_ref().len())
            .or_else(|| self.promoted.as_ref().map(bytes::Bytes::len))
    }
    #[cfg(test)]
    fn bytes(&self) -> Option<&[u8]> {
        self.raw
            .as_ref()
            .map(AsRef::as_ref)
            .or(self.promoted.as_deref())
    }
    fn has_reader(&self) -> bool {
        self.reader.is_some() || self.open_result.as_ref().is_some_and(Result::is_ok)
    }
}

// This pays both the live source owner and its terminal error/return carrier,
// the actual open/read return-and-move slots, and the caught Result metadata.
// It is source-visible logical framing, not a stack/RSS or panic-payload bound.
pub(crate) fn read_custody_cost() -> Result<(usize, usize), StorageFetchFailure> {
    let values = [
        size_of::<ReadCustody>(),
        size_of::<StorageFetchReadError>(),
        size_of::<Result<Vec<crate::StoredBatch>, StorageFetchReadError>>(),
        size_of::<Result<(), StorageFetchFailure>>(),
        size_of::<Result<Result<(), StorageFetchFailure>, Box<dyn std::any::Any + Send>>>(),
        size_of::<Result<std::fs::File, StorageError>>(),
        size_of::<Result<std::fs::File, StorageError>>(),
        size_of::<Result<usize, StorageError>>(),
        size_of::<Result<usize, StorageError>>(),
        size_of::<std::alloc::Layout>(),
        size_of::<std::alloc::Layout>(),
    ];
    let bytes = values.into_iter().try_fold(0usize, checked)?;
    Ok((bytes, values.len()))
}

/// A refused native read retaining every completed payload owner.
#[derive(Debug)]
pub struct StorageFetchReadError {
    pub(crate) failure: StorageFetchFailure,
    pub(crate) batches: Vec<crate::StoredBatch>,
    pub(crate) current: Option<ReadCustody>,
}
impl StorageFetchReadError {
    // Shardlog-only physical owner tests need the actual allocation identity.
    // This range can contain unselected records and is never a public byte view.
    #[cfg(test)]
    pub(crate) fn unpublished_bytes(&self) -> Option<&[u8]> {
        self.current.as_ref().and_then(ReadCustody::bytes)
    }
    /// Length of the retained initialized current buffer, if any.
    /// Coalesced gaps may hold unselected records; no range contents are exposed.
    pub fn unpublished_buffer_len(&self) -> Option<usize> {
        self.current.as_ref().and_then(ReadCustody::buffer_len)
    }
    /// Whether the same actual returned native File is still owned.
    /// No File/FD or retry capability is exposed.
    pub fn has_native_reader(&self) -> bool {
        self.current.as_ref().is_some_and(ReadCustody::has_reader)
    }
    /// Original pack-relative start and requested length of the retained range.
    pub fn read_range(&self) -> Option<(u64, usize)> {
        let owner = self.current.as_ref()?;
        Some((owner.range_start, owner.range_len))
    }
    /// Last actual successful read_at result, even if its postcheck refused.
    /// EOF is the native Ok(0), distinct from the derived UnexpectedEof cause.
    pub fn last_read_bytes(&self) -> Option<usize> {
        self.current
            .as_ref()?
            .read_result
            .as_ref()?
            .as_ref()
            .ok()
            .copied()
    }
    /// Whether an actual read_at was entered in the current retained range.
    pub fn read_entered(&self) -> bool {
        self.current
            .as_ref()
            .is_some_and(|owner| owner.read_entered)
    }
    /// Whether the actual open/read returned Err now owned by the terminal cause.
    pub fn native_error_returned(&self) -> bool {
        self.current
            .as_ref()
            .is_some_and(|owner| owner.open_error_returned || owner.read_error_returned)
    }
    /// Whether the last actual read_at returned a Result before refusal/unwind.
    /// A returned Err may have moved into failure(); it is never reconstructed.
    pub fn read_returned(&self) -> bool {
        self.current
            .as_ref()
            .is_some_and(|owner| owner.read_returned)
    }
    /// Bytes accepted through the original post-read checkpoint in this range.
    pub fn confirmed_read_bytes(&self) -> usize {
        self.current
            .as_ref()
            .map_or(0, |owner| owner.confirmed_bytes)
    }
    /// Original source/native read unwind payload, without formatting or retry.
    pub fn read_panic_payload(&self) -> Option<&(dyn std::any::Any + Send)> {
        match &self.failure {
            StorageFetchFailure::NativeReadUnwind { payload, .. } => Some(payload.as_ref()),
            _ => None,
        }
    }

    /// Actual native deadline/resource/I/O refusal.
    pub fn failure(&self) -> &StorageFetchFailure {
        &self.failure
    }
    /// Every completed native payload and its actual buffer owner.
    pub fn batches(&self) -> &[crate::StoredBatch] {
        &self.batches
    }
}
impl fmt::Display for StorageFetchReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.failure, f)
    }
}
impl Error for StorageFetchReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.failure)
    }
}

#[cfg(all(test, unix))]
#[path = "fetch_limits_checked_tests.rs"]
mod checked_tests;
