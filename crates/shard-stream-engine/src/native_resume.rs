//! Finite recovered-engine continuation work and actual callback outcomes.
//!
//! These counters and observations grant no currentness or serving permission.
//! The consuming native continuation keeps the same EngineHandle through every
//! entered worker/callback/transport completion, including late/error results.

use super::*;
use crate::durable_sink::NativeResumeSinkOutcome;
#[cfg(any(target_os = "linux", target_os = "android"))]
use crate::worker::{GcTicket, NativeGcOutcome, NativeResumeGcPolicy};
use crate::{EngineHandle, NativeFetchCost};
#[cfg(any(target_os = "linux", target_os = "android"))]
use shard_stream_core::owned_pinned_shard_gc_cost;
#[cfg(any(target_os = "linux", target_os = "android"))]
use shardlog::{PinnedShardGcChecks, ShardDirectoryCheckFailure, ShardDirectoryChecks};
use shardlog::{StorageFetchBudget, StorageFetchFailure, StorageFetchUsage};

use std::{any::Any, error::Error, sync::Mutex, time::Instant};

/// Finite cumulative ceilings for one actual recovered-engine continuation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeResumeLimits {
    /// Entered partition passes, including repeated caller partitions.
    pub max_partitions: usize,
    /// Native stored-fetch passes across all partitions, without counter resets.
    pub max_fetch_passes: usize,
    /// Actual retained batches considered across all fetch passes.
    pub max_batches: usize,
    /// Cumulative group observations, including provider/native inspection passes.
    pub max_atomic_groups: usize,
    /// Reserved actual sink/factory callback invocations, including late refusal.
    pub max_callbacks: usize,
    /// Reserved entered worker/transport jobs, including late refused entry.
    pub max_jobs: usize,
    /// Reserved typed native outcome slots, never recycled during this attempt.
    pub max_steps: usize,
    /// Cumulative source-visible native allocation/value slots.
    pub max_entries: usize,
    /// Cumulative source-visible allocation requests, including retained inputs.
    pub max_declared_bytes: usize,
    /// Cumulative explicit native work and entry fees.
    pub max_work_bytes: usize,
    /// One SAME native fetch allowance shared by every replay pass.
    pub fetch: NativeFetchLimits,
}

/// Actual cumulative prepaid continuation fees; these never imply completion.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NativeResumeUsage {
    /// Reserved partition passes.
    pub partitions: usize,
    /// Reserved fetch passes.
    pub fetch_passes: usize,
    /// Actual batch observations admitted for replay.
    pub batches: usize,
    /// Actual group observations admitted by bounded providers/native work.
    pub atomic_groups: usize,
    /// Reserved sink/factory callbacks.
    pub callbacks: usize,
    /// Reserved worker/transport jobs.
    pub jobs: usize,
    /// Reserved typed outcome slots.
    pub steps: usize,
    /// Source-visible allocation/value slots.
    pub entries: usize,
    /// Source-visible logical allocation requests, including String subsets.
    pub declared_bytes: usize,
    /// Path/diagnostic subset already included in declared bytes.
    pub string_bytes: usize,
    /// Source-visible native work, including entry fees.
    pub work_bytes: usize,
}

/// An ordinary cumulative native-work refusal, not an authority decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeResumeRefusal {
    /// The unchanged absolute operation deadline elapsed.
    Expired,
    /// A checked finite allowance or arithmetic calculation refused.
    Capacity,
    /// The same counter owner was poisoned.
    Poisoned,
    /// An earlier sibling refused; no new native phase may enter.
    Denied,
}
impl fmt::Display for NativeResumeRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "native resume work refused: {self:?}")
    }
}
impl Error for NativeResumeRefusal {}

struct State {
    usage: NativeResumeUsage,
    denied: bool,
}
struct Inner {
    expiry: Instant,
    limits: NativeResumeLimits,
    state: Mutex<State>,
    fetch: StorageFetchBudget,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    directory: Option<Arc<DirectoryFetchChecks>>,
    #[cfg(test)]
    probe_observer: Mutex<Option<ProbeObserver>>,
}

#[cfg(any(target_os = "linux", target_os = "android"))]
struct DirectoryFetchChecks {
    directory: Arc<shard_stream_core::NativeDirectoryBudget>,
    first: std::sync::OnceLock<shard_stream_core::NativeDirectoryFailure>,
}
#[cfg(any(target_os = "linux", target_os = "android"))]
impl DirectoryFetchChecks {
    fn cost() -> Result<(usize, usize), NativeResumeRefusal> {
        Ok((add(size_of::<Self>(), size_of::<[usize; 2]>())?, 3))
    }
    fn new(
        directory: Arc<shard_stream_core::NativeDirectoryBudget>,
    ) -> Result<Arc<Self>, shard_stream_core::NativeDirectoryFailure> {
        let (declared, entries) =
            Self::cost().map_err(|_| shard_stream_core::NativeDirectoryFailure::Capacity)?;
        directory.reserve(0, 0, declared, 0, entries, declared)?;
        directory.check()?;
        Ok(Arc::new(Self {
            directory,
            first: std::sync::OnceLock::new(),
        }))
    }
    fn check_native(&self) -> Result<(), NativeResumeRefusal> {
        match self.directory.check() {
            Ok(()) => Ok(()),
            Err(error) => {
                let refusal = match &error {
                    shard_stream_core::NativeDirectoryFailure::Expired(_) => {
                        NativeResumeRefusal::Expired
                    }
                    shard_stream_core::NativeDirectoryFailure::Capacity => {
                        NativeResumeRefusal::Capacity
                    }
                    shard_stream_core::NativeDirectoryFailure::Poisoned => {
                        NativeResumeRefusal::Poisoned
                    }
                    _ => NativeResumeRefusal::Denied,
                };
                let _ = self.first.set(error);
                Err(refusal)
            }
        }
    }
    fn first_failure(&self) -> Option<&shard_stream_core::NativeDirectoryFailure> {
        self.first.get()
    }
}
#[cfg(any(target_os = "linux", target_os = "android"))]
impl ShardDirectoryChecks for DirectoryFetchChecks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        self.check_native()
            .map_err(|_| ShardDirectoryCheckFailure::Refused)
    }
}

#[cfg(test)]
type ProbeObserver = Arc<dyn Fn(&NativeResumeWork, TableProbe) + Send + Sync>;
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProbePhase {
    Before,
    Paid,
    Refused,
}
#[cfg(test)]
#[derive(Clone, Copy, Debug)]
struct TableProbe {
    site: &'static str,
    capacity: usize,
    fee: usize,
    phase: ProbePhase,
}

/// Same-E cumulative ordinary work supplied to actual bounded sink producers.
///
/// This is not admission or an owner/serving witness. Producers must charge
/// their actual source-visible requests before allocation and check this SAME
/// E before/after every physical phase. Entered work must physically settle;
/// returning an error is not permission to leave detached work behind.
#[derive(Clone)]
pub struct NativeResumeWork {
    inner: Arc<Inner>,
}
impl fmt::Debug for NativeResumeWork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeResumeWork")
            .field("expiry", &self.inner.expiry)
            .field("limits", &self.inner.limits)
            .finish_non_exhaustive()
    }
}
#[derive(Default)]
struct Debit {
    partitions: usize,
    fetch_passes: usize,
    batches: usize,
    atomic_groups: usize,
    callbacks: usize,
    jobs: usize,
    steps: usize,
    entries: usize,
    declared: usize,
    strings: usize,
    work: usize,
}
fn add(a: usize, b: usize) -> Result<usize, NativeResumeRefusal> {
    a.checked_add(b).ok_or(NativeResumeRefusal::Capacity)
}
fn mul(a: usize, b: usize) -> Result<usize, NativeResumeRefusal> {
    a.checked_mul(b).ok_or(NativeResumeRefusal::Capacity)
}
// Selected bytes 1.12.1 may allocate one Shared header when cloning a
// promotable Vec-backed Bytes, including a losing promotion CAS. Its fields
// are a buffer pointer, capacity and AtomicUsize. Prepay every possible clone,
// even when this particular buffer is static or already shared. Underlying
// payload ownership is retained separately; allocator metadata/RSS is excluded.
fn bytes_clone_promotion_bytes(count: usize) -> Result<usize, NativeResumeRefusal> {
    mul(
        count,
        size_of::<(*mut u8, usize, std::sync::atomic::AtomicUsize)>(),
    )
}
impl NativeResumeWork {
    // Pay the actual table's full source-visible element-slot ceiling before
    // each point operation. Growth/movement fees are independent; callers read
    // capacity again after any reservation. No private table layout/RSS claim.
    pub(crate) fn reserve_table_probe<T>(
        &self,
        capacity: usize,
        site: &'static str,
    ) -> Result<(), NativeResumeRefusal> {
        let Some(fee) = capacity
            .checked_add(1)
            .and_then(|slots| slots.checked_mul(size_of::<T>().max(1)))
        else {
            self.deny();
            return Err(NativeResumeRefusal::Capacity);
        };
        #[cfg(test)]
        self.observe_probe(TableProbe {
            site,
            capacity,
            fee,
            phase: ProbePhase::Before,
        });
        #[cfg(not(test))]
        let _ = site;
        let observed = self.reserve_values(0, 0, 0, fee);
        #[cfg(test)]
        self.observe_probe(TableProbe {
            site,
            capacity,
            fee,
            phase: if observed.is_ok() {
                ProbePhase::Paid
            } else {
                ProbePhase::Refused
            },
        });
        observed?;
        self.check()
    }
    #[cfg(test)]
    fn observe_probe(&self, probe: TableProbe) {
        let observer = self
            .inner
            .probe_observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(observer) = observer {
            observer(self, probe);
        }
    }
    /// The original absolute deadline, capped only by the SAME directory owner.
    /// No duration is refreshed and later calls cannot extend its minimum.
    pub fn absolute_expiry(&self) -> Instant {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(directory) = &self.inner.directory {
            return self.inner.expiry.min(directory.directory.absolute_expiry());
        }
        self.inner.expiry
    }
    /// The original immutable cumulative allowances.
    pub fn limits(&self) -> NativeResumeLimits {
        self.inner.limits
    }
    /// Observe actual cumulative prepaid fees, including unused late-entry fees.
    ///
    /// # Errors
    /// Returns `Poisoned` if the shared counter mutex was poisoned.
    pub fn usage(&self) -> Result<NativeResumeUsage, NativeResumeRefusal> {
        self.inner
            .state
            .lock()
            .map(|state| state.usage)
            .map_err(|_| NativeResumeRefusal::Poisoned)
    }
    /// Check the SAME original E and sticky refusal state before a new phase.
    ///
    /// # Errors
    /// Returns the original expiry, poisoned-counter or sticky refusal. No
    /// resources or deadline are refreshed by this check.
    pub fn check(&self) -> Result<(), NativeResumeRefusal> {
        self.charge(Debit::default())
    }
    /// Pay actual source-visible value/path/work requests before producing them.
    ///
    /// String bytes are a subset of declared bytes. This ordinary accounting
    /// operation supplies no permission to perform I/O or publish results.
    ///
    /// # Errors
    /// Refuses checked arithmetic, an exceeded cumulative ceiling, a String
    /// subset larger than declared bytes, original expiry or prior denial.
    pub fn reserve_values(
        &self,
        declared_bytes: usize,
        string_bytes: usize,
        entries: usize,
        work_bytes: usize,
    ) -> Result<(), NativeResumeRefusal> {
        if string_bytes > declared_bytes {
            self.deny();
            return Err(NativeResumeRefusal::Capacity);
        }
        self.charge(Debit {
            declared: declared_bytes,
            strings: string_bytes,
            entries,
            work: work_bytes,
            ..Debit::default()
        })
    }
    /// Pay a finite group observation before a bounded provider copies it.
    ///
    /// # Errors
    /// Refuses the cumulative group ceiling, checked arithmetic, original
    /// expiry, poisoned counters or a prior sibling refusal.
    pub fn reserve_groups(&self, groups: usize) -> Result<(), NativeResumeRefusal> {
        self.charge(Debit {
            atomic_groups: groups,
            ..Debit::default()
        })
    }
    /// Reserve one worker or transport job before attempting entry.
    ///
    /// This spends the SAME original job allowance and expiry. Queue values,
    /// allocations and work require their own reservations; entered jobs must
    /// still physically settle before the producer releases their owners.
    ///
    /// # Errors
    /// Refuses the cumulative job ceiling, checked arithmetic, original
    /// expiry, poisoned counters or a prior sibling refusal.
    pub fn reserve_job(&self) -> Result<(), NativeResumeRefusal> {
        self.charge(Debit {
            jobs: 1,
            ..Debit::default()
        })
    }
    pub(crate) fn reserve_capacity_excess<T>(
        &self,
        actual_capacity: usize,
        prepaid_capacity: usize,
    ) -> Result<(), NativeResumeRefusal> {
        let extra = actual_capacity.saturating_sub(prepaid_capacity);
        let bytes = mul(extra, size_of::<T>())?;
        self.reserve_values(bytes, 0, extra, bytes)
    }
    pub(crate) fn reserve_command(
        &self,
        declared: usize,
        strings: usize,
        entries: usize,
        steps: usize,
    ) -> Result<(), NativeResumeRefusal> {
        self.charge(Debit {
            jobs: 1,
            steps,
            declared,
            strings,
            entries,
            work: declared,
            ..Debit::default()
        })
    }
    pub(crate) fn reserve_callback(&self) -> Result<(), NativeResumeRefusal> {
        self.charge(Debit {
            callbacks: 1,
            ..Debit::default()
        })
    }
    fn reserve_outcomes(&self, slots: usize) -> Result<(), NativeResumeRefusal> {
        self.charge(Debit {
            steps: slots,
            entries: slots,
            ..Debit::default()
        })
    }
    fn charge(&self, debit: Debit) -> Result<(), NativeResumeRefusal> {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(directory) = &self.inner.directory
            && let Err(error) = directory.check_native()
        {
            self.inner.fetch.deny();
            return Err(error);
        }
        if Instant::now() >= self.absolute_expiry() {
            return Err(NativeResumeRefusal::Expired);
        }
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| NativeResumeRefusal::Poisoned)?;
        if Instant::now() >= self.absolute_expiry() {
            state.denied = true;
            return Err(NativeResumeRefusal::Expired);
        }
        if state.denied {
            return Err(NativeResumeRefusal::Denied);
        }
        let next = (|| {
            let old = state.usage;
            let next = NativeResumeUsage {
                partitions: add(old.partitions, debit.partitions)?,
                fetch_passes: add(old.fetch_passes, debit.fetch_passes)?,
                batches: add(old.batches, debit.batches)?,
                atomic_groups: add(old.atomic_groups, debit.atomic_groups)?,
                callbacks: add(old.callbacks, debit.callbacks)?,
                jobs: add(old.jobs, debit.jobs)?,
                steps: add(old.steps, debit.steps)?,
                entries: add(old.entries, debit.entries)?,
                declared_bytes: add(old.declared_bytes, debit.declared)?,
                string_bytes: add(old.string_bytes, debit.strings)?,
                work_bytes: add(old.work_bytes, add(debit.work, debit.entries)?)?,
            };
            let l = self.inner.limits;
            if next.partitions > l.max_partitions
                || next.fetch_passes > l.max_fetch_passes
                || next.batches > l.max_batches
                || next.atomic_groups > l.max_atomic_groups
                || next.callbacks > l.max_callbacks
                || next.jobs > l.max_jobs
                || next.steps > l.max_steps
                || next.entries > l.max_entries
                || next.declared_bytes > l.max_declared_bytes
                || next.work_bytes > l.max_work_bytes
                || next.string_bytes > next.declared_bytes
            {
                return Err(NativeResumeRefusal::Capacity);
            }
            Ok(next)
        })();
        match next {
            Ok(next) => {
                state.usage = next;
                Ok(())
            }
            Err(error) => {
                state.denied = true;
                Err(error)
            }
        }
    }
    pub(crate) fn deny(&self) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .denied = true;
        self.inner.fetch.deny();
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(directory) = &self.inner.directory {
            directory.directory.deny();
        }
    }
}

enum CallbackCause {
    Native(EngineError),
    External(Box<dyn Error + Send>),
    Unsupported(&'static str),
    QueueFull(&'static str),
    WorkerStopped(&'static str),
    Panic(Box<dyn Any + Send>),
    Refused(NativeResumeRefusal),
    Allocation(std::collections::TryReserveError),
    InvalidState(&'static str),
}

/// Actual strict callback refusal, retaining its original typed native cause.
///
/// A callback returning this must already have settled its entered physical
/// work. An entered pending job must instead remain owned by its actual ticket.
pub struct NativeResumeCallbackError {
    cause: CallbackCause,
}
impl NativeResumeCallbackError {
    /// Retain an already-produced actual engine refusal without reformatting it.
    pub fn from_engine(error: EngineError) -> Self {
        Self {
            cause: CallbackCause::Native(error),
        }
    }
    /// Retain an already-owned bounded producer error, including partial owners.
    pub fn from_external(error: Box<dyn Error + Send>) -> Self {
        Self {
            cause: CallbackCause::External(error),
        }
    }
    /// Refuse a producer that supplies no strict implementation; never success.
    pub fn unsupported(reason: &'static str) -> Self {
        Self {
            cause: CallbackCause::Unsupported(reason),
        }
    }
    /// Preserve a refusal from these same cumulative ordinary counters.
    pub fn refused(error: NativeResumeRefusal) -> Self {
        Self {
            cause: CallbackCause::Refused(error),
        }
    }
    pub(crate) fn queue_full(component: &'static str) -> Self {
        Self {
            cause: CallbackCause::QueueFull(component),
        }
    }
    pub(crate) fn worker_stopped(component: &'static str) -> Self {
        Self {
            cause: CallbackCause::WorkerStopped(component),
        }
    }
    pub(crate) fn from_panic(payload: Box<dyn Any + Send>) -> Self {
        Self {
            cause: CallbackCause::Panic(payload),
        }
    }
    pub(crate) fn from_allocation(error: std::collections::TryReserveError) -> Self {
        Self {
            cause: CallbackCause::Allocation(error),
        }
    }
    pub(crate) fn invalid_state(reason: &'static str) -> Self {
        Self {
            cause: CallbackCause::InvalidState(reason),
        }
    }
    /// Borrow the actual fallible native allocation error.
    pub fn allocation_error(&self) -> Option<&std::collections::TryReserveError> {
        match &self.cause {
            CallbackCause::Allocation(error) => Some(error),
            _ => None,
        }
    }
    /// Borrow the actual native engine error, if the producer returned one.
    pub fn engine_error(&self) -> Option<&EngineError> {
        match &self.cause {
            CallbackCause::Native(error) => Some(error),
            _ => None,
        }
    }
    /// Borrow the actual retained bounded producer error.
    pub fn external_error(&self) -> Option<&(dyn Error + Send + 'static)> {
        match &self.cause {
            CallbackCause::External(error) => Some(error.as_ref()),
            _ => None,
        }
    }
    /// Borrow the actual panic payload; no modeled completion is substituted.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        match &self.cause {
            CallbackCause::Panic(payload) => Some(payload.as_ref()),
            _ => None,
        }
    }
}
impl fmt::Debug for NativeResumeCallbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.cause {
            CallbackCause::Native(error) => f.debug_tuple("Native").field(error).finish(),
            CallbackCause::External(error) => f.debug_tuple("External").field(error).finish(),
            CallbackCause::Unsupported(reason) => {
                f.debug_tuple("Unsupported").field(reason).finish()
            }
            CallbackCause::QueueFull(component) => {
                f.debug_tuple("QueueFull").field(component).finish()
            }
            CallbackCause::WorkerStopped(component) => {
                f.debug_tuple("WorkerStopped").field(component).finish()
            }
            CallbackCause::Panic(_) => f.write_str("Panic(actual retained payload)"),
            CallbackCause::Refused(error) => f.debug_tuple("Refused").field(error).finish(),
            CallbackCause::Allocation(error) => f.debug_tuple("Allocation").field(error).finish(),
            CallbackCause::InvalidState(reason) => {
                f.debug_tuple("InvalidState").field(reason).finish()
            }
        }
    }
}
impl fmt::Display for NativeResumeCallbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Error for NativeResumeCallbackError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.cause {
            CallbackCause::Native(error) => Some(error),
            CallbackCause::External(error) => Some(error.as_ref()),
            CallbackCause::Refused(error) => Some(error),
            CallbackCause::Allocation(error) => Some(error),
            _ => None,
        }
    }
}
/// Actual strict callback result; this carries data/errors, never authority.
pub type NativeResumeCallbackResult<T> = Result<T, NativeResumeCallbackError>;

/// The actual typed result saved before observing the post-phase deadline.
///
/// A successful native effect can be present together with a late refusal.
/// Neither presence nor a success value grants currentness or serving access.
#[derive(Debug)]
pub struct NativeResumeObservation<T> {
    pub(crate) result: NativeResumeCallbackResult<T>,
    pub(crate) refusal: Option<NativeResumeRefusal>,
}
impl<T> NativeResumeObservation<T> {
    /// Borrow the actual completed result, including original native causes.
    pub fn result(&self) -> &NativeResumeCallbackResult<T> {
        &self.result
    }
    /// A deadline/counter refusal observed AFTER that actual result was saved.
    pub fn refusal(&self) -> Option<NativeResumeRefusal> {
        self.refusal
    }
    pub(crate) fn completed(
        result: NativeResumeCallbackResult<T>,
        work: &NativeResumeWork,
    ) -> Self {
        let mut observed = Self {
            result,
            refusal: None,
        };
        observed.refusal = work.check().err();
        if observed.refusal.is_some() || observed.result.is_err() {
            work.deny();
        }
        observed
    }
    pub(crate) fn refused(error: NativeResumeRefusal) -> Self {
        Self {
            result: Err(NativeResumeCallbackError::refused(error)),
            refusal: Some(error),
        }
    }
}

#[derive(Debug)]
pub(crate) enum CoordinatorValue {
    Watermarks(PartitionWatermarks),
    AtomicGroup(AtomicGroupInspection),
}

/// Plain source-visible setup and cumulative continuation ceilings.
///
/// Includes the SAME fetch budget, typed outcome slots and retained native
/// input values. Provider-specific native allocations must use the supplied
/// work or independently admitted real provider ceilings. This is not an
/// allocator/kernel/RSS bound or admission/serving witness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeResumeCost {
    /// Complete logical source setup and cumulative requested byte ceilings.
    pub declared_bytes: usize,
    /// Source-visible setup plus cumulative entry and outcome ceilings.
    pub entries: usize,
    /// String/path subset already included in declared bytes.
    pub string_bytes: usize,
    /// Complete logical source setup, native work and entry fees.
    pub work_bytes: usize,
}

/// An actual native continuation refusal; retained data is in its same owner.
#[derive(Debug)]
pub enum NativeResumeFailure {
    /// A refusal from the SAME cumulative original-E counters.
    Work(NativeResumeRefusal),
    /// Actual checked native engine/configuration refusal.
    Engine(EngineError),
    /// Actual fetch admission/refusal; the full child error is retained.
    Fetch,
    /// Actual callback or coordinator observation refused; raw outcomes remain.
    Callback,
    /// Actual recovered transport entry/completion refused.
    Replication,
    /// The immutable native transport was absent at actual dispatch.
    MissingTransport,
    /// The immutable configured sink was absent at actual dispatch.
    MissingSink,
    /// Actual finite outcome/input vector reservation failed.
    Allocation,
    /// Actual owning replay frame unwound; its original payload is retained.
    Unwind,
    /// Actual native fetch setup/cost refusal, without replacing its error.
    FetchAdmission(NativeFetchFailure),
    /// Original E elapsed while deriving plain initial costs or setup.
    Expired,
}
impl From<NativeResumeRefusal> for NativeResumeFailure {
    fn from(error: NativeResumeRefusal) -> Self {
        Self::Work(error)
    }
}
impl From<EngineError> for NativeResumeFailure {
    fn from(error: EngineError) -> Self {
        Self::Engine(error)
    }
}
impl fmt::Display for NativeResumeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "native resume refused: {self:?}")
    }
}
impl Error for NativeResumeFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Work(error) => Some(error),
            Self::Engine(error) => Some(error),
            Self::FetchAdmission(error) => Some(error),
            _ => None,
        }
    }
}

/// Actual native coordinator data and its completed-phase result.
#[derive(Debug)]
pub struct NativeResumeCoordinatorOutcome {
    partition: TopicPartition,
    observed: NativeResumeObservation<CoordinatorValue>,
}
impl NativeResumeCoordinatorOutcome {
    /// Actual queried native partition.
    pub fn partition(&self) -> TopicPartition {
        self.partition
    }
    /// Actual completed watermarks, even if the subsequent clock refused.
    pub fn watermarks(&self) -> Option<&PartitionWatermarks> {
        match &self.observed.result {
            Ok(CoordinatorValue::Watermarks(value)) => Some(value),
            _ => None,
        }
    }
    /// Actual completed native atomic-group inspection.
    pub fn atomic_group(&self) -> Option<&AtomicGroupInspection> {
        match &self.observed.result {
            Ok(CoordinatorValue::AtomicGroup(value)) => Some(value),
            _ => None,
        }
    }
    /// Original typed coordinator/queue refusal.
    pub fn error(&self) -> Option<&NativeResumeCallbackError> {
        self.observed.result.as_ref().err()
    }
    /// Same-E/counter refusal observed after the native result was saved.
    pub fn refusal(&self) -> Option<NativeResumeRefusal> {
        self.observed.refusal
    }
}

/// Actual native cache phase selected by strict sink replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeResumeSinkCacheOperation {
    /// Retire this partition's actual dirty marker.
    ClearDirty(TopicPartition),
    /// Observe the real checkpoint after physically completed strict applies.
    Checkpoint(DurableSinkCheckpoint),
    /// Observe the real decision cache after strict finalization.
    Decision(DurableAtomicGroup),
}

/// Retained actual native replay observations; none imply serving authority.
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "retain actual GC store and result custody inline under the existing outcome prepayment"
)]
pub enum NativeResumeOutcome {
    /// Actual finite native cache mutation/observation, retained before post-E.
    SinkCache {
        /// Exact native operation and its real target.
        operation: NativeResumeSinkCacheOperation,
        /// Actual state result, separately retaining subsequent work refusal.
        observed: NativeResumeObservation<()>,
    },
    /// Actual same-lane consuming Core GC completion or retained refusal.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    GarbageCollected {
        /// Actual physical lane receiving this native operation.
        shard: ShardId,
        /// Actual command receive, report/error and physical owner custody.
        result: Result<NativeGcOutcome, std::sync::mpsc::RecvError>,
        /// SAME-work refusal observed only after retaining actual completion.
        refusal: Option<NativeResumeRefusal>,
    },
    /// Actual bounded same-worker sink callback and separate cache observation.
    Sink {
        /// Actual native input partition, absent only for global pending groups.
        partition: Option<TopicPartition>,
        /// Actual atomic decision input, when that command was selected.
        decision: Option<DurableAtomicGroup>,
        /// Original callback data/error saved before post-E/native publication.
        observed: NativeResumeSinkOutcome,
    },
    /// Actual same-worker native coordinator observation.
    Coordinator(NativeResumeCoordinatorOutcome),
    /// Actual transport start result; success means a real ticket was retained.
    ReplicationStarted {
        /// Actual native batch identity.
        batch_id: BatchId,
        /// Start result saved before its post-E observation.
        result: NativeResumeCallbackResult<()>,
        /// Refusal observed after the real start result/ticket were retained.
        refusal: Option<NativeResumeRefusal>,
    },
    /// Actual ticket receive result after physical transport settlement.
    ReplicationCompleted {
        /// Actual native batch identity.
        batch_id: BatchId,
        /// Original transport result or actual completion-channel disconnection.
        result: Result<EngineResult<u32>, std::sync::mpsc::RecvError>,
        /// Refusal observed after the original typed result was saved.
        refusal: Option<NativeResumeRefusal>,
    },
}

/// Completed ordinary native replay data and its actual cumulative fees.
#[derive(Debug)]
pub struct NativeResumeReport {
    outcomes: Vec<NativeResumeOutcome>,
    usage: NativeResumeUsage,
    fetch_usage: StorageFetchUsage,
}
impl NativeResumeReport {
    /// All actual entered coordinator/callback/transport observations.
    pub fn outcomes(&self) -> &[NativeResumeOutcome] {
        &self.outcomes
    }
    /// Same original-E continuation counters without reset or refund.
    pub fn usage(&self) -> NativeResumeUsage {
        self.usage
    }
    /// The SAME cumulative native fetch counters over all replay passes.
    pub fn fetch_usage(&self) -> StorageFetchUsage {
        self.fetch_usage
    }
}

struct Scope {
    work: NativeResumeWork,
    outcomes: Vec<NativeResumeOutcome>,
    buffered: VecDeque<StoredBatch>,
    fetched_copy: Vec<StoredBatch>,
    recovered_decisions: HashMap<DurableAtomicGroup, DurableAtomicDecision>,
    pending_groups: Vec<DurableAtomicGroup>,
    pending_decisions: Vec<DurableAtomicGroup>,
    current_replica: Option<ReplicaAppend>,
    current_append: Option<DurableAppend>,
    fetch_error: Option<RetainedNativeFetchError>,
    setup_allocation_error: Option<std::collections::TryReserveError>,
    panic_payload: Option<Box<dyn Any + Send>>,
    pending_replication: Option<(BatchId, ReplicationTicket)>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pending_gc: Option<(ShardId, GcTicket)>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    gc_policy: Option<NativeResumeGcPolicy>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    gc_actual_policy: Option<NativeResumeGcPolicy>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    gc_deny_panic: Option<Box<dyn Any + Send>>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    gc_budget_error: Option<shard_stream_core::NativeDirectoryFailure>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    gc_preentry_denial_attempted: bool,
    // Last: all native jobs, typed outcomes and payload owners precede custody.
    owner: EngineHandle,
}
impl Scope {
    fn new(owner: EngineHandle, work: NativeResumeWork) -> Self {
        Self {
            work,
            outcomes: Vec::new(),
            buffered: VecDeque::new(),
            fetched_copy: Vec::new(),
            recovered_decisions: HashMap::new(),
            pending_groups: Vec::new(),
            pending_decisions: Vec::new(),
            current_replica: None,
            current_append: None,
            fetch_error: None,
            setup_allocation_error: None,
            panic_payload: None,
            pending_replication: None,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            pending_gc: None,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            gc_policy: None,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            gc_actual_policy: None,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            gc_deny_panic: None,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            gc_budget_error: None,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            gc_preentry_denial_attempted: false,
            owner,
        }
    }
    fn fail(mut self: Box<Self>, failure: NativeResumeFailure) -> RetainedNativeResumeError {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if self.work.inner.directory.is_some() || self.gc_budget_error.is_none() {
            let _ = self.finish_directory();
        }
        self.work.deny();
        self.drain_replication();
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            self.deny_gc();
            self.drain_gc();
        }
        RetainedNativeResumeError {
            failure,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            gc_early: None,
            custody: Custody::Entered(self),
        }
    }
    fn drain_replication(&mut self) {
        // Borrow the stored ticket across the actual blocking receive. A panic
        // before receive completes cannot take/drop the sole ticket first.
        if let Some((batch_id, ticket)) = self.pending_replication.as_ref() {
            let batch_id = *batch_id;
            let result = ticket.receiver.recv();
            self.outcomes
                .push(NativeResumeOutcome::ReplicationCompleted {
                    batch_id,
                    result,
                    refusal: None,
                });
            self.pending_replication.take();
            let refusal = self.work.check().err();
            if let Some(NativeResumeOutcome::ReplicationCompleted { refusal: saved, .. }) =
                self.outcomes.last_mut()
            {
                *saved = refusal;
            }
        }
    }
    fn watermarks(
        &mut self,
        partition: TopicPartition,
    ) -> Result<PartitionWatermarks, NativeResumeFailure> {
        self.work.reserve_outcomes(1)?;
        self.work.check()?;
        let observed = self
            .owner
            .coordinators
            .resume_watermarks(partition, &self.work);
        let value = match (&observed.result, observed.refusal) {
            (Ok(CoordinatorValue::Watermarks(value)), None) => Some(*value),
            _ => None,
        };
        self.outcomes.push(NativeResumeOutcome::Coordinator(
            NativeResumeCoordinatorOutcome {
                partition,
                observed,
            },
        ));
        value.ok_or(NativeResumeFailure::Callback)
    }
    fn inspect_group(
        &mut self,
        group: DurableAtomicGroup,
    ) -> Result<AtomicGroupInspection, NativeResumeFailure> {
        self.work.reserve_groups(1)?;
        self.work.reserve_outcomes(1)?;
        self.work.check()?;
        let observed = self.owner.coordinators.resume_atomic_group(
            group.topic_partition,
            group.transaction_id,
            &self.work,
        );
        let value = match (&observed.result, observed.refusal) {
            (Ok(CoordinatorValue::AtomicGroup(value)), None) => Some(*value),
            _ => None,
        };
        self.outcomes.push(NativeResumeOutcome::Coordinator(
            NativeResumeCoordinatorOutcome {
                partition: group.topic_partition,
                observed,
            },
        ));
        value.ok_or(NativeResumeFailure::Callback)
    }
    fn sink_checkpoint(
        &mut self,
        partition: TopicPartition,
    ) -> Result<DurableSinkCheckpoint, NativeResumeFailure> {
        self.work.reserve_outcomes(1)?;
        self.work.check()?;
        let Some(dispatcher) = self.owner.durable_sink.as_ref() else {
            return Err(NativeResumeFailure::MissingSink);
        };
        let observed = dispatcher.resume_checkpoint(partition, &self.work);
        let checkpoint = if observed.accepted() {
            observed
                .checkpoint()
                .map(|value| value.unwrap_or_else(|| DurableSinkCheckpoint::initial(partition)))
        } else {
            None
        };
        self.outcomes.push(NativeResumeOutcome::Sink {
            partition: Some(partition),
            decision: None,
            observed,
        });
        self.work.check()?;
        checkpoint.ok_or(NativeResumeFailure::Callback)
    }
    fn sink_append(
        &mut self,
        expected: DurableSinkCheckpoint,
        append: DurableAppend,
        next: DurableSinkCheckpoint,
    ) -> Result<(), NativeResumeFailure> {
        // Move the exact owned input into outside custody before any fee or
        // clock can refuse. Only the already-paid clone enters the dispatcher.
        self.current_append = Some(append);
        self.work.reserve_outcomes(1)?;
        let current = self
            .current_append
            .as_ref()
            .ok_or(NativeResumeFailure::MissingSink)?;
        let buffers = 1 + usize::from(current.transient_context.is_some());
        let values = add(
            size_of::<DurableAppend>() * 2,
            bytes_clone_promotion_bytes(buffers)?,
        )?;
        self.work
            .reserve_values(values, 0, add(2, buffers)?, values)?;
        self.work.check()?;
        let append = current.clone();
        self.work.check()?;
        let Some(dispatcher) = self.owner.durable_sink.as_ref() else {
            return Err(NativeResumeFailure::MissingSink);
        };
        let observed = dispatcher.resume_apply(expected, append, next, &self.work);
        let accepted = observed.accepted();
        self.outcomes.push(NativeResumeOutcome::Sink {
            partition: Some(expected.topic_partition),
            decision: None,
            observed,
        });
        self.work.check()?;
        if !accepted {
            return Err(NativeResumeFailure::Callback);
        }
        self.current_append.take();
        Ok(())
    }
    fn sink_decision(
        &mut self,
        decision: DurableAtomicDecision,
    ) -> Result<(), NativeResumeFailure> {
        self.work.reserve_outcomes(1)?;
        self.work.check()?;
        let Some(dispatcher) = self.owner.durable_sink.as_ref() else {
            return Err(NativeResumeFailure::MissingSink);
        };
        let observed = dispatcher.resume_decision(decision, &self.work);
        let accepted = observed.accepted();
        self.outcomes.push(NativeResumeOutcome::Sink {
            partition: Some(decision.group.topic_partition),
            decision: Some(decision.group),
            observed,
        });
        self.work.check()?;
        if accepted {
            Ok(())
        } else {
            Err(NativeResumeFailure::Callback)
        }
    }
    fn sink_pending(&mut self) -> Result<Vec<DurableAtomicGroup>, NativeResumeFailure> {
        self.work.reserve_outcomes(1)?;
        self.work.check()?;
        let Some(dispatcher) = self.owner.durable_sink.as_ref() else {
            return Err(NativeResumeFailure::MissingSink);
        };
        let observed = dispatcher.resume_pending(&self.work);
        let accepted = observed.accepted();
        self.outcomes.push(NativeResumeOutcome::Sink {
            partition: None,
            decision: None,
            observed,
        });
        self.work.check()?;
        if !accepted {
            return Err(NativeResumeFailure::Callback);
        }
        let Some(NativeResumeOutcome::Sink { observed, .. }) = self.outcomes.last() else {
            return Err(NativeResumeFailure::Callback);
        };
        let groups = observed
            .pending_groups()
            .ok_or(NativeResumeFailure::Callback)?;
        let count = groups.len();
        let bytes = add(
            mul(count, size_of::<DurableAtomicGroup>())?,
            size_of::<Vec<DurableAtomicGroup>>(),
        )?;
        self.work.reserve_groups(count)?;
        self.work.reserve_values(bytes, 0, add(count, 1)?, bytes)?;
        self.work.check()?;
        self.pending_groups = Vec::new();
        self.setup_allocation_error = self.pending_groups.try_reserve_exact(count).err();
        self.work.check()?;
        if self.setup_allocation_error.is_some() {
            return Err(NativeResumeFailure::Allocation);
        }
        self.work
            .reserve_capacity_excess::<DurableAtomicGroup>(self.pending_groups.capacity(), count)?;
        for group in groups {
            self.work.check()?;
            self.pending_groups.push(*group);
            self.work.check()?;
        }
        self.work.check()?;
        Ok(std::mem::take(&mut self.pending_groups))
    }
    fn fetch_stored(
        &mut self,
        partition: TopicPartition,
        offset: LogicalOffset,
        max_bytes: u32,
    ) -> Result<(), NativeResumeFailure> {
        self.work.charge(Debit {
            fetch_passes: 1,
            ..Debit::default()
        })?;
        match super::bounded_fetch::stored_with_budget(
            &self.owner,
            FetchRequest {
                request_id: 0,
                topic_id: partition.topic_id,
                partition_id: partition.partition_id,
                start_offset: offset,
                max_bytes,
                mode: FetchMode::Ordered,
            },
            self.work.limits().fetch,
            &self.work.inner.fetch,
        ) {
            Ok(batches) => self.buffered = batches,
            Err(error) => {
                self.fetch_error = Some(error);
                return Err(NativeResumeFailure::Fetch);
            }
        }
        self.work.check()?;
        Ok(())
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if self.pending_gc.is_some() {
            self.deny_gc();
            self.drain_gc();
        }
        if self.pending_replication.is_some() {
            self.work.deny();
            self.drain_replication();
        }
    }
}
enum Custody {
    Early(EngineHandle),
    Entered(Box<Scope>),
}

/// Refusal retaining the SAME recovered engine and actual entered native work.
///
/// Every pending ticket is drained before normal return or Scope destruction.
/// A producer that violates its physical-completion contract is never qualified
/// by a channel value, and no enqueue/fixture becomes serving permission.
pub struct RetainedNativeResumeError {
    failure: NativeResumeFailure,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    gc_early: Option<gc::Early>,
    custody: Custody,
}
impl fmt::Debug for RetainedNativeResumeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedNativeResumeError")
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedNativeResumeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.failure, f)
    }
}
impl Error for RetainedNativeResumeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.failure)
    }
}
impl RetainedNativeResumeError {
    /// Actual continuation refusal, with original typed observations retained.
    pub fn failure(&self) -> &NativeResumeFailure {
        &self.failure
    }
    /// Borrow the SAME actual strong engine without raw Arc extraction.
    pub fn engine_handle(&self) -> &EngineHandle {
        match &self.custody {
            Custody::Early(owner) => owner,
            Custody::Entered(scope) => &scope.owner,
        }
    }
    /// Every actual typed native observation completed before refusal.
    pub fn outcomes(&self) -> &[NativeResumeOutcome] {
        match &self.custody {
            Custody::Early(_) => &[],
            Custody::Entered(scope) => &scope.outcomes,
        }
    }
    /// Full actual partial fetch error, including completed body owners.
    pub fn fetch_error(&self) -> Option<&RetainedNativeFetchError> {
        match &self.custody {
            Custody::Early(_) => None,
            Custody::Entered(scope) => scope.fetch_error.as_ref(),
        }
    }
    /// Original setup reservation error, including a subsequent late refusal.
    pub fn allocation_error(&self) -> Option<&std::collections::TryReserveError> {
        match &self.custody {
            Custody::Early(_) => None,
            Custody::Entered(scope) => scope.setup_allocation_error.as_ref(),
        }
    }
    /// Original owning-frame unwind payload, if actual replay unwound.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        match &self.custody {
            Custody::Early(_) => None,
            Custody::Entered(scope) => scope.panic_payload.as_deref(),
        }
    }
    /// Actual same-owner cumulative work fees.
    pub fn usage(&self) -> Result<NativeResumeUsage, NativeResumeRefusal> {
        match &self.custody {
            Custody::Early(_) => Ok(NativeResumeUsage::default()),
            Custody::Entered(scope) => scope.work.usage(),
        }
    }
    /// Actual SAME native fetch fees across all entered replay passes.
    pub fn fetch_usage(&self) -> Result<StorageFetchUsage, StorageFetchFailure> {
        match &self.custody {
            Custody::Early(_) => Ok(StorageFetchUsage::default()),
            Custody::Entered(scope) => scope.work.inner.fetch.usage(),
        }
    }
    /// Borrow actual current replay payloads retained through refusal.
    pub fn retained_payloads(&self) -> impl Iterator<Item = &[u8]> {
        let scope = match &self.custody {
            Custody::Early(_) => None,
            Custody::Entered(scope) => Some(scope.as_ref()),
        };
        scope.into_iter().flat_map(|scope| {
            scope
                .buffered
                .iter()
                .map(|batch| batch.payload.as_ref())
                .chain(
                    scope
                        .current_replica
                        .iter()
                        .map(|batch| batch.payload.as_ref()),
                )
                .chain(
                    scope
                        .current_append
                        .iter()
                        .map(|append| append.payload.as_ref()),
                )
        })
    }
}

/// Derive checked ordinary costs before constructing any native resume owner.
///
/// # Errors
/// Returns the actual native fetch/configuration refusal or checked capacity
/// overflow. It does not allocate a resume owner or admit work.
pub fn native_resume_cost(
    config: &EngineConfig,
    limits: NativeResumeLimits,
) -> Result<NativeResumeCost, NativeResumeFailure> {
    let fetch: NativeFetchCost =
        native_fetch_cost(config, limits.fetch).map_err(NativeResumeFailure::FetchAdmission)?;
    let fixed = [
        size_of::<Inner>(),
        size_of::<NativeResumeWork>(),
        size_of::<Scope>(),
        size_of::<RetainedNativeResumeError>(),
        size_of::<NativeResumeCost>(),
        size_of::<NativeResumeReport>(),
        size_of::<[usize; 2]>(),
    ]
    .into_iter()
    .try_fold(0usize, add)?;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let fixed = add(fixed, DirectoryFetchChecks::cost()?.0)?;
    let outcomes = mul(limits.max_steps, size_of::<NativeResumeOutcome>())?;
    let native_declared = add(add(fixed, outcomes)?, limits.max_declared_bytes)?;
    let native_entries = add(add(limits.max_entries, limits.max_steps)?, 8)?;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let native_entries = add(native_entries, DirectoryFetchChecks::cost()?.1)?;
    Ok(NativeResumeCost {
        declared_bytes: add(native_declared, fetch.declared_bytes)?,
        entries: add(native_entries, fetch.entries)?,
        string_bytes: add(limits.max_declared_bytes, fetch.string_bytes)?,
        work_bytes: add(
            add(add(native_declared, native_entries)?, limits.max_work_bytes)?,
            fetch.work_bytes,
        )?,
    })
}

#[allow(
    clippy::result_large_err,
    reason = "retain the original engine and early refusal without a new allocation"
)]
fn start(
    owner: &EngineHandle,
    limits: NativeResumeLimits,
    expiry: Instant,
) -> Result<Box<Scope>, RetainedNativeResumeError> {
    start_inner(
        owner,
        limits,
        expiry,
        #[cfg(any(target_os = "linux", target_os = "android"))]
        None,
    )
}

#[allow(
    clippy::result_large_err,
    reason = "retain the same engine and early refusal without a new allocation"
)]
fn start_inner(
    owner: &EngineHandle,
    limits: NativeResumeLimits,
    expiry: Instant,
    #[cfg(any(target_os = "linux", target_os = "android"))] directory: Option<
        Arc<DirectoryFetchChecks>,
    >,
) -> Result<Box<Scope>, RetainedNativeResumeError> {
    let early = |failure| RetainedNativeResumeError {
        failure,
        #[cfg(any(target_os = "linux", target_os = "android"))]
        gc_early: None,
        custody: Custody::Early(owner.clone()),
    };
    if Instant::now() >= expiry {
        return Err(early(NativeResumeFailure::Expired));
    }
    let measured = native_resume_cost(&owner.config, limits);
    if Instant::now() >= expiry {
        return Err(early(NativeResumeFailure::Expired));
    }
    measured.map_err(&early)?;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let fetch = if let Some(checks) = &directory {
        let shared: Arc<dyn ShardDirectoryChecks + Send + Sync> =
            Arc::clone(checks) as Arc<dyn ShardDirectoryChecks + Send + Sync>;
        StorageFetchBudget::new_with_checks(limits.fetch.storage(), expiry, shared)
    } else {
        StorageFetchBudget::new(limits.fetch.storage(), expiry)
    };
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let fetch = StorageFetchBudget::new(limits.fetch.storage(), expiry);
    let fetch = fetch.map_err(|error| early(NativeResumeFailure::FetchAdmission(error.into())))?;
    if Instant::now() >= expiry {
        return Err(early(NativeResumeFailure::Expired));
    }
    let work = NativeResumeWork {
        inner: Arc::new(Inner {
            expiry,
            limits,
            state: Mutex::new(State {
                usage: NativeResumeUsage::default(),
                denied: false,
            }),
            fetch,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            directory,
            #[cfg(test)]
            probe_observer: Mutex::new(None),
        }),
    };
    work.check().map_err(|error| early(error.into()))?;
    let mut scope = Box::new(Scope::new(owner.clone(), work));
    scope.setup_allocation_error = scope.outcomes.try_reserve_exact(limits.max_steps).err();
    if let Err(error) = scope.work.check() {
        return Err(scope.fail(error.into()));
    }
    if scope.setup_allocation_error.is_some() {
        return Err(scope.fail(NativeResumeFailure::Allocation));
    }
    if let Err(error) = scope
        .work
        .reserve_capacity_excess::<NativeResumeOutcome>(scope.outcomes.capacity(), limits.max_steps)
    {
        return Err(scope.fail(error.into()));
    }
    Ok(scope)
}

trait ReplicationReplay {
    type Error: From<EngineError>;
    fn before_partition(&mut self) -> Result<(), Self::Error>;
    fn before_batch(&mut self) -> Result<(), Self::Error>;
    fn watermarks(&mut self, partition: TopicPartition)
    -> Result<PartitionWatermarks, Self::Error>;
    fn fetch_stored_internal(
        &mut self,
        request: FetchRequest,
        include_unresolved: bool,
    ) -> Result<Vec<StoredBatch>, Self::Error>;
    fn replicate(
        &mut self,
        batch: ReplicaAppend,
        required_followers: u32,
    ) -> Result<u32, Self::Error>;
}

struct OrdinaryReplication<'a> {
    engine: &'a StreamEngine,
    transport: &'a Arc<dyn ReplicationTransport>,
}
impl ReplicationReplay for OrdinaryReplication<'_> {
    type Error = EngineError;
    fn before_partition(&mut self) -> EngineResult<()> {
        Ok(())
    }
    fn before_batch(&mut self) -> EngineResult<()> {
        Ok(())
    }
    fn watermarks(&mut self, partition: TopicPartition) -> EngineResult<PartitionWatermarks> {
        self.engine.watermarks(partition)
    }
    fn fetch_stored_internal(
        &mut self,
        request: FetchRequest,
        include_unresolved: bool,
    ) -> EngineResult<Vec<StoredBatch>> {
        self.engine
            .fetch_stored_internal(request, include_unresolved)
    }
    fn replicate(&mut self, batch: ReplicaAppend, required_followers: u32) -> EngineResult<u32> {
        self.transport.replicate(batch, required_followers)
    }
}

// This is the ordinary original native replay algorithm. Only its operations
// are abstracted; the unbounded/ordinary delegate above keeps every old native
// call and the original required_followers=0 behavior. Strict replay below
// substitutes real bounded producers and physical ticket completion.
fn replay_replication<R: ReplicationReplay>(
    replay: &mut R,
    partitions: &[TopicPartition],
    max_bytes: u32,
) -> Result<(), R::Error> {
    for &topic_partition in partitions {
        replay.before_partition()?;
        let watermarks = replay.watermarks(topic_partition)?;
        let mut offset = watermarks.log_start;
        let end = watermarks.contiguous_log_end;
        while offset < end {
            let batches = replay.fetch_stored_internal(
                FetchRequest {
                    request_id: 0,
                    topic_id: topic_partition.topic_id,
                    partition_id: topic_partition.partition_id,
                    start_offset: offset,
                    max_bytes,
                    mode: FetchMode::Ordered,
                },
                true,
            )?;
            if batches.is_empty() {
                return Err(EngineError::CorruptState(format!(
                    "recovery replication found no batch at offset {} for {}/{}",
                    offset.get(),
                    topic_partition.topic_id,
                    topic_partition.partition_id,
                ))
                .into());
            }
            for batch in batches {
                replay.before_batch()?;
                let producer = batch.producer.map(|producer| ProducerIdentity {
                    producer_id: producer.producer_id,
                    epoch: producer.epoch,
                    first_sequence: producer.first_sequence,
                    event_id: producer.event_id,
                });
                let payload_digest = batch
                    .producer
                    .map_or([0; 16], |producer| producer.payload_digest);
                offset = LogicalOffset::new(
                    batch
                        .reservation
                        .last_offset
                        .get()
                        .checked_add(1)
                        .ok_or_else(|| {
                            EngineError::CorruptState(
                                "recovery replication offset space exhausted".into(),
                            )
                        })?,
                );
                replay.replicate(
                    ReplicaAppend {
                        reservation: batch.reservation,
                        producer,
                        atomic_group: batch.atomic_group.map(|group| AtomicGroupIdentity {
                            transaction_id: group.transaction_id,
                            chunk_sequence: group.chunk_sequence,
                        }),
                        extension_context: None,
                        payload_digest,
                        payload: batch.payload,
                    },
                    0,
                )?;
            }
        }
    }
    Ok(())
}

pub(super) fn ordinary_recovered_replication(
    engine: &StreamEngine,
    partitions: &[TopicPartition],
) -> EngineResult<()> {
    let Some(transport) = &engine.replication else {
        return Ok(());
    };
    let max_bytes = u32::try_from(engine.config.max_fetch_bytes).map_err(|_| {
        EngineError::InvalidConfig("max_fetch_bytes exceeds the recovery fetch limit".into())
    })?;
    replay_replication(
        &mut OrdinaryReplication { engine, transport },
        partitions,
        max_bytes,
    )
}

struct StrictReplication<'a>(&'a mut Scope);
impl ReplicationReplay for StrictReplication<'_> {
    type Error = NativeResumeFailure;
    fn before_partition(&mut self) -> Result<(), Self::Error> {
        self.0.work.charge(Debit {
            partitions: 1,
            ..Debit::default()
        })?;
        // Native replay diagnostics contain bounded u64/u128/u32 displays, not
        // arbitrary caller strings. Pay their source-visible ceiling in advance.
        let diagnostic = "recovery replication found no batch at offset  for /".len()
            + (u64::MAX.ilog10() as usize + 1)
            + (u128::MAX.ilog10() as usize + 1)
            + (u32::MAX.ilog10() as usize + 1);
        self.0
            .work
            .reserve_values(diagnostic, diagnostic, 1, diagnostic)?;
        Ok(())
    }
    fn before_batch(&mut self) -> Result<(), Self::Error> {
        self.0.work.charge(Debit {
            batches: 1,
            ..Debit::default()
        })?;
        let diagnostic = "recovery replication offset space exhausted".len();
        self.0
            .work
            .reserve_values(diagnostic, diagnostic, 1, diagnostic)?;
        Ok(())
    }
    fn watermarks(
        &mut self,
        partition: TopicPartition,
    ) -> Result<PartitionWatermarks, Self::Error> {
        self.0.watermarks(partition)
    }
    fn fetch_stored_internal(
        &mut self,
        request: FetchRequest,
        _include_unresolved: bool,
    ) -> Result<Vec<StoredBatch>, Self::Error> {
        let partition = TopicPartition::new(request.topic_id, request.partition_id);
        self.0
            .fetch_stored(partition, request.start_offset, request.max_bytes)?;
        let count = self.0.buffered.len();
        let bytes = add(
            add(
                mul(count, size_of::<StoredBatch>())?,
                bytes_clone_promotion_bytes(count)?,
            )?,
            size_of::<Vec<StoredBatch>>(),
        )?;
        self.0
            .work
            .reserve_values(bytes, 0, add(mul(count, 2)?, 1)?, bytes)?;
        self.0.work.check()?;
        // Keep the original read body owners in Scope while the shared ordinary
        // loop consumes only charged metadata/Bytes clones. A late/error replay
        // therefore retains every batch read in this actual native pass.
        self.0.fetched_copy = Vec::new();
        self.0.setup_allocation_error = self.0.fetched_copy.try_reserve_exact(count).err();
        self.0.work.check()?;
        if self.0.setup_allocation_error.is_some() {
            return Err(NativeResumeFailure::Allocation);
        }
        self.0
            .work
            .reserve_capacity_excess::<StoredBatch>(self.0.fetched_copy.capacity(), count)?;
        for batch in &self.0.buffered {
            self.0.work.check()?;
            self.0.fetched_copy.push(batch.clone());
            self.0.work.check()?;
        }
        Ok(std::mem::take(&mut self.0.fetched_copy))
    }
    fn replicate(
        &mut self,
        batch: ReplicaAppend,
        _required_followers: u32,
    ) -> Result<u32, Self::Error> {
        let batch_id = batch.reservation.batch_id;
        self.0.current_replica = Some(batch);
        self.0.work.reserve_outcomes(2)?;
        let current = self
            .0
            .current_replica
            .as_ref()
            .ok_or(NativeResumeFailure::MissingTransport)?;
        let buffers = 1 + usize::from(current.extension_context.is_some());
        let values = add(
            size_of::<ReplicaAppend>() * 2 + size_of::<ReplicationTicket>(),
            bytes_clone_promotion_bytes(buffers)?,
        )?;
        self.0
            .work
            .reserve_command(values, 0, add(3, buffers)?, 0)?;
        self.0.work.check()?;
        let batch = current.clone();
        self.0.work.check()?;
        let Some(transport) = self.0.owner.replication.as_ref() else {
            return Err(NativeResumeFailure::MissingTransport);
        };
        let started = catch_unwind(AssertUnwindSafe(|| {
            transport.begin_recovered_replicate_until(batch, self.0.work.absolute_expiry())
        }));
        let result = match started {
            Ok(Ok(ticket)) => {
                // Store the actual pending ticket BEFORE post-E observation.
                self.0.pending_replication = Some((batch_id, ticket));
                Ok(())
            }
            Ok(Err(error)) => Err(NativeResumeCallbackError::from_engine(error)),
            Err(payload) => Err(NativeResumeCallbackError {
                cause: CallbackCause::Panic(payload),
            }),
        };
        self.0
            .outcomes
            .push(NativeResumeOutcome::ReplicationStarted {
                batch_id,
                result,
                refusal: None,
            });
        let refusal = self.0.work.check().err();
        let entered = match self.0.outcomes.last_mut() {
            Some(NativeResumeOutcome::ReplicationStarted {
                result,
                refusal: saved,
                ..
            }) => {
                *saved = refusal;
                result.is_ok()
            }
            _ => false,
        };
        // Even a late start must physically drain its real pending ticket.
        self.0.drain_replication();
        if !entered {
            return Err(NativeResumeFailure::Replication);
        }
        if let Some(error) = refusal {
            return Err(error.into());
        }
        let replicas = match self.0.outcomes.last() {
            Some(NativeResumeOutcome::ReplicationCompleted {
                result: Ok(Ok(replicas)),
                refusal: None,
                ..
            }) => Some(*replicas),
            _ => None,
        };
        self.0.work.check()?;
        let replicas = replicas.ok_or(NativeResumeFailure::Replication)?;
        self.0.current_replica.take();
        Ok(replicas)
    }
}

impl Scope {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn finish_directory(&mut self) -> Result<(), NativeResumeFailure> {
        if let Some(checks) = &self.work.inner.directory {
            // Record the ACTUAL observation through the same immutable escrow,
            // including this final phase before any later failure/denial.
            return checks
                .check_native()
                .map_err(|_| NativeResumeFailure::Callback);
        }
        // Ordinary/no-checker compatibility: retain the actual original result.
        if let Some(policy) = &self.gc_policy
            && let Err(error) = policy.budget.check()
        {
            if self.gc_budget_error.is_none() {
                self.gc_budget_error = Some(error);
            }
            return Err(NativeResumeFailure::Callback);
        }
        Ok(())
    }
    #[allow(
        clippy::result_large_err,
        reason = "retain the original engine and actual outcomes without a refusal allocation"
    )]
    fn finish(mut self: Box<Self>) -> Result<NativeResumeReport, RetainedNativeResumeError> {
        let usage = match self.work.usage() {
            Ok(usage) => usage,
            Err(error) => return Err(self.fail(error.into())),
        };
        let fetch_usage = match self.work.inner.fetch.usage() {
            Ok(usage) => usage,
            Err(error) => return Err(self.fail(NativeResumeFailure::FetchAdmission(error.into()))),
        };
        if let Err(error) = self.work.check() {
            return Err(self.fail(error.into()));
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Err(error) = self.finish_directory() {
            return Err(self.fail(error));
        }
        let outcomes = std::mem::take(&mut self.outcomes);
        Ok(NativeResumeReport {
            outcomes,
            usage,
            fetch_usage,
        })
    }
}

impl EngineHandle {
    /// Replays retained native batches and waits for actual transport jobs.
    ///
    /// Uses one SAME fetch allowance across every pass. Every entered ticket is
    /// retained before post-E and physically drained, including on error/unwind.
    /// The transport's strict producer must implement the actual physical ticket
    /// contract; old enqueue-only transports refuse without being relabeled.
    /// Success is ordinary replay completion, never currentness/serving authority.
    ///
    /// # Errors
    /// Retains the same engine with actual partial fetch, coordinator, transport
    /// and unwind results on expiry, capacity or producer refusal. An entered
    /// ticket is physically drained before returning, even after expiry.
    #[allow(
        clippy::result_large_err,
        reason = "retain the original engine and actual outcomes without a refusal allocation"
    )]
    pub fn resume_recovered_replication_until(
        &self,
        partitions: &[TopicPartition],
        limits: NativeResumeLimits,
        original_expiry: Instant,
    ) -> Result<NativeResumeReport, RetainedNativeResumeError> {
        let mut scope = start(self, limits, original_expiry)?;
        // The real engine, stored ticket and typed outcomes remain OUTSIDE the
        // owning loop's unwind boundary. No entered job is dropped to detach it.
        let run = catch_unwind(AssertUnwindSafe(|| {
            scope.work.check()?;
            if partitions.len() > limits.max_partitions {
                return Err(NativeResumeRefusal::Capacity.into());
            }
            if scope.owner.replication.is_none() {
                return Ok(());
            }
            let diagnostic = "max_fetch_bytes exceeds the recovery fetch limit".len();
            scope
                .work
                .reserve_values(diagnostic, diagnostic, 1, diagnostic)?;
            let max_bytes = u32::try_from(scope.owner.config.max_fetch_bytes).map_err(|_| {
                EngineError::InvalidConfig(
                    "max_fetch_bytes exceeds the recovery fetch limit".into(),
                )
            })?;
            scope.work.check()?;
            replay_replication(&mut StrictReplication(&mut scope), partitions, max_bytes)
        }));
        let run = match run {
            Ok(result) => result,
            Err(payload) => {
                scope.work.deny();
                scope.panic_payload = Some(payload);
                Err(NativeResumeFailure::Unwind)
            }
        };
        if let Err(error) = run {
            return Err(scope.fail(error));
        }
        scope.finish()
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[path = "native_resume_gc.rs"]
mod gc;

trait SinkReplay {
    type Error: From<EngineError>;
    fn before_partition(&mut self) -> Result<(), Self::Error>;
    fn before_batch(&mut self) -> Result<(), Self::Error>;
    fn before_decision_insert(
        &mut self,
        decisions: &mut HashMap<DurableAtomicGroup, DurableAtomicDecision>,
    ) -> Result<(), Self::Error>;
    fn before_pending_group(&mut self, partitions: usize) -> Result<(), Self::Error>;
    fn before_pending_push(
        &mut self,
        pending: &mut Vec<DurableAtomicGroup>,
    ) -> Result<(), Self::Error>;
    fn before_decision_scan(&mut self, capacity: usize) -> Result<(), Self::Error>;
    fn checkpoint(
        &mut self,
        partition: TopicPartition,
    ) -> Result<DurableSinkCheckpoint, Self::Error>;
    fn watermarks(&mut self, partition: TopicPartition)
    -> Result<PartitionWatermarks, Self::Error>;
    fn fetch_stored_internal(
        &mut self,
        request: FetchRequest,
        unresolved: bool,
    ) -> Result<Vec<StoredBatch>, Self::Error>;
    fn make_append(&mut self, batch: StoredBatch) -> Result<DurableAppend, Self::Error>;
    fn enqueue_replay(
        &mut self,
        append: DurableAppend,
        expected: DurableSinkCheckpoint,
        next: DurableSinkCheckpoint,
    ) -> Result<(), Self::Error>;
    fn inspect_atomic_group(
        &mut self,
        partition: TopicPartition,
        transaction: u128,
    ) -> Result<AtomicGroupInspection, Self::Error>;
    fn enqueue_replay_decision(
        &mut self,
        decision: DurableAtomicDecision,
    ) -> Result<(), Self::Error>;
    fn clear_dirty(&mut self, partition: TopicPartition) -> Result<(), Self::Error>;
    fn recovery_mode(&self) -> DurableSinkRecoveryMode;
    fn wait_for(&mut self, checkpoint: DurableSinkCheckpoint) -> Result<(), Self::Error>;
    fn wait_for_decision(&mut self, group: DurableAtomicGroup) -> Result<(), Self::Error>;
    fn collect_garbage(
        &mut self,
        partition: TopicPartition,
        start: LogicalOffset,
    ) -> Result<(), Self::Error>;
    fn pending_atomic_groups(&mut self) -> Result<Vec<DurableAtomicGroup>, Self::Error>;
}
struct OrdinarySink<'a> {
    engine: &'a StreamEngine,
    dispatcher: &'a DurableSinkDispatcher,
}
impl SinkReplay for OrdinarySink<'_> {
    type Error = EngineError;
    fn before_partition(&mut self) -> EngineResult<()> {
        Ok(())
    }
    fn before_batch(&mut self) -> EngineResult<()> {
        Ok(())
    }
    fn before_decision_insert(
        &mut self,
        _: &mut HashMap<DurableAtomicGroup, DurableAtomicDecision>,
    ) -> EngineResult<()> {
        Ok(())
    }
    fn before_pending_group(&mut self, _: usize) -> EngineResult<()> {
        Ok(())
    }
    fn before_pending_push(&mut self, _: &mut Vec<DurableAtomicGroup>) -> EngineResult<()> {
        Ok(())
    }
    fn before_decision_scan(&mut self, _: usize) -> EngineResult<()> {
        Ok(())
    }
    fn checkpoint(&mut self, partition: TopicPartition) -> EngineResult<DurableSinkCheckpoint> {
        self.dispatcher.checkpoint(partition)
    }
    fn watermarks(&mut self, partition: TopicPartition) -> EngineResult<PartitionWatermarks> {
        self.engine.watermarks(partition)
    }
    fn fetch_stored_internal(
        &mut self,
        request: FetchRequest,
        unresolved: bool,
    ) -> EngineResult<Vec<StoredBatch>> {
        self.engine.fetch_stored_internal(request, unresolved)
    }
    fn make_append(&mut self, batch: StoredBatch) -> EngineResult<DurableAppend> {
        Ok(durable_append_from_stored(&self.engine.config, batch))
    }
    fn enqueue_replay(
        &mut self,
        append: DurableAppend,
        _: DurableSinkCheckpoint,
        _: DurableSinkCheckpoint,
    ) -> EngineResult<()> {
        self.dispatcher.enqueue_replay(append)
    }
    fn inspect_atomic_group(
        &mut self,
        partition: TopicPartition,
        transaction: u128,
    ) -> EngineResult<AtomicGroupInspection> {
        self.engine.inspect_atomic_group(partition, transaction)
    }
    fn enqueue_replay_decision(&mut self, decision: DurableAtomicDecision) -> EngineResult<()> {
        self.dispatcher.enqueue_replay_decision(decision)
    }
    fn clear_dirty(&mut self, partition: TopicPartition) -> EngineResult<()> {
        self.dispatcher.clear_dirty(partition);
        Ok(())
    }
    fn recovery_mode(&self) -> DurableSinkRecoveryMode {
        self.dispatcher.recovery_mode()
    }
    fn wait_for(&mut self, checkpoint: DurableSinkCheckpoint) -> EngineResult<()> {
        self.dispatcher.wait_for(checkpoint)
    }
    fn wait_for_decision(&mut self, group: DurableAtomicGroup) -> EngineResult<()> {
        self.dispatcher.wait_for_decision(group)
    }
    fn collect_garbage(
        &mut self,
        partition: TopicPartition,
        start: LogicalOffset,
    ) -> EngineResult<()> {
        self.engine.collect_garbage(partition, start)
    }
    fn pending_atomic_groups(&mut self) -> EngineResult<Vec<DurableAtomicGroup>> {
        self.dispatcher.pending_atomic_groups()
    }
}
// One actual native sink replay algorithm. Ordinary operations above preserve
// their configured retry/background behavior; strict operations must substitute
// actual bounded callbacks, native reads, cache observations and consuming Core GC.
fn replay_sink<R: SinkReplay>(
    replay: &mut R,
    partitions: &[TopicPartition],
    max_bytes: u32,
) -> Result<(), R::Error> {
    for &topic_partition in partitions {
        replay.before_partition()?;
        let mut checkpoint = replay.checkpoint(topic_partition)?;
        let mut recovered_decisions = HashMap::new();
        let watermarks = replay.watermarks(topic_partition)?;
        if checkpoint.next_offset < watermarks.log_start {
            return Err(EngineError::DurableSinkCheckpoint(format!(
                "checkpoint offset {} for {}/{} is behind retained log start {}",
                checkpoint.next_offset,
                topic_partition.topic_id,
                topic_partition.partition_id,
                watermarks.log_start
            ))
            .into());
        }
        if checkpoint.next_offset > watermarks.contiguous_log_end {
            return Err(EngineError::DurableSinkCheckpoint(format!(
                "checkpoint offset {} for {}/{} is ahead of durable log end {}",
                checkpoint.next_offset,
                topic_partition.topic_id,
                topic_partition.partition_id,
                watermarks.contiguous_log_end
            ))
            .into());
        }
        let target_end = watermarks.contiguous_log_end;
        while checkpoint.next_offset < target_end {
            let batches = replay.fetch_stored_internal(
                FetchRequest {
                    request_id: 0,
                    topic_id: topic_partition.topic_id,
                    partition_id: topic_partition.partition_id,
                    start_offset: checkpoint.next_offset,
                    max_bytes,
                    mode: FetchMode::Ordered,
                },
                true,
            )?;
            if batches.is_empty() {
                return Err(EngineError::CorruptState(format!(
                    "durable sink recovery found no batch at offset {} for {}/{}",
                    checkpoint.next_offset, topic_partition.topic_id, topic_partition.partition_id
                ))
                .into());
            }
            for batch in batches {
                replay.before_batch()?;
                if batch.reservation.first_offset >= target_end {
                    break;
                }
                let next = checkpoint.after(batch.reservation)?;
                let recovered_group = batch.atomic_group.map(|group| DurableAtomicGroup {
                    topic_partition,
                    transaction_id: group.transaction_id,
                });
                let append = replay.make_append(batch)?;
                replay.enqueue_replay(append, checkpoint, next)?;
                if let Some(group) = recovered_group {
                    let inspection =
                        replay.inspect_atomic_group(topic_partition, group.transaction_id)?;
                    if inspection.state != AtomicGroupState::Unresolved {
                        replay.before_decision_insert(&mut recovered_decisions)?;
                        recovered_decisions.insert(
                            group,
                            DurableAtomicDecision {
                                group,
                                outcome: inspection.state,
                            },
                        );
                    }
                }
                checkpoint = next;
            }
        }
        replay.before_decision_scan(recovered_decisions.capacity())?;
        for decision in recovered_decisions.values().copied() {
            replay.enqueue_replay_decision(decision)?;
        }
        replay.clear_dirty(topic_partition)?;
        if replay.recovery_mode() == DurableSinkRecoveryMode::Blocking {
            replay.wait_for(checkpoint)?;
            replay.before_decision_scan(recovered_decisions.capacity())?;
            for group in recovered_decisions.keys().copied() {
                replay.wait_for_decision(group)?;
            }
        }
        let log_start = replay.watermarks(topic_partition)?.log_start;
        replay.collect_garbage(topic_partition, log_start)?;
    }

    let mut pending_decisions = Vec::new();
    for group in replay.pending_atomic_groups()? {
        replay.before_pending_group(partitions.len())?;
        if !partitions.contains(&group.topic_partition) {
            continue;
        }
        let inspection =
            replay.inspect_atomic_group(group.topic_partition, group.transaction_id)?;
        if inspection.state != AtomicGroupState::Unresolved {
            let decision = DurableAtomicDecision {
                group,
                outcome: inspection.state,
            };
            replay.enqueue_replay_decision(decision)?;
            replay.before_pending_push(&mut pending_decisions)?;
            pending_decisions.push(group);
        }
    }
    if replay.recovery_mode() == DurableSinkRecoveryMode::Blocking {
        for group in pending_decisions {
            replay.wait_for_decision(group)?;
        }
    }
    Ok(())
}

pub(super) fn ordinary_durable_sink(
    engine: &StreamEngine,
    partitions: &[TopicPartition],
) -> EngineResult<()> {
    let Some(dispatcher) = &engine.durable_sink else {
        return Ok(());
    };
    let max_bytes = u32::try_from(engine.config.max_fetch_bytes).map_err(|_| {
        EngineError::InvalidConfig("max_fetch_bytes exceeds the sink recovery limit".into())
    })?;
    replay_sink(
        &mut OrdinarySink { engine, dispatcher },
        partitions,
        max_bytes,
    )
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[path = "native_resume_sink.rs"]
mod sink;

#[cfg(all(test, unix))]
#[path = "native_resume_tests.rs"]
mod tests;
