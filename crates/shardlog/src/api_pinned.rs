//! Consume the genuinely opened pinned store and inspect its canonical history.
//!
//! This constructor performs no path open or file-descriptor reconstruction.
//! Its caller retains the actual native pin and composing directory owner until
//! the returned log or error is destroyed. Catalog iteration pays the actual
//! hash-table capacity before traversal; ordinary canonical sequence rules are
//! shared. This is one bounded conversion, not a complete pinned read/write API.

use super::{
    CANONICAL_STREAM_DIAGNOSTIC, RETAINED_GAP_DIAGNOSTIC, SEQUENCE_OVERFLOW_DIAGNOSTIC,
    ShardLogResult, retained_bounds, validate_catalog_entry,
};
use crate::PinnedShardFetchChecks;
use crate::{
    OpenedPinnedShardStore, Reservation, ShardDirectoryCheckFailure, ShardDirectoryChecks,
    StorageError, StorageFetchBudget, StorageFetchCost, StorageFetchFailure, StorageFetchLimits,
    StorageFetchUsage, storage_fetch_cost,
};
use std::{
    any::Any,
    collections::TryReserveError,
    error::Error,
    fmt,
    mem::size_of,
    panic::{AssertUnwindSafe, catch_unwind},
    time::Instant,
};

/// Checks of the same composing operation, with mandatory terminal denial.
///
/// Callbacks are borrowed only for this conversion. They must check the actual
/// composing counters/minimum E and latch those same counters and health on
/// denial. They confer no authority and are never stored in the completed log.
pub trait PinnedShardLogOpenChecks: ShardDirectoryChecks {
    /// Deny the same composing operation before the retained store can drop.
    fn deny(&self);
}

/// Terminal conversion refusal; native results remain separately observable.
#[derive(Debug)]
pub enum PinnedShardLogOpenFailure {
    /// Original native deadline, counter, poison or previous refusal.
    Native(StorageFetchFailure),
    /// The actual composing checkpoint refused.
    Checkpoint(ShardDirectoryCheckFailure),
    /// Shared canonical stream, contiguity or sequence arithmetic rejected.
    Canonical,
    /// The actual requested sequence allocation failed.
    Allocation,
    /// A checkpoint or entered phase unwound; the original payload is retained.
    Unwind,
    /// A completed conversion did not retain its actual bounds result.
    Invariant,
}

/// Actual values retained before postchecks, including a refused scan prefix.
///
/// Sequence and string fees describe requested/source-visible storage, not
/// allocator internals, kernel storage or RSS. No native descriptor is exposed.
pub struct PinnedShardLogOpenObservation {
    original_expiry: Instant,
    shape: Option<(usize, usize, usize)>,
    sequences: Vec<u64>,
    reservation: Option<Reservation>,
    catalog_result: Option<ShardLogResult<u64>>,
    allocation: Option<Result<(), TryReserveError>>,
    bounds: Option<ShardLogResult<Option<(u64, u64)>>>,
    usage: Option<Result<StorageFetchUsage, StorageFetchFailure>>,
    failure: Option<PinnedShardLogOpenFailure>,
    panic_payload: Option<Box<dyn Any + Send>>,
    budget_deny_panic: Option<Box<dyn Any + Send>>,
    composing_deny_panic: Option<Box<dyn Any + Send>>,
}

impl fmt::Debug for PinnedShardLogOpenObservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardLogOpenObservation")
            .field("shape", &self.shape)
            .field("retained_sequences", &self.sequences.len())
            .field("bounds", &self.bounds)
            .field("usage", &self.usage)
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}

impl PinnedShardLogOpenObservation {
    fn new(original_expiry: Instant) -> Self {
        Self {
            original_expiry,
            shape: None,
            sequences: Vec::new(),
            reservation: None,
            catalog_result: None,
            allocation: None,
            bounds: None,
            usage: None,
            failure: None,
            panic_payload: None,
            budget_deny_panic: None,
            composing_deny_panic: None,
        }
    }
    /// The original conversion E, retained only as an observation.
    pub fn absolute_expiry(&self) -> Instant {
        self.original_expiry
    }
    /// Actual catalog length, table capacity and prepaid scan work bytes.
    pub fn catalog_shape(&self) -> Option<(usize, usize, usize)> {
        self.shape
    }
    /// Actual validated prefix; canonical bounds sort it in place when entered.
    pub fn retained_sequences(&self) -> &[u64] {
        &self.sequences
    }
    /// Actual allocated sequence capacity, including a refused excess capacity.
    pub fn sequence_capacity(&self) -> usize {
        self.sequences.capacity()
    }
    /// Actual last reservation saved before its canonical inspection.
    pub fn retained_reservation(&self) -> Option<Reservation> {
        self.reservation
    }
    /// Actual latest canonical-entry result, saved before its postcheck.
    pub fn catalog_result(&self) -> Option<&ShardLogResult<u64>> {
        self.catalog_result.as_ref()
    }
    /// Actual complete shared canonical bounds result, saved before its postcheck.
    pub fn bounds_result(&self) -> Option<&ShardLogResult<Option<(u64, u64)>>> {
        self.bounds.as_ref()
    }
    /// Actual allocation error, if a sequence allocation was entered and refused.
    pub fn allocation_error(&self) -> Option<&TryReserveError> {
        self.allocation
            .as_ref()
            .and_then(|result| result.as_ref().err())
    }
    /// Actual same-budget counters observed after terminal denial, if required.
    pub fn usage(&self) -> Option<&Result<StorageFetchUsage, StorageFetchFailure>> {
        self.usage.as_ref()
    }
    /// The terminal refusal; successful conversion has none.
    pub fn failure(&self) -> Option<&PinnedShardLogOpenFailure> {
        self.failure.as_ref()
    }
    /// Original operation, native denial and composing denial panic payloads.
    pub fn panic_payloads(&self) -> [Option<&(dyn Any + Send)>; 3] {
        [
            self.panic_payload.as_deref(),
            self.budget_deny_panic.as_deref(),
            self.composing_deny_panic.as_deref(),
        ]
    }
    fn native_error(&self) -> Option<&StorageError> {
        self.catalog_result
            .as_ref()
            .and_then(|r| r.as_ref().err())
            .or_else(|| self.bounds.as_ref().and_then(|r| r.as_ref().err()))
    }
}

struct Scope {
    observation: PinnedShardLogOpenObservation,
    // Last: all actual results and unwind payloads drop before the opened store.
    opened: OpenedPinnedShardStore,
}

/// A canonical log that owns the entire actual opened pinned store.
///
/// Construction consumes the genuine native carrier. There is no plain-store,
/// raw-parts, pin or file-descriptor extractor. The composing caller continues
/// retaining its actual native pin and Core parent last. Startup counters and
/// callbacks are not stored; later operations require fresh admitted work.
pub struct OpenedPinnedShardLog {
    bounds: Option<(u64, u64)>,
    scope: Scope,
}
impl fmt::Debug for OpenedPinnedShardLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenedPinnedShardLog")
            .field("bounds", &self.bounds)
            .field("observation", &self.scope.observation)
            .finish_non_exhaustive()
    }
}
impl OpenedPinnedShardLog {
    /// Actual canonical half-open sequence range observed during conversion.
    pub fn bounds(&self) -> Option<(u64, u64)> {
        self.bounds
    }
    /// Constructor results retained with the same opened native store.
    pub fn observation(&self) -> &PinnedShardLogOpenObservation {
        &self.scope.observation
    }
}

/// A refused conversion retaining its entire original opened store and results.
///
/// The uncertain owner cannot be retried or extracted through this error. Its
/// composing caller must keep the same native pin and Core parent until drop.
pub struct PinnedShardLogOpenError {
    scope: Scope,
}
impl fmt::Debug for PinnedShardLogOpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardLogOpenError")
            .field("observation", &self.scope.observation)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for PinnedShardLogOpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "pinned canonical log conversion refused: {:?}",
            self.scope.observation.failure
        )
    }
}
impl Error for PinnedShardLogOpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(error) = self.scope.observation.native_error() {
            return Some(error);
        }
        if let Some(error) = self.scope.observation.allocation_error() {
            return Some(error);
        }
        match self.scope.observation.failure.as_ref() {
            Some(PinnedShardLogOpenFailure::Native(error)) => Some(error),
            _ => None,
        }
    }
}
impl PinnedShardLogOpenError {
    /// Actual canonical prefix, raw results, refusal and panic facts.
    pub fn observation(&self) -> &PinnedShardLogOpenObservation {
        &self.scope.observation
    }
}

fn fixed_fee() -> Result<(usize, usize, usize), StorageFetchFailure> {
    // Name each retained/result/panic request independently. The larger outer
    // carriers are not an implicit fee for their inner value or buffer slots.
    let slots = [
        size_of::<Scope>(),
        size_of::<OpenedPinnedShardLog>(),
        size_of::<PinnedShardLogOpenError>(),
        size_of::<PinnedShardLogOpenObservation>(),
        size_of::<StorageFetchCost>(),
        size_of::<StorageFetchUsage>(),
        size_of::<Instant>(),
        size_of::<Option<(usize, usize, usize)>>(),
        size_of::<Vec<u64>>(),
        size_of::<Option<Reservation>>(),
        size_of::<Option<ShardLogResult<u64>>>(),
        size_of::<Option<Result<(), TryReserveError>>>(),
        size_of::<Option<ShardLogResult<Option<(u64, u64)>>>>(),
        size_of::<Option<Result<StorageFetchUsage, StorageFetchFailure>>>(),
        size_of::<Option<PinnedShardLogOpenFailure>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<String>(),
        size_of::<Result<Result<(), PinnedShardLogOpenFailure>, Box<dyn Any + Send>>>(),
        size_of::<Option<Option<(u64, u64)>>>(),
        size_of::<[usize; 3]>(),
    ];
    let strings = CANONICAL_STREAM_DIAGNOSTIC
        .len()
        .max(RETAINED_GAP_DIAGNOSTIC.len())
        .max(SEQUENCE_OVERFLOW_DIAGNOSTIC.len());
    let bytes = slots
        .into_iter()
        .try_fold(strings, |sum, value| sum.checked_add(value))
        .ok_or(StorageFetchFailure::Capacity)?;
    // One actual canonical diagnostic can own a string buffer independently
    // of its String header. Both canonical failure paths terminate at once.
    let entries = slots
        .len()
        .checked_add(1)
        .ok_or(StorageFetchFailure::Capacity)?;
    Ok((bytes, entries, strings))
}

/// Plain native budget ceiling plus conversion values and diagnostic storage.
///
/// The composing owner prepays this checked cost before constructing the SAME
/// `StorageFetchBudget`. Conversion charges also consume that budget's finite
/// ceiling. This conservative cost excludes existing store/index allocations,
/// allocator/kernel/RSS overhead, and all future read/write operations.
pub fn pinned_shard_log_open_cost(
    limits: StorageFetchLimits,
) -> Result<StorageFetchCost, StorageFetchFailure> {
    let mut cost = storage_fetch_cost(limits)?;
    let (bytes, entries, strings) = fixed_fee()?;
    cost.declared_bytes = cost
        .declared_bytes
        .checked_add(bytes)
        .ok_or(StorageFetchFailure::Capacity)?;
    cost.entries = cost
        .entries
        .checked_add(entries)
        .ok_or(StorageFetchFailure::Capacity)?;
    cost.string_bytes = cost
        .string_bytes
        .checked_add(strings)
        .ok_or(StorageFetchFailure::Capacity)?;
    cost.work_bytes = cost
        .work_bytes
        .checked_add(bytes)
        .and_then(|n| n.checked_add(entries))
        .ok_or(StorageFetchFailure::Capacity)?;
    Ok(cost)
}

fn checkpoint(
    budget: &StorageFetchBudget,
    checks: &dyn PinnedShardLogOpenChecks,
) -> Result<(), PinnedShardLogOpenFailure> {
    budget.check().map_err(PinnedShardLogOpenFailure::Native)?;
    checks
        .check()
        .map_err(PinnedShardLogOpenFailure::Checkpoint)?;
    budget.check().map_err(PinnedShardLogOpenFailure::Native)
}

impl Scope {
    fn inspect(
        &mut self,
        budget: &StorageFetchBudget,
        checks: &dyn PinnedShardLogOpenChecks,
    ) -> Result<(), PinnedShardLogOpenFailure> {
        checkpoint(budget, checks)?;
        let (fixed, entries, _) = fixed_fee().map_err(PinnedShardLogOpenFailure::Native)?;
        budget
            .charge_values(fixed, entries, fixed)
            .map_err(PinnedShardLogOpenFailure::Native)?;
        let shape =
            self.opened
                .store()
                .log_catalog_shape()
                .ok_or(PinnedShardLogOpenFailure::Native(
                    StorageFetchFailure::Capacity,
                ))?;
        self.observation.shape = Some(shape);
        if shape.0 > budget.limits().max_plan_batches {
            return Err(PinnedShardLogOpenFailure::Native(
                StorageFetchFailure::Capacity,
            ));
        }
        let slots =
            shape
                .0
                .checked_mul(size_of::<u64>())
                .ok_or(PinnedShardLogOpenFailure::Native(
                    StorageFetchFailure::Capacity,
                ))?;
        let levels = usize::BITS as usize - shape.0.leading_zeros() as usize;
        let work = shape
            .0
            .checked_mul(levels.max(1))
            .and_then(|n| n.checked_mul(size_of::<u64>()))
            .and_then(|n| n.checked_add(shape.2))
            .and_then(|n| n.checked_add(slots))
            .ok_or(PinnedShardLogOpenFailure::Native(
                StorageFetchFailure::Capacity,
            ))?;
        budget
            .charge_values(slots, shape.0, work)
            .map_err(PinnedShardLogOpenFailure::Native)?;
        checkpoint(budget, checks)?;
        self.observation.allocation = Some(self.observation.sequences.try_reserve_exact(shape.0));
        #[cfg(test)]
        tests::completed(tests::Phase::Allocated);
        checkpoint(budget, checks)?;
        if matches!(self.observation.allocation, Some(Err(_))) {
            return Err(PinnedShardLogOpenFailure::Allocation);
        }
        let actual_capacity = self.observation.sequences.capacity();
        if actual_capacity > budget.limits().max_plan_batches {
            return Err(PinnedShardLogOpenFailure::Native(
                StorageFetchFailure::Capacity,
            ));
        }
        let excess = actual_capacity
            .checked_sub(shape.0)
            .and_then(|n| n.checked_mul(size_of::<u64>()))
            .ok_or(PinnedShardLogOpenFailure::Native(
                StorageFetchFailure::Capacity,
            ))?;
        budget
            .charge_values(excess, actual_capacity - shape.0, excess)
            .map_err(PinnedShardLogOpenFailure::Native)?;
        checkpoint(budget, checks)?;
        for reservation in self.opened.store().log_catalog_reservations() {
            checkpoint(budget, checks)?;
            self.observation.reservation = Some(reservation);
            self.observation.catalog_result = Some(validate_catalog_entry(reservation));
            #[cfg(test)]
            tests::completed(tests::Phase::Validated);
            checkpoint(budget, checks)?;
            match self.observation.catalog_result.as_ref() {
                Some(Ok(sequence)) => self.observation.sequences.push(*sequence),
                _ => return Err(PinnedShardLogOpenFailure::Canonical),
            }
        }
        checkpoint(budget, checks)?;
        self.observation.bounds = Some(retained_bounds(&mut self.observation.sequences));
        #[cfg(test)]
        tests::completed(tests::Phase::Bounds);
        checkpoint(budget, checks)?;
        if matches!(self.observation.bounds, Some(Err(_))) {
            return Err(PinnedShardLogOpenFailure::Canonical);
        }
        self.observation.usage = Some(budget.usage());
        checkpoint(budget, checks)?;
        Ok(())
    }
    fn deny(&mut self, budget: &StorageFetchBudget, checks: &dyn PinnedShardLogOpenChecks) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| budget.deny())) {
            self.observation.budget_deny_panic = Some(payload);
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| checks.deny())) {
            self.observation.composing_deny_panic = Some(payload);
        }
    }
}

impl OpenedPinnedShardStore {
    /// Consume this actual opened store and inspect its entire canonical index.
    ///
    /// Prepay `pinned_shard_log_open_cost` before creating the same supplied
    /// budget. All catalog passes use its original cumulative counters/E. The
    /// caller retains the genuine native pin and composing owner throughout;
    /// success or error retains this whole store until its own destruction.
    /// No I/O, plain reopen or new store/lock owner is fabricated. This method
    /// does not supply bounded reads, appends, sealing, compaction or serving.
    pub fn into_shard_log_until(
        self,
        budget: &StorageFetchBudget,
        checks: &dyn PinnedShardLogOpenChecks,
    ) -> Result<OpenedPinnedShardLog, PinnedShardLogOpenError> {
        let mut scope = Scope {
            observation: PinnedShardLogOpenObservation::new(budget.absolute_expiry()),
            opened: self,
        };
        let result = catch_unwind(AssertUnwindSafe(|| scope.inspect(budget, checks)));
        let failure = match result {
            Ok(Ok(())) => None,
            Ok(Err(failure)) => Some(failure),
            Err(payload) => {
                scope.observation.panic_payload = Some(payload);
                Some(PinnedShardLogOpenFailure::Unwind)
            }
        };
        scope.observation.failure = failure;
        let bounds = match scope.observation.bounds.as_ref() {
            Some(Ok(bounds)) if scope.observation.failure.is_none() => Some(*bounds),
            _ => None,
        };
        if bounds.is_none() && scope.observation.failure.is_none() {
            scope.observation.failure = Some(PinnedShardLogOpenFailure::Invariant);
        }
        if scope.observation.failure.is_some() {
            scope.deny(budget, checks);
        }
        if scope.observation.usage.is_none() {
            scope.observation.usage = Some(budget.usage());
        }
        match bounds {
            Some(bounds) if scope.observation.failure.is_none() => {
                Ok(OpenedPinnedShardLog { bounds, scope })
            }
            _ => Err(PinnedShardLogOpenError { scope }),
        }
    }
}

#[cfg(test)]
#[path = "api_pinned_tests.rs"]
mod tests;

/// Terminal consuming-read refusal; original typed native results stay retained.
#[derive(Debug)]
pub enum PinnedShardLogReadFailure {
    /// Actual shared native deadline/counters/sticky denial.
    Native(StorageFetchFailure),
    /// Actual SAME composing checkpoint refusal.
    Checkpoint {
        result: ShardDirectoryCheckFailure,
        native_postcheck: Option<StorageFetchFailure>,
    },
    /// Native metadata planning refused; actual plan error remains retained.
    Plan,
    /// Native physical reading refused; actual read error remains retained.
    Read,
    /// Shared canonical record validation refused.
    Canonical,
    /// Actual requested record allocation refused.
    Allocation,
    /// Actual bridge phase/checkpoint unwound.
    Unwind,
}

/// Actual completed canonical read checkpoints; no completion/serving witness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PinnedShardLogReadPhase {
    /// Actual native metadata planning returned and is retained.
    Planned,
    /// Actual physical read returned and is retained.
    Read,
    /// Actual canonical output vector reservation returned and is retained.
    Allocated,
    /// Actual shared canonical mapping returned and is retained.
    Mapped,
}
fn read_completed(phase: PinnedShardLogReadPhase) {
    #[cfg(test)]
    tests::read_completed(phase);
    #[cfg(not(test))]
    let _ = phase;
}
struct ReadChecks<'a> {
    outer: &'a dyn crate::PinnedShardFetchChecks,
    denial_attempted: std::cell::Cell<bool>,
    denial_completed: std::cell::Cell<bool>,
}
impl ShardDirectoryChecks for ReadChecks<'_> {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        self.outer.check()
    }
}
impl crate::PinnedShardFetchChecks for ReadChecks<'_> {
    fn deny(&self) {
        if !self.denial_attempted.replace(true) {
            self.outer.deny();
            self.denial_completed.set(true);
        }
    }
}
struct LogReadScope {
    records: Vec<super::ShardLogRecord>,
    plan: Option<Result<crate::PinnedShardFetchPlan, crate::PinnedShardFetchPlanError>>,
    read: Option<Result<crate::PinnedShardFetchComplete, crate::PinnedShardFetchReadError>>,
    mapped: Option<ShardLogResult<super::ShardLogRecord>>,
    allocation: Option<Result<(), TryReserveError>>,
    failure: Option<PinnedShardLogReadFailure>,
    panic_payload: Option<Box<dyn Any + Send>>,
    budget_deny_panic: Option<Box<dyn Any + Send>>,
    composing_deny_panic: Option<Box<dyn Any + Send>>,
    denial_completed: bool,
    // Last: every actual native result/partial buffer/record precedes whole log.
    log: OpenedPinnedShardLog,
}
/// Real canonical records and native results with the whole actual log LAST.
pub struct PinnedShardLogReadComplete {
    scope: LogReadScope,
}
impl PinnedShardLogReadComplete {
    /// Actual shared canonical mapping output, in native selection order.
    pub fn records(&self) -> &[super::ShardLogRecord] {
        &self.scope.records
    }
    /// Safe original native physical observations retained before postchecks.
    pub fn observation(&self) -> Option<&crate::PinnedShardFetchObservation> {
        self.scope.read_observation()
    }
    /// Borrow the same actual canonical log, without extracting native storage.
    pub fn log(&self) -> &OpenedPinnedShardLog {
        &self.scope.log
    }
    /// Destroy read results first and return the WHOLE same log for next pass.
    pub fn into_log(self) -> OpenedPinnedShardLog {
        let LogReadScope {
            records,
            plan,
            read,
            mapped,
            allocation,
            failure,
            panic_payload,
            budget_deny_panic,
            composing_deny_panic,
            denial_completed: _,
            log,
        } = self.scope;
        drop((
            records,
            plan,
            read,
            mapped,
            allocation,
            failure,
            panic_payload,
            budget_deny_panic,
            composing_deny_panic,
        ));
        log
    }
}
/// Refusal retains every actual native result and the original whole log LAST.
pub struct PinnedShardLogReadError {
    scope: LogReadScope,
}
impl PinnedShardLogReadError {
    /// Actual complete canonical prefix before refusal.
    pub fn records(&self) -> &[super::ShardLogRecord] {
        &self.scope.records
    }
    /// Actual safe physical observations, including partial buffers and raw IO.
    pub fn observation(&self) -> Option<&crate::PinnedShardFetchObservation> {
        self.scope.read_observation()
    }
    /// Actual native plan-stage facts when no physical read entered.
    pub fn plan_observation(&self) -> Option<&crate::PinnedShardFetchObservation> {
        match self.scope.plan.as_ref()? {
            Ok(p) => Some(p.observation()),
            Err(e) => Some(e.observation()),
        }
    }
    /// Original pure canonical mapping error, when one was returned.
    pub fn canonical_error(&self) -> Option<&StorageError> {
        self.scope.mapped.as_ref()?.as_ref().err()
    }
    /// Actual retained requested output-allocation refusal.
    pub fn allocation_error(&self) -> Option<&TryReserveError> {
        self.scope.allocation.as_ref()?.as_ref().err()
    }
    /// Actual terminal bridge category; detailed typed native facts stay retained.
    pub fn failure(&self) -> Option<&PinnedShardLogReadFailure> {
        self.scope.failure.as_ref()
    }
    /// Independent bridge/denial panic payloads; native ones remain in observation.
    pub fn panic_payloads(&self) -> [Option<&(dyn Any + Send)>; 3] {
        [
            self.scope.panic_payload.as_deref(),
            self.scope.budget_deny_panic.as_deref(),
            self.scope.composing_deny_panic.as_deref(),
        ]
    }
    /// Whether actual mandatory outer denial returned without unwinding.
    pub fn denial_completed(&self) -> bool {
        self.scope.denial_completed
    }
}
impl fmt::Debug for PinnedShardLogReadComplete {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardLogReadComplete")
            .field("records", &self.records().len())
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for PinnedShardLogReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardLogReadError")
            .field("failure", &self.failure())
            .field("records", &self.records().len())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for PinnedShardLogReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pinned canonical read refused: {:?}", self.failure())
    }
}
impl Error for PinnedShardLogReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(error) = self.canonical_error() {
            return Some(error);
        }
        if let Some(error) = self.allocation_error() {
            return Some(error);
        }
        if let Some(observation) = self.observation() {
            if let Some(error) = observation.io_error() {
                return Some(error);
            }
            if let Some(error) = observation.native_error() {
                return Some(error);
            }
        }
        match self.failure() {
            Some(PinnedShardLogReadFailure::Native(error)) => Some(error),
            _ => None,
        }
    }
}
fn log_read_fixed_fee() -> Result<(usize, usize, usize), StorageFetchFailure> {
    let values = [
        size_of::<LogReadScope>(),
        size_of::<PinnedShardLogReadComplete>(),
        size_of::<PinnedShardLogReadError>(),
        size_of::<ReadChecks<'static>>(),
        size_of::<Vec<super::ShardLogRecord>>(),
        size_of::<StorageFetchCost>(),
        size_of::<Option<Result<crate::PinnedShardFetchPlan, crate::PinnedShardFetchPlanError>>>(),
        size_of::<Option<Result<crate::PinnedShardFetchComplete, crate::PinnedShardFetchReadError>>>(
        ),
        size_of::<Option<ShardLogResult<super::ShardLogRecord>>>(),
        size_of::<Option<Result<(), TryReserveError>>>(),
        size_of::<Option<PinnedShardLogReadFailure>>(),
        size_of::<String>(),
        size_of::<crate::StoredBatch>(),
        size_of::<bytes::Bytes>(),
        size_of::<[usize; 2]>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Result<Result<(), PinnedShardLogReadFailure>, Box<dyn Any + Send>>>(),
    ];
    let strings = CANONICAL_STREAM_DIAGNOSTIC.len();
    let bytes = values
        .into_iter()
        .try_fold(strings, |sum, n| sum.checked_add(n))
        .ok_or(StorageFetchFailure::Capacity)?;
    Ok((
        bytes,
        values
            .len()
            .checked_add(1)
            .ok_or(StorageFetchFailure::Capacity)?,
        strings,
    ))
}
/// Plain read/canonical bridge ceiling, paid before the SAME global budget.
/// Never create a fresh budget per canonical record/log or recovery page.
pub fn pinned_shard_log_read_cost(
    limits: StorageFetchLimits,
) -> Result<StorageFetchCost, StorageFetchFailure> {
    // One global budget ceiling, plus separately named constructor and read
    // headers. Never sum two complete configured ceilings or reset counters.
    let native = crate::pinned_shard_fetch_cost(limits)?;
    let (read_bytes, read_entries, read_strings) = log_read_fixed_fee()?;
    let (open_bytes, open_entries, open_strings) = fixed_fee()?;
    let fixed = read_bytes
        .checked_add(open_bytes)
        .ok_or(StorageFetchFailure::Capacity)?;
    let entries = read_entries
        .checked_add(open_entries)
        .ok_or(StorageFetchFailure::Capacity)?;
    let strings = read_strings
        .checked_add(open_strings)
        .ok_or(StorageFetchFailure::Capacity)?;
    Ok(StorageFetchCost {
        declared_bytes: native
            .declared_bytes
            .checked_add(fixed)
            .ok_or(StorageFetchFailure::Capacity)?,
        entries: native
            .entries
            .checked_add(entries)
            .ok_or(StorageFetchFailure::Capacity)?,
        string_bytes: native
            .string_bytes
            .checked_add(strings)
            .ok_or(StorageFetchFailure::Capacity)?,
        work_bytes: native
            .work_bytes
            .checked_add(fixed)
            .and_then(|n| n.checked_add(entries))
            .ok_or(StorageFetchFailure::Capacity)?,
    })
}
impl LogReadScope {
    fn read_observation(&self) -> Option<&crate::PinnedShardFetchObservation> {
        match self.read.as_ref()? {
            Ok(read) => Some(read.observation()),
            Err(error) => Some(error.observation()),
        }
    }
    fn run(
        &mut self,
        pin: &crate::PinnedShardDirectory,
        start: u64,
        max_bytes: usize,
        max_records: usize,
        budget: &StorageFetchBudget,
        checks: &ReadChecks<'_>,
    ) -> Result<(), PinnedShardLogReadFailure> {
        let (fixed, entries, _) =
            log_read_fixed_fee().map_err(PinnedShardLogReadFailure::Native)?;
        budget
            .charge_values(fixed, entries, fixed)
            .map_err(PinnedShardLogReadFailure::Native)?;
        checkpoint_read(budget, checks)?;
        self.plan = Some(self.log.scope.opened.plan_fetch_pinned_until(
            super::topic_partition(),
            crate::LogicalOffset::new(start),
            max_bytes,
            max_records,
            budget,
            checks,
        ));
        read_completed(PinnedShardLogReadPhase::Planned);
        checkpoint_read(budget, checks)?;
        let plan = match self.plan.take() {
            Some(Ok(plan)) => plan,
            actual => {
                self.plan = actual;
                return Err(PinnedShardLogReadFailure::Plan);
            }
        };
        self.read = Some(
            self.log
                .scope
                .opened
                .read_fetch_plan_pinned_until(pin, plan, budget, checks),
        );
        read_completed(PinnedShardLogReadPhase::Read);
        checkpoint_read(budget, checks)?;
        let read = match self.read.as_ref() {
            Some(Ok(read)) => read,
            _ => return Err(PinnedShardLogReadFailure::Read),
        };
        let n = read.batches().len();
        let bytes = n.checked_mul(size_of::<super::ShardLogRecord>()).ok_or(
            PinnedShardLogReadFailure::Native(StorageFetchFailure::Capacity),
        )?;
        budget
            .charge_values(bytes, n, bytes)
            .map_err(PinnedShardLogReadFailure::Native)?;
        checkpoint_read(budget, checks)?;
        self.allocation = Some(self.records.try_reserve_exact(n));
        read_completed(PinnedShardLogReadPhase::Allocated);
        checkpoint_read(budget, checks)?;
        if matches!(self.allocation, Some(Err(_))) {
            return Err(PinnedShardLogReadFailure::Allocation);
        }
        let capacity = self.records.capacity();
        if capacity > budget.limits().max_plan_batches {
            return Err(PinnedShardLogReadFailure::Native(
                StorageFetchFailure::Capacity,
            ));
        }
        let excess = capacity.saturating_sub(n);
        let extra = excess
            .checked_mul(size_of::<super::ShardLogRecord>())
            .ok_or(PinnedShardLogReadFailure::Native(
                StorageFetchFailure::Capacity,
            ))?;
        budget
            .charge_values(extra, excess, extra)
            .map_err(PinnedShardLogReadFailure::Native)?;
        for batch in read.batches() {
            budget
                .charge_values(
                    size_of::<crate::StoredBatch>(),
                    1,
                    size_of::<crate::StoredBatch>() + 1,
                )
                .map_err(PinnedShardLogReadFailure::Native)?;
            checkpoint_read(budget, checks)?;
            self.mapped = Some(super::record_from_batch(batch.clone()));
            read_completed(PinnedShardLogReadPhase::Mapped);
            checkpoint_read(budget, checks)?;
            match self.mapped.take() {
                Some(Ok(record)) => self.records.push(record),
                actual => {
                    self.mapped = actual;
                    return Err(PinnedShardLogReadFailure::Canonical);
                }
            }
        }
        checkpoint_read(budget, checks)
    }
}
fn checkpoint_read(
    budget: &StorageFetchBudget,
    checks: &ReadChecks<'_>,
) -> Result<(), PinnedShardLogReadFailure> {
    budget.check().map_err(PinnedShardLogReadFailure::Native)?;
    let actual = checks.check();
    let post = budget.check();
    match (actual, post) {
        (Err(result), post) => Err(PinnedShardLogReadFailure::Checkpoint {
            result,
            native_postcheck: post.err(),
        }),
        (Ok(()), Err(error)) => Err(PinnedShardLogReadFailure::Native(error)),
        (Ok(()), Ok(())) => Ok(()),
    }
}
impl OpenedPinnedShardLog {
    /// Consume this actual log through a bounded pinned physical canonical read.
    ///
    /// Caller retains the SAME actual pin/Core parent through success/error Drop.
    /// Every plan, physical range and record uses the supplied cumulative native
    /// budget/original E and SAME mandatory composing checks. No reopen, copied
    /// parser, standalone-store factory or operational mutation fallback exists.
    ///
    /// # Errors
    /// Retains actual plan/read/allocation/mapping Results, partial buffers and
    /// canonical prefix before whole log LAST. Denies SAME counters and attempts
    /// composing health denial once, preserving independent panic payloads.
    pub fn read_from_until(
        self,
        pin: &crate::PinnedShardDirectory,
        start_sequence: u64,
        max_bytes: usize,
        max_records: usize,
        budget: &StorageFetchBudget,
        outer: &dyn crate::PinnedShardFetchChecks,
    ) -> Result<PinnedShardLogReadComplete, PinnedShardLogReadError> {
        let mut scope = LogReadScope {
            records: Vec::new(),
            plan: None,
            read: None,
            mapped: None,
            allocation: None,
            failure: None,
            panic_payload: None,
            budget_deny_panic: None,
            composing_deny_panic: None,
            denial_completed: false,
            log: self,
        };
        let checks = ReadChecks {
            outer,
            denial_attempted: std::cell::Cell::new(false),
            denial_completed: std::cell::Cell::new(false),
        };
        let actual = catch_unwind(AssertUnwindSafe(|| {
            scope.run(pin, start_sequence, max_bytes, max_records, budget, &checks)
        }));
        let failure = match actual {
            Ok(Ok(())) => None,
            Ok(Err(f)) => Some(f),
            Err(payload) => {
                scope.panic_payload = Some(payload);
                Some(PinnedShardLogReadFailure::Unwind)
            }
        };
        if let Some(failure) = failure {
            scope.failure = Some(failure);
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| budget.deny())) {
                scope.budget_deny_panic = Some(payload);
            }
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| checks.deny())) {
                scope.composing_deny_panic = Some(payload);
            }
            scope.denial_completed = checks.denial_completed.get();
            return Err(PinnedShardLogReadError { scope });
        }
        Ok(PinnedShardLogReadComplete { scope })
    }
}
