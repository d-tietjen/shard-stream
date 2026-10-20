//! Consuming canonical append/seal with private live retired-descriptor custody.
//! Provisional source draft: no Core, tracked compaction or serving claim.
use super::*;
use crate::{
    PinnedShardAppendBudget, PinnedShardAppendChecks, PinnedShardAppendError,
    PinnedShardAppendFailure, PinnedShardAppendInput, PinnedShardAppendLimits,
    PinnedShardAppendReport, PinnedShardDirectory, PinnedShardFetchChecks, TrackedWriteFailure,
    TrackedWriteLimits, TrackedWriteObservation, pinned_shard_append_cost, tracking_fixed_fee,
};
use bytes::Bytes;
use std::cell::{Cell, RefCell};

/// Immutable live owner ceilings, stored on first write and never widened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PinnedShardLogWriteLimits {
    /// Actual Files retained by the opened store plus worst entered creates.
    /// External native pin/Core parent has a separately prepaid lifetime.
    pub max_live_files: usize,
    /// Initial and subsequently created pack identity slots/capacity.
    pub max_tracked_packs: usize,
    /// Actual retired ActivePack destination slots/capacity.
    pub max_retired_slots: usize,
    /// Actual input Vec capacity, including its unused reserved slots.
    pub max_input_slots: usize,
}
impl PinnedShardLogWriteLimits {
    fn native(self) -> TrackedWriteLimits {
        TrackedWriteLimits {
            max_live_files: self.max_live_files,
            max_tracked_packs: self.max_tracked_packs,
            max_retired_slots: self.max_retired_slots,
            max_input_slots: self.max_input_slots,
        }
    }
}
/// Ordinary checks of SAME work and actual captured health, never stored.
pub trait PinnedShardLogWriteChecks: ShardDirectoryChecks {
    /// Deny SAME work and actual captured health before entered cleanup.
    fn deny(&self);
}
/// Terminal write/seal refusal; original inputs and native outcomes stay retained.
#[derive(Debug)]
pub enum PinnedShardLogWriteFailure {
    /// Actual shared counter/deadline fee refused; its typed result is retained.
    Budget,
    /// SAME composing checkpoint or original common expiry refused.
    Checkpoint,
    /// Actual input count/capacity or shared canonical arithmetic refused.
    Input,
    /// Actual retained allocation Result or permitted capacity refused.
    Allocation,
    /// Actual bounded native write/seal returned its retained error.
    Native,
    /// Actual descriptor transfer, identity or tracked directory check refused.
    Tracking,
    /// Shared canonical catalog validation refused after actual native effects.
    Canonical,
    /// Actual producer/checkpoint unwound; the original payload is retained.
    Unwind,
    /// An actual owning transition could not complete.
    Invariant,
}
struct Denial {
    native_pre: RefCell<Option<Result<(), StorageFetchFailure>>>,
    charge: RefCell<Option<Result<(), StorageFetchFailure>>>,
    expired: Cell<bool>,
    native_post: RefCell<Option<Result<(), StorageFetchFailure>>>,
    composing: Cell<Option<Result<(), ShardDirectoryCheckFailure>>>,
    native_denied: Cell<bool>,
    composing_denied: Cell<bool>,
    budget_panic: RefCell<Option<Box<dyn Any + Send>>>,
    composing_panic: RefCell<Option<Box<dyn Any + Send>>>,
}
impl Denial {
    fn new() -> Self {
        Self {
            native_pre: RefCell::new(None),
            charge: RefCell::new(None),
            expired: Cell::new(false),
            native_post: RefCell::new(None),
            composing: Cell::new(None),
            native_denied: Cell::new(false),
            composing_denied: Cell::new(false),
            budget_panic: RefCell::new(None),
            composing_panic: RefCell::new(None),
        }
    }
}
struct Adapter<'a> {
    expiry: Instant,
    budget: &'a StorageFetchBudget,
    checks: &'a dyn PinnedShardLogWriteChecks,
    state: &'a Denial,
}
impl ShardDirectoryChecks for Adapter<'_> {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        if Instant::now() >= self.expiry {
            self.state.expired.set(true);
            return Err(ShardDirectoryCheckFailure::Refused);
        }
        if self.state.native_denied.get() || self.state.composing_denied.get() {
            return Err(ShardDirectoryCheckFailure::Refused);
        }
        *self.state.native_pre.borrow_mut() = Some(self.budget.check());
        if self
            .state
            .native_pre
            .borrow()
            .as_ref()
            .is_some_and(Result::is_err)
        {
            return Err(ShardDirectoryCheckFailure::Refused);
        }
        let actual = self.checks.check();
        self.state.composing.set(Some(actual));
        *self.state.native_post.borrow_mut() = Some(self.budget.check());
        if Instant::now() >= self.expiry {
            self.state.expired.set(true);
        }
        if self.state.expired.get()
            || actual.is_err()
            || self
                .state
                .native_post
                .borrow()
                .as_ref()
                .is_some_and(Result::is_err)
        {
            Err(ShardDirectoryCheckFailure::Refused)
        } else {
            Ok(())
        }
    }
}
impl Adapter<'_> {
    fn deny_once(&self) {
        // Latch first; both actual denial calls independently settle/retain their
        // original payload, so a panic never skips the other composing owner.
        if !self.state.native_denied.replace(true)
            && let Err(p) = catch_unwind(AssertUnwindSafe(|| self.budget.deny()))
        {
            *self.state.budget_panic.borrow_mut() = Some(p);
        }
        if !self.state.composing_denied.replace(true)
            && let Err(p) = catch_unwind(AssertUnwindSafe(|| self.checks.deny()))
        {
            *self.state.composing_panic.borrow_mut() = Some(p);
        }
    }
}
impl PinnedShardAppendChecks for Adapter<'_> {
    fn deny(&self) {
        self.deny_once();
    }
}
impl PinnedShardFetchChecks for Adapter<'_> {
    fn deny(&self) {
        self.deny_once();
    }
}

struct WriteScope {
    common_expiry: Instant,
    records: Vec<Bytes>,
    inputs: Vec<PinnedShardAppendInput>,
    input_allocation: Option<Result<(), TryReserveError>>,
    sequence_allocation: Option<Result<(), TryReserveError>>,
    mapping_result: Option<ShardLogResult<u64>>,
    native_result: Option<Result<PinnedShardAppendReport, PinnedShardAppendError>>,
    tracking_result: Option<Result<(), TrackedWriteFailure>>,
    tracking: TrackedWriteObservation,
    sequences: Vec<u64>,
    reservation: Option<Reservation>,
    catalog_result: Option<ShardLogResult<u64>>,
    bounds_result: Option<ShardLogResult<Option<(u64, u64)>>>,
    receipt: Option<super::super::ShardLogAppendReceipt>,
    failure: Option<PinnedShardLogWriteFailure>,
    panic: Option<Box<dyn Any + Send>>,
    catch_result: Option<Result<Result<(), PinnedShardLogWriteFailure>, Box<dyn Any + Send>>>,
    denial: Denial,
    usage: Option<Result<StorageFetchUsage, StorageFetchFailure>>,
    // LAST: inputs/raw effects/partial moves/panics physically drop while the
    // actual log and every moved live descriptor still have their real owner.
    log: OpenedPinnedShardLog,
}
impl WriteScope {
    fn native_report(&self) -> Option<&PinnedShardAppendReport> {
        match self.native_result.as_ref()? {
            Ok(report) => Some(report),
            Err(error) => Some(error.report()),
        }
    }
}
/// Safe diagnostics: no native report/error wrapper, File or store escapes.
pub struct PinnedShardLogWriteObservation<'a> {
    scope: &'a WriteScope,
}
impl PinnedShardLogWriteObservation<'_> {
    /// A receipt is visible only after the entire canonical transition succeeds.
    pub fn receipt(&self) -> Option<&super::super::ShardLogAppendReceipt> {
        if self.scope.failure.is_none() {
            self.scope.receipt.as_ref()
        } else {
            None
        }
    }
    /// Actual typed native refusal scalar, with the full private owner retained.
    pub fn native_failure(&self) -> Option<PinnedShardAppendFailure> {
        self.scope
            .native_result
            .as_ref()
            .and_then(|r| r.as_ref().err())
            .map(PinnedShardAppendError::failure)
    }
    /// Count of actual completed retired-file moves in the retained ledger.
    pub fn transferred_files(&self) -> usize {
        self.scope.tracking.transferred_count()
    }
    /// Actual source/destination indices and completion status; no File escapes.
    pub fn transfer_observations(&self) -> impl Iterator<Item = (usize, usize, Option<bool>)> + '_ {
        self.scope.tracking.transfer_observations()
    }
    /// Actual retained bridge tracking-budget refusal, without reconstruction.
    pub fn tracking_budget_error(&self) -> Option<&StorageFetchFailure> {
        match self.scope.tracking_result.as_ref()? {
            Err(TrackedWriteFailure::Budget(error)) => Some(error),
            _ => None,
        }
    }
    /// Actual requested allocation refusal from native, bridge or tracking state.
    pub fn allocation_error(&self) -> Option<&TryReserveError> {
        self.scope
            .input_allocation
            .as_ref()
            .and_then(|r| r.as_ref().err())
            .or_else(|| {
                self.scope
                    .sequence_allocation
                    .as_ref()
                    .and_then(|r| r.as_ref().err())
            })
            .or_else(|| self.scope.tracking.allocation_error())
            .or_else(|| {
                self.scope
                    .native_report()
                    .and_then(PinnedShardAppendReport::allocation_error)
            })
    }
    /// Borrow original direct native write results without exposing their Files.
    pub fn write_results(&self) -> impl Iterator<Item = &std::io::Result<usize>> {
        self.scope
            .native_report()
            .into_iter()
            .flat_map(PinnedShardAppendReport::write_results)
    }
    /// Borrow original direct held/named metadata results after tracked moves.
    pub fn metadata_results(&self) -> impl Iterator<Item = &std::io::Result<rustix::fs::Stat>> {
        self.scope.tracking.stats()
    }
    /// The terminal bridge refusal, if any.
    pub fn failure(&self) -> Option<&PinnedShardLogWriteFailure> {
        self.scope.failure.as_ref()
    }
    /// Actual cumulative shared counters, including a retained refused result.
    pub fn usage(&self) -> Option<&Result<StorageFetchUsage, StorageFetchFailure>> {
        self.scope.usage.as_ref()
    }
    /// Actual input Vec capacity, including unused caller-reserved slots.
    pub fn original_record_capacity(&self) -> usize {
        self.scope.records.capacity()
    }
}
/// Whole log and every operation payload/result; success contains no report FD.
pub struct PinnedShardLogWriteComplete {
    scope: WriteScope,
}
impl PinnedShardLogWriteComplete {
    // Library-private borrowing only: the successful actual seal completion
    // stays intact and LAST in the consuming GC carrier. No native parts escape.
    pub(super) fn retained_log_mut(&mut self) -> &mut OpenedPinnedShardLog {
        &mut self.scope.log
    }
    pub(super) fn retained_log(&self) -> &OpenedPinnedShardLog {
        &self.scope.log
    }
    /// Safe observations borrow this actual whole operation owner.
    pub fn observation(&self) -> PinnedShardLogWriteObservation<'_> {
        PinnedShardLogWriteObservation { scope: &self.scope }
    }
    /// Release only settled non-FD operation state; all actual retired/active
    /// descriptors remain in this SAME whole log, without reopening or cloning.
    pub fn into_log(self) -> OpenedPinnedShardLog {
        let WriteScope { log, .. } = self.scope;
        log
    }
}
/// Terminal refusal keeps all actual effects and whole log; no retry/extractor.
pub struct PinnedShardLogWriteError {
    scope: WriteScope,
}
impl PinnedShardLogWriteError {
    /// Safe observations borrow this actual whole operation owner.
    pub fn observation(&self) -> PinnedShardLogWriteObservation<'_> {
        PinnedShardLogWriteObservation { scope: &self.scope }
    }
}
impl fmt::Debug for PinnedShardLogWriteComplete {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardLogWriteComplete")
            .field("receipt", &self.scope.receipt)
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for PinnedShardLogWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardLogWriteError")
            .field("failure", &self.scope.failure)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for PinnedShardLogWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "bounded pinned canonical write refused: {:?}",
            self.scope.failure
        )
    }
}
// No source downcast may expose the File-bearing actual native error wrapper.
impl Error for PinnedShardLogWriteError {}

fn fixed_fee() -> Option<(usize, usize, usize)> {
    let fields = [
        size_of::<WriteScope>(),
        size_of::<PinnedShardLogWriteComplete>(),
        size_of::<PinnedShardLogWriteError>(),
        size_of::<PinnedShardLogWriteObservation<'static>>(),
        size_of::<Vec<Bytes>>(),
        size_of::<Vec<PinnedShardAppendInput>>(),
        size_of::<Vec<u64>>(),
        size_of::<Option<Result<(), TryReserveError>>>(),
        size_of::<Option<Result<(), TryReserveError>>>(),
        size_of::<Option<ShardLogResult<u64>>>(),
        size_of::<Option<Result<PinnedShardAppendReport, PinnedShardAppendError>>>(),
        size_of::<Option<Result<(), TrackedWriteFailure>>>(),
        size_of::<Option<Reservation>>(),
        size_of::<Option<ShardLogResult<u64>>>(),
        size_of::<Option<ShardLogResult<Option<(u64, u64)>>>>(),
        size_of::<Option<super::super::ShardLogAppendReceipt>>(),
        size_of::<Option<PinnedShardLogWriteFailure>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Option<Result<Result<(), PinnedShardLogWriteFailure>, Box<dyn Any + Send>>>>(),
        size_of::<Denial>(),
        size_of::<Instant>(),
        // Three independent real flags: expired/native_denied/composing_denied.
        size_of::<Cell<bool>>(),
        size_of::<Cell<bool>>(),
        size_of::<Cell<bool>>(),
        // Actual native_pre/charge/native_post RefCells and their typed slots.
        size_of::<RefCell<Option<Result<(), StorageFetchFailure>>>>(),
        size_of::<RefCell<Option<Result<(), StorageFetchFailure>>>>(),
        size_of::<RefCell<Option<Result<(), StorageFetchFailure>>>>(),
        size_of::<Option<Result<(), StorageFetchFailure>>>(),
        size_of::<Option<Result<(), StorageFetchFailure>>>(),
        size_of::<Option<Result<(), StorageFetchFailure>>>(),
        // Actual composing Cell and retained Copy result slot.
        size_of::<Cell<Option<Result<(), ShardDirectoryCheckFailure>>>>(),
        size_of::<Option<Result<(), ShardDirectoryCheckFailure>>>(),
        // Both independent denial-panic RefCells, all three optional payload
        // slots (including Scope.panic), and both denial catch-result headers.
        // Three independent Box<Any+Send> value fees already appear above.
        size_of::<RefCell<Option<Box<dyn Any + Send>>>>(),
        size_of::<RefCell<Option<Box<dyn Any + Send>>>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Result<(), Box<dyn Any + Send>>>(),
        size_of::<Result<(), Box<dyn Any + Send>>>(),
        size_of::<Option<Result<StorageFetchUsage, StorageFetchFailure>>>(),
        size_of::<String>(),
    ];
    let diagnostic = super::super::CANONICAL_STREAM_DIAGNOSTIC
        .len()
        .max(super::super::RETAINED_GAP_DIAGNOSTIC.len())
        .max(super::super::SEQUENCE_OVERFLOW_DIAGNOSTIC.len())
        .max("ShardLog append group cannot be empty".len());
    let (tracking, tracking_entries) = tracking_fixed_fee()?;
    Some((
        fields
            .iter()
            .try_fold(diagnostic, |n, v| n.checked_add(*v))?
            .checked_add(tracking)?,
        fields.len().checked_add(1)?.checked_add(tracking_entries)?,
        diagnostic,
    ))
}
/// Caller prepayment for one bridge/native allowance plus the SAME global budget.
/// Allocator/kernel/RSS costs and external actual pin/Core parent are separate.
pub fn pinned_shard_log_write_cost(
    fetch: StorageFetchLimits,
    native: PinnedShardAppendLimits,
    limits: PinnedShardLogWriteLimits,
) -> Option<StorageFetchCost> {
    if limits.max_live_files == 0
        || limits.max_tracked_packs == 0
        || limits.max_retired_slots == 0
        || limits.max_input_slots == 0
    {
        return None;
    }
    let mut cost = storage_fetch_cost(fetch).ok()?;
    let native = pinned_shard_append_cost(native)?;
    let (fixed, entries, strings) = fixed_fee()?;
    cost.declared_bytes = cost
        .declared_bytes
        .checked_add(native.declared_bytes)?
        .checked_add(fixed)?;
    cost.entries = cost
        .entries
        .checked_add(native.entries)?
        .checked_add(entries)?;
    cost.string_bytes = cost
        .string_bytes
        .checked_add(native.string_bytes)?
        .checked_add(strings)?;
    cost.work_bytes = cost
        .work_bytes
        .checked_add(native.work_bytes)?
        .checked_add(fixed)?
        .checked_add(entries)?;
    Some(cost)
}
fn checked(_b: &StorageFetchBudget, c: &Adapter<'_>) -> Result<(), PinnedShardLogWriteFailure> {
    c.check()
        .map_err(|_| PinnedShardLogWriteFailure::Checkpoint)
}
fn debit(
    b: &StorageFetchBudget,
    c: &Adapter<'_>,
    n: usize,
    e: usize,
    w: usize,
) -> Result<(), PinnedShardLogWriteFailure> {
    *c.state.charge.borrow_mut() = Some(b.charge_values(n, e, w));
    if c.state.charge.borrow().as_ref().is_some_and(Result::is_err) {
        Err(PinnedShardLogWriteFailure::Budget)
    } else {
        Ok(())
    }
}
fn times(a: usize, b: usize) -> Result<usize, PinnedShardLogWriteFailure> {
    a.checked_mul(b).ok_or(PinnedShardLogWriteFailure::Budget)
}
fn sum(a: usize, b: usize) -> Result<usize, PinnedShardLogWriteFailure> {
    a.checked_add(b).ok_or(PinnedShardLogWriteFailure::Budget)
}
impl WriteScope {
    #[allow(
        clippy::too_many_arguments,
        reason = "keep the actual pin, native budget and composing budget/checks borrowed explicitly"
    )]
    fn run(
        &mut self,
        pin: &PinnedShardDirectory,
        first: u64,
        sync: bool,
        seal: bool,
        native: &mut PinnedShardAppendBudget,
        b: &StorageFetchBudget,
        limits: PinnedShardLogWriteLimits,
        c: &dyn PinnedShardLogWriteChecks,
    ) -> Result<(), PinnedShardLogWriteFailure> {
        let checks = Adapter {
            expiry: self.common_expiry,
            budget: b,
            checks: c,
            state: &self.denial,
        };
        checked(b, &checks)?;
        let (fixed, entries, _) = fixed_fee().ok_or(PinnedShardLogWriteFailure::Budget)?;
        debit(b, &checks, fixed, entries, fixed)?;
        completed(WritePhase::FixedPaid);
        checked(b, &checks)?;
        if self.records.capacity() > limits.max_input_slots
            || self.records.len() > native.limits().max_frames
        {
            return Err(PinnedShardLogWriteFailure::Input);
        }
        // The actual caller Vec's excess capacity is retained and paid too.
        let input_bytes = times(self.records.capacity(), size_of::<Bytes>())?;
        debit(
            b,
            &checks,
            input_bytes,
            self.records.capacity(),
            input_bytes,
        )?;
        checked(b, &checks)?;
        let physical =
            pinned_shard_append_cost(native.limits()).ok_or(PinnedShardLogWriteFailure::Budget)?;
        debit(
            b,
            &checks,
            physical.declared_bytes,
            physical.entries,
            physical.work_bytes,
        )?;
        native.cap_expiry(b.absolute_expiry());
        checked(b, &checks)?;
        self.tracking_result = Some(self.log.scope.opened.prepare_write_tracking(
            pin,
            limits.native(),
            native,
            b,
            &checks,
            &mut self.tracking,
        ));
        checked(b, &checks)?;
        if !matches!(self.tracking_result, Some(Ok(()))) {
            return Err(PinnedShardLogWriteFailure::Tracking);
        }
        if !seal {
            self.mapping_result = Some(super::super::append_group_last_sequence(
                first,
                &self.records,
            ));
            completed(WritePhase::Mapped);
            checked(b, &checks)?;
            if self.mapping_result.as_ref().is_some_and(Result::is_err) {
                return Err(PinnedShardLogWriteFailure::Input);
            }
            // The actual input element, cloned Bytes value and reservation are
            // separately named source-visible values, not an implicit carrier
            // slot. Unused Vec capacity is charged separately below.
            let width = sum(
                size_of::<PinnedShardAppendInput>(),
                sum(size_of::<Bytes>(), size_of::<Reservation>())?,
            )?;
            let values = times(self.records.len(), width)?;
            debit(b, &checks, values, times(self.records.len(), 3)?, values)?;
            checked(b, &checks)?;
            self.input_allocation = Some(self.inputs.try_reserve_exact(self.records.len()));
            completed(WritePhase::Allocated);
            checked(b, &checks)?;
            if matches!(self.input_allocation, Some(Err(_))) {
                return Err(PinnedShardLogWriteFailure::Allocation);
            }
            let excess = self.inputs.capacity().saturating_sub(self.records.len());
            debit(
                b,
                &checks,
                times(excess, size_of::<PinnedShardAppendInput>())?,
                excess,
                times(excess, size_of::<PinnedShardAppendInput>())?,
            )?;
            if self.inputs.capacity() > native.limits().max_frames {
                return Err(PinnedShardLogWriteFailure::Allocation);
            }
            let mut payload_bytes = 0u64;
            for (i, payload) in self.records.iter().enumerate() {
                checked(b, &checks)?;
                self.mapping_result = Some(super::super::append_group_sequence(first, i));
                checked(b, &checks)?;
                let sequence = *self
                    .mapping_result
                    .as_ref()
                    .and_then(|r| r.as_ref().ok())
                    .ok_or(PinnedShardLogWriteFailure::Input)?;
                payload_bytes = payload_bytes
                    .checked_add(
                        u64::try_from(payload.len())
                            .map_err(|_| PinnedShardLogWriteFailure::Input)?,
                    )
                    .ok_or(PinnedShardLogWriteFailure::Input)?;
                self.inputs.push(PinnedShardAppendInput {
                    reservation: super::super::reservation(sequence),
                    producer: None,
                    atomic_group: None,
                    payload: payload.clone(),
                });
            }
            self.receipt = Some(super::super::ShardLogAppendReceipt {
                first_sequence: first,
                last_sequence: *self
                    .mapping_result
                    .as_ref()
                    .and_then(|r| r.as_ref().ok())
                    .ok_or(PinnedShardLogWriteFailure::Input)?,
                record_count: self.records.len(),
                payload_bytes,
            });
        }
        checked(b, &checks)?;
        // Native result is placed in this outside-catch owner BEFORE any hook
        // or post-E check. The native producer settles entered work on refusal.
        self.native_result = Some(if seal {
            self.log
                .scope
                .opened
                .store_mut()
                .seal_active_pinned_until(pin, native, &checks)
        } else {
            self.log.scope.opened.store_mut().append_group_pinned_until(
                pin,
                std::mem::take(&mut self.inputs),
                sync,
                native,
                &checks,
            )
        });
        completed(WritePhase::NativeCompleted);
        checked(b, &checks)?;
        let report = self
            .native_result
            .as_mut()
            .and_then(|r| r.as_mut().ok())
            .ok_or(PinnedShardLogWriteFailure::Native)?;
        self.tracking_result = Some(self.log.scope.opened.finish_write_tracking(
            pin,
            report,
            b,
            &checks,
            &mut self.tracking,
        ));
        completed(WritePhase::TrackingCompleted);
        checked(b, &checks)?;
        if !matches!(self.tracking_result, Some(Ok(()))) {
            return Err(PinnedShardLogWriteFailure::Tracking);
        }
        // Recompute bounds by the same canonical pure predicates. Do not add
        // first==old_next policy: ordinary append may produce a later gap.
        let shape = self
            .log
            .scope
            .opened
            .store()
            .log_catalog_shape()
            .ok_or(PinnedShardLogWriteFailure::Budget)?;
        if shape.0 > b.limits().max_plan_batches {
            return Err(PinnedShardLogWriteFailure::Budget);
        }
        let levels = (usize::BITS as usize - shape.0.leading_zeros() as usize).max(1);
        let bytes = times(shape.0, size_of::<u64>())?;
        debit(
            b,
            &checks,
            bytes,
            shape.0,
            sum(shape.2, sum(bytes, times(bytes, levels)?)?)?,
        )?;
        checked(b, &checks)?;
        self.sequence_allocation = Some(self.sequences.try_reserve_exact(shape.0));
        completed(WritePhase::Allocated);
        checked(b, &checks)?;
        if matches!(self.sequence_allocation, Some(Err(_))) {
            return Err(PinnedShardLogWriteFailure::Allocation);
        }
        let excess = self.sequences.capacity().saturating_sub(shape.0);
        debit(
            b,
            &checks,
            times(excess, size_of::<u64>())?,
            excess,
            times(excess, size_of::<u64>())?,
        )?;
        if self.sequences.capacity() > b.limits().max_plan_batches {
            return Err(PinnedShardLogWriteFailure::Allocation);
        }
        for reservation in self.log.scope.opened.store().log_catalog_reservations() {
            checked(b, &checks)?;
            self.reservation = Some(reservation);
            self.catalog_result = Some(super::super::validate_catalog_entry(reservation));
            completed(WritePhase::Mapped);
            checked(b, &checks)?;
            self.sequences.push(
                *self
                    .catalog_result
                    .as_ref()
                    .and_then(|r| r.as_ref().ok())
                    .ok_or(PinnedShardLogWriteFailure::Canonical)?,
            );
        }
        self.bounds_result = Some(super::super::retained_bounds(&mut self.sequences));
        completed(WritePhase::Bounds);
        checked(b, &checks)?;
        self.log.bounds = *self
            .bounds_result
            .as_ref()
            .and_then(|r| r.as_ref().ok())
            .ok_or(PinnedShardLogWriteFailure::Canonical)?;
        self.usage = Some(b.usage());
        checked(b, &checks)
    }
}
impl OpenedPinnedShardLog {
    /// Consume this actual log and append using the unchanged native group policy.
    ///
    /// Prepay the plain bridge/native ceilings first, retaining the actual pin
    /// and Core parent LAST. All plan/read/write passes use the same global
    /// budget; no startup budget or composing checks enter the completed owner.
    /// A late/gap/error result retains all real effects and has no retry escape.
    #[allow(
        clippy::result_large_err,
        reason = "retain original payloads, native write outcomes and whole log without a failure allocation"
    )]
    #[allow(
        clippy::too_many_arguments,
        reason = "keep payloads, durability, actual pin and original budgets/checks explicit"
    )]
    pub fn append_group_until(
        self,
        pin: &PinnedShardDirectory,
        first_sequence: u64,
        records: Vec<Bytes>,
        durable: bool,
        native: &mut PinnedShardAppendBudget,
        budget: &StorageFetchBudget,
        limits: PinnedShardLogWriteLimits,
        checks: &dyn PinnedShardLogWriteChecks,
    ) -> Result<PinnedShardLogWriteComplete, PinnedShardLogWriteError> {
        write_entry(
            self,
            pin,
            first_sequence,
            records,
            durable,
            false,
            native,
            budget,
            limits,
            checks,
        )
    }
    /// Consume this actual log and seal via the genuine bounded native producer.
    /// Successful retired Files join the same private live catalog. Ordinary
    /// mutation or untracked GC is refused; tracked GC is a later producer.
    #[allow(
        clippy::result_large_err,
        reason = "retain actual seal outcomes and whole log without allocating during refusal"
    )]
    pub fn seal_until(
        self,
        pin: &PinnedShardDirectory,
        native: &mut PinnedShardAppendBudget,
        budget: &StorageFetchBudget,
        limits: PinnedShardLogWriteLimits,
        checks: &dyn PinnedShardLogWriteChecks,
    ) -> Result<PinnedShardLogWriteComplete, PinnedShardLogWriteError> {
        write_entry(
            self,
            pin,
            0,
            Vec::new(),
            false,
            true,
            native,
            budget,
            limits,
            checks,
        )
    }
}
#[allow(
    clippy::result_large_err,
    reason = "retain original inputs, actual write outcomes and whole log without a failure allocation"
)]
#[allow(
    clippy::too_many_arguments,
    reason = "keep write mode, durability, actual pin and original budgets/checks explicit"
)]
fn write_entry(
    log: OpenedPinnedShardLog,
    pin: &PinnedShardDirectory,
    first: u64,
    records: Vec<Bytes>,
    sync: bool,
    seal: bool,
    native: &mut PinnedShardAppendBudget,
    budget: &StorageFetchBudget,
    limits: PinnedShardLogWriteLimits,
    checks: &dyn PinnedShardLogWriteChecks,
) -> Result<PinnedShardLogWriteComplete, PinnedShardLogWriteError> {
    let mut scope = WriteScope {
        common_expiry: native.absolute_expiry().min(budget.absolute_expiry()),
        records,
        inputs: Vec::new(),
        input_allocation: None,
        sequence_allocation: None,
        mapping_result: None,
        native_result: None,
        tracking_result: None,
        tracking: TrackedWriteObservation::new(),
        sequences: Vec::new(),
        reservation: None,
        catalog_result: None,
        bounds_result: None,
        receipt: None,
        failure: None,
        panic: None,
        catch_result: None,
        denial: Denial::new(),
        usage: None,
        log,
    };
    scope.catch_result = Some(catch_unwind(AssertUnwindSafe(|| {
        scope.run(pin, first, sync, seal, native, budget, limits, checks)
    })));
    let failure = match scope.catch_result.take() {
        Some(Ok(Ok(()))) => None,
        Some(Ok(Err(f))) => Some(f),
        Some(Err(p)) => {
            scope.panic = Some(p);
            Some(PinnedShardLogWriteFailure::Unwind)
        }
        None => Some(PinnedShardLogWriteFailure::Invariant),
    };
    if let Some(failure) = failure {
        scope.failure = Some(failure);
        native.deny();
        Adapter {
            expiry: scope.common_expiry,
            budget,
            checks,
            state: &scope.denial,
        }
        .deny_once();
        scope.usage = Some(budget.usage());
        return Err(PinnedShardLogWriteError { scope });
    }
    Ok(PinnedShardLogWriteComplete { scope })
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WritePhase {
    FixedPaid,
    Mapped,
    Allocated,
    NativeCompleted,
    TrackingCompleted,
    Bounds,
}
fn completed(phase: WritePhase) {
    #[cfg(test)]
    tests::completed(phase);
    #[cfg(not(test))]
    let _ = phase;
}
#[cfg(test)]
#[path = "api_pinned_write_tests.rs"]
pub(crate) mod tests;
