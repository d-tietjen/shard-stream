//! Consuming tracked canonical compaction on the actual opened log and Files.
//! Source draft only; native pin and genuine Core parent remain caller-owned LAST.
use super::write::{
    PinnedShardLogWriteChecks, PinnedShardLogWriteComplete, PinnedShardLogWriteError,
    PinnedShardLogWriteLimits, pinned_shard_log_write_cost,
};
use super::*;
use crate::{
    LogicalOffset, PinnedShardAppendBudget, PinnedShardAppendLimits, PinnedShardDirectory,
    PinnedShardGcChecks, PinnedShardGcError, PinnedShardGcFailure, PinnedShardGcLimits,
    PinnedShardGcReport, TopicPartition, TrackedGcTrace, tracking_gc_cost,
};
use std::cell::{Cell, RefCell, RefMut};
use std::collections::HashMap;

/// SAME ordinary operation/health checks; denial grants no authority.
pub trait PinnedShardLogGcChecks: ShardDirectoryChecks {
    /// Deny the captured operation and actual health BEFORE native settlement.
    fn deny(&self);
}
/// Actual canonical compaction refusal, with the whole private owner retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PinnedShardLogGcFailure {
    /// Original min E or cumulative global accounting refused.
    Budget,
    /// Actual composing check refused.
    Checkpoint,
    /// Genuine bounded seal refused; its whole typed error remains private.
    Seal,
    /// Real reservation/capacity refused.
    Allocation,
    /// Actual native collector returned a retained error or partial effect.
    Native,
    /// Descriptor quarantine, source retag or epoch transition refused.
    Tracking,
    /// Shared ordinary canonical predicate refused after actual effects.
    Canonical,
    /// Actual callback/producer unwound; no completion is manufactured.
    Unwind,
    /// An actual owner transition disagreed with retained state.
    Invariant,
}
struct Denial {
    pre: RefCell<Option<Result<(), StorageFetchFailure>>>,
    charged: RefCell<Option<Result<(), StorageFetchFailure>>>,
    post: RefCell<Option<Result<(), StorageFetchFailure>>>,
    composing: Cell<Option<Result<(), ShardDirectoryCheckFailure>>>,
    expired: Cell<bool>,
    native_denied: Cell<bool>,
    composing_denied: Cell<bool>,
    native_panic: RefCell<Option<Box<dyn Any + Send>>>,
    composing_panic: RefCell<Option<Box<dyn Any + Send>>>,
}
impl Denial {
    fn new() -> Self {
        Self {
            pre: RefCell::new(None),
            charged: RefCell::new(None),
            post: RefCell::new(None),
            composing: Cell::new(None),
            expired: Cell::new(false),
            native_denied: Cell::new(false),
            composing_denied: Cell::new(false),
            native_panic: RefCell::new(None),
            composing_panic: RefCell::new(None),
        }
    }
}
struct Adapter<'a> {
    expiry: Instant,
    budget: &'a StorageFetchBudget,
    checks: &'a dyn PinnedShardLogGcChecks,
    state: &'a Denial,
}
impl ShardDirectoryChecks for Adapter<'_> {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        if Instant::now() >= self.expiry {
            self.state.expired.set(true);
        }
        if self.state.expired.get()
            || self.state.native_denied.get()
            || self.state.composing_denied.get()
        {
            return Err(ShardDirectoryCheckFailure::Refused);
        }
        *self.state.pre.borrow_mut() = Some(self.budget.check());
        if self.state.pre.borrow().as_ref().is_some_and(Result::is_err) {
            return Err(ShardDirectoryCheckFailure::Refused);
        }
        self.state.composing.set(Some(self.checks.check()));
        *self.state.post.borrow_mut() = Some(self.budget.check());
        if Instant::now() >= self.expiry {
            self.state.expired.set(true);
        }
        if self.state.expired.get()
            || matches!(self.state.composing.get(), Some(Err(_)))
            || self
                .state
                .post
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
        if !self.state.native_denied.replace(true)
            && let Err(p) = catch_unwind(AssertUnwindSafe(|| self.budget.deny()))
        {
            *self.state.native_panic.borrow_mut() = Some(p);
        }
        if !self.state.composing_denied.replace(true)
            && let Err(p) = catch_unwind(AssertUnwindSafe(|| self.checks.deny()))
        {
            *self.state.composing_panic.borrow_mut() = Some(p);
        }
    }
}
impl PinnedShardGcChecks for Adapter<'_> {
    fn deny(&self) {
        self.deny_once();
    }
}
impl PinnedShardLogWriteChecks for Adapter<'_> {
    fn deny(&self) {
        self.deny_once();
    }
}
// Actual append-denial outcome survives the ephemeral borrowed GC adapter.
// This state is before the native result and whole original log in GcScope.
struct AppendDenial {
    attempted: Cell<bool>,
    returned: Cell<bool>,
    result: RefCell<Option<Result<(), Box<dyn Any + Send>>>>,
}
impl AppendDenial {
    fn new() -> Self {
        Self {
            attempted: Cell::new(false),
            returned: Cell::new(false),
            result: RefCell::new(None),
        }
    }
    fn deny_direct(&self, append: &mut PinnedShardAppendBudget) {
        if !self.attempted.replace(true) {
            let actual = catch_unwind(AssertUnwindSafe(|| {
                append.deny();
                self.returned.set(true);
                append_denied(append.is_refused());
            }));
            *self.result.borrow_mut() = Some(actual);
        }
    }
}
// Constructed only AFTER seal's exclusive append borrow ends. No reference to
// this adapter or the borrowed append counter is retained by an outcome owner.
struct GcChecks<'append, 'prior, 'inputs> {
    append: RefCell<&'append mut PinnedShardAppendBudget>,
    prior: &'prior Adapter<'inputs>,
    state: &'prior AppendDenial,
}
impl ShardDirectoryChecks for GcChecks<'_, '_, '_> {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        self.prior.check()
    }
}
impl PinnedShardGcChecks for GcChecks<'_, '_, '_> {
    fn deny(&self) {
        if !self.state.attempted.replace(true) {
            let actual = catch_unwind(AssertUnwindSafe(|| {
                let refused = {
                    let mut append = self.append.borrow_mut();
                    append.deny();
                    self.state.returned.set(true);
                    append.is_refused()
                }; // Release the genuine RefMut BEFORE any observer/callback.
                append_denied(refused);
            }));
            *self.state.result.borrow_mut() = Some(actual);
        }
        // An append-denial/observer unwind never skips either existing action.
        self.prior.deny_once();
    }
}
fn append_denied(refused: bool) {
    #[cfg(test)]
    tests::append_denied(refused);
    #[cfg(not(test))]
    let _ = refused;
}
struct GcScope {
    expiry: Instant,
    sequence: u64,
    floors: HashMap<TopicPartition, LogicalOffset>,
    floor_allocation: Option<Result<(), TryReserveError>>,
    replaced_floor: Option<LogicalOffset>,
    sequences: Vec<u64>,
    sequence_allocation: Option<Result<(), TryReserveError>>,
    prepare: Option<Result<(), PinnedShardGcFailure>>,
    tracking: TrackedGcTrace,
    native: Option<Result<PinnedShardGcReport, PinnedShardGcError>>,
    transfer: Option<Result<(), PinnedShardGcFailure>>,
    reservation: Option<Reservation>,
    catalog_result: Option<ShardLogResult<u64>>,
    bounds: Option<ShardLogResult<Option<(u64, u64)>>>,
    receipt: Option<super::super::ShardLogCompactionReceipt>,
    failure: Option<PinnedShardLogGcFailure>,
    panic: Option<Box<dyn Any + Send>>,
    catch_result: Option<Result<Result<(), PinnedShardLogGcFailure>, Box<dyn Any + Send>>>,
    denial: Denial,
    append_denial: AppendDenial,
    usage: Option<Result<StorageFetchUsage, StorageFetchFailure>>,
    // The genuine completed seal owns every actual write outcome and SAME log.
    // Tracking quarantine/raw results above are destroyed BEFORE either owner.
    seal: Option<Result<PinnedShardLogWriteComplete, PinnedShardLogWriteError>>,
    unsealed: Option<OpenedPinnedShardLog>,
}
impl GcScope {
    fn report(&self) -> Option<&PinnedShardGcReport> {
        match self.native.as_ref()? {
            Ok(r) => Some(r),
            Err(e) => Some(e.report()),
        }
    }
}
/// Safe diagnostics with no native File-bearing wrapper, descriptor or parts.
pub struct PinnedShardLogGcObservation<'a> {
    scope: &'a GcScope,
}
impl PinnedShardLogGcObservation<'_> {
    /// Only a completely successful transition supplies the actual receipt.
    pub fn receipt(&self) -> Option<&super::super::ShardLogCompactionReceipt> {
        if self.scope.failure.is_none() {
            self.scope.receipt.as_ref()
        } else {
            None
        }
    }
    /// Native scalar refusal; actual native report/error stays private.
    pub fn native_failure(&self) -> Option<PinnedShardGcFailure> {
        self.scope
            .native
            .as_ref()?
            .as_ref()
            .err()
            .map(PinnedShardGcError::failure)
    }
    /// Actual reserved/entered native call facts, without File access.
    pub fn native_usage(&self) -> Option<crate::PinnedShardGcUsage> {
        self.scope.report().map(PinnedShardGcReport::usage)
    }
    /// Actual direct held/named stat outcomes at all three cuts.
    pub fn metadata_results(&self) -> impl Iterator<Item = &std::io::Result<rustix::fs::Stat>> {
        self.scope.tracking.stats()
    }
    /// Original native rename/unlink/sync results remain in this whole owner.
    pub fn physical_results(&self) -> impl Iterator<Item = &std::io::Result<()>> {
        self.scope
            .report()
            .into_iter()
            .flat_map(PinnedShardGcReport::physical_results)
    }
    /// Actual completed moves into quarantine; no close errno is invented.
    pub fn quarantined_files(&self) -> usize {
        self.scope.tracking.moved()
    }
    /// Actual dense catalog repair outcomes, including original source tags.
    pub fn retagged_slots(&self) -> usize {
        self.scope.tracking.retagged()
    }
    /// Actual strict identity cut index, including a refused entered cut.
    pub fn identity_cut(&self) -> Option<usize> {
        self.scope.tracking.phase()
    }
    /// Number selected only after the saved overall native Ok result.
    pub fn completed_selection(&self) -> Option<usize> {
        self.scope.tracking.selected()
    }
    /// Actual shared native vector capacities, requested slots and element size.
    pub fn native_capacities(&self) -> Option<[(usize, usize, usize); 5]> {
        self.scope.tracking.native_capacities()
    }
    /// Actual allocation error, borrowed without the owning native wrapper.
    pub fn allocation_error(&self) -> Option<&TryReserveError> {
        self.scope
            .floor_allocation
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
                    .report()
                    .and_then(PinnedShardGcReport::allocation_error)
            })
    }
    /// Actual source/destination completion ledger, with no descriptor escape.
    pub fn transfers(&self) -> impl Iterator<Item = (u64, usize, Option<bool>, Option<u64>)> + '_ {
        self.scope.tracking.transfers()
    }
    /// Actual dense catalog move and registration-retag ledger.
    pub fn retags(
        &self,
    ) -> impl Iterator<Item = (usize, usize, Option<u64>, Option<bool>, bool)> + '_ {
        self.scope.tracking.retags()
    }
    /// Actual strict-source accounting refusal.
    pub fn tracking_budget_error(&self) -> Option<&StorageFetchFailure> {
        self.scope.tracking.budget_error()
    }
    /// Actual bridge/callback failure and original payloads remain retained.
    pub fn failure(&self) -> Option<PinnedShardLogGcFailure> {
        self.scope.failure
    }
    /// Actual global cumulative usage, including a refused result.
    pub fn usage(&self) -> Option<&Result<StorageFetchUsage, StorageFetchFailure>> {
        self.scope.usage.as_ref()
    }
    /// Borrow the actual retained producer panic payload.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.scope.panic.as_deref()
    }
    /// Actual append-denial attempt, returned action and caught-panic occupancy.
    /// These are diagnostics, not a permission or completion witness.
    pub fn append_denial(&self) -> (bool, bool, bool) {
        (
            self.scope.append_denial.attempted.get(),
            self.scope.append_denial.returned.get(),
            self.scope
                .append_denial
                .result
                .borrow()
                .as_ref()
                .is_some_and(Result::is_err),
        )
    }
    /// Whether either independently retained actual denial call unwound.
    pub fn denial_panics(&self) -> [bool; 2] {
        [
            self.scope.denial.native_panic.borrow().is_some(),
            self.scope.denial.composing_panic.borrow().is_some(),
        ]
    }
}
/// Complete compaction; quarantined Files drop before returning the SAME log.
pub struct PinnedShardLogGcComplete {
    scope: GcScope,
    seal: PinnedShardLogWriteComplete,
}
impl PinnedShardLogGcComplete {
    /// Current canonical bounds on the SAME retained log.
    pub fn bounds(&self) -> Option<(u64, u64)> {
        self.seal.retained_log().bounds()
    }
    /// Borrow safe diagnostics of the actual completed owner.
    pub fn observation(&self) -> PinnedShardLogGcObservation<'_> {
        PinnedShardLogGcObservation { scope: &self.scope }
    }
    /// Drop all raw outcomes/quarantine first, then return the whole real log.
    pub fn into_log(self) -> OpenedPinnedShardLog {
        let Self { scope, seal } = self;
        drop(scope);
        seal.into_log()
    }
}
/// Terminal failure retains partial index changes, Files and entire real log.
pub struct PinnedShardLogGcError {
    scope: GcScope,
}
impl PinnedShardLogGcError {
    /// Borrow safe facts without extraction or a retry/whole-log escape.
    pub fn observation(&self) -> PinnedShardLogGcObservation<'_> {
        PinnedShardLogGcObservation { scope: &self.scope }
    }
}
impl fmt::Debug for PinnedShardLogGcComplete {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardLogGcComplete")
            .field("receipt", &self.scope.receipt)
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for PinnedShardLogGcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardLogGcError")
            .field("failure", &self.scope.failure)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for PinnedShardLogGcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "bounded tracked canonical GC refused: {:?}",
            self.scope.failure
        )
    }
}
// No Error::source downcast may expose private native owners or File getters.
impl Error for PinnedShardLogGcError {}
fn fixed_fee() -> Option<(usize, usize, usize)> {
    let fields = [
        size_of::<GcScope>(),
        size_of::<PinnedShardLogGcComplete>(),
        size_of::<PinnedShardLogGcError>(),
        size_of::<PinnedShardLogGcObservation<'static>>(),
        size_of::<Instant>(),
        size_of::<u64>(),
        size_of::<HashMap<TopicPartition, LogicalOffset>>(),
        size_of::<Vec<u64>>(),
        size_of::<Option<Result<(), TryReserveError>>>(),
        size_of::<Option<Result<(), TryReserveError>>>(),
        size_of::<Option<LogicalOffset>>(),
        size_of::<Option<Result<(), PinnedShardGcFailure>>>(),
        size_of::<Option<Result<(), PinnedShardGcFailure>>>(),
        size_of::<Option<Result<PinnedShardGcReport, PinnedShardGcError>>>(),
        size_of::<Option<Reservation>>(),
        size_of::<Option<ShardLogResult<u64>>>(),
        size_of::<Option<ShardLogResult<Option<(u64, u64)>>>>(),
        size_of::<Option<super::super::ShardLogCompactionReceipt>>(),
        size_of::<Option<PinnedShardLogGcFailure>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Option<Result<Result<(), PinnedShardLogGcFailure>, Box<dyn Any + Send>>>>(),
        size_of::<Option<Result<PinnedShardLogWriteComplete, PinnedShardLogWriteError>>>(),
        size_of::<Option<OpenedPinnedShardLog>>(),
        size_of::<Denial>(),
        size_of::<Adapter<'static>>(),
        size_of::<Cell<bool>>(),
        size_of::<Cell<bool>>(),
        size_of::<Cell<bool>>(),
        size_of::<RefCell<Option<Result<(), StorageFetchFailure>>>>(),
        size_of::<RefCell<Option<Result<(), StorageFetchFailure>>>>(),
        size_of::<RefCell<Option<Result<(), StorageFetchFailure>>>>(),
        size_of::<Option<Result<(), StorageFetchFailure>>>(),
        size_of::<Option<Result<(), StorageFetchFailure>>>(),
        size_of::<Option<Result<(), StorageFetchFailure>>>(),
        size_of::<Cell<Option<Result<(), ShardDirectoryCheckFailure>>>>(),
        size_of::<Option<Result<(), ShardDirectoryCheckFailure>>>(),
        size_of::<RefCell<Option<Box<dyn Any + Send>>>>(),
        size_of::<RefCell<Option<Box<dyn Any + Send>>>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Result<(), Box<dyn Any + Send>>>(),
        size_of::<Result<(), Box<dyn Any + Send>>>(),
        size_of::<Option<Result<StorageFetchUsage, StorageFetchFailure>>>(),
        size_of::<String>(),
        // Independent append-first phase values, not implicit carrier padding.
        size_of::<AppendDenial>(),
        size_of::<GcChecks<'static, 'static, 'static>>(),
        size_of::<Cell<bool>>(),
        size_of::<Cell<bool>>(),
        size_of::<bool>(),
        size_of::<bool>(),
        size_of::<bool>(),
        size_of::<RefCell<Option<Result<(), Box<dyn Any + Send>>>>>(),
        size_of::<Option<Result<(), Box<dyn Any + Send>>>>(),
        size_of::<Result<(), Box<dyn Any + Send>>>(),
        size_of::<Result<(), Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<RefCell<&'static mut PinnedShardAppendBudget>>(),
        size_of::<&'static mut PinnedShardAppendBudget>(),
        size_of::<RefMut<'static, &'static mut PinnedShardAppendBudget>>(),
        size_of::<RefMut<'static, Option<Result<(), Box<dyn Any + Send>>>>>(),
        size_of::<&'static Adapter<'static>>(),
        size_of::<&'static AppendDenial>(),
    ];
    let diagnostic = super::super::CANONICAL_STREAM_DIAGNOSTIC
        .len()
        .max(super::super::RETAINED_GAP_DIAGNOSTIC.len())
        .max(super::super::SEQUENCE_OVERFLOW_DIAGNOSTIC.len());
    Some((
        fields
            .iter()
            .try_fold(diagnostic, |n, v| n.checked_add(*v))?,
        fields.len().checked_add(1)?,
        diagnostic,
    ))
}
/// Plain conservative setup/operation cost; actual inner charges use SAME global
/// counters. External pin/Core parent and allocator/RSS are separate domains.
pub fn pinned_shard_log_gc_cost(
    fetch: StorageFetchLimits,
    append: PinnedShardAppendLimits,
    write: PinnedShardLogWriteLimits,
    gc: PinnedShardGcLimits,
) -> Option<StorageFetchCost> {
    let mut cost = pinned_shard_log_write_cost(fetch, append, write)?;
    let native = tracking_gc_cost(gc)?;
    let fixed = fixed_fee()?;
    cost.declared_bytes = cost
        .declared_bytes
        .checked_add(native.declared_bytes)?
        .checked_add(fixed.0)?;
    cost.entries = cost
        .entries
        .checked_add(native.entries)?
        .checked_add(fixed.1)?;
    cost.string_bytes = cost
        .string_bytes
        .checked_add(native.string_bytes)?
        .checked_add(fixed.2)?;
    cost.work_bytes = cost
        .work_bytes
        .checked_add(native.work_bytes)?
        .checked_add(fixed.0)?
        .checked_add(fixed.1)?;
    Some(cost)
}
fn check(c: &Adapter<'_>) -> Result<(), PinnedShardLogGcFailure> {
    c.check().map_err(|_| PinnedShardLogGcFailure::Checkpoint)
}
fn pay(
    c: &Adapter<'_>,
    bytes: usize,
    entries: usize,
    work: usize,
) -> Result<(), PinnedShardLogGcFailure> {
    *c.state.charged.borrow_mut() = Some(c.budget.charge_values(bytes, entries, work));
    if c.state
        .charged
        .borrow()
        .as_ref()
        .is_some_and(Result::is_err)
    {
        Err(PinnedShardLogGcFailure::Budget)
    } else {
        Ok(())
    }
}
fn sum(a: usize, b: usize) -> Result<usize, PinnedShardLogGcFailure> {
    a.checked_add(b).ok_or(PinnedShardLogGcFailure::Budget)
}
fn times(a: usize, b: usize) -> Result<usize, PinnedShardLogGcFailure> {
    a.checked_mul(b).ok_or(PinnedShardLogGcFailure::Budget)
}
impl GcScope {
    fn run(
        &mut self,
        pin: &PinnedShardDirectory,
        append: &mut PinnedShardAppendBudget,
        b: &StorageFetchBudget,
        write: PinnedShardLogWriteLimits,
        limits: PinnedShardGcLimits,
        checks: &dyn PinnedShardLogGcChecks,
    ) -> Result<(), PinnedShardLogGcFailure> {
        let c = Adapter {
            expiry: self.expiry,
            budget: b,
            checks,
            state: &self.denial,
        };
        check(&c)?;
        let fixed = fixed_fee().ok_or(PinnedShardLogGcFailure::Budget)?;
        pay(&c, fixed.0, fixed.1, fixed.0)?;
        completed(GcPhase::FixedPaid);
        check(&c)?;
        let native = tracking_gc_cost(limits).ok_or(PinnedShardLogGcFailure::Budget)?;
        // Native original scope allocation/work ceiling is admitted before seal
        // or collection. Tracking headers/Vec slots are debited by prepare.
        let base = crate::pinned_shard_gc_cost(limits).ok_or(PinnedShardLogGcFailure::Budget)?;
        pay(&c, base.declared_bytes, base.entries, base.work_bytes)?;
        if limits.max_io_calls < native.native_call_slots {
            return Err(PinnedShardLogGcFailure::Budget);
        }
        self.prepare = Some(self.tracking.prepare(limits, b, &c));
        completed(GcPhase::Prepared);
        check(&c)?;
        if !matches!(self.prepare, Some(Ok(()))) {
            return Err(PinnedShardLogGcFailure::Tracking);
        }
        let log = self
            .unsealed
            .take()
            .ok_or(PinnedShardLogGcFailure::Invariant)?;
        self.seal = Some(log.seal_until(pin, append, b, write, &c));
        completed(GcPhase::Sealed);
        check(&c)?;
        let owner = self
            .seal
            .as_mut()
            .and_then(|r| r.as_mut().ok())
            .ok_or(PinnedShardLogGcFailure::Seal)?;
        // Fallible actual floor storage/reservation is retained before callbacks.
        let width = size_of::<(TopicPartition, LogicalOffset)>();
        pay(&c, width, 1, width)?;
        self.floor_allocation = Some(self.floors.try_reserve(1));
        completed(GcPhase::Allocated);
        check(&c)?;
        if matches!(self.floor_allocation, Some(Err(_))) {
            return Err(PinnedShardLogGcFailure::Allocation);
        }
        let excess = self.floors.capacity().saturating_sub(1);
        pay(&c, times(excess, width)?, excess, times(excess, width)?)?;
        if self.floors.capacity() > limits.max_log_start_slots {
            return Err(PinnedShardLogGcFailure::Allocation);
        }
        pay(&c, 0, 0, times(sum(self.floors.capacity(), 1)?, width)?)?;
        check(&c)?;
        self.replaced_floor = self.floors.insert(
            super::super::topic_partition(),
            LogicalOffset::new(self.sequence),
        );
        check(&c)?;
        {
            let gc_checks = GcChecks {
                append: RefCell::new(&mut *append),
                prior: &c,
                state: &self.append_denial,
            };
            self.native = Some(owner.retained_log_mut().scope.opened.run_tracked_gc(
                pin,
                &self.floors,
                limits,
                self.expiry,
                &gc_checks,
                b,
                &mut self.tracking,
            ));
        } // WHOLE actual Result saved before releasing the temporary borrow.
        // All post-GC checks/transfers use the original adapter with no alias.
        completed(GcPhase::NativeCompleted);
        check(&c)?;
        let report = self
            .native
            .as_ref()
            .and_then(|r| r.as_ref().ok())
            .ok_or(PinnedShardLogGcFailure::Native)?;
        self.transfer = Some(owner.retained_log_mut().scope.opened.retire_completed_gc(
            report,
            b,
            &mut self.tracking,
            &c,
        ));
        completed(GcPhase::TrackingCompleted);
        check(&c)?;
        if !matches!(self.transfer, Some(Ok(()))) {
            return Err(PinnedShardLogGcFailure::Tracking);
        }
        let log = owner.retained_log_mut();
        let shape = log
            .scope
            .opened
            .store()
            .log_catalog_shape()
            .ok_or(PinnedShardLogGcFailure::Budget)?;
        if shape.0 > b.limits().max_plan_batches {
            return Err(PinnedShardLogGcFailure::Budget);
        }
        let bytes = times(shape.0, size_of::<u64>())?;
        let levels = (usize::BITS as usize - shape.0.leading_zeros() as usize).max(1);
        pay(
            &c,
            bytes,
            shape.0,
            sum(shape.2, sum(bytes, times(bytes, levels)?)?)?,
        )?;
        check(&c)?;
        self.sequence_allocation = Some(self.sequences.try_reserve_exact(shape.0));
        completed(GcPhase::Allocated);
        check(&c)?;
        if matches!(self.sequence_allocation, Some(Err(_))) {
            return Err(PinnedShardLogGcFailure::Allocation);
        }
        let excess = self.sequences.capacity().saturating_sub(shape.0);
        pay(
            &c,
            times(excess, size_of::<u64>())?,
            excess,
            times(excess, size_of::<u64>())?,
        )?;
        if self.sequences.capacity() > b.limits().max_plan_batches {
            return Err(PinnedShardLogGcFailure::Allocation);
        }
        for reservation in log.scope.opened.store().log_catalog_reservations() {
            check(&c)?;
            self.reservation = Some(reservation);
            self.catalog_result = Some(super::super::validate_catalog_entry(reservation));
            check(&c)?;
            self.sequences.push(
                *self
                    .catalog_result
                    .as_ref()
                    .and_then(|r| r.as_ref().ok())
                    .ok_or(PinnedShardLogGcFailure::Canonical)?,
            );
        }
        self.bounds = Some(super::super::retained_bounds(&mut self.sequences));
        completed(GcPhase::Bounds);
        check(&c)?;
        log.bounds = *self
            .bounds
            .as_ref()
            .and_then(|r| r.as_ref().ok())
            .ok_or(PinnedShardLogGcFailure::Canonical)?;
        self.receipt = Some(super::super::compaction_receipt(report.stats()));
        self.usage = Some(b.usage());
        check(&c)
    }
}
impl OpenedPinnedShardLog {
    /// Seal and compact this genuine log using retained actual descriptors.
    ///
    /// The SAME global budget spans reads, seal, collection and canonical scans.
    /// Admit the plain cost first and retain the real pin/Core parent LAST.
    /// Overall NativeOk alone permits prepaid source-to-quarantine moves; any
    /// native/bridge refusal is terminal and exposes no owner retry/extractor.
    #[allow(
        clippy::result_large_err,
        reason = "retain actual collection outcomes and whole log without allocating during refusal"
    )]
    #[allow(
        clippy::too_many_arguments,
        reason = "keep the actual pin, original budgets, limits and composing checks explicit"
    )]
    pub fn compact_before_until(
        self,
        pin: &PinnedShardDirectory,
        sequence: u64,
        append: &mut PinnedShardAppendBudget,
        budget: &StorageFetchBudget,
        write: PinnedShardLogWriteLimits,
        gc: PinnedShardGcLimits,
        checks: &dyn PinnedShardLogGcChecks,
    ) -> Result<PinnedShardLogGcComplete, PinnedShardLogGcError> {
        let mut scope = GcScope {
            expiry: append.absolute_expiry().min(budget.absolute_expiry()),
            sequence,
            floors: HashMap::new(),
            floor_allocation: None,
            replaced_floor: None,
            sequences: Vec::new(),
            sequence_allocation: None,
            prepare: None,
            tracking: TrackedGcTrace::new(),
            native: None,
            transfer: None,
            reservation: None,
            catalog_result: None,
            bounds: None,
            receipt: None,
            failure: None,
            panic: None,
            catch_result: None,
            denial: Denial::new(),
            append_denial: AppendDenial::new(),
            usage: None,
            seal: None,
            unsealed: Some(self),
        };
        scope.catch_result = Some(catch_unwind(AssertUnwindSafe(|| {
            scope.run(pin, append, budget, write, gc, checks)
        })));
        let failure = match scope.catch_result.take() {
            Some(Ok(Ok(()))) => None,
            Some(Ok(Err(e))) => Some(e),
            Some(Err(p)) => {
                scope.panic = Some(p);
                Some(PinnedShardLogGcFailure::Unwind)
            }
            None => Some(PinnedShardLogGcFailure::Invariant),
        };
        if let Some(failure) = failure {
            scope.failure = Some(failure);
            scope.append_denial.deny_direct(append);
            Adapter {
                expiry: scope.expiry,
                budget,
                checks,
                state: &scope.denial,
            }
            .deny_once();
            scope.usage = Some(budget.usage());
            Err(PinnedShardLogGcError { scope })
        } else {
            match scope.seal.take() {
                Some(Ok(seal)) => Ok(PinnedShardLogGcComplete { scope, seal }),
                actual => {
                    scope.seal = actual;
                    scope.failure = Some(PinnedShardLogGcFailure::Invariant);
                    scope.append_denial.deny_direct(append);
                    Adapter {
                        expiry: scope.expiry,
                        budget,
                        checks,
                        state: &scope.denial,
                    }
                    .deny_once();
                    scope.usage = Some(budget.usage());
                    Err(PinnedShardLogGcError { scope })
                }
            }
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GcPhase {
    FixedPaid,
    Prepared,
    Sealed,
    Allocated,
    NativeCompleted,
    TrackingCompleted,
    Bounds,
}
fn completed(phase: GcPhase) {
    #[cfg(test)]
    tests::completed(phase);
    #[cfg(not(test))]
    let _ = phase;
}
#[cfg(test)]
#[path = "api_pinned_gc_tests.rs"]
pub(crate) mod tests;
