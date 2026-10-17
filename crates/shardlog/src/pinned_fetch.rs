//! Bounded reads of the exact descriptors retained by genuine pinned open.
//!
//! No pathname open, descriptor clone or replacement store is performed here.
//! The borrowed opened store and actual pin must outlive this synchronous call
//! and its physical frames. Canonical log entry points additionally consume and
//! retain their whole log. Only observations and payload bytes can escape.

use super::*;
use crate::extent::fetch::{coalesces, payload_end, selected_fetch_bytes};
use crate::{
    StorageFetchBudget, StorageFetchCost, StorageFetchFailure, StorageFetchLimits,
    StorageFetchUsage, storage_fetch_cost,
};
use rustix::fs::CWD;

/// Same composing work/minimum deadline and mandatory captured-health denial.
/// No callback is stored in a completed read or log.
pub trait PinnedShardFetchChecks: ShardDirectoryChecks {
    /// Latch the SAME actual composing counters/health before retained cleanup.
    fn deny(&self);
}

/// Completed real phases, also used by genuine filesystem regression observers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PinnedShardFetchPhase {
    /// Actual plan vector allocation completed.
    PlanAllocated,
    /// One actual selected metadata value was retained.
    Planned,
    /// The retained original pack path was copied.
    PathCopied,
    /// Actual bounded native pathname conversion completed.
    NameConverted,
    /// A direct directory or acquired-lock stat completed.
    DirectoryStat,
    /// A direct held/named pack stat completed.
    PackStat,
    /// Actual read buffer allocation completed.
    BufferAllocated,
    /// One direct pread attempt completed, including EINTR, short read or EOF.
    Read,
    /// Actual buffer ownership was transferred into Bytes.
    BufferPublished,
    /// One completed batch and its actual payload owner were retained.
    BatchPublished,
}

/// Actual unmodified typed result, retained before any postcheck or observer.
#[derive(Debug)]
pub enum PinnedShardFetchOutcome {
    /// One direct fstat/statat result; physical slots consume cumulative entries.
    Stat {
        phase: PinnedShardFetchPhase,
        result: io::Result<Stat>,
    },
    /// One direct pread result; requested bytes include short/EOF/error attempts.
    Read {
        offset: u64,
        requested: usize,
        result: io::Result<usize>,
    },
}

/// Terminal refusal; actual IO/allocation/native results remain in observations.
#[derive(Debug)]
pub enum PinnedShardFetchFailure {
    /// SAME original native deadline/counters/sticky refusal state.
    Budget(StorageFetchFailure),
    /// SAME actual composing operation refused.
    Checkpoint {
        result: ShardDirectoryCheckFailure,
        native_postcheck: Option<StorageFetchFailure>,
    },
    /// Actual directory/name/held-file identity no longer agrees.
    Identity,
    /// Plan token/epoch or active-file state is untracked by this actual open.
    Stale,
    /// Zero request limits or a native selection/range rule refused.
    Input,
    /// A real requested allocation refused or exceeded its permitted capacity.
    Allocation,
    /// A retained real IO result refused.
    Io,
    /// A real pread returned EOF before the planned range completed.
    Eof,
    /// A producer/checkpoint/observer unwound; the original payload is retained.
    Unwind,
    /// An internal owning transition refused.
    Invariant,
}

type Bounded<T> = Result<T, PinnedShardFetchFailure>;
fn add(a: usize, b: usize) -> Bounded<usize> {
    a.checked_add(b).ok_or(PinnedShardFetchFailure::Budget(
        StorageFetchFailure::Capacity,
    ))
}
fn mul(a: usize, b: usize) -> Bounded<usize> {
    a.checked_mul(b).ok_or(PinnedShardFetchFailure::Budget(
        StorageFetchFailure::Capacity,
    ))
}
fn debit(budget: &StorageFetchBudget, declared: usize, entries: usize, work: usize) -> Bounded<()> {
    budget
        .charge_values(declared, entries, work)
        .map_err(PinnedShardFetchFailure::Budget)
}
fn checkpoint(budget: &StorageFetchBudget, checks: &dyn PinnedShardFetchChecks) -> Bounded<()> {
    budget.check().map_err(PinnedShardFetchFailure::Budget)?;
    let actual = checks.check();
    // The typed composing error is a Copy scalar; it survives this post-E check.
    let post = budget.check();
    match (actual, post) {
        (Err(result), post) => Err(PinnedShardFetchFailure::Checkpoint {
            result,
            native_postcheck: post.err(),
        }),
        (Ok(()), Err(error)) => Err(PinnedShardFetchFailure::Budget(error)),
        (Ok(()), Ok(())) => Ok(()),
    }
}
fn completed(phase: PinnedShardFetchPhase) {
    #[cfg(test)]
    tests::completed(phase);
    #[cfg(not(test))]
    let _ = phase;
}

struct ReadRange {
    sequence: u64,
    path: PathBuf,
    name: Option<CString>,
    name_error: Option<NulError>,
    buffer: Vec<u8>,
    filled: usize,
    bytes: Option<Bytes>,
}
impl ReadRange {
    fn new(sequence: u64) -> Self {
        Self {
            sequence,
            path: PathBuf::new(),
            name: None,
            name_error: None,
            buffer: Vec::new(),
            filled: 0,
            bytes: None,
        }
    }
}

/// Safe actual retained results, partial bytes, fees and independent panics.
/// This surface exposes no File, native store, descriptor or movable FD wrapper.
pub struct PinnedShardFetchObservation {
    outcomes: Vec<PinnedShardFetchOutcome>,
    ranges: Vec<ReadRange>,
    allocation: Option<Result<(), TryReserveError>>,
    native_error: Option<StorageError>,
    failure: Option<PinnedShardFetchFailure>,
    usage: Option<Result<StorageFetchUsage, StorageFetchFailure>>,
    directory_name: Option<CString>,
    directory_name_error: Option<NulError>,
    panic_payload: Option<Box<dyn Any + Send>>,
    budget_deny_panic: Option<Box<dyn Any + Send>>,
    composing_deny_panic: Option<Box<dyn Any + Send>>,
}
impl PinnedShardFetchObservation {
    fn new() -> Self {
        Self {
            outcomes: Vec::new(),
            ranges: Vec::new(),
            allocation: None,
            native_error: None,
            failure: None,
            usage: None,
            directory_name: None,
            directory_name_error: None,
            panic_payload: None,
            budget_deny_panic: None,
            composing_deny_panic: None,
        }
    }
    /// Real entered IO results, saved before hooks and postchecks.
    pub fn outcomes(&self) -> &[PinnedShardFetchOutcome] {
        &self.outcomes
    }
    /// Actual partial requested buffer and its completed prefix length.
    pub fn partial_buffer(&self, range: usize) -> Option<(&[u8], usize)> {
        let range = self.ranges.get(range)?;
        Some((
            range.bytes.as_deref().unwrap_or(&range.buffer),
            range.filled,
        ))
    }
    /// Exact retained native name, including accepted leading-zero spellings.
    pub fn pack_name(&self, range: usize) -> Option<&std::ffi::CStr> {
        self.ranges.get(range)?.name.as_deref()
    }
    /// Original typed requested allocation error, if any.
    pub fn allocation_error(&self) -> Option<&TryReserveError> {
        self.allocation.as_ref()?.as_ref().err()
    }
    /// Actual pure native selection/range error, without synthetic replacement.
    pub fn native_error(&self) -> Option<&StorageError> {
        self.native_error.as_ref()
    }
    /// First terminal category, with real native budget error where applicable.
    pub fn failure(&self) -> Option<&PinnedShardFetchFailure> {
        self.failure.as_ref()
    }
    /// Actual cumulative fees observed on completion/refusal, never reset.
    pub fn usage(&self) -> Option<&Result<StorageFetchUsage, StorageFetchFailure>> {
        self.usage.as_ref()
    }
    /// Actual independent operation/native-denial/composing-denial payloads.
    pub fn panic_payloads(&self) -> [Option<&(dyn Any + Send)>; 3] {
        [
            self.panic_payload.as_deref(),
            self.budget_deny_panic.as_deref(),
            self.composing_deny_panic.as_deref(),
        ]
    }
    /// Original actual native IO error, if an entered call failed.
    pub fn io_error(&self) -> Option<&io::Error> {
        self.outcomes.iter().find_map(|o| match o {
            PinnedShardFetchOutcome::Stat { result, .. } => result.as_ref().err(),
            PinnedShardFetchOutcome::Read { result, .. } => result.as_ref().err(),
        })
    }
    fn refuse(
        &mut self,
        budget: &StorageFetchBudget,
        checks: &dyn PinnedShardFetchChecks,
        failure: PinnedShardFetchFailure,
    ) {
        self.failure = Some(failure);
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| budget.deny())) {
            self.budget_deny_panic = Some(payload);
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| checks.deny())) {
            self.composing_deny_panic = Some(payload);
        }
        self.usage = Some(budget.usage());
    }
    fn native<T: Copy>(
        &mut self,
        actual: StorageResult<T>,
        budget: &StorageFetchBudget,
        checks: &dyn PinnedShardFetchChecks,
    ) -> Bounded<T> {
        let actual = match actual {
            Ok(value) => Some(value),
            Err(error) => {
                self.native_error = Some(error);
                None
            }
        };
        checkpoint(budget, checks)?;
        actual.ok_or(PinnedShardFetchFailure::Input)
    }
}
impl fmt::Debug for PinnedShardFetchObservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardFetchObservation")
            .field("outcomes", &self.outcomes.len())
            .field("ranges", &self.ranges.len())
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}

/// Opaque metadata produced by an actual opened store, with its native token/epoch.
pub struct PinnedShardFetchPlan {
    planned: Vec<StoredBatchMetadata>,
    store_token: u64,
    epoch: u64,
    observation: PinnedShardFetchObservation,
}
impl PinnedShardFetchPlan {
    /// Actual produced metadata prefix; no public construction or mutation.
    pub fn planned(&self) -> &[StoredBatchMetadata] {
        &self.planned
    }
    /// Real plan-stage allocation/refusal observations.
    pub fn observation(&self) -> &PinnedShardFetchObservation {
        &self.observation
    }
}
impl fmt::Debug for PinnedShardFetchPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardFetchPlan")
            .field("batches", &self.planned.len())
            .finish_non_exhaustive()
    }
}
/// Refusal retains every actual materialized metadata value and native outcome.
#[derive(Debug)]
pub struct PinnedShardFetchPlanError {
    plan: PinnedShardFetchPlan,
}
impl PinnedShardFetchPlanError {
    /// Actual planned prefix and safe observations, without parts extraction.
    pub fn plan(&self) -> &PinnedShardFetchPlan {
        &self.plan
    }
    /// Actual terminal native/category refusal.
    pub fn observation(&self) -> &PinnedShardFetchObservation {
        &self.plan.observation
    }
}

struct ReadState {
    plan: PinnedShardFetchPlan,
    batches: Vec<StoredBatch>,
    observation: PinnedShardFetchObservation,
}
/// Completed payloads and all actual physical read observations.
pub struct PinnedShardFetchComplete {
    state: ReadState,
}
impl PinnedShardFetchComplete {
    /// Actual completed native batches in original selection order.
    pub fn batches(&self) -> &[StoredBatch] {
        &self.state.batches
    }
    /// Original read outcomes and partial buffers retained before postchecks.
    pub fn observation(&self) -> &PinnedShardFetchObservation {
        &self.state.observation
    }
}
/// Refusal retains original plan, partial buffers, completed batches and results.
pub struct PinnedShardFetchReadError {
    state: ReadState,
}
impl PinnedShardFetchReadError {
    /// Actual completed payload owners before refusal.
    pub fn batches(&self) -> &[StoredBatch] {
        &self.state.batches
    }
    /// Actual input metadata, with its original store token/epoch retained.
    pub fn plan(&self) -> &PinnedShardFetchPlan {
        &self.state.plan
    }
    /// Safe actual IO/allocation/partial-byte/unwind observations.
    pub fn observation(&self) -> &PinnedShardFetchObservation {
        &self.state.observation
    }
}
macro_rules! result_error {
    ($t:ty) => {
        impl fmt::Debug for $t {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($t))
                    .field("observation", self.observation())
                    .finish_non_exhaustive()
            }
        }
        impl fmt::Display for $t {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(
                    f,
                    "pinned fetch refused: {:?}",
                    self.observation().failure()
                )
            }
        }
        impl Error for $t {
            fn source(&self) -> Option<&(dyn Error + 'static)> {
                let o = self.observation();
                if let Some(e) = o.io_error() {
                    return Some(e);
                }
                if let Some(e) = o.native_error() {
                    return Some(e);
                }
                if let Some(e) = o.allocation_error() {
                    return Some(e);
                }
                match o.failure() {
                    Some(PinnedShardFetchFailure::Budget(e)) => Some(e),
                    _ => None,
                }
            }
        }
    };
}
// Plan already has a derived safe Debug implementation.
impl fmt::Display for PinnedShardFetchPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pinned plan refused: {:?}", self.observation().failure())
    }
}
impl Error for PinnedShardFetchPlanError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.observation().native_error().map(|e| e as &dyn Error)
    }
}
result_error!(PinnedShardFetchReadError);
impl fmt::Debug for PinnedShardFetchComplete {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardFetchComplete")
            .field("batches", &self.batches().len())
            .finish_non_exhaustive()
    }
}

/// Plain one-budget ceiling and explicit source-visible read carrier fees.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PinnedShardFetchCost {
    /// Fixed values plus configured cumulative native allocations.
    pub declared_bytes: usize,
    /// Native path/diagnostic subset of declared bytes.
    pub string_bytes: usize,
    /// Actual fixed slots plus configured cumulative entry ceiling.
    pub entries: usize,
    /// Setup and configured native metadata/path/read work.
    pub work_bytes: usize,
    /// Conservative actual IO ceiling: each call spends at least one entry.
    pub native_call_slots: usize,
}
fn fixed_fee() -> Bounded<(usize, usize, usize)> {
    let values = [
        size_of::<PinnedShardFetchPlan>(),
        size_of::<PinnedShardFetchPlanError>(),
        size_of::<ReadState>(),
        size_of::<PinnedShardFetchComplete>(),
        size_of::<PinnedShardFetchReadError>(),
        size_of::<PinnedShardFetchObservation>(),
        size_of::<ReadRange>(),
        size_of::<PinnedShardFetchOutcome>(),
        size_of::<StorageFetchCost>(),
        size_of::<PinnedShardFetchCost>(),
        size_of::<Instant>(),
        size_of::<StorageFetchUsage>(),
        size_of::<u64>(),
        size_of::<u64>(),
        size_of::<PinnedShardFetchPhase>(),
        size_of::<Option<CString>>(),
        size_of::<Option<NulError>>(),
        size_of::<Vec<StoredBatchMetadata>>(),
        size_of::<Vec<StoredBatch>>(),
        size_of::<Vec<ReadRange>>(),
        size_of::<Vec<PinnedShardFetchOutcome>>(),
        size_of::<Vec<u8>>(),
        size_of::<Bytes>(),
        size_of::<CString>(),
        size_of::<NulError>(),
        size_of::<PathBuf>(),
        size_of::<String>(),
        size_of::<ScannedPack>(),
        size_of::<Option<ScannedPack>>(),
        size_of::<(u64, u64)>(),
        size_of::<Facts>(),
        size_of::<Result<(&File, ScannedPack), PinnedShardFetchFailure>>(),
        size_of::<Option<Result<(), TryReserveError>>>(),
        size_of::<Option<StorageError>>(),
        size_of::<Option<Result<StorageFetchUsage, StorageFetchFailure>>>(),
        size_of::<Option<PinnedShardFetchFailure>>(),
        size_of::<io::Result<Stat>>(),
        size_of::<io::Result<usize>>(),
        size_of::<Result<(), PinnedShardFetchFailure>>(),
        size_of::<Result<Result<(), PinnedShardFetchFailure>, Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
    ];
    // Shared selection/payload arithmetic can allocate exactly one terminal diagnostic.
    let strings = "fetch byte count overflow"
        .len()
        .max("payload length exceeds u64".len())
        .max("payload end overflow".len());
    let bytes = values.into_iter().try_fold(strings, add)?;
    Ok((bytes, add(values.len(), 1)?, strings))
}
/// Prepay once before constructing the SAME shared StorageFetchBudget.
/// All plan/read/canonical passes consume its original cumulative allowances;
/// this is logical source-visible accounting, not allocator/kernel/RSS admission.
pub fn pinned_shard_fetch_cost(
    limits: StorageFetchLimits,
) -> Result<PinnedShardFetchCost, StorageFetchFailure> {
    let native = storage_fetch_cost(limits)?;
    let (fixed, entries, strings) = fixed_fee().map_err(|_| StorageFetchFailure::Capacity)?;
    Ok(PinnedShardFetchCost {
        declared_bytes: native
            .declared_bytes
            .checked_add(fixed)
            .ok_or(StorageFetchFailure::Capacity)?,
        string_bytes: native
            .string_bytes
            .checked_add(strings)
            .ok_or(StorageFetchFailure::Capacity)?,
        entries: native
            .entries
            .checked_add(entries)
            .ok_or(StorageFetchFailure::Capacity)?,
        work_bytes: native
            .work_bytes
            .checked_add(fixed)
            .and_then(|n| n.checked_add(entries))
            .ok_or(StorageFetchFailure::Capacity)?,
        native_call_slots: limits.max_entries,
    })
}

// Every actual Vec reservation is in a caller-owned outside-catch field. The
// retained Result precedes hooks/checks; excess actual capacity is charged too.
fn grow<T>(
    target: &mut Vec<T>,
    additional: usize,
    allocation: &mut Option<Result<(), TryReserveError>>,
    budget: &StorageFetchBudget,
    checks: &dyn PinnedShardFetchChecks,
) -> Bounded<()> {
    let wanted = add(target.len(), additional)?;
    if wanted > budget.limits().max_entries {
        return Err(PinnedShardFetchFailure::Budget(
            StorageFetchFailure::Capacity,
        ));
    }
    if wanted <= target.capacity() {
        return checkpoint(budget, checks);
    }
    let previous = target.capacity();
    let slots = wanted - previous;
    debit(
        budget,
        size_of::<Result<(), TryReserveError>>(),
        1,
        size_of::<Result<(), TryReserveError>>(),
    )?;
    debit(
        budget,
        mul(slots, size_of::<T>())?,
        slots,
        mul(add(previous, slots)?, size_of::<T>())?,
    )?;
    checkpoint(budget, checks)?;
    *allocation = Some(target.try_reserve_exact(wanted - target.len()));
    checkpoint(budget, checks)?;
    if matches!(allocation, Some(Err(_))) {
        return Err(PinnedShardFetchFailure::Allocation);
    }
    if target.capacity() > budget.limits().max_entries {
        return Err(PinnedShardFetchFailure::Allocation);
    }
    let excess = target.capacity().saturating_sub(wanted);
    debit(
        budget,
        mul(excess, size_of::<T>())?,
        excess,
        mul(excess, size_of::<T>())?,
    )?;
    checkpoint(budget, checks)
}

impl OpenedPinnedShardStore {
    /// Plan using this actual store and SAME cumulative native work/deadline.
    /// No read, new owner or completion authority is produced by metadata.
    pub fn plan_fetch_pinned_until(
        &self,
        topic: TopicPartition,
        start: LogicalOffset,
        max_bytes: usize,
        max_batches: usize,
        budget: &StorageFetchBudget,
        checks: &dyn PinnedShardFetchChecks,
    ) -> Result<PinnedShardFetchPlan, PinnedShardFetchPlanError> {
        let mut plan = PinnedShardFetchPlan {
            planned: Vec::new(),
            store_token: self.store.store_token,
            epoch: self.store.plan_epoch,
            observation: PinnedShardFetchObservation::new(),
        };
        let observed = catch_unwind(AssertUnwindSafe(|| {
            let (fixed, entries, _) = fixed_fee()?;
            debit(budget, fixed, entries, fixed)?;
            checkpoint(budget, checks)?;
            if max_bytes == 0 || max_batches == 0 || max_batches > budget.limits().max_plan_batches
            {
                return Err(PinnedShardFetchFailure::Input);
            }
            // Real sparse HashMap capacity is paid BEFORE its actual point probe.
            debit(
                budget,
                0,
                0,
                mul(
                    add(self.store.index.capacity(), 1)?,
                    size_of::<(TopicPartition, Vec<ExtentLocation>)>(),
                )?,
            )?;
            checkpoint(budget, checks)?;
            let Some(locations) = self.store.index.get(&topic) else {
                checkpoint(budget, checks)?;
                plan.observation.usage = Some(budget.usage());
                return checkpoint(budget, checks);
            };
            debit(
                budget,
                0,
                0,
                mul(usize::BITS as usize + 1, size_of::<ExtentLocation>())?,
            )?;
            checkpoint(budget, checks)?;
            let first =
                locations.partition_point(|location| location.reservation.last_offset < start);
            let capacity = max_batches.min(locations.len().saturating_sub(first));
            grow(
                &mut plan.planned,
                capacity,
                &mut plan.observation.allocation,
                budget,
                checks,
            )?;
            completed(PinnedShardFetchPhase::PlanAllocated);
            checkpoint(budget, checks)?;
            if plan.planned.capacity() > budget.limits().max_plan_batches {
                return Err(PinnedShardFetchFailure::Allocation);
            }
            let mut used = 0;
            for location in &locations[first..] {
                if plan.planned.len() == max_batches {
                    break;
                }
                debit(
                    budget,
                    0,
                    1,
                    size_of::<ExtentLocation>() + size_of::<StoredBatchMetadata>(),
                )?;
                checkpoint(budget, checks)?;
                let actual = selected_fetch_bytes(
                    used,
                    location.payload_len,
                    !plan.planned.is_empty(),
                    max_bytes,
                );
                let Some(next) = plan.observation.native(actual, budget, checks)? else {
                    break;
                };
                plan.planned.push(self.store.metadata_for(location));
                used = next;
                completed(PinnedShardFetchPhase::Planned);
                checkpoint(budget, checks)?;
            }
            plan.observation.usage = Some(budget.usage());
            checkpoint(budget, checks)
        }));
        let failed = match observed {
            Ok(Ok(())) => None,
            Ok(Err(f)) => Some(f),
            Err(p) => {
                plan.observation.panic_payload = Some(p);
                Some(PinnedShardFetchFailure::Unwind)
            }
        };
        if let Some(f) = failed {
            plan.observation.refuse(budget, checks, f);
            return Err(PinnedShardFetchPlanError { plan });
        }
        Ok(plan)
    }
}

#[derive(Clone, Copy)]
struct Facts {
    dev: u64,
    ino: u64,
    single_link: bool,
    linked: bool,
    kind: FileType,
}
fn facts(result: &PinnedShardFetchOutcome) -> Bounded<Facts> {
    let PinnedShardFetchOutcome::Stat { result: Ok(s), .. } = result else {
        return Err(PinnedShardFetchFailure::Io);
    };
    Ok(Facts {
        dev: s.st_dev,
        ino: s.st_ino,
        single_link: s.st_nlink == 1,
        linked: s.st_nlink > 0,
        kind: FileType::from_raw_mode(s.st_mode),
    })
}
impl Facts {
    fn identity(self) -> (u64, u64) {
        (self.dev, self.ino)
    }
    fn regular(self) -> bool {
        self.kind == FileType::RegularFile && self.single_link
    }
}
impl PinnedShardFetchObservation {
    fn stat(
        &mut self,
        phase: PinnedShardFetchPhase,
        budget: &StorageFetchBudget,
        checks: &dyn PinnedShardFetchChecks,
        call: impl FnOnce() -> io::Result<Stat>,
    ) -> Bounded<Facts> {
        grow(&mut self.outcomes, 1, &mut self.allocation, budget, checks)?;
        debit(budget, size_of::<io::Result<Stat>>(), 2, size_of::<Stat>())?;
        checkpoint(budget, checks)?;
        self.outcomes.push(PinnedShardFetchOutcome::Stat {
            phase,
            result: call(),
        });
        completed(phase);
        checkpoint(budget, checks)?;
        facts(
            self.outcomes
                .last()
                .ok_or(PinnedShardFetchFailure::Invariant)?,
        )
    }
    fn validate_pin(
        &mut self,
        opened: &OpenedPinnedShardStore,
        pin: &PinnedShardDirectory,
        budget: &StorageFetchBudget,
        checks: &dyn PinnedShardFetchChecks,
    ) -> Bounded<()> {
        let path = pin.directory();
        let bytes = path.as_os_str().as_bytes().len();
        if bytes > budget.limits().max_path_bytes
            || opened.store.config.directory.as_os_str().len() > budget.limits().max_path_bytes
        {
            return Err(PinnedShardFetchFailure::Budget(
                StorageFetchFailure::Capacity,
            ));
        }
        debit(budget, add(bytes, 1)?, 1, mul(add(bytes, 1)?, 4)?)?;
        checkpoint(budget, checks)?;
        if path.as_os_str() != opened.store.config.directory.as_os_str() {
            return Err(PinnedShardFetchFailure::Identity);
        }
        match CString::new(path.as_os_str().as_bytes()) {
            Ok(name) => self.directory_name = Some(name),
            Err(error) => self.directory_name_error = Some(error),
        }
        completed(PinnedShardFetchPhase::NameConverted);
        checkpoint(budget, checks)?;
        if self.directory_name_error.is_some() {
            return Err(PinnedShardFetchFailure::Identity);
        }
        let slot = opened
            .scope
            .directory_slot
            .ok_or(PinnedShardFetchFailure::Invariant)?;
        let directory = opened
            .scope
            .file(slot)
            .map_err(|_| PinnedShardFetchFailure::Invariant)?;
        let held = self.stat(PinnedShardFetchPhase::DirectoryStat, budget, checks, || {
            fstat(directory).map_err(io::Error::from)
        })?;
        let original = self.stat(PinnedShardFetchPhase::DirectoryStat, budget, checks, || {
            fstat(&pin.scope.directory).map_err(io::Error::from)
        })?;
        // Temporarily take no ownership: CString stays in this outside Scope;
        // immutable split fields allow the direct syscall to borrow it.
        grow(&mut self.outcomes, 1, &mut self.allocation, budget, checks)?;
        debit(budget, size_of::<io::Result<Stat>>(), 2, size_of::<Stat>())?;
        checkpoint(budget, checks)?;
        let name = self
            .directory_name
            .as_deref()
            .ok_or(PinnedShardFetchFailure::Invariant)?;
        self.outcomes.push(PinnedShardFetchOutcome::Stat {
            phase: PinnedShardFetchPhase::DirectoryStat,
            result: statat(CWD, name, AtFlags::SYMLINK_NOFOLLOW).map_err(io::Error::from),
        });
        completed(PinnedShardFetchPhase::DirectoryStat);
        checkpoint(budget, checks)?;
        let named = facts(
            self.outcomes
                .last()
                .ok_or(PinnedShardFetchFailure::Invariant)?,
        )?;
        if held.identity() != original.identity()
            || held.identity() != named.identity()
            || held.kind != FileType::Directory
            || named.kind != FileType::Directory
            || !held.linked
        {
            return Err(PinnedShardFetchFailure::Identity);
        }
        let lock = self.stat(PinnedShardFetchPhase::DirectoryStat, budget, checks, || {
            fstat(&pin.scope.guard.guard().file).map_err(io::Error::from)
        })?;
        let own = self.stat(PinnedShardFetchPhase::DirectoryStat, budget, checks, || {
            fstat(&opened.store._lock_file).map_err(io::Error::from)
        })?;
        let named = self.stat(PinnedShardFetchPhase::DirectoryStat, budget, checks, || {
            statat(
                &pin.scope.directory,
                c".shard-store.lock",
                AtFlags::SYMLINK_NOFOLLOW,
            )
            .map_err(io::Error::from)
        })?;
        if !lock.regular()
            || !own.regular()
            || !named.regular()
            || lock.identity() != own.identity()
            || lock.identity() != named.identity()
        {
            return Err(PinnedShardFetchFailure::Identity);
        }
        Ok(())
    }
}

impl ReadState {
    fn tracked<'a>(
        opened: &'a OpenedPinnedShardStore,
        sequence: u64,
        budget: &StorageFetchBudget,
        checks: &dyn PinnedShardFetchChecks,
    ) -> Bounded<(&'a File, ScannedPack)> {
        debit(
            budget,
            0,
            0,
            mul(
                add(opened.scope.scanned.capacity(), 1)?,
                size_of::<ScannedPack>(),
            )?,
        )?;
        checkpoint(budget, checks)?;
        debit(
            budget,
            size_of::<Option<ScannedPack>>()
                + size_of::<Result<(&File, ScannedPack), PinnedShardFetchFailure>>(),
            2,
            size_of::<FileSlot>()
                + size_of::<Option<ScannedPack>>()
                + size_of::<Result<(&File, ScannedPack), PinnedShardFetchFailure>>(),
        )?;
        checkpoint(budget, checks)?;
        let pack = opened
            .scope
            .scanned
            .iter()
            .find(|p| p.sequence == sequence)
            .copied()
            .ok_or(PinnedShardFetchFailure::Stale)?;
        if opened.scope.active_slot == Some(pack.slot) {
            if opened.store.active.sequence != sequence
                || opened.store.active.bytes != opened.scope.active_bytes
            {
                return Err(PinnedShardFetchFailure::Stale);
            }
            return Ok((&opened.store.active.file, pack));
        }
        let file = opened
            .scope
            .file(pack.slot)
            .map_err(|_| PinnedShardFetchFailure::Stale)?;
        Ok((file, pack))
    }
    fn prepare_range(
        &mut self,
        opened: &OpenedPinnedShardStore,
        pin: &PinnedShardDirectory,
        sequence: u64,
        budget: &StorageFetchBudget,
        checks: &dyn PinnedShardFetchChecks,
    ) -> Bounded<usize> {
        grow(
            &mut self.observation.ranges,
            1,
            &mut self.observation.allocation,
            budget,
            checks,
        )?;
        let range_values = [
            size_of::<ReadRange>(),
            size_of::<PathBuf>(),
            size_of::<OsString>(),
            size_of::<CString>(),
            size_of::<Option<CString>>(),
            size_of::<NulError>(),
            size_of::<Option<NulError>>(),
            size_of::<Result<CString, NulError>>(),
            size_of::<Vec<u8>>(),
            size_of::<Bytes>(),
            size_of::<Option<Bytes>>(),
            size_of::<Result<(), TryReserveError>>(),
            size_of::<usize>(),
            size_of::<u64>(),
        ];
        let bytes = range_values.into_iter().try_fold(0, add)?;
        debit(budget, bytes, range_values.len(), bytes)?;
        checkpoint(budget, checks)?;
        let slot = self.observation.ranges.len();
        self.observation.ranges.push(ReadRange::new(sequence));
        // Actual retained BTree cardinality times full value width conservatively
        // pays a complete traversal before the point lookup (also for absence).
        debit(
            budget,
            0,
            0,
            mul(
                add(opened.store.pack_paths.len(), 1)?,
                size_of::<(u64, PathBuf)>(),
            )?,
        )?;
        checkpoint(budget, checks)?;
        let path = opened
            .store
            .pack_paths
            .get(&sequence)
            .ok_or(PinnedShardFetchFailure::Stale)?;
        let bytes = path.as_os_str().as_bytes().len();
        if bytes > budget.limits().max_path_bytes {
            return Err(PinnedShardFetchFailure::Budget(
                StorageFetchFailure::Capacity,
            ));
        }
        debit(budget, bytes, 1, mul(add(bytes, 1)?, 8)?)?;
        checkpoint(budget, checks)?;
        self.observation.allocation =
            Some(self.observation.ranges[slot].path.try_reserve_exact(bytes));
        checkpoint(budget, checks)?;
        if matches!(self.observation.allocation, Some(Err(_))) {
            return Err(PinnedShardFetchFailure::Allocation);
        }
        let actual = self.observation.ranges[slot].path.capacity();
        if actual > budget.limits().max_path_bytes {
            return Err(PinnedShardFetchFailure::Allocation);
        }
        debit(
            budget,
            actual.saturating_sub(bytes),
            0,
            actual.saturating_sub(bytes),
        )?;
        self.observation.ranges[slot].path.push(path);
        completed(PinnedShardFetchPhase::PathCopied);
        checkpoint(budget, checks)?;
        // Full pathname scans were paid above, before file_name/parent/equality.
        let saved = &self.observation.ranges[slot].path;
        if saved.parent() != Some(pin.directory()) {
            return Err(PinnedShardFetchFailure::Identity);
        }
        let name = saved.file_name().ok_or(PinnedShardFetchFailure::Identity)?;
        let name_bytes = name.as_bytes().len();
        debit(budget, add(name_bytes, 1)?, 2, mul(add(name_bytes, 1)?, 4)?)?;
        checkpoint(budget, checks)?;
        if name.to_str().and_then(pack_sequence_from_name) != Some(sequence) {
            return Err(PinnedShardFetchFailure::Identity);
        }
        match CString::new(name.as_bytes()) {
            Ok(name) => self.observation.ranges[slot].name = Some(name),
            Err(error) => self.observation.ranges[slot].name_error = Some(error),
        }
        completed(PinnedShardFetchPhase::NameConverted);
        checkpoint(budget, checks)?;
        if self.observation.ranges[slot].name_error.is_some() {
            return Err(PinnedShardFetchFailure::Identity);
        }
        Ok(slot)
    }
    fn validate_range(
        &mut self,
        opened: &OpenedPinnedShardStore,
        pin: &PinnedShardDirectory,
        slot: usize,
        budget: &StorageFetchBudget,
        checks: &dyn PinnedShardFetchChecks,
    ) -> Bounded<()> {
        let sequence = self
            .observation
            .ranges
            .get(slot)
            .ok_or(PinnedShardFetchFailure::Invariant)?
            .sequence;
        let (reader, scanned) = Self::tracked(opened, sequence, budget, checks)?;
        let held =
            self.observation
                .stat(PinnedShardFetchPhase::PackStat, budget, checks, || {
                    fstat(reader).map_err(io::Error::from)
                })?;
        grow(
            &mut self.observation.outcomes,
            1,
            &mut self.observation.allocation,
            budget,
            checks,
        )?;
        debit(budget, size_of::<io::Result<Stat>>(), 2, size_of::<Stat>())?;
        checkpoint(budget, checks)?;
        let name = self.observation.ranges[slot]
            .name
            .as_deref()
            .ok_or(PinnedShardFetchFailure::Invariant)?;
        self.observation
            .outcomes
            .push(PinnedShardFetchOutcome::Stat {
                phase: PinnedShardFetchPhase::PackStat,
                result: statat(&pin.scope.directory, name, AtFlags::SYMLINK_NOFOLLOW)
                    .map_err(io::Error::from),
            });
        completed(PinnedShardFetchPhase::PackStat);
        checkpoint(budget, checks)?;
        let named = facts(
            self.observation
                .outcomes
                .last()
                .ok_or(PinnedShardFetchFailure::Invariant)?,
        )?;
        if !held.regular()
            || !named.regular()
            || held.identity() != named.identity()
            || held.identity() != scanned.identity
        {
            return Err(PinnedShardFetchFailure::Identity);
        }
        Ok(())
    }
    fn read_range(
        &mut self,
        opened: &OpenedPinnedShardStore,
        pin: &PinnedShardDirectory,
        slot: usize,
        offset: u64,
        length: usize,
        budget: &StorageFetchBudget,
        checks: &dyn PinnedShardFetchChecks,
    ) -> Bounded<()> {
        if length > budget.limits().max_read_bytes {
            return Err(PinnedShardFetchFailure::Budget(
                StorageFetchFailure::Capacity,
            ));
        }
        debit(budget, length, 1, length)?;
        checkpoint(budget, checks)?;
        self.observation.allocation = Some(
            self.observation.ranges[slot]
                .buffer
                .try_reserve_exact(length),
        );
        checkpoint(budget, checks)?;
        if matches!(self.observation.allocation, Some(Err(_))) {
            return Err(PinnedShardFetchFailure::Allocation);
        }
        let capacity = self.observation.ranges[slot].buffer.capacity();
        if capacity > budget.limits().max_declared_bytes {
            return Err(PinnedShardFetchFailure::Allocation);
        }
        debit(
            budget,
            capacity.saturating_sub(length),
            0,
            capacity.saturating_sub(length),
        )?;
        checkpoint(budget, checks)?;
        self.observation.ranges[slot].buffer.resize(length, 0);
        completed(PinnedShardFetchPhase::BufferAllocated);
        checkpoint(budget, checks)?;
        while self.observation.ranges[slot].filled < length {
            // Revalidate actual held/named identity before each entered attempt,
            // including retries of a real EINTR. Never reopen or use old pool IO.
            self.validate_range(opened, pin, slot, budget, checks)?;
            let sequence = self.observation.ranges[slot].sequence;
            let (reader, _) = Self::tracked(opened, sequence, budget, checks)?;
            grow(
                &mut self.observation.outcomes,
                1,
                &mut self.observation.allocation,
                budget,
                checks,
            )?;
            let filled = self.observation.ranges[slot].filled;
            let remaining = length - filled;
            let at = offset
                .checked_add(u64::try_from(filled).map_err(|_| PinnedShardFetchFailure::Input)?)
                .ok_or(PinnedShardFetchFailure::Input)?;
            budget
                .charge(size_of::<io::Result<usize>>(), 2, 1, remaining, remaining)
                .map_err(PinnedShardFetchFailure::Budget)?;
            checkpoint(budget, checks)?;
            let actual = pread(
                reader,
                &mut self.observation.ranges[slot].buffer[filled..],
                at,
            )
            .map_err(io::Error::from);
            self.observation
                .outcomes
                .push(PinnedShardFetchOutcome::Read {
                    offset: at,
                    requested: remaining,
                    result: actual,
                });
            // Record actual completed prefix before a late/panicking observer.
            let result = match self.observation.outcomes.last() {
                Some(PinnedShardFetchOutcome::Read { result: Ok(n), .. }) => Ok(*n),
                Some(PinnedShardFetchOutcome::Read {
                    result: Err(error), ..
                }) => Err(error.kind()),
                _ => return Err(PinnedShardFetchFailure::Invariant),
            };
            if let Ok(n) = result {
                self.observation.ranges[slot].filled = add(filled, n)?;
            }
            completed(PinnedShardFetchPhase::Read);
            checkpoint(budget, checks)?;
            match result {
                Ok(0) => return Err(PinnedShardFetchFailure::Eof),
                Ok(_) => {}
                Err(io::ErrorKind::Interrupted) => {}
                Err(_) => return Err(PinnedShardFetchFailure::Io),
            }
        }
        debit(
            budget,
            size_of::<Bytes>() + size_of::<[usize; 2]>(),
            3,
            length,
        )?;
        checkpoint(budget, checks)?;
        let range = &mut self.observation.ranges[slot];
        range.bytes = Some(Bytes::from(std::mem::take(&mut range.buffer)));
        completed(PinnedShardFetchPhase::BufferPublished);
        checkpoint(budget, checks)
    }
    fn run(
        &mut self,
        opened: &OpenedPinnedShardStore,
        pin: &PinnedShardDirectory,
        budget: &StorageFetchBudget,
        checks: &dyn PinnedShardFetchChecks,
    ) -> Bounded<()> {
        let (fixed, entries, _) = fixed_fee()?;
        debit(budget, fixed, entries, fixed)?;
        checkpoint(budget, checks)?;
        let planned = &self.plan.planned;
        if planned.len() > budget.limits().max_plan_batches {
            return Err(PinnedShardFetchFailure::Input);
        }
        debit(
            budget,
            0,
            0,
            mul(planned.len(), size_of::<StoredBatchMetadata>())?,
        )?;
        checkpoint(budget, checks)?;
        if self.plan.store_token != opened.store.store_token
            || self.plan.epoch != opened.store.plan_epoch
            || planned.iter().any(|p| {
                p.store_token != opened.store.store_token || p.plan_epoch != opened.store.plan_epoch
            })
        {
            return Err(PinnedShardFetchFailure::Stale);
        }
        self.observation.validate_pin(opened, pin, budget, checks)?;
        let mut first = 0;
        while first < self.plan.planned.len() {
            let mut end = first + 1;
            let actual = payload_end(&self.plan.planned[first]);
            let mut range_end = self.observation.native(actual, budget, checks)?;
            while end < self.plan.planned.len() {
                debit(budget, 0, 0, size_of::<StoredBatchMetadata>())?;
                checkpoint(budget, checks)?;
                let next = &self.plan.planned[end];
                if !coalesces(&self.plan.planned[first], next, range_end) {
                    break;
                }
                let actual = payload_end(next);
                range_end = self.observation.native(actual, budget, checks)?;
                end += 1;
            }
            let start = self.plan.planned[first].payload_offset;
            let length = usize::try_from(
                range_end
                    .checked_sub(start)
                    .ok_or(PinnedShardFetchFailure::Input)?,
            )
            .map_err(|_| PinnedShardFetchFailure::Input)?;
            let slot = self.prepare_range(
                opened,
                pin,
                self.plan.planned[first].pack_sequence,
                budget,
                checks,
            )?;
            // Also validate zero-payload ranges: no zero-byte bypass of identity.
            self.validate_range(opened, pin, slot, budget, checks)?;
            self.read_range(opened, pin, slot, start, length, budget, checks)?;
            for index in first..end {
                let metadata = self.plan.planned[index];
                debit(
                    budget,
                    0,
                    1,
                    size_of::<StoredBatchMetadata>() + size_of::<StoredBatch>(),
                )?;
                grow(
                    &mut self.batches,
                    1,
                    &mut self.observation.allocation,
                    budget,
                    checks,
                )?;
                let start = usize::try_from(metadata.payload_offset - start)
                    .map_err(|_| PinnedShardFetchFailure::Input)?;
                let stop = add(start, metadata.payload_len)?;
                let bytes = self.observation.ranges[slot]
                    .bytes
                    .as_ref()
                    .ok_or(PinnedShardFetchFailure::Invariant)?;
                if stop > bytes.len() {
                    return Err(PinnedShardFetchFailure::Input);
                }
                self.batches.push(StoredBatch {
                    reservation: metadata.reservation,
                    producer: metadata.producer,
                    atomic_group: metadata.atomic_group,
                    payload: bytes.slice(start..stop),
                });
                completed(PinnedShardFetchPhase::BatchPublished);
                checkpoint(budget, checks)?;
            }
            first = end;
        }
        for slot in 0..self.observation.ranges.len() {
            self.validate_range(opened, pin, slot, budget, checks)?;
        }
        self.observation.validate_pin(opened, pin, budget, checks)?;
        self.observation.usage = Some(budget.usage());
        checkpoint(budget, checks)
    }
}
impl OpenedPinnedShardStore {
    /// Read this real opaque plan from SAME scanned descriptors and real pin.
    ///
    /// No path reopen/fallback occurs. Untracked rotations/active appends refuse.
    /// The caller retains this actual opened store and pin/Core parent through
    /// synchronous physical settlement and returned observations. Canonical log
    /// APIs below enforce this custody by consuming their whole log.
    ///
    /// # Errors
    /// Retains input plan, actual IO/allocation results, completed payloads,
    /// partial bytes and independent unwind/denial payloads. All refusals deny
    /// SAME native counters and composing health before retained cleanup.
    pub fn read_fetch_plan_pinned_until(
        &self,
        pin: &PinnedShardDirectory,
        plan: PinnedShardFetchPlan,
        budget: &StorageFetchBudget,
        checks: &dyn PinnedShardFetchChecks,
    ) -> Result<PinnedShardFetchComplete, PinnedShardFetchReadError> {
        let mut state = ReadState {
            plan,
            batches: Vec::new(),
            observation: PinnedShardFetchObservation::new(),
        };
        let actual = catch_unwind(AssertUnwindSafe(|| state.run(self, pin, budget, checks)));
        let failure = match actual {
            Ok(Ok(())) => None,
            Ok(Err(f)) => Some(f),
            Err(p) => {
                state.observation.panic_payload = Some(p);
                Some(PinnedShardFetchFailure::Unwind)
            }
        };
        if let Some(f) = failure {
            state.observation.refuse(budget, checks, f);
            return Err(PinnedShardFetchReadError { state });
        }
        Ok(PinnedShardFetchComplete { state })
    }
}

#[cfg(test)]
#[path = "pinned_fetch_tests.rs"]
mod tests;
