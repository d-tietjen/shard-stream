//! Bounded native retention GC on the actual pinned directory descriptor.

use super::*;
use rustix::fs::{CWD, RenameFlags, fsync, renameat_with, unlinkat};
use std::collections::TryReserveError;
use std::ffi::{CStr, CString, NulError, OsString};
use std::os::unix::ffi::OsStrExt;

/// Finite source allowances for one actual native collection invocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PinnedShardGcLimits {
    /// Complete held/configured diagnostic directory pathname ceiling.
    pub max_path_bytes: usize,
    /// Literal pack or pending-pack filename ceiling, before conversion/copy.
    pub max_name_bytes: usize,
    /// Existing pack-map length, including the active pack.
    pub max_packs: usize,
    /// Existing batch-map capacity and retained batch-removal slots.
    pub max_batch_slots: usize,
    /// Existing immutable log-start map capacity, before native floor lookups.
    pub max_log_start_slots: usize,
    /// Existing topic-map capacity, copied topic keys and removed topic slots.
    pub max_topic_slots: usize,
    /// Sum of existing topic-vector capacities and retained removed locations.
    pub max_location_slots: usize,
    /// Maximum selected sealed packs and retained per-pack native outcomes.
    pub max_plan_packs: usize,
    /// Cumulative reserved explicit native calls, including settlement pairs.
    pub max_io_calls: usize,
    /// Explicit loop, copy, index movement and native entry work allowance.
    pub max_work_bytes: usize,
}

/// Plain logical source ceilings, prepaid by the composing owner before entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PinnedShardGcCost {
    /// Fixed values, requested vector slots and native name conversion ceilings.
    pub declared_bytes: usize,
    /// Path/name subset already included in declared bytes.
    pub string_bytes: usize,
    /// Fixed values, container values, allocation/native outcomes and panics.
    pub entries: usize,
    /// Setup plus the configured explicit cumulative inner work ceiling.
    pub work_bytes: usize,
    /// Five identity calls plus five native calls per selected sealed pack.
    pub native_call_slots: usize,
}

/// SAME composing checkpoints and a mandatory denial action before settlement.
///
/// `deny` must latch the SAME actual work and captured health owned by the
/// composing operation, when that operation has health. It returns no grant,
/// completion value or witness. The collector stores neither this context nor
/// a Session in its report. A denial panic remains an actual retained refusal;
/// it never becomes a claim that external health was successfully denied.
pub trait PinnedShardGcChecks: ShardDirectoryChecks {
    /// Deny the actual composing operation before physical settlement/cleanup.
    fn deny(&self);
}

/// Actual collector refusal; native outcomes and partial effects remain held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PinnedShardGcFailure {
    /// Original absolute E elapsed.
    Expired,
    /// A SAME-operation checkpoint refused.
    Checkpoint,
    /// A finite input/container/call/work allowance refused.
    Capacity,
    /// Checked cost, counter, epoch or native index arithmetic overflowed.
    Arithmetic,
    /// A real requested vector/name reservation failed.
    Allocation,
    /// Actual bounded native name conversion returned its original NulError.
    Name,
    /// Actual descriptor/name/lock/pack identity refused.
    Identity,
    /// An actual native metadata/rename/sync/unlink returned an error.
    Io,
    /// An actual entered producer phase unwound.
    Unwind,
    /// Actual mutable index state disagreed with its already-built native plan.
    Invariant,
}

/// Actual entered phase, also retained for an entered call without a result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PinnedShardGcPhase {
    /// Actual bounded directory C-name conversion, retained before checking E.
    DirectoryNamePrepared,
    /// Actual pinned directory descriptor metadata.
    DirectoryMetadata,
    /// No-follow original directory pathname metadata.
    NamedDirectoryMetadata,
    /// Actual successfully acquired lock descriptor stat.
    LockDescriptor,
    /// No-follow descriptor-relative named lock stat.
    NamedLock,
    /// Actual store's retained lock descriptor stat.
    StoreLock,
    /// Actual completed directory/lock/store identity predicate.
    Identity,
    /// Actual selected pack and pending C names, retained before checking E.
    NativeNamesPrepared,
    /// Complete bounded native retention plan, before mutable store effects.
    PlanBuilt,
    /// Actual fetch plan epoch advance.
    EpochAdvanced,
    /// Actual selected pack's no-follow native stat.
    PackMetadata,
    /// Actual descriptor-relative no-replace rename attempt.
    Rename,
    /// Actual already-prepaid parent sync after entered rename.
    SyncAfterRename,
    /// Actual selected native pack-map removal.
    PackIndex,
    /// Actual selected native batch-map removal.
    BatchIndex,
    /// Actual selected native topic-vector location removal.
    LocationIndex,
    /// Actual empty native topic-map removal.
    EmptyTopicIndex,
    /// Actual descriptor-relative pending-pack unlink attempt.
    Unlink,
    /// Actual already-prepaid parent sync after entered unlink.
    SyncAfterUnlink,
}

struct Pack {
    sequence: u64,
    name: OsString,
    pending: OsString,
    name_reservation: Option<Result<(), TryReserveError>>,
    pending_reservation: Option<Result<(), TryReserveError>>,
    native_name: Option<CString>,
    native_pending: Option<CString>,
    name_error: Option<NulError>,
    pending_name_error: Option<NulError>,
    extents: u64,
    metadata: Option<io::Result<Stat>>,
    rename_entered: bool,
    rename: Option<io::Result<()>>,
    sync_rename_entered: bool,
    sync_rename: Option<io::Result<()>>,
    sync_rename_outcome: Option<Result<(), PinnedShardGcFailure>>,
    removed_path: Option<Option<PathBuf>>,
    unlink_entered: bool,
    unlink: Option<io::Result<()>>,
    sync_unlink_entered: bool,
    sync_unlink: Option<io::Result<()>>,
    sync_unlink_outcome: Option<Result<(), PinnedShardGcFailure>>,
}
struct BatchRemoval {
    pack: usize,
    key: (TopicPartition, BatchId),
    result: Option<Option<ExtentLocation>>,
}
struct LocationRemoval {
    pack: usize,
    topic: TopicPartition,
    position: usize,
    actual: ExtentLocation,
}
struct TopicRemoval {
    pack: usize,
    topic: TopicPartition,
    actual: Option<Vec<ExtentLocation>>,
}

/// Borrowed actual per-pack outcomes; this contains no owner or serving grant.
pub struct PinnedShardGcPack<'a> {
    pack: &'a Pack,
}
impl PinnedShardGcPack<'_> {
    /// Actual selected native pack sequence.
    pub fn sequence(&self) -> u64 {
        self.pack.sequence
    }
    /// Actual original relative native filename.
    pub fn name(&self) -> &std::ffi::OsStr {
        &self.pack.name
    }
    /// Actual proposed relative pending filename, never silently overwritten.
    pub fn pending_name(&self) -> &std::ffi::OsStr {
        &self.pack.pending
    }
    /// Actual original-name reservation result, retained before its checkpoint.
    pub fn name_reservation(&self) -> Option<&Result<(), TryReserveError>> {
        self.pack.name_reservation.as_ref()
    }
    /// Actual pending-name reservation result, retained before its checkpoint.
    pub fn pending_name_reservation(&self) -> Option<&Result<(), TryReserveError>> {
        self.pack.pending_reservation.as_ref()
    }
    /// Actual prepared native original name, retained before its checkpoint.
    pub fn native_name(&self) -> Option<&CStr> {
        self.pack.native_name.as_deref()
    }
    /// Actual prepared native pending name, retained before its checkpoint.
    pub fn native_pending_name(&self) -> Option<&CStr> {
        self.pack.native_pending.as_deref()
    }
    /// Actual original-name conversion error, independently retained.
    pub fn name_error(&self) -> Option<&NulError> {
        self.pack.name_error.as_ref()
    }
    /// Actual pending-name conversion error, independently retained.
    pub fn pending_name_error(&self) -> Option<&NulError> {
        self.pack.pending_name_error.as_ref()
    }
    /// Actual native stat before mutation, including the original errno.
    pub fn metadata(&self) -> Option<&io::Result<Stat>> {
        self.pack.metadata.as_ref()
    }
    /// Whether the rename actually entered, including an uncertain unwind.
    pub fn rename_entered(&self) -> bool {
        self.pack.rename_entered
    }
    /// Actual native no-replace rename result saved before the next clock.
    pub fn rename_result(&self) -> Option<&io::Result<()>> {
        self.pack.rename.as_ref()
    }
    /// Actual parent synchronization result after entered rename.
    pub fn rename_sync_result(&self) -> Option<&io::Result<()>> {
        self.pack.sync_rename.as_ref()
    }
    /// Actual normal sync-helper result, saved before its next checkpoint.
    pub fn rename_sync_outcome(&self) -> Option<Result<(), PinnedShardGcFailure>> {
        self.pack.sync_rename_outcome
    }
    /// Actual native pack-map removal, distinct from an unentered removal.
    pub fn removed_path(&self) -> Option<Option<&Path>> {
        self.pack.removed_path.as_ref().map(|p| p.as_deref())
    }
    /// Whether unlink actually entered, including an uncertain unwind.
    pub fn unlink_entered(&self) -> bool {
        self.pack.unlink_entered
    }
    /// Actual native pending-pack unlink result.
    pub fn unlink_result(&self) -> Option<&io::Result<()>> {
        self.pack.unlink.as_ref()
    }
    /// Actual parent synchronization result after entered unlink.
    pub fn unlink_sync_result(&self) -> Option<&io::Result<()>> {
        self.pack.sync_unlink.as_ref()
    }
    /// Actual normal sync-helper result, saved before its next checkpoint.
    pub fn unlink_sync_outcome(&self) -> Option<Result<(), PinnedShardGcFailure>> {
        self.pack.sync_unlink_outcome
    }
}

/// Actual cumulative entered/reserved work; no counter is reset during GC.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PinnedShardGcUsage {
    /// Reserved native slots, including already-prepaid namespace settlements.
    pub reserved_io_calls: usize,
    /// Native calls actually entered, including physical settlement after E.
    pub entered_io_calls: usize,
    /// Explicit source work prepaid before loops/copies/index movements.
    pub work_bytes: usize,
}

struct Scope {
    expiry: Instant,
    usage: PinnedShardGcUsage,
    refused: bool,
    checkpoint: Option<ShardDirectoryCheckFailure>,
    directory_name: Option<CString>,
    directory_name_error: Option<NulError>,
    directory: Option<io::Result<Stat>>,
    named_directory: Option<io::Result<Stat>>,
    lock: Option<io::Result<Stat>>,
    named_lock: Option<io::Result<Stat>>,
    store_lock: Option<io::Result<Stat>>,
    identity_matches: Option<bool>,
    allocations: [Option<Result<(), TryReserveError>>; 5],
    packs: Vec<Pack>,
    batches: Vec<BatchRemoval>,
    topics: Vec<TopicPartition>,
    locations: Vec<LocationRemoval>,
    removed_topics: Vec<TopicRemoval>,
    original_epoch: u64,
    actual_epoch: u64,
    stats: GarbageCollectionStats,
    unfinished: Option<(PinnedShardGcPhase, Option<usize>)>,
    pending_sync: Option<(PinnedShardGcPhase, usize)>,
    panic_payload: Option<Box<dyn Any + Send>>,
    denial_panic_payload: Option<Box<dyn Any + Send>>,
    settlement_panic_payload: Option<Box<dyn Any + Send>>,
    settlement_failure: Option<PinnedShardGcFailure>,
}

/// Retained actual native plan, raw outcomes and partial mutable index effects.
///
/// The caller retains the SAME actual store and pinned directory outside this
/// result through physical completion. This report owns no Session, descriptor
/// extractor, recovery witness, serving permission or implicit rollback.
pub struct PinnedShardGcReport {
    scope: Scope,
}
impl fmt::Debug for PinnedShardGcReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardGcReport")
            .field("usage", &self.scope.usage)
            .field("selected_packs", &self.scope.packs.len())
            .field("stats", &self.scope.stats)
            .field("actual_epoch", &self.scope.actual_epoch)
            .field("unfinished", &self.scope.unfinished)
            .field("io_error", &self.io_error())
            .finish_non_exhaustive()
    }
}
impl PinnedShardGcReport {
    /// Actual native directory name prepared before the final entry check.
    pub fn directory_name(&self) -> Option<&CStr> {
        self.scope.directory_name.as_deref()
    }
    /// First actual native C-name conversion refusal, if one occurred.
    pub fn name_error(&self) -> Option<&NulError> {
        self.scope.directory_name_error.as_ref().or_else(|| {
            self.scope.packs.iter().find_map(|pack| {
                pack.name_error
                    .as_ref()
                    .or(pack.pending_name_error.as_ref())
            })
        })
    }
    /// Actual cumulative prepaid/entered ordinary work.
    pub fn usage(&self) -> PinnedShardGcUsage {
        self.scope.usage
    }
    /// Original absolute E retained as diagnostic data, never a renewed budget.
    pub fn absolute_expiry(&self) -> Instant {
        self.scope.expiry
    }
    /// Actual one of five vector reservation results, before any mutable effect.
    pub fn allocation_result(&self, index: usize) -> Option<&Result<(), TryReserveError>> {
        self.scope.allocations.get(index)?.as_ref()
    }
    /// Actual finite selected pack count, including unentered later plans.
    pub fn selected_packs(&self) -> usize {
        self.scope.packs.len()
    }
    /// Borrow actual per-pack native results without copying them.
    pub fn pack(&self, index: usize) -> Option<PinnedShardGcPack<'_>> {
        self.scope
            .packs
            .get(index)
            .map(|pack| PinnedShardGcPack { pack })
    }
    /// Statistics for timely fully processed packs; raw late effects stay held.
    pub fn stats(&self) -> GarbageCollectionStats {
        self.scope.stats
    }
    /// Original and actual fetch epochs, including a refused partial operation.
    pub fn epochs(&self) -> (u64, u64) {
        (self.scope.original_epoch, self.scope.actual_epoch)
    }
    /// Actual completed identity predicate, saved before its post-E checkpoint.
    pub fn identity_matches(&self) -> Option<bool> {
        self.scope.identity_matches
    }
    /// Actual retained original directory descriptor metadata result.
    pub fn directory_metadata(&self) -> Option<&io::Result<Stat>> {
        self.scope.directory.as_ref()
    }
    /// Actual retained no-follow original directory metadata result.
    pub fn named_directory_metadata(&self) -> Option<&io::Result<Stat>> {
        self.scope.named_directory.as_ref()
    }
    /// Actual retained acquired lock descriptor stat result.
    pub fn lock_stat(&self) -> Option<&io::Result<Stat>> {
        self.scope.lock.as_ref()
    }
    /// Actual retained descriptor-relative named lock stat result.
    pub fn named_lock_stat(&self) -> Option<&io::Result<Stat>> {
        self.scope.named_lock.as_ref()
    }
    /// Actual retained store lock descriptor stat result.
    pub fn store_lock_stat(&self) -> Option<&io::Result<Stat>> {
        self.scope.store_lock.as_ref()
    }
    /// Actual first composing checkpoint refusal, if one occurred.
    pub fn checkpoint_failure(&self) -> Option<ShardDirectoryCheckFailure> {
        self.scope.checkpoint
    }
    /// Original producer panic payload, without fabricating a native result.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.scope.panic_payload.as_deref()
    }
    /// Actual denial callback panic; this never asserts successful health denial.
    pub fn denial_panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.scope.denial_panic_payload.as_deref()
    }
    /// Actual physical settlement panic, kept separately from producer failure.
    pub fn settlement_panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.scope.settlement_panic_payload.as_deref()
    }
    /// Actual typed settlement refusal; native errors also stay in their slots.
    pub fn settlement_failure(&self) -> Option<PinnedShardGcFailure> {
        self.scope.settlement_failure
    }
    /// An actual entered call without a saved result, including its plan index.
    pub fn unfinished_phase(&self) -> Option<(PinnedShardGcPhase, Option<usize>)> {
        self.scope.unfinished
    }
    /// Actual entered batch-map removals and actual present values removed.
    pub fn batch_removals(&self) -> (usize, usize) {
        (
            self.scope
                .batches
                .iter()
                .filter(|v| v.result.is_some())
                .count(),
            self.scope
                .batches
                .iter()
                .filter(|v| matches!(v.result, Some(Some(_))))
                .count(),
        )
    }
    /// Actual planned key and entered-removal presence, without a false result.
    pub fn batch_removal(
        &self,
        index: usize,
    ) -> Option<(usize, (TopicPartition, BatchId), Option<bool>)> {
        self.scope
            .batches
            .get(index)
            .map(|v| (v.pack, v.key, v.result.as_ref().map(Option::is_some)))
    }
    /// Actual retained removed batch reservation; unentered/missing stay absent.
    pub fn removed_batch_reservation(&self, index: usize) -> Option<Reservation> {
        self.scope
            .batches
            .get(index)?
            .result
            .as_ref()?
            .as_ref()
            .map(|v| v.reservation)
    }
    /// Actual retained location-vector removal count.
    pub fn location_removals(&self) -> usize {
        self.scope.locations.len()
    }
    /// Actual retained removed location and its native topic/index provenance.
    pub fn removed_location(
        &self,
        index: usize,
    ) -> Option<(usize, TopicPartition, usize, Reservation)> {
        self.scope
            .locations
            .get(index)
            .map(|v| (v.pack, v.topic, v.position, v.actual.reservation))
    }
    /// Actual retained empty-topic removal and the original vector allocation.
    pub fn removed_topic(&self, index: usize) -> Option<(usize, TopicPartition, Option<usize>)> {
        self.scope
            .removed_topics
            .get(index)
            .map(|v| (v.pack, v.topic, v.actual.as_ref().map(Vec::capacity)))
    }
    /// The first original native error; all later outcomes remain in the plan.
    pub fn io_error(&self) -> Option<&io::Error> {
        for value in [
            &self.scope.directory,
            &self.scope.named_directory,
            &self.scope.lock,
            &self.scope.named_lock,
            &self.scope.store_lock,
        ] {
            if let Some(Err(error)) = value {
                return Some(error);
            }
        }
        for pack in &self.scope.packs {
            if let Some(Err(error)) = &pack.metadata {
                return Some(error);
            }
            for value in [
                &pack.rename,
                &pack.sync_rename,
                &pack.unlink,
                &pack.sync_unlink,
            ] {
                if let Some(Err(error)) = value {
                    return Some(error);
                }
            }
        }
        None
    }
    /// The actual first allocation refusal, distinct from a native errno.
    pub fn allocation_error(&self) -> Option<&TryReserveError> {
        self.scope
            .allocations
            .iter()
            .chain(
                self.scope
                    .packs
                    .iter()
                    .flat_map(|pack| [&pack.name_reservation, &pack.pending_reservation]),
            )
            .find_map(|value| value.as_ref().and_then(|value| value.as_ref().err()))
    }
}

/// Actual refusal retaining native outcomes and all partial collector effects.
pub struct PinnedShardGcError {
    failure: PinnedShardGcFailure,
    report: PinnedShardGcReport,
}
impl PinnedShardGcError {
    /// Actual terminal producer refusal.
    pub fn failure(&self) -> PinnedShardGcFailure {
        self.failure
    }
    /// Exact partial native observations and retained index effects.
    pub fn report(&self) -> &PinnedShardGcReport {
        &self.report
    }
}
impl fmt::Debug for PinnedShardGcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardGcError")
            .field("failure", &self.failure)
            .field("report", &self.report)
            .finish()
    }
}
impl fmt::Display for PinnedShardGcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pinned native GC refused: {:?}", self.failure)
    }
}
impl Error for PinnedShardGcError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(error) = self.report.io_error() {
            return Some(error);
        }
        if let Some(error) = self.report.name_error() {
            return Some(error);
        }
        self.report
            .allocation_error()
            .map(|error| error as &dyn Error)
    }
}

/// Derive checked source ceilings before entering the actual collector.
///
/// Existing store/log-start/pin allocations are already owned inputs and are
/// not copied by this cost. Requested new vector/name slots and raw results are
/// included. Allocator/library/kernel/RSS overhead requires measured gates.
pub fn pinned_shard_gc_cost(limits: PinnedShardGcLimits) -> Option<PinnedShardGcCost> {
    if limits.max_plan_packs > limits.max_packs
        || limits.max_name_bytes == 0
        || limits.max_path_bytes == 0
    {
        return None;
    }
    let values = [
        size_of::<Scope>(),
        size_of::<PinnedShardGcReport>(),
        size_of::<PinnedShardGcError>(),
        size_of::<PinnedShardGcLimits>(),
        size_of::<PinnedShardGcCost>(),
        size_of::<PinnedShardGcUsage>(),
        size_of::<CString>(),
        size_of::<NulError>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Result<(), PinnedShardGcFailure>>(),
    ];
    let fixed = values.into_iter().try_fold(0usize, usize::checked_add)?;
    let slots = [
        (limits.max_plan_packs, size_of::<Pack>()),
        (limits.max_batch_slots, size_of::<BatchRemoval>()),
        (limits.max_topic_slots, size_of::<TopicPartition>()),
        (limits.max_location_slots, size_of::<LocationRemoval>()),
        (limits.max_topic_slots, size_of::<TopicRemoval>()),
    ];
    let vector_bytes = slots.iter().try_fold(0usize, |sum, (n, bytes)| {
        sum.checked_add(n.checked_mul(*bytes)?)
    })?;
    let vector_entries = slots
        .iter()
        .try_fold(0usize, |sum, (n, _)| sum.checked_add(*n))?;
    // Two retained names and two retained conversions with temporary ceilings.
    let per_pack_strings = limits
        .max_name_bytes
        .checked_mul(2)?
        .checked_add(limits.max_name_bytes.checked_add(1)?.checked_mul(4)?)?;
    let strings = limits
        .max_path_bytes
        .checked_add(1)?
        .checked_mul(2)?
        .checked_add(".shard-store.lock".len() + 1)?
        .checked_add(per_pack_strings.checked_mul(limits.max_plan_packs)?)?;
    let per_pack_values = [
        size_of::<CString>(),
        size_of::<CString>(),
        size_of::<NulError>(),
        size_of::<NulError>(),
        size_of::<Result<(), PinnedShardGcFailure>>(),
        size_of::<Result<(), PinnedShardGcFailure>>(),
    ];
    let per_pack_value_bytes = per_pack_values
        .into_iter()
        .try_fold(0usize, usize::checked_add)?;
    let declared_bytes = fixed
        .checked_add(vector_bytes)?
        .checked_add(strings)?
        .checked_add(per_pack_value_bytes.checked_mul(limits.max_plan_packs)?)?;
    let calls = limits.max_plan_packs.checked_mul(5)?.checked_add(5)?;
    // Five vector reservations, all native outcomes, two original names and
    // their reservation results, retained conversions/errors and sync results.
    // The fixed values already reserve each of three independent panic slots.
    let entries = values
        .len()
        .checked_add(vector_entries)?
        .checked_add(5)?
        .checked_add(calls)?
        .checked_add(
            limits
                .max_plan_packs
                .checked_mul(4usize.checked_add(per_pack_values.len())?)?,
        )?;
    Some(PinnedShardGcCost {
        declared_bytes,
        string_bytes: strings,
        entries,
        work_bytes: declared_bytes
            .checked_add(entries)?
            .checked_add(limits.max_work_bytes)?,
        native_call_slots: calls,
    })
}

fn completed(phase: PinnedShardGcPhase) {
    #[cfg(test)]
    tests::completed(phase);
    #[cfg(not(test))]
    let _ = phase;
}
impl Scope {
    fn new(expiry: Instant, epoch: u64) -> Self {
        Self {
            expiry,
            usage: PinnedShardGcUsage::default(),
            refused: false,
            checkpoint: None,
            directory_name: None,
            directory_name_error: None,
            directory: None,
            named_directory: None,
            lock: None,
            named_lock: None,
            store_lock: None,
            identity_matches: None,
            allocations: [None, None, None, None, None],
            packs: Vec::new(),
            batches: Vec::new(),
            topics: Vec::new(),
            locations: Vec::new(),
            removed_topics: Vec::new(),
            original_epoch: epoch,
            actual_epoch: epoch,
            stats: GarbageCollectionStats::default(),
            unfinished: None,
            pending_sync: None,
            panic_payload: None,
            denial_panic_payload: None,
            settlement_panic_payload: None,
            settlement_failure: None,
        }
    }
    fn check(&mut self, checks: &dyn PinnedShardGcChecks) -> Result<(), PinnedShardGcFailure> {
        if Instant::now() >= self.expiry {
            return Err(PinnedShardGcFailure::Expired);
        }
        if let Err(error) = checks.check() {
            self.checkpoint = Some(error);
        }
        if Instant::now() >= self.expiry {
            return Err(PinnedShardGcFailure::Expired);
        }
        if self.checkpoint.is_some() {
            return Err(PinnedShardGcFailure::Checkpoint);
        }
        if self.refused {
            return Err(PinnedShardGcFailure::Invariant);
        }
        Ok(())
    }
    fn work(
        &mut self,
        bytes: usize,
        limits: PinnedShardGcLimits,
        checks: &dyn PinnedShardGcChecks,
    ) -> Result<(), PinnedShardGcFailure> {
        self.check(checks)?;
        let next = self
            .usage
            .work_bytes
            .checked_add(bytes)
            .ok_or(PinnedShardGcFailure::Arithmetic)?;
        if next > limits.max_work_bytes {
            return Err(PinnedShardGcFailure::Capacity);
        }
        self.usage.work_bytes = next;
        self.check(checks)
    }
    fn reserve_calls(
        &mut self,
        count: usize,
        limits: PinnedShardGcLimits,
        checks: &dyn PinnedShardGcChecks,
    ) -> Result<(), PinnedShardGcFailure> {
        self.work(count, limits, checks)?;
        let next = self
            .usage
            .reserved_io_calls
            .checked_add(count)
            .ok_or(PinnedShardGcFailure::Arithmetic)?;
        if next > limits.max_io_calls {
            return Err(PinnedShardGcFailure::Capacity);
        }
        self.usage.reserved_io_calls = next;
        self.check(checks)
    }
    fn enter(
        &mut self,
        phase: PinnedShardGcPhase,
        pack: Option<usize>,
    ) -> Result<(), PinnedShardGcFailure> {
        self.usage.entered_io_calls = self
            .usage
            .entered_io_calls
            .checked_add(1)
            .ok_or(PinnedShardGcFailure::Arithmetic)?;
        self.unfinished = Some((phase, pack));
        Ok(())
    }
    fn after(
        &mut self,
        phase: PinnedShardGcPhase,
        checks: &dyn PinnedShardGcChecks,
    ) -> Result<(), PinnedShardGcFailure> {
        self.unfinished = None;
        completed(phase);
        self.check(checks)
    }
    fn allocate(
        &mut self,
        limits: PinnedShardGcLimits,
        checks: &dyn PinnedShardGcChecks,
    ) -> Result<(), PinnedShardGcFailure> {
        self.check(checks)?;
        self.allocations[0] = Some(self.packs.try_reserve_exact(limits.max_plan_packs));
        self.check(checks)?;
        if matches!(self.allocations[0], Some(Err(_))) {
            return Err(PinnedShardGcFailure::Allocation);
        }
        self.allocations[1] = Some(self.batches.try_reserve_exact(limits.max_batch_slots));
        self.check(checks)?;
        if matches!(self.allocations[1], Some(Err(_))) {
            return Err(PinnedShardGcFailure::Allocation);
        }
        self.allocations[2] = Some(self.topics.try_reserve_exact(limits.max_topic_slots));
        self.check(checks)?;
        if matches!(self.allocations[2], Some(Err(_))) {
            return Err(PinnedShardGcFailure::Allocation);
        }
        self.allocations[3] = Some(self.locations.try_reserve_exact(limits.max_location_slots));
        self.check(checks)?;
        if matches!(self.allocations[3], Some(Err(_))) {
            return Err(PinnedShardGcFailure::Allocation);
        }
        self.allocations[4] = Some(
            self.removed_topics
                .try_reserve_exact(limits.max_topic_slots),
        );
        self.check(checks)?;
        if matches!(self.allocations[4], Some(Err(_))) {
            return Err(PinnedShardGcFailure::Allocation);
        }
        Ok(())
    }
    fn identify(
        &mut self,
        store: &ShardStore,
        pin: &PinnedShardDirectory,
        limits: PinnedShardGcLimits,
        checks: &dyn PinnedShardGcChecks,
    ) -> Result<(), PinnedShardGcFailure> {
        self.check(checks)?;
        if store.config.directory.as_os_str().len() > limits.max_path_bytes
            || pin.directory().as_os_str().len() > limits.max_path_bytes
        {
            return Err(PinnedShardGcFailure::Capacity);
        }
        self.work(
            store
                .config
                .directory
                .as_os_str()
                .len()
                .checked_add(pin.directory().as_os_str().len())
                .ok_or(PinnedShardGcFailure::Arithmetic)?,
            limits,
            checks,
        )?;
        if store.config.directory != pin.directory() {
            return Err(PinnedShardGcFailure::Identity);
        }
        self.work(
            pin.directory()
                .as_os_str()
                .len()
                .checked_add(1)
                .and_then(|n| n.checked_mul(4))
                .ok_or(PinnedShardGcFailure::Arithmetic)?,
            limits,
            checks,
        )?;
        match CString::new(pin.directory().as_os_str().as_bytes()) {
            Ok(name) => self.directory_name = Some(name),
            Err(error) => self.directory_name_error = Some(error),
        }
        completed(PinnedShardGcPhase::DirectoryNamePrepared);
        self.check(checks)?;
        if self.directory_name_error.is_some() {
            return Err(PinnedShardGcFailure::Name);
        }
        self.reserve_calls(1, limits, checks)?;
        self.enter(PinnedShardGcPhase::DirectoryMetadata, None)?;
        self.directory = Some(fstat(&pin.scope.directory).map_err(io::Error::from));
        self.after(PinnedShardGcPhase::DirectoryMetadata, checks)?;
        if matches!(self.directory, Some(Err(_))) {
            return Err(PinnedShardGcFailure::Io);
        }
        self.reserve_calls(1, limits, checks)?;
        self.enter(PinnedShardGcPhase::NamedDirectoryMetadata, None)?;
        self.named_directory = Some(
            statat(
                CWD,
                self.directory_name
                    .as_deref()
                    .ok_or(PinnedShardGcFailure::Invariant)?,
                AtFlags::SYMLINK_NOFOLLOW,
            )
            .map_err(io::Error::from),
        );
        self.after(PinnedShardGcPhase::NamedDirectoryMetadata, checks)?;
        if matches!(self.named_directory, Some(Err(_))) {
            return Err(PinnedShardGcFailure::Io);
        }
        self.reserve_calls(1, limits, checks)?;
        self.enter(PinnedShardGcPhase::LockDescriptor, None)?;
        self.lock = Some(fstat(&pin.scope.guard.guard().file).map_err(io::Error::from));
        self.after(PinnedShardGcPhase::LockDescriptor, checks)?;
        if matches!(self.lock, Some(Err(_))) {
            return Err(PinnedShardGcFailure::Io);
        }
        self.reserve_calls(1, limits, checks)?;
        self.enter(PinnedShardGcPhase::NamedLock, None)?;
        self.named_lock = Some(
            statat(
                &pin.scope.directory,
                c".shard-store.lock",
                AtFlags::SYMLINK_NOFOLLOW,
            )
            .map_err(io::Error::from),
        );
        self.after(PinnedShardGcPhase::NamedLock, checks)?;
        if matches!(self.named_lock, Some(Err(_))) {
            return Err(PinnedShardGcFailure::Io);
        }
        self.reserve_calls(1, limits, checks)?;
        self.enter(PinnedShardGcPhase::StoreLock, None)?;
        self.store_lock = Some(fstat(&store._lock_file).map_err(io::Error::from));
        self.after(PinnedShardGcPhase::StoreLock, checks)?;
        if matches!(self.store_lock, Some(Err(_))) {
            return Err(PinnedShardGcFailure::Io);
        }
        let matched = match (
            &self.directory,
            &self.named_directory,
            &self.lock,
            &self.named_lock,
            &self.store_lock,
        ) {
            (
                Some(Ok(dir)),
                Some(Ok(named)),
                Some(Ok(lock)),
                Some(Ok(named_lock)),
                Some(Ok(store_lock)),
            ) => {
                FileType::from_raw_mode(dir.st_mode) == FileType::Directory
                    && FileType::from_raw_mode(named.st_mode) == FileType::Directory
                    && dir.st_dev == named.st_dev
                    && dir.st_ino == named.st_ino
                    && [lock, named_lock, store_lock].into_iter().all(|v| {
                        FileType::from_raw_mode(v.st_mode) == FileType::RegularFile
                            && v.st_dev == lock.st_dev
                            && v.st_ino == lock.st_ino
                    })
            }
            _ => false,
        };
        self.identity_matches = Some(matched);
        completed(PinnedShardGcPhase::Identity);
        self.check(checks)?;
        if !matched {
            return Err(PinnedShardGcFailure::Identity);
        }
        Ok(())
    }
    fn copy_name(
        &mut self,
        index: usize,
        name: &std::ffi::OsStr,
        suffix: &str,
        limits: PinnedShardGcLimits,
        checks: &dyn PinnedShardGcChecks,
    ) -> Result<(), PinnedShardGcFailure> {
        let len = name
            .as_bytes()
            .len()
            .checked_add(suffix.len())
            .ok_or(PinnedShardGcFailure::Arithmetic)?;
        if len > limits.max_name_bytes {
            return Err(PinnedShardGcFailure::Capacity);
        }
        // Admit the two byte scans and the eventual copy before scanning.
        self.work(
            len.checked_mul(3).ok_or(PinnedShardGcFailure::Arithmetic)?,
            limits,
            checks,
        )?;
        if name.as_bytes().is_empty()
            || name.as_bytes().contains(&0)
            || name.as_bytes().contains(&b'/')
            || name == std::ffi::OsStr::new(".")
            || name == std::ffi::OsStr::new("..")
        {
            return Err(PinnedShardGcFailure::Capacity);
        }
        let pack = self
            .packs
            .get_mut(index)
            .ok_or(PinnedShardGcFailure::Invariant)?;
        let (target, reservation) = if suffix.is_empty() {
            (&mut pack.name, &mut pack.name_reservation)
        } else {
            (&mut pack.pending, &mut pack.pending_reservation)
        };
        *reservation = Some(target.try_reserve_exact(len));
        let refused = matches!(reservation, Some(Err(_)));
        self.check(checks)?;
        if refused {
            return Err(PinnedShardGcFailure::Allocation);
        }
        let pack = self
            .packs
            .get_mut(index)
            .ok_or(PinnedShardGcFailure::Invariant)?;
        let target = if suffix.is_empty() {
            &mut pack.name
        } else {
            &mut pack.pending
        };
        target.push(name);
        target.push(suffix);
        self.check(checks)
    }
    fn prepare_native_names(
        &mut self,
        index: usize,
        limits: PinnedShardGcLimits,
        checks: &dyn PinnedShardGcChecks,
    ) -> Result<(), PinnedShardGcFailure> {
        let pack = self
            .packs
            .get(index)
            .ok_or(PinnedShardGcFailure::Invariant)?;
        self.work(
            pack.name
                .len()
                .checked_add(1)
                .and_then(|n| n.checked_mul(4))
                .ok_or(PinnedShardGcFailure::Arithmetic)?,
            limits,
            checks,
        )?;
        let pack = self
            .packs
            .get_mut(index)
            .ok_or(PinnedShardGcFailure::Invariant)?;
        match CString::new(pack.name.as_bytes()) {
            Ok(name) => pack.native_name = Some(name),
            Err(error) => pack.name_error = Some(error),
        }
        self.check(checks)?;
        if self.packs[index].name_error.is_some() {
            return Err(PinnedShardGcFailure::Name);
        }
        let pack = self
            .packs
            .get(index)
            .ok_or(PinnedShardGcFailure::Invariant)?;
        self.work(
            pack.pending
                .len()
                .checked_add(1)
                .and_then(|n| n.checked_mul(4))
                .ok_or(PinnedShardGcFailure::Arithmetic)?,
            limits,
            checks,
        )?;
        let pack = self
            .packs
            .get_mut(index)
            .ok_or(PinnedShardGcFailure::Invariant)?;
        match CString::new(pack.pending.as_bytes()) {
            Ok(name) => pack.native_pending = Some(name),
            Err(error) => pack.pending_name_error = Some(error),
        }
        completed(PinnedShardGcPhase::NativeNamesPrepared);
        self.check(checks)?;
        if self.packs[index].pending_name_error.is_some() {
            return Err(PinnedShardGcFailure::Name);
        }
        Ok(())
    }
    fn plan(
        &mut self,
        store: &ShardStore,
        starts: &HashMap<TopicPartition, LogicalOffset>,
        limits: PinnedShardGcLimits,
        checks: &dyn PinnedShardGcChecks,
    ) -> Result<(), PinnedShardGcFailure> {
        self.check(checks)?;
        if store.pack_paths.len() > limits.max_packs
            || store.by_batch.capacity() > limits.max_batch_slots
            || starts.capacity() > limits.max_log_start_slots
            || store.index.capacity() > limits.max_topic_slots
        {
            return Err(PinnedShardGcFailure::Capacity);
        }
        let mut capacity = 0usize;
        // Native HashMap iteration visits buckets, including empty buckets.
        // Pay the actual bounded capacity before constructing each traversal.
        self.work(store.index.capacity(), limits, checks)?;
        let mut topics = store.index.iter();
        loop {
            self.work(1, limits, checks)?;
            let actual = topics.next();
            self.check(checks)?;
            let Some((&topic, locations)) = actual else {
                break;
            };
            capacity = capacity
                .checked_add(locations.capacity())
                .ok_or(PinnedShardGcFailure::Arithmetic)?;
            if capacity > limits.max_location_slots || self.topics.len() >= limits.max_topic_slots {
                return Err(PinnedShardGcFailure::Capacity);
            }
            self.topics.push(topic);
        }
        self.work(store.pack_paths.len(), limits, checks)?;
        let mut packs = store.pack_paths.range(..store.active.sequence);
        loop {
            self.work(1, limits, checks)?;
            let actual = packs.next();
            self.check(checks)?;
            let Some((&sequence, path)) = actual else {
                break;
            };
            let start = self.batches.len();
            let mut extents = 0u64;
            let mut eligible = true;
            self.work(store.by_batch.capacity(), limits, checks)?;
            let mut batches = store.by_batch.iter();
            loop {
                self.work(1, limits, checks)?;
                let actual = batches.next();
                self.check(checks)?;
                let Some((&key, location)) = actual else {
                    break;
                };
                if location.pack_sequence != sequence {
                    continue;
                }
                extents = extents
                    .checked_add(1)
                    .ok_or(PinnedShardGcFailure::Arithmetic)?;
                self.work(starts.capacity(), limits, checks)?;
                eligible &= gc_location_before_start(location, starts);
                if self.batches.len() >= limits.max_batch_slots {
                    return Err(PinnedShardGcFailure::Capacity);
                }
                self.batches.push(BatchRemoval {
                    pack: self.packs.len(),
                    key,
                    result: None,
                });
            }
            if extents == 0 || !eligible {
                self.batches.truncate(start);
                continue;
            }
            if self.packs.len() >= limits.max_plan_packs {
                return Err(PinnedShardGcFailure::Capacity);
            }
            let path_bytes = path.as_os_str().len();
            if path_bytes
                > limits
                    .max_path_bytes
                    .checked_add(1)
                    .and_then(|n| n.checked_add(limits.max_name_bytes))
                    .ok_or(PinnedShardGcFailure::Arithmetic)?
            {
                return Err(PinnedShardGcFailure::Capacity);
            }
            self.work(
                // Full component/name/parent/equality scans, including both
                // compared paths, are paid before the first component parse.
                path_bytes
                    .checked_mul(8)
                    .ok_or(PinnedShardGcFailure::Arithmetic)?,
                limits,
                checks,
            )?;
            let name = path.file_name().ok_or(PinnedShardGcFailure::Identity)?;
            if name.as_bytes().len() > limits.max_name_bytes {
                return Err(PinnedShardGcFailure::Capacity);
            }
            // Preserve the native UTF-8 and accepted decimal-spelling parser.
            self.work(
                name.as_bytes()
                    .len()
                    .checked_mul(4)
                    .ok_or(PinnedShardGcFailure::Arithmetic)?,
                limits,
                checks,
            )?;
            if path.parent() != Some(store.config.directory.as_path()) {
                return Err(PinnedShardGcFailure::Identity);
            }
            if name.to_str().and_then(pack_sequence_from_name) != Some(sequence) {
                return Err(PinnedShardGcFailure::Identity);
            }
            let index = self.packs.len();
            self.packs.push(Pack {
                sequence,
                name: OsString::new(),
                pending: OsString::new(),
                name_reservation: None,
                pending_reservation: None,
                native_name: None,
                native_pending: None,
                name_error: None,
                pending_name_error: None,
                extents,
                metadata: None,
                rename_entered: false,
                rename: None,
                sync_rename_entered: false,
                sync_rename: None,
                sync_rename_outcome: None,
                removed_path: None,
                unlink_entered: false,
                unlink: None,
                sync_unlink_entered: false,
                sync_unlink: None,
                sync_unlink_outcome: None,
            });
            self.copy_name(index, name, "", limits, checks)?;
            self.copy_name(index, name, ".gc-pending", limits, checks)?;
            self.prepare_native_names(index, limits, checks)?;
        }
        completed(PinnedShardGcPhase::PlanBuilt);
        self.check(checks)
    }
    fn sync_pending(
        &mut self,
        pin: &PinnedShardDirectory,
        notify: bool,
    ) -> Result<(), PinnedShardGcFailure> {
        let Some((phase, index)) = self.pending_sync.take() else {
            return Ok(());
        };
        let pack = self
            .packs
            .get_mut(index)
            .ok_or(PinnedShardGcFailure::Invariant)?;
        match phase {
            PinnedShardGcPhase::SyncAfterRename => pack.sync_rename_entered = true,
            PinnedShardGcPhase::SyncAfterUnlink => pack.sync_unlink_entered = true,
            _ => return Err(PinnedShardGcFailure::Invariant),
        }
        self.enter(phase, Some(index))?;
        let actual = fsync(&pin.scope.directory).map_err(io::Error::from);
        let failed = actual.is_err();
        match phase {
            PinnedShardGcPhase::SyncAfterRename => self.packs[index].sync_rename = Some(actual),
            PinnedShardGcPhase::SyncAfterUnlink => self.packs[index].sync_unlink = Some(actual),
            _ => return Err(PinnedShardGcFailure::Invariant),
        }
        self.unfinished = None;
        if notify {
            completed(phase);
        }
        if failed {
            return Err(PinnedShardGcFailure::Io);
        }
        Ok(())
    }
    fn remove_indexes(
        &mut self,
        store: &mut ShardStore,
        index: usize,
        limits: PinnedShardGcLimits,
        checks: &dyn PinnedShardGcChecks,
    ) -> Result<(), PinnedShardGcFailure> {
        let sequence = self.packs[index].sequence;
        self.work(store.pack_paths.len(), limits, checks)?;
        self.packs[index].removed_path = Some(store.pack_paths.remove(&sequence));
        completed(PinnedShardGcPhase::PackIndex);
        self.check(checks)?;
        if !matches!(self.packs[index].removed_path, Some(Some(_))) {
            return Err(PinnedShardGcFailure::Invariant);
        }
        for slot in 0..self.batches.len() {
            self.work(1, limits, checks)?;
            if self.batches[slot].pack != index {
                continue;
            }
            let key = self.batches[slot].key;
            self.work(store.by_batch.capacity(), limits, checks)?;
            self.batches[slot].result = Some(store.by_batch.remove(&key));
            completed(PinnedShardGcPhase::BatchIndex);
            self.check(checks)?;
            if !matches!(self.batches[slot].result, Some(Some(_))) {
                return Err(PinnedShardGcFailure::Invariant);
            }
        }
        for slot in 0..self.topics.len() {
            self.work(1, limits, checks)?;
            let topic = self.topics[slot];
            self.work(store.index.capacity(), limits, checks)?;
            let Some(locations) = store.index.get_mut(&topic) else {
                continue;
            };
            let mut position = 0usize;
            while position < locations.len() {
                self.work(1, limits, checks)?;
                if locations[position].pack_sequence != sequence {
                    position += 1;
                    continue;
                }
                if self.locations.len() >= limits.max_location_slots {
                    return Err(PinnedShardGcFailure::Capacity);
                }
                let movement = locations
                    .len()
                    .checked_sub(position + 1)
                    .and_then(|n| n.checked_mul(size_of::<ExtentLocation>()))
                    .ok_or(PinnedShardGcFailure::Arithmetic)?;
                self.work(movement, limits, checks)?;
                let actual = locations.remove(position);
                self.locations.push(LocationRemoval {
                    pack: index,
                    topic,
                    position,
                    actual,
                });
                completed(PinnedShardGcPhase::LocationIndex);
                self.check(checks)?;
            }
            if locations.is_empty() {
                if self.removed_topics.len() >= limits.max_topic_slots {
                    return Err(PinnedShardGcFailure::Capacity);
                }
                self.work(store.index.capacity(), limits, checks)?;
                let actual = store.index.remove(&topic);
                self.removed_topics.push(TopicRemoval {
                    pack: index,
                    topic,
                    actual,
                });
                completed(PinnedShardGcPhase::EmptyTopicIndex);
                self.check(checks)?;
            }
        }
        Ok(())
    }
    fn collect(
        &mut self,
        store: &mut ShardStore,
        pin: &PinnedShardDirectory,
        limits: PinnedShardGcLimits,
        checks: &dyn PinnedShardGcChecks,
    ) -> Result<(), PinnedShardGcFailure> {
        if !self.packs.is_empty() {
            self.work(1, limits, checks)?;
            let next = store
                .plan_epoch
                .checked_add(1)
                .ok_or(PinnedShardGcFailure::Arithmetic)?;
            store.plan_epoch = next;
            self.actual_epoch = next;
            completed(PinnedShardGcPhase::EpochAdvanced);
            self.check(checks)?;
        }
        for index in 0..self.packs.len() {
            self.reserve_calls(1, limits, checks)?;
            self.enter(PinnedShardGcPhase::PackMetadata, Some(index))?;
            self.packs[index].metadata = Some(
                statat(
                    &pin.scope.directory,
                    self.packs[index]
                        .native_name
                        .as_deref()
                        .ok_or(PinnedShardGcFailure::Invariant)?,
                    AtFlags::SYMLINK_NOFOLLOW,
                )
                .map_err(io::Error::from),
            );
            self.after(PinnedShardGcPhase::PackMetadata, checks)?;
            let bytes = match self.packs[index].metadata.as_ref() {
                Some(Ok(stat))
                    if FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile
                        && stat.st_nlink == 1 =>
                {
                    u64::try_from(stat.st_size).map_err(|_| PinnedShardGcFailure::Identity)?
                }
                Some(Ok(_)) => return Err(PinnedShardGcFailure::Identity),
                Some(Err(_)) => return Err(PinnedShardGcFailure::Io),
                None => return Err(PinnedShardGcFailure::Invariant),
            };
            self.reserve_calls(2, limits, checks)?;
            self.enter(PinnedShardGcPhase::Rename, Some(index))?;
            self.packs[index].rename_entered = true;
            self.pending_sync = Some((PinnedShardGcPhase::SyncAfterRename, index));
            let pack = &self.packs[index];
            let actual = renameat_with(
                &pin.scope.directory,
                pack.native_name
                    .as_deref()
                    .ok_or(PinnedShardGcFailure::Invariant)?,
                &pin.scope.directory,
                pack.native_pending
                    .as_deref()
                    .ok_or(PinnedShardGcFailure::Invariant)?,
                RenameFlags::NOREPLACE,
            )
            .map_err(io::Error::from);
            self.packs[index].rename = Some(actual);
            self.after(PinnedShardGcPhase::Rename, checks)?;
            if !matches!(self.packs[index].rename, Some(Ok(()))) {
                return Err(PinnedShardGcFailure::Io);
            }
            self.check(checks)?;
            self.packs[index].sync_rename_outcome = Some(self.sync_pending(pin, true));
            self.check(checks)?;
            self.packs[index]
                .sync_rename_outcome
                .ok_or(PinnedShardGcFailure::Invariant)??;
            self.remove_indexes(store, index, limits, checks)?;
            self.reserve_calls(2, limits, checks)?;
            self.enter(PinnedShardGcPhase::Unlink, Some(index))?;
            self.packs[index].unlink_entered = true;
            self.pending_sync = Some((PinnedShardGcPhase::SyncAfterUnlink, index));
            self.packs[index].unlink = Some(
                unlinkat(
                    &pin.scope.directory,
                    self.packs[index]
                        .native_pending
                        .as_deref()
                        .ok_or(PinnedShardGcFailure::Invariant)?,
                    AtFlags::empty(),
                )
                .map_err(io::Error::from),
            );
            self.after(PinnedShardGcPhase::Unlink, checks)?;
            if !matches!(self.packs[index].unlink, Some(Ok(()))) {
                return Err(PinnedShardGcFailure::Io);
            }
            self.check(checks)?;
            self.packs[index].sync_unlink_outcome = Some(self.sync_pending(pin, true));
            self.check(checks)?;
            self.packs[index]
                .sync_unlink_outcome
                .ok_or(PinnedShardGcFailure::Invariant)??;
            self.stats.packs_removed = self.stats.packs_removed.saturating_add(1);
            self.stats.extents_removed = self
                .stats
                .extents_removed
                .saturating_add(self.packs[index].extents);
            self.stats.bytes_removed = self.stats.bytes_removed.saturating_add(bytes);
        }
        self.check(checks)
    }
    fn run(
        &mut self,
        store: &mut ShardStore,
        pin: &PinnedShardDirectory,
        starts: &HashMap<TopicPartition, LogicalOffset>,
        limits: PinnedShardGcLimits,
        checks: &dyn PinnedShardGcChecks,
    ) -> Result<(), PinnedShardGcFailure> {
        self.check(checks)?;
        if limits.max_plan_packs > limits.max_packs
            || limits.max_name_bytes == 0
            || limits.max_path_bytes == 0
        {
            return Err(PinnedShardGcFailure::Capacity);
        }
        pinned_shard_gc_cost(limits).ok_or(PinnedShardGcFailure::Arithmetic)?;
        self.identify(store, pin, limits, checks)?;
        // Reject actual native container capacities before requesting plan slots.
        if store.pack_paths.len() > limits.max_packs
            || store.by_batch.capacity() > limits.max_batch_slots
            || starts.capacity() > limits.max_log_start_slots
            || store.index.capacity() > limits.max_topic_slots
        {
            return Err(PinnedShardGcFailure::Capacity);
        }
        self.allocate(limits, checks)?;
        self.plan(store, starts, limits, checks)?;
        self.collect(store, pin, limits, checks)
    }
}

#[allow(
    clippy::result_large_err,
    reason = "retain all actual native plan/results/index effects without allocation during error cleanup"
)]
impl ShardStore {
    /// Collect actual sealed packs under finite work and the original deadline.
    ///
    /// Admit `pinned_shard_gc_cost` before entry. Retain this SAME store and the
    /// actual pinned directory/Core parent outside the call and every refusal.
    /// Native lock/directory/store identity is checked before mutation. The
    /// ordinary native retention predicate is shared; unknown pending names are
    /// never overwritten. Actual native outcomes/index removals precede every
    /// post-call checkpoint. No pathname is reopened and no clock is refreshed.
    ///
    /// On late/error/unwind the actual composing denial runs before cleanup.
    /// Parent sync already prepaid with an entered rename/unlink settles once,
    /// even after E. An entered sync is never retried. An uncertain native call
    /// or cleanup panic retains its unfinished phase; no result is fabricated.
    ///
    /// # Errors
    /// Returns all retained actual partial native/index/allocation/panic facts.
    /// A successful open, collection or report never supplies serving permission.
    pub fn garbage_collect_pinned_until(
        &mut self,
        pin: &PinnedShardDirectory,
        log_starts: &HashMap<TopicPartition, LogicalOffset>,
        limits: PinnedShardGcLimits,
        original_expiry: Instant,
        checks: &dyn PinnedShardGcChecks,
    ) -> Result<PinnedShardGcReport, PinnedShardGcError> {
        let mut scope = Scope::new(original_expiry, self.plan_epoch);
        let actual = catch_unwind(AssertUnwindSafe(|| {
            scope.run(self, pin, log_starts, limits, checks)
        }));
        let actual = match actual {
            Ok(result) => result,
            Err(payload) => {
                scope.panic_payload = Some(payload);
                Err(PinnedShardGcFailure::Unwind)
            }
        };
        match actual {
            Ok(()) => Ok(PinnedShardGcReport { scope }),
            Err(failure) => {
                scope.refused = true;
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| checks.deny())) {
                    scope.denial_panic_payload = Some(payload);
                }
                match catch_unwind(AssertUnwindSafe(|| scope.sync_pending(pin, false))) {
                    Ok(Ok(())) => {}
                    Ok(Err(actual)) => scope.settlement_failure = Some(actual),
                    Err(payload) => scope.settlement_panic_payload = Some(payload),
                }
                Err(PinnedShardGcError {
                    failure,
                    report: PinnedShardGcReport { scope },
                })
            }
        }
    }
}

#[cfg(test)]
#[path = "pinned_gc_tests.rs"]
mod tests;
