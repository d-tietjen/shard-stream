//! Bounded descriptor-relative native store discovery, scanning and repair.
//!
//! Linux/Android RawDir returns each getdents attempt without an internal EINTR
//! retry loop. The caller retains the real pinned directory/Core parent around
//! this ordinary storage producer; neither its output nor diagnostics grant
//! recovery completeness, currentness or serving permission.
//!
//! This additive producer requires byte-exact configured/retained directory
//! spelling and real no-follow, regular, single-linked pack/lease entries.
//! Native accepted frame/index/active-tail policies remain shared. Like the
//! ordinary native lease protocol, it assumes cooperating directory writers;
//! it does not grant future namespace stability against arbitrary writers.

use super::*;
use rustix::fs::{
    AtFlags, FileType, Mode, OFlags, RawDir, Stat, fdatasync, fstat, fsync, ftruncate, openat,
    statat, unlinkat,
};
use rustix::io::{fcntl_dupfd_cloexec, pread};
use std::collections::TryReserveError;
use std::ffi::{CString, NulError, OsStr, OsString};
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;

/// Scoped composing checks and denial used only during this real producer.
///
/// `deny` must latch the SAME actual composing counters/health before prepaid
/// durability settlement. It returns no grant and has no default. An original
/// panic from either callback remains retained; these callbacks are not stored
/// in the completed store or its physical directory owner.
pub trait PinnedStoreOpenChecks: ShardDirectoryChecks {
    /// Deny the actual composing operation before settling an entered repair.
    fn deny(&self);
}

/// Finite cumulative ceilings for one actual pinned native open/repair call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PinnedStoreOpenLimits {
    /// RawDir visits, including dot entries, errors and the final EOF.
    pub max_directory_visits: usize,
    /// Actual fixed caller-provided RawDir buffer bytes; never grown.
    pub directory_buffer_bytes: usize,
    /// Maximum accepted literal filename before role parsing/path construction.
    pub max_name_bytes: usize,
    /// Maximum complete diagnostic/configuration pathname bytes.
    pub max_path_bytes: usize,
    /// Discovered native pack files, including a created initial pack.
    pub max_packs: usize,
    /// Complete native frames decoded across all packs.
    pub max_frames: usize,
    /// Complete native frame bytes, including prefix/header/checksum.
    pub max_frame_bytes: usize,
    /// Individual native read attempts, including interrupted/short/EOF calls.
    pub max_read_attempts: usize,
    /// File request bytes plus RawDir buffer ceilings, including repetitions.
    pub max_read_bytes: usize,
    /// Cumulative native index slots across the batch and per-topic indexes.
    pub max_index_entries: usize,
    /// Prepaid truncate/unlink/parent-sync repair calls, including late slots.
    pub max_repair_calls: usize,
    /// Explicit native I/O phase slots, including directory refill attempts.
    pub max_io_calls: usize,
    /// Typed outcome slots reserved before their actual phase.
    pub max_outcomes: usize,
    /// Cumulative source-visible allocation/value slots.
    pub max_entries: usize,
    /// Cumulative source-visible allocation requests after fixed setup.
    pub max_declared_bytes: usize,
    /// Cumulative explicit byte/index/local-value work and entry fees.
    pub max_work_bytes: usize,
}

/// Plain setup and cumulative source-visible ceilings; not admission or RSS.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PinnedStoreOpenCost {
    /// Input/configuration, fixed queues/buffers, outcomes and dynamic ceiling.
    pub declared_bytes: usize,
    /// Native input/path/name subset already included in declared bytes.
    pub string_bytes: usize,
    /// Fixed values/outcomes/files plus the cumulative dynamic slot ceiling.
    pub entries: usize,
    /// Fixed setup and cumulative native work including entry fees.
    pub work_bytes: usize,
    /// Maximum simultaneously retained native file-result slots.
    pub retained_file_slots: usize,
}

/// Actual cumulative prepaid native fees; no phase resets these counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PinnedStoreOpenUsage {
    /// Prepaid visits, including dot entries, errors and genuine EOF.
    pub directory_visits: usize,
    /// Prepaid discovered/created pack slots, including duplicate refusals.
    pub packs: usize,
    /// Complete-frame slots prepaid before reading/materializing their bodies.
    pub frames: usize,
    /// Enterable individual file-read/getdents attempt slots, never recycled.
    pub read_attempts: usize,
    /// Prepaid file requests and RawDir buffer ceilings before internal trimming.
    /// RawDir does not expose its actual kernel refill length or byte count.
    pub requested_read_bytes: usize,
    /// Actual successful file-read bytes only; excludes unknown RawDir bytes.
    pub completed_read_bytes: usize,
    /// Prepaid batch/per-topic native index slots, including duplicate refusal.
    pub index_entries: usize,
    /// Mutation/sync slots reserved together before any mutation enters.
    pub repair_calls: usize,
    /// Cumulative reserved explicit native phase slots, including late cleanup.
    pub reserved_io_calls: usize,
    /// Native phase boundaries entered after their last actual checkpoint.
    pub entered_io_calls: usize,
    /// Reserved raw result rows, including not-yet-entered settlement slots.
    pub outcomes: usize,
    /// Cumulative dynamic source value/buffer/container slot fees.
    pub entries: usize,
    /// Cumulative requested dynamic source bytes, never peak allocator/RSS.
    pub declared_bytes: usize,
    /// Name/path/diagnostic subset already included in declared bytes.
    pub string_bytes: usize,
    /// Explicit requested byte/index/value work and cumulative entry fees.
    pub work_bytes: usize,
}

/// The actual open/repair phase associated with a retained native result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PinnedStoreOpenPhase {
    /// Exact native configuration validation, with its real Result retained.
    Configuration,
    /// Actual descriptor-relative open of the pinned directory itself.
    DirectoryOpened,
    /// Actual same-open-file-description native lease duplication.
    LeaseCloned,
    /// One native RawDir result, including a genuine error or EOF.
    DirectoryVisit,
    /// The true final native directory EOF was observed and checked.
    DirectoryEnumerated,
    /// Actual descriptor-relative no-follow pack open attempt.
    FileOpened,
    /// Actual retained descriptor metadata/stat attempt.
    DescriptorMetadata,
    /// Actual descriptor-relative no-follow named entry stat attempt.
    NamedMetadata,
    /// Actual native pending-role unlink attempt.
    PendingUnlinked,
    /// One prepaid direct directory fsync attempt, including late settlement.
    ParentSynced,
    /// Actual native initial pack CreateNew attempt.
    InitialPackCreated,
    /// Actual individual native prefix pread attempt.
    PrefixRead,
    /// Actual shared native prefix/length validation Result.
    FrameSized,
    /// Actual individual remaining-frame pread attempt.
    FrameRead,
    /// Actual complete native decoder Result, before mutable index insertion.
    FrameDecoded,
    /// Actual shared duplicate/index insertion Result.
    Indexed,
    /// Actual active-tail truncate attempt.
    TailTruncated,
    /// One prepaid direct data sync attempt, including late settlement.
    TailSynced,
    /// Actual native stable per-topic offset sort.
    IndexSorted,
    /// The same scanned active FD is selected and revalidated.
    ActiveSelected,
    /// Actual global native store-token fetch_update Result.
    StoreToken,
    /// Actual ordinary native bounded empty read-pool construction.
    BufferPool,
    /// The real ordinary store and all transferred inputs are fully retained.
    Assembled,
}

/// Terminal ordinary refusal; actual partial native values stay in its owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PinnedStoreOpenFailure {
    /// The original absolute operation E elapsed.
    Expired,
    /// A finite cumulative allowance or checked arithmetic refused.
    Capacity,
    /// SAME composing counters/earlier E refused.
    Checkpoint,
    /// Actual directory/name/descriptor/regular-single-link identity refused.
    Identity,
    /// An actual retained native I/O Result is Err.
    Io,
    /// The actual native configuration/frame/duplicate policy refused.
    Native,
    /// An actual fallible source-visible reservation failed.
    Allocation,
    /// Actual bounded native C-name preparation returned NulError.
    Name,
    /// An actual producer/checkpoint/completion callback unwound.
    Unwind,
    /// Private producer state disagreed with its actual prepaid inputs.
    Invariant,
}

/// A true consumed directory entry copied from the native RawDir result.
#[derive(Debug)]
pub struct PinnedStoreDirectoryEntry {
    name: OsString,
    inode: u64,
    kind: FileType,
    cookie: u64,
}
impl PinnedStoreDirectoryEntry {
    /// Original literal name bytes, without role normalization.
    pub fn name(&self) -> &OsStr {
        &self.name
    }
    /// Native directory entry inode number, not an ownership claim.
    pub fn inode(&self) -> u64 {
        self.inode
    }
    /// Native directory entry type; a later descriptor is validated separately.
    pub fn kind(&self) -> FileType {
        self.kind
    }
    /// Native iterator continuation cookie, without a completeness claim.
    pub fn cookie(&self) -> u64 {
        self.cookie
    }
}

/// Actual native outcomes saved before every post-phase E/checkpoint.
#[derive(Debug)]
pub enum PinnedStoreOpenOutcome {
    /// Actual unchanged native configuration validator Result.
    Configuration {
        /// Exact native acceptance or original validation error.
        result: StorageResult<()>,
    },
    /// The actual file Result is retained in the same private file slot.
    File {
        /// Exact actual open/duplicate phase.
        phase: PinnedStoreOpenPhase,
        /// Same private owning file-result slot; no descriptor is exposed.
        slot: usize,
    },
    /// Original native descriptor or descriptor-relative no-follow stat Result.
    Stat {
        /// Exact entered identity observation phase.
        phase: PinnedStoreOpenPhase,
        /// Original complete native stat or native error.
        result: io::Result<Stat>,
    },
    /// Original mutation or already-prepaid settlement Result.
    Unit {
        /// Exact entered unlink/truncate/sync phase.
        phase: PinnedStoreOpenPhase,
        /// Original native success or error, including EINTR without retry.
        result: io::Result<()>,
    },
    /// One true native directory iterator result, without hidden retries.
    Directory {
        /// A native refill boundary entered; its actual byte count is unknown.
        refill_entered: bool,
        /// Actual owned entry, actual EOF, or original getdents error.
        result: io::Result<Option<PinnedStoreDirectoryEntry>>,
    },
    /// RawDir consumed a real entry but its owned name reservation failed.
    /// The actual iterator buffer and allocation error remain retained.
    DirectoryEntryUncopied {
        /// Whether this native iterator visit entered a refill boundary.
        refill_entered: bool,
        /// Actual consumed entry's native inode observation.
        inode: u64,
        /// Actual consumed entry's native kind observation.
        kind: FileType,
        /// Actual native continuation cookie, without an EOF claim.
        cookie: u64,
        /// Actual borrowed name length whose reservation failed.
        name_bytes: usize,
    },
    /// One true native pread attempt, including short reads and errors.
    Read {
        /// Actual prefix or body-read phase.
        phase: PinnedStoreOpenPhase,
        /// Actual requested absolute file offset.
        offset: u64,
        /// Exact requested file-buffer length for this attempt.
        requested: usize,
        /// Exact native completed byte count or original error.
        result: io::Result<usize>,
    },
    /// Actual unchanged native prefix/length validation Result.
    FrameLength {
        /// Actual frame prefix's absolute file offset.
        offset: u64,
        /// Actual native declared frame size or original native refusal.
        result: StorageResult<usize>,
    },
    /// Complete actual location remains in the same current/index owner.
    Decode {
        /// Actual complete native frame offset.
        offset: u64,
        /// Success means the actual location is retained; errors stay original.
        result: StorageResult<()>,
    },
    /// Actual unchanged duplicate-overwrite/refusal and index-insertion Result.
    Indexed {
        /// Actual complete native frame offset.
        offset: u64,
        /// Original native helper acceptance or native duplicate error.
        result: StorageResult<()>,
    },
    /// An actual completed stable sort of the native per-topic locations.
    Sorted {
        /// Actual native topic/partition index key.
        topic_partition: TopicPartition,
        /// Actual number of native locations sorted in place.
        locations: usize,
    },
    /// Actual native global store-token update Result, without a serving grant.
    StoreToken {
        /// Actual previous token on success or exhausted value on refusal.
        result: Result<u64, u64>,
    },
}

#[derive(Debug)]
struct FileSlot {
    phase: PinnedStoreOpenPhase,
    // None only after this exact actual FD moved into the completed store.
    result: Option<io::Result<File>>,
}
#[derive(Default)]
struct Debit {
    visits: usize,
    packs: usize,
    frames: usize,
    read_attempts: usize,
    read_bytes: usize,
    index_entries: usize,
    repair_calls: usize,
    io_calls: usize,
    outcomes: usize,
    entries: usize,
    declared: usize,
    strings: usize,
    work: usize,
}
fn add(a: usize, b: usize) -> Result<usize, PinnedStoreOpenFailure> {
    a.checked_add(b).ok_or(PinnedStoreOpenFailure::Capacity)
}
fn mul(a: usize, b: usize) -> Result<usize, PinnedStoreOpenFailure> {
    a.checked_mul(b).ok_or(PinnedStoreOpenFailure::Capacity)
}
struct Work {
    limits: PinnedStoreOpenLimits,
    expiry: Instant,
    usage: PinnedStoreOpenUsage,
    checkpoint_failure: Option<ShardDirectoryCheckFailure>,
    unfinished_phase: Option<PinnedStoreOpenPhase>,
}
impl Work {
    fn check(&mut self, checks: &dyn ShardDirectoryChecks) -> Result<(), PinnedStoreOpenFailure> {
        if Instant::now() >= self.expiry {
            return Err(PinnedStoreOpenFailure::Expired);
        }
        let observed = checks.check();
        self.checkpoint_failure = observed.err();
        if Instant::now() >= self.expiry {
            return Err(PinnedStoreOpenFailure::Expired);
        }
        if self.checkpoint_failure.is_some() {
            return Err(PinnedStoreOpenFailure::Checkpoint);
        }
        Ok(())
    }
    fn charge(
        &mut self,
        debit: Debit,
        checks: &dyn ShardDirectoryChecks,
    ) -> Result<(), PinnedStoreOpenFailure> {
        self.check(checks)?;
        let old = self.usage;
        let next = PinnedStoreOpenUsage {
            directory_visits: add(old.directory_visits, debit.visits)?,
            packs: add(old.packs, debit.packs)?,
            frames: add(old.frames, debit.frames)?,
            read_attempts: add(old.read_attempts, debit.read_attempts)?,
            requested_read_bytes: add(old.requested_read_bytes, debit.read_bytes)?,
            completed_read_bytes: old.completed_read_bytes,
            index_entries: add(old.index_entries, debit.index_entries)?,
            repair_calls: add(old.repair_calls, debit.repair_calls)?,
            reserved_io_calls: add(old.reserved_io_calls, debit.io_calls)?,
            entered_io_calls: old.entered_io_calls,
            outcomes: add(old.outcomes, debit.outcomes)?,
            entries: add(old.entries, debit.entries)?,
            declared_bytes: add(old.declared_bytes, debit.declared)?,
            string_bytes: add(old.string_bytes, debit.strings)?,
            work_bytes: add(
                old.work_bytes,
                add(add(debit.work, debit.entries)?, debit.io_calls)?,
            )?,
        };
        let l = self.limits;
        if next.directory_visits > l.max_directory_visits
            || next.packs > l.max_packs
            || next.frames > l.max_frames
            || next.read_attempts > l.max_read_attempts
            || next.requested_read_bytes > l.max_read_bytes
            || next.index_entries > l.max_index_entries
            || next.repair_calls > l.max_repair_calls
            || next.reserved_io_calls > l.max_io_calls
            || next.outcomes > l.max_outcomes
            || next.entries > l.max_entries
            || next.declared_bytes > l.max_declared_bytes
            || next.work_bytes > l.max_work_bytes
            || next.string_bytes > next.declared_bytes
        {
            return Err(PinnedStoreOpenFailure::Capacity);
        }
        self.usage = next;
        self.check(checks)
    }
    fn enter(
        &mut self,
        checks: &dyn ShardDirectoryChecks,
        phase: PinnedStoreOpenPhase,
    ) -> Result<(), PinnedStoreOpenFailure> {
        self.check(checks)?;
        self.usage.entered_io_calls = add(self.usage.entered_io_calls, 1)?;
        if self.usage.entered_io_calls > self.usage.reserved_io_calls {
            return Err(PinnedStoreOpenFailure::Invariant);
        }
        self.unfinished_phase = Some(phase);
        Ok(())
    }
}

struct Scope {
    work: Work,
    outcomes: Vec<PinnedStoreOpenOutcome>,
    files: Vec<FileSlot>,
    directory_buffer: Vec<MaybeUninit<u8>>,
    // Transient owned names/paths remain outside the unwind closure as well.
    current_name: Option<OsString>,
    current_path: Option<PathBuf>,
    replaced_path: Option<PathBuf>,
    native_name: Option<CString>,
    name_error: Option<NulError>,
    frame: Vec<u8>,
    prefix: [u8; PREFIX_LEN],
    current_location: Option<ExtentLocation>,
    index: HashMap<TopicPartition, Vec<ExtentLocation>>,
    by_batch: HashMap<(TopicPartition, BatchId), ExtentLocation>,
    pack_paths: BTreeMap<u64, PathBuf>,
    // A pre-reserved Vec is consumed only by a true new native index entry.
    spare_locations: Option<Vec<ExtentLocation>>,
    directory_slot: Option<usize>,
    lease_slot: Option<usize>,
    active_slot: Option<usize>,
    active_bytes: u64,
    last_sequence: u64,
    pool: Option<ReadBufferPool>,
    assembly_lock: Option<File>,
    assembly_active: Option<File>,
    allocation_error: Option<TryReserveError>,
    native_error: Option<StorageError>,
    pending_tail_sync: Option<usize>,
    pending_parent_sync: bool,
    panic_payload: Option<Box<dyn Any + Send>>,
    deny_panic_payload: Option<Box<dyn Any + Send>>,
    settlement_panic_payload: Option<Box<dyn Any + Send>>,
    denial_completed: bool,
    terminal_failure: Option<PinnedStoreOpenFailure>,
    refused_phase: Option<PinnedStoreOpenPhase>,
    tail_settlement: Option<Result<(), PinnedStoreOpenFailure>>,
    parent_settlement: Option<Result<(), PinnedStoreOpenFailure>>,
    store: Option<ShardStore>,
    config: Option<ShardStoreConfig>,
}

fn prepare_name(
    work: &mut Work,
    native: &mut Option<CString>,
    failure: &mut Option<NulError>,
    name: &OsStr,
    checks: &dyn ShardDirectoryChecks,
) -> Result<(), PinnedStoreOpenFailure> {
    if name.len() > work.limits.max_name_bytes {
        return Err(PinnedStoreOpenFailure::Capacity);
    }
    let bytes = add(name.len(), 1)?;
    let values = add(bytes, add(size_of::<CString>(), size_of::<NulError>())?)?;
    work.charge(
        Debit {
            declared: values,
            strings: bytes,
            entries: 3,
            work: add(values, bytes)?,
            ..Debit::default()
        },
        checks,
    )?;
    // Save actual name conversion before its postcheck. Passing this CStr to
    // Rustix then has no hidden name allocation between the final E and syscall.
    match CString::new(name.as_bytes()) {
        Ok(value) => *native = Some(value),
        Err(error) => *failure = Some(error),
    }
    work.check(checks)?;
    if failure.is_some() {
        Err(PinnedStoreOpenFailure::Name)
    } else {
        Ok(())
    }
}

/// Plain actual setup/dynamic costs before calling the bounded open producer.
///
/// The same composing owner must prepay this finite logical domain. It includes
/// actual source-visible buffers, maps, file/result/outcome slots and both
/// three retained unwind slots. It excludes allocator/library/kernel/RSS overhead.
pub fn pinned_store_open_cost(
    config: &ShardStoreConfig,
    limits: PinnedStoreOpenLimits,
) -> Option<PinnedStoreOpenCost> {
    let all = [
        limits.max_directory_visits,
        limits.directory_buffer_bytes,
        limits.max_name_bytes,
        limits.max_path_bytes,
        limits.max_packs,
        limits.max_frames,
        limits.max_frame_bytes,
        limits.max_read_attempts,
        limits.max_read_bytes,
        limits.max_index_entries,
        limits.max_repair_calls,
        limits.max_io_calls,
        limits.max_outcomes,
        limits.max_entries,
        limits.max_declared_bytes,
        limits.max_work_bytes,
    ];
    if all.into_iter().any(|n| n == 0 || n > isize::MAX as usize)
        || config.directory.as_os_str().len() > limits.max_path_bytes
        || limits.max_name_bytes > limits.directory_buffer_bytes
        || limits.max_frame_bytes < recovery_frame_minimum()
    {
        return None;
    }
    let files = limits.max_packs.checked_add(4)?;
    let fixed = [
        size_of::<Scope>(),
        size_of::<OpenedPinnedShardStore>(),
        size_of::<PinnedStoreOpenError>(),
        size_of::<Work>(),
        size_of::<PinnedStoreOpenLimits>(),
        size_of::<PinnedStoreOpenCost>(),
        size_of::<ReadBufferPool>(),
        size_of::<RawDir<'static, &'static File>>(),
        size_of::<TryReserveError>(),
        size_of::<Result<(), PinnedStoreOpenFailure>>(),
        size_of::<Result<(), PinnedStoreOpenFailure>>(),
        size_of::<HashMap<TopicPartition, Vec<ExtentLocation>>>(),
        size_of::<HashMap<(TopicPartition, BatchId), ExtentLocation>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<std::sync::atomic::AtomicUsize>(),
        size_of::<[usize; 2]>(),
    ];
    let fixed_bytes = fixed.into_iter().try_fold(0usize, usize::checked_add)?;
    let outcome_bytes = limits
        .max_outcomes
        .checked_mul(size_of::<PinnedStoreOpenOutcome>())?;
    let file_bytes = files.checked_mul(size_of::<FileSlot>())?;
    // Actual empty native read pool allocates available slots and a bounded
    // recycled-message channel, both with the configured native slot count.
    let pool_entries = READ_BUFFER_POOL_SLOTS.checked_mul(2)?;
    let pool_bytes = pool_entries.checked_mul(size_of::<Vec<u8>>())?;
    let setup = fixed_bytes
        .checked_add(outcome_bytes)?
        .checked_add(file_bytes)?
        .checked_add(limits.directory_buffer_bytes)?
        .checked_add(pool_bytes)?;
    let entries = fixed
        .len()
        .checked_add(limits.max_outcomes)?
        .checked_add(files)?
        .checked_add(pool_entries)?
        .checked_add(1)?
        .checked_add(limits.max_entries)?;
    let declared_bytes = setup
        .checked_add(config.directory.as_os_str().len())?
        .checked_add(limits.max_declared_bytes)?;
    Some(PinnedStoreOpenCost {
        declared_bytes,
        string_bytes: config
            .directory
            .as_os_str()
            .len()
            .checked_add(limits.max_declared_bytes)?,
        entries,
        work_bytes: declared_bytes
            .checked_add(entries)?
            .checked_add(limits.max_work_bytes)?,
        retained_file_slots: files,
    })
}

impl Scope {
    fn new(config: ShardStoreConfig, limits: PinnedStoreOpenLimits, expiry: Instant) -> Self {
        Self {
            work: Work {
                limits,
                expiry,
                usage: PinnedStoreOpenUsage::default(),
                checkpoint_failure: None,
                unfinished_phase: None,
            },
            outcomes: Vec::new(),
            files: Vec::new(),
            directory_buffer: Vec::new(),
            current_name: None,
            current_path: None,
            replaced_path: None,
            native_name: None,
            name_error: None,
            frame: Vec::new(),
            prefix: [0; PREFIX_LEN],
            current_location: None,
            index: HashMap::new(),
            by_batch: HashMap::new(),
            pack_paths: BTreeMap::new(),
            spare_locations: None,
            directory_slot: None,
            lease_slot: None,
            active_slot: None,
            active_bytes: 0,
            last_sequence: 0,
            pool: None,
            assembly_lock: None,
            assembly_active: None,
            allocation_error: None,
            native_error: None,
            pending_tail_sync: None,
            pending_parent_sync: false,
            panic_payload: None,
            deny_panic_payload: None,
            settlement_panic_payload: None,
            denial_completed: false,
            terminal_failure: None,
            refused_phase: None,
            tail_settlement: None,
            parent_settlement: None,
            store: None,
            config: Some(config),
        }
    }
    fn setup(&mut self, checks: &dyn PinnedStoreOpenChecks) -> Result<(), PinnedStoreOpenFailure> {
        self.work.check(checks)?;
        self.allocation_error = self
            .outcomes
            .try_reserve_exact(self.work.limits.max_outcomes)
            .err();
        self.work.check(checks)?;
        if self.allocation_error.is_some() {
            return Err(PinnedStoreOpenFailure::Allocation);
        }
        let files = self
            .work
            .limits
            .max_packs
            .checked_add(4)
            .ok_or(PinnedStoreOpenFailure::Capacity)?;
        self.allocation_error = self.files.try_reserve_exact(files).err();
        self.work.check(checks)?;
        if self.allocation_error.is_some() {
            return Err(PinnedStoreOpenFailure::Allocation);
        }
        self.allocation_error = self
            .directory_buffer
            .try_reserve_exact(self.work.limits.directory_buffer_bytes)
            .err();
        self.work.check(checks)?;
        if self.allocation_error.is_some() {
            return Err(PinnedStoreOpenFailure::Allocation);
        }
        self.directory_buffer
            .resize_with(self.work.limits.directory_buffer_bytes, MaybeUninit::uninit);
        self.work.check(checks)
    }
    fn discover(
        &mut self,
        checks: &dyn PinnedStoreOpenChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<(), PinnedStoreOpenFailure> {
        let slot = self
            .directory_slot
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        // Split genuine owning fields: RawDir borrows the retained File and
        // buffer in this OUTSIDE-catch Scope, never a detached/local buffer.
        let Scope {
            work,
            outcomes,
            files,
            directory_buffer,
            allocation_error,
            ..
        } = self;
        let file = match files.get(slot).and_then(|s| s.result.as_ref()) {
            Some(Ok(file)) => file,
            _ => return Err(PinnedStoreOpenFailure::Invariant),
        };
        let mut iterator = RawDir::new(file, directory_buffer.as_mut_slice());
        loop {
            let refill = iterator.is_buffer_empty();
            let buffer_bytes = work.limits.directory_buffer_bytes;
            let values = add(buffer_bytes, size_of::<PinnedStoreDirectoryEntry>())?;
            work.charge(
                Debit {
                    visits: 1,
                    io_calls: usize::from(refill),
                    outcomes: 1,
                    read_attempts: usize::from(refill),
                    read_bytes: if refill { buffer_bytes } else { 0 },
                    declared: values,
                    strings: buffer_bytes,
                    entries: 2,
                    work: mul(buffer_bytes, 2)?,
                    ..Debit::default()
                },
                checks,
            )?;
            if refill {
                work.enter(checks, PinnedStoreOpenPhase::DirectoryVisit)?;
            } else {
                work.check(checks)?;
            }
            // RawDir performs at most one getdents attempt here and never
            // retries EINTR internally. Its returned entry/EOF/error is saved
            // before any post-call checkpoint. It does not expose the kernel
            // refill byte count; no invented completed-byte value is reported.
            let observed = iterator.next();
            let mut failure = None;
            let outcome = match observed {
                None => PinnedStoreOpenOutcome::Directory {
                    refill_entered: refill,
                    result: Ok(None),
                },
                Some(Err(error)) => {
                    let error = io::Error::from(error);
                    let interrupted = error.kind() == io::ErrorKind::Interrupted;
                    if !interrupted {
                        failure = Some(PinnedStoreOpenFailure::Io);
                    }
                    PinnedStoreOpenOutcome::Directory {
                        refill_entered: refill,
                        result: Err(error),
                    }
                }
                Some(Ok(entry)) => {
                    let bytes = entry.file_name().to_bytes();
                    let inode = entry.ino();
                    let kind = entry.file_type();
                    let cookie = entry.next_entry_cookie();
                    let mut name = OsString::new();
                    *allocation_error = name.try_reserve_exact(bytes.len()).err();
                    if allocation_error.is_some() {
                        failure = Some(PinnedStoreOpenFailure::Allocation);
                        PinnedStoreOpenOutcome::DirectoryEntryUncopied {
                            refill_entered: refill,
                            inode,
                            kind,
                            cookie,
                            name_bytes: bytes.len(),
                        }
                    } else {
                        name.push(OsStr::from_bytes(bytes));
                        if bytes.len() > work.limits.max_name_bytes {
                            failure = Some(PinnedStoreOpenFailure::Capacity);
                        }
                        PinnedStoreOpenOutcome::Directory {
                            refill_entered: refill,
                            result: Ok(Some(PinnedStoreDirectoryEntry {
                                name,
                                inode,
                                kind,
                                cookie,
                            })),
                        }
                    }
                }
            };
            let ended = matches!(
                &outcome,
                PinnedStoreOpenOutcome::Directory {
                    result: Ok(None),
                    ..
                }
            );
            outcomes.push(outcome);
            work.unfinished_phase = None;
            if outcomes.len() > work.usage.outcomes {
                return Err(PinnedStoreOpenFailure::Invariant);
            }
            completed(PinnedStoreOpenPhase::DirectoryVisit);
            work.check(checks)?;
            if let Some(failure) = failure {
                return Err(failure);
            }
            if ended {
                break;
            }
        }
        Ok(())
    }
    fn path_for_name(
        &mut self,
        checks: &dyn PinnedStoreOpenChecks,
    ) -> Result<(), PinnedStoreOpenFailure> {
        let name = self
            .current_name
            .as_deref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        let config = self
            .config
            .as_ref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        let bytes = add(add(config.directory.as_os_str().len(), 1)?, name.len())?;
        if bytes > self.work.limits.max_path_bytes {
            return Err(PinnedStoreOpenFailure::Capacity);
        }
        let values = add(bytes, size_of::<PathBuf>())?;
        self.work.charge(
            Debit {
                declared: values,
                strings: bytes,
                entries: 2,
                work: values,
                ..Debit::default()
            },
            checks,
        )?;
        self.current_path = Some(PathBuf::new());
        let path = self
            .current_path
            .as_mut()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        self.allocation_error = path.try_reserve_exact(bytes).err();
        #[cfg(test)]
        tests::materialized(tests::MaterializationPhase::PathReserved);
        self.work.check(checks)?;
        if self.allocation_error.is_some() {
            return Err(PinnedStoreOpenFailure::Allocation);
        }
        path.push(&config.directory);
        path.push(name);
        #[cfg(test)]
        tests::materialized(tests::MaterializationPhase::PathCopied);
        self.work.check(checks)
    }
    fn copy_directory_name(
        &mut self,
        entry: usize,
        checks: &dyn PinnedStoreOpenChecks,
    ) -> Result<(), PinnedStoreOpenFailure> {
        let name = match self.outcomes.get(entry) {
            Some(PinnedStoreOpenOutcome::Directory {
                result: Ok(Some(entry)),
                ..
            }) => entry.name(),
            _ => return Err(PinnedStoreOpenFailure::Invariant),
        };
        let bytes = name.len();
        let values = add(bytes, size_of::<OsString>())?;
        self.work.charge(
            Debit {
                declared: values,
                strings: bytes,
                entries: 2,
                work: values,
                ..Debit::default()
            },
            checks,
        )?;
        self.current_name = Some(OsString::new());
        let copy = self
            .current_name
            .as_mut()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        self.allocation_error = copy.try_reserve_exact(bytes).err();
        #[cfg(test)]
        tests::materialized(tests::MaterializationPhase::NameReserved);
        self.work.check(checks)?;
        if self.allocation_error.is_some() {
            return Err(PinnedStoreOpenFailure::Allocation);
        }
        copy.push(name);
        #[cfg(test)]
        tests::materialized(tests::MaterializationPhase::NameCopied);
        self.work.check(checks)
    }
    fn collect_pack_names(
        &mut self,
        checks: &dyn PinnedStoreOpenChecks,
    ) -> Result<(), PinnedStoreOpenFailure> {
        let visited = self.outcomes.len();
        for entry in 0..visited {
            let bytes = match &self.outcomes[entry] {
                PinnedStoreOpenOutcome::Directory {
                    result: Ok(Some(entry)),
                    ..
                } => entry.name().len(),
                _ => 0,
            };
            // This later role pass is separate from RawDir/copy fees: UTF-8,
            // prefix/suffix and native decimal parse scans plus one row fee.
            self.work.charge(
                Debit {
                    entries: 1,
                    work: add(mul(4, bytes)?, 1)?,
                    ..Debit::default()
                },
                checks,
            )?;
            let sequence = match &self.outcomes[entry] {
                PinnedStoreOpenOutcome::Directory {
                    result: Ok(Some(entry)),
                    ..
                } => entry.name().to_str().and_then(pack_sequence_from_name),
                _ => None,
            };
            let Some(sequence) = sequence else { continue };
            self.copy_directory_name(entry, checks)?;
            self.path_for_name(checks)?;
            let bytes = size_of::<(u64, PathBuf)>();
            let diagnostic = "duplicate pack sequence ".len() + u64::MAX.ilog10() as usize + 1;
            let values = add(
                add(bytes, diagnostic)?,
                add(size_of::<StorageError>(), size_of::<String>())?,
            )?;
            self.work.charge(
                Debit {
                    packs: 1,
                    declared: values,
                    strings: diagnostic,
                    entries: 4,
                    work: add(values, self.pack_paths.len())?,
                    ..Debit::default()
                },
                checks,
            )?;
            let path = self
                .current_path
                .take()
                .ok_or(PinnedStoreOpenFailure::Invariant)?;
            self.replaced_path = self.pack_paths.insert(sequence, path);
            let duplicate = self.replaced_path.is_some();
            if duplicate {
                self.native_error = Some(StorageError::InvalidInput(format!(
                    "duplicate pack sequence {sequence}"
                )));
            }
            #[cfg(test)]
            if duplicate {
                tests::materialized(tests::MaterializationPhase::ReplacedPath);
            }
            self.work.check(checks)?;
            if duplicate {
                return Err(PinnedStoreOpenFailure::Native);
            }
            self.current_name = None;
        }
        Ok(())
    }
    fn file(&self, slot: usize) -> Result<&File, PinnedStoreOpenFailure> {
        match self.files.get(slot).and_then(|slot| slot.result.as_ref()) {
            Some(Ok(file)) => Ok(file),
            Some(Err(_)) => Err(PinnedStoreOpenFailure::Io),
            None => Err(PinnedStoreOpenFailure::Invariant),
        }
    }
    fn after(
        &mut self,
        outcome: PinnedStoreOpenOutcome,
        phase: PinnedStoreOpenPhase,
        checks: &dyn ShardDirectoryChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<(), PinnedStoreOpenFailure> {
        self.outcomes.push(outcome);
        self.work.unfinished_phase = None;
        if self.outcomes.len() > self.work.usage.outcomes {
            return Err(PinnedStoreOpenFailure::Invariant);
        }
        completed(phase);
        self.work.check(checks)
    }
    fn open(
        &mut self,
        pin: &PinnedShardDirectory,
        name: &OsStr,
        flags: OFlags,
        phase: PinnedStoreOpenPhase,
        checks: &dyn ShardDirectoryChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<usize, PinnedStoreOpenFailure> {
        self.prepare_native_name(name, checks)?;
        self.open_prepared(pin, flags, phase, checks, completed)
    }
    fn open_prepared(
        &mut self,
        pin: &PinnedShardDirectory,
        flags: OFlags,
        phase: PinnedStoreOpenPhase,
        checks: &dyn ShardDirectoryChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<usize, PinnedStoreOpenFailure> {
        self.work.charge(
            Debit {
                io_calls: 1,
                outcomes: 1,
                ..Debit::default()
            },
            checks,
        )?;
        if self.files.len()
            >= self
                .work
                .limits
                .max_packs
                .checked_add(4)
                .ok_or(PinnedStoreOpenFailure::Capacity)?
        {
            return Err(PinnedStoreOpenFailure::Capacity);
        }
        self.work.enter(checks, phase)?;
        let name = self
            .native_name
            .as_deref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        let observed = openat(
            &pin.scope.directory,
            name,
            flags | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map(File::from)
        .map_err(io::Error::from);
        let slot = self.files.len();
        self.files.push(FileSlot {
            phase,
            result: Some(observed),
        });
        self.after(
            PinnedStoreOpenOutcome::File { phase, slot },
            phase,
            checks,
            completed,
        )?;
        self.file(slot)?;
        Ok(slot)
    }
    fn metadata(
        &mut self,
        slot: usize,
        checks: &dyn ShardDirectoryChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<u64, PinnedStoreOpenFailure> {
        self.work.charge(
            Debit {
                io_calls: 1,
                outcomes: 1,
                ..Debit::default()
            },
            checks,
        )?;
        self.work
            .enter(checks, PinnedStoreOpenPhase::DescriptorMetadata)?;
        let observed = fstat(self.file(slot)?).map_err(io::Error::from);
        let failed = observed.is_err();
        let length = observed
            .as_ref()
            .ok()
            .and_then(|stat| u64::try_from(stat.st_size).ok());
        self.after(
            PinnedStoreOpenOutcome::Stat {
                phase: PinnedStoreOpenPhase::DescriptorMetadata,
                result: observed,
            },
            PinnedStoreOpenPhase::DescriptorMetadata,
            checks,
            completed,
        )?;
        if failed {
            return Err(PinnedStoreOpenFailure::Io);
        }
        length.ok_or(PinnedStoreOpenFailure::Identity)
    }
    fn read(
        &mut self,
        slot: usize,
        offset: u64,
        start: usize,
        end: usize,
        prefix: bool,
        checks: &dyn ShardDirectoryChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<usize, PinnedStoreOpenFailure> {
        let requested = end
            .checked_sub(start)
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        self.work.charge(
            Debit {
                io_calls: 1,
                outcomes: 1,
                read_attempts: 1,
                read_bytes: requested,
                work: requested,
                ..Debit::default()
            },
            checks,
        )?;
        let phase = if prefix {
            PinnedStoreOpenPhase::PrefixRead
        } else {
            PinnedStoreOpenPhase::FrameRead
        };
        self.work.enter(checks, phase)?;
        let observed = match self.files.get(slot).and_then(|s| s.result.as_ref()) {
            Some(Ok(file)) => {
                if prefix {
                    pread(file, &mut self.prefix[start..end], offset).map_err(io::Error::from)
                } else {
                    pread(file, &mut self.frame[start..end], offset).map_err(io::Error::from)
                }
            }
            _ => return Err(PinnedStoreOpenFailure::Invariant),
        };
        let count = observed.as_ref().ok().copied();
        self.outcomes.push(PinnedStoreOpenOutcome::Read {
            phase,
            offset,
            requested,
            result: observed,
        });
        self.work.unfinished_phase = None;
        if self.outcomes.len() > self.work.usage.outcomes {
            return Err(PinnedStoreOpenFailure::Invariant);
        }
        if let Some(count) = count {
            self.work.usage.completed_read_bytes =
                add(self.work.usage.completed_read_bytes, count)?;
        }
        completed(phase);
        self.work.check(checks)?;
        count.ok_or(PinnedStoreOpenFailure::Io)
    }
    fn fill(
        &mut self,
        slot: usize,
        offset: u64,
        start: usize,
        end: usize,
        prefix: bool,
        checks: &dyn PinnedStoreOpenChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<(), PinnedStoreOpenFailure> {
        let mut filled = start;
        while filled < end {
            self.work.check(checks)?;
            let at = offset
                .checked_add((filled - start) as u64)
                .ok_or(PinnedStoreOpenFailure::Capacity)?;
            match self.read(slot, at, filled, end, prefix, checks, completed) {
                Ok(0) => {
                    let diagnostic = "failed to fill native recovery frame".len();
                    let values = add(
                        diagnostic,
                        add(size_of::<StorageError>(), size_of::<io::Error>())?,
                    )?;
                    self.work.charge(
                        Debit {
                            declared: values,
                            strings: diagnostic,
                            entries: 3,
                            work: values,
                            ..Debit::default()
                        },
                        checks,
                    )?;
                    self.native_error = Some(StorageError::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "failed to fill native recovery frame",
                    )));
                    return Err(PinnedStoreOpenFailure::Io);
                }
                Ok(count) => filled = add(filled, count)?,
                Err(PinnedStoreOpenFailure::Io) => {
                    let interrupted = matches!(self.outcomes.last(),Some(PinnedStoreOpenOutcome::Read {result:Err(error),..})
                        if error.kind()==io::ErrorKind::Interrupted);
                    if !interrupted {
                        return Err(PinnedStoreOpenFailure::Io);
                    }
                    // The real failed read remains retained. A fresh attempt
                    // spends another actual slot/request and checks SAME E.
                }
                Err(failure) => return Err(failure),
            }
        }
        self.work.check(checks)
    }
    fn decoder_fee(
        &mut self,
        frame_bytes: usize,
        checks: &dyn PinnedStoreOpenChecks,
    ) -> Result<(), PinnedStoreOpenFailure> {
        let config = self
            .config
            .as_ref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        let path = self
            .current_path
            .as_deref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        let l = self.work.limits;
        let limits = crate::RecoveryExtentInspectionLimits {
            max_batch_bytes: config.max_batch_bytes,
            max_frame_bytes: l.max_frame_bytes,
            max_path_bytes: l.max_path_bytes,
            max_declared_bytes: l.max_declared_bytes,
            max_work_bytes: l.max_work_bytes,
            max_entries: l.max_entries,
        };
        let measured =
            crate::recovery_extent_inspection_cost(frame_bytes, path.as_os_str().len(), limits);
        self.work.check(checks)?;
        let measured = measured.map_err(|_| PinnedStoreOpenFailure::Capacity)?;
        self.work.charge(
            Debit {
                declared: measured.declared_bytes(),
                strings: add(
                    mul(path.as_os_str().len(), 3)?,
                    "record range does not match record count".len(),
                )?,
                entries: measured.entries(),
                work: measured.work_bytes(),
                ..Debit::default()
            },
            checks,
        )
    }
    fn frame_length(
        &mut self,
        offset: u64,
        checks: &dyn PinnedStoreOpenChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<usize, PinnedStoreOpenFailure> {
        self.decoder_fee(PREFIX_LEN, checks)?;
        self.work.charge(
            Debit {
                outcomes: 1,
                ..Debit::default()
            },
            checks,
        )?;
        let batch_bytes = self
            .config
            .as_ref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?
            .max_batch_bytes;
        let path = self
            .current_path
            .as_deref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        let observed = crate::recovery_extent_frame_bytes(path, offset, &self.prefix, batch_bytes);
        let length = observed.as_ref().ok().copied();
        self.after(
            PinnedStoreOpenOutcome::FrameLength {
                offset,
                result: observed,
            },
            PinnedStoreOpenPhase::FrameSized,
            checks,
            completed,
        )?;
        let length = length.ok_or(PinnedStoreOpenFailure::Native)?;
        if length > self.work.limits.max_frame_bytes {
            return Err(PinnedStoreOpenFailure::Capacity);
        }
        Ok(length)
    }
    fn prepare_frame(
        &mut self,
        length: usize,
        checks: &dyn PinnedStoreOpenChecks,
    ) -> Result<(), PinnedStoreOpenFailure> {
        let values = add(length, size_of::<Vec<u8>>())?;
        self.work.charge(
            Debit {
                declared: values,
                entries: 2,
                work: values,
                ..Debit::default()
            },
            checks,
        )?;
        self.frame.clear();
        self.allocation_error = self.frame.try_reserve_exact(length).err();
        self.work.check(checks)?;
        if self.allocation_error.is_some() {
            return Err(PinnedStoreOpenFailure::Allocation);
        }
        self.frame.resize(length, 0);
        self.frame[..PREFIX_LEN].copy_from_slice(&self.prefix);
        self.work.check(checks)
    }
    fn decode(
        &mut self,
        offset: u64,
        checks: &dyn PinnedStoreOpenChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<(), PinnedStoreOpenFailure> {
        self.decoder_fee(self.frame.len(), checks)?;
        self.work.charge(
            Debit {
                outcomes: 1,
                ..Debit::default()
            },
            checks,
        )?;
        let shard = self
            .config
            .as_ref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?
            .shard_id;
        let path = self
            .current_path
            .as_deref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        let observed = decode_extent(path, offset, &self.frame, shard);
        let observed = match observed {
            Ok(location) => {
                self.current_location = Some(location);
                Ok(())
            }
            Err(error) => Err(error),
        };
        let failed = observed.is_err();
        self.after(
            PinnedStoreOpenOutcome::Decode {
                offset,
                result: observed,
            },
            PinnedStoreOpenPhase::FrameDecoded,
            checks,
            completed,
        )?;
        if failed {
            return Err(PinnedStoreOpenFailure::Native);
        }
        Ok(())
    }
    fn reserve_indexes(
        &mut self,
        checks: &dyn PinnedStoreOpenChecks,
    ) -> Result<(), PinnedStoreOpenFailure> {
        let location = self
            .current_location
            .as_ref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        let topic = TopicPartition::new(
            location.reservation.topic_id,
            location.reservation.partition_id,
        );
        let key = (topic, location.reservation.batch_id);
        self.work.charge(
            Debit {
                index_entries: 1,
                work: add(self.by_batch.capacity(), mul(2, self.index.capacity())?)?,
                ..Debit::default()
            },
            checks,
        )?;
        // A duplicate must take the exact existing native overwrite/refusal
        // path. Do not create a new per-topic Vec before that real predicate.
        if self.by_batch.contains_key(&key) {
            return Ok(());
        }
        let batch_slots = add(self.by_batch.len(), 1)?;
        let batch_bytes = mul(
            batch_slots,
            size_of::<((TopicPartition, BatchId), ExtentLocation)>(),
        )?;
        self.work.charge(
            Debit {
                declared: batch_bytes,
                entries: batch_slots,
                work: batch_bytes,
                ..Debit::default()
            },
            checks,
        )?;
        self.allocation_error = self.by_batch.try_reserve(1).err();
        self.work.check(checks)?;
        if self.allocation_error.is_some() {
            return Err(PinnedStoreOpenFailure::Allocation);
        }
        let len = self.index.get(&topic).map_or(0, Vec::len);
        let slots = add(len, 1)?;
        let bytes = mul(slots, size_of::<ExtentLocation>())?;
        self.work.charge(
            Debit {
                index_entries: 1,
                declared: bytes,
                entries: slots,
                work: bytes,
                ..Debit::default()
            },
            checks,
        )?;
        if let Some(locations) = self.index.get_mut(&topic) {
            self.allocation_error = locations.try_reserve_exact(1).err();
        } else {
            let topics = add(self.index.len(), 1)?;
            let topic_bytes = mul(topics, size_of::<(TopicPartition, Vec<ExtentLocation>)>())?;
            self.work.charge(
                Debit {
                    declared: topic_bytes,
                    entries: topics,
                    work: topic_bytes,
                    ..Debit::default()
                },
                checks,
            )?;
            self.allocation_error = self.index.try_reserve(1).err();
            self.work.check(checks)?;
            if self.allocation_error.is_some() {
                return Err(PinnedStoreOpenFailure::Allocation);
            }
            self.spare_locations = Some(Vec::new());
            let Some(spare) = self.spare_locations.as_mut() else {
                return Err(PinnedStoreOpenFailure::Invariant);
            };
            self.allocation_error = spare.try_reserve_exact(1).err();
        }
        self.work.check(checks)?;
        if self.allocation_error.is_some() {
            return Err(PinnedStoreOpenFailure::Allocation);
        }
        Ok(())
    }
    fn index_location(
        &mut self,
        offset: u64,
        sequence: u64,
        checks: &dyn PinnedStoreOpenChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<(), PinnedStoreOpenFailure> {
        self.reserve_indexes(checks)?;
        let path = self
            .current_path
            .as_deref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        let diagnostic = add(mul(path.as_os_str().len(), 3)?, "duplicate batch ID".len())?;
        let copies = mul(2, size_of::<ExtentLocation>())?;
        let values = add(
            add(diagnostic, copies)?,
            add(size_of::<StorageError>(), mul(2, size_of::<String>())?)?,
        )?;
        self.work.charge(
            Debit {
                outcomes: 1,
                declared: values,
                strings: diagnostic,
                entries: 8,
                work: add(
                    values,
                    add(self.by_batch.capacity(), self.index.capacity())?,
                )?,
                ..Debit::default()
            },
            checks,
        )?;
        let location = self
            .current_location
            .as_ref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?
            .clone();
        let observed = index_recovered_extent(
            path,
            offset,
            sequence,
            location,
            &mut self.index,
            &mut self.by_batch,
            &mut self.spare_locations,
        );
        let failed = observed.is_err();
        self.after(
            PinnedStoreOpenOutcome::Indexed {
                offset,
                result: observed,
            },
            PinnedStoreOpenPhase::Indexed,
            checks,
            completed,
        )?;
        if failed {
            return Err(PinnedStoreOpenFailure::Native);
        }
        self.current_location = None;
        Ok(())
    }
    fn validate_file(
        &mut self,
        pin: &PinnedShardDirectory,
        slot: usize,
        name: &OsStr,
        checks: &dyn ShardDirectoryChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<(), PinnedStoreOpenFailure> {
        self.prepare_native_name(name, checks)?;
        self.validate_prepared_file(pin, slot, checks, completed)
    }
    fn validate_prepared_file(
        &mut self,
        pin: &PinnedShardDirectory,
        slot: usize,
        checks: &dyn ShardDirectoryChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<(), PinnedStoreOpenFailure> {
        self.work.charge(
            Debit {
                io_calls: 1,
                outcomes: 1,
                ..Debit::default()
            },
            checks,
        )?;
        self.work
            .enter(checks, PinnedStoreOpenPhase::DescriptorMetadata)?;
        let observed = fstat(self.file(slot)?).map_err(io::Error::from);
        let actual = observed.as_ref().ok().map(|s| {
            (
                s.st_dev,
                s.st_ino,
                s.st_nlink,
                FileType::from_raw_mode(s.st_mode),
            )
        });
        self.after(
            PinnedStoreOpenOutcome::Stat {
                phase: PinnedStoreOpenPhase::DescriptorMetadata,
                result: observed,
            },
            PinnedStoreOpenPhase::DescriptorMetadata,
            checks,
            completed,
        )?;
        let actual = actual.ok_or(PinnedStoreOpenFailure::Io)?;
        self.work.charge(
            Debit {
                io_calls: 1,
                outcomes: 1,
                ..Debit::default()
            },
            checks,
        )?;
        self.work
            .enter(checks, PinnedStoreOpenPhase::NamedMetadata)?;
        let name = self
            .native_name
            .as_deref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        let observed =
            statat(&pin.scope.directory, name, AtFlags::SYMLINK_NOFOLLOW).map_err(io::Error::from);
        let named = observed.as_ref().ok().map(|s| {
            (
                s.st_dev,
                s.st_ino,
                s.st_nlink,
                FileType::from_raw_mode(s.st_mode),
            )
        });
        self.after(
            PinnedStoreOpenOutcome::Stat {
                phase: PinnedStoreOpenPhase::NamedMetadata,
                result: observed,
            },
            PinnedStoreOpenPhase::NamedMetadata,
            checks,
            completed,
        )?;
        let named = named.ok_or(PinnedStoreOpenFailure::Io)?;
        if actual != named || actual.2 != 1 || actual.3 != FileType::RegularFile {
            return Err(PinnedStoreOpenFailure::Identity);
        }
        Ok(())
    }
}

impl Scope {
    fn prepare_native_name(
        &mut self,
        name: &OsStr,
        checks: &dyn ShardDirectoryChecks,
    ) -> Result<(), PinnedStoreOpenFailure> {
        prepare_name(
            &mut self.work,
            &mut self.native_name,
            &mut self.name_error,
            name,
            checks,
        )
    }
    fn prepare_current_name(
        &mut self,
        checks: &dyn ShardDirectoryChecks,
    ) -> Result<(), PinnedStoreOpenFailure> {
        let name = self
            .current_name
            .as_deref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        prepare_name(
            &mut self.work,
            &mut self.native_name,
            &mut self.name_error,
            name,
            checks,
        )
    }
    fn name_from_current_path(
        &mut self,
        checks: &dyn PinnedStoreOpenChecks,
    ) -> Result<(), PinnedStoreOpenFailure> {
        // The earlier materialization paid for its copy, not this new
        // component scan. Cap retained bytes without parsing, then prepay
        // conservative pathname passes under the SAME cumulative E/work.
        let path_bytes = self
            .current_path
            .as_deref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?
            .as_os_str()
            .len();
        if path_bytes > self.work.limits.max_path_bytes {
            return Err(PinnedStoreOpenFailure::Capacity);
        }
        self.work.charge(
            Debit {
                work: mul(add(path_bytes, 1)?, 4)?,
                ..Debit::default()
            },
            checks,
        )?;
        #[cfg(test)]
        tests::materialized(tests::MaterializationPhase::CurrentPathScanPaid);
        self.work.check(checks)?;
        let name = self
            .current_path
            .as_deref()
            .and_then(Path::file_name)
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        let bytes = name.len();
        let values = add(bytes, size_of::<OsString>())?;
        self.work.charge(
            Debit {
                declared: values,
                strings: bytes,
                entries: 2,
                work: values,
                ..Debit::default()
            },
            checks,
        )?;
        self.current_name = Some(OsString::new());
        let copy = self
            .current_name
            .as_mut()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        self.allocation_error = copy.try_reserve_exact(bytes).err();
        self.work.check(checks)?;
        if self.allocation_error.is_some() {
            return Err(PinnedStoreOpenFailure::Allocation);
        }
        copy.push(name);
        self.work.check(checks)
    }
    fn clone_lease(
        &mut self,
        pin: &PinnedShardDirectory,
        checks: &dyn PinnedStoreOpenChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<(), PinnedStoreOpenFailure> {
        self.work.charge(
            Debit {
                io_calls: 1,
                outcomes: 1,
                ..Debit::default()
            },
            checks,
        )?;
        self.work.enter(checks, PinnedStoreOpenPhase::LeaseCloned)?;
        let observed = fcntl_dupfd_cloexec(&pin.scope.guard.guard().file, 0)
            .map(File::from)
            .map_err(io::Error::from);
        let slot = self.files.len();
        self.files.push(FileSlot {
            phase: PinnedStoreOpenPhase::LeaseCloned,
            result: Some(observed),
        });
        self.lease_slot = Some(slot);
        self.after(
            PinnedStoreOpenOutcome::File {
                phase: PinnedStoreOpenPhase::LeaseCloned,
                slot,
            },
            PinnedStoreOpenPhase::LeaseCloned,
            checks,
            completed,
        )?;
        self.file(slot)?;
        self.validate_file(
            pin,
            slot,
            OsStr::new(".shard-store.lock"),
            checks,
            completed,
        )
    }
    fn directory(
        &mut self,
        pin: &PinnedShardDirectory,
        checks: &dyn PinnedStoreOpenChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<(), PinnedStoreOpenFailure> {
        let slot = self.open(
            pin,
            OsStr::new("."),
            OFlags::RDONLY | OFlags::DIRECTORY,
            PinnedStoreOpenPhase::DirectoryOpened,
            checks,
            completed,
        )?;
        self.directory_slot = Some(slot);
        self.work.charge(
            Debit {
                io_calls: 1,
                outcomes: 1,
                ..Debit::default()
            },
            checks,
        )?;
        self.work
            .enter(checks, PinnedStoreOpenPhase::DescriptorMetadata)?;
        let observed = fstat(self.file(slot)?).map_err(io::Error::from);
        let actual = observed
            .as_ref()
            .ok()
            .map(|s| (s.st_dev, s.st_ino, FileType::from_raw_mode(s.st_mode)));
        self.after(
            PinnedStoreOpenOutcome::Stat {
                phase: PinnedStoreOpenPhase::DescriptorMetadata,
                result: observed,
            },
            PinnedStoreOpenPhase::DescriptorMetadata,
            checks,
            completed,
        )?;
        let expected = pin
            .observation()
            .directory_metadata()
            .and_then(|r| r.as_ref().ok())
            .map(|s| (s.dev(), s.ino(), FileType::Directory));
        if actual.is_none() {
            return Err(PinnedStoreOpenFailure::Io);
        }
        if actual != expected {
            return Err(PinnedStoreOpenFailure::Identity);
        }
        Ok(())
    }
    fn reserve_repair_pair(
        &mut self,
        checks: &dyn PinnedStoreOpenChecks,
    ) -> Result<(), PinnedStoreOpenFailure> {
        if self.pending_tail_sync.is_some() || self.pending_parent_sync {
            return Err(PinnedStoreOpenFailure::Invariant);
        }
        let result_bytes = size_of::<Result<(), PinnedStoreOpenFailure>>();
        self.work.charge(
            Debit {
                repair_calls: 2,
                io_calls: 2,
                outcomes: 2,
                entries: 1,
                declared: result_bytes,
                work: result_bytes,
                ..Debit::default()
            },
            checks,
        )
    }
    // Settlement enters only an already-reserved single native slot. There is
    // no new admission, callback, retry, pathname open or replacement E here.
    fn settle_tail(&mut self) -> Result<(), PinnedStoreOpenFailure> {
        let Some(slot) = self.pending_tail_sync.take() else {
            return Ok(());
        };
        self.work.usage.entered_io_calls = add(self.work.usage.entered_io_calls, 1)?;
        self.work.unfinished_phase = Some(PinnedStoreOpenPhase::TailSynced);
        let observed = fdatasync(self.file(slot)?).map_err(io::Error::from);
        let failed = observed.is_err();
        self.outcomes.push(PinnedStoreOpenOutcome::Unit {
            phase: PinnedStoreOpenPhase::TailSynced,
            result: observed,
        });
        self.work.unfinished_phase = None;
        if failed {
            Err(PinnedStoreOpenFailure::Io)
        } else {
            Ok(())
        }
    }
    fn settle_parent(&mut self, pin: &PinnedShardDirectory) -> Result<(), PinnedStoreOpenFailure> {
        if !self.pending_parent_sync {
            return Ok(());
        }
        self.pending_parent_sync = false;
        self.work.usage.entered_io_calls = add(self.work.usage.entered_io_calls, 1)?;
        self.work.unfinished_phase = Some(PinnedStoreOpenPhase::ParentSynced);
        let observed = fsync(&pin.scope.directory).map_err(io::Error::from);
        let failed = observed.is_err();
        self.outcomes.push(PinnedStoreOpenOutcome::Unit {
            phase: PinnedStoreOpenPhase::ParentSynced,
            result: observed,
        });
        self.work.unfinished_phase = None;
        if failed {
            Err(PinnedStoreOpenFailure::Io)
        } else {
            Ok(())
        }
    }
    fn repair_tail(
        &mut self,
        slot: usize,
        offset: u64,
        checks: &dyn PinnedStoreOpenChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<(), PinnedStoreOpenFailure> {
        self.reserve_repair_pair(checks)?;
        self.work
            .enter(checks, PinnedStoreOpenPhase::TailTruncated)?;
        // An entered truncate may have changed bytes even if it unwinds or
        // returns an error. Its already-paid data sync must still settle once.
        self.pending_tail_sync = Some(slot);
        let observed = ftruncate(self.file(slot)?, offset).map_err(io::Error::from);
        let failed = observed.is_err();
        self.after(
            PinnedStoreOpenOutcome::Unit {
                phase: PinnedStoreOpenPhase::TailTruncated,
                result: observed,
            },
            PinnedStoreOpenPhase::TailTruncated,
            checks,
            completed,
        )?;
        if failed {
            return Err(PinnedStoreOpenFailure::Io);
        }
        self.tail_settlement = Some(self.settle_tail());
        completed(PinnedStoreOpenPhase::TailSynced);
        self.work.check(checks)?;
        self.tail_settlement
            .ok_or(PinnedStoreOpenFailure::Invariant)?
    }
    fn cleanup_pending(
        &mut self,
        pin: &PinnedShardDirectory,
        checks: &dyn PinnedStoreOpenChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<(), PinnedStoreOpenFailure> {
        let entries = self.outcomes.len();
        for entry in 0..entries {
            let bytes = match &self.outcomes[entry] {
                PinnedStoreOpenOutcome::Directory {
                    result: Ok(Some(entry)),
                    ..
                } => entry.name().len(),
                _ => 0,
            };
            // Independently prepay this actual saved-name UTF-8/role scan;
            // discovery's visit/copy fees are not reused as another allowance.
            self.work.charge(
                Debit {
                    entries: 1,
                    work: add(mul(4, bytes)?, 1)?,
                    ..Debit::default()
                },
                checks,
            )?;
            let pending = matches!(&self.outcomes[entry],PinnedStoreOpenOutcome::Directory {result:Ok(Some(entry)),..}
                if entry.name().to_str().is_some_and(is_pending_pack_deletion_name));
            if !pending {
                continue;
            }
            self.copy_directory_name(entry, checks)?;
            self.prepare_current_name(checks)?;
            // Keep the native cleanup role, but require a real regular,
            // single-linked entry before descriptor-relative removal.
            self.work.charge(
                Debit {
                    io_calls: 1,
                    outcomes: 1,
                    ..Debit::default()
                },
                checks,
            )?;
            self.work
                .enter(checks, PinnedStoreOpenPhase::NamedMetadata)?;
            let name = self
                .native_name
                .as_deref()
                .ok_or(PinnedStoreOpenFailure::Invariant)?;
            let observed = statat(&pin.scope.directory, name, AtFlags::SYMLINK_NOFOLLOW)
                .map_err(io::Error::from);
            let acceptable = observed.as_ref().ok().map(|s| {
                s.st_nlink == 1 && FileType::from_raw_mode(s.st_mode) == FileType::RegularFile
            });
            self.after(
                PinnedStoreOpenOutcome::Stat {
                    phase: PinnedStoreOpenPhase::NamedMetadata,
                    result: observed,
                },
                PinnedStoreOpenPhase::NamedMetadata,
                checks,
                completed,
            )?;
            match acceptable {
                None => return Err(PinnedStoreOpenFailure::Io),
                Some(false) => return Err(PinnedStoreOpenFailure::Identity),
                Some(true) => {}
            }
            self.reserve_repair_pair(checks)?;
            self.work
                .enter(checks, PinnedStoreOpenPhase::PendingUnlinked)?;
            self.pending_parent_sync = true;
            let name = self
                .native_name
                .as_deref()
                .ok_or(PinnedStoreOpenFailure::Invariant)?;
            let observed =
                unlinkat(&pin.scope.directory, name, AtFlags::empty()).map_err(io::Error::from);
            let failed = observed.is_err();
            self.after(
                PinnedStoreOpenOutcome::Unit {
                    phase: PinnedStoreOpenPhase::PendingUnlinked,
                    result: observed,
                },
                PinnedStoreOpenPhase::PendingUnlinked,
                checks,
                completed,
            )?;
            if failed {
                return Err(PinnedStoreOpenFailure::Io);
            }
            self.parent_settlement = Some(self.settle_parent(pin));
            completed(PinnedStoreOpenPhase::ParentSynced);
            self.work.check(checks)?;
            self.parent_settlement
                .ok_or(PinnedStoreOpenFailure::Invariant)??;
            self.current_name = None;
        }
        Ok(())
    }
    fn create_initial(
        &mut self,
        pin: &PinnedShardDirectory,
        checks: &dyn PinnedStoreOpenChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<(), PinnedStoreOpenFailure> {
        // Native pack_path uses this exact width/spelling. No wire parser is
        // introduced; the complete literal is bounded before allocation/open.
        const INITIAL: &str = "pack-00000000000000000000.sse";
        let values = add(INITIAL.len(), size_of::<OsString>())?;
        self.work.charge(
            Debit {
                packs: 1,
                declared: values,
                strings: INITIAL.len(),
                entries: 2,
                work: values,
                ..Debit::default()
            },
            checks,
        )?;
        self.current_name = Some(OsString::from(INITIAL));
        self.path_for_name(checks)?;
        self.prepare_native_name(OsStr::new(INITIAL), checks)?;
        self.reserve_repair_pair(checks)?;
        self.work
            .enter(checks, PinnedStoreOpenPhase::InitialPackCreated)?;
        self.pending_parent_sync = true;
        let name = self
            .native_name
            .as_deref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        let observed = openat(
            &pin.scope.directory,
            name,
            OFlags::CREATE | OFlags::EXCL | OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map(File::from)
        .map_err(io::Error::from);
        let slot = self.files.len();
        self.files.push(FileSlot {
            phase: PinnedStoreOpenPhase::InitialPackCreated,
            result: Some(observed),
        });
        self.after(
            PinnedStoreOpenOutcome::File {
                phase: PinnedStoreOpenPhase::InitialPackCreated,
                slot,
            },
            PinnedStoreOpenPhase::InitialPackCreated,
            checks,
            completed,
        )?;
        self.file(slot)?;
        self.parent_settlement = Some(self.settle_parent(pin));
        completed(PinnedStoreOpenPhase::ParentSynced);
        self.work.check(checks)?;
        self.parent_settlement
            .ok_or(PinnedStoreOpenFailure::Invariant)??;
        self.validate_file(pin, slot, OsStr::new(INITIAL), checks, completed)?;
        let bytes = size_of::<(u64, PathBuf)>();
        self.work.charge(
            Debit {
                declared: bytes,
                entries: 1,
                work: bytes,
                ..Debit::default()
            },
            checks,
        )?;
        let path = self
            .current_path
            .take()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        self.pack_paths.insert(0, path);
        self.work.check(checks)
    }
    fn corrupt(
        &mut self,
        offset: u64,
        reason: &'static str,
        checks: &dyn PinnedStoreOpenChecks,
    ) -> Result<(), PinnedStoreOpenFailure> {
        let path = self
            .current_path
            .as_deref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        // Native corrupt() retains a display-formatted String. Invalid UTF-8
        // can expand each original pathname byte to three UTF-8 bytes.
        let bytes = add(mul(path.as_os_str().len(), 3)?, reason.len())?;
        let values = add(
            bytes,
            add(size_of::<StorageError>(), mul(2, size_of::<String>())?)?,
        )?;
        self.work.charge(
            Debit {
                declared: values,
                strings: bytes,
                entries: 5,
                work: values,
                ..Debit::default()
            },
            checks,
        )?;
        self.native_error = Some(StorageError::corrupt(path, offset, reason));
        self.work.check(checks)?;
        Err(PinnedStoreOpenFailure::Native)
    }
    fn scan(
        &mut self,
        pin: &PinnedShardDirectory,
        sequence: u64,
        checks: &dyn PinnedStoreOpenChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<(), PinnedStoreOpenFailure> {
        self.name_from_current_path(checks)?;
        self.prepare_current_name(checks)?;
        let slot = self.open_prepared(
            pin,
            OFlags::RDWR,
            PinnedStoreOpenPhase::FileOpened,
            checks,
            completed,
        )?;
        if sequence == self.last_sequence {
            self.active_slot = Some(slot);
        }
        self.prepare_current_name(checks)?;
        self.validate_prepared_file(pin, slot, checks, completed)?;
        let file_len = self.metadata(slot, checks, completed)?;
        let mut offset = 0u64;
        while offset < file_len {
            self.work.check(checks)?;
            if file_len - offset < PREFIX_LEN as u64 {
                if sequence == self.last_sequence {
                    return self.repair_tail(slot, offset, checks, completed);
                }
                return self.corrupt(offset, "truncated prefix in sealed pack", checks);
            }
            self.fill(slot, offset, 0, PREFIX_LEN, true, checks, completed)?;
            let length = self.frame_length(offset, checks, completed)?;
            let end = offset.checked_add(length as u64);
            if end.is_none_or(|end| end > file_len) {
                if sequence == self.last_sequence {
                    return self.repair_tail(slot, offset, checks, completed);
                }
                return self.corrupt(offset, "truncated frame in sealed pack", checks);
            }
            self.work.charge(
                Debit {
                    frames: 1,
                    ..Debit::default()
                },
                checks,
            )?;
            self.prepare_frame(length, checks)?;
            let body_offset = offset
                .checked_add(PREFIX_LEN as u64)
                .ok_or(PinnedStoreOpenFailure::Capacity)?;
            self.fill(
                slot,
                body_offset,
                PREFIX_LEN,
                length,
                false,
                checks,
                completed,
            )?;
            self.decode(offset, checks, completed)?;
            self.index_location(offset, sequence, checks, completed)?;
            offset = end.ok_or(PinnedStoreOpenFailure::Invariant)?;
        }
        self.work.check(checks)
    }
    fn copy_pack_path(
        &mut self,
        sequence: u64,
        checks: &dyn PinnedStoreOpenChecks,
    ) -> Result<(), PinnedStoreOpenFailure> {
        let path = self
            .pack_paths
            .get(&sequence)
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        let bytes = path.as_os_str().len();
        let values = add(bytes, size_of::<PathBuf>())?;
        self.work.charge(
            Debit {
                declared: values,
                strings: bytes,
                entries: 2,
                work: add(values, self.pack_paths.len())?,
                ..Debit::default()
            },
            checks,
        )?;
        self.current_path = Some(PathBuf::new());
        let owned = self
            .current_path
            .as_mut()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        self.allocation_error = owned.try_reserve_exact(bytes).err();
        #[cfg(test)]
        tests::materialized(tests::MaterializationPhase::PackPathReserved);
        self.work.check(checks)?;
        if self.allocation_error.is_some() {
            return Err(PinnedStoreOpenFailure::Allocation);
        }
        owned.push(path);
        #[cfg(test)]
        tests::materialized(tests::MaterializationPhase::PackPathCopied);
        self.work.check(checks)
    }
    fn sort(
        &mut self,
        checks: &dyn PinnedStoreOpenChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<(), PinnedStoreOpenFailure> {
        // HashMap iteration cost is its actual capacity, paid before visiting
        // buckets. Each genuine stable sort's scratch and n*log2(n) movement
        // ceiling are independently paid before that native operation.
        self.work.charge(
            Debit {
                work: self.index.capacity(),
                ..Debit::default()
            },
            checks,
        )?;
        for (topic, locations) in &mut self.index {
            let n = locations.len();
            let bytes = mul(n, size_of::<ExtentLocation>())?;
            let levels = if n < 2 {
                0
            } else {
                usize::BITS as usize - (n - 1).leading_zeros() as usize
            };
            self.work.charge(
                Debit {
                    outcomes: 1,
                    declared: bytes,
                    entries: n,
                    work: mul(bytes, add(levels, 1)?)?,
                    ..Debit::default()
                },
                checks,
            )?;
            locations.sort_by_key(|location| location.reservation.first_offset);
            self.outcomes.push(PinnedStoreOpenOutcome::Sorted {
                topic_partition: *topic,
                locations: n,
            });
            completed(PinnedStoreOpenPhase::IndexSorted);
            self.work.check(checks)?;
        }
        Ok(())
    }
    fn assemble(
        &mut self,
        pin: &PinnedShardDirectory,
        checks: &dyn PinnedStoreOpenChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<(), PinnedStoreOpenFailure> {
        self.copy_pack_path(self.last_sequence, checks)?;
        self.name_from_current_path(checks)?;
        // Reuse the actual active descriptor that was scanned/repaired. A
        // second pathname open must not silently select a replacement inode
        // while the rebuilt native indexes still describe the first file.
        let active = self.active_slot.ok_or(PinnedStoreOpenFailure::Invariant)?;
        self.prepare_current_name(checks)?;
        self.validate_prepared_file(pin, active, checks, completed)?;
        self.active_bytes = self.metadata(active, checks, completed)?;
        completed(PinnedStoreOpenPhase::ActiveSelected);
        self.work.check(checks)?;
        self.work.charge(
            Debit {
                outcomes: 1,
                ..Debit::default()
            },
            checks,
        )?;
        let token = NEXT_STORE_TOKEN.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |token| {
            token.checked_add(1)
        });
        let value = token.as_ref().ok().copied();
        self.after(
            PinnedStoreOpenOutcome::StoreToken { result: token },
            PinnedStoreOpenPhase::StoreToken,
            checks,
            completed,
        )?;
        let token = value.ok_or(PinnedStoreOpenFailure::Capacity)?;
        self.work.check(checks)?;
        self.pool = Some(ReadBufferPool::new());
        completed(PinnedStoreOpenPhase::BufferPool);
        self.work.check(checks)?;
        let opened_at = Instant::now();
        let lease = self.lease_slot.ok_or(PinnedStoreOpenFailure::Invariant)?;
        let slot = self
            .files
            .get_mut(lease)
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        match slot.result.take() {
            Some(Ok(file)) => self.assembly_lock = Some(file),
            Some(Err(error)) => {
                slot.result = Some(Err(error));
                return Err(PinnedStoreOpenFailure::Io);
            }
            None => return Err(PinnedStoreOpenFailure::Invariant),
        }
        let slot = self
            .files
            .get_mut(active)
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        match slot.result.take() {
            Some(Ok(file)) => self.assembly_active = Some(file),
            Some(Err(error)) => {
                slot.result = Some(Err(error));
                return Err(PinnedStoreOpenFailure::Io);
            }
            None => return Err(PinnedStoreOpenFailure::Invariant),
        }
        self.work.check(checks)?;
        let index_empty = HashMap::with_hasher(self.index.hasher().clone());
        let batches_empty = HashMap::with_hasher(self.by_batch.hasher().clone());
        self.store = Some(ShardStore {
            config: self
                .config
                .take()
                .ok_or(PinnedStoreOpenFailure::Invariant)?,
            _lock_file: self
                .assembly_lock
                .take()
                .ok_or(PinnedStoreOpenFailure::Invariant)?,
            store_token: token,
            plan_epoch: 0,
            active: ActivePack {
                sequence: self.last_sequence,
                file: self
                    .assembly_active
                    .take()
                    .ok_or(PinnedStoreOpenFailure::Invariant)?,
                bytes: self.active_bytes,
                opened_at,
            },
            index: std::mem::replace(&mut self.index, index_empty),
            by_batch: std::mem::replace(&mut self.by_batch, batches_empty),
            pack_paths: std::mem::take(&mut self.pack_paths),
            read_buffer_pool: self.pool.take().ok_or(PinnedStoreOpenFailure::Invariant)?,
        });
        completed(PinnedStoreOpenPhase::Assembled);
        self.work.check(checks)
    }
    fn run(
        &mut self,
        pin: &PinnedShardDirectory,
        checks: &dyn PinnedStoreOpenChecks,
        completed: &mut impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<(), PinnedStoreOpenFailure> {
        self.setup(checks)?;
        let config = self
            .config
            .as_ref()
            .ok_or(PinnedStoreOpenFailure::Invariant)?;
        if config.directory.as_os_str() != pin.directory().as_os_str() {
            return Err(PinnedStoreOpenFailure::Identity);
        }
        let diagnostic = "target_pack_bytes must be nonzero"
            .len()
            .max("max_batch_bytes must be nonzero".len());
        let values = add(
            diagnostic,
            add(size_of::<StorageError>(), size_of::<String>())?,
        )?;
        self.work.charge(
            Debit {
                outcomes: 1,
                declared: values,
                strings: diagnostic,
                entries: 3,
                work: values,
                ..Debit::default()
            },
            checks,
        )?;
        let observed = config.validate();
        let failed = observed.is_err();
        self.after(
            PinnedStoreOpenOutcome::Configuration { result: observed },
            PinnedStoreOpenPhase::Configuration,
            checks,
            completed,
        )?;
        if failed {
            return Err(PinnedStoreOpenFailure::Native);
        }
        self.clone_lease(pin, checks, completed)?;
        self.directory(pin, checks, completed)?;
        self.discover(checks, completed)?;
        completed(PinnedStoreOpenPhase::DirectoryEnumerated);
        self.work.check(checks)?;
        self.cleanup_pending(pin, checks, completed)?;
        self.collect_pack_names(checks)?;
        if self.pack_paths.is_empty() {
            self.create_initial(pin, checks, completed)?;
        }
        self.last_sequence = *self
            .pack_paths
            .last_key_value()
            .ok_or(PinnedStoreOpenFailure::Invariant)?
            .0;
        let mut next = self
            .pack_paths
            .first_key_value()
            .map(|(sequence, _)| *sequence);
        while let Some(sequence) = next {
            self.copy_pack_path(sequence, checks)?;
            self.scan(pin, sequence, checks, completed)?;
            next = self
                .pack_paths
                .range((
                    std::ops::Bound::Excluded(sequence),
                    std::ops::Bound::Unbounded,
                ))
                .next()
                .map(|(sequence, _)| *sequence);
            self.work.check(checks)?;
        }
        self.sort(checks, completed)?;
        self.assemble(pin, checks, completed)
    }
    fn refuse(
        &mut self,
        pin: &PinnedShardDirectory,
        failure: PinnedStoreOpenFailure,
        checks: &dyn PinnedStoreOpenChecks,
    ) {
        self.terminal_failure = Some(failure);
        self.refused_phase = self.work.unfinished_phase;
        let denied = catch_unwind(AssertUnwindSafe(|| checks.deny()));
        match denied {
            Ok(()) => self.denial_completed = true,
            Err(payload) => self.deny_panic_payload = Some(payload),
        }
        let settled = catch_unwind(AssertUnwindSafe(|| {
            // Both possible effects already own two slots before entry. A sync
            // attempt is consumed before calling the native syscall, so no
            // second attempt is made after an unwind or EINTR result.
            // Keep the original terminal refusal. Additional true settlement
            // failures remain distinguishable in their raw Unit outcomes.
            if self.pending_tail_sync.is_some() {
                self.tail_settlement = Some(self.settle_tail());
            }
            if self.pending_parent_sync {
                self.parent_settlement = Some(self.settle_parent(pin));
            }
        }));
        if let Err(payload) = settled {
            self.settlement_panic_payload = Some(payload);
        }
    }
}

/// Completed actual native open result. Ordinary later store methods are not
/// thereby qualified as descriptor-relative or bounded operations.
pub struct OpenedPinnedShardStore {
    store: ShardStore,
    scope: Scope,
}
impl fmt::Debug for OpenedPinnedShardStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenedPinnedShardStore")
            .field("usage", &self.scope.work.usage)
            .field("outcomes", &self.scope.outcomes)
            .finish_non_exhaustive()
    }
}
impl OpenedPinnedShardStore {
    /// Borrow the actual opened ordinary store; retain SAME pin/Core parent.
    pub fn store(&self) -> &ShardStore {
        &self.store
    }
    /// Mutably borrow that SAME store for a separately admitted native producer.
    pub fn store_mut(&mut self) -> &mut ShardStore {
        &mut self.store
    }
    /// Actual cumulative prepaid work, including refused slots.
    pub fn usage(&self) -> PinnedStoreOpenUsage {
        self.scope.work.usage
    }
    /// Original absolute open/repair deadline, never an operational clock.
    pub fn absolute_expiry(&self) -> Instant {
        self.scope.work.expiry
    }
    /// Actual native phase results retained before their postchecks.
    pub fn outcomes(&self) -> &[PinnedStoreOpenOutcome] {
        &self.scope.outcomes
    }
}
/// Refusal retains every actual file/index/repair result and unwind payload.
/// The composing owner must continue retaining SAME pin/Core parent LAST.
pub struct PinnedStoreOpenError {
    scope: Scope,
}
impl fmt::Debug for PinnedStoreOpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedStoreOpenError")
            .field("failure", &self.failure())
            .field("usage", &self.scope.work.usage)
            .field("outcomes", &self.scope.outcomes)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for PinnedStoreOpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "native pinned store open refused: {:?}", self.failure())
    }
}
impl Error for PinnedStoreOpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(error) = &self.scope.native_error {
            return Some(error);
        }
        for file in &self.scope.files {
            if let Some(Err(error)) = &file.result {
                return Some(error);
            }
        }
        for outcome in &self.scope.outcomes {
            match outcome {
                PinnedStoreOpenOutcome::Stat {
                    result: Err(error), ..
                } => return Some(error),
                PinnedStoreOpenOutcome::Unit {
                    result: Err(error), ..
                } => return Some(error),
                PinnedStoreOpenOutcome::Directory {
                    result: Err(error), ..
                } => return Some(error),
                PinnedStoreOpenOutcome::Read {
                    result: Err(error), ..
                } => return Some(error),
                PinnedStoreOpenOutcome::FrameLength {
                    result: Err(error), ..
                }
                | PinnedStoreOpenOutcome::Decode {
                    result: Err(error), ..
                }
                | PinnedStoreOpenOutcome::Indexed {
                    result: Err(error), ..
                }
                | PinnedStoreOpenOutcome::Configuration { result: Err(error) } => {
                    return Some(error);
                }
                _ => {}
            }
        }
        self.scope
            .allocation_error
            .as_ref()
            .map(|error| error as &dyn Error)
    }
}
impl PinnedStoreOpenError {
    /// Local terminal refusal; raw results may expose additional earlier causes.
    pub fn failure(&self) -> PinnedStoreOpenFailure {
        self.scope
            .terminal_failure
            .unwrap_or(PinnedStoreOpenFailure::Invariant)
    }
    /// Exact actual fees, with no counter reset on error.
    pub fn usage(&self) -> PinnedStoreOpenUsage {
        self.scope.work.usage
    }
    /// SAME original absolute open deadline; no replacement clock.
    pub fn absolute_expiry(&self) -> Instant {
        self.scope.work.expiry
    }
    /// Actual scoped composing refusal, independently retained from native IO.
    pub fn checkpoint_failure(&self) -> Option<ShardDirectoryCheckFailure> {
        self.scope.work.checkpoint_failure
    }
    /// Every captured native outcome; file slots remain privately owned here.
    pub fn outcomes(&self) -> &[PinnedStoreOpenOutcome] {
        &self.scope.outcomes
    }
    /// The SAME composing denial returned normally; never a grant.
    pub fn denial_completed(&self) -> bool {
        self.scope.denial_completed
    }
    /// Actual original producer/test/checkpoint panic payload.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.scope.panic_payload.as_deref()
    }
    /// Actual panic from the composing denial, if denial did not return normally.
    pub fn deny_panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.scope.deny_panic_payload.as_deref()
    }
    /// Actual panic from prepaid cleanup; no successful-settlement claim.
    pub fn settlement_panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.scope.settlement_panic_payload.as_deref()
    }
    /// Native corruption or derived unexpected-EOF value outside a raw row.
    pub fn native_error(&self) -> Option<&StorageError> {
        self.scope.native_error.as_ref()
    }
    /// Actual failed fallible source-visible reservation, when one occurred.
    pub fn allocation_error(&self) -> Option<&TryReserveError> {
        self.scope.allocation_error.as_ref()
    }
    /// Actual native name conversion error, observed before any syscall.
    pub fn name_error(&self) -> Option<&NulError> {
        self.scope.name_error.as_ref()
    }
    /// Real entered phase without a result at the original refusal boundary.
    pub fn unfinished_phase(&self) -> Option<PinnedStoreOpenPhase> {
        self.scope.refused_phase
    }
    /// A settlement phase that itself unwound without a completed result.
    pub fn unfinished_settlement_phase(&self) -> Option<PinnedStoreOpenPhase> {
        self.scope.work.unfinished_phase
    }
    /// Actual tail cleanup helper Result, including a non-I/O refusal.
    pub fn tail_settlement(&self) -> Option<&Result<(), PinnedStoreOpenFailure>> {
        self.scope.tail_settlement.as_ref()
    }
    /// Actual parent cleanup helper Result retained before returning cleanup.
    pub fn parent_settlement(&self) -> Option<&Result<(), PinnedStoreOpenFailure>> {
        self.scope.parent_settlement.as_ref()
    }
    /// Actual copied name retained before any allocation/copy postcheck.
    pub fn current_name(&self) -> Option<&OsStr> {
        self.scope.current_name.as_deref()
    }
    /// Actual copied path retained before any allocation/copy postcheck.
    pub fn current_path(&self) -> Option<&Path> {
        self.scope.current_path.as_deref()
    }
    /// Actual duplicate pack-map value replaced by the native insertion rule.
    pub fn replaced_path(&self) -> Option<&Path> {
        self.scope.replaced_path.as_deref()
    }
    /// Actual failed file-open/clone Result, without exposing its descriptor.
    pub fn file_error(&self, slot: usize) -> Option<&io::Error> {
        self.scope.files.get(slot)?.result.as_ref()?.as_ref().err()
    }
    /// Actual phase attached to that privately retained native file Result.
    pub fn file_phase(&self, slot: usize) -> Option<PinnedStoreOpenPhase> {
        self.scope.files.get(slot).map(|s| s.phase)
    }
    /// Retained file-Result slots; FDs already moved into a store are excluded.
    pub fn retained_file_slots(&self) -> usize {
        self.scope
            .files
            .iter()
            .filter(|s| s.result.is_some())
            .count()
    }
    /// Actual partial native index cardinalities; no completeness claim.
    pub fn partial_index_entries(&self) -> (usize, usize) {
        match self.scope.store.as_ref() {
            Some(store) => (store.index.len(), store.by_batch.len()),
            None => (self.scope.index.len(), self.scope.by_batch.len()),
        }
    }
}

#[allow(
    clippy::result_large_err,
    reason = "retain actual partial native files, indexes and outcomes without a failure allocation"
)]
impl ShardStore {
    /// Open/repair using the SAME borrowed physical shard directory and lease.
    ///
    /// Prepay `pinned_store_open_cost` before entry; finite counters and original
    /// E are checked inside actual directory/read/decode/index/repair phases.
    /// The composing owner must retain the real pin/Core parent through output
    /// distribution and last worker cleanup. Ordinary later store operations
    /// retain their existing behavior and need separate strict producers.
    ///
    /// # Errors
    /// Returns actual retained partial files/indexes/outcomes on capacity,
    /// expiry, native validation/I/O, checkpoint or unwind refusal. Entered
    /// mutations settle their already-prepaid sync once after SAME denial.
    pub fn open_pinned_until(
        config: ShardStoreConfig,
        pin: &PinnedShardDirectory,
        limits: PinnedStoreOpenLimits,
        original_expiry: Instant,
        checks: &dyn PinnedStoreOpenChecks,
    ) -> Result<OpenedPinnedShardStore, PinnedStoreOpenError> {
        Self::open_pinned_observed(config, pin, limits, original_expiry, checks, |_| {})
    }
    fn open_pinned_observed(
        config: ShardStoreConfig,
        pin: &PinnedShardDirectory,
        limits: PinnedStoreOpenLimits,
        original_expiry: Instant,
        checks: &dyn PinnedStoreOpenChecks,
        mut completed: impl FnMut(PinnedStoreOpenPhase),
    ) -> Result<OpenedPinnedShardStore, PinnedStoreOpenError> {
        let mut scope = Scope::new(config, limits, original_expiry);
        let observed = catch_unwind(AssertUnwindSafe(|| {
            let config = scope
                .config
                .as_ref()
                .ok_or(PinnedStoreOpenFailure::Invariant)?;
            pinned_store_open_cost(config, limits).ok_or(PinnedStoreOpenFailure::Capacity)?;
            scope.run(pin, checks, &mut completed)
        }));
        let observed = match observed {
            Ok(result) => result,
            Err(payload) => {
                scope.panic_payload = Some(payload);
                Err(PinnedStoreOpenFailure::Unwind)
            }
        };
        match observed {
            Ok(()) => match scope.store.take() {
                Some(store) => Ok(OpenedPinnedShardStore { store, scope }),
                None => {
                    scope.refuse(pin, PinnedStoreOpenFailure::Invariant, checks);
                    Err(PinnedStoreOpenError { scope })
                }
            },
            Err(failure) => {
                scope.refuse(pin, failure, checks);
                Err(PinnedStoreOpenError { scope })
            }
        }
    }
}

#[cfg(test)]
#[path = "pinned_store_open_tests.rs"]
mod tests;
