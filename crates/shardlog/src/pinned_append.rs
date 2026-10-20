//! Bounded append and rotation on the actual retained store and pinned lease.
//!
//! A composing owner retains both borrowed native owners through report/error
//! cleanup. These observations carry no ownership, currentness or serving grant.

use super::*;
use rustix::fs::{Mode, OFlags, fdatasync, fsync, ftruncate, openat};
use std::collections::TryReserveError;
use std::ffi::{CString, NulError, OsStr, OsString};
use std::os::unix::ffi::OsStrExt;

#[path = "pinned_append_io.rs"]
mod physical;

/// Cumulative explicit source limits for one append operation budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PinnedShardAppendLimits {
    /// Maximum entered append/seal invocations using this budget.
    pub max_operations: usize,
    /// Cumulative input extents and corresponding prepared/index values.
    pub max_frames: usize,
    /// Cumulative input payload bytes retained by actual input values.
    pub max_payload_bytes: usize,
    /// Cumulative native segment flush boundaries.
    pub max_segments: usize,
    /// Cumulative actual native rotations.
    pub max_rotations: usize,
    /// Maximum configured directory and actual complete retained pack pathname.
    pub max_path_bytes: usize,
    /// Maximum literal actual active basename, before copying/conversion.
    pub max_name_bytes: usize,
    /// Existing pack-map length plus requested new native pack slots.
    pub max_packs: usize,
    /// Existing batch-map capacity plus requested new batch slots.
    pub max_batch_slots: usize,
    /// Existing topic-map capacity plus requested new topic slots.
    pub max_topic_slots: usize,
    /// Existing summed location-vector capacities plus new locations.
    pub max_location_slots: usize,
    /// Cumulative actual calls, including reserved rollback/durability calls.
    pub max_io_calls: usize,
    /// Cumulative entered direct write attempts, including Interrupted.
    pub max_write_calls: usize,
    /// Cumulative byte lengths submitted to direct native write attempts.
    pub max_write_bytes: usize,
    /// Explicit source loops, comparisons, checksums, copies and index movement.
    pub max_work_bytes: usize,
}

/// Plain checked source cost; this admits no credits or native permission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PinnedShardAppendCost {
    /// Fixed values, requested slots, headers, names and retained inputs.
    pub declared_bytes: usize,
    /// Native names, paths and bounded native diagnostics, included above.
    pub string_bytes: usize,
    /// Fixed values plus requested container/native/partial-result slots.
    pub entries: usize,
    /// Declared setup plus explicit cumulative inner work allowance.
    pub work_bytes: usize,
    /// Retained input payload ceiling, included in declared bytes.
    pub payload_bytes: usize,
}

/// SAME composing deadline/counters and deny-before-settlement action.
///
/// This ordinary callback is not an authority witness. The caller retains its
/// actual Core owner, store and pin until all reports and errors are cleaned up.
pub trait PinnedShardAppendChecks: ShardDirectoryChecks {
    /// Deny the SAME composing work/health before entered physical cleanup.
    fn deny(&self);
}

/// An owned actual native append input, retained on every refusal.
#[derive(Debug)]
pub struct PinnedShardAppendInput {
    /// Actual reservation decoded/derived by the composing native consumer.
    pub reservation: Reservation,
    /// Actual optional native producer tuple.
    pub producer: Option<ProducerSequence>,
    /// Actual optional native atomic-group tuple.
    pub atomic_group: Option<AtomicGroupSequence>,
    /// Actual original payload; no complete frame or BODY permission is inferred.
    pub payload: Bytes,
}

/// Actual cumulative usage; reservations are never refunded after refusal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PinnedShardAppendUsage {
    /// Entered append/seal invocations.
    pub operations: usize,
    /// Admitted input frame slots.
    pub frames: usize,
    /// Admitted input bytes.
    pub payload_bytes: usize,
    /// Entered segment boundaries.
    pub segments: usize,
    /// Entered rotations.
    pub rotations: usize,
    /// Reserved native calls, including still-unentered settlement slots.
    pub reserved_io_calls: usize,
    /// Actually entered native calls.
    pub entered_io_calls: usize,
    /// Actually entered direct write attempts.
    pub write_calls: usize,
    /// Cumulative submitted write lengths.
    pub write_bytes: usize,
    /// Admitted explicit work.
    pub work_bytes: usize,
}

/// Original-E cumulative budget; it contains no owner or authority.
#[derive(Debug)]
pub struct PinnedShardAppendBudget {
    limits: PinnedShardAppendLimits,
    expiry: Instant,
    usage: PinnedShardAppendUsage,
    refused: bool,
}
impl PinnedShardAppendBudget {
    /// Capture the original E after the composing caller prepays the plain cost.
    pub fn new(limits: PinnedShardAppendLimits, expiry: Instant) -> Option<Self> {
        pinned_shard_append_cost(limits)?;
        Some(Self {
            limits,
            expiry,
            usage: PinnedShardAppendUsage::default(),
            refused: false,
        })
    }
    /// Tighten the SAME absolute E without renewing counters or clearing refusal.
    pub fn cap_expiry(&mut self, earlier: Instant) {
        self.expiry = self.expiry.min(earlier);
    }
    /// Actual original/minimum absolute expiry.
    pub fn absolute_expiry(&self) -> Instant {
        self.expiry
    }
    /// Exact immutable actual limits.
    pub fn limits(&self) -> PinnedShardAppendLimits {
        self.limits
    }
    /// Actual cumulative usage, including failed calls.
    pub fn usage(&self) -> PinnedShardAppendUsage {
        self.usage
    }
    /// Deny this same cumulative native budget after a composing late/refusal.
    pub fn deny(&mut self) {
        self.refused = true;
    }
    /// Sticky refusal, which no successful subsequent call can clear.
    pub fn is_refused(&self) -> bool {
        self.refused
    }
}

/// Actual refusal category; original typed results remain in the report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PinnedShardAppendFailure {
    /// The original/minimum E elapsed.
    Expired,
    /// SAME composing checkpoint refused.
    Checkpoint,
    /// Explicit cumulative source limit refused.
    Capacity,
    /// Checked source/native arithmetic overflowed.
    Arithmetic,
    /// An actual fallible reservation failed.
    Allocation,
    /// Native input/producer/group validation returned its retained error.
    Native,
    /// Native C-name conversion failed.
    Name,
    /// Actual store/pin/directory/lease/active-file identity mismatched.
    Identity,
    /// An actual physical native call returned its retained error.
    Io,
    /// An entered native phase unwound.
    Unwind,
    /// The SAME budget was already denied or a native state invariant refused.
    Refused,
}

/// Entered or completed actual native producer phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PinnedShardAppendPhase {
    /// Actual retained names, paths or frame-header reservation/conversion.
    Prepared,
    /// Real original/native directory metadata.
    Directory,
    /// Real held or named lease metadata.
    Lock,
    /// Actual active descriptor and no-follow named pack metadata.
    ActiveIdentity,
    /// Actual append seek.
    Seek,
    /// One direct physical write attempt.
    Write,
    /// Actual native descriptor flush.
    Flush,
    /// Actual native descriptor sync_data.
    SyncData,
    /// Actual same-FD rollback after an unsuccessful unindexed segment.
    Rollback,
    /// Actual native index installation (possibly only partial on refusal).
    Index,
    /// Actual descriptor-relative CreateNew next-pack attempt.
    Create,
    /// Already-prepaid actual parent fsync after entered CreateNew.
    DirectorySync,
    /// Actual new pack-map publication.
    PackIndex,
    /// Actual native active-file swap; old FD remains in report.
    Rotation,
}

struct WriteAttempt {
    sequence: u64,
    frame: usize,
    part: usize,
    offset: usize,
    requested: usize,
    result: io::Result<usize>,
}
enum Outcome {
    Stat(PinnedShardAppendPhase, io::Result<Stat>),
    Seek(io::Result<u64>),
    Write(WriteAttempt),
    Unit(PinnedShardAppendPhase, io::Result<()>),
    Created(Option<File>, Option<io::Error>),
}
struct Rotation {
    sequence: u64,
    name: String,
    native_name: Option<CString>,
    name_error: Option<NulError>,
    path: Option<PathBuf>,
    created_outcome: Option<usize>,
    retired: Option<ActivePack>,
    transferred: bool,
}
struct NewTopic {
    topic: TopicPartition,
    locations: Option<Vec<ExtentLocation>>,
}
struct PendingFile {
    sequence: u64,
    rollback: Option<u64>,
    rollback_entered: bool,
    flush_entered: bool,
    sync_entered: bool,
}
struct Scope {
    inputs: Vec<PinnedShardAppendInput>,
    prepared: Vec<PreparedExtent>,
    new_topics: Vec<NewTopic>,
    keys: HashSet<(TopicPartition, BatchId)>,
    outcomes: Vec<Outcome>,
    rotations: Vec<Rotation>,
    allocation_slots: Option<Result<(), TryReserveError>>,
    allocations: Vec<Result<(), TryReserveError>>,
    native_error: Option<StorageError>,
    directory_name: Option<CString>,
    directory_name_error: Option<NulError>,
    active_original_name: Option<OsString>,
    active_path_bytes: Option<usize>,
    active_name: Option<CString>,
    active_name_error: Option<NulError>,
    checkpoint: Option<ShardDirectoryCheckFailure>,
    unfinished: Option<PinnedShardAppendPhase>,
    original_unfinished: Option<PinnedShardAppendPhase>,
    pending_file: Option<PendingFile>,
    pending_directory_sync: bool,
    directory_sync_entered: bool,
    panic_payload: Option<Box<dyn Any + Send>>,
    denial_panic: Option<Box<dyn Any + Send>>,
    settlement_panic: Option<Box<dyn Any + Send>>,
    settlement_phase_panics: [Option<Box<dyn Any + Send>>; 4],
    last_completed: Option<PinnedShardAppendPhase>,
    settlement_failure: Option<PinnedShardAppendFailure>,
    installed_frames: usize,
    sealed: Option<SealedPack>,
    sealed_completed: bool,
    original_active_sequence: Option<u64>,
    usage: PinnedShardAppendUsage,
}

/// Actual typed append/rotation effects, with original inputs retained.
pub struct PinnedShardAppendReport {
    scope: Scope,
}
// Private actual ownership transfer used only by the canonical owner. It is
// deliberately absent from the public native report API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RetiredTransferFailure {
    OutOfRange,
    Occupied,
    Missing,
}

impl PinnedShardAppendReport {
    pub(super) fn transfer_shape(&self) -> Option<(usize, usize)> {
        Some((
            self.scope.rotations.len(),
            self.scope
                .outcomes
                .capacity()
                .checked_add(1)?
                .checked_mul(size_of::<Outcome>())?,
        ))
    }
    pub(super) fn has_created_file(&self) -> bool {
        self.scope
            .outcomes
            .iter()
            .any(|outcome| matches!(outcome, Outcome::Created(Some(_), _)))
    }
    pub(super) fn transfer_retired_into(
        &mut self,
        index: usize,
        destination: &mut Option<ActivePack>,
    ) -> Result<(), RetiredTransferFailure> {
        if destination.is_some() {
            return Err(RetiredTransferFailure::Occupied);
        }
        let rotation = self
            .scope
            .rotations
            .get_mut(index)
            .ok_or(RetiredTransferFailure::OutOfRange)?;
        if rotation.retired.is_none() {
            return Err(RetiredTransferFailure::Missing);
        }
        // All destination capacity and move work is paid by the whole owner.
        // No callback, allocation, fallible step or displaced File drop occurs
        // between the actual source take and assignment into an empty slot.
        *destination = rotation.retired.take();
        Ok(())
    }
    /// Actual original input values, including payload bytes.
    pub fn inputs(&self) -> &[PinnedShardAppendInput] {
        &self.scope.inputs
    }
    /// Actual complete or partial native prepared frames, retained before E.
    pub fn prepared(&self) -> &[PreparedExtent] {
        &self.scope.prepared
    }
    /// Actual installed native index count, including partial failed work.
    pub fn installed_frames(&self) -> usize {
        self.scope.installed_frames
    }
    /// Actual cumulative budget usage at terminal return.
    pub fn usage(&self) -> PinnedShardAppendUsage {
        self.scope.usage
    }
    /// Actual sealed old pack, when explicit sealing completed.
    pub fn sealed_pack(&self) -> Option<&SealedPack> {
        self.scope
            .sealed
            .as_ref()
            .filter(|_| self.scope.sealed_completed)
    }
    /// Actual original active sequence before any entered work.
    pub fn original_active_sequence(&self) -> Option<u64> {
        self.scope.original_active_sequence
    }
    /// Actual proposed sealed metadata, also retained when rotation refused.
    pub fn sealed_candidate(&self) -> Option<&SealedPack> {
        self.scope.sealed.as_ref()
    }
    /// Actual native headers, including a header whose native codec refused.
    pub fn prepared_headers(&self) -> impl Iterator<Item = &[u8]> {
        self.scope
            .prepared
            .iter()
            .map(|p| p.encoded.header.as_slice())
    }
    /// Entered phase without a completed result (including unwind).
    pub fn unfinished_phase(&self) -> Option<PinnedShardAppendPhase> {
        self.scope.original_unfinished.or(self.scope.unfinished)
    }
    /// Original native validation/codec error, never stringified away.
    pub fn native_error(&self) -> Option<&StorageError> {
        self.scope.native_error.as_ref()
    }
    /// Actual physical write results in original entered order.
    pub fn write_results(&self) -> impl Iterator<Item = &io::Result<usize>> {
        self.scope.outcomes.iter().filter_map(|v| match v {
            Outcome::Write(v) => Some(&v.result),
            _ => None,
        })
    }
    /// Actual sequence/frame/part/byte-offset/requested-length and typed write result.
    /// Part zero is the native header, one payload and two checksum.
    pub fn write_observations(
        &self,
    ) -> impl Iterator<Item = (u64, usize, usize, usize, usize, &io::Result<usize>)> {
        self.scope.outcomes.iter().filter_map(|v| match v {
            Outcome::Write(v) => Some((
                v.sequence,
                v.frame,
                v.part,
                v.offset,
                v.requested,
                &v.result,
            )),
            _ => None,
        })
    }
    /// Actual seek results, including their original errors.
    pub fn seek_results(&self) -> impl Iterator<Item = &io::Result<u64>> {
        self.scope.outcomes.iter().filter_map(|v| match v {
            Outcome::Seek(v) => Some(v),
            _ => None,
        })
    }
    /// Actual flush/sync/truncate results, including prepaid late settlement.
    pub fn unit_results(&self) -> impl Iterator<Item = (PinnedShardAppendPhase, &io::Result<()>)> {
        self.scope.outcomes.iter().filter_map(|v| match v {
            Outcome::Unit(p, v) => Some((*p, v)),
            _ => None,
        })
    }
    /// Actual descriptor/name stat results, including native errno.
    pub fn metadata_results(
        &self,
    ) -> impl Iterator<Item = (PinnedShardAppendPhase, &io::Result<Stat>)> {
        self.scope.outcomes.iter().filter_map(|v| match v {
            Outcome::Stat(p, v) => Some((*p, v)),
            _ => None,
        })
    }
    /// Actual CreateNew errors; existing unknown names are never unlinked.
    pub fn create_errors(&self) -> impl Iterator<Item = &io::Error> {
        self.scope.outcomes.iter().filter_map(|v| match v {
            Outcome::Created(_, Some(e)) => Some(e),
            _ => None,
        })
    }
    /// First actual native IO error, retaining its original type and errno.
    pub fn io_error(&self) -> Option<&io::Error> {
        self.scope.outcomes.iter().find_map(|v| match v {
            Outcome::Stat(_, Err(e))
            | Outcome::Seek(Err(e))
            | Outcome::Unit(_, Err(e))
            | Outcome::Created(_, Some(e)) => Some(e),
            Outcome::Write(WriteAttempt { result: Err(e), .. }) => Some(e),
            _ => None,
        })
    }
    /// Actual native C-name conversion error, including its original bytes.
    pub fn name_error(&self) -> Option<&NulError> {
        self.scope
            .directory_name_error
            .as_ref()
            .or(self.scope.active_name_error.as_ref())
            .or_else(|| {
                self.scope
                    .rotations
                    .iter()
                    .find_map(|v| v.name_error.as_ref())
            })
    }
    /// Real created files still held by this report on partial rotation.
    pub fn retained_created_files(&self) -> impl Iterator<Item = &File> {
        self.scope.outcomes.iter().filter_map(|v| match v {
            Outcome::Created(Some(f), _) => Some(f),
            _ => None,
        })
    }
    /// Real retired active files, not duplicate or reopened descriptors.
    pub fn retired_files(&self) -> impl Iterator<Item = &File> {
        self.scope
            .rotations
            .iter()
            .filter_map(|v| v.retired.as_ref().map(|v| &v.file))
    }
    /// Literal actual native names whose creation was attempted.
    pub fn rotation_names(&self) -> impl Iterator<Item = &OsStr> {
        self.scope.rotations.iter().map(|v| OsStr::new(&v.name))
    }
    /// Sequence/name/create-completed/FD-transferred facts for every rotation.
    /// An uncertain entered create is also retained by `unfinished_phase`.
    pub fn rotation_observations(&self) -> impl Iterator<Item = (u64, &OsStr, bool, bool)> {
        self.scope.rotations.iter().map(|v| {
            (
                v.sequence,
                OsStr::new(&v.name),
                v.created_outcome.is_some(),
                v.transferred,
            )
        })
    }
    /// Actual fallible reservation failures retained before post-E checks.
    pub fn allocation_error(&self) -> Option<&TryReserveError> {
        self.scope
            .allocation_slots
            .as_ref()
            .and_then(|r| r.as_ref().err())
            .or_else(|| self.scope.allocations.iter().find_map(|r| r.as_ref().err()))
    }
    /// Original actual active basename retained before C-name/post-E checks.
    pub fn original_active_name(&self) -> Option<&OsStr> {
        self.scope.active_original_name.as_deref()
    }
    /// Actual complete stored path byte length, before any component scan.
    pub fn active_path_bytes(&self) -> Option<usize> {
        self.scope.active_path_bytes
    }
    /// Actual converted active basename, retained before its post-E check.
    pub fn active_c_name(&self) -> Option<&std::ffi::CStr> {
        self.scope.active_name.as_deref()
    }
    /// Actual original native producer panic.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.scope.panic_payload.as_deref()
    }
    /// Actual SAME composing denial panic, if denial itself unwound.
    pub fn denial_panic(&self) -> Option<&(dyn Any + Send)> {
        self.scope.denial_panic.as_deref()
    }
    /// Last completed phase, also retained when its completion hook unwound.
    pub fn last_completed_phase(&self) -> Option<PinnedShardAppendPhase> {
        self.scope.last_completed
    }
    /// Actual per-settlement-phase panics; later prepaid phases still settle.
    pub fn settlement_phase_panics(&self) -> impl Iterator<Item = &(dyn Any + Send)> {
        self.scope
            .settlement_phase_panics
            .iter()
            .filter_map(Option::as_deref)
    }
    /// Actual late durability-settlement panic, retained separately.
    pub fn settlement_panic(&self) -> Option<&(dyn Any + Send)> {
        self.scope.settlement_panic.as_deref()
    }
    /// Still-unentered prepaid file settlement (sequence, rollback, flush, sync).
    /// An entered uncertain phase is instead recorded by `unfinished_phase`.
    pub fn pending_file_settlement(&self) -> Option<(u64, bool, bool, bool)> {
        self.scope.pending_file.as_ref().map(|p| {
            (
                p.sequence,
                p.rollback.is_some() && !p.rollback_entered,
                !p.flush_entered,
                !p.sync_entered,
            )
        })
    }
    /// Whether a prepaid parent fsync has not entered yet.
    pub fn pending_directory_sync(&self) -> bool {
        self.scope.pending_directory_sync && !self.scope.directory_sync_entered
    }
    /// Actual unfinished settlement refusal, never converted to success.
    pub fn settlement_failure(&self) -> Option<PinnedShardAppendFailure> {
        self.scope.settlement_failure
    }
}
impl fmt::Debug for PinnedShardAppendReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardAppendReport")
            .field("inputs", &self.scope.inputs.len())
            .field("installed_frames", &self.scope.installed_frames)
            .field("usage", &self.scope.usage)
            .field("unfinished", &self.scope.unfinished)
            .finish_non_exhaustive()
    }
}
/// Refusal owning all actual partial source/physical results.
#[derive(Debug)]
pub struct PinnedShardAppendError {
    failure: PinnedShardAppendFailure,
    report: PinnedShardAppendReport,
}
impl PinnedShardAppendError {
    /// Actual terminal refusal.
    pub fn failure(&self) -> PinnedShardAppendFailure {
        self.failure
    }
    /// Actual retained native inputs/results/effects.
    pub fn report(&self) -> &PinnedShardAppendReport {
        &self.report
    }
}
impl fmt::Display for PinnedShardAppendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pinned append refused: {:?}", self.failure)
    }
}
impl Error for PinnedShardAppendError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(e) = self.report.native_error() {
            return Some(e);
        }
        if let Some(e) = self.report.io_error() {
            return Some(e);
        }
        if let Some(e) = self.report.name_error() {
            return Some(e);
        }
        self.report.allocation_error().map(|e| e as &dyn Error)
    }
}

/// Source-derived logical values before the caller enters this producer.
///
/// Input payload allocations and existing store allocations are already held;
/// their retained byte/slot ceilings are included. Allocator/kernel overhead is
/// not an RSS guarantee. Actual IO attempts and loops spend cumulative limits.
pub fn pinned_shard_append_cost(l: PinnedShardAppendLimits) -> Option<PinnedShardAppendCost> {
    if l.max_operations == 0 || l.max_path_bytes == 0 || l.max_name_bytes == 0 {
        return None;
    }
    let fixed = size_of::<Scope>()
        .checked_add(size_of::<PinnedShardAppendBudget>())?
        .checked_add(size_of::<PinnedShardAppendReport>())?
        .checked_add(size_of::<PinnedShardAppendError>())?;
    let slots = [
        (
            l.max_frames,
            size_of::<PinnedShardAppendInput>() + size_of::<PreparedExtent>() + HEADER_LEN,
        ),
        (
            l.max_frames,
            size_of::<NewTopic>() + size_of::<(TopicPartition, BatchId)>(),
        ),
        (l.max_io_calls, size_of::<Outcome>()),
        (l.max_rotations, size_of::<Rotation>()),
        (
            l.max_frames.checked_mul(4)?.checked_add(8)?,
            size_of::<Result<(), TryReserveError>>(),
        ),
        (
            l.max_batch_slots,
            size_of::<((TopicPartition, BatchId), ExtentLocation)>(),
        ),
        (
            l.max_topic_slots,
            size_of::<(TopicPartition, Vec<ExtentLocation>)>(),
        ),
        (l.max_location_slots, size_of::<ExtentLocation>()),
        (l.max_packs, size_of::<(u64, PathBuf)>()),
    ];
    // Every separately retained operation owns its report capacities; charge
    // those capacities for every permitted invocation, even unused slots.
    let bytes = slots
        .iter()
        .try_fold(fixed, |n, (count, size)| {
            n.checked_add(count.checked_mul(*size)?)
        })?
        .checked_mul(l.max_operations)?;
    // Actual native pack name has twenty decimal u64 digits; diagnostic errors
    // below use only native scalar validation (no path-formatting diagnostics).
    let path = l.max_path_bytes.checked_add(1)?.checked_mul(4)?;
    let name = physical::NATIVE_PACK_NAME_BYTES;
    let strings = path
        .checked_add(l.max_name_bytes.checked_add(1)?.checked_mul(2)?)?
        .checked_add(
            l.max_rotations.checked_mul(
                l.max_path_bytes
                    .checked_add(name)?
                    .checked_add(1)?
                    .checked_mul(3)?,
            )?,
        )?
        .checked_add(l.max_frames.checked_mul(1024)?)?
        .checked_mul(l.max_operations)?;
    let declared = bytes
        .checked_add(strings)?
        .checked_add(l.max_payload_bytes)?;
    let entries = slots
        .iter()
        .try_fold(24usize, |n, (count, _)| n.checked_add(*count))?
        .checked_mul(l.max_operations)?;
    Some(PinnedShardAppendCost {
        declared_bytes: declared,
        string_bytes: strings,
        entries,
        payload_bytes: l.max_payload_bytes,
        work_bytes: declared
            .checked_add(entries)?
            .checked_add(l.max_work_bytes)?,
    })
}

fn completed(phase: PinnedShardAppendPhase) {
    #[cfg(test)]
    tests::completed(phase);
    #[cfg(not(test))]
    let _ = phase;
}
impl Scope {
    fn new(inputs: Vec<PinnedShardAppendInput>) -> Self {
        Self {
            inputs,
            prepared: Vec::new(),
            new_topics: Vec::new(),
            keys: HashSet::new(),
            outcomes: Vec::new(),
            rotations: Vec::new(),
            allocation_slots: None,
            allocations: Vec::new(),
            native_error: None,
            directory_name: None,
            directory_name_error: None,
            active_original_name: None,
            active_path_bytes: None,
            active_name: None,
            active_name_error: None,
            checkpoint: None,
            unfinished: None,
            original_unfinished: None,
            pending_file: None,
            pending_directory_sync: false,
            directory_sync_entered: false,
            panic_payload: None,
            denial_panic: None,
            settlement_panic: None,
            settlement_phase_panics: [None, None, None, None],
            last_completed: None,
            settlement_failure: None,
            installed_frames: 0,
            sealed: None,
            sealed_completed: false,
            original_active_sequence: None,
            usage: PinnedShardAppendUsage::default(),
        }
    }
    fn check(
        &mut self,
        b: &PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
    ) -> Result<(), PinnedShardAppendFailure> {
        if Instant::now() >= b.expiry {
            return Err(PinnedShardAppendFailure::Expired);
        }
        if b.refused {
            return Err(PinnedShardAppendFailure::Refused);
        }
        if let Err(e) = c.check() {
            self.checkpoint = Some(e);
        }
        if Instant::now() >= b.expiry {
            return Err(PinnedShardAppendFailure::Expired);
        }
        if self.checkpoint.is_some() {
            return Err(PinnedShardAppendFailure::Checkpoint);
        }
        Ok(())
    }
    fn work(
        &mut self,
        n: usize,
        b: &mut PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
    ) -> Result<(), PinnedShardAppendFailure> {
        self.check(b, c)?;
        let next = b
            .usage
            .work_bytes
            .checked_add(n)
            .ok_or(PinnedShardAppendFailure::Arithmetic)?;
        if next > b.limits.max_work_bytes {
            return Err(PinnedShardAppendFailure::Capacity);
        }
        b.usage.work_bytes = next;
        self.check(b, c)
    }
    fn reserve_calls(
        &mut self,
        n: usize,
        b: &mut PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
    ) -> Result<(), PinnedShardAppendFailure> {
        self.work(n, b, c)?;
        let next = b
            .usage
            .reserved_io_calls
            .checked_add(n)
            .ok_or(PinnedShardAppendFailure::Arithmetic)?;
        if next > b.limits.max_io_calls {
            return Err(PinnedShardAppendFailure::Capacity);
        }
        b.usage.reserved_io_calls = next;
        self.check(b, c)
    }
    fn enter(
        &mut self,
        p: PinnedShardAppendPhase,
        b: &mut PinnedShardAppendBudget,
    ) -> Result<(), PinnedShardAppendFailure> {
        let next = b
            .usage
            .entered_io_calls
            .checked_add(1)
            .ok_or(PinnedShardAppendFailure::Arithmetic)?;
        if next > b.usage.reserved_io_calls || self.outcomes.len() >= self.outcomes.capacity() {
            return Err(PinnedShardAppendFailure::Capacity);
        }
        b.usage.entered_io_calls = next;
        self.unfinished = Some(p);
        Ok(())
    }
    fn after(
        &mut self,
        p: PinnedShardAppendPhase,
        b: &PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
    ) -> Result<(), PinnedShardAppendFailure> {
        self.unfinished = None;
        self.last_completed = Some(p);
        completed(p);
        self.check(b, c)
    }
    fn native(
        &mut self,
        r: StorageResult<()>,
        b: &PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
    ) -> Result<(), PinnedShardAppendFailure> {
        if let Err(e) = r {
            self.native_error = Some(e);
        }
        self.check(b, c)?;
        if self.native_error.is_some() {
            Err(PinnedShardAppendFailure::Native)
        } else {
            Ok(())
        }
    }
    fn allocate(
        &mut self,
        b: &PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
    ) -> Result<(), PinnedShardAppendFailure> {
        self.check(b, c)?;
        // Retain the reservation of the reservation-result vector itself first.
        let r = self.allocations.try_reserve_exact(
            b.limits
                .max_frames
                .checked_mul(4)
                .and_then(|n| n.checked_add(8))
                .ok_or(PinnedShardAppendFailure::Arithmetic)?,
        );
        self.allocation_slots = Some(r);
        self.check(b, c)?;
        if self.allocation_slots.as_ref().is_some_and(Result::is_err) {
            return Err(PinnedShardAppendFailure::Allocation);
        }
        let r = self.prepared.try_reserve_exact(self.inputs.len());
        self.allocations.push(r);
        let r = self.new_topics.try_reserve_exact(self.inputs.len());
        self.allocations.push(r);
        let r = self.keys.try_reserve(self.inputs.len());
        self.allocations.push(r);
        let r = self.outcomes.try_reserve_exact(b.limits.max_io_calls);
        self.allocations.push(r);
        let r = self.rotations.try_reserve_exact(b.limits.max_rotations);
        self.allocations.push(r);
        self.check(b, c)?;
        if self.allocations.iter().any(Result::is_err) {
            return Err(PinnedShardAppendFailure::Allocation);
        }
        Ok(())
    }
}

impl ShardStore {
    /// Append actual payloads using the SAME store, pinned lease, budget and E.
    ///
    /// The caller prepays `pinned_shard_append_cost` and retains its genuine
    /// composing Core owner, this store and pin through all report/error cleanup.
    /// On any refusal the budget and SAME composing operation are denied before
    /// physical rollback/durability settlement; this store must not be reused.
    #[allow(
        clippy::result_large_err,
        reason = "retain actual append inputs and partial native settlement without a failure allocation"
    )]
    pub fn append_group_pinned_until(
        &mut self,
        pin: &PinnedShardDirectory,
        inputs: Vec<PinnedShardAppendInput>,
        sync: bool,
        budget: &mut PinnedShardAppendBudget,
        checks: &dyn PinnedShardAppendChecks,
    ) -> Result<PinnedShardAppendReport, PinnedShardAppendError> {
        append_entry(self, pin, inputs, sync, false, budget, checks)
    }
    /// Seal the actual nonempty active pack by the same bounded rotation path.
    #[allow(
        clippy::result_large_err,
        reason = "retain actual native seal settlement without allocating during refusal"
    )]
    pub fn seal_active_pinned_until(
        &mut self,
        pin: &PinnedShardDirectory,
        budget: &mut PinnedShardAppendBudget,
        checks: &dyn PinnedShardAppendChecks,
    ) -> Result<PinnedShardAppendReport, PinnedShardAppendError> {
        append_entry(self, pin, Vec::new(), false, true, budget, checks)
    }
}
#[allow(
    clippy::result_large_err,
    reason = "retain original payloads and actual native settlement without a failure allocation"
)]
fn append_entry(
    store: &mut ShardStore,
    pin: &PinnedShardDirectory,
    inputs: Vec<PinnedShardAppendInput>,
    sync: bool,
    seal: bool,
    b: &mut PinnedShardAppendBudget,
    c: &dyn PinnedShardAppendChecks,
) -> Result<PinnedShardAppendReport, PinnedShardAppendError> {
    let mut s = Scope::new(inputs);
    s.original_active_sequence = Some(store.active.sequence);
    let result = catch_unwind(AssertUnwindSafe(|| s.run(store, pin, sync, seal, b, c)));
    let failure = match result {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(e),
        Err(p) => {
            s.panic_payload = Some(p);
            Some(PinnedShardAppendFailure::Unwind)
        }
    };
    if let Some(failure) = failure {
        s.original_unfinished = s.unfinished;
        b.refused = true;
        if let Err(p) = catch_unwind(AssertUnwindSafe(|| c.deny())) {
            s.denial_panic = Some(p);
        }
        match catch_unwind(AssertUnwindSafe(|| s.settle(store, pin, b))) {
            Ok(Ok(())) => {}
            Ok(Err(e)) => s.settlement_failure = Some(e),
            Err(p) => s.settlement_panic = Some(p),
        }
        s.usage = b.usage;
        return Err(PinnedShardAppendError {
            failure,
            report: PinnedShardAppendReport { scope: s },
        });
    }
    s.usage = b.usage;
    Ok(PinnedShardAppendReport { scope: s })
}

#[cfg(test)]
#[path = "pinned_append_tests.rs"]
mod tests;
