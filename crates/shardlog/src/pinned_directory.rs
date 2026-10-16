//! Actual shard-directory descriptors joined to successfully acquired leases.
//!
//! Every native result precedes its post-call clock/checkpoint. Scoped ordinary
//! checks can tighten the composing owner's deadline; they are not stored in
//! the resulting physical owner and supply no currentness/serving authority.

use super::*;
use rustix::fs::{AtFlags, FileType, Stat, fstat, statat};
use std::any::Any;
use std::error::Error;
use std::fmt;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Finite source allowances for one real directory/lease identity observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PinnedShardDirectoryLimits {
    /// Maximum complete expected and held native directory pathname bytes.
    pub max_path_bytes: usize,
    /// At most four actual metadata/stat calls, including refused late slots.
    pub max_io_calls: usize,
}

/// Checked source-visible values and temporary native pathname conversion fees.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PinnedShardDirectoryCost {
    /// Fixed input/observation/error values plus native conversion ceilings.
    pub declared_bytes: usize,
    /// Path conversion subset already included in declared bytes.
    pub string_bytes: usize,
    /// Fixed values and four native outcome slots.
    pub entries: usize,
    /// Finite source setup, pathname comparison and observation fees.
    pub work_bytes: usize,
    /// Four real native observation calls on the fitting path.
    pub native_call_slots: usize,
}

/// Ordinary composing-counter refusal; this never grants physical permission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardDirectoryCheckFailure {
    /// The composing owner's actual counters or tighter minimum E refused.
    Refused,
}

/// Scoped ordinary checkpoints inside the actual native producer.
///
/// Implementations must use the composing operation's SAME counters/minimum E
/// and retain any richer native refusal they translate. The native producer
/// independently checks its original E before/after every checkpoint and keeps
/// its own finite native-call allowance. No callback or budget is kept in the
/// successful directory owner; later operations require their own actual work.
pub trait ShardDirectoryChecks {
    /// Check the original/minimum deadline and existing refusal state.
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure>;
}

/// Actual refusal; exact metadata, native errors and unwind facts remain held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PinnedShardDirectoryFailure {
    /// The producer's original absolute expiry elapsed.
    Expired,
    /// A finite pathname or physical-call allowance refused.
    Capacity,
    /// Checked source cost arithmetic overflowed.
    Arithmetic,
    /// The SAME composing operation's actual checkpoint refused.
    Checkpoint,
    /// An actual native metadata/stat call returned an error.
    Io,
    /// The expected name, directory descriptor or acquired lock did not match.
    Identity,
    /// An actual producer/checkpoint unwound; the original payload is retained.
    Unwind,
}

/// Actual observations only; no descriptor, guard or serving authority.
pub struct PinnedShardDirectoryObservation {
    expiry: Instant,
    reserved_calls: usize,
    entered_calls: usize,
    directory: Option<io::Result<fs::Metadata>>,
    named_directory: Option<io::Result<fs::Metadata>>,
    lock_descriptor: Option<io::Result<Stat>>,
    named_lock: Option<io::Result<Stat>>,
    identity_matches: Option<bool>,
    checkpoint_failure: Option<ShardDirectoryCheckFailure>,
    panic_payload: Option<Box<dyn Any + Send>>,
}
impl fmt::Debug for PinnedShardDirectoryObservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardDirectoryObservation")
            .field("expiry", &self.expiry)
            .field("reserved_calls", &self.reserved_calls)
            .field("entered_calls", &self.entered_calls)
            .field("identity_matches", &self.identity_matches)
            .field("checkpoint_failure", &self.checkpoint_failure)
            .field("io_error", &self.io_error())
            .field("unwound", &self.panic_payload.is_some())
            .finish_non_exhaustive()
    }
}
impl PinnedShardDirectoryObservation {
    /// Original absolute observation expiry, retained as diagnostic data only.
    pub fn absolute_expiry(&self) -> Instant {
        self.expiry
    }
    /// Reserved real native slots, including a slot refused before actual entry.
    pub fn reserved_calls(&self) -> usize {
        self.reserved_calls
    }
    /// Native observation calls actually entered after their last checkpoint.
    pub fn entered_calls(&self) -> usize {
        self.entered_calls
    }
    /// Unmodified actual directory-descriptor metadata result.
    pub fn directory_metadata(&self) -> Option<&io::Result<fs::Metadata>> {
        self.directory.as_ref()
    }
    /// Unmodified no-follow metadata from the held guard's directory pathname.
    pub fn named_directory_metadata(&self) -> Option<&io::Result<fs::Metadata>> {
        self.named_directory.as_ref()
    }
    /// Unmodified actual acquired-lock descriptor stat result.
    pub fn lock_descriptor_stat(&self) -> Option<&io::Result<Stat>> {
        self.lock_descriptor.as_ref()
    }
    /// Unmodified descriptor-relative no-follow named-lock stat result.
    pub fn named_lock_stat(&self) -> Option<&io::Result<Stat>> {
        self.named_lock.as_ref()
    }
    /// Actual completed directory/name/lock predicate, before its final clock.
    pub fn identity_matches(&self) -> Option<bool> {
        self.identity_matches
    }
    /// Actual scoped composing refusal, when one occurred.
    pub fn checkpoint_failure(&self) -> Option<ShardDirectoryCheckFailure> {
        self.checkpoint_failure
    }
    /// Actual retained producer/checkpoint unwind payload.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.panic_payload.as_deref()
    }
    /// Original native error, including its actual OS error code.
    pub fn io_error(&self) -> Option<&io::Error> {
        for value in [&self.directory, &self.named_directory] {
            if let Some(Err(error)) = value {
                return Some(error);
            }
        }
        for value in [&self.lock_descriptor, &self.named_lock] {
            if let Some(Err(error)) = value {
                return Some(error);
            }
        }
        None
    }
    fn new(expiry: Instant) -> Self {
        Self {
            expiry,
            reserved_calls: 0,
            entered_calls: 0,
            directory: None,
            named_directory: None,
            lock_descriptor: None,
            named_lock: None,
            identity_matches: None,
            checkpoint_failure: None,
            panic_payload: None,
        }
    }
    fn clock(
        &mut self,
        checks: &dyn ShardDirectoryChecks,
    ) -> Result<(), PinnedShardDirectoryFailure> {
        if Instant::now() >= self.expiry {
            return Err(PinnedShardDirectoryFailure::Expired);
        }
        let observed = checks.check();
        self.checkpoint_failure = observed.err();
        if Instant::now() >= self.expiry {
            return Err(PinnedShardDirectoryFailure::Expired);
        }
        if self.checkpoint_failure.is_some() {
            return Err(PinnedShardDirectoryFailure::Checkpoint);
        }
        Ok(())
    }
    fn enter(
        &mut self,
        limits: PinnedShardDirectoryLimits,
        checks: &dyn ShardDirectoryChecks,
    ) -> Result<(), PinnedShardDirectoryFailure> {
        self.clock(checks)?;
        if self.reserved_calls >= limits.max_io_calls {
            return Err(PinnedShardDirectoryFailure::Capacity);
        }
        self.reserved_calls += 1;
        self.clock(checks)?;
        self.entered_calls += 1;
        Ok(())
    }
}

struct Scope {
    observation: PinnedShardDirectoryObservation,
    directory: File,
    // Last: native results/directory settle before the actual acquired lock.
    guard: AcquiredShardRecoveryGuard,
}

/// Actual directory descriptor joined to its SAME successfully acquired lock.
///
/// The only constructor consumes both actual inputs and observes the expected
/// directory, opened descriptor and descriptor-relative native lock identity.
/// No original Core owner, recovery completeness, store open or serving grant
/// is inferred. There is no raw file/guard extraction or descriptor clone API.
pub struct PinnedShardDirectory {
    scope: Scope,
}
impl fmt::Debug for PinnedShardDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardDirectory")
            .field("observation", self.observation())
            .finish_non_exhaustive()
    }
}

/// Refusal retaining the actual input directory, successful guard and outcomes.
pub struct PinnedShardDirectoryError {
    failure: PinnedShardDirectoryFailure,
    scope: Scope,
}
impl fmt::Debug for PinnedShardDirectoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedShardDirectoryError")
            .field("failure", &self.failure)
            .field("observation", self.observation())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for PinnedShardDirectoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "native pinned shard directory refused: {:?}",
            self.failure
        )
    }
}
impl Error for PinnedShardDirectoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.scope
            .observation
            .io_error()
            .map(|error| error as &dyn Error)
    }
}
impl PinnedShardDirectoryError {
    /// Actual terminal refusal without discarding native outcomes or the lock.
    pub fn failure(&self) -> PinnedShardDirectoryFailure {
        self.failure
    }
    /// Exact partial native/checkpoint/unwind observations; no access grant.
    pub fn observation(&self) -> &PinnedShardDirectoryObservation {
        &self.scope.observation
    }
}

/// Checked plain source cost before transferring the already-owned inputs.
///
/// Existing input guard/path allocations are not copied or reconstructed.
/// Pathname conversion ceilings are included; allocator/library/kernel/RSS
/// overhead remains a separate measured gate, as with native guard acquisition.
pub fn pinned_shard_directory_cost(
    limits: PinnedShardDirectoryLimits,
) -> Option<PinnedShardDirectoryCost> {
    let values = [
        size_of::<Scope>(),
        size_of::<PinnedShardDirectory>(),
        size_of::<PinnedShardDirectoryError>(),
        size_of::<PinnedShardDirectoryObservation>(),
        size_of::<PinnedShardDirectoryCost>(),
    ];
    let fixed = values.into_iter().try_fold(0usize, usize::checked_add)?;
    let strings = limits
        .max_path_bytes
        .checked_add(1)?
        .checked_add(".shard-store.lock".len() + 1)?;
    let declared_bytes = fixed.checked_add(strings)?;
    Some(PinnedShardDirectoryCost {
        declared_bytes,
        string_bytes: strings,
        entries: values.len().checked_add(5)?,
        work_bytes: declared_bytes
            .checked_add(limits.max_path_bytes.checked_mul(2)?)?
            .checked_add(4)?,
        native_call_slots: 4,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    DirectoryMetadata,
    NamedDirectoryMetadata,
    LockDescriptorStat,
    NamedLockStat,
    Identity,
}

impl Scope {
    fn inspect(
        &mut self,
        expected: &Path,
        limits: PinnedShardDirectoryLimits,
        checks: &dyn ShardDirectoryChecks,
        completed: &mut impl FnMut(Phase),
    ) -> Result<(), PinnedShardDirectoryFailure> {
        self.observation.clock(checks)?;
        if expected.as_os_str().len() > limits.max_path_bytes
            || self.guard.guard().directory.as_os_str().len() > limits.max_path_bytes
        {
            return Err(PinnedShardDirectoryFailure::Capacity);
        }
        pinned_shard_directory_cost(limits).ok_or(PinnedShardDirectoryFailure::Arithmetic)?;
        if expected != self.guard.guard().directory {
            return Err(PinnedShardDirectoryFailure::Identity);
        }
        self.observation.enter(limits, checks)?;
        self.observation.directory = Some(self.directory.metadata());
        completed(Phase::DirectoryMetadata);
        self.observation.clock(checks)?;
        if self.observation.io_error().is_some() {
            return Err(PinnedShardDirectoryFailure::Io);
        }
        self.observation.enter(limits, checks)?;
        self.observation.named_directory =
            Some(fs::symlink_metadata(&self.guard.guard().directory));
        completed(Phase::NamedDirectoryMetadata);
        self.observation.clock(checks)?;
        if self.observation.io_error().is_some() {
            return Err(PinnedShardDirectoryFailure::Io);
        }
        self.observation.enter(limits, checks)?;
        self.observation.lock_descriptor =
            Some(fstat(&self.guard.guard().file).map_err(io::Error::from));
        completed(Phase::LockDescriptorStat);
        self.observation.clock(checks)?;
        if self.observation.io_error().is_some() {
            return Err(PinnedShardDirectoryFailure::Io);
        }
        self.observation.enter(limits, checks)?;
        self.observation.named_lock = Some(
            statat(
                &self.directory,
                ".shard-store.lock",
                AtFlags::SYMLINK_NOFOLLOW,
            )
            .map_err(io::Error::from),
        );
        completed(Phase::NamedLockStat);
        self.observation.clock(checks)?;
        if self.observation.io_error().is_some() {
            return Err(PinnedShardDirectoryFailure::Io);
        }
        let matched = match (
            &self.observation.directory,
            &self.observation.named_directory,
            &self.observation.lock_descriptor,
            &self.observation.named_lock,
        ) {
            (Some(Ok(actual)), Some(Ok(named)), Some(Ok(lock)), Some(Ok(named_lock))) => {
                actual.is_dir()
                    && named.is_dir()
                    && !named.file_type().is_symlink()
                    && actual.dev() == named.dev()
                    && actual.ino() == named.ino()
                    && FileType::from_raw_mode(lock.st_mode) == FileType::RegularFile
                    && FileType::from_raw_mode(named_lock.st_mode) == FileType::RegularFile
                    && lock.st_dev == named_lock.st_dev
                    && lock.st_ino == named_lock.st_ino
            }
            _ => false,
        };
        self.observation.identity_matches = Some(matched);
        completed(Phase::Identity);
        self.observation.clock(checks)?;
        if !matched {
            return Err(PinnedShardDirectoryFailure::Identity);
        }
        Ok(())
    }
}

#[allow(
    clippy::result_large_err,
    reason = "retain actual input descriptors, acquired lock and native outcomes without allocating on refusal"
)]
impl PinnedShardDirectory {
    /// Join an actual directory descriptor to its acquired native lease under E.
    ///
    /// Admit `pinned_shard_directory_cost` before entry. Scoped checks must share
    /// the caller's actual operation budget; original E is also checked inside
    /// every native phase. Inputs remain owned on late/error/unwind, with every
    /// completed raw result saved first. No name is opened, removed or repaired.
    pub fn from_descriptor_until(
        directory: File,
        guard: AcquiredShardRecoveryGuard,
        expected_directory: &Path,
        limits: PinnedShardDirectoryLimits,
        original_expiry: Instant,
        checks: &dyn ShardDirectoryChecks,
    ) -> Result<Self, PinnedShardDirectoryError> {
        Self::from_descriptor_observed(
            directory,
            guard,
            expected_directory,
            limits,
            original_expiry,
            checks,
            |_| {},
        )
    }
    fn from_descriptor_observed(
        directory: File,
        guard: AcquiredShardRecoveryGuard,
        expected_directory: &Path,
        limits: PinnedShardDirectoryLimits,
        original_expiry: Instant,
        checks: &dyn ShardDirectoryChecks,
        mut completed: impl FnMut(Phase),
    ) -> Result<Self, PinnedShardDirectoryError> {
        let mut scope = Scope {
            observation: PinnedShardDirectoryObservation::new(original_expiry),
            directory,
            guard,
        };
        let observed = catch_unwind(AssertUnwindSafe(|| {
            scope.inspect(expected_directory, limits, checks, &mut completed)
        }));
        let observed = match observed {
            Ok(observed) => observed,
            Err(payload) => {
                scope.observation.panic_payload = Some(payload);
                Err(PinnedShardDirectoryFailure::Unwind)
            }
        };
        match observed {
            Ok(()) => Ok(Self { scope }),
            Err(failure) => Err(PinnedShardDirectoryError { failure, scope }),
        }
    }
    /// Actual successful constructor observations, not a permanent deadline.
    pub fn observation(&self) -> &PinnedShardDirectoryObservation {
        &self.scope.observation
    }
    /// Borrowed original directory spelling; actual I/O uses the retained FD.
    pub fn directory(&self) -> &Path {
        &self.scope.guard.guard().directory
    }
}

#[cfg(test)]
#[path = "pinned_directory_tests.rs"]
mod tests;
