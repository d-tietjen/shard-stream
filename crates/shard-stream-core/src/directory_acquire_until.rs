//! Bounded descriptor-relative acquisition, retaining late/partial custody.

use super::{DirectoryOwnership, DirectoryOwnershipError, DirectoryOwnershipUntilError};
use std::{error::Error, fmt, path::Path, sync::Arc, time::Instant};

/// Finite pathname and creation policy for one real native acquisition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectoryAcquireLimits {
    /// Maximum borrowed pathname bytes, before allocation or filesystem work.
    pub max_path_bytes: usize,
    /// Maximum literal components after the absolute root.
    pub max_components: usize,
    /// Maximum directory-creation attempts; zero permits existing directories.
    /// Creating the literal advisory-lock file is a separate fixed phase.
    pub max_created_directories: usize,
}

/// Checked logical ceiling to prepay in the actual composing recovery owner.
/// This creates no admission, directory custody or serving permission. Library,
/// allocator and kernel internals and whole-process RSS remain separate gates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectoryAcquireCost {
    declared: usize,
    entries: usize,
    strings: usize,
    work: usize,
    #[cfg(unix)]
    calls: usize,
    #[cfg(unix)]
    files: usize,
    #[cfg(unix)]
    components: usize,
}
impl DirectoryAcquireCost {
    /// Full source-visible requested buffer/value ceiling, including strings.
    pub const fn declared_bytes(self) -> usize {
        self.declared
    }
    /// Full source-visible allocation/value-slot ceiling.
    pub const fn entries(self) -> usize {
        self.entries
    }
    /// Path and native NUL-conversion subset within declared bytes.
    pub const fn string_bytes(self) -> usize {
        self.strings
    }
    /// Full source-visible scanning, copying, native-phase and cleanup ceiling.
    pub const fn work_bytes(self) -> usize {
        self.work
    }
}

/// Actual cumulative prepaid work, including unused creation-settlement fees.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DirectoryAcquireUsage {
    /// Prepaid native calls; a raced or refused creation may leave unused fees.
    pub calls: usize,
    /// Requested source-visible allocation/value bytes.
    pub declared_bytes: usize,
    /// Path/name subset already included in declared bytes.
    pub string_bytes: usize,
    /// Source-visible slots, including allocated excess capacity.
    pub entries: usize,
    /// Prepaid source-visible work, including entry fees.
    pub work_bytes: usize,
    /// Entered directory-creation attempts, including an actual EEXIST race.
    pub creation_attempts: usize,
    /// Actual successfully created directories, never rolled back by this API.
    pub created_directories: usize,
}

/// Physical or construction phase represented by a retained observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectoryAcquirePhase {
    /// Open a literal directory without following symlinks.
    DirectoryOpened,
    /// Metadata from an actual retained directory descriptor.
    DirectoryMetadata,
    /// Descriptor-relative literal directory creation.
    DirectoryCreated,
    /// Synchronize an actual newly created child directory/file.
    ChildSynced,
    /// Independently settle synchronization of its actual parent directory.
    ParentSynced,
    /// Exclusive creation of the fixed advisory-lock name.
    LockCreated,
    /// Open an existing literal advisory-lock file without following symlinks.
    LockOpened,
    /// Metadata from the actual advisory-lock descriptor.
    LockMetadata,
    /// The actual nonblocking native advisory-lock attempt.
    AdvisoryLock,
    /// Descriptor-relative named lock metadata without following symlinks.
    NamedLockMetadata,
    /// The complete owner was stored before its final deadline observation.
    OwnerCompleted,
}

/// Terminal acquisition refusal. Actual native errors and partial resources
/// remain in the returned error, including an already acquired advisory lock.
#[derive(Debug)]
pub enum DirectoryAcquireFailure {
    /// Path policy, finite-cost arithmetic or original deadline refused.
    Until(DirectoryOwnershipUntilError),
    /// A measured allocation/call/work ceiling refused.
    Capacity,
    /// A real fallible path/vector allocation failed.
    Allocation,
    /// Index of the actual failing native outcome.
    Native(usize),
    /// An opened object or revalidated name has a different identity/type.
    Identity,
    /// An entered phase unwound; actual payloads and settlement stay retained.
    Unwind,
    /// Internal custody consistency refused, without manufacturing an owner.
    Invariant,
}
impl fmt::Display for DirectoryAcquireFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Until(e) => fmt::Display::fmt(e, f),
            Self::Capacity => f.write_str("directory acquisition capacity exceeded"),
            Self::Allocation => f.write_str("directory acquisition allocation failed"),
            Self::Native(i) => write!(f, "directory acquisition native outcome {i} failed"),
            Self::Identity => f.write_str("directory acquisition identity changed"),
            Self::Unwind => f.write_str("directory acquisition phase unwound"),
            Self::Invariant => f.write_str("directory acquisition custody refused"),
        }
    }
}
impl Error for DirectoryAcquireFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Until(e) => Some(e),
            _ => None,
        }
    }
}
impl From<DirectoryOwnershipUntilError> for DirectoryAcquireFailure {
    fn from(e: DirectoryOwnershipUntilError) -> Self {
        Self::Until(e)
    }
}
#[cfg(unix)]
type Result<T> = std::result::Result<T, DirectoryAcquireFailure>;

#[cfg(unix)]
#[path = "directory_acquire_native.rs"]
mod native;

/// Borrow of an actual physical outcome; no descriptor or owner extraction.
pub struct DirectoryAcquireObservation<'a> {
    #[cfg(unix)]
    native: &'a native::Observation,
    #[cfg(not(unix))]
    _unsupported: std::marker::PhantomData<&'a ()>,
}
impl DirectoryAcquireObservation<'_> {
    /// Actual entered operation.
    pub fn phase(&self) -> DirectoryAcquirePhase {
        #[cfg(unix)]
        {
            self.native.phase
        }
        #[cfg(not(unix))]
        {
            unreachable!("unsupported platforms produce no observations")
        }
    }
    /// Actual original native I/O error, including its OS error code.
    pub fn io_error(&self) -> Option<&std::io::Error> {
        #[cfg(unix)]
        {
            self.native.io_error()
        }
        #[cfg(not(unix))]
        {
            None
        }
    }
    /// Whether the actual native operation succeeded, even when it was late.
    pub fn succeeded(&self) -> bool {
        #[cfg(unix)]
        {
            self.native.succeeded()
        }
        #[cfg(not(unix))]
        {
            false
        }
    }
    /// Whether this is an actual nonblocking lock-contention result.
    pub fn contended(&self) -> bool {
        #[cfg(unix)]
        {
            self.native.contended()
        }
        #[cfg(not(unix))]
        {
            false
        }
    }
}

/// A refused acquisition retaining all entered results and partial custody.
/// Dropping this value settles/drops its resources before releasing its lock.
pub struct RetainedDirectoryAcquireError {
    failure: DirectoryAcquireFailure,
    #[cfg(unix)]
    attempt: native::Attempt,
}
impl fmt::Debug for RetainedDirectoryAcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedDirectoryAcquireError")
            .field("failure", &self.failure)
            .field("usage", &self.usage())
            .field("owns_lock", &self.owns_lock())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedDirectoryAcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.failure, f)
    }
}
impl Error for RetainedDirectoryAcquireError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.failure)
    }
}
impl RetainedDirectoryAcquireError {
    /// Terminal category; native outcomes remain separately inspectable.
    pub fn failure(&self) -> &DirectoryAcquireFailure {
        &self.failure
    }
    /// Actual cumulative acquisition fees, without a replacement allowance.
    pub fn usage(&self) -> DirectoryAcquireUsage {
        #[cfg(unix)]
        {
            self.attempt.usage()
        }
        #[cfg(not(unix))]
        {
            DirectoryAcquireUsage::default()
        }
    }
    /// Whether this error still retains the actual acquired advisory lock.
    pub fn owns_lock(&self) -> bool {
        #[cfg(unix)]
        {
            self.attempt.locked()
        }
        #[cfg(not(unix))]
        {
            false
        }
    }
    /// Actual entered outcomes, including ordinary ENOENT/EEXIST observations.
    pub fn observations(&self) -> impl ExactSizeIterator<Item = DirectoryAcquireObservation<'_>> {
        #[cfg(unix)]
        {
            self.attempt
                .observations()
                .iter()
                .map(|native| DirectoryAcquireObservation { native })
        }
        #[cfg(not(unix))]
        {
            std::iter::empty()
        }
    }
    /// Actual fallible allocation error, when one was returned.
    pub fn allocation_error(&self) -> Option<&std::collections::TryReserveError> {
        #[cfg(unix)]
        {
            self.attempt.allocation_error()
        }
        #[cfg(not(unix))]
        {
            None
        }
    }
    /// Original native/observer panic plus independent settlement panic payloads.
    pub fn panic_payloads(&self) -> impl Iterator<Item = &(dyn std::any::Any + Send)> {
        #[cfg(unix)]
        {
            self.attempt.panic_payloads()
        }
        #[cfg(not(unix))]
        {
            std::iter::empty()
        }
    }
}

impl DirectoryOwnership {
    /// Derive finite source-visible acquisition costs without allocation or I/O.
    /// The additive API accepts only normalized absolute literal paths: no
    /// repeated/trailing separators, dot/dot-dot components, NUL or aliases.
    /// Ordinary acquisition retains its existing pathname behavior.
    ///
    /// # Errors
    /// Returns native unsupported/path-policy refusal or checked capacity refusal.
    pub fn directory_acquire_cost(
        directory: &Path,
        limits: DirectoryAcquireLimits,
    ) -> std::result::Result<DirectoryAcquireCost, DirectoryOwnershipUntilError> {
        #[cfg(unix)]
        {
            native::cost(directory, limits)
        }
        #[cfg(not(unix))]
        {
            let _ = (directory, limits);
            Err(DirectoryOwnershipError::Unsupported.into())
        }
    }

    /// Acquire actual directory custody using the caller's original E.
    ///
    /// Prepay `directory_acquire_cost` in the actual composing recovery owner.
    /// Each physical phase spends only that acquisition's finite ceiling; it
    /// does not replenish the subsequent child-operation budget. Creation
    /// prepays child-open/sync and independent parent-sync settlement, which
    /// physically completes through expiry/unwind before return. No created
    /// path is rolled back. The lock attempt is nonblocking and each component
    /// is opened descriptor-relatively with NOFOLLOW. Revalidation compares
    /// the actual first and final descriptor walks and exact named lock inode.
    ///
    /// # Errors
    /// A returned error retains actual files, results, partial creations and
    /// any acquired lock through its destruction. It grants no serving authority.
    #[allow(
        clippy::result_large_err,
        reason = "retain actual partial acquisition inline"
    )]
    pub fn acquire_until(
        directory: &Path,
        limits: DirectoryAcquireLimits,
        original_expiry: Instant,
    ) -> std::result::Result<Arc<Self>, RetainedDirectoryAcquireError> {
        #[cfg(unix)]
        {
            native::acquire(directory, limits, original_expiry, |_, _| {})
        }
        #[cfg(not(unix))]
        {
            let _ = (directory, limits, original_expiry);
            Err(RetainedDirectoryAcquireError {
                failure: DirectoryAcquireFailure::Until(
                    DirectoryOwnershipError::Unsupported.into(),
                ),
            })
        }
    }
}
