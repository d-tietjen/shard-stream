//! Original-deadline validation of the actual native directory owner.
//!
//! The bounded API requires the exact retained canonical absolute name and opens
//! its real directory components without following symlinks. Ordinary APIs keep
//! their existing path policy.
//! Each explicit filesystem phase uses the caller's same absolute deadline.

use super::{DirectoryOwnership, DirectoryOwnershipError};
use std::{error::Error, fmt, path::Path, time::Instant};

#[cfg(unix)]
use super::{FileIdentity, LOCK_NAME};
#[cfg(unix)]
use rustix::{
    fd::OwnedFd,
    fs::{AtFlags, CWD, FileType, Mode, OFlags, Stat, fstat, openat, statat},
};
#[cfg(unix)]
use std::{
    fs, io,
    mem::size_of,
    path::{Component, Components},
};

/// A deadline or native refusal from bounded directory validation.
///
/// The existing exhaustive `DirectoryOwnershipError` remains unchanged.
#[derive(Debug)]
pub enum DirectoryOwnershipUntilError {
    /// The supplied absolute deadline elapsed; no later explicit phase starts.
    Expired,
    /// Checked arithmetic cannot represent the ordinary native cost facts.
    Capacity,
    /// The canonical-name, native identity or filesystem checks refused.
    Ownership(DirectoryOwnershipError),
}

impl fmt::Display for DirectoryOwnershipUntilError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Expired => formatter.write_str("directory validation deadline elapsed"),
            Self::Capacity => formatter.write_str("directory validation cost overflow"),
            Self::Ownership(error) => fmt::Display::fmt(error, formatter),
        }
    }
}

impl Error for DirectoryOwnershipUntilError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Ownership(error) => Some(error),
            _ => None,
        }
    }
}

impl From<DirectoryOwnershipError> for DirectoryOwnershipUntilError {
    fn from(error: DirectoryOwnershipError) -> Self {
        Self::Ownership(error)
    }
}

/// Ordinary conservative logical costs before native directory validation.
///
/// These facts allocate nothing and grant no resource admission or authority.
/// A composing recovery owner must charge them before calling the validator.
/// Declared bytes include the String-axis subset; work includes the entry work.
/// They describe explicit native buffers/phases, not allocator or kernel internals.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectoryValidationCost {
    declared_bytes: usize,
    entries: usize,
    string_bytes: usize,
    work_bytes: usize,
}

impl DirectoryValidationCost {
    /// Complete logical native buffer reservation, including string bytes.
    pub const fn declared_bytes(self) -> usize {
        self.declared_bytes
    }
    /// Conservative explicit native phase and local buffer slots.
    pub const fn entries(self) -> usize {
        self.entries
    }
    /// Path-buffer subset already included in declared bytes.
    pub const fn string_bytes(self) -> usize {
        self.string_bytes
    }
    /// Complete logical native path, metadata and local-value work.
    pub const fn work_bytes(self) -> usize {
        self.work_bytes
    }
}

#[cfg(unix)]
fn add(a: usize, b: usize) -> Result<usize, DirectoryOwnershipUntilError> {
    a.checked_add(b)
        .ok_or(DirectoryOwnershipUntilError::Capacity)
}

#[cfg(unix)]
fn mul(a: usize, b: usize) -> Result<usize, DirectoryOwnershipUntilError> {
    a.checked_mul(b)
        .ok_or(DirectoryOwnershipUntilError::Capacity)
}

fn clock(expiry: Instant) -> Result<(), DirectoryOwnershipUntilError> {
    if Instant::now() >= expiry {
        return Err(DirectoryOwnershipUntilError::Expired);
    }
    Ok(())
}

// Both entries use the same native phases. The original entry keeps its
// deadline-first error precedence; carried work preserves a completed native
// failure in its same budget before the post-call deadline observation.
trait ValidationChecks {
    fn check(&self) -> Result<(), DirectoryOwnershipUntilError>;
    fn completed<T>(
        &self,
        observed: Result<T, DirectoryOwnershipError>,
    ) -> Result<T, DirectoryOwnershipUntilError>;
    fn completed_optional<T>(
        &self,
        observed: Result<T, DirectoryOwnershipError>,
    ) -> Result<Option<T>, DirectoryOwnershipUntilError>;
}

struct OriginalChecks(Instant);
impl ValidationChecks for OriginalChecks {
    fn check(&self) -> Result<(), DirectoryOwnershipUntilError> {
        clock(self.0)
    }
    fn completed<T>(
        &self,
        observed: Result<T, DirectoryOwnershipError>,
    ) -> Result<T, DirectoryOwnershipUntilError> {
        self.check()?;
        observed.map_err(Into::into)
    }
    fn completed_optional<T>(
        &self,
        observed: Result<T, DirectoryOwnershipError>,
    ) -> Result<Option<T>, DirectoryOwnershipUntilError> {
        self.check()?;
        match observed {
            Err(DirectoryOwnershipError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(None)
            }
            observed => observed.map(Some).map_err(Into::into),
        }
    }
}

#[cfg(unix)]
struct BudgetChecks<'a> {
    budget: &'a crate::NativeDirectoryBudget,
    refusal: std::cell::RefCell<Option<crate::NativeDirectoryFailure>>,
}
#[cfg(unix)]
impl BudgetChecks<'_> {
    fn retain<T>(
        &self,
        observed: Result<T, crate::NativeDirectoryFailure>,
    ) -> Result<T, DirectoryOwnershipUntilError> {
        observed.map_err(|error| {
            *self.refusal.borrow_mut() = Some(error);
            DirectoryOwnershipUntilError::Capacity
        })
    }
}
#[cfg(unix)]
impl ValidationChecks for BudgetChecks<'_> {
    fn check(&self) -> Result<(), DirectoryOwnershipUntilError> {
        self.retain(self.budget.check())
    }
    fn completed<T>(
        &self,
        observed: Result<T, DirectoryOwnershipError>,
    ) -> Result<T, DirectoryOwnershipUntilError> {
        self.retain(self.budget.completed(observed))
    }
    fn completed_optional<T>(
        &self,
        observed: Result<T, DirectoryOwnershipError>,
    ) -> Result<Option<T>, DirectoryOwnershipUntilError> {
        self.retain(self.budget.completed_optional(observed))
    }
}

#[cfg(unix)]
macro_rules! completed {
    ($owner:expr, $phase:ident) => {
        #[cfg(test)]
        tests::completed($owner, tests::Phase::$phase);
        #[cfg(test)]
        crate::owned_directory_handle::operation_until::completed_native_root_phase(stringify!(
            $phase
        ));
    };
}

impl DirectoryOwnership {
    /// Derive ordinary costs from the borrowed path and this actual native owner.
    ///
    /// Uses checked scalar arithmetic only, without filesystem calls, allocation,
    /// a path clone or an ancestor walk. The path's byte length plus one bounds
    /// its ancestor and component counts, including the root. Only the exact
    /// retained canonical absolute name is accepted. No OS-derived pathname is
    /// allocated; the facts include bounded component/NUL conversion scratch,
    /// two simultaneous transient descriptors, native results and cleanup work.
    ///
    /// # Errors
    /// Returns `Ownership(IdentityMismatch)` for any other spelling, `Capacity`
    /// on arithmetic overflow and the native `Unsupported` refusal on platforms
    /// without the original ownership implementation.
    pub fn directory_validation_cost(
        &self,
        directory: &Path,
    ) -> Result<DirectoryValidationCost, DirectoryOwnershipUntilError> {
        #[cfg(not(unix))]
        {
            let _ = directory;
            Err(DirectoryOwnershipError::Unsupported.into())
        }
        #[cfg(unix)]
        {
            if !directory.is_absolute()
                || directory.as_os_str() != self.canonical_directory.as_os_str()
            {
                return Err(DirectoryOwnershipError::IdentityMismatch.into());
            }
            let path = directory.as_os_str().as_encoded_bytes().len();
            let canonical = self
                .canonical_directory
                .as_os_str()
                .as_encoded_bytes()
                .len();
            let ancestors = add(path, 1)?;
            // Borrowed input/retained name, component conversions with NUL and
            // the literal lock name. No canonicalize result can grow with CWD,
            // renames or newly introduced symlinks.
            let string_bytes = add(
                add(mul(add(path, 1)?, 3)?, canonical)?,
                add(LOCK_NAME.len(), 1)?,
            )?;
            let mut fixed = 0usize;
            for bytes in [
                size_of::<[fs::Metadata; 4]>(),
                size_of::<[Stat; 4]>(),
                size_of::<[OwnedFd; 2]>(),
                size_of::<[Result<OwnedFd, rustix::io::Errno>; 2]>(),
                size_of::<Components<'_>>(),
                size_of::<[DirectoryOwnershipUntilError; 3]>(),
                size_of::<DirectoryValidationCost>(),
                size_of::<Instant>(),
            ] {
                fixed = add(fixed, bytes)?;
            }
            let declared_bytes = add(string_bytes, fixed)?;
            // Per-ancestor metadata, per-component opens and descriptor closes,
            // plus root open, initial metadata, four native stats and cleanup.
            let calls = add(mul(ancestors, 3)?, 8)?;
            let entries = add(calls, 5)?;
            let work_bytes = add(
                add(
                    declared_bytes,
                    mul(
                        calls,
                        add(
                            add(path, 1)?,
                            size_of::<fs::Metadata>().max(size_of::<Stat>()),
                        )?,
                    )?,
                )?,
                mul(canonical, 2)?,
            )?;
            Ok(DirectoryValidationCost {
                declared_bytes,
                entries,
                string_bytes,
                work_bytes,
            })
        }
    }

    /// Revalidate the exact retained canonical name under the original deadline.
    ///
    /// The input must be byte-for-byte equal to the retained canonical absolute
    /// name; relative names and alternate spellings fail before filesystem I/O.
    /// Every directory component must permit read access for the descriptor walk.
    /// Each component is opened with `DIRECTORY | NOFOLLOW`; final directory and
    /// lock identities are checked against this owner's actual retained FDs.
    /// No arbitrary canonical path is allocated. Unlike the ordinary API this
    /// deliberately narrower API supplies no alias normalization.
    ///
    /// Checks the original deadline before and after each metadata/open/stat
    /// phase. An entered synchronous
    /// filesystem call may complete late; no later phase starts in that case.
    /// The borrowed owner retains its actual directory and advisory lock through
    /// completion. This creates neither a new owner nor permission to serve.
    /// Callers precharge `directory_validation_cost` to their same resource owner.
    ///
    /// # Errors
    /// Returns `Expired` on the supplied deadline, `Capacity` on cost overflow,
    /// or `Ownership` for canonical-name, native identity or filesystem refusal.
    pub fn validate_directory_until(
        &self,
        directory: &Path,
        original_expiry: Instant,
    ) -> Result<(), DirectoryOwnershipUntilError> {
        self.validate_directory_checked(directory, &OriginalChecks(original_expiry))
    }

    #[cfg(unix)]
    pub(crate) fn validate_directory_with_budget(
        &self,
        directory: &Path,
        budget: &crate::NativeDirectoryBudget,
    ) -> Result<(), crate::NativeDirectoryFailure> {
        // Keep the genuine native failure type even though the shared original
        // validator has the existing public error type. No native policy or
        // physical operation is copied into this adapter.
        let bytes = size_of::<BudgetChecks<'_>>();
        budget.charge(0, 0, bytes, 0, 1, bytes)?;
        let checks = BudgetChecks {
            budget,
            refusal: std::cell::RefCell::new(None),
        };
        let observed = self.validate_directory_checked(directory, &checks);
        let observed = match checks.refusal.into_inner() {
            Some(error) => Err(error),
            None => observed.map_err(crate::NativeDirectoryFailure::from),
        };
        budget.finish(observed)
    }

    fn validate_directory_checked(
        &self,
        directory: &Path,
        check: &impl ValidationChecks,
    ) -> Result<(), DirectoryOwnershipUntilError> {
        check.check()?;
        self.directory_validation_cost(directory)?;
        #[cfg(not(unix))]
        {
            Err(DirectoryOwnershipError::Unsupported.into())
        }
        #[cfg(unix)]
        {
            check.check()?;
            let metadata = fs::symlink_metadata(directory);
            completed!(self, PathMetadata);
            let metadata = check.completed(metadata.map_err(DirectoryOwnershipError::from))?;
            for ancestor in directory
                .ancestors()
                .filter(|path| !path.as_os_str().is_empty())
            {
                check.check()?;
                let observed = fs::symlink_metadata(ancestor);
                completed!(self, AncestorMetadata);
                let observed = match check
                    .completed_optional(observed.map_err(DirectoryOwnershipError::from))?
                {
                    Some(value) => value,
                    None => continue,
                };
                if !observed.is_dir() || observed.file_type().is_symlink() {
                    return Err(DirectoryOwnershipError::IdentityMismatch.into());
                }
            }
            completed!(self, AncestorsValidated);
            check.check()?;
            if !metadata.is_dir()
                || metadata.file_type().is_symlink()
                || FileIdentity::of(&metadata) != self.directory_identity
            {
                return Err(DirectoryOwnershipError::IdentityMismatch.into());
            }
            check.check()?;
            let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let root = openat(CWD, "/", flags, Mode::empty());
            completed!(self, DirectoryOpened);
            let mut resolved = check.completed(root.map_err(native_ownership_error))?;
            for component in directory.components() {
                let name = match component {
                    Component::RootDir => continue,
                    Component::Normal(name) => name,
                    _ => return Err(DirectoryOwnershipError::IdentityMismatch.into()),
                };
                check.check()?;
                let next = openat(&resolved, name, flags, Mode::empty());
                completed!(self, DirectoryOpened);
                // The parent remains open through the actual child operation.
                // On a late/error return both temporary descriptors are cleaned
                // up while the borrowed original owner remains held.
                resolved = check.completed(next.map_err(native_ownership_error))?;
                check.check()?;
            }
            check.check()?;
            let opened = fstat(&resolved);
            completed!(self, OpenedMetadata);
            let opened = check.completed(opened.map_err(native_ownership_error))?;
            check.check()?;
            let retained = fstat(&self._directory);
            completed!(self, RetainedDirectoryMetadata);
            let retained = check.completed(retained.map_err(native_ownership_error))?;
            // Both values use the SAME native stat types on every Unix target.
            if FileType::from_raw_mode(opened.st_mode) != FileType::Directory
                || opened.st_dev != retained.st_dev
                || opened.st_ino != retained.st_ino
            {
                return Err(DirectoryOwnershipError::IdentityMismatch.into());
            }
            check.check()?;
            let named = statat(&resolved, LOCK_NAME, AtFlags::SYMLINK_NOFOLLOW);
            completed!(self, LockMetadata);
            let named = check.completed(named.map_err(native_ownership_error))?;
            check.check()?;
            let retained = fstat(&self._lock);
            completed!(self, RetainedLockMetadata);
            let retained = check.completed(retained.map_err(native_ownership_error))?;
            if FileType::from_raw_mode(named.st_mode) != FileType::RegularFile
                || named.st_nlink != 1
                || retained.st_nlink != 1
                || named.st_dev != retained.st_dev
                || named.st_ino != retained.st_ino
            {
                return Err(DirectoryOwnershipError::IdentityMismatch.into());
            }
            check.check()
        }
    }
}

#[cfg(unix)]
fn native_ownership_error(error: rustix::io::Errno) -> DirectoryOwnershipError {
    DirectoryOwnershipError::from(io::Error::from(error))
}

#[cfg(all(test, unix))]
#[path = "directory_validation_until_tests.rs"]
mod tests;
