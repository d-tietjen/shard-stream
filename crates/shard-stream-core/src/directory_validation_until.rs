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

#[cfg(unix)]
macro_rules! completed {
    ($owner:expr, $phase:ident) => {
        #[cfg(test)]
        tests::completed($owner, tests::Phase::$phase);
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
        clock(original_expiry)?;
        self.directory_validation_cost(directory)?;
        #[cfg(not(unix))]
        {
            Err(DirectoryOwnershipError::Unsupported.into())
        }
        #[cfg(unix)]
        {
            clock(original_expiry)?;
            let metadata = fs::symlink_metadata(directory);
            completed!(self, PathMetadata);
            clock(original_expiry)?;
            let metadata = metadata.map_err(DirectoryOwnershipError::from)?;
            for ancestor in directory
                .ancestors()
                .filter(|path| !path.as_os_str().is_empty())
            {
                clock(original_expiry)?;
                let observed = fs::symlink_metadata(ancestor);
                completed!(self, AncestorMetadata);
                clock(original_expiry)?;
                let observed = match observed {
                    Ok(value) => value,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(DirectoryOwnershipError::from(error).into()),
                };
                if !observed.is_dir() || observed.file_type().is_symlink() {
                    return Err(DirectoryOwnershipError::IdentityMismatch.into());
                }
            }
            completed!(self, AncestorsValidated);
            clock(original_expiry)?;
            if !metadata.is_dir()
                || metadata.file_type().is_symlink()
                || FileIdentity::of(&metadata) != self.directory_identity
            {
                return Err(DirectoryOwnershipError::IdentityMismatch.into());
            }
            clock(original_expiry)?;
            let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let root = openat(CWD, "/", flags, Mode::empty());
            completed!(self, DirectoryOpened);
            clock(original_expiry)?;
            let mut resolved = root.map_err(native_error)?;
            for component in directory.components() {
                let name = match component {
                    Component::RootDir => continue,
                    Component::Normal(name) => name,
                    _ => return Err(DirectoryOwnershipError::IdentityMismatch.into()),
                };
                clock(original_expiry)?;
                let next = openat(&resolved, name, flags, Mode::empty());
                completed!(self, DirectoryOpened);
                clock(original_expiry)?;
                // The parent remains open through the actual child operation.
                // On a late/error return both temporary descriptors are cleaned
                // up while the borrowed original owner remains held.
                resolved = next.map_err(native_error)?;
                clock(original_expiry)?;
            }
            clock(original_expiry)?;
            let opened = fstat(&resolved);
            completed!(self, OpenedMetadata);
            clock(original_expiry)?;
            let opened = opened.map_err(native_error)?;
            clock(original_expiry)?;
            let retained = fstat(&self._directory);
            completed!(self, RetainedDirectoryMetadata);
            clock(original_expiry)?;
            let retained = retained.map_err(native_error)?;
            // Both values use the SAME native stat types on every Unix target.
            if FileType::from_raw_mode(opened.st_mode) != FileType::Directory
                || opened.st_dev != retained.st_dev
                || opened.st_ino != retained.st_ino
            {
                return Err(DirectoryOwnershipError::IdentityMismatch.into());
            }
            clock(original_expiry)?;
            let named = statat(&resolved, LOCK_NAME, AtFlags::SYMLINK_NOFOLLOW);
            completed!(self, LockMetadata);
            clock(original_expiry)?;
            let named = named.map_err(native_error)?;
            clock(original_expiry)?;
            let retained = fstat(&self._lock);
            completed!(self, RetainedLockMetadata);
            clock(original_expiry)?;
            let retained = retained.map_err(native_error)?;
            if FileType::from_raw_mode(named.st_mode) != FileType::RegularFile
                || named.st_nlink != 1
                || retained.st_nlink != 1
                || named.st_dev != retained.st_dev
                || named.st_ino != retained.st_ino
            {
                return Err(DirectoryOwnershipError::IdentityMismatch.into());
            }
            clock(original_expiry)
        }
    }
}

#[cfg(unix)]
fn native_error(error: rustix::io::Errno) -> DirectoryOwnershipUntilError {
    DirectoryOwnershipError::from(io::Error::from(error)).into()
}

#[cfg(all(test, unix))]
#[path = "directory_validation_until_tests.rs"]
mod tests;
