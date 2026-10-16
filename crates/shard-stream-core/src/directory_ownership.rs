//! Exclusive, cooperating-process custody of an engine's local directory.

#[cfg(unix)]
#[path = "directory_root_until.rs"]
mod root_until;

#[path = "directory_validation_until.rs"]
mod validation_until;
pub use validation_until::{DirectoryOwnershipUntilError, DirectoryValidationCost};

use std::error::Error;
use std::fmt;
use std::io;
use std::path::Path;
use std::sync::Arc;

#[cfg(unix)]
use std::fs::{self, File, OpenOptions, TryLockError};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::path::PathBuf;

#[cfg(unix)]
const LOCK_NAME: &str = ".shard-stream.directory.lock";

/// A sealed, live operating-system lock for one local engine directory.
///
/// Cloning its `Arc` retains custody. The lock file is never removed, replaced
/// or truncated by this type, including on destruction. This is advisory
/// exclusion between cooperating owners, not process identity, authorization,
/// recovery lineage or protection against an actor replacing filesystem paths.
/// The owned API currently requires Unix file identities; other platforms fail
/// closed without changing engines opened through the existing constructors.
pub struct DirectoryOwnership {
    #[cfg(not(unix))]
    _unsupported: (),
    #[cfg(unix)]
    canonical_directory: PathBuf,
    #[cfg(unix)]
    directory_identity: FileIdentity,
    #[cfg(unix)]
    lock_identity: FileIdentity,
    // Keep both opened objects alive. The lock is released only with the last
    // Arc, after the composing owner has physically destroyed its resources.
    #[cfg(unix)]
    _directory: File,
    #[cfg(unix)]
    _lock: File,
}

impl fmt::Debug for DirectoryOwnership {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectoryOwnership")
            .finish_non_exhaustive()
    }
}

/// Failure to establish or revalidate exclusive directory custody.
#[derive(Debug)]
pub enum DirectoryOwnershipError {
    /// Another live file handle owns the directory's advisory lock.
    Contended,
    /// The path is a symlink, has the wrong type, or no longer names the opened objects.
    IdentityMismatch,
    /// This platform does not provide the file-identity checks used by this API.
    Unsupported,
    /// The filesystem could not complete acquisition or validation.
    Io(io::Error),
}

impl fmt::Display for DirectoryOwnershipError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contended => formatter.write_str("engine directory is already owned"),
            Self::IdentityMismatch => formatter.write_str("engine directory identity changed"),
            Self::Unsupported => {
                formatter.write_str("directory ownership is unsupported on this platform")
            }
            Self::Io(_) => {
                formatter.write_str("engine directory ownership filesystem operation failed")
            }
        }
    }
}

impl Error for DirectoryOwnershipError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for DirectoryOwnershipError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[cfg(unix)]
#[derive(Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
impl FileIdentity {
    fn of(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
}

impl DirectoryOwnership {
    #[cfg(unix)]
    pub(crate) fn validate_retained_root(&self) -> Result<(), DirectoryOwnershipError> {
        self.validate_directory(&self.canonical_directory)
    }

    /// Borrows the actual opened root into a retained descriptor-relative handle.
    ///
    /// Child operations retain this SAME allocation, not a path-labelled owner.
    /// This grants storage custody only, never process or write permission.
    #[cfg(unix)]
    pub fn root_handle(
        self: &Arc<Self>,
    ) -> Result<crate::OwnedDirectoryHandle, DirectoryOwnershipError> {
        self.validate_directory(&self.canonical_directory)?;
        crate::owned_directory_handle::from_root(
            self._directory.try_clone()?,
            self.canonical_directory.clone(),
            Arc::clone(self),
        )
    }

    /// Creates the directory if absent and acquires its nonblocking exclusive lock.
    ///
    /// Acquire this before advancing any process identity or opening mutable
    /// recovery state. The directory and lock must remain at their original
    /// paths for the entire owned lifetime. Existing lock files are reused.
    pub fn acquire(directory: &Path) -> Result<Arc<Self>, DirectoryOwnershipError> {
        #[cfg(not(unix))]
        {
            let _ = directory;
            Err(DirectoryOwnershipError::Unsupported)
        }
        #[cfg(unix)]
        {
            validate_path_components(directory)?;
            match fs::symlink_metadata(directory) {
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                    return Err(DirectoryOwnershipError::IdentityMismatch);
                }
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    fs::create_dir_all(directory)?;
                }
                Err(error) => return Err(error.into()),
            }
            validate_directory_path(directory)?;
            let canonical_directory = fs::canonicalize(directory)?;
            let directory_file = File::open(&canonical_directory)?;
            let metadata = directory_file.metadata()?;
            if !metadata.is_dir() {
                return Err(DirectoryOwnershipError::IdentityMismatch);
            }
            let directory_identity = FileIdentity::of(&metadata);
            let lock_path = canonical_directory.join(LOCK_NAME);
            match fs::symlink_metadata(&lock_path) {
                Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
                    return Err(DirectoryOwnershipError::IdentityMismatch);
                }
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path)?;
            let metadata = lock.metadata()?;
            if !metadata.is_file() || metadata.nlink() != 1 {
                return Err(DirectoryOwnershipError::IdentityMismatch);
            }
            lock.try_lock().map_err(|error| match error {
                TryLockError::WouldBlock => DirectoryOwnershipError::Contended,
                TryLockError::Error(error) => DirectoryOwnershipError::Io(error),
            })?;
            let owner = Arc::new(Self {
                canonical_directory,
                directory_identity,
                lock_identity: FileIdentity::of(&metadata),
                _directory: directory_file,
                _lock: lock,
            });
            owner.validate_directory(directory)?;
            Ok(owner)
        }
    }

    /// Revalidates the exact directory and lock paths before owned storage opens.
    ///
    /// This performs filesystem I/O and belongs on construction/recovery paths,
    /// never a physical-admission or serving callback. Validation does not make
    /// arbitrary subsequent path replacement safe.
    pub fn validate_directory(&self, directory: &Path) -> Result<(), DirectoryOwnershipError> {
        #[cfg(not(unix))]
        {
            let _ = directory;
            Err(DirectoryOwnershipError::Unsupported)
        }
        #[cfg(unix)]
        {
            let metadata = validate_directory_path(directory)?;
            if !metadata.is_dir()
                || metadata.file_type().is_symlink()
                || FileIdentity::of(&metadata) != self.directory_identity
                || fs::canonicalize(directory)? != self.canonical_directory
            {
                return Err(DirectoryOwnershipError::IdentityMismatch);
            }
            let metadata = fs::symlink_metadata(self.canonical_directory.join(LOCK_NAME))?;
            if !metadata.is_file()
                || metadata.file_type().is_symlink()
                || metadata.nlink() != 1
                || FileIdentity::of(&metadata) != self.lock_identity
            {
                return Err(DirectoryOwnershipError::IdentityMismatch);
            }
            Ok(())
        }
    }
}

#[cfg(unix)]
fn validate_directory_path(directory: &Path) -> Result<fs::Metadata, DirectoryOwnershipError> {
    let metadata = fs::symlink_metadata(directory)?;
    validate_path_components(directory)?;
    Ok(metadata)
}

#[cfg(unix)]
fn validate_path_components(directory: &Path) -> Result<(), DirectoryOwnershipError> {
    for ancestor in directory
        .ancestors()
        .filter(|path| !path.as_os_str().is_empty())
    {
        let metadata = match fs::symlink_metadata(ancestor) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(DirectoryOwnershipError::IdentityMismatch);
        }
    }
    Ok(())
}

#[cfg(all(test, unix))]
#[path = "directory_ownership_tests.rs"]
mod tests;
