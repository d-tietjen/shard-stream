//! Descriptor-relative storage handles retaining the original directory owner.

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use rustix::fs::{AtFlags, Dir, Mode, OFlags, fstat, linkat, mkdirat, openat, statat, unlinkat};

use crate::{DirectoryOwnership, DirectoryOwnershipError};

type Result<T> = std::result::Result<T, DirectoryOwnershipError>;

#[path = "owned_directory_until.rs"]
pub(crate) mod operation_until;
pub use operation_until::{NativeDirectoryProgress, OwnedEntryKind, RetainedDirectoryError};

#[path = "owned_file_replacement.rs"]
mod file_replacement;
pub use file_replacement::{
    NativeFileIdentity, NativeFileReplacement, NativeFileReplacementCost,
    NativeFileReplacementFailure, NativeFileReplacementLimits, NativeFileReplacementOutcome,
    NativeFileReplacementPhase, RetainedNativeFileReplacementError, native_file_replacement_cost,
};

#[path = "owned_shard_directory.rs"]
mod shard_directory;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use shard_directory::{
    OwnedPinnedShardAppendComplete, OwnedPinnedShardAppendCost, OwnedPinnedShardAppendFailure,
    OwnedPinnedShardAppendLimits, OwnedPinnedShardAppendObservation, OwnedPinnedShardAppendReport,
    RetainedOwnedPinnedShardAppendError, owned_pinned_shard_append_cost,
};
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use shard_directory::{
    OwnedPinnedShardGcComplete, OwnedPinnedShardGcCost, OwnedPinnedShardGcFailure,
    OwnedPinnedShardGcReport, RetainedOwnedPinnedShardGcError, owned_pinned_shard_gc_cost,
};
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use shard_directory::{
    OwnedPinnedShardStore, OwnedPinnedShardStoreCost, OwnedPinnedShardStoreFailure,
    OwnedShardStoreDirectory, RetainedOwnedPinnedShardStoreError, owned_pinned_shard_store_cost,
};
pub use shard_directory::{
    OwnedShardDirectory, OwnedShardDirectoryCost, OwnedShardDirectoryFailure,
    RetainedOwnedShardDirectoryError, owned_shard_directory_cost,
};

/// Native file opening modes; none silently truncates an existing file.
#[derive(Clone, Copy, Debug)]
pub enum OwnedFileAccess {
    /// Read an existing regular file.
    ReadOnly,
    /// Read/write an existing regular file.
    ReadWrite,
    /// Read/write, creating the literal file only when absent.
    ReadWriteCreate,
    /// Create a new read/write file, refusing an existing name.
    CreateNew,
}

struct Directory {
    file: File,
    path: PathBuf,
    parent: Option<(OwnedDirectoryHandle, OsString)>,
    // Every derived file/directory/iterator retains the SAME allocation last.
    owner: Arc<DirectoryOwnership>,
}

/// Opaque opened directory derived only from a live original owner.
///
/// There is no constructor accepting an arbitrary file plus a claimed owner.
/// Components are opened with `openat` and `NOFOLLOW`, retaining their real FDs.
pub struct OwnedDirectoryHandle {
    inner: Arc<Directory>,
}

/// Opaque regular file retaining its opened parent and original owner.
pub struct OwnedFileHandle {
    file: File,
    name: OsString,
    parent: OwnedDirectoryHandle,
}

/// One native directory stream retaining its actual directory and owner.
///
/// Callers must admit the native directory-buffer and one filesystem name
/// before advancing, and impose their own finite entry/byte/deadline caps.
pub struct OwnedDirectoryEntries {
    entries: Dir,
    _parent: OwnedDirectoryHandle,
}

pub(crate) fn from_root(
    file: File,
    path: PathBuf,
    owner: Arc<DirectoryOwnership>,
) -> Result<OwnedDirectoryHandle> {
    let result = OwnedDirectoryHandle {
        inner: Arc::new(Directory {
            file,
            path,
            parent: None,
            owner,
        }),
    };
    result.validate()?;
    Ok(result)
}

impl OwnedDirectoryHandle {
    /// Retains this exact opened object without reopening its pathname.
    pub fn retain(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Diagnostic path; actual I/O uses the retained descriptor.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Actual same owner allocation, without any history or serving authority.
    pub fn owner(&self) -> &Arc<DirectoryOwnership> {
        &self.inner.owner
    }

    /// Revalidates the actual root lock and the opened directory's current name.
    pub fn validate(&self) -> Result<()> {
        if let Some((parent, name)) = &self.inner.parent {
            parent.validate()?;
            return validate_entry(&parent.inner.file, name, &self.inner.file, true);
        }
        // The original root API checks its own retained lock/root identities.
        self.inner.owner.validate_retained_root()?;
        let observed = std::fs::symlink_metadata(&self.inner.path)?;
        let actual = self.inner.file.metadata()?;
        if !observed.is_dir()
            || observed.file_type().is_symlink()
            || observed.dev() != actual.dev()
            || observed.ino() != actual.ino()
        {
            return Err(DirectoryOwnershipError::IdentityMismatch);
        }
        Ok(())
    }

    /// Opens one literal child directory without following a symlink.
    ///
    /// Creating a missing child is explicit; this does not acquire a new root
    /// owner or replace the original clock. Callers retain their own deadline.
    pub fn open_directory(&self, name: &OsStr, create: bool) -> Result<Self> {
        component(name)?;
        self.validate()?;
        if create {
            match mkdirat(&self.inner.file, name, Mode::RWXU) {
                Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                Err(error) => return Err(io::Error::from(error).into()),
            }
        }
        let fd = openat(
            &self.inner.file,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io::Error::from)?;
        let file = File::from(fd);
        validate_entry(&self.inner.file, name, &file, true)?;
        let result = Self {
            inner: Arc::new(Directory {
                file,
                path: self.inner.path.join(name),
                parent: Some((self.retain(), name.to_owned())),
                owner: Arc::clone(&self.inner.owner),
            }),
        };
        self.validate()?;
        result.validate()?;
        Ok(result)
    }

    /// Opens one literal regular child, retaining its original parent/owner.
    pub fn open_file(&self, name: &OsStr, access: OwnedFileAccess) -> Result<OwnedFileHandle> {
        component(name)?;
        self.validate()?;
        let flags = match access {
            OwnedFileAccess::ReadOnly => OFlags::RDONLY,
            OwnedFileAccess::ReadWrite => OFlags::RDWR,
            OwnedFileAccess::ReadWriteCreate => OFlags::RDWR | OFlags::CREATE,
            OwnedFileAccess::CreateNew => OFlags::RDWR | OFlags::CREATE | OFlags::EXCL,
        };
        let fd = openat(
            &self.inner.file,
            name,
            flags | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(io::Error::from)?;
        let file = File::from(fd);
        validate_entry(&self.inner.file, name, &file, false)?;
        self.validate()?;
        Ok(OwnedFileHandle {
            file,
            name: name.to_owned(),
            parent: self.retain(),
        })
    }

    /// Durably synchronizes this retained directory without reopening its path.
    ///
    /// Callers admit the physical operation and check their original expiry;
    /// this primitive supplies no timeout, authority or ownership reconstruction.
    pub fn sync(&self) -> Result<()> {
        self.validate()?;
        self.inner.file.sync_all()?;
        self.validate()
    }

    /// Publishes one already-written file in this SAME actual opened parent.
    ///
    /// The descriptor-relative hard link is atomic and never replaces an
    /// existing destination. The caller must preadmit names/I/O and retain its
    /// original deadline. After the final link is validated and synchronized,
    /// only this exact known temporary name is unlinked and the SAME directory
    /// synchronized again. Success leaves one link; the caller must also close
    /// its temporary descriptor before ordinary GC can reclaim that inode.
    /// An error can follow publication or unlink and must retain truthful
    /// alternatives/custody. No unknown, source or final name is removed. This
    /// supplies storage custody only, without validating file contents.
    pub fn publish_new_file(&self, source: &OwnedFileHandle, name: &OsStr) -> Result<()> {
        component(name)?;
        self.validate()?;
        source.validate()?;
        if !Arc::ptr_eq(&self.inner, &source.parent.inner) {
            return Err(DirectoryOwnershipError::IdentityMismatch);
        }
        source.file.sync_all()?;
        linkat(
            &self.inner.file,
            &source.name,
            &self.inner.file,
            name,
            AtFlags::empty(),
        )
        .map_err(io::Error::from)?;
        validate_entry(&self.inner.file, name, &source.file, false)?;
        self.inner.file.sync_all()?;
        source.validate()?;
        validate_entry(&self.inner.file, name, &source.file, false)?;
        // The same validated parent/name still names this exact temporary FD.
        // Unlink no caller-supplied arbitrary path and never the final name.
        unlinkat(&self.inner.file, &source.name, AtFlags::empty()).map_err(io::Error::from)?;
        self.inner.file.sync_all()?;
        validate_entry(&self.inner.file, name, &source.file, false)?;
        self.validate()
    }

    /// Publishes under the caller's original absolute recovery expiry.
    ///
    /// Identity and no-replacement policy match `publish_new_file`. Each later
    /// sync/unlink phase checks the same E. A late error can retain both links
    /// or an already-published final link; callers must retain truthful custody.
    pub fn publish_new_file_until(
        &self,
        source: &OwnedFileHandle,
        name: &OsStr,
        expiry: std::time::Instant,
    ) -> Result<()> {
        publication_clock(expiry)?;
        component(name)?;
        publication_clock(expiry)?;
        publication_clock(expiry)?;
        self.validate()?;
        publication_clock(expiry)?;
        source.validate()?;
        if !Arc::ptr_eq(&self.inner, &source.parent.inner) {
            return Err(DirectoryOwnershipError::IdentityMismatch);
        }
        publication_clock(expiry)?;
        source.file.sync_all()?;
        publication_clock(expiry)?;
        linkat(
            &self.inner.file,
            &source.name,
            &self.inner.file,
            name,
            AtFlags::empty(),
        )
        .map_err(io::Error::from)?;
        publication_clock(expiry)?;
        validate_entry(&self.inner.file, name, &source.file, false)?;
        publication_clock(expiry)?;
        self.inner.file.sync_all()?;
        publication_clock(expiry)?;
        source.validate()?;
        publication_clock(expiry)?;
        validate_entry(&self.inner.file, name, &source.file, false)?;
        // The same validated parent/name still names this exact temporary FD.
        // Unlink no caller-supplied arbitrary path and never the final name.
        publication_clock(expiry)?;
        unlinkat(&self.inner.file, &source.name, AtFlags::empty()).map_err(io::Error::from)?;
        publication_clock(expiry)?;
        self.inner.file.sync_all()?;
        publication_clock(expiry)?;
        validate_entry(&self.inner.file, name, &source.file, false)?;
        publication_clock(expiry)?;
        self.validate()?;
        publication_clock(expiry)
    }

    /// Opens a native directory stream against the retained FD.
    pub fn entries(&self) -> Result<OwnedDirectoryEntries> {
        self.validate()?;
        Ok(OwnedDirectoryEntries {
            entries: Dir::read_from(&self.inner.file).map_err(io::Error::from)?,
            _parent: self.retain(),
        })
    }
}

fn publication_clock(expiry: std::time::Instant) -> Result<()> {
    if std::time::Instant::now() >= expiry {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "native recovery original expiry elapsed",
        )
        .into());
    }
    Ok(())
}

impl OwnedFileHandle {
    /// Borrowed original parent path for admission before a diagnostic clone.
    pub fn parent_path(&self) -> &Path {
        self.parent.path()
    }

    /// Borrowed literal file name for admission before any path allocation.
    pub fn name(&self) -> &OsStr {
        &self.name
    }

    /// Actual pinned native file. No ownership is inferred from another path.
    pub fn file(&self) -> &File {
        &self.file
    }

    /// Diagnostic path of this retained object.
    pub fn path(&self) -> PathBuf {
        self.parent.path().join(&self.name)
    }

    /// Actual root owner retained through this file's physical destruction.
    pub fn owner(&self) -> &Arc<DirectoryOwnership> {
        self.parent.owner()
    }

    /// Revalidates root/parent and actual file identity without reopening it.
    pub fn validate(&self) -> Result<()> {
        self.parent.validate()?;
        validate_entry(&self.parent.inner.file, &self.name, &self.file, false)
    }
}

impl Iterator for OwnedDirectoryEntries {
    type Item = Result<OsString>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let entry = match self.entries.next()? {
                Ok(entry) => entry,
                Err(error) => return Some(Err(io::Error::from(error).into())),
            };
            let name = OsStr::from_bytes(entry.file_name().to_bytes());
            if name != OsStr::new(".") && name != OsStr::new("..") {
                return Some(Ok(name.to_owned()));
            }
        }
    }
}

fn component(name: &OsStr) -> Result<()> {
    let mut parts = Path::new(name).components();
    if !matches!(parts.next(), Some(Component::Normal(_))) || parts.next().is_some() {
        return Err(DirectoryOwnershipError::IdentityMismatch);
    }
    Ok(())
}

fn validate_entry(parent: &File, name: &OsStr, file: &File, directory: bool) -> Result<()> {
    let actual = file.metadata()?;
    // statat/fstat use the SAME native Stat field types on every Unix target;
    // std MetadataExt widens dev_t differently on platforms such as Apple.
    let opened = fstat(file).map_err(io::Error::from)?;
    let named = statat(parent, name, AtFlags::SYMLINK_NOFOLLOW).map_err(io::Error::from)?;
    validate_entry_identity(&actual, &opened, &named, directory)
}

fn validate_entry_identity(
    actual: &std::fs::Metadata,
    opened: &rustix::fs::Stat,
    named: &rustix::fs::Stat,
    directory: bool,
) -> Result<()> {
    if (directory && !actual.is_dir())
        || (!directory && !actual.is_file())
        || named.st_dev != opened.st_dev
        || named.st_ino != opened.st_ino
    {
        return Err(DirectoryOwnershipError::IdentityMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn temporary() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "owned-native-handles-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("temporary directory");
        path
    }

    #[test]
    fn child_file_retains_same_real_owner_until_physical_drop() {
        let path = temporary();
        let owner = DirectoryOwnership::acquire(&path).expect("real root");
        let weak = Arc::downgrade(&owner);
        let root = owner.root_handle().expect("actual root handle");
        let child = root
            .open_directory(OsStr::new("ha-metadata"), true)
            .expect("actual child");
        let file = child
            .open_file(OsStr::new("metadata.ssm"), OwnedFileAccess::CreateNew)
            .expect("actual file");
        assert!(Arc::ptr_eq(file.owner(), &owner));
        assert_eq!(file.name(), OsStr::new("metadata.ssm"));
        assert_eq!(file.parent_path(), path.join("ha-metadata"));
        drop(owner);
        drop(root);
        drop(child);
        assert!(weak.upgrade().is_some());
        assert!(matches!(
            DirectoryOwnership::acquire(&path),
            Err(DirectoryOwnershipError::Contended)
        ));
        file.validate().expect("original inode still pinned");
        drop(file);
        assert!(weak.upgrade().is_none());
        drop(DirectoryOwnership::acquire(&path).expect("owner released"));
        std::fs::remove_dir_all(path).expect("cleanup");
    }

    #[test]
    fn descriptor_children_refuse_symlink_and_parent_escape() {
        let path = temporary();
        let owner = DirectoryOwnership::acquire(&path).expect("root");
        let root = owner.root_handle().expect("root handle");
        std::os::unix::fs::symlink("..", path.join("outside")).expect("symlink");
        assert!(root.open_directory(OsStr::new("outside"), false).is_err());
        assert!(root.open_directory(OsStr::new(".."), false).is_err());
        assert!(
            root.open_file(OsStr::new("outside/file"), OwnedFileAccess::CreateNew)
                .is_err()
        );
        drop(root);
        drop(owner);
        std::fs::remove_dir_all(path).expect("cleanup");
    }

    #[test]
    fn pinned_file_detects_actual_name_replacement() {
        let path = temporary();
        let owner = DirectoryOwnership::acquire(&path).expect("root");
        let root = owner.root_handle().expect("root handle");
        let file = root
            .open_file(OsStr::new("metadata.ssm"), OwnedFileAccess::CreateNew)
            .expect("file");
        std::fs::rename(path.join("metadata.ssm"), path.join("old.ssm")).expect("move original");
        std::fs::write(path.join("metadata.ssm"), b"replacement").expect("replacement");
        assert!(matches!(
            file.validate(),
            Err(DirectoryOwnershipError::IdentityMismatch)
        ));
        drop(file);
        drop(root);
        drop(owner);
        std::fs::remove_dir_all(path).expect("cleanup");
    }

    #[test]
    fn nested_child_refuses_ancestor_symlink_even_when_deepest_inode_is_same() {
        let path = temporary();
        let owner = DirectoryOwnership::acquire(&path).expect("root");
        let root = owner.root_handle().expect("root handle");
        let shards = root
            .open_directory(OsStr::new("shards"), true)
            .expect("shards");
        let child = shards
            .open_directory(OsStr::new("shard-0"), true)
            .expect("nested child");
        let file = child
            .open_file(OsStr::new("pack.sse"), OwnedFileAccess::CreateNew)
            .expect("nested file");
        std::fs::rename(path.join("shards"), path.join("moved-shards"))
            .expect("move actual ancestor");
        std::os::unix::fs::symlink("moved-shards", path.join("shards"))
            .expect("replace ancestor by alias");
        assert!(matches!(
            child.validate(),
            Err(DirectoryOwnershipError::IdentityMismatch)
        ));
        assert!(matches!(
            file.validate(),
            Err(DirectoryOwnershipError::IdentityMismatch)
        ));
        drop(file);
        drop(child);
        drop(shards);
        drop(root);
        drop(owner);
        std::fs::remove_dir_all(path).expect("cleanup");
    }
    #[test]
    fn anchored_publication_is_same_parent_no_replace_and_keeps_actual_custody() {
        use std::io::Write;
        let path = temporary();
        let owner = DirectoryOwnership::acquire(&path).expect("actual OS owner");
        let root = owner.root_handle().expect("opaque anchored root");
        let source = root
            .open_file(OsStr::new(".prepared-copy"), OwnedFileAccess::CreateNew)
            .expect("real new native copy");
        let mut file = source.file();
        file.write_all(b"actual immutable copy")
            .expect("real file write");
        root.publish_new_file(&source, OsStr::new("pack.sse"))
            .expect("actual atomic no-replace publication");
        assert_eq!(
            std::fs::read(path.join("pack.sse")).expect("published bytes"),
            b"actual immutable copy"
        );
        assert!(!path.join(".prepared-copy").exists());
        assert_eq!(
            std::fs::metadata(path.join("pack.sse"))
                .expect("one actual final link")
                .nlink(),
            1
        );
        let other = root
            .open_file(OsStr::new(".conflicting-copy"), OwnedFileAccess::CreateNew)
            .expect("real different copy");
        let mut file = other.file();
        file.write_all(b"different bytes").expect("write conflict");
        assert!(
            root.publish_new_file(&other, OsStr::new("pack.sse"))
                .is_err()
        );
        assert_eq!(
            std::fs::read(path.join("pack.sse")).expect("never overwritten"),
            b"actual immutable copy"
        );
        let child = root
            .open_directory(OsStr::new("child"), true)
            .expect("real other parent");
        other
            .validate()
            .expect("failed conflict retains a valid source name");
        assert!(
            child
                .publish_new_file(&other, OsStr::new("alias.sse"))
                .is_err()
        );
        assert!(!path.join("child/alias.sse").exists());
        drop(owner);
        drop(root);
        assert!(DirectoryOwnership::acquire(&path).is_err());
        drop(child);
        drop(other);
        assert!(DirectoryOwnership::acquire(&path).is_err());
        drop(source);
        DirectoryOwnership::acquire(&path)
            .expect("real final handle drop releases original custody");
        std::fs::remove_dir_all(path).expect("fixture cleanup");
    }
}
