use std::os::unix::fs::{MetadataExt, symlink};
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "shard-stream-owner-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("temporary directory");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn directory_ownership_contends_until_last_arc_and_reuses_the_lock_inode() {
    let directory = TempDir::new("contention");
    let owner = DirectoryOwnership::acquire(&directory.0).expect("first owner");
    let lock = fs::metadata(directory.0.join(LOCK_NAME)).expect("lock metadata");
    let escaped = Arc::clone(&owner);
    assert!(matches!(
        DirectoryOwnership::acquire(&directory.0),
        Err(DirectoryOwnershipError::Contended)
    ));
    drop(owner);
    assert!(matches!(
        DirectoryOwnership::acquire(&directory.0),
        Err(DirectoryOwnershipError::Contended)
    ));
    escaped
        .validate_directory(&directory.0.join("."))
        .expect("same directory");
    drop(escaped);
    let next = DirectoryOwnership::acquire(&directory.0).expect("next owner");
    let retained = fs::metadata(directory.0.join(LOCK_NAME)).expect("retained lock");
    assert_eq!((lock.dev(), lock.ino()), (retained.dev(), retained.ino()));
    assert_eq!(retained.len(), 0);
    next.validate_directory(&directory.0)
        .expect("unchanged directory");
}

#[test]
fn directory_ownership_refuses_wrong_and_replaced_directories() {
    let root = TempDir::new("directory-replacement");
    let directory = root.0.join("owned");
    let owner = DirectoryOwnership::acquire(&directory).expect("owner");
    assert!(matches!(
        owner.validate_directory(&root.0),
        Err(DirectoryOwnershipError::IdentityMismatch)
    ));
    fs::rename(&directory, root.0.join("retired")).expect("move original directory");
    fs::create_dir(&directory).expect("replacement directory");
    assert!(matches!(
        owner.validate_directory(&directory),
        Err(DirectoryOwnershipError::IdentityMismatch)
    ));
    assert!(!directory.join(LOCK_NAME).exists());
}

#[test]
fn directory_ownership_refuses_directory_and_lock_symlinks() {
    let root = TempDir::new("symlinks");
    let target = root.0.join("target");
    fs::create_dir(&target).expect("target");
    let linked = root.0.join("linked");
    symlink(&target, &linked).expect("directory symlink");
    assert!(matches!(
        DirectoryOwnership::acquire(&linked),
        Err(DirectoryOwnershipError::IdentityMismatch)
    ));
    assert!(!target.join(LOCK_NAME).exists());
    fs::create_dir(target.join("child")).expect("nested target");
    assert!(matches!(
        DirectoryOwnership::acquire(&linked.join("child")),
        Err(DirectoryOwnershipError::IdentityMismatch)
    ));
    assert!(!target.join("child").join(LOCK_NAME).exists());
    assert!(matches!(
        DirectoryOwnership::acquire(&linked.join("absent")),
        Err(DirectoryOwnershipError::IdentityMismatch)
    ));
    assert!(!target.join("absent").exists());
    let regular = root.0.join("regular");
    fs::write(&regular, b"unchanged").expect("regular file");
    symlink(&regular, target.join(LOCK_NAME)).expect("lock symlink");
    assert!(matches!(
        DirectoryOwnership::acquire(&target),
        Err(DirectoryOwnershipError::IdentityMismatch)
    ));
    assert_eq!(fs::read(regular).expect("regular content"), b"unchanged");
}

#[test]
fn directory_ownership_detects_lock_replacement_and_rejects_hardlinks() {
    let directory = TempDir::new("lock-replacement");
    let owner = DirectoryOwnership::acquire(&directory.0).expect("owner");
    let lock = directory.0.join(LOCK_NAME);
    fs::rename(&lock, directory.0.join("retired-lock")).expect("retain original inode");
    fs::write(&lock, b"").expect("replacement lock");
    assert!(matches!(
        owner.validate_directory(&directory.0),
        Err(DirectoryOwnershipError::IdentityMismatch)
    ));
    fs::hard_link(&lock, directory.0.join("linked-lock")).expect("hard link");
    assert!(matches!(
        DirectoryOwnership::acquire(&directory.0),
        Err(DirectoryOwnershipError::IdentityMismatch)
    ));
}
