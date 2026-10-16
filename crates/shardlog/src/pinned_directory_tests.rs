//! Actual directory/lock identity and retained late/unwind custody.

use super::*;
use std::cell::Cell;
use std::os::unix::fs::symlink;
use std::sync::atomic::{AtomicU64, Ordering};

struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "pinned-shard-directory-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual directory");
        Self(path)
    }
    fn lock_file(&self) -> File {
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.0.join(".shard-store.lock"))
            .expect("actual native lock FD")
    }
    fn acquired(&self) -> AcquiredShardRecoveryGuard {
        ShardRecoveryGuard::try_acquire_until(
            self.lock_file(),
            self.0.clone(),
            ShardGuardAcquireLimits {
                max_path_bytes: 8192,
                max_io_calls: 5,
            },
            expiry(),
        )
        .expect("actual successfully acquired native guard")
    }
    fn directory(&self) -> File {
        File::open(&self.0).expect("actual directory FD")
    }
    fn contended(&self) {
        assert!(ShardRecoveryGuard::try_acquire(self.lock_file(), self.0.clone()).is_err());
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct Checks(Cell<bool>);
impl Checks {
    fn new() -> Self {
        Self(Cell::new(false))
    }
}
impl ShardDirectoryChecks for Checks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        if self.0.get() {
            Err(ShardDirectoryCheckFailure::Refused)
        } else {
            Ok(())
        }
    }
}
fn expiry() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
fn limits() -> PinnedShardDirectoryLimits {
    PinnedShardDirectoryLimits {
        max_path_bytes: 8192,
        max_io_calls: 4,
    }
}

#[test]
fn actual_matching_descriptor_and_acquired_lock_survive_until_last_owner_drop() {
    fn send<T: Send>() {}
    send::<PinnedShardDirectory>();
    send::<PinnedShardDirectoryError>();
    let root = Temporary::new();
    let pinned = PinnedShardDirectory::from_descriptor_until(
        root.directory(),
        root.acquired(),
        &root.0,
        limits(),
        expiry(),
        &Checks::new(),
    )
    .expect("actual matched inputs");
    assert_eq!(pinned.directory(), root.0);
    let actual = pinned.observation();
    assert_eq!((actual.reserved_calls(), actual.entered_calls()), (4, 4));
    assert_eq!(actual.identity_matches(), Some(true));
    let directory = actual
        .directory_metadata()
        .expect("saved FD metadata")
        .as_ref()
        .expect("actual FD");
    let named = actual
        .named_directory_metadata()
        .expect("saved pathname metadata")
        .as_ref()
        .expect("actual name");
    assert_eq!(
        (directory.dev(), directory.ino()),
        (named.dev(), named.ino())
    );
    let lock = actual
        .lock_descriptor_stat()
        .expect("actual fstat")
        .as_ref()
        .expect("actual lock");
    let named_lock = actual
        .named_lock_stat()
        .expect("actual statat")
        .as_ref()
        .expect("actual named lock");
    assert_eq!(
        (lock.st_dev, lock.st_ino),
        (named_lock.st_dev, named_lock.st_ino)
    );
    root.contended();
    drop(pinned);
    ShardRecoveryGuard::try_acquire(root.lock_file(), root.0.clone())
        .expect("real last owner released lock");
}

#[test]
fn wrong_actual_directory_and_substituted_native_lock_are_refused_without_repair() {
    let root = Temporary::new();
    let other = Temporary::new();
    let error = PinnedShardDirectory::from_descriptor_until(
        other.directory(),
        root.acquired(),
        &root.0,
        limits(),
        expiry(),
        &Checks::new(),
    )
    .expect_err("unrelated opened directory");
    assert_eq!(error.failure(), PinnedShardDirectoryFailure::Io);
    assert_eq!(
        error
            .observation()
            .io_error()
            .expect("real missing other lock")
            .kind(),
        io::ErrorKind::NotFound
    );
    root.contended();
    drop(error);
    let guard = root.acquired();
    let old = root.0.join("retained.lock");
    fs::rename(root.0.join(".shard-store.lock"), &old).expect("actual inode rename");
    symlink("retained.lock", root.0.join(".shard-store.lock"))
        .expect("actual symlink substitution");
    let error = PinnedShardDirectory::from_descriptor_until(
        root.directory(),
        guard,
        &root.0,
        limits(),
        expiry(),
        &Checks::new(),
    )
    .expect_err("no-follow native lock identity");
    assert_eq!(error.failure(), PinnedShardDirectoryFailure::Identity);
    assert_eq!(error.observation().identity_matches(), Some(false));
    let old_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&old)
        .expect("actual old inode");
    assert!(fs2::FileExt::try_lock_exclusive(&old_file).is_err());
    assert!(
        fs::symlink_metadata(root.0.join(".shard-store.lock"))
            .expect("name preserved")
            .file_type()
            .is_symlink()
    );
    drop(error);
    fs2::FileExt::try_lock_exclusive(&old_file).expect("old lock releases after retained refusal");
}

#[test]
fn every_completed_phase_keeps_actual_results_on_tighter_checkpoint_or_unwind() {
    for target in [
        Phase::DirectoryMetadata,
        Phase::NamedDirectoryMetadata,
        Phase::LockDescriptorStat,
        Phase::NamedLockStat,
        Phase::Identity,
    ] {
        for unwind in [false, true] {
            let root = Temporary::new();
            let checks = Checks::new();
            let error = PinnedShardDirectory::from_descriptor_observed(
                root.directory(),
                root.acquired(),
                &root.0,
                limits(),
                expiry(),
                &checks,
                |phase| {
                    if phase == target {
                        if unwind {
                            std::panic::panic_any(target);
                        }
                        checks.0.set(true);
                    }
                },
            )
            .expect_err("actual completed phase refuses");
            assert_eq!(
                error.failure(),
                if unwind {
                    PinnedShardDirectoryFailure::Unwind
                } else {
                    PinnedShardDirectoryFailure::Checkpoint
                }
            );
            let actual = error.observation();
            assert!(matches!(actual.directory_metadata(), Some(Ok(_))));
            let calls = match target {
                Phase::DirectoryMetadata => 1,
                Phase::NamedDirectoryMetadata => 2,
                Phase::LockDescriptorStat => 3,
                Phase::NamedLockStat | Phase::Identity => 4,
            };
            assert_eq!(actual.entered_calls(), calls);
            assert_eq!(actual.named_directory_metadata().is_some(), calls >= 2);
            assert_eq!(actual.lock_descriptor_stat().is_some(), calls >= 3);
            assert_eq!(actual.named_lock_stat().is_some(), calls >= 4);
            assert_eq!(
                actual.identity_matches(),
                (target == Phase::Identity).then_some(true)
            );
            if unwind {
                assert_eq!(
                    actual
                        .panic_payload()
                        .expect("original payload")
                        .downcast_ref::<Phase>(),
                    Some(&target)
                );
            } else {
                assert_eq!(
                    actual.checkpoint_failure(),
                    Some(ShardDirectoryCheckFailure::Refused)
                );
            }
            root.contended();
            drop(error);
            ShardRecoveryGuard::try_acquire(root.lock_file(), root.0.clone())
                .expect("only retained owner cleanup releases actual lock");
        }
    }
}

#[test]
fn actual_missing_lock_errno_survives_later_checkpoint_failure() {
    let root = Temporary::new();
    let guard = root.acquired();
    let old = root.0.join("retained.lock");
    fs::rename(root.0.join(".shard-store.lock"), &old).expect("actual missing named lock");
    let checks = Checks::new();
    let error = PinnedShardDirectory::from_descriptor_observed(
        root.directory(),
        guard,
        &root.0,
        limits(),
        expiry(),
        &checks,
        |phase| {
            if phase == Phase::NamedLockStat {
                checks.0.set(true);
            }
        },
    )
    .expect_err("actual missing lock plus tighter composing check");
    assert_eq!(error.failure(), PinnedShardDirectoryFailure::Checkpoint);
    let raw = error
        .observation()
        .named_lock_stat()
        .expect("saved native result")
        .as_ref()
        .expect_err("actual statat errno");
    assert_eq!(raw.kind(), io::ErrorKind::NotFound);
    assert!(raw.raw_os_error().is_some());
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(old)
        .expect("original lock inode");
    assert!(fs2::FileExt::try_lock_exclusive(&file).is_err());
    drop(error);
    fs2::FileExt::try_lock_exclusive(&file).expect("error retained original lock");
}

#[test]
fn original_expiry_and_path_caps_enter_nothing_and_one_less_slot_keeps_three_results() {
    let root = Temporary::new();
    for expired in [false, true] {
        let mut cap = limits();
        if !expired {
            cap.max_path_bytes = root.0.as_os_str().len() - 1;
        }
        let error = PinnedShardDirectory::from_descriptor_until(
            root.directory(),
            root.acquired(),
            &root.0,
            cap,
            if expired { Instant::now() } else { expiry() },
            &Checks::new(),
        )
        .expect_err("pre-entry refusal");
        assert_eq!(
            error.failure(),
            if expired {
                PinnedShardDirectoryFailure::Expired
            } else {
                PinnedShardDirectoryFailure::Capacity
            }
        );
        assert_eq!(error.observation().entered_calls(), 0);
        assert!(error.observation().directory_metadata().is_none());
        root.contended();
        drop(error);
    }
    let mut cap = limits();
    cap.max_io_calls = 3;
    let error = PinnedShardDirectory::from_descriptor_until(
        root.directory(),
        root.acquired(),
        &root.0,
        cap,
        expiry(),
        &Checks::new(),
    )
    .expect_err("fourth actual call not admitted");
    assert_eq!(error.failure(), PinnedShardDirectoryFailure::Capacity);
    assert_eq!(
        (
            error.observation().reserved_calls(),
            error.observation().entered_calls()
        ),
        (3, 3)
    );
    assert!(matches!(
        error.observation().lock_descriptor_stat(),
        Some(Ok(_))
    ));
    assert!(error.observation().named_lock_stat().is_none());
    root.contended();
}
