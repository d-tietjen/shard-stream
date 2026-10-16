//! Real native lock acquisition, late ownership and cumulative call regressions.

use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        let id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("native-guard-acquire-{}-{id}", std::process::id()));
        fs::create_dir(&path).expect("actual native root");
        Self(path)
    }
    fn file(&self) -> File {
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.0.join(".shard-store.lock"))
            .expect("actual native lock FD")
    }
    fn limits(&self) -> ShardGuardAcquireLimits {
        ShardGuardAcquireLimits {
            max_path_bytes: self.0.as_os_str().len() + 1 + ".shard-store.lock".len(),
            max_io_calls: 5,
        }
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn late(expiry: Instant) {
    while Instant::now() < expiry {
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn contended(root: &Root) {
    assert!(
        ShardRecoveryGuard::try_acquire(root.file(), root.0.clone()).is_err(),
        "same actual named native inode stays locked"
    );
}

#[test]
fn real_lock_and_each_saved_validation_crossing_original_e_keep_exact_native_outcomes() {
    for selected in [Phase::BeforeValidation, Phase::Lock, Phase::AfterValidation] {
        let root = Root::new();
        let expiry = Instant::now() + Duration::from_secs(1);
        let mut seen = false;
        let error = ShardRecoveryGuard::acquire_until_observed(
            root.file(),
            root.0.clone(),
            root.limits(),
            expiry,
            |phase| {
                if phase == selected {
                    seen = true;
                    late(expiry);
                }
            },
        )
        .expect_err("actual completed phase crossed original E");
        assert!(seen);
        assert_eq!(error.failure(), ShardGuardAcquireFailure::Expired);
        assert_eq!(error.observation().absolute_expiry(), expiry);
        assert!(matches!(
            error.observation().before_validation(),
            Some(Ok(_))
        ));
        match selected {
            Phase::BeforeValidation => {
                assert!(error.observation().lock_result().is_none());
                assert_eq!(error.observation().entered_io_calls(), 2);
                // An early failure retains its FD but has never acquired a lock.
                let guard = ShardRecoveryGuard::try_acquire(root.file(), root.0.clone())
                    .expect("no lock call entered");
                drop(guard);
            }
            Phase::Lock => {
                assert!(matches!(error.observation().lock_result(), Some(Ok(()))));
                assert!(error.observation().after_validation().is_none());
                assert_eq!(error.observation().entered_io_calls(), 3);
                contended(&root);
            }
            Phase::AfterValidation => {
                assert!(matches!(error.observation().lock_result(), Some(Ok(()))));
                assert!(matches!(
                    error.observation().after_validation(),
                    Some(Ok(_))
                ));
                assert_eq!(error.observation().entered_io_calls(), 5);
                contended(&root);
            }
        }
        drop(error);
        ShardRecoveryGuard::try_acquire(root.file(), root.0.clone())
            .expect("actual error FD cleanup releases any acquired native lock");
    }
}

#[test]
fn cumulative_one_less_call_refuses_inside_second_validation_and_retains_real_lock() {
    let root = Root::new();
    let expiry = Instant::now() + Duration::from_secs(30);
    let cost = shard_guard_acquire_cost(&root.0, root.limits()).expect("checked actual input cost");
    assert_eq!(cost.native_call_slots, 5);
    let mut cap = root.limits();
    cap.max_io_calls = 4;
    let error = ShardRecoveryGuard::try_acquire_until(root.file(), root.0.clone(), cap, expiry)
        .expect_err("one less cumulative native call");
    assert_eq!(error.failure(), ShardGuardAcquireFailure::Capacity);
    assert_eq!(error.observation().io_calls(), 4);
    assert_eq!(error.observation().entered_io_calls(), 4);
    let after = error
        .observation()
        .after_validation()
        .expect("actual second validation")
        .as_ref()
        .expect_err("descriptor metadata call denied");
    assert!(after.observation().named_metadata().is_some());
    assert!(after.observation().descriptor_metadata().is_none());
    assert!(matches!(error.observation().lock_result(), Some(Ok(()))));
    contended(&root);
    drop(error);
    let mut short_path = root.limits();
    short_path.max_path_bytes -= 1;
    assert!(shard_guard_acquire_cost(&root.0, short_path).is_none());
    let error =
        ShardRecoveryGuard::try_acquire_until(root.file(), root.0.clone(), short_path, expiry)
            .expect_err("path cap refuses before first native validation");
    assert_eq!(error.failure(), ShardGuardAcquireFailure::Capacity);
    assert_eq!(error.observation().entered_io_calls(), 0);
    assert!(error.observation().before_validation().is_none());
    let expired = ShardRecoveryGuard::try_acquire_until(
        root.file(),
        root.0.clone(),
        root.limits(),
        Instant::now(),
    )
    .expect_err("already expired original E");
    assert_eq!(expired.failure(), ShardGuardAcquireFailure::Expired);
    assert_eq!(expired.observation().entered_io_calls(), 0);
    ShardRecoveryGuard::try_acquire(root.file(), root.0.clone())
        .expect("refused unacquired FDs grant no native lock");
}

#[test]
fn actual_competing_native_lock_error_is_retained_without_unlocking_original_owner() {
    let root = Root::new();
    let first = ShardRecoveryGuard::try_acquire(root.file(), root.0.clone())
        .expect("actual original native lock");
    let error = ShardRecoveryGuard::try_acquire_until(
        root.file(),
        root.0.clone(),
        root.limits(),
        Instant::now() + Duration::from_secs(30),
    )
    .expect_err("genuine incompatible native lock");
    assert_eq!(error.failure(), ShardGuardAcquireFailure::Lock);
    assert!(
        matches!(error.observation().lock_result(), Some(Err(error)) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    assert_eq!(error.observation().entered_io_calls(), 3);
    assert!(error.observation().after_validation().is_none());
    contended(&root);
    drop(first);
    // The failed attempt's distinct FD is retained but never became the winner.
    ShardRecoveryGuard::try_acquire(root.file(), root.0.clone())
        .expect("failed attempt does not fabricate acquisition");
}

#[test]
fn actual_name_replacement_after_acquire_keeps_old_locked_inode_and_both_names() {
    let root = Root::new();
    let old = root.0.join("retained.lock");
    let named = root.0.join(".shard-store.lock");
    let error = ShardRecoveryGuard::acquire_until_observed(
        root.file(),
        root.0.clone(),
        root.limits(),
        Instant::now() + Duration::from_secs(30),
        |phase| {
            if phase == Phase::Lock {
                fs::rename(&named, &old)
                    .expect("actual same-inode rename after native acquisition");
                std::os::unix::fs::symlink("retained.lock", &named)
                    .expect("actual substituted name");
            }
        },
    )
    .expect_err("actual final native identity predicate refuses replacement");
    assert_eq!(error.failure(), ShardGuardAcquireFailure::Validation);
    assert!(matches!(error.observation().lock_result(), Some(Ok(()))));
    let after = error
        .observation()
        .after_validation()
        .expect("actual final validation")
        .as_ref()
        .expect_err("native identity refusal");
    assert_eq!(after.failure(), ShardGuardValidationFailure::Identity);
    assert_eq!(after.observation().identity_changed(), Some(true));
    assert!(
        fs::symlink_metadata(&named)
            .expect("producer leaves replacement evidence")
            .file_type()
            .is_symlink()
    );
    let original = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&old)
        .expect("same actual renamed lock inode");
    assert!(fs2::FileExt::try_lock_exclusive(&original).is_err());
    drop(error);
    fs2::FileExt::try_lock_exclusive(&original)
        .expect("actual retained FD released its native lock");
}
