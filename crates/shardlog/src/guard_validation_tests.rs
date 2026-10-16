//! Actual native recovery-guard metadata, limits and elapsed-phase regressions.

use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "native-guard-until-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&p).expect("actual native directory");
        Self(p)
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn acquire(root: &Root) -> ShardRecoveryGuard {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.0.join(LOCK_NAME))
        .expect("actual native lock descriptor");
    ShardRecoveryGuard::try_acquire(file, root.0.clone()).expect("actual native lease fixture")
}
fn limits(root: &Root) -> ShardGuardValidationLimits {
    ShardGuardValidationLimits {
        max_path_bytes: root.0.as_os_str().len() + 1 + LOCK_NAME.len(),
        max_io_calls: 2,
    }
}

#[test]
fn native_path_and_metadata_completion_crossing_original_expiry_retains_actual_outcomes() {
    for phase in [
        Phase::PathReserved,
        Phase::NamedMetadata,
        Phase::DescriptorMetadata,
        Phase::Identity,
    ] {
        let root = Root::new();
        let guard = acquire(&root);
        let original = Instant::now() + Duration::from_millis(200);
        let mut observed = false;
        let error = guard
            .validate_until_observed(&root.0, limits(&root), original, |actual| {
                if std::mem::discriminant(&phase) == std::mem::discriminant(&actual) {
                    observed = true;
                    while Instant::now() < original {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
            })
            .expect_err("actual native completed phase crosses original E");
        assert!(observed);
        assert_eq!(error.failure(), ShardGuardValidationFailure::Expired);
        assert_eq!(error.observation().absolute_expiry(), original);
        match phase {
            Phase::PathReserved => {
                assert_eq!(error.observation().entered_io_calls(), 0);
                assert!(error.observation.lock_path.capacity() >= limits(&root).max_path_bytes);
            }
            Phase::NamedMetadata => {
                assert!(error.observation().named_metadata().is_some());
                assert!(error.observation().descriptor_metadata().is_none());
            }
            Phase::DescriptorMetadata => {
                assert!(error.observation().named_metadata().is_some());
                assert!(error.observation().descriptor_metadata().is_some());
            }
            Phase::Identity => assert_eq!(error.observation().identity_changed(), Some(false)),
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.0.join(LOCK_NAME))
            .expect("same actual named lock inode");
        assert!(
            ShardRecoveryGuard::try_acquire(file, root.0.clone()).is_err(),
            "caller retains same real guard through refusal"
        );
        guard
            .validate(&root.0)
            .expect("ordinary native validation remains available");
    }
}

#[test]
fn one_less_path_or_metadata_allowance_refuses_before_next_native_phase_and_symlink_policy_is_shared()
 {
    let root = Root::new();
    let guard = acquire(&root);
    let expiry = Instant::now() + Duration::from_secs(30);
    let mut short_path = limits(&root);
    short_path.max_path_bytes -= 1;
    let error = guard
        .validate_until(&root.0, short_path, expiry)
        .expect_err("one less path byte");
    assert_eq!(error.failure(), ShardGuardValidationFailure::Capacity);
    assert_eq!(error.observation().requested_path_bytes(), 0);
    assert_eq!(error.observation().entered_io_calls(), 0);
    let mut one_call = limits(&root);
    one_call.max_io_calls = 1;
    let error = guard
        .validate_until(&root.0, one_call, expiry)
        .expect_err("one less metadata call");
    assert_eq!(error.failure(), ShardGuardValidationFailure::Capacity);
    assert_eq!(error.observation().entered_io_calls(), 1);
    assert!(error.observation().named_metadata().is_some());
    assert!(error.observation().descriptor_metadata().is_none());
    fs::rename(root.0.join(LOCK_NAME), root.0.join("original.lock"))
        .expect("actual same-inode rename");
    std::os::unix::fs::symlink("original.lock", root.0.join(LOCK_NAME))
        .expect("actual named symlink");
    assert!(
        guard.validate(&root.0).is_err(),
        "ordinary native nofollow identity policy"
    );
    let error = guard
        .validate_until(&root.0, limits(&root), expiry)
        .expect_err("same native policy");
    assert_eq!(error.failure(), ShardGuardValidationFailure::Identity);
    assert_eq!(error.observation().identity_changed(), Some(true));
    assert!(
        error
            .observation()
            .named_metadata()
            .expect("raw native named metadata")
            .file_type()
            .is_symlink()
    );
}
