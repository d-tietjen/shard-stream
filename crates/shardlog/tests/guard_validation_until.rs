//! Public-path original-expiry observation of the actual native recovery lease.

#![cfg(unix)]

use shardlog::{ShardGuardValidationLimits, ShardRecoveryGuard, shard_guard_validation_cost};
use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Root(PathBuf);
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn public_bounded_guard_observation_uses_actual_named_file_and_same_held_descriptor() {
    let root = Root(std::env::temp_dir().join(
        format!("public-native-guard-{}-{}", std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos()),
    ));
    fs::create_dir_all(&root.0).expect("actual native directory");
    let path = root.0.join(".shard-store.lock");
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
        .expect("real native lock file");
    let guard = ShardRecoveryGuard::try_acquire(file, root.0.clone())
        .expect("ordinary native fixture acquisition");
    let limits = ShardGuardValidationLimits {
        max_path_bytes: root.0.as_os_str().len() + 1 + ".shard-store.lock".len(),
        max_io_calls: 2,
    };
    let cost = shard_guard_validation_cost(limits).expect("checked native source cost");
    assert!(cost.declared_bytes >= cost.string_bytes);
    let original = Instant::now() + Duration::from_secs(30);
    let observation = guard
        .validate_until(&root.0, limits, original)
        .expect("real bounded native observation");
    assert_eq!(observation.absolute_expiry(), original);
    assert_eq!(observation.lock_path(), path.as_path());
    assert_eq!(observation.io_calls(), 2);
    assert_eq!(observation.entered_io_calls(), 2);
    assert_eq!(observation.identity_changed(), Some(false));
    assert!(observation.named_metadata().is_some() && observation.descriptor_metadata().is_some());
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("same actual lock inode");
    assert!(ShardRecoveryGuard::try_acquire(file, root.0.clone()).is_err());
    drop(guard);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("actual released lock inode");
    drop(
        ShardRecoveryGuard::try_acquire(file, root.0.clone())
            .expect("physical lease now reacquired"),
    );
}
