//! Public genuine same-descriptor native acquisition and cleanup contract.

#![cfg(unix)]

use shardlog::{ShardGuardAcquireLimits, ShardRecoveryGuard, shard_guard_acquire_cost};
use std::fs::OpenOptions;
use std::time::{Duration, Instant};

#[test]
fn public_acquisition_borrows_only_the_successfully_acquired_same_guard() {
    let directory = tempfile::tempdir().expect("actual native root");
    let path = directory.path().join(".shard-store.lock");
    let open = || {
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .expect("actual native lock FD")
    };
    let limits = ShardGuardAcquireLimits {
        max_path_bytes: directory.path().as_os_str().len() + 1 + ".shard-store.lock".len(),
        max_io_calls: 5,
    };
    let cost =
        shard_guard_acquire_cost(directory.path(), limits).expect("plain costs before acquisition");
    assert_eq!(cost.native_call_slots, 5);
    let expiry = Instant::now() + Duration::from_secs(30);
    let owner =
        ShardRecoveryGuard::try_acquire_until(open(), directory.path().to_owned(), limits, expiry)
            .expect("actual public native acquisition");
    assert_eq!(owner.observation().absolute_expiry(), expiry);
    assert_eq!(owner.observation().entered_io_calls(), 5);
    assert!(matches!(owner.observation().lock_result(), Some(Ok(()))));
    owner
        .guard()
        .validate(directory.path())
        .expect("same actual guard is borrowed");
    assert!(ShardRecoveryGuard::try_acquire(open(), directory.path().to_owned()).is_err());
    drop(owner);
    ShardRecoveryGuard::try_acquire(open(), directory.path().to_owned())
        .expect("actual acquisition owner cleanup releases native lock");
}
