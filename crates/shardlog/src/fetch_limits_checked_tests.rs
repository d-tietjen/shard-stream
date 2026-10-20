use super::*;
use std::{
    fs, io,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, SystemTime},
};

struct Checks {
    mode: AtomicUsize,
    calls: AtomicUsize,
    reentrant: Mutex<Option<std::sync::Weak<Inner>>>,
}
impl Checks {
    fn new(mode: usize) -> Arc<Self> {
        Arc::new(Self {
            mode: AtomicUsize::new(mode),
            calls: AtomicUsize::new(0),
            reentrant: Mutex::new(None),
        })
    }
}
impl ShardDirectoryChecks for Checks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        if let Some(inner) = self
            .reentrant
            .lock()
            .expect("actual checker fixture")
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
        {
            let _state = inner
                .state
                .try_lock()
                .expect("no fetch counter lock across a composing check");
        }
        match self.mode.load(Ordering::Acquire) {
            1 => Err(ShardDirectoryCheckFailure::Refused),
            2 => std::panic::panic_any(417usize),
            _ => Ok(()),
        }
    }
}
fn limits() -> StorageFetchLimits {
    StorageFetchLimits {
        max_plan_batches: 8,
        max_entries: 128,
        max_path_bytes: 4096,
        max_read_attempts: 8,
        max_read_bytes: 4096,
        max_declared_bytes: 8192,
        max_work_bytes: 16384,
    }
}
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let stamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("actual clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "checked-fetch-native-{}-{stamp}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual independent native root");
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn checked_fetch_plain_projection_keeps_original_usage_and_reentrant_checks() {
    let expiry = Instant::now() + Duration::from_secs(30);
    let plain = StorageFetchBudget::new(limits(), expiry).expect("ordinary native budget");
    let checks = Checks::new(0);
    let checked = StorageFetchBudget::new_with_checks(limits(), expiry, checks.clone())
        .expect("checked native budget");
    *checks.reentrant.lock().expect("actual weak observer") = Some(Arc::downgrade(&checked.inner));
    plain
        .charge(7, 3, 1, 11, 13)
        .expect("original cumulative debit");
    checked
        .charge(7, 3, 1, 11, 13)
        .expect("same debit and no locked callback");
    assert_eq!(
        plain.usage().expect("ordinary fees"),
        checked.usage().expect("checked fees")
    );
    assert_eq!(plain.absolute_expiry(), expiry);
    assert_eq!(checked.absolute_expiry(), expiry);
    assert_eq!(
        plain.completed(Ok(31usize)).expect("actual plain result"),
        checked
            .completed(Ok(31usize))
            .expect("actual checked result")
    );
    plain.deny();
    checked.deny();
    let calls = checks.calls.load(Ordering::Acquire);
    assert!(matches!(plain.check(), Err(StorageFetchFailure::Denied)));
    assert!(matches!(checked.check(), Err(StorageFetchFailure::Denied)));
    assert_eq!(
        checks.calls.load(Ordering::Acquire),
        calls,
        "sticky denial performs no new composing callback"
    );
    drop(checked);
    assert_eq!(
        Arc::strong_count(&checks),
        1,
        "weak observation creates no owner cycle"
    );
}

#[test]
fn checked_fetch_actual_native_error_and_checkpoint_unwind_survive_late_refusal() {
    for mode in [1, 2] {
        let root = Temp::new();
        let checks = Checks::new(0);
        let budget = StorageFetchBudget::new_with_checks(
            limits(),
            Instant::now() + Duration::from_secs(30),
            checks.clone(),
        )
        .expect("actual initial checked work");
        let sibling = budget.clone();
        let actual = fs::File::open(root.0.join("genuinely-absent-pack"))
            .expect_err("actual native missing-file error");
        assert_eq!(actual.kind(), io::ErrorKind::NotFound);
        checks.mode.store(mode, Ordering::Release);
        let failure = budget
            .completed::<usize>(Err(StorageError::Io(actual)))
            .expect_err("independent actual native and checkpoint causes");
        match &failure {
            StorageFetchFailure::Checkpoint {
                failure: ShardDirectoryCheckFailure::Refused,
                storage_error: Some(StorageError::Io(error)),
                late: false,
            } => {
                assert_eq!(mode, 1);
                assert_eq!(error.kind(), io::ErrorKind::NotFound);
            }
            StorageFetchFailure::CheckpointUnwind {
                payload,
                storage_error: Some(StorageError::Io(error)),
                late: false,
            } => {
                assert_eq!(mode, 2);
                assert_eq!(payload.downcast_ref::<usize>(), Some(&417));
                assert_eq!(error.kind(), io::ErrorKind::NotFound);
            }
            other => panic!("actual independent causes were lost: {other:?}"),
        }
        let late = failure.late();
        match late {
            StorageFetchFailure::Checkpoint {
                storage_error: Some(StorageError::Io(error)),
                late: true,
                ..
            } => {
                assert_eq!(mode, 1);
                assert_eq!(error.kind(), io::ErrorKind::NotFound);
            }
            StorageFetchFailure::CheckpointUnwind {
                payload,
                storage_error: Some(StorageError::Io(error)),
                late: true,
            } => {
                assert_eq!(mode, 2);
                assert_eq!(payload.downcast_ref::<usize>(), Some(&417));
                assert_eq!(error.kind(), io::ErrorKind::NotFound);
            }
            other => panic!("late flag replaced actual checkpoint or native cause: {other:?}"),
        }
        let calls = checks.calls.load(Ordering::Acquire);
        assert!(matches!(sibling.check(), Err(StorageFetchFailure::Denied)));
        assert_eq!(checks.calls.load(Ordering::Acquire), calls);
    }
}

#[test]
fn checked_fetch_constructor_keeps_refusal_or_original_panic_without_retry() {
    for mode in [1, 2] {
        let checks = Checks::new(mode);
        let failure = StorageFetchBudget::new_with_checks(
            limits(),
            Instant::now() + Duration::from_secs(30),
            checks.clone(),
        )
        .expect_err("checkpoint refuses before shared budget allocation");
        match failure {
            StorageFetchFailure::Checkpoint {
                failure: ShardDirectoryCheckFailure::Refused,
                storage_error: None,
                late: false,
            } => assert_eq!(mode, 1),
            StorageFetchFailure::CheckpointUnwind {
                payload,
                storage_error: None,
                late: false,
            } => {
                assert_eq!(mode, 2);
                assert_eq!(payload.downcast_ref::<usize>(), Some(&417));
            }
            other => panic!("constructor erased the actual checkpoint result: {other:?}"),
        }
        assert_eq!(checks.calls.load(Ordering::Acquire), 1);
        assert_eq!(Arc::strong_count(&checks), 1);
    }
    let checks = Checks::new(2);
    assert!(matches!(
        StorageFetchBudget::new_with_checks(limits(), Instant::now(), checks.clone()),
        Err(StorageFetchFailure::Expired(None))
    ));
    assert_eq!(
        checks.calls.load(Ordering::Acquire),
        0,
        "original expiry refuses before composing callback"
    );
}
