//! Genuine native writer/open/Core custody regressions; unrun until qualified.

use super::*;
use crate::{
    BatchId, DirectoryOwnershipError, LeaderEpoch, LogicalOffset, LogicalPartitionId,
    NativeDirectoryLimits, OwnedDirectoryHandle, Placement, PlacementSequence, RingEpoch, ShardId,
    TopicId, VirtualLaneId,
};
use shardlog::{
    AcquiredShardRecoveryGuard, PinnedShardDirectoryLimits, PinnedStoreOpenFailure,
    PinnedStoreOpenOutcome, Reservation, ShardGuardAcquireLimits, ShardRecoveryGuard, StorageError,
};
use std::fs::{self, File, OpenOptions};
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Duration;

type Observer = Box<dyn FnMut(Phase)>;
thread_local! { static OBSERVER:RefCell<Option<Observer>>=RefCell::new(None); }
pub(super) fn completed(phase: Phase) {
    OBSERVER.with(|observer| {
        if let Some(observer) = &mut *observer.borrow_mut() {
            observer(phase);
        }
    });
}
struct Observe;
impl Drop for Observe {
    fn drop(&mut self) {
        OBSERVER.with(|observer| *observer.borrow_mut() = None);
    }
}
fn observe(callback: impl FnMut(Phase) + 'static) -> Observe {
    OBSERVER.with(|observer| {
        assert!(observer.borrow().is_none());
        *observer.borrow_mut() = Some(Box::new(callback));
    });
    Observe
}

struct Checks {
    denied: Cell<bool>,
    panic_check: Rc<Cell<bool>>,
    panic_deny: Cell<bool>,
    deny_attempts: Cell<usize>,
}
impl Checks {
    fn new() -> Self {
        Self {
            denied: Cell::new(false),
            panic_check: Rc::new(Cell::new(false)),
            panic_deny: Cell::new(false),
            deny_attempts: Cell::new(0),
        }
    }
}
impl ShardDirectoryChecks for Checks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        if self.panic_check.replace(false) {
            std::panic::panic_any("actual Core bridge checkpoint unwind");
        }
        if self.denied.get() {
            Err(ShardDirectoryCheckFailure::Refused)
        } else {
            Ok(())
        }
    }
}
impl PinnedStoreOpenChecks for Checks {
    fn deny(&self) {
        self.deny_attempts.set(self.deny_attempts.get() + 1);
        self.denied.set(true);
        if self.panic_deny.get() {
            std::panic::panic_any("actual Core bridge deny unwind");
        }
    }
}

struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "owned-pinned-native-store-{}-{:020}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual temporary native directory");
        Self(path)
    }
    fn config(&self) -> ShardStoreConfig {
        ShardStoreConfig {
            directory: self.0.clone(),
            shard_id: ShardId::new(4),
            target_pack_bytes: 1 << 20,
            max_batch_bytes: 4096,
        }
    }
    fn lock(&self) -> File {
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.0.join(".shard-store.lock"))
            .expect("actual shard lock file")
    }
    fn acquired(&self) -> AcquiredShardRecoveryGuard {
        ShardRecoveryGuard::try_acquire_until(
            self.lock(),
            self.0.clone(),
            ShardGuardAcquireLimits {
                max_path_bytes: 8192,
                max_io_calls: 5,
            },
            expiry(),
        )
        .expect("actual acquired guard; no old plain guard conversion")
    }
    fn pin(&self, root: &OwnedDirectoryHandle) -> OwnedShardDirectory {
        root.pin_shard_directory_until(
            self.acquired(),
            PinnedShardDirectoryLimits {
                max_path_bytes: 8192,
                max_io_calls: 4,
            },
            &budget(core_limits()),
        )
        .expect("actual Core descriptor derives the same pin")
    }
    fn contended(&self) {
        assert!(matches!(
            DirectoryOwnership::acquire(&self.0),
            Err(DirectoryOwnershipError::Contended)
        ));
        assert!(ShardRecoveryGuard::try_acquire(self.lock(), self.0.clone()).is_err());
    }
    fn native_pack_count(&self) -> usize {
        fs::read_dir(&self.0)
            .expect("actual directory listing")
            .map(|entry| entry.expect("actual row").file_name())
            .filter(|name| name.to_string_lossy().starts_with("pack-"))
            .count()
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn expiry() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
fn core_limits() -> NativeDirectoryLimits {
    NativeDirectoryLimits {
        max_depth: 16,
        max_component_bytes: 4096,
        max_path_bytes: 8192,
        max_operations: 1024,
        max_calls: 1_000_000,
        max_entries: 1_000_000,
        max_declared_bytes: 1 << 28,
        max_work_bytes: 1 << 30,
    }
}
fn budget(limits: NativeDirectoryLimits) -> NativeDirectoryBudget {
    NativeDirectoryBudget::new(limits, expiry()).expect("actual fresh bounded Core counters")
}
fn open_limits() -> PinnedStoreOpenLimits {
    PinnedStoreOpenLimits {
        max_directory_visits: 128,
        directory_buffer_bytes: 8192,
        max_name_bytes: 1024,
        max_path_bytes: 8192,
        max_packs: 16,
        max_frames: 64,
        max_frame_bytes: 8192,
        max_read_attempts: 256,
        max_read_bytes: 1 << 22,
        max_index_entries: 256,
        max_repair_calls: 64,
        max_io_calls: 512,
        max_outcomes: 1024,
        max_entries: 1 << 16,
        max_declared_bytes: 1 << 20,
        max_work_bytes: 1 << 24,
    }
}
fn reservation(batch: u128) -> Reservation {
    Reservation {
        topic_id: TopicId::new(19),
        partition_id: LogicalPartitionId::new(2),
        batch_id: BatchId::new(batch),
        first_offset: LogicalOffset::new(batch as u64),
        last_offset: LogicalOffset::new(batch as u64),
        record_count: NonZeroU32::new(1).expect("one actual record"),
        placement: Placement {
            virtual_lane_id: VirtualLaneId::new(3),
            ring_epoch: RingEpoch::new(1),
            leader_epoch: LeaderEpoch::new(7),
            sequence: PlacementSequence::new(batch as u64),
        },
    }
}
fn write_actual_frames(path: &Temporary, count: usize) {
    let mut writer = ShardStore::open(path.config()).expect("ordinary actual fixture writer");
    for n in 0..count {
        writer
            .append(
                reservation(n as u128),
                None,
                b"genuine native fixture body",
                true,
            )
            .expect("actual complete frame and sync");
    }
}

#[test]
fn actual_open_and_physical_file_job_keep_same_pin_and_root_until_last_drop() {
    fn send<T: Send>() {}
    fn sync<T: Sync>() {}
    send::<OwnedPinnedShardStore>();
    send::<OwnedShardStoreDirectory>();
    sync::<OwnedShardStoreDirectory>();
    send::<RetainedOwnedPinnedShardStoreError>();
    let path = Temporary::new();
    write_actual_frames(&path, 2);
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual original Core owner");
    let weak = Arc::downgrade(&owner);
    let root = owner.root_handle().expect("actual Core directory FD");
    let paid = budget(core_limits());
    let checks = Checks::new();
    let owned = path
        .pin(&root)
        .open_store_until(path.config(), open_limits(), &paid, &checks)
        .expect("actual Core/native open, no reconstructed store");
    assert!(Arc::ptr_eq(owned.owner(), &owner));
    assert_eq!(owned.store().recovery_catalog_len(), 2);
    assert!(owned.store().contains(&reservation(0)));
    assert!(owned.store().contains(&reservation(1)));
    assert_eq!(owned.open_expiry(), paid.absolute_expiry());
    assert!(owned.open_usage().entered_io_calls > 0);
    assert!(!checks.denied.get());
    let directory = owned.retain_directory();
    assert!(Arc::ptr_eq(directory.owner(), &owner));
    assert!(Arc::ptr_eq(&directory.inner, &owned.directory.inner));
    struct Job {
        file: File,
        directory: OwnedShardStoreDirectory,
    }
    let job = Job {
        file: owned
            .store()
            .active_file_clone()
            .expect("actual native active FD clone"),
        directory,
    };
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        let job = job;
        entered_tx.send(()).expect("actual physical job entered");
        release_rx.recv().expect("actual job released");
        let observed = job.file.sync_data();
        observed.expect("actual file durability call settled");
        let _ = job.directory.owner();
        drop(job);
    });
    entered_rx
        .recv()
        .expect("actual physical worker holds both inputs");
    drop(owned);
    drop(paid);
    drop(root);
    drop(owner);
    path.contended();
    assert!(weak.upgrade().is_some());
    release_tx.send(()).expect("release actual retained job");
    worker
        .join()
        .expect("actual file and directory custody physically joined");
    assert!(weak.upgrade().is_none());
    DirectoryOwnership::acquire(&path.0).expect("actual Core root finally released");
    ShardRecoveryGuard::try_acquire(path.lock(), path.0.clone())
        .expect("same native acquired lease finally released");
}

#[test]
fn every_completed_bridge_phase_retains_actual_inputs_or_open_store_on_late_or_unwind() {
    for target in [
        Phase::DirectoryShared,
        Phase::ParentValidated,
        Phase::NativeCompleted,
        Phase::FinalParentValidated,
    ] {
        for unwind in [false, true] {
            let path = Temporary::new();
            let owner = DirectoryOwnership::acquire(&path.0).expect("actual Core owner");
            let root = owner.root_handle().expect("actual root descriptor");
            let paid = budget(core_limits());
            let same = paid.clone();
            let checks = Checks::new();
            let _observe = observe(move |phase| {
                if phase == target {
                    if unwind {
                        std::panic::panic_any(target);
                    }
                    assert!(matches!(
                        same.cap_expiry(Instant::now()),
                        Err(NativeDirectoryFailure::Expired(_))
                    ));
                }
            });
            let error = path
                .pin(&root)
                .open_store_until(path.config(), open_limits(), &paid, &checks)
                .expect_err("actual completed phase must refuse");
            assert_eq!(
                error.failure(),
                if unwind {
                    OwnedPinnedShardStoreFailure::Unwind
                } else {
                    OwnedPinnedShardStoreFailure::Budget
                }
            );
            assert_eq!(
                error.retains_opened_store(),
                matches!(target, Phase::NativeCompleted | Phase::FinalParentValidated)
            );
            assert_eq!(error.native_usage().is_some(), error.retains_opened_store());
            assert_eq!(
                error.input_config().is_some(),
                !error.retains_opened_store()
            );
            assert!(Arc::ptr_eq(error.owner(), &owner));
            assert!(checks.denied.get());
            assert!(error.denial_completed());
            if unwind {
                assert_eq!(
                    error
                        .panic_payload()
                        .expect("original producer panic")
                        .downcast_ref::<Phase>(),
                    Some(&target)
                );
                assert!(matches!(paid.check(), Err(NativeDirectoryFailure::Denied)));
            } else {
                assert!(matches!(
                    error.budget_error(),
                    Some(NativeDirectoryFailure::Expired(_))
                ));
            }
            drop(root);
            drop(owner);
            path.contended();
            drop(error);
            DirectoryOwnership::acquire(&path.0).expect("actual error was last Core owner");
            ShardRecoveryGuard::try_acquire(path.lock(), path.0.clone())
                .expect("actual error was last native pin");
        }
    }
}

#[test]
fn unpaid_bridge_cost_refuses_before_arc_native_open_or_core_validation() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual Core owner");
    let root = owner.root_handle().expect("actual root descriptor");
    let config = path.config();
    let cost = owned_pinned_shard_store_cost(&config, open_limits())
        .expect("actual checked source ceilings");
    let mut limits = core_limits();
    limits.max_declared_bytes = cost.declared_bytes - 1;
    let paid = budget(limits);
    let checks = Checks::new();
    let error = path
        .pin(&root)
        .open_store_until(config, open_limits(), &paid, &checks)
        .expect_err("entire native ceiling must be prepaid");
    assert!(matches!(
        error.budget_error(),
        Some(NativeDirectoryFailure::Capacity)
    ));
    assert!(error.native_usage().is_none());
    assert!(error.input_config().is_some());
    assert_eq!(path.native_pack_count(), 0);
    let actual = paid.usage().expect("actual counters still observable");
    assert_eq!(actual.operations, 0);
    assert_eq!(actual.calls, 0);
    assert_eq!(actual.declared_bytes, 0);
    assert!(checks.denied.get());
    drop(root);
    drop(owner);
    path.contended();
    drop(error);
    DirectoryOwnership::acquire(&path.0).expect("unpaid failure retained actual input root");
}

#[test]
fn earlier_shared_expiry_cannot_be_revived_and_early_deny_panic_keeps_input_custody() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual Core owner");
    let root = owner.root_handle().expect("actual Core root");
    let pin = path.pin(&root);
    let paid = budget(core_limits());
    let checks = Checks::new();
    checks.panic_deny.set(true);
    assert!(paid.cap_expiry(Instant::now()).is_err());
    let earlier = paid.absolute_expiry();
    assert!(paid.cap_expiry(expiry()).is_err());
    assert_eq!(paid.absolute_expiry(), earlier);
    let error = pin
        .open_store_until(path.config(), open_limits(), &paid, &checks)
        .expect_err("earlier actual E remains in force");
    assert!(matches!(
        error.budget_error(),
        Some(NativeDirectoryFailure::Expired(_))
    ));
    assert_eq!(
        error
            .deny_panic_payload()
            .expect("actual early denial payload")
            .downcast_ref::<&'static str>(),
        Some(&"actual Core bridge deny unwind")
    );
    assert!(!error.denial_completed());
    assert_eq!(checks.deny_attempts.get(), 1);
    assert_eq!(path.native_pack_count(), 0);
    drop(root);
    drop(owner);
    path.contended();
    drop(error);
    ShardRecoveryGuard::try_acquire(path.lock(), path.0.clone())
        .expect("input native lock retained until error drop");
}

#[test]
fn actual_foreign_config_refuses_without_rebuilding_the_pinned_directory() {
    let path = Temporary::new();
    let other = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual Core owner");
    let root = owner.root_handle().expect("actual Core FD");
    let checks = Checks::new();
    let error = path
        .pin(&root)
        .open_store_until(
            other.config(),
            open_limits(),
            &budget(core_limits()),
            &checks,
        )
        .expect_err("actual config and native pin name disagree");
    assert_eq!(
        error
            .native_error()
            .expect("actual producer refusal held")
            .failure(),
        PinnedStoreOpenFailure::Identity
    );
    assert_eq!(
        error
            .native_usage()
            .expect("actual native phase ledger")
            .entered_io_calls,
        0
    );
    assert_eq!(path.native_pack_count(), 0);
    assert_eq!(other.native_pack_count(), 0);
    assert!(checks.denied.get());
    assert_eq!(checks.deny_attempts.get(), 1);
    drop(root);
    drop(owner);
    path.contended();
    drop(error);
    DirectoryOwnership::acquire(&path.0)
        .expect("original owner retained across foreign-config failure");
}

#[test]
fn actual_corrupt_native_frame_and_partial_reader_survive_bridge_error_custody() {
    let path = Temporary::new();
    write_actual_frames(&path, 1);
    let pack = fs::read_dir(&path.0)
        .expect("actual native listing")
        .map(|entry| entry.expect("actual row").path())
        .find(|p| {
            p.file_name()
                .expect("actual name")
                .to_string_lossy()
                .starts_with("pack-")
        })
        .expect("actual writer pack");
    let mut bytes = fs::read(&pack).expect("actual complete native frame");
    let last = bytes.last_mut().expect("actual nonempty native bytes");
    *last ^= 0x80;
    fs::write(&pack, &bytes).expect("actual CRC corruption");
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual Core owner");
    let root = owner.root_handle().expect("actual Core FD");
    let checks = Checks::new();
    let error = path
        .pin(&root)
        .open_store_until(
            path.config(),
            open_limits(),
            &budget(core_limits()),
            &checks,
        )
        .expect_err("actual decoder refuses corrupted native frame");
    let native = error
        .native_error()
        .expect("actual typed native failure retained");
    assert_eq!(native.failure(), PinnedStoreOpenFailure::Native);
    let corrupt = native
        .outcomes()
        .iter()
        .find_map(|outcome| {
            let PinnedStoreOpenOutcome::Decode {
                offset: 0,
                result: Err(error),
            } = outcome
            else {
                return None;
            };
            match error {
                StorageError::Corrupt {
                    path,
                    offset: 0,
                    reason,
                } if std::path::Path::new(path) == pack.as_path()
                    && reason == "extent checksum mismatch" =>
                {
                    Some(error)
                }
                _ => None,
            }
        })
        .expect("actual original native checksum error retained in Decode row");
    let source = std::error::Error::source(native)
        .and_then(|error| error.downcast_ref::<StorageError>())
        .expect("actual retained native error is the Error source");
    assert!(std::ptr::eq(corrupt, source));
    assert!(native.retained_file_slots() > 0);
    assert!(native.usage().completed_read_bytes > 0);
    assert_eq!(fs::read(&pack).expect("actual pack after refusal"), bytes);
    assert!(checks.denied.get());
    drop(root);
    drop(owner);
    path.contended();
    drop(error);
    DirectoryOwnership::acquire(&path.0).expect("actual decoder error held Core parent");
}

#[test]
fn completed_native_store_is_retained_when_actual_core_directory_is_replaced() {
    let path = Temporary::new();
    let backup = path.0.with_extension("actual-retained-directory");
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual Core owner");
    let weak = Arc::downgrade(&owner);
    let root = owner.root_handle().expect("actual Core FD");
    let pin = path.pin(&root);
    let source = path.0.clone();
    let destination = backup.clone();
    let _observe = observe(move |phase| {
        if phase == Phase::NativeCompleted {
            fs::rename(&source, &destination).expect("move actual original directory");
            fs::create_dir(&source).expect("install actual foreign directory at old name");
        }
    });
    let checks = Checks::new();
    let error = pin
        .open_store_until(
            path.config(),
            open_limits(),
            &budget(core_limits()),
            &checks,
        )
        .expect_err("real final Core identity validation must refuse");
    assert_eq!(error.failure(), OwnedPinnedShardStoreFailure::Directory);
    assert!(error.directory_error().is_some());
    assert!(error.retains_opened_store());
    assert!(
        error
            .native_usage()
            .expect("actual native open completed")
            .entered_io_calls
            > 0
    );
    assert!(checks.denied.get());
    drop(root);
    drop(owner);
    assert!(weak.upgrade().is_some());
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(backup.join(".shard-store.lock"))
        .expect("actual original lock moved with directory");
    assert!(ShardRecoveryGuard::try_acquire(lock, backup.clone()).is_err());
    drop(error);
    assert!(weak.upgrade().is_none());
    fs::remove_dir_all(&backup)
        .expect("remove genuine isolated original fixture after custody drops");
}

#[test]
fn original_checkpoint_and_deny_panics_are_independent_and_keep_actual_inputs() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual Core owner");
    let root = owner.root_handle().expect("actual Core root FD");
    let checks = Checks::new();
    checks.panic_deny.set(true);
    let armed = Rc::clone(&checks.panic_check);
    let _observe = observe(move |phase| {
        if phase == Phase::ParentValidated {
            armed.set(true);
        }
    });
    let paid = budget(core_limits());
    let error = path
        .pin(&root)
        .open_store_until(path.config(), open_limits(), &paid, &checks)
        .expect_err("actual bridge callback unwinds before native entry");
    assert_eq!(error.failure(), OwnedPinnedShardStoreFailure::Unwind);
    assert_eq!(
        error
            .panic_payload()
            .expect("original checkpoint panic")
            .downcast_ref::<&'static str>(),
        Some(&"actual Core bridge checkpoint unwind")
    );
    assert_eq!(
        error
            .deny_panic_payload()
            .expect("separate denial panic")
            .downcast_ref::<&'static str>(),
        Some(&"actual Core bridge deny unwind")
    );
    assert_eq!(checks.deny_attempts.get(), 1);
    assert!(!error.denial_completed());
    assert!(error.input_config().is_some());
    assert!(error.native_usage().is_none());
    assert!(matches!(paid.check(), Err(NativeDirectoryFailure::Denied)));
    drop(root);
    drop(owner);
    path.contended();
    drop(error);
    DirectoryOwnership::acquire(&path.0)
        .expect("both actual panic payloads retained ahead of parent");
}
