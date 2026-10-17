//! Genuine Core/native files, inputs and custody; no fabricated host authority.

use super::super::SharedDirectory;
use super::*;
use crate::{
    BatchId, DirectoryOwnershipError, LeaderEpoch, LogicalOffset, LogicalPartitionId,
    NativeDirectoryLimits, Placement, PlacementSequence, RingEpoch, ShardId, TopicId,
    VirtualLaneId,
};
use shardlog::{
    PinnedShardAppendFailure, PinnedShardDirectoryLimits, PinnedStoreOpenChecks,
    PinnedStoreOpenLimits, Reservation, ShardGuardAcquireLimits, ShardRecoveryGuard, ShardStore,
    ShardStoreConfig,
};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

type Observer = Box<dyn FnMut(Phase)>;
thread_local! { static OBSERVER: RefCell<Option<Observer>> = RefCell::new(None); }
pub(super) fn completed(phase: Phase) {
    OBSERVER.with(|value| {
        if let Some(callback) = &mut *value.borrow_mut() {
            callback(phase);
        }
    });
}
struct Observe;
impl Drop for Observe {
    fn drop(&mut self) {
        OBSERVER.with(|value| *value.borrow_mut() = None);
    }
}
fn observe(callback: impl FnMut(Phase) + 'static) -> Observe {
    OBSERVER.with(|value| {
        assert!(value.borrow().is_none());
        *value.borrow_mut() = Some(Box::new(callback));
    });
    Observe
}

enum AfterWrite {
    Panic,
    Expire(NativeDirectoryBudget),
}
struct HostChecks {
    denied: Cell<bool>,
    deny_attempts: Cell<usize>,
    panic_deny: Cell<bool>,
    inspect_pin: Option<Arc<SharedDirectory>>,
    after_write: Option<(PathBuf, u64, AfterWrite)>,
    fired: Cell<bool>,
}
impl HostChecks {
    fn new() -> Self {
        Self {
            denied: Cell::new(false),
            deny_attempts: Cell::new(0),
            panic_deny: Cell::new(false),
            inspect_pin: None,
            after_write: None,
            fired: Cell::new(false),
        }
    }
    fn deny_actual(&self) {
        self.denied.set(true);
        self.deny_attempts.set(self.deny_attempts.get() + 1);
        if let Some(shared) = &self.inspect_pin {
            assert!(
                matches!(shared.native.try_lock(), Err(TryLockError::WouldBlock)),
                "real private pin still held through composing denial"
            );
        }
        if self.panic_deny.get() {
            std::panic::panic_any("actual append denial panic");
        }
    }
}
impl ShardDirectoryChecks for HostChecks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        if self.denied.get() {
            return Err(ShardDirectoryCheckFailure::Refused);
        }
        if !self.fired.get() {
            if let Some((path, initial, action)) = &self.after_write {
                if fs::metadata(path)
                    .expect("actual append file metadata")
                    .len()
                    > *initial
                {
                    self.fired.set(true);
                    match action {
                        AfterWrite::Panic => {
                            std::panic::panic_any(io::Error::other("after actual native write"))
                        }
                        AfterWrite::Expire(budget) => {
                            assert!(budget.cap_expiry(Instant::now()).is_err());
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
impl PinnedShardAppendChecks for HostChecks {
    fn deny(&self) {
        self.deny_actual();
    }
}
impl PinnedStoreOpenChecks for HostChecks {
    fn deny(&self) {
        self.deny_actual();
    }
}

struct Temporary {
    path: PathBuf,
    target: u64,
}
impl Temporary {
    fn new(target: u64) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "core-owned-append-{}-{:020}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual directory");
        // Ordinary open creates a genuine empty native pack. It is closed
        // before acquiring a new actual OS root and native lease for Core.
        drop(
            ShardStore::open(ShardStoreConfig {
                directory: path.clone(),
                shard_id: ShardId::new(3),
                target_pack_bytes: target,
                max_batch_bytes: 4096,
            })
            .expect("actual initial native directory"),
        );
        Self { path, target }
    }
    fn config(&self) -> ShardStoreConfig {
        ShardStoreConfig {
            directory: self.path.clone(),
            shard_id: ShardId::new(3),
            target_pack_bytes: self.target,
            max_batch_bytes: 4096,
        }
    }
    fn lock(&self) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.path.join(".shard-store.lock"))
            .expect("actual lock file")
    }
    fn open(&self, root: &OwnedDirectoryHandle) -> OwnedPinnedShardStore {
        let setup = budget(core_limits());
        let guard = ShardRecoveryGuard::try_acquire_until(
            self.lock(),
            self.path.clone(),
            ShardGuardAcquireLimits {
                max_path_bytes: 8192,
                max_io_calls: 5,
            },
            setup.absolute_expiry(),
        )
        .expect("genuinely acquired same-OFD lease");
        root.pin_shard_directory_until(
            guard,
            PinnedShardDirectoryLimits {
                max_path_bytes: 8192,
                max_io_calls: 4,
            },
            &setup,
        )
        .expect("actual Core-derived pin")
        .open_store_until(self.config(), open_limits(), &setup, &HostChecks::new())
        .expect("actual Core consuming store open")
    }
    fn contended(&self) {
        assert!(matches!(
            DirectoryOwnership::acquire(&self.path),
            Err(DirectoryOwnershipError::Contended)
        ));
        assert!(ShardRecoveryGuard::try_acquire(self.lock(), self.path.clone()).is_err());
    }
    fn free(&self) {
        drop(DirectoryOwnership::acquire(&self.path).expect("actual last Core owner released"));
        drop(
            ShardRecoveryGuard::try_acquire(self.lock(), self.path.clone())
                .expect("actual last pin released"),
        );
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
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
        max_operations: 4096,
        max_calls: 1_000_000,
        max_entries: 1_000_000,
        max_declared_bytes: 1 << 28,
        max_work_bytes: 1 << 30,
    }
}
fn budget(limits: NativeDirectoryLimits) -> NativeDirectoryBudget {
    NativeDirectoryBudget::new(limits, expiry()).expect("actual command counters")
}
fn open_limits() -> PinnedStoreOpenLimits {
    PinnedStoreOpenLimits {
        max_directory_visits: 128,
        directory_buffer_bytes: 8192,
        max_name_bytes: 1024,
        max_path_bytes: 8192,
        max_packs: 32,
        max_frames: 64,
        max_frame_bytes: 8192,
        max_read_attempts: 256,
        max_read_bytes: 1 << 22,
        max_index_entries: 256,
        max_repair_calls: 64,
        max_io_calls: 512,
        max_outcomes: 1024,
        max_entries: 1 << 16,
        max_declared_bytes: 1 << 22,
        max_work_bytes: 1 << 26,
    }
}
fn limits() -> OwnedPinnedShardAppendLimits {
    OwnedPinnedShardAppendLimits {
        max_input_capacity: 32,
        native: PinnedShardAppendLimits {
            max_operations: 1,
            max_frames: 16,
            max_payload_bytes: 8192,
            max_segments: 16,
            max_rotations: 16,
            max_path_bytes: 8192,
            max_name_bytes: 1024,
            max_packs: 32,
            max_batch_slots: 256,
            max_topic_slots: 64,
            max_location_slots: 256,
            max_io_calls: 512,
            max_write_calls: 256,
            max_write_bytes: 1 << 22,
            max_work_bytes: 1 << 25,
        },
    }
}
fn reservation(batch: u128) -> Reservation {
    Reservation {
        topic_id: TopicId::new(21),
        partition_id: LogicalPartitionId::new(2),
        batch_id: BatchId::new(batch),
        first_offset: LogicalOffset::new(batch as u64),
        last_offset: LogicalOffset::new(batch as u64),
        record_count: NonZeroU32::new(1).expect("one real record"),
        placement: Placement {
            virtual_lane_id: VirtualLaneId::new(3),
            ring_epoch: RingEpoch::new(1),
            leader_epoch: LeaderEpoch::new(7),
            sequence: PlacementSequence::new(batch as u64),
        },
    }
}
fn input(batch: u128) -> PinnedShardAppendInput {
    PinnedShardAppendInput {
        reservation: reservation(batch),
        producer: None,
        atomic_group: None,
        payload: b"actual Core append body".to_vec().into(),
    }
}
fn owned(
    path: &Temporary,
) -> (
    Arc<DirectoryOwnership>,
    OwnedDirectoryHandle,
    OwnedPinnedShardStore,
) {
    let owner = DirectoryOwnership::acquire(&path.path).expect("actual Core root");
    let root = owner.root_handle().expect("actual root handle");
    let store = path.open(&root);
    (owner, root, store)
}

#[test]
fn actual_core_append_rotation_seal_reopens_exact_native_frames_and_epochs() {
    let path = Temporary::new(256);
    let (owner, root, store) = owned(&path);
    let paid = budget(core_limits());
    let checks = HostChecks::new();
    let complete = store
        .append_group_until(
            vec![input(0), input(1), input(2)],
            true,
            limits(),
            &paid,
            &checks,
        )
        .expect("genuine grouped Core append");
    assert_eq!(complete.report().installed_frames(), 3);
    assert_eq!(
        complete.report().inputs()[2]
            .reservation
            .placement
            .leader_epoch,
        LeaderEpoch::new(7)
    );
    assert!(complete.report().write_results().all(Result::is_ok));
    assert!(complete.report().usage().rotations > 0);
    assert_eq!(complete.report().usage().operations, 1);
    let before_seal = paid.usage().expect("same cumulative fees");
    let complete = complete
        .into_store()
        .seal_active_until(limits(), &paid, &checks)
        .expect("actual same owner seal");
    assert!(complete.report().sealed_pack().is_some());
    assert!(
        paid.usage()
            .expect("unchanged counters grow")
            .declared_bytes
            > before_seal.declared_bytes
    );
    let actual = complete.store().store().catalog();
    assert_eq!(actual.len(), 3);
    drop(complete);
    drop(root);
    drop(owner);
    path.free();
    let reopened = ShardStore::open(path.config()).expect("actual native reopen");
    assert_eq!(reopened.catalog().len(), 3);
    for n in 0..3 {
        assert!(reopened.contains(&reservation(n)));
    }
}

#[test]
fn real_leading_zero_active_name_survives_core_append_and_seal_without_reconstruction() {
    let path = Temporary::new(4096);
    let mut initial = ShardStore::open(path.config()).expect("real original writer");
    initial
        .append(reservation(0), None, b"original body", true)
        .expect("real native frame");
    let original_path = initial.active_path().to_path_buf();
    drop(initial);
    let name = format!("pack-{}.sse", "0".repeat(192));
    let actual_path = path.path.join(&name);
    fs::rename(original_path, &actual_path).expect("genuine accepted leading-zero spelling");
    let first = fs::read(&actual_path).expect("original native bytes");
    let (owner, root, store) = owned(&path);
    assert_eq!(store.store().active_path(), actual_path);
    let paid = budget(core_limits());
    let checks = HostChecks::new();
    let appended = store
        .append_group_until(vec![input(1)], true, limits(), &paid, &checks)
        .expect("actual Core append to native discovered spelling");
    assert_eq!(
        appended.report().original_active_name(),
        Some(std::ffi::OsStr::new(&name))
    );
    assert!(
        fs::read(&actual_path)
            .expect("appended native bytes")
            .starts_with(&first)
    );
    let sealed = appended
        .into_store()
        .seal_active_until(limits(), &paid, &checks)
        .expect("actual Core seal");
    assert_eq!(
        sealed
            .report()
            .sealed_pack()
            .expect("actual sealed pack")
            .path,
        actual_path
    );
    drop(sealed);
    drop(root);
    drop(owner);
    let reopened = ShardStore::open(path.config()).expect("actual native reopen");
    assert!(reopened.contains(&reservation(0)) && reopened.contains(&reservation(1)));
}

#[test]
fn actual_vec_capacity_above_len_refuses_before_entry_and_retains_original_allocation() {
    let path = Temporary::new(4096);
    let (owner, root, store) = owned(&path);
    let weak = Arc::downgrade(&owner);
    let original = fs::read(store.store().active_path()).expect("real initial bytes");
    let mut inputs = Vec::with_capacity(129);
    inputs.push(input(0));
    let cap = inputs.capacity();
    let ptr = inputs.as_ptr();
    let paid = budget(core_limits());
    let mut policy = limits();
    policy.max_input_capacity = cap - 1;
    let checks = HostChecks::new();
    let error = store
        .append_group_until(inputs, true, policy, &paid, &checks)
        .expect_err("actual capacity, not len1, exceeds cap");
    assert_eq!(error.input_capacity(), cap);
    assert_eq!(error.inputs().as_ptr(), ptr);
    assert_eq!(error.inputs().len(), 1);
    assert!(error.native_report().is_none());
    assert_eq!(
        paid.usage().expect("zero fees before entry").declared_bytes,
        0
    );
    assert_eq!(
        fs::read(path.path.join("pack-00000000000000000000.sse")).expect("unchanged"),
        original
    );
    assert!(checks.denied.get());
    drop(root);
    drop(owner);
    path.contended();
    assert!(weak.upgrade().is_some());
    drop(error);
    assert!(weak.upgrade().is_none());
    path.free();
}

#[test]
fn exact_actual_capacity_fee_enters_and_one_less_declared_fee_stops_before_entry() {
    for short in [true, false] {
        let path = Temporary::new(4096);
        let (owner, root, store) = owned(&path);
        let mut inputs = Vec::with_capacity(9);
        inputs.push(input(0));
        let mut policy = limits();
        policy.max_input_capacity = inputs.capacity();
        let cost = owned_pinned_shard_append_cost(policy).expect("checked actual-capacity fee");
        let empty = owned_pinned_shard_append_cost(OwnedPinnedShardAppendLimits {
            max_input_capacity: 0,
            native: policy.native,
        })
        .expect("fixed/native fee");
        assert_eq!(
            cost.declared_bytes - empty.declared_bytes,
            inputs.capacity() * size_of::<PinnedShardAppendInput>()
        );
        let mut caps = core_limits();
        caps.max_declared_bytes = cost.declared_bytes - usize::from(short);
        let paid = budget(caps);
        let entered = Arc::new(Cell::new(false));
        let seen = entered.clone();
        let _observe = observe(move |phase| {
            if phase == Phase::InputsAdmitted {
                seen.set(true);
            }
        });
        let checks = HostChecks::new();
        let error = store
            .append_group_until(inputs, true, policy, &paid, &checks)
            .expect_err("exact bridge fee leaves no additional parent-validation allowance");
        assert_eq!(entered.get(), !short);
        assert_eq!(
            paid.usage().expect("actual charged fees").declared_bytes,
            if short { 0 } else { cost.declared_bytes }
        );
        assert!(error.native_report().is_none());
        assert!(checks.denied.get());
        drop(error);
        drop(root);
        drop(owner);
    }
}

#[test]
fn every_actual_core_phase_retains_results_pin_and_whole_root_on_expiry_or_panic() {
    for phase in [
        Phase::InputsAdmitted,
        Phase::ParentValidated,
        Phase::PinBorrowed,
        Phase::NativeCompleted,
        Phase::FinalParentValidated,
    ] {
        for panic in [false, true] {
            let path = Temporary::new(4096);
            let (owner, root, store) = owned(&path);
            let paid = budget(core_limits());
            let same = paid.clone();
            let weak = Arc::downgrade(&owner);
            let shared = store.directory.inner.clone();
            let mut checks = HostChecks::new();
            if matches!(
                phase,
                Phase::PinBorrowed | Phase::NativeCompleted | Phase::FinalParentValidated
            ) {
                checks.inspect_pin = Some(shared.clone());
            }
            let _observe = observe(move |actual| {
                if actual == phase {
                    if panic {
                        std::panic::panic_any(phase);
                    }
                    assert!(same.cap_expiry(Instant::now()).is_err());
                }
            });
            let error = store
                .append_group_until(vec![input(0)], true, limits(), &paid, &checks)
                .expect_err("real entered phase refuses");
            assert_eq!(
                error.failure(),
                if panic {
                    OwnedPinnedShardAppendFailure::Unwind
                } else {
                    OwnedPinnedShardAppendFailure::Budget
                }
            );
            if panic {
                assert_eq!(
                    error
                        .panic_payload()
                        .and_then(|p| p.downcast_ref::<Phase>()),
                    Some(&phase)
                );
            }
            assert_eq!(error.inputs().len(), 1);
            assert!(checks.denied.get());
            assert_eq!(checks.deny_attempts.get(), 1);
            assert!(paid.check().is_err());
            if matches!(phase, Phase::NativeCompleted | Phase::FinalParentValidated) {
                assert_eq!(
                    error
                        .native_report()
                        .expect("completed actual report")
                        .installed_frames(),
                    1
                );
                assert!(
                    error
                        .native_report()
                        .expect("actual writes")
                        .write_results()
                        .any(Result::is_ok)
                );
            } else {
                assert!(error.native_report().is_none());
            }
            drop(checks);
            drop(shared);
            drop(root);
            drop(owner);
            assert!(weak.upgrade().is_some());
            path.contended();
            drop(error);
            assert!(weak.upgrade().is_none());
            path.free();
        }
    }
}

#[test]
fn real_native_write_completion_then_panic_or_elapsed_e_retains_actual_raw_settlement() {
    for panic in [false, true] {
        let path = Temporary::new(4096);
        let (owner, root, store) = owned(&path);
        let active = store.store().active_path().to_path_buf();
        let initial = fs::metadata(&active).expect("actual original file").len();
        let paid = budget(core_limits());
        let mut checks = HostChecks::new();
        checks.inspect_pin = Some(store.directory.inner.clone());
        checks.after_write = Some((
            active,
            initial,
            if panic {
                AfterWrite::Panic
            } else {
                AfterWrite::Expire(paid.clone())
            },
        ));
        let error = store
            .append_group_until(vec![input(0)], true, limits(), &paid, &checks)
            .expect_err("real next checkpoint follows an actual successful write");
        assert!(checks.fired.get());
        let native = error
            .native_failure()
            .expect("actual native producer refusal");
        let report = error.native_report().expect("actual safe observation view");
        assert!(report.write_results().any(Result::is_ok));
        assert!(
            !report.unit_results().collect::<Vec<_>>().is_empty(),
            "native paid settlement actually entered"
        );
        if panic {
            assert_eq!(native, PinnedShardAppendFailure::Unwind);
            assert!(
                report
                    .panic_payload()
                    .and_then(|p| p.downcast_ref::<io::Error>())
                    .is_some()
            );
        }
        assert!(checks.denied.get());
        assert_eq!(checks.deny_attempts.get(), 1);
        assert!(error.pin_borrowed());
        assert!(paid.check().is_err());
        drop(checks);
        drop(root);
        drop(owner);
        path.contended();
        drop(error);
        path.free();
    }
}

#[test]
fn actual_missing_named_active_native_io_error_retains_original_file_input_and_root() {
    let path = Temporary::new(4096);
    let (owner, root, store) = owned(&path);
    let active = store.store().active_path().to_path_buf();
    let retained = path.path.join("actual-original-active-retained.bin");
    let original = fs::read(&active).expect("actual original body");
    let _observe = observe(move |phase| {
        if phase == Phase::PinBorrowed {
            fs::rename(&active, &retained)
                .expect("actual namespace disappearance after pin borrow");
        }
    });
    let checks = HostChecks::new();
    let paid = budget(core_limits());
    let error = store
        .append_group_until(vec![input(0)], true, limits(), &paid, &checks)
        .expect_err("actual no-follow metadata call returns NotFound");
    let report = error.native_report().expect("real native partial report");
    assert!(report.metadata_results().any(|(_, result)| result.is_err()));
    assert!(
        !std::error::Error::source(&error)
            .is_some_and(|source| source.downcast_ref::<PinnedShardAppendError>().is_some()),
        "dyn Error must not downcast to the raw FD-bearing native wrapper"
    );
    assert!(report.write_results().next().is_none());
    assert_eq!(
        fs::read(path.path.join("actual-original-active-retained.bin"))
            .expect("actual retained original"),
        original
    );
    assert_eq!(error.inputs().len(), 1);
    assert!(checks.denied.get());
    drop(root);
    drop(owner);
    path.contended();
    drop(error);
    path.free();
}

#[test]
fn opaque_actual_report_and_whole_store_keep_same_root_in_both_drop_orders() {
    fn send<T: Send>() {}
    send::<OwnedPinnedShardAppendComplete>();
    send::<OwnedPinnedShardAppendReport>();
    send::<RetainedOwnedPinnedShardAppendError>();
    for store_first in [false, true] {
        let path = Temporary::new(256);
        let (owner, root, store) = owned(&path);
        let weak = Arc::downgrade(&owner);
        let completed = store
            .append_group_until(
                vec![input(0), input(1), input(2)],
                true,
                limits(),
                &budget(core_limits()),
                &HostChecks::new(),
            )
            .expect("genuine actual append");
        let (store, report) = completed.into_store_and_report();
        // Test-only private access confirms actual retired producer FDs remain
        // privately retained; the public view has no File/native-report getter.
        assert!(report.report.retired_files().count() > 0);
        assert!(
            report
                .report
                .retired_files()
                .all(|file| file.metadata().is_ok())
        );
        assert!(report.report().write_observations().next().is_some());
        assert!(Arc::ptr_eq(report.owner(), store.owner()));
        assert_eq!(
            report.report().inputs()[0].payload.as_ref(),
            b"actual Core append body"
        );
        drop(root);
        drop(owner);
        if store_first {
            drop(store);
            path.contended();
            assert!(weak.upgrade().is_some());
            drop(report);
        } else {
            drop(report);
            path.contended();
            assert!(weak.upgrade().is_some());
            drop(store);
        }
        assert!(weak.upgrade().is_none());
        path.free();
        assert!(
            ShardStore::open(path.config())
                .expect("actual reopen")
                .contains(&reservation(0))
        );
    }
}

#[test]
fn actual_private_pin_busy_poison_and_denial_panic_preserve_typed_custody() {
    for poisoned in [false, true] {
        let path = Temporary::new(4096);
        let (owner, root, store) = owned(&path);
        let shared = store.directory.inner.clone();
        let held = if poisoned {
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    let _held = shared.native.lock().expect("actual input pin");
                    std::panic::panic_any("actual pin poison");
                }))
                .is_err()
            );
            None
        } else {
            Some(shared.native.lock().expect("actual contending borrow"))
        };
        let checks = HostChecks::new();
        checks.panic_deny.set(true);
        let paid = budget(core_limits());
        let error = store
            .append_group_until(vec![input(0)], true, limits(), &paid, &checks)
            .expect_err("actual typed private borrow refuses");
        assert_eq!(error.failure(), OwnedPinnedShardAppendFailure::PinBorrow);
        assert_eq!(error.pin_poisoned(), poisoned);
        assert_eq!(error.pin_would_block(), !poisoned);
        assert!(
            error
                .deny_panic_payload()
                .and_then(|p| p.downcast_ref::<&str>())
                .is_some()
        );
        assert!(!error.denial_completed());
        assert!(error.native_report().is_none());
        assert!(paid.check().is_err());
        assert_eq!(checks.deny_attempts.get(), 1);
        drop(held);
        drop(shared);
        drop(root);
        drop(owner);
        path.contended();
        drop(error);
        path.free();
    }
}

#[test]
fn payload_scan_is_prepaid_and_retains_actual_oversize_input_without_native_entry() {
    let path = Temporary::new(4096);
    let (owner, root, store) = owned(&path);
    let mut policy = limits();
    policy.native.max_payload_bytes = 1;
    let paid = budget(core_limits());
    let checks = HostChecks::new();
    let inputs = vec![input(0)];
    let ptr = inputs.as_ptr();
    let error = store
        .append_group_until(inputs, true, policy, &paid, &checks)
        .expect_err("actual payload len exceeds original configured native maximum");
    assert_eq!(error.failure(), OwnedPinnedShardAppendFailure::Budget);
    assert_eq!(error.inputs().as_ptr(), ptr);
    assert!(error.native_usage().is_none());
    assert!(paid.usage().expect("prepaid before scan").declared_bytes > 0);
    assert!(checks.denied.get());
    drop(root);
    drop(owner);
    drop(error);
    let mut overflow = limits();
    overflow.max_input_capacity = usize::MAX;
    assert!(owned_pinned_shard_append_cost(overflow).is_none());
}

#[test]
fn actual_native_work_refusal_has_real_report_without_reentering_or_resetting_counters() {
    let path = Temporary::new(4096);
    let (owner, root, store) = owned(&path);
    let paid = budget(core_limits());
    let checks = HostChecks::new();
    let mut policy = limits();
    policy.native.max_work_bytes = 1;
    let error = store
        .append_group_until(vec![input(0)], true, policy, &paid, &checks)
        .expect_err("actual native configured work refuses");
    assert_eq!(error.failure(), OwnedPinnedShardAppendFailure::Native);
    assert_eq!(
        error
            .native_report()
            .expect("actual native input report")
            .inputs()
            .len(),
        1
    );
    assert!(
        error
            .native_usage()
            .expect("same actual native budget")
            .operations
            <= 1
    );
    assert_eq!(error.native_expiry(), Some(paid.absolute_expiry()));
    assert!(checks.denied.get());
    assert!(paid.check().is_err());
    drop(root);
    drop(owner);
    path.contended();
    drop(error);
    path.free();
}

#[test]
fn complete_actual_core_cost_fits_exactly_and_each_one_less_retains_truthful_results() {
    // Independent real commands/fixtures establish actual source fees; no
    // counter is reset/refunded within any command and no Session is forged.
    let measured = {
        let path = Temporary::new(4096);
        let (owner, root, store) = owned(&path);
        let paid = budget(core_limits());
        let mut inputs = Vec::with_capacity(9);
        inputs.push(input(0));
        let ptr = inputs.as_ptr();
        let complete = store
            .append_group_until(inputs, true, limits(), &paid, &HostChecks::new())
            .expect("actual complete native append and parent validations");
        assert_eq!(complete.report().inputs().as_ptr(), ptr);
        let actual = paid.usage().expect("actual complete cumulative fees");
        drop(complete);
        drop(root);
        drop(owner);
        actual
    };
    for axis in 0..4 {
        for short in [false, true] {
            let path = Temporary::new(4096);
            let (owner, root, store) = owned(&path);
            let mut caps = core_limits();
            match axis {
                0 => caps.max_declared_bytes = measured.declared_bytes - usize::from(short),
                1 => caps.max_entries = measured.entries - usize::from(short),
                2 => caps.max_calls = measured.calls - usize::from(short),
                3 => caps.max_work_bytes = measured.work_bytes - usize::from(short),
                _ => unreachable!("fixed actual axes"),
            }
            let paid = budget(caps);
            let checks = HostChecks::new();
            let mut inputs = Vec::with_capacity(9);
            inputs.push(input(0));
            let result = store.append_group_until(inputs, true, limits(), &paid, &checks);
            if short {
                let error = result.expect_err("one actual source fee less refuses");
                assert!(error.budget_error().is_some() || error.directory_error().is_some());
                assert_eq!(error.inputs().len(), 1);
                if let Some(report) = error.native_report() {
                    assert_eq!(
                        report.installed_frames(),
                        1,
                        "late refusal retains completed native success"
                    );
                    assert!(report.write_results().any(Result::is_ok));
                }
                assert!(checks.denied.get() && paid.check().is_err());
                drop(error);
            } else {
                let complete = result.expect("exact genuine complete source fee fits");
                assert_eq!(complete.report().installed_frames(), 1);
                assert!(!checks.denied.get());
                drop(complete);
            }
            drop(root);
            drop(owner);
            path.free();
        }
    }
}
