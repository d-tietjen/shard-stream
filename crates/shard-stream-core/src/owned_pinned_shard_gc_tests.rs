//! Actual Core/native collector custody regressions; no host/authority witness.

use super::super::SharedDirectory;
use super::*;
use crate::{
    BatchId, DirectoryOwnershipError, LeaderEpoch, LogicalPartitionId, NativeDirectoryLimits,
    Placement, PlacementSequence, RingEpoch, ShardId, TopicId, VirtualLaneId,
};
use shardlog::{
    PinnedShardDirectoryLimits, PinnedStoreOpenChecks, PinnedStoreOpenLimits, Reservation,
    ShardGuardAcquireLimits, ShardRecoveryGuard, ShardStore, ShardStoreConfig,
};
use std::fs::{self, File, OpenOptions};
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

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

struct HostChecks {
    denied: Cell<bool>,
    deny_attempts: Cell<usize>,
    panic_deny: Cell<bool>,
    // A real SAME pin clone used only to observe physical borrowing in tests.
    inspect_pin_on_deny: Option<Arc<SharedDirectory>>,
}
impl HostChecks {
    fn new() -> Self {
        Self {
            denied: Cell::new(false),
            deny_attempts: Cell::new(0),
            panic_deny: Cell::new(false),
            inspect_pin_on_deny: None,
        }
    }
    fn latch(&self) {
        self.denied.set(true);
        self.deny_attempts.set(self.deny_attempts.get() + 1);
        if let Some(shared) = &self.inspect_pin_on_deny {
            assert!(
                matches!(shared.native.try_lock(), Err(TryLockError::WouldBlock)),
                "actual producer pin borrow must still be held during denial"
            );
        }
        if self.panic_deny.get() {
            std::panic::panic_any("actual GC composing denial unwind");
        }
    }
}
impl ShardDirectoryChecks for HostChecks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        if self.denied.get() {
            Err(ShardDirectoryCheckFailure::Refused)
        } else {
            Ok(())
        }
    }
}
impl PinnedStoreOpenChecks for HostChecks {
    fn deny(&self) {
        self.latch();
    }
}
impl PinnedShardGcChecks for HostChecks {
    fn deny(&self) {
        self.latch();
    }
}

struct Temporary {
    path: PathBuf,
    first_sealed: PathBuf,
}
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "owned-pinned-GC-{}-{:020}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual native directory");
        let config = ShardStoreConfig {
            directory: path.clone(),
            shard_id: ShardId::new(4),
            target_pack_bytes: 1 << 20,
            max_batch_bytes: 4096,
        };
        let mut writer = ShardStore::open(config).expect("real ordinary fixture writer");
        let first_sealed = writer.active_path().to_path_buf();
        for n in 0..3 {
            writer
                .append(
                    reservation(n),
                    None,
                    b"actual collector fixture frame",
                    true,
                )
                .expect("real native frame and durability");
            if n < 2 {
                writer
                    .seal_active()
                    .expect("actual rotation")
                    .expect("nonempty sealed pack");
            }
        }
        drop(writer);
        Self { path, first_sealed }
    }
    fn config(&self) -> ShardStoreConfig {
        ShardStoreConfig {
            directory: self.path.clone(),
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
            .open(self.path.join(".shard-store.lock"))
            .expect("actual native lock file")
    }
    fn open(
        &self,
        root: &OwnedDirectoryHandle,
        paid: &NativeDirectoryBudget,
    ) -> OwnedPinnedShardStore {
        let acquired = ShardRecoveryGuard::try_acquire_until(
            self.lock(),
            self.path.clone(),
            ShardGuardAcquireLimits {
                max_path_bytes: 8192,
                max_io_calls: 5,
            },
            paid.absolute_expiry(),
        )
        .expect("genuine acquired guard, no old guard conversion");
        root.pin_shard_directory_until(
            acquired,
            PinnedShardDirectoryLimits {
                max_path_bytes: 8192,
                max_io_calls: 4,
            },
            paid,
        )
        .expect("actual Core-derived native pin")
        .open_store_until(self.config(), open_limits(), paid, &HostChecks::new())
        .expect("actual same-budget bounded native open")
    }
    fn contended(&self) {
        assert!(matches!(
            DirectoryOwnership::acquire(&self.path),
            Err(DirectoryOwnershipError::Contended)
        ));
        assert!(ShardRecoveryGuard::try_acquire(self.lock(), self.path.clone()).is_err());
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
        max_operations: 1024,
        max_calls: 1_000_000,
        max_entries: 1_000_000,
        max_declared_bytes: 1 << 28,
        max_work_bytes: 1 << 30,
    }
}
fn budget(limits: NativeDirectoryLimits) -> NativeDirectoryBudget {
    NativeDirectoryBudget::new(limits, expiry()).expect("actual same bounded Core counters")
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
fn gc_limits() -> PinnedShardGcLimits {
    PinnedShardGcLimits {
        max_path_bytes: 8192,
        max_name_bytes: 1024,
        max_packs: 16,
        max_batch_slots: 256,
        max_log_start_slots: 256,
        max_topic_slots: 256,
        max_location_slots: 256,
        max_plan_packs: 16,
        max_io_calls: 128,
        max_work_bytes: 1 << 25,
    }
}
fn reservation(batch: u128) -> Reservation {
    Reservation {
        topic_id: TopicId::new(19),
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
fn floors() -> HashMap<TopicPartition, LogicalOffset> {
    HashMap::from([(
        TopicPartition::new(TopicId::new(19), LogicalPartitionId::new(2)),
        LogicalOffset::new(2),
    )])
}

#[test]
fn actual_open_collect_and_whole_owner_return_keep_original_pin_and_core_parent() {
    fn send<T: Send>() {}
    send::<OwnedPinnedShardGcComplete>();
    send::<RetainedOwnedPinnedShardGcError>();
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.path).expect("actual Core owner");
    let weak = Arc::downgrade(&owner);
    let root = owner.root_handle().expect("actual Core FD");
    let paid = budget(core_limits());
    let owned = path.open(&root, &paid);
    let before = paid.usage().expect("actual cumulative pin/open fees");
    let checks = HostChecks::new();
    let complete = owned
        .garbage_collect_until(&floors(), gc_limits(), &paid, &checks)
        .expect("actual same-pin bounded GC");
    assert_eq!(complete.report().stats().packs_removed, 2);
    assert!(Arc::ptr_eq(complete.store().owner(), &owner));
    assert!(!path.first_sealed.exists());
    assert!(!checks.denied.get());
    assert!(paid.usage().expect("actual later fees").calls > before.calls);
    let returned = complete.into_store();
    assert!(!returned.store().contains(&reservation(0)));
    assert!(!returned.store().contains(&reservation(1)));
    assert!(returned.store().contains(&reservation(2)));
    let retained = returned.retain_directory();
    drop(paid);
    drop(root);
    drop(owner);
    drop(returned);
    path.contended();
    assert!(weak.upgrade().is_some());
    drop(retained);
    assert!(weak.upgrade().is_none());
    DirectoryOwnership::acquire(&path.path).expect("actual last Core parent released");
    ShardRecoveryGuard::try_acquire(path.lock(), path.path.clone())
        .expect("actual last pin released");
}

#[test]
fn all_bridge_phases_keep_actual_store_and_native_results_on_expiry_or_unwind() {
    for target in [
        Phase::ParentValidated,
        Phase::PinBorrowed,
        Phase::NativeCompleted,
        Phase::FinalParentValidated,
    ] {
        for unwind in [false, true] {
            let path = Temporary::new();
            let owner = DirectoryOwnership::acquire(&path.path).expect("actual Core owner");
            let root = owner.root_handle().expect("actual Core FD");
            let paid = budget(core_limits());
            let same = paid.clone();
            let owned = path.open(&root, &paid);
            let _observe = observe(move |phase| {
                if phase == target {
                    if unwind {
                        std::panic::panic_any(target);
                    }
                    assert!(same.cap_expiry(Instant::now()).is_err());
                }
            });
            let checks = HostChecks::new();
            let error = owned
                .garbage_collect_until(&floors(), gc_limits(), &paid, &checks)
                .expect_err("actual completed phase refuses");
            assert_eq!(
                error.failure(),
                if unwind {
                    OwnedPinnedShardGcFailure::Unwind
                } else {
                    OwnedPinnedShardGcFailure::Budget
                }
            );
            assert_eq!(error.pin_borrowed(), target != Phase::ParentValidated);
            assert_eq!(
                error.native_report().is_some(),
                matches!(target, Phase::NativeCompleted | Phase::FinalParentValidated)
            );
            if let Some(actual) = error.native_report() {
                assert_eq!(actual.stats().packs_removed, 2);
            }
            assert!(checks.denied.get());
            assert!(error.denial_completed());
            assert!(Arc::ptr_eq(error.owner(), &owner));
            if unwind {
                assert_eq!(
                    error
                        .panic_payload()
                        .expect("original exact phase payload")
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
            DirectoryOwnership::acquire(&path.path)
                .expect("whole actual input retained until error drop");
            ShardRecoveryGuard::try_acquire(path.lock(), path.path.clone())
                .expect("same actual native lock released last");
        }
    }
}

#[test]
fn unpaid_whole_gc_ceiling_retains_input_before_pin_borrow_or_native_io() {
    let path = Temporary::new();
    let before = fs::read(&path.first_sealed).expect("real sealed bytes");
    let owner = DirectoryOwnership::acquire(&path.path).expect("actual Core owner");
    let root = owner.root_handle().expect("actual Core FD");
    let owned = path.open(&root, &budget(core_limits()));
    let cost = owned_pinned_shard_gc_cost(gc_limits()).expect("actual native/fixed ceiling");
    let mut limits = core_limits();
    limits.max_declared_bytes = cost.declared_bytes - 1;
    let paid = budget(limits);
    let checks = HostChecks::new();
    let error = owned
        .garbage_collect_until(&floors(), gc_limits(), &paid, &checks)
        .expect_err("one-less prepayment must refuse before all producer work");
    assert!(matches!(
        error.budget_error(),
        Some(NativeDirectoryFailure::Capacity)
    ));
    assert!(!error.pin_borrowed());
    assert!(error.native_report().is_none());
    let usage = paid.usage().expect("actual refused-entry counters");
    assert_eq!(usage.calls, 0);
    assert_eq!(usage.operations, 0);
    assert_eq!(usage.declared_bytes, 0);
    assert_eq!(
        fs::read(&path.first_sealed).expect("actual sealed pack remains"),
        before
    );
    assert!(checks.denied.get());
    drop(root);
    drop(owner);
    path.contended();
    drop(error);
    DirectoryOwnership::acquire(&path.path)
        .expect("unpaid failure retained actual whole store/root");
}

#[test]
fn actual_missing_pack_error_retains_native_report_and_whole_mutable_store() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.path).expect("actual Core owner");
    let root = owner.root_handle().expect("actual Core FD");
    let paid = budget(core_limits());
    let owned = path.open(&root, &paid);
    let missing = path.first_sealed.clone();
    let _observe = observe(move |phase| {
        if phase == Phase::ParentValidated {
            fs::remove_file(&missing).expect("actual selected pack removed before native call");
        }
    });
    let checks = HostChecks::new();
    let error = owned
        .garbage_collect_until(&floors(), gc_limits(), &paid, &checks)
        .expect_err("actual native pack metadata must refuse");
    assert_eq!(error.failure(), OwnedPinnedShardGcFailure::Native);
    let native = error
        .native_error()
        .expect("actual native typed failure retained");
    assert_eq!(
        native
            .report()
            .io_error()
            .expect("original NotFound")
            .kind(),
        std::io::ErrorKind::NotFound
    );
    assert!(native.report().usage().entered_io_calls > 0);
    assert!(
        error.store.store().contains(&reservation(0)),
        "actual index not rewritten after failed native stat"
    );
    assert!(checks.denied.get());
    assert!(error.pin_borrowed());
    assert_eq!(checks.deny_attempts.get(), 1);
    drop(root);
    drop(owner);
    path.contended();
    drop(error);
    DirectoryOwnership::acquire(&path.path)
        .expect("native refusal retained actual whole store and Core root");
}

#[test]
fn actual_pin_guard_stays_held_through_distinct_bridge_and_deny_unwinds() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.path).expect("actual Core owner");
    let weak = Arc::downgrade(&owner);
    let root = owner.root_handle().expect("actual Core FD");
    let paid = budget(core_limits());
    let owned = path.open(&root, &paid);
    let shared = Arc::clone(&owned.directory.inner);
    let mut checks = HostChecks::new();
    checks.inspect_pin_on_deny = Some(Arc::clone(&shared));
    checks.panic_deny.set(true);
    let _observe = observe(move |phase| {
        if phase == Phase::PinBorrowed {
            std::panic::panic_any(Phase::PinBorrowed);
        }
    });
    let error = owned
        .garbage_collect_until(&floors(), gc_limits(), &paid, &checks)
        .expect_err("real entered bridge hook and actual composing deny unwind");
    assert_eq!(error.failure(), OwnedPinnedShardGcFailure::Unwind);
    assert_eq!(
        error
            .panic_payload()
            .expect("original producer payload")
            .downcast_ref::<Phase>(),
        Some(&Phase::PinBorrowed)
    );
    assert_eq!(
        error
            .deny_panic_payload()
            .expect("independent actual denial payload")
            .downcast_ref::<&'static str>(),
        Some(&"actual GC composing denial unwind")
    );
    assert_eq!(checks.deny_attempts.get(), 1);
    assert!(!error.denial_completed());
    assert!(error.pin_borrowed());
    assert!(error.native_report().is_none());
    assert!(matches!(paid.check(), Err(NativeDirectoryFailure::Denied)));
    assert!(
        shared.native.try_lock().is_ok(),
        "typed pin result released normally only after denial"
    );
    drop(checks);
    drop(shared);
    drop(root);
    drop(owner);
    path.contended();
    assert!(weak.upgrade().is_some());
    drop(error);
    assert!(weak.upgrade().is_none());
    DirectoryOwnership::acquire(&path.path)
        .expect("both panic payloads drop before same actual Core parent");
}

#[test]
fn actual_completed_collection_and_store_survive_final_core_identity_refusal() {
    let path = Temporary::new();
    let backup = path.path.with_extension("actual-core-parent");
    let owner = DirectoryOwnership::acquire(&path.path).expect("actual Core owner");
    let weak = Arc::downgrade(&owner);
    let root = owner.root_handle().expect("actual Core FD");
    let paid = budget(core_limits());
    let owned = path.open(&root, &paid);
    let source = path.path.clone();
    let destination = backup.clone();
    let _observe = observe(move |phase| {
        if phase == Phase::NativeCompleted {
            fs::rename(&source, &destination).expect("move actual original Core directory");
            fs::create_dir(&source).expect("install actual foreign directory at old pathname");
        }
    });
    let checks = HostChecks::new();
    let error = owned
        .garbage_collect_until(&floors(), gc_limits(), &paid, &checks)
        .expect_err("real post-GC Core validation must refuse changed identity");
    assert_eq!(error.failure(), OwnedPinnedShardGcFailure::Directory);
    assert!(error.directory_error().is_some());
    assert_eq!(
        error
            .native_report()
            .expect("actual collection completed before bridge refused")
            .stats()
            .packs_removed,
        2
    );
    assert!(error.native_error().is_none());
    assert!(error.store.store().contains(&reservation(2)));
    assert!(!error.store.store().contains(&reservation(0)));
    assert!(checks.denied.get());
    drop(root);
    drop(owner);
    assert!(weak.upgrade().is_some());
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(backup.join(".shard-store.lock"))
        .expect("same actual acquired lock moved with original directory");
    assert!(ShardRecoveryGuard::try_acquire(lock, backup.clone()).is_err());
    drop(error);
    assert!(weak.upgrade().is_none());
    fs::remove_dir_all(&backup)
        .expect("clean isolated original directory after whole custody drops");
}

#[test]
fn actual_private_pin_would_block_and_poison_results_remain_observable_after_denial() {
    for poison in [false, true] {
        let path = Temporary::new();
        let owner = DirectoryOwnership::acquire(&path.path).expect("actual Core owner");
        let root = owner.root_handle().expect("actual Core FD");
        let paid = budget(core_limits());
        let owned = path.open(&root, &paid);
        let shared = Arc::clone(&owned.directory.inner);
        let mut held = None;
        let mut original_panic = None;
        if poison {
            let actual = catch_unwind(AssertUnwindSafe(|| {
                let _actual_pin = shared.native.lock().expect("actual same private pin mutex");
                std::panic::panic_any("actual pin mutex fixture unwind");
            }));
            assert!(actual.is_err());
            original_panic = actual.err();
        } else {
            held = Some(shared.native.lock().expect("actual preexisting pin borrow"));
        }
        let checks = HostChecks::new();
        let error = owned
            .garbage_collect_until(&floors(), gc_limits(), &paid, &checks)
            .expect_err("actual private pin cannot be borrowed");
        assert_eq!(error.failure(), OwnedPinnedShardGcFailure::PinBorrow);
        assert_eq!(error.pin_poisoned(), poison);
        assert_eq!(error.pin_would_block(), !poison);
        assert!(!error.pin_borrowed());
        assert!(error.native_report().is_none());
        assert!(checks.denied.get());
        assert!(error.denial_completed());
        assert!(error.store.store().contains(&reservation(0)));
        drop(held);
        drop(original_panic);
        drop(shared);
        drop(root);
        drop(owner);
        path.contended();
        drop(error);
        DirectoryOwnership::acquire(&path.path)
            .expect("actual borrow refusal kept whole original owner");
    }
}
