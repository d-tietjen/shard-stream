//! Genuine filesystem history and whole Core/native lifetime regression bodies.

use super::*;
use crate::{DirectoryOwnership, DirectoryOwnershipError, ShardId};
use shardlog::{
    PinnedShardDirectoryLimits, PinnedStoreOpenChecks, PinnedStoreOpenLimits,
    ShardGuardAcquireLimits, ShardLog, ShardLogConfig, ShardRecoveryGuard, ShardStoreConfig,
    StorageFetchLimits,
};
use std::{
    cell::Cell,
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

type Observer = Box<dyn FnMut(Phase)>;
thread_local! { static OBSERVER: RefCell<Option<Observer>> = RefCell::new(None); }
pub(super) fn completed(phase: Phase) {
    OBSERVER.with(|slot| {
        if let Some(f) = &mut *slot.borrow_mut() {
            f(phase);
        }
    });
}
struct Observe;
impl Drop for Observe {
    fn drop(&mut self) {
        OBSERVER.with(|slot| *slot.borrow_mut() = None);
    }
}
fn observe(f: impl FnMut(Phase) + 'static) -> Observe {
    OBSERVER.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(f));
    });
    Observe
}
struct Checks {
    denied: Rc<Cell<bool>>,
    calls: Cell<usize>,
    denial_calls: Cell<usize>,
    panic_deny: Cell<bool>,
    core: Option<NativeDirectoryBudget>,
    native: Option<StorageFetchBudget>,
    action: RefCell<Option<Box<dyn FnMut(usize)>>>,
}
impl Checks {
    fn new() -> Self {
        Self {
            denied: Rc::new(Cell::new(false)),
            calls: Cell::new(0),
            denial_calls: Cell::new(0),
            panic_deny: Cell::new(false),
            core: None,
            native: None,
            action: RefCell::new(None),
        }
    }
    fn for_command(core: &NativeDirectoryBudget, native: &StorageFetchBudget) -> Self {
        Self {
            core: Some(core.clone()),
            native: Some(native.clone()),
            ..Self::new()
        }
    }
    fn deny_actual(&self) {
        self.denial_calls.set(self.denial_calls.get() + 1);
        self.denied.set(true);
        if let Some(core) = &self.core {
            assert!(
                core.check().is_err(),
                "SAME Core denied before captured health"
            );
        }
        if let Some(native) = &self.native {
            assert!(
                native.check().is_err(),
                "SAME native denied before captured health"
            );
        }
        if self.panic_deny.get() {
            std::panic::panic_any("actual outer-health denial unwind");
        }
    }
}
impl ShardDirectoryChecks for Checks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        let n = self.calls.get() + 1;
        self.calls.set(n);
        if let Some(f) = &mut *self.action.borrow_mut() {
            f(n);
        }
        if self.denied.get() {
            Err(ShardDirectoryCheckFailure::Refused)
        } else {
            Ok(())
        }
    }
}
impl CheckedDirectoryValidationChecks for Checks {
    fn deny(&self) {
        self.deny_actual();
    }
}
impl PinnedStoreOpenChecks for Checks {
    fn deny(&self) {
        self.denied.set(true);
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
        max_declared_bytes: 1 << 29,
        max_work_bytes: 1 << 31,
    }
}
fn native_limits() -> StorageFetchLimits {
    StorageFetchLimits {
        max_plan_batches: 1024,
        max_entries: 1 << 18,
        max_path_bytes: 8192,
        max_read_attempts: 1024,
        max_read_bytes: 1 << 22,
        max_declared_bytes: 1 << 26,
        max_work_bytes: 1 << 29,
    }
}
fn core(limits: NativeDirectoryLimits) -> NativeDirectoryBudget {
    NativeDirectoryBudget::new(limits, expiry()).expect("actual cumulative Core policy")
}
fn native(limits: StorageFetchLimits) -> StorageFetchBudget {
    StorageFetchBudget::new(limits, expiry()).expect("actual original shared native policy")
}
fn open_limits() -> PinnedStoreOpenLimits {
    PinnedStoreOpenLimits {
        max_directory_visits: 128,
        directory_buffer_bytes: 8192,
        max_name_bytes: 1024,
        max_path_bytes: 8192,
        max_packs: 32,
        max_frames: 128,
        max_frame_bytes: 8192,
        max_read_attempts: 1024,
        max_read_bytes: 1 << 24,
        max_index_entries: 1024,
        max_repair_calls: 128,
        max_io_calls: 2048,
        max_outcomes: 4096,
        max_entries: 1 << 19,
        max_declared_bytes: 1 << 26,
        max_work_bytes: 1 << 29,
    }
}
struct Fixture {
    root: PathBuf,
    shard: PathBuf,
    target: u64,
}
impl Fixture {
    fn new(sequences: &[u64], target: u64) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "core-owned-canonical-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("actual fixture root");
        let shard = root.join("shard");
        fs::create_dir(&shard).expect("actual child directory");
        let mut config = ShardLogConfig::new(&shard);
        config.target_pack_bytes = target;
        config.max_record_bytes = 4096;
        let mut writer = ShardLog::open(config).expect("actual ordinary history writer");
        for &sequence in sequences {
            // Bytes is inferred from the actual ordinary API, without a new
            // Core dependency or an invented native catalog/record constructor.
            writer
                .append_group(sequence, &[b"real canonical fixture".to_vec().into()], true)
                .expect("actual durable history");
        }
        drop(writer);
        Self {
            root,
            shard,
            target,
        }
    }
    fn lock_at(path: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join(".shard-store.lock"))
            .expect("actual same native lease file")
    }
    fn open(
        &self,
    ) -> (
        OwnedPinnedShardStore,
        Arc<DirectoryOwnership>,
        OwnedDirectoryHandle,
    ) {
        let owner = DirectoryOwnership::acquire(&self.root).expect("actual original Core OS owner");
        let root = owner.root_handle().expect("actual root descriptor");
        let child = root
            .open_directory(OsStr::new("shard"), false)
            .expect("actual anchored child");
        drop(root);
        let setup = core(core_limits());
        let acquired = ShardRecoveryGuard::try_acquire_until(
            Self::lock_at(&self.shard),
            self.shard.clone(),
            ShardGuardAcquireLimits {
                max_path_bytes: 8192,
                max_io_calls: 5,
            },
            setup.absolute_expiry(),
        )
        .expect("actual acquired native lease");
        let directory = child
            .pin_shard_directory_until(
                acquired,
                PinnedShardDirectoryLimits {
                    max_path_bytes: 8192,
                    max_io_calls: 4,
                },
                &setup,
            )
            .expect("actual Core-derived native pin");
        let store = directory
            .open_store_until(
                ShardStoreConfig {
                    directory: self.shard.clone(),
                    shard_id: ShardId::new(0),
                    target_pack_bytes: self.target,
                    max_batch_bytes: 4096,
                },
                open_limits(),
                &setup,
                &Checks::new(),
            )
            .expect("actual bounded native store open");
        (store, owner, child)
    }
    fn contended_at(&self, shard: &Path) {
        assert!(matches!(
            DirectoryOwnership::acquire(&self.root),
            Err(DirectoryOwnershipError::Contended)
        ));
        assert!(
            ShardRecoveryGuard::try_acquire(Self::lock_at(shard), shard.to_path_buf()).is_err()
        );
    }
    fn contended(&self) {
        self.contended_at(&self.shard);
    }
    fn free(&self) {
        drop(DirectoryOwnership::acquire(&self.root).expect("actual last Core owner released"));
        drop(
            ShardRecoveryGuard::try_acquire(Self::lock_at(&self.shard), self.shard.clone())
                .expect("actual last native owner released"),
        );
    }
    fn bytes(&self) -> Vec<(PathBuf, Vec<u8>)> {
        let mut values = fs::read_dir(&self.shard)
            .expect("actual shard entries")
            .map(|r| {
                let e = r.expect("actual entry");
                (e.path(), fs::read(e.path()).expect("actual native body"))
            })
            .collect::<Vec<_>>();
        values.sort_by(|a, b| a.0.cmp(&b.0));
        values
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn actual_empty_and_contiguous_history_retain_both_validations_and_native_bounds() {
    for sequences in [&[][..], &[9, 10, 11][..]] {
        let fixture = Fixture::new(sequences, 4096);
        let before = fixture.bytes();
        let (store, owner, child) = fixture.open();
        let strong = Arc::strong_count(&child.inner);
        let c = core(core_limits());
        let n = native(native_limits());
        let checks = Checks::for_command(&c, &n);
        let log = store
            .into_shard_log_until(&n, &c, &checks)
            .expect("real bounded conversion");
        assert_eq!(
            log.bounds(),
            sequences
                .first()
                .copied()
                .zip(sequences.last().map(|v| v + 1))
        );
        assert!(
            log.pre_validation()
                .expect("actual pre report")
                .returned_success()
        );
        assert!(
            log.post_validation()
                .expect("actual post report")
                .returned_success()
        );
        assert!(log.pre_validation().expect("pre").retained_capacity() > 0);
        assert!(log.post_validation().expect("post").retained_capacity() > 0);
        assert!(
            Arc::strong_count(&child.inner) >= strong + 2,
            "two genuine same-handle retentions remain owned"
        );
        assert_eq!(c.usage().expect("actual cumulative usage").operations, 3);
        assert_eq!(fixture.bytes(), before);
        assert_eq!(checks.denial_calls.get(), 0);
        drop(owner);
        drop(child);
        fixture.contended();
        drop(log);
        fixture.free();
    }
}

#[test]
fn actual_retained_canonical_gap_keeps_prevalidation_native_error_and_both_leases() {
    let fixture = Fixture::new(&[5, 7], 4096);
    let (store, owner, child) = fixture.open();
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let error = store
        .into_shard_log_until(&n, &c, &checks)
        .expect_err("actual canonical gap refuses");
    assert_eq!(error.native_returned_success(), Some(false));
    assert!(
        error
            .pre_validation()
            .expect("real successful pre report")
            .returned_success()
    );
    assert!(error.post_validation().is_none());
    assert!(matches!(
        error
            .observation()
            .expect("retained native error")
            .failure(),
        Some(PinnedShardLogOpenFailure::Canonical)
    ));
    assert_eq!(checks.denial_calls.get(), 1);
    assert_eq!(error.outer_denial(), (true, true));
    drop(owner);
    drop(child);
    fixture.contended();
    drop(error);
    fixture.free();
}

#[test]
fn real_ordinary_foreign_stream_append_is_refused_without_catalog_or_owner_fabrication() {
    let fixture = Fixture::new(&[], 4096);
    let (mut store, owner, child) = fixture.open();
    let reservation = Reservation {
        topic_id: crate::TopicId::new(17),
        partition_id: crate::LogicalPartitionId::new(0),
        batch_id: crate::BatchId::new(1),
        first_offset: crate::LogicalOffset::new(0),
        last_offset: crate::LogicalOffset::new(0),
        record_count: std::num::NonZeroU32::new(1).expect("actual one-record ordinary append"),
        placement: crate::Placement {
            virtual_lane_id: crate::VirtualLaneId::new(0),
            ring_epoch: crate::RingEpoch::new(1),
            leader_epoch: crate::LeaderEpoch::new(1),
            sequence: crate::PlacementSequence::new(1),
        },
    };
    let prepared = store
        .store()
        .prepare_append(reservation, None, b"actual foreign stream".to_vec().into())
        .expect("ordinary real prepared frame");
    store
        .store_mut()
        .append_prepared_group(&[&prepared], true)
        .expect("ordinary genuine durable frame");
    drop(prepared);
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let error = store
        .into_shard_log_until(&n, &c, &checks)
        .expect_err("actual foreign stream cannot become canonical");
    assert_eq!(error.native_returned_success(), Some(false));
    assert!(matches!(
        error
            .observation()
            .expect("actual retained native canonical error")
            .failure(),
        Some(PinnedShardLogOpenFailure::Canonical)
    ));
    assert!(
        error
            .pre_validation()
            .expect("real pre-validation retained")
            .returned_success()
    );
    assert!(error.post_validation().is_none());
    assert_eq!(checks.denial_calls.get(), 1);
    drop(owner);
    drop(child);
    fixture.contended();
    drop(error);
    fixture.free();
}

#[test]
fn every_real_bridge_completion_retains_actual_outcomes_before_same_native_sibling_refusal() {
    for phase in [
        Phase::FixedAdmission,
        Phase::Split,
        Phase::PreRetain,
        Phase::PreValidation,
        Phase::Native,
        Phase::PostRetain,
        Phase::PostValidation,
        Phase::Usage,
    ] {
        let fixture = Fixture::new(&[1], 4096);
        let (store, owner, child) = fixture.open();
        let c = core(core_limits());
        let n = native(native_limits());
        let checks = Checks::for_command(&c, &n);
        let sibling = n.clone();
        let seen = Rc::new(Cell::new(false));
        let mark = Rc::clone(&seen);
        let hook = observe(move |actual| {
            if actual == phase {
                mark.set(true);
                sibling.deny();
            }
        });
        let error = store
            .into_shard_log_until(&n, &c, &checks)
            .expect_err("same native sibling denies next entry");
        drop(hook);
        assert!(seen.get());
        assert!(c.check().is_err());
        assert!(n.check().is_err());
        if matches!(
            phase,
            Phase::Native | Phase::PostRetain | Phase::PostValidation | Phase::Usage
        ) {
            assert_eq!(error.native_returned_success(), Some(true));
        }
        if matches!(phase, Phase::PostValidation | Phase::Usage) {
            assert!(
                error
                    .post_validation()
                    .expect("actual post carrier before check")
                    .returned_success()
            );
        }
        assert_eq!(checks.denial_calls.get(), 1);
        drop(owner);
        drop(child);
        fixture.contended();
        drop(error);
        fixture.free();
    }
}

#[test]
fn later_core_cap_and_actual_health_refusal_share_original_native_expiry_without_renewal() {
    for refuse_health in [false, true] {
        let fixture = Fixture::new(&[1], 4096);
        let (store, owner, child) = fixture.open();
        let original = expiry();
        let c = NativeDirectoryBudget::new(core_limits(), original + Duration::from_secs(30))
            .expect("original Core");
        let n =
            StorageFetchBudget::new(native_limits(), original).expect("earlier original native E");
        let checks = Checks::for_command(&c, &n);
        let later_cap = c.clone();
        let arm = Rc::new(Cell::new(false));
        let read = Rc::clone(&arm);
        *checks.action.borrow_mut() = Some(Box::new(move |_| {
            if read.get() && !refuse_health {
                assert!(later_cap.cap_expiry(Instant::now()).is_err());
            }
        }));
        let health = Rc::clone(&checks.denied);
        let hook = observe(move |phase| {
            if phase == Phase::Native {
                arm.set(true);
                if refuse_health {
                    health.set(true);
                }
            }
        });
        let error = store
            .into_shard_log_until(&n, &c, &checks)
            .expect_err("later actual cap/health event refuses");
        drop(hook);
        assert_eq!(error.native_returned_success(), Some(true));
        assert_eq!(checks.denial_calls.get(), 1);
        if refuse_health {
            assert_eq!(error.failure(), OwnedPinnedShardLogFailure::Checkpoint);
            assert_eq!(error.has_panic_payloads(), [false; 4]);
        }
        assert!(c.absolute_expiry() <= original);
        assert_eq!(n.absolute_expiry(), original);
        drop(owner);
        drop(child);
        fixture.contended();
        drop(error);
        fixture.free();
    }
}

#[test]
fn completed_native_ok_and_real_child_rename_survive_postvalidation_error() {
    let fixture = Fixture::new(&[10, 11], 4096);
    let (store, owner, child) = fixture.open();
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let actual = fixture.shard.clone();
    let renamed = fixture.root.join("renamed-shard");
    let move_to = renamed.clone();
    let hook = observe(move |phase| {
        if phase == Phase::Native {
            fs::rename(&actual, &move_to).expect("actual native child rename after native Ok");
        }
    });
    let error = store
        .into_shard_log_until(&n, &c, &checks)
        .expect_err("genuine postvalidation pathname refusal");
    drop(hook);
    assert_eq!(error.native_returned_success(), Some(true));
    assert!(
        error
            .pre_validation()
            .expect("actual pre")
            .returned_success()
    );
    assert!(
        !error
            .post_validation()
            .expect("actual retained post error")
            .returned_success()
    );
    drop(owner);
    drop(child);
    fixture.contended_at(&renamed);
    fs::rename(&renamed, &fixture.shard).expect("restore only this test's actual renamed child");
    drop(error);
    fixture.free();
}

#[test]
fn independent_operation_and_outer_denial_panics_keep_native_ok_and_once_attempt() {
    let fixture = Fixture::new(&[1], 4096);
    let (store, owner, child) = fixture.open();
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    checks.panic_deny.set(true);
    let hook = observe(|phase| {
        if phase == Phase::Native {
            std::panic::panic_any("actual bridge operation unwind");
        }
    });
    let error = store
        .into_shard_log_until(&n, &c, &checks)
        .expect_err("actual completed native Ok then independent panics");
    drop(hook);
    assert_eq!(error.native_returned_success(), Some(true));
    assert_eq!(error.has_panic_payloads(), [true, false, false, true]);
    assert_eq!(error.outer_denial(), (true, false));
    assert_eq!(checks.denial_calls.get(), 1);
    drop(owner);
    drop(child);
    fixture.contended();
    drop(error);
    fixture.free();
}

#[test]
fn native_owned_denial_panic_is_not_retried_by_final_bridge_cleanup() {
    let fixture = Fixture::new(&[1, 3], 4096);
    let (store, owner, child) = fixture.open();
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    checks.panic_deny.set(true);
    let error = store
        .into_shard_log_until(&n, &c, &checks)
        .expect_err("actual native canonical and composing denial error");
    assert_eq!(checks.denial_calls.get(), 1);
    assert_eq!(error.outer_denial(), (true, false));
    assert!(
        error
            .observation()
            .expect("actual native carrier")
            .has_panic_payloads()[2]
    );
    assert!(
        !error.has_panic_payloads()[3],
        "the one native-owned panic is not cloned into a bridge slot"
    );
    drop(owner);
    drop(child);
    fixture.contended();
    drop(error);
    fixture.free();
}

#[test]
fn actual_prevalidation_operation_and_health_unwinds_stay_in_that_carrier_once() {
    let fixture = Fixture::new(&[1], 4096);
    let (store, owner, child) = fixture.open();
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    checks.panic_deny.set(true);
    let armed = Rc::new(Cell::new(false));
    let read = Rc::clone(&armed);
    let after_retain = Rc::new(Cell::new(0usize));
    let count = Rc::clone(&after_retain);
    *checks.action.borrow_mut() = Some(Box::new(move |_| {
        if read.get() {
            let next = count.get() + 1;
            count.set(next);
            // After the real parent retain: bridge postcheck, validation-entry
            // bridge check, then the actual consuming validator's first check.
            if next == 3 {
                std::panic::panic_any("actual borrowed check inside prevalidation");
            }
        }
    }));
    let hook = observe(move |phase| {
        if phase == Phase::PreRetain {
            armed.set(true);
        }
    });
    let error = store
        .into_shard_log_until(&n, &c, &checks)
        .expect_err("actual validator operation and composing denial unwind");
    drop(hook);
    let validation = error
        .pre_validation()
        .expect("whole actual pre-validation refusal retained");
    assert_eq!(validation.panic_observations(), [true, false, false, true]);
    assert_eq!(
        error.has_panic_payloads(),
        [false; 4],
        "actual producer-owned payloads are not duplicated into bridge slots"
    );
    assert_eq!(error.native_returned_success(), None);
    assert!(error.post_validation().is_none());
    assert_eq!(checks.denial_calls.get(), 1);
    assert_eq!(error.outer_denial(), (true, false));
    drop(owner);
    drop(child);
    fixture.contended();
    drop(error);
    fixture.free();
}

#[test]
fn fixed_bridge_one_less_refuses_before_split_or_real_parent_arc_increment() {
    let fixture = Fixture::new(&[], 4096);
    let (store, owner, child) = fixture.open();
    let before = Arc::strong_count(&child.inner);
    let original_directory = Arc::downgrade(&store.directory.inner);
    // Independent immutable lower bound: the complete retained scope is one
    // named value before all independent result/panic/header requests.
    let mut limits = core_limits();
    limits.max_declared_bytes = size_of::<Scope>() - 1;
    let c = core(limits);
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let error = store
        .into_shard_log_until(&n, &c, &checks)
        .expect_err("one-less required whole scope refuses before entry");
    assert!(error.scope.early.is_some());
    assert!(error.scope.directory.is_none());
    assert_eq!(Arc::strong_count(&child.inner), before);
    assert_eq!(original_directory.strong_count(), 1);
    assert_eq!(c.usage().expect("actual zero-fee usage").declared_bytes, 0);
    assert!(error.pre_validation().is_none());
    assert_eq!(error.native_returned_success(), None);
    assert_eq!(checks.denial_calls.get(), 1);
    drop(owner);
    drop(child);
    fixture.contended();
    drop(error);
    fixture.free();
}

#[test]
fn two_true_stores_share_remaining_native_and_core_counters_without_new_budget() {
    let first = Fixture::new(&[1], 4096);
    let second = Fixture::new(&[9], 4096);
    let (a, ao, ah) = first.open();
    let (b, bo, bh) = second.open();
    let c = core(core_limits());
    let mut limits = native_limits();
    // Immutable source contract: native conversion has23 independently paid
    // fixed slots and at least one real sequence slot for each actual history.
    //46 slots fit the first conversion; fewer than23 then remain for another.
    limits.max_entries = 46;
    let n = native(limits);
    let checks = Checks::for_command(&c, &n);
    let a = a
        .into_shard_log_until(&n, &c, &checks)
        .expect("first actual conversion");
    let before = n.usage().expect("actual native spent prefix");
    assert!(before.entries > 0);
    assert!(
        n.check().is_ok(),
        "remaining healthy native counter is used, not an artificial denial"
    );
    let b = b
        .into_shard_log_until(&n, &c, &checks)
        .expect_err("same remaining native slots cannot admit another actual fixed23");
    assert_eq!(n.usage().expect("same cumulative native counters"), before);
    assert_eq!(b.native_returned_success(), Some(false));
    assert!(matches!(
        b.observation()
            .expect("actual refused native result")
            .failure(),
        Some(PinnedShardLogOpenFailure::Native(
            StorageFetchFailure::Capacity
        ))
    ));
    assert!(c.usage().expect("same Core policy counters").operations > 3);
    drop(ao);
    drop(ah);
    drop(bo);
    drop(bh);
    first.contended();
    second.contended();
    drop(a);
    first.free();
    second.contended();
    drop(b);
    second.free();
}

#[test]
fn both_drop_orders_keep_same_shared_directory_until_actual_last_opaque_job_owner() {
    for drop_log_first in [false, true] {
        let fixture = Fixture::new(&[1], 4096);
        let (store, owner, child) = fixture.open();
        let c = core(core_limits());
        let n = native(native_limits());
        let checks = Checks::for_command(&c, &n);
        let bytes = size_of::<OwnedShardStoreDirectory>();
        c.reserve(0, 0, bytes, 0, 1, bytes + 1)
            .expect("prepaid actual physical-job Arc retention");
        let job = store.retain_directory();
        let same = Arc::downgrade(&job.inner);
        let weak_root = Arc::downgrade(&owner);
        let log = store
            .into_shard_log_until(&n, &c, &checks)
            .expect("actual Core conversion");
        assert!(Arc::ptr_eq(
            &job.inner,
            &log.scope
                .directory
                .as_ref()
                .expect("SAME moved directory")
                .inner
        ));
        drop(owner);
        drop(child);
        fixture.contended();
        if drop_log_first {
            drop(log);
            fixture.contended();
            assert!(same.upgrade().is_some());
            assert!(weak_root.upgrade().is_some());
            drop(job);
        } else {
            drop(job);
            fixture.contended();
            assert!(same.upgrade().is_some());
            drop(log);
        }
        assert!(same.upgrade().is_none());
        assert!(weak_root.upgrade().is_none());
        fixture.free();
    }
}

#[test]
fn plain_cost_separates_two_validation_frames_from_one_configured_policy() {
    let limits = core_limits();
    let global = native_directory_cost(limits).expect("one policy fact");
    let validation = checked_directory_validation_cost(limits).expect("one real validation frame");
    let cost = owned_pinned_shard_log_cost(limits).expect("actual per-conversion bridge fact");
    assert_eq!(
        cost.validation_framing_bytes,
        2 * (validation.declared_bytes - global.declared_bytes)
    );
    assert_eq!(
        cost.validation_framing_entries,
        2 * (validation.entries - global.entries)
    );
    assert_eq!(
        cost.parent_retention_bytes,
        2 * (size_of::<OwnedDirectoryHandle>() + size_of::<Option<OwnedDirectoryHandle>>())
    );
    assert_eq!(cost.string_bytes, 0);
    assert_eq!(cost.core_operations, 3);
    assert_eq!(
        cost.entries - cost.validation_framing_entries - 4,
        54,
        "immutable54 bridge logical values, with four separate retain slots"
    );
    let independently_named_panic_headers =
        4 * (size_of::<Option<Box<dyn Any + Send>>>() + size_of::<Box<dyn Any + Send>>());
    assert!(
        cost.declared_bytes
            >= size_of::<Scope>()
                + independently_named_panic_headers
                + size_of::<Native>()
                + 2 * size_of::<Validation>()
    );
}

#[test]
fn immutable_fixed54_exact_and_one_less_entries_and_work_are_real_preentry_guards() {
    const FIXED_ENTRIES: usize = 54;
    for by_entries in [false, true] {
        for fits in [false, true] {
            let fixture = Fixture::new(&[], 4096);
            let (store, owner, child) = fixture.open();
            let fact = owned_pinned_shard_log_cost(core_limits())
                .expect("plain early conversion fee facts");
            let fixed_bytes =
                fact.declared_bytes - fact.validation_framing_bytes - fact.parent_retention_bytes;
            let mut limits = core_limits();
            if by_entries {
                limits.max_entries = FIXED_ENTRIES - usize::from(!fits);
            } else {
                limits.max_work_bytes = fixed_bytes + FIXED_ENTRIES - usize::from(!fits);
            }
            let c = core(limits);
            let n = native(native_limits());
            let checks = Checks::for_command(&c, &n);
            let seen = Rc::new(Cell::new(false));
            let mark = Rc::clone(&seen);
            let sibling = n.clone();
            let before = Arc::strong_count(&child.inner);
            let hook = observe(move |phase| {
                if phase == Phase::FixedAdmission {
                    mark.set(true);
                    sibling.deny();
                }
            });
            let error = store
                .into_shard_log_until(&n, &c, &checks)
                .expect_err("stop at the actual fixed-fee boundary");
            drop(hook);
            assert_eq!(seen.get(), fits);
            assert_eq!(Arc::strong_count(&child.inner), before);
            let spent = c
                .usage()
                .expect("actual unchanged-or-fixed cumulative counters");
            assert_eq!(spent.entries, if fits { FIXED_ENTRIES } else { 0 });
            assert_eq!(
                spent.work_bytes,
                if fits { fixed_bytes + FIXED_ENTRIES } else { 0 }
            );
            assert!(error.scope.early.is_some());
            assert!(error.pre_validation().is_none());
            drop(owner);
            drop(child);
            fixture.contended();
            drop(error);
            fixture.free();
        }
    }
}

#[test]
fn public_safe_core_views_are_send_without_native_owner_or_any_downcast_escape() {
    fn send<T: Send>() {}
    send::<OwnedPinnedShardLog>();
    send::<RetainedOwnedPinnedShardLogError>();
    let fixture = Fixture::new(&[1], 4096);
    let (store, owner, child) = fixture.open();
    let mut limits = core_limits();
    limits.max_declared_bytes = 1;
    let c = core(limits);
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let error = store
        .into_shard_log_until(&n, &c, &checks)
        .expect_err("actual definite budget refusal");
    if let Some(source) = error.source() {
        assert!(source.downcast_ref::<PinnedShardLogOpenError>().is_none());
        assert!(
            source
                .downcast_ref::<RetainedCheckedDirectoryValidationError>()
                .is_none()
        );
        assert!(source.downcast_ref::<std::io::Error>().is_none());
    }
    drop(owner);
    drop(child);
    fixture.contended();
    drop(error);
    fixture.free();
}
