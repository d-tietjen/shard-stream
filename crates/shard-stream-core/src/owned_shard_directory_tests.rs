//! Real retained Core/native directory continuation and refusal custody.

use super::*;
use crate::{NativeDirectoryLimits, NativeDirectoryUsage};
use shardlog::{PinnedShardDirectoryFailure, ShardGuardAcquireLimits, ShardRecoveryGuard};
use std::fs::{self, OpenOptions};
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
struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "owned-shard-directory-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual temporary directory");
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
            Instant::now() + Duration::from_secs(30),
        )
        .expect("actual acquired native shard guard")
    }
    fn contended(&self) {
        assert!(matches!(
            DirectoryOwnership::acquire(&self.0),
            Err(DirectoryOwnershipError::Contended)
        ));
        assert!(ShardRecoveryGuard::try_acquire(self.lock_file(), self.0.clone()).is_err());
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn limits() -> NativeDirectoryLimits {
    NativeDirectoryLimits {
        max_depth: 16,
        max_component_bytes: 4096,
        max_path_bytes: 8192,
        max_operations: 1024,
        max_calls: 1_000_000,
        max_entries: 1_000_000,
        max_declared_bytes: 1 << 26,
        max_work_bytes: 1 << 30,
    }
}
fn budget(limits: NativeDirectoryLimits) -> NativeDirectoryBudget {
    NativeDirectoryBudget::new(limits, Instant::now() + Duration::from_secs(30))
        .expect("actual finite same budget")
}
fn native_limits() -> PinnedShardDirectoryLimits {
    PinnedShardDirectoryLimits {
        max_path_bytes: 8192,
        max_io_calls: 4,
    }
}

#[test]
fn actual_directory_bridge_keeps_same_root_and_native_lock_without_startup_budget() {
    fn send<T: Send>() {}
    send::<OwnedShardDirectory>();
    send::<RetainedOwnedShardDirectoryError>();
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual original Core owner");
    let weak = Arc::downgrade(&owner);
    let root = owner.root_handle().expect("actual Core root FD");
    let paid = budget(limits());
    let pinned = root
        .pin_shard_directory_until(path.acquired(), native_limits(), &paid)
        .expect("actual Core-derived directory FD");
    assert!(Arc::ptr_eq(pinned.owner(), &owner));
    assert_eq!(pinned.directory(), path.0);
    assert_eq!(pinned.observation().identity_matches(), Some(true));
    assert_eq!(pinned.observation().entered_calls(), 4);
    paid.check().expect("same budget healthy");
    drop(paid);
    drop(root);
    drop(owner);
    path.contended();
    assert!(weak.upgrade().is_some());
    drop(pinned);
    assert!(weak.upgrade().is_none());
    DirectoryOwnership::acquire(&path.0).expect("actual final root custody released");
    ShardRecoveryGuard::try_acquire(path.lock_file(), path.0.clone())
        .expect("actual final native lock custody released");
}

#[test]
fn each_actual_bridge_phase_keeps_raw_partial_custody_on_expiry_or_unwind() {
    for target in [
        Phase::ParentValidated,
        Phase::DescriptorCloned,
        Phase::NativeCompleted,
        Phase::FinalParentValidated,
    ] {
        for unwind in [false, true] {
            let path = Temporary::new();
            let owner = DirectoryOwnership::acquire(&path.0).expect("actual Core owner");
            let root = owner.root_handle().expect("actual Core FD");
            let paid = budget(limits());
            let shared = paid.clone();
            let _observe = observe(move |phase| {
                if phase == target {
                    if unwind {
                        std::panic::panic_any(target);
                    }
                    assert!(matches!(
                        shared.cap_expiry(Instant::now()),
                        Err(NativeDirectoryFailure::Expired(_))
                    ));
                }
            });
            let error = root
                .pin_shard_directory_until(path.acquired(), native_limits(), &paid)
                .expect_err("completed actual phase refused");
            assert_eq!(
                error.failure(),
                if unwind {
                    OwnedShardDirectoryFailure::Unwind
                } else {
                    OwnedShardDirectoryFailure::Budget
                }
            );
            assert!(Arc::ptr_eq(error.owner(), &owner));
            assert_eq!(
                error.retains_input_guard(),
                matches!(target, Phase::ParentValidated | Phase::DescriptorCloned)
            );
            assert_eq!(
                error.retains_directory_clone(),
                target == Phase::DescriptorCloned
            );
            assert_eq!(
                error.native_observation().is_some(),
                matches!(target, Phase::NativeCompleted | Phase::FinalParentValidated)
            );
            if let Some(actual) = error.native_observation() {
                assert_eq!(actual.identity_matches(), Some(true));
            }
            if unwind {
                assert_eq!(
                    error
                        .panic_payload()
                        .expect("original actual payload")
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
            DirectoryOwnership::acquire(&path.0)
                .expect("retained error was last actual Core owner");
            ShardRecoveryGuard::try_acquire(path.lock_file(), path.0.clone())
                .expect("retained error was last actual native lock owner");
        }
    }
}

#[test]
fn wrong_acquired_guard_refuses_before_native_io_and_retains_both_actual_roots() {
    let path = Temporary::new();
    let other = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual Core owner");
    let root = owner.root_handle().expect("actual Core root");
    let paid = budget(limits());
    let error = root
        .pin_shard_directory_until(other.acquired(), native_limits(), &paid)
        .expect_err("actual guard has another native directory");
    let native = error
        .native_error()
        .expect("actual native refusal retained");
    assert_eq!(native.failure(), PinnedShardDirectoryFailure::Identity);
    assert_eq!(native.observation().entered_calls(), 0);
    assert!(matches!(paid.check(), Err(NativeDirectoryFailure::Denied)));
    assert!(Arc::ptr_eq(error.owner(), &owner));
    assert!(ShardRecoveryGuard::try_acquire(other.lock_file(), other.0.clone()).is_err());
    drop(root);
    drop(owner);
    assert!(matches!(
        DirectoryOwnership::acquire(&path.0),
        Err(DirectoryOwnershipError::Contended)
    ));
    drop(error);
    DirectoryOwnership::acquire(&path.0).expect("same Core root retained until error drop");
    ShardRecoveryGuard::try_acquire(other.lock_file(), other.0.clone())
        .expect("other actual native lock retained until error drop");
}

#[test]
fn exact_cumulative_native_fees_fit_and_one_less_refuses_without_losing_inputs() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual Core owner");
    let root = owner.root_handle().expect("actual Core root");
    let generous = budget(limits());
    let pinned = root
        .pin_shard_directory_until(path.acquired(), native_limits(), &generous)
        .expect("derive actual physical path usage");
    let used = generous.usage().expect("actual cumulative fees");
    assert!(
        used.calls
            >= owned_shard_directory_cost(native_limits())
                .expect("checked bridge cost")
                .native_call_slots
    );
    drop(pinned);
    let mut exact = limits();
    exact.max_operations = used.operations;
    exact.max_calls = used.calls;
    exact.max_entries = used.entries;
    exact.max_declared_bytes = used.declared_bytes;
    exact.max_work_bytes = used.work_bytes;
    let fitting = budget(exact);
    let pinned = root
        .pin_shard_directory_until(path.acquired(), native_limits(), &fitting)
        .expect("exact cumulative fees fit");
    assert_eq!(fitting.usage().expect("same actual fees"), used);
    drop(pinned);
    for category in 0..5 {
        let mut short = exact;
        match category {
            0 => short.max_operations -= 1,
            1 => short.max_calls -= 1,
            2 => short.max_entries -= 1,
            3 => short.max_declared_bytes -= 1,
            _ => short.max_work_bytes -= 1,
        }
        let paid = budget(short);
        let error = root
            .pin_shard_directory_until(path.acquired(), native_limits(), &paid)
            .expect_err("one less actual cumulative fee");
        assert!(error.budget_error().is_some() || error.directory_error().is_some());
        path.contended();
        drop(error);
    }
}

#[test]
fn pre_entry_path_or_unpaid_bridge_panic_keeps_actual_inputs_without_io_or_fees() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual original Core owner");
    let root = owner.root_handle().expect("actual root FD");
    for missing_bridge_panic in [false, true] {
        let mut allowance = limits();
        let mut cap = native_limits();
        if missing_bridge_panic {
            // Pay native's own panic and six bridge carrier/result values,
            // leaving its independent retained bridge panic unpaid.
            allowance.max_entries = pinned_shard_directory_cost(cap)
                .expect("actual native fees")
                .entries
                + 6;
        } else {
            cap.max_path_bytes = allowance.max_path_bytes + 1;
        }
        let paid = budget(allowance);
        let error = root
            .pin_shard_directory_until(path.acquired(), cap, &paid)
            .expect_err("the entire actual producer must be paid before entry");
        assert_eq!(error.failure(), OwnedShardDirectoryFailure::Budget);
        assert!(matches!(
            error.budget_error(),
            Some(NativeDirectoryFailure::Capacity)
        ));
        assert!(error.retains_input_guard());
        assert!(!error.retains_directory_clone());
        assert!(error.native_observation().is_none());
        assert_eq!(
            paid.usage().expect("actual zero fees"),
            NativeDirectoryUsage::default()
        );
        path.contended();
        drop(error);
    }
}
