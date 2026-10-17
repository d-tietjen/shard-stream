//! Real original owners, descriptor walks and shared counters; source-only tests.

use super::*;
use shardlog::StorageFetchLimits;
use std::{
    cell::{Cell, RefCell},
    fs,
    path::PathBuf,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Event(CheckedDirectoryValidationPhase, bool);
type Observer = Box<dyn FnMut(Event)>;
thread_local! { static OBSERVER:RefCell<Option<Observer>>=RefCell::new(None); }
pub(super) fn completed(phase: CheckedDirectoryValidationPhase, allocation: bool) {
    OBSERVER.with(|v| {
        if let Some(f) = &mut *v.borrow_mut() {
            f(Event(phase, allocation));
        }
    });
}
struct Observe;
impl Drop for Observe {
    fn drop(&mut self) {
        OBSERVER.with(|v| *v.borrow_mut() = None);
    }
}
fn observe(f: impl FnMut(Event) + 'static) -> Observe {
    OBSERVER.with(|v| {
        assert!(v.borrow().is_none());
        *v.borrow_mut() = Some(Box::new(f));
    });
    Observe
}
struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let p = std::env::temp_dir().join(format!(
            "retained-both-counter-validation-{}-{now}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p).expect("actual fixture root");
        Self(p)
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
        max_operations: 128,
        max_calls: 100_000,
        max_entries: 1_000_000,
        max_declared_bytes: 1 << 26,
        max_work_bytes: 1 << 30,
    }
}
fn native_limits() -> StorageFetchLimits {
    StorageFetchLimits {
        max_plan_batches: 64,
        max_entries: 100_000,
        max_path_bytes: 8192,
        max_read_attempts: 128,
        max_read_bytes: 1 << 20,
        max_declared_bytes: 1 << 24,
        max_work_bytes: 1 << 28,
    }
}
fn budgets() -> (NativeDirectoryBudget, StorageFetchBudget) {
    let e = Instant::now() + Duration::from_secs(30);
    (
        NativeDirectoryBudget::new(limits(), e).expect("original Core counters"),
        StorageFetchBudget::new(native_limits(), e)
            .expect("original supplied shared native counters"),
    )
}
fn fixture(p: &Temporary, depth: usize) -> (Arc<DirectoryOwnership>, OwnedDirectoryHandle) {
    let owner = DirectoryOwnership::acquire(&p.0).expect("genuine exclusive native owner");
    let mut handle = owner.root_handle().expect("genuine root descriptor");
    for n in 0..depth {
        handle = handle
            .open_directory(OsStr::new(&format!("child-{n}")), true)
            .expect("real child");
    }
    (owner, handle)
}
struct HostChecks {
    core: NativeDirectoryBudget,
    native: StorageFetchBudget,
    refused: Rc<Cell<bool>>,
    denials: Cell<usize>,
    panic_deny: Cell<bool>,
    path: Option<PathBuf>,
}
impl HostChecks {
    fn new(core: &NativeDirectoryBudget, native: &StorageFetchBudget) -> Self {
        Self {
            core: core.clone(),
            native: native.clone(),
            refused: Rc::new(Cell::new(false)),
            denials: Cell::new(0),
            panic_deny: Cell::new(false),
            path: None,
        }
    }
}
impl ShardDirectoryChecks for HostChecks {
    fn check(&self) -> std::result::Result<(), ShardDirectoryCheckFailure> {
        if self.refused.get() {
            Err(ShardDirectoryCheckFailure::Refused)
        } else {
            Ok(())
        }
    }
}
impl CheckedDirectoryValidationChecks for HostChecks {
    fn deny(&self) {
        assert!(
            self.core.check().is_err(),
            "actual Core denial before health"
        );
        assert!(
            self.native.check().is_err(),
            "actual native denial before health"
        );
        self.denials.set(self.denials.get() + 1);
        self.refused.set(true);
        if let Some(p) = &self.path {
            contended(p);
        }
        if self.panic_deny.get() {
            std::panic::panic_any(29u64);
        }
    }
}
fn contended(path: &Path) {
    assert!(matches!(
        DirectoryOwnership::acquire(path),
        Err(DirectoryOwnershipError::Contended)
    ));
}
fn trace(handle: &OwnedDirectoryHandle) -> Vec<Event> {
    let (core, native) = budgets();
    let checks = HostChecks::new(&core, &native);
    let events = Rc::new(RefCell::new(Vec::new()));
    let out = events.clone();
    let hook = observe(move |e| out.borrow_mut().push(e));
    let report = handle
        .retain()
        .validate_for_storage_fetch_until(&core, &native, &checks)
        .expect("actual fitting reference walk");
    drop(hook);
    assert_eq!(checks.denials.get(), 0);
    drop(report);
    Rc::try_unwrap(events)
        .ok()
        .expect("only reference collector")
        .into_inner()
}
fn raw_descriptors(e: &Effects) -> usize {
    e.values
        .iter()
        .filter(|v| matches!(&v.value, Value::Open(Ok(_))))
        .count()
}

#[test]
fn actual_root_and_nested_chain_share_original_identity_and_return_only_whole_input() {
    let p = Temporary::new();
    let (owner, child) = fixture(&p, 3);
    fs::write(child.path().join("kept.bin"), b"actual existing file")
        .expect("native fixture bytes");
    child.validate().expect("ordinary acceptance before");
    let expected = child.path().to_owned();
    let inner = Arc::downgrade(&child.inner);
    let (core, native) = budgets();
    let checks = HostChecks::new(&core, &native);
    let report = child
        .validate_for_storage_fetch_until(&core, &native, &checks)
        .expect("fitting shared-counter validation");
    let phases = report.observations().map(|x| x.phase()).collect::<Vec<_>>();
    for phase in [
        CheckedDirectoryValidationPhase::RootPathMetadata,
        CheckedDirectoryValidationPhase::AncestorMetadata,
        CheckedDirectoryValidationPhase::RootOpened,
        CheckedDirectoryValidationPhase::ComponentName,
        CheckedDirectoryValidationPhase::ComponentOpened,
        CheckedDirectoryValidationPhase::ResolvedRootStat,
        CheckedDirectoryValidationPhase::RetainedRootStat,
        CheckedDirectoryValidationPhase::NamedLockStat,
        CheckedDirectoryValidationPhase::RetainedLockStat,
        CheckedDirectoryValidationPhase::HandlePathMetadata,
        CheckedDirectoryValidationPhase::HandleDescriptorMetadata,
        CheckedDirectoryValidationPhase::ChildMetadata,
        CheckedDirectoryValidationPhase::ChildDescriptorStat,
        CheckedDirectoryValidationPhase::NamedChildStat,
    ] {
        assert!(phases.contains(&phase));
    }
    assert!(report.observations().all(|x| x.returned_success()));
    assert!(report.retained_capacity() >= phases.len());
    assert!(raw_descriptors(&report.scope.effects) > 1);
    assert_eq!(
        report.entry_deadlines(),
        Some((native.absolute_expiry(), native.absolute_expiry()))
    );
    assert_eq!(
        report
            .core_usage()
            .expect("actual usage")
            .as_ref()
            .expect("usage")
            .operations,
        1
    );
    let whole = report.into_handle();
    assert!(Arc::ptr_eq(
        &whole.inner,
        &inner.upgrade().expect("same input allocation")
    ));
    assert!(Arc::ptr_eq(whole.owner(), &owner));
    assert_eq!(whole.path(), expected);
    whole.validate().expect("ordinary acceptance after");
    assert_eq!(
        fs::read(whole.path().join("kept.bin")).expect("same real bytes"),
        b"actual existing file"
    );
    assert_eq!(checks.denials.get(), 0);
}

#[test]
fn every_real_completed_phase_and_allocation_observes_same_native_sibling_denial_before_next_phase()
{
    let p = Temporary::new();
    let (_owner, handle) = fixture(&p, 2);
    let expected = trace(&handle);
    assert!(expected.iter().any(|e| e.1));
    for target in 0..expected.len() {
        let (core, native) = budgets();
        let checks = HostChecks::new(&core, &native);
        let sibling = native.clone();
        let events = Rc::new(RefCell::new(Vec::new()));
        let seen = events.clone();
        let hook = observe(move |e| {
            seen.borrow_mut().push(e);
            if seen.borrow().len() == target + 1 {
                sibling.deny();
            }
        });
        let error = handle
            .retain()
            .validate_for_storage_fetch_until(&core, &native, &checks)
            .expect_err("same real native counter denied");
        drop(hook);
        assert_eq!(*events.borrow(), expected[..=target]);
        assert_eq!(error.failure(), CheckedDirectoryValidationFailure::Native);
        assert!(matches!(
            error.native_error(),
            Some(StorageFetchFailure::Denied)
        ));
        assert_eq!(checks.denials.get(), 1);
        assert_eq!(error.composing_denial(), (true, true));
        assert!(core.check().is_err());
        assert!(native.check().is_err());
        if !expected[target].1 {
            assert_eq!(
                error
                    .observations()
                    .last()
                    .expect("actual phase retained")
                    .phase(),
                expected[target].0
            );
            assert!(
                error
                    .observations()
                    .last()
                    .expect("successful native result")
                    .returned_success()
            );
        }
    }
}

#[test]
fn actual_health_refusal_at_every_entered_boundary_retains_result_and_denies_both_budgets_once() {
    let p = Temporary::new();
    let (_owner, handle) = fixture(&p, 1);
    let expected = trace(&handle);
    for target in 0..expected.len() {
        let (core, native) = budgets();
        let checks = HostChecks::new(&core, &native);
        let refused = checks.refused.clone();
        let count = Rc::new(Cell::new(0));
        let n = count.clone();
        let hook = observe(move |_| {
            n.set(n.get() + 1);
            if n.get() == target + 1 {
                refused.set(true);
            }
        });
        let e = handle
            .retain()
            .validate_for_storage_fetch_until(&core, &native, &checks)
            .expect_err("actual health refused");
        drop(hook);
        assert_eq!(count.get(), target + 1);
        assert_eq!(e.failure(), CheckedDirectoryValidationFailure::Checkpoint);
        assert_eq!(
            e.checkpoint_error(),
            Some(ShardDirectoryCheckFailure::Refused)
        );
        assert_eq!(checks.denials.get(), 1);
        assert!(core.check().is_err());
        assert!(native.check().is_err());
    }
}

#[test]
fn original_native_expiry_caps_core_and_later_actual_core_cap_is_honored_without_revival() {
    let p = Temporary::new();
    let (_owner, handle) = fixture(&p, 0);
    let native_e = Instant::now() + Duration::from_secs(5);
    let core = NativeDirectoryBudget::new(limits(), native_e + Duration::from_secs(10))
        .expect("Core original E");
    let native =
        StorageFetchBudget::new(native_limits(), native_e).expect("earlier genuine native E");
    let checks = HostChecks::new(&core, &native);
    let report = handle
        .retain()
        .validate_for_storage_fetch_until(&core, &native, &checks)
        .expect("same earlier E");
    assert_eq!(report.entry_deadlines(), Some((native_e, native_e)));
    assert_eq!(core.absolute_expiry(), native_e);
    drop(report);
    let (core, native) = budgets();
    let checks = HostChecks::new(&core, &native);
    let cap = core.clone();
    let hook = observe(move |e| {
        if !e.1 {
            assert!(cap.cap_expiry(Instant::now()).is_err());
        }
    });
    let error = handle
        .retain()
        .validate_for_storage_fetch_until(&core, &native, &checks)
        .expect_err("same shared minimum shortened after actual result");
    drop(hook);
    assert_eq!(error.observations().count(), 1);
    assert!(matches!(
        error.core_error(),
        Some(NativeDirectoryFailure::Expired(_))
    ));
    let observed = core.absolute_expiry();
    assert!(
        core.cap_expiry(Instant::now() + Duration::from_secs(60))
            .is_err()
    );
    assert_eq!(core.absolute_expiry(), observed);
    assert_eq!(checks.denials.get(), 1);
}

#[test]
fn actual_named_lock_error_is_retained_before_late_core_refusal_and_root_custody() {
    let p = Temporary::new();
    let (owner, handle) = fixture(&p, 0);
    let weak = Arc::downgrade(&owner);
    let moved = p.0.join("kept-original-lock");
    fs::rename(p.0.join(".shard-stream.directory.lock"), &moved)
        .expect("actual lock pathname disappears; original FD stays held");
    let (core, native) = budgets();
    let mut checks = HostChecks::new(&core, &native);
    checks.path = None;
    let cap = core.clone();
    let hook = observe(move |event| {
        if event == Event(CheckedDirectoryValidationPhase::NamedLockStat, false) {
            assert!(cap.cap_expiry(Instant::now()).is_err());
        }
    });
    drop(owner);
    let error = handle
        .validate_for_storage_fetch_until(&core, &native, &checks)
        .expect_err("actual stat ENOENT plus later E refusal");
    drop(hook);
    assert!(matches!(
        error.core_error(),
        Some(NativeDirectoryFailure::Expired(_))
    ));
    let last = error
        .observations()
        .last()
        .expect("real lock error retained");
    assert_eq!(last.phase(), CheckedDirectoryValidationPhase::NamedLockStat);
    assert_eq!(last.native_errno(), Some(rustix::io::Errno::NOENT));
    assert!(!last.returned_success());
    assert!(raw_descriptors(&error.scope.effects) > 1);
    assert!(weak.upgrade().is_some());
    fs::rename(&moved, p.0.join(".shard-stream.directory.lock"))
        .expect("restore actual original lock name for contention observation");
    contended(&p.0);
    drop(error);
    assert!(weak.upgrade().is_none());
    DirectoryOwnership::acquire(&p.0).expect("last real owner released");
}

#[test]
fn actual_phase_unwind_and_independent_health_denial_unwind_preserve_both_payloads_and_all_results()
{
    let p = Temporary::new();
    let (_owner, handle) = fixture(&p, 1);
    let expected = trace(&handle);
    for target in 0..expected.len() {
        let (core, native) = budgets();
        let mut checks = HostChecks::new(&core, &native);
        checks.path = Some(p.0.clone());
        checks.panic_deny.set(true);
        let n = Rc::new(Cell::new(0));
        let seen = n.clone();
        let hook = observe(move |_| {
            seen.set(seen.get() + 1);
            if seen.get() == target + 1 {
                std::panic::panic_any(17u64);
            }
        });
        let e = handle
            .retain()
            .validate_for_storage_fetch_until(&core, &native, &checks)
            .expect_err("original phase unwind retained");
        drop(hook);
        assert_eq!(e.failure(), CheckedDirectoryValidationFailure::Unwind);
        assert_eq!(e.panic_observations(), [true, false, false, true]);
        assert_eq!(
            e.scope
                .effects
                .operation_panic
                .as_ref()
                .expect("actual original payload")
                .downcast_ref::<u64>(),
            Some(&17)
        );
        assert_eq!(
            e.scope
                .effects
                .outer_deny_panic
                .as_ref()
                .expect("independent actual denial payload")
                .downcast_ref::<u64>(),
            Some(&29)
        );
        assert_eq!(e.composing_denial(), (true, false));
        assert_eq!(checks.denials.get(), 1);
        assert_eq!(n.get(), target + 1);
        contended(&p.0);
        if !expected[target].1 {
            assert_eq!(
                e.observations()
                    .last()
                    .expect("actual raw value before panic")
                    .phase(),
                expected[target].0
            );
        }
    }
}

#[test]
fn complete_and_refused_carriers_keep_whole_actual_owner_until_their_true_last_drop() {
    for refused in [false, true] {
        let p = Temporary::new();
        let (owner, handle) = fixture(&p, 2);
        let weak = Arc::downgrade(&owner);
        let (core, native) = budgets();
        let checks = HostChecks::new(&core, &native);
        drop(owner);
        if refused {
            let sibling = native.clone();
            let hook = observe(move |e| {
                if e == Event(CheckedDirectoryValidationPhase::ComponentOpened, false) {
                    sibling.deny();
                }
            });
            let e = handle
                .validate_for_storage_fetch_until(&core, &native, &checks)
                .expect_err("entered native refusal");
            drop(hook);
            assert!(raw_descriptors(&e.scope.effects) >= 2);
            contended(&p.0);
            assert!(weak.upgrade().is_some());
            drop(e);
        } else {
            let report = handle
                .validate_for_storage_fetch_until(&core, &native, &checks)
                .expect("actual complete carrier");
            assert!(raw_descriptors(&report.scope.effects) > 1);
            contended(&p.0);
            let whole = report.into_handle();
            contended(&p.0);
            drop(whole);
        }
        assert!(weak.upgrade().is_none());
        DirectoryOwnership::acquire(&p.0).expect("only true last owner releases lock");
    }
}

#[test]
fn named_fixed_result_panic_and_header_slots_have_independent_exact_and_one_less_admission() {
    let fixed = [
        size_of::<Scope>(),
        size_of::<Effects>(),
        size_of::<Checks>(),
        size_of::<Work<'static>>(),
        size_of::<RetainedCheckedDirectoryValidation>(),
        size_of::<RetainedCheckedDirectoryValidationError>(),
        size_of::<CheckedDirectoryValidationCost>(),
        size_of::<CheckedDirectoryValidationFailure>(),
        size_of::<OwnedDirectoryHandle>(),
        size_of::<Vec<Outcome>>(),
        size_of::<Option<Outcome>>(),
        size_of::<Outcome>(),
        size_of::<Value>(),
        size_of::<Option<std::result::Result<(), TryReserveError>>>(),
        size_of::<std::result::Result<(), TryReserveError>>(),
        size_of::<TryReserveError>(),
        size_of::<Option<std::result::Result<(), NativeDirectoryFailure>>>(),
        size_of::<Option<std::result::Result<(), NativeDirectoryFailure>>>(),
        size_of::<Option<std::result::Result<(), NativeDirectoryFailure>>>(),
        size_of::<Option<std::result::Result<(), NativeDirectoryFailure>>>(),
        size_of::<Option<std::result::Result<(), NativeDirectoryFailure>>>(),
        size_of::<Option<std::result::Result<(), StorageFetchFailure>>>(),
        size_of::<Option<std::result::Result<(), StorageFetchFailure>>>(),
        size_of::<Option<std::result::Result<(), ShardDirectoryCheckFailure>>>(),
        size_of::<Option<Instant>>(),
        size_of::<Option<Instant>>(),
        size_of::<Option<CheckedDirectoryValidationPhase>>(),
        size_of::<Option<std::result::Result<NativeDirectoryUsage, NativeDirectoryFailure>>>(),
        size_of::<Option<std::result::Result<StorageFetchUsage, StorageFetchFailure>>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Option<Box<dyn Any + Send>>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<std::result::Result<Step<()>, Box<dyn Any + Send>>>(),
        size_of::<std::result::Result<(), Box<dyn Any + Send>>>(),
        size_of::<std::result::Result<(), Box<dyn Any + Send>>>(),
        size_of::<std::result::Result<(), Box<dyn Any + Send>>>(),
        size_of::<Step<()>>(),
        size_of::<
            std::result::Result<
                RetainedCheckedDirectoryValidation,
                RetainedCheckedDirectoryValidationError,
            >,
        >(),
        size_of::<bool>(),
        size_of::<bool>(),
        size_of::<[usize; 6]>(),
        size_of::<[usize; 2]>(),
    ];
    assert_eq!(fixed.len(), 47);
    let bytes = fixed.into_iter().sum::<usize>();
    assert_eq!(fixed_fee(), Some((bytes, 47)));
    let p = Temporary::new();
    let (_owner, handle) = fixture(&p, 0);
    for axis in 0..3 {
        for exact in [false, true] {
            let mut policy = limits();
            let amount = if exact { 1 } else { 0 };
            match axis {
                0 => policy.max_declared_bytes = bytes - 1 + amount,
                1 => policy.max_entries = 46 + amount,
                _ => policy.max_work_bytes = bytes + 46 + amount,
            }
            let e = Instant::now() + Duration::from_secs(30);
            let core =
                NativeDirectoryBudget::new(policy, e).expect("valid independently fixed policy");
            let native = StorageFetchBudget::new(native_limits(), e).expect("native counters");
            let checks = HostChecks::new(&core, &native);
            let error=handle.retain().validate_for_storage_fetch_until(&core,&native,&checks).expect_err("exact fixed header fits, subsequent recursive frame does not; one less refuses fixed header");
            assert!(error.observations().next().is_none());
            assert_eq!(error.retained_capacity(), 0);
            assert!(matches!(
                error.core_error(),
                Some(NativeDirectoryFailure::Capacity)
            ));
            let usage = core.usage().expect("actual fixed admission");
            assert_eq!(usage.operations, if exact { 1 } else { 0 });
            assert_eq!(usage.declared_bytes, if exact { bytes } else { 0 });
            assert_eq!(usage.entries, if exact { 47 } else { 0 });
            assert_eq!(usage.work_bytes, if exact { bytes + 47 } else { 0 });
            assert_eq!(checks.denials.get(), 1);
        }
    }
    let plain = checked_directory_validation_cost(limits()).expect("plain checked ceiling");
    let original = native_directory_cost(limits()).expect("same original Core ceiling");
    assert_eq!(plain.declared_bytes, original.declared_bytes + bytes);
    assert_eq!(plain.entries, original.entries + 47);
    assert_eq!(plain.work_bytes, original.work_bytes + bytes + 47);
    assert_eq!(plain.string_bytes, original.string_bytes);
}

#[test]
fn real_deep_walk_pays_actual_result_capacity_and_bounded_name_before_any_next_io() {
    let p = Temporary::new();
    let (_owner, handle) = fixture(&p, 6);
    let (core, native) = budgets();
    let checks = HostChecks::new(&core, &native);
    let report = handle
        .retain()
        .validate_for_storage_fetch_until(&core, &native, &checks)
        .expect("actual deep descriptor walk");
    let usage = core.usage().expect("same real cumulative fees");
    let count = report.observations().count();
    assert!(count > 32);
    assert!(report.retained_capacity() >= count);
    assert!(usage.declared_bytes >= report.retained_capacity() * size_of::<Outcome>());
    assert!(usage.entries >= report.retained_capacity() + count * 4);
    assert!(usage.string_bytes > 0);
    drop(report);
    let mut policy = limits();
    policy.max_component_bytes = 1;
    let e = Instant::now() + Duration::from_secs(30);
    let core = NativeDirectoryBudget::new(policy, e).expect("finite low component bound");
    let native = StorageFetchBudget::new(native_limits(), e).expect("same native kind");
    let checks = HostChecks::new(&core, &native);
    let error = handle
        .retain()
        .validate_for_storage_fetch_until(&core, &native, &checks)
        .expect_err("real filesystem component exceeds policy before native conversion/open");
    assert!(matches!(
        error.core_error(),
        Some(NativeDirectoryFailure::Capacity)
    ));
    assert!(
        !error
            .observations()
            .any(|e| e.phase() == CheckedDirectoryValidationPhase::ComponentName)
    );
    assert_eq!(checks.denials.get(), 1);
}

#[test]
fn shared_remaining_counters_and_preentry_refusal_retain_input_without_new_arc_or_heap() {
    let p = Temporary::new();
    let (owner, handle) = fixture(&p, 0);
    let original = Arc::downgrade(&handle.inner);
    let mut policy = limits();
    policy.max_operations = 1;
    let e = Instant::now() + Duration::from_secs(30);
    let core = NativeDirectoryBudget::new(policy, e).expect("one cumulative public operation");
    let native = StorageFetchBudget::new(native_limits(), e).expect("same native counters");
    native
        .charge_values(13, 2, 17)
        .expect("real sibling native debit");
    let expected_native = native.usage().expect("actual initial native debit");
    let checks = HostChecks::new(&core, &native);
    let report = handle
        .validate_for_storage_fetch_until(&core, &native, &checks)
        .expect("first real operation");
    assert_eq!(
        report
            .native_usage()
            .expect("native usage")
            .as_ref()
            .expect("actual"),
        &expected_native
    );
    let whole = report.into_handle();
    let e = whole
        .validate_for_storage_fetch_until(&core, &native, &checks)
        .expect_err("second cannot reset cumulative operation allowance");
    assert!(e.observations().next().is_none());
    assert_eq!(e.retained_capacity(), 0);
    assert_eq!(core.usage().expect("same cumulative fees").operations, 1);
    assert!(Arc::ptr_eq(
        &e.scope.input.inner,
        &original.upgrade().expect("same actual whole input")
    ));
    drop(e);
    drop(owner);
    let p = Temporary::new();
    let (owner, handle) = fixture(&p, 0);
    let strong = Arc::strong_count(&handle.inner);
    let (core, native) = budgets();
    native.deny();
    let checks = HostChecks::new(&core, &native);
    let e = handle
        .validate_for_storage_fetch_until(&core, &native, &checks)
        .expect_err("already denied actual native counter");
    assert_eq!(Arc::strong_count(&e.scope.input.inner), strong);
    assert_eq!(e.retained_capacity(), 0);
    assert!(e.scope.effects.pending.is_none());
    assert!(e.scope.effects.allocation.is_none());
    assert_eq!(
        core.usage().expect("zero preentry usage"),
        NativeDirectoryUsage::default()
    );
    assert!(Arc::ptr_eq(e.scope.input.owner(), &owner));
    assert_eq!(checks.denials.get(), 1);
}

#[test]
fn actual_named_child_replacement_refuses_same_original_ordinary_identity_predicate() {
    let p = Temporary::new();
    let (_owner, handle) = fixture(&p, 1);
    let original = handle.path().to_owned();
    let moved = p.0.join("actual-retained-old-child");
    fs::rename(&original, &moved).expect("real child name replacement");
    fs::create_dir(&original).expect("real different inode at same name");
    assert!(matches!(
        handle.validate(),
        Err(DirectoryOwnershipError::IdentityMismatch)
    ));
    let (core, native) = budgets();
    let checks = HostChecks::new(&core, &native);
    let error = handle
        .validate_for_storage_fetch_until(&core, &native, &checks)
        .expect_err("same ordinary identity policy");
    assert_eq!(
        error.failure(),
        CheckedDirectoryValidationFailure::Ownership
    );
    assert!(error.observations().any(|v| v.phase()
        == CheckedDirectoryValidationPhase::NamedChildStat
        && v.returned_success()));
    assert!(matches!(
        &error
            .scope
            .effects
            .values
            .last()
            .expect("shared pure result retained")
            .value,
        Value::Identity(Err(DirectoryOwnershipError::IdentityMismatch))
    ));
    assert_eq!(checks.denials.get(), 1);
}

#[test]
fn public_safe_carriers_are_send_without_artificial_sync_or_native_owner_error_escape() {
    fn send<T: Send>() {}
    send::<RetainedCheckedDirectoryValidation>();
    send::<RetainedCheckedDirectoryValidationError>();
    let p = Temporary::new();
    let (_owner, handle) = fixture(&p, 0);
    let (core, native) = budgets();
    let checks = HostChecks::new(&core, &native);
    native.deny();
    let error = handle
        .validate_for_storage_fetch_until(&core, &native, &checks)
        .expect_err("actual typed refusal");
    let source = error.source().expect("safe actual native refusal");
    assert!(source.downcast_ref::<StorageFetchFailure>().is_some());
    assert!(
        source
            .downcast_ref::<RetainedCheckedDirectoryValidationError>()
            .is_none()
    );
}
