//! Genuine filesystem history and whole Core/native lifetime regression bodies.

use super::*;
use crate::{DirectoryOwnership, DirectoryOwnershipError, OwnedPinnedShardStore, ShardId};
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
type CheckAction = RefCell<Option<Box<dyn FnMut(usize)>>>;

struct Checks {
    denied: Rc<Cell<bool>>,
    calls: Cell<usize>,
    denial_calls: Cell<usize>,
    panic_deny: Cell<bool>,
    core: Option<NativeDirectoryBudget>,
    native: Option<StorageFetchBudget>,
    action: CheckAction,
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
            "core-owned-pinned-read-{}-{}",
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

fn opened_log(
    fixture: &Fixture,
    c: &NativeDirectoryBudget,
    n: &StorageFetchBudget,
    checks: &Checks,
) -> (
    OwnedPinnedShardLog,
    Arc<DirectoryOwnership>,
    OwnedDirectoryHandle,
) {
    let (store, owner, child) = fixture.open();
    let log = store
        .into_shard_log_until(n, c, checks)
        .expect("genuine original Core/native conversion");
    (log, owner, child)
}

#[test]
fn actual_empty_contiguous_and_rotated_reads_keep_constructor_and_fresh_read_reports() {
    for sequences in [&[][..], &[9][..], &[9, 10, 11][..]] {
        for target in [1, 1 << 20] {
            let fixture = Fixture::new(sequences, target);
            let before = fixture.bytes();
            let c = core(core_limits());
            let n = native(native_limits());
            let checks = Checks::for_command(&c, &n);
            let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
            let original_e = n.absolute_expiry();
            let pre_capacity = log
                .pre_validation()
                .expect("actual constructor pre")
                .retained_capacity();
            let post_capacity = log
                .post_validation()
                .expect("actual constructor post")
                .retained_capacity();
            let complete = log
                .read_from_until(0, 8192, 32, &n, &c, &checks)
                .expect("actual descriptor-pinned canonical read");
            assert_eq!(
                complete
                    .records()
                    .iter()
                    .map(|r| r.sequence)
                    .collect::<Vec<_>>(),
                sequences
            );
            assert!(
                complete
                    .records()
                    .iter()
                    .all(|r| r.payload.as_ref() == b"real canonical fixture")
            );
            assert_eq!(
                complete
                    .constructor_pre_validation()
                    .expect("same constructor pre")
                    .retained_capacity(),
                pre_capacity
            );
            assert_eq!(
                complete
                    .constructor_post_validation()
                    .expect("same constructor post")
                    .retained_capacity(),
                post_capacity
            );
            assert!(
                complete
                    .pre_validation()
                    .expect("fresh read pre")
                    .returned_success()
            );
            assert!(
                complete
                    .post_validation()
                    .expect("fresh read post")
                    .returned_success()
            );
            assert_eq!(c.usage().expect("same cumulative Core state").operations, 6);
            assert_eq!(n.absolute_expiry(), original_e);
            assert_eq!(fixture.bytes(), before);
            drop(owner);
            drop(child);
            fixture.contended();
            let log = complete.into_log();
            fixture.contended();
            assert_eq!(
                log.bounds(),
                sequences
                    .first()
                    .copied()
                    .zip(sequences.last().map(|v| v + 1))
            );
            assert_eq!(
                log.pre_validation()
                    .expect("original constructor restored")
                    .retained_capacity(),
                pre_capacity
            );
            drop(log);
            fixture.free();
        }
    }
}

#[test]
fn actual_pages_restore_same_log_and_spend_remaining_original_native_attempts() {
    let fixture = Fixture::new(&[2, 3], 1 << 20);
    let c = core(core_limits());
    let mut policy = native_limits();
    policy.max_read_attempts = 1;
    let n = native(policy);
    let checks = Checks::for_command(&c, &n);
    let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
    let original_e = n.absolute_expiry();
    let first = log
        .read_from_until(2, 8192, 1, &n, &c, &checks)
        .expect("first genuine physical page");
    assert_eq!(first.records()[0].sequence, 2);
    let usage = n.usage().expect("original first-page usage");
    assert_eq!(usage.read_attempts, 1);
    let log = first.into_log();
    let error = log
        .read_from_until(3, 8192, 1, &n, &c, &checks)
        .expect_err("no per-page native counter reconstruction");
    assert_eq!(error.native_returned_success(), Some(false));
    assert_eq!(n.usage().expect("same spent native usage").read_attempts, 1);
    assert_eq!(n.absolute_expiry(), original_e);
    assert!(
        error
            .pre_validation()
            .expect("fresh second read pre")
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
fn actual_zero_attempt_native_error_keeps_input_and_typed_read_results() {
    let fixture = Fixture::new(&[6], 1 << 20);
    let c = core(core_limits());
    let mut policy = native_limits();
    policy.max_read_attempts = 0;
    let n = native(policy);
    let checks = Checks::for_command(&c, &n);
    let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
    let error = log
        .read_from_until(6, 8192, 1, &n, &c, &checks)
        .expect_err("actual immutable no-read-attempt policy");
    assert_eq!(error.native_returned_success(), Some(false));
    assert!(error.observation().is_some());
    assert!(error.pin_borrow_observations().0);
    assert_eq!(checks.denial_calls.get(), 1);
    assert_eq!(error.outer_denial(), (true, true));
    assert!(c.check().is_err());
    assert!(n.check().is_err());
    drop(owner);
    drop(child);
    fixture.contended();
    drop(error);
    fixture.free();
}

#[test]
fn every_real_completion_retains_native_ok_and_fresh_reports_before_sibling_refusal() {
    for phase in [
        Phase::FixedAdmission,
        Phase::Split,
        Phase::PreRetain,
        Phase::PreValidation,
        Phase::PinBorrowed,
        Phase::Native,
        Phase::PostRetain,
        Phase::PostValidation,
        Phase::Usage,
    ] {
        let fixture = Fixture::new(&[4, 5], 1 << 20);
        let c = core(core_limits());
        let n = native(native_limits());
        let checks = Checks::for_command(&c, &n);
        let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
        let sibling = n.clone();
        let seen = Rc::new(Cell::new(false));
        let mark = Rc::clone(&seen);
        let hook = observe(move |actual| {
            if actual == phase {
                mark.set(true);
                sibling.deny();
            }
        });
        let error = log
            .read_from_until(4, 8192, 2, &n, &c, &checks)
            .expect_err("actual sibling denies the SAME state");
        drop(hook);
        assert!(seen.get());
        assert!(c.check().is_err());
        assert!(n.check().is_err());
        if matches!(
            phase,
            Phase::Native | Phase::PostRetain | Phase::PostValidation | Phase::Usage
        ) {
            assert_eq!(error.native_returned_success(), Some(true));
            assert_eq!(
                error
                    .records()
                    .iter()
                    .map(|r| r.sequence)
                    .collect::<Vec<_>>(),
                vec![4, 5]
            );
            assert!(
                error
                    .observation()
                    .expect("actual native success retained")
                    .outcomes()
                    .iter()
                    .any(|o| matches!(o, PinnedShardFetchOutcome::Read { result: Ok(_), .. }))
            );
        }
        if matches!(phase, Phase::PostValidation | Phase::Usage) {
            assert!(
                error
                    .post_validation()
                    .expect("actual post report before postcheck")
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
fn later_core_minimum_and_real_health_refusal_keep_native_ok_and_original_e() {
    for shorten in [false, true] {
        let fixture = Fixture::new(&[12], 1 << 20);
        let c = core(core_limits());
        let n = native(native_limits());
        let checks = Checks::for_command(&c, &n);
        let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
        let original_e = n.absolute_expiry();
        let core_sibling = c.clone();
        let health = Rc::clone(&checks.denied);
        let hook = observe(move |phase| {
            if phase == Phase::Native {
                if shorten {
                    let _ = core_sibling.cap_expiry(Instant::now());
                } else {
                    health.set(true);
                }
            }
        });
        let error = log
            .read_from_until(12, 8192, 1, &n, &c, &checks)
            .expect_err("later SAME minimum/health refusal");
        drop(hook);
        assert_eq!(error.native_returned_success(), Some(true));
        assert_eq!(error.records()[0].sequence, 12);
        assert_eq!(n.absolute_expiry(), original_e);
        assert_eq!(checks.denial_calls.get(), 1);
        drop(owner);
        drop(child);
        fixture.contended();
        drop(error);
        fixture.free();
    }
}

#[test]
fn genuine_child_rename_after_physical_read_retains_native_ok_and_postvalidation_error() {
    let fixture = Fixture::new(&[7], 1 << 20);
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
    let source = fixture.shard.clone();
    let moved = fixture.root.join("actual-moved-shard");
    let destination = moved.clone();
    let hook = observe(move |phase| {
        if phase == Phase::Native {
            fs::rename(&source, &destination)
                .expect("real retained-directory rename after completed physical read");
        }
    });
    let error = log
        .read_from_until(7, 8192, 1, &n, &c, &checks)
        .expect_err("genuine path identity refused by actual postvalidator");
    drop(hook);
    assert_eq!(error.native_returned_success(), Some(true));
    assert_eq!(error.records()[0].sequence, 7);
    assert!(
        !error
            .post_validation()
            .expect("whole actual validation refusal")
            .returned_success()
    );
    drop(owner);
    drop(child);
    fixture.contended_at(&moved);
    drop(error);
    fs::rename(&moved, &fixture.shard)
        .expect("fixture restores actual renamed child after every owner dropped");
    fixture.free();
}

#[test]
fn actual_native_name_replacement_refuses_read_and_keeps_original_held_file() {
    let fixture = Fixture::new(&[14], 1 << 20);
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
    let pack = fs::read_dir(&fixture.shard)
        .expect("actual pack names")
        .map(|v| v.expect("real entry").path())
        .find(|p| p.file_name().expect("real name") != ".shard-store.lock")
        .expect("real native pack");
    let moved = fixture.root.join("actual-original-pack");
    fs::rename(&pack, &moved).expect("move actual opened native pack");
    fs::copy(&moved, &pack).expect("real distinct inode at original basename");
    let error = log
        .read_from_until(14, 8192, 1, &n, &c, &checks)
        .expect_err("real held/named identity mismatch");
    assert_eq!(error.native_returned_success(), Some(false));
    assert!(error.observation().is_some());
    assert_eq!(checks.denial_calls.get(), 1);
    drop(owner);
    drop(child);
    fixture.contended();
    drop(error);
    fixture.free();
}

#[test]
fn actual_bridge_completion_unwind_keeps_native_result_and_independent_denial_panic() {
    for phase in [
        Phase::FixedAdmission,
        Phase::Split,
        Phase::PreRetain,
        Phase::PreValidation,
        Phase::PinBorrowed,
        Phase::Native,
        Phase::PostRetain,
        Phase::PostValidation,
        Phase::Usage,
    ] {
        let fixture = Fixture::new(&[8], 1 << 20);
        let c = core(core_limits());
        let n = native(native_limits());
        let checks = Checks::for_command(&c, &n);
        let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
        checks.panic_deny.set(true);
        let hook = observe(move |actual| {
            if actual == phase {
                std::panic::panic_any("actual Core read completion unwind");
            }
        });
        let error = log
            .read_from_until(8, 8192, 1, &n, &c, &checks)
            .expect_err("actual observer unwound");
        drop(hook);
        assert!(error.has_panic_payloads()[0]);
        assert!(error.has_panic_payloads()[3]);
        assert_eq!(error.outer_denial(), (true, false));
        assert_eq!(checks.denial_calls.get(), 1);
        if matches!(
            phase,
            Phase::Native | Phase::PostRetain | Phase::PostValidation | Phase::Usage
        ) {
            assert_eq!(error.native_returned_success(), Some(true));
        }
        drop(owner);
        drop(child);
        fixture.contended();
        drop(error);
        fixture.free();
    }
}

#[test]
fn actual_nested_native_denial_panic_remains_in_native_carrier_not_retried() {
    let fixture = Fixture::new(&[15], 1 << 20);
    let c = core(core_limits());
    let mut policy = native_limits();
    policy.max_read_attempts = 0;
    let n = native(policy);
    let checks = Checks::for_command(&c, &n);
    let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
    checks.panic_deny.set(true);
    let error = log
        .read_from_until(15, 8192, 1, &n, &c, &checks)
        .expect_err("actual native producer denial unwinds");
    assert_eq!(checks.denial_calls.get(), 1);
    assert_eq!(error.outer_denial(), (true, false));
    assert!(
        error
            .observation()
            .expect("actual physical native carrier")
            .has_panic_payloads()[2]
    );
    assert!(
        !error.has_panic_payloads()[3],
        "original nested payload is not fabricated or duplicated at Core bridge"
    );
    drop(owner);
    drop(child);
    fixture.contended();
    drop(error);
    fixture.free();
}

#[test]
fn actual_read_local_validator_panic_and_denial_remain_in_real_validation_result() {
    let fixture = Fixture::new(&[16], 1 << 20);
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
    let armed = Rc::new(Cell::new(false));
    let gate = Rc::clone(&armed);
    let after_retain = Rc::new(Cell::new(0usize));
    let count = Rc::clone(&after_retain);
    *checks.action.borrow_mut() = Some(Box::new(move |_| {
        if gate.get() {
            let next = count.get() + 1;
            count.set(next);
            // Real retain postcheck, real bridge validation-entry check, then
            // the consuming validator's first actual composing callback.
            if next == 3 {
                std::panic::panic_any("real read-local validation check unwind");
            }
        }
    }));
    checks.panic_deny.set(true);
    let hook = observe(move |phase| {
        if phase == Phase::PreRetain {
            armed.set(true);
        }
    });
    let error = log
        .read_from_until(16, 8192, 1, &n, &c, &checks)
        .expect_err("actual composed check unwind retains input");
    drop(hook);
    assert_eq!(checks.denial_calls.get(), 1);
    assert!(error.native_returned_success().is_none());
    assert_eq!(
        error
            .pre_validation()
            .expect("whole genuine read-local validator error")
            .panic_observations(),
        [true, false, false, true]
    );
    assert_eq!(
        error.has_panic_payloads(),
        [false; 4],
        "actual nested payloads remain in their actual carrier"
    );
    drop(owner);
    drop(child);
    fixture.contended();
    drop(error);
    fixture.free();
}

#[test]
fn actual_pin_busy_and_poison_results_settle_before_same_context_can_drop() {
    for poison in [false, true] {
        let fixture = Fixture::new(&[17], 1 << 20);
        let c = core(core_limits());
        let n = native(native_limits());
        let checks = Checks::for_command(&c, &n);
        let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
        // Genuine SAME directory clone is a test's separately owned job input;
        // the product read never clones it or fabricates a native pin.
        let retained = size_of::<super::super::OwnedShardStoreDirectory>();
        c.reserve(0, 0, retained, 0, 1, retained + 1)
            .expect("prepaid actual test-job Arc header and increment");
        let directory = log
            .scope
            .directory
            .as_ref()
            .expect("actual retained original directory")
            .clone();
        let guard = if poison {
            let actual = std::panic::catch_unwind(AssertUnwindSafe(|| {
                let _held = directory
                    .inner
                    .native
                    .lock()
                    .expect("real private pin guard");
                std::panic::panic_any("actual guard poison");
            }));
            assert!(actual.is_err());
            None
        } else {
            Some(
                directory
                    .inner
                    .native
                    .lock()
                    .expect("actual competing job's pin borrow"),
            )
        };
        let error = log
            .read_from_until(17, 8192, 1, &n, &c, &checks)
            .expect_err("actual private borrow refuses");
        assert_eq!(error.pin_borrow_observations(), (false, !poison, poison));
        assert!(error.native_returned_success().is_none());
        assert_eq!(checks.denial_calls.get(), 1);
        drop(guard);
        drop(owner);
        drop(child);
        fixture.contended();
        drop(error);
        fixture.contended();
        drop(directory);
        fixture.free();
    }
}

#[test]
fn both_read_completion_and_restored_log_drop_orders_hold_real_root_and_native_lease() {
    for report_first in [false, true] {
        let fixture = Fixture::new(&[18], 1 << 20);
        let c = core(core_limits());
        let n = native(native_limits());
        let checks = Checks::for_command(&c, &n);
        let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
        let retained = size_of::<super::super::OwnedShardStoreDirectory>();
        c.reserve(0, 0, retained, 0, 1, retained + 1)
            .expect("prepaid actual physical job owner");
        let directory = log
            .scope
            .directory
            .as_ref()
            .expect("same physical test job owner")
            .clone();
        let complete = log
            .read_from_until(18, 8192, 1, &n, &c, &checks)
            .expect("genuine completed physical read");
        drop(owner);
        drop(child);
        fixture.contended();
        if report_first {
            drop(complete);
            fixture.contended();
            drop(directory);
        } else {
            drop(directory);
            let log = complete.into_log();
            fixture.contended();
            drop(log);
        }
        fixture.free();
    }
}

#[test]
fn fixed_bridge_entries_are_exact_or_one_less_before_any_read_local_arc_or_io() {
    const FIXED_ENTRIES: usize = 82;
    let (bytes, fixed_entries, raw_work) = fixed_fee().expect("plain actual named fixed values");
    assert_eq!(
        fixed_entries, FIXED_ENTRIES,
        "immutable82 headers plus independent retained flags/results"
    );
    for fitting in [false, true] {
        let fixture = Fixture::new(&[19], 1 << 20);
        let c = core(core_limits());
        let n = native(native_limits());
        let checks = Checks::for_command(&c, &n);
        let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
        let usage = c.usage().expect("already spent real constructor prefix");
        let limits = c.limits();
        let remainder = limits.max_entries - usage.entries;
        let target = FIXED_ENTRIES - usize::from(!fitting);
        c.reserve(0, 0, 0, 0, remainder - target, 0)
            .expect("preconsume original real entries without resetting policy");
        let seen = Rc::new(Cell::new(false));
        let mark = Rc::clone(&seen);
        let hook = observe(move |phase| {
            if phase == Phase::FixedAdmission {
                mark.set(true);
            }
        });
        let strong = Arc::strong_count(&child.inner);
        let error = log
            .read_from_until(19, 8192, 1, &n, &c, &checks)
            .expect_err("later actual parent retention still needs its own slots");
        drop(hook);
        assert_eq!(seen.get(), fitting);
        assert!(error.native_returned_success().is_none());
        assert_eq!(
            Arc::strong_count(&child.inner),
            strong,
            "no unpaid parent Arc increment"
        );
        assert!(bytes > 0 && raw_work == bytes);
        assert_eq!(checks.denial_calls.get(), 1);
        drop(owner);
        drop(child);
        fixture.contended();
        drop(error);
        fixture.free();
    }
}

#[test]
fn plain_cost_has_two_validation_frames_two_retains_and_incremental_native_only() {
    let c = core_limits();
    let n = native_limits();
    let cost = owned_pinned_shard_read_cost(c, n).expect("plain source-derived read framing");
    let core_cost = native_directory_cost(c).expect("one configured Core ceiling");
    let validator = checked_directory_validation_cost(c).expect("one validator frame");
    let open = pinned_shard_log_open_cost(n).expect("one original native global+constructor");
    let read = pinned_shard_log_read_cost(n).expect("one native global+constructor+read");
    let (fixed, entries, work) = fixed_fee().expect("actual independent bridge values");
    let (retain, retain_entries, retain_work) =
        retain_fee().expect("actual parent retain source fee");
    assert_eq!(
        cost.validation_framing_bytes,
        2 * (validator.declared_bytes - core_cost.declared_bytes)
    );
    assert_eq!(
        cost.declared_bytes,
        fixed + 2 * retain + cost.validation_framing_bytes
    );
    assert_eq!(
        cost.entries,
        entries + 2 * retain_entries + cost.validation_framing_entries
    );
    assert_eq!(
        cost.work_bytes,
        work + entries
            + 2 * (retain_work + retain_entries)
            + 2 * (validator.work_bytes - core_cost.work_bytes)
    );
    assert_eq!(
        cost.native_framing_declared_bytes,
        read.declared_bytes - open.declared_bytes
    );
    assert_eq!(cost.native_framing_entries, read.entries - open.entries);
    assert_eq!(
        cost.native_framing_string_bytes,
        read.string_bytes - open.string_bytes
    );
    assert_eq!(
        cost.native_framing_work_bytes,
        read.work_bytes - open.work_bytes
    );
    assert_eq!(cost.string_bytes, 0);
    assert_eq!(cost.core_operations, 3);
}

#[test]
fn safe_complete_and_error_surfaces_are_send_without_native_error_downcast_escape() {
    fn send<T: Send>() {}
    send::<OwnedPinnedShardReadComplete>();
    send::<RetainedOwnedPinnedShardReadError>();
    let fixture = Fixture::new(&[20], 1 << 20);
    let c = core(core_limits());
    let mut policy = native_limits();
    policy.max_read_attempts = 0;
    let n = native(policy);
    let checks = Checks::for_command(&c, &n);
    let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
    let error = log
        .read_from_until(20, 8192, 1, &n, &c, &checks)
        .expect_err("real retained native error");
    assert!(
        error.source().is_none(),
        "native owning error must not be reachable through Error::source"
    );
    drop(owner);
    drop(child);
    fixture.contended();
    drop(error);
    fixture.free();
}

#[test]
fn genuine_leading_zero_basename_survives_actual_open_conversion_and_read() {
    let fixture = Fixture::new(&[21], 1 << 20);
    let original = fs::read_dir(&fixture.shard)
        .expect("actual native directory")
        .map(|e| e.expect("real entry").path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("pack-"))
        })
        .expect("genuine native pack");
    let spelling = original
        .file_name()
        .expect("actual pack spelling")
        .to_str()
        .expect("actual ASCII native name");
    let suffix = spelling.strip_prefix("pack-").expect("actual native role");
    let renamed = fixture
        .shard
        .join(format!("pack-{}{}", "0".repeat(96), suffix));
    fs::rename(&original, &renamed).expect("real leading-zero name before genuine bounded reopen");
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
    let complete = log
        .read_from_until(21, 8192, 1, &n, &c, &checks)
        .expect("actual scanned descriptor keeps its original spelling");
    assert_eq!(complete.records()[0].sequence, 21);
    assert_eq!(
        complete
            .observation()
            .expect("real physical report")
            .pack_name(0)
            .expect("actual bounded native CStr")
            .to_bytes(),
        renamed
            .file_name()
            .expect("actual renamed file")
            .as_encoded_bytes()
    );
    assert!(!original.exists());
    assert!(renamed.exists());
    drop(owner);
    drop(child);
    fixture.contended();
    drop(complete);
    fixture.free();
}

#[test]
fn actual_native_selection_boundaries_include_first_oversize_and_limit_record_count() {
    for (start, bytes, records, expected) in [
        (30, 1, 8, vec![30]),
        (31, 8192, 1, vec![31]),
        (40, 8192, 1, vec![]),
    ] {
        let fixture = Fixture::new(&[30, 31, 32], 1 << 20);
        let c = core(core_limits());
        let n = native(native_limits());
        let checks = Checks::for_command(&c, &n);
        let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
        let complete = log
            .read_from_until(start, bytes, records, &n, &c, &checks)
            .expect("shared actual native selection semantics");
        assert_eq!(
            complete
                .records()
                .iter()
                .map(|r| r.sequence)
                .collect::<Vec<_>>(),
            expected
        );
        assert!(
            complete
                .pre_validation()
                .expect("fresh read pre")
                .returned_success()
        );
        assert!(
            complete
                .post_validation()
                .expect("fresh read post")
                .returned_success()
        );
        drop(owner);
        drop(child);
        fixture.contended();
        drop(complete);
        fixture.free();
    }
    for (bytes, records) in [(0, 1), (8192, 0)] {
        let fixture = Fixture::new(&[30], 1 << 20);
        let c = core(core_limits());
        let n = native(native_limits());
        let checks = Checks::for_command(&c, &n);
        let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
        let error = log
            .read_from_until(30, bytes, records, &n, &c, &checks)
            .expect_err("native zero request policy preserved");
        assert_eq!(error.native_returned_success(), Some(false));
        assert_eq!(checks.denial_calls.get(), 1);
        drop(owner);
        drop(child);
        fixture.contended();
        drop(error);
        fixture.free();
    }
}

#[test]
fn exact_and_one_less_fixed_work_and_whole_scope_lower_bound_refuse_before_input_split() {
    const FIXED_ENTRIES: usize = 82;
    let (fixed, entries, raw_work) = fixed_fee().expect("independent source value inventory");
    assert_eq!(entries, FIXED_ENTRIES);
    assert_eq!(raw_work, fixed);
    for by_work in [false, true] {
        for fitting in [false, true] {
            let fixture = Fixture::new(&[22], 1 << 20);
            let c = core(core_limits());
            let n = native(native_limits());
            let checks = Checks::for_command(&c, &n);
            let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
            let usage = c.usage().expect("actual already-spent constructor prefix");
            let limits = c.limits();
            if by_work {
                let leave = fixed + FIXED_ENTRIES - usize::from(!fitting);
                c.reserve(
                    0,
                    0,
                    0,
                    0,
                    0,
                    limits.max_work_bytes - usage.work_bytes - leave,
                )
                .expect("spend SAME original work without policy reconstruction");
            } else {
                let leave = if fitting {
                    fixed
                } else {
                    size_of::<Scope>() - 1
                };
                c.reserve(
                    0,
                    0,
                    limits.max_declared_bytes - usage.declared_bytes - leave,
                    0,
                    0,
                    0,
                )
                .expect("spend SAME original declared bytes");
            }
            let seen = Rc::new(Cell::new(false));
            let mark = Rc::clone(&seen);
            let sibling = n.clone();
            let hook = observe(move |phase| {
                if phase == Phase::FixedAdmission {
                    mark.set(true);
                    sibling.deny();
                }
            });
            let before = Arc::strong_count(&child.inner);
            let error = log
                .read_from_until(22, 8192, 1, &n, &c, &checks)
                .expect_err("real fixed boundary or immutable whole-scope lower bound");
            drop(hook);
            assert_eq!(seen.get(), fitting);
            assert_eq!(Arc::strong_count(&child.inner), before);
            assert!(error.scope.early.is_some());
            assert!(error.scope.context.is_none());
            assert!(error.native_returned_success().is_none());
            assert_eq!(checks.denial_calls.get(), 1);
            drop(owner);
            drop(child);
            fixture.contended();
            drop(error);
            fixture.free();
        }
    }
}

#[test]
fn actual_coalesced_unselected_canonical_record_stays_private_through_core_refusal() {
    const A1: &[u8] = b"selected canonical first";
    const B: &[u8] = b"UNSELECTED-CANONICAL-PRIVATE-SENTINEL";
    const A2: &[u8] = b"selected canonical second";
    let fixture = Fixture::new(&[], 1 << 20);
    let mut config = ShardLogConfig::new(&fixture.shard);
    config.target_pack_bytes = fixture.target;
    config.max_record_bytes = 4096;
    let mut writer = ShardLog::open(config).expect("actual ordinary canonical writer");
    // Real accepted canonical sequences are contiguous, but physical order may
    // differ. The selected 0/1 payloads surround the unselected sequence 2.
    for (sequence, payload) in [(0, A1), (2, B), (1, A2)] {
        writer
            .append_group(sequence, &[payload.to_vec().into()], true)
            .expect("actual durable canonical payload");
    }
    assert_eq!(
        writer.bounds().expect("actual complete canonical bounds"),
        Some((0, 3))
    );
    let expected = writer
        .read_from(0, 8192, 2)
        .expect("ordinary selected records");
    assert_eq!(
        expected.iter().map(|v| v.sequence).collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(expected[0].payload.as_ref(), A1);
    assert_eq!(expected[1].payload.as_ref(), A2);
    drop(writer);
    let before = fixture.bytes();
    let c = core(core_limits());
    let n = native(native_limits());
    let original_e = n.absolute_expiry();
    let checks = Checks::for_command(&c, &n);
    let (log, owner, child) = opened_log(&fixture, &c, &n, &checks);
    assert_eq!(log.bounds(), Some((0, 3)));
    let denied = Rc::clone(&checks.denied);
    let _observer = observe(move |phase| {
        if phase == Phase::Native {
            // The real native Result and its raw physical owner already exist.
            denied.set(true);
        }
    });
    let error = log
        .read_from_until(0, 8192, 2, &n, &c, &checks)
        .expect_err("actual captured-health refusal after native read");
    assert_eq!(error.native_returned_success(), Some(true));
    assert_eq!(error.failure(), OwnedPinnedShardReadFailure::Checkpoint);
    assert_eq!(error.records(), expected.as_slice());
    // Only this test module can traverse the private owning bridge state. The
    // public Core facade never provides this underlying physical observation.
    let native = error
        .scope
        .operation
        .native
        .as_ref()
        .expect("actual returned native Result");
    let raw = native
        .as_ref()
        .expect("real completed native read")
        .observation()
        .expect("actual raw report");
    let (bytes, filled) = raw
        .partial_buffer(0)
        .expect("actual retained coalesced range");
    assert_eq!(filled, bytes.len());
    for payload in [A1, B, A2] {
        assert!(bytes.windows(payload.len()).any(|w| w == payload));
    }
    let observed = error.observation().expect("attenuated Core observation");
    assert_eq!(
        observed.partial_buffer_lengths(0),
        Some((bytes.len(), filled))
    );
    assert_eq!(observed.partial_buffer_lengths(1), None);
    assert!(
        observed
            .outcomes()
            .iter()
            .any(|v| matches!(v, PinnedShardFetchOutcome::Read { result: Ok(n), .. } if *n > 0))
    );
    let sentinel = std::str::from_utf8(B).expect("static ASCII sentinel");
    assert!(!format!("{error:?} {error}").contains(sentinel));
    assert!(!format!("{raw:?}").contains(sentinel));
    assert!(c.check().is_err());
    assert!(n.check().is_err());
    assert_eq!(n.absolute_expiry(), original_e);
    assert_eq!(fixture.bytes(), before);
    drop(owner);
    drop(child);
    fixture.contended();
    drop(error);
    fixture.free();
}
