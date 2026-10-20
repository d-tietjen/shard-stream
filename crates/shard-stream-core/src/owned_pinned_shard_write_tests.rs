//! Genuine file-backed canonical mutation, failure and lifetime regressions.

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
    // Every canonical call prepays its complete physical policy into this SAME
    // fetch ledger. The longest fixture performs four append/GC cycles. Check
    // all three cumulative axes before creating the original budget; do not
    // renew it between operations or enlarge its existing global ceilings.
    let global = storage_fetch_cost(limits).expect("actual shared fetch cost");
    let per_call = pinned_shard_log_gc_cost(limits, append_limits(), write_limits(), gc_limits())
        .expect("actual conservative write/GC cost");
    let entry_work = per_call
        .entries
        .checked_sub(global.entries)
        .expect("incremental entry cost");
    for (axis, total, setup, ceiling) in [
        (
            "declared bytes",
            per_call.declared_bytes,
            global.declared_bytes,
            limits.max_declared_bytes,
        ),
        (
            "entries",
            per_call.entries,
            global.entries,
            limits.max_entries,
        ),
        (
            "work bytes",
            per_call
                .work_bytes
                .checked_add(entry_work)
                .expect("shared fetch also charges one work byte per entry"),
            global.work_bytes,
            limits.max_work_bytes,
        ),
    ] {
        let cumulative = total
            .checked_sub(setup)
            .and_then(|fee| fee.checked_mul(MAX_FIXTURE_MUTATIONS))
            .expect("finite cumulative fixture cost");
        assert!(
            cumulative < ceiling,
            "actual fixture {axis} fees must fit original global ceiling: {cumulative} >= {ceiling}"
        );
    }
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
            "core-owned-pinned-mutation-{}-{}",
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

// Four new records and at most four rotations occur in any fixture. The
// longest successful restore sequence is four append/GC pairs: eight native
// append-or-seal entries. Open already limits the original scan to 32 packs.
const MAX_FIXTURE_MUTATIONS: usize = 8;
const MAX_FIXTURE_RECORDS: usize = 4;
const MAX_FIXTURE_ROTATIONS: usize = MAX_FIXTURE_RECORDS;
const MAX_FIXTURE_OPEN_PACKS: usize = 32;
fn append_limits() -> PinnedShardAppendLimits {
    PinnedShardAppendLimits {
        max_operations: MAX_FIXTURE_MUTATIONS,
        max_frames: MAX_FIXTURE_RECORDS,
        max_payload_bytes: MAX_FIXTURE_RECORDS * b"actual owned canonical payload".len(),
        max_segments: MAX_FIXTURE_RECORDS,
        max_rotations: MAX_FIXTURE_ROTATIONS,
        max_path_bytes: 8192,
        max_name_bytes: 1024,
        max_packs: MAX_FIXTURE_OPEN_PACKS + MAX_FIXTURE_ROTATIONS,
        // Finite map capacity headroom for the original/new four-record sets.
        max_batch_slots: 64,
        max_topic_slots: 16,
        max_location_slots: 64,
        // Explicit finite syscall/short-write headroom; actual attempts remain
        // counted by the native producer rather than assumed to complete once.
        max_io_calls: MAX_FIXTURE_MUTATIONS * 64,
        max_write_calls: MAX_FIXTURE_RECORDS * 16,
        max_write_bytes: MAX_FIXTURE_RECORDS * 4096 * 4,
        max_work_bytes: MAX_FIXTURE_MUTATIONS * MAX_FIXTURE_OPEN_PACKS * 4096,
    }
}
fn append() -> PinnedShardAppendBudget {
    PinnedShardAppendBudget::new(append_limits(), expiry()).expect("actual original append budget")
}
fn write_limits() -> PinnedShardLogWriteLimits {
    let native = append_limits();
    let write = PinnedShardLogWriteLimits {
        // Initial scan/active companions plus both lease/active files and all
        // remaining possible creates; each actual descriptor is still checked.
        max_live_files: 2 * MAX_FIXTURE_OPEN_PACKS + 2 + MAX_FIXTURE_ROTATIONS,
        max_tracked_packs: MAX_FIXTURE_OPEN_PACKS + MAX_FIXTURE_ROTATIONS,
        max_retired_slots: MAX_FIXTURE_ROTATIONS,
        max_input_slots: 16,
    };
    assert!(write.max_retired_slots >= native.max_rotations);
    assert!(write.max_tracked_packs >= open_limits().max_packs + native.max_rotations);
    write
}
fn gc_limits() -> PinnedShardGcLimits {
    let packs = MAX_FIXTURE_OPEN_PACKS + MAX_FIXTURE_ROTATIONS;
    PinnedShardGcLimits {
        max_path_bytes: 8192,
        max_name_bytes: 1024,
        max_packs: packs,
        max_batch_slots: 64,
        max_log_start_slots: 4,
        max_topic_slots: 16,
        max_location_slots: 64,
        max_plan_packs: packs,
        max_io_calls: 5 + 11 * packs,
        max_work_bytes: MAX_FIXTURE_MUTATIONS * MAX_FIXTURE_OPEN_PACKS * 4096,
    }
}
fn records(n: usize) -> Vec<Bytes> {
    (0..n)
        .map(|_| Bytes::from_static(b"actual owned canonical payload"))
        .collect()
}

#[test]
fn real_append_seal_read_and_gc_restore_same_log_and_original_kernel_custody() {
    let fixture = Fixture::new(&[], 1);
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let (log, owner, pin) = opened_log(&fixture, &c, &n, &checks);
    let mut a = append();
    let completed = log
        .append_group_until(0, records(4), true, &mut a, &n, &c, write_limits(), &checks)
        .expect("real pinned append");
    assert_eq!(
        completed
            .observation()
            .append_receipt()
            .expect("native receipt")
            .record_count,
        4
    );
    assert!(
        completed
            .observation()
            .write_result_count()
            .expect("real write Results")
            > 0
    );
    assert!(
        completed
            .pre_validation()
            .expect("fresh prevalidation")
            .returned_success()
    );
    assert!(
        completed
            .post_validation()
            .expect("fresh postvalidation")
            .returned_success()
    );
    let read = completed
        .into_log()
        .read_from_until(0, 1 << 16, 16, &n, &c, &checks)
        .expect("same tracked descriptors read");
    assert_eq!(
        read.records()
            .iter()
            .map(|r| r.sequence)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    let log = read
        .into_log()
        .seal_until(&mut a, &n, &c, write_limits(), &checks)
        .expect("real seal")
        .into_log();
    let completed = log
        .compact_before_until(2, &mut a, &n, &c, write_limits(), gc_limits(), &checks)
        .expect("real tracked GC");
    assert!(
        completed
            .observation()
            .compaction_receipt()
            .expect("native receipt")
            .packs_removed
            > 0
    );
    assert!(
        completed
            .observation()
            .quarantined_files()
            .expect("actual quarantine")
            > 0
    );
    drop(owner);
    drop(pin);
    fixture.contended();
    let read = completed
        .into_log()
        .read_from_until(0, 1 << 16, 16, &n, &c, &checks)
        .expect("same retained suffix");
    assert_eq!(
        read.records()
            .iter()
            .map(|r| r.sequence)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert!(a.usage().operations > 1);
    drop(read);
    fixture.free();
}

#[test]
fn repeated_real_cycles_keep_original_policies_counters_and_dense_catalog() {
    let fixture = Fixture::new(&[], 1);
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let (mut log, owner, pin) = opened_log(&fixture, &c, &n, &checks);
    let mut a = append();
    let limits = a.limits();
    let original_e = a.absolute_expiry();
    for sequence in 0..4 {
        log = log
            .append_group_until(
                sequence,
                records(1),
                true,
                &mut a,
                &n,
                &c,
                write_limits(),
                &checks,
            )
            .expect("same native append")
            .into_log();
        log = log
            .compact_before_until(
                sequence,
                &mut a,
                &n,
                &c,
                write_limits(),
                gc_limits(),
                &checks,
            )
            .expect("same native GC")
            .into_log();
        let read = log
            .read_from_until(sequence, 8192, 8, &n, &c, &checks)
            .expect("same dense tracked source");
        assert_eq!(read.records().len(), 1);
        assert_eq!(read.records()[0].sequence, sequence);
        log = read.into_log();
        assert_eq!(a.limits(), limits);
        assert!(a.absolute_expiry() <= original_e);
    }
    assert!(a.usage().operations >= 8);
    drop(owner);
    drop(pin);
    fixture.contended();
    drop(log);
    fixture.free();
}

#[test]
fn original_vec_excess_capacity_and_native_file_policy_refuse_without_new_wal() {
    for input_capacity in [false, true] {
        let fixture = Fixture::new(&[], 4096);
        let c = core(core_limits());
        let n = native(native_limits());
        let checks = Checks::for_command(&c, &n);
        let (log, owner, pin) = opened_log(&fixture, &c, &n, &checks);
        let before = fixture.bytes();
        let mut a = append();
        let mut limits = write_limits();
        let mut input = Vec::with_capacity(16);
        input.push(Bytes::from_static(b"real caller input"));
        if input_capacity {
            limits.max_input_slots = 1;
        } else {
            limits.max_live_files = 1;
        }
        let error = log
            .append_group_until(0, input, true, &mut a, &n, &c, limits, &checks)
            .expect_err("immutable limit refusal");
        assert_eq!(error.original_input_capacity(), 16);
        assert_eq!(fixture.bytes(), before);
        assert!(a.is_refused());
        assert_eq!(error.outer_denial(), (true, true));
        drop(owner);
        drop(pin);
        fixture.contended();
        drop(error);
        fixture.free();
    }
}

#[test]
fn original_fixed_entries_are_exact_or_one_less_before_parent_or_native_io() {
    // Immutable policy for the 106 independently named bridge values; the
    // production fee is checked against this fixture, not used as its policy.
    let fixed_entries = 106;
    assert_eq!(fixed_fee().expect("plain fixed inventory").1, fixed_entries);
    for exact in [false, true] {
        let fixture = Fixture::new(&[], 4096);
        let setup_c = core(core_limits());
        let n = native(native_limits());
        let (log, owner, pin) = opened_log(&fixture, &setup_c, &n, &Checks::new());
        let mut limits = core_limits();
        limits.max_entries = fixed_entries - usize::from(!exact);
        let c = core(limits);
        let checks = Checks::for_command(&c, &n);
        let admitted = Rc::new(Cell::new(false));
        let entered = Rc::clone(&admitted);
        let same = c.clone();
        let _observe = observe(move |phase| {
            if phase == Phase::FixedAdmission {
                entered.set(true);
                same.deny();
            }
        });
        let before = fixture.bytes();
        let mut a = append();
        let error = log
            .append_group_until(0, records(1), true, &mut a, &n, &c, write_limits(), &checks)
            .expect_err("stop at fixed boundary");
        assert_eq!(admitted.get(), exact);
        assert_eq!(error.native_returned_success(), None);
        assert_eq!(fixture.bytes(), before);
        assert!(a.is_refused());
        drop(owner);
        drop(pin);
        drop(error);
        fixture.free();
    }
}

#[test]
fn late_core_refusal_keeps_actual_native_append_ok_and_physical_parent() {
    let fixture = Fixture::new(&[], 1);
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let (log, owner, pin) = opened_log(&fixture, &c, &n, &checks);
    let same = c.clone();
    let _observe = observe(move |phase| {
        if phase == Phase::Native {
            same.deny();
        }
    });
    let mut a = append();
    let error = log
        .append_group_until(0, records(2), true, &mut a, &n, &c, write_limits(), &checks)
        .expect_err("native Ok does not erase Core refusal");
    assert_eq!(error.native_returned_success(), Some(true));
    assert_eq!(
        error
            .observation()
            .expect("real native owner")
            .append_receipt()
            .expect("native receipt")
            .record_count,
        2
    );
    assert!(a.is_refused());
    assert_eq!(checks.denial_calls.get(), 1);
    drop(owner);
    drop(pin);
    fixture.contended();
    drop(error);
    fixture.free();
}

#[test]
fn completion_unwind_and_independent_health_panic_keep_actual_owners() {
    let fixture = Fixture::new(&[], 1);
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let (log, owner, pin) = opened_log(&fixture, &c, &n, &checks);
    checks.panic_deny.set(true);
    let _observe = observe(|phase| {
        if phase == Phase::Native {
            std::panic::panic_any("real post-native observer");
        }
    });
    let mut a = append();
    let error = log
        .append_group_until(0, records(2), true, &mut a, &n, &c, write_limits(), &checks)
        .expect_err("separate real panics retained");
    assert_eq!(error.native_returned_success(), Some(true));
    assert_eq!(
        error.has_panic_payloads(),
        [true, false, false, true, false]
    );
    assert_eq!(error.outer_denial(), (true, false));
    assert_eq!(error.append_denial(), (true, true));
    assert_eq!(checks.denial_calls.get(), 1);
    assert!(a.is_refused());
    drop(owner);
    drop(pin);
    fixture.contended();
    drop(error);
    fixture.free();
}

#[test]
fn real_parent_rename_after_append_keeps_native_ok_and_postvalidation_error() {
    let fixture = Fixture::new(&[], 1);
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let (log, owner, pin) = opened_log(&fixture, &c, &n, &checks);
    let old = fixture.shard.clone();
    let new = fixture.root.join("renamed");
    let renamed = new.clone();
    let _observe = observe(move |phase| {
        if phase == Phase::Native {
            fs::rename(&old, &renamed).expect("real held directory rename");
        }
    });
    let mut a = append();
    let error = log
        .append_group_until(0, records(2), true, &mut a, &n, &c, write_limits(), &checks)
        .expect_err("fresh validation refuses changed path");
    assert_eq!(error.native_returned_success(), Some(true));
    assert!(
        !error
            .post_validation()
            .expect("actual postvalidation")
            .returned_success()
    );
    drop(owner);
    drop(pin);
    fixture.contended_at(&new);
    drop(error);
    fs::rename(new, &fixture.shard).expect("restore fixture path after actual owner drop");
    fixture.free();
}

#[test]
fn same_name_regular_active_pack_replacement_refuses_without_writing_replacement() {
    let fixture = Fixture::new(&[0, 1], 1);
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let (store, owner, pin) = fixture.open();
    // Append validates the held active descriptor against its actual name.
    // Capture that real path before consuming the store; a directory listing
    // may select an unrelated sealed pack that this append does not touch.
    let pack = store.store().active_path().to_owned();
    let log = store
        .into_shard_log_until(&n, &c, &checks)
        .expect("genuine original Core/native conversion");
    let original = fs::read(&pack).expect("actual original bytes");
    let replacement = fixture.root.join("replacement");
    fs::write(&replacement, &original).expect("different inode");
    fs::rename(&replacement, &pack).expect("same-name replacement");
    let before = fixture.bytes();
    let mut a = append();
    let error = log
        .append_group_until(2, records(1), true, &mut a, &n, &c, write_limits(), &checks)
        .expect_err("held/name identity refuses");
    assert_eq!(error.native_returned_success(), Some(false));
    assert_eq!(fixture.bytes(), before);
    drop(owner);
    drop(pin);
    fixture.contended();
    drop(error);
    fixture.free();
}

#[test]
fn shorter_original_append_expiry_prevents_new_wal_without_renewal() {
    let fixture = Fixture::new(&[], 4096);
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let (log, owner, pin) = opened_log(&fixture, &c, &n, &checks);
    let short = Instant::now();
    let mut a = PinnedShardAppendBudget::new(append_limits(), short).expect("actual short E");
    let before = fixture.bytes();
    let error = log
        .append_group_until(0, records(1), true, &mut a, &n, &c, write_limits(), &checks)
        .expect_err("expired before IO");
    assert_eq!(error.native_returned_success(), None);
    assert_eq!(a.absolute_expiry(), short);
    assert_eq!(fixture.bytes(), before);
    assert!(a.is_refused());
    drop(owner);
    drop(pin);
    drop(error);
    fixture.free();
}

#[test]
fn later_core_expiry_and_prior_append_denial_keep_original_counters() {
    for initial_refusal in [false, true] {
        let fixture = Fixture::new(&[], 4096);
        let c = core(core_limits());
        let n = native(native_limits());
        let checks = Checks::for_command(&c, &n);
        let (log, owner, pin) = opened_log(&fixture, &c, &n, &checks);
        let mut a = append();
        let original = a.absolute_expiry();
        if initial_refusal {
            a.deny();
        }
        let same = c.clone();
        let _observe = observe(move |phase| {
            if !initial_refusal && phase == Phase::Native {
                let _ = same.cap_expiry(Instant::now());
            }
        });
        let error = log
            .append_group_until(0, records(1), true, &mut a, &n, &c, write_limits(), &checks)
            .expect_err("same state refuses");
        assert_eq!(
            error.native_returned_success(),
            if initial_refusal { None } else { Some(true) }
        );
        assert!(a.absolute_expiry() <= original);
        assert!(a.is_refused());
        drop(owner);
        drop(pin);
        drop(error);
        fixture.free();
    }
}

#[test]
fn cumulative_append_operation_limit_is_not_refreshed_by_successful_restore() {
    let fixture = Fixture::new(&[], 4096);
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let (log, owner, pin) = opened_log(&fixture, &c, &n, &checks);
    let mut limits = append_limits();
    limits.max_operations = 1;
    let mut a = PinnedShardAppendBudget::new(limits, expiry()).expect("one actual operation");
    let log = log
        .append_group_until(0, records(1), true, &mut a, &n, &c, write_limits(), &checks)
        .expect("first native append")
        .into_log();
    assert_eq!(a.usage().operations, 1);
    let before = fixture.bytes();
    let error = log
        .seal_until(&mut a, &n, &c, write_limits(), &checks)
        .expect_err("second operation cannot renew counters");
    assert_eq!(error.native_returned_success(), Some(false));
    assert_eq!(fixture.bytes(), before);
    assert!(a.is_refused());
    drop(owner);
    drop(pin);
    drop(error);
    fixture.free();
}

#[test]
fn actual_busy_or_poisoned_pin_refuses_before_wal_and_retains_same_root() {
    for poison in [false, true] {
        let fixture = Fixture::new(&[], 4096);
        let c = core(core_limits());
        let n = native(native_limits());
        let checks = Checks::for_command(&c, &n);
        let (log, owner, pin) = opened_log(&fixture, &c, &n, &checks);
        let directory = log
            .scope
            .directory
            .as_ref()
            .expect("actual constructor context")
            .clone();
        if poison {
            let shared = directory.clone();
            let result = catch_unwind(AssertUnwindSafe(|| {
                let _guard = shared.inner.native.lock().expect("actual private guard");
                panic!("actual poisoning");
            }));
            assert!(result.is_err());
        }
        let guard = if poison {
            None
        } else {
            Some(directory.inner.native.lock().expect("actual busy pin"))
        };
        let before = fixture.bytes();
        let mut a = append();
        let error = log
            .append_group_until(0, records(1), true, &mut a, &n, &c, write_limits(), &checks)
            .expect_err("immediate actual borrow refuses");
        assert_eq!(error.pin_borrow_observations(), (false, !poison, poison));
        assert_eq!(error.native_returned_success(), None);
        assert_eq!(fixture.bytes(), before);
        drop(guard);
        drop(directory);
        drop(owner);
        drop(pin);
        fixture.contended();
        drop(error);
        fixture.free();
    }
}

#[test]
fn native_gc_ok_and_quarantine_survive_late_core_refusal_until_error_drop() {
    let fixture = Fixture::new(&[0, 1, 2, 3], 1);
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let (log, owner, pin) = opened_log(&fixture, &c, &n, &checks);
    let same = c.clone();
    let _observe = observe(move |phase| {
        if phase == Phase::Native {
            same.deny();
        }
    });
    let mut a = append();
    let error = log
        .compact_before_until(2, &mut a, &n, &c, write_limits(), gc_limits(), &checks)
        .expect_err("native GC Ok cannot clear Core refusal");
    assert_eq!(error.native_returned_success(), Some(true));
    let facts = error.observation().expect("actual retained GC");
    assert!(
        facts
            .compaction_receipt()
            .expect("real native receipt")
            .packs_removed
            > 0
    );
    assert!(facts.quarantined_files().expect("actual quarantine") > 0);
    drop(owner);
    drop(pin);
    fixture.contended();
    drop(error);
    fixture.free();
}

#[test]
fn actual_prevalidation_rename_refuses_before_native_entry_and_denies_append() {
    let fixture = Fixture::new(&[], 4096);
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let (log, owner, pin) = opened_log(&fixture, &c, &n, &checks);
    let old = fixture.shard.clone();
    let new = fixture.root.join("pre-renamed");
    let renamed = new.clone();
    let _observe = observe(move |phase| {
        if phase == Phase::Split {
            fs::rename(&old, &renamed).expect("real path mutation before actual fresh validation");
        }
    });
    let mut a = append();
    let error = log
        .append_group_until(0, records(1), true, &mut a, &n, &c, write_limits(), &checks)
        .expect_err("actual prevalidation refuses");
    assert_eq!(error.native_returned_success(), None);
    assert!(
        !error
            .pre_validation()
            .expect("actual entered prevalidation")
            .returned_success()
    );
    assert_eq!(error.append_denial(), (true, true));
    assert!(a.is_refused());
    assert_eq!(checks.denial_calls.get(), 1);
    drop(owner);
    drop(pin);
    fixture.contended_at(&new);
    drop(error);
    fs::rename(new, &fixture.shard).expect("restore fixture after actual owners drop");
    fixture.free();
}

#[test]
fn actual_gc_call_capacity_refusal_retains_native_error_and_original_root() {
    let fixture = Fixture::new(&[0, 1, 2], 1);
    let c = core(core_limits());
    let n = native(native_limits());
    let checks = Checks::for_command(&c, &n);
    let (log, owner, pin) = opened_log(&fixture, &c, &n, &checks);
    let mut a = append();
    let mut gc = gc_limits();
    gc.max_io_calls = 5;
    let error = log
        .compact_before_until(2, &mut a, &n, &c, write_limits(), gc, &checks)
        .expect_err("actual native per-pack call reservation refuses");
    assert_eq!(error.native_returned_success(), Some(false));
    assert!(
        error
            .observation()
            .expect("actual retained GC error")
            .compaction_receipt()
            .is_none()
    );
    assert!(a.is_refused());
    assert_eq!(checks.denial_calls.get(), 1);
    drop(owner);
    drop(pin);
    fixture.contended();
    drop(error);
    fixture.free();
}
