//! Genuine ordinary histories and consuming bounded canonical write/read owners.
//! Source-only fixtures: no Core, tracked GC, C1, or production-size claim.
use super::*;
use crate::{
    PinnedShardDirectoryLimits, PinnedStoreOpenChecks, PinnedStoreOpenLimits,
    ShardGuardAcquireLimits, ShardRecoveryGuard, ShardStore, ShardStoreConfig,
};
use std::{
    fs::{self, File, OpenOptions},
    path::PathBuf,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

type CatalogDropObserver = Box<dyn FnMut()>;
type Observer = Box<dyn FnMut(WritePhase)>;
type SealObserver = Box<dyn FnMut(bool, usize, usize) -> Option<usize>>;
type TrackingObserver = Box<dyn FnMut(crate::WriteTrackingPhase)>;
thread_local! {static OBSERVER:RefCell<Option<Observer>>=RefCell::new(None);
static TRACKING:RefCell<Option<TrackingObserver>>=RefCell::new(None);
static SEAL:RefCell<Option<SealObserver>>=RefCell::new(None);
static CATALOG_DROP:RefCell<Option<CatalogDropObserver>>=RefCell::new(None);}
pub(crate) fn completed(phase: WritePhase) {
    OBSERVER.with(|s| {
        if let Some(f) = &mut *s.borrow_mut() {
            f(phase);
        }
    });
}
pub(crate) fn tracking_completed(phase: crate::WriteTrackingPhase) {
    TRACKING.with(|s| {
        if let Some(f) = &mut *s.borrow_mut() {
            f(phase);
        }
    });
}
pub(crate) fn seal_probe_limit(done: bool, used: usize, fee: usize) -> Option<usize> {
    SEAL.with(|s| s.borrow_mut().as_mut().and_then(|f| f(done, used, fee)))
}
pub(crate) fn catalog_drop_started() {
    CATALOG_DROP.with(|s| {
        if let Some(f) = &mut *s.borrow_mut() {
            f();
        }
    });
}
struct Observe;
impl Drop for Observe {
    fn drop(&mut self) {
        OBSERVER.with(|s| *s.borrow_mut() = None);
        TRACKING.with(|s| *s.borrow_mut() = None);
        SEAL.with(|s| *s.borrow_mut() = None);
        CATALOG_DROP.with(|s| *s.borrow_mut() = None);
    }
}
fn observe(f: impl FnMut(WritePhase) + 'static) -> Observe {
    OBSERVER.with(|s| {
        assert!(s.borrow().is_none());
        *s.borrow_mut() = Some(Box::new(f));
    });
    Observe
}
fn tracking(f: impl FnMut(crate::WriteTrackingPhase) + 'static) -> Observe {
    TRACKING.with(|s| {
        assert!(s.borrow().is_none());
        *s.borrow_mut() = Some(Box::new(f));
    });
    Observe
}
struct Checks {
    denials: Cell<usize>,
    armed: Rc<Cell<bool>>,
    panic_deny: Cell<bool>,
}
impl Checks {
    fn new() -> Self {
        Self {
            denials: Cell::new(0),
            armed: Rc::new(Cell::new(false)),
            panic_deny: Cell::new(false),
        }
    }
}
impl ShardDirectoryChecks for Checks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        if self.denials.get() > 0 || self.armed.get() {
            Err(ShardDirectoryCheckFailure::Refused)
        } else {
            Ok(())
        }
    }
}
impl PinnedStoreOpenChecks for Checks {
    fn deny(&self) {
        self.denials.set(self.denials.get() + 1);
    }
}
impl PinnedShardLogOpenChecks for Checks {
    fn deny(&self) {
        self.denials.set(self.denials.get() + 1);
    }
}
impl PinnedShardFetchChecks for Checks {
    fn deny(&self) {
        self.denials.set(self.denials.get() + 1);
    }
}
impl PinnedShardLogWriteChecks for Checks {
    fn deny(&self) {
        self.denials.set(self.denials.get() + 1);
        if self.panic_deny.get() {
            std::panic::panic_any("actual composing denial");
        }
    }
}
fn expiry() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
fn fetch_limits() -> StorageFetchLimits {
    StorageFetchLimits {
        max_plan_batches: 1024,
        max_entries: 1 << 20,
        max_path_bytes: 8192,
        max_read_attempts: 4096,
        max_read_bytes: 1 << 24,
        max_declared_bytes: 1 << 28,
        max_work_bytes: 1 << 30,
    }
}
fn append_limits() -> PinnedShardAppendLimits {
    PinnedShardAppendLimits {
        max_operations: 16,
        max_frames: 64,
        max_payload_bytes: 1 << 16,
        max_segments: 64,
        max_rotations: 16,
        max_path_bytes: 8192,
        max_name_bytes: 1024,
        max_packs: 64,
        max_batch_slots: 1024,
        max_topic_slots: 128,
        max_location_slots: 1024,
        max_io_calls: 2048,
        max_write_calls: 1024,
        max_write_bytes: 1 << 24,
        max_work_bytes: 1 << 28,
    }
}
fn write_limits() -> PinnedShardLogWriteLimits {
    PinnedShardLogWriteLimits {
        max_live_files: 128,
        max_tracked_packs: 128,
        max_retired_slots: 64,
        max_input_slots: 64,
    }
}
fn open_limits() -> PinnedStoreOpenLimits {
    PinnedStoreOpenLimits {
        max_directory_visits: 2048,
        directory_buffer_bytes: 8192,
        max_name_bytes: 1024,
        max_path_bytes: 8192,
        max_packs: 128,
        max_frames: 1024,
        max_frame_bytes: 8192,
        max_read_attempts: 8192,
        max_read_bytes: 1 << 26,
        max_index_entries: 2048,
        max_repair_calls: 256,
        max_io_calls: 16384,
        max_outcomes: 32768,
        max_entries: 1 << 22,
        max_declared_bytes: 1 << 29,
        max_work_bytes: 1 << 31,
    }
}
struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "canonical-owned-write-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual fixture root");
        Self(path)
    }
    fn lock(&self) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.0.join(".shard-store.lock"))
            .expect("actual lease FD")
    }
    fn contended(&self) {
        assert!(fs2::FileExt::try_lock_exclusive(&self.lock()).is_err());
    }
    fn released(&self) {
        assert!(fs2::FileExt::try_lock_exclusive(&self.lock()).is_ok());
    }
    fn snapshot(&self) -> Vec<(std::ffi::OsString, Vec<u8>)> {
        let mut out = fs::read_dir(&self.0)
            .expect("actual names")
            .map(|e| {
                let e = e.expect("actual entry");
                (e.file_name(), fs::read(e.path()).expect("actual body"))
            })
            .collect::<Vec<_>>();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    pin: PinnedShardDirectory,
    config: ShardStoreConfig,
    root: Root,
}
impl Fixture {
    fn new(history: &[u64], target: u64) -> Self {
        let root = Root::new();
        let mut config = super::super::super::ShardLogConfig::new(&root.0);
        config.target_pack_bytes = target;
        config.max_record_bytes = 4096;
        let mut ordinary =
            super::super::super::ShardLog::open(config).expect("actual ordinary writer");
        for &seq in history {
            ordinary
                .append_group(seq, &[Bytes::from_static(b"original")], true)
                .expect("actual ordinary history");
        }
        drop(ordinary);
        let guard = ShardRecoveryGuard::try_acquire_until(
            root.lock(),
            root.0.clone(),
            ShardGuardAcquireLimits {
                max_path_bytes: 8192,
                max_io_calls: 5,
            },
            expiry(),
        )
        .expect("actual acquired native lease");
        let pin = PinnedShardDirectory::from_descriptor_until(
            File::open(&root.0).expect("actual directory FD"),
            guard,
            &root.0,
            PinnedShardDirectoryLimits {
                max_path_bytes: 8192,
                max_io_calls: 4,
            },
            expiry(),
            &Checks::new(),
        )
        .expect("actual pin");
        let config = ShardStoreConfig {
            directory: root.0.clone(),
            shard_id: super::super::super::SHARDLOG_SHARD_ID,
            target_pack_bytes: target,
            max_batch_bytes: 4096,
        };
        Self { pin, config, root }
    }
    fn log(&self, b: &StorageFetchBudget) -> OpenedPinnedShardLog {
        ShardStore::open_pinned_until(
            self.config.clone(),
            &self.pin,
            open_limits(),
            expiry(),
            &Checks::new(),
        )
        .expect("actual bounded store open")
        .into_shard_log_until(b, &Checks::new())
        .expect("actual canonical log")
    }
}
fn records(n: usize) -> Vec<Bytes> {
    (0..n)
        .map(|_| Bytes::from_static(b"new actual canonical payload"))
        .collect()
}

#[test]
fn actual_multi_pack_append_transfers_retired_files_and_reads_original_and_new_frames() {
    let f = Fixture::new(&[4, 5], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
    let log = f.log(&b);
    let mut native = PinnedShardAppendBudget::new(append_limits(), b.absolute_expiry())
        .expect("actual native allowance");
    let complete = log
        .append_group_until(
            &f.pin,
            6,
            records(4),
            true,
            &mut native,
            &b,
            write_limits(),
            &Checks::new(),
        )
        .expect("actual append");
    assert_eq!(
        complete
            .observation()
            .receipt()
            .expect("actual receipt")
            .last_sequence,
        9
    );
    assert!(complete.observation().transferred_files() > 0);
    assert!(complete.observation().write_results().any(Result::is_ok));
    let log = complete.into_log();
    assert_eq!(log.bounds(), Some((4, 10)));
    let read = log
        .read_from_until(&f.pin, 4, 1 << 20, 64, &b, &Checks::new())
        .expect("same actual retained descriptors");
    assert_eq!(
        read.records()
            .iter()
            .map(|r| r.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5, 6, 7, 8, 9]
    );
    drop(read);
    drop(f);
}
#[test]
fn actual_empty_first_group_and_seal_share_ordinary_policy_and_retained_active_file() {
    let f = Fixture::new(&[], 4096);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
    let mut n = PinnedShardAppendBudget::new(append_limits(), b.absolute_expiry())
        .expect("actual allowance");
    let log = f
        .log(&b)
        .append_group_until(
            &f.pin,
            12,
            records(2),
            false,
            &mut n,
            &b,
            write_limits(),
            &Checks::new(),
        )
        .expect("ordinary accepts first12")
        .into_log();
    assert_eq!(log.bounds(), Some((12, 14)));
    let log = log
        .seal_until(&f.pin, &mut n, &b, write_limits(), &Checks::new())
        .expect("actual native seal")
        .into_log();
    let read = log
        .read_from_until(&f.pin, 12, 1 << 20, 4, &b, &Checks::new())
        .expect("actual sealed descriptor");
    assert_eq!(read.records().len(), 2);
}
#[test]
fn actual_gap_is_written_then_canonical_refusal_keeps_real_report_and_whole_log() {
    let f = Fixture::new(&[10], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("counters");
    let mut n = PinnedShardAppendBudget::new(append_limits(), expiry()).expect("allowance");
    let checks = Checks::new();
    let error = f
        .log(&b)
        .append_group_until(
            &f.pin,
            12,
            records(1),
            true,
            &mut n,
            &b,
            write_limits(),
            &checks,
        )
        .expect_err("actual later canonical gap");
    assert!(matches!(
        error.observation().failure(),
        Some(PinnedShardLogWriteFailure::Canonical)
    ));
    assert!(matches!(error.scope.native_result, Some(Ok(_))));
    assert!(matches!(error.scope.bounds_result, Some(Err(_))));
    assert!(error.observation().receipt().is_none());
    assert_eq!(checks.denials.get(), 1);
    assert!(n.is_refused());
    let Fixture { pin, root, .. } = f;
    drop(pin);
    root.contended();
    drop(error);
    root.released();
}
#[test]
fn actual_empty_input_and_overflow_refuse_without_native_physical_entry() {
    for (first, input) in [(3, Vec::new()), (u64::MAX, records(2))] {
        let f = Fixture::new(&[], 1);
        let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("counters");
        let log = f.log(&b);
        let before = f.root.snapshot();
        let mut n = PinnedShardAppendBudget::new(append_limits(), expiry()).expect("allowance");
        let error = log
            .append_group_until(
                &f.pin,
                first,
                input,
                true,
                &mut n,
                &b,
                write_limits(),
                &Checks::new(),
            )
            .expect_err("shared actual mapper refusal");
        assert!(matches!(error.scope.mapping_result, Some(Err(_))));
        assert!(error.scope.native_result.is_none());
        assert_eq!(f.root.snapshot(), before);
    }
}
#[test]
fn immutable_live_descriptor_and_input_capacity_limits_refuse_before_create_or_write() {
    for input_cap in [false, true] {
        let f = Fixture::new(&[1], 1);
        let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("counters");
        let log = f.log(&b);
        let before = f.root.snapshot();
        let mut n = PinnedShardAppendBudget::new(append_limits(), expiry()).expect("allowance");
        let mut policy = write_limits();
        let input = if input_cap {
            policy.max_input_slots = 1;
            let mut v = Vec::with_capacity(8);
            v.push(Bytes::from_static(b"actual input"));
            v
        } else {
            policy.max_live_files = 2;
            records(1)
        };
        let actual_capacity = input.capacity();
        let error = log
            .append_group_until(&f.pin, 2, input, true, &mut n, &b, policy, &Checks::new())
            .expect_err("original immutable policy");
        assert_eq!(
            error.observation().original_record_capacity(),
            actual_capacity
        );
        assert!(error.scope.native_result.is_none());
        assert_eq!(f.root.snapshot(), before);
    }
}
#[test]
fn original_low_entry_policy_refuses_real_headers_before_native_entry_without_limit_mutation() {
    let f = Fixture::new(&[1], 1);
    let convert =
        StorageFetchBudget::new(fetch_limits(), expiry()).expect("actual conversion counters");
    let log = f.log(&convert);
    let mut low = fetch_limits();
    low.max_entries = 8;
    let b = StorageFetchBudget::new(low, expiry()).expect("original fixed eight-entry policy");
    let before = f.root.snapshot();
    let mut n = PinnedShardAppendBudget::new(append_limits(), expiry()).expect("allowance");
    let error = log
        .append_group_until(
            &f.pin,
            2,
            records(1),
            true,
            &mut n,
            &b,
            write_limits(),
            &Checks::new(),
        )
        .expect_err("explicit independent fixed values");
    assert!(error.scope.native_result.is_none());
    assert_eq!(f.root.snapshot(), before);
    assert!(b.check().is_err());
}
#[test]
fn actual_partial_transfer_late_checks_retain_both_moved_descriptors_and_remaining_report() {
    for phase in [
        crate::WriteTrackingPhase::Transferred,
        crate::WriteTrackingPhase::Registered,
        crate::WriteTrackingPhase::PathCopied,
        crate::WriteTrackingPhase::NameConverted,
        crate::WriteTrackingPhase::Stat,
    ] {
        let f = Fixture::new(&[1], 1);
        let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("counters");
        let log = f.log(&b);
        let checks = Checks::new();
        let armed = checks.armed.clone();
        let _observer = tracking(move |p| {
            if p == phase {
                armed.set(true);
            }
        });
        let mut n = PinnedShardAppendBudget::new(append_limits(), expiry()).expect("allowance");
        let error = log
            .append_group_until(
                &f.pin,
                2,
                records(3),
                true,
                &mut n,
                &b,
                write_limits(),
                &checks,
            )
            .expect_err("actual completed transfer/check refusal");
        assert!(matches!(error.scope.native_result, Some(Ok(_))));
        assert_eq!(checks.denials.get(), 1);
        assert!(b.check().is_err());
        let Fixture { pin, root, .. } = f;
        drop(pin);
        root.contended();
        drop(error);
        root.released();
    }
}
#[test]
fn actual_transfer_observer_unwind_and_denial_unwind_keep_original_payloads_and_owners() {
    let f = Fixture::new(&[1], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("counters");
    let log = f.log(&b);
    let checks = Checks::new();
    checks.panic_deny.set(true);
    let _observer = tracking(|p| {
        if p == crate::WriteTrackingPhase::Transferred {
            std::panic::panic_any("actual moved File checkpoint");
        }
    });
    let mut n = PinnedShardAppendBudget::new(append_limits(), expiry()).expect("allowance");
    let error = log
        .append_group_until(
            &f.pin,
            2,
            records(3),
            true,
            &mut n,
            &b,
            write_limits(),
            &checks,
        )
        .expect_err("actual unwind");
    assert!(error.scope.panic.is_some());
    assert!(error.scope.denial.composing_panic.borrow().is_some());
    assert_eq!(checks.denials.get(), 1);
    assert_eq!(error.observation().transferred_files(), 1);
    let Fixture { pin, root, .. } = f;
    drop(pin);
    root.contended();
    drop(error);
    root.released();
}
#[test]
fn actual_native_success_late_bridge_check_retains_raw_results_and_paid_physical_effects() {
    let f = Fixture::new(&[1], 4096);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("counters");
    let log = f.log(&b);
    let before = f.root.snapshot();
    let checks = Checks::new();
    let armed = checks.armed.clone();
    let _observer = observe(move |p| {
        if p == WritePhase::NativeCompleted {
            armed.set(true);
        }
    });
    let mut n = PinnedShardAppendBudget::new(append_limits(), expiry()).expect("allowance");
    let error = log
        .append_group_until(
            &f.pin,
            2,
            records(1),
            true,
            &mut n,
            &b,
            write_limits(),
            &checks,
        )
        .expect_err("late after actual native success");
    assert!(matches!(error.scope.native_result, Some(Ok(_))));
    assert!(error.observation().write_results().any(Result::is_ok));
    assert_ne!(f.root.snapshot(), before);
    assert_eq!(checks.denials.get(), 1);
    assert!(error.observation().receipt().is_none());
}
#[test]
fn tracked_catalog_policy_is_immutable_and_repeated_writes_keep_only_current_operation_report() {
    let f = Fixture::new(&[1], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("counters");
    let mut n = PinnedShardAppendBudget::new(append_limits(), expiry()).expect("allowance");
    let log = f
        .log(&b)
        .append_group_until(
            &f.pin,
            2,
            records(1),
            true,
            &mut n,
            &b,
            write_limits(),
            &Checks::new(),
        )
        .expect("actual first write")
        .into_log();
    let complete = log
        .append_group_until(
            &f.pin,
            3,
            records(1),
            true,
            &mut n,
            &b,
            write_limits(),
            &Checks::new(),
        )
        .expect("actual next write");
    assert!(
        complete
            .scope
            .native_result
            .as_ref()
            .expect("actual current report")
            .as_ref()
            .expect("actual successful report")
            .inputs()
            .len()
            == 1
    );
    let log = complete.into_log();
    let mut changed = write_limits();
    changed.max_retired_slots += 1;
    let before = f.root.snapshot();
    let error = log
        .append_group_until(
            &f.pin,
            4,
            records(1),
            true,
            &mut n,
            &b,
            changed,
            &Checks::new(),
        )
        .expect_err("same immutable catalog policy");
    assert!(error.scope.native_result.is_none());
    assert_eq!(f.root.snapshot(), before);
}
#[test]
fn same_name_regular_replacement_after_native_success_is_detected_without_reopen() {
    let f = Fixture::new(&[1], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("counters");
    let log = f.log(&b);
    let directory = f.root.0.clone();
    let _observer = observe(move |p| {
        if p == WritePhase::NativeCompleted {
            let path = fs::read_dir(&directory)
                .expect("real created packs")
                .map(|v| v.expect("actual entry").path())
                .filter(|p| p.extension().is_some_and(|v| v == "sse"))
                .max()
                .expect("actual current pack");
            let retained = path.with_extension("retained");
            fs::rename(&path, &retained).expect("keep actual old inode");
            fs::copy(&retained, &path).expect("genuine regular replacement");
        }
    });
    let mut n = PinnedShardAppendBudget::new(append_limits(), expiry()).expect("allowance");
    let checks = Checks::new();
    let error = log
        .append_group_until(
            &f.pin,
            2,
            records(1),
            true,
            &mut n,
            &b,
            write_limits(),
            &checks,
        )
        .expect_err("named versus actual held inode");
    assert!(matches!(error.scope.native_result, Some(Ok(_))));
    assert!(matches!(error.scope.tracking_result, Some(Err(_))));
    assert_eq!(checks.denials.get(), 1);
}
#[test]
fn successful_completion_and_whole_log_release_the_actual_lock_in_both_drop_orders() {
    for pin_first in [false, true] {
        let f = Fixture::new(&[1], 1);
        let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("counters");
        let mut n = PinnedShardAppendBudget::new(append_limits(), expiry()).expect("allowance");
        let complete = f
            .log(&b)
            .append_group_until(
                &f.pin,
                2,
                records(2),
                true,
                &mut n,
                &b,
                write_limits(),
                &Checks::new(),
            )
            .expect("actual write");
        let Fixture { pin, root, .. } = f;
        if pin_first {
            drop(pin);
            root.contended();
            drop(complete);
            root.released();
        } else {
            drop(complete);
            root.contended();
            drop(pin);
            root.released();
        }
    }
}

#[test]
fn real_leading_zero_pack_names_remain_tracked_across_retirement_and_reads() {
    let f = Fixture::new(&[3, 4], 1);
    for entry in fs::read_dir(&f.root.0).expect("actual pack names") {
        let entry = entry.expect("actual pack");
        let name = entry.file_name();
        if let Some(name) = name
            .to_str()
            .filter(|name| name.starts_with("pack-") && name.ends_with(".sse"))
        {
            fs::rename(
                entry.path(),
                f.root.0.join(format!(
                    "pack-0000000000000000000000000000000000000000{}",
                    &name[5..]
                )),
            )
            .expect("actual accepted long spelling");
        }
    }
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
    let mut n = PinnedShardAppendBudget::new(append_limits(), expiry()).expect("allowance");
    let log = f
        .log(&b)
        .append_group_until(
            &f.pin,
            5,
            records(2),
            true,
            &mut n,
            &b,
            write_limits(),
            &Checks::new(),
        )
        .expect("native accepted name spellings")
        .into_log();
    let read = log
        .read_from_until(&f.pin, 3, 1 << 20, 8, &b, &Checks::new())
        .expect("actual original and moved descriptors");
    assert_eq!(
        read.records()
            .iter()
            .map(|r| r.sequence)
            .collect::<Vec<_>>(),
        vec![3, 4, 5, 6]
    );
}
#[test]
fn actual_untracked_gc_leaves_sparse_native_tables_but_refuses_before_new_write_or_read() {
    for write in [false, true] {
        let history = (1..=24).collect::<Vec<_>>();
        let f = Fixture::new(&history, 1);
        let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
        let mut log = f.log(&b);
        let store = log.scope.opened.store_mut();
        let before_shape = store.log_catalog_shape().expect("actual shape");
        let starts = std::collections::HashMap::from([(
            super::super::super::topic_partition(),
            crate::LogicalOffset::new(24),
        )]);
        let stats = store
            .garbage_collect(&starts)
            .expect("actual ordinary lifecycle, intentionally unqualified");
        assert!(stats.packs_removed > 0);
        let shape = store.log_catalog_shape().expect("actual sparse shape");
        assert!(
            shape.0 < before_shape.0,
            "actual GC removed catalog records"
        );
        // HashMap usable capacity can change when retain leaves tombstones.
        // Actual sparse capacity, not a monotonic allocation-size witness,
        // determines the native probe fee after this untracked lifecycle.
        assert!(shape.1 > shape.0);
        let before = f.root.snapshot();
        if write {
            let mut n = PinnedShardAppendBudget::new(append_limits(), expiry()).expect("allowance");
            let error = log
                .append_group_until(
                    &f.pin,
                    25,
                    records(1),
                    true,
                    &mut n,
                    &b,
                    write_limits(),
                    &Checks::new(),
                )
                .expect_err("untracked epoch");
            assert!(error.scope.native_result.is_none());
        } else {
            let error = log
                .read_from_until(&f.pin, 24, 1024, 2, &b, &Checks::new())
                .expect_err("untracked epoch");
            assert!(error.observation().is_none_or(|o| o.outcomes().is_empty()));
        }
        assert_eq!(f.root.snapshot(), before);
    }
}
#[test]
fn native_earlier_original_expiry_is_checked_after_actual_composing_callback() {
    struct Slow {
        armed: Cell<bool>,
        expiry: Instant,
        denied: Cell<bool>,
    }
    impl ShardDirectoryChecks for Slow {
        fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
            if self.armed.replace(true) {
                while Instant::now() < self.expiry {
                    std::thread::yield_now();
                }
            }
            Ok(())
        }
    }
    impl PinnedShardLogWriteChecks for Slow {
        fn deny(&self) {
            self.denied.set(true);
        }
    }
    let f = Fixture::new(&[1], 4096);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("later unchanged global E");
    let log = f.log(&b);
    let before = f.root.snapshot();
    let early = Instant::now() + Duration::from_millis(5);
    let mut n =
        PinnedShardAppendBudget::new(append_limits(), early).expect("original earlier native E");
    let checks = Slow {
        armed: Cell::new(false),
        expiry: early,
        denied: Cell::new(false),
    };
    let error = log
        .append_group_until(
            &f.pin,
            2,
            records(1),
            true,
            &mut n,
            &b,
            write_limits(),
            &checks,
        )
        .expect_err("actual completion crossed common E");
    assert!(error.scope.denial.expired.get());
    assert!(error.scope.native_result.is_none());
    assert!(checks.denied.get());
    assert_eq!(f.root.snapshot(), before);
}

#[test]
fn actual_second_seal_pack_path_probe_has_its_own_exact_and_one_less_native_fee() {
    for one_less in [false, true] {
        let f = Fixture::new(&[1], 4096);
        let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same global counters");
        let log = f.log(&b);
        let before = f.root.snapshot();
        let rows = Rc::new(RefCell::new(Vec::new()));
        let saved = rows.clone();
        SEAL.with(|s| {
            *s.borrow_mut() = Some(Box::new(move |done, used, fee| {
                saved.borrow_mut().push((done, used, fee));
                if done {
                    None
                } else {
                    Some(
                        used.checked_add(fee).expect("finite actual probe fee")
                            - usize::from(one_less),
                    )
                }
            }))
        });
        let _observer = Observe;
        let original = expiry();
        let mut n = PinnedShardAppendBudget::new(append_limits(), original)
            .expect("unchanged original E/counters");
        let checks = Checks::new();
        let error = log
            .seal_until(&f.pin, &mut n, &b, write_limits(), &checks)
            .expect_err("only this probe fits; next independent name work remains unpaid");
        let rows = rows.borrow();
        let (done, used, fee) = rows[0];
        assert!(!done);
        assert!(fee > 0);
        assert_eq!(rows.iter().filter(|r| r.0).count(), usize::from(!one_less));
        assert_eq!(n.absolute_expiry(), original.min(b.absolute_expiry()));
        assert_eq!(
            error
                .scope
                .native_report()
                .expect("actual retained native report")
                .usage()
                .work_bytes,
            used + if one_less { 0 } else { fee }
        );
        assert_eq!(f.root.snapshot(), before);
        assert_eq!(checks.denials.get(), 1);
    }
}
#[test]
fn actual_ordinary_rotation_is_refused_even_when_new_active_length_matches_initial_empty() {
    let f = Fixture::new(&[], 4096);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same global counters");
    let mut log = f.log(&b);
    // Genuine ordinary write+seal changes active identity. This deliberately
    // bypasses the strict catalog and must never be adopted by outer checks.
    let store = log.scope.opened.store_mut();
    let input = super::super::super::reservation(1);
    let prepared = store
        .prepare_append(input, None, Bytes::from_static(b"ordinary untracked"))
        .expect("actual ordinary prepare");
    store
        .append_prepared_group(&[&prepared], true)
        .expect("actual ordinary append");
    store.seal_active().expect("actual ordinary rotation");
    let before = f.root.snapshot();
    let mut n = PinnedShardAppendBudget::new(append_limits(), expiry()).expect("actual allowance");
    let error = log
        .append_group_until(
            &f.pin,
            2,
            records(1),
            true,
            &mut n,
            &b,
            write_limits(),
            &Checks::new(),
        )
        .expect_err("active sequence was never tracked");
    assert!(error.scope.native_result.is_none());
    assert_eq!(f.root.snapshot(), before);
}

#[test]
fn fixed_independent_denial_values_are_paid_at_exact_and_one_less_immutable_entry_policy() {
    // Independent source inventory, not an observed-usage oracle:
    // bridge43 + diagnostic1 + tracking27 + native-fetch43+diagnostic1 =115.
    // The old missing-flag/RefCell inventory charged102, so both unchanged
    // original policies would incorrectly cross FixedPaid on that generation.
    const FIXED_INDEPENDENT_ENTRIES: usize = 115;
    for one_less in [false, true] {
        let f = Fixture::new(&[1], 1);
        let conversion =
            StorageFetchBudget::new(fetch_limits(), expiry()).expect("actual conversion counters");
        let log = f.log(&conversion);
        let mut original_policy = fetch_limits();
        original_policy.max_entries = FIXED_INDEPENDENT_ENTRIES - usize::from(one_less);
        let original_e = expiry();
        let b = StorageFetchBudget::new(original_policy, original_e)
            .expect("unchanged exact/one-less policy");
        let fixed_paid = Rc::new(Cell::new(false));
        let observed = fixed_paid.clone();
        let _observer = observe(move |p| {
            if p == WritePhase::FixedPaid {
                observed.set(true);
            }
        });
        let before = f.root.snapshot();
        let checks = Checks::new();
        let mut n = PinnedShardAppendBudget::new(append_limits(), original_e)
            .expect("original native allowance");
        let error = log
            .append_group_until(
                &f.pin,
                2,
                records(1),
                true,
                &mut n,
                &b,
                write_limits(),
                &checks,
            )
            .expect_err("only fixed values fit; a separate real input-capacity fee remains unpaid");
        assert_eq!(fixed_paid.get(), !one_less);
        assert_eq!(
            b.usage().expect("actual preserved cumulative fees").entries,
            if one_less {
                0
            } else {
                FIXED_INDEPENDENT_ENTRIES
            }
        );
        assert_eq!(b.limits(), original_policy);
        assert_eq!(b.absolute_expiry(), original_e);
        assert!(error.scope.native_result.is_none());
        assert_eq!(checks.denials.get(), 1);
        assert_eq!(f.root.snapshot(), before);
        let Fixture { pin, root, .. } = f;
        drop(pin);
        root.contended();
        drop(error);
        root.released();
    }
}
#[test]
fn retired_catalog_drop_starts_before_actual_original_store_lease_and_context_drop() {
    let f = Fixture::new(&[1], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
    let mut n =
        PinnedShardAppendBudget::new(append_limits(), expiry()).expect("actual native allowance");
    let complete = f
        .log(&b)
        .append_group_until(
            &f.pin,
            2,
            records(3),
            true,
            &mut n,
            &b,
            write_limits(),
            &Checks::new(),
        )
        .expect("actual rotated files");
    assert!(complete.observation().transferred_files() > 0);
    let log = complete.into_log();
    let Fixture { pin, root, .. } = f;
    drop(pin);
    let lock_path = root.0.join(".shard-store.lock");
    let calls = Rc::new(Cell::new(0));
    let observed = calls.clone();
    CATALOG_DROP.with(|s|*s.borrow_mut()=Some(Box::new(move ||{
        observed.set(observed.get()+1);
        let competing=OpenOptions::new().read(true).write(true).open(&lock_path).expect("actual independent lease FD");
        assert!(fs2::FileExt::try_lock_exclusive(&competing).is_err(),"original actual store lease must remain held while new catalog starts physical destruction");
    })));
    let _observer = Observe;
    root.contended();
    drop(log);
    assert_eq!(calls.get(), 1);
    root.released();
    // File Drop supplies no raw close Result. This verifies actual lock lifetime
    // plus source declaration order, not an invented errno/completion receipt.
}
