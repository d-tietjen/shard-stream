//! Actual native opens, canonical histories, cumulative fees and retained leases.
//!
//! Ordinary writes create fixture history. These tests do not qualify Core,
//! bounded pinned reads/writes, complete recovery, or production memory limits.

use super::*;
use crate::{
    LogicalOffset, PinnedShardDirectory, PinnedShardDirectoryLimits, PinnedStoreOpenChecks,
    PinnedStoreOpenLimits, ShardGuardAcquireLimits, ShardRecoveryGuard, ShardStore,
    ShardStoreConfig,
};
use bytes::Bytes;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    path::PathBuf,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    Allocated,
    Validated,
    Bounds,
}
type Observer = Box<dyn FnMut(Phase)>;
thread_local! { static OBSERVER: RefCell<Option<Observer>> = RefCell::new(None); }
pub(super) fn completed(phase: Phase) {
    OBSERVER.with(|slot| {
        if let Some(observer) = &mut *slot.borrow_mut() {
            observer(phase);
        }
    });
}
struct Observe;
impl Drop for Observe {
    fn drop(&mut self) {
        OBSERVER.with(|slot| *slot.borrow_mut() = None);
    }
}
fn observe(callback: impl FnMut(Phase) + 'static) -> Observe {
    OBSERVER.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(callback));
    });
    Observe
}
struct Checks {
    denied: Cell<bool>,
    armed: Rc<Cell<bool>>,
    panic_deny: Cell<bool>,
}
impl Checks {
    fn new() -> Self {
        Self {
            denied: Cell::new(false),
            armed: Rc::new(Cell::new(false)),
            panic_deny: Cell::new(false),
        }
    }
}
impl ShardDirectoryChecks for Checks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        if self.denied.get() || self.armed.get() {
            Err(ShardDirectoryCheckFailure::Refused)
        } else {
            Ok(())
        }
    }
}
impl PinnedStoreOpenChecks for Checks {
    fn deny(&self) {
        self.denied.set(true);
    }
}
impl PinnedShardLogOpenChecks for Checks {
    fn deny(&self) {
        self.denied.set(true);
        if self.panic_deny.get() {
            std::panic::panic_any("actual composing denial unwind");
        }
    }
}
fn expiry() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
fn limits() -> StorageFetchLimits {
    StorageFetchLimits {
        max_plan_batches: 1024,
        max_entries: 1 << 16,
        max_path_bytes: 8192,
        max_read_attempts: 1024,
        max_read_bytes: 1 << 22,
        max_declared_bytes: 1 << 24,
        max_work_bytes: 1 << 27,
    }
}
fn open_limits() -> PinnedStoreOpenLimits {
    PinnedStoreOpenLimits {
        max_directory_visits: 1024,
        directory_buffer_bytes: 8192,
        max_name_bytes: 1024,
        max_path_bytes: 8192,
        max_packs: 128,
        max_frames: 256,
        max_frame_bytes: 8192,
        max_read_attempts: 2048,
        max_read_bytes: 1 << 24,
        max_index_entries: 1024,
        max_repair_calls: 256,
        max_io_calls: 4096,
        max_outcomes: 8192,
        max_entries: 1 << 20,
        max_declared_bytes: 1 << 27,
        max_work_bytes: 1 << 29,
    }
}
struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "pinned-canonical-log-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual fixture directory");
        Self(path)
    }
    fn lock(&self) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.0.join(".shard-store.lock"))
            .expect("actual competing lease FD")
    }
    fn snapshot(&self) -> Vec<(OsString, Vec<u8>)> {
        let mut files = fs::read_dir(&self.0)
            .expect("actual native names")
            .map(|entry| {
                let entry = entry.expect("actual entry");
                (
                    entry.file_name(),
                    fs::read(entry.path()).expect("actual file body"),
                )
            })
            .collect::<Vec<_>>();
        files.sort_by(|a, b| a.0.cmp(&b.0));
        files
    }
    fn contended(&self) {
        assert!(fs2::FileExt::try_lock_exclusive(&self.lock()).is_err());
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    // Actual native pin drops before the fixture directory is removed.
    pin: PinnedShardDirectory,
    config: ShardStoreConfig,
    root: Root,
}
impl Fixture {
    fn new(sequences: &[u64], target: u64) -> Self {
        let root = Root::new();
        let mut config = super::super::ShardLogConfig::new(&root.0);
        config.target_pack_bytes = target;
        config.max_record_bytes = 4096;
        let mut log = super::super::ShardLog::open(config).expect("actual ordinary history writer");
        for &sequence in sequences {
            log.append_group(
                sequence,
                &[Bytes::from_static(b"real canonical record")],
                true,
            )
            .expect("actual durable history");
        }
        drop(log);
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
        .expect("actual joined native pin");
        let config = ShardStoreConfig {
            directory: root.0.clone(),
            shard_id: super::super::SHARDLOG_SHARD_ID,
            target_pack_bytes: target,
            max_batch_bytes: 4096,
        };
        Self { pin, config, root }
    }
    fn opened(&self) -> OpenedPinnedShardStore {
        ShardStore::open_pinned_until(
            self.config.clone(),
            &self.pin,
            open_limits(),
            expiry(),
            &Checks::new(),
        )
        .expect("actual bounded native open")
    }
}

#[test]
fn real_empty_and_contiguous_histories_match_ordinary_bounds_and_reopen_bytes() {
    for history in [vec![], vec![4], vec![4, 5, 6]] {
        let f = Fixture::new(&history, 1);
        let opened = f.opened();
        let before = f.root.snapshot();
        let budget = StorageFetchBudget::new(limits(), expiry()).expect("native counters");
        let log = opened
            .into_shard_log_until(&budget, &Checks::new())
            .expect("actual canonical conversion");
        let expected = history
            .first()
            .zip(history.last())
            .map(|(&first, &last)| (first, last + 1));
        assert_eq!(log.bounds(), expected);
        assert_eq!(log.observation().retained_sequences(), history);
        assert_eq!(
            log.observation().absolute_expiry(),
            budget.absolute_expiry()
        );
        assert_eq!(f.root.snapshot(), before);
        f.root.contended();
        let Fixture {
            pin,
            config: _,
            root,
        } = f;
        drop(pin);
        root.contended();
        drop(log);
        let mut ordinary = super::super::ShardLog::open(super::super::ShardLogConfig::new(&root.0))
            .expect("actual ordinary reopen");
        assert_eq!(ordinary.bounds().expect("ordinary bounds"), expected);
        let records = ordinary
            .read_from(0, 4096, 1024)
            .expect("actual ordinary bytes after conversion");
        assert_eq!(
            records
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            history
        );
        assert!(
            records
                .iter()
                .all(|record| record.payload.as_ref() == b"real canonical record")
        );
        drop(ordinary);
    }
}

#[test]
fn actual_gap_retains_sorted_catalog_error_and_lease_after_pin_drop() {
    let f = Fixture::new(&[2, 4], 1);
    let opened = f.opened();
    let before = f.root.snapshot();
    let checks = Checks::new();
    let budget = StorageFetchBudget::new(limits(), expiry()).expect("native counters");
    let error = opened
        .into_shard_log_until(&budget, &checks)
        .expect_err("real gap refused");
    assert!(matches!(
        error.observation().failure(),
        Some(PinnedShardLogOpenFailure::Canonical)
    ));
    assert_eq!(error.observation().retained_sequences(), &[2, 4]);
    assert!(
        matches!(error.observation().bounds_result(), Some(Err(StorageError::InvalidInput(message))) if message == RETAINED_GAP_DIAGNOSTIC)
    );
    assert!(
        error
            .source()
            .expect("real canonical cause")
            .downcast_ref::<StorageError>()
            .is_some()
    );
    assert!(checks.denied.get());
    assert!(matches!(budget.check(), Err(StorageFetchFailure::Denied)));
    assert_eq!(f.root.snapshot(), before);
    let Fixture {
        pin,
        config: _,
        root,
    } = f;
    drop(pin);
    root.contended();
    drop(error);
    assert!(fs2::FileExt::try_lock_exclusive(&root.lock()).is_ok());
}

#[test]
fn actual_foreign_stream_retains_the_original_reservation_and_native_cause() {
    let f = Fixture::new(&[], 4096);
    let mut opened = f.opened();
    let mut reservation = super::super::reservation(7);
    reservation.topic_id = crate::TopicId::new(99);
    let prepared = opened
        .store()
        .prepare_append(
            reservation,
            None,
            Bytes::from_static(b"foreign actual frame"),
        )
        .expect("real native preparation");
    opened
        .store_mut()
        .append_prepared_group(&[&prepared], true)
        .expect("actual foreign native append");
    let before = f.root.snapshot();
    let checks = Checks::new();
    let budget = StorageFetchBudget::new(limits(), expiry()).expect("native counters");
    let error = opened
        .into_shard_log_until(&budget, &checks)
        .expect_err("actual foreign stream refused");
    assert_eq!(
        error.observation().retained_reservation(),
        Some(reservation)
    );
    assert!(
        matches!(error.observation().catalog_result(), Some(Err(StorageError::InvalidInput(message))) if message == CANONICAL_STREAM_DIAGNOSTIC)
    );
    assert!(error.observation().bounds_result().is_none());
    assert_eq!(f.root.snapshot(), before);
    assert!(checks.denied.get());
    f.root.contended();
    drop(error);
}

#[test]
fn each_exact_and_one_less_real_cumulative_allowance_is_enforced() {
    let f = Fixture::new(&[10, 11], 4096);
    let baseline = StorageFetchBudget::new(limits(), expiry()).expect("native counters");
    let log = f
        .opened()
        .into_shard_log_until(&baseline, &Checks::new())
        .expect("measured actual conversion");
    let used = baseline.usage().expect("actual cumulative native usage");
    drop(log);
    for axis in 0..3 {
        for exact in [true, false] {
            let mut finite = limits();
            finite.max_declared_bytes = used.declared_bytes;
            finite.max_entries = used.entries;
            finite.max_work_bytes = used.work_bytes;
            if !exact {
                match axis {
                    0 => finite.max_declared_bytes -= 1,
                    1 => finite.max_entries -= 1,
                    _ => finite.max_work_bytes -= 1,
                }
            }
            let opened = f.opened();
            let before = f.root.snapshot();
            let checks = Checks::new();
            let budget = StorageFetchBudget::new(finite, expiry())
                .expect("actual unchanged finite counters");
            let result = opened.into_shard_log_until(&budget, &checks);
            assert_eq!(result.is_ok(), exact);
            if exact {
                assert_eq!(budget.usage().expect("exact usage"), used);
            } else {
                assert!(checks.denied.get());
                assert!(matches!(budget.check(), Err(StorageFetchFailure::Denied)));
            }
            assert_eq!(f.root.snapshot(), before);
            f.root.contended();
            drop(result);
        }
    }
}

#[test]
fn actual_catalog_count_refuses_before_any_sequence_allocation_or_entry_inspection() {
    let f = Fixture::new(&[1, 2], 4096);
    let opened = f.opened();
    let mut finite = limits();
    finite.max_plan_batches = 1;
    let budget = StorageFetchBudget::new(finite, expiry()).expect("native counters");
    let error = opened
        .into_shard_log_until(&budget, &Checks::new())
        .expect_err("actual count refused");
    assert_eq!(error.observation().sequence_capacity(), 0);
    assert!(error.observation().catalog_result().is_none());
    assert_eq!(
        error.observation().catalog_shape().expect("actual table").0,
        2
    );
    f.root.contended();
}

#[test]
fn actual_sparse_table_capacity_scan_is_paid_before_materializing_selected_entries() {
    let history = (0..48).collect::<Vec<_>>();
    let f = Fixture::new(&history, 1);
    let mut opened = f.opened();
    opened
        .store_mut()
        .garbage_collect(&HashMap::from([(
            super::super::topic_partition(),
            LogicalOffset::new(46),
        )]))
        .expect("actual native GC leaves sparse table");
    let shape = opened
        .store()
        .log_catalog_shape()
        .expect("actual sparse shape");
    assert!(shape.1 > shape.0);
    let before = f.root.snapshot();
    let (fixed, entries, _) = fixed_fee().expect("source fee");
    let mut finite = limits();
    finite.max_work_bytes = fixed + entries + shape.2 - 1;
    let budget = StorageFetchBudget::new(finite, expiry()).expect("finite sparse work counters");
    let error = opened
        .into_shard_log_until(&budget, &Checks::new())
        .expect_err("actual capacity scan refused");
    assert_eq!(error.observation().catalog_shape(), Some(shape));
    assert_eq!(error.observation().sequence_capacity(), 0);
    assert!(error.observation().catalog_result().is_none());
    assert_eq!(f.root.snapshot(), before);
}

#[test]
fn completed_actual_bounds_are_retained_before_composing_refusal_and_denial_unwind() {
    let f = Fixture::new(&[8, 9], 4096);
    let opened = f.opened();
    let before = f.root.snapshot();
    let checks = Checks::new();
    checks.panic_deny.set(true);
    let armed = Rc::clone(&checks.armed);
    let _observer = observe(move |phase| {
        if phase == Phase::Bounds {
            armed.set(true);
        }
    });
    let budget = StorageFetchBudget::new(limits(), expiry()).expect("native counters");
    let error = opened
        .into_shard_log_until(&budget, &checks)
        .expect_err("actual post-bounds refusal");
    assert!(matches!(
        error.observation().bounds_result(),
        Some(Ok(Some((8, 10))))
    ));
    assert!(matches!(
        error.observation().failure(),
        Some(PinnedShardLogOpenFailure::Checkpoint(_))
    ));
    assert!(error.observation().panic_payloads()[2].is_some());
    assert!(checks.denied.get());
    assert!(matches!(budget.check(), Err(StorageFetchFailure::Denied)));
    assert_eq!(f.root.snapshot(), before);
    f.root.contended();
}

#[test]
fn original_expiry_after_real_bounds_retains_success_and_denies_same_counters() {
    let f = Fixture::new(&[3, 4], 4096);
    let opened = f.opened();
    let original = Instant::now() + Duration::from_millis(30);
    let budget = StorageFetchBudget::new(limits(), original).expect("actual original E");
    let _observer = observe(move |phase| {
        if phase == Phase::Bounds {
            while Instant::now() < original {
                std::thread::yield_now();
            }
        }
    });
    let checks = Checks::new();
    let error = opened
        .into_shard_log_until(&budget, &checks)
        .expect_err("actual late bounds");
    assert!(matches!(
        error.observation().bounds_result(),
        Some(Ok(Some((3, 5))))
    ));
    assert!(matches!(
        error.observation().failure(),
        Some(PinnedShardLogOpenFailure::Native(
            StorageFetchFailure::Expired(_)
        ))
    ));
    assert_eq!(error.observation().absolute_expiry(), original);
    assert!(checks.denied.get());
    f.root.contended();
}

#[test]
fn unwind_after_real_allocation_or_bounds_keeps_results_and_original_lease() {
    for phase in [Phase::Allocated, Phase::Bounds] {
        let f = Fixture::new(&[20, 21], 4096);
        let opened = f.opened();
        let before = f.root.snapshot();
        let _observer = observe(move |observed| {
            if observed == phase {
                std::panic::panic_any("actual conversion checkpoint unwind");
            }
        });
        let checks = Checks::new();
        let budget = StorageFetchBudget::new(limits(), expiry()).expect("native counters");
        let error = opened
            .into_shard_log_until(&budget, &checks)
            .expect_err("actual phase unwind");
        assert!(matches!(
            error.observation().failure(),
            Some(PinnedShardLogOpenFailure::Unwind)
        ));
        assert!(error.observation().panic_payloads()[0].is_some());
        assert!(checks.denied.get());
        assert!(error.observation().sequence_capacity() >= 2);
        if phase == Phase::Bounds {
            assert!(matches!(
                error.observation().bounds_result(),
                Some(Ok(Some((20, 22))))
            ));
        }
        let Fixture {
            pin,
            config: _,
            root,
        } = f;
        assert_eq!(root.snapshot(), before);
        drop(pin);
        root.contended();
        drop(error);
        assert!(fs2::FileExt::try_lock_exclusive(&root.lock()).is_ok());
    }
}

#[test]
fn canonical_conversion_consumes_remaining_same_budget_instead_of_resetting_allowances() {
    let f = Fixture::new(&[30, 31], 4096);
    let baseline = StorageFetchBudget::new(limits(), expiry()).expect("native counters");
    let log = f
        .opened()
        .into_shard_log_until(&baseline, &Checks::new())
        .expect("actual first conversion");
    let used = baseline.usage().expect("actual consumed counters");
    drop(log);
    let mut finite = limits();
    finite.max_declared_bytes = used.declared_bytes;
    let budget = StorageFetchBudget::new(finite, expiry()).expect("shared cumulative counters");
    budget
        .charge_values(1, 0, 0)
        .expect("actual earlier native work debit");
    let checks = Checks::new();
    let error = f
        .opened()
        .into_shard_log_until(&budget, &checks)
        .expect_err("remaining actual byte allowance refused");
    assert!(checks.denied.get());
    assert!(matches!(budget.check(), Err(StorageFetchFailure::Denied)));
    assert!(error.observation().bounds_result().is_none());
    f.root.contended();
}

#[test]
fn retained_result_panic_and_diagnostic_slots_are_paid_before_real_catalog_work() {
    let history = (100..116).collect::<Vec<_>>();
    let f = Fixture::new(&history, 4096);
    let generous =
        StorageFetchBudget::new(limits(), expiry()).expect("actual finite generous counters");
    let log = f
        .opened()
        .into_shard_log_until(&generous, &Checks::new())
        .expect("same real history succeeds with complete allowance");
    assert_eq!(log.bounds(), Some((100, 116)));
    drop(log);
    // This immutable 22-entry policy can fund sixteen sequence slots and six
    // outer values. It cannot also fund the actual retained results, three
    // independent panic payload headers, String header and diagnostic buffer.
    let mut finite = limits();
    finite.max_entries = 22;
    let budget =
        StorageFetchBudget::new(finite, expiry()).expect("original immutable entry policy");
    let opened = f.opened();
    let before = f.root.snapshot();
    let checks = Checks::new();
    let error = opened
        .into_shard_log_until(&budget, &checks)
        .expect_err("inner retained-value slots refuse before catalog work");
    assert!(matches!(
        error.observation().failure(),
        Some(PinnedShardLogOpenFailure::Native(
            StorageFetchFailure::Capacity
        ))
    ));
    assert_eq!(error.observation().sequence_capacity(), 0);
    assert!(error.observation().catalog_result().is_none());
    assert!(error.observation().bounds_result().is_none());
    assert!(checks.denied.get());
    assert!(matches!(budget.check(), Err(StorageFetchFailure::Denied)));
    assert_eq!(f.root.snapshot(), before);
    f.root.contended();
}
