//! Real filesystem tracked compaction fixtures. All are source-only and UNRUN.
use super::*;
use crate::{
    PinnedShardDirectoryLimits, PinnedShardFetchChecks, PinnedShardLogWriteChecks,
    PinnedStoreOpenChecks, PinnedStoreOpenLimits, ShardGuardAcquireLimits, ShardRecoveryGuard,
    ShardStore, ShardStoreConfig,
};
use bytes::Bytes;
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
type IdentityObserver = Box<dyn FnMut(crate::TrackedGcStage)>;
type TransferObserver = Box<dyn FnMut(crate::TrackedGcTransferPhase)>;
type Observer = Box<dyn FnMut(GcPhase)>;
thread_local! {static IDENTITY:RefCell<Option<IdentityObserver>>=RefCell::new(None);
static TRANSFER:RefCell<Option<TransferObserver>>=RefCell::new(None);
static OBSERVER:RefCell<Option<Observer>>=RefCell::new(None);}
pub(crate) fn identity(stage: crate::TrackedGcStage) {
    IDENTITY.with(|s| {
        if let Some(f) = &mut *s.borrow_mut() {
            f(stage);
        }
    });
}
pub(crate) fn transferred(phase: crate::TrackedGcTransferPhase) {
    TRANSFER.with(|s| {
        if let Some(f) = &mut *s.borrow_mut() {
            f(phase);
        }
    });
}
pub(crate) fn completed(phase: GcPhase) {
    OBSERVER.with(|s| {
        if let Some(f) = &mut *s.borrow_mut() {
            f(phase);
        }
    });
}
struct Observe;
impl Drop for Observe {
    fn drop(&mut self) {
        IDENTITY.with(|s| *s.borrow_mut() = None);
        TRANSFER.with(|s| *s.borrow_mut() = None);
        OBSERVER.with(|s| *s.borrow_mut() = None);
    }
}
fn at_identity(f: impl FnMut(crate::TrackedGcStage) + 'static) -> Observe {
    IDENTITY.with(|s| {
        assert!(s.borrow().is_none());
        *s.borrow_mut() = Some(Box::new(f));
    });
    Observe
}
fn at_transfer(f: impl FnMut(crate::TrackedGcTransferPhase) + 'static) -> Observe {
    TRANSFER.with(|s| {
        assert!(s.borrow().is_none());
        *s.borrow_mut() = Some(Box::new(f));
    });
    Observe
}
fn observe(f: impl FnMut(GcPhase) + 'static) -> Observe {
    OBSERVER.with(|s| {
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
impl PinnedShardLogGcChecks for Checks {
    fn deny(&self) {
        self.denials.set(self.denials.get() + 1);
        if self.panic_deny.get() {
            std::panic::panic_any("actual GC composing denial");
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
        max_declared_bytes: 1 << 29,
        max_work_bytes: usize::MAX / 16,
    }
}
fn append_limits() -> PinnedShardAppendLimits {
    PinnedShardAppendLimits {
        max_operations: 16,
        max_frames: 64,
        max_payload_bytes: 1 << 16,
        max_segments: 64,
        max_rotations: 64,
        max_path_bytes: 8192,
        max_name_bytes: 1024,
        max_packs: 512,
        max_batch_slots: 1024,
        max_topic_slots: 128,
        max_location_slots: 1024,
        max_io_calls: 2048,
        max_write_calls: 1024,
        max_write_bytes: 1 << 24,
        max_work_bytes: 1 << 24,
    }
}
fn write_limits() -> PinnedShardLogWriteLimits {
    PinnedShardLogWriteLimits {
        max_live_files: 512,
        max_tracked_packs: 256,
        max_retired_slots: 128,
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
            "canonical-owned-tracked-gc-{}-{}",
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

fn gc_limits() -> PinnedShardGcLimits {
    PinnedShardGcLimits {
        max_path_bytes: 8192,
        max_name_bytes: 1024,
        max_packs: 256,
        max_batch_slots: 2048,
        max_log_start_slots: 64,
        max_topic_slots: 128,
        max_location_slots: 2048,
        max_plan_packs: 128,
        max_io_calls: 5 + 11 * 128,
        max_work_bytes: 1 << 24,
    }
}
fn packs(root: &Path) -> Vec<PathBuf> {
    let mut names = fs::read_dir(root)
        .expect("real directory")
        .map(|v| v.expect("real entry").path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("pack-") && n.ends_with(".sse"))
        })
        .collect::<Vec<_>>();
    names.sort();
    names
}
fn pending(root: &Path) -> PathBuf {
    fs::read_dir(root)
        .expect("real pending directory")
        .map(|v| v.expect("real entry").path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(".gc-pending"))
        })
        .expect("actual renamed pending pack")
}
fn native(b: &StorageFetchBudget) -> PinnedShardAppendBudget {
    PinnedShardAppendBudget::new(append_limits(), b.absolute_expiry())
        .expect("original bounded native append counters")
}
#[allow(
    clippy::result_large_err,
    reason = "retain the actual native GC owner/error in the fixture without a failure allocation"
)]
fn compact(
    log: OpenedPinnedShardLog,
    f: &Fixture,
    floor: u64,
    n: &mut PinnedShardAppendBudget,
    b: &StorageFetchBudget,
    c: &Checks,
) -> Result<PinnedShardLogGcComplete, PinnedShardLogGcError> {
    log.compact_before_until(&f.pin, floor, n, b, write_limits(), gc_limits(), c)
}
#[test]
fn initial_sealed_descriptors_quarantine_only_after_native_ok_and_retained_suffix_reads() {
    let f = Fixture::new(&[1, 2, 3], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same lifetime counters");
    let mut n = native(&b);
    let complete = compact(f.log(&b), &f, 3, &mut n, &b, &Checks::new())
        .expect("actual tracked native collection");
    let receipt = complete
        .observation()
        .receipt()
        .copied()
        .expect("actual receipt");
    assert_eq!(receipt.records_removed, 2);
    assert_eq!(complete.observation().quarantined_files(), 2);
    assert_eq!(
        complete
            .observation()
            .metadata_results()
            .filter(|r| r.is_ok())
            .count(),
        12
    );
    assert_eq!(
        complete
            .observation()
            .native_usage()
            .expect("actual physical usage")
            .entered_io_calls,
        5 + 11 * 2
    );
    assert!(complete.observation().physical_results().all(Result::is_ok));
    let log = complete.into_log();
    assert_eq!(log.bounds(), Some((3, 4)));
    let read = log
        .read_from_until(&f.pin, 1, 1 << 20, 32, &b, &Checks::new())
        .expect("same retained suffix File");
    assert_eq!(
        read.records()
            .iter()
            .map(|r| r.sequence)
            .collect::<Vec<_>>(),
        vec![3]
    );
}
#[test]
fn managed_retired_origin_dense_tail_move_retags_the_genuine_surviving_file() {
    let f = Fixture::new(&[1], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
    let mut n = native(&b);
    let log = f
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
        .expect("genuine retired packs")
        .into_log();
    let complete = compact(log, &f, 3, &mut n, &b, &Checks::new()).expect("real prefix removal");
    assert_eq!(complete.observation().quarantined_files(), 2);
    assert!(
        complete
            .observation()
            .retags()
            .any(|(from, to, seq, result, empty)| from > to
                && seq.is_some()
                && result == Some(true)
                && empty)
    );
    assert!(
        complete
            .observation()
            .transfers()
            .all(|(seq, _, ok, removed)| ok == Some(true) && removed == Some(seq))
    );
    let read = complete
        .into_log()
        .read_from_until(&f.pin, 3, 1 << 20, 32, &b, &Checks::new())
        .expect("retagged retired inode resolves without reopen");
    assert_eq!(
        read.records()
            .iter()
            .map(|r| r.sequence)
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
}
#[test]
fn repeated_actual_write_seal_collect_cycles_reuse_original_dense_catalog_policy() {
    let f = Fixture::new(&[], 4096);
    let b = StorageFetchBudget::new(fetch_limits(), expiry())
        .expect("one global budget for every cycle");
    let mut policy = append_limits();
    policy.max_rotations = 4;
    let mut n = PinnedShardAppendBudget::new(policy, b.absolute_expiry())
        .expect("one unchanged native budget");
    let mut write = write_limits();
    write.max_retired_slots = 4;
    write.max_tracked_packs = 8;
    write.max_live_files = 16;
    // The actual opening policy prepays eight real scanned slots too; no
    // smaller logical policy is claimed over the ordinary 128-slot fixture.
    let mut opening = open_limits();
    opening.max_packs = 8;
    let mut log =
        ShardStore::open_pinned_until(f.config.clone(), &f.pin, opening, expiry(), &Checks::new())
            .expect("actual bounded eight-slot open")
            .into_shard_log_until(&b, &Checks::new())
            .expect("actual same-budget conversion");
    for seq in 1..=4 {
        let owner = log
            .append_group_until(
                &f.pin,
                seq,
                records(1),
                true,
                &mut n,
                &b,
                write,
                &Checks::new(),
            )
            .expect("actual write with unchanged policy");
        let complete = owner
            .into_log()
            .compact_before_until(
                &f.pin,
                seq + 1,
                &mut n,
                &b,
                write,
                gc_limits(),
                &Checks::new(),
            )
            .expect("same cumulative native/global counters");
        assert_eq!(complete.observation().quarantined_files(), 1);
        assert_eq!(complete.bounds(), None);
        log = complete.into_log();
    }
    assert_eq!(n.usage().rotations, 4);
    assert!(!n.is_refused());
    drop(log);
}
#[test]
fn actual_original_and_pending_name_substitution_refuse_at_all_three_identity_cuts() {
    for cut in [
        crate::TrackedGcStage::BeforeEpoch,
        crate::TrackedGcStage::BeforeIndex,
        crate::TrackedGcStage::BeforeUnlink,
    ] {
        let f = Fixture::new(&[1, 2], 1);
        let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
        let log = f.log(&b);
        let root = f.root.0.clone();
        let mut once = false;
        let _observer = at_identity(move |stage| {
            if stage == cut && !once {
                once = true;
                let path = if cut == crate::TrackedGcStage::BeforeEpoch {
                    packs(&root)[0].clone()
                } else {
                    pending(&root)
                };
                let kept = root.join("actual-substituted-inode.kept");
                fs::rename(&path, &kept).expect("retain actual original inode");
                fs::copy(&kept, &path).expect("genuine different inode containing native bytes");
            }
        });
        let mut n = native(&b);
        let checks = Checks::new();
        let error =
            compact(log, &f, 2, &mut n, &b, &checks).expect_err("held and named inode mismatch");
        assert_eq!(
            error.observation().native_failure(),
            Some(PinnedShardGcFailure::Identity)
        );
        assert_eq!(error.observation().quarantined_files(), 0);
        let report = error
            .scope
            .report()
            .expect("actual retained native partial report");
        let pack = report.pack(0).expect("actual selected native pack");
        match cut {
            crate::TrackedGcStage::BeforeEpoch => {
                assert_eq!(report.epochs().0, report.epochs().1);
                assert!(!pack.rename_entered());
            }
            crate::TrackedGcStage::BeforeIndex => {
                assert!(matches!(pack.rename_result(), Some(Ok(()))));
                assert!(pack.removed_path().is_none());
            }
            crate::TrackedGcStage::BeforeUnlink => {
                assert!(pack.removed_path().is_some());
                assert!(!pack.unlink_entered());
            }
        }
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
fn symlink_and_hardlink_replacements_are_actual_strict_identity_refusals() {
    for hardlink in [false, true] {
        let f = Fixture::new(&[1, 2], 1);
        let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
        let log = f.log(&b);
        let root = f.root.0.clone();
        let mut once = false;
        let _observer = at_identity(move |cut| {
            if cut == crate::TrackedGcStage::BeforeEpoch && !once {
                once = true;
                let path = packs(&root)[0].clone();
                let kept = root.join("actual-link-target.kept");
                if hardlink {
                    fs::hard_link(&path, &kept).expect("real second hardlink");
                } else {
                    fs::rename(&path, &kept).expect("actual retained inode");
                    std::os::unix::fs::symlink(&kept, &path).expect("actual replacement symlink");
                }
            }
        });
        let mut n = native(&b);
        let error = compact(log, &f, 2, &mut n, &b, &Checks::new())
            .expect_err("regular single-link identity");
        assert_eq!(
            error.observation().native_failure(),
            Some(PinnedShardGcFailure::Identity)
        );
        assert_eq!(error.observation().quarantined_files(), 0);
    }
}
#[test]
fn preexisting_pending_is_never_overwritten_and_native_error_never_extracts_quarantine() {
    let f = Fixture::new(&[1, 2], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
    let log = f.log(&b);
    let original = packs(&f.root.0)[0].clone();
    let pending = PathBuf::from(format!("{}.gc-pending", original.display()));
    fs::write(&pending, b"unknown existing tombstone").expect("real pending obstacle");
    let mut n = native(&b);
    let error =
        compact(log, &f, 2, &mut n, &b, &Checks::new()).expect_err("native atomic no-replace");
    assert_eq!(
        error.observation().native_failure(),
        Some(PinnedShardGcFailure::Io)
    );
    assert_eq!(
        fs::read(&pending).expect("actual retained pending bytes"),
        b"unknown existing tombstone"
    );
    assert!(original.exists());
    assert_eq!(error.observation().quarantined_files(), 0);
    let p = error
        .scope
        .report()
        .expect("native error report")
        .pack(0)
        .expect("selected pack");
    assert!(matches!(p.rename_result(), Some(Err(_))));
    assert!(matches!(p.rename_sync_result(), Some(Ok(()))));
}
#[test]
fn all_partial_quarantine_move_and_dense_retag_boundaries_retain_actual_native_ok() {
    for phase in [
        crate::TrackedGcTransferPhase::Quarantined,
        crate::TrackedGcTransferPhase::RegistrationRemoved,
        crate::TrackedGcTransferPhase::CatalogMoved,
        crate::TrackedGcTransferPhase::Retagged,
    ] {
        for unwind in [false, true] {
            let f = Fixture::new(&[1], 1);
            let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
            let mut n = native(&b);
            let log = f
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
                .expect("actual retired catalog")
                .into_log();
            let checks = Checks::new();
            let armed = checks.armed.clone();
            let mut once = false;
            let _observer = at_transfer(move |p| {
                if p == phase && !once {
                    once = true;
                    if unwind {
                        std::panic::panic_any("actual partial GC descriptor move/retag");
                    } else {
                        armed.set(true);
                    }
                }
            });
            let error = compact(log, &f, 3, &mut n, &b, &checks)
                .expect_err("actual entered transfer refusal");
            assert!(matches!(error.scope.native, Some(Ok(_))));
            assert!(error.observation().quarantined_files() > 0);
            assert_eq!(checks.denials.get(), 1);
            assert!(b.check().is_err());
            assert_eq!(error.observation().panic_payload().is_some(), unwind);
            let Fixture { pin, root, .. } = f;
            drop(pin);
            root.contended();
            drop(error);
            root.released();
        }
    }
}
#[test]
fn saved_native_ok_late_check_keeps_original_unlinked_files_without_transfer() {
    let f = Fixture::new(&[1, 2], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
    let log = f.log(&b);
    let checks = Checks::new();
    let armed = checks.armed.clone();
    let _observer = observe(move |phase| {
        if phase == GcPhase::NativeCompleted {
            armed.set(true);
        }
    });
    let mut n = native(&b);
    let error = compact(log, &f, 2, &mut n, &b, &checks)
        .expect_err("actual native completion followed by real refusal");
    assert!(matches!(error.scope.native, Some(Ok(_))));
    assert_eq!(error.observation().quarantined_files(), 0);
    assert!(error.scope.seal.as_ref().is_some_and(Result::is_ok));
    assert_eq!(checks.denials.get(), 1);
    let Fixture { pin, root, .. } = f;
    drop(pin);
    root.contended();
    drop(error);
    root.released();
}
#[test]
fn actual_callback_and_denial_panics_are_independent_retained_failures() {
    let f = Fixture::new(&[1, 2], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
    let log = f.log(&b);
    let checks = Checks::new();
    checks.panic_deny.set(true);
    let _observer = at_identity(|p| {
        if p == crate::TrackedGcStage::BeforeIndex {
            std::panic::panic_any("actual post-rename callback");
        }
    });
    let mut n = native(&b);
    let error = compact(log, &f, 2, &mut n, &b, &checks).expect_err("actual two panics");
    assert_eq!(
        error.observation().native_failure(),
        Some(PinnedShardGcFailure::Unwind)
    );
    assert!(
        error
            .scope
            .report()
            .expect("native result")
            .panic_payload()
            .is_some()
    );
    assert_eq!(error.observation().denial_panics(), [false, true]);
    assert_eq!(checks.denials.get(), 1);
}
#[test]
fn actual_min_expiry_after_seal_never_enters_new_collection_or_renews_global_budget() {
    let f = Fixture::new(&[1, 2], 1);
    let e = Instant::now() + Duration::from_millis(100);
    let b = StorageFetchBudget::new(fetch_limits(), e).expect("original E");
    let log = f.log(&b);
    let _observer = observe(move |phase| {
        if phase == GcPhase::Sealed {
            while Instant::now() < e {
                std::thread::yield_now();
            }
        }
    });
    let mut n = native(&b);
    let checks = Checks::new();
    let error = compact(log, &f, 2, &mut n, &b, &checks)
        .expect_err("same min E expires after genuine completed seal");
    assert!(error.scope.native.is_none());
    assert!(error.scope.seal.as_ref().is_some_and(Result::is_ok));
    assert!(b.check().is_err());
    assert_eq!(checks.denials.get(), 1);
}
#[test]
fn original_exact_and_one_less_native_call_policies_derive_five_plus_eleven_per_pack() {
    for calls in [16, 15] {
        let f = Fixture::new(&[1, 2], 1);
        let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
        let log = f.log(&b);
        let mut n = native(&b);
        let mut policy = gc_limits();
        policy.max_plan_packs = 1;
        policy.max_io_calls = calls;
        assert_eq!(
            tracking_gc_cost(policy)
                .expect("checked real producer contract")
                .native_call_slots,
            16
        );
        let result = log.compact_before_until(
            &f.pin,
            2,
            &mut n,
            &b,
            write_limits(),
            policy,
            &Checks::new(),
        );
        match (calls, result) {
            (16, Ok(owner)) => {
                assert_eq!(
                    owner
                        .observation()
                        .native_usage()
                        .expect("real native calls")
                        .entered_io_calls,
                    16
                );
                assert_eq!(owner.observation().quarantined_files(), 1);
            }
            (15, Err(error)) => {
                assert!(error.scope.seal.is_none());
                assert!(error.scope.native.is_none());
            }
            _ => panic!("real original exact/one-less policy disagreement"),
        }
    }
}
#[test]
fn immutable_small_entry_policy_refuses_before_seal_and_holds_genuine_initial_log() {
    let f = Fixture::new(&[], 4096);
    let mut limits = fetch_limits();
    limits.max_entries = 96;
    let b = StorageFetchBudget::new(limits, expiry()).expect("unchanged original finite policy");
    let log = f.log(&b);
    let before = f.root.snapshot();
    let mut n = native(&b);
    let checks = Checks::new();
    let error = compact(log, &f, 1, &mut n, &b, &checks)
        .expect_err("explicit real GC/header slots before seal");
    assert!(error.scope.seal.is_none());
    assert!(error.scope.native.is_none());
    assert_eq!(f.root.snapshot(), before);
    assert!(b.check().is_err());
    assert_eq!(checks.denials.get(), 1);
}
#[test]
fn actual_leading_zero_names_preserve_native_policy_through_tracked_collection() {
    let f = Fixture::new(&[1, 2, 3], 1);
    for path in packs(&f.root.0) {
        let name = path
            .file_name()
            .expect("real name")
            .to_str()
            .expect("ordinary native decimal name");
        fs::rename(
            &path,
            f.root.0.join(format!(
                "pack-0000000000000000000000000000000000000000{}",
                &name[5..]
            )),
        )
        .expect("actual accepted leading-zero native spelling");
    }
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
    let mut n = native(&b);
    let complete =
        compact(f.log(&b), &f, 3, &mut n, &b, &Checks::new()).expect("actual long retained names");
    assert_eq!(
        complete
            .observation()
            .receipt()
            .expect("actual receipt")
            .records_removed,
        2
    );
    let read = complete
        .into_log()
        .read_from_until(&f.pin, 3, 1 << 20, 32, &b, &Checks::new())
        .expect("same retained leading-zero suffix inode");
    assert_eq!(read.records().len(), 1);
}
#[test]
fn ordinary_gc_epoch_mutation_is_refused_without_reopening_or_fabricated_tracking() {
    let f = Fixture::new(&[1, 2], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
    let mut log = f.log(&b);
    log.scope
        .opened
        .store_mut()
        .garbage_collect(&HashMap::from([(
            super::super::super::topic_partition(),
            LogicalOffset::new(2),
        )]))
        .expect("real untracked ordinary mutation");
    let mut n = native(&b);
    let error = compact(log, &f, 2, &mut n, &b, &Checks::new())
        .expect_err("genuine tracking refuses stale epoch");
    assert!(error.scope.native.is_none());
    assert!(error.scope.seal.as_ref().is_some_and(Result::is_err));
}
#[test]
fn successful_quarantine_destruction_and_whole_log_return_preserve_real_kernel_lease() {
    let f = Fixture::new(&[1, 2], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
    let mut n = native(&b);
    let complete =
        compact(f.log(&b), &f, 2, &mut n, &b, &Checks::new()).expect("actual completed collection");
    assert_eq!(complete.observation().quarantined_files(), 1);
    let Fixture { pin, root, .. } = f;
    drop(pin);
    root.contended();
    let log = complete.into_log();
    root.contended();
    drop(log);
    root.released();
}
#[test]
fn actual_missing_pending_stat_retains_not_found_after_real_rename_and_before_index() {
    let f = Fixture::new(&[1, 2], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
    let log = f.log(&b);
    let root = f.root.0.clone();
    let mut once = false;
    let _observer = at_identity(move |phase| {
        if phase == crate::TrackedGcStage::BeforeIndex && !once {
            once = true;
            fs::remove_file(pending(&root)).expect("actual external pending disappearance");
        }
    });
    let mut n = native(&b);
    let error = compact(log, &f, 2, &mut n, &b, &Checks::new())
        .expect_err("actual retained native stat error");
    assert_eq!(
        error.observation().native_failure(),
        Some(PinnedShardGcFailure::Io)
    );
    assert!(error.observation().metadata_results().any(|r| {
        r.as_ref()
            .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound)
    }));
    assert!(
        error
            .scope
            .report()
            .expect("native report")
            .pack(0)
            .expect("actual renamed pack")
            .removed_path()
            .is_none()
    );
    assert_eq!(error.observation().quarantined_files(), 0);
}
#[test]
fn actual_parent_rename_refuses_named_directory_identity_and_retains_original_lease() {
    let f = Fixture::new(&[1, 2], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same counters");
    let log = f.log(&b);
    let old = f.root.0.clone();
    let moved = old.with_extension("moved");
    let to = moved.clone();
    let _observer = observe(move |phase| {
        if phase == GcPhase::Sealed {
            fs::rename(&old, &to).expect("actual original parent rename");
            fs::create_dir(&old).expect("actual different named parent inode");
        }
    });
    let mut n = native(&b);
    let error = compact(log, &f, 2, &mut n, &b, &Checks::new())
        .expect_err("actual held parent differs from named parent");
    assert_eq!(
        error.observation().native_failure(),
        Some(PinnedShardGcFailure::Identity)
    );
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(moved.join(".shard-store.lock"))
        .expect("SAME original real lease path");
    drop(f);
    assert!(fs2::FileExt::try_lock_exclusive(&lock).is_err());
    drop(error);
    assert!(fs2::FileExt::try_lock_exclusive(&lock).is_ok());
    drop(lock);
    fs::remove_dir_all(moved).expect("actual fixture cleanup");
}
#[test]
fn actual_sparse_native_maps_keep_capacity_and_refuse_one_less_original_capacity_policy() {
    for short in [false, true] {
        let history = (1..=64).collect::<Vec<_>>();
        let f = Fixture::new(&history, 1);
        let b = StorageFetchBudget::new(fetch_limits(), expiry())
            .expect("same counters across both collections");
        let mut n = native(&b);
        let complete = compact(f.log(&b), &f, 64, &mut n, &b, &Checks::new())
            .expect("genuine ordinary-history sparse index after removal");
        let shape = complete
            .seal
            .retained_log()
            .scope
            .opened
            .store()
            .log_catalog_shape()
            .expect("actual retained native map");
        assert!(shape.1 > shape.0);
        let log = complete.into_log();
        let mut cap = gc_limits();
        cap.max_batch_slots = if short { shape.1 - 1 } else { shape.1 };
        let result =
            log.compact_before_until(&f.pin, 65, &mut n, &b, write_limits(), cap, &Checks::new());
        if short {
            let error =
                result.expect_err("actual capacity is larger than original unchanged limit");
            assert_eq!(
                error.observation().native_failure(),
                Some(PinnedShardGcFailure::Capacity)
            );
            assert_eq!(error.observation().quarantined_files(), 0);
        } else {
            let complete = result.expect("exact actual capacity policy");
            assert_eq!(
                complete
                    .observation()
                    .receipt()
                    .expect("real receipt")
                    .records_removed,
                1
            );
            assert_eq!(complete.bounds(), None);
        }
    }
}

#[test]
fn actual_new_epoch_reads_survive_and_old_genuine_plan_refuses_before_any_read_attempt() {
    let f = Fixture::new(&[1, 2, 3], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same global counters");
    let log = f.log(&b);
    let old = log
        .scope
        .opened
        .plan_fetch_pinned_until(
            super::super::super::topic_partition(),
            LogicalOffset::new(1),
            1 << 20,
            32,
            &b,
            &Checks::new(),
        )
        .expect("actual old tracked plan");
    let mut n = native(&b);
    let complete =
        compact(log, &f, 3, &mut n, &b, &Checks::new()).expect("actual tracked epoch transition");
    let read = complete
        .into_log()
        .read_from_until(&f.pin, 3, 1 << 20, 32, &b, &Checks::new())
        .expect("actual fresh plan with retagged Files");
    assert_eq!(read.records().len(), 1);
    let log = read.into_log();
    let before = b
        .usage()
        .expect("actual counters before stale plan")
        .read_attempts;
    let error = log
        .scope
        .opened
        .read_fetch_plan_pinned_until(&f.pin, old, &b, &Checks::new())
        .expect_err("old actual store epoch");
    assert!(matches!(
        error.observation().failure(),
        Some(crate::PinnedShardFetchFailure::Stale)
    ));
    assert_eq!(
        b.usage().expect("actual retained counters").read_attempts,
        before
    );
}
#[test]
fn original_eighty_nine_and_eighty_eight_entry_policies_detect_every_fixed_gc_header_slot() {
    // Empty genuine canonical conversion has 22 values + one buffer (23).
    // The original 47 bridge values gain 18 independent append-phase values:
    // AppendDenial/GcChecks, two Cells, three bools, result RefCell/Option,
    // two actual Result headers, fourth Box, append RefCell/reference/RefMut,
    // the result-storage RefMut and two adapter references: 65 values + one buffer (66).
    // Immutable policies 89/88 check this fixed admission; the exact case then
    // refuses the DISTINCT native allowance, not a claimed full-GC fit.
    for cap in [89, 88] {
        let f = Fixture::new(&[], 4096);
        let mut policy = fetch_limits();
        policy.max_entries = cap;
        let b = StorageFetchBudget::new(policy, expiry())
            .expect("original immutable exact/one-less entry policy");
        let log = f.log(&b);
        let seen = Rc::new(Cell::new(false));
        let observed = seen.clone();
        let _observer = observe(move |p| {
            if p == GcPhase::FixedPaid {
                observed.set(true);
            }
        });
        let before = f.root.snapshot();
        let mut n = native(&b);
        let error = compact(log, &f, 1, &mut n, &b, &Checks::new())
            .expect_err("later distinct native fee or fixed header fee");
        assert_eq!(seen.get(), cap == 89);
        assert!(error.scope.seal.is_none());
        assert!(error.scope.native.is_none());
        assert_eq!(f.root.snapshot(), before);
        assert!(b.check().is_err());
        assert_eq!(
            b.usage().expect("actual retained entries").entries,
            if cap == 89 { 89 } else { 23 }
        );
    }
}

// Successor-only hooks observe the actual returned sticky denial action. No
// owner, grant, guessed completion bit or production policy is constructed.
type AppendObserver = Box<dyn FnMut(bool)>;
thread_local! {static APPEND_OBSERVER:RefCell<Option<AppendObserver>>=RefCell::new(None);}
pub(crate) fn append_denied(refused: bool) {
    APPEND_OBSERVER.with(|s| {
        if let Some(f) = &mut *s.borrow_mut() {
            f(refused);
        }
    });
}
struct AppendObserve;
impl Drop for AppendObserve {
    fn drop(&mut self) {
        APPEND_OBSERVER.with(|s| *s.borrow_mut() = None);
    }
}
fn observe_append(f: impl FnMut(bool) + 'static) -> AppendObserve {
    APPEND_OBSERVER.with(|s| {
        assert!(s.borrow().is_none());
        *s.borrow_mut() = Some(Box::new(f));
    });
    AppendObserve
}
struct PendingChecks<'a> {
    root: &'a Path,
    budget: &'a StorageFetchBudget,
    append_seen: Rc<Cell<usize>>,
    denials: Cell<usize>,
    saw_pending: Cell<bool>,
    snapshot: Cell<Option<(usize, bool)>>,
    expire_at: Option<Instant>,
    panic_deny: bool,
}
impl ShardDirectoryChecks for PendingChecks<'_> {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        let pending = fs::read_dir(self.root)
            .expect("actual pending-name observation")
            .any(|e| {
                e.expect("actual entry")
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".gc-pending")
            });
        if pending {
            self.saw_pending.set(true);
            if let Some(e) = self.expire_at {
                while Instant::now() < e {
                    std::thread::yield_now();
                }
                // The unchanged adapter itself observes its original min E.
                return Ok(());
            }
            return Err(ShardDirectoryCheckFailure::Refused);
        }
        Ok(())
    }
}
impl PinnedShardLogGcChecks for PendingChecks<'_> {
    fn deny(&self) {
        self.denials.set(self.denials.get() + 1);
        let actual = (self.append_seen.get(), self.budget.check().is_err());
        self.snapshot.set(Some(actual));
        assert_eq!(
            actual,
            (1, true),
            "actual append action and global denial precede outer health"
        );
        assert!(
            self.saw_pending.get(),
            "refusal follows the genuine native rename"
        );
        if self.panic_deny {
            std::panic::panic_any("actual independent outer health denial panic");
        }
    }
}
#[test]
fn same_append_is_denied_before_health_and_prepaid_pending_rename_sync() {
    let f = Fixture::new(&[1, 2], 1);
    let b =
        StorageFetchBudget::new(fetch_limits(), expiry()).expect("same original global counters");
    let log = f.log(&b);
    let seen = Rc::new(Cell::new(0));
    let observed = seen.clone();
    let _observer = observe_append(move |refused| {
        assert!(refused);
        observed.set(observed.get() + 1);
    });
    let c = PendingChecks {
        root: &f.root.0,
        budget: &b,
        append_seen: seen.clone(),
        denials: Cell::new(0),
        saw_pending: Cell::new(false),
        snapshot: Cell::new(None),
        expire_at: None,
        panic_deny: false,
    };
    let mut n = native(&b);
    let original = n.limits();
    let error = log
        .compact_before_until(&f.pin, 2, &mut n, &b, write_limits(), gc_limits(), &c)
        .expect_err("real post-rename refusal");
    assert_eq!(c.snapshot.get(), Some((1, true)));
    assert_eq!(c.denials.get(), 1);
    assert_eq!(seen.get(), 1);
    assert!(n.is_refused());
    assert_eq!(n.limits(), original);
    assert_eq!(error.observation().append_denial(), (true, true, false));
    assert_eq!(error.observation().denial_panics(), [false, false]);
    let report = error
        .scope
        .report()
        .expect("whole actual native error report");
    let p = report.pack(0).expect("actual renamed selected pack");
    assert!(matches!(p.rename_result(), Some(Ok(()))));
    assert!(
        p.rename_sync_outcome().is_none(),
        "normal sync never started before the refused post-rename checkpoint"
    );
    assert!(
        matches!(p.rename_sync_result(), Some(Ok(()))),
        "already-paid cleanup sync physically returned"
    );
    assert!(p.removed_path().is_none());
    assert!(!p.unlink_entered());
    drop(c);
    let Fixture { pin, root, .. } = f;
    drop(pin);
    root.contended();
    drop(error);
    root.released();
}
#[test]
fn append_hook_and_health_panics_remain_independent_before_real_cleanup_sync() {
    for append_panic in [false, true] {
        for health_panic in [false, true] {
            let f = Fixture::new(&[1, 2], 1);
            let b = StorageFetchBudget::new(fetch_limits(), expiry())
                .expect("unchanged global counters");
            let log = f.log(&b);
            let seen = Rc::new(Cell::new(0));
            let observed = seen.clone();
            let _observer = observe_append(move |refused| {
                assert!(refused);
                observed.set(observed.get() + 1);
                if append_panic {
                    std::panic::panic_any("actual append-denied observer panic");
                }
            });
            let c = PendingChecks {
                root: &f.root.0,
                budget: &b,
                append_seen: seen.clone(),
                denials: Cell::new(0),
                saw_pending: Cell::new(false),
                snapshot: Cell::new(None),
                expire_at: None,
                panic_deny: health_panic,
            };
            let mut n = native(&b);
            let error = log
                .compact_before_until(&f.pin, 2, &mut n, &b, write_limits(), gc_limits(), &c)
                .expect_err("actual post-rename refusal with independent denial panics");
            assert_eq!(
                error.observation().append_denial(),
                (true, true, append_panic)
            );
            assert_eq!(error.observation().denial_panics(), [false, health_panic]);
            assert_eq!(c.snapshot.get(), Some((1, true)));
            assert_eq!(c.denials.get(), 1);
            assert_eq!(seen.get(), 1);
            assert!(n.is_refused());
            assert!(b.check().is_err());
            let p = error
                .scope
                .report()
                .expect("actual native retained failure")
                .pack(0)
                .expect("actual renamed target");
            assert!(matches!(p.rename_result(), Some(Ok(()))));
            assert!(p.rename_sync_outcome().is_none());
            assert!(matches!(p.rename_sync_result(), Some(Ok(()))));
            assert!(!p.unlink_entered());
            assert!(p.removed_path().is_none());
            drop(c);
            let Fixture { pin, root, .. } = f;
            drop(pin);
            root.contended();
            drop(error);
            root.released();
        }
    }
}
#[test]
fn original_append_min_expiry_after_actual_rename_denies_before_paid_settlement() {
    let f = Fixture::new(&[1, 2], 1);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("original longer global E");
    let log = f.log(&b);
    let min_e = Instant::now() + Duration::from_millis(250);
    let mut n =
        PinnedShardAppendBudget::new(append_limits(), min_e).expect("original earlier append E");
    let seen = Rc::new(Cell::new(0));
    let observed = seen.clone();
    let _observer = observe_append(move |refused| {
        assert!(refused);
        observed.set(observed.get() + 1);
    });
    let c = PendingChecks {
        root: &f.root.0,
        budget: &b,
        append_seen: seen.clone(),
        denials: Cell::new(0),
        saw_pending: Cell::new(false),
        snapshot: Cell::new(None),
        expire_at: Some(min_e),
        panic_deny: false,
    };
    let error = log
        .compact_before_until(&f.pin, 2, &mut n, &b, write_limits(), gc_limits(), &c)
        .expect_err("saved min E after a genuine entered rename");
    assert!(c.saw_pending.get());
    assert_eq!(c.snapshot.get(), Some((1, true)));
    assert_eq!(c.denials.get(), 1);
    assert_eq!(seen.get(), 1);
    assert!(n.is_refused());
    assert_eq!(n.absolute_expiry(), min_e);
    assert_eq!(error.scope.expiry, min_e);
    assert_eq!(error.observation().append_denial(), (true, true, false));
    let p = error
        .scope
        .report()
        .expect("actual late native error")
        .pack(0)
        .expect("actual entered target");
    assert!(matches!(p.rename_result(), Some(Ok(()))));
    assert!(p.rename_sync_outcome().is_none());
    assert!(matches!(p.rename_sync_result(), Some(Ok(()))));
    assert!(!p.unlink_entered());
    drop(c);
    let Fixture { pin, root, .. } = f;
    drop(pin);
    root.contended();
    drop(error);
    root.released();
}
#[test]
fn public_sticky_append_deny_preserves_limits_counters_min_e_and_real_owned_failure() {
    let f = Fixture::new(&[], 4096);
    let b = StorageFetchBudget::new(fetch_limits(), expiry()).expect("same global lifetime budget");
    let mut n = native(&b);
    let log = f
        .log(&b)
        .append_group_until(
            &f.pin,
            1,
            records(1),
            true,
            &mut n,
            &b,
            write_limits(),
            &Checks::new(),
        )
        .expect("genuine first bounded write")
        .into_log();
    let earlier = n.absolute_expiry() - Duration::from_millis(1);
    n.cap_expiry(earlier);
    let policy = n.limits();
    let used = n.usage();
    let before = f.root.snapshot();
    n.deny();
    n.deny();
    assert!(n.is_refused());
    assert_eq!(n.limits(), policy);
    assert_eq!(n.usage(), used);
    assert_eq!(n.absolute_expiry(), earlier);
    let c = Checks::new();
    let error = log
        .append_group_until(&f.pin, 2, records(1), true, &mut n, &b, write_limits(), &c)
        .expect_err("actual same-owner call after public sticky denial");
    assert!(n.is_refused());
    assert_eq!(n.limits(), policy);
    assert_eq!(n.usage(), used);
    assert_eq!(n.absolute_expiry(), earlier);
    assert_eq!(f.root.snapshot(), before);
    assert!(b.check().is_err());
    assert_eq!(c.denials.get(), 1);
    let Fixture { pin, root, .. } = f;
    drop(pin);
    root.contended();
    drop(error);
    root.released();
}
