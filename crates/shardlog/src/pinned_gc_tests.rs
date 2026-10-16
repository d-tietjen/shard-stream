//! Genuine native files, locks, index effects and bounded GC refusal custody.
//!
//! The ordinary fixture opener builds actual input stores. These tests do not
//! qualify bounded store opening, Core custody, a signed Session or SDK startup.

use super::*;
use std::cell::{Cell, RefCell};
use std::num::NonZeroU32;
use std::os::unix::fs::symlink;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

type Observer = Box<dyn FnMut(PinnedShardGcPhase)>;
thread_local! { static OBSERVER:RefCell<Option<Observer>>=RefCell::new(None); }
pub(super) fn completed(phase: PinnedShardGcPhase) {
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
fn observe(callback: impl FnMut(PinnedShardGcPhase) + 'static) -> Observe {
    OBSERVER.with(|observer| {
        assert!(observer.borrow().is_none());
        *observer.borrow_mut() = Some(Box::new(callback));
    });
    Observe
}
struct Checks {
    expiry: Cell<Instant>,
    denied: Cell<bool>,
    panic_on_deny: Cell<bool>,
}
impl Checks {
    fn new() -> Self {
        Self {
            expiry: Cell::new(expiry()),
            denied: Cell::new(false),
            panic_on_deny: Cell::new(false),
        }
    }
}
impl ShardDirectoryChecks for Checks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        if self.denied.get() || Instant::now() >= self.expiry.get() {
            Err(ShardDirectoryCheckFailure::Refused)
        } else {
            Ok(())
        }
    }
}
impl PinnedShardGcChecks for Checks {
    fn deny(&self) {
        self.denied.set(true);
        if self.panic_on_deny.get() {
            std::panic::panic_any("actual composing denial panic");
        }
    }
}
impl PinnedStoreOpenChecks for Checks {
    fn deny(&self) {
        <Self as PinnedShardGcChecks>::deny(self);
    }
}
struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "pinned-native-gc-{}-{:020}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual native directory");
        Self(path)
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    store: ShardStore,
    pin: PinnedShardDirectory,
    root: Temporary,
}
fn expiry() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
fn reservation(batch: u128, topic: u128, first: u64) -> Reservation {
    Reservation {
        topic_id: TopicId::new(topic),
        partition_id: LogicalPartitionId::new(0),
        batch_id: BatchId::new(batch),
        first_offset: LogicalOffset::new(first),
        last_offset: LogicalOffset::new(first + 1),
        record_count: NonZeroU32::new(2).expect("real two-record batch"),
        placement: Placement {
            virtual_lane_id: ShardId::new(3),
            ring_epoch: RingEpoch::new(1),
            leader_epoch: LeaderEpoch::new(1),
            sequence: PlacementSequence::new(u64::try_from(batch).expect("fixture sequence")),
        },
    }
}
fn starts() -> HashMap<TopicPartition, LogicalOffset> {
    HashMap::from([(
        TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0)),
        LogicalOffset::new(2),
    )])
}
fn limits() -> PinnedShardGcLimits {
    PinnedShardGcLimits {
        max_path_bytes: 8192,
        max_name_bytes: 4096,
        max_packs: 8,
        max_batch_slots: 64,
        max_log_start_slots: 16,
        max_topic_slots: 64,
        max_location_slots: 64,
        max_plan_packs: 8,
        max_io_calls: 64,
        max_work_bytes: 1 << 24,
    }
}
impl Fixture {
    fn new() -> Self {
        let root = Temporary::new();
        let mut store = ShardStore::open(ShardStoreConfig {
            directory: root.0.clone(),
            shard_id: ShardId::new(3),
            target_pack_bytes: 1 << 20,
            max_batch_bytes: 1024,
        })
        .expect("actual ordinary input store");
        store
            .append(
                reservation(0, 1, 0),
                None,
                b"actual old native extent",
                true,
            )
            .expect("actual native old batch");
        store
            .seal_active()
            .expect("actual physical rotation")
            .expect("old native pack");
        store
            .append(
                reservation(1, 2, 0),
                None,
                b"actual retained native extent",
                true,
            )
            .expect("actual native retained batch");
        // A genuine same-open-file-description clone of the held native lock,
        // not a fabricated lock/descriptor or any claimed Core root owner.
        let guard = ShardRecoveryGuard::try_acquire_until(
            store._lock_file.try_clone().expect("same actual lock FD"),
            root.0.clone(),
            ShardGuardAcquireLimits {
                max_path_bytes: 8192,
                max_io_calls: 5,
            },
            expiry(),
        )
        .expect("actual same native lease");
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
        .expect("actual native descriptor/lease join");
        Self { store, pin, root }
    }
    fn old_path(&self) -> PathBuf {
        pack_path(&self.root.0, 0)
    }
    fn pending_path(&self) -> PathBuf {
        pending_pack_deletion_path(&self.old_path())
    }
    fn collect(&mut self, checks: &Checks) -> Result<PinnedShardGcReport, PinnedShardGcError> {
        self.store
            .garbage_collect_pinned_until(&self.pin, &starts(), limits(), expiry(), checks)
    }
    fn contended(&self) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.root.0.join(".shard-store.lock"))
            .expect("actual competing lock FD");
        assert!(fs2::FileExt::try_lock_exclusive(&file).is_err());
    }
}

#[test]
fn actual_native_gc_retains_results_and_same_physical_store_lock_until_last_drop() {
    fn send<T: Send>() {}
    send::<PinnedShardGcReport>();
    send::<PinnedShardGcError>();
    let mut fixture = Fixture::new();
    let old = fs::read(fixture.old_path()).expect("actual old native bytes");
    let checks = Checks::new();
    let report = fixture
        .collect(&checks)
        .expect("actual bounded descriptor-relative native collection");
    assert!(!checks.denied.get());
    assert_eq!(report.identity_matches(), Some(true));
    assert_eq!(
        report
            .directory_name()
            .expect("actual retained native root name")
            .to_bytes(),
        fixture.root.0.as_os_str().as_bytes()
    );
    assert!(report.name_error().is_none());
    assert_eq!(report.usage().reserved_io_calls, 10);
    assert_eq!(report.usage().entered_io_calls, 10);
    assert_eq!(report.epochs(), (0, 1));
    assert_eq!(
        report.stats(),
        GarbageCollectionStats {
            packs_removed: 1,
            extents_removed: 1,
            bytes_removed: old.len() as u64
        }
    );
    assert_eq!(report.selected_packs(), 1);
    let pack = report.pack(0).expect("actual selected plan");
    assert_eq!(pack.sequence(), 0);
    assert!(matches!(pack.metadata(), Some(Ok(_))));
    assert!(matches!(pack.rename_result(), Some(Ok(()))));
    assert!(matches!(pack.rename_sync_result(), Some(Ok(()))));
    assert_eq!(pack.rename_sync_outcome(), Some(Ok(())));
    assert_eq!(pack.unlink_sync_outcome(), Some(Ok(())));
    assert_eq!(
        pack.native_name()
            .expect("actual retained pack C name")
            .to_bytes(),
        pack.name().as_bytes()
    );
    assert_eq!(
        pack.native_pending_name()
            .expect("actual retained pending C name")
            .to_bytes(),
        pack.pending_name().as_bytes()
    );
    assert!(matches!(pack.removed_path(), Some(Some(_))));
    assert!(matches!(pack.unlink_result(), Some(Ok(()))));
    assert!(matches!(pack.unlink_sync_result(), Some(Ok(()))));
    assert_eq!(report.batch_removals(), (1, 1));
    assert_eq!(
        report.removed_batch_reservation(0),
        Some(reservation(0, 1, 0))
    );
    assert_eq!(report.location_removals(), 1);
    assert_eq!(
        report
            .removed_location(0)
            .expect("actual removed index value")
            .3,
        reservation(0, 1, 0)
    );
    assert!(
        report
            .removed_topic(0)
            .expect("actual removed topic allocation")
            .2
            .is_some()
    );
    assert!(!fixture.old_path().exists() && !fixture.pending_path().exists());
    assert_eq!(
        fixture
            .store
            .catalog()
            .iter()
            .map(|v| v.reservation.batch_id)
            .collect::<Vec<_>>(),
        [BatchId::new(1)]
    );
    fixture.contended();
    let Fixture { store, pin, root } = fixture;
    drop(store);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.0.join(".shard-store.lock"))
        .expect("same actual named lock");
    assert!(fs2::FileExt::try_lock_exclusive(&file).is_err());
    drop(pin);
    fs2::FileExt::try_lock_exclusive(&file).expect("actual last native owner releases lock");
}

#[test]
fn real_native_retention_predicate_matches_ordinary_gc_and_preserves_active_pack() {
    let mut bounded = Fixture::new();
    let empty = HashMap::new();
    let report = bounded
        .store
        .garbage_collect_pinned_until(&bounded.pin, &empty, limits(), expiry(), &Checks::new())
        .expect("missing floor retains real native extent");
    assert_eq!(report.selected_packs(), 0);
    assert_eq!(report.epochs(), (0, 0));
    assert_eq!(report.usage().entered_io_calls, 5);
    let equal = HashMap::from([(
        TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0)),
        LogicalOffset::new(1),
    )]);
    let report = bounded
        .store
        .garbage_collect_pinned_until(&bounded.pin, &equal, limits(), expiry(), &Checks::new())
        .expect("last offset equal to floor stays retained");
    assert_eq!(report.selected_packs(), 0);
    assert!(bounded.old_path().exists());
    let mut ordinary = Fixture::new();
    let expected = ordinary
        .store
        .garbage_collect(&starts())
        .expect("actual unchanged ordinary retention policy");
    let actual = bounded
        .collect(&Checks::new())
        .expect("actual bounded shared native retention");
    assert_eq!(actual.stats(), expected);
    assert_eq!(bounded.store.catalog(), ordinary.store.catalog());
    assert!(bounded.store.active_path().exists());
}

fn phases() -> [PinnedShardGcPhase; 19] {
    [
        PinnedShardGcPhase::DirectoryNamePrepared,
        PinnedShardGcPhase::DirectoryMetadata,
        PinnedShardGcPhase::NamedDirectoryMetadata,
        PinnedShardGcPhase::LockDescriptor,
        PinnedShardGcPhase::NamedLock,
        PinnedShardGcPhase::StoreLock,
        PinnedShardGcPhase::Identity,
        PinnedShardGcPhase::NativeNamesPrepared,
        PinnedShardGcPhase::PlanBuilt,
        PinnedShardGcPhase::EpochAdvanced,
        PinnedShardGcPhase::PackMetadata,
        PinnedShardGcPhase::Rename,
        PinnedShardGcPhase::SyncAfterRename,
        PinnedShardGcPhase::PackIndex,
        PinnedShardGcPhase::BatchIndex,
        PinnedShardGcPhase::LocationIndex,
        PinnedShardGcPhase::EmptyTopicIndex,
        PinnedShardGcPhase::Unlink,
        PinnedShardGcPhase::SyncAfterUnlink,
    ]
}
fn check_refused_effects(
    fixture: &Fixture,
    report: &PinnedShardGcReport,
    target: PinnedShardGcPhase,
    old: &[u8],
) {
    let renamed = matches!(
        target,
        PinnedShardGcPhase::Rename
            | PinnedShardGcPhase::SyncAfterRename
            | PinnedShardGcPhase::PackIndex
            | PinnedShardGcPhase::BatchIndex
            | PinnedShardGcPhase::LocationIndex
            | PinnedShardGcPhase::EmptyTopicIndex
            | PinnedShardGcPhase::Unlink
            | PinnedShardGcPhase::SyncAfterUnlink
    );
    let unlinked = matches!(
        target,
        PinnedShardGcPhase::Unlink | PinnedShardGcPhase::SyncAfterUnlink
    );
    if renamed {
        let pack = report.pack(0).expect("actual entered selected pack");
        assert!(matches!(pack.rename_result(), Some(Ok(()))));
        assert!(matches!(pack.rename_sync_result(), Some(Ok(()))));
        assert!(!fixture.old_path().exists());
        if unlinked {
            assert!(matches!(pack.unlink_result(), Some(Ok(()))));
            assert!(matches!(pack.unlink_sync_result(), Some(Ok(()))));
            assert!(!fixture.pending_path().exists());
            assert_eq!(report.usage().entered_io_calls, 10);
        } else {
            assert!(pack.unlink_result().is_none());
            assert_eq!(
                fs::read(fixture.pending_path()).expect("actual retained pending bytes"),
                old
            );
            assert_eq!(report.usage().entered_io_calls, 8);
        }
    } else {
        assert_eq!(
            fs::read(fixture.old_path()).expect("actual original native bytes retained"),
            old
        );
        assert!(!fixture.pending_path().exists());
        assert!(report.pack(0).is_none_or(|p| p.rename_result().is_none()));
    }
    fixture.contended();
}

#[test]
fn every_completed_actual_phase_keeps_partial_native_effects_after_tighter_e() {
    for target in phases() {
        let mut fixture = Fixture::new();
        let old = fs::read(fixture.old_path()).expect("actual native bytes before collection");
        let checks = Rc::new(Checks::new());
        let shared = Rc::clone(&checks);
        let _observe = observe(move |phase| {
            if phase == target {
                shared.expiry.set(Instant::now());
            }
        });
        let error = fixture
            .collect(&checks)
            .expect_err("actual phase completed beyond same minimum E");
        assert_eq!(error.failure(), PinnedShardGcFailure::Checkpoint);
        assert!(checks.denied.get());
        assert_eq!(
            error.report().checkpoint_failure(),
            Some(ShardDirectoryCheckFailure::Refused)
        );
        assert!(error.report().panic_payload().is_none());
        assert!(error.report().unfinished_phase().is_none());
        check_refused_effects(&fixture, error.report(), target, &old);
        if target == PinnedShardGcPhase::SyncAfterRename {
            assert_eq!(
                error
                    .report()
                    .pack(0)
                    .expect("actual normal rename sync owner")
                    .rename_sync_outcome(),
                Some(Ok(()))
            );
        }
        if target == PinnedShardGcPhase::SyncAfterUnlink {
            assert_eq!(
                error
                    .report()
                    .pack(0)
                    .expect("actual normal unlink sync owner")
                    .unlink_sync_outcome(),
                Some(Ok(()))
            );
        }
        if target == PinnedShardGcPhase::DirectoryNamePrepared {
            assert!(error.report().directory_name().is_some());
            assert_eq!(error.report().usage().entered_io_calls, 0);
        }
        if target == PinnedShardGcPhase::NativeNamesPrepared {
            let pack = error
                .report()
                .pack(0)
                .expect("actual prepared native-name owner");
            assert!(pack.native_name().is_some() && pack.native_pending_name().is_some());
            assert!(pack.metadata().is_none() && !pack.rename_entered());
        }
    }
}

#[test]
fn each_actual_phase_unwind_retains_payload_and_settles_prepaid_namespace_once() {
    for target in phases() {
        let mut fixture = Fixture::new();
        let old = fs::read(fixture.old_path()).expect("actual native bytes before unwind");
        let checks = Checks::new();
        let _observe = observe(move |phase| {
            if phase == target {
                std::panic::panic_any(target);
            }
        });
        let error = fixture
            .collect(&checks)
            .expect_err("actual producer phase unwound");
        assert_eq!(error.failure(), PinnedShardGcFailure::Unwind);
        assert!(checks.denied.get());
        assert_eq!(
            error
                .report()
                .panic_payload()
                .expect("original actual phase payload")
                .downcast_ref::<PinnedShardGcPhase>(),
            Some(&target)
        );
        assert!(error.report().settlement_panic_payload().is_none());
        check_refused_effects(&fixture, error.report(), target, &old);
    }
}

#[test]
fn unknown_pending_pack_is_never_overwritten_and_real_rename_errno_survives_late_check() {
    for late in [false, true] {
        let mut fixture = Fixture::new();
        let old = fs::read(fixture.old_path()).expect("actual original pack bytes");
        fs::write(fixture.pending_path(), b"unknown retained tombstone")
            .expect("actual unrecognized pending file");
        let checks = Rc::new(Checks::new());
        let shared = Rc::clone(&checks);
        let _observe = observe(move |phase| {
            if late && phase == PinnedShardGcPhase::Rename {
                shared.expiry.set(Instant::now());
            }
        });
        let error = fixture
            .collect(&checks)
            .expect_err("real native no-replace refusal");
        assert_eq!(
            error.failure(),
            if late {
                PinnedShardGcFailure::Checkpoint
            } else {
                PinnedShardGcFailure::Io
            }
        );
        let pack = error.report().pack(0).expect("actual selected plan");
        let raw = pack
            .rename_result()
            .expect("actual native rename result")
            .as_ref()
            .expect_err("actual existing pending name");
        assert_eq!(raw.kind(), io::ErrorKind::AlreadyExists);
        assert!(raw.raw_os_error().is_some());
        assert!(matches!(pack.rename_sync_result(), Some(Ok(()))));
        assert!(pack.removed_path().is_none() && pack.unlink_result().is_none());
        assert_eq!(
            fs::read(fixture.old_path()).expect("original untouched"),
            old
        );
        assert_eq!(
            fs::read(fixture.pending_path()).expect("unknown pending untouched"),
            b"unknown retained tombstone"
        );
        assert_eq!(fixture.store.catalog().len(), 2);
        assert!(checks.denied.get());
    }
}

#[test]
fn actual_missing_pack_errno_and_no_follow_single_link_refusals_keep_raw_facts() {
    for kind in 0..3 {
        let mut fixture = Fixture::new();
        let old = fixture.old_path();
        let retained = fixture.root.0.join("retained-native-data");
        match kind {
            0 => fs::remove_file(&old).expect("actual external missing native file"),
            1 => {
                fs::rename(&old, &retained).expect("preserve actual native data");
                symlink("retained-native-data", &old).expect("actual substituted symlink");
            }
            _ => fs::hard_link(&old, &retained).expect("actual second physical name"),
        }
        let checks = Rc::new(Checks::new());
        let shared = Rc::clone(&checks);
        let _observe = observe(move |phase| {
            if kind == 0 && phase == PinnedShardGcPhase::PackMetadata {
                shared.expiry.set(Instant::now());
            }
        });
        let error = fixture
            .collect(&checks)
            .expect_err("actual native missing/symlink/link-count refusal");
        let pack = error.report().pack(0).expect("actual selected plan");
        if kind == 0 {
            assert_eq!(error.failure(), PinnedShardGcFailure::Checkpoint);
            let raw = pack
                .metadata()
                .expect("actual statat result")
                .as_ref()
                .expect_err("actual native missing file");
            assert_eq!(raw.kind(), io::ErrorKind::NotFound);
            assert!(raw.raw_os_error().is_some());
        } else {
            assert_eq!(error.failure(), PinnedShardGcFailure::Identity);
            let actual = pack
                .metadata()
                .expect("actual native stat")
                .as_ref()
                .expect("native observed substitution");
            if kind == 1 {
                assert_eq!(FileType::from_raw_mode(actual.st_mode), FileType::Symlink);
            } else {
                assert_eq!(actual.st_nlink, 2);
            }
            assert!(retained.exists());
        }
        assert!(!pack.rename_entered());
        assert!(!fixture.pending_path().exists());
        assert_eq!(error.report().usage().entered_io_calls, 6);
        assert_eq!(fixture.store.catalog().len(), 2);
        assert!(checks.denied.get());
    }
}

#[test]
fn actual_exact_work_and_calls_fit_and_one_less_retains_real_partial_effects() {
    let mut baseline = Fixture::new();
    let actual = baseline
        .collect(&Checks::new())
        .expect("real reference usage")
        .usage();
    let mut exact = limits();
    exact.max_io_calls = actual.reserved_io_calls;
    exact.max_work_bytes = actual.work_bytes;
    let mut fitting = Fixture::new();
    let report = fitting
        .store
        .garbage_collect_pinned_until(&fitting.pin, &starts(), exact, expiry(), &Checks::new())
        .expect("exact finite actual work and calls");
    assert_eq!(report.usage(), actual);
    for work in [false, true] {
        let mut short = exact;
        if work {
            short.max_work_bytes -= 1;
        } else {
            short.max_io_calls -= 1;
        }
        let mut fixture = Fixture::new();
        let checks = Checks::new();
        let error = fixture
            .store
            .garbage_collect_pinned_until(&fixture.pin, &starts(), short, expiry(), &checks)
            .expect_err("one less actual finite fee");
        assert_eq!(error.failure(), PinnedShardGcFailure::Capacity);
        assert!(checks.denied.get());
        assert!(error.report().usage().reserved_io_calls <= short.max_io_calls);
        assert!(error.report().usage().work_bytes <= short.max_work_bytes);
        if let Some(pack) = error.report().pack(0) {
            if matches!(pack.rename_result(), Some(Ok(()))) {
                assert!(matches!(pack.rename_sync_result(), Some(Ok(()))));
                if pack.unlink_result().is_none() {
                    assert!(fixture.pending_path().exists());
                }
            }
        }
        fixture.contended();
    }
}

#[test]
fn native_container_name_and_plan_caps_refuse_before_any_filesystem_mutation() {
    for kind in 0..8 {
        let mut fixture = Fixture::new();
        let mut cap = limits();
        let floor = starts();
        match kind {
            0 => cap.max_packs = fixture.store.pack_paths.len() - 1,
            1 => cap.max_batch_slots = fixture.store.by_batch.capacity() - 1,
            2 => cap.max_topic_slots = fixture.store.index.capacity() - 1,
            3 => {
                cap.max_location_slots = fixture
                    .store
                    .index
                    .values()
                    .map(Vec::capacity)
                    .sum::<usize>()
                    - 1
            }
            4 => cap.max_log_start_slots = floor.capacity() - 1,
            5 => cap.max_plan_packs = 0,
            6 => {
                cap.max_name_bytes = fixture
                    .pending_path()
                    .file_name()
                    .expect("literal native pending name")
                    .as_bytes()
                    .len()
                    - 1
            }
            _ => {
                // Two real records in a genuinely sparse native HashMap.
                // A fitting entry-count limit cannot pay for all its buckets.
                fixture.store.by_batch.reserve(4096);
                cap.max_batch_slots = fixture.store.by_batch.capacity();
                cap.max_work_bytes = fixture.store.by_batch.capacity() - 1;
            }
        }
        if cap.max_plan_packs > cap.max_packs {
            cap.max_plan_packs = cap.max_packs;
        }
        let checks = Checks::new();
        let error = fixture
            .store
            .garbage_collect_pinned_until(&fixture.pin, &floor, cap, expiry(), &checks)
            .expect_err("real source/container ceiling");
        assert_eq!(error.failure(), PinnedShardGcFailure::Capacity);
        assert!(checks.denied.get());
        assert_eq!(error.report().epochs(), (0, 0));
        assert!(fixture.old_path().exists() && !fixture.pending_path().exists());
        assert!(error.report().pack(0).is_none_or(|p| !p.rename_entered()));
        assert_eq!(fixture.store.catalog().len(), 2);
        fixture.contended();
    }
}

#[test]
fn actual_foreign_pin_or_store_lock_refuses_before_mutation_without_claimed_owner() {
    for foreign_lock in [false, true] {
        let mut fixture = Fixture::new();
        let other = Fixture::new();
        let checks = Checks::new();
        let error = if foreign_lock {
            let foreign = other
                .store
                ._lock_file
                .try_clone()
                .expect("actual foreign lock descriptor");
            let original = std::mem::replace(&mut fixture.store._lock_file, foreign);
            let result = fixture.collect(&checks);
            fixture.store._lock_file = original;
            result
        } else {
            fixture.store.garbage_collect_pinned_until(
                &other.pin,
                &starts(),
                limits(),
                expiry(),
                &checks,
            )
        }
        .expect_err("actual native identity mismatch");
        assert_eq!(error.failure(), PinnedShardGcFailure::Identity);
        assert_eq!(
            error.report().usage().entered_io_calls,
            if foreign_lock { 5 } else { 0 }
        );
        if foreign_lock {
            assert_eq!(error.report().identity_matches(), Some(false));
        }
        assert_eq!(error.report().epochs(), (0, 0));
        assert!(fixture.old_path().exists() && !fixture.pending_path().exists());
        fixture.contended();
        other.contended();
        assert!(checks.denied.get());
    }
}

#[test]
fn original_expiry_enters_nothing_and_actual_denial_panic_is_retained_before_settlement() {
    let mut fixture = Fixture::new();
    let checks = Checks::new();
    let error = fixture
        .store
        .garbage_collect_pinned_until(&fixture.pin, &starts(), limits(), Instant::now(), &checks)
        .expect_err("original E already elapsed");
    assert_eq!(error.failure(), PinnedShardGcFailure::Expired);
    assert!(checks.denied.get());
    assert_eq!(error.report().usage(), PinnedShardGcUsage::default());
    assert!(error.report().directory_metadata().is_none());
    assert!(fixture.old_path().exists() && !fixture.pending_path().exists());
    let checks = Rc::new(Checks::new());
    checks.panic_on_deny.set(true);
    let shared = Rc::clone(&checks);
    let _observe = observe(move |phase| {
        if phase == PinnedShardGcPhase::Rename {
            shared.expiry.set(Instant::now());
        }
    });
    let error = fixture
        .collect(&checks)
        .expect_err("actual late rename and actual denial unwind");
    assert_eq!(error.failure(), PinnedShardGcFailure::Checkpoint);
    assert!(checks.denied.get());
    assert_eq!(
        error
            .report()
            .denial_panic_payload()
            .expect("actual distinct denial panic")
            .downcast_ref::<&'static str>(),
        Some(&"actual composing denial panic")
    );
    assert!(error.report().panic_payload().is_none());
    let pack = error.report().pack(0).expect("actual entered native plan");
    assert!(matches!(pack.rename_result(), Some(Ok(()))));
    assert!(matches!(pack.rename_sync_result(), Some(Ok(()))));
    assert!(pack.unlink_result().is_none());
    assert_eq!(error.report().usage().entered_io_calls, 8);
    fixture.contended();
}

#[test]
fn actual_bounded_native_opener_and_collector_keep_the_same_scanned_store_and_pin() {
    let Fixture { store, pin, root } = Fixture::new();
    let config = store.config.clone();
    let old = fs::read(pack_path(&root.0, 0)).expect("actual native data before bounded reopen");
    drop(store);
    let open_limits = PinnedStoreOpenLimits {
        max_directory_visits: 64,
        directory_buffer_bytes: 4096,
        max_name_bytes: 255,
        max_path_bytes: 8192,
        max_packs: 8,
        max_frames: 16,
        max_frame_bytes: 4096,
        max_read_attempts: 256,
        max_read_bytes: 1 << 20,
        max_index_entries: 64,
        max_repair_calls: 64,
        max_io_calls: 256,
        max_outcomes: 512,
        max_entries: 8192,
        max_declared_bytes: 1 << 22,
        max_work_bytes: 1 << 26,
    };
    let checks = Checks::new();
    let original_e = expiry();
    let mut opened = ShardStore::open_pinned_until(config, &pin, open_limits, original_e, &checks)
        .expect("actual bounded native discovery/frame decode/index assembly");
    assert_eq!(opened.store().catalog().len(), 2);
    let report = opened
        .store_mut()
        .garbage_collect_pinned_until(&pin, &starts(), limits(), original_e, &checks)
        .expect("actual same opened store and native pin collection");
    assert_eq!(report.absolute_expiry(), original_e);
    assert_eq!(
        report.stats(),
        GarbageCollectionStats {
            packs_removed: 1,
            extents_removed: 1,
            bytes_removed: old.len() as u64
        }
    );
    assert_eq!(
        opened
            .store()
            .catalog()
            .iter()
            .map(|v| v.reservation.batch_id)
            .collect::<Vec<_>>(),
        [BatchId::new(1)]
    );
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.0.join(".shard-store.lock"))
        .expect("actual competing native lock");
    assert!(fs2::FileExt::try_lock_exclusive(&file).is_err());
    drop(opened);
    assert!(fs2::FileExt::try_lock_exclusive(&file).is_err());
    drop(pin);
    fs2::FileExt::try_lock_exclusive(&file)
        .expect("actual pin stayed last through opened store cleanup");
}

#[test]
fn real_leading_zero_pack_names_preserve_native_acceptance_and_preparse_limits() {
    for short_name_limit in [true, false] {
        let Fixture { store, pin, root } = Fixture::new();
        let config = store.config.clone();
        let old = fs::read(pack_path(&root.0, 0)).expect("actual native original bytes");
        drop(store);
        // The ordinary native decimal parser accepts leading zeros. Rebuild
        // the actual map through directory discovery, not private map edits.
        let name = OsString::from(format!("pack-{}.sse", "0".repeat(128)));
        let actual_path = root.0.join(&name);
        fs::rename(pack_path(&root.0, 0), &actual_path)
            .expect("actual accepted noncanonical native spelling");
        let open_limits = PinnedStoreOpenLimits {
            max_directory_visits: 64,
            directory_buffer_bytes: 4096,
            max_name_bytes: 255,
            max_path_bytes: 8192,
            max_packs: 8,
            max_frames: 16,
            max_frame_bytes: 4096,
            max_read_attempts: 256,
            max_read_bytes: 1 << 20,
            max_index_entries: 64,
            max_repair_calls: 64,
            max_io_calls: 256,
            max_outcomes: 512,
            max_entries: 8192,
            max_declared_bytes: 1 << 22,
            max_work_bytes: 1 << 26,
        };
        let original_e = expiry();
        let checks = Checks::new();
        let mut opened =
            ShardStore::open_pinned_until(config, &pin, open_limits, original_e, &checks)
                .expect("real native opener accepts the shared leading-zero spelling");
        assert_eq!(opened.store().catalog().len(), 2);
        let mut cap = limits();
        cap.max_path_bytes = root.0.as_os_str().len();
        cap.max_name_bytes = if short_name_limit { 64 } else { 192 };
        let result = opened.store_mut().garbage_collect_pinned_until(
            &pin,
            &starts(),
            cap,
            original_e,
            &checks,
        );
        if short_name_limit {
            let error =
                result.expect_err("raw actual path exceeds this invocation's complete ceiling");
            assert_eq!(error.failure(), PinnedShardGcFailure::Capacity);
            assert_eq!(error.report().epochs(), (0, 0));
            assert_eq!(error.report().usage().entered_io_calls, 5);
            assert_eq!(error.report().selected_packs(), 0);
            assert_eq!(
                fs::read(&actual_path).expect("actual long-named native data retained"),
                old
            );
            assert_eq!(opened.store().catalog().len(), 2);
            assert!(checks.denied.get());
        } else {
            let report =
                result.expect("same native spelling fits an admitted literal name ceiling");
            assert_eq!(
                report
                    .pack(0)
                    .expect("actual selected noncanonical pack")
                    .name(),
                name.as_os_str()
            );
            assert_eq!(
                report.stats(),
                GarbageCollectionStats {
                    packs_removed: 1,
                    extents_removed: 1,
                    bytes_removed: old.len() as u64
                }
            );
            assert!(!actual_path.exists());
            assert_eq!(opened.store().catalog().len(), 1);
            assert!(!checks.denied.get());
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.0.join(".shard-store.lock"))
            .expect("actual competing native lease");
        drop(opened);
        assert!(fs2::FileExt::try_lock_exclusive(&file).is_err());
        drop(pin);
        fs2::FileExt::try_lock_exclusive(&file)
            .expect("same real pin retained lease through actual native cleanup");
    }
}
