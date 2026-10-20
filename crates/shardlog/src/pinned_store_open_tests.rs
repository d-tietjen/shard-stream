//! Actual writer/FD/lock/decoder/repair regressions; source-only until executed.

use super::*;
use std::cell::{Cell, RefCell};
use std::num::NonZeroU32;
use std::os::unix::fs::symlink;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MaterializationPhase {
    NameReserved,
    NameCopied,
    PathReserved,
    PathCopied,
    PackPathReserved,
    PackPathCopied,
    CurrentPathScanPaid,
    ReplacedPath,
}
type MaterializationObserver = Box<dyn FnMut(MaterializationPhase)>;
thread_local! {static MATERIALIZATION:RefCell<Option<MaterializationObserver>>=RefCell::new(None);}
pub(super) fn materialized(phase: MaterializationPhase) {
    MATERIALIZATION.with(|observer| {
        if let Some(observer) = &mut *observer.borrow_mut() {
            observer(phase);
        }
    });
}
struct MaterializationGuard;
impl Drop for MaterializationGuard {
    fn drop(&mut self) {
        MATERIALIZATION.with(|observer| *observer.borrow_mut() = None);
    }
}
fn observe_materialization(
    callback: impl FnMut(MaterializationPhase) + 'static,
) -> MaterializationGuard {
    MATERIALIZATION.with(|observer| {
        assert!(observer.borrow().is_none());
        *observer.borrow_mut() = Some(Box::new(callback));
    });
    MaterializationGuard
}
struct MaterializationChecks {
    armed: Rc<Cell<bool>>,
    denied: Cell<bool>,
    expiry: Instant,
    panic: bool,
}
impl ShardDirectoryChecks for MaterializationChecks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        if self.armed.replace(false) {
            if self.panic {
                std::panic::panic_any(io::Error::other("actual materialization checkpoint unwind"));
            }
            elapsed(self.expiry);
        }
        if self.denied.get() {
            Err(ShardDirectoryCheckFailure::Refused)
        } else {
            Ok(())
        }
    }
}
impl PinnedStoreOpenChecks for MaterializationChecks {
    fn deny(&self) {
        self.denied.set(true);
    }
}

struct Checks {
    denied: Cell<bool>,
    panic_on_deny: Cell<bool>,
}
impl Checks {
    fn new() -> Self {
        Self {
            denied: Cell::new(false),
            panic_on_deny: Cell::new(false),
        }
    }
}
impl ShardDirectoryChecks for Checks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        if self.denied.get() {
            Err(ShardDirectoryCheckFailure::Refused)
        } else {
            Ok(())
        }
    }
}
impl PinnedStoreOpenChecks for Checks {
    fn deny(&self) {
        self.denied.set(true);
        if self.panic_on_deny.get() {
            std::panic::panic_any("actual composing deny unwind");
        }
    }
}
struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "pinned-native-open-{}-{:020}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual native directory");
        Self(path)
    }
    fn lock(&self) -> File {
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.0.join(".shard-store.lock"))
            .expect("actual native lock FD")
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
    pin: PinnedShardDirectory,
    root: Root,
    config: ShardStoreConfig,
}
fn expiry() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
fn limits() -> PinnedStoreOpenLimits {
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
        max_declared_bytes: 1 << 25,
        max_work_bytes: 1 << 27,
    }
}
fn reservation(batch: u128) -> Reservation {
    Reservation {
        topic_id: TopicId::new(19),
        partition_id: LogicalPartitionId::new(2),
        batch_id: BatchId::new(batch),
        first_offset: LogicalOffset::new(batch as u64),
        last_offset: LogicalOffset::new(batch as u64),
        record_count: NonZeroU32::new(1).expect("one actual record"),
        placement: Placement {
            virtual_lane_id: VirtualLaneId::new(3),
            ring_epoch: RingEpoch::new(1),
            leader_epoch: LeaderEpoch::new(7),
            sequence: PlacementSequence::new(batch as u64),
        },
    }
}
impl Fixture {
    fn new(batches: usize, separate_packs: bool) -> Self {
        let root = Root::new();
        let config = ShardStoreConfig {
            directory: root.0.clone(),
            shard_id: ShardId::new(3),
            target_pack_bytes: 1 << 20,
            max_batch_bytes: 4096,
        };
        if batches > 0 {
            let mut writer =
                ShardStore::open(config.clone()).expect("actual ordinary native writer");
            for n in 0..batches {
                writer
                    .append(
                        reservation(n as u128),
                        None,
                        b"actual complete native writer body",
                        true,
                    )
                    .expect("actual native append and sync");
                if separate_packs && n + 1 < batches {
                    writer
                        .seal_active()
                        .expect("actual rotation")
                        .expect("sealed real nonempty pack");
                }
            }
            drop(writer);
        }
        let acquired = ShardRecoveryGuard::try_acquire_until(
            root.lock(),
            root.0.clone(),
            ShardGuardAcquireLimits {
                max_path_bytes: 8192,
                max_io_calls: 5,
            },
            expiry(),
        )
        .expect("actual native lease acquired");
        let pin = PinnedShardDirectory::from_descriptor_until(
            File::open(&root.0).expect("actual directory FD"),
            acquired,
            &root.0,
            PinnedShardDirectoryLimits {
                max_path_bytes: 8192,
                max_io_calls: 4,
            },
            expiry(),
            &Checks::new(),
        )
        .expect("actual same descriptor/guard identity join");
        Self { pin, root, config }
    }
    #[allow(
        clippy::result_large_err,
        reason = "retain actual native open partials and original inputs in the fixture without boxing"
    )]
    fn open(&self, checks: &Checks) -> Result<OpenedPinnedShardStore, PinnedStoreOpenError> {
        ShardStore::open_pinned_until(self.config.clone(), &self.pin, limits(), expiry(), checks)
    }
    fn path(&self, sequence: u64) -> PathBuf {
        pack_path(&self.root.0, sequence)
    }
    fn append_tail(&self, sequence: u64) -> u64 {
        let path = self.path(sequence);
        let before = fs::metadata(&path).expect("real pack").len();
        OpenOptions::new()
            .append(true)
            .open(path)
            .expect("actual tail FD")
            .write_all(b"short")
            .expect("actual physical incomplete tail");
        before
    }
}
fn unit_count(outcomes: &[PinnedStoreOpenOutcome], selected: PinnedStoreOpenPhase) -> usize {
    outcomes.iter().filter(|row|matches!(row,PinnedStoreOpenOutcome::Unit {phase,result:Ok(() )} if *phase==selected)).count()
}
fn elapsed(original: Instant) {
    while Instant::now() < original {
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn complete_native_packs_rebuild_actual_indexes_and_retain_same_physical_lease() {
    fn send<T: Send>() {}
    send::<OpenedPinnedShardStore>();
    send::<PinnedStoreOpenError>();
    let fixture = Fixture::new(3, true);
    let before = (0..3)
        .map(|n| fs::read(fixture.path(n)).expect("actual original pack bytes"))
        .collect::<Vec<_>>();
    let checks = Checks::new();
    let mut opened = fixture
        .open(&checks)
        .expect("actual descriptor-relative complete open");
    assert_eq!(opened.store().recovery_catalog_len(), 3);
    assert_eq!(opened.usage().frames, 3);
    assert!(!checks.denied.get());
    for n in 0..3 {
        let read = opened
            .store_mut()
            .read_batch(&reservation(n))
            .expect("ordinary existing reader semantic comparison")
            .expect("same actual native indexed batch");
        assert_eq!(read.payload.as_ref(), b"actual complete native writer body");
        assert_eq!(
            fs::read(fixture.path(n as u64)).expect("source retained"),
            before[n as usize]
        );
    }
    fixture.root.contended();
    let Fixture {
        pin,
        root,
        config: _,
    } = fixture;
    drop(pin);
    root.contended();
    drop(opened);
    fs2::FileExt::try_lock_exclusive(&root.lock())
        .expect("last actual open result released native lease");
}

#[test]
fn actual_empty_directory_creates_only_native_initial_pack_and_syncs_parent() {
    let fixture = Fixture::new(0, false);
    let checks = Checks::new();
    let opened = fixture
        .open(&checks)
        .expect("actual empty native initialization");
    assert_eq!(opened.store().recovery_catalog_len(), 0);
    assert!(fixture.path(0).is_file());
    assert_eq!(
        fs::metadata(fixture.path(0))
            .expect("actual initial pack")
            .len(),
        0
    );
    assert_eq!(
        unit_count(opened.outcomes(), PinnedStoreOpenPhase::ParentSynced),
        1
    );
    assert_eq!(opened.usage().packs, 1);
}

#[test]
fn native_pending_cleanup_uses_real_entry_and_prepaid_parent_sync() {
    let fixture = Fixture::new(1, false);
    let pending = fixture
        .root
        .0
        .join("pack-00000000000000000999.sse.gc-pending");
    fs::copy(fixture.path(0), &pending).expect("real pending pack bytes");
    let opened = fixture
        .open(&Checks::new())
        .expect("actual native pending role cleanup");
    assert!(!pending.exists());
    assert_eq!(
        unit_count(opened.outcomes(), PinnedStoreOpenPhase::PendingUnlinked),
        1
    );
    assert_eq!(
        unit_count(opened.outcomes(), PinnedStoreOpenPhase::ParentSynced),
        1
    );
    assert_eq!(opened.store().recovery_catalog_len(), 1);
}

#[test]
fn actual_active_incomplete_tail_repairs_once_with_native_data_sync() {
    for complete_header in [false, true] {
        let fixture = Fixture::new(3, false);
        let before = if complete_header {
            let path = fixture.path(0);
            let bytes = fs::read(&path).expect("actual native frames");
            let before = bytes.len() as u64;
            OpenOptions::new()
                .append(true)
                .open(path)
                .expect("actual tail FD")
                .write_all(&bytes[..crate::RECOVERY_EXTENT_HEADER_BYTES])
                .expect("real incomplete native body");
            before
        } else {
            fixture.append_tail(0)
        };
        let opened = fixture
            .open(&Checks::new())
            .expect("actual active tail repair");
        assert_eq!(opened.store().recovery_catalog_len(), 3);
        assert_eq!(
            fs::metadata(fixture.path(0))
                .expect("actual repaired pack")
                .len(),
            before
        );
        assert_eq!(
            unit_count(opened.outcomes(), PinnedStoreOpenPhase::TailTruncated),
            1
        );
        assert_eq!(
            unit_count(opened.outcomes(), PinnedStoreOpenPhase::TailSynced),
            1
        );
    }
}

#[test]
fn sealed_tail_and_native_crc_errors_refuse_without_repair_or_source_change() {
    for sealed in [true, false] {
        let fixture = Fixture::new(2, sealed);
        if sealed {
            fixture.append_tail(0);
        } else {
            let path = fixture.path(0);
            let mut actual = fs::read(&path).expect("actual native frames");
            let last = actual.last_mut().expect("genuine native checksum byte");
            *last ^= 1;
            fs::write(path, actual).expect("actual physical checksum corruption fixture");
        }
        let before = fs::read(fixture.path(0)).expect("actual original corrupt input");
        let checks = Checks::new();
        let error = fixture
            .open(&checks)
            .expect_err("native decoder/sealed policy refusal");
        assert_eq!(error.failure(), PinnedStoreOpenFailure::Native);
        assert!(checks.denied.get());
        assert_eq!(
            unit_count(error.outcomes(), PinnedStoreOpenPhase::TailTruncated),
            0
        );
        assert_eq!(
            fs::read(fixture.path(0)).expect("actual source unchanged"),
            before
        );
    }
}

#[test]
fn duplicate_actual_native_batch_uses_existing_overwrite_then_refusal_algorithm() {
    let fixture = Fixture::new(1, false);
    let path = fixture.path(0);
    let original = fs::read(&path).expect("actual single native frame");
    OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("actual native pack FD")
        .write_all(&original)
        .expect("actual repeated native frame fixture");
    let error = fixture
        .open(&Checks::new())
        .expect_err("actual native duplicate policy");
    assert_eq!(error.failure(), PinnedStoreOpenFailure::Native);
    assert_eq!(error.partial_index_entries(), (1, 1));
    assert!(error.outcomes().iter().any(|row|matches!(row,PinnedStoreOpenOutcome::Indexed {result:Err(StorageError::Corrupt {reason,..}),..} if reason=="duplicate batch ID")));
}

#[test]
fn real_prepaid_native_calls_fit_exactly_and_one_less_refuses_without_new_phase() {
    let fixture = Fixture::new(3, true);
    let baseline = fixture
        .open(&Checks::new())
        .expect("measure actual finite native calls");
    let calls = baseline.usage().reserved_io_calls;
    drop(baseline);
    let mut exact = limits();
    exact.max_io_calls = calls;
    let opened = ShardStore::open_pinned_until(
        fixture.config.clone(),
        &fixture.pin,
        exact,
        expiry(),
        &Checks::new(),
    )
    .expect("actual exact native call limit");
    assert_eq!(opened.usage().entered_io_calls, calls);
    drop(opened);
    exact.max_io_calls = calls - 1;
    let checks = Checks::new();
    let error = ShardStore::open_pinned_until(
        fixture.config.clone(),
        &fixture.pin,
        exact,
        expiry(),
        &checks,
    )
    .expect_err("one less actual native call");
    assert_eq!(error.failure(), PinnedStoreOpenFailure::Capacity);
    assert!(checks.denied.get());
    assert!(error.usage().entered_io_calls < calls);
    assert!(error.usage().reserved_io_calls < calls);
    assert!(error.scope.store.is_none());
}

#[test]
fn native_frame_and_raw_directory_visit_limits_refuse_before_unbounded_materialization() {
    for frame_limit in [true, false] {
        let fixture = Fixture::new(3, true);
        let mut cap = limits();
        if frame_limit {
            cap.max_frames = 2;
        } else {
            cap.max_directory_visits = 1;
        }
        let error = ShardStore::open_pinned_until(
            fixture.config.clone(),
            &fixture.pin,
            cap,
            expiry(),
            &Checks::new(),
        )
        .expect_err("actual finite frame/directory admission");
        assert_eq!(error.failure(), PinnedStoreOpenFailure::Capacity);
        if frame_limit {
            assert_eq!(error.usage().frames, 2);
        } else {
            assert_eq!(error.usage().directory_visits, 1);
        }
        assert!(error.scope.store.is_none());
        fixture.root.contended();
    }
}

#[test]
fn expired_or_invalid_entry_refuses_before_any_new_native_io_and_keeps_actual_owner() {
    let fixture = Fixture::new(1, false);
    for variant in 0..4 {
        let mut cap = limits();
        let mut config = fixture.config.clone();
        let mut original = expiry();
        match variant {
            0 => original = Instant::now(),
            1 => cap.max_io_calls = 0,
            2 => cap.max_outcomes = usize::MAX,
            _ => config.target_pack_bytes = 0,
        }
        let checks = Checks::new();
        let error = ShardStore::open_pinned_until(config, &fixture.pin, cap, original, &checks)
            .expect_err("actual entry expiry/configuration/cost refusal");
        assert_eq!(error.usage().entered_io_calls, 0);
        assert_eq!(error.retained_file_slots(), 0);
        assert!(checks.denied.get());
        fixture.root.contended();
        if variant == 3 {
            assert!(error.outcomes().iter().any(|row| matches!(
                row,
                PinnedStoreOpenOutcome::Configuration {
                    result: Err(StorageError::InvalidInput(_))
                }
            )));
        }
    }
}

#[test]
fn original_expiry_after_real_phases_retains_outcomes_and_stops_all_later_entry() {
    for selected in [
        PinnedStoreOpenPhase::LeaseCloned,
        PinnedStoreOpenPhase::DirectoryVisit,
        PinnedStoreOpenPhase::PrefixRead,
        PinnedStoreOpenPhase::FrameDecoded,
        PinnedStoreOpenPhase::Indexed,
        PinnedStoreOpenPhase::Assembled,
    ] {
        let fixture = Fixture::new(1, false);
        let checks = Checks::new();
        let original = Instant::now() + Duration::from_millis(150);
        let mut observed = Vec::new();
        let mut crossed = false;
        let error = ShardStore::open_pinned_observed(
            fixture.config.clone(),
            &fixture.pin,
            limits(),
            original,
            &checks,
            |phase| {
                observed.push(phase);
                if phase == selected && !crossed {
                    crossed = true;
                    elapsed(original);
                }
            },
        )
        .expect_err("same original E elapsed after actual completed phase");
        assert!(crossed);
        assert_eq!(observed.last(), Some(&selected));
        assert_eq!(error.failure(), PinnedStoreOpenFailure::Expired);
        assert!(checks.denied.get());
        assert!(error.denial_completed());
        fixture.root.contended();
        assert!(
            error
                .scope
                .files
                .iter()
                .any(|slot| matches!(&slot.result, Some(Ok(_))))
                || error.scope.store.is_some()
        );
    }
}

#[test]
fn entered_native_mutation_unwind_or_late_result_settles_prepaid_sync_once() {
    for selected in [
        PinnedStoreOpenPhase::TailTruncated,
        PinnedStoreOpenPhase::InitialPackCreated,
        PinnedStoreOpenPhase::PendingUnlinked,
    ] {
        for panic in [false, true] {
            let fixture = Fixture::new(
                usize::from(selected != PinnedStoreOpenPhase::InitialPackCreated),
                false,
            );
            if selected == PinnedStoreOpenPhase::TailTruncated {
                fixture.append_tail(0);
            }
            if selected == PinnedStoreOpenPhase::PendingUnlinked {
                fs::copy(
                    fixture.path(0),
                    fixture
                        .root
                        .0
                        .join("pack-00000000000000000999.sse.gc-pending"),
                )
                .expect("actual native pending entry");
            }
            let checks = Checks::new();
            checks.panic_on_deny.set(panic);
            let original = Instant::now() + Duration::from_millis(150);
            let mut crossed = false;
            let error = ShardStore::open_pinned_observed(
                fixture.config.clone(),
                &fixture.pin,
                limits(),
                original,
                &checks,
                |phase| {
                    if phase == selected {
                        crossed = true;
                        if panic {
                            std::panic::panic_any(io::Error::other(
                                "actual completed mutation unwind",
                            ));
                        }
                        elapsed(original);
                    }
                },
            )
            .expect_err("actual entered mutation needs prepaid physical settlement");
            assert!(crossed);
            assert!(checks.denied.get());
            if panic {
                assert_eq!(error.failure(), PinnedStoreOpenFailure::Unwind);
                assert!(
                    error
                        .panic_payload()
                        .and_then(|v| v.downcast_ref::<io::Error>())
                        .is_some()
                );
                assert!(error.deny_panic_payload().is_some());
                assert!(!error.denial_completed());
            } else {
                assert_eq!(error.failure(), PinnedStoreOpenFailure::Expired);
            }
            let sync = if selected == PinnedStoreOpenPhase::TailTruncated {
                PinnedStoreOpenPhase::TailSynced
            } else {
                PinnedStoreOpenPhase::ParentSynced
            };
            assert_eq!(unit_count(error.outcomes(), sync), 1);
            if selected == PinnedStoreOpenPhase::TailTruncated {
                assert!(matches!(error.tail_settlement(), Some(Ok(()))));
            } else {
                assert!(matches!(error.parent_settlement(), Some(Ok(()))));
            }
            assert_eq!(
                error.usage().reserved_io_calls,
                error.usage().entered_io_calls
            );
            fixture.root.contended();
        }
    }
}

#[test]
fn real_nofollow_pack_failure_finishing_late_retains_its_actual_notfound_error() {
    let fixture = Fixture::new(1, false);
    let checks = Checks::new();
    let original = Instant::now() + Duration::from_millis(150);
    let mut removed = false;
    let error = ShardStore::open_pinned_observed(
        fixture.config.clone(),
        &fixture.pin,
        limits(),
        original,
        &checks,
        |phase| {
            if phase == PinnedStoreOpenPhase::DirectoryEnumerated && !removed {
                fs::remove_file(fixture.path(0))
                    .expect("actual pack disappearance after native directory refill");
                removed = true;
            }
            if phase == PinnedStoreOpenPhase::FileOpened {
                elapsed(original);
            }
        },
    )
    .expect_err("real named pack open completed with NotFound after same E");
    assert_eq!(error.failure(), PinnedStoreOpenFailure::Expired);
    assert!(
        error
            .scope
            .files
            .iter()
            .filter_map(|s| s.result.as_ref())
            .any(|r| matches!(r,Err(e) if e.kind()==io::ErrorKind::NotFound))
    );
    let Fixture {
        pin,
        root,
        config: _,
    } = fixture;
    drop(pin);
    root.contended();
    drop(error);
    fs2::FileExt::try_lock_exclusive(&root.lock())
        .expect("last retained real error released native lease");
}

#[test]
fn actual_symlink_substitution_is_rejected_without_opening_the_other_file() {
    let fixture = Fixture::new(1, false);
    let other = Root::new();
    let other_path = other.0.join("unrelated");
    fs::write(&other_path, b"actual unrelated file remains untouched").expect("real other input");
    let mut changed = false;
    let error = ShardStore::open_pinned_observed(
        fixture.config.clone(),
        &fixture.pin,
        limits(),
        expiry(),
        &Checks::new(),
        |phase| {
            if phase == PinnedStoreOpenPhase::DirectoryEnumerated && !changed {
                fs::rename(fixture.path(0), fixture.root.0.join("retained-original"))
                    .expect("actual source inode retained under another name");
                symlink(&other_path, fixture.path(0)).expect("actual late leaf symlink");
                changed = true;
            }
        },
    )
    .expect_err("actual native nofollow refuses alias substitution");
    assert!(changed);
    assert_eq!(error.failure(), PinnedStoreOpenFailure::Io);
    assert_eq!(
        fs::read(&other_path).expect("actual other source"),
        b"actual unrelated file remains untouched"
    );
    fixture.root.contended();
}

#[test]
fn actual_name_path_and_replaced_values_survive_late_or_unwinding_scoped_checks() {
    for selected in [
        MaterializationPhase::NameReserved,
        MaterializationPhase::NameCopied,
        MaterializationPhase::PathReserved,
        MaterializationPhase::PathCopied,
        MaterializationPhase::PackPathReserved,
        MaterializationPhase::PackPathCopied,
        MaterializationPhase::CurrentPathScanPaid,
        MaterializationPhase::ReplacedPath,
    ] {
        for panic in [false, true] {
            let fixture = Fixture::new(1, false);
            if selected == MaterializationPhase::ReplacedPath {
                fs::copy(fixture.path(0), fixture.root.0.join("pack-0.sse"))
                    .expect("real second accepted native spelling of same sequence");
            }
            let original = Instant::now() + Duration::from_millis(150);
            let armed = Rc::new(Cell::new(false));
            let observed = Rc::new(Cell::new(false));
            let checks = MaterializationChecks {
                armed: Rc::clone(&armed),
                denied: Cell::new(false),
                expiry: original,
                panic,
            };
            let captured = Rc::clone(&observed);
            let _observer = observe_materialization(move |phase| {
                if phase == selected && !captured.replace(true) {
                    armed.set(true);
                }
            });
            let error = ShardStore::open_pinned_until(
                fixture.config.clone(),
                &fixture.pin,
                limits(),
                original,
                &checks,
            )
            .expect_err("actual scoped check after retained materialization must refuse");
            assert!(observed.get());
            assert!(checks.denied.get());
            assert_eq!(
                error.failure(),
                if panic {
                    PinnedStoreOpenFailure::Unwind
                } else {
                    PinnedStoreOpenFailure::Expired
                }
            );
            match selected {
                MaterializationPhase::NameReserved | MaterializationPhase::NameCopied => {
                    let name = error
                        .scope
                        .current_name
                        .as_ref()
                        .expect("actual outside-catch name");
                    assert!(name.capacity() > 0);
                    if selected == MaterializationPhase::NameCopied {
                        assert!(!name.is_empty());
                    }
                }
                MaterializationPhase::ReplacedPath => {
                    let replaced = error
                        .replaced_path()
                        .expect("actual replaced native map path retained");
                    assert!(replaced.is_file());
                    assert_ne!(
                        Some(replaced),
                        error.scope.pack_paths.get(&0).map(PathBuf::as_path)
                    );
                }
                _ => {
                    let path = error
                        .scope
                        .current_path
                        .as_ref()
                        .expect("actual outside-catch path");
                    assert!(path.capacity() > 0);
                    if matches!(
                        selected,
                        MaterializationPhase::PathCopied
                            | MaterializationPhase::PackPathCopied
                            | MaterializationPhase::CurrentPathScanPaid
                    ) {
                        assert!(!path.as_os_str().is_empty());
                    }
                    if selected == MaterializationPhase::CurrentPathScanPaid {
                        assert!(error.usage().work_bytes >= (path.as_os_str().len() + 1) * 4);
                    }
                }
            }
            if panic {
                assert!(
                    error
                        .panic_payload()
                        .and_then(|v| v.downcast_ref::<io::Error>())
                        .is_some()
                );
            }
            fixture.root.contended();
        }
    }
}

#[test]
fn real_postscan_active_name_replacement_refuses_and_keeps_actual_scanned_inode() {
    let fixture = Fixture::new(1, false);
    let other = Fixture::new(2, false);
    let original_inode = fs::metadata(fixture.path(0))
        .expect("actual original active inode")
        .ino();
    let retained = fixture.root.0.join("scanned-original");
    let mut replaced = false;
    let error = ShardStore::open_pinned_observed(
        fixture.config.clone(),
        &fixture.pin,
        limits(),
        expiry(),
        &Checks::new(),
        |phase| {
            if phase == PinnedStoreOpenPhase::IndexSorted && !replaced {
                fs::rename(fixture.path(0), &retained)
                    .expect("retain actual scanned inode under real name");
                fs::copy(other.path(0), fixture.path(0))
                    .expect("real different writer-generated native pack inode");
                replaced = true;
            }
        },
    )
    .expect_err("actual selected scanned FD cannot silently adopt replacement inode");
    assert!(replaced);
    assert_eq!(error.failure(), PinnedStoreOpenFailure::Identity);
    assert_eq!(error.partial_index_entries(), (1, 1));
    let active = error
        .scope
        .active_slot
        .expect("actual scanned active FD slot");
    assert_eq!(
        error
            .scope
            .file(active)
            .expect("actual retained scanned FD")
            .metadata()
            .expect("actual test observation")
            .ino(),
        original_inode
    );
    assert_eq!(
        fs::metadata(retained)
            .expect("actual retained original inode")
            .ino(),
        original_inode
    );
    assert_ne!(
        fs::metadata(fixture.path(0))
            .expect("real replacement inode")
            .ino(),
        original_inode
    );
    fixture.root.contended();
}
