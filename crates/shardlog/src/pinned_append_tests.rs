//! Genuine files, native frames, same-OFD leases and partial durability effects.
//!
//! Ordinary open builds actual inputs. These source-only tests do not qualify
//! bounded store opening, Core composition, ShardLog or full owned startup.

use super::*;
use std::cell::{Cell, RefCell};
use std::num::NonZeroU32;
use std::os::unix::fs::symlink;
use std::rc::Rc;

type Observer = Box<dyn FnMut(PinnedShardAppendPhase)>;
thread_local! {static OBSERVER:RefCell<Option<Observer>>=RefCell::new(None);}
pub(super) fn completed(p: PinnedShardAppendPhase) {
    OBSERVER.with(|v| {
        if let Some(callback) = &mut *v.borrow_mut() {
            callback(p);
        }
    });
}
struct Observe;
impl Drop for Observe {
    fn drop(&mut self) {
        OBSERVER.with(|v| *v.borrow_mut() = None);
    }
}
fn observe(callback: impl FnMut(PinnedShardAppendPhase) + 'static) -> Observe {
    OBSERVER.with(|v| {
        assert!(v.borrow().is_none());
        *v.borrow_mut() = Some(Box::new(callback));
    });
    Observe
}
struct Checks {
    denied: Cell<bool>,
    panic_deny: Cell<bool>,
}
impl Checks {
    fn new() -> Self {
        Self {
            denied: Cell::new(false),
            panic_deny: Cell::new(false),
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
impl PinnedShardAppendChecks for Checks {
    fn deny(&self) {
        self.denied.set(true);
        if self.panic_deny.get() {
            std::panic::panic_any("actual denial unwind");
        }
    }
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
        record_count: NonZeroU32::new(2).expect("actual two-record reservation"),
        placement: Placement {
            virtual_lane_id: ShardId::new(3),
            ring_epoch: RingEpoch::new(1),
            leader_epoch: LeaderEpoch::new(7),
            sequence: PlacementSequence::new(u64::try_from(batch).expect("fixture batch")),
        },
    }
}
fn input(batch: u128, first: u64) -> PinnedShardAppendInput {
    PinnedShardAppendInput {
        reservation: reservation(batch, 1, first),
        producer: None,
        atomic_group: None,
        payload: Bytes::from_static(b"actual native payload"),
    }
}
fn limits() -> PinnedShardAppendLimits {
    PinnedShardAppendLimits {
        max_operations: 8,
        max_frames: 16,
        max_payload_bytes: 4096,
        max_segments: 16,
        max_rotations: 8,
        max_path_bytes: 8192,
        max_name_bytes: 1024,
        max_packs: 16,
        max_batch_slots: 256,
        max_topic_slots: 64,
        max_location_slots: 256,
        max_io_calls: 256,
        max_write_calls: 128,
        max_write_bytes: 1 << 20,
        max_work_bytes: 1 << 24,
    }
}
struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "pinned-append-{}-{:020}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual input root");
        Self(path)
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    store: ShardStore,
    pin: PinnedShardDirectory,
    root: Root,
}
impl Fixture {
    fn new(target: u64) -> Self {
        let root = Root::new();
        let store = ShardStore::open(ShardStoreConfig {
            directory: root.0.clone(),
            shard_id: ShardId::new(3),
            target_pack_bytes: target,
            max_batch_bytes: 1024,
        })
        .expect("actual ordinary input store");
        let guard = ShardRecoveryGuard::try_acquire_until(
            store._lock_file.try_clone().expect("SAME actual OFD"),
            root.0.clone(),
            ShardGuardAcquireLimits {
                max_path_bytes: 8192,
                max_io_calls: 5,
            },
            expiry(),
        )
        .expect("real acquired native lease");
        let pin = PinnedShardDirectory::from_descriptor_until(
            File::open(&root.0).expect("actual dir FD"),
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
        Self { store, pin, root }
    }
    #[allow(
        clippy::result_large_err,
        reason = "retain actual native append inputs and settlement in the fixture without boxing"
    )]
    fn append(
        &mut self,
        inputs: Vec<PinnedShardAppendInput>,
        sync: bool,
        b: &mut PinnedShardAppendBudget,
        c: &Checks,
    ) -> Result<PinnedShardAppendReport, PinnedShardAppendError> {
        self.store
            .append_group_pinned_until(&self.pin, inputs, sync, b, c)
    }
    fn contended(&self) {
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.root.0.join(".shard-store.lock"))
            .expect("competing actual lock FD");
        assert!(fs2::FileExt::try_lock_exclusive(&fd).is_err());
    }
}

#[test]
fn real_native_group_frames_sorted_index_producer_and_atomic_metadata_reopen_exactly() {
    let mut f = Fixture::new(4096);
    let checks = Checks::new();
    let mut a = input(2, 2);
    a.producer = Some(ProducerSequence {
        producer_id: 9,
        epoch: 4,
        first_sequence: 2,
        digest_algorithm: DIGEST_XXH3_128,
        payload_digest: [7; 16],
        event_id: Some(RecordId::new(11)),
    });
    a.atomic_group = Some(AtomicGroupSequence {
        transaction_id: 13,
        chunk_sequence: 1,
        payload_digest: [8; 16],
    });
    let mut b = PinnedShardAppendBudget::new(limits(), expiry()).expect("plain admitted limits");
    let r = f
        .append(vec![a, input(1, 0)], true, &mut b, &checks)
        .expect("actual native group");
    assert_eq!(r.installed_frames(), 2);
    assert_eq!(r.prepared().len(), 2);
    assert!(!checks.denied.get());
    assert!(r.write_results().all(Result::is_ok));
    assert_eq!(
        r.unit_results()
            .filter(|(p, _)| *p == PinnedShardAppendPhase::SyncData)
            .count(),
        1
    );
    f.contended();
    let actual = fs::read(f.store.active_path()).expect("actual frame bytes");
    let other = Root::new();
    let mut ordinary = ShardStore::open(ShardStoreConfig {
        directory: other.0.clone(),
        shard_id: ShardId::new(3),
        target_pack_bytes: 4096,
        max_batch_bytes: 1024,
    })
    .expect("ordinary comparator");
    let prepared = r
        .inputs()
        .iter()
        .map(|i| {
            ordinary
                .prepare_append_atomic(i.reservation, i.producer, i.atomic_group, i.payload.clone())
                .expect("real ordinary codec")
        })
        .collect::<Vec<_>>();
    ordinary
        .append_prepared_group(&prepared.iter().collect::<Vec<_>>(), true)
        .expect("ordinary native group");
    assert_eq!(
        actual,
        fs::read(ordinary.active_path()).expect("ordinary bytes")
    );
    let cfg = f.store.config.clone();
    drop(f.store);
    drop(f.pin);
    let mut reopened = ShardStore::open(cfg).expect("actual disk recovery");
    let rows = reopened
        .fetch(
            TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0)),
            LogicalOffset::new(0),
            4096,
        )
        .expect("native fetch");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].reservation.batch_id, BatchId::new(1));
    assert_eq!(rows[1].producer, r.inputs()[0].producer);
    assert_eq!(rows[1].atomic_group, r.inputs()[0].atomic_group);
}

#[test]
fn real_rotation_retains_retired_fd_and_seal_uses_same_budget_and_ofd() {
    let mut f = Fixture::new(1);
    let c = Checks::new();
    let mut b = PinnedShardAppendBudget::new(limits(), expiry()).expect("budget");
    let r = f
        .append(vec![input(1, 0), input(2, 2)], true, &mut b, &c)
        .expect("actual rotation");
    assert_eq!(b.usage().rotations, 1);
    assert_eq!(r.retired_files().count(), 1);
    assert_eq!(f.store.pack_paths.len(), 2);
    assert_eq!(
        r.rotation_names().collect::<Vec<_>>(),
        vec![OsStr::new("pack-00000000000000000001.sse")]
    );
    let sealed = f
        .store
        .seal_active_pinned_until(&f.pin, &mut b, &c)
        .expect("same actual budget seal");
    assert_eq!(
        sealed.sealed_pack().expect("nonempty native pack").sequence,
        1
    );
    assert_eq!(b.usage().operations, 2);
    assert_eq!(b.usage().rotations, 2);
    f.contended();
    let cfg = f.store.config.clone();
    drop(r);
    drop(sealed);
    drop(f.store);
    drop(f.pin);
    let reopened = ShardStore::open(cfg).expect("reopen actual rotated packs");
    assert_eq!(reopened.catalog().len(), 2);
}

#[test]
fn exact_direct_write_attempts_fit_and_one_less_preserves_partial_write_and_rollback() {
    let mut exact = limits();
    exact.max_write_calls = 3;
    let mut f = Fixture::new(4096);
    let c = Checks::new();
    let mut b = PinnedShardAppendBudget::new(exact, expiry()).expect("exact");
    let r = f
        .append(vec![input(1, 0)], false, &mut b, &c)
        .expect("header payload checksum");
    assert_eq!(b.usage().write_calls, 3);
    drop(r);
    let mut f = Fixture::new(4096);
    exact.max_write_calls = 2;
    let c = Checks::new();
    let mut b = PinnedShardAppendBudget::new(exact, expiry()).expect("one less");
    let e = f
        .append(vec![input(1, 0)], false, &mut b, &c)
        .expect_err("bounded partial native write");
    assert_eq!(e.failure(), PinnedShardAppendFailure::Capacity);
    assert_eq!(e.report().write_results().count(), 2);
    assert!(
        e.report()
            .unit_results()
            .any(|(p, r)| p == PinnedShardAppendPhase::Rollback && r.is_ok())
    );
    assert!(
        e.report()
            .unit_results()
            .any(|(p, r)| p == PinnedShardAppendPhase::SyncData && r.is_ok())
    );
    assert_eq!(
        fs::metadata(f.store.active_path())
            .expect("actual length")
            .len(),
        0
    );
    assert!(f.store.catalog().is_empty());
    assert!(b.is_refused() && c.denied.get());
    let before = b.usage();
    let _ = f
        .append(vec![input(2, 2)], false, &mut b, &c)
        .expect_err("sticky original budget");
    assert_eq!(before, b.usage());
}

#[test]
fn actual_elapsed_write_result_is_retained_and_denial_precedes_prepaid_durability() {
    let mut f = Fixture::new(4096);
    let c = Rc::new(Checks::new());
    let watch = c.clone();
    let e = Instant::now() + Duration::from_millis(250);
    let mut b = PinnedShardAppendBudget::new(limits(), e).expect("budget");
    let _observe = observe(move |p| {
        if p == PinnedShardAppendPhase::Write {
            std::thread::sleep(Duration::from_millis(300));
        }
        if p == PinnedShardAppendPhase::Rollback || p == PinnedShardAppendPhase::SyncData {
            assert!(watch.denied.get());
        }
    });
    let error = f
        .append(vec![input(1, 0)], true, &mut b, &c)
        .expect_err("actual original E elapsed");
    assert_eq!(error.failure(), PinnedShardAppendFailure::Expired);
    assert!(
        error
            .report()
            .write_results()
            .next()
            .expect("real write")
            .is_ok()
    );
    assert_eq!(error.report().installed_frames(), 0);
    assert!(
        error
            .report()
            .unit_results()
            .any(|(p, _)| p == PinnedShardAppendPhase::SyncData)
    );
    assert!(c.denied.get());
    f.contended();
}

#[test]
fn actual_write_hook_and_rollback_hook_panics_do_not_skip_prepaid_sync_or_hide_inputs() {
    let mut f = Fixture::new(4096);
    let c = Rc::new(Checks::new());
    let watch = c.clone();
    c.panic_deny.set(true);
    let mut b = PinnedShardAppendBudget::new(limits(), expiry()).expect("budget");
    let _observe = observe(move |p| {
        if p == PinnedShardAppendPhase::Write {
            std::panic::panic_any("after actual physical write");
        }
        if p == PinnedShardAppendPhase::Rollback {
            assert!(watch.denied.get());
            std::panic::panic_any("after actual rollback");
        }
        if p == PinnedShardAppendPhase::SyncData {
            assert!(watch.denied.get());
        }
    });
    let e = f
        .append(vec![input(1, 0)], true, &mut b, &c)
        .expect_err("retained genuine phase panic");
    assert_eq!(e.failure(), PinnedShardAppendFailure::Unwind);
    assert!(e.report().panic_payload().is_some());
    assert!(e.report().denial_panic().is_some());
    assert_eq!(e.report().settlement_phase_panics().count(), 1);
    assert_eq!(
        e.report().inputs()[0].payload.as_ref(),
        b"actual native payload"
    );
    assert_eq!(
        e.report()
            .unit_results()
            .filter(|(p, _)| *p == PinnedShardAppendPhase::SyncData)
            .count(),
        1
    );
    assert_eq!(
        fs::metadata(f.store.active_path())
            .expect("real rollback")
            .len(),
        0
    );
}

#[test]
fn actual_late_create_retains_new_file_and_settles_parent_once_without_active_swap() {
    let mut f = Fixture::new(1);
    f.store
        .append(reservation(1, 1, 0), None, b"old native", true)
        .expect("actual old input");
    let c = Rc::new(Checks::new());
    let watch = c.clone();
    let original = Instant::now() + Duration::from_millis(250);
    let mut b = PinnedShardAppendBudget::new(limits(), original).expect("budget");
    let _observe = observe(move |p| {
        if p == PinnedShardAppendPhase::Create {
            std::thread::sleep(Duration::from_millis(300));
        }
        if p == PinnedShardAppendPhase::DirectorySync {
            assert!(watch.denied.get());
        }
    });
    let e = f
        .append(vec![input(2, 2)], false, &mut b, &c)
        .expect_err("late entered CreateNew");
    assert_eq!(e.failure(), PinnedShardAppendFailure::Expired);
    assert_eq!(e.report().retained_created_files().count(), 1);
    assert_eq!(f.store.active.sequence, 0);
    assert_eq!(
        e.report()
            .unit_results()
            .filter(|(p, _)| *p == PinnedShardAppendPhase::DirectorySync)
            .count(),
        1
    );
    assert!(f.root.0.join("pack-00000000000000000001.sse").is_file());
    assert!(c.denied.get());
}

#[test]
fn genuine_unknown_next_name_and_symlink_active_are_never_replaced_or_unlinked() {
    let mut f = Fixture::new(1);
    f.store
        .append(reservation(1, 1, 0), None, b"old", true)
        .expect("actual prior frame");
    let next = f.root.0.join("pack-00000000000000000001.sse");
    fs::write(&next, b"unknown native name").expect("genuine existing next name");
    let mut b = PinnedShardAppendBudget::new(limits(), expiry()).expect("budget");
    let c = Checks::new();
    let e = f
        .append(vec![input(2, 2)], false, &mut b, &c)
        .expect_err("CreateNew conflict");
    assert_eq!(e.failure(), PinnedShardAppendFailure::Io);
    assert_eq!(
        e.report()
            .create_errors()
            .next()
            .expect("native errno")
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(
        fs::read(&next).expect("unknown file remains"),
        b"unknown native name"
    );
    let mut f = Fixture::new(4096);
    let path = f.store.active_path().to_path_buf();
    let retained = f.root.0.join("retained-pack");
    fs::rename(&path, &retained).expect("real rename");
    symlink(&retained, &path).expect("actual symlink");
    let mut b = PinnedShardAppendBudget::new(limits(), expiry()).expect("budget");
    let c = Checks::new();
    let e = f
        .append(vec![input(1, 0)], false, &mut b, &c)
        .expect_err("nofollow identity");
    assert_eq!(e.failure(), PinnedShardAppendFailure::Identity);
    assert_eq!(e.report().write_results().count(), 0);
    assert!(
        fs::symlink_metadata(&path)
            .expect("original symlink remains")
            .file_type()
            .is_symlink()
    );
}

#[test]
fn actual_native_codec_rejection_keeps_partial_header_and_duplicate_group_enters_no_write() {
    let mut f = Fixture::new(4096);
    let mut bad = input(1, 0);
    bad.producer = Some(ProducerSequence {
        producer_id: 1,
        epoch: 1,
        first_sequence: 0,
        digest_algorithm: u8::MAX,
        payload_digest: [0; 16],
        event_id: None,
    });
    let mut b = PinnedShardAppendBudget::new(limits(), expiry()).expect("budget");
    let c = Checks::new();
    let e = f
        .append(vec![bad], false, &mut b, &c)
        .expect_err("real native producer codec refusal");
    assert!(matches!(
        e.report().native_error(),
        Some(StorageError::InvalidInput(_))
    ));
    assert_eq!(e.report().prepared().len(), 1);
    assert!(!e.report().prepared()[0].encoded.header.is_empty());
    assert_eq!(e.report().write_results().count(), 0);
    let mut f = Fixture::new(4096);
    let mut b = PinnedShardAppendBudget::new(limits(), expiry()).expect("fresh");
    let c = Checks::new();
    let e = f
        .append(vec![input(1, 0), input(1, 0)], false, &mut b, &c)
        .expect_err("actual duplicate group refusal");
    assert!(matches!(
        e.report().native_error(),
        Some(StorageError::InvalidInput(_))
    ));
    assert_eq!(e.report().write_results().count(), 0);
}

#[test]
fn exact_native_call_ceiling_fits_and_one_less_refuses_without_budget_or_e_renewal() {
    let mut f = Fixture::new(4096);
    let c = Checks::new();
    let mut b = PinnedShardAppendBudget::new(limits(), expiry()).expect("measurement input");
    let r = f
        .append(vec![input(1, 0)], true, &mut b, &c)
        .expect("real original call sequence");
    let used = r.usage();
    drop(r);
    let mut exact = limits();
    exact.max_io_calls = used.reserved_io_calls;
    let mut f = Fixture::new(4096);
    let c = Checks::new();
    let mut b = PinnedShardAppendBudget::new(exact, expiry()).expect("exact calls");
    f.append(vec![input(1, 0)], true, &mut b, &c)
        .expect("exact actual ceiling");
    exact.max_io_calls -= 1;
    let mut f = Fixture::new(4096);
    let c = Checks::new();
    let original = expiry();
    let mut b = PinnedShardAppendBudget::new(exact, original).expect("one less");
    let e = f
        .append(vec![input(1, 0)], true, &mut b, &c)
        .expect_err("one less prepaid native call");
    assert_eq!(e.failure(), PinnedShardAppendFailure::Capacity);
    assert!(b.is_refused());
    b.cap_expiry(original + Duration::from_secs(30));
    assert_eq!(b.absolute_expiry(), original);
    b.cap_expiry(Instant::now());
    let saved = b.usage();
    assert!(f.append(Vec::new(), false, &mut b, &c).is_err());
    assert_eq!(saved, b.usage());
}

#[test]
fn actual_readonly_same_inode_descriptor_returns_retained_os_write_error_and_settlement() {
    let mut f = Fixture::new(4096);
    // A deliberately faulty real input FD: same inode, opened read-only. This
    // tests the actual OS error, not a synthetic StorageError or owner factory.
    f.store.active.file =
        File::open(f.store.active_path()).expect("same native inode read-only FD");
    let mut b = PinnedShardAppendBudget::new(limits(), expiry()).expect("budget");
    let c = Checks::new();
    let e = f
        .append(vec![input(1, 0)], true, &mut b, &c)
        .expect_err("actual write EBADF");
    assert_eq!(e.failure(), PinnedShardAppendFailure::Io);
    assert!(
        e.report()
            .write_results()
            .next()
            .expect("entered real OS write")
            .is_err()
    );
    assert!(
        e.report()
            .unit_results()
            .any(|(p, r)| p == PinnedShardAppendPhase::Rollback && r.is_err())
    );
    assert!(
        e.report()
            .unit_results()
            .any(|(p, _)| p == PinnedShardAppendPhase::SyncData)
    );
    assert!(c.denied.get());
}

#[test]
fn real_completed_write_error_survives_later_e_and_original_longer_e_cannot_widen_cap() {
    let mut f = Fixture::new(4096);
    f.store.active.file =
        File::open(f.store.active_path()).expect("genuine read-only same-inode FD");
    let longer = expiry();
    let earlier = Instant::now() + Duration::from_millis(250);
    let mut b = PinnedShardAppendBudget::new(limits(), longer).expect("original longer E");
    b.cap_expiry(earlier);
    b.cap_expiry(longer);
    let _observe = observe(move |p| {
        if p == PinnedShardAppendPhase::Write {
            std::thread::sleep(Duration::from_millis(300));
        }
    });
    let c = Checks::new();
    let e = f
        .append(vec![input(1, 0)], false, &mut b, &c)
        .expect_err("late completed native Err");
    assert_eq!(e.failure(), PinnedShardAppendFailure::Expired);
    assert!(Instant::now() < longer);
    assert!(
        e.report()
            .write_results()
            .next()
            .expect("actual first native Result")
            .is_err()
    );
    assert_eq!(b.absolute_expiry(), earlier);
    assert!(c.denied.get());
}

#[test]
fn real_create_hook_unwind_retains_created_fd_and_once_only_parent_sync_even_if_hook_unwinds() {
    let mut f = Fixture::new(1);
    f.store
        .append(reservation(1, 1, 0), None, b"old", true)
        .expect("actual prior frame");
    let c = Rc::new(Checks::new());
    let watch = c.clone();
    let _observe = observe(move |p| {
        if p == PinnedShardAppendPhase::Create {
            std::panic::panic_any("after actual CreateNew");
        }
        if p == PinnedShardAppendPhase::DirectorySync {
            assert!(watch.denied.get());
            std::panic::panic_any("after actual parent fsync");
        }
    });
    let mut b = PinnedShardAppendBudget::new(limits(), expiry()).expect("budget");
    let e = f
        .append(vec![input(2, 2)], false, &mut b, &c)
        .expect_err("actual create phase unwind");
    assert_eq!(e.failure(), PinnedShardAppendFailure::Unwind);
    assert!(e.report().panic_payload().is_some());
    assert_eq!(e.report().retained_created_files().count(), 1);
    assert_eq!(e.report().settlement_phase_panics().count(), 1);
    assert_eq!(
        e.report()
            .unit_results()
            .filter(|(p, _)| *p == PinnedShardAppendPhase::DirectorySync)
            .count(),
        1
    );
    assert_eq!(f.store.active.sequence, 0);
    assert!(f.root.0.join("pack-00000000000000000001.sse").is_file());
}

#[test]
fn real_rotation_flush_unwind_denies_before_one_prepaid_sync_and_enters_no_create() {
    let mut f = Fixture::new(1);
    f.store
        .append(reservation(1, 1, 0), None, b"old", true)
        .expect("actual prior frame");
    let c = Rc::new(Checks::new());
    let watch = c.clone();
    let _observe = observe(move |p| {
        if p == PinnedShardAppendPhase::Flush {
            std::panic::panic_any("after actual old-pack flush");
        }
        if p == PinnedShardAppendPhase::SyncData {
            assert!(watch.denied.get());
        }
    });
    let mut b = PinnedShardAppendBudget::new(limits(), expiry()).expect("budget");
    let e = f
        .append(vec![input(2, 2)], false, &mut b, &c)
        .expect_err("entered actual old flush unwind");
    assert_eq!(e.failure(), PinnedShardAppendFailure::Unwind);
    assert_eq!(
        e.report()
            .unit_results()
            .filter(|(p, _)| *p == PinnedShardAppendPhase::Flush)
            .count(),
        1
    );
    assert_eq!(
        e.report()
            .unit_results()
            .filter(|(p, _)| *p == PinnedShardAppendPhase::SyncData)
            .count(),
        1
    );
    assert_eq!(e.report().retained_created_files().count(), 0);
    assert!(!f.root.0.join("pack-00000000000000000001.sse").exists());
}

#[test]
fn real_partial_index_unwind_keeps_complete_physical_frames_and_never_truncates_indexed_bytes() {
    let mut f = Fixture::new(4096);
    let c = Checks::new();
    let _observe = observe(|p| {
        if p == PinnedShardAppendPhase::Index {
            std::panic::panic_any("after actual first native index insertion");
        }
    });
    let mut b = PinnedShardAppendBudget::new(limits(), expiry()).expect("budget");
    let e = f
        .append(vec![input(1, 0), input(2, 2)], true, &mut b, &c)
        .expect_err("partial actual index");
    assert_eq!(e.failure(), PinnedShardAppendFailure::Unwind);
    assert_eq!(e.report().installed_frames(), 1);
    assert_eq!(f.store.by_batch.len(), 1);
    assert_eq!(f.store.active.bytes, 0);
    assert!(
        fs::metadata(f.store.active_path())
            .expect("actual frames remain")
            .len()
            > 0
    );
    assert!(
        !e.report()
            .unit_results()
            .any(|(p, _)| p == PinnedShardAppendPhase::Rollback)
    );
    assert_eq!(
        e.report()
            .unit_results()
            .filter(|(p, _)| *p == PinnedShardAppendPhase::SyncData)
            .count(),
        1
    );
    assert!(b.is_refused() && c.denied.get());
    let cfg = f.store.config.clone();
    drop(e);
    drop(f.store);
    drop(f.pin);
    let recovered =
        ShardStore::open(cfg).expect("ordinary actual physical recovery, no permission claim");
    assert_eq!(recovered.catalog().len(), 2);
}

impl PinnedStoreOpenChecks for Checks {
    fn deny(&self) {
        <Self as PinnedShardAppendChecks>::deny(self);
    }
}
struct LeadingActive {
    opened: OpenedPinnedShardStore,
    pin: PinnedShardDirectory,
    path: PathBuf,
    name: OsString,
    config: ShardStoreConfig,
    root: Root,
}
fn leading_active() -> LeadingActive {
    let Fixture {
        mut store,
        pin,
        root,
    } = Fixture::new(4096);
    store
        .append(reservation(1, 1, 0), None, b"actual old active frame", true)
        .expect("real original native writer");
    let config = store.config.clone();
    let old = store.active_path().to_path_buf();
    drop(store);
    // Actual filesystem mutation and actual native discovery, never a private
    // pack-map edit, fabricated owner or old-guard-to-acquired conversion.
    let name = OsString::from(format!("pack-{}.sse", "0".repeat(192)));
    let path = root.0.join(&name);
    fs::rename(&old, &path).expect("real accepted extra leading-zero active filename");
    let native = PinnedStoreOpenLimits {
        max_directory_visits: 64,
        directory_buffer_bytes: 4096,
        max_name_bytes: 255,
        max_path_bytes: 8192,
        max_packs: 8,
        max_frames: 16,
        max_frame_bytes: 8192,
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
    let opened =
        ShardStore::open_pinned_until(config.clone(), &pin, native, expiry(), &Checks::new())
            .expect("actual pinned native reopen accepts literal filename");
    assert_eq!(opened.store().active_path(), path);
    assert_eq!(opened.store().catalog().len(), 1);
    LeadingActive {
        opened,
        pin,
        path,
        name,
        config,
        root,
    }
}

#[test]
fn real_leading_zero_active_pinned_reopen_appends_and_seals_the_actual_stored_path() {
    let mut f = leading_active();
    let original = fs::read(&f.path).expect("real original native bytes");
    let mut policy = limits();
    policy.max_name_bytes = f.name.as_os_str().len();
    policy.max_path_bytes = f.path.as_os_str().len();
    let mut b = PinnedShardAppendBudget::new(policy, expiry()).expect("literal raw/name ceilings");
    let c = Checks::new();
    let appended = f
        .opened
        .store_mut()
        .append_group_pinned_until(&f.pin, vec![input(2, 2)], true, &mut b, &c)
        .expect("actual existing native spelling append");
    assert_eq!(appended.original_active_name(), Some(f.name.as_os_str()));
    assert_eq!(
        appended
            .active_c_name()
            .expect("actual native name")
            .to_bytes(),
        f.name.as_os_str().as_bytes()
    );
    assert_eq!(appended.active_path_bytes(), Some(f.path.as_os_str().len()));
    assert!(
        fs::read(&f.path)
            .expect("actual appended bytes")
            .starts_with(&original)
    );
    assert_eq!(f.opened.store().active_path(), f.path);
    let sealed = f
        .opened
        .store_mut()
        .seal_active_pinned_until(&f.pin, &mut b, &c)
        .expect("actual noncanonical active seal");
    assert_eq!(
        sealed.sealed_pack().expect("actual sealed candidate").path,
        f.path
    );
    assert_eq!(sealed.original_active_name(), Some(f.name.as_os_str()));
    assert_eq!(
        f.opened.store().active_path(),
        f.root.0.join("pack-00000000000000000001.sse")
    );
    assert!(!c.denied.get());
    let config = f.config.clone();
    drop(sealed);
    drop(appended);
    drop(f.opened);
    drop(f.pin);
    let reopened =
        ShardStore::open(config).expect("actual physical reopen of accepted old and new names");
    assert_eq!(reopened.catalog().len(), 2);
}

#[test]
fn real_leading_zero_active_raw_path_or_name_one_less_refuses_before_name_copy_and_write() {
    for short_path in [true, false] {
        let mut f = leading_active();
        let original = fs::read(&f.path).expect("actual original frame");
        let mut policy = limits();
        policy.max_path_bytes = f.path.as_os_str().len() - usize::from(short_path);
        policy.max_name_bytes = f.name.as_os_str().len() - usize::from(!short_path);
        let mut b =
            PinnedShardAppendBudget::new(policy, expiry()).expect("genuine finite short policy");
        let c = Checks::new();
        let e = f
            .opened
            .store_mut()
            .append_group_pinned_until(&f.pin, vec![input(2, 2)], true, &mut b, &c)
            .expect_err("actual raw/name cap is one short");
        assert_eq!(e.failure(), PinnedShardAppendFailure::Capacity);
        assert_eq!(
            e.report().active_path_bytes(),
            Some(f.path.as_os_str().len())
        );
        assert!(e.report().original_active_name().is_none());
        assert!(e.report().active_c_name().is_none());
        assert_eq!(e.report().metadata_results().count(), 5);
        assert_eq!(e.report().write_results().count(), 0);
        assert_eq!(
            fs::read(&f.path).expect("same real input bytes remain"),
            original
        );
        assert_eq!(f.opened.store().catalog().len(), 1);
        assert!(c.denied.get());
    }
}

#[test]
fn actual_raw_scan_and_name_materialization_work_is_prepaid_at_exact_and_one_less_boundary() {
    let mut f = leading_active();
    let mut policy = limits();
    policy.max_path_bytes = f.path.as_os_str().len() - 1;
    policy.max_name_bytes = f.name.as_os_str().len();
    let mut b = PinnedShardAppendBudget::new(policy, expiry()).expect("real prefix observation");
    let c = Checks::new();
    let e = f
        .opened
        .store_mut()
        .append_group_pinned_until(&f.pin, vec![input(2, 2)], false, &mut b, &c)
        .expect_err("actual raw preparse refusal");
    let prefix = e.report().usage().work_bytes;
    assert!(e.report().original_active_name().is_none());
    drop(e);
    let raw = f.path.as_os_str().len();
    let names = f.name.as_os_str().len();
    let exact = prefix + raw + 4 * (names + 1);
    let mut exact_policy = policy;
    exact_policy.max_path_bytes = raw;
    exact_policy.max_work_bytes = exact;
    let exact_cost = pinned_shard_append_cost(exact_policy).expect("actual plain slot/name costs");
    let mut short_name = exact_policy;
    short_name.max_name_bytes -= 1;
    assert!(
        pinned_shard_append_cost(short_name)
            .expect("smaller plain cost")
            .string_bytes
            < exact_cost.string_bytes
    );
    for one_less in [false, true] {
        let mut f = leading_active();
        let c = Checks::new();
        let mut policy = exact_policy;
        policy.max_work_bytes -= usize::from(one_less);
        let mut b = PinnedShardAppendBudget::new(policy, expiry())
            .expect("same actual source precharge ceiling");
        let e = f
            .opened
            .store_mut()
            .append_group_pinned_until(&f.pin, vec![input(2, 2)], false, &mut b, &c)
            .expect_err("only preparse work paid; later active stat remains unpaid");
        assert_eq!(e.failure(), PinnedShardAppendFailure::Capacity);
        assert_eq!(e.report().metadata_results().count(), 5);
        assert_eq!(e.report().write_results().count(), 0);
        if one_less {
            assert!(e.report().original_active_name().is_none());
            assert!(e.report().active_c_name().is_none());
        } else {
            assert_eq!(e.report().original_active_name(), Some(f.name.as_os_str()));
            assert_eq!(
                e.report()
                    .active_c_name()
                    .expect("paid actual name")
                    .to_bytes(),
                f.name.as_os_str().as_bytes()
            );
            assert_eq!(e.report().usage().work_bytes, exact);
        }
        assert!(c.denied.get());
    }
}

type PointLookupObserver = Box<dyn FnMut(&'static str, bool, &mut PinnedShardAppendBudget, usize)>;
thread_local! {
    static POINT_LOOKUP_OBSERVER: RefCell<Option<PointLookupObserver>> = RefCell::new(None);
}
pub(super) fn point_lookup_checkpoint(
    point: &'static str,
    completed: bool,
    budget: &mut PinnedShardAppendBudget,
    fee: usize,
) {
    POINT_LOOKUP_OBSERVER.with(|observer| {
        if let Some(observer) = &mut *observer.borrow_mut() {
            observer(point, completed, budget, fee);
        }
    });
}
struct PointLookupObserve;
impl Drop for PointLookupObserve {
    fn drop(&mut self) {
        POINT_LOOKUP_OBSERVER.with(|observer| *observer.borrow_mut() = None);
    }
}
fn observe_point_lookups(
    observer: impl FnMut(&'static str, bool, &mut PinnedShardAppendBudget, usize) + 'static,
) -> PointLookupObserve {
    POINT_LOOKUP_OBSERVER.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(observer));
    });
    PointLookupObserve
}

fn sparse_native_history() -> Fixture {
    let mut fixture = Fixture::new(1 << 20);
    let mut starts = HashMap::new();
    for topic in 2..=64 {
        fixture
            .store
            .append(
                reservation(topic, topic, 0),
                None,
                b"actual old native topic",
                false,
            )
            .expect("actual native population, not a fabricated map");
        starts.insert(
            TopicPartition::new(TopicId::new(topic), LogicalPartitionId::new(0)),
            LogicalOffset::new(2),
        );
    }
    fixture
        .store
        .seal_active()
        .expect("actual old populated pack sealed");
    for n in 0..4 {
        fixture
            .store
            .append(
                reservation(1000 + n, 1, 2 * n as u64),
                None,
                b"actual retained native topic",
                true,
            )
            .expect("actual retained native append");
    }
    let removed = fixture
        .store
        .garbage_collect(&starts)
        .expect("actual native old-pack collection");
    assert_eq!(removed.extents_removed, 63);
    assert_eq!(fixture.store.index.len(), 1);
    assert!(fixture.store.index.capacity() > fixture.store.index.len());
    let retained = fixture
        .store
        .index
        .get(&TopicPartition::new(
            TopicId::new(1),
            LogicalPartitionId::new(0),
        ))
        .expect("real retained topic");
    assert_eq!(retained.len(), 4);
    assert_eq!(
        retained.capacity(),
        retained.len(),
        "actual fixture fills its retained native Vec"
    );
    fixture.contended();
    fixture
}

#[test]
fn genuine_sparse_native_history_fits_new_lookup_fees_and_reopens_all_retained_records() {
    let mut fixture = sparse_native_history();
    let mut policy = limits();
    policy.max_topic_slots = 256;
    let mut budget =
        PinnedShardAppendBudget::new(policy, expiry()).expect("same finite actual policy");
    let checks = Checks::new();
    let observed = Rc::new(RefCell::new(Vec::new()));
    let saved = observed.clone();
    let _observer = observe_point_lookups(move |point, completed, budget, fee| {
        saved
            .borrow_mut()
            .push((point, completed, budget.usage(), fee));
    });
    let report = fixture
        .append(vec![input(2000, 8)], true, &mut budget, &checks)
        .expect("actual sparse-map append fits genuine new point-lookup work");
    let rows = observed.borrow();
    for point in ["prepare", "segment"] {
        let before = rows
            .iter()
            .find(|row| row.0 == point && !row.1)
            .expect("actual point prepayment");
        let after = rows
            .iter()
            .find(|row| row.0 == point && row.1)
            .expect("actual completed point lookup");
        assert_eq!(after.2.work_bytes, before.2.work_bytes + before.3);
        assert_eq!(before.3, after.3);
        assert!(before.3 > size_of::<(TopicPartition, Vec<ExtentLocation>)>());
    }
    assert_eq!(report.installed_frames(), 1);
    assert!(!checks.denied.get());
    let config = fixture.store.config.clone();
    drop(rows);
    drop(report);
    drop(fixture.store);
    drop(fixture.pin);
    let recovered = ShardStore::open(config).expect("actual native physical reopen");
    assert_eq!(recovered.catalog().len(), 5);
    assert_eq!(recovered.by_batch.len(), 5);
    assert_eq!(recovered.index.len(), 1);
}

#[test]
fn actual_sparse_lookup_exact_and_one_less_boundaries_precede_each_point_and_keep_native_effects() {
    for point in ["prepare", "segment"] {
        for one_less in [false, true] {
            let mut fixture = sparse_native_history();
            let active = fixture.store.active_path().to_path_buf();
            let original_bytes = fs::read(&active).expect("actual current pack before operation");
            let original_location_capacity = fixture
                .store
                .index
                .values()
                .next()
                .expect("actual retained topic")
                .capacity();
            let mut policy = limits();
            policy.max_topic_slots = 256;
            let original_e = expiry();
            let mut budget =
                PinnedShardAppendBudget::new(policy, original_e).expect("actual original budget");
            let observed = Rc::new(RefCell::new(Vec::new()));
            let saved = observed.clone();
            let _observer = observe_point_lookups(move |actual, completed, budget, fee| {
                if actual != point {
                    return;
                }
                let before = budget.usage();
                saved.borrow_mut().push((completed, before, fee));
                if !completed {
                    // Local test fault injection only: tighten this SAME real
                    // ceiling, never reset usage, E or another policy axis.
                    let exact = before
                        .work_bytes
                        .checked_add(fee)
                        .expect("actual finite fee");
                    let cap = exact - usize::from(one_less);
                    assert!(cap <= budget.limits.max_work_bytes);
                    budget.limits.max_work_bytes = cap;
                    assert_eq!(budget.usage(), before);
                    assert_eq!(budget.absolute_expiry(), original_e);
                }
            });
            let checks = Checks::new();
            let error = fixture
                .append(vec![input(2000, 8)], false, &mut budget, &checks)
                .expect_err(
                    "only the actual lookup fee fits; later separate native work is unpaid",
                );
            assert_eq!(error.failure(), PinnedShardAppendFailure::Capacity);
            let rows = observed.borrow();
            assert_eq!(rows.len(), if one_less { 1 } else { 2 });
            let (_, before, fee) = rows[0];
            assert!(fee > size_of::<(TopicPartition, Vec<ExtentLocation>)>());
            assert_eq!(
                error.report().usage().work_bytes,
                before.work_bytes + if one_less { 0 } else { fee }
            );
            assert_eq!(budget.absolute_expiry(), original_e);
            assert!(checks.denied.get() && budget.is_refused());
            assert_eq!(error.report().inputs().len(), 1);
            assert_eq!(error.report().prepared().len(), 1);
            assert_eq!(error.report().installed_frames(), 0);
            assert_eq!(fixture.store.catalog().len(), 4);
            assert_eq!(
                fs::read(&active).expect("actual same-FD settled pack"),
                original_bytes
            );
            if point == "prepare" {
                assert_eq!(error.report().write_results().count(), 0);
                let actual = fixture
                    .store
                    .index
                    .get(&TopicPartition::new(
                        TopicId::new(1),
                        LogicalPartitionId::new(0),
                    ))
                    .expect("same actual retained topic");
                if one_less {
                    assert_eq!(actual.capacity(), original_location_capacity);
                } else {
                    assert!(actual.capacity() > original_location_capacity);
                }
            } else {
                assert!(
                    error
                        .report()
                        .write_results()
                        .any(|actual| matches!(actual, Ok(n) if *n > 0))
                );
                assert!(
                    error
                        .report()
                        .unit_results()
                        .any(|(phase, actual)| phase == PinnedShardAppendPhase::Rollback
                            && actual.is_ok())
                );
                assert!(
                    error
                        .report()
                        .unit_results()
                        .any(|(phase, actual)| phase == PinnedShardAppendPhase::SyncData
                            && actual.is_ok())
                );
            }
            fixture.contended();
        }
    }
}

fn sparse_install_input(new_topic: bool) -> PinnedShardAppendInput {
    let mut actual = input(2000, 8);
    if new_topic {
        actual.reservation = reservation(2000, 2, 0);
    }
    actual
}

#[test]
fn actual_sparse_install_probes_fit_immutable_budgets_for_existing_and_new_topics() {
    for new_topic in [false, true] {
        let mut fixture = sparse_native_history();
        assert!(fixture.store.by_batch.capacity() > fixture.store.by_batch.len());
        let topic_capacity = fixture.store.index.capacity();
        let batch_capacity = fixture.store.by_batch.capacity();
        let topic_fee = topic_capacity * size_of::<(TopicPartition, Vec<ExtentLocation>)>();
        let batch_fee = batch_capacity * size_of::<((TopicPartition, BatchId), ExtentLocation)>();
        let mut policy = limits();
        policy.max_topic_slots = 256;
        let original_e = expiry();
        let mut budget = PinnedShardAppendBudget::new(policy, original_e)
            .expect("immutable generous actual install policy");
        let rows = Rc::new(RefCell::new(Vec::new()));
        let saved = rows.clone();
        let _observer = observe_point_lookups(move |point, completed, budget, fee| {
            if matches!(point, "install_insert" | "install_location") {
                saved
                    .borrow_mut()
                    .push((point, completed, budget.usage(), fee));
            }
        });
        let checks = Checks::new();
        let report = fixture
            .append(
                vec![sparse_install_input(new_topic)],
                true,
                &mut budget,
                &checks,
            )
            .expect("actual sparse index and batch-map install fit before native entry");
        let rows = rows.borrow();
        for (point, expected_fee) in [
            ("install_insert", topic_fee),
            ("install_location", topic_fee + batch_fee),
        ] {
            let before = rows.iter().find(|row| row.0 == point && !row.1);
            if point == "install_insert" && !new_topic {
                assert!(
                    before.is_none(),
                    "existing topic performs no conditional insertion"
                );
                continue;
            }
            let before = before.expect("actual install prepayment observed");
            let after = rows
                .iter()
                .find(|row| row.0 == point && row.1)
                .expect("actual complete native point/helper");
            assert_eq!(before.3, expected_fee);
            assert_eq!(after.3, expected_fee);
            assert_eq!(after.2.work_bytes, before.2.work_bytes + expected_fee);
        }
        assert_eq!(fixture.store.index.capacity(), topic_capacity);
        assert_eq!(fixture.store.by_batch.capacity(), batch_capacity);
        assert_eq!(budget.limits(), policy, "no generous-policy mutation");
        assert_eq!(budget.absolute_expiry(), original_e);
        assert!(!budget.is_refused() && !checks.denied.get());
        assert_eq!(report.installed_frames(), 1);
        assert!(
            report
                .unit_results()
                .any(|(phase, actual)| phase == PinnedShardAppendPhase::SyncData && actual.is_ok())
        );
        assert_eq!(fixture.store.by_batch.len(), 5);
        assert_eq!(fixture.store.index.len(), 1 + usize::from(new_topic));
        let config = fixture.store.config.clone();
        drop(rows);
        drop(report);
        drop(fixture.store);
        drop(fixture.pin);
        let reopened = ShardStore::open(config).expect("actual same native frames reopen");
        assert_eq!(reopened.catalog().len(), 5);
        assert_eq!(reopened.by_batch.len(), 5);
        assert_eq!(reopened.index.len(), 1 + usize::from(new_topic));
    }
}

#[test]
fn actual_sparse_install_fee_exact_and_one_less_keep_each_entered_native_effect() {
    for (new_topic, point) in [
        (false, "install_location"),
        (true, "install_insert"),
        (true, "install_location"),
    ] {
        for one_less in [false, true] {
            let mut fixture = sparse_native_history();
            assert!(fixture.store.by_batch.capacity() > fixture.store.by_batch.len());
            let active = fixture.store.active_path().to_path_buf();
            let original_bytes = fs::read(&active).expect("actual native active bytes");
            let topic_fee =
                fixture.store.index.capacity() * size_of::<(TopicPartition, Vec<ExtentLocation>)>();
            let batch_fee = fixture.store.by_batch.capacity()
                * size_of::<((TopicPartition, BatchId), ExtentLocation)>();
            let mut policy = limits();
            policy.max_topic_slots = 256;
            let original_e = expiry();
            let mut budget = PinnedShardAppendBudget::new(policy, original_e)
                .expect("same actual cumulative native policy");
            let rows = Rc::new(RefCell::new(Vec::new()));
            let saved = rows.clone();
            let _observer = observe_point_lookups(move |actual, completed, budget, fee| {
                if actual != point {
                    return;
                }
                let before = budget.usage();
                saved.borrow_mut().push((completed, before, fee));
                if !completed {
                    // Explicit local fault injection: this is not an originally
                    // configured immutable ceiling or a complete startup proof.
                    let old_limits = budget.limits();
                    let exact = before
                        .work_bytes
                        .checked_add(fee)
                        .expect("actual finite fee");
                    let cap = exact - usize::from(one_less);
                    assert!(cap <= old_limits.max_work_bytes);
                    budget.limits.max_work_bytes = cap;
                    let mut expected = old_limits;
                    expected.max_work_bytes = cap;
                    assert_eq!(budget.limits(), expected);
                    assert_eq!(budget.usage(), before);
                    assert_eq!(budget.absolute_expiry(), original_e);
                }
            });
            let checks = Checks::new();
            let result = fixture.append(
                vec![sparse_install_input(new_topic)],
                false,
                &mut budget,
                &checks,
            );
            // Fitting the final helper completes this sync=false native call.
            // Fitting only the conditional insert cannot pay the next helper.
            let completed = point == "install_location" && !one_less;
            assert_eq!(result.is_ok(), completed);
            let report = match &result {
                Ok(report) => report,
                Err(error) => {
                    assert_eq!(error.failure(), PinnedShardAppendFailure::Capacity);
                    error.report()
                }
            };
            let rows = rows.borrow();
            assert_eq!(rows.len(), if one_less { 1 } else { 2 });
            let (_, before, fee) = rows[0];
            assert_eq!(
                fee,
                topic_fee
                    + if point == "install_location" {
                        batch_fee
                    } else {
                        0
                    }
            );
            assert_eq!(
                report.usage().work_bytes,
                before.work_bytes + if one_less { 0 } else { fee }
            );
            assert_eq!(budget.absolute_expiry(), original_e);
            assert_eq!(budget.is_refused(), !completed);
            assert_eq!(checks.denied.get(), !completed);
            assert_eq!(report.inputs().len(), 1);
            assert_eq!(report.prepared().len(), 1);
            assert_eq!(report.installed_frames(), usize::from(completed));
            assert_eq!(fixture.store.by_batch.len(), 4 + usize::from(completed));
            assert_eq!(fixture.store.catalog().len(), 4 + usize::from(completed));
            let topic_inserted = new_topic && (point == "install_location" || !one_less);
            assert_eq!(fixture.store.index.len(), 1 + usize::from(topic_inserted));
            let actual_bytes = fs::read(&active).expect("actual written native segment retained");
            assert!(actual_bytes.starts_with(&original_bytes));
            assert!(actual_bytes.len() > original_bytes.len());
            assert!(
                report
                    .write_results()
                    .any(|actual| matches!(actual, Ok(n) if *n > 0))
            );
            assert!(report.unit_results().any(|(phase, actual)| phase == PinnedShardAppendPhase::Flush && actual.is_ok()));
            // The install phase retains complete written frames and possible
            // partial index changes. It does not truncate those named effects.
            assert!(
                !report
                    .unit_results()
                    .any(|(phase, _)| phase == PinnedShardAppendPhase::Rollback)
            );
            if !completed {
                assert!(
                    report
                        .unit_results()
                        .any(|(phase, actual)| phase == PinnedShardAppendPhase::SyncData
                            && actual.is_ok())
                );
            }
            fixture.contended();
        }
    }
}
