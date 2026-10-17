//! Real Core/native stores and actual lane/sync threads; no C1 authority factory.

use super::*;
use crate::{
    EngineHandle, NativeFetchLimits, NativePrimaryRecovery, NativeRecoveryCensus,
    NativeRecoveryLimits, NativeResumeLimits, NativeResumeOutcome, StreamEngine,
};
use shard_stream_core::{
    DirectoryOwnership, DirectoryOwnershipError, NativeDirectoryBudget, NativeDirectoryLimits,
    OwnedFileAccess, OwnedPinnedShardStore,
};
use shardlog::{
    BatchId, CoordinatorJournal, JournalEvent, LeaderEpoch, LogicalPartitionId,
    PinnedShardDirectoryLimits, PinnedShardGcChecks, PinnedShardGcLimits, PinnedStoreOpenChecks,
    PinnedStoreOpenLimits, Placement, PlacementSequence, RingEpoch, ShardDirectoryCheckFailure,
    ShardDirectoryChecks, ShardGuardAcquireLimits, ShardRecoveryGuard, TopicId, VirtualLaneId,
};
use std::fs::{self, OpenOptions};
use std::num::NonZeroU32;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};

pub(super) struct HostChecks {
    denied: AtomicBool,
    denies: AtomicUsize,
}
impl HostChecks {
    fn new() -> Self {
        Self {
            denied: AtomicBool::new(false),
            denies: AtomicUsize::new(0),
        }
    }
}
impl ShardDirectoryChecks for HostChecks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        if self.denied.load(Ordering::Acquire) {
            Err(ShardDirectoryCheckFailure::Refused)
        } else {
            Ok(())
        }
    }
}
impl PinnedStoreOpenChecks for HostChecks {
    fn deny(&self) {
        self.denies.fetch_add(1, Ordering::AcqRel);
        self.denied.store(true, Ordering::Release);
    }
}
impl PinnedShardGcChecks for HostChecks {
    fn deny(&self) {
        PinnedStoreOpenChecks::deny(self);
    }
}

struct Temporary {
    root: PathBuf,
    shard: PathBuf,
    first: PathBuf,
}
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "engine-owned-primary-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("actual temporary root");
        let root = fs::canonicalize(root).expect("fixture canonical input before bounded work");
        let shard = root.join("shards/shard-0");
        let (mut journal, _) = CoordinatorJournal::open(root.join("coordinator.ssj"))
            .expect("actual native coordinator writer");
        journal
            .append(
                &JournalEvent::Configure {
                    topic_partition: partition(),
                    ring_epoch: RingEpoch::new(1),
                    shards: vec![ShardId::new(3)],
                },
                true,
            )
            .expect("actual native configured virtual lane");
        drop(journal);
        let mut writer = ShardStore::open(store_config(&shard)).expect("actual native writer");
        let first = writer.active_path().to_owned();
        for n in 0..3 {
            writer
                .append(reservation(n), None, b"actual owned worker frame", true)
                .expect("actual frame durability");
            if n < 2 {
                writer
                    .seal_active()
                    .expect("real native seal")
                    .expect("nonempty pack");
            }
        }
        drop(writer);
        Self { root, shard, first }
    }
    fn lock(&self) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.shard.join(".shard-store.lock"))
            .expect("actual existing lock")
    }
    fn contended(&self) {
        assert!(matches!(
            DirectoryOwnership::acquire(&self.root),
            Err(DirectoryOwnershipError::Contended)
        ));
        assert!(ShardRecoveryGuard::try_acquire(self.lock(), self.shard.clone()).is_err());
    }
    fn opened(
        &self,
        owner: &Arc<DirectoryOwnership>,
        paid: &NativeDirectoryBudget,
        checks: &HostChecks,
    ) -> OwnedPinnedShardStore {
        let root = owner.root_handle_until(paid).expect("actual bounded root");
        let shards = root
            .open_directory_until(std::ffi::OsStr::new("shards"), false, paid)
            .expect("actual bounded shards directory");
        let shard = shards
            .open_directory_until(std::ffi::OsStr::new("shard-0"), false, paid)
            .expect("actual bounded shard directory");
        let lock = shard
            .open_file_until(
                std::ffi::OsStr::new(".shard-store.lock"),
                OwnedFileAccess::ReadWrite,
                paid,
            )
            .expect("actual native lock handle");
        let acquired = ShardRecoveryGuard::try_acquire_until(
            lock.file()
                .try_clone()
                .expect("actual same-file fixture clone"),
            self.shard.clone(),
            guard_limits(),
            paid.absolute_expiry(),
        )
        .expect("actual acquired guard, no conversion");
        shard
            .pin_shard_directory_until(acquired, pin_limits(), paid)
            .expect("actual Core-derived native pin")
            .open_store_until(store_config(&self.shard), open_limits(), paid, checks)
            .expect("actual bounded native open under same pin")
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
fn partition() -> TopicPartition {
    TopicPartition::new(TopicId::new(11), LogicalPartitionId::new(0))
}
fn reservation(n: u64) -> Reservation {
    Reservation {
        topic_id: partition().topic_id,
        partition_id: partition().partition_id,
        batch_id: BatchId::new(u128::from(n)),
        first_offset: LogicalOffset::new(n),
        last_offset: LogicalOffset::new(n),
        record_count: NonZeroU32::new(1).expect("one record"),
        placement: Placement {
            virtual_lane_id: VirtualLaneId::new(3),
            ring_epoch: RingEpoch::new(1),
            leader_epoch: LeaderEpoch::new(7),
            sequence: PlacementSequence::new(n),
        },
    }
}
fn store_config(directory: &Path) -> ShardStoreConfig {
    ShardStoreConfig {
        directory: directory.to_owned(),
        shard_id: ShardId::new(0),
        target_pack_bytes: 1 << 20,
        max_batch_bytes: 4096,
    }
}
fn config(root: &Path) -> EngineConfig {
    EngineConfig {
        data_dir: root.to_owned(),
        object_store_dir: None,
        shard_count: 1,
        virtual_lane_count: 4,
        replication_factor: 1,
        min_in_sync_replicas: 1,
        queue_slots_per_shard: 16,
        queue_bytes_per_shard: 1 << 20,
        target_pack_bytes: 1 << 20,
        max_pack_age: Duration::from_secs(30),
        max_batch_bytes: 4096,
        max_fetch_bytes: 4096,
        append_linger: Duration::ZERO,
    }
}
fn expiry() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
fn budget(e: Instant) -> NativeDirectoryBudget {
    NativeDirectoryBudget::new(
        NativeDirectoryLimits {
            max_depth: 16,
            max_component_bytes: 4096,
            max_path_bytes: 8192,
            max_operations: 100_000,
            max_calls: 1_000_000,
            max_entries: 10_000_000,
            max_declared_bytes: 1 << 30,
            max_work_bytes: 8usize << 30,
        },
        e,
    )
    .expect("actual cumulative native work; no Session cap qualification")
}
fn guard_limits() -> ShardGuardAcquireLimits {
    ShardGuardAcquireLimits {
        max_path_bytes: 8192,
        max_io_calls: 5,
    }
}
fn pin_limits() -> PinnedShardDirectoryLimits {
    PinnedShardDirectoryLimits {
        max_path_bytes: 8192,
        max_io_calls: 4,
    }
}
fn open_limits() -> PinnedStoreOpenLimits {
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
        max_declared_bytes: 1 << 20,
        max_work_bytes: 1 << 24,
    }
}
fn gc_limits() -> PinnedShardGcLimits {
    PinnedShardGcLimits {
        max_path_bytes: 8192,
        max_name_bytes: 1024,
        max_packs: 16,
        max_batch_slots: 256,
        max_log_start_slots: 256,
        max_topic_slots: 256,
        max_location_slots: 256,
        max_plan_packs: 16,
        max_io_calls: 128,
        max_work_bytes: 1 << 25,
    }
}
fn primary_context(
    paid: Arc<NativeDirectoryBudget>,
    checks: Arc<HostChecks>,
) -> NativePrimaryRecovery {
    NativePrimaryRecovery {
        budget: paid,
        guard_limits: guard_limits(),
        pin_limits: pin_limits(),
        open_limits: open_limits(),
        gc_limits: gc_limits(),
        open_checks: checks.clone(),
        gc_checks: checks,
    }
}
fn recovery_limits() -> NativeRecoveryLimits {
    NativeRecoveryLimits {
        max_files: 100_000,
        max_entries: 100_000_000,
        max_read_bytes: 512 << 20,
        max_declared_bytes: 1 << 30,
        max_work_bytes: 16u64 << 30,
        max_frame_bytes: 1 << 20,
        max_manifest_bytes: 1 << 20,
        max_partial_tail_bytes: 1 << 20,
        max_path_bytes: 8192,
    }
}
fn resume_limits() -> NativeResumeLimits {
    NativeResumeLimits {
        max_partitions: 16,
        max_fetch_passes: 32,
        max_batches: 64,
        max_atomic_groups: 64,
        max_callbacks: 64,
        max_jobs: 16,
        max_steps: 64,
        max_entries: 1_000_000,
        max_declared_bytes: 1 << 28,
        max_work_bytes: 1 << 30,
        fetch: NativeFetchLimits {
            max_plan_batches_per_shard: 128,
            max_selected_batches: 16,
            max_candidate_window: 1024,
            max_entries: 1_000_000,
            max_path_bytes: 8192,
            max_read_attempts: 4096,
            max_read_bytes: 1 << 24,
            max_declared_bytes: 1 << 26,
            max_work_bytes: 1 << 30,
        },
    }
}

struct SyncBarrier {
    identity: (u64, u64),
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}
static SYNC_BARRIER: Mutex<Option<Arc<SyncBarrier>>> = Mutex::new(None);
struct ReleaseSync;
impl Drop for ReleaseSync {
    fn drop(&mut self) {
        if let Some(barrier) = SYNC_BARRIER
            .lock()
            .expect("actual test registration")
            .take()
        {
            barrier.state.lock().expect("actual release").1 = true;
            barrier.changed.notify_all();
        }
    }
}
pub(super) fn before_sync(file: &File) {
    let stat = file.metadata().expect("real sync request identity");
    let barrier = SYNC_BARRIER
        .lock()
        .expect("test registration")
        .as_ref()
        .filter(|v| v.identity == (stat.dev(), stat.ino()))
        .cloned();
    if let Some(barrier) = barrier {
        let mut state = barrier.state.lock().expect("actual physical request");
        state.0 = true;
        barrier.changed.notify_all();
        while !state.1 {
            state = barrier.changed.wait(state).expect("actual sync release");
        }
    }
}

#[test]
fn actual_sync_worker_keeps_native_pin_after_lane_store_and_lane_thread_are_gone() {
    let temp = Temporary::new();
    let owner = DirectoryOwnership::acquire(&temp.root).expect("real root");
    let weak = Arc::downgrade(&owner);
    let paid = Arc::new(budget(expiry()));
    let checks = Arc::new(HostChecks::new());
    let store = temp.opened(&owner, &paid, &checks);
    let stat = fs::metadata(store.store().active_path()).expect("actual active pack identity");
    let (mut handle, _) = ShardHandle::spawn_recovered_primary(
        &config(&temp.root),
        ShardId::new(0),
        PrimaryStore::Owned(store),
        None,
        owner.clone(),
        paid.absolute_expiry(),
        Some(NativeResumeGcPolicy {
            budget: paid.clone(),
            limits: gc_limits(),
            checks: checks.clone(),
        }),
    )
    .expect("real recovered physical workers");
    let barrier = Arc::new(SyncBarrier {
        identity: (stat.dev(), stat.ino()),
        state: Mutex::new((false, false)),
        changed: Condvar::new(),
    });
    *SYNC_BARRIER.lock().expect("register actual inode") = Some(barrier.clone());
    let release = ReleaseSync;
    let completion = handle.begin_sync().expect("actual sync command");
    let state = barrier.state.lock().expect("actual request barrier");
    let (state, wait) = barrier
        .changed
        .wait_timeout_while(state, Duration::from_secs(5), |v| !v.0)
        .expect("actual request entry");
    assert!(state.0 && !wait.timed_out());
    drop(state);
    drop(paid);
    drop(owner);
    handle.sender.take();
    handle
        .thread
        .take()
        .expect("real lane thread")
        .join()
        .expect("physical lane return");
    // The actual lane store and its holder have now been destroyed. The real
    // sync File remains blocked in its separate physical frame.
    temp.contended();
    assert!(weak.upgrade().is_some());
    assert!(matches!(
        completion.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    drop(release);
    completion
        .recv()
        .expect("actual response")
        .expect("real fsync");
    drop(handle);
    assert!(weak.upgrade().is_none());
    DirectoryOwnership::acquire(&temp.root).expect("last physical sync worker returned");
    ShardRecoveryGuard::try_acquire(temp.lock(), temp.shard.clone())
        .expect("last actual native pin released");
}

#[test]
fn actual_gc_report_retains_same_native_pin_after_all_workers_physically_join() {
    let temp = Temporary::new();
    let owner = DirectoryOwnership::acquire(&temp.root).expect("real root");
    let weak = Arc::downgrade(&owner);
    let paid = Arc::new(budget(expiry()));
    let checks = Arc::new(HostChecks::new());
    let store = temp.opened(&owner, &paid, &checks);
    let (handle, _) = ShardHandle::spawn_recovered_primary(
        &config(&temp.root),
        ShardId::new(0),
        PrimaryStore::Owned(store),
        None,
        owner.clone(),
        paid.absolute_expiry(),
        Some(NativeResumeGcPolicy {
            budget: paid.clone(),
            limits: gc_limits(),
            checks: checks.clone(),
        }),
    )
    .expect("actual owned workers");
    let policy = NativeResumeGcPolicy {
        budget: paid.clone(),
        limits: gc_limits(),
        checks: checks.clone(),
    };
    let mut ticket = handle
        .begin_owned_gc(
            partition(),
            LogicalOffset::new(2),
            &policy,
            paid.absolute_expiry(),
        )
        .expect("actual command");
    let outcome = ticket.settle().expect("actual physical GC completion");
    assert!(outcome.accepted());
    assert_eq!(
        outcome
            .native_report()
            .expect("actual native report")
            .stats()
            .packs_removed,
        2
    );
    assert!(!temp.first.exists());
    drop(ticket);
    drop(handle);
    drop(owner);
    drop(paid);
    drop(policy);
    temp.contended();
    assert!(weak.upgrade().is_some());
    assert!(!checks.denied.load(Ordering::Acquire));
    drop(outcome);
    assert!(weak.upgrade().is_none());
    DirectoryOwnership::acquire(&temp.root).expect("actual report last-drop releases root");
    ShardRecoveryGuard::try_acquire(temp.lock(), temp.shard.clone())
        .expect("actual report last-drop releases pin");
}

#[test]
fn actual_consuming_gc_refusal_owns_store_and_vacant_lane_refuses_later_commands() {
    let temp = Temporary::new();
    let owner = DirectoryOwnership::acquire(&temp.root).expect("real root");
    let weak = Arc::downgrade(&owner);
    let paid = Arc::new(budget(expiry()));
    let checks = Arc::new(HostChecks::new());
    let store = temp.opened(&owner, &paid, &checks);
    let (handle, _) = ShardHandle::spawn_recovered_primary(
        &config(&temp.root),
        ShardId::new(0),
        PrimaryStore::Owned(store),
        None,
        owner.clone(),
        paid.absolute_expiry(),
        Some(NativeResumeGcPolicy {
            budget: paid.clone(),
            limits: gc_limits(),
            checks: checks.clone(),
        }),
    )
    .expect("actual owned workers");
    fs::remove_file(&temp.first).expect("real selected-pack disappearance");
    let policy = NativeResumeGcPolicy {
        budget: paid.clone(),
        limits: gc_limits(),
        checks: checks.clone(),
    };
    let mut ticket = handle
        .begin_owned_gc(
            partition(),
            LogicalOffset::new(2),
            &policy,
            paid.absolute_expiry(),
        )
        .expect("actual queued GC");
    let outcome = ticket.settle().expect("actual refusal response");
    assert!(!outcome.accepted());
    assert!(outcome.native_error().is_some());
    assert!(checks.denied.load(Ordering::Acquire));
    handle
        .begin_sync()
        .expect("actual next command")
        .recv()
        .expect("actual vacant lane reply")
        .expect_err("no reconstruction or pathname reopen");
    drop(ticket);
    drop(handle);
    drop(owner);
    drop(paid);
    drop(policy);
    temp.contended();
    assert!(weak.upgrade().is_some());
    drop(outcome);
    assert!(weak.upgrade().is_none());
}

#[test]
fn actual_census_acquires_core_primary_and_retains_late_native_acquisition_error() {
    for short in [false, true] {
        let temp = Temporary::new();
        let owner = DirectoryOwnership::acquire(&temp.root).expect("real root");
        let weak = Arc::downgrade(&owner);
        let paid = Arc::new(budget(expiry()));
        let checks = Arc::new(HostChecks::new());
        let mut context = primary_context(paid.clone(), checks.clone());
        if short {
            context.guard_limits.max_io_calls = 4;
        }
        let bytes = fs::read(&temp.first).expect("actual native bytes");
        let result = NativeRecoveryCensus::read_for_engine_with_primary(
            config(&temp.root),
            owner.clone(),
            None,
            recovery_limits(),
            paid.absolute_expiry() + Duration::from_secs(1),
            context,
        );
        drop(owner);
        if short {
            let error = result.expect_err("actual post-lock native validation allowance refuses");
            let guard = error
                .primary_outcomes()
                .and_then(|v| v.guard_error())
                .expect("original typed acquired-lock error");
            assert!(matches!(guard.observation().lock_result(), Some(Ok(()))));
            assert_eq!(
                fs::read(&temp.first).expect("unchanged native source"),
                bytes
            );
            temp.contended();
            assert!(checks.denied.load(Ordering::Acquire));
            drop(error);
            assert!(weak.upgrade().is_none());
        } else {
            let census = result.expect("actual additive census");
            assert_eq!(census.absolute_expiry(), paid.absolute_expiry());
            let storage = census
                .restore_storage()
                .expect("same acquired guard to actual Core pin/open");
            let mut storage = storage;
            assert!(
                storage
                    .primary(ShardId::new(0))
                    .expect("actual borrowed native store")
                    .contains(&reservation(0))
            );
            temp.contended();
            drop(storage);
            assert!(weak.upgrade().is_none());
        }
    }
}

struct ResetStartHook;
impl Drop for ResetStartHook {
    fn drop(&mut self) {
        RECOVERED_OPEN_STEP.with(|v| *v.borrow_mut() = None);
    }
}
#[test]
fn actual_partial_start_retains_store_and_threads_through_expiry_and_unwind() {
    for (target, panic) in [
        ("before", false),
        ("sync-spawned", false),
        ("sync-spawned", true),
        ("lane-spawned", true),
        ("budget-after-sync", false),
    ] {
        let temp = Temporary::new();
        let owner = DirectoryOwnership::acquire(&temp.root).expect("real root");
        let weak = Arc::downgrade(&owner);
        let paid = Arc::new(budget(expiry()));
        let checks = Arc::new(HostChecks::new());
        let store = temp.opened(&owner, &paid, &checks);
        let shorter = Instant::now() + Duration::from_secs(1);
        let e = if target == "before" {
            Instant::now()
        } else if target == "budget-after-sync" {
            paid.absolute_expiry()
        } else {
            shorter
        };
        let setup_budget = paid.clone();
        RECOVERED_OPEN_STEP.with(|v| {
            *v.borrow_mut() = Some(Box::new(move |phase| {
                if phase == target || (target == "budget-after-sync" && phase == "sync-spawned") {
                    if panic {
                        std::panic::panic_any(io::Error::other(
                            "actual physical start phase panic",
                        ));
                    }
                    if target == "budget-after-sync" {
                        setup_budget
                            .cap_expiry(shorter)
                            .expect("tighten SAME budget while original census E still fits");
                    }
                    thread::sleep(
                        shorter.saturating_duration_since(Instant::now())
                            + Duration::from_millis(5),
                    );
                }
            }))
        });
        let reset = ResetStartHook;
        let error = match ShardHandle::spawn_recovered_primary(
            &config(&temp.root),
            ShardId::new(0),
            PrimaryStore::Owned(store),
            None,
            owner.clone(),
            e,
            Some(NativeResumeGcPolicy {
                budget: paid.clone(),
                limits: gc_limits(),
                checks: checks.clone(),
            }),
        ) {
            Ok(_) => panic!("real earlier phase must prevent further startup"),
            Err(error) => error,
        };
        if panic {
            assert!(
                error
                    .panic_payload()
                    .and_then(|v| v.downcast_ref::<io::Error>())
                    .is_some()
            );
        }
        if target == "budget-after-sync" {
            assert!(Instant::now() < e);
            assert!(error.budget_error().is_some());
            assert!(!error.lane_spawned());
            assert_eq!(checks.denies.load(Ordering::Acquire), 1);
        }
        drop(owner);
        drop(paid);
        temp.contended();
        assert!(weak.upgrade().is_some());
        drop(reset);
        drop(error);
        assert!(weak.upgrade().is_none());
        ShardRecoveryGuard::try_acquire(temp.lock(), temp.shard.clone())
            .expect("actual partial-start workers physically joined");
    }
}

struct GcBarrier {
    path: PathBuf,
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}
static GC_BARRIER: Mutex<Option<Arc<GcBarrier>>> = Mutex::new(None);
struct ReleaseGc;
impl Drop for ReleaseGc {
    fn drop(&mut self) {
        if let Some(barrier) = GC_BARRIER.lock().expect("actual GC registration").take() {
            barrier.state.lock().expect("actual GC release").1 = true;
            barrier.changed.notify_all();
        }
    }
}
pub(super) fn before_gc(path: Option<&Path>) {
    let barrier = GC_BARRIER
        .lock()
        .expect("actual GC registry")
        .as_ref()
        .filter(|v| Some(v.path.as_path()) == path)
        .cloned();
    if let Some(barrier) = barrier {
        let mut state = barrier.state.lock().expect("actual GC queue entry");
        state.0 = true;
        barrier.changed.notify_all();
        while !state.1 {
            state = barrier
                .changed
                .wait(state)
                .expect("actual GC queue release");
        }
    }
}
#[test]
fn actual_queued_gc_preserves_original_expiry_and_denies_before_any_native_collection() {
    let temp = Temporary::new();
    let owner = DirectoryOwnership::acquire(&temp.root).expect("real root");
    let paid = Arc::new(budget(expiry()));
    let checks = Arc::new(HostChecks::new());
    let store = temp.opened(&owner, &paid, &checks);
    let (handle, _) = ShardHandle::spawn_recovered_primary(
        &config(&temp.root),
        ShardId::new(0),
        PrimaryStore::Owned(store),
        None,
        owner.clone(),
        paid.absolute_expiry(),
        Some(NativeResumeGcPolicy {
            budget: paid.clone(),
            limits: gc_limits(),
            checks: checks.clone(),
        }),
    )
    .expect("actual owned workers");
    let bytes = fs::read(&temp.first).expect("actual original pack bytes");
    let calls = paid.usage().expect("same original counters").calls;
    let barrier = Arc::new(GcBarrier {
        path: temp.shard.clone(),
        state: Mutex::new((false, false)),
        changed: Condvar::new(),
    });
    *GC_BARRIER.lock().expect("register actual path") = Some(barrier.clone());
    let release = ReleaseGc;
    let e = Instant::now() + Duration::from_secs(1);
    let policy = NativeResumeGcPolicy {
        budget: paid.clone(),
        limits: gc_limits(),
        checks: checks.clone(),
    };
    let mut ticket = handle
        .begin_owned_gc(partition(), LogicalOffset::new(2), &policy, e)
        .expect("real queued command");
    let state = barrier.state.lock().expect("actual dequeue");
    let (state, wait) = barrier
        .changed
        .wait_timeout_while(state, Duration::from_secs(3), |v| !v.0)
        .expect("actual worker entry");
    assert!(state.0 && !wait.timed_out());
    drop(state);
    thread::sleep(e.saturating_duration_since(Instant::now()) + Duration::from_millis(5));
    drop(release);
    let outcome = ticket.settle().expect("actual physical worker refusal");
    assert!(!outcome.accepted());
    assert!(outcome.native_report().is_none());
    assert!(outcome.budget_error().is_some());
    assert!(checks.denied.load(Ordering::Acquire));
    assert_eq!(
        paid.usage().expect("same counters after refusal").calls,
        calls
    );
    assert_eq!(fs::read(&temp.first).expect("unmodified real pack"), bytes);
    assert!(!temp.first.with_extension("sse.gc-pending").exists());
    drop(ticket);
    drop(outcome);
    drop(handle);
    drop(policy);
    drop(owner);
    DirectoryOwnership::acquire(&temp.root).expect("real joined workers release original root");
}

struct RenameChecks {
    first: PathBuf,
    pending: PathBuf,
    late: Mutex<Option<Instant>>,
    observed_rename: AtomicBool,
    denied: AtomicBool,
    denies: AtomicUsize,
    denied_with_pending: AtomicBool,
}
impl ShardDirectoryChecks for RenameChecks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        if self.denied.load(Ordering::Acquire) {
            return Err(ShardDirectoryCheckFailure::Refused);
        }
        if !self.first.exists()
            && self.pending.exists()
            && !self.observed_rename.swap(true, Ordering::AcqRel)
        {
            if let Some(e) = *self.late.lock().expect("actual scoped rename callback") {
                thread::sleep(
                    e.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
                );
            }
        }
        Ok(())
    }
}
impl PinnedShardGcChecks for RenameChecks {
    fn deny(&self) {
        self.denies.fetch_add(1, Ordering::AcqRel);
        self.denied_with_pending
            .store(self.pending.exists(), Ordering::Release);
        self.denied.store(true, Ordering::Release);
    }
}

#[test]
fn actual_engine_gc_uses_same_resume_work_and_retains_late_rename_settlement() {
    for case in ["fit", "before-entry", "job-cap", "late-rename"] {
        let temp = Temporary::new();
        let owner = DirectoryOwnership::acquire(&temp.root).expect("real root");
        let weak = Arc::downgrade(&owner);
        let paid = Arc::new(budget(expiry()));
        let open_checks = Arc::new(HostChecks::new());
        let checks = Arc::new(RenameChecks {
            first: temp.first.clone(),
            pending: temp.first.with_extension("sse.gc-pending"),
            late: Mutex::new(None),
            observed_rename: AtomicBool::new(false),
            denied: AtomicBool::new(false),
            denies: AtomicUsize::new(0),
            denied_with_pending: AtomicBool::new(false),
        });
        let mut primary = primary_context(paid.clone(), open_checks);
        primary.gc_checks = checks.clone();
        let storage = NativeRecoveryCensus::read_for_engine_with_primary(
            config(&temp.root),
            owner.clone(),
            None,
            recovery_limits(),
            paid.absolute_expiry(),
            primary,
        )
        .expect("actual acquired guard/census")
        .restore_storage()
        .expect("actual consuming Core open");
        let engine = StreamEngine::open_recovered_storage(storage, None, None, None, None)
            .expect("actual owned workers without fabricated Engine fields");
        let handle = EngineHandle::from_recovered(Arc::new(engine));
        drop(owner);
        let e = if case == "before-entry" {
            Instant::now()
        } else {
            Instant::now() + Duration::from_secs(2)
        };
        if case == "late-rename" {
            *checks.late.lock().expect("arm actual completed rename") = Some(e);
        }
        let mut limits = resume_limits();
        if case == "job-cap" {
            limits.max_jobs = 0;
        }
        let policy = NativeResumeGcPolicy {
            budget: paid.clone(),
            limits: gc_limits(),
            checks: checks.clone(),
        };
        let result =
            handle.collect_garbage_until(partition(), LogicalOffset::new(2), policy, limits, e);
        if case == "fit" {
            let report = result.expect("actual same-lane native collection");
            assert_eq!(report.usage().jobs, 1);
            assert!(
                matches!(report.outcomes(),[NativeResumeOutcome::GarbageCollected {result:Ok(outcome),refusal:None,..}] if outcome.accepted())
            );
            assert_eq!(checks.denies.load(Ordering::Acquire), 0);
            drop(handle);
            drop(paid);
            temp.contended();
            assert!(weak.upgrade().is_some());
            drop(report);
        } else {
            let error = result.expect_err("actual saved E or immutable job allowance refusal");
            assert_eq!(checks.denies.load(Ordering::Acquire), 1);
            assert!(checks.denied.load(Ordering::Acquire));
            assert!(paid.check().is_err());
            if case == "late-rename" {
                assert_eq!(
                    error.usage().expect("same retained resume counters").jobs,
                    1
                );
                let outcome = match error.outcomes() {
                    [
                        NativeResumeOutcome::GarbageCollected {
                            result: Ok(outcome),
                            ..
                        },
                    ] => outcome,
                    other => panic!("actual settled native outcome required: {other:?}"),
                };
                let native = outcome
                    .native_report()
                    .expect("actual partial native report");
                let pack = native.pack(0).expect("actual first selected pack");
                assert!(matches!(pack.rename_result(), Some(Ok(()))));
                assert!(matches!(pack.rename_sync_result(), Some(Ok(()))));
                assert!(pack.unlink_result().is_none());
                assert!(checks.denied_with_pending.load(Ordering::Acquire));
                assert_eq!(native.absolute_expiry(), e);
            } else {
                assert!(error.outcomes().is_empty());
                assert!(temp.first.exists());
                assert_eq!(error.usage().expect("same pre-entry counters").jobs, 0);
            }
            drop(handle);
            drop(paid);
            temp.contended();
            assert!(weak.upgrade().is_some());
            drop(error);
        }
        assert!(weak.upgrade().is_none());
        DirectoryOwnership::acquire(&temp.root)
            .expect("all actual engine work and report owners dropped");
        ShardRecoveryGuard::try_acquire(temp.lock(), temp.shard.clone())
            .expect("actual native pin last-drop");
    }
}

#[test]
fn actual_gc_command_fee_one_less_refuses_before_queue_and_exact_fee_retains_core_refusal() {
    for short in [false, true] {
        let temp = Temporary::new();
        let owner = DirectoryOwnership::acquire(&temp.root).expect("real root");
        let paid = Arc::new(budget(expiry()));
        let checks = Arc::new(HostChecks::new());
        let store = temp.opened(&owner, &paid, &checks);
        let (handle, _) = ShardHandle::spawn_recovered_primary(
            &config(&temp.root),
            ShardId::new(0),
            PrimaryStore::Owned(store),
            None,
            owner.clone(),
            paid.absolute_expiry(),
            Some(NativeResumeGcPolicy {
                budget: paid.clone(),
                limits: gc_limits(),
                checks: checks.clone(),
            }),
        )
        .expect("actual owned physical workers");
        let before = paid.usage().expect("same immutable budget");
        let fee = gc::command_cost().expect("checked source-derived real command fee");
        let available = fee - usize::from(short);
        let prepaid = paid.limits().max_declared_bytes - before.declared_bytes - available;
        // A real ordinary reservation may remain unused. Preserve all fees;
        // do not create a fresh budget with a reset counter for this command.
        paid.reserve(0, 0, prepaid, 0, 0, prepaid)
            .expect("prepay all but the actual remaining command allowance");
        let policy = NativeResumeGcPolicy {
            budget: paid.clone(),
            limits: gc_limits(),
            checks: checks.clone(),
        };
        let result = handle.begin_owned_gc(
            partition(),
            LogicalOffset::new(2),
            &policy,
            paid.absolute_expiry(),
        );
        if short {
            let error = match result {
                Ok(_) => panic!("one-less command fee must refuse before dispatch"),
                Err(error) => error,
            };
            assert!(error.native_error().is_none());
            assert!(error.budget_error().is_some());
            drop(error);
            assert_eq!(
                paid.usage()
                    .expect("no partial fee adoption")
                    .declared_bytes,
                before.declared_bytes + prepaid
            );
        } else {
            let mut ticket = result.expect("exact queue fee reaches the actual lane");
            let error = ticket
                .settle()
                .expect("actual Core refusal physically settled");
            assert!(error.native_error().is_some());
            assert_eq!(
                paid.usage()
                    .expect("complete queue fee retained")
                    .declared_bytes,
                paid.limits().max_declared_bytes
            );
            drop(error);
            drop(ticket);
        }
        assert_eq!(
            paid.usage()
                .expect("no Core native call after admission refusal")
                .calls,
            before.calls
        );
        assert_eq!(checks.denies.load(Ordering::Acquire), 1);
        assert!(temp.first.exists());
        drop(handle);
        drop(policy);
        drop(paid);
        drop(owner);
        DirectoryOwnership::acquire(&temp.root).expect("real command/refusal cleanup joined");
    }
}

#[test]
fn actual_ordinary_worker_gc_diagnostic_exact_and_one_less_fees_precede_queue_entry() {
    for axis in ["declared", "work"] {
        for short in [false, true] {
            let temp = Temporary::new();
            let owner = DirectoryOwnership::acquire(&temp.root).expect("real ordinary root");
            let weak = Arc::downgrade(&owner);
            let (handle, catalog) = ShardHandle::spawn_with_ownership(
                &config(&temp.root),
                ShardId::new(0),
                Some(owner.clone()),
            )
            .expect("genuine ordinary native store and physical lane/sync workers");
            assert_eq!(catalog.len(), 3);
            drop(owner);
            let original = fs::read(&temp.first).expect("actual unchanged pack input");
            let e = expiry();
            let paid = Arc::new(budget(e));
            let checks = Arc::new(HostChecks::new());
            let policy = NativeResumeGcPolicy {
                budget: paid.clone(),
                limits: gc_limits(),
                checks: checks.clone(),
            };
            let original_limits = paid.limits();
            let fee = gc::command_cost().expect("checked actual command values and diagnostic");
            let diagnostic = gc::COMMAND_DIAGNOSTIC_BYTES;
            assert_eq!(diagnostic, 60);
            assert!(diagnostic >= "native GC command arithmetic refused".len());
            let work_fee = fee
                .checked_add(gc::COMMAND_ENTRIES)
                .expect("actual entry work");
            let available = (if axis == "declared" { fee } else { work_fee })
                .checked_sub(usize::from(short))
                .expect("actual positive fee boundary");
            // Consume ordinary cumulative allowance without changing immutable
            // limits, spent usage or E. This is not a private Session fixture.
            if axis == "declared" {
                let prepaid = original_limits
                    .max_declared_bytes
                    .checked_sub(available)
                    .expect("finite remaining declared allowance");
                paid.reserve(0, 0, prepaid, 0, 0, 0)
                    .expect("actual ordinary reservation");
            } else {
                let prepaid = original_limits
                    .max_work_bytes
                    .checked_sub(available)
                    .expect("finite remaining work allowance");
                paid.reserve(0, 0, 0, 0, 0, prepaid)
                    .expect("actual ordinary work reservation");
            }
            let before = paid
                .usage()
                .expect("same cumulative counters before command");
            let result = handle.begin_owned_gc(partition(), LogicalOffset::new(2), &policy, e);
            let outcome = if short {
                let outcome = match result {
                    Ok(_) => panic!("unpaid diagnostic command must refuse before queue entry"),
                    Err(outcome) => outcome,
                };
                assert!(matches!(
                    outcome.budget_error(),
                    Some(shard_stream_core::NativeDirectoryFailure::Capacity)
                ));
                assert!(outcome.error().is_none());
                assert_eq!(paid.usage().expect("no partial fee adoption"), before);
                outcome
            } else {
                let mut ticket = result.expect("exact actual fees admit the real queue command");
                let outcome = ticket
                    .settle()
                    .expect("actual ordinary lane error physically settled");
                assert!(
                    matches!(outcome.error(), Some(EngineError::InvalidConfig(message)) if message == "bounded consuming GC requires the actual owned primary store" && message.len() == diagnostic)
                );
                assert!(outcome.budget_error().is_none());
                let after = paid.usage().expect("actual diagnostic request retained");
                assert_eq!(after.declared_bytes, before.declared_bytes + fee);
                assert_eq!(after.string_bytes, before.string_bytes + diagnostic);
                assert_eq!(after.entries, before.entries + gc::COMMAND_ENTRIES);
                assert_eq!(after.work_bytes, before.work_bytes + work_fee);
                assert_eq!(after.calls, before.calls);
                assert_eq!(after.operations, before.operations);
                drop(ticket);
                outcome
            };
            assert!(outcome.native_error().is_none() && outcome.native_report().is_none());
            assert_eq!(paid.usage().expect("no Core producer entry").calls, 0);
            assert_eq!(paid.absolute_expiry(), e);
            assert_eq!(paid.limits(), original_limits);
            assert!(paid.check().is_err());
            assert_eq!(checks.denies.load(Ordering::Acquire), 1);
            assert_eq!(
                fs::read(&temp.first).expect("same native pack remains"),
                original
            );
            temp.contended();
            assert!(weak.upgrade().is_some());
            drop(handle);
            assert!(weak.upgrade().is_none());
            let released = DirectoryOwnership::acquire(&temp.root)
                .expect("actual ordinary worker and sync cleanup release root");
            let lease = ShardRecoveryGuard::try_acquire(temp.lock(), temp.shard.clone())
                .expect("actual ordinary native lease released after physical joins");
            // The real wrong-kind/pre-entry outcome stays alive while the
            // ordinary worker root and store are demonstrably released.
            assert!(outcome.native_report().is_none());
            drop(lease);
            drop(released);
            drop(outcome);
        }
    }
}
