//! Genuine native writer/open/stat/pread/lease regression source.
//! Uncompiled and unexecuted until the sole normal validation owner runs it.
use super::*;
use crate::{PinnedShardDirectoryLimits, ShardGuardAcquireLimits, ShardRecoveryGuard};
use std::{
    cell::{Cell, RefCell},
    num::NonZeroU32,
    os::unix::fs::symlink,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

type Observer = Box<dyn FnMut(PinnedShardFetchPhase)>;
thread_local! {static OBSERVER:RefCell<Option<Observer>>=RefCell::new(None);}
pub(super) fn completed(phase: PinnedShardFetchPhase) {
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
fn observe(callback: impl FnMut(PinnedShardFetchPhase) + 'static) -> Observe {
    OBSERVER.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(callback));
    });
    Observe
}
struct Checks {
    denied: Cell<bool>,
    deny_calls: Cell<usize>,
    panic_deny: bool,
}
impl Checks {
    fn new() -> Self {
        Self {
            denied: Cell::new(false),
            deny_calls: Cell::new(0),
            panic_deny: false,
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
impl PinnedShardFetchChecks for Checks {
    fn deny(&self) {
        self.denied.set(true);
        self.deny_calls.set(self.deny_calls.get() + 1);
        if self.panic_deny {
            std::panic::panic_any(io::Error::other("real composing read denial unwind"));
        }
    }
}
impl PinnedStoreOpenChecks for Checks {
    fn deny(&self) {
        self.denied.set(true);
    }
}
struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "native-pinned-read-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("real test directory");
        Self(path)
    }
    fn lock(&self) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.0.join(".shard-store.lock"))
            .expect("real competing lease")
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
fn expiry() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
fn native_limits() -> StorageFetchLimits {
    StorageFetchLimits {
        max_plan_batches: 1024,
        max_entries: 1 << 20,
        max_path_bytes: 8192,
        max_read_attempts: 4096,
        max_read_bytes: 1 << 24,
        max_declared_bytes: 1 << 27,
        max_work_bytes: 1 << 30,
    }
}
fn budget(limits: StorageFetchLimits) -> StorageFetchBudget {
    StorageFetchBudget::new(limits, expiry()).expect("actual finite counters")
}
fn open_limits() -> PinnedStoreOpenLimits {
    PinnedStoreOpenLimits {
        max_directory_visits: 2048,
        directory_buffer_bytes: 8192,
        max_name_bytes: 1024,
        max_path_bytes: 8192,
        max_packs: 256,
        max_frames: 2048,
        max_frame_bytes: 8192,
        max_read_attempts: 8192,
        max_read_bytes: 1 << 27,
        max_index_entries: 4096,
        max_repair_calls: 1024,
        max_io_calls: 16384,
        max_outcomes: 32768,
        max_entries: 1 << 22,
        max_declared_bytes: 1 << 29,
        max_work_bytes: 1 << 30,
    }
}
fn topic(id: u128) -> TopicPartition {
    TopicPartition::new(TopicId::new(id), LogicalPartitionId::new(0))
}
fn reservation(id: u64, topic_id: u128) -> Reservation {
    Reservation {
        topic_id: TopicId::new(topic_id),
        partition_id: LogicalPartitionId::new(0),
        batch_id: BatchId::new(u128::from(id)),
        first_offset: LogicalOffset::new(id),
        last_offset: LogicalOffset::new(id),
        record_count: NonZeroU32::new(1).expect("real single record"),
        placement: Placement {
            virtual_lane_id: VirtualLaneId::new(0),
            ring_epoch: RingEpoch::new(1),
            leader_epoch: LeaderEpoch::new(1),
            sequence: PlacementSequence::new(id),
        },
    }
}
struct Fixture {
    pin: PinnedShardDirectory,
    config: ShardStoreConfig,
    root: Root,
}
impl Fixture {
    fn new(payloads: &[&[u8]], separate: bool, many_topics: bool) -> Self {
        let root = Root::new();
        let config = ShardStoreConfig {
            directory: root.0.clone(),
            shard_id: ShardId::new(0),
            target_pack_bytes: 1 << 20,
            max_batch_bytes: 4096,
        };
        let mut writer = ShardStore::open(config.clone()).expect("actual ordinary writer");
        for (n, payload) in payloads.iter().enumerate() {
            writer
                .append(
                    reservation(n as u64, if many_topics { n as u128 + 1 } else { 1 }),
                    None,
                    payload,
                    true,
                )
                .expect("actual synced batch");
            if separate && n + 1 < payloads.len() {
                writer
                    .seal_active()
                    .expect("actual seal")
                    .expect("nonempty pack");
            }
        }
        drop(writer);
        let guard = ShardRecoveryGuard::try_acquire_until(
            root.lock(),
            root.0.clone(),
            ShardGuardAcquireLimits {
                max_path_bytes: 8192,
                max_io_calls: 5,
            },
            expiry(),
        )
        .expect("real acquired lease");
        let pin = PinnedShardDirectory::from_descriptor_until(
            File::open(&root.0).expect("real parent FD"),
            guard,
            &root.0,
            PinnedShardDirectoryLimits {
                max_path_bytes: 8192,
                max_io_calls: 4,
            },
            expiry(),
            &Checks::new(),
        )
        .expect("actual joined pin");
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
        .expect("actual bounded scan")
    }
    fn path(&self, sequence: u64) -> PathBuf {
        pack_path(&self.root.0, sequence)
    }
}
fn plan(
    opened: &OpenedPinnedShardStore,
    budget: &StorageFetchBudget,
    checks: &Checks,
) -> PinnedShardFetchPlan {
    opened
        .plan_fetch_pinned_until(topic(1), LogicalOffset::new(0), 4096, 128, budget, checks)
        .expect("actual selected metadata")
}
fn reads(observation: &PinnedShardFetchObservation) -> usize {
    observation
        .outcomes()
        .iter()
        .filter(|o| matches!(o, PinnedShardFetchOutcome::Read { .. }))
        .count()
}

#[test]
fn genuine_empty_multi_pack_zero_payload_and_first_oversize_match_ordinary_reads() {
    for payloads in [
        vec![],
        vec![b"".as_slice(), b"".as_slice()],
        vec![
            b"first real payload".as_slice(),
            b"second real payload".as_slice(),
        ],
    ] {
        for separate in [false, true] {
            let fixture = Fixture::new(&payloads, separate, false);
            let mut opened = fixture.opened();
            let ordinary = opened
                .store
                .plan_fetch(topic(1), LogicalOffset::new(0), 1, 128)
                .expect("ordinary first-oversize policy");
            let expected = opened
                .store
                .read_fetch_plan(&ordinary)
                .expect("ordinary complete payloads");
            let b = budget(native_limits());
            let c = Checks::new();
            let p = opened
                .plan_fetch_pinned_until(topic(1), LogicalOffset::new(0), 1, 128, &b, &c)
                .expect("real strict plan");
            assert_eq!(p.planned(), ordinary);
            let actual = opened
                .read_fetch_plan_pinned_until(&fixture.pin, p, &b, &c)
                .expect("real held descriptor read");
            assert_eq!(actual.batches(), expected);
            assert!(!c.denied.get());
            fixture.root.contended();
        }
    }
}
#[test]
fn genuine_leading_zero_pack_names_use_actual_scanned_descriptor_and_spelling() {
    let fixture = Fixture::new(&[b"first", b"second"], true, false);
    let original = fixture.path(0);
    let renamed = fixture.root.0.join(format!("pack-{}0.sse", "0".repeat(96)));
    fs::rename(&original, &renamed).expect("actual retained leading-zero name");
    let opened = fixture.opened();
    let b = budget(native_limits());
    let c = Checks::new();
    let read = opened
        .read_fetch_plan_pinned_until(&fixture.pin, plan(&opened, &b, &c), &b, &c)
        .expect("same scanned leading-zero file");
    assert_eq!(read.batches().len(), 2);
    assert!(
        read.observation()
            .pack_name(0)
            .expect("actual native CStr")
            .to_bytes()
            .starts_with(b"pack-000000")
    );
    assert!(!original.exists());
    fixture.root.contended();
}
#[test]
fn genuine_other_store_token_and_actual_gc_epoch_refuse_before_any_read() {
    let first = Fixture::new(&[b"a", b"b"], true, false);
    let other = Fixture::new(&[b"a"], false, false);
    let mut opened = first.opened();
    let second = other.opened();
    let b = budget(native_limits());
    let c = Checks::new();
    let p = plan(&opened, &b, &c);
    let error = second
        .read_fetch_plan_pinned_until(&other.pin, p, &b, &c)
        .expect_err("different actual store token");
    assert!(matches!(
        error.observation().failure(),
        Some(PinnedShardFetchFailure::Stale)
    ));
    assert_eq!(reads(error.observation()), 0);
    let b = budget(native_limits());
    let c = Checks::new();
    let p = plan(&opened, &b, &c);
    let wrong_pin = opened
        .read_fetch_plan_pinned_until(&other.pin, p, &b, &c)
        .expect_err("actual wrong pin cannot accompany this store");
    assert!(matches!(
        wrong_pin.observation().failure(),
        Some(PinnedShardFetchFailure::Identity)
    ));
    assert_eq!(reads(wrong_pin.observation()), 0);
    let b = budget(native_limits());
    let c = Checks::new();
    let p = plan(&opened, &b, &c);
    opened
        .store_mut()
        .garbage_collect(&HashMap::from([(topic(1), LogicalOffset::new(1))]))
        .expect("actual ordinary GC epoch transition");
    let error = opened
        .read_fetch_plan_pinned_until(&first.pin, p, &b, &c)
        .expect_err("real stale epoch");
    assert!(matches!(
        error.observation().failure(),
        Some(PinnedShardFetchFailure::Stale)
    ));
    assert_eq!(reads(error.observation()), 0);
}
#[test]
fn genuine_untracked_active_append_and_rotation_never_reopen_or_fallback() {
    for rotate in [false, true] {
        let fixture = Fixture::new(&[b"first"], false, false);
        let mut opened = fixture.opened();
        if rotate {
            opened
                .store_mut()
                .seal_active()
                .expect("real ordinary rotation");
        }
        opened
            .store_mut()
            .append(reservation(1, 1), None, b"actual later append", true)
            .expect("actual ordinary append");
        let b = budget(native_limits());
        let c = Checks::new();
        let error = opened
            .read_fetch_plan_pinned_until(&fixture.pin, plan(&opened, &b, &c), &b, &c)
            .expect_err("untracked active generation");
        assert!(matches!(
            error.observation().failure(),
            Some(PinnedShardFetchFailure::Stale)
        ));
        assert_eq!(reads(error.observation()), 0);
    }
}
#[test]
fn genuine_regular_symlink_and_hardlink_substitution_refuse_scanned_identity() {
    for mode in 0..3 {
        let fixture = Fixture::new(&[b"actual bytes"], false, false);
        let opened = fixture.opened();
        let path = fixture.path(0);
        let old = fixture.root.0.join("original-held-native-pack");
        fs::rename(&path, &old).expect("actual rename retains scanned inode");
        match mode {
            0 => {
                fs::copy(&old, &path).expect("actual different regular inode");
            }
            1 => symlink(&old, &path).expect("actual symlink"),
            _ => fs::hard_link(&old, &path).expect("actual nlink2"),
        }
        let b = budget(native_limits());
        let c = Checks::new();
        let error = opened
            .read_fetch_plan_pinned_until(&fixture.pin, plan(&opened, &b, &c), &b, &c)
            .expect_err("actual physical substitution");
        assert!(matches!(
            error.observation().failure(),
            Some(PinnedShardFetchFailure::Identity)
        ));
        assert_eq!(reads(error.observation()), 0);
        assert!(c.denied.get());
        fixture.root.contended();
    }
}
#[test]
fn genuine_parent_rename_symlink_and_regular_replacement_cannot_redirect_read() {
    for symlink_name in [false, true] {
        let fixture = Fixture::new(&[b"actual original bytes"], false, false);
        let opened = fixture.opened();
        let moved = fixture.root.0.with_extension("held-original");
        fs::rename(&fixture.root.0, &moved).expect("actual parent rename");
        if symlink_name {
            symlink(&moved, &fixture.root.0).expect("actual replacement symlink");
        } else {
            File::create(&fixture.root.0).expect("actual replacement regular name");
        }
        let b = budget(native_limits());
        let c = Checks::new();
        let error = opened
            .read_fetch_plan_pinned_until(&fixture.pin, plan(&opened, &b, &c), &b, &c)
            .expect_err("actual parent identity refusal");
        assert!(matches!(
            error.observation().failure(),
            Some(PinnedShardFetchFailure::Identity)
        ));
        assert_eq!(reads(error.observation()), 0);
        fs::remove_file(&fixture.root.0).expect("remove only fixture replacement");
        fs::rename(&moved, &fixture.root.0).expect("restore fixture pathname for cleanup");
        fixture.root.contended();
    }
}
#[test]
fn actual_partial_pread_then_eof_retains_filled_bytes_and_every_raw_result() {
    let fixture = Fixture::new(&[b"actual sufficiently long payload"], false, false);
    let opened = fixture.opened();
    let b = budget(native_limits());
    let c = Checks::new();
    let p = plan(&opened, &b, &c);
    let offset = p.planned()[0].payload_offset;
    let path = fixture.path(0);
    let mut once = false;
    let _observer = observe(move |phase| {
        if phase == PinnedShardFetchPhase::BufferAllocated && !once {
            once = true;
            OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("actual fixture writer FD")
                .set_len(offset + 3)
                .expect("real physical short file");
        }
    });
    let error = opened
        .read_fetch_plan_pinned_until(&fixture.pin, p, &b, &c)
        .expect_err("true partial read and EOF");
    assert!(matches!(
        error.observation().failure(),
        Some(PinnedShardFetchFailure::Eof)
    ));
    let (buffer, filled) = error
        .observation()
        .partial_buffer(0)
        .expect("actual retained raw buffer");
    assert_eq!(filled, 3);
    assert_eq!(&buffer[..filled], b"act");
    assert!(
        error
            .observation()
            .outcomes()
            .iter()
            .any(|o| matches!(o, PinnedShardFetchOutcome::Read { result: Ok(0), .. }))
    );
    assert!(c.denied.get());
    fixture.root.contended();
}
#[test]
fn actual_notfound_stat_remains_typed_even_when_completed_phase_becomes_late() {
    let fixture = Fixture::new(&[b"native body"], false, false);
    let opened = fixture.opened();
    let path = fixture.path(0);
    fs::remove_file(path).expect("actual unlink held original name");
    let e = Instant::now() + Duration::from_secs(2);
    let b = StorageFetchBudget::new(native_limits(), e).expect("actual E");
    let c = Checks::new();
    let p = plan(&opened, &b, &c);
    let mut visits = 0;
    let _observer = observe(move |phase| {
        if phase == PinnedShardFetchPhase::PackStat {
            visits += 1;
            if visits == 2 {
                while Instant::now() < e {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
    });
    let error = opened
        .read_fetch_plan_pinned_until(&fixture.pin, p, &b, &c)
        .expect_err("late actual NotFound");
    assert_eq!(
        error
            .observation()
            .io_error()
            .expect("original actual native error")
            .kind(),
        io::ErrorKind::NotFound
    );
    assert_eq!(reads(error.observation()), 0);
    assert!(c.denied.get());
}
#[test]
fn actual_sparse_native_map_probe_fee_is_prepaid_with_immutable_exact_and_one_less_work() {
    let fixture = Fixture::new(&vec![b"native sparse record".as_slice(); 96], true, true);
    let mut opened = fixture.opened();
    let starts = (1..96)
        .map(|id| (topic(id), LogicalOffset::new(u64::MAX)))
        .collect::<HashMap<_, _>>();
    opened
        .store_mut()
        .garbage_collect(&starts)
        .expect("ordinary actual sparse retained index lifecycle");
    assert!(opened.store.index.capacity() > opened.store.index.len() * 4);
    let (fixed, entries, _) = fixed_fee().expect("independent fixed native value inventory");
    let probe =
        (opened.store.index.capacity() + 1) * size_of::<(TopicPartition, Vec<ExtentLocation>)>();
    for fits in [false, true] {
        let mut limits = native_limits();
        limits.max_work_bytes = fixed + entries + probe - usize::from(!fits);
        let b = budget(limits);
        let c = Checks::new();
        let actual =
            opened.plan_fetch_pinned_until(topic(10_000), LogicalOffset::new(0), 64, 1, &b, &c);
        if fits {
            assert!(
                actual
                    .expect("actual absent probe fits exactly")
                    .planned()
                    .is_empty()
            );
            assert!(!c.denied.get());
        } else {
            assert!(matches!(
                actual
                    .expect_err("one less refuses before sparse probe")
                    .observation()
                    .failure(),
                Some(PinnedShardFetchFailure::Budget(
                    StorageFetchFailure::Capacity
                ))
            ));
            assert!(c.denied.get());
        }
    }
}
#[test]
fn same_original_budget_read_attempts_and_bytes_are_never_reset_between_passes() {
    let fixture = Fixture::new(&[b"physical single read"], false, false);
    let opened = fixture.opened();
    let mut limits = native_limits();
    limits.max_read_attempts = 1;
    let b = budget(limits);
    let c = Checks::new();
    let first = opened
        .read_fetch_plan_pinned_until(&fixture.pin, plan(&opened, &b, &c), &b, &c)
        .expect("one actual entered pread");
    assert_eq!(reads(first.observation()), 1);
    let second = opened
        .read_fetch_plan_pinned_until(&fixture.pin, plan(&opened, &b, &c), &b, &c)
        .expect_err("second pass uses remaining zero attempts");
    assert_eq!(reads(second.observation()), 0);
    assert_eq!(
        b.usage().expect("same cumulative counters").read_attempts,
        1
    );
    assert!(c.denied.get());
    let mut limits = native_limits();
    limits.max_read_bytes = b"physical single read".len() - 1;
    let b = budget(limits);
    let c = Checks::new();
    let error = opened
        .read_fetch_plan_pinned_until(&fixture.pin, plan(&opened, &b, &c), &b, &c)
        .expect_err("one less requested byte refuses");
    assert_eq!(reads(error.observation()), 0);
}
#[test]
fn completed_actual_phases_retain_partial_work_through_late_or_unwind_and_deny_first() {
    let phases = [
        PinnedShardFetchPhase::PlanAllocated,
        PinnedShardFetchPhase::Planned,
        PinnedShardFetchPhase::PathCopied,
        PinnedShardFetchPhase::NameConverted,
        PinnedShardFetchPhase::DirectoryStat,
        PinnedShardFetchPhase::PackStat,
        PinnedShardFetchPhase::BufferAllocated,
        PinnedShardFetchPhase::Read,
        PinnedShardFetchPhase::BufferPublished,
        PinnedShardFetchPhase::BatchPublished,
    ];
    for phase in phases {
        for unwind in [false, true] {
            let fixture = Fixture::new(&[b"actual checkpoint bytes"], false, false);
            let opened = fixture.opened();
            let e = Instant::now() + Duration::from_secs(2);
            let b = StorageFetchBudget::new(native_limits(), e).expect("real original E");
            let c = Checks::new();
            let mut once = false;
            let _observer = observe(move |actual| {
                if actual == phase && !once {
                    once = true;
                    if unwind {
                        std::panic::panic_any(io::Error::other("actual completed phase unwind"));
                    }
                    while Instant::now() < e {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
            });
            match opened.plan_fetch_pinned_until(topic(1), LogicalOffset::new(0), 1024, 4, &b, &c) {
                Err(error) => {
                    if unwind {
                        assert!(error.observation().panic_payloads()[0].is_some());
                    }
                }
                Ok(plan) => {
                    let error = opened
                        .read_fetch_plan_pinned_until(&fixture.pin, plan, &b, &c)
                        .expect_err("actual completed phase refuses");
                    if unwind {
                        assert!(error.observation().panic_payloads()[0].is_some());
                    }
                    if phase == PinnedShardFetchPhase::Read {
                        assert!(
                            error
                                .observation()
                                .partial_buffer(0)
                                .expect("retained physical buffer")
                                .1
                                > 0
                        );
                    }
                }
            }
            assert!(c.denied.get());
            assert_eq!(c.deny_calls.get(), 1);
            assert!(b.check().is_err());
            fixture.root.contended();
        }
    }
}
#[test]
fn independent_actual_denial_unwind_preserves_original_read_unwind_and_same_real_lease() {
    let fixture = Fixture::new(&[b"actual native payload"], false, false);
    let opened = fixture.opened();
    let b = budget(native_limits());
    let mut c = Checks::new();
    c.panic_deny = true;
    let p = plan(&opened, &b, &c);
    let _observer = observe(|phase| {
        if phase == PinnedShardFetchPhase::Read {
            std::panic::panic_any(io::Error::other("actual entered pread completion unwind"));
        }
    });
    let error = opened
        .read_fetch_plan_pinned_until(&fixture.pin, p, &b, &c)
        .expect_err("real independent panics");
    let panics = error.observation().panic_payloads();
    assert!(panics[0].is_some());
    assert!(panics[2].is_some());
    assert!(
        error
            .observation()
            .partial_buffer(0)
            .expect("retained successful pread")
            .1
            > 0
    );
    assert_eq!(c.deny_calls.get(), 1);
    assert!(b.check().is_err());
    fixture.root.contended();
}

#[test]
fn genuine_scanned_identity_requests_have_independent_immutable_entry_fees_before_io() {
    let fixture = Fixture::new(&[b"real initial scanned identity"], false, false);
    let packs = open_limits().max_packs;
    let files = packs + 4;
    // Six independently named new setup values, two requested/actual capacity
    // slots per ScannedPack+identity pair, and each FileSlot's typed identity.
    let entries = 6 + 4 * packs + files;
    for fits in [false, true] {
        let mut policy = open_limits();
        policy.max_entries = entries - usize::from(!fits);
        let error = ShardStore::open_pinned_until(
            fixture.config.clone(),
            &fixture.pin,
            policy,
            expiry(),
            &Checks::new(),
        )
        .expect_err("later producer still needs its own finite entry fees");
        assert_eq!(error.usage().entered_io_calls, 0);
        assert_eq!(error.usage().entries, if fits { entries } else { 0 });
        assert_eq!(error.scanned_pack_capacity(), if fits { packs } else { 0 });
        if fits {
            assert!(matches!(error.scope.scanned_allocation, Some(Ok(()))));
        } else {
            assert!(error.scope.scanned_allocation.is_none());
        }
    }
    fixture.root.contended();
}
#[test]
fn real_range_header_inventory_refuses_immutable_low_entry_policy_before_materialization() {
    let fixture = Fixture::new(&[b"real bounded payload"], false, false);
    let opened = fixture.opened();
    // One selected batch, both fixed 44-entry inventories, six four-entry
    // native pin stats, actual plan/range reservation results and native name.
    // This original immutable policy funds118 entries BEFORE the independent
    // fourteen named range values; removing that fee wrongly creates a range.
    let mut policy = native_limits();
    policy.max_plan_batches = 1;
    policy.max_entries = 118;
    let b = budget(policy);
    let c = Checks::new();
    let p = opened
        .plan_fetch_pinned_until(topic(1), LogicalOffset::new(0), 4096, 1, &b, &c)
        .expect("actual one-record plan fits original policy");
    let error = opened
        .read_fetch_plan_pinned_until(&fixture.pin, p, &b, &c)
        .expect_err("independent range inventory refuses before materialization");
    assert!(matches!(
        error.observation().failure(),
        Some(PinnedShardFetchFailure::Budget(
            StorageFetchFailure::Capacity
        ))
    ));
    assert!(error.observation().ranges.is_empty());
    assert_eq!(reads(error.observation()), 0);
    assert!(c.denied.get());
    fixture.root.contended();
}

#[test]
fn actual_native_fixed_result_panic_and_diagnostic_slots_have_immutable_exact_entry_control() {
    let fixture = Fixture::new(&[b"real retained store"], false, false);
    let opened = fixture.opened();
    // Forty-three actual named fixed values and one diagnostic buffer are
    // separate requests. An absent real partition requires no metadata buffer.
    for fits in [false, true] {
        let mut policy = native_limits();
        policy.max_plan_batches = 1;
        policy.max_entries = if fits { 44 } else { 43 };
        let b = budget(policy);
        let c = Checks::new();
        let actual =
            opened.plan_fetch_pinned_until(topic(999), LogicalOffset::new(0), 64, 1, &b, &c);
        if fits {
            assert!(
                actual
                    .expect("all44 actual fixed entries fit")
                    .planned()
                    .is_empty()
            );
            assert_eq!(b.usage().expect("same actual counters").entries, 44);
        } else {
            let error =
                actual.expect_err("missing any fixed slot refuses before probe/materialization");
            assert!(error.plan().planned().is_empty());
            assert!(error.observation().allocation.is_none());
            assert_eq!(
                b.usage()
                    .expect("no partially committed entry debit")
                    .entries,
                0
            );
            assert!(c.denied.get());
        }
        fixture.root.contended();
    }
}
