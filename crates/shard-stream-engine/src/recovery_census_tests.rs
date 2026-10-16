use super::*;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use shardlog::{
    BatchId, CoordinatorJournal, LeaderEpoch, LogicalOffset, LogicalPartitionId, Placement,
    PlacementSequence, Reservation, RingEpoch, TopicId, VirtualLaneId,
};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "native-preopen-census-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).expect("temporary root");
        Self(path)
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
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
        queue_bytes_per_shard: 2 * 1024 * 1024,
        target_pack_bytes: 4096,
        max_pack_age: Duration::from_secs(30),
        max_batch_bytes: 1024 * 1024,
        max_fetch_bytes: 1024 * 1024,
        append_linger: Duration::ZERO,
    }
}

fn limits() -> NativeRecoveryLimits {
    NativeRecoveryLimits {
        max_files: 128,
        max_entries: 1_000_000,
        max_read_bytes: 64 * 1024 * 1024,
        max_declared_bytes: 64 * 1024 * 1024,
        max_work_bytes: 256 * 1024 * 1024,
        max_frame_bytes: 2 * 1024 * 1024,
        max_manifest_bytes: 1024 * 1024,
        max_partial_tail_bytes: 1024 * 1024,
        max_path_bytes: 4096,
    }
}

fn reservation(sequence: u64, term: u64) -> Reservation {
    Reservation {
        topic_id: TopicId::new(11),
        partition_id: LogicalPartitionId::new(0),
        batch_id: BatchId::new(u128::from(sequence)),
        first_offset: LogicalOffset::new(sequence),
        last_offset: LogicalOffset::new(sequence),
        record_count: NonZeroU32::new(1).expect("nonzero"),
        placement: Placement {
            virtual_lane_id: VirtualLaneId::new(3),
            ring_epoch: RingEpoch::new(1),
            leader_epoch: LeaderEpoch::new(term),
            sequence: PlacementSequence::new(sequence),
        },
    }
}

fn store_config(root: &Path) -> ShardStoreConfig {
    ShardStoreConfig {
        directory: root.join("shards/shard-0"),
        shard_id: ShardId::new(0),
        target_pack_bytes: 4096,
        max_batch_bytes: 1024 * 1024,
    }
}

fn write_native(root: &Path) -> PathBuf {
    let (mut journal, _) =
        CoordinatorJournal::open(root.join("coordinator.ssj")).expect("real journal");
    journal
        .append(
            &JournalEvent::Configure {
                topic_partition: TopicPartition::new(TopicId::new(11), LogicalPartitionId::new(0)),
                ring_epoch: RingEpoch::new(1),
                shards: vec![ShardId::new(0)],
            },
            true,
        )
        .expect("native configure");
    drop(journal);
    let mut store = ShardStore::open(store_config(root)).expect("real store");
    store
        .append(reservation(0, 91), None, b"earlier native high term", true)
        .expect("first native frame");
    store
        .append(reservation(1, 5), None, b"later native low term", true)
        .expect("second native frame");
    let path = store.active_path().to_owned();
    drop(store);
    path
}

fn read(root: &Path, config: EngineConfig, caps: NativeRecoveryLimits) -> NativeRecoveryCensus {
    let owner = DirectoryOwnership::acquire(root).expect("actual owner");
    NativeRecoveryCensus::read(
        config,
        owner,
        None,
        caps,
        Instant::now() + Duration::from_secs(30),
    )
    .expect("native census")
}

#[test]
fn owned_native_census_keeps_nonlatest_maximum_and_actual_lane_mapping() {
    let temp = Temporary::new();
    let pack = write_native(&temp.0);
    let before = fs::read(&pack).expect("original bytes");
    let census = read(&temp.0, config(&temp.0), limits());
    let tp = TopicPartition::new(TopicId::new(11), LogicalPartitionId::new(0));
    assert_eq!(census.observed_leader_epoch(tp), Some(91));
    assert_eq!(census.extents().len(), 2);
    assert_eq!(
        census.extents()[0]
            .native
            .reservation
            .placement
            .virtual_lane_id
            .get(),
        3
    );
    assert_eq!(census.extents()[0].configured_shard.get(), 0);
    assert_eq!(fs::read(pack).expect("unchanged"), before);
    assert!(matches!(
        DirectoryOwnership::acquire(&temp.0),
        Err(DirectoryOwnershipError::Contended)
    ));
}

#[test]
fn owned_native_census_preserves_gc_pending_and_raw_partial_tails() {
    let temp = Temporary::new();
    let pack = write_native(&temp.0);
    let pending = pack.with_extension("sse.gc-pending");
    fs::rename(&pack, &pending).expect("real native pending deletion name");
    OpenOptions::new()
        .append(true)
        .open(&pending)
        .expect("pending source")
        .write_all(b"SSE")
        .expect("incomplete tail");
    OpenOptions::new()
        .append(true)
        .open(temp.0.join("coordinator.ssj"))
        .expect("journal")
        .write_all(b"SS")
        .expect("journal tail");
    let before = fs::read(&pending).expect("before");
    let census = read(&temp.0, config(&temp.0), limits());
    assert_eq!(
        census.observed_leader_epoch(TopicPartition::new(
            TopicId::new(11),
            LogicalPartitionId::new(0)
        )),
        Some(91)
    );
    assert!(
        census
            .sources()
            .any(|s| s.kind == RecoverySourceKind::PendingDeletion && s.partial_tail == b"SSE")
    );
    assert!(
        census
            .sources()
            .any(|s| s.kind == RecoverySourceKind::Coordinator && s.partial_tail == b"SS")
    );
    assert_eq!(fs::read(pending).expect("no repair/deletion"), before);
}

#[test]
fn owned_native_census_refuses_native_corruption_without_repair() {
    let temp = Temporary::new();
    let pack = write_native(&temp.0);
    let mut bytes = fs::read(&pack).expect("pack");
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    fs::write(&pack, &bytes).expect("real corrupt CRC");
    let owner = DirectoryOwnership::acquire(&temp.0).expect("owner");
    let refusal = NativeRecoveryCensus::read(
        config(&temp.0),
        owner,
        None,
        limits(),
        Instant::now() + Duration::from_secs(30),
    );
    assert!(refusal.is_err());
    assert_eq!(fs::read(pack).expect("unchanged corrupt evidence"), bytes);
}

#[test]
fn owned_native_census_budget_and_original_expiry_refuse_before_read() {
    let temp = Temporary::new();
    write_native(&temp.0);
    let owner = DirectoryOwnership::acquire(&temp.0).expect("owner");
    let mut caps = limits();
    caps.max_read_bytes = 1;
    let error = match NativeRecoveryCensus::read(
        config(&temp.0),
        Arc::clone(&owner),
        None,
        caps,
        Instant::now() + Duration::from_secs(30),
    ) {
        Err(error) => error,
        Ok(_) => panic!("expected actual bounded read refusal"),
    };
    assert_eq!(error.usage().read_bytes, 0);
    drop(error);
    let error =
        match NativeRecoveryCensus::read(config(&temp.0), owner, None, limits(), Instant::now()) {
            Err(error) => error,
            Ok(_) => panic!("expected original E refusal"),
        };
    assert_eq!(error.usage().read_bytes, 0);
}

#[test]
fn owned_native_census_moves_same_shard_lease_into_real_opener() {
    let temp = Temporary::new();
    write_native(&temp.0);
    let census = read(&temp.0, config(&temp.0), limits());
    assert!(ShardStore::open(store_config(&temp.0)).is_err());
    let mut storage = match census.open_storage() {
        Ok(storage) => storage,
        Err(error) => panic!("actual same-lock handoff: {error}"),
    };
    assert_eq!(
        storage
            .primary(ShardId::new(0))
            .expect("actual native primary")
            .catalog()
            .len(),
        2
    );
    assert!(ShardStore::open(store_config(&temp.0)).is_err());
    drop(storage);
    drop(ShardStore::open(store_config(&temp.0)).expect("actual native lease released"));
}

#[test]
fn owned_native_census_reads_object_only_high_term_with_real_tier_coverage() {
    let temp = Temporary::new();
    write_native(&temp.0);
    let mut store = ShardStore::open(store_config(&temp.0)).expect("native seal");
    let pack = store.seal_active().expect("native seal").expect("nonempty");
    let coverage = store
        .catalog()
        .into_iter()
        .filter(|e| e.pack_sequence == pack.sequence)
        .map(|e| TierExtent::from_reservation(e.reservation))
        .collect();
    let tier_dir = temp.0.join("tier");
    let mut tier = LocalObjectTier::open(&tier_dir, ShardId::new(0)).expect("actual tier");
    tier.offload_pack(pack.sequence, &pack.path, coverage)
        .expect("actual native offload");
    drop(tier);
    drop(store);
    fs::remove_file(&pack.path).expect("object-only retained copy");
    let owner = DirectoryOwnership::acquire(&temp.0).expect("primary owner");
    let tier_owner = DirectoryOwnership::acquire(&tier_dir).expect("real tier owner");
    let mut cfg = config(&temp.0);
    cfg.object_store_dir = Some(tier_dir);
    let census = NativeRecoveryCensus::read(
        cfg,
        owner,
        Some(tier_owner.root_handle().expect("actual tier root")),
        limits(),
        Instant::now() + Duration::from_secs(30),
    )
    .expect("actual object scan");
    assert!(
        census
            .sources()
            .any(|s| s.kind == RecoverySourceKind::TierObject)
    );
    assert_eq!(
        census.observed_leader_epoch(TopicPartition::new(
            TopicId::new(11),
            LogicalPartitionId::new(0)
        )),
        Some(91)
    );
    // This is floor observation, NOT proof that ordinary engine fetch can
    // currently rehydrate object-only batches. That implementation remains due.
}

#[test]
fn owned_native_census_observes_absence_without_minting_zero_floor() {
    let temp = Temporary::new();
    let census = read(&temp.0, config(&temp.0), limits());
    assert!(!census.missing_paths().is_empty());
    assert_eq!(
        census.observed_leader_epoch(TopicPartition::new(
            TopicId::new(11),
            LogicalPartitionId::new(0)
        )),
        None
    );
}

fn write_tier(root: &Path) -> (PathBuf, PathBuf) {
    write_native(root);
    let mut store = ShardStore::open(store_config(root)).expect("real native primary");
    let pack = store.seal_active().expect("seal").expect("nonempty");
    let coverage = store
        .catalog()
        .into_iter()
        .filter(|entry| entry.pack_sequence == pack.sequence)
        .map(|entry| TierExtent::from_reservation(entry.reservation))
        .collect();
    let tier_dir = root.join("tier");
    let mut tier = LocalObjectTier::open(&tier_dir, ShardId::new(0)).expect("real native tier");
    tier.offload_pack(pack.sequence, &pack.path, coverage)
        .expect("native offload");
    (tier_dir, pack.path)
}

fn read_tier(
    root: &Path,
    tier_dir: &Path,
) -> std::result::Result<NativeRecoveryCensus, NativeRecoveryError> {
    let owner = DirectoryOwnership::acquire(root).expect("real root custody");
    let tier_owner = DirectoryOwnership::acquire(tier_dir).expect("real tier custody");
    let mut cfg = config(root);
    cfg.object_store_dir = Some(tier_dir.to_owned());
    NativeRecoveryCensus::read(
        cfg,
        owner,
        Some(tier_owner.root_handle().expect("tier handle")),
        limits(),
        Instant::now() + Duration::from_secs(30),
    )
}

#[test]
fn owned_native_census_keeps_higher_unselected_manifest_without_claiming_missing_payload() {
    let temp = Temporary::new();
    let (tier_dir, _) = write_tier(&temp.0);
    let mut store = ShardStore::open(store_config(&temp.0)).expect("real second pack");
    store
        .append(
            reservation(2, 333),
            None,
            b"higher retained generation",
            true,
        )
        .expect("native append");
    let pack = store.seal_active().expect("seal").expect("nonempty");
    let coverage = store
        .catalog()
        .into_iter()
        .filter(|entry| entry.pack_sequence == pack.sequence)
        .map(|entry| TierExtent::from_reservation(entry.reservation))
        .collect();
    let mut tier =
        LocalObjectTier::open(&tier_dir, ShardId::new(0)).expect("native second snapshot");
    let object = tier
        .offload_pack(pack.sequence, &pack.path, coverage)
        .expect("native generation2");
    drop(tier);
    drop(store);
    // A real retained generation remains while CURRENT and payload are older.
    fs::write(tier_dir.join("manifests/shard-0/CURRENT"), b"1\n").expect("restored pointer");
    fs::remove_file(tier_dir.join(object.object_key)).expect("missing unselected object");
    fs::remove_file(pack.path).expect("missing newer primary");
    let census = read_tier(&temp.0, &tier_dir).expect("retained native snapshot census");
    let tp = TopicPartition::new(TopicId::new(11), LogicalPartitionId::new(0));
    assert_eq!(census.observed_leader_epoch(tp), Some(333));
    assert!(
        census
            .tier_manifests()
            .iter()
            .any(|manifest| manifest.native.generation == 2 && !manifest.selected_by_current)
    );
    assert!(
        !census
            .extents()
            .iter()
            .any(|entry| entry.native.reservation.placement.leader_epoch.get() == 333)
    );
}

#[test]
fn owned_native_census_refuses_corrupt_referenced_tier_object_without_repair() {
    let temp = Temporary::new();
    let (tier_dir, _) = write_tier(&temp.0);
    let tier = LocalObjectTier::open(&tier_dir, ShardId::new(0)).expect("real native manifest");
    let object_path = tier_dir.join(&tier.manifest().objects[0].object_key);
    drop(tier);
    let mut bytes = fs::read(&object_path).expect("real object");
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    fs::write(&object_path, &bytes).expect("actual corrupted object");
    assert!(read_tier(&temp.0, &tier_dir).is_err());
    assert_eq!(fs::read(object_path).expect("unrepaired evidence"), bytes);
}

#[test]
fn owned_native_census_transfers_tier_copy_and_publication_lock_without_reacquisition() {
    let temp = Temporary::new();
    let (tier_dir, _) = write_tier(&temp.0);
    let census = read_tier(&temp.0, &tier_dir).expect("real census");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tier_dir.join("manifests/shard-0/UPDATE.lock"))
        .expect("actual native lease");
    assert!(lock.try_lock().is_err());
    let mut storage = match census.open_storage() {
        Ok(storage) => storage,
        Err(error) => panic!("same concrete lease into native tier: {error}"),
    };
    let primary = storage.primary(ShardId::new(0)).expect("real primary");
    primary
        .append(reservation(2, 7), None, b"after same-lock handoff", true)
        .expect("native append");
    let pack = primary.seal_active().expect("seal").expect("nonempty");
    let coverage = primary
        .catalog()
        .into_iter()
        .filter(|entry| entry.pack_sequence == pack.sequence)
        .map(|entry| TierExtent::from_reservation(entry.reservation))
        .collect();
    storage
        .tier(ShardId::new(0))
        .expect("real tier")
        .offload_pack(pack.sequence, &pack.path, coverage)
        .expect("no self-contention during native copy/publication");
    assert!(lock.try_lock().is_err());
    drop(storage);
    lock.try_lock()
        .expect("physical final drop releases tier lease");
}

#[test]
fn owned_native_census_reads_retained_shard_outside_current_ring_count() {
    let temp = Temporary::new();
    write_native(&temp.0);
    let mut old_config = store_config(&temp.0);
    old_config.directory = temp.0.join("shards/shard-9");
    old_config.shard_id = ShardId::new(9);
    let mut store = ShardStore::open(old_config).expect("real older physical shard");
    store
        .append(
            reservation(3, 777),
            None,
            b"retained old-ring maximum",
            true,
        )
        .expect("native old-ring frame");
    drop(store);
    let census = read(&temp.0, config(&temp.0), limits());
    assert_eq!(
        census.observed_leader_epoch(TopicPartition::new(
            TopicId::new(11),
            LogicalPartitionId::new(0)
        )),
        Some(777)
    );
    assert!(
        census
            .extents()
            .iter()
            .any(|entry| entry.physical_shard == ShardId::new(9)
                && entry.configured_shard == ShardId::new(0))
    );
}

#[test]
fn owned_native_census_refuses_native_manifest_coverage_that_disagrees_with_real_pack() {
    let temp = Temporary::new();
    let (tier_dir, _) = write_tier(&temp.0);
    let manifest_path = tier_dir.join("manifests/shard-0/manifest-00000000000000000001.json");
    let mut manifest: TierManifest =
        serde_json::from_slice(&fs::read(&manifest_path).expect("native manifest bytes"))
            .expect("native manifest");
    manifest.objects[0].extents[0].leader_epoch += 1;
    let changed =
        serde_json::to_vec(&manifest).expect("real native format with deliberately false coverage");
    fs::write(&manifest_path, &changed).expect("corrupt native coverage");
    assert!(read_tier(&temp.0, &tier_dir).is_err());
    assert_eq!(
        fs::read(manifest_path).expect("unchanged evidence"),
        changed
    );
}

#[test]
fn owned_native_census_refuses_missing_current_payload_without_deleting_manifest() {
    let temp = Temporary::new();
    let (tier_dir, _) = write_tier(&temp.0);
    let tier = LocalObjectTier::open(&tier_dir, ShardId::new(0)).expect("native manifest");
    let object = tier_dir.join(&tier.manifest().objects[0].object_key);
    drop(tier);
    let manifest_path = tier_dir.join("manifests/shard-0/manifest-00000000000000000001.json");
    let before = fs::read(&manifest_path).expect("native manifest bytes");
    fs::remove_file(object).expect("missing current payload");
    assert!(read_tier(&temp.0, &tier_dir).is_err());
    assert_eq!(fs::read(manifest_path).expect("manifest retained"), before);
}

fn empty_tier_census(root: &Path, tier: &Path, caps: NativeRecoveryLimits) -> NativeRecoveryCensus {
    let owner = DirectoryOwnership::acquire(root).expect("actual empty primary custody");
    let tier_owner = DirectoryOwnership::acquire(tier).expect("actual empty tier custody");
    let mut cfg = config(root);
    cfg.shard_count = 2;
    cfg.object_store_dir = Some(tier.to_owned());
    NativeRecoveryCensus::read(
        cfg,
        owner,
        Some(tier_owner.root_handle().expect("tier root")),
        caps,
        Instant::now() + Duration::from_secs(30),
    )
    .expect("actual empty pre-open observation")
}
fn remove_fixture_opened_dirs(root: &Path, tier: &Path) {
    for path in [
        root.join("shards"),
        tier.join("objects"),
        tier.join("manifests"),
    ] {
        fs::remove_dir_all(path)
            .expect("remove only test-created native directories after physical drop");
    }
}
#[test]
fn owned_native_empty_open_exact_caps_and_one_less_refuse_before_native_creation() {
    let temp = Temporary::new();
    let tier = temp.0.join("tier");
    fs::create_dir(&tier).expect("actual separate empty tier root");
    let baseline = empty_tier_census(&temp.0, &tier, limits());
    assert!(baseline.extents().is_empty());
    assert_eq!(baseline.sources().count(), 0);
    let storage = match baseline.open_storage() {
        Ok(storage) => storage,
        Err(error) => panic!("real empty native open: {error}"),
    };
    let measured = storage.usage();
    assert!(temp.0.join("shards/shard-1/.shard-store.lock").is_file());
    assert!(
        temp.0
            .join("shards/shard-1/pack-00000000000000000000.sse")
            .is_file()
    );
    assert!(tier.join("objects/shard-1").is_dir());
    assert!(tier.join("manifests/shard-1").is_dir());
    drop(storage);
    remove_fixture_opened_dirs(&temp.0, &tier);
    let mut exact = limits();
    exact.max_files = measured.files;
    exact.max_entries = measured.entries;
    exact.max_declared_bytes = measured.declared_bytes;
    exact.max_work_bytes = measured.work_bytes;
    // Empty native storage has no existing payload reads to debit; the four
    // nonzero fixed/open caps are the relevant boundaries, not a fake read.
    assert_eq!(measured.read_bytes, 0);
    let storage = match empty_tier_census(&temp.0, &tier, exact).open_storage() {
        Ok(storage) => storage,
        Err(error) => panic!("actual measured exact native admission: {error}"),
    };
    let actual = storage.usage();
    assert_eq!(
        (
            actual.files,
            actual.entries,
            actual.declared_bytes,
            actual.work_bytes
        ),
        (
            measured.files,
            measured.entries,
            measured.declared_bytes,
            measured.work_bytes
        )
    );
    drop(storage);
    remove_fixture_opened_dirs(&temp.0, &tier);
    for which in 0..4 {
        let mut short = exact;
        match which {
            0 => short.max_files -= 1,
            1 => short.max_entries -= 1,
            2 => short.max_declared_bytes -= 1,
            _ => short.max_work_bytes -= 1,
        }
        let observed = empty_tier_census(&temp.0, &tier, short);
        let refusal = match observed.open_storage() {
            Err(error) => error,
            Ok(_) => panic!("one-less native open cap must refuse"),
        };
        assert!(
            !temp.0.join("shards").exists(),
            "no native shard/lock/pack may be created"
        );
        assert!(
            !tier.join("objects").exists(),
            "no native tier directories may be created"
        );
        assert!(
            !tier.join("manifests").exists(),
            "no native tier manifest/lock may be created"
        );
        drop(refusal);
    }
}

#[test]
fn owned_native_manifest_unknown_array_debits_only_remaining_cumulative_nodes() {
    let temp = Temporary::new();
    let (tier, _) = write_tier(&temp.0);
    let baseline = read_tier(&temp.0, &tier).expect("genuine native baseline");
    let prior_complete_entries = baseline.usage().entries;
    drop(baseline);
    let path = tier.join("manifests/shard-0/manifest-00000000000000000001.json");
    let mut native: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).expect("native manifest"))
            .expect("actual native JSON");
    native.as_object_mut().expect("native object").insert(
        "unknown_retained_array".into(),
        serde_json::json!(vec![0u8; prior_complete_entries + 64]),
    );
    let bytes = serde_json::to_vec(&native).expect("native-compatible unknown field");
    fs::write(&path, &bytes).expect("retained native manifest with unknown array");
    let owner = DirectoryOwnership::acquire(&temp.0).expect("real primary custody");
    let tier_owner = DirectoryOwnership::acquire(&tier).expect("real tier custody");
    let mut cfg = config(&temp.0);
    cfg.object_store_dir = Some(tier);
    let mut caps = limits();
    caps.max_entries = prior_complete_entries + 16;
    let refusal = match NativeRecoveryCensus::read(
        cfg,
        owner,
        Some(tier_owner.root_handle().expect("actual tier root")),
        caps,
        Instant::now() + Duration::from_secs(30),
    ) {
        Err(error) => error,
        Ok(_) => panic!("unknown JSON nodes exceed actual remaining cumulative allowance"),
    };
    assert!(refusal.error().to_string().contains("JSON shape refused"));
    assert_eq!(refusal.usage().entries, caps.max_entries);
    assert_eq!(fs::read(path).expect("native bytes unrepaired"), bytes);
}

#[test]
fn owned_native_long_root_ignored_discovery_paths_are_admitted_before_any_repair() {
    let temp = Temporary::new();
    let root = temp
        .0
        .join("a".repeat(180))
        .join("b".repeat(180))
        .join("c".repeat(180))
        .join("d".repeat(180));
    fs::create_dir_all(&root).expect("genuine long native root");
    let pack = write_native(&root);
    let shard_dir = store_config(&root).directory;
    for number in 0..512 {
        fs::write(
            shard_dir.join(format!("ignored-{number:04}-{}", "x".repeat(96))),
            [],
        )
        .expect("actual ignored native-directory entry");
    }
    let native_path_bytes: usize = fs::read_dir(&shard_dir)
        .expect("actual entries")
        .map(|entry| entry.expect("entry").path().as_os_str().len())
        .sum();
    OpenOptions::new()
        .append(true)
        .open(&pack)
        .expect("real native pack")
        .write_all(b"SSE")
        .expect("actual partial native tail");
    let original = fs::read(&pack).expect("native bytes before repair");
    let mut cfg = config(&root);
    cfg.shard_count = 2;
    let before = read(&root, cfg.clone(), limits());
    let read_usage = before.usage();
    let opened = match before.open_storage() {
        Ok(storage) => storage,
        Err(error) => panic!("long-root fitting real native open: {error}"),
    };
    let measured = opened.usage();
    assert!(
        measured.declared_bytes - read_usage.declared_bytes >= native_path_bytes * 2,
        "both actual native scans must admit full parent+name copies, including ignored entries"
    );
    assert_eq!(
        fs::read(&pack).expect("native repair happened"),
        original[..original.len() - 3]
    );
    drop(opened);
    // Restore the same genuine interrupted fixture; remove only the native
    // second-shard output from this test after its real store has settled.
    fs::write(&pack, &original).expect("same real native prefix and incomplete tail");
    fs::remove_dir_all(root.join("shards/shard-1")).expect("test's settled new shard");
    let mut exact = limits();
    exact.max_declared_bytes = measured.declared_bytes;
    let opened = match read(&root, cfg.clone(), exact).open_storage() {
        Ok(storage) => storage,
        Err(error) => panic!("measured exact long-root admission: {error}"),
    };
    assert_eq!(opened.usage().declared_bytes, measured.declared_bytes);
    drop(opened);
    fs::write(&pack, &original).expect("same interrupted native bytes");
    fs::remove_dir_all(root.join("shards/shard-1")).expect("test's settled second shard");
    exact.max_declared_bytes -= 1;
    let observed = read(&root, cfg.clone(), exact);
    let refusal = match observed.open_storage() {
        Err(error) => error,
        Ok(_) => panic!("one-less discovery materialization must refuse before any native open"),
    };
    assert_eq!(
        fs::read(&pack).expect("partial tail NOT repaired on refusal"),
        original
    );
    assert!(
        !root.join("shards/shard-1").exists(),
        "no native new shard/lock/pack on refusal"
    );
    drop(refusal);
    let mut short_path = limits();
    short_path.max_path_bytes = pack.as_os_str().len();
    let owner = DirectoryOwnership::acquire(&root).expect("real path-bound custody");
    assert!(
        NativeRecoveryCensus::read(
            cfg,
            owner,
            None,
            short_path,
            Instant::now() + Duration::from_secs(30)
        )
        .is_err(),
        "ignored native entry paths must respect the existing generated-path ceiling too"
    );
    assert_eq!(
        fs::read(pack).expect("native evidence still intact"),
        original
    );
}
