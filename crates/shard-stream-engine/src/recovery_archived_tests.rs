use super::*;
use crate::{EngineConfig, NativeRecoveryCensus, NativeRecoveryLimits, StreamEngine, TopicConfig};
use bytes::Bytes;
use shard_stream_core::{DirectoryOwnership, DirectoryOwnershipError, LogicalPartitionId, TopicId};
use shard_stream_protocol::{AppendRequest, Durability};
use shardlog::{
    AtomicGroupSequence, BatchId, CoordinatorJournal, LeaderEpoch, LogicalOffset, Placement,
    PlacementSequence, ProducerSequence, Reservation, RingEpoch, ShardStore, ShardStoreConfig,
    TopicPartition, VirtualLaneId,
};
use std::fs;
use std::io::Write;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("actual clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "native-archive-{}-{time}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual unique root");
        Self(path)
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn expiry() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
fn limits() -> NativeArchivedSourceLimits {
    NativeArchivedSourceLimits {
        max_source_bytes: 4 << 20,
        max_frame_bytes: 1 << 20,
        max_frames: 4096,
        max_manifest_bytes: 1 << 20,
        max_json_nodes: 65_536,
        max_json_depth: 32,
        max_path_bytes: 8192,
        max_batch_bytes: 1 << 19,
        max_declared_bytes: 256 << 20,
        max_work_bytes: 1 << 30,
        max_entries: 4_000_000,
    }
}
fn native_caps() -> NativeRecoveryLimits {
    NativeRecoveryLimits {
        max_files: 256,
        max_entries: 1_000_000,
        max_read_bytes: 128 << 20,
        max_declared_bytes: 128 << 20,
        max_work_bytes: 1 << 30,
        max_frame_bytes: 1 << 20,
        max_manifest_bytes: 1 << 20,
        max_partial_tail_bytes: 1 << 20,
        max_path_bytes: 8192,
    }
}
fn config(data: &Path, objects: &Path) -> EngineConfig {
    EngineConfig {
        data_dir: data.to_owned(),
        object_store_dir: Some(objects.to_owned()),
        shard_count: 1,
        virtual_lane_count: 4,
        replication_factor: 1,
        min_in_sync_replicas: 1,
        queue_slots_per_shard: 16,
        queue_bytes_per_shard: 2 << 20,
        target_pack_bytes: 4096,
        max_pack_age: Duration::from_secs(30),
        max_batch_bytes: 1 << 19,
        max_fetch_bytes: 1 << 20,
        append_linger: Duration::ZERO,
    }
}
struct Fixture {
    census: NativeRecoveryCensus,
    _temporary: Temporary,
}
impl Fixture {
    fn new() -> Self {
        let root = Temporary::new();
        let data = root.0.join("data");
        let objects = root.0.join("objects");
        let cfg = config(&data, &objects);
        let engine = StreamEngine::open(cfg.clone()).expect("actual native writer");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(11),
                partitions: 1,
                shards: None,
            })
            .expect("actual native CreatePartition");
        for id in 1..=2 {
            engine
                .append(AppendRequest {
                    request_id: id,
                    topic_id: TopicId::new(11),
                    partition_id: LogicalPartitionId::new(0),
                    record_count: 1,
                    payload: Bytes::from_static(b"actual original native object body"),
                    durability: Durability::Object,
                    producer: None,
                    atomic_group: None,
                    leader_epoch: None,
                    extension_context: None,
                })
                .expect("actual native pack/object/CURRENT/manifest producer");
        }
        drop(engine);
        let owner = DirectoryOwnership::acquire(&data).expect("actual original owner");
        let tier = DirectoryOwnership::acquire(&objects).expect("actual separate object owner");
        let census = NativeRecoveryCensus::read_for_engine(
            cfg,
            owner,
            Some(tier.root_handle().expect("actual anchored tier")),
            native_caps(),
            expiry(),
        )
        .expect("actual native census before repair/worker open");
        Self {
            census,
            _temporary: root,
        }
    }
    fn raw(&self, kind: RecoverySourceKind) -> (PathBuf, Vec<u8>) {
        let source = self
            .census
            .sources()
            .find(|s| s.kind == kind)
            .expect("actual native source role");
        (
            source.path.clone(),
            fs::read(&source.path).expect("original bytes outside inspector"),
        )
    }
}
#[derive(Default)]
struct Probe {
    pause: Option<&'static str>,
    phases: Vec<&'static str>,
}
std::thread_local! { static PROBE: std::cell::RefCell<Option<Probe>> = const { std::cell::RefCell::new(None) }; }
pub(super) fn phase(name: &'static str, saved: Instant) {
    let delay = PROBE.with(|p| {
        let mut p = p.borrow_mut();
        let Some(p) = p.as_mut() else {
            return false;
        };
        p.phases.push(name);
        p.pause == Some(name)
    });
    if delay {
        std::thread::sleep(
            saved.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
        );
    }
}
struct Observe;
impl Observe {
    fn new(pause: &'static str) -> Self {
        PROBE.with(|p| {
            assert!(p.borrow().is_none());
            *p.borrow_mut() = Some(Probe {
                pause: Some(pause),
                phases: Vec::new(),
            });
        });
        Self
    }
    fn phases(&self) -> Vec<&'static str> {
        PROBE.with(|p| p.borrow().as_ref().expect("actual observer").phases.clone())
    }
}
impl Drop for Observe {
    fn drop(&mut self) {
        PROBE.with(|p| *p.borrow_mut() = None);
    }
}

#[test]
fn actual_native_sources_dispatch_journal_objects_current_and_all_manifest_generations() {
    let f = Fixture::new();
    let mut old_manifest = 0;
    let mut current_manifest = 0;
    let mut packs = 0;
    for fact in f.census.sources() {
        let raw = fs::read(&fact.path).expect("actual original source");
        let before = raw.clone();
        let view = inspect_archived_native_source(
            fact.kind,
            &fact.path,
            Some(ShardId::new(0)),
            &raw,
            limits(),
            expiry(),
        )
        .expect("native byte interpretation");
        assert_eq!(view.kind(), fact.kind);
        assert!(std::ptr::eq(view.raw_bytes().as_ptr(), raw.as_ptr()));
        match fact.kind {
            RecoverySourceKind::Coordinator => {
                assert!(!view.journal().is_empty());
                assert!(matches!(
                    view.journal()[0].native(),
                    JournalEvent::CreatePartition { .. }
                ));
                assert!(view.tail().is_none());
            }
            RecoverySourceKind::TierCurrent => assert_eq!(view.current_generation(), Some(2)),
            RecoverySourceKind::TierManifest | RecoverySourceKind::UnreferencedTierManifest => {
                let native =
                    decode_recovery_tier_manifest(&raw).expect("same native typed decoder");
                assert_eq!(view.manifest(), Some(&native));
                if fact.kind == RecoverySourceKind::TierManifest {
                    current_manifest += 1;
                } else {
                    old_manifest += 1;
                }
            }
            _ => {
                for extent in view.extents() {
                    assert_eq!(
                        extent.native().payload(),
                        b"actual original native object body"
                    );
                    let full = extent.native().frame();
                    assert_eq!(full.as_ptr(), raw[extent.file_offset() as usize..].as_ptr());
                    packs += 1;
                }
            }
        }
        assert_eq!(raw, before);
        assert_eq!(*blake3::hash(view.raw_bytes()).as_bytes(), fact.blake3);
    }
    assert_eq!(old_manifest, 1);
    assert_eq!(current_manifest, 1);
    assert_eq!(packs, 4);
    assert!(matches!(
        DirectoryOwnership::acquire(&f._temporary.0.join("data")),
        Err(DirectoryOwnershipError::Contended)
    ));
}

fn value(batch: u128) -> Reservation {
    Reservation {
        topic_id: TopicId::new(u128::MAX),
        partition_id: LogicalPartitionId::new(0),
        batch_id: BatchId::new(batch),
        first_offset: LogicalOffset::new(batch as u64),
        last_offset: LogicalOffset::new(batch as u64),
        record_count: NonZeroU32::new(1).expect("nonzero native count"),
        placement: Placement {
            virtual_lane_id: VirtualLaneId::new(3),
            ring_epoch: RingEpoch::new(1),
            leader_epoch: LeaderEpoch::new(u64::MAX),
            sequence: PlacementSequence::new(batch as u64),
        },
    }
}
fn two_frames() -> (Temporary, PathBuf, Vec<u8>) {
    let root = Temporary::new();
    let mut store = ShardStore::open(ShardStoreConfig {
        directory: root.0.join("primary"),
        shard_id: ShardId::new(0),
        target_pack_bytes: 1 << 20,
        max_batch_bytes: 4096,
    })
    .expect("actual native store");
    store
        .append(value(1), None, b"first actual frame", true)
        .expect("native writer first");
    store
        .append(value(2), None, b"second actual frame", true)
        .expect("native writer second");
    let path = store.active_path().to_owned();
    drop(store);
    let bytes = fs::read(&path).expect("actual pack");
    (root, path, bytes)
}

#[test]
fn real_truncated_native_checksum_preserves_full_max_header_and_short_unknown() {
    let (_root, path, full) = two_frames();
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("actual interrupted native source")
        .set_len((full.len() - 1) as u64)
        .expect("actual final checksum truncation");
    let raw = fs::read(&path).expect("retained truncated original");
    let view = inspect_archived_native_source(
        RecoverySourceKind::PrimaryPack,
        &path,
        Some(ShardId::new(0)),
        &raw,
        limits(),
        expiry(),
    )
    .expect("actual native tail observation");
    assert_eq!(view.extents().len(), 1);
    let tail = view.tail().expect("real incomplete final frame");
    let Some(RecoveryExtentTail::Header(header)) = tail.extent() else {
        panic!("actual complete native header");
    };
    assert_eq!(header.reservation, value(2));
    assert_eq!(header.reservation.placement.leader_epoch.get(), u64::MAX);
    assert_eq!(tail.raw_bytes(), &raw[tail.file_offset() as usize..]);
    let short = inspect_archived_native_source(
        RecoverySourceKind::PendingDeletion,
        &path,
        Some(ShardId::new(0)),
        b"SSE",
        limits(),
        expiry(),
    )
    .expect("short original suffix");
    assert!(matches!(
        short.tail().expect("raw unknown").extent(),
        Some(RecoveryExtentTail::Unknown(_))
    ));
    assert_eq!(fs::read(&path).expect("no repair"), raw);
}

#[test]
fn actual_complete_native_checksum_refusal_never_becomes_unknown_or_empty() {
    let (_root, path, mut raw) = two_frames();
    let before = fs::read(&path).expect("actual source");
    *raw.last_mut().expect("native checksum") ^= 1;
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::TierObject,
            &path,
            Some(ShardId::new(0)),
            &raw,
            limits(),
            expiry()
        ),
        Err(NativeArchivedSourceError::Native(_))
    ));
    assert_eq!(fs::read(&path).expect("physical original remains"), before);
}

#[test]
fn native_journal_events_and_original_partial_tail_match_actual_frame_decoder() {
    let root = Temporary::new();
    let path = root.0.join("coordinator.ssj");
    let event = JournalEvent::Configure {
        topic_partition: TopicPartition::new(TopicId::new(7), LogicalPartitionId::new(2)),
        ring_epoch: RingEpoch::new(9),
        shards: vec![ShardId::new(0), ShardId::new(3)],
    };
    let (mut journal, _) = CoordinatorJournal::open(&path).expect("actual journal");
    journal
        .append(&event, true)
        .expect("actual original native event");
    drop(journal);
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("actual interrupted write");
    file.write_all(b"SS").expect("actual partial prefix");
    file.sync_all().expect("tail durable");
    drop(file);
    let raw = fs::read(&path).expect("original coordinator");
    let view = inspect_archived_native_source(
        RecoverySourceKind::Coordinator,
        &path,
        Some(ShardId::new(0)),
        &raw,
        limits(),
        expiry(),
    )
    .expect("native event plus original tail");
    assert_eq!(view.journal().len(), 1);
    assert_eq!(view.journal()[0].native(), &event);
    let frame = view.journal()[0].raw_frame();
    assert_eq!(
        inspect_recovery_journal(&path, 0, frame).expect("ordinary native decoder"),
        event
    );
    let tail = view.tail().expect("original SS tail");
    assert_eq!(tail.raw_bytes(), b"SS");
    assert!(tail.extent().is_none());
}

#[test]
fn actual_pack_cost_exact_and_one_less_axes_refuse_before_corrupt_body_decode() {
    let (_root, path, raw) = two_frames();
    let kind = RecoverySourceKind::PrimaryPack;
    let path_bytes = path.as_os_str().as_encoded_bytes().len();
    let cost = native_archived_source_inspection_cost(kind, raw.len(), path_bytes, limits())
        .expect("actual plain native facts");
    let mut exact = limits();
    exact.max_declared_bytes = cost.declared_bytes();
    exact.max_work_bytes = cost.work_bytes();
    exact.max_entries = cost.entries();
    assert_eq!(
        inspect_archived_native_source(kind, &path, Some(ShardId::new(0)), &raw, exact, expiry())
            .expect("exact real source cost")
            .extents()
            .len(),
        2
    );
    let mut bad = raw.clone();
    *bad.last_mut().expect("native checksum") ^= 1;
    for axis in 0..3 {
        let mut short = exact;
        match axis {
            0 => short.max_declared_bytes -= 1,
            1 => short.max_work_bytes -= 1,
            _ => short.max_entries -= 1,
        }
        assert!(matches!(
            inspect_archived_native_source(
                kind,
                &path,
                Some(ShardId::new(0)),
                &bad,
                short,
                expiry()
            ),
            Err(NativeArchivedSourceError::Capacity)
        ));
    }
    let mut short = limits();
    short.max_frames = 1;
    assert!(matches!(
        inspect_archived_native_source(kind, &path, Some(ShardId::new(0)), &raw, short, expiry()),
        Err(NativeArchivedSourceError::Capacity)
    ));
    assert_eq!(fs::read(&path).expect("no native mutation"), raw);
}

struct Count(usize);
impl Budget for Count {
    fn node(&mut self) -> bool {
        self.0 += 1;
        true
    }
    fn maximum_depth(&self) -> usize {
        32
    }
}
#[test]
fn genuine_native_manifest_shape_precedes_typed_decode_with_exact_and_short_nodes() {
    let f = Fixture::new();
    let (path, raw) = f.raw(RecoverySourceKind::TierManifest);
    let mut count = Count(0);
    recovery_json::inspect(&raw, &mut count).expect("same shared native shape");
    let mut exact = limits();
    exact.max_json_nodes = count.0;
    assert!(
        inspect_archived_native_source(
            RecoverySourceKind::TierManifest,
            &path,
            Some(ShardId::new(0)),
            &raw,
            exact,
            expiry()
        )
        .is_ok()
    );
    exact.max_json_nodes -= 1;
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::TierManifest,
            &path,
            Some(ShardId::new(0)),
            &raw,
            exact,
            expiry()
        ),
        Err(NativeArchivedSourceError::Capacity)
    ));
    assert_eq!(fs::read(&path).expect("actual manifest retained"), raw);
}

#[test]
fn original_expiry_after_first_real_frame_or_json_shape_prevents_later_work() {
    let (_root, path, raw) = two_frames();
    let saved = Instant::now() + Duration::from_millis(250);
    let observer = Observe::new("after-frame");
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::PrimaryPack,
            &path,
            Some(ShardId::new(0)),
            &raw,
            limits(),
            saved
        ),
        Err(NativeArchivedSourceError::Deadline)
    ));
    assert_eq!(observer.phases(), vec!["after-frame"]);
    drop(observer);
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::PrimaryPack,
            &path,
            Some(ShardId::new(0)),
            &raw,
            limits(),
            saved
        ),
        Err(NativeArchivedSourceError::Deadline)
    ));
    let f = Fixture::new();
    let (path, raw) = f.raw(RecoverySourceKind::TierManifest);
    let observer = Observe::new("after-shape");
    let saved = Instant::now() + Duration::from_millis(250);
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::TierManifest,
            &path,
            Some(ShardId::new(0)),
            &raw,
            limits(),
            saved
        ),
        Err(NativeArchivedSourceError::Deadline)
    ));
    assert_eq!(observer.phases(), vec!["after-shape"]);
    assert!(matches!(
        DirectoryOwnership::acquire(&f._temporary.0.join("data")),
        Err(DirectoryOwnershipError::Contended)
    ));
}

#[test]
fn ordinary_current_loader_keeps_notfound_and_invalid_utf8_io_policy() {
    let root = Temporary::new();
    let tier = shardlog::LocalObjectTier::open(&root.0, ShardId::new(0))
        .expect("ordinary missing CURRENT");
    assert_eq!(tier.manifest().generation, 0);
    drop(tier);
    let path = root.0.join("manifests/shard-0/CURRENT");
    fs::write(&path, b"\xff").expect("actual invalid UTF8 file");
    assert!(
        matches!(shardlog::LocalObjectTier::open(&root.0, ShardId::new(0)), Err(StorageError::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData)
    );
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::TierCurrent,
            &path,
            Some(ShardId::new(0)),
            b"\xff",
            limits(),
            expiry()
        ),
        Err(NativeArchivedSourceError::CurrentUtf8)
    ));
    let text = b" \t18446744073709551615\n";
    assert_eq!(
        inspect_archived_native_source(
            RecoverySourceKind::TierCurrent,
            &path,
            Some(ShardId::new(0)),
            text,
            limits(),
            expiry()
        )
        .expect("actual native trim grammar")
        .current_generation(),
        Some(u64::MAX)
    );
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::TierCurrent,
            &path,
            Some(ShardId::new(0)),
            b"18446744073709551616",
            limits(),
            expiry()
        ),
        Err(NativeArchivedSourceError::CurrentValue)
    ));
}

#[test]
fn actual_manifest_native_version_shard_and_trailing_json_refusals_match() {
    let f = Fixture::new();
    let (path, raw) = f.raw(RecoverySourceKind::TierManifest);
    let mut native = decode_recovery_tier_manifest(&raw).expect("actual native typed original");
    assert!(native.validate(ShardId::new(1)).is_err());
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::TierManifest,
            &path,
            Some(ShardId::new(1)),
            &raw,
            limits(),
            expiry()
        ),
        Err(NativeArchivedSourceError::Native(_))
    ));
    native.format_version = u8::MAX;
    let bad = serde_json::to_vec(&native).expect("native serde invalid-version fixture");
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::UnreferencedTierManifest,
            &path,
            Some(ShardId::new(0)),
            &bad,
            limits(),
            expiry()
        ),
        Err(NativeArchivedSourceError::Native(_))
    ));
    let mut trailing = raw.clone();
    trailing.extend_from_slice(b" true");
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::TierManifest,
            &path,
            Some(ShardId::new(0)),
            &trailing,
            limits(),
            expiry()
        ),
        Err(NativeArchivedSourceError::JsonShape)
    ));
    let mut invalid = limits();
    invalid.max_json_depth = 33;
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::TierManifest,
            &path,
            Some(ShardId::new(0)),
            &raw,
            invalid,
            expiry()
        ),
        Err(NativeArchivedSourceError::InvalidLimits)
    ));
}

#[test]
fn actual_native_ignored_json_extension_still_pays_shared_shape_depth() {
    let f = Fixture::new();
    let (path, raw) = f.raw(RecoverySourceKind::TierManifest);
    let original = decode_recovery_tier_manifest(&raw).expect("native original");
    assert_eq!(raw.last(), Some(&b'}'));
    let mut nested = raw[..raw.len() - 1].to_vec();
    nested.extend_from_slice(b",\"ordinary_ignored_extension\":[[[[[[[[0]]]]]]]]}");
    assert_eq!(
        decode_recovery_tier_manifest(&nested).expect("same native unknown-field policy"),
        original
    );
    let observed = inspect_archived_native_source(
        RecoverySourceKind::TierManifest,
        &path,
        Some(ShardId::new(0)),
        &nested,
        limits(),
        expiry(),
    )
    .expect("native shape and typed fields");
    assert_eq!(observed.manifest(), Some(&original));
    let mut tighter = limits();
    tighter.max_json_depth = 4;
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::TierManifest,
            &path,
            Some(ShardId::new(0)),
            &nested,
            tighter,
            expiry()
        ),
        Err(NativeArchivedSourceError::JsonShape)
    ));
}

#[test]
fn original_expiry_and_all_finite_caps_apply_before_even_empty_or_malformed_sources() {
    let path = Path::new("ordinary-native-diagnostic");
    let mut invalid = limits();
    invalid.max_entries = 0;
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::Coordinator,
            path,
            Some(ShardId::new(0)),
            b"",
            invalid,
            Instant::now()
        ),
        Err(NativeArchivedSourceError::Deadline)
    ));
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::Coordinator,
            path,
            Some(ShardId::new(0)),
            b"",
            invalid,
            expiry()
        ),
        Err(NativeArchivedSourceError::InvalidLimits)
    ));
    assert!(matches!(
        native_archived_source_inspection_cost(
            RecoverySourceKind::PrimaryPack,
            usize::MAX,
            1,
            limits()
        ),
        Err(NativeArchivedSourceError::Capacity)
    ));
    let (_root, actual_path, raw) = two_frames();
    let mut short = limits();
    short.max_source_bytes = raw.len() - 1;
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::PrimaryPack,
            &actual_path,
            Some(ShardId::new(0)),
            &raw,
            short,
            expiry()
        ),
        Err(NativeArchivedSourceError::Capacity)
    ));
    let mut short = limits();
    short.max_path_bytes = actual_path.as_os_str().as_encoded_bytes().len() - 1;
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::PrimaryPack,
            &actual_path,
            Some(ShardId::new(0)),
            &raw,
            short,
            expiry()
        ),
        Err(NativeArchivedSourceError::Capacity)
    ));
}

#[test]
fn real_native_atomic_producer_and_physically_unbound_manifest_remain_ordinary() {
    let root = Temporary::new();
    let mut store = ShardStore::open(ShardStoreConfig {
        directory: root.0.join("primary"),
        shard_id: ShardId::new(7),
        target_pack_bytes: 1 << 20,
        max_batch_bytes: 4096,
    })
    .expect("native shard seven writer");
    let payload = b"\0actual native atomic body\xff";
    let digest = xxhash_rust::xxh3::xxh3_128(payload).to_le_bytes();
    let producer = ProducerSequence {
        producer_id: u128::MAX,
        epoch: u32::MAX,
        first_sequence: 9,
        event_id: None,
        digest_algorithm: shardlog::DIGEST_XXH3_128,
        payload_digest: digest,
    };
    let group = AtomicGroupSequence {
        transaction_id: u128::MAX,
        chunk_sequence: 3,
        payload_digest: digest,
    };
    store
        .append_atomic(value(1), Some(producer), Some(group), payload, true)
        .expect("actual native atomic writer");
    let path = store.active_path().to_owned();
    drop(store);
    let raw = fs::read(&path).expect("original native complete frame");
    let view = inspect_archived_native_source(
        RecoverySourceKind::UnreferencedTierObject,
        &path,
        None,
        &raw,
        limits(),
        expiry(),
    )
    .expect("ordinary full native fields");
    assert_eq!(view.extents()[0].native().producer(), Some(producer));
    assert_eq!(view.extents()[0].native().atomic_group(), Some(group));
    assert_eq!(view.extents()[0].native().payload(), payload);
    let mut tier = shardlog::LocalObjectTier::open(root.0.join("objects"), ShardId::new(7))
        .expect("actual native tier shard seven");
    tier.offload_pack(
        0,
        &path,
        vec![shardlog::TierExtent::from_reservation(value(1))],
    )
    .expect("actual native manifest producer");
    let manifest_path = root
        .0
        .join("objects/manifests/shard-7/manifest-00000000000000000001.json");
    let raw = fs::read(&manifest_path).expect("original native shard seven manifest");
    let view = inspect_archived_native_source(
        RecoverySourceKind::UnreferencedTierManifest,
        &manifest_path,
        None,
        &raw,
        limits(),
        expiry(),
    )
    .expect("physically unbound own-shard native validation");
    assert_eq!(view.manifest().expect("actual native manifest").shard_id, 7);
    assert_eq!(
        view.manifest().expect("every old extent").objects[0].extents[0].leader_epoch,
        u64::MAX
    );
    assert!(matches!(
        inspect_archived_native_source(
            RecoverySourceKind::UnreferencedTierManifest,
            &manifest_path,
            Some(ShardId::new(0)),
            &raw,
            limits(),
            expiry()
        ),
        Err(NativeArchivedSourceError::Native(_))
    ));
}
