//! Real native producers, object offload and OS leases; no history/permission factory.
use super::*;
use crate::{EngineResult, StreamEngine, TopicConfig};
use shard_stream_protocol::{AppendRequest, Durability, FetchMode, FetchRequest};
use std::sync::{Condvar, Mutex};
use std::thread;

fn write_engine_native(root: &Path) {
    let (mut journal, _) =
        CoordinatorJournal::open(root.join("coordinator.ssj")).expect("actual native journal");
    journal
        .append(
            &JournalEvent::Configure {
                topic_partition: TopicPartition::new(TopicId::new(11), LogicalPartitionId::new(0)),
                ring_epoch: RingEpoch::new(1),
                shards: vec![ShardId::new(3)],
            },
            true,
        )
        .expect("actual ring includes the real encoded virtual lane3");
    let mut store = ShardStore::open(store_config(root)).expect("real native store");
    store
        .append(reservation(0, 91), None, b"earlier native high term", true)
        .expect("actual first native frame");
    store
        .append(reservation(1, 5), None, b"later native low term", true)
        .expect("actual native accepted lower later observation");
}
fn write_engine_tier(root: &Path) -> (PathBuf, PathBuf) {
    write_engine_native(root);
    let mut store = ShardStore::open(store_config(root)).expect("real native primary");
    let pack = store
        .seal_active()
        .expect("seal")
        .expect("native nonempty pack");
    let coverage = store
        .catalog()
        .into_iter()
        .filter(|entry| entry.pack_sequence == pack.sequence)
        .map(|entry| TierExtent::from_reservation(entry.reservation))
        .collect();
    let tier = root.join("tier");
    LocalObjectTier::open(&tier, ShardId::new(0))
        .expect("actual native tier")
        .offload_pack(pack.sequence, &pack.path, coverage)
        .expect("actual native offload");
    (tier, pack.path)
}

fn fetch() -> FetchRequest {
    FetchRequest {
        request_id: 1,
        topic_id: TopicId::new(11),
        partition_id: LogicalPartitionId::new(0),
        start_offset: LogicalOffset::new(0),
        max_bytes: 4096,
        mode: FetchMode::Ordered,
    }
}
fn object_append(id: u128, body: &'static [u8]) -> AppendRequest {
    AppendRequest {
        request_id: id,
        topic_id: TopicId::new(11),
        partition_id: LogicalPartitionId::new(0),
        record_count: 1,
        payload: bytes::Bytes::from_static(body),
        durability: Durability::Object,
        producer: None,
        atomic_group: None,
        leader_epoch: None,
        extension_context: None,
    }
}
fn ample() -> NativeRecoveryLimits {
    NativeRecoveryLimits {
        max_files: 100_000,
        max_entries: 100_000_000,
        max_declared_bytes: 512 * 1024 * 1024,
        max_read_bytes: 512 * 1024 * 1024,
        max_work_bytes: 8 * 1024 * 1024 * 1024,
        ..limits()
    }
}
fn engine_census(
    root: &Path,
    tier: Option<&Path>,
    caps: NativeRecoveryLimits,
    expiry: Instant,
) -> std::result::Result<NativeRecoveryCensus, NativeRecoveryError> {
    let owner = DirectoryOwnership::acquire(root).expect("real primary OS custody");
    let mut cfg = config(root);
    cfg.object_store_dir = tier.map(Path::to_owned);
    let tier = tier.map(|path| {
        DirectoryOwnership::acquire(path)
            .expect("real tier OS custody")
            .root_handle()
            .expect("actual opaque root")
    });
    NativeRecoveryCensus::read_for_engine(cfg, owner, tier, caps, expiry)
}
fn remove_copy_temporary(root: &Path) {
    for entry in fs::read_dir(root.join("shards/shard-0")).expect("real native directory") {
        let p = entry.expect("entry").path();
        if p.file_name()
            .expect("name")
            .to_string_lossy()
            .ends_with(".owned-restore")
        {
            fs::remove_file(p).expect("reset only this fixture's exact copied temporary");
        }
    }
}

#[test]
fn actual_engine_object_ack_missing_primary_owned_restore_fetch_and_real_restart() {
    let temp = Temporary::new();
    let tier = temp.0.join("tier");
    let mut cfg = config(&temp.0);
    cfg.object_store_dir = Some(tier.clone());
    {
        let producer = StreamEngine::open(cfg.clone()).expect("actual ordinary native producer");
        producer
            .create_topic(TopicConfig {
                topic_id: TopicId::new(11),
                partitions: 1,
                shards: None,
            })
            .expect("actual durable journal");
        producer
            .append(object_append(1, b"genuine object-only payload"))
            .expect("actual native object ACK offload");
    }
    let native_tier = LocalObjectTier::open(&tier, ShardId::new(0)).expect("native CURRENT");
    let object = native_tier.manifest().objects[0].clone();
    let primary = temp
        .0
        .join("shards/shard-0")
        .join(format!("pack-{:020}.sse", object.pack_sequence));
    let expected = fs::read(tier.join(&object.object_key)).expect("actual native BLAKE3 object");
    drop(native_tier);
    fs::remove_file(&primary).expect("real primary copy absent");
    let expiry = Instant::now() + Duration::from_secs(30);
    let census = engine_census(&temp.0, Some(&tier), ample(), expiry)
        .expect("actual nonmutating pinned native snapshot");
    assert!(!primary.exists());
    let storage = census
        .restore_storage()
        .expect("bounded no-replace real copy before native scan");
    assert_eq!(
        fs::read(&primary).expect("native primary restored"),
        expected
    );
    let owned = StreamEngine::open_recovered_storage(storage, None, None, None, None)
        .expect("same concrete stores into normal workers");
    assert_eq!(owned.absolute_expiry(), expiry);
    assert!(
        owned
            .engine()
            .has_directory_ownership(owned.directory_ownership())
    );
    let rows = owned
        .engine()
        .fetch(fetch())
        .expect("NORMAL native catalog/fetch reads restored object payload");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].payload.as_ref(), b"genuine object-only payload");
    assert!(DirectoryOwnership::acquire(&temp.0).is_err());
    assert!(DirectoryOwnership::acquire(&tier).is_err());
    assert!(ShardStore::open(store_config(&temp.0)).is_err());
    owned
        .engine()
        .append(object_append(
            2,
            b"second actual object offload with same retained lease",
        ))
        .expect("no self-lock during native copy and CURRENT publication");
    drop(rows);
    drop(owned);
    let reopened =
        StreamEngine::open(cfg).expect("ordinary native restart uses actual restored primary");
    let rows = reopened
        .fetch(fetch())
        .expect("actual native restart fetch");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].payload.as_ref(), b"genuine object-only payload");
}

#[test]
fn missing_whole_primary_shard_rehydrates_current_objects_before_native_scan() {
    let temp = Temporary::new();
    let (tier, pack) = write_engine_tier(&temp.0);
    let expected = fs::read(&pack).expect("native sealed bytes");
    fs::remove_dir_all(temp.0.join("shards")).expect("whole primary shard absent");
    let storage = engine_census(
        &temp.0,
        Some(&tier),
        ample(),
        Instant::now() + Duration::from_secs(30),
    )
    .expect("retains missing paths honestly")
    .restore_storage()
    .expect("creates and guards actual missing primary before copy");
    assert_eq!(fs::read(pack).expect("restored exact pack"), expected);
    let engine = StreamEngine::open_recovered_storage(storage, None, None, None, None)
        .expect("actual normal native recovered state");
    let rows = engine.engine().fetch(fetch()).expect("normal fetch");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].payload.as_ref(), b"earlier native high term");
    drop(rows);
    engine
        .engine()
        .append(object_append(99, b"new object after whole-primary loss"))
        .expect("restored CURRENT pack stays sealed; actual successor offloads normally");
    assert_eq!(
        engine
            .engine()
            .fetch(fetch())
            .expect("normal fetch after successor append")
            .len(),
        3
    );
}

#[test]
fn conflicting_valid_primary_and_pending_bytes_are_never_overwritten_or_deleted() {
    for pending in [false, true] {
        let temp = Temporary::new();
        let (tier, pack) = write_engine_tier(&temp.0);
        // A real native writer produces another valid SSE8 body at the same
        // sequence; this is a content conflict, not a manufactured checksum.
        fs::remove_file(&pack).expect("remove old primary copy only");
        let other = Temporary::new();
        let mut store =
            ShardStore::open(store_config(&other.0)).expect("real native conflicting producer");
        store
            .append(
                reservation(0, 999),
                None,
                b"different genuine native pack",
                true,
            )
            .expect("native frame");
        let actual = fs::read(store.active_path()).expect("valid native bytes");
        drop(store);
        let destination = if pending {
            pack.with_extension("sse.gc-pending")
        } else {
            pack.clone()
        };
        fs::write(&destination, &actual).expect("actual conflicting recognized source");
        let census = engine_census(
            &temp.0,
            Some(&tier),
            ample(),
            Instant::now() + Duration::from_secs(30),
        )
        .expect("both real native sources decode");
        let error = census
            .restore_storage()
            .expect_err("conflicting real primary/pending must refuse BEFORE mutable scan/GC");
        assert_eq!(
            fs::read(&destination).expect("unchanged conflict/raw evidence"),
            actual
        );
        assert!(DirectoryOwnership::acquire(&temp.0).is_err());
        drop(error);
        DirectoryOwnership::acquire(&temp.0).expect("error cleanup releases real OS owner");
    }
}

#[test]
fn late_destination_and_changed_actual_source_refuse_without_replacement() {
    for source_change in [false, true] {
        let temp = Temporary::new();
        let (tier, pack) = write_engine_tier(&temp.0);
        fs::remove_file(&pack).expect("object-only source");
        let census = engine_census(
            &temp.0,
            Some(&tier),
            ample(),
            Instant::now() + Duration::from_secs(30),
        )
        .expect("genuine pinned sources");
        let changed = if source_change {
            census
                .sources()
                .find(|s| s.kind == RecoverySourceKind::TierObject)
                .expect("actual CURRENT source")
                .path
                .clone()
        } else {
            pack.clone()
        };
        let bytes = b"actual post-census mutation must remain untouched";
        fs::write(&changed, bytes).expect("real filesystem race");
        let error = census
            .restore_storage()
            .expect_err("original identity/hash or absent-destination binding refuses");
        assert_eq!(fs::read(&changed).expect("no replacement"), bytes);
        if source_change {
            assert!(!pack.exists());
        }
        drop(error);
    }
}

#[test]
fn complete_restore_exact_caps_and_each_one_less_precede_any_payload_publication() {
    let temp = Temporary::new();
    let (tier, pack) = write_engine_tier(&temp.0);
    fs::remove_file(&pack).expect("object-only");
    let storage = engine_census(
        &temp.0,
        Some(&tier),
        ample(),
        Instant::now() + Duration::from_secs(30),
    )
    .expect("actual census")
    .restore_storage()
    .expect("measure actual complete native reservation");
    let used = storage.usage();
    drop(storage);
    fs::remove_file(&pack).expect("fixture restores original missing-primary boundary");
    remove_copy_temporary(&temp.0);
    let exact = NativeRecoveryLimits {
        max_files: used.files,
        max_entries: used.entries,
        max_read_bytes: used.read_bytes,
        max_declared_bytes: used.declared_bytes,
        max_work_bytes: used.work_bytes,
        ..ample()
    };
    for which in 0..5 {
        let mut short = exact;
        match which {
            0 => short.max_files -= 1,
            1 => short.max_entries -= 1,
            2 => short.max_read_bytes -= 1,
            3 => short.max_declared_bytes -= 1,
            _ => short.max_work_bytes -= 1,
        }
        let result = engine_census(
            &temp.0,
            Some(&tier),
            short,
            Instant::now() + Duration::from_secs(30),
        )
        .and_then(NativeRecoveryCensus::restore_storage);
        assert!(
            result.is_err(),
            "one less actual whole reservation must refuse"
        );
        assert!(
            !pack.exists(),
            "all copy+native-open reservations precede primary publication"
        );
        drop(result);
    }
    let storage = engine_census(
        &temp.0,
        Some(&tier),
        exact,
        Instant::now() + Duration::from_secs(30),
    )
    .expect("exact census")
    .restore_storage()
    .expect("exact real full restore cap fits");
    assert_eq!(storage.usage().files, used.files);
    assert_eq!(storage.usage().declared_bytes, used.declared_bytes);
    assert!(pack.exists());
}

#[test]
fn original_expiry_refuses_read_restore_and_engine_without_releasing_custody_early() {
    let temp = Temporary::new();
    let (tier, pack) = write_engine_tier(&temp.0);
    fs::remove_file(&pack).expect("object-only");
    let error = engine_census(&temp.0, Some(&tier), ample(), Instant::now())
        .expect_err("actual expired constructor");
    assert!(DirectoryOwnership::acquire(&temp.0).is_err());
    drop(error);
    let expiry = Instant::now() + Duration::from_secs(1);
    let census =
        engine_census(&temp.0, Some(&tier), ample(), expiry).expect("genuine earlier saved E");
    thread::sleep(expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(20));
    let error = census
        .restore_storage()
        .expect_err("no new timeout for copy/native scan");
    assert_eq!(error.absolute_expiry(), expiry);
    assert!(!pack.exists());
    assert!(DirectoryOwnership::acquire(&tier).is_err());
    drop(error);
    let expiry = Instant::now() + Duration::from_secs(1);
    let storage = engine_census(&temp.0, Some(&tier), ample(), expiry)
        .expect("later actual separate attempt")
        .restore_storage()
        .expect("native store ready");
    thread::sleep(expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(20));
    let error = StreamEngine::open_recovered_storage(storage, None, None, None, None)
        .expect_err("engine inherits same expired E");
    assert_eq!(error.absolute_expiry(), expiry);
    assert!(DirectoryOwnership::acquire(&temp.0).is_err());
    drop(error);
    let owned = StreamEngine::open_recovered_storage(
        engine_census(
            &temp.0,
            Some(&tier),
            ample(),
            Instant::now() + Duration::from_secs(30),
        )
        .expect("actual healthy later owner")
        .restore_storage()
        .expect("native restore"),
        None,
        None,
        None,
        None,
    )
    .expect("genuine later owner progress");
    assert_eq!(
        owned.engine().fetch(fetch()).expect("normal fetch").len(),
        2
    );
}

#[test]
fn engine_refusal_keeps_same_native_guards_and_original_observations() {
    let temp = Temporary::new();
    write_engine_native(&temp.0);
    let census = engine_census(
        &temp.0,
        None,
        ample(),
        Instant::now() + Duration::from_secs(30),
    )
    .expect("real census");
    let mut storage = census.restore_storage().expect("real storage");
    // Tighten only the genuine already-used policy, without fabricating state.
    storage.attempt.limits.max_declared_bytes = storage.usage().declared_bytes;
    let error = StreamEngine::open_recovered_storage(storage, None, None, None, None)
        .expect_err("next native worker reservation must refuse");
    assert!(DirectoryOwnership::acquire(&temp.0).is_err());
    assert!(ShardStore::open(store_config(&temp.0)).is_err());
    drop(error);
    let store = ShardStore::open(store_config(&temp.0))
        .expect("actual native lease released after error cleanup");
    assert_eq!(store.catalog().len(), 2);
}

type DropGate = Arc<(Mutex<(bool, bool)>, Condvar)>;
#[derive(Debug)]
struct BlockingDropTransport {
    gate: DropGate,
    panic: bool,
}
impl crate::ReplicationTransport for BlockingDropTransport {
    fn install_progress(&self, _: crate::ReplicationProgress) -> EngineResult<()> {
        assert!(!self.panic, "actual external installation unwind");
        Err(EngineError::CorruptState(
            "actual extension installation refuses".into(),
        ))
    }
    fn replicate(&self, _: shard_stream_protocol::ReplicaAppend, _: u32) -> EngineResult<u32> {
        Err(EngineError::CorruptState(
            "fixture never acknowledges replication".into(),
        ))
    }
}
impl Drop for BlockingDropTransport {
    fn drop(&mut self) {
        let (lock, changed) = &*self.gate;
        let mut state = lock.lock().expect("real destructor state");
        state.0 = true;
        changed.notify_all();
        while !state.1 {
            state = changed.wait(state).expect("physical destructor release");
        }
    }
}
struct ReleaseDrop(DropGate);
impl Drop for ReleaseDrop {
    fn drop(&mut self) {
        let (m, c) = &*self.0;
        m.lock().expect("release actual physical cleanup").1 = true;
        c.notify_all();
    }
}
#[test]
fn actual_failed_worker_installation_and_unwind_keep_custody_through_blocked_component_drop() {
    for panic in [false, true] {
        let temp = Temporary::new();
        write_engine_native(&temp.0);
        let storage = engine_census(
            &temp.0,
            None,
            ample(),
            Instant::now() + Duration::from_secs(30),
        )
        .expect("actual census")
        .restore_storage()
        .expect("native stores");
        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
        thread::scope(|scope| {
            let _release = ReleaseDrop(gate.clone());
            let transport = Arc::new(BlockingDropTransport {
                gate: gate.clone(),
                panic,
            });
            let job = scope.spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    StreamEngine::open_recovered_storage(storage, None, Some(transport), None, None)
                }));
                match result {
                    Ok(error) => {
                        assert!(!panic, "unwind case must reach the actual callback panic");
                        drop(error.expect_err("actual callback refuses after workers exist"));
                    }
                    Err(_) => assert!(panic),
                }
            });
            let (lock, changed) = &*gate;
            let mut state = lock.lock().expect("physical cleanup observation");
            while !state.0 {
                state = changed
                    .wait(state)
                    .expect("wait actual blocking destructor");
            }
            assert!(DirectoryOwnership::acquire(&temp.0).is_err());
            assert!(ShardStore::open(store_config(&temp.0)).is_err());
            assert!(!job.is_finished());
            state.1 = true;
            changed.notify_all();
            drop(state);
            job.join()
                .expect("all actual cleanup completes before join");
        });
        DirectoryOwnership::acquire(&temp.0)
            .expect("later same OS owner progress after real cleanup");
        ShardStore::open(store_config(&temp.0)).expect("actual native worker lease fully released");
    }
}

#[test]
fn native_repair_retains_original_partial_tails_and_does_not_touch_opaque_files() {
    let temp = Temporary::new();
    write_engine_native(&temp.0);
    let pack = temp.0.join("shards/shard-0/pack-00000000000000000000.sse");
    OpenOptions::new()
        .append(true)
        .open(&pack)
        .expect("actual native pack")
        .write_all(b"SSE")
        .expect("actual incomplete native prefix");
    OpenOptions::new()
        .append(true)
        .open(temp.0.join("coordinator.ssj"))
        .expect("actual journal")
        .write_all(b"SS")
        .expect("actual incomplete journal prefix");
    let opaque = temp.0.join("shards/shard-0/opaque-existing-input");
    fs::write(&opaque, b"exact unknown evidence must stay unchanged")
        .expect("real original unknown file");
    let census = engine_census(
        &temp.0,
        None,
        ample(),
        Instant::now() + Duration::from_secs(30),
    )
    .expect("real native tails retained before mutation");
    assert!(
        census
            .sources()
            .any(|s| s.kind == RecoverySourceKind::PrimaryPack && s.partial_tail == b"SSE")
    );
    let owned = StreamEngine::open_recovered_storage(
        census.restore_storage().expect("native primary repair"),
        None,
        None,
        None,
        None,
    )
    .expect("native journal repair and real workers");
    assert!(
        owned
            .prior_sources()
            .any(|s| s.kind == RecoverySourceKind::PrimaryPack && s.partial_tail == b"SSE")
    );
    assert!(
        owned
            .prior_sources()
            .any(|s| s.kind == RecoverySourceKind::Coordinator && s.partial_tail == b"SS")
    );
    assert_eq!(
        fs::read(&opaque).expect("unchanged opaque input"),
        b"exact unknown evidence must stay unchanged"
    );
    assert_eq!(
        owned
            .engine()
            .fetch(fetch())
            .expect("native normal fetch after repair")
            .len(),
        2
    );
}

#[test]
fn genuine_native_current_pack_maximum_refuses_immutable_successor_before_any_copy() {
    let temp = Temporary::new();
    write_engine_native(&temp.0);
    let mut store = ShardStore::open(store_config(&temp.0)).expect("real native producer");
    let pack = store
        .seal_active()
        .expect("seal")
        .expect("actual native pack");
    let coverage = store
        .catalog()
        .into_iter()
        .filter(|e| e.pack_sequence == pack.sequence)
        .map(|e| TierExtent::from_reservation(e.reservation))
        .collect();
    let tier = temp.0.join("tier");
    // This is accepted by the real native tier writer. The pack sequence is
    // manifest metadata, not an encoded SSE8 or authenticated leader-term field.
    LocalObjectTier::open(&tier, ShardId::new(0))
        .expect("native tier")
        .offload_pack(u64::MAX, &pack.path, coverage)
        .expect("genuine native MAX sequence object");
    drop(store);
    fs::remove_dir_all(temp.0.join("shards")).expect("actual whole-primary loss");
    let census = engine_census(
        &temp.0,
        Some(&tier),
        ample(),
        Instant::now() + Duration::from_secs(30),
    )
    .expect("actual MAX CURRENT snapshot");
    let error = census
        .restore_storage()
        .expect_err("no native empty successor exists above MAX; no fabricated zero/reset");
    assert!(
        !temp.0.join("shards").exists(),
        "checked exhaustion BEFORE first primary mutation"
    );
    assert!(DirectoryOwnership::acquire(&tier).is_err());
    drop(error);
}

#[test]
fn actual_object_restore_then_native_gc_reclaims_links_and_snapshot_fds_before_restart() {
    let temp = Temporary::new();
    let tier = temp.0.join("tier");
    let mut cfg = config(&temp.0);
    cfg.object_store_dir = Some(tier.clone());
    {
        let engine = StreamEngine::open(cfg.clone()).expect("actual native producer");
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(11),
                partitions: 1,
                shards: None,
            })
            .expect("native journal");
        engine
            .append(object_append(1, b"first object"))
            .expect("actual pack0 offload");
        engine
            .append(object_append(2, b"second object"))
            .expect("actual pack1 offload");
    }
    let shard = temp.0.join("shards/shard-0");
    let pack0 = shard.join("pack-00000000000000000000.sse");
    let pack1 = shard.join("pack-00000000000000000001.sse");
    fs::remove_file(&pack0).expect("real object-only missing pack0");
    let storage = engine_census(
        &temp.0,
        Some(&tier),
        ample(),
        Instant::now() + Duration::from_secs(30),
    )
    .expect("actual pinned snapshot")
    .restore_storage()
    .expect("actual no-replace copy and temp cleanup");
    let identities = [
        fs::metadata(&pack0).expect("restored actual primary").dev(),
        fs::metadata(&pack1).expect("original actual primary").dev(),
    ];
    let inodes = [
        fs::metadata(&pack0).expect("restored inode").ino(),
        fs::metadata(&pack1).expect("original inode").ino(),
    ];
    for pack in [&pack0, &pack1] {
        assert_eq!(fs::metadata(pack).expect("one canonical link").nlink(), 1);
    }
    assert!(
        fs::read_dir(&shard)
            .expect("actual native directory")
            .all(|e| !e
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .ends_with(".owned-restore"))
    );
    let engine = StreamEngine::open_recovered_storage(storage, None, None, None, None)
        .expect("actual completed worker setup retires snapshot descriptors");
    let tp = TopicPartition::new(TopicId::new(11), LogicalPartitionId::new(0));
    engine
        .engine()
        .truncate_partition(tp, LogicalOffset::new(2))
        .expect("real journal checkpoint and native pack GC");
    assert!(!pack0.exists());
    assert!(!pack1.exists());
    assert!(
        engine
            .prior_sources()
            .any(|f| f.device == identities[1] && f.inode == inodes[1]),
        "ordinary old facts survive source FD retirement"
    );
    #[cfg(target_os = "linux")]
    for entry in fs::read_dir("/proc/self/fd").expect("actual process descriptor census") {
        let entry = entry.expect("actual FD name");
        if let Ok(metadata) = fs::metadata(entry.path()) {
            assert!(
                !(0..2).any(|i| metadata.dev() == identities[i] && metadata.ino() == inodes[i]),
                "native GC must not leave a retired copy or original snapshot descriptor holding primary bytes"
            );
        }
    }
    assert!(
        DirectoryOwnership::acquire(&temp.0).is_err(),
        "SAME physical custody survives retirement and GC"
    );
    drop(engine);
    let reopened =
        StreamEngine::open(cfg).expect("actual ordinary native restart after restore and GC");
    let mut request = fetch();
    request.start_offset = LogicalOffset::new(2);
    assert!(
        reopened
            .fetch(request)
            .expect("real retained checkpoint has no old payload")
            .is_empty()
    );
    let appended = reopened
        .append(object_append(3, b"new after actual GC and restart"))
        .expect("normal native producer continues from durable checkpoint");
    assert_eq!(appended.first_offset, LogicalOffset::new(2));
    assert_eq!(
        reopened.fetch(request).expect("actual normal fetch")[0]
            .payload
            .as_ref(),
        b"new after actual GC and restart"
    );
}

fn write_checkpoint_then_legacy_truncate(root: &Path, sequence: u64) {
    let tp = TopicPartition::new(TopicId::new(11), LogicalPartitionId::new(0));
    let (mut journal, _) =
        CoordinatorJournal::open(root.join("coordinator.ssj")).expect("native journal");
    for event in [
        JournalEvent::Configure {
            topic_partition: tp,
            ring_epoch: RingEpoch::new(1),
            shards: vec![ShardId::new(3)],
        },
        JournalEvent::TruncateCheckpoint {
            topic_partition: tp,
            log_start: LogicalOffset::new(sequence),
            next_batch_id: BatchId::new(u128::from(sequence)),
            next_offset: LogicalOffset::new(sequence),
            next_placement_sequence: PlacementSequence::new(sequence),
        },
        JournalEvent::Truncate {
            topic_partition: tp,
            log_start: LogicalOffset::new(sequence),
        },
    ] {
        journal
            .append(&event, true)
            .expect("real native ordered retention event");
    }
    let mut store = ShardStore::open(store_config(root)).expect("real native retained pack");
    store
        .append(
            reservation(sequence, 73),
            None,
            b"retained native boundary",
            true,
        )
        .expect("native SSE8 reservation");
}

#[test]
fn ordered_native_truncate_clears_checkpoint_sizing_floor_before_abort_materialization() {
    let temp = Temporary::new();
    let sequence = u64::try_from(ordered_segment_map::DEFAULT_SEGMENT_CAPACITY)
        .expect("native dense floor")
        + 17;
    write_checkpoint_then_legacy_truncate(&temp.0, sequence);
    let (journal, recovered) = CoordinatorJournal::open(temp.0.join("coordinator.ssj"))
        .expect("actual ordered native replay");
    let store = ShardStore::open(store_config(&temp.0)).expect("actual native catalog");
    let catalog = store
        .catalog()
        .into_iter()
        .map(|entry| {
            (
                (
                    TopicPartition::new(entry.reservation.topic_id, entry.reservation.partition_id),
                    entry.reservation.batch_id,
                ),
                entry,
            )
        })
        .collect();
    assert_eq!(
        crate::coordinator::recovery_batch_slots(
            &recovered.events,
            &catalog,
            Instant::now() + Duration::from_secs(30)
        )
        .expect("ordered sizing"),
        usize::try_from(sequence).expect("slots") + 1
    );
    let expiry = Instant::now() + Duration::from_millis(50);
    let appends = std::rc::Rc::new(std::cell::Cell::new(0));
    let observed = appends.clone();
    crate::coordinator::RECOVERY_REDUCE_STEP.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |step| {
            assert_eq!(step, "abort-appended");
            observed.set(observed.get() + 1);
            thread::sleep(
                expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
            );
        }))
    });
    let late = crate::coordinator::recover_partition_states_until(
        &config(&temp.0),
        &recovered.events,
        &catalog,
        1,
        expiry,
    );
    crate::coordinator::RECOVERY_REDUCE_STEP.with(|hook| *hook.borrow_mut() = None);
    assert!(late.is_err());
    assert_eq!(
        appends.get(),
        1,
        "a real first abort settled late; no second allocation began"
    );
    assert_eq!(
        crate::coordinator::recover_partition_states(
            &config(&temp.0),
            &recovered.events,
            &catalog,
            1
        )
        .expect("actual native accepts legacy reset and builds then truncates real aborts")
        .len(),
        1
    );
    drop(store);
    drop(journal);
    let original_owner = DirectoryOwnership::acquire(&temp.0).expect("actual original OS owner");
    let census = NativeRecoveryCensus::read_for_engine(
        config(&temp.0),
        original_owner.clone(),
        None,
        ample(),
        Instant::now() + Duration::from_secs(30),
    )
    .expect("census");
    assert!(Arc::ptr_eq(census.directory_ownership(), &original_owner));
    let storage = census.restore_storage().expect("native stores");
    drop(original_owner);
    let engine = StreamEngine::open_recovered_storage(storage, None, None, None, None)
        .expect("actual native recovery");
    let used = engine.usage();
    let mut request = fetch();
    request.start_offset = LogicalOffset::new(sequence);
    assert_eq!(
        engine
            .engine()
            .fetch(request)
            .expect("actual retained native boundary")
            .len(),
        1
    );
    drop(engine);
    // Exact admitted native output succeeds; one less refuses through the real
    // constructor before its actual synthetic-abort reducer materialization.
    for less in [false, true] {
        let mut caps = ample();
        caps.max_declared_bytes = used.declared_bytes - usize::from(less);
        let storage = engine_census(
            &temp.0,
            None,
            caps,
            Instant::now() + Duration::from_secs(30),
        )
        .expect("all earlier census work fits")
        .restore_storage()
        .expect("all earlier native store work fits");
        let result = StreamEngine::open_recovered_storage(storage, None, None, None, None);
        assert_eq!(result.is_ok(), !less);
        drop(result);
    }
}

#[test]
fn late_first_native_pack_publication_starts_no_second_pack_or_recovered_worker() {
    let temp = Temporary::new();
    write_engine_native(&temp.0);
    let mut store = ShardStore::open(store_config(&temp.0)).expect("real native pack producer");
    let first = store
        .seal_active()
        .expect("seal first")
        .expect("first native sealed pack");
    store
        .append(reservation(2, 91), None, b"next actual native pack", true)
        .expect("native next reservation");
    let second = store
        .seal_active()
        .expect("seal second")
        .expect("second native sealed pack");
    drop(store);
    let tier = temp.0.join("tier");
    drop(LocalObjectTier::open(&tier, ShardId::new(0)).expect("empty actual native tier"));
    let expiry = Instant::now() + Duration::from_secs(2);
    let storage = engine_census(&temp.0, Some(&tier), ample(), expiry)
        .expect("native pinned history")
        .restore_storage()
        .expect("actual native stores");
    let observations = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let seen = observations.clone();
    crate::worker::RECOVERED_OPEN_STEP.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |step| {
            seen.borrow_mut().push(step.to_owned());
            if step == "pack-complete" {
                thread::sleep(
                    expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
                );
            }
        }))
    });
    let error = StreamEngine::open_recovered_storage(storage, None, None, None, None)
        .expect_err("first real publication settles late");
    crate::worker::RECOVERED_OPEN_STEP.with(|hook| *hook.borrow_mut() = None);
    assert_eq!(&*observations.borrow(), &["pack-complete"]);
    assert_eq!(error.absolute_expiry(), expiry);
    assert!(DirectoryOwnership::acquire(&temp.0).is_err());
    let manifest = LocalObjectTier::open(&tier, ShardId::new(0)).expect("actual durable CURRENT");
    assert_eq!(manifest.manifest().generation, 1);
    assert_eq!(manifest.manifest().objects[0].pack_sequence, first.sequence);
    assert!(
        !tier
            .join(format!("objects/shard-0/pack-{:020}.sse", second.sequence))
            .exists()
    );
    drop(manifest);
    drop(error);
    let mut cfg = config(&temp.0);
    cfg.object_store_dir = Some(tier);
    let ordinary = StreamEngine::open(cfg)
        .expect("ordinary native reopen finishes remaining publication after real cleanup");
    assert_eq!(
        ordinary
            .fetch(fetch())
            .expect("native complete payload")
            .len(),
        3
    );
}

#[derive(Debug)]
struct OpeningOnlySink;
impl crate::DurableAppendSink for OpeningOnlySink {
    fn apply(
        &self,
        _: crate::DurableSinkCheckpoint,
        _: &[crate::DurableAppend],
        _: crate::DurableSinkCheckpoint,
    ) -> EngineResult<crate::DurableSinkApply> {
        Err(crate::EngineError::InvalidConfig(
            "opening-only fixture never acknowledges delivery".into(),
        ))
    }
}
#[derive(Debug)]
struct LateOpeningFactory {
    expiry: Option<Instant>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}
impl crate::DurableAppendSinkFactory for LateOpeningFactory {
    fn validate_append(&self, _: &[u8], _: std::num::NonZeroU32) -> EngineResult<()> {
        Ok(())
    }
    fn load_checkpoint(
        &self,
        _: TopicPartition,
    ) -> EngineResult<Option<crate::DurableSinkCheckpoint>> {
        Ok(None)
    }
    fn open_shard(&self, _: ShardId) -> EngineResult<Arc<dyn crate::DurableAppendSink>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(end) = self.expiry {
            thread::sleep(end.saturating_duration_since(Instant::now()) + Duration::from_millis(5));
        }
        Ok(Arc::new(OpeningOnlySink))
    }
}
#[test]
fn owned_sink_late_first_real_factory_call_starts_no_later_callback_or_dispatcher() {
    let temp = Temporary::new();
    let owner = DirectoryOwnership::acquire(&temp.0).expect("actual OS custody");
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let expiry = Instant::now() + Duration::from_millis(50);
    crate::durable_sink::RECOVERY_SINK_STARTS.with(|starts| starts.set(0));
    let cfg = crate::DurableSinkConfig::new(Arc::new(LateOpeningFactory {
        expiry: Some(expiry),
        calls: calls.clone(),
    }));
    assert!(
        crate::durable_sink::DurableSinkDispatcher::open_with_recovery_ownership(
            cfg,
            &[ShardId::new(0), ShardId::new(1)],
            2 * 1024 * 1024,
            1024 * 1024,
            owner.clone(),
            expiry
        )
        .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    crate::durable_sink::RECOVERY_SINK_STARTS.with(|starts| assert_eq!(starts.get(), 0));
    assert!(DirectoryOwnership::acquire(&temp.0).is_err());
    let cfg = crate::DurableSinkConfig::new(Arc::new(LateOpeningFactory {
        expiry: None,
        calls: calls.clone(),
    }));
    let mut dispatcher = crate::durable_sink::DurableSinkDispatcher::open_with_recovery_ownership(
        cfg,
        &[ShardId::new(0), ShardId::new(1)],
        2 * 1024 * 1024,
        1024 * 1024,
        owner.clone(),
        Instant::now() + Duration::from_secs(2),
    )
    .expect("actual fitting factory and real dispatcher startup");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    crate::durable_sink::RECOVERY_SINK_STARTS.with(|starts| assert!(starts.get() > 0));
    dispatcher.join_owned_recovery();
    drop(dispatcher);
    drop(owner);
    DirectoryOwnership::acquire(&temp.0)
        .expect("later actual OS ownership progresses after real joins");
}

#[test]
fn actual_recovered_worker_checks_original_gc_expiry_after_real_queue_residence() {
    let temp = Temporary::new();
    write_engine_native(&temp.0);
    let mut store = ShardStore::open(store_config(&temp.0)).expect("actual native store");
    let pack = store
        .seal_active()
        .expect("seal")
        .expect("real retained pack");
    let original = fs::read(&pack.path).expect("native original bytes");
    let owner = DirectoryOwnership::acquire(&temp.0).expect("actual OS custody");
    let (handle, _) = crate::worker::ShardHandle::spawn_recovered(
        &config(&temp.0),
        ShardId::new(0),
        crate::worker::PrimaryStore::Ordinary(store),
        None,
        owner.clone(),
        Instant::now() + Duration::from_secs(30),
    )
    .expect("actual native recovered workers");
    let barrier = Arc::new((Mutex::new((false, false)), Condvar::new()));
    *crate::worker::RECOVERY_GC_QUEUE_BARRIER
        .lock()
        .expect("test registration") = Some((temp.0.join("shards/shard-0"), barrier.clone()));
    struct Release(Arc<(Mutex<(bool, bool)>, Condvar)>);
    impl Drop for Release {
        fn drop(&mut self) {
            let (lock, changed) = &*self.0;
            lock.lock().expect("release actual worker").1 = true;
            changed.notify_all();
            *crate::worker::RECOVERY_GC_QUEUE_BARRIER
                .lock()
                .expect("clear hook") = None;
        }
    }
    let release = Release(barrier.clone());
    let expiry = Instant::now() + Duration::from_secs(1);
    thread::scope(|scope| {
        let handle_ref = &handle;
        let work = scope.spawn(move || {
            handle_ref.garbage_collect_until(
                TopicPartition::new(TopicId::new(11), LogicalPartitionId::new(0)),
                LogicalOffset::new(2),
                expiry,
            )
        });
        let (lock, changed) = &*barrier;
        let state = lock.lock().expect("actual worker barrier");
        let (state, timeout) = changed
            .wait_timeout_while(state, Duration::from_secs(2), |value| !value.0)
            .expect("actual dequeue");
        assert!(
            state.0 && !timeout.timed_out(),
            "real worker must actually dequeue this command"
        );
        drop(state);
        assert!(DirectoryOwnership::acquire(&temp.0).is_err());
        thread::sleep(expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(5));
        drop(release);
        assert!(
            work.join()
                .expect("real worker command completion")
                .is_err()
        );
    });
    assert_eq!(
        fs::read(&pack.path).expect("expired queued GC did not begin native mutation"),
        original
    );
    assert!(!pack.path.with_extension("sse.gc-pending").exists());
    handle
        .garbage_collect(
            TopicPartition::new(TopicId::new(11), LogicalPartitionId::new(0)),
            LogicalOffset::new(2),
        )
        .expect("ordinary native same-worker GC remains available");
    assert!(!pack.path.exists());
    drop(owner);
    assert!(DirectoryOwnership::acquire(&temp.0).is_err());
    drop(handle);
    DirectoryOwnership::acquire(&temp.0)
        .expect("actual worker joins release last original OS custody");
}
