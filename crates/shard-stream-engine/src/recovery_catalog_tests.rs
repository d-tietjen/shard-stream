use super::*;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use shard_stream_core::{LogicalPartitionId, TopicId};
use shardlog::CoordinatorJournal;

use crate::{StreamEngine, TopicConfig};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "native-config-catalog-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).expect("actual temporary directory");
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
fn tp() -> TopicPartition {
    TopicPartition::new(TopicId::new(u128::MAX - 11), LogicalPartitionId::new(0))
}
fn read(root: &Path, caps: NativeRecoveryLimits, expiry: Instant) -> NativeRecoveryCensus {
    NativeRecoveryCensus::read(
        config(root),
        DirectoryOwnership::acquire(root).expect("actual OS owner"),
        None,
        caps,
        expiry,
    )
    .expect("actual retained native census")
}
fn write_events(root: &Path, events: &[JournalEvent]) {
    let (mut journal, _) =
        CoordinatorJournal::open(root.join("coordinator.ssj")).expect("native writer");
    for event in events {
        journal.append(event, true).expect("actual SSJ3 append");
    }
}

#[test]
fn catalog_uses_real_engine_policy_ring_producers_and_retains_original_owner() {
    let temp = Temporary::new();
    let cfg = config(&temp.0);
    let engine = StreamEngine::open(cfg.clone()).expect("ordinary native RF1 engine");
    let policy = PartitionReplicationPolicy {
        policy_generation: 7,
        replication_factor: 1,
        min_in_sync_replicas: 1,
    };
    engine
        .create_topic_with_policy(
            TopicConfig {
                topic_id: tp().topic_id,
                partitions: 1,
                shards: Some(vec![ShardId::new(0), ShardId::new(3)]),
            },
            policy,
        )
        .expect("actual native partition producer");
    engine
        .reconfigure_partition(tp(), RingEpoch::new(2), vec![ShardId::new(2)])
        .expect("actual native ring producer");
    let effective = PartitionReplicationPolicy {
        policy_generation: 9,
        ..policy
    };
    engine
        .update_topic_replication_policy(tp().topic_id, 7, effective)
        .expect("actual native policy producer");
    assert_eq!(
        engine
            .partition_replication_policy(tp())
            .expect("live policy"),
        effective
    );
    drop(engine);
    let original = fs::read(temp.0.join("coordinator.ssj")).expect("original journal");
    let expiry = Instant::now() + Duration::from_secs(30);
    let mut census = read(&temp.0, limits(), expiry);
    let catalog = census.catalog().expect("bounded same-native catalog");
    assert!(Arc::ptr_eq(
        catalog.directory_ownership(),
        census.directory_ownership()
    ));
    assert_eq!(catalog.absolute_expiry(), expiry);
    let partition = catalog.partition(tp()).expect("actual configured TP");
    assert_eq!(partition.replication_policy(), effective);
    assert_eq!(partition.virtual_lane_count(), cfg.virtual_lane_count);
    assert_eq!(partition.ring_history().len(), 2);
    assert_eq!(
        partition.ring_history()[&RingEpoch::new(1)],
        vec![ShardId::new(0), ShardId::new(3)]
    );
    assert_eq!(
        cfg.physical_shard_for_lane(partition.ring_history()[&RingEpoch::new(1)][1]),
        ShardId::new(0),
        "native placement lane 3 maps to physical shard 0 in this one-shard engine"
    );
    assert_eq!(
        partition.ring_history()[&RingEpoch::new(2)],
        vec![ShardId::new(2)]
    );
    assert_eq!(catalog.partitions().collect::<Vec<_>>(), vec![tp()]);
    assert!(
        catalog
            .partition(TopicPartition::new(
                TopicId::new(0),
                LogicalPartitionId::new(0)
            ))
            .is_none()
    );
    assert_eq!(
        fs::read(temp.0.join("coordinator.ssj")).expect("still original"),
        original
    );
    drop(census);
    assert!(
        DirectoryOwnership::acquire(&temp.0).is_err(),
        "catalog still owns the real root allocation"
    );
    drop(catalog);
    let owner = DirectoryOwnership::acquire(&temp.0).expect("catalog custody released");
    drop(owner);
    let reopened = StreamEngine::open(cfg).expect("ordinary full native restart");
    assert_eq!(
        reopened
            .partition_replication_policy(tp())
            .expect("restart policy"),
        effective
    );
}

#[test]
fn catalog_preserves_legacy_configure_defaults_and_native_recovery_idempotence() {
    let temp = Temporary::new();
    let effective = PartitionReplicationPolicy {
        policy_generation: 6,
        replication_factor: 1,
        min_in_sync_replicas: 1,
    };
    write_events(
        &temp.0,
        &[
            JournalEvent::Configure {
                topic_partition: tp(),
                ring_epoch: RingEpoch::new(1),
                shards: vec![ShardId::new(3)],
            },
            // Genuine SSJ3 production and parsing require the proposed policy
            // to advance its encoded expected generation.
            JournalEvent::UpdateReplicationPolicy {
                topic_partition: tp(),
                expected_policy_generation: 1,
                policy_generation: 5,
                replication_factor: 1,
                min_in_sync_replicas: 1,
            },
            JournalEvent::UpdateReplicationPolicy {
                topic_partition: tp(),
                expected_policy_generation: 5,
                policy_generation: 6,
                replication_factor: 1,
                min_in_sync_replicas: 1,
            },
            // An exact repeated policy is idempotent even with another expected value.
            JournalEvent::UpdateReplicationPolicy {
                topic_partition: tp(),
                expected_policy_generation: 5,
                policy_generation: 6,
                replication_factor: 1,
                min_in_sync_replicas: 1,
            },
        ],
    );
    let original = fs::read(temp.0.join("coordinator.ssj")).expect("native bytes");
    let mut census = read(&temp.0, limits(), Instant::now() + Duration::from_secs(30));
    let catalog = census.catalog().expect("native recovery semantics");
    assert_eq!(
        catalog
            .partition(tp())
            .expect("legacy TP")
            .replication_policy(),
        effective
    );
    drop(catalog);
    drop(census);
    let engine =
        StreamEngine::open(config(&temp.0)).expect("same genuine native full recovery accepts");
    assert_eq!(
        engine
            .partition_replication_policy(tp())
            .expect("full effective policy"),
        effective
    );
    drop(engine);
    assert_eq!(
        fs::read(temp.0.join("coordinator.ssj")).expect("native journal unchanged"),
        original
    );

    let legacy = Temporary::new();
    write_events(
        &legacy.0,
        &[JournalEvent::Configure {
            topic_partition: tp(),
            ring_epoch: RingEpoch::new(1),
            shards: vec![ShardId::new(0)],
        }],
    );
    let mut census = read(
        &legacy.0,
        limits(),
        Instant::now() + Duration::from_secs(30),
    );
    let catalog = census.catalog().expect("actual default path");
    assert_eq!(
        catalog
            .partition(tp())
            .expect("legacy TP")
            .replication_policy(),
        config(&legacy.0).default_replication_policy()
    );
}

#[test]
fn catalog_exact_original_caps_and_one_less_refuse_without_native_mutation() {
    let temp = Temporary::new();
    write_events(
        &temp.0,
        &[JournalEvent::CreatePartition {
            topic_partition: tp(),
            ring_epoch: RingEpoch::new(1),
            shards: vec![ShardId::new(0), ShardId::new(3)],
            policy_generation: 4,
            replication_factor: 1,
            min_in_sync_replicas: 1,
        }],
    );
    let original = fs::read(temp.0.join("coordinator.ssj")).expect("native bytes");
    let mut first = read(&temp.0, limits(), Instant::now() + Duration::from_secs(30));
    let catalog = first.catalog().expect("measured native reservation");
    let exact = catalog.usage();
    assert_eq!(
        exact.read_bytes,
        first.usage().read_bytes,
        "catalog performs no source I/O"
    );
    drop(catalog);
    drop(first);
    for dimension in 0..3 {
        let mut caps = limits();
        match dimension {
            0 => caps.max_entries = exact.entries,
            1 => caps.max_declared_bytes = exact.declared_bytes,
            _ => caps.max_work_bytes = exact.work_bytes,
        }
        let mut fitting = read(&temp.0, caps, Instant::now() + Duration::from_secs(30));
        let fitting_catalog = fitting.catalog().expect("exact original total allowance");
        assert!(
            fitting.catalog().is_err(),
            "repeated reduction cannot reset/refund its original allowance"
        );
        drop(fitting_catalog);
        drop(fitting);
        match dimension {
            0 => caps.max_entries -= 1,
            1 => caps.max_declared_bytes -= 1,
            _ => caps.max_work_bytes -= 1,
        }
        let mut short = read(&temp.0, caps, Instant::now() + Duration::from_secs(30));
        assert!(
            short.catalog().is_err(),
            "one less refuses before shared native map materialization"
        );
        assert_eq!(
            fs::read(temp.0.join("coordinator.ssj")).expect("unchanged bytes"),
            original
        );
    }
}

#[test]
fn catalog_uses_original_instant_and_keeps_custody_on_expiry() {
    let temp = Temporary::new();
    write_events(
        &temp.0,
        &[JournalEvent::Configure {
            topic_partition: tp(),
            ring_epoch: RingEpoch::new(1),
            shards: vec![ShardId::new(0)],
        }],
    );
    let expiry = Instant::now() + Duration::from_millis(500);
    let mut census = read(&temp.0, limits(), expiry);
    let original = fs::read(temp.0.join("coordinator.ssj")).expect("original bytes");
    std::thread::sleep(expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(2));
    let before = census.usage();
    assert!(census.catalog().is_err());
    assert_eq!(census.usage().declared_bytes, before.declared_bytes);
    assert_eq!(census.usage().entries, before.entries);
    assert_eq!(census.usage().work_bytes, before.work_bytes);
    assert!(DirectoryOwnership::acquire(&temp.0).is_err());
    assert_eq!(
        fs::read(temp.0.join("coordinator.ssj")).expect("no late native mutation"),
        original
    );
    drop(census);
    let owner = DirectoryOwnership::acquire(&temp.0).expect("actual cleanup permits later owner");
    drop(owner);
    let mut later = read(&temp.0, limits(), Instant::now() + Duration::from_secs(30));
    assert!(
        later.catalog().is_ok(),
        "new actual owner, not a renewed old attempt"
    );
}

#[test]
fn catalog_refuses_real_native_invalid_configuration_with_full_recovery_error() {
    let initial = JournalEvent::Configure {
        topic_partition: tp(),
        ring_epoch: RingEpoch::new(1),
        shards: vec![ShardId::new(0)],
    };
    let cases = vec![
        vec![JournalEvent::Configure {
            topic_partition: tp(),
            ring_epoch: RingEpoch::new(1),
            shards: vec![ShardId::new(4)],
        }],
        vec![initial.clone(), initial.clone()],
        vec![
            initial.clone(),
            JournalEvent::CreatePartition {
                topic_partition: tp(),
                ring_epoch: RingEpoch::new(2),
                shards: vec![ShardId::new(0)],
                policy_generation: 1,
                replication_factor: 1,
                min_in_sync_replicas: 1,
            },
        ],
        vec![JournalEvent::UpdateReplicationPolicy {
            topic_partition: tp(),
            expected_policy_generation: 1,
            policy_generation: 2,
            replication_factor: 1,
            min_in_sync_replicas: 1,
        }],
        vec![
            initial.clone(),
            JournalEvent::UpdateReplicationPolicy {
                topic_partition: tp(),
                expected_policy_generation: 2,
                policy_generation: 3,
                replication_factor: 1,
                min_in_sync_replicas: 1,
            },
        ],
        vec![
            initial,
            JournalEvent::UpdateReplicationPolicy {
                topic_partition: tp(),
                expected_policy_generation: 1,
                policy_generation: 2,
                replication_factor: 2,
                min_in_sync_replicas: 1,
            },
        ],
    ];
    for events in cases {
        let temp = Temporary::new();
        write_events(&temp.0, &events);
        let original = fs::read(temp.0.join("coordinator.ssj")).expect("actual bytes");
        let mut census = read(&temp.0, limits(), Instant::now() + Duration::from_secs(30));
        let error = match census.catalog() {
            Ok(_) => panic!("invalid catalog accepted"),
            Err(e) => e.to_string(),
        };
        drop(census);
        let full_error = match StreamEngine::open(config(&temp.0)) {
            Ok(_) => panic!("invalid full recovery accepted"),
            Err(e) => e.to_string(),
        };
        assert_eq!(
            error, full_error,
            "same native ordered validation and error"
        );
        assert_eq!(
            fs::read(temp.0.join("coordinator.ssj")).expect("no repair required"),
            original
        );
    }
}
