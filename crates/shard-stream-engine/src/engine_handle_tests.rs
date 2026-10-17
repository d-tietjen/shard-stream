use super::*;
use crate::{EngineConfig, TopicConfig};
use shard_stream_core::{LogicalOffset, LogicalPartitionId, TopicId};
use shard_stream_protocol::{AppendRequest, Durability, FetchMode, FetchRequest};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("actual clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "engine-custody-{}-{now}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual unique native fixture directory");
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
        data_dir: root.join("data"),
        object_store_dir: Some(root.join("objects")),
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
fn append(id: u128, body: &'static [u8]) -> AppendRequest {
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
fn topic(engine: &StreamEngine) {
    engine
        .create_topic(TopicConfig {
            topic_id: TopicId::new(11),
            partitions: 1,
            shards: None,
        })
        .expect("real native CreatePartition");
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

#[test]
fn ordinary_arc_identity_survives_independent_handles_and_dead_weak_clones() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<EngineHandle>();
    send_sync::<WeakEngineHandle>();
    let first = Temporary::new();
    let second = Temporary::new();
    let engine = Arc::new(StreamEngine::open(config(&first.0)).expect("real ordinary engine"));
    topic(&engine);
    engine
        .append(append(1, b"ordinary exact payload"))
        .expect("real RF1 object ACK");
    let original_weak = Arc::downgrade(&engine);
    let left = EngineHandle::from(Arc::clone(&engine));
    let right = EngineHandle::from(Arc::clone(&engine));
    assert!(left.ptr_eq(&right));
    assert!(std::ptr::eq(left.engine(), engine.as_ref()));
    let weak = left.downgrade();
    let weak_clone = right.downgrade().clone();
    assert!(weak.ptr_eq(&weak_clone));
    let other = EngineHandle::from(Arc::new(
        StreamEngine::open(config(&second.0)).expect("different actual ordinary engine"),
    ));
    assert!(!left.ptr_eq(&other));
    assert!(!weak.ptr_eq(&other.downgrade()));
    drop(engine);
    drop(left);
    drop(right);
    assert!(weak.upgrade().is_none());
    assert!(original_weak.upgrade().is_none());
    assert!(
        weak.ptr_eq(&weak_clone),
        "dead weak identity is still the original allocation"
    );
    let restarted = StreamEngine::open(config(&first.0)).expect("normal RF1 restart");
    assert_eq!(
        restarted.fetch(fetch()).expect("native ordinary body")[0]
            .payload
            .as_ref(),
        b"ordinary exact payload"
    );
}

#[cfg(unix)]
mod recovered {
    use super::*;
    use crate::durable_apply::{QueueSpawnObserver, QueueSpawnPhase};
    use crate::{
        DurableAppend, DurableAppendSink, DurableAppendSinkFactory, DurableApplyConfig,
        DurableApplyQueue, DurableSinkApply, DurableSinkCheckpoint, DurableSinkConfig,
        DurableSinkOptions, DurableSinkRecoveryMode, EngineError, EngineResult,
        NativeRecoveryCensus, NativeRecoveryLimits, ReplicationTransport,
    };
    use shard_stream_core::{DirectoryOwnership, DirectoryOwnershipError, ShardId, TopicPartition};
    use shard_stream_protocol::ReplicaAppend;
    use std::num::NonZeroU32;
    use std::sync::{Condvar, Mutex};
    use std::time::Instant;

    fn limits() -> NativeRecoveryLimits {
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
    // Every owner comes from actual producer -> object offload -> primary loss
    // -> pre-open census -> native restore -> real worker construction.
    fn owned(root: &Path, sink: Option<DurableSinkConfig>) -> Arc<crate::OwnedRecoveredEngine> {
        let cfg = config(root);
        {
            let producer = StreamEngine::open(cfg.clone()).expect("actual native RF1 producer");
            topic(&producer);
            producer
                .append(append(1, b"original native object body"))
                .expect("real native object durability");
        }
        let tier = shardlog::LocalObjectTier::open(root.join("objects"), ShardId::new(0))
            .expect("actual native CURRENT");
        let pack = tier.manifest().objects[0].pack_sequence;
        drop(tier);
        fs::remove_file(
            root.join("data/shards/shard-0")
                .join(format!("pack-{pack:020}.sse")),
        )
        .expect("actual missing primary");
        let owner =
            DirectoryOwnership::acquire(&cfg.data_dir).expect("actual original primary owner");
        let objects = DirectoryOwnership::acquire(root.join("objects"))
            .expect("actual original object owner");
        let census = NativeRecoveryCensus::read_for_engine(
            cfg,
            owner,
            Some(objects.root_handle().expect("actual anchored tier")),
            limits(),
            Instant::now() + Duration::from_secs(30),
        )
        .expect("real complete native source census");
        let storage = census
            .restore_storage()
            .expect("actual native object-only rehydration");
        Arc::new(
            StreamEngine::open_recovered_storage(storage, None, None, None, sink)
                .expect("actual consumed native worker setup"),
        )
    }
    fn contended(root: &Path) {
        for path in [root.join("data"), root.join("objects")] {
            assert!(matches!(
                DirectoryOwnership::acquire(path),
                Err(DirectoryOwnershipError::Contended)
            ));
        }
    }
    fn free(root: &Path) {
        for path in [root.join("data"), root.join("objects")] {
            drop(
                DirectoryOwnership::acquire(path)
                    .expect("real native owners released after complete cleanup"),
            );
        }
    }

    #[test]
    fn recovered_weak_upgrade_retains_exact_wrapper_bodies_and_both_native_owners() {
        let root = Temporary::new();
        let other_root = Temporary::new();
        let native = owned(&root.0, None);
        let native_weak = Arc::downgrade(&native);
        let owner_weak = Arc::downgrade(native.directory_ownership());
        let left = EngineHandle::from_recovered(Arc::clone(&native));
        let right = EngineHandle::from_recovered(Arc::clone(&native));
        assert!(left.ptr_eq(&right));
        assert!(std::ptr::eq(left.engine(), native.engine()));
        let other = EngineHandle::from(Arc::new(
            StreamEngine::open(config(&other_root.0)).expect("actual distinct ordinary engine"),
        ));
        assert!(!left.ptr_eq(&other));
        assert!(!left.downgrade().ptr_eq(&other.downgrade()));
        let weak = left.downgrade();
        assert!(weak.ptr_eq(&right.downgrade()));
        let callback = weak
            .upgrade()
            .expect("actual callback retains recovered wrapper");
        drop(native);
        drop(left);
        drop(right);
        assert!(native_weak.upgrade().is_some());
        contended(&root.0);
        assert_eq!(
            callback
                .fetch(fetch())
                .expect("native normal fetch through borrowed owner")[0]
                .payload
                .as_ref(),
            b"original native object body"
        );
        drop(callback);
        assert!(weak.upgrade().is_none());
        assert!(native_weak.upgrade().is_none());
        assert!(owner_weak.upgrade().is_none());
        free(&root.0);
        let ordinary = StreamEngine::open(config(&root.0))
            .expect("real normal restart after complete wrapper cleanup");
        assert_eq!(
            ordinary.fetch(fetch()).expect("native restart payload")[0]
                .payload
                .as_ref(),
            b"original native object body"
        );
    }

    #[derive(Debug, Default)]
    struct Recording(
        Mutex<Vec<ReplicaAppend>>,
        Mutex<Option<crate::ReplicationProgress>>,
    );
    impl ReplicationTransport for Recording {
        fn install_progress(&self, progress: crate::ReplicationProgress) -> EngineResult<()> {
            let mut installed = self.1.lock().expect("actual installed native progress");
            assert!(installed.is_none(), "native progress installs exactly once");
            *installed = Some(progress);
            Ok(())
        }
        fn replicate(&self, batch: ReplicaAppend, required_followers: u32) -> EngineResult<u32> {
            assert_eq!(
                required_followers, 0,
                "RF1 fixture does not claim remote ACKs"
            );
            self.0
                .lock()
                .expect("actual transport native batch")
                .push(batch);
            Ok(0)
        }
    }
    #[test]
    fn actual_replica_workers_retain_recovered_owner_after_caller_drop_and_write_native_batch() {
        let root = Temporary::new();
        let producer_root = Temporary::new();
        let recording = Arc::new(Recording::default());
        let source = StreamEngine::open_with_components(
            config(&producer_root.0),
            None,
            Some(recording.clone()),
        )
        .expect("actual RF1 native source transport");
        assert!(
            recording
                .1
                .lock()
                .expect("actual progress installation")
                .is_some()
        );
        topic(&source);
        source
            .append(append(1, b"original native object body"))
            .expect("actual matching first source batch");
        source
            .append(append(2, b"real next replicated body"))
            .expect("actual next native replica batch");
        let incoming = recording.0.lock().expect("actual callback bytes")[1].clone();
        assert_eq!(incoming.reservation.batch_id.get(), 1);
        let native = owned(&root.0, None);
        let owner_weak = Arc::downgrade(native.directory_ownership());
        let handle = EngineHandle::from_recovered(native);
        let weak = handle.downgrade();
        let queue = DurableApplyQueue::spawn_with_engine_handle(
            handle,
            DurableApplyConfig {
                worker_count: 1,
                queue_slots_per_worker: 4,
                max_pending_per_partition: 4,
                max_gap_batches: 4,
                max_pending_bytes_per_partition: 4096,
            },
        )
        .expect("actual recovered owner queue");
        contended(&root.0);
        let response = queue
            .append(incoming)
            .expect("native admission and physical replica append");
        assert_eq!(response.batch_id.get(), 1);
        let callback = weak.upgrade().expect("actual worker holds native wrapper");
        assert_eq!(
            callback
                .fetch(fetch())
                .expect("native fetched queue result")[1]
                .payload
                .as_ref(),
            b"real next replicated body"
        );
        drop(callback);
        drop(queue);
        assert!(weak.upgrade().is_none());
        assert!(owner_weak.upgrade().is_none());
        free(&root.0);
        let ordinary =
            StreamEngine::open(config(&root.0)).expect("native restart after queue worker joins");
        assert_eq!(
            ordinary.fetch(fetch()).expect("actual durable second body")[1]
                .payload
                .as_ref(),
            b"real next replicated body"
        );
    }

    #[derive(Debug, Default)]
    struct State {
        entered: bool,
        release: bool,
        checkpoint: Option<DurableSinkCheckpoint>,
        applied: usize,
    }
    type Gate = Arc<(Mutex<State>, Condvar)>;
    #[derive(Debug)]
    struct BlockingSink {
        gate: Gate,
    }
    impl DurableAppendSink for BlockingSink {
        fn apply(
            &self,
            expected: DurableSinkCheckpoint,
            appends: &[DurableAppend],
            next: DurableSinkCheckpoint,
        ) -> EngineResult<DurableSinkApply> {
            let (lock, changed) = &*self.gate;
            let mut state = lock.lock().expect("real sink callback state");
            assert_eq!(
                state
                    .checkpoint
                    .unwrap_or(DurableSinkCheckpoint::initial(expected.topic_partition)),
                expected
            );
            assert_eq!(appends.len(), 1);
            assert_eq!(appends[0].payload.as_ref(), b"original native object body");
            state.entered = true;
            changed.notify_all();
            while !state.release {
                state = changed.wait(state).expect("actual physical callback gate");
            }
            state.applied += appends.len();
            state.checkpoint = Some(next);
            Ok(DurableSinkApply::Applied)
        }
    }
    #[derive(Debug)]
    struct Factory {
        gate: Gate,
    }
    impl DurableAppendSinkFactory for Factory {
        fn validate_append(&self, _: &[u8], _: NonZeroU32) -> EngineResult<()> {
            Ok(())
        }
        fn load_checkpoint(
            &self,
            _: TopicPartition,
        ) -> EngineResult<Option<DurableSinkCheckpoint>> {
            Ok(self
                .gate
                .0
                .lock()
                .expect("actual shared checkpoint")
                .checkpoint)
        }
        fn open_shard(&self, _: ShardId) -> EngineResult<Arc<dyn DurableAppendSink>> {
            Ok(Arc::new(BlockingSink {
                gate: Arc::clone(&self.gate),
            }))
        }
    }
    struct Release(Gate);
    impl Drop for Release {
        fn drop(&mut self) {
            let (lock, changed) = &*self.0;
            let mut state = lock
                .lock()
                .expect("release real callback on every fixture exit");
            state.release = true;
            changed.notify_all();
        }
    }
    #[test]
    fn last_recovered_handle_and_unwind_join_actual_blocked_sink_before_releasing_tier_custody() {
        for unwind in [false, true] {
            let root = Temporary::new();
            let gate = Arc::new((Mutex::new(State::default()), Condvar::new()));
            let native = owned(
                &root.0,
                Some(
                    DurableSinkConfig::new(Arc::new(Factory {
                        gate: Arc::clone(&gate),
                    }))
                    .with_options(DurableSinkOptions {
                        recovery_mode: DurableSinkRecoveryMode::Background,
                        worker_count: 1,
                        max_pending_items: 4,
                        max_pending_bytes: Some(2 << 20),
                        ..DurableSinkOptions::default()
                    }),
                ),
            );
            let owner_weak = Arc::downgrade(native.directory_ownership());
            let handle = EngineHandle::from_recovered(native);
            let weak = handle.downgrade();
            handle
                .resume_durable_sink(&handle.recovered_partitions())
                .expect("genuine native replay queues original body");
            std::thread::scope(|scope| {
                let _release = Release(Arc::clone(&gate));
                let (lock, changed) = &*gate;
                let state = lock.lock().expect("observe actual callback");
                let (state, _) = changed
                    .wait_timeout_while(state, Duration::from_secs(10), |s| !s.entered)
                    .expect("bounded wait for actual callback entry");
                assert!(state.entered);
                drop(state);
                let (started, seen) = std::sync::mpsc::sync_channel(1);
                let job = scope.spawn(move || {
                    started.send(()).expect("real destructor job started");
                    if unwind {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            let _owner = handle;
                            panic!("genuine owning-frame unwind");
                        }));
                        assert!(result.is_err());
                    } else {
                        drop(handle);
                    }
                });
                seen.recv_timeout(Duration::from_secs(10))
                    .expect("actual destructor start");
                contended(&root.0);
                assert!(
                    !job.is_finished(),
                    "physical sink callback has not completed"
                );
                let mut state = lock.lock().expect("release actual blocked physical work");
                state.release = true;
                changed.notify_all();
                drop(state);
                job.join()
                    .expect("actual owner destruction finishes only after callback");
            });
            assert_eq!(
                gate.0.lock().expect("actual applied native body").applied,
                1
            );
            assert!(weak.upgrade().is_none());
            assert!(owner_weak.upgrade().is_none());
            free(&root.0);
            let ordinary =
                StreamEngine::open(config(&root.0)).expect("native progress after real cleanup");
            assert_eq!(
                ordinary
                    .fetch(fetch())
                    .expect("real original body survives")[0]
                    .payload
                    .as_ref(),
                b"original native object body"
            );
        }
    }

    #[test]
    fn invalid_queue_config_releases_consumed_native_owner_without_starting_worker_state() {
        let root = Temporary::new();
        let native = owned(&root.0, None);
        let owner_weak = Arc::downgrade(native.directory_ownership());
        let handle = EngineHandle::from_recovered(native);
        let weak = handle.downgrade();
        let result = DurableApplyQueue::spawn_with_engine_handle(
            handle,
            DurableApplyConfig {
                worker_count: 0,
                ..DurableApplyConfig::default()
            },
        );
        assert!(matches!(result, Err(EngineError::InvalidConfig(_))));
        assert!(weak.upgrade().is_none());
        assert!(owner_weak.upgrade().is_none());
        free(&root.0);
        StreamEngine::open(config(&root.0))
            .expect("real native later progress after refused queue");
    }

    #[test]
    fn original_expiry_refuses_before_any_replica_worker_and_releases_actual_recovered_owner() {
        let root = Temporary::new();
        let native = owned(&root.0, None);
        let owner_weak = Arc::downgrade(native.directory_ownership());
        let handle = EngineHandle::from_recovered(native);
        let weak = handle.downgrade();
        contended(&root.0);
        let observed = Arc::new(Mutex::new(Vec::new()));
        let record = Arc::clone(&observed);
        let _observer = QueueSpawnObserver::install(move |phase, index| {
            record
                .lock()
                .expect("actual worker-start observations")
                .push((phase, index));
        });
        let original_expiry = Instant::now();
        let result = DurableApplyQueue::spawn_with_engine_handle_until(
            handle,
            DurableApplyConfig::default(),
            original_expiry,
        );
        assert!(
            matches!(result, Err(EngineError::InvalidConfig(ref message))
            if message == "durable apply original startup expiry elapsed")
        );
        assert!(
            observed
                .lock()
                .expect("no physical queue worker started")
                .is_empty()
        );
        assert!(weak.upgrade().is_none());
        assert!(owner_weak.upgrade().is_none());
        free(&root.0);
        let ordinary = StreamEngine::open(config(&root.0))
            .expect("real native restart after expired constructor cleanup");
        assert_eq!(
            ordinary
                .fetch(fetch())
                .expect("actual recovered payload remains durable")[0]
                .payload
                .as_ref(),
            b"original native object body"
        );
    }

    #[test]
    fn original_expiry_after_actual_spawn_joins_workers_without_starting_the_next_worker() {
        for pause in [
            QueueSpawnPhase::AppendStarted,
            QueueSpawnPhase::AdmissionStarted,
        ] {
            let root = Temporary::new();
            let native = owned(&root.0, None);
            let owner_weak = Arc::downgrade(native.directory_ownership());
            let handle = EngineHandle::from_recovered(native);
            let weak = handle.downgrade();
            let gate = Arc::new((Mutex::new(State::default()), Condvar::new()));
            let observed = Arc::new(Mutex::new(Vec::new()));
            std::thread::scope(|scope| {
                // This fixture guard releases the actual in-constructor observer
                // before scope cleanup joins its thread, including assertion unwind.
                let _release = Release(Arc::clone(&gate));
                let callback_gate = Arc::clone(&gate);
                let record = Arc::clone(&observed);
                let (entered, seen) = std::sync::mpsc::sync_channel(1);
                let job = scope.spawn(move || {
                    let original_expiry = Instant::now() + Duration::from_secs(1);
                    let _observer = QueueSpawnObserver::install(move |phase, index| {
                        record
                            .lock()
                            .expect("actual ordered worker starts")
                            .push((phase, index));
                        if phase == pause && index == 0 {
                            entered
                                .send(original_expiry)
                                .expect("actual completed spawn reports its original expiry");
                            let (lock, changed) = &*callback_gate;
                            let mut state = lock.lock().expect("actual post-spawn scheduling gate");
                            while !state.release {
                                state = changed
                                    .wait(state)
                                    .expect("retain actual stored worker and owner");
                            }
                        }
                    });
                    DurableApplyQueue::spawn_with_engine_handle_until(
                        handle,
                        DurableApplyConfig {
                            worker_count: 2,
                            queue_slots_per_worker: 4,
                            max_pending_per_partition: 4,
                            max_gap_batches: 4,
                            max_pending_bytes_per_partition: 4096,
                        },
                        original_expiry,
                    )
                });
                let original_expiry = seen
                    .recv_timeout(Duration::from_secs(10))
                    .expect("actual worker spawned before the gate");
                contended(&root.0);
                assert!(weak.upgrade().is_some());
                assert!(owner_weak.upgrade().is_some());
                assert!(
                    !job.is_finished(),
                    "the actual constructor retains its complete owner"
                );
                std::thread::sleep(
                    original_expiry.saturating_duration_since(Instant::now())
                        + Duration::from_millis(1),
                );
                let (lock, changed) = &*gate;
                let mut state = lock
                    .lock()
                    .expect("release after the SAME original Instant elapsed");
                state.release = true;
                changed.notify_all();
                drop(state);
                let result = job
                    .join()
                    .expect("actual constructor and physical worker joins complete");
                assert!(
                    matches!(result, Err(EngineError::InvalidConfig(ref message))
                    if message == "durable apply original startup expiry elapsed")
                );
            });
            let expected = match pause {
                QueueSpawnPhase::AppendStarted => vec![(QueueSpawnPhase::AppendStarted, 0)],
                QueueSpawnPhase::AdmissionStarted => vec![
                    (QueueSpawnPhase::AppendStarted, 0),
                    (QueueSpawnPhase::AdmissionStarted, 0),
                ],
            };
            assert_eq!(*observed.lock().expect("no later worker start"), expected);
            assert!(weak.upgrade().is_none());
            assert!(owner_weak.upgrade().is_none());
            free(&root.0);
            let ordinary = StreamEngine::open(config(&root.0))
                .expect("native progress after late constructor settlement");
            assert_eq!(
                ordinary
                    .fetch(fetch())
                    .expect("real native object body after cleanup")[0]
                    .payload
                    .as_ref(),
                b"original native object body"
            );
        }
    }

    #[test]
    fn fitting_original_expiry_constructs_real_queue_and_keeps_recovered_custody_until_join() {
        let root = Temporary::new();
        let native = owned(&root.0, None);
        let owner_weak = Arc::downgrade(native.directory_ownership());
        let handle = EngineHandle::from_recovered(native);
        let weak = handle.downgrade();
        let original_expiry = Instant::now() + Duration::from_secs(30);
        let queue = DurableApplyQueue::spawn_with_engine_handle_until(
            handle,
            DurableApplyConfig {
                worker_count: 1,
                queue_slots_per_worker: 4,
                max_pending_per_partition: 4,
                max_gap_batches: 4,
                max_pending_bytes_per_partition: 4096,
            },
            original_expiry,
        )
        .expect("actual fitting original-expiry queue");
        contended(&root.0);
        let callback = weak
            .upgrade()
            .expect("real queue workers keep the whole recovered engine alive");
        assert_eq!(
            callback
                .fetch(fetch())
                .expect("actual recovered native body")[0]
                .payload
                .as_ref(),
            b"original native object body"
        );
        drop(callback);
        drop(queue);
        assert!(weak.upgrade().is_none());
        assert!(owner_weak.upgrade().is_none());
        free(&root.0);
        let ordinary = StreamEngine::open(config(&root.0))
            .expect("real native restart after fitting queue joins");
        assert_eq!(
            ordinary
                .fetch(fetch())
                .expect("actual durable body after positive queue cleanup")[0]
                .payload
                .as_ref(),
            b"original native object body"
        );
    }
}
