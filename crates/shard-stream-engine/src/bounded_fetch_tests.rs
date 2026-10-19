use super::*;
use crate::worker::observe_bounded_fetch;
#[cfg(unix)]
use crate::{NativeRecoveryCensus, NativeRecoveryLimits};
#[cfg(unix)]
use shard_stream_core::DirectoryOwnershipError;
use shard_stream_core::TopicId;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, SystemTime};

struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "native-engine-bounded-fetch-{}-{now}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("real unique native root");
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
        object_store_dir: None,
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
fn partition() -> TopicPartition {
    TopicPartition::new(TopicId::new(11), LogicalPartitionId::new(0))
}
fn request() -> FetchRequest {
    FetchRequest {
        request_id: 1,
        topic_id: TopicId::new(11),
        partition_id: LogicalPartitionId::new(0),
        start_offset: LogicalOffset::new(0),
        max_bytes: 4096,
        mode: FetchMode::Ordered,
    }
}
fn limits() -> NativeFetchLimits {
    NativeFetchLimits {
        max_plan_batches_per_shard: 128,
        max_selected_batches: 128,
        max_candidate_window: 1024,
        max_entries: 1_000_000,
        max_path_bytes: 8192,
        max_read_attempts: 4096,
        max_read_bytes: 1 << 24,
        max_declared_bytes: 1 << 26,
        max_work_bytes: 1 << 30,
    }
}
fn produce(config: EngineConfig, count: u128, body: &'static [u8]) -> StreamEngine {
    let engine = StreamEngine::open(config).expect("actual native engine");
    engine
        .create_topic(TopicConfig {
            topic_id: TopicId::new(11),
            partitions: 1,
            shards: None,
        })
        .expect("native journal topic creation");
    for id in 0..count {
        engine
            .append(AppendRequest {
                request_id: id,
                topic_id: TopicId::new(11),
                partition_id: LogicalPartitionId::new(0),
                record_count: 1,
                payload: Bytes::from_static(body),
                durability: Durability::Leader,
                producer: None,
                atomic_group: None,
                leader_epoch: None,
                extension_context: None,
            })
            .expect("real native committed extent");
    }
    engine
}
#[cfg(unix)]
fn native_limits() -> NativeRecoveryLimits {
    NativeRecoveryLimits {
        max_files: 128,
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
#[cfg(unix)]
fn recovered(root: &Path) -> EngineHandle {
    let cfg = config(root);
    drop(produce(cfg.clone(), 3, b"actual retained native body"));
    let directory = DirectoryOwnership::acquire(&cfg.data_dir).expect("same actual primary owner");
    let census = NativeRecoveryCensus::read_for_engine(
        cfg,
        directory,
        None,
        native_limits(),
        Instant::now() + Duration::from_secs(30),
    )
    .expect("actual pre-open native census");
    let storage = census
        .restore_storage()
        .expect("actual retained native stores");
    EngineHandle::from_recovered(Arc::new(
        StreamEngine::open_recovered_storage(storage, None, None, None, None)
            .expect("genuine recovered native workers"),
    ))
}
#[cfg(unix)]
fn contended(root: &Path) {
    assert!(matches!(
        DirectoryOwnership::acquire(&root.join("data")),
        Err(DirectoryOwnershipError::Contended)
    ));
}

#[test]
fn native_zero_payload_prefix_is_bounded_and_ordinary_fetch_keeps_all_history() {
    let root = Temporary::new();
    let handle = EngineHandle::from(Arc::new(produce(config(&root.0), 64, b"")));
    let ordinary = handle.fetch(request()).expect("ordinary unchanged fetch");
    assert_eq!(ordinary.len(), 64);
    for mode in [
        FetchMode::Ordered,
        FetchMode::Replicated,
        FetchMode::AsAvailable,
    ] {
        let mut req = request();
        req.mode = mode;
        let mut cap = limits();
        cap.max_selected_batches = 7;
        let bounded = handle
            .fetch_until(req, cap, Instant::now() + Duration::from_secs(10))
            .expect("real finite native pipeline");
        assert_eq!(bounded.batches(), &ordinary[..7]);
        assert!(
            bounded
                .batches()
                .iter()
                .all(|batch| batch.payload.is_empty())
        );
        let mut next = req;
        next.start_offset = LogicalOffset::new(
            bounded
                .batches()
                .last()
                .expect("actual prefix")
                .last_offset
                .get()
                .checked_add(1)
                .expect("next offset"),
        );
        assert_eq!(
            handle
                .fetch_until(next, cap, Instant::now() + Duration::from_secs(10))
                .expect("real next native prefix")
                .batches(),
            &ordinary[7..14]
        );
    }
    assert_eq!(
        handle
            .fetch(request())
            .expect("ordinary whole history after bounded reads"),
        ordinary
    );
}

#[test]
#[cfg(unix)]
fn real_pipeline_exact_entry_ceiling_fits_and_one_less_refuses_with_same_engine_retained() {
    let root = Temporary::new();
    let handle = recovered(&root.0);
    let fitting = handle
        .fetch_until(
            request(),
            limits(),
            Instant::now() + Duration::from_secs(10),
        )
        .expect("measure actual fees");
    let mut exact = limits();
    exact.max_entries = fitting.usage().entries;
    let same = handle
        .fetch_until(request(), exact, Instant::now() + Duration::from_secs(10))
        .expect("exact actual cumulative ceiling");
    assert_eq!(same.batches(), fitting.batches());
    assert_eq!(same.usage(), fitting.usage());
    exact.max_entries -= 1;
    let error = handle
        .fetch_until(request(), exact, Instant::now() + Duration::from_secs(10))
        .expect_err("one fewer actual entry");
    assert!(matches!(
        error.failure(),
        NativeFetchFailure::Budget(StorageFetchFailure::Capacity)
    ));
    assert!(error.engine_handle().ptr_eq(&handle));
    assert!(
        !error.completed_payloads().collect::<Vec<_>>().is_empty(),
        "actual read payloads remain when final conversion admission refuses"
    );
    drop(handle);
    contended(&root.0);
    drop(error);
    StreamEngine::open(config(&root.0)).expect("actual last native cleanup completed");
}

#[test]
fn actual_candidate_span_is_refused_before_bitmap_allocation_and_expired_entry_does_no_read() {
    let root = Temporary::new();
    let handle = EngineHandle::from(Arc::new(produce(config(&root.0), 64, b"")));
    let mut cap = limits();
    cap.max_candidate_window = 63;
    let error = handle
        .fetch_until(request(), cap, Instant::now() + Duration::from_secs(10))
        .expect_err("real native candidate span exceeds 63");
    assert!(matches!(
        error.failure(),
        NativeFetchFailure::Budget(StorageFetchFailure::Capacity)
    ));
    assert_eq!(
        error.usage().expect("actual paid metadata").read_attempts,
        0
    );
    let fitting = handle
        .fetch_until(
            request(),
            limits(),
            Instant::now() + Duration::from_secs(10),
        )
        .expect("native later progress");
    assert_eq!(fitting.batches().len(), 64);
    let error = handle
        .fetch_until(request(), limits(), Instant::now())
        .expect_err("already expired original E");
    assert!(matches!(
        error.failure(),
        NativeFetchFailure::Budget(StorageFetchFailure::Expired(_))
    ));
    assert_eq!(
        error.usage().expect("no native work entered"),
        StorageFetchUsage::default()
    );
}

#[test]
#[cfg(unix)]
fn actual_worker_queue_residence_past_e_waits_for_physical_command_and_retains_root() {
    let root = Temporary::new();
    let handle = recovered(&root.0);
    let weak = handle.downgrade();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let worker_gate = Arc::clone(&gate);
    let (entered, actual_entered) = sync_channel(1);
    let expiry = Instant::now() + Duration::from_secs(1);
    let worker = std::thread::spawn(move || {
        let _observer = observe_bounded_fetch(Arc::new(move |_| {
            entered.send(()).expect("actual worker entered");
            let (mutex, condition) = &*worker_gate;
            let mut released = mutex.lock().expect("actual gate");
            while !*released {
                released = condition.wait(released).expect("actual worker waits");
            }
        }));
        handle.fetch_until(request(), limits(), expiry)
    });
    actual_entered
        .recv()
        .expect("real queued worker reached callback");
    contended(&root.0);
    std::thread::sleep(expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(2));
    assert!(
        !worker.is_finished(),
        "logical E cannot report physical settlement while actual worker is blocked"
    );
    {
        let (mutex, condition) = &*gate;
        *mutex.lock().expect("release actual worker") = true;
        condition.notify_all();
    }
    let error = worker
        .join()
        .expect("actual caller joins")
        .expect_err("late real native queue command");
    assert!(matches!(error.failure(), NativeFetchFailure::Shard(_)));
    assert_eq!(
        error.usage().expect("no read after queued E").read_attempts,
        0
    );
    assert!(weak.upgrade().is_some());
    contended(&root.0);
    drop(error);
    assert!(weak.upgrade().is_none());
    StreamEngine::open(config(&root.0)).expect("actual native worker/root cleanup");
}

#[test]
#[cfg(unix)]
fn actual_native_worker_unwind_settles_refused_command_without_success_or_root_escape() {
    let root = Temporary::new();
    let handle = recovered(&root.0);
    let weak = handle.downgrade();
    let _observer = observe_bounded_fetch(Arc::new(|_| panic!("genuine entered worker unwind")));
    let error = handle
        .fetch_until(
            request(),
            limits(),
            Instant::now() + Duration::from_secs(10),
        )
        .expect_err("actual worker unwind is a failure");
    assert!(matches!(
        error.failure(),
        NativeFetchFailure::WorkerStopped(_)
    ));
    assert_eq!(
        error.usage().expect("no read before unwind").read_attempts,
        0
    );
    drop(handle);
    contended(&root.0);
    assert!(weak.upgrade().is_some());
    drop(error);
    assert!(weak.upgrade().is_none());
    StreamEngine::open(config(&root.0))
        .expect("actual stopped worker physically joined on last owner");
}

#[test]
fn shared_native_selector_matches_whole_original_oracle_on_real_multishard_history() {
    let root = Temporary::new();
    let mut cfg = config(&root.0);
    cfg.shard_count = 3;
    let handle = EngineHandle::from(Arc::new(produce(cfg, 32, b"real native payload")));
    let (sender, receiver) = channel();
    for (lane, shard) in handle.shards.iter().enumerate() {
        shard
            .begin_fetch_plan(
                lane,
                partition(),
                LogicalOffset::new(0),
                4096,
                128,
                sender.clone(),
            )
            .expect("actual ordinary shard planner");
    }
    let mut plans = (0..handle.shards.len())
        .map(|_| Vec::new())
        .collect::<Vec<_>>();
    for _ in 0..handle.shards.len() {
        let FetchCompletion::Plan { lane, result } =
            receiver.recv().expect("actual native planner completed")
        else {
            panic!("expected real plan");
        };
        plans[lane] = result.expect("native metadata");
    }
    let candidates = plans
        .iter()
        .flatten()
        .map(|metadata| metadata.reservation().batch_id)
        .collect();
    let committed = handle
        .coordinators
        .committed_candidates(partition(), candidates, false)
        .expect("actual current coordinator bitmap");
    let bounds = handle
        .coordinators
        .fetch_snapshot(partition(), LogicalOffset::new(0))
        .expect("actual current watermarks");
    for mode in [
        FetchMode::Ordered,
        FetchMode::Replicated,
        FetchMode::AsAvailable,
    ] {
        for bytes in [1, 19, 4096] {
            let expected = ordinary_selector_before_bounded(
                &plans,
                mode,
                bounds.ordered_end,
                Some(&committed),
                bytes,
            )
            .expect("whole native original oracle");
            let ordinary = super::super::select_fetch_plan(
                &plans,
                mode,
                bounds.ordered_end,
                Some(&committed),
                bytes,
            )
            .expect("ordinary shared entry");
            let actual_budget = StorageFetchBudget::new(
                limits().storage(),
                Instant::now() + Duration::from_secs(10),
            )
            .expect("same original finite counters");
            let bounded = super::super::select_fetch_plan_bounded(
                &plans,
                mode,
                bounds.ordered_end,
                Some(&committed),
                bytes,
                128,
                Some(&actual_budget),
            )
            .expect("bounded shared native entry");
            assert_eq!(ordinary.by_shard, expected.by_shard);
            assert_eq!(ordinary.merge_order, expected.merge_order);
            assert_eq!(bounded.by_shard, expected.by_shard);
            assert_eq!(bounded.merge_order, expected.merge_order);
        }
    }
}

// The entire pre-change native selector is retained as an independent source
// oracle; only its function name changes. It receives real native plans/maps.
fn ordinary_selector_before_bounded(
    shard_plans: &[Vec<StoredBatchMetadata>],
    mode: FetchMode,
    ordered_end: LogicalOffset,
    committed: Option<&CommittedBatchSet>,
    max_bytes: usize,
) -> EngineResult<FetchSelection> {
    let mut positions = vec![0usize; shard_plans.len()];
    let mut ready = BinaryHeap::with_capacity(shard_plans.len());
    let mut selected = shard_plans.iter().map(|_| Vec::new()).collect::<Vec<_>>();
    let mut merge_order = Vec::new();
    for (lane, planned) in shard_plans.iter().enumerate() {
        if let Some(metadata) = planned.first() {
            ready.push(Reverse((metadata.reservation().first_offset, lane)));
        }
    }

    let mut used = 0usize;
    let mut selected_count = 0usize;
    while let Some(Reverse((_, lane))) = ready.pop() {
        let metadata = shard_plans[lane][positions[lane]];
        positions[lane] += 1;
        if let Some(next) = shard_plans[lane].get(positions[lane]) {
            ready.push(Reverse((next.reservation().first_offset, lane)));
        }

        if matches!(mode, FetchMode::Ordered | FetchMode::Replicated)
            && metadata.reservation().last_offset >= ordered_end
        {
            break;
        }
        let visible = committed
            .expect("fetch planning has a committed snapshot")
            .contains(metadata.reservation().batch_id);
        if !visible {
            if mode == FetchMode::AsAvailable {
                continue;
            }
            if metadata.reservation().first_offset >= ordered_end {
                break;
            }
            continue;
        }

        let next = used.checked_add(metadata.payload_len()).ok_or_else(|| {
            EngineError::InvalidConfig("selected fetch byte count overflow".into())
        })?;
        if selected_count > 0 && next > max_bytes {
            break;
        }
        used = next;
        selected[lane].push(metadata);
        merge_order.push(lane);
        selected_count += 1;
    }
    Ok(FetchSelection {
        by_shard: selected,
        merge_order,
    })
}
