//! Genuine native origin-producer history and actual WAL reads; no private AA
//! codec/currentness/route/leader qualification is inferred from these data.

use super::*;
use crate::{NativeRecoveryCensus, NativeRecoveryLimits};
use shard_stream_core::{DirectoryOwnershipError, TopicId};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

const PRODUCER: u128 = 45;
const EPOCH: u32 = 7;
const BODIES: [&[u8]; 3] = [
    b"first actual native origin event",
    b"second actual native origin event",
    b"last actual native origin event",
];

struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let stamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("actual clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "native-origin-progress-{}-{stamp}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual source root");
        Self(path)
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn partition() -> TopicPartition {
    TopicPartition::new(TopicId::new(24), LogicalPartitionId::new(0))
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
fn limits() -> NativeOriginProgressLimits {
    NativeOriginProgressLimits {
        max_queries: 8,
        max_commands: 64,
        fetch: NativeFetchLimits {
            max_plan_batches_per_shard: 128,
            max_selected_batches: 1,
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
fn policy() -> NativeOriginProgressBudget {
    NativeOriginProgressBudget::new(limits(), Instant::now() + Duration::from_secs(30))
        .expect("real finite original-E budget")
}
fn recovery_limits() -> NativeRecoveryLimits {
    NativeRecoveryLimits {
        // Complete one-pack history has at most four extents (including the
        // unresolved tail): read/restore/engine/journal reserve 82+17*packs+8*N.
        max_files: 82 + 17 + 8 * 4,
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
fn append(
    engine: &StreamEngine,
    sequence: u64,
    body: &'static [u8],
    group: Option<AtomicGroupIdentity>,
) {
    engine
        .append(AppendRequest {
            request_id: u128::from(sequence),
            topic_id: partition().topic_id,
            partition_id: partition().partition_id,
            record_count: 1,
            payload: Bytes::from_static(body),
            durability: Durability::Leader,
            producer: Some(ProducerIdentity {
                producer_id: PRODUCER,
                epoch: EPOCH,
                first_sequence: sequence,
                event_id: Some(RecordId::for_producer_event(
                    partition().topic_id,
                    partition().partition_id,
                    PRODUCER,
                    EPOCH,
                    sequence,
                )),
            }),
            atomic_group: group,
            leader_epoch: None,
            extension_context: None,
        })
        .expect("actual durable origin frame");
}
fn recovered(root: &Path, count: usize, unresolved: bool) -> EngineHandle {
    let cfg = config(root);
    {
        let writer = StreamEngine::open(cfg.clone()).expect("actual native writer");
        writer
            .create_topic(TopicConfig {
                topic_id: partition().topic_id,
                partitions: 1,
                shards: None,
            })
            .expect("actual topic created before strict query");
        for (sequence, body) in BODIES.iter().take(count).enumerate() {
            append(&writer, sequence as u64, body, None);
        }
        if unresolved {
            append(
                &writer,
                count as u64,
                b"actual unresolved origin event",
                Some(AtomicGroupIdentity {
                    transaction_id: 99,
                    chunk_sequence: 0,
                }),
            );
        }
    }
    let owner = DirectoryOwnership::acquire(&cfg.data_dir).expect("same actual source root");
    let storage = NativeRecoveryCensus::read_for_engine(
        cfg,
        owner,
        None,
        recovery_limits(),
        Instant::now() + Duration::from_secs(30),
    )
    .expect("actual native pre-open census")
    .restore_storage()
    .expect("genuine retained native storage");
    EngineHandle::from_recovered(Arc::new(
        StreamEngine::open_recovered_storage(storage, None, None, None, None)
            .expect("actual recovered engine workers"),
    ))
}
fn contended(root: &Path) {
    assert!(matches!(
        DirectoryOwnership::acquire(&root.join("data")),
        Err(DirectoryOwnershipError::Contended)
    ));
}
fn ordinary_tail(handle: &EngineHandle, sequence: u64) -> Vec<FetchedBatch> {
    handle
        .fetch(FetchRequest {
            request_id: 0,
            topic_id: partition().topic_id,
            partition_id: partition().partition_id,
            start_offset: LogicalOffset::new(sequence),
            max_bytes: 1,
            mode: FetchMode::Ordered,
        })
        .expect("unchanged ordinary native read oracle")
}

#[test]
fn bounded_origin_reads_actual_recovered_producer_and_last_wal_event_and_keeps_owner() {
    let root = Temporary::new();
    let handle = recovered(&root.0, 3, false);
    let expected = handle
        .producer_progress(partition(), PRODUCER)
        .expect("ordinary cursor oracle")
        .expect("actual durable producer");
    let tail = ordinary_tail(&handle, expected.next_sequence - 1);
    let budget = policy();
    let report = handle
        .origin_progress_until(partition(), PRODUCER, &budget)
        .expect("actual strict native progress");
    assert_eq!(report.producer_progress(), Some(expected));
    assert_eq!(expected.next_sequence, 3);
    assert_eq!(expected.epoch, EPOCH);
    assert_eq!(report.last_batch(), tail.first());
    assert_eq!(
        report.last_producer_identity(),
        Some(ProducerIdentity {
            producer_id: PRODUCER,
            epoch: EPOCH,
            first_sequence: 2,
            event_id: Some(RecordId::for_producer_event(
                partition().topic_id,
                partition().partition_id,
                PRODUCER,
                EPOCH,
                2
            )),
        })
    );
    assert_eq!(
        report.last_producer_digest(),
        Some((
            shardlog::DIGEST_XXH3_128,
            xxhash_rust::xxh3::xxh3_128(BODIES[2]).to_le_bytes()
        ))
    );
    assert_eq!(
        report
            .last_batch()
            .expect("actual native tail")
            .payload
            .as_ref(),
        BODIES[2]
    );
    assert_eq!(
        report
            .last_batch()
            .expect("actual native identity")
            .event_id,
        Some(RecordId::for_producer_event(
            partition().topic_id,
            partition().partition_id,
            PRODUCER,
            EPOCH,
            2
        ))
    );
    assert_eq!(report.usage().queries, 1);
    assert_eq!(report.usage().commands, 5);
    assert!(report.usage().storage.read_attempts > 0);
    assert!(report.engine_handle().ptr_eq(&handle));
    assert_eq!(report.original_expiry(), budget.original_expiry());
    let weak = handle.downgrade();
    drop(handle);
    contended(&root.0);
    assert!(weak.upgrade().is_some());
    drop(report);
    assert!(weak.upgrade().is_none());
    StreamEngine::open(config(&root.0)).expect("last genuine report settles workers/root");
}

#[test]
fn bounded_origin_real_absence_stays_distinct_from_unknown_partition_and_never_creates_topic() {
    let root = Temporary::new();
    let handle = recovered(&root.0, 0, false);
    let budget = policy();
    let report = handle
        .origin_progress_until(partition(), PRODUCER, &budget)
        .expect("actual absent producer on configured source");
    assert_eq!(report.producer_progress(), None);
    assert!(report.last_batch().is_none());
    assert_eq!(report.usage().storage.read_attempts, 0);
    assert_eq!(report.usage().commands, 1);
    let before =
        fs::read(root.0.join("data/coordinator.ssj")).expect("actual durable control input");
    let unknown = TopicPartition::new(TopicId::new(999), LogicalPartitionId::new(0));
    let error = handle
        .origin_progress_until(unknown, PRODUCER, &budget)
        .expect_err("unknown topic is not empty progress");
    assert!(matches!(
        error
            .observation()
            .and_then(NativeOriginProgressObservation::error),
        Some(NativeOriginProgressQueryError::Engine(
            EngineError::UnknownPartition { .. }
        ))
    ));
    assert_eq!(
        fs::read(root.0.join("data/coordinator.ssj")).expect("unchanged actual control input"),
        before
    );
    assert!(handle.topic_partitions(unknown.topic_id).is_empty());
    assert!(error.query_entered());
    assert_eq!(error.usage().expect("same cumulative attempts").queries, 2);
}

#[test]
fn bounded_origin_uses_ordered_visibility_and_does_not_read_unresolved_as_last_event() {
    let root = Temporary::new();
    let handle = recovered(&root.0, 3, true);
    let cursor = handle
        .producer_progress(partition(), PRODUCER)
        .expect("actual cursor")
        .expect("producer");
    assert_eq!(cursor.next_sequence, 4);
    assert!(ordinary_tail(&handle, 3).is_empty());
    let budget = policy();
    let before = handle
        .origin_progress_until(partition(), PRODUCER, &budget)
        .expect("real known cursor, no visible tail");
    assert_eq!(before.producer_progress(), Some(cursor));
    assert!(before.last_batch().is_none());
    handle
        .commit_atomic_group(partition(), 99)
        .expect("genuine durable native commit");
    let after = handle
        .origin_progress_until(partition(), PRODUCER, &budget)
        .expect("same counters after actual commit");
    assert_eq!(
        after
            .last_batch()
            .expect("now visible physical origin record")
            .payload
            .as_ref(),
        b"actual unresolved origin event"
    );
    assert_eq!(after.usage().queries, 2);
    assert_eq!(after.usage().commands, 10);
}

#[test]
fn bounded_origin_query_command_and_read_caps_are_cumulative_real_refusals() {
    for case in ["queries", "commands", "read-bytes"] {
        let root = Temporary::new();
        let handle = recovered(&root.0, 3, false);
        let mut allowed = limits();
        match case {
            "queries" => allowed.max_queries = 1,
            "commands" => allowed.max_commands = 4,
            _ => allowed.fetch.max_read_bytes = 0,
        }
        let budget =
            NativeOriginProgressBudget::new(allowed, Instant::now() + Duration::from_secs(30))
                .expect("real finite ceiling");
        let first = handle.origin_progress_until(partition(), PRODUCER, &budget);
        let error = if case == "queries" {
            let report = first.expect("one actual query fits");
            assert_eq!(report.usage().commands, 5);
            handle
                .origin_progress_until(partition(), PRODUCER, &budget)
                .expect_err("no fresh counters for next origin")
        } else {
            first.expect_err("actual immutable command/read ceiling")
        };
        assert_eq!(error.usage().expect("same actual counters").queries, 1);
        assert!(budget.check().is_err());
        if case != "queries" {
            assert!(
                error
                    .observation()
                    .and_then(NativeOriginProgressObservation::producer_progress)
                    .flatten()
                    .is_some_and(|p| p.next_sequence == 3)
            );
            assert_eq!(
                error
                    .usage()
                    .expect("same actual reads")
                    .storage
                    .read_attempts,
                0
            );
        }
        if case == "commands" {
            assert!(error.fetch_error().is_none());
            assert_eq!(
                error.usage().expect("only actual producer slot").commands,
                1
            );
        }
        if case == "read-bytes" {
            assert!(error.fetch_error().is_some());
        }
        drop(handle);
        contended(&root.0);
        drop(error);
    }
}

#[test]
fn bounded_origin_exact_native_entry_fee_fits_and_one_less_retains_actual_tail_body() {
    let root = Temporary::new();
    let handle = recovered(&root.0, 3, false);
    let measured = handle
        .origin_progress_until(partition(), PRODUCER, &policy())
        .expect("measure genuine native fees");
    let mut allowed = limits();
    allowed.fetch.max_entries = measured.usage().storage.entries;
    let exact = NativeOriginProgressBudget::new(allowed, Instant::now() + Duration::from_secs(30))
        .expect("exact policy");
    let fitting = handle
        .origin_progress_until(partition(), PRODUCER, &exact)
        .expect("exact actual fees fit");
    assert_eq!(fitting.last_batch(), measured.last_batch());
    assert_eq!(fitting.usage().storage, measured.usage().storage);
    allowed.fetch.max_entries -= 1;
    let fewer = NativeOriginProgressBudget::new(allowed, Instant::now() + Duration::from_secs(30))
        .expect("one-less policy");
    let error = handle
        .origin_progress_until(partition(), PRODUCER, &fewer)
        .expect_err("one fewer actual entry");
    assert!(error.completed_payloads().any(|b| b == BODIES[2]));
    assert!(
        error
            .observation()
            .and_then(NativeOriginProgressObservation::producer_progress)
            .flatten()
            .is_some_and(|p| p.next_sequence == 3)
    );
}

struct Gate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}
impl Gate {
    fn block(&self) {
        let mut state = self.state.lock().expect("actual observation");
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            state = self.changed.wait(state).expect("physical completion wait");
        }
    }
    fn wait(&self) {
        let state = self.state.lock().expect("actual entry");
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(10), |s| !s.0)
            .expect("actual native observation");
        assert!(state.0 && !timeout.timed_out());
    }
    fn release(&self) {
        self.state.lock().expect("actual release").1 = true;
        self.changed.notify_all();
    }
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[test]
fn bounded_origin_late_actual_cursor_is_retained_and_its_entered_worker_physically_drains() {
    let root = Temporary::new();
    let handle = recovered(&root.0, 3, false);
    let weak = handle.downgrade();
    let gate = Arc::new(Gate {
        state: Mutex::new((false, false)),
        changed: Condvar::new(),
    });
    let _release = Release(gate.clone());
    let actual_gate = gate.clone();
    let expiry = Instant::now() + Duration::from_secs(1);
    let job = thread::spawn(move || {
        let _observe =
            crate::coordinator::observe_origin_progress(Arc::new(move |_| actual_gate.block()));
        let budget = NativeOriginProgressBudget::new(limits(), expiry).expect("actual saved E");
        handle.origin_progress_until(partition(), PRODUCER, &budget)
    });
    gate.wait();
    contended(&root.0);
    thread::sleep(expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(1));
    assert!(!job.is_finished());
    gate.release();
    let error = job
        .join()
        .expect("actual owning caller joined")
        .expect_err("post-observation expiry");
    assert!(error.query_entered());
    let observed = error.observation().expect("actual native data retained");
    assert!(
        observed
            .producer_progress()
            .flatten()
            .is_some_and(|p| p.next_sequence == 3)
    );
    assert!(matches!(
        observed.refusal(),
        Some(StorageFetchFailure::Expired(_))
    ));
    assert_eq!(
        error
            .usage()
            .expect("same original fees")
            .storage
            .read_attempts,
        0
    );
    assert!(weak.upgrade().is_some());
    contended(&root.0);
    drop(error);
    assert!(weak.upgrade().is_none());
}

#[test]
fn bounded_origin_post_observation_unwind_keeps_original_cursor_and_engine() {
    let root = Temporary::new();
    let handle = recovered(&root.0, 3, false);
    let _observe = crate::coordinator::observe_origin_progress(Arc::new(|_| {
        std::panic::panic_any(std::io::Error::other(
            "actual native observation cleanup unwind",
        ))
    }));
    let error = handle
        .origin_progress_until(partition(), PRODUCER, &policy())
        .expect_err("real native cleanup panic");
    let observed = error.observation().expect("actual cursor preserved");
    assert!(
        observed
            .producer_progress()
            .flatten()
            .is_some_and(|p| p.next_sequence == 3)
    );
    assert!(
        observed
            .panic_payload()
            .is_some_and(|p| p.downcast_ref::<std::io::Error>().is_some())
    );
    drop(handle);
    contended(&root.0);
    drop(error);
}

#[test]
fn bounded_origin_real_missing_pack_error_keeps_producer_and_full_native_fetch_custody() {
    let root = Temporary::new();
    let handle = recovered(&root.0, 3, false);
    let directory = root.0.join("data/shards/shard-0");
    let pack = fs::read_dir(&directory)
        .expect("actual native pack directory")
        .map(|e| e.expect("actual entry").path())
        .find(|p| p.extension().is_some_and(|e| e == "sse"))
        .expect("actual committed WAL pack");
    let moved = pack.with_extension("moved");
    fs::rename(&pack, &moved).expect("real namespace removal");
    let error = handle
        .origin_progress_until(partition(), PRODUCER, &policy())
        .expect_err("actual retained pack open fails");
    assert!(
        error
            .observation()
            .and_then(NativeOriginProgressObservation::producer_progress)
            .flatten()
            .is_some_and(|p| p.next_sequence == 3)
    );
    assert!(error.fetch_error().is_some());
    assert!(
        error
            .fetch_error()
            .expect("whole native read error")
            .storage_failures()
            .any(|failure| matches!(failure, StorageFetchFailure::Storage(_)))
    );
    fs::rename(moved, pack).expect("restore actual unchanged source WAL");
    drop(handle);
    contended(&root.0);
    drop(error);
}

#[test]
fn bounded_origin_expired_saved_budget_enters_no_query_or_file_read_and_cannot_refresh() {
    let root = Temporary::new();
    let handle = recovered(&root.0, 3, false);
    let budget =
        NativeOriginProgressBudget::new(limits(), Instant::now() + Duration::from_millis(20))
            .expect("saved short original E");
    thread::sleep(
        budget
            .original_expiry()
            .saturating_duration_since(Instant::now())
            + Duration::from_millis(1),
    );
    let error = handle
        .origin_progress_until(partition(), PRODUCER, &budget)
        .expect_err("same original E elapsed");
    assert!(!error.query_entered());
    assert!(error.observation().is_none());
    assert_eq!(
        error.usage().expect("no work entered"),
        NativeOriginProgressUsage::default()
    );
    assert!(
        handle
            .origin_progress_until(partition(), PRODUCER, &budget)
            .is_err()
    );
}

#[test]
fn bounded_origin_cost_is_plain_checked_and_rejects_query_frame_overflow() {
    let root = Temporary::new();
    let config = config(&root.0);
    let mut allowed = limits();
    let cost = native_origin_progress_cost(&config, allowed).expect("actual finite full cost");
    assert!(cost.declared_bytes >= allowed.fetch.max_declared_bytes);
    assert!(cost.entries >= allowed.fetch.max_entries);
    assert!(cost.work_bytes >= allowed.fetch.max_work_bytes);
    allowed.max_queries = usize::MAX;
    assert!(matches!(
        native_origin_progress_cost(&config, allowed),
        Err(NativeOriginProgressFailure::Budget(
            StorageFetchFailure::Capacity
        ))
    ));
    assert!(!config.data_dir.exists());
}
