//! Genuine native replay/ticket/worker fixtures; not production transport or
//! SDK qualification, issuer authority, currentness or serving evidence.

use super::*;
use crate::{
    DurableAppendSink, DurableAppendSinkFactory, DurableSinkApply, DurableSinkOptions,
    NativeRecoveryCensus, NativeRecoveryLimits,
};
use shard_stream_core::{DirectoryOwnershipError, TopicId};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::sync::{Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime};

struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let stamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("actual clock")
            .as_nanos();
        let p = std::env::temp_dir().join(format!(
            "native-resume-{}-{stamp}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p).expect("actual native fixture root");
        Self(p)
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
    TopicPartition::new(TopicId::new(23), LogicalPartitionId::new(0))
}
fn topic(engine: &StreamEngine) {
    engine
        .create_topic(TopicConfig {
            topic_id: partition().topic_id,
            partitions: 1,
            shards: None,
        })
        .expect("actual native topic journal");
}
fn fetch() -> FetchRequest {
    FetchRequest {
        request_id: 0,
        topic_id: partition().topic_id,
        partition_id: partition().partition_id,
        start_offset: LogicalOffset::new(0),
        max_bytes: 4096,
        mode: FetchMode::Ordered,
    }
}
fn limits() -> NativeResumeLimits {
    NativeResumeLimits {
        max_partitions: 16,
        max_fetch_passes: 32,
        max_batches: 32,
        max_atomic_groups: 32,
        max_callbacks: 64,
        max_jobs: 128,
        max_steps: 256,
        max_entries: 1_000_000,
        max_declared_bytes: 1 << 26,
        max_work_bytes: 1 << 30,
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
fn recovered(root: &Path, transport: Arc<dyn ReplicationTransport>) -> EngineHandle {
    recovered_with_sink(root, transport, None)
}
fn recovered_with_sink(
    root: &Path,
    transport: Arc<dyn ReplicationTransport>,
    sink: Option<DurableSinkConfig>,
) -> EngineHandle {
    let cfg = config(root);
    {
        let writer = StreamEngine::open(cfg.clone()).expect("actual native RF1 writer");
        topic(&writer);
        for request_id in 0..3 {
            writer
                .append(AppendRequest {
                    request_id,
                    topic_id: partition().topic_id,
                    partition_id: partition().partition_id,
                    record_count: 1,
                    payload: Bytes::from_static(b"actual retained resume body"),
                    durability: Durability::Leader,
                    producer: None,
                    atomic_group: None,
                    leader_epoch: None,
                    extension_context: None,
                })
                .expect("real durable native frame");
        }
    }
    let owner = DirectoryOwnership::acquire(&cfg.data_dir).expect("same actual source owner");
    let census = NativeRecoveryCensus::read_for_engine(
        cfg,
        owner,
        None,
        native_limits(),
        Instant::now() + Duration::from_secs(60),
    )
    .expect("actual native pre-open source census");
    let storage = census
        .restore_storage()
        .expect("actual native same-owner restore");
    EngineHandle::from_recovered(Arc::new(
        StreamEngine::open_recovered_storage(storage, None, Some(transport), None, sink)
            .expect("actual recovered native workers"),
    ))
}
fn contended(root: &Path) {
    assert!(matches!(
        DirectoryOwnership::acquire(&root.join("data")),
        Err(DirectoryOwnershipError::Contended)
    ));
}

#[derive(Debug, Default)]
struct GateState {
    entered: bool,
    release: bool,
}
type Gate = Arc<(Mutex<GateState>, Condvar)>;
fn gate(open: bool) -> Gate {
    Arc::new((
        Mutex::new(GateState {
            entered: false,
            release: open,
        }),
        Condvar::new(),
    ))
}
fn release(gate: &Gate) {
    let mut state = gate
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.release = true;
    gate.1.notify_all();
}
struct Release(Gate);
impl Drop for Release {
    fn drop(&mut self) {
        release(&self.0);
    }
}
struct PhysicalJob {
    batch: ReplicaAppend,
    expiry: Instant,
    response: SyncSender<EngineResult<u32>>,
}

// One bounded actual supervisor joins each entered target thread before ticket
// response. The target performs a real native follower append and can block in
// actual cleanup. This fixture tests ticket/custody mechanics; it does not
// qualify the ordinary follower append as a production per-phase-E producer.
#[derive(Debug)]
struct PhysicalTransport {
    sender: Mutex<Option<SyncSender<PhysicalJob>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    strict_calls: AtomicUsize,
    ordinary_calls: AtomicUsize,
}
impl PhysicalTransport {
    fn new(follower: EngineHandle, gate: Gate) -> Arc<Self> {
        let (sender, receiver) = sync_channel::<PhysicalJob>(2);
        let worker = thread::spawn(move || {
            while let Ok(job) = receiver.recv() {
                let engine = follower.clone();
                let actual_gate = Arc::clone(&gate);
                let target = thread::spawn(move || {
                    if Instant::now() >= job.expiry {
                        return Err(EngineError::UnsupportedDurability(
                            "fixture target expired before append",
                        ));
                    }
                    let result = engine.append_replica(job.batch).map(|_| 1);
                    let mut state = actual_gate.0.lock().expect("actual target cleanup gate");
                    state.entered = true;
                    actual_gate.1.notify_all();
                    while !state.release {
                        state = actual_gate
                            .1
                            .wait(state)
                            .expect("actual target cleanup wait");
                    }
                    drop(state);
                    drop(engine);
                    result
                });
                let result = target.join().expect("actual target physically joined");
                let _ = job.response.send(result);
            }
            drop(follower);
        });
        Arc::new(Self {
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(Some(worker)),
            strict_calls: AtomicUsize::new(0),
            ordinary_calls: AtomicUsize::new(0),
        })
    }
    fn enter(&self, batch: ReplicaAppend, expiry: Instant) -> EngineResult<ReplicationTicket> {
        let (response, receiver) = sync_channel(1);
        let queue = self.sender.lock().expect("actual bounded transport queue");
        let sender = queue.as_ref().expect("actual live transport");
        sender
            .try_send(PhysicalJob {
                batch,
                expiry,
                response,
            })
            .map_err(|error| match error {
                TrySendError::Full(_) => {
                    EngineError::UnsupportedDurability("actual fixture queue full")
                }
                TrySendError::Disconnected(_) => {
                    EngineError::UnsupportedDurability("actual fixture worker stopped")
                }
            })?;
        Ok(ReplicationTicket::pending(receiver))
    }
}
impl ReplicationTransport for PhysicalTransport {
    fn install_progress(&self, _: ReplicationProgress) -> EngineResult<()> {
        Ok(())
    }
    fn replicate(&self, batch: ReplicaAppend, required_followers: u32) -> EngineResult<u32> {
        assert_eq!(
            required_followers, 0,
            "unchanged ordinary zero-follower policy"
        );
        self.ordinary_calls.fetch_add(1, Ordering::Relaxed);
        drop(self.enter(batch, Instant::now() + Duration::from_secs(30))?);
        Ok(0)
    }
    fn begin_recovered_replicate_until(
        &self,
        batch: ReplicaAppend,
        expiry: Instant,
    ) -> EngineResult<ReplicationTicket> {
        self.strict_calls.fetch_add(1, Ordering::Relaxed);
        self.enter(batch, expiry)
    }
}
impl Drop for PhysicalTransport {
    fn drop(&mut self) {
        self.sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            worker.join().expect("actual transport supervisor joined");
        }
    }
}
fn transport(root: &Path, gate: Gate) -> (Arc<PhysicalTransport>, EngineHandle) {
    let follower = EngineHandle::from(Arc::new(
        StreamEngine::open(config(root)).expect("actual follower native storage"),
    ));
    topic(&follower);
    (PhysicalTransport::new(follower.clone(), gate), follower)
}

#[test]
fn same_native_fetch_budget_replays_every_real_body_and_exact_cumulative_entries_fit() {
    let source = Temporary::new();
    let target = Temporary::new();
    let (transport, follower) = transport(&target.0, gate(true));
    let handle = recovered(&source.0, transport.clone());
    let report = handle
        .resume_recovered_replication_until(
            &[partition()],
            limits(),
            Instant::now() + Duration::from_secs(20),
        )
        .expect("actual physically settled recovered replay");
    assert_eq!(report.usage().fetch_passes, 3);
    assert_eq!(report.usage().batches, 3);
    assert_eq!(transport.strict_calls.load(Ordering::Relaxed), 3);
    assert_eq!(transport.ordinary_calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        follower.fetch(fetch()).expect("actual follower WAL bodies"),
        handle.fetch(fetch()).expect("actual original WAL bodies")
    );
    assert_eq!(
        report
            .outcomes()
            .iter()
            .filter(|o| matches!(
                o,
                NativeResumeOutcome::ReplicationCompleted {
                    result: Ok(Ok(1)),
                    refusal: None,
                    ..
                }
            ))
            .count(),
        3
    );
    let mut exact = limits();
    exact.max_entries = report.usage().entries;
    exact.fetch.max_entries = report.fetch_usage().entries;
    let fitting = handle
        .resume_recovered_replication_until(
            &[partition()],
            exact,
            Instant::now() + Duration::from_secs(20),
        )
        .expect("same actual exact cumulative allowances");
    assert_eq!(fitting.usage(), report.usage());
    assert_eq!(fitting.fetch_usage(), report.fetch_usage());
    exact.max_entries -= 1;
    let error = handle
        .resume_recovered_replication_until(
            &[partition()],
            exact,
            Instant::now() + Duration::from_secs(20),
        )
        .expect_err("one less actual native entry");
    assert!(matches!(
        error.failure(),
        NativeResumeFailure::Work(NativeResumeRefusal::Capacity)
    ));
    assert!(error.engine_handle().ptr_eq(&handle));
    assert!(error.retained_payloads().next().is_some());
    drop(handle);
    contended(&source.0);
    drop(error);
    StreamEngine::open(config(&source.0)).expect("source root releases only after real last owner");
}

#[test]
fn a_real_late_ticket_is_physically_joined_and_raw_completion_retained_before_failure() {
    let source = Temporary::new();
    let target = Temporary::new();
    let gate = gate(false);
    let _release = Release(Arc::clone(&gate));
    let (transport, follower) = transport(&target.0, Arc::clone(&gate));
    let handle = recovered(&source.0, transport.clone());
    let weak = handle.downgrade();
    let expiry = Instant::now() + Duration::from_secs(5);
    let job = thread::spawn(move || {
        handle.resume_recovered_replication_until(&[partition()], limits(), expiry)
    });
    let state = gate.0.lock().expect("actual target observation");
    let (state, _) = gate
        .1
        .wait_timeout_while(state, Duration::from_secs(10), |s| !s.entered)
        .expect("actual physical target entry");
    assert!(state.entered);
    drop(state);
    assert!(!job.is_finished());
    contended(&source.0);
    assert!(weak.upgrade().is_some());
    thread::sleep(expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(1));
    assert!(
        !job.is_finished(),
        "logical E does not drop the actual entered target job"
    );
    release(&gate);
    let error = job
        .join()
        .expect("actual native replay caller joined")
        .expect_err("late physical completion refuses success");
    assert_eq!(
        transport.strict_calls.load(Ordering::Relaxed),
        1,
        "no later target may enter after original E"
    );
    assert!(error.outcomes().iter().any(|o| matches!(
        o,
        NativeResumeOutcome::ReplicationCompleted {
            result: Ok(Ok(1)),
            refusal: Some(NativeResumeRefusal::Expired),
            ..
        }
    )));
    assert_eq!(
        follower
            .fetch(fetch())
            .expect("actual completed follower WAL append")
            .len(),
        1
    );
    assert_eq!(
        error.retained_payloads().next(),
        Some(b"actual retained resume body".as_slice())
    );
    contended(&source.0);
    drop(error);
    assert!(weak.upgrade().is_none());
    StreamEngine::open(config(&source.0)).expect("actual source cleanup after error drops");
}

#[derive(Debug, Default)]
struct LegacyOnly(AtomicUsize);
impl ReplicationTransport for LegacyOnly {
    fn install_progress(&self, _: ReplicationProgress) -> EngineResult<()> {
        Ok(())
    }
    fn replicate(&self, _: ReplicaAppend, required: u32) -> EngineResult<u32> {
        assert_eq!(required, 0);
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(0)
    }
}
#[test]
fn ordinary_enqueue_only_transport_stays_ordinary_and_strict_default_refuses_before_legacy_work() {
    let root = Temporary::new();
    let transport = Arc::new(LegacyOnly::default());
    let handle = recovered(&root.0, transport.clone());
    let error = handle
        .resume_recovered_replication_until(
            &[partition()],
            limits(),
            Instant::now() + Duration::from_secs(20),
        )
        .expect_err("old enqueue-only transport has no strict completion producer");
    assert_eq!(transport.0.load(Ordering::Relaxed), 0);
    assert!(error.outcomes().iter().any(|o| matches!(o,NativeResumeOutcome::ReplicationStarted { result: Err(error),.. } if matches!(error.engine_error(),Some(EngineError::UnsupportedDurability(_))))));
    handle
        .resume_recovered_replication(&[partition()])
        .expect("actual old public path uses shared original native loop");
    assert_eq!(transport.0.load(Ordering::Relaxed), 3);
}

#[test]
fn actual_partition_pass_caps_expired_entry_and_overflow_keep_same_recovered_owner() {
    let root = Temporary::new();
    let transport = Arc::new(LegacyOnly::default());
    let handle = recovered(&root.0, transport.clone());
    let expiry = Instant::now();
    let error = handle
        .resume_recovered_replication_until(&[partition()], limits(), expiry)
        .expect_err("already expired before first native phase");
    assert!(matches!(error.failure(), NativeResumeFailure::Expired));
    assert_eq!(
        error.usage().expect("pre-entry fees"),
        NativeResumeUsage::default()
    );
    assert!(error.outcomes().is_empty());
    let mut cap = limits();
    cap.max_partitions = 0;
    let refused = handle
        .resume_recovered_replication_until(
            &[partition()],
            cap,
            Instant::now() + Duration::from_secs(20),
        )
        .expect_err("partition admission before worker entry");
    assert!(matches!(
        refused.failure(),
        NativeResumeFailure::Work(NativeResumeRefusal::Capacity)
    ));
    assert!(refused.outcomes().is_empty());
    cap = limits();
    cap.max_steps = usize::MAX;
    let overflow = handle
        .resume_recovered_replication_until(
            &[partition()],
            cap,
            Instant::now() + Duration::from_secs(20),
        )
        .expect_err("checked real outcome capacity arithmetic");
    assert!(matches!(
        overflow.failure(),
        NativeResumeFailure::Work(NativeResumeRefusal::Capacity)
    ));
    assert_eq!(transport.0.load(Ordering::Relaxed), 0);
    assert!(
        error.engine_handle().ptr_eq(&handle)
            && refused.engine_handle().ptr_eq(&handle)
            && overflow.engine_handle().ptr_eq(&handle)
    );
}

#[test]
fn real_failed_follower_ticket_retains_original_native_error_and_does_not_enter_later_jobs() {
    let source = Temporary::new();
    let target = Temporary::new();
    let follower = EngineHandle::from(Arc::new(
        StreamEngine::open(config(&target.0)).expect("real follower without this topic"),
    ));
    let transport = PhysicalTransport::new(follower, gate(true));
    let handle = recovered(&source.0, transport.clone());
    let error = handle
        .resume_recovered_replication_until(
            &[partition()],
            limits(),
            Instant::now() + Duration::from_secs(20),
        )
        .expect_err("actual follower's native UnknownPartition result");
    assert_eq!(transport.strict_calls.load(Ordering::Relaxed), 1);
    assert!(error.outcomes().iter().any(|o| matches!(o,NativeResumeOutcome::ReplicationCompleted { result: Ok(Err(EngineError::UnknownPartition { topic_id,partition_id })),.. } if *topic_id==partition().topic_id && *partition_id==partition().partition_id)));
    assert!(error.engine_handle().ptr_eq(&handle));
    assert!(error.retained_payloads().next().is_some());
    drop(handle);
    contended(&source.0);
    drop(error);
    StreamEngine::open(config(&source.0)).expect("actual native last error owner released");
}

#[derive(Debug)]
struct CheckpointOnlySink;
impl DurableAppendSink for CheckpointOnlySink {
    fn apply(
        &self,
        _: DurableSinkCheckpoint,
        _: &[DurableAppend],
        _: DurableSinkCheckpoint,
    ) -> EngineResult<DurableSinkApply> {
        Err(EngineError::UnsupportedDurability(
            "fixture tests checkpoint worker custody, not SDK application",
        ))
    }
}
// This real existing native-worker callback has one observed memory result and
// actual blocked cleanup. No database durability or full sink replay is claimed.
#[derive(Debug)]
struct CheckpointFactory {
    gate: Gate,
    ordinary: AtomicUsize,
    strict: AtomicUsize,
}
impl DurableAppendSinkFactory for CheckpointFactory {
    fn validate_append(&self, _: &[u8], _: NonZeroU32) -> EngineResult<()> {
        Ok(())
    }
    fn load_checkpoint(&self, _: TopicPartition) -> EngineResult<Option<DurableSinkCheckpoint>> {
        self.ordinary.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }
    fn open_shard(&self, _: ShardId) -> EngineResult<Arc<dyn DurableAppendSink>> {
        Ok(Arc::new(CheckpointOnlySink))
    }
    fn load_checkpoint_until(
        &self,
        _: TopicPartition,
        work: &NativeResumeWork,
    ) -> NativeResumeCallbackResult<Option<DurableSinkCheckpoint>> {
        work.reserve_values(
            size_of::<Option<DurableSinkCheckpoint>>(),
            0,
            1,
            size_of::<Option<DurableSinkCheckpoint>>(),
        )
        .map_err(NativeResumeCallbackError::refused)?;
        work.check().map_err(NativeResumeCallbackError::refused)?;
        let mut state = self.gate.0.lock().expect("actual native callback lock");
        work.check().map_err(NativeResumeCallbackError::refused)?;
        // Actual supported initial absence is observed before cleanup starts.
        let observed = None;
        self.strict.fetch_add(1, Ordering::Relaxed);
        state.entered = true;
        self.gate.1.notify_all();
        while !state.release {
            state = self
                .gate
                .1
                .wait(state)
                .expect("actual native callback cleanup");
        }
        drop(state);
        // No new phase follows cleanup. The caller retains this real result
        // first, then records a possibly late E separately.
        Ok(observed)
    }
}
fn sink_config(factory: Arc<CheckpointFactory>) -> DurableSinkConfig {
    DurableSinkConfig::new(factory).with_options(DurableSinkOptions {
        recovery_mode: DurableSinkRecoveryMode::Background,
        worker_count: 1,
        max_pending_items: 4,
        max_pending_bytes: Some(2 << 20),
        ..DurableSinkOptions::default()
    })
}
#[test]
fn real_same_worker_checkpoint_absence_fits_and_uses_actual_strict_provider_without_legacy_fallback()
 {
    let root = Temporary::new();
    let factory = Arc::new(CheckpointFactory {
        gate: gate(true),
        ordinary: AtomicUsize::new(0),
        strict: AtomicUsize::new(0),
    });
    let handle = recovered_with_sink(
        &root.0,
        Arc::new(LegacyOnly::default()),
        Some(sink_config(factory.clone())),
    );
    let mut scope = start(&handle, limits(), Instant::now() + Duration::from_secs(20))
        .expect("actual same native work");
    assert_eq!(
        scope
            .sink_checkpoint(partition())
            .expect("actual fitting strict checkpoint"),
        DurableSinkCheckpoint::initial(partition())
    );
    assert_eq!(factory.strict.load(Ordering::Relaxed), 1);
    assert_eq!(factory.ordinary.load(Ordering::Relaxed), 0);
    assert!(scope.outcomes.iter().any(|o| matches!(o,NativeResumeOutcome::Sink { observed,.. } if observed.callback_entered() && observed.checkpoint()==Some(None) && observed.progress().is_some_and(|p| p.result().is_ok() && p.refusal().is_none()))));
    assert_eq!(
        scope
            .work
            .usage()
            .expect("actual charged callback")
            .callbacks,
        1
    );
    let report = scope
        .finish()
        .expect("this actual checkpoint command physically settled");
    assert_eq!(report.usage().jobs, 1);
    // This command report is not a completed durable-sink replay or serving grant.
}
#[test]
fn real_native_sink_callback_cleanup_drains_after_e_and_keeps_actual_none_result_and_root() {
    let root = Temporary::new();
    let gate = gate(false);
    let _release = Release(Arc::clone(&gate));
    let factory = Arc::new(CheckpointFactory {
        gate: Arc::clone(&gate),
        ordinary: AtomicUsize::new(0),
        strict: AtomicUsize::new(0),
    });
    let handle = recovered_with_sink(
        &root.0,
        Arc::new(LegacyOnly::default()),
        Some(sink_config(factory.clone())),
    );
    let weak = handle.downgrade();
    let expiry = Instant::now() + Duration::from_secs(5);
    let job = thread::spawn(move || {
        let mut scope =
            start(&handle, limits(), expiry).expect("actual native scope before callback");
        match scope.sink_checkpoint(partition()) {
            Ok(_) => scope.finish(),
            Err(error) => Err(scope.fail(error)),
        }
    });
    let state = gate.0.lock().expect("observe actual sink worker");
    let (state, _) = gate
        .1
        .wait_timeout_while(state, Duration::from_secs(10), |s| !s.entered)
        .expect("actual callback entry");
    assert!(state.entered);
    drop(state);
    contended(&root.0);
    assert!(!job.is_finished());
    thread::sleep(expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(1));
    assert!(
        !job.is_finished(),
        "deadline alone cannot detach actual callback cleanup"
    );
    release(&gate);
    let error = job
        .join()
        .expect("actual native caller joined")
        .expect_err("actual late callback refuses");
    assert_eq!(factory.strict.load(Ordering::Relaxed), 1);
    assert_eq!(factory.ordinary.load(Ordering::Relaxed), 0);
    assert!(error.outcomes().iter().any(|o| matches!(o,NativeResumeOutcome::Sink { observed,.. } if observed.callback_entered() && observed.checkpoint()==Some(None) && observed.callback_refusal()==Some(NativeResumeRefusal::Expired) && observed.progress().is_none())));
    assert!(weak.upgrade().is_some());
    contended(&root.0);
    drop(error);
    assert!(weak.upgrade().is_none());
    StreamEngine::open(config(&root.0))
        .expect("actual last native callback error owner releases root");
}

#[derive(Debug)]
struct FailedReadFactory {
    path: PathBuf,
    gate: Gate,
    panic_after_read: bool,
    strict: AtomicUsize,
    ordinary: AtomicUsize,
}
impl DurableAppendSinkFactory for FailedReadFactory {
    fn validate_append(&self, _: &[u8], _: NonZeroU32) -> EngineResult<()> {
        Ok(())
    }
    fn load_checkpoint(&self, _: TopicPartition) -> EngineResult<Option<DurableSinkCheckpoint>> {
        self.ordinary.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }
    fn open_shard(&self, _: ShardId) -> EngineResult<Arc<dyn DurableAppendSink>> {
        Ok(Arc::new(CheckpointOnlySink))
    }
    fn load_checkpoint_until(
        &self,
        _: TopicPartition,
        work: &NativeResumeWork,
    ) -> NativeResumeCallbackResult<Option<DurableSinkCheckpoint>> {
        let bytes = size_of::<Result<fs::File, io::Error>>()
            + size_of::<io::Error>()
            + size_of::<NativeResumeCallbackError>();
        work.reserve_values(bytes, 0, 3, bytes)
            .map_err(NativeResumeCallbackError::refused)?;
        work.check().map_err(NativeResumeCallbackError::refused)?;
        let observed = fs::File::open(&self.path);
        self.strict.fetch_add(1, Ordering::Relaxed);
        // Retain the actual completed OS result through real blocked cleanup.
        let mut state = self.gate.0.lock().expect("failed-read cleanup lock");
        state.entered = true;
        self.gate.1.notify_all();
        while !state.release {
            state = self.gate.1.wait(state).expect("failed-read cleanup wait");
        }
        drop(state);
        let error = match observed {
            Err(error) => error,
            Ok(file) => {
                drop(file);
                return Err(NativeResumeCallbackError::unsupported(
                    "fixture expected an actual missing file",
                ));
            }
        };
        if self.panic_after_read {
            std::panic::panic_any(error);
        }
        Err(NativeResumeCallbackError::from_external(Box::new(error)))
    }
}
fn failed_read_sink(factory: Arc<FailedReadFactory>) -> DurableSinkConfig {
    DurableSinkConfig::new(factory).with_options(DurableSinkOptions {
        recovery_mode: DurableSinkRecoveryMode::Background,
        worker_count: 1,
        max_pending_items: 4,
        max_pending_bytes: Some(2 << 20),
        ..DurableSinkOptions::default()
    })
}

#[test]
fn actual_failed_native_callback_result_survives_fitting_and_late_cleanup_without_legacy_fallback()
{
    for late in [false, true] {
        let root = Temporary::new();
        let gate = gate(!late);
        let _release = Release(Arc::clone(&gate));
        let factory = Arc::new(FailedReadFactory {
            path: root.0.join("actual-absent-checkpoint"),
            gate: Arc::clone(&gate),
            panic_after_read: false,
            strict: AtomicUsize::new(0),
            ordinary: AtomicUsize::new(0),
        });
        assert!(!factory.path.exists());
        let handle = recovered_with_sink(
            &root.0,
            Arc::new(LegacyOnly::default()),
            Some(failed_read_sink(factory.clone())),
        );
        let weak = handle.downgrade();
        let expiry = Instant::now() + Duration::from_secs(if late { 5 } else { 20 });
        let job = thread::spawn(move || {
            let mut scope = start(&handle, limits(), expiry).expect("actual native callback work");
            match scope.sink_checkpoint(partition()) {
                Ok(_) => scope.finish(),
                Err(error) => Err(scope.fail(error)),
            }
        });
        if late {
            let state = gate.0.lock().expect("observe real failed OS phase");
            let (state, _) = gate
                .1
                .wait_timeout_while(state, Duration::from_secs(10), |s| !s.entered)
                .expect("real failed OS phase entered");
            assert!(state.entered);
            drop(state);
            contended(&root.0);
            assert!(!job.is_finished());
            thread::sleep(
                expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
            );
            assert!(
                !job.is_finished(),
                "late error cleanup must physically settle"
            );
            release(&gate);
        }
        let error = job
            .join()
            .expect("actual callback caller joined")
            .expect_err("the real missing file refuses");
        let observed = error
            .outcomes()
            .iter()
            .find_map(|outcome| match outcome {
                NativeResumeOutcome::Sink { observed, .. } => Some(observed),
                _ => None,
            })
            .expect("actual worker outcome retained");
        assert!(observed.callback_entered());
        let native = observed
            .callback_error()
            .expect("actual provider error")
            .external_error()
            .expect("same actual OS error")
            .downcast_ref::<io::Error>()
            .expect("typed actual io error");
        assert_eq!(native.kind(), io::ErrorKind::NotFound);
        assert_eq!(
            observed.callback_refusal(),
            late.then_some(NativeResumeRefusal::Expired)
        );
        assert!(observed.progress().is_none());
        assert_eq!(factory.strict.load(Ordering::Relaxed), 1);
        assert_eq!(factory.ordinary.load(Ordering::Relaxed), 0);
        assert!(!factory.path.exists());
        assert!(weak.upgrade().is_some());
        contended(&root.0);
        drop(error);
        assert!(weak.upgrade().is_none());
        StreamEngine::open(config(&root.0)).expect("real root releases after typed error owner");
    }
}

#[test]
fn actual_callback_unwind_retains_original_io_payload_and_same_root_after_worker_settlement() {
    let root = Temporary::new();
    let factory = Arc::new(FailedReadFactory {
        path: root.0.join("actual-absent-checkpoint"),
        gate: gate(true),
        panic_after_read: true,
        strict: AtomicUsize::new(0),
        ordinary: AtomicUsize::new(0),
    });
    let handle = recovered_with_sink(
        &root.0,
        Arc::new(LegacyOnly::default()),
        Some(failed_read_sink(factory.clone())),
    );
    let weak = handle.downgrade();
    let mut scope = start(&handle, limits(), Instant::now() + Duration::from_secs(20))
        .expect("actual native callback work");
    let failure = scope
        .sink_checkpoint(partition())
        .expect_err("actual callback unwound");
    let error = scope.fail(failure);
    let observed = error
        .outcomes()
        .iter()
        .find_map(|outcome| match outcome {
            NativeResumeOutcome::Sink { observed, .. } => Some(observed),
            _ => None,
        })
        .expect("same actual entered command outcome");
    let native = observed
        .callback_error()
        .expect("actual unwind error")
        .panic_payload()
        .expect("actual retained panic payload")
        .downcast_ref::<io::Error>()
        .expect("original typed OS result in panic payload");
    assert_eq!(native.kind(), io::ErrorKind::NotFound);
    assert!(observed.progress().is_none());
    assert_eq!(factory.strict.load(Ordering::Relaxed), 1);
    assert_eq!(factory.ordinary.load(Ordering::Relaxed), 0);
    assert_eq!(error.usage().expect("same denied work usage").callbacks, 1);
    drop(handle);
    contended(&root.0);
    assert!(weak.upgrade().is_some());
    drop(error);
    assert!(weak.upgrade().is_none());
    StreamEngine::open(config(&root.0))
        .expect("real root releases after actual unwind payload owner");
}

mod table_probe_regressions {
    use super::*;
    use crate::{DurableSinkAtomicVisibility, DurableSinkCheckpoint};
    use shard_stream_protocol::AtomicGroupIdentity;
    use std::io::Write;
    use std::sync::atomic::AtomicBool;

    #[derive(Clone, Copy)]
    enum Fault {
        None,
        Exact,
        OneLess,
        Expire,
    }
    #[derive(Default)]
    struct Trace {
        events: Mutex<Vec<TableProbe>>,
        injected: AtomicBool,
    }
    impl Trace {
        fn phase(&self, site: &str, phase: ProbePhase) -> bool {
            self.events
                .lock()
                .expect("actual probe observations")
                .iter()
                .any(|probe| probe.site == site && probe.phase == phase)
        }
        fn capacity(&self, site: &str) -> usize {
            self.events
                .lock()
                .expect("actual probe observations")
                .iter()
                .find(|probe| probe.site == site && probe.phase == ProbePhase::Before)
                .expect("actual native table reached")
                .capacity
        }
    }
    fn observe(work: &NativeResumeWork, target: &'static str, fault: Fault) -> Arc<Trace> {
        let trace = Arc::new(Trace::default());
        let recorded = Arc::clone(&trace);
        *work
            .inner
            .probe_observer
            .lock()
            .expect("test-only same-work observer") = Some(Arc::new(move |work, probe| {
            recorded.events.lock().expect("test-only trace").push(probe);
            if probe.site != target
                || probe.phase != ProbePhase::Before
                || recorded.injected.swap(true, Ordering::Relaxed)
            {
                return;
            }
            match fault {
                Fault::None => {}
                Fault::Expire => thread::sleep(
                    work.absolute_expiry()
                        .saturating_duration_since(Instant::now())
                        + Duration::from_millis(1),
                ),
                Fault::Exact | Fault::OneLess => {
                    // Explicit DOWNWARD TEST FAULT: consume ordinary work fees
                    // to leave exactly this real probe fee (or one less).
                    // Production limits/E are immutable; no refund/reset or
                    // composed-resource acceptance is inferred from this case.
                    let leave = probe.fee - usize::from(matches!(fault, Fault::OneLess));
                    let remaining = work.limits().max_work_bytes
                        - work.usage().expect("same real usage").work_bytes;
                    assert!(remaining >= leave);
                    work.reserve_values(0, 0, 0, remaining - leave)
                        .expect("test-only downward fee consumption");
                }
            }
        }));
        trace
    }
    fn scope(handle: &EngineHandle) -> Box<Scope> {
        start(handle, limits(), Instant::now() + Duration::from_secs(30))
            .expect("genuine same-owner native continuation")
    }
    fn add_topic(handle: &EngineHandle, id: u128) -> TopicPartition {
        let partition = TopicPartition::new(TopicId::new(id), LogicalPartitionId::new(0));
        handle
            .create_topic(TopicConfig {
                topic_id: partition.topic_id,
                partitions: 1,
                shards: None,
            })
            .expect("ordinary native catalog population");
        partition
    }
    fn atomic(handle: &EngineHandle, transaction: u128) -> AtomicGroupInspection {
        handle
            .append(AppendRequest {
                request_id: transaction,
                topic_id: partition().topic_id,
                partition_id: partition().partition_id,
                record_count: 1,
                payload: Bytes::from_static(b"genuine atomic history for retained capacity"),
                durability: Durability::Leader,
                producer: None,
                atomic_group: Some(AtomicGroupIdentity {
                    transaction_id: transaction,
                    chunk_sequence: 0,
                }),
                leader_epoch: None,
                extension_context: None,
            })
            .expect("actual native atomic frame");
        handle
            .commit_atomic_group(partition(), transaction)
            .expect("actual native atomic decision")
    }

    #[test]
    fn actual_populated_coordinator_state_hit_and_miss_pay_before_native_probe() {
        let root = Temporary::new();
        let handle = recovered(&root.0, Arc::new(LegacyOnly::default()));
        for id in 1000..1064 {
            add_topic(&handle, id);
        }
        let missing = TopicPartition::new(TopicId::new(999999), LogicalPartitionId::new(0));
        for partition in [partition(), missing] {
            for fault in [Fault::None, Fault::Exact, Fault::OneLess] {
                let mut scope = scope(&handle);
                let trace = observe(&scope.work, "coordinator.states", fault);
                let result = scope.watermarks(partition);
                assert!(trace.capacity("coordinator.states") > 1);
                assert_eq!(
                    trace.phase("coordinator.states", ProbePhase::Paid),
                    !matches!(fault, Fault::OneLess)
                );
                if matches!(fault, Fault::OneLess) {
                    assert!(result.is_err());
                    assert!(trace.phase("coordinator.states", ProbePhase::Refused));
                    assert!(matches!(
                        scope.work.check(),
                        Err(NativeResumeRefusal::Denied)
                    ));
                } else if partition == missing {
                    assert!(result.is_err());
                    assert!(scope.outcomes.iter().any(|outcome| matches!(outcome,
                        NativeResumeOutcome::Coordinator(value) if matches!(value.error().and_then(NativeResumeCallbackError::engine_error), Some(EngineError::UnknownPartition { .. }))
                    )));
                } else {
                    result.expect("exact actual coordinator lookup fee fits");
                }
            }
        }
    }

    #[test]
    fn actual_atomic_retention_leaves_sparse_native_capacity_and_prepaid_inspection() {
        let root = Temporary::new();
        let handle = recovered(&root.0, Arc::new(LegacyOnly::default()));
        let mut last = None;
        for transaction in 5000..5064 {
            last = Some(atomic(&handle, transaction));
        }
        let last = last.expect("actual retained atomic tail");
        handle
            .truncate_partition(partition(), last.first_offset)
            .expect("ordinary native retention removes groups without shrinking map");
        for transaction in 5000..5063 {
            assert!(matches!(
                handle.inspect_atomic_group(partition(), transaction),
                Err(EngineError::UnknownAtomicGroup(_))
            ));
        }
        for fault in [Fault::None, Fault::Exact, Fault::OneLess] {
            let mut scope = scope(&handle);
            let trace = observe(&scope.work, "coordinator.atomic_groups", fault);
            let result = scope.inspect_group(DurableAtomicGroup {
                topic_partition: partition(),
                transaction_id: 5063,
            });
            assert!(
                trace.capacity("coordinator.atomic_groups") > 1,
                "one real group retained in a grown native map"
            );
            assert!(trace.phase("coordinator.states", ProbePhase::Paid));
            assert_eq!(
                trace.phase("coordinator.atomic_groups", ProbePhase::Paid),
                !matches!(fault, Fault::OneLess)
            );
            if matches!(fault, Fault::OneLess) {
                assert!(result.is_err());
            } else {
                assert_eq!(
                    result
                        .expect("actual retained tail inspection")
                        .transaction_id,
                    5063
                );
            }
        }
        contended(&root.0);
    }

    #[derive(Debug)]
    struct FileSink {
        file: Mutex<fs::File>,
        strict: AtomicUsize,
    }
    impl FileSink {
        fn write_ordinary(&self, bytes: &[u8]) -> EngineResult<()> {
            let mut file = self.file.lock().expect("isolated actual sink file");
            file.write_all(bytes)
                .map_err(shardlog::StorageError::from)?;
            file.sync_data().map_err(shardlog::StorageError::from)?;
            Ok(())
        }
        fn write_until(
            &self,
            bytes: &[u8],
            work: &NativeResumeWork,
        ) -> NativeResumeCallbackResult<()> {
            let values = size_of::<Result<(), io::Error>>() * 2;
            work.reserve_values(values, 0, 2, values + bytes.len())
                .map_err(NativeResumeCallbackError::refused)?;
            work.check().map_err(NativeResumeCallbackError::refused)?;
            let mut file = self.file.lock().expect("actual bounded fixture file");
            work.check().map_err(NativeResumeCallbackError::refused)?;
            let written = file.write_all(bytes);
            if let Err(error) = written {
                return Err(NativeResumeCallbackError::from_external(Box::new(error)));
            }
            work.check().map_err(NativeResumeCallbackError::refused)?;
            let synced = file.sync_data();
            if let Err(error) = synced {
                return Err(NativeResumeCallbackError::from_external(Box::new(error)));
            }
            // Caller retains the actual completed callback before post-E.
            self.strict.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }
    impl DurableAppendSink for FileSink {
        fn apply(
            &self,
            _: DurableSinkCheckpoint,
            appends: &[DurableAppend],
            _: DurableSinkCheckpoint,
        ) -> EngineResult<DurableSinkApply> {
            for append in appends {
                self.write_ordinary(&append.payload)?;
            }
            Ok(DurableSinkApply::Applied)
        }
        fn apply_until(
            &self,
            _: DurableSinkCheckpoint,
            appends: &[DurableAppend],
            _: DurableSinkCheckpoint,
            work: &NativeResumeWork,
        ) -> NativeResumeCallbackResult<DurableSinkApply> {
            for append in appends {
                self.write_until(&append.payload, work)?;
            }
            Ok(DurableSinkApply::Applied)
        }
    }
    #[derive(Debug)]
    struct FileFactory {
        sink: Arc<FileSink>,
        checkpoints: AtomicUsize,
        decisions: AtomicUsize,
    }
    impl FileFactory {
        fn new(path: &Path) -> Arc<Self> {
            Arc::new(Self {
                sink: Arc::new(FileSink {
                    file: Mutex::new(fs::File::create(path).expect("actual fixture sink")),
                    strict: AtomicUsize::new(0),
                }),
                checkpoints: AtomicUsize::new(0),
                decisions: AtomicUsize::new(0),
            })
        }
    }
    impl DurableAppendSinkFactory for FileFactory {
        fn validate_append(&self, _: &[u8], _: NonZeroU32) -> EngineResult<()> {
            Ok(())
        }
        fn load_checkpoint(
            &self,
            _: TopicPartition,
        ) -> EngineResult<Option<DurableSinkCheckpoint>> {
            Ok(None)
        }
        fn load_checkpoint_until(
            &self,
            _: TopicPartition,
            work: &NativeResumeWork,
        ) -> NativeResumeCallbackResult<Option<DurableSinkCheckpoint>> {
            work.reserve_values(
                size_of::<Option<DurableSinkCheckpoint>>(),
                0,
                1,
                size_of::<Option<DurableSinkCheckpoint>>(),
            )
            .map_err(NativeResumeCallbackError::refused)?;
            work.check().map_err(NativeResumeCallbackError::refused)?;
            self.checkpoints.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }
        fn open_shard(&self, _: ShardId) -> EngineResult<Arc<dyn DurableAppendSink>> {
            Ok(self.sink.clone())
        }
        // This isolated append-only fixture records staged bytes and the
        // physically synchronized decision marker; no SDK visibility claim.
        fn supports_committed_only(&self) -> bool {
            true
        }
        fn finalize_atomic_group(&self, _: DurableAtomicDecision) -> EngineResult<()> {
            self.sink.write_ordinary(b"actual native decision\n")
        }
        fn finalize_atomic_group_until(
            &self,
            _: DurableAtomicDecision,
            work: &NativeResumeWork,
        ) -> NativeResumeCallbackResult<()> {
            self.sink.write_until(b"actual bounded decision\n", work)?;
            self.decisions.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }
    fn with_sink(root: &Path, factory: Arc<FileFactory>) -> EngineHandle {
        recovered_with_sink(
            root,
            Arc::new(LegacyOnly::default()),
            Some(
                DurableSinkConfig::new(factory).with_options(DurableSinkOptions {
                    atomic_visibility: DurableSinkAtomicVisibility::CommittedOnly,
                    recovery_mode: DurableSinkRecoveryMode::Background,
                    worker_count: 1,
                    max_pending_items: 512,
                    max_pending_bytes: Some(2 << 20),
                    ..DurableSinkOptions::default()
                }),
            ),
        )
    }
    fn finalized_history_recovery_limits(
        config: &EngineConfig,
        sink: &DurableSinkConfig,
        journal_bytes: usize,
    ) -> (NativeRecoveryLimits, usize, usize) {
        // This positive fixture retains its 3 initial frames plus all 96 real
        // committed atomic frames. Bound only its complete setup suffix; the
        // shared native_limits and exact/one-less probe budgets stay unchanged.
        const EXTENTS: usize = 99;
        const FACTS: usize = 8; // One journal and seven actual primary packs.
        const EVENTS: usize = 256;
        const SOURCE_BYTES: usize = 1 << 20;
        let ordinary = native_limits();
        assert_eq!(config.shard_count, 1);
        assert!(journal_bytes <= SOURCE_BYTES);
        let sources = FACTS + EXTENTS;
        let visits = (FACTS + 1 + 1) * 4;
        // Source has exactly OwnedFileHandle and RecoverySourceFact fields.
        // Include both field alignments rather than assume a Rust repr layout.
        let source_width = size_of::<shard_stream_core::OwnedFileHandle>()
            + size_of::<crate::RecoverySourceFact>()
            + std::mem::align_of::<shard_stream_core::OwnedFileHandle>()
            + std::mem::align_of::<crate::RecoverySourceFact>();
        let sizing_bytes = 2 * size_of::<crate::worker::PrimaryStore>();
        let facts_bytes = FACTS * size_of::<crate::RecoverySourceFact>();
        let per_object = 4 * size_of::<shardlog::TierObject>() + 4 * ordinary.max_path_bytes + 1024;
        let native_tier = (sources + 1) * per_object
            + (EXTENTS + 1) * (4 * size_of::<shardlog::TierExtent>() + 1024);
        let worker = crate::worker::recovery_worker_bytes(config)
            .expect("actual ordinary worker typed cost");
        let fixed = worker
            + 4 * size_of::<crate::worker::ShardHandle>()
            + 16 * ordinary.max_path_bytes
            + journal_bytes * 64;
        let admission_bytes = fixed
            + native_tier * (sources + 8)
            + (sources + 1) * shardlog::LocalObjectTier::recovery_hash_buffer_bytes()
            + (EXTENTS + 1) * size_of::<shardlog::ExtentCatalogEntry>() * 8;
        let opening = crate::engine::owned::recovery_opening_framing_bytes_for_test();
        let journal_open = ordinary.max_path_bytes * 4 + 1024;
        let dispatcher =
            crate::durable_sink::recovery_dispatcher_bytes(sink, 1, config.queue_bytes_per_shard)
                .expect("same real sink configuration typed cost");
        let coordinator =
            crate::coordinator::recovery_coordinator_bytes(1, EXTENTS + EVENTS + 1, EXTENTS)
                .expect("actual complete coordinator queue/map/dense-slot upper cost");
        let suffix_bytes = sizing_bytes
            + facts_bytes
            + admission_bytes
            + opening
            + journal_open
            + dispatcher
            + coordinator;
        let wide = |bytes: usize| u64::try_from(bytes).expect("bounded typed work fits u64");
        let sizing_work = wide(visits * (size_of::<crate::worker::PrimaryStore>() + source_width));
        let admission_work = 16 * wide(SOURCE_BYTES)
            + 8 * wide(admission_bytes)
            + wide((EXTENTS + 1) * (sources + 1) * 256);
        let slot_sizing_work = (EXTENTS + 1)
            * (EXTENTS + EVENTS + 1)
            * (size_of::<shardlog::ExtentCatalogEntry>() + size_of::<shardlog::JournalEvent>());
        let suffix_work = sizing_work
            + admission_work
            + wide(opening)
            + wide(dispatcher)
            + 1024
            + wide(slot_sizing_work)
            + 8 * wide(coordinator);
        let suffix_entries = visits + FACTS + (EXTENTS + 1) * (sources + 8) + 2;
        let suffix_read = 8 * SOURCE_BYTES;
        let limits = NativeRecoveryLimits {
            max_files: ordinary
                .max_files
                .checked_add(8 * (sources + 1) + 2)
                .expect("bounded actual file-operation fee"),
            max_declared_bytes: ordinary
                .max_declared_bytes
                .checked_add(suffix_bytes)
                .expect("typed complete setup byte fee"),
            max_work_bytes: ordinary
                .max_work_bytes
                .checked_add(suffix_work)
                .expect("complete cumulative setup work"),
            ..ordinary
        };
        (limits, suffix_entries, suffix_read)
    }
    fn reopen_with_sink(root: &Path, factory: Arc<FileFactory>) -> EngineHandle {
        let config = config(root);
        let sink = DurableSinkConfig::new(factory).with_options(DurableSinkOptions {
            atomic_visibility: DurableSinkAtomicVisibility::CommittedOnly,
            recovery_mode: DurableSinkRecoveryMode::Background,
            worker_count: 1,
            max_pending_items: 512,
            max_pending_bytes: Some(2 << 20),
            ..DurableSinkOptions::default()
        });
        let expiry = Instant::now() + Duration::from_secs(60);
        let journal_bytes = usize::try_from(
            fs::metadata(config.data_dir.join("coordinator.ssj"))
                .expect("actual finalized fixture journal before sizing")
                .len(),
        )
        .expect("actual fixture journal length fits usize");
        let (recovery, suffix_entries, suffix_read) =
            finalized_history_recovery_limits(&config, &sink, journal_bytes);
        let owner =
            DirectoryOwnership::acquire(&config.data_dir).expect("actual released source root");
        let census = NativeRecoveryCensus::read_for_engine(config, owner, None, recovery, expiry)
            .expect("actual existing committed history census");
        assert_eq!(census.extents().len(), 99);
        assert_eq!(census.sources().count(), 8);
        assert_eq!(
            census
                .sources()
                .filter(|s| s.kind == crate::RecoverySourceKind::PrimaryPack)
                .count(),
            7
        );
        assert!(census.sources().map(|s| s.bytes).sum::<u64>() <= 1 << 20);
        assert_eq!(
            census
                .sources()
                .find(|s| s.kind == crate::RecoverySourceKind::Coordinator)
                .expect("same actual journal")
                .bytes,
            u64::try_from(journal_bytes).expect("bounded journal bytes")
        );
        assert!(census.journal().len() <= 256);
        let batch_ids = census
            .extents()
            .iter()
            .map(|e| e.native.reservation.batch_id.get())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            batch_ids,
            (0..99).collect(),
            "actual contiguous completed batch identities bound recovery slots"
        );
        assert!(census.journal().iter().all(|event| !matches!(
            event,
            shardlog::JournalEvent::Truncate { .. }
                | shardlog::JournalEvent::TruncateCheckpoint { .. }
        )));
        assert_eq!(census.absolute_expiry(), expiry);
        let storage = census
            .restore_storage()
            .expect("actual same-source storage");
        let prefix = storage.usage();
        let ordinary = native_limits();
        assert!(prefix.files <= ordinary.max_files);
        assert!(prefix.declared_bytes <= ordinary.max_declared_bytes);
        assert!(prefix.work_bytes <= ordinary.max_work_bytes);
        assert!(
            prefix
                .entries
                .checked_add(suffix_entries)
                .expect("bounded entries")
                <= ordinary.max_entries
        );
        assert!(
            prefix
                .read_bytes
                .checked_add(u64::try_from(suffix_read).expect("bounded reads"))
                .expect("bounded cumulative reads")
                <= ordinary.max_read_bytes
        );
        assert_eq!(storage.absolute_expiry(), expiry);
        let recovered = StreamEngine::open_recovered_storage(
            storage,
            None,
            Some(Arc::new(LegacyOnly::default())),
            None,
            Some(sink),
        )
        .expect("genuine committed-source recovered workers");
        assert_eq!(recovered.absolute_expiry(), expiry);
        EngineHandle::from_recovered(Arc::new(recovered))
    }

    #[test]
    fn actual_checkpoint_cache_growth_recomputes_capacity_and_each_probe_can_refuse() {
        let root = Temporary::new();
        let factory = FileFactory::new(&root.0.join("actual-effects"));
        let handle = with_sink(&root.0, factory);
        let dispatcher = handle
            .durable_sink
            .as_ref()
            .expect("actual native dispatcher");
        for id in 2000..2032 {
            dispatcher
                .checkpoint(add_topic(&handle, id))
                .expect("ordinary native cache population");
        }
        let mut growth = false;
        for id in 2032..2096 {
            let partition = add_topic(&handle, id);
            let mut scope = scope(&handle);
            let trace = observe(&scope.work, "", Fault::None);
            scope
                .sink_checkpoint(partition)
                .expect("genuine provider and publication fit");
            growth |=
                trace.capacity("sink.partitions.entry") > trace.capacity("sink.partitions.get");
            growth |=
                trace.capacity("sink.checkpoints.insert") > trace.capacity("sink.checkpoints.get");
        }
        assert!(growth, "actual native try_reserve grew a retained table");
        for site in [
            "sink.partitions.get",
            "sink.partitions.entry",
            "sink.checkpoints.get",
            "sink.checkpoints.insert",
        ] {
            for fault in [Fault::None, Fault::Exact, Fault::OneLess] {
                let mut scope = scope(&handle);
                let trace = observe(&scope.work, site, fault);
                let result = scope.sink_checkpoint(TopicPartition::new(
                    TopicId::new(2000),
                    LogicalPartitionId::new(0),
                ));
                assert!(trace.capacity(site) > 1);
                assert_eq!(
                    trace.phase(site, ProbePhase::Paid),
                    !matches!(fault, Fault::OneLess)
                );
                if matches!(fault, Fault::None) {
                    assert!(result.is_ok(), "all genuine cache probes fit: {result:?}");
                }
                if matches!(fault, Fault::OneLess) {
                    assert!(result.is_err());
                    assert!(scope.outcomes.iter().any(|outcome| matches!(outcome,
                        NativeResumeOutcome::Sink { observed, .. } if observed.checkpoint() == Some(None) && observed.progress().is_some_and(|progress| progress.result().is_err())
                    )));
                    assert!(matches!(
                        scope.work.check(),
                        Err(NativeResumeRefusal::Denied)
                    ));
                }
                // Exact is probe-entry coverage only: a later independent fee
                // may refuse. It is not a whole-command resource positive.
            }
        }
    }

    #[test]
    fn actual_finalized_group_contains_and_post_growth_insert_pay_separate_fees() {
        let root = Temporary::new();
        let factory = FileFactory::new(&root.0.join("actual-decisions"));
        let writer = recovered(&root.0, Arc::new(LegacyOnly::default()));
        for transaction in 6000..6096 {
            atomic(&writer, transaction);
        }
        drop(writer);
        let handle = reopen_with_sink(&root.0, factory.clone());
        for transaction in 6000..6032 {
            let mut scope = scope(&handle);
            scope
                .sink_decision(DurableAtomicDecision {
                    group: DurableAtomicGroup {
                        topic_partition: partition(),
                        transaction_id: transaction,
                    },
                    outcome: AtomicGroupState::Committed,
                })
                .expect("genuine source decisions populate actual shared cache");
        }
        let mut growth = false;
        for transaction in 6032..6096 {
            assert_eq!(
                handle
                    .inspect_atomic_group(partition(), transaction)
                    .expect("actual retained source decision")
                    .state,
                AtomicGroupState::Committed
            );
            let decision = DurableAtomicDecision {
                group: DurableAtomicGroup {
                    topic_partition: partition(),
                    transaction_id: transaction,
                },
                outcome: AtomicGroupState::Committed,
            };
            let mut scope = scope(&handle);
            let trace = observe(&scope.work, "", Fault::None);
            scope
                .sink_decision(decision)
                .expect("actual native decision callback fits");
            growth |= trace.capacity("sink.groups.insert") > trace.capacity("sink.groups.contains");
        }
        assert!(
            growth,
            "actual new native decision grew the shared set before insert"
        );
        for site in ["sink.groups.contains", "sink.groups.insert"] {
            for fault in [Fault::None, Fault::Exact, Fault::OneLess] {
                let mut scope = scope(&handle);
                let trace = observe(&scope.work, site, fault);
                let result = scope.sink_decision(DurableAtomicDecision {
                    group: DurableAtomicGroup {
                        topic_partition: partition(),
                        transaction_id: 6000,
                    },
                    outcome: AtomicGroupState::Committed,
                });
                assert!(trace.capacity(site) > 1);
                assert_eq!(
                    trace.phase(site, ProbePhase::Paid),
                    !matches!(fault, Fault::OneLess)
                );
                if matches!(fault, Fault::None) {
                    assert!(
                        result.is_ok(),
                        "actual decision cache probe fees fit: {result:?}"
                    );
                }
                if matches!(fault, Fault::OneLess) {
                    assert!(result.is_err());
                    assert!(scope.outcomes.iter().any(|outcome| matches!(outcome, NativeResumeOutcome::Sink { observed, .. } if observed.finalized() && observed.callback_entered() && observed.progress().is_some_and(|progress| progress.result().is_err()))));
                }
            }
        }
        assert!(factory.decisions.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn actual_apply_effect_survives_later_cache_refusal_while_unpaid_sink_probe_never_enters() {
        for (site, fault) in [
            ("sink.sinks.get", Fault::OneLess),
            ("sink.checkpoints.insert", Fault::OneLess),
            ("", Fault::None),
        ] {
            let root = Temporary::new();
            let path = root.0.join("actual-applied-bytes");
            let factory = FileFactory::new(&path);
            let handle = with_sink(&root.0, factory.clone());
            let weak = handle.downgrade();
            let batch = handle
                .fetch_stored_internal(fetch(), false)
                .expect("genuine stored native input")
                .remove(0);
            let append = durable_append_from_stored(&handle.config, batch);
            let payload = append.payload.clone();
            let expected = DurableSinkCheckpoint::initial(partition());
            let next = expected
                .after(append.reservation)
                .expect("actual native next checkpoint");
            let mut scope = scope(&handle);
            let trace = observe(&scope.work, site, fault);
            let result = scope.sink_append(expected, append, next);
            if site == "sink.sinks.get" {
                assert!(result.is_err());
                assert!(!trace.phase(site, ProbePhase::Paid));
                assert_eq!(factory.sink.strict.load(Ordering::Relaxed), 0);
                assert!(fs::read(&path).expect("actual unchanged sink").is_empty());
            } else {
                assert_eq!(
                    fs::read(&path)
                        .expect("actual synchronized callback effect")
                        .as_slice(),
                    payload.as_ref()
                );
                assert_eq!(factory.sink.strict.load(Ordering::Relaxed), 1);
                assert!(scope.outcomes.iter().any(|outcome| matches!(outcome, NativeResumeOutcome::Sink { observed, .. } if observed.apply_result() == Some(DurableSinkApply::Applied))));
                if site.is_empty() {
                    assert!(
                        result.is_ok(),
                        "all actual native apply/cache phases fit: {result:?}"
                    );
                } else {
                    assert!(result.is_err());
                    assert!(scope.outcomes.iter().any(|outcome| matches!(outcome, NativeResumeOutcome::Sink { observed, .. } if observed.apply_result() == Some(DurableSinkApply::Applied) && observed.progress().is_some_and(|progress| progress.result().is_err()))));
                }
            }
            let retained = match result {
                Ok(()) => scope.finish(),
                Err(failure) => Err(scope.fail(failure)),
            };
            assert_eq!(retained.is_ok(), site.is_empty());
            drop(handle);
            match &retained {
                Ok(report) => {
                    assert!(weak.upgrade().is_none());
                    let owner = DirectoryOwnership::acquire(&root.0.join("data"))
                        .expect("plain completed report releases actual engine root");
                    assert!(report.outcomes().iter().any(|outcome| matches!(outcome, NativeResumeOutcome::Sink { observed, .. } if observed.apply_result() == Some(DurableSinkApply::Applied))));
                    drop(owner);
                }
                Err(_error) => {
                    contended(&root.0);
                    assert!(weak.upgrade().is_some());
                }
            }
            drop(retained);
            assert!(weak.upgrade().is_none());
            let owner = DirectoryOwnership::acquire(&root.0.join("data"))
                .expect("dropping actual retained refusal releases engine root");
            drop(owner);
        }
    }

    #[test]
    fn actual_same_work_probe_fees_accumulate_and_elapsed_original_e_stops_entry() {
        let root = Temporary::new();
        let handle = recovered(&root.0, Arc::new(LegacyOnly::default()));
        let mut actual = scope(&handle);
        let trace = observe(&actual.work, "", Fault::None);
        actual.watermarks(partition()).expect("first real command");
        let first = actual.work.usage().expect("first actual cumulative fees");
        actual.watermarks(partition()).expect("second real command");
        assert!(actual.work.usage().expect("second actual fees").work_bytes > first.work_bytes);
        assert!(trace.phase("coordinator.states", ProbePhase::Paid));
        let expiry = Instant::now() + Duration::from_secs(1);
        let mut late = start(&handle, limits(), expiry).expect("actual original E");
        let trace = observe(&late.work, "coordinator.states", Fault::Expire);
        assert!(late.watermarks(partition()).is_err());
        assert!(!trace.phase("coordinator.states", ProbePhase::Paid));
        assert_eq!(late.work.absolute_expiry(), expiry);
        assert!(trace.phase("coordinator.states", ProbePhase::Refused));
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[path = "native_resume_sink_tests.rs"]
mod strict_sink;

#[cfg(any(target_os = "linux", target_os = "android"))]
#[path = "native_resume_directory_tests.rs"]
mod directory_fetch;
