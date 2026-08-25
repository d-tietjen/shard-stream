//! Listener-free embedded lifecycle for shard-stream.
//!
//! [`EmbeddedRuntime`] owns engine recovery, readiness, write admission,
//! draining, synchronization, leadership-transfer preparation, and shutdown.
//! It deliberately starts no REST, Kafka, gRPC, or HTTP/3 listeners.

mod health;

use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use shard_stream_core::{AssignmentProvider, BrokerId, TopicPartition, WriteFence};
use shard_stream_engine::{
    DurableSinkConfig, DurableSinkStats, EngineConfig, EngineError, FetchedBatch,
    PartitionWatermarks, ReplicationStats, ReplicationTransport, RetentionGuard, StreamEngine,
    TopicConfig,
};
use shard_stream_protocol::{AppendRequest, AppendResponse, FetchRequest, ReplicaAppend};

const IDENTITY_MAGIC: &[u8; 8] = b"SSNODE01";
const IDENTITY_VERSION: u16 = 1;
const IDENTITY_BODY_BYTES: usize = 8 + 2 + 4 + 8;
const IDENTITY_BYTES: usize = IDENTITY_BODY_BYTES + 32;
pub use health::{
    FilesystemHealthProbe, HealthObservation, HealthPolicy, HealthProbe, HealthProbeSnapshot,
    HealthSnapshot, PartitionHealth, PartitionHealthSnapshot, ReadinessBlocker,
};

/// Dedicated execution and admission budgets for an embedded engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeBudgets {
    /// Physical append-owner threads.
    pub worker_threads: u32,
    /// Dedicated fsync/I/O threads. The current engine owns one per worker.
    pub io_threads: u32,
    pub queue_slots_per_worker: usize,
    pub queue_bytes_per_worker: usize,
}

impl RuntimeBudgets {
    fn apply(self, config: &mut EngineConfig) -> RuntimeResult<()> {
        if self.worker_threads == 0
            || self.io_threads == 0
            || self.queue_slots_per_worker == 0
            || self.queue_bytes_per_worker == 0
        {
            return Err(RuntimeError::InvalidConfig(
                "runtime worker, I/O, slot, and byte budgets must be nonzero".into(),
            ));
        }
        if self.io_threads != self.worker_threads {
            return Err(RuntimeError::InvalidConfig(
                "the current engine requires one dedicated I/O thread per worker".into(),
            ));
        }
        config.shard_count = self.worker_threads;
        config.queue_slots_per_shard = self.queue_slots_per_worker;
        config.queue_bytes_per_shard = self.queue_bytes_per_worker;
        Ok(())
    }
}

/// Storage and engine configuration supplied to [`EmbeddedRuntimeBuilder`].
#[derive(Debug, Clone)]
pub struct EmbeddedStorage {
    pub engine: EngineConfig,
}

impl EmbeddedStorage {
    #[must_use]
    pub const fn new(engine: EngineConfig) -> Self {
        Self { engine }
    }
}

impl From<EngineConfig> for EmbeddedStorage {
    fn from(engine: EngineConfig) -> Self {
        Self::new(engine)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum LifecycleState {
    Recovering = 0,
    CatchingUp = 1,
    Ready = 2,
    Draining = 3,
    Fenced = 4,
    Failed = 5,
    Stopped = 6,
}

impl LifecycleState {
    fn from_raw(raw: u8) -> Self {
        match raw {
            0 => Self::Recovering,
            1 => Self::CatchingUp,
            2 => Self::Ready,
            3 => Self::Draining,
            4 => Self::Fenced,
            5 => Self::Failed,
            _ => Self::Stopped,
        }
    }
}

/// Stable local broker identity. `incarnation` increases after every
/// successful runtime open using the same data directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeIdentity {
    pub broker_id: BrokerId,
    pub incarnation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeadershipTransferReceipt {
    pub identity: NodeIdentity,
    pub prepared_at: Instant,
}

#[derive(Debug, Clone)]
pub struct EmbeddedRuntimeOptions {
    pub shutdown_timeout: Duration,
    pub catch_up_timeout: Duration,
    pub require_replication_caught_up: bool,
}

impl Default for EmbeddedRuntimeOptions {
    fn default() -> Self {
        Self {
            shutdown_timeout: Duration::from_secs(30),
            catch_up_timeout: Duration::from_secs(30),
            require_replication_caught_up: true,
        }
    }
}

#[derive(Default)]
pub struct EmbeddedRuntimeBuilder {
    storage: Option<EmbeddedStorage>,
    replication: Option<Arc<dyn ReplicationTransport>>,
    durable_sink: Option<DurableSinkConfig>,
    write_fence: Option<Arc<dyn WriteFence>>,
    retention_guard: Option<Arc<dyn RetentionGuard>>,
    assignments: Option<Arc<dyn AssignmentProvider>>,
    broker_id: Option<BrokerId>,
    budgets: Option<RuntimeBudgets>,
    options: EmbeddedRuntimeOptions,
    health_probe: Option<Arc<dyn HealthProbe>>,
    health_policy: HealthPolicy,
}

impl fmt::Debug for EmbeddedRuntimeBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EmbeddedRuntimeBuilder")
            .field("storage", &self.storage)
            .field("replication", &self.replication.is_some())
            .field("durable_sink", &self.durable_sink.is_some())
            .field("write_fence", &self.write_fence.is_some())
            .field("retention_guard", &self.retention_guard.is_some())
            .field("assignments", &self.assignments.is_some())
            .field("broker_id", &self.broker_id)
            .field("budgets", &self.budgets)
            .field("options", &self.options)
            .field("health_probe", &self.health_probe.is_some())
            .field("health_policy", &self.health_policy)
            .finish()
    }
}

impl EmbeddedRuntimeBuilder {
    #[must_use]
    pub fn storage(mut self, storage: impl Into<EmbeddedStorage>) -> Self {
        self.storage = Some(storage.into());
        self
    }

    #[must_use]
    pub fn replication(mut self, replication: Arc<dyn ReplicationTransport>) -> Self {
        self.replication = Some(replication);
        self
    }

    #[must_use]
    pub fn durable_sink(mut self, durable_sink: DurableSinkConfig) -> Self {
        self.durable_sink = Some(durable_sink);
        self
    }

    #[must_use]
    pub fn write_fence(mut self, write_fence: Arc<dyn WriteFence>) -> Self {
        self.write_fence = Some(write_fence);
        self
    }

    #[must_use]
    pub fn retention_guard(mut self, retention_guard: Arc<dyn RetentionGuard>) -> Self {
        self.retention_guard = Some(retention_guard);
        self
    }

    #[must_use]
    pub fn assignment_provider(mut self, assignments: Arc<dyn AssignmentProvider>) -> Self {
        self.assignments = Some(assignments);
        self
    }

    #[must_use]
    pub const fn broker_id(mut self, broker_id: BrokerId) -> Self {
        self.broker_id = Some(broker_id);
        self
    }

    #[must_use]
    pub const fn budgets(mut self, budgets: RuntimeBudgets) -> Self {
        self.budgets = Some(budgets);
        self
    }

    #[must_use]
    pub fn options(mut self, options: EmbeddedRuntimeOptions) -> Self {
        self.options = options;
        self
    }

    /// Replaces the default filesystem probe with a composable host probe.
    #[must_use]
    pub fn health_probe(mut self, health_probe: Arc<dyn HealthProbe>) -> Self {
        self.health_probe = Some(health_probe);
        self
    }

    #[must_use]
    pub const fn health_policy(mut self, health_policy: HealthPolicy) -> Self {
        self.health_policy = health_policy;
        self
    }

    pub fn open(self) -> RuntimeResult<EmbeddedRuntime> {
        validate_options(&self.options)?;
        let mut storage = self
            .storage
            .ok_or_else(|| RuntimeError::InvalidConfig("embedded storage is required".into()))?;
        if let Some(budgets) = self.budgets {
            budgets.apply(&mut storage.engine)?;
        }
        storage.engine.validate().map_err(RuntimeError::Engine)?;

        let broker_id = match (self.broker_id, self.assignments.as_ref()) {
            (Some(explicit), Some(assignments)) if explicit != assignments.local_broker_id() => {
                return Err(RuntimeError::InvalidConfig(
                    "explicit broker ID differs from assignment provider".into(),
                ));
            }
            (Some(explicit), _) => explicit,
            (None, Some(assignments)) => assignments.local_broker_id(),
            (None, None) => {
                return Err(RuntimeError::InvalidConfig(
                    "broker_id or assignment_provider is required".into(),
                ));
            }
        };

        fs::create_dir_all(&storage.engine.data_dir)?;
        let identity = open_node_identity(&storage.engine.data_dir, broker_id)?;
        let data_dir = storage.engine.data_dir.clone();
        let runtime_write_fence = self.write_fence.clone();
        let engine = StreamEngine::open_with_all_components_and_durable_sink(
            storage.engine,
            self.write_fence,
            self.replication,
            self.retention_guard,
            self.durable_sink,
        )?;
        let health_probe = self
            .health_probe
            .unwrap_or_else(|| Arc::new(FilesystemHealthProbe::new(data_dir)));
        let mut health_policy = self.health_policy;
        health_policy.require_replication_caught_up = self.options.require_replication_caught_up;
        Ok(EmbeddedRuntime {
            engine: Some(engine),
            identity,
            assignments: self.assignments,
            lifecycle: AtomicU8::new(LifecycleState::Recovering as u8),
            options: self.options,
            health_probe,
            health_policy,
            recovery_replication_caught_up: AtomicBool::new(false),
            corruption_detected: AtomicBool::new(false),
            write_fence: runtime_write_fence,
        })
    }
}

pub struct EmbeddedRuntime {
    engine: Option<StreamEngine>,
    identity: NodeIdentity,
    assignments: Option<Arc<dyn AssignmentProvider>>,
    lifecycle: AtomicU8,
    options: EmbeddedRuntimeOptions,
    health_probe: Arc<dyn HealthProbe>,
    health_policy: HealthPolicy,
    recovery_replication_caught_up: AtomicBool,
    corruption_detected: AtomicBool,
    write_fence: Option<Arc<dyn WriteFence>>,
}

impl fmt::Debug for EmbeddedRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EmbeddedRuntime")
            .field("identity", &self.identity)
            .field("lifecycle", &self.lifecycle())
            .field("assignments", &self.assignments.is_some())
            .field("health_policy", &self.health_policy)
            .field("write_fence", &self.write_fence.is_some())
            .finish_non_exhaustive()
    }
}

impl EmbeddedRuntime {
    #[must_use]
    pub fn builder() -> EmbeddedRuntimeBuilder {
        EmbeddedRuntimeBuilder::default()
    }

    #[must_use]
    pub const fn identity(&self) -> NodeIdentity {
        self.identity
    }

    #[must_use]
    pub fn lifecycle(&self) -> LifecycleState {
        LifecycleState::from_raw(self.lifecycle.load(Ordering::Acquire))
    }

    /// Completes local WAL recovery and schedules follower reconciliation for
    /// locally led recovered partitions.
    pub fn recover(&self) -> RuntimeResult<()> {
        self.require_state(LifecycleState::Recovering)?;
        let engine = self.engine()?;
        let mut recovered = engine.recovered_partitions();
        if let Some(assignments) = &self.assignments {
            recovered.retain(|partition| assignments.is_local_leader(*partition));
        }
        if let Err(error) = engine.resume_durable_sink(&recovered) {
            self.corruption_detected
                .store(error.is_corruption(), Ordering::Release);
            self.lifecycle
                .store(LifecycleState::Failed as u8, Ordering::Release);
            return Err(error.into());
        }
        if let Err(error) = engine.resume_recovered_replication(&recovered) {
            self.corruption_detected
                .store(error.is_corruption(), Ordering::Release);
            self.lifecycle
                .store(LifecycleState::Failed as u8, Ordering::Release);
            return Err(error.into());
        }
        self.lifecycle
            .store(LifecycleState::CatchingUp as u8, Ordering::Release);
        Ok(())
    }

    /// Waits for configured catch-up requirements and enables producer writes.
    pub fn ready(&self) -> RuntimeResult<()> {
        self.require_state(LifecycleState::CatchingUp)?;
        if let Some(assignments) = &self.assignments
            && assignments.local_broker_id() != self.identity.broker_id
        {
            self.lifecycle
                .store(LifecycleState::Fenced as u8, Ordering::Release);
            return Err(RuntimeError::Fenced(
                "assignment provider local broker changed".into(),
            ));
        }
        if self.options.require_replication_caught_up {
            self.wait_for_replication(Instant::now() + self.options.catch_up_timeout)?;
        }
        self.recovery_replication_caught_up
            .store(true, Ordering::Release);
        let snapshot = self.health_snapshot();
        if let Some(blocker) = snapshot
            .blockers
            .iter()
            .find(|blocker| !matches!(blocker, ReadinessBlocker::Lifecycle { .. }))
        {
            if snapshot.corruption_detected {
                self.lifecycle
                    .store(LifecycleState::Failed as u8, Ordering::Release);
            }
            return Err(RuntimeError::NotReady(format!("{blocker:?}")));
        }
        self.lifecycle
            .store(LifecycleState::Ready as u8, Ordering::Release);
        Ok(())
    }

    pub fn create_topic(&self, topic: TopicConfig) -> RuntimeResult<()> {
        self.require_state(LifecycleState::Ready)?;
        let result = self.engine()?.create_topic(topic);
        self.handle_engine_result(result)
    }

    pub fn append(&self, request: AppendRequest) -> RuntimeResult<AppendResponse> {
        self.require_state(LifecycleState::Ready)?;
        let result = self.engine()?.append(request);
        self.handle_engine_result(result)
    }

    pub fn append_replica(&self, batch: ReplicaAppend) -> RuntimeResult<AppendResponse> {
        match self.lifecycle() {
            LifecycleState::CatchingUp | LifecycleState::Ready | LifecycleState::Draining => {
                let result = self.engine()?.append_replica(batch);
                self.handle_engine_result(result)
            }
            state => Err(RuntimeError::InvalidLifecycle {
                expected: "CatchingUp, Ready, or Draining",
                actual: state,
            }),
        }
    }

    pub fn fetch(&self, request: FetchRequest) -> RuntimeResult<Vec<FetchedBatch>> {
        match self.lifecycle() {
            LifecycleState::Ready | LifecycleState::Draining => {
                let result = self.engine()?.fetch(request);
                self.handle_engine_result(result)
            }
            state => Err(RuntimeError::InvalidLifecycle {
                expected: "Ready or Draining",
                actual: state,
            }),
        }
    }

    pub fn watermarks(
        &self,
        topic_partition: TopicPartition,
    ) -> RuntimeResult<PartitionWatermarks> {
        let result = self.engine()?.watermarks(topic_partition);
        self.handle_engine_result(result)
    }

    #[must_use]
    pub fn replication_stats(&self) -> ReplicationStats {
        self.engine
            .as_ref()
            .map_or_else(ReplicationStats::default, StreamEngine::replication_stats)
    }

    #[must_use]
    pub fn durable_sink_stats(&self) -> DurableSinkStats {
        self.engine
            .as_ref()
            .map_or_else(DurableSinkStats::default, StreamEngine::durable_sink_stats)
    }

    /// Returns a typed, conservative readiness snapshot without mutating the
    /// data path or acquiring a global engine lock.
    #[must_use]
    pub fn health_snapshot(&self) -> HealthSnapshot {
        let replication = self.replication_stats();
        let assignment_epoch = self
            .assignments
            .as_ref()
            .map_or(0, |assignments| assignments.assignment_epoch().get());
        let quorum_available = true;
        let mut assignment_current = true;
        if self
            .assignments
            .as_ref()
            .is_some_and(|assignments| assignments.local_broker_id() != self.identity.broker_id)
        {
            assignment_current = false;
        }

        let mut partitions = Vec::new();
        let mut health_error = None;
        if let Some(engine) = &self.engine {
            for topic_partition in engine.all_partitions() {
                let assignment = self
                    .assignments
                    .as_ref()
                    .and_then(|assignments| assignments.assignment(topic_partition));
                let hosted = assignment.as_ref().is_none_or(|assignment| {
                    assignment.replicas.contains(&self.identity.broker_id)
                });
                if !hosted {
                    continue;
                }
                let watermarks = match engine.watermarks(topic_partition) {
                    Ok(watermarks) => watermarks,
                    Err(error) => {
                        if error.is_corruption() {
                            self.corruption_detected.store(true, Ordering::Release);
                        }
                        health_error = Some(error.to_string());
                        continue;
                    }
                };
                let epochs = match engine.partition_epochs(topic_partition) {
                    Ok(epochs) => epochs,
                    Err(error) => {
                        if error.is_corruption() {
                            self.corruption_detected.store(true, Ordering::Release);
                        }
                        health_error = Some(error.to_string());
                        continue;
                    }
                };
                let local_leader = assignment
                    .as_ref()
                    .is_none_or(|assignment| assignment.leader == self.identity.broker_id);
                let in_sync_replicas = vec![self.identity.broker_id.get()];
                let minimum_in_sync_replicas = assignment
                    .as_ref()
                    .map_or(1, |assignment| assignment.min_in_sync_replicas);
                let partition_assignment_current =
                    assignment.is_some() || self.assignments.is_none();
                if !partition_assignment_current {
                    assignment_current = false;
                }
                let leader_epoch = assignment
                    .as_ref()
                    .map_or(epochs.leader_epoch.get(), |assignment| {
                        assignment.leader_epoch.get()
                    });
                let writable = local_leader
                    && in_sync_replicas.len() >= minimum_in_sync_replicas as usize
                    && self.write_fence.as_ref().is_none_or(|fence| {
                        fence.check_write(topic_partition, leader_epoch).is_ok()
                    });
                partitions.push(PartitionHealth {
                    topic_id: topic_partition.topic_id.get(),
                    partition_id: topic_partition.partition_id.get(),
                    local_leader,
                    assignment_current: partition_assignment_current,
                    leader_epoch,
                    in_sync_replicas,
                    minimum_in_sync_replicas,
                    writable,
                    visible_end: watermarks.last_stable_offset.get(),
                    replicated_end: watermarks.replicated_high_watermark.get(),
                });
            }
        }
        let probe = match (self.health_probe.snapshot(), health_error) {
            (Ok(probe), None) => Ok(probe),
            (Ok(_), Some(error)) | (Err(error), _) => Err(error),
        };
        HealthSnapshot::evaluate(
            HealthObservation {
                lifecycle: self.lifecycle(),
                quorum_available,
                assignment_current,
                assignment_epoch,
                recovery_replication_caught_up: self
                    .recovery_replication_caught_up
                    .load(Ordering::Acquire),
                replication_backlog_bytes: replication.pending_bytes,
                partitions,
                probe,
                runtime_corruption_detected: self.corruption_detected.load(Ordering::Acquire),
            },
            self.health_policy,
        )
    }

    pub fn sync(&self) -> RuntimeResult<()> {
        let result = self.engine()?.sync();
        self.handle_engine_result(result)
    }

    /// Stops new producer admission, drains accepted replication, and syncs all
    /// local WAL and coordinator state before `deadline`.
    pub fn drain(&self, deadline: Instant) -> RuntimeResult<()> {
        loop {
            let current = self.lifecycle();
            match current {
                LifecycleState::Ready => {
                    if self
                        .lifecycle
                        .compare_exchange(
                            LifecycleState::Ready as u8,
                            LifecycleState::Draining as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                LifecycleState::Draining => break,
                LifecycleState::Recovering | LifecycleState::CatchingUp => {
                    self.lifecycle
                        .store(LifecycleState::Draining as u8, Ordering::Release);
                    break;
                }
                state => {
                    return Err(RuntimeError::InvalidLifecycle {
                        expected: "Recovering, CatchingUp, Ready, or Draining",
                        actual: state,
                    });
                }
            }
        }
        self.wait_for_replication(deadline)?;
        if Instant::now() >= deadline {
            return Err(RuntimeError::DeadlineExceeded("runtime drain"));
        }
        self.sync()
    }

    /// Produces a durable handoff point. The caller can publish the next
    /// leader epoch only after this method succeeds.
    pub fn prepare_leadership_transfer(
        &self,
        deadline: Instant,
    ) -> RuntimeResult<LeadershipTransferReceipt> {
        self.drain(deadline)?;
        Ok(LeadershipTransferReceipt {
            identity: self.identity,
            prepared_at: Instant::now(),
        })
    }

    /// Drains and shuts down using the configured bounded timeout.
    pub fn shutdown(self) -> RuntimeResult<()> {
        let deadline = Instant::now() + self.options.shutdown_timeout;
        self.shutdown_by(deadline)
    }

    /// Drains, synchronizes, and transfers engine destruction to a bounded
    /// join. If a platform I/O call cannot be cancelled, the destructor
    /// continues safely in the background and this method returns a timeout.
    pub fn shutdown_by(mut self, deadline: Instant) -> RuntimeResult<()> {
        let mut first_error = self.drain(deadline).err();
        self.lifecycle
            .store(LifecycleState::Stopped as u8, Ordering::Release);
        if let Some(engine) = self.engine.take() {
            let (done_tx, done_rx) = mpsc::sync_channel(1);
            thread::Builder::new()
                .name("shard-stream-runtime-shutdown".into())
                .spawn(move || {
                    drop(engine);
                    let _ = done_tx.send(());
                })
                .map_err(|error| {
                    RuntimeError::Shutdown(format!("failed to spawn shutdown owner: {error}"))
                })?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if done_rx.recv_timeout(remaining).is_err() && first_error.is_none() {
                first_error = Some(RuntimeError::DeadlineExceeded("runtime shutdown"));
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn wait_for_replication(&self, deadline: Instant) -> RuntimeResult<()> {
        loop {
            if self.replication_stats().pending_jobs == 0 {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(RuntimeError::DeadlineExceeded("replication catch-up"));
            }
            thread::sleep(
                deadline
                    .saturating_duration_since(now)
                    .min(Duration::from_millis(2)),
            );
        }
    }

    fn require_state(&self, expected: LifecycleState) -> RuntimeResult<()> {
        let actual = self.lifecycle();
        if actual == expected {
            Ok(())
        } else {
            Err(RuntimeError::InvalidLifecycle {
                expected: lifecycle_name(expected),
                actual,
            })
        }
    }

    fn handle_engine_result<T>(&self, result: Result<T, EngineError>) -> RuntimeResult<T> {
        if let Err(error) = &result
            && error.is_corruption()
        {
            self.corruption_detected.store(true, Ordering::Release);
            self.lifecycle
                .store(LifecycleState::Failed as u8, Ordering::Release);
        }
        result.map_err(Into::into)
    }

    fn engine(&self) -> RuntimeResult<&StreamEngine> {
        self.engine.as_ref().ok_or(RuntimeError::InvalidLifecycle {
            expected: "running",
            actual: LifecycleState::Stopped,
        })
    }
}

fn validate_options(options: &EmbeddedRuntimeOptions) -> RuntimeResult<()> {
    if options.shutdown_timeout.is_zero() || options.catch_up_timeout.is_zero() {
        return Err(RuntimeError::InvalidConfig(
            "runtime shutdown and catch-up timeouts must be nonzero".into(),
        ));
    }
    Ok(())
}

const fn lifecycle_name(state: LifecycleState) -> &'static str {
    match state {
        LifecycleState::Recovering => "Recovering",
        LifecycleState::CatchingUp => "CatchingUp",
        LifecycleState::Ready => "Ready",
        LifecycleState::Draining => "Draining",
        LifecycleState::Fenced => "Fenced",
        LifecycleState::Failed => "Failed",
        LifecycleState::Stopped => "Stopped",
    }
}

/// Opens and crash-safely advances the stable broker incarnation stored in a
/// data directory. Listener-based hosts use the same identity contract as the
/// embedded runtime.
pub fn open_node_identity(data_dir: &Path, broker_id: BrokerId) -> RuntimeResult<NodeIdentity> {
    let path = data_dir.join("node.identity");
    let incarnation = if path.exists() {
        let previous = read_node_identity(&path)?;
        if previous.broker_id != broker_id {
            return Err(RuntimeError::Identity(format!(
                "data directory belongs to broker {}, not {}",
                previous.broker_id.get(),
                broker_id.get()
            )));
        }
        previous
            .incarnation
            .checked_add(1)
            .ok_or_else(|| RuntimeError::Identity("node incarnation exhausted".into()))?
    } else {
        1
    };
    let identity = NodeIdentity {
        broker_id,
        incarnation,
    };
    write_node_identity(&path, identity)?;
    Ok(identity)
}

fn read_node_identity(path: &Path) -> RuntimeResult<NodeIdentity> {
    let mut file = File::open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    if bytes.len() != IDENTITY_BYTES
        || bytes.get(..8) != Some(IDENTITY_MAGIC)
        || u16::from_le_bytes(bytes[8..10].try_into().expect("version width")) != IDENTITY_VERSION
    {
        return Err(RuntimeError::Identity(
            "node identity has an incompatible format".into(),
        ));
    }
    let expected = blake3::hash(&bytes[..IDENTITY_BODY_BYTES]);
    if expected.as_bytes() != &bytes[IDENTITY_BODY_BYTES..] {
        return Err(RuntimeError::Identity(
            "node identity checksum mismatch".into(),
        ));
    }
    Ok(NodeIdentity {
        broker_id: BrokerId::new(u32::from_le_bytes(
            bytes[10..14].try_into().expect("broker width"),
        )),
        incarnation: u64::from_le_bytes(bytes[14..22].try_into().expect("incarnation width")),
    })
}

fn write_node_identity(path: &Path, identity: NodeIdentity) -> RuntimeResult<()> {
    let mut bytes = Vec::with_capacity(IDENTITY_BYTES);
    bytes.extend_from_slice(IDENTITY_MAGIC);
    bytes.extend_from_slice(&IDENTITY_VERSION.to_le_bytes());
    bytes.extend_from_slice(&identity.broker_id.get().to_le_bytes());
    bytes.extend_from_slice(&identity.incarnation.to_le_bytes());
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());

    let temporary = temporary_identity_path(path);
    let _ = fs::remove_file(&temporary);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn temporary_identity_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(format!(".tmp-{}", std::process::id()));
    PathBuf::from(value)
}

#[derive(Debug)]
pub enum RuntimeError {
    InvalidConfig(String),
    InvalidLifecycle {
        expected: &'static str,
        actual: LifecycleState,
    },
    Fenced(String),
    NotReady(String),
    Identity(String),
    DeadlineExceeded(&'static str),
    Shutdown(String),
    Engine(EngineError),
    Io(std::io::Error),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "invalid runtime config: {message}"),
            Self::InvalidLifecycle { expected, actual } => {
                write!(
                    formatter,
                    "runtime must be {expected}, currently {actual:?}"
                )
            }
            Self::Fenced(message) => write!(formatter, "runtime fenced: {message}"),
            Self::NotReady(message) => write!(formatter, "runtime is not ready: {message}"),
            Self::Identity(message) => write!(formatter, "node identity error: {message}"),
            Self::DeadlineExceeded(phase) => write!(formatter, "{phase} deadline exceeded"),
            Self::Shutdown(message) => write!(formatter, "runtime shutdown failed: {message}"),
            Self::Engine(error) => write!(formatter, "{error}"),
            Self::Io(error) => write!(formatter, "{error}"),
        }
    }
}

impl Error for RuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Engine(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<EngineError> for RuntimeError {
    fn from(error: EngineError) -> Self {
        Self::Engine(error)
    }
}

impl From<std::io::Error> for RuntimeError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

pub type RuntimeResult<T> = Result<T, RuntimeError>;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use shard_stream_core::{
        AssignmentProvider, BrokerDescriptor, BrokerId, LogicalPartitionId, ShardId,
        SingleNodeAssignment, TopicId, TopicPartition, WriteFence, WriteFenceError,
    };
    use shard_stream_engine::{
        DurableAppend, DurableAppendSink, DurableAppendSinkFactory, DurableSinkApply,
        DurableSinkCheckpoint, DurableSinkConfig, EngineConfig, EngineError, EngineResult,
        ReplicationProgress, ReplicationStats, ReplicationTransport, TopicConfig,
    };
    use shard_stream_protocol::{
        AppendRequest, Durability, FetchMode, FetchRequest, ReplicaAppend,
    };

    use super::{
        EmbeddedRuntime, EmbeddedRuntimeOptions, HealthPolicy, HealthProbe, HealthProbeSnapshot,
        LifecycleState, RuntimeBudgets, RuntimeError,
    };

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "shard-stream-runtime-{label}-{}-{}",
                std::process::id(),
                Instant::now().elapsed().as_nanos()
            ));
            std::fs::create_dir_all(&path).expect("create temp");
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Debug)]
    struct ExactFence;

    impl WriteFence for ExactFence {
        fn check_write(
            &self,
            _topic_partition: TopicPartition,
            leader_epoch: u64,
        ) -> Result<(), WriteFenceError> {
            if leader_epoch == 7 {
                Ok(())
            } else {
                Err(WriteFenceError::new("lease expired"))
            }
        }
    }

    #[derive(Debug, Default)]
    struct NoopReplication;

    impl ReplicationTransport for NoopReplication {
        fn install_progress(&self, _progress: ReplicationProgress) -> EngineResult<()> {
            Ok(())
        }

        fn replicate(&self, _batch: ReplicaAppend, _required_followers: u32) -> EngineResult<u32> {
            Ok(0)
        }

        fn stats(&self) -> ReplicationStats {
            ReplicationStats::default()
        }
    }

    #[derive(Debug, Clone, Default)]
    struct RuntimeSinkState {
        checkpoints: Arc<Mutex<HashMap<TopicPartition, DurableSinkCheckpoint>>>,
    }

    impl DurableAppendSink for RuntimeSinkState {
        fn apply(
            &self,
            expected: DurableSinkCheckpoint,
            _appends: &[DurableAppend],
            next: DurableSinkCheckpoint,
        ) -> EngineResult<DurableSinkApply> {
            let mut checkpoints = self.checkpoints.lock().map_err(|_| {
                EngineError::DurableSinkUnavailable("runtime test sink poisoned".into())
            })?;
            let actual = checkpoints
                .get(&expected.topic_partition)
                .copied()
                .unwrap_or_else(|| DurableSinkCheckpoint::initial(expected.topic_partition));
            if actual != expected {
                return Ok(DurableSinkApply::CheckpointConflict(actual));
            }
            checkpoints.insert(next.topic_partition, next);
            Ok(DurableSinkApply::Applied)
        }
    }

    impl DurableAppendSinkFactory for RuntimeSinkState {
        fn validate_append(&self, _payload: &[u8], _record_count: NonZeroU32) -> EngineResult<()> {
            Ok(())
        }

        fn load_checkpoint(
            &self,
            topic_partition: TopicPartition,
        ) -> EngineResult<Option<DurableSinkCheckpoint>> {
            Ok(self
                .checkpoints
                .lock()
                .map_err(|_| {
                    EngineError::DurableSinkUnavailable("runtime test sink poisoned".into())
                })?
                .get(&topic_partition)
                .copied())
        }

        fn open_shard(&self, _shard_id: ShardId) -> EngineResult<Arc<dyn DurableAppendSink>> {
            Ok(Arc::new(self.clone()))
        }
    }

    #[derive(Debug)]
    struct FixedHealthProbe(HealthProbeSnapshot);

    impl HealthProbe for FixedHealthProbe {
        fn snapshot(&self) -> Result<HealthProbeSnapshot, String> {
            Ok(self.0)
        }
    }

    fn config(path: &std::path::Path) -> EngineConfig {
        EngineConfig {
            data_dir: path.join("wal"),
            object_store_dir: None,
            shard_count: 2,
            virtual_lane_count: 2,
            replication_factor: 1,
            min_in_sync_replicas: 1,
            queue_slots_per_shard: 64,
            queue_bytes_per_shard: 4 * 1024 * 1024,
            target_pack_bytes: 1024 * 1024,
            max_pack_age: Duration::from_secs(1),
            max_batch_bytes: 1024 * 1024,
            max_fetch_bytes: 1024 * 1024,
            append_linger: Duration::ZERO,
        }
    }

    fn append_request(epoch: u64) -> AppendRequest {
        AppendRequest {
            request_id: 1,
            topic_id: TopicId::new(1),
            partition_id: LogicalPartitionId::new(0),
            record_count: 1,
            payload: Bytes::from_static(b"eden"),
            durability: Durability::Leader,
            producer: None,
            atomic_group: None,
            leader_epoch: Some(epoch),
            extension_context: None,
        }
    }

    fn single_node_assignments() -> Arc<dyn AssignmentProvider> {
        Arc::new(
            SingleNodeAssignment::new(
                BrokerDescriptor {
                    broker_id: BrokerId::new(0),
                    incarnation: 1,
                    rack: String::new(),
                    zone: String::new(),
                    internal_rest: "http://127.0.0.1:1".into(),
                    internal_replication: String::new(),
                    advertised_rest: "http://127.0.0.1:3".into(),
                    advertised_grpc: "http://127.0.0.1:4".into(),
                    kafka_host: "127.0.0.1".into(),
                    kafka_port: 5,
                    cpu_capacity: 1,
                    disk_capacity_bytes: 0,
                    network_capacity_bytes_per_sec: 0,
                    certificate_digest: [0; 32],
                },
                2,
            )
            .expect("assignment"),
        )
    }

    #[test]
    fn runtime_supports_fence_and_replication_together() {
        let temp = TempDir::new("components");
        let runtime = EmbeddedRuntime::builder()
            .storage(config(&temp.0))
            .broker_id(BrokerId::new(1))
            .write_fence(Arc::new(ExactFence))
            .replication(Arc::new(NoopReplication))
            .open()
            .expect("open");
        runtime.recover().expect("recover");
        runtime.ready().expect("ready");
        runtime
            .create_topic(TopicConfig {
                topic_id: TopicId::new(1),
                partitions: 1,
                shards: None,
            })
            .expect("topic");
        assert!(matches!(
            runtime.append(append_request(6)),
            Err(RuntimeError::Engine(_))
        ));
        let receipt = runtime.append(append_request(7)).expect("append");
        assert_eq!(receipt.first_offset.get(), 0);
        let fetched = runtime
            .fetch(FetchRequest {
                request_id: 2,
                topic_id: TopicId::new(1),
                partition_id: LogicalPartitionId::new(0),
                start_offset: shard_stream_core::LogicalOffset::new(0),
                max_bytes: 1024,
                mode: FetchMode::Ordered,
            })
            .expect("fetch");
        assert_eq!(fetched.len(), 1);
        runtime.shutdown().expect("shutdown");
    }

    #[test]
    fn runtime_replays_only_after_assignment_aware_recovery() {
        let temp = TempDir::new("durable-sink-recovery");
        {
            let runtime = EmbeddedRuntime::builder()
                .storage(config(&temp.0))
                .broker_id(BrokerId::new(0))
                .open()
                .expect("open writer");
            runtime.recover().expect("recover writer");
            runtime.ready().expect("ready writer");
            runtime
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(1),
                    partitions: 1,
                    shards: None,
                })
                .expect("topic");
            runtime.append(append_request(0)).expect("append");
            runtime.shutdown().expect("shutdown writer");
        }

        let sink = Arc::new(RuntimeSinkState::default());
        let runtime = EmbeddedRuntime::builder()
            .storage(config(&temp.0))
            .assignment_provider(single_node_assignments())
            .durable_sink(DurableSinkConfig::new(sink.clone()))
            .open()
            .expect("open with sink");
        assert!(sink.checkpoints.lock().expect("sink").is_empty());
        runtime.recover().expect("assignment-aware recovery");
        assert_eq!(
            sink.checkpoints
                .lock()
                .expect("sink")
                .values()
                .next()
                .expect("checkpoint")
                .next_offset,
            shard_stream_core::LogicalOffset::new(1)
        );
        runtime.ready().expect("ready");
        assert_eq!(runtime.durable_sink_stats().applied_appends, 1);
        runtime.shutdown().expect("shutdown");
    }

    #[test]
    fn health_tracks_lifecycle_checkpoint_and_disk_admission() {
        let temp = TempDir::new("health");
        let runtime = EmbeddedRuntime::builder()
            .storage(config(&temp.0))
            .broker_id(BrokerId::new(1))
            .health_probe(Arc::new(FixedHealthProbe(HealthProbeSnapshot {
                oldest_consumer_checkpoint: Some(shard_stream_core::LogicalOffset::new(23)),
                disk_free_bytes: 99,
                corruption_detected: false,
            })))
            .health_policy(HealthPolicy {
                minimum_disk_free_bytes: 100,
                ..HealthPolicy::default()
            })
            .open()
            .expect("open");
        assert_eq!(
            runtime.health_snapshot().lifecycle,
            LifecycleState::Recovering
        );
        runtime.recover().expect("recover");
        let error = runtime.ready().expect_err("low disk blocks readiness");
        assert!(matches!(error, RuntimeError::NotReady(_)));
        let snapshot = runtime.health_snapshot();
        assert_eq!(snapshot.lifecycle, LifecycleState::CatchingUp);
        assert_eq!(snapshot.oldest_consumer_checkpoint, Some(23));
        assert!(!snapshot.ready);
    }

    #[test]
    fn identity_is_stable_and_incarnation_increases() {
        let temp = TempDir::new("identity");
        let first = EmbeddedRuntime::builder()
            .storage(config(&temp.0))
            .broker_id(BrokerId::new(9))
            .open()
            .expect("first open");
        assert_eq!(first.identity().broker_id, BrokerId::new(9));
        assert_eq!(first.identity().incarnation, 1);
        first.shutdown().expect("first shutdown");

        let second = EmbeddedRuntime::builder()
            .storage(config(&temp.0))
            .broker_id(BrokerId::new(9))
            .open()
            .expect("second open");
        assert_eq!(second.identity().incarnation, 2);
        second.shutdown().expect("second shutdown");
    }

    #[test]
    fn drain_rejects_new_producer_writes() {
        let temp = TempDir::new("drain");
        let runtime = EmbeddedRuntime::builder()
            .storage(config(&temp.0))
            .broker_id(BrokerId::new(1))
            .open()
            .expect("open");
        runtime.recover().expect("recover");
        runtime.ready().expect("ready");
        runtime
            .drain(Instant::now() + Duration::from_secs(1))
            .expect("drain");
        assert_eq!(runtime.lifecycle(), LifecycleState::Draining);
        assert!(matches!(
            runtime.append(append_request(7)),
            Err(RuntimeError::InvalidLifecycle { .. })
        ));
        runtime.shutdown().expect("shutdown");
    }

    #[test]
    fn budgets_are_applied_and_must_match_current_io_ownership() {
        let temp = TempDir::new("budgets");
        let error = EmbeddedRuntime::builder()
            .storage(config(&temp.0))
            .broker_id(BrokerId::new(1))
            .budgets(RuntimeBudgets {
                worker_threads: 2,
                io_threads: 1,
                queue_slots_per_worker: 64,
                queue_bytes_per_worker: 4 * 1024 * 1024,
            })
            .open()
            .expect_err("mismatched budgets");
        assert!(error.to_string().contains("one dedicated I/O thread"));
    }

    #[test]
    fn assignment_provider_supplies_and_validates_local_identity() {
        let temp = TempDir::new("assignment");
        let runtime = EmbeddedRuntime::builder()
            .storage(config(&temp.0))
            .assignment_provider(single_node_assignments())
            .options(EmbeddedRuntimeOptions {
                require_replication_caught_up: false,
                ..EmbeddedRuntimeOptions::default()
            })
            .open()
            .expect("open");
        assert_eq!(runtime.identity().broker_id, BrokerId::new(0));
        runtime.shutdown().expect("shutdown");
    }
}
