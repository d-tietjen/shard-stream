//! Real owned native recovery and synchronized sink-file effects. This fixture
//! exercises composition; it does not qualify a production SDK transaction.

use super::*;
use crate::{DurableSinkAtomicVisibility, NativePrimaryRecovery, NativeResumeGcPolicy};
use shard_stream_core::{NativeDirectoryBudget, NativeDirectoryLimits};
use shard_stream_protocol::AtomicGroupIdentity;
use shardlog::{
    PinnedShardDirectoryLimits, PinnedShardGcChecks, PinnedShardGcLimits, PinnedStoreOpenChecks,
    PinnedStoreOpenLimits, ShardDirectoryCheckFailure, ShardDirectoryChecks,
    ShardGuardAcquireLimits,
};
use std::io::Write;
use std::sync::atomic::AtomicBool;

const GROUP: u128 = 88;
const BODY: &[u8] = b"physical strict sink replay payload";

struct Checks {
    denied: AtomicBool,
    denies: AtomicUsize,
}
impl ShardDirectoryChecks for Checks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        if self.denied.load(Ordering::Acquire) {
            Err(ShardDirectoryCheckFailure::Refused)
        } else {
            Ok(())
        }
    }
}
impl PinnedStoreOpenChecks for Checks {
    fn deny(&self) {
        self.denies.fetch_add(1, Ordering::AcqRel);
        self.denied.store(true, Ordering::Release);
    }
}
impl PinnedShardGcChecks for Checks {
    fn deny(&self) {
        PinnedStoreOpenChecks::deny(self);
    }
}

#[derive(Debug)]
struct State {
    file: fs::File,
    checkpoint: Option<DurableSinkCheckpoint>,
}
#[derive(Debug)]
struct FileSink {
    state: Mutex<State>,
    applied: AtomicUsize,
    cleanup: Option<Gate>,
    panic_after_commit: AtomicBool,
    tighten_after_commit: Mutex<Option<(Arc<NativeDirectoryBudget>, Instant)>>,
}
impl FileSink {
    fn record(
        &self,
        kind: u8,
        payload: &[u8],
        next: DurableSinkCheckpoint,
        work: &NativeResumeWork,
    ) -> NativeResumeCallbackResult<()> {
        work.reserve_values(
            25 + size_of::<DurableSinkCheckpoint>(),
            0,
            2,
            25 + payload.len(),
        )
        .map_err(NativeResumeCallbackError::refused)?;
        work.check().map_err(NativeResumeCallbackError::refused)?;
        let mut state = self
            .state
            .try_lock()
            .map_err(|_| NativeResumeCallbackError::invalid_state("fixture sink busy"))?;
        let mut header = [0; 25];
        header[0] = kind;
        header[1..9].copy_from_slice(&next.next_placement_sequence.get().to_le_bytes());
        header[9..17].copy_from_slice(&next.next_offset.get().to_le_bytes());
        header[17..25].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        for bytes in [header.as_slice(), payload] {
            let mut position = 0;
            while position < bytes.len() {
                let count = (bytes.len() - position).min(4096);
                work.reserve_values(0, 0, 0, count + 1)
                    .map_err(NativeResumeCallbackError::refused)?;
                let actual = state.file.write(&bytes[position..position + count]);
                let written = match actual {
                    Ok(0) => {
                        return Err(NativeResumeCallbackError::from_external(Box::new(
                            io::Error::new(
                                io::ErrorKind::WriteZero,
                                "fixture physical write stalled",
                            ),
                        )));
                    }
                    Ok(written) => written,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                        work.check().map_err(NativeResumeCallbackError::refused)?;
                        continue;
                    }
                    Err(error) => {
                        return Err(NativeResumeCallbackError::from_external(Box::new(error)));
                    }
                };
                position += written;
                work.check().map_err(NativeResumeCallbackError::refused)?;
            }
        }
        work.reserve_values(0, 0, 0, 1)
            .map_err(NativeResumeCallbackError::refused)?;
        let actual = state.file.sync_data();
        if let Err(error) = actual {
            return Err(NativeResumeCallbackError::from_external(Box::new(error)));
        }
        if kind == 1 {
            state.checkpoint = Some(next);
            self.applied.fetch_add(1, Ordering::Relaxed);
        }
        drop(state);
        if let Some((budget, earlier)) = self
            .tighten_after_commit
            .lock()
            .expect("fixture cap")
            .take()
        {
            let _ = budget.cap_expiry(earlier);
        }
        if self.panic_after_commit.load(Ordering::Acquire) {
            std::panic::panic_any(io::Error::other(
                "actual sink transaction committed before unwind",
            ));
        }
        if let Some(gate) = &self.cleanup {
            let mut state = gate.0.lock().expect("actual callback cleanup");
            state.entered = true;
            gate.1.notify_all();
            while !state.release {
                state = gate.1.wait(state).expect("physical cleanup wait");
            }
        }
        // The real synced effect is already complete. The worker retains this
        // actual Ok independently of its subsequent original-E observation.
        Ok(())
    }
}
impl DurableAppendSink for FileSink {
    fn apply(
        &self,
        _: DurableSinkCheckpoint,
        _: &[DurableAppend],
        _: DurableSinkCheckpoint,
    ) -> EngineResult<DurableSinkApply> {
        panic!("strict replay must not enter legacy apply")
    }
    fn apply_until(
        &self,
        expected: DurableSinkCheckpoint,
        appends: &[DurableAppend],
        next: DurableSinkCheckpoint,
        work: &NativeResumeWork,
    ) -> NativeResumeCallbackResult<DurableSinkApply> {
        work.check().map_err(NativeResumeCallbackError::refused)?;
        let actual = self
            .state
            .try_lock()
            .expect("fixture checkpoint")
            .checkpoint
            .unwrap_or_else(|| DurableSinkCheckpoint::initial(expected.topic_partition));
        if actual != expected {
            return Ok(DurableSinkApply::CheckpointConflict(actual));
        }
        assert_eq!(appends.len(), 1);
        assert_eq!(
            appends[0].delivery,
            if appends[0].atomic_group.is_some() {
                crate::DurableAppendDelivery::Stage
            } else {
                crate::DurableAppendDelivery::Publish
            }
        );
        self.record(1, &appends[0].payload, next, work)?;
        Ok(DurableSinkApply::Applied)
    }
}

#[derive(Debug)]
struct Factory {
    sink: Arc<FileSink>,
    strict: AtomicUsize,
    ordinary: AtomicUsize,
    pending: bool,
    unsupported: AtomicBool,
    decisions: AtomicUsize,
}
impl Factory {
    fn new(path: &Path, cleanup: Option<Gate>, read_only: bool) -> Arc<Self> {
        if read_only {
            fs::write(path, []).expect("actual read-only sink target");
        }
        let file = if read_only {
            fs::File::open(path).expect("real read-only descriptor")
        } else {
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(path)
                .expect("actual sink transaction journal")
        };
        Arc::new(Self {
            sink: Arc::new(FileSink {
                state: Mutex::new(State {
                    file,
                    checkpoint: None,
                }),
                applied: AtomicUsize::new(0),
                cleanup,
                panic_after_commit: AtomicBool::new(false),
                tighten_after_commit: Mutex::new(None),
            }),
            strict: AtomicUsize::new(0),
            ordinary: AtomicUsize::new(0),
            pending: true,
            unsupported: AtomicBool::new(false),
            decisions: AtomicUsize::new(0),
        })
    }
}
impl DurableAppendSinkFactory for Factory {
    fn validate_append(&self, _: &[u8], _: NonZeroU32) -> EngineResult<()> {
        Ok(())
    }
    fn load_checkpoint(&self, _: TopicPartition) -> EngineResult<Option<DurableSinkCheckpoint>> {
        self.ordinary.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }
    fn load_checkpoint_until(
        &self,
        _: TopicPartition,
        work: &NativeResumeWork,
    ) -> NativeResumeCallbackResult<Option<DurableSinkCheckpoint>> {
        if self.unsupported.load(Ordering::Acquire) {
            return Err(NativeResumeCallbackError::unsupported(
                "fixture strict checkpoint unavailable",
            ));
        }
        work.reserve_values(
            size_of::<Option<DurableSinkCheckpoint>>(),
            0,
            1,
            size_of::<Option<DurableSinkCheckpoint>>(),
        )
        .map_err(NativeResumeCallbackError::refused)?;
        let value = self
            .sink
            .state
            .try_lock()
            .expect("fixture checkpoint")
            .checkpoint;
        self.strict.fetch_add(1, Ordering::Relaxed);
        Ok(value)
    }
    fn open_shard(&self, _: ShardId) -> EngineResult<Arc<dyn DurableAppendSink>> {
        Ok(self.sink.clone())
    }
    fn supports_committed_only(&self) -> bool {
        true
    }
    fn pending_atomic_groups_until(
        &self,
        work: &NativeResumeWork,
    ) -> NativeResumeCallbackResult<Vec<DurableAtomicGroup>> {
        let count = usize::from(self.pending);
        work.reserve_groups(count)
            .map_err(NativeResumeCallbackError::refused)?;
        let bytes = count * size_of::<DurableAtomicGroup>();
        work.reserve_values(
            bytes + size_of::<Vec<DurableAtomicGroup>>(),
            0,
            count + 1,
            bytes,
        )
        .map_err(NativeResumeCallbackError::refused)?;
        let mut groups = Vec::new();
        groups
            .try_reserve_exact(count)
            .map_err(NativeResumeCallbackError::from_allocation)?;
        if self.pending {
            groups.push(DurableAtomicGroup {
                topic_partition: partition(),
                transaction_id: GROUP,
            });
        }
        work.check().map_err(NativeResumeCallbackError::refused)?;
        Ok(groups)
    }
    fn finalize_atomic_group_until(
        &self,
        decision: DurableAtomicDecision,
        work: &NativeResumeWork,
    ) -> NativeResumeCallbackResult<()> {
        assert_eq!(decision.outcome, AtomicGroupState::Committed);
        let checkpoint = self
            .sink
            .state
            .try_lock()
            .expect("fixture checkpoint")
            .checkpoint
            .expect("actual replayed checkpoint");
        self.sink.record(
            2,
            &decision.group.transaction_id.to_le_bytes(),
            checkpoint,
            work,
        )?;
        self.decisions.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

// This one-shard fixture opens three actual source frames and one decision, and
// the repeat-replay case performs at most two GC passes on the SAME budget.
// Each child ceiling is smaller than the original broad fixture geometry.
fn fixture_directory_limits(
    cfg: &EngineConfig,
    owner: &DirectoryOwnership,
    guard: ShardGuardAcquireLimits,
    pin: PinnedShardDirectoryLimits,
    open: PinnedStoreOpenLimits,
    gc: PinnedShardGcLimits,
) -> NativeDirectoryLimits {
    fn sum(values: &[usize]) -> usize {
        values
            .iter()
            .try_fold(0usize, |a, b| a.checked_add(*b))
            .expect("finite fixture sum")
    }
    fn product(a: usize, b: usize) -> usize {
        a.checked_mul(b).expect("finite fixture product")
    }
    let store_config = shardlog::ShardStoreConfig {
        directory: cfg.data_dir.join("shards").join("shard-0"),
        shard_id: ShardId::new(0),
        target_pack_bytes: cfg.target_pack_bytes,
        max_batch_bytes: cfg.max_batch_bytes,
    };
    let guarded = shardlog::shard_guard_acquire_cost(&store_config.directory, guard)
        .expect("actual guard cost");
    let pinned = shard_stream_core::owned_shard_directory_cost(pin).expect("actual pin cost");
    let opened = shard_stream_core::owned_pinned_shard_store_cost(&store_config, open)
        .expect("actual owned open cost");
    let collected =
        shard_stream_core::owned_pinned_shard_gc_cost(gc).expect("actual owned GC cost");
    let root = owner
        .directory_validation_cost(&cfg.data_dir)
        .expect("actual root validation cost");
    let mut limits = NativeDirectoryLimits {
        max_depth: 8,
        max_component_bytes: 1024,
        max_path_bytes: 8192,
        max_operations: 1,
        max_calls: 1,
        max_entries: 0,
        max_declared_bytes: 0,
        max_work_bytes: 0,
    };
    // The published setup includes every retained native operation-scope/handle
    // width. Add the full path/name conversion ceiling before each phase.
    let fixed = shard_stream_core::native_directory_cost(limits).expect("native fixed widths");
    let phases = sum(&[
        native_limits().max_files,
        guard.max_io_calls,
        pin.max_io_calls,
        open.max_io_calls,
        product(2, gc.max_io_calls),
    ]);
    // At most four pre/post validation brackets per admitted native phase; each
    // recursive validation descends through at most the declared parent depth.
    let validations = product(product(4, limits.max_depth + 1), phases);
    let strings = sum(&[
        product(4, limits.max_component_bytes + 1),
        product(2, limits.max_path_bytes + 1),
    ]);
    let per_declared = sum(&[fixed.declared_bytes, root.declared_bytes(), strings]);
    let per_entries = sum(&[fixed.entries, root.entries(), 16]);
    let per_work = sum(&[fixed.work_bytes, root.work_bytes(), strings, per_entries]);
    limits.max_operations = validations;
    limits.max_calls = sum(&[
        product(validations, root.entries() + 4),
        guarded.native_call_slots,
        pinned.native_call_slots,
        opened.native_call_slots,
        product(2, collected.native_call_slots),
    ]);
    limits.max_declared_bytes = sum(&[
        product(validations, per_declared),
        guarded.declared_bytes,
        pinned.declared_bytes,
        opened.declared_bytes,
        product(2, collected.declared_bytes),
    ]);
    limits.max_entries = sum(&[
        product(validations, per_entries),
        guarded.entries,
        pinned.entries,
        opened.entries,
        product(2, collected.entries),
    ]);
    limits.max_work_bytes = sum(&[
        product(validations, per_work),
        guarded.work_bytes,
        pinned.work_bytes,
        opened.work_bytes,
        product(2, collected.work_bytes),
    ]);
    limits
}

fn open_owned(
    root: &Path,
    factory: Arc<Factory>,
    mode: DurableSinkRecoveryMode,
) -> (EngineHandle, NativeResumeGcPolicy, Arc<Checks>) {
    let cfg = config(root);
    {
        let writer = StreamEngine::open(cfg.clone()).expect("actual source writer");
        topic(&writer);
        for id in 0..3 {
            writer
                .append(AppendRequest {
                    request_id: id,
                    topic_id: partition().topic_id,
                    partition_id: partition().partition_id,
                    record_count: 1,
                    payload: Bytes::from_static(BODY),
                    durability: Durability::Leader,
                    producer: None,
                    atomic_group: (id == 1).then_some(AtomicGroupIdentity {
                        transaction_id: GROUP,
                        chunk_sequence: 0,
                    }),
                    leader_epoch: None,
                    extension_context: None,
                })
                .expect("actual durable source frame");
        }
        writer
            .commit_atomic_group(partition(), GROUP)
            .expect("actual durable source decision");
    }
    let expiry = Instant::now() + Duration::from_secs(60);
    let checks = Arc::new(Checks {
        denied: AtomicBool::new(false),
        denies: AtomicUsize::new(0),
    });
    let gc = PinnedShardGcLimits {
        max_path_bytes: 8192,
        max_name_bytes: 1024,
        max_packs: 4,
        max_batch_slots: 8,
        max_log_start_slots: 8,
        max_topic_slots: 8,
        max_location_slots: 8,
        max_plan_packs: 4,
        max_io_calls: 32,
        max_work_bytes: 1 << 20,
    };
    let guard_limits = ShardGuardAcquireLimits {
        max_path_bytes: 8192,
        max_io_calls: 5,
    };
    let pin_limits = PinnedShardDirectoryLimits {
        max_path_bytes: 8192,
        max_io_calls: 4,
    };
    let open_limits = PinnedStoreOpenLimits {
        max_directory_visits: 16,
        directory_buffer_bytes: 8192,
        max_name_bytes: 1024,
        max_path_bytes: 8192,
        max_packs: 4,
        max_frames: 8,
        max_frame_bytes: 8192,
        max_read_attempts: 32,
        max_read_bytes: 1 << 16,
        max_index_entries: 16,
        max_repair_calls: 16,
        max_io_calls: 64,
        max_outcomes: 128,
        max_entries: 1 << 13,
        max_declared_bytes: 1 << 18,
        max_work_bytes: 1 << 20,
    };
    let owner = DirectoryOwnership::acquire(&cfg.data_dir).expect("actual source owner");
    let budget = Arc::new(
        NativeDirectoryBudget::new(
            fixture_directory_limits(&cfg, &owner, guard_limits, pin_limits, open_limits, gc),
            expiry,
        )
        .expect("same finite source-derived directory policy"),
    );
    let primary = NativePrimaryRecovery {
        budget: budget.clone(),
        guard_limits,
        pin_limits,
        open_limits,
        open_checks: checks.clone(),
        gc_checks: checks.clone(),
        gc_limits: gc,
    };
    let storage = NativeRecoveryCensus::read_for_engine_with_primary(
        cfg,
        owner,
        None,
        native_limits(),
        expiry,
        primary,
    )
    .expect("actual bounded native census")
    .restore_storage()
    .expect("actual consuming owned store");
    let sink = DurableSinkConfig::new(factory).with_options(DurableSinkOptions {
        recovery_mode: mode,
        atomic_visibility: DurableSinkAtomicVisibility::CommittedOnly,
        worker_count: 1,
        max_pending_items: 32,
        max_pending_bytes: Some(2 << 20),
        ..DurableSinkOptions::default()
    });
    let engine = StreamEngine::open_recovered_storage(storage, None, None, None, Some(sink))
        .expect("actual engine and sink workers");
    (
        EngineHandle::from_recovered(Arc::new(engine)),
        NativeResumeGcPolicy {
            budget,
            limits: gc,
            checks: checks.clone(),
        },
        checks,
    )
}

fn physical_records(path: &Path) -> Vec<(u8, u64, Vec<u8>)> {
    let bytes = fs::read(path).expect("actual sink bytes");
    let mut records = Vec::new();
    let mut position = 0;
    while position < bytes.len() {
        assert!(bytes.len() - position >= 25);
        let header = &bytes[position..position + 25];
        let offset = u64::from_le_bytes(header[9..17].try_into().expect("offset"));
        let len = usize::try_from(u64::from_le_bytes(
            header[17..25].try_into().expect("length"),
        ))
        .expect("fixture length");
        position += 25;
        let end = position + len;
        records.push((header[0], offset, bytes[position..end].to_vec()));
        position = end;
    }
    records
}

#[test]
fn strict_sink_replay_physically_applies_atomic_history_in_both_modes_and_retains_gc_root() {
    for mode in [
        DurableSinkRecoveryMode::Blocking,
        DurableSinkRecoveryMode::Background,
    ] {
        let root = Temporary::new();
        let path = root.0.join("sink-transactions");
        let factory = Factory::new(&path, None, false);
        let (handle, policy, checks) = open_owned(&root.0, factory.clone(), mode);
        let report = handle
            .resume_durable_sink_until(
                &[partition()],
                policy.clone(),
                limits(),
                Instant::now() + Duration::from_secs(30),
            )
            .expect("actual strict sink replay and GC");
        assert_eq!(factory.sink.applied.load(Ordering::Relaxed), 3);
        assert_eq!(factory.ordinary.load(Ordering::Relaxed), 0);
        let records = physical_records(&path);
        let applied: Vec<_> = records.iter().filter(|(kind, _, _)| *kind == 1).collect();
        assert_eq!(applied.len(), 3);
        for (index, record) in applied.iter().enumerate() {
            assert_eq!(record.1, index as u64 + 1);
            assert_eq!(record.2, BODY);
        }
        assert!(
            records
                .iter()
                .any(|(kind, _, data)| *kind == 2 && data == &GROUP.to_le_bytes())
        );
        assert!(report.outcomes().iter().any(|o| matches!(o, NativeResumeOutcome::GarbageCollected { result: Ok(actual), refusal: None, .. } if actual.accepted())));
        assert!(report.usage().fetch_passes >= 3);
        assert_eq!(report.usage().batches, 3);
        assert_eq!(checks.denies.load(Ordering::Relaxed), 0);
        let before = fs::read(&path).expect("committed source");
        let repeated = handle
            .resume_durable_sink_until(
                &[partition()],
                policy,
                limits(),
                Instant::now() + Duration::from_secs(30),
            )
            .expect("actual checkpoint skips completed data");
        assert_eq!(repeated.usage().fetch_passes, 0);
        assert_eq!(factory.sink.applied.load(Ordering::Relaxed), 3);
        assert!(fs::read(&path).expect("same source").starts_with(&before));
        drop(repeated);
        drop(handle);
        contended(&root.0);
        drop(report);
        DirectoryOwnership::acquire(&root.0.join("data"))
            .expect("last actual report releases root");
    }
}

#[test]
fn strict_sink_capacity_and_missing_producer_refuse_without_ordinary_callback_fallback() {
    for case in ["callbacks", "jobs", "batches", "fetches", "producer"] {
        let root = Temporary::new();
        let path = root.0.join("sink-transactions");
        let factory = Factory::new(&path, None, false);
        let (handle, policy, checks) =
            open_owned(&root.0, factory.clone(), DurableSinkRecoveryMode::Blocking);
        let mut allowed = limits();
        match case {
            "callbacks" => allowed.max_callbacks = 0,
            "jobs" => allowed.max_jobs = 0,
            "batches" => allowed.max_batches = 0,
            "fetches" => allowed.max_fetch_passes = 0,
            _ => factory.unsupported.store(true, Ordering::Release),
        }
        let error = handle
            .resume_durable_sink_until(
                &[partition()],
                policy.clone(),
                allowed,
                Instant::now() + Duration::from_secs(30),
            )
            .expect_err("genuine strict refusal");
        assert_eq!(factory.sink.applied.load(Ordering::Relaxed), 0);
        assert_eq!(fs::metadata(path).expect("actual target").len(), 0);
        assert_eq!(factory.ordinary.load(Ordering::Relaxed), 0);
        assert!(policy.budget.check().is_err());
        assert_eq!(checks.denies.load(Ordering::Relaxed), 1);
        drop(handle);
        contended(&root.0);
        drop(error);
    }
}

#[test]
fn strict_sink_late_committed_callback_is_drained_and_its_actual_ok_is_retained() {
    let root = Temporary::new();
    let path = root.0.join("sink-transactions");
    let gate = gate(false);
    let _release = Release(gate.clone());
    let factory = Factory::new(&path, Some(gate.clone()), false);
    let (handle, policy, _) =
        open_owned(&root.0, factory.clone(), DurableSinkRecoveryMode::Blocking);
    let weak = handle.downgrade();
    let expiry = Instant::now() + Duration::from_secs(2);
    let job = thread::spawn(move || {
        handle.resume_durable_sink_until(&[partition()], policy, limits(), expiry)
    });
    let state = gate.0.lock().expect("actual provider gate");
    let (state, wait) = gate
        .1
        .wait_timeout_while(state, Duration::from_secs(10), |s| !s.entered)
        .expect("actual provider entry");
    assert!(state.entered && !wait.timed_out());
    drop(state);
    assert_eq!(physical_records(&path)[0].2, BODY);
    contended(&root.0);
    thread::sleep(expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(1));
    assert!(!job.is_finished());
    release(&gate);
    let error = job
        .join()
        .expect("actual owning caller joined")
        .expect_err("post-commit expiry");
    assert!(error.outcomes().iter().any(|o| matches!(o, NativeResumeOutcome::Sink { observed, .. } if observed.apply_result() == Some(DurableSinkApply::Applied) && observed.callback_refusal() == Some(NativeResumeRefusal::Expired))));
    assert_eq!(factory.sink.applied.load(Ordering::Relaxed), 1);
    assert!(error.retained_payloads().any(|bytes| bytes == BODY));
    assert!(weak.upgrade().is_some());
    contended(&root.0);
    drop(error);
    assert!(weak.upgrade().is_none());
}

#[test]
fn strict_sink_retains_real_write_error_and_real_post_commit_panic_separately_from_refusal() {
    for panic_after in [false, true] {
        let root = Temporary::new();
        let path = root.0.join("sink-transactions");
        let factory = Factory::new(&path, None, !panic_after);
        factory
            .sink
            .panic_after_commit
            .store(panic_after, Ordering::Release);
        let (handle, policy, _) =
            open_owned(&root.0, factory.clone(), DurableSinkRecoveryMode::Blocking);
        let error = handle
            .resume_durable_sink_until(
                &[partition()],
                policy,
                limits(),
                Instant::now() + Duration::from_secs(30),
            )
            .expect_err("actual provider error/unwind");
        let actual = error
            .outcomes()
            .iter()
            .find_map(|o| match o {
                NativeResumeOutcome::Sink { observed, .. } => observed.callback_error(),
                _ => None,
            })
            .expect("actual provider cause");
        if panic_after {
            assert!(
                actual
                    .panic_payload()
                    .is_some_and(|p| p.downcast_ref::<io::Error>().is_some())
            );
            assert_eq!(physical_records(&path)[0].2, BODY);
        } else {
            assert!(
                actual
                    .external_error()
                    .is_some_and(|p| p.downcast_ref::<io::Error>().is_some())
            );
            assert_eq!(fs::metadata(&path).expect("target").len(), 0);
        }
        drop(handle);
        contended(&root.0);
        drop(error);
    }
}

#[test]
fn strict_sink_observes_later_shared_minimum_and_never_revives_directory_deadline() {
    let root = Temporary::new();
    let path = root.0.join("sink-transactions");
    let factory = Factory::new(&path, None, false);
    let (handle, policy, _) =
        open_owned(&root.0, factory.clone(), DurableSinkRecoveryMode::Blocking);
    let earlier = Instant::now();
    *factory
        .sink
        .tighten_after_commit
        .lock()
        .expect("actual cap") = Some((policy.budget.clone(), earlier));
    let error = handle
        .resume_durable_sink_until(
            &[partition()],
            policy.clone(),
            limits(),
            Instant::now() + Duration::from_secs(30),
        )
        .expect_err("original policy tightened during actual callback");
    assert_eq!(policy.budget.absolute_expiry(), earlier);
    assert_eq!(factory.sink.applied.load(Ordering::Relaxed), 1);
    assert!(error.outcomes().iter().any(|o| matches!(o, NativeResumeOutcome::Sink { observed, .. } if observed.apply_result() == Some(DurableSinkApply::Applied) && observed.callback_refusal() == Some(NativeResumeRefusal::Expired))));
    let before = error.usage().expect("same cumulative work");
    let retry = handle
        .resume_durable_sink_until(
            &[partition()],
            policy.clone(),
            limits(),
            Instant::now() + Duration::from_secs(60),
        )
        .expect_err("later caller cannot extend expired minimum");
    assert_eq!(policy.budget.absolute_expiry(), earlier);
    assert!(retry.outcomes().is_empty());
    assert_eq!(factory.sink.applied.load(Ordering::Relaxed), 1);
    assert!(before.callbacks >= 2);
}

#[test]
fn strict_sink_cache_contention_refuses_immediately_without_clearing_real_dirty_state() {
    let root = Temporary::new();
    let path = root.0.join("sink-transactions");
    let factory = Factory::new(&path, None, false);
    let (handle, policy, _) = open_owned(&root.0, factory, DurableSinkRecoveryMode::Blocking);
    let scope = gc::start_owned(
        &handle,
        limits(),
        Instant::now() + Duration::from_secs(30),
        policy,
    )
    .expect("real same-policy scope");
    let dispatcher = handle.durable_sink.as_ref().expect("actual dispatcher");
    let mut dirty = dispatcher.test_lock_dirty();
    dirty.insert(partition());
    let started = Instant::now();
    let observed = dispatcher.resume_cache(
        NativeResumeSinkCacheOperation::ClearDirty(partition()),
        &scope.work,
    );
    assert!(observed.result().is_err());
    assert!(started.elapsed() < Duration::from_secs(1));
    drop(dirty);
    assert!(dispatcher.test_lock_dirty().contains(&partition()));
    scope.work.deny();
    let error = scope.fail(NativeResumeFailure::Callback);
    drop(handle);
    contended(&root.0);
    drop(error);
}

#[test]
fn strict_sink_directory_expiry_refuses_before_callbacks_and_retains_same_engine() {
    let root = Temporary::new();
    let path = root.0.join("sink-transactions");
    let factory = Factory::new(&path, None, false);
    let (handle, policy, checks) =
        open_owned(&root.0, factory.clone(), DurableSinkRecoveryMode::Blocking);
    let earlier = Instant::now();
    assert!(policy.budget.cap_expiry(earlier).is_err());
    let error = handle
        .resume_durable_sink_until(
            &[partition()],
            policy.clone(),
            limits(),
            Instant::now() + Duration::from_secs(30),
        )
        .expect_err("saved directory minimum already elapsed");
    assert!(error.outcomes().is_empty());
    assert_eq!(factory.strict.load(Ordering::Relaxed), 0);
    assert_eq!(factory.sink.applied.load(Ordering::Relaxed), 0);
    assert_eq!(checks.denies.load(Ordering::Relaxed), 1);
    assert!(matches!(
        error.gc_budget_error(),
        Some(shard_stream_core::NativeDirectoryFailure::Expired(_))
    ));
    assert_eq!(policy.budget.absolute_expiry(), earlier);
    drop(handle);
    contended(&root.0);
    drop(error);
}

#[test]
fn strict_sink_callback_allowance_is_cumulative_across_actual_native_fetch_passes() {
    let root = Temporary::new();
    let path = root.0.join("sink-transactions");
    let factory = Factory::new(&path, None, false);
    let (handle, policy, _) =
        open_owned(&root.0, factory.clone(), DurableSinkRecoveryMode::Blocking);
    let mut allowed = limits();
    allowed.max_callbacks = 3; // real initial checkpoint plus exactly two applies
    let error = handle
        .resume_durable_sink_until(
            &[partition()],
            policy,
            allowed,
            Instant::now() + Duration::from_secs(30),
        )
        .expect_err("third actual apply cannot refresh callback counters");
    assert_eq!(error.usage().expect("same work").callbacks, 3);
    assert_eq!(factory.sink.applied.load(Ordering::Relaxed), 2);
    assert_eq!(physical_records(&path).len(), 2);
    assert_eq!(error.usage().expect("same fetch passes").fetch_passes, 3);
    assert!(error.retained_payloads().any(|p| p == BODY));
}

#[test]
fn strict_sink_real_dirty_table_probe_accepts_exact_fee_and_refuses_one_less() {
    for one_less in [false, true] {
        let root = Temporary::new();
        let path = root.0.join("sink-transactions");
        let factory = Factory::new(&path, None, false);
        let (handle, policy, _) = open_owned(&root.0, factory, DurableSinkRecoveryMode::Blocking);
        let scope = gc::start_owned(
            &handle,
            limits(),
            Instant::now() + Duration::from_secs(30),
            policy,
        )
        .expect("actual scope");
        let dispatcher = handle.durable_sink.as_ref().expect("actual dispatcher");
        let mut dirty = dispatcher.test_lock_dirty();
        dirty.insert(partition());
        let fee = (dirty.capacity() + 1) * size_of::<TopicPartition>();
        drop(dirty);
        let remain = scope.work.limits().max_work_bytes
            - scope.work.usage().expect("actual usage").work_bytes;
        // Downward allowance fault on the actual SAME owner; no counters/E are
        // replaced or reset, and the real table population stays in place.
        let leave = fee - usize::from(one_less);
        scope
            .work
            .reserve_values(0, 0, 0, remain - leave)
            .expect("consume earlier cumulative work");
        let actual = dispatcher.resume_cache(
            NativeResumeSinkCacheOperation::ClearDirty(partition()),
            &scope.work,
        );
        assert_eq!(actual.result().is_ok(), !one_less);
        assert_eq!(
            dispatcher.test_lock_dirty().contains(&partition()),
            one_less
        );
        drop(scope);
    }
}

#[test]
fn public_job_reservation_shares_actual_checkpoint_command_ceiling() {
    for fits in [true, false] {
        let root = Temporary::new();
        let factory = Factory::new(&root.0.join("shared-job-checkpoint"), None, false);
        let (handle, policy, _) =
            open_owned(&root.0, factory.clone(), DurableSinkRecoveryMode::Blocking);
        let mut allowed = limits();
        allowed.max_jobs = if fits { 2 } else { 1 };
        let scope = gc::start_owned(
            &handle,
            allowed,
            Instant::now() + Duration::from_secs(30),
            policy,
        )
        .expect("actual recovered owner and original native work");
        let shared = scope.work.clone();
        let original_expiry = shared.absolute_expiry();
        let before = scope.work.usage().expect("actual initial counters");
        assert_eq!(before.jobs, 0);
        // Reserve a provider's job before its later entry. The actual native
        // checkpoint command below must spend the SAME remaining job ceiling.
        shared.reserve_job().expect("one actual job reservation");
        let reserved = scope.work.usage().expect("shared original counters");
        assert_eq!(reserved.jobs, 1);
        assert_eq!(reserved.entries, before.entries);
        assert_eq!(reserved.declared_bytes, before.declared_bytes);
        assert_eq!(reserved.work_bytes, before.work_bytes);
        let strict_before = factory.strict.load(Ordering::Acquire);
        let ordinary_before = factory.ordinary.load(Ordering::Acquire);
        let actual = handle
            .durable_sink
            .as_ref()
            .expect("actual sink dispatcher")
            .resume_checkpoint(partition(), &scope.work);
        assert_eq!(actual.callback_entered(), fits);
        if fits {
            assert!(actual.accepted());
            assert_eq!(actual.checkpoint(), Some(None));
            assert_eq!(shared.usage().expect("two original job debits").jobs, 2);
            assert_eq!(shared.reserve_job(), Err(NativeResumeRefusal::Capacity));
        } else {
            assert!(matches!(
                actual
                    .callback_error()
                    .expect("actual pre-entry refusal")
                    .cause,
                CallbackCause::Refused(NativeResumeRefusal::Capacity)
            ));
            assert_eq!(
                shared.usage().expect("refused entry is not charged"),
                reserved
            );
        }
        assert_eq!(shared.reserve_job(), Err(NativeResumeRefusal::Denied));
        assert_eq!(shared.absolute_expiry(), original_expiry);
        assert_eq!(scope.work.absolute_expiry(), original_expiry);
        assert_eq!(
            factory.strict.load(Ordering::Acquire),
            strict_before + usize::from(fits)
        );
        assert_eq!(factory.ordinary.load(Ordering::Acquire), ordinary_before);
        // The real dispatcher call has physically completed before these
        // outcomes/work and the recovered engine owner may be released.
        drop(actual);
        drop(shared);
        drop(scope);
        drop(handle);
    }
}

#[test]
fn strict_sink_actual_pending_capacity_is_admitted_or_retained_before_push() {
    for fits in [true, false] {
        let root = Temporary::new();
        let path = root.0.join("sink-transactions");
        let factory = Factory::new(&path, None, false);
        let (handle, policy, _) = open_owned(&root.0, factory, DurableSinkRecoveryMode::Blocking);
        let mut allowed = limits();
        if !fits {
            allowed.max_declared_bytes = 1024;
        }
        let mut scope = gc::start_owned(
            &handle,
            allowed,
            Instant::now() + Duration::from_secs(30),
            policy,
        )
        .expect("actual scope");
        let group = DurableAtomicGroup {
            topic_partition: partition(),
            transaction_id: GROUP,
        };
        let mut actual = Vec::with_capacity(1024);
        actual.push(group);
        let capacity = actual.capacity();
        let before = scope.work.usage().expect("actual prior fees");
        let result = sink::test_pending_push(&mut scope, &mut actual);
        if fits {
            result.expect("actual excess capacity is prepaid");
            assert_eq!(actual.capacity(), capacity);
            assert_eq!(
                scope.work.usage().expect("same fees").declared_bytes - before.declared_bytes,
                capacity * size_of::<DurableAtomicGroup>()
            );
            actual.push(group);
        } else {
            assert!(result.is_err());
            assert_eq!(actual.capacity(), 0);
            assert_eq!(scope.pending_decisions.capacity(), capacity);
            assert_eq!(scope.pending_decisions.as_slice(), &[group]);
            let error = scope.fail(NativeResumeFailure::Callback);
            drop(handle);
            contended(&root.0);
            drop(error);
        }
    }
}

#[test]
fn strict_sink_clone_budget_refusal_keeps_original_promotable_input_before_dispatch() {
    for with_context in [false, true] {
        for fits in [false, true] {
            let root = Temporary::new();
            let path = root.0.join("sink-clone-custody");
            let factory = Factory::new(&path, None, false);
            let (handle, policy, _) =
                open_owned(&root.0, factory.clone(), DurableSinkRecoveryMode::Blocking);
            let mut scope = gc::start_owned(
                &handle,
                limits(),
                Instant::now() + Duration::from_secs(30),
                policy,
            )
            .expect("actual recovered owner and cumulative work");
            scope
                .fetch_stored(partition(), LogicalOffset::new(0), 1 << 20)
                .expect("actual bounded native WAL read");
            let batch = scope
                .buffered
                .pop_front()
                .expect("actual first native WAL batch");
            let mut append = durable_append_from_stored(&scope.owner.config, batch);
            // Explicit caller-owned fixture copies use real native descriptor
            // and payload bytes, with promotable Vec-backed Bytes owners.
            append.payload = Bytes::from(append.payload.to_vec());
            append.transient_context = with_context.then(|| Bytes::from(vec![7; 32]));
            let payload_pointer = append.payload.as_ptr();
            let context_pointer = append.transient_context.as_ref().map(|b| b.as_ptr());
            let expected = DurableSinkCheckpoint::initial(partition());
            let next = expected
                .after(append.reservation)
                .expect("actual native first batch");
            if !fits {
                let remaining = scope.work.limits().max_declared_bytes
                    - scope
                        .work
                        .usage()
                        .expect("same original counters")
                        .declared_bytes;
                let clone_fee = 2 * size_of::<DurableAppend>()
                    + bytes_clone_promotion_bytes(1 + usize::from(with_context))
                        .expect("actual selected Bytes promotion layout");
                scope
                    .work
                    .reserve_values(remaining - (clone_fee - 1), 0, 0, 0)
                    .expect("consume prior allowance without changing limits or E");
            }
            let before = scope.work.usage().expect("same prior work");
            let result = scope.sink_append(expected, append, next);
            if fits {
                result.expect("paid clone reaches actual synchronous sink callback");
                assert!(scope.current_append.is_none());
                assert_eq!(factory.sink.applied.load(Ordering::Acquire), 1);
                assert_eq!(
                    physical_records(&path),
                    [(1, next.next_offset.get(), BODY.to_vec())]
                );
            } else {
                assert!(matches!(
                    result,
                    Err(NativeResumeFailure::Work(NativeResumeRefusal::Capacity))
                ));
                assert_eq!(factory.sink.applied.load(Ordering::Acquire), 0);
                assert_eq!(
                    scope
                        .work
                        .usage()
                        .expect("refused clone was not debited")
                        .declared_bytes,
                    before.declared_bytes
                );
                let retained = scope
                    .current_append
                    .as_ref()
                    .expect("exact original moved before fee");
                assert_eq!(retained.payload.as_ptr(), payload_pointer);
                assert_eq!(
                    retained.transient_context.as_ref().map(|b| b.as_ptr()),
                    context_pointer
                );
                let error = scope.fail(NativeResumeFailure::Callback);
                assert!(error.retained_payloads().any(|p| p == BODY));
                drop(handle);
                contended(&root.0);
                drop(error);
            }
        }
    }
}

#[test]
fn strict_replica_clone_budget_refusal_retains_actual_owned_input_before_transport() {
    for fits in [false, true] {
        let root = Temporary::new();
        let factory = Factory::new(&root.0.join("replica-clone-custody"), None, false);
        let (handle, policy, _) = open_owned(&root.0, factory, DurableSinkRecoveryMode::Blocking);
        let mut scope = gc::start_owned(
            &handle,
            limits(),
            Instant::now() + Duration::from_secs(30),
            policy,
        )
        .expect("actual recovered owner and original work");
        scope
            .fetch_stored(partition(), LogicalOffset::new(0), 1 << 20)
            .expect("actual bounded WAL source");
        let batch = scope
            .buffered
            .pop_front()
            .expect("actual first native WAL batch");
        assert!(batch.producer.is_none() && batch.atomic_group.is_none());
        let input = ReplicaAppend {
            reservation: batch.reservation,
            producer: None,
            atomic_group: None,
            extension_context: Some(Bytes::from(vec![9; 32])),
            payload_digest: xxhash_rust::xxh3::xxh3_128(&batch.payload).to_le_bytes(),
            payload: Bytes::from(batch.payload.to_vec()),
        };
        let pointer = input.payload.as_ptr();
        let context_pointer = input.extension_context.as_ref().map(|b| b.as_ptr());
        if !fits {
            let remaining = scope.work.limits().max_declared_bytes
                - scope
                    .work
                    .usage()
                    .expect("same original counters")
                    .declared_bytes;
            let clone_fee = 2 * size_of::<ReplicaAppend>()
                + size_of::<ReplicationTicket>()
                + bytes_clone_promotion_bytes(2)
                    .expect("actual possible payload/context promotions");
            scope
                .work
                .reserve_values(remaining - (clone_fee - 1), 0, 0, 0)
                .expect("consume prior allowance");
        }
        let before = scope.work.usage().expect("same pre-entry counters");
        let result = StrictReplication(&mut scope).replicate(input, 1);
        if fits {
            assert!(matches!(result, Err(NativeResumeFailure::MissingTransport)));
            assert_eq!(
                scope.work.usage().expect("reserved clone/job fee").jobs,
                before.jobs + 1
            );
        } else {
            assert!(matches!(
                result,
                Err(NativeResumeFailure::Work(NativeResumeRefusal::Capacity))
            ));
            assert_eq!(
                scope.work.usage().expect("no transport fee entered").jobs,
                before.jobs
            );
        }
        assert!(scope.pending_replication.is_none());
        let retained = scope
            .current_replica
            .as_ref()
            .expect("exact original before fees/transport");
        assert_eq!(retained.payload.as_ptr(), pointer);
        assert_eq!(
            retained.extension_context.as_ref().map(|b| b.as_ptr()),
            context_pointer
        );
        let error = scope.fail(NativeResumeFailure::Replication);
        assert!(error.retained_payloads().any(|p| p == BODY));
        drop(handle);
        contended(&root.0);
        drop(error);
    }
}

#[test]
fn strict_sink_checkpoint_publication_contention_keeps_completed_callback() {
    let root = Temporary::new();
    let path = root.0.join("held-checkpoint-publication");
    let factory = Factory::new(&path, None, false);
    let (handle, policy, _) =
        open_owned(&root.0, factory.clone(), DurableSinkRecoveryMode::Blocking);
    let mut scope = gc::start_owned(
        &handle,
        limits(),
        Instant::now() + Duration::from_secs(30),
        policy,
    )
    .expect("actual same-owner scope");
    scope
        .fetch_stored(partition(), LogicalOffset::new(0), 4096)
        .expect("real source frame");
    let batch = scope
        .buffered
        .pop_front()
        .expect("actual first stored frame");
    let append = durable_append_from_stored(&scope.owner.config, batch);
    let expected = DurableSinkCheckpoint::initial(partition());
    let next = expected
        .after(append.reservation)
        .expect("actual first transition");
    let dispatcher = handle.durable_sink.as_ref().expect("actual sink workers");
    let held = dispatcher.test_lock_checkpoints();
    let (done, received) = sync_channel(1);
    let worker = thread::spawn(move || {
        let actual = scope.sink_append(expected, append, next);
        let _ = done.send(());
        (scope, actual)
    });
    let settled_while_held = received.recv_timeout(Duration::from_secs(5)).is_ok();
    // Release even on a regression so the test joins its real worker and reports
    // the finite-publication failure instead of leaving a blocked thread behind.
    drop(held);
    let (scope, actual) = worker.join().expect("actual caller physically joined");
    assert!(
        settled_while_held,
        "strict cache publication must refuse while held"
    );
    assert!(
        matches!(
            actual,
            Err(NativeResumeFailure::Work(NativeResumeRefusal::Denied))
        ),
        "actual publication result: {actual:?}; original usage: {:?}",
        scope.work.usage()
    );
    assert_eq!(factory.sink.applied.load(Ordering::Acquire), 1);
    assert_eq!(
        physical_records(&path),
        [(1, next.next_offset.get(), BODY.to_vec())]
    );
    assert!(scope.outcomes.iter().any(|value| matches!(value, NativeResumeOutcome::Sink { observed, .. }
        if observed.apply_result() == Some(DurableSinkApply::Applied)
        && observed.callback_refusal().is_none()
        && observed.progress().is_some_and(|progress| matches!(progress.result(), Err(error) if matches!(&error.cause, CallbackCause::QueueFull("native sink progress lock")))))));
    assert!(
        !dispatcher
            .test_lock_checkpoints()
            .contains_key(&partition())
    );
    let error = scope.fail(NativeResumeFailure::Callback);
    drop(handle);
    contended(&root.0);
    assert!(error.retained_payloads().any(|bytes| bytes == BODY));
    drop(error);
    DirectoryOwnership::acquire(&root.0.join("data")).expect("last real refusal releases root");
}

#[test]
fn strict_sink_decision_publication_contention_keeps_completed_callback() {
    let root = Temporary::new();
    let path = root.0.join("held-finalized-publication");
    let factory = Factory::new(&path, None, false);
    let (handle, policy, _) =
        open_owned(&root.0, factory.clone(), DurableSinkRecoveryMode::Blocking);
    let mut scope = gc::start_owned(
        &handle,
        limits(),
        Instant::now() + Duration::from_secs(30),
        policy,
    )
    .expect("actual same-owner scope");
    scope
        .fetch_stored(partition(), LogicalOffset::new(0), 4096)
        .expect("real source frame");
    let batch = scope.buffered.pop_front().expect("actual stored frame");
    let append = durable_append_from_stored(&scope.owner.config, batch);
    let expected = DurableSinkCheckpoint::initial(partition());
    let next = expected
        .after(append.reservation)
        .expect("actual first transition");
    scope
        .sink_append(expected, append, next)
        .expect("actual synced transaction before decision");
    let group = DurableAtomicGroup {
        topic_partition: partition(),
        transaction_id: GROUP,
    };
    let decision = DurableAtomicDecision {
        group,
        outcome: AtomicGroupState::Committed,
    };
    let dispatcher = handle.durable_sink.as_ref().expect("actual sink workers");
    let held = dispatcher.test_lock_finalized();
    let (done, received) = sync_channel(1);
    let worker = thread::spawn(move || {
        let actual = scope.sink_decision(decision);
        let _ = done.send(());
        (scope, actual)
    });
    let settled_while_held = received.recv_timeout(Duration::from_secs(5)).is_ok();
    drop(held);
    let (scope, actual) = worker.join().expect("actual caller physically joined");
    assert!(
        settled_while_held,
        "strict finalization publication must refuse while held"
    );
    assert!(
        matches!(
            actual,
            Err(NativeResumeFailure::Work(NativeResumeRefusal::Denied))
        ),
        "actual publication result: {actual:?}; original usage: {:?}",
        scope.work.usage()
    );
    assert_eq!(factory.decisions.load(Ordering::Acquire), 1);
    assert!(
        physical_records(&path)
            .iter()
            .any(|(kind, _, bytes)| *kind == 2 && bytes == &GROUP.to_le_bytes())
    );
    assert!(scope.outcomes.iter().any(|value| matches!(value, NativeResumeOutcome::Sink { observed, .. }
        if observed.finalized() && observed.callback_refusal().is_none()
        && observed.progress().is_some_and(|progress| matches!(progress.result(), Err(error) if matches!(&error.cause, CallbackCause::QueueFull("native sink progress lock")))))));
    assert!(!dispatcher.test_lock_finalized().contains(&group));
    let error = scope.fail(NativeResumeFailure::Callback);
    drop(handle);
    contended(&root.0);
    drop(error);
    DirectoryOwnership::acquire(&root.0.join("data"))
        .expect("last retained decision releases root");
}

#[test]
fn strict_sink_growth_pays_actual_table_capacity_or_retains_refusal() {
    for kind in ["decisions", "checkpoints", "finalized"] {
        for fits in [false, true] {
            let root = Temporary::new();
            let factory = Factory::new(&root.0.join("growth-custody"), None, false);
            let (handle, policy, _) =
                open_owned(&root.0, factory, DurableSinkRecoveryMode::Blocking);
            let mut scope = gc::start_owned(
                &handle,
                limits(),
                Instant::now() + Duration::from_secs(30),
                policy,
            )
            .expect("actual scope");
            let site = match kind {
                "decisions" => "resume.sink.decisions.grow",
                "checkpoints" => "sink.checkpoints.grow",
                _ => "sink.groups.grow",
            };
            if !fits {
                *scope
                    .work
                    .inner
                    .probe_observer
                    .lock()
                    .expect("actual observer") = Some(Arc::new(move |work, probe| {
                    if probe.site == site && probe.phase == ProbePhase::Paid {
                        let remaining = work.limits().max_declared_bytes
                            - work.usage().expect("same counters").declared_bytes;
                        work.reserve_values(remaining, 0, 0, 0).expect("consume original allowance after requested slots and before actual allocation");
                    }
                }));
            }
            let before = scope.work.usage().expect("actual original counters");
            let dispatcher = handle.durable_sink.as_ref().expect("actual shared caches");
            let mut decisions = HashMap::new();
            let actual = match kind {
                "decisions" => {
                    sink::test_decision_growth(&mut scope, &mut decisions).map_err(|_| ())
                }
                "checkpoints" => dispatcher
                    .test_cache_growth(false, &scope.work)
                    .map_err(|_| ()),
                _ => dispatcher
                    .test_cache_growth(true, &scope.work)
                    .map_err(|_| ()),
            };
            *scope
                .work
                .inner
                .probe_observer
                .lock()
                .expect("observer released") = None;
            let (capacity, slot_bytes) = match kind {
                "decisions" => (
                    if fits {
                        decisions.capacity()
                    } else {
                        scope.recovered_decisions.capacity()
                    },
                    size_of::<(DurableAtomicGroup, DurableAtomicDecision)>(),
                ),
                "checkpoints" => (
                    dispatcher.test_lock_checkpoints().capacity(),
                    size_of::<(TopicPartition, DurableSinkCheckpoint)>(),
                ),
                _ => (
                    dispatcher.test_lock_finalized().capacity(),
                    size_of::<DurableAtomicGroup>(),
                ),
            };
            assert!(
                capacity > 1,
                "actual native map/set growth exceeds one requested logical slot"
            );
            assert_eq!(actual.is_ok(), fits);
            if fits {
                assert_eq!(
                    scope
                        .work
                        .usage()
                        .expect("paid actual capacity")
                        .declared_bytes
                        - before.declared_bytes,
                    capacity * slot_bytes
                );
            } else {
                assert_eq!(
                    scope
                        .work
                        .usage()
                        .expect("same spent ledger")
                        .declared_bytes,
                    scope.work.limits().max_declared_bytes
                );
                assert_eq!(scope.work.check(), Err(NativeResumeRefusal::Denied));
                if kind == "decisions" {
                    assert_eq!(decisions.capacity(), 0);
                }
                let error = scope.fail(NativeResumeFailure::Callback);
                drop(handle);
                contended(&root.0);
                drop(error);
            }
        }
    }
}

#[test]
fn strict_sink_pending_initial_admission_keeps_nonempty_allocation() {
    let root = Temporary::new();
    let factory = Factory::new(&root.0.join("pending-initial-cutoff"), None, false);
    let (handle, policy, _) = open_owned(&root.0, factory, DurableSinkRecoveryMode::Blocking);
    let mut scope = gc::start_owned(
        &handle,
        limits(),
        Instant::now() + Duration::from_secs(30),
        policy,
    )
    .expect("actual scope");
    let original_expiry = scope.work.absolute_expiry();
    let group = DurableAtomicGroup {
        topic_partition: partition(),
        transaction_id: GROUP,
    };
    let mut pending = Vec::with_capacity(8);
    pending.push(group);
    let pointer = pending.as_ptr();
    let capacity = pending.capacity();
    let remaining = scope.work.limits().max_declared_bytes
        - scope.work.usage().expect("same counters").declared_bytes;
    scope
        .work
        .reserve_values(remaining, 0, 0, 0)
        .expect("consume actual original allowance");
    let before = scope.work.usage().expect("spent original ledger");
    let actual = sink::test_pending_push(&mut scope, &mut pending);
    assert!(matches!(
        actual,
        Err(NativeResumeFailure::Work(NativeResumeRefusal::Capacity))
    ));
    assert_eq!(pending.capacity(), 0);
    assert_eq!(scope.pending_decisions.capacity(), capacity);
    assert_eq!(scope.pending_decisions.as_ptr(), pointer);
    assert_eq!(scope.pending_decisions.as_slice(), &[group]);
    assert_eq!(scope.work.usage().expect("refused debit unchanged"), before);
    assert_eq!(scope.work.absolute_expiry(), original_expiry);
    let error = scope.fail(NativeResumeFailure::Callback);
    drop(handle);
    contended(&root.0);
    drop(error);
    DirectoryOwnership::acquire(&root.0.join("data")).expect("last retained prefix releases root");
}

#[test]
fn captured_resume_policy_keeps_original_budget_checks_and_partition_sources() {
    let root = Temporary::new();
    let path = root.0.join("captured-policy-transactions");
    let factory = Factory::new(&path, None, false);
    let (handle, supplied, checks) =
        open_owned(&root.0, factory.clone(), DurableSinkRecoveryMode::Blocking);
    let captured = handle
        .recovered_gc_policy()
        .expect("actual owned primary policy");
    assert!(Arc::ptr_eq(&captured.budget, &supplied.budget));
    assert!(Arc::ptr_eq(&captured.checks, &supplied.checks));
    assert_eq!(handle.recovered_partition_sources(), &[partition()]);
    let original = captured.budget.absolute_expiry();
    let weak = handle.downgrade();
    let report = handle
        .resume_durable_sink_until(
            handle.recovered_partition_sources(),
            captured.clone(),
            limits(),
            original + Duration::from_secs(30),
        )
        .expect("actual captured-policy replay");
    assert_eq!(supplied.budget.absolute_expiry(), original);
    assert_eq!(factory.sink.applied.load(Ordering::Relaxed), 3);
    assert!(
        physical_records(&path)
            .iter()
            .any(|(kind, _, body)| *kind == 1 && body == BODY)
    );
    let captured = handle.recovered_gc_policy().expect("same retained policy");
    PinnedShardGcChecks::deny(captured.checks.as_ref());
    assert!(checks.denied.load(Ordering::Acquire));
    assert!(captured.checks.check().is_err());
    drop(handle);
    assert!(
        weak.upgrade().is_none(),
        "completed reports retain Core outcomes, not the engine allocation"
    );
    contended(&root.0);
    drop(report);
    DirectoryOwnership::acquire(&root.0.join("data")).expect("settled last source owner");
}
