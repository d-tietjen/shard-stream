use super::*;
use crate::worker::observe_bounded_fetch_phases;
use shard_stream_core::{NativeDirectoryBudget, NativeDirectoryFailure, NativeDirectoryLimits};
use std::sync::atomic::AtomicBool;

fn directory_limits(declared: usize, entries: usize, work: usize) -> NativeDirectoryLimits {
    NativeDirectoryLimits {
        max_depth: 8,
        max_component_bytes: 1024,
        max_path_bytes: 8192,
        max_operations: 1,
        max_calls: 1,
        max_entries: entries,
        max_declared_bytes: declared,
        max_work_bytes: work,
    }
}
fn directory(expiry: Instant) -> Arc<NativeDirectoryBudget> {
    let (declared, entries) = DirectoryFetchChecks::cost().expect("actual wrapper widths");
    Arc::new(
        NativeDirectoryBudget::new(
            directory_limits(declared, entries, declared + entries),
            expiry,
        )
        .expect("exact original wrapper ledger"),
    )
}
fn checked_scope(
    handle: &EngineHandle,
    original: Arc<NativeDirectoryBudget>,
    expiry: Instant,
) -> Box<Scope> {
    let checks = DirectoryFetchChecks::new(original).expect("prepaid exact check owner");
    let mut allowed = limits();
    allowed.fetch.max_selected_batches = 64;
    start_inner(handle, allowed, expiry, Some(checks)).expect("actual checked fetch work")
}
fn two_lane_recovered(root: &Path) -> EngineHandle {
    let mut cfg = config(root);
    cfg.shard_count = 2;
    {
        let writer = StreamEngine::open(cfg.clone()).expect("actual two-shard source writer");
        topic(&writer);
        // One actual batch on each physical shard selects both required read lanes.
        // Extra history would consume unrelated native recovery file reservations.
        for id in 0..2 {
            writer
                .append(AppendRequest {
                    request_id: id,
                    topic_id: partition().topic_id,
                    partition_id: partition().partition_id,
                    record_count: 1,
                    payload: Bytes::from_static(b"actual published directory fetch prefix"),
                    durability: Durability::Leader,
                    producer: None,
                    atomic_group: None,
                    leader_epoch: None,
                    extension_context: None,
                })
                .expect("actual native append on real placement");
        }
    }
    let owner = DirectoryOwnership::acquire(&cfg.data_dir).expect("actual original native owner");
    let mut allowed = native_limits();
    allowed.max_files = allowed
        .max_files
        .checked_mul(usize::try_from(cfg.shard_count).expect("physical shard count"))
        .expect("same per-shard file allowance");
    let storage = NativeRecoveryCensus::read_for_engine(
        cfg,
        owner,
        None,
        allowed,
        Instant::now() + Duration::from_secs(60),
    )
    .expect("actual two-shard bounded census")
    .restore_storage()
    .expect("actual original storage restore");
    EngineHandle::from_recovered(Arc::new(
        StreamEngine::open_recovered_storage(storage, None, None, None, None)
            .expect("real recovered workers"),
    ))
}

#[test]
fn dynamic_directory_two_lanes_keep_published_prefix_and_veto_next_native_read() {
    let root = Temporary::new();
    let handle = two_lane_recovered(&root.0);
    let ordinary = handle
        .fetch(fetch())
        .expect("actual ordinary history oracle");
    assert_eq!(ordinary.len(), 2);
    let expiry = Instant::now() + Duration::from_secs(30);
    let original = directory(expiry);
    let scope = checked_scope(&handle, original.clone(), expiry);
    let shared = scope.work.clone();
    let weak = handle.downgrade();
    let control = Arc::new((Mutex::new((false, false, false)), Condvar::new()));
    let worker_control = control.clone();
    let observer_work = shared.clone();
    let first_read_usage = Arc::new(Mutex::new(None));
    let usage_saved = first_read_usage.clone();
    let job = thread::spawn(move || {
        let _observer = observe_bounded_fetch_phases(Arc::new(move |phase, lane| {
            if phase == "read_entered" && lane == 1 {
                let (mutex, cv) = &*worker_control;
                let mut state = mutex.lock().expect("actual second-lane gate");
                state.1 = true;
                cv.notify_all();
                while !state.2 {
                    state = cv.wait(state).expect("actual held native phase");
                }
            }
            if phase == "read_published" && lane == 0 {
                *usage_saved.lock().expect("actual native usage snapshot") = Some(
                    observer_work
                        .inner
                        .fetch
                        .usage()
                        .expect("same original fetch usage"),
                );
                let (mutex, cv) = &*worker_control;
                mutex.lock().expect("real first completion").0 = true;
                cv.notify_all();
            }
        }));
        let mut scope = scope;
        let actual = scope.fetch_stored(partition(), LogicalOffset::new(0), 4096);
        (scope, actual)
    });
    let (mutex, cv) = &*control;
    let state = mutex.lock().expect("actual phase state");
    let (mut state, timeout) = cv
        .wait_timeout_while(state, Duration::from_secs(10), |state| {
            !(state.0 && state.1)
        })
        .expect("real two-lane phases");
    let both_entered = state.0 && state.1 && !timeout.timed_out();
    let earlier = Instant::now();
    assert!(original.cap_expiry(earlier).is_err());
    state.2 = true;
    cv.notify_all();
    drop(state);
    let (scope, actual) = job
        .join()
        .expect("all real queued lanes physically settled");
    assert!(
        both_entered,
        "both real lanes must be selected, with first result published before the cutoff"
    );
    assert!(actual.is_err());
    let error = scope.fail(NativeResumeFailure::Fetch);
    let fetch_error = error.fetch_error().expect("actual refused native pipeline");
    assert!(
        fetch_error
            .completed_payloads()
            .any(|body| body == b"actual published directory fetch prefix")
    );
    assert_eq!(
        shared
            .inner
            .fetch
            .usage()
            .expect("same final usage")
            .read_attempts,
        first_read_usage
            .lock()
            .expect("actual prior read usage")
            .expect("first real lane completed")
            .read_attempts,
        "second lane entered no native read_at after current directory veto"
    );
    assert!(
        shared
            .inner
            .fetch
            .usage()
            .expect("same cumulative usage")
            .read_attempts
            > 0
    );
    assert_eq!(shared.inner.fetch.absolute_expiry(), expiry);
    assert_eq!(original.absolute_expiry(), earlier);
    assert!(matches!(
        error.gc_budget_error(),
        Some(NativeDirectoryFailure::Expired(_))
    ));
    assert!(fetch_error.storage_failures().any(|failure| matches!(
        failure,
        StorageFetchFailure::Checkpoint {
            failure: ShardDirectoryCheckFailure::Refused,
            ..
        }
    )));
    drop(handle);
    contended(&root.0);
    assert!(weak.upgrade().is_some());
    drop(error);
    assert!(
        weak.upgrade().is_none(),
        "escaped check/work handles do not retain the engine"
    );
    StreamEngine::open({
        let mut cfg = config(&root.0);
        cfg.shard_count = 2;
        cfg
    })
    .expect("actual native owner released after drained refusal");
}

#[test]
fn dynamic_directory_first_typed_failure_is_sticky_in_both_check_orders() {
    for outer_first in [false, true] {
        let root = Temporary::new();
        let handle = recovered(&root.0, Arc::new(LegacyOnly::default()));
        let expiry = Instant::now() + Duration::from_secs(30);
        let original = directory(expiry);
        let scope = checked_scope(&handle, original.clone(), expiry);
        let escaped = scope.work.clone();
        let weak = handle.downgrade();
        assert!(original.cap_expiry(Instant::now()).is_err());
        if outer_first {
            assert_eq!(scope.work.check(), Err(NativeResumeRefusal::Expired));
            assert!(matches!(
                scope.work.inner.fetch.check(),
                Err(StorageFetchFailure::Denied)
            ));
        } else {
            assert!(matches!(
                scope.work.inner.fetch.check(),
                Err(StorageFetchFailure::Checkpoint {
                    failure: ShardDirectoryCheckFailure::Refused,
                    ..
                })
            ));
            assert_eq!(scope.work.check(), Err(NativeResumeRefusal::Expired));
            assert!(matches!(
                scope.work.inner.fetch.check(),
                Err(StorageFetchFailure::Denied)
            ));
        }
        let first = scope
            .work
            .inner
            .directory
            .as_ref()
            .expect("same concrete checker")
            .first_failure()
            .expect("actual first typed failure")
            as *const NativeDirectoryFailure;
        let error = scope.fail(NativeResumeFailure::Callback);
        assert!(matches!(
            error.gc_budget_error(),
            Some(NativeDirectoryFailure::Expired(_))
        ));
        assert_eq!(
            error.gc_budget_error().expect("retained first failure")
                as *const NativeDirectoryFailure,
            first
        );
        escaped.deny();
        assert!(escaped.check().is_err());
        assert_eq!(
            error.gc_budget_error().expect("never reset first failure")
                as *const NativeDirectoryFailure,
            first
        );
        assert_eq!(escaped.inner.fetch.absolute_expiry(), expiry);
        drop(handle);
        contended(&root.0);
        drop(error);
        assert!(weak.upgrade().is_none());
        assert!(
            escaped.check().is_err(),
            "owner-free escaped counters remain refused"
        );
    }
}

struct DenyChecks(AtomicBool);
impl ShardDirectoryChecks for DenyChecks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        Ok(())
    }
}
impl PinnedShardGcChecks for DenyChecks {
    fn deny(&self) {
        self.0.store(true, Ordering::Release);
    }
}
#[test]
fn dynamic_directory_setup_exact_and_one_less_keep_original_owner_and_failure() {
    for fits in [false, true] {
        let root = Temporary::new();
        let handle = recovered(&root.0, Arc::new(LegacyOnly::default()));
        let expiry = Instant::now() + Duration::from_secs(30);
        let (declared, entries) =
            DirectoryFetchChecks::cost().expect("actual concrete owner and Arc header widths");
        let budget = Arc::new(
            NativeDirectoryBudget::new(
                directory_limits(declared - usize::from(!fits), entries, declared + entries),
                expiry,
            )
            .expect("independent original setup allowance"),
        );
        let checks = Arc::new(DenyChecks(AtomicBool::new(false)));
        let policy = NativeResumeGcPolicy {
            budget: budget.clone(),
            limits: shardlog::PinnedShardGcLimits {
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
            },
            checks: checks.clone(),
        };
        let actual = gc::start_owned(&handle, limits(), expiry, policy);
        if fits {
            let scope = actual.expect("exact wrapper setup fits before native entry");
            assert_eq!(
                budget
                    .usage()
                    .expect("actual original directory fees")
                    .declared_bytes,
                declared
            );
            assert_eq!(budget.usage().expect("actual entry fees").entries, entries);
            assert_eq!(
                budget.usage().expect("actual cumulative work").work_bytes,
                declared + entries
            );
            assert_eq!(
                scope
                    .work
                    .inner
                    .fetch
                    .usage()
                    .expect("no native reads during setup"),
                StorageFetchUsage::default()
            );
            assert!(!checks.0.load(Ordering::Acquire));
            drop(scope);
        } else {
            let error = match actual {
                Err(error) => error,
                Ok(_) => panic!("one less must refuse before checker allocation"),
            };
            assert!(matches!(
                error.gc_budget_error(),
                Some(NativeDirectoryFailure::Capacity)
            ));
            assert_eq!(
                budget
                    .usage()
                    .expect("refused setup debits no replacement ledger")
                    .declared_bytes,
                0
            );
            assert!(checks.0.load(Ordering::Acquire));
            assert_eq!(budget.absolute_expiry(), expiry);
            drop(handle);
            contended(&root.0);
            drop(error);
            DirectoryOwnership::acquire(&root.0.join("data"))
                .expect("actual retained early owner released");
        }
    }
}

#[test]
fn dynamic_directory_finish_observation_keeps_first_error_before_later_denial() {
    let root = Temporary::new();
    let handle = recovered(&root.0, Arc::new(LegacyOnly::default()));
    let expiry = Instant::now() + Duration::from_secs(30);
    let original = directory(expiry);
    let mut scope = checked_scope(&handle, original.clone(), expiry);
    let escaped = scope.work.clone();
    scope
        .work
        .check()
        .expect("real preceding finish work check succeeds");
    assert!(original.cap_expiry(Instant::now()).is_err());
    // Exercise the actual production helper called immediately after finish's
    // preceding work check, with a real cap between those two phases.
    assert!(matches!(
        scope.finish_directory(),
        Err(NativeResumeFailure::Callback)
    ));
    let first = scope
        .work
        .inner
        .directory
        .as_ref()
        .expect("same checker")
        .first_failure()
        .expect("actual final directory observation")
        as *const NativeDirectoryFailure;
    let error = scope.fail(NativeResumeFailure::Callback);
    assert!(matches!(
        error.gc_budget_error(),
        Some(NativeDirectoryFailure::Expired(_))
    ));
    assert_eq!(
        error.gc_budget_error().expect("retained actual first") as *const NativeDirectoryFailure,
        first
    );
    escaped.deny();
    assert!(escaped.check().is_err());
    assert!(matches!(
        escaped.inner.fetch.check(),
        Err(StorageFetchFailure::Denied)
    ));
    assert_eq!(
        error
            .gc_budget_error()
            .expect("later denial cannot change borrowed identity")
            as *const NativeDirectoryFailure,
        first
    );
    drop(handle);
    contended(&root.0);
    drop(error);
    DirectoryOwnership::acquire(&root.0.join("data"))
        .expect("last retained engine owner releases root");
}
