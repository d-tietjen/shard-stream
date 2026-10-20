//! Actual filesystem custody and physical engine/sink lifetimes, not HA authority.

use std::sync::Condvar;

use shard_stream_core::DirectoryOwnershipError;

use super::*;

#[derive(Debug, Default)]
struct DestructionBarrier {
    armed: AtomicBool,
    entered: AtomicBool,
    released: Mutex<bool>,
    changed: Condvar,
}

impl DestructionBarrier {
    fn wait(&self) {
        if !self.armed.load(Ordering::Acquire) {
            return;
        }
        self.entered.store(true, Ordering::Release);
        let mut released = self.released.lock().expect("destruction barrier");
        while !*released {
            released = self.changed.wait(released).expect("destruction release");
        }
    }

    fn release(&self) {
        *self.released.lock().expect("release lock") = true;
        self.changed.notify_all();
    }
}

struct ReleaseOpening {
    barrier: Arc<DestructionBarrier>,
    thread: Option<std::thread::JoinHandle<EngineResult<StreamEngine>>>,
}

impl Drop for ReleaseOpening {
    fn drop(&mut self) {
        self.barrier.release();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct ReleaseSink {
    apply: Arc<RecordingSinkControl>,
    destruction: Arc<DestructionBarrier>,
}

impl Drop for ReleaseSink {
    fn drop(&mut self) {
        self.apply.block.store(false, Ordering::Release);
        self.destruction.release();
    }
}

#[derive(Debug)]
struct DestructionSinkFactory {
    inner: RecordingSinkFactory,
    destruction: Arc<DestructionBarrier>,
}

impl Drop for DestructionSinkFactory {
    fn drop(&mut self) {
        self.destruction.wait();
    }
}

impl DurableAppendSinkFactory for DestructionSinkFactory {
    fn validate_append(&self, payload: &[u8], count: NonZeroU32) -> EngineResult<()> {
        self.inner.validate_append(payload, count)
    }

    fn load_checkpoint(
        &self,
        partition: TopicPartition,
    ) -> EngineResult<Option<DurableSinkCheckpoint>> {
        self.inner.load_checkpoint(partition)
    }

    fn open_shard(&self, shard: ShardId) -> EngineResult<Arc<dyn DurableAppendSink>> {
        self.inner.open_shard(shard)
    }
}

fn assert_contended(path: &std::path::Path) {
    assert!(matches!(
        DirectoryOwnership::acquire(path),
        Err(DirectoryOwnershipError::Contended)
    ));
}

fn acquire_after_settlement(path: &std::path::Path) -> Arc<DirectoryOwnership> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match DirectoryOwnership::acquire(path) {
            Ok(owner) => return owner,
            Err(DirectoryOwnershipError::Contended) => {
                assert!(
                    Instant::now() < deadline,
                    "physical directory ownership did not settle"
                );
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(error) => panic!("unexpected ownership failure: {error}"),
        }
    }
}

#[test]
fn directory_ownership_outlives_escaped_engine_and_preserves_legacy_constructor() {
    let temp = TempDir::new("owned-escape");
    let owner = DirectoryOwnership::acquire(&temp.0).expect("owner");
    let engine = Arc::new(
        StreamEngine::open_with_owned_components(
            config(&temp.0),
            Arc::clone(&owner),
            None,
            None,
            None,
            None,
        )
        .expect("owned engine"),
    );
    assert!(engine.has_directory_ownership(&owner));
    let escaped = Arc::clone(&engine);
    drop(owner);
    drop(engine);
    assert_contended(&temp.0);
    escaped
        .create_topic(TopicConfig {
            topic_id: TopicId::new(1),
            partitions: 1,
            shards: None,
        })
        .expect("live escaped engine");
    escaped
        .append(append_request(1, b"still-owned", None))
        .expect("escaped append");
    drop(escaped);
    let next = acquire_after_settlement(&temp.0);
    drop(next);
    // Existing constructors do not silently adopt the new custody contract.
    let legacy = StreamEngine::open(config(&temp.0)).expect("legacy reopen");
    let independent = DirectoryOwnership::acquire(&temp.0).expect("legacy remains unowned");
    assert!(!legacy.has_directory_ownership(&independent));
}

#[test]
fn directory_ownership_rejects_wrong_directory_before_storage_opens() {
    let owned = TempDir::new("owned-source");
    let other = TempDir::new("owned-other");
    let owner = DirectoryOwnership::acquire(&owned.0).expect("owner");
    let result =
        StreamEngine::open_with_owned_components(config(&other.0), owner, None, None, None, None);
    assert!(matches!(result, Err(EngineError::InvalidConfig(_))));
    assert_eq!(
        fs::read_dir(&other.0)
            .expect("unchanged foreign directory")
            .count(),
        0
    );
}

#[test]
fn directory_ownership_survives_detached_sink_apply_and_factory_destruction() {
    let temp = TempDir::new("owned-detached-sink");
    let owner = DirectoryOwnership::acquire(&temp.0).expect("owner");
    let control = Arc::new(RecordingSinkControl {
        block: AtomicBool::new(true),
        ..RecordingSinkControl::default()
    });
    let destruction = Arc::new(DestructionBarrier::default());
    // Release both physical barriers on every assertion/unwind path.
    let _release = ReleaseSink {
        apply: Arc::clone(&control),
        destruction: Arc::clone(&destruction),
    };
    let sink = DurableSinkConfig::new(Arc::new(DestructionSinkFactory {
        inner: RecordingSinkFactory {
            control: Arc::clone(&control),
        },
        destruction: Arc::clone(&destruction),
    }))
    .with_options(DurableSinkOptions {
        worker_count: 1,
        ..DurableSinkOptions::default()
    });
    let engine = StreamEngine::open_with_owned_components(
        config(&temp.0),
        owner,
        None,
        None,
        None,
        Some(sink),
    )
    .expect("owned sink engine");
    engine
        .create_topic(TopicConfig {
            topic_id: TopicId::new(1),
            partitions: 1,
            shards: None,
        })
        .expect("topic");
    let receipt = engine
        .append(append_request(1, b"physical-sink", None))
        .expect("durable WAL append");
    wait_for_test(|| control.entered.load(Ordering::Acquire));
    assert_eq!(receipt.first_offset, LogicalOffset::new(0));
    // Before this point a failed setup must not block a last factory destructor
    // on the test thread. Now the actual apply frame owns the last worker Arc.
    destruction.armed.store(true, Ordering::Release);
    let started = Instant::now();
    drop(engine);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "dispatcher destruction must detach the blocked callback"
    );
    assert_contended(&temp.0);
    control.block.store(false, Ordering::Release);
    wait_for_test(|| destruction.entered.load(Ordering::Acquire));
    assert_contended(&temp.0);
    destruction.release();
    let next = acquire_after_settlement(&temp.0);
    assert_eq!(control.state.lock().expect("sink state").appends.len(), 1);
    drop(next);
}

#[test]
fn directory_ownership_constructor_error_releases_only_after_started_workers_finish() {
    assert_constructor_error_holds_custody(true);
}

#[test]
fn directory_ownership_invalid_config_retains_owner_through_factory_destruction() {
    assert_constructor_error_holds_custody(false);
}

fn assert_constructor_error_holds_custody(fail_after_spawn: bool) {
    let temp = TempDir::new("owned-open-error");
    if fail_after_spawn {
        fs::create_dir(temp.0.join("coordinator.ssj")).expect("force genuine journal open failure");
    }
    let owner = DirectoryOwnership::acquire(&temp.0).expect("owner");
    let control = Arc::new(RecordingSinkControl::default());
    let destruction = Arc::new(DestructionBarrier::default());
    let mut release = ReleaseOpening {
        barrier: Arc::clone(&destruction),
        thread: None,
    };
    let sink = DurableSinkConfig::new(Arc::new(DestructionSinkFactory {
        inner: RecordingSinkFactory {
            control: Arc::clone(&control),
        },
        destruction: Arc::clone(&destruction),
    }))
    .with_options(DurableSinkOptions {
        worker_count: 1,
        ..DurableSinkOptions::default()
    });
    let mut engine_config = config(&temp.0);
    if !fail_after_spawn {
        engine_config.shard_count = 0;
    }
    let worker_destruction = Arc::clone(&destruction);
    release.thread = Some(std::thread::spawn(move || {
        worker_destruction.armed.store(true, Ordering::Release);
        StreamEngine::open_with_owned_components(engine_config, owner, None, None, None, Some(sink))
    }));
    wait_for_test(|| destruction.entered.load(Ordering::Acquire));
    assert_contended(&temp.0);
    destruction.release();
    let result = release
        .thread
        .take()
        .expect("opening thread")
        .join()
        .expect("opening result");
    if fail_after_spawn {
        assert!(matches!(result, Err(EngineError::Storage(_))));
    } else {
        assert!(matches!(result, Err(EngineError::InvalidConfig(_))));
    }
    let next = acquire_after_settlement(&temp.0);
    assert!(
        control
            .state
            .lock()
            .expect("no deliveries")
            .appends
            .is_empty()
    );
    drop(next);
}
