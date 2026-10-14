//! Real shard/WAL/sync ownership with a bounded local admission implementation.
//! These fixtures qualify public command lifetime, not an external authority.

use super::*;
use std::fs;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use shard_stream_core::{LogicalPartitionId, RingEpoch, TopicId, TopicSequencer, WriteFenceError};

#[derive(Debug, Default)]
struct FixedAdmission {
    active: Arc<AtomicUsize>,
    refuse: AtomicBool,
    panic_once: AtomicBool,
}

#[derive(Debug)]
struct FixedGuard(Arc<AtomicUsize>);

impl PhysicalWriteGuard for FixedGuard {}

impl Drop for FixedGuard {
    fn drop(&mut self) {
        assert_eq!(self.0.fetch_sub(1, Ordering::AcqRel), 1);
    }
}

impl PendingWriteAdmission for FixedAdmission {
    fn validate_before_reserve(&self) -> Result<(), WriteFenceError> {
        Ok(())
    }

    fn try_admit_physical(
        &self,
        reservation: &Reservation,
    ) -> Result<Box<dyn PhysicalWriteGuard>, WriteFenceError> {
        assert_eq!(reservation.topic_id, TopicId::new(1));
        assert_eq!(reservation.placement.ring_epoch, RingEpoch::new(1));
        if self.panic_once.swap(false, Ordering::AcqRel) {
            panic!("bounded admission fixture panic");
        }
        if self.refuse.load(Ordering::Acquire) {
            return Err(WriteFenceError::new("bounded admission fixture refusal"));
        }
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active == 0).then_some(1)
            })
            .map_err(|_| WriteFenceError::new("bounded fixture guard capacity"))?;
        Ok(Box::new(FixedGuard(Arc::clone(&self.active))))
    }
}

struct Fixture {
    directory: PathBuf,
    sender: Option<ByteBoundedSender<ShardCommand>>,
    sync_receiver: Option<Receiver<SyncRequest>>,
    worker: Option<JoinHandle<()>>,
    sequencer: TopicSequencer,
}

impl Fixture {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("fixture clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-stream-physical-admission-{}-{nonce}",
            std::process::id(),
        ));
        let primary = ShardStore::open(ShardStoreConfig {
            directory: directory.clone(),
            shard_id: ShardId::new(0),
            target_pack_bytes: 64 * 1024,
            max_batch_bytes: 64 * 1024,
        })
        .expect("actual shard store");
        let (sender, receiver) =
            byte_bounded_channel(1, 2 + COMMAND_OVERHEAD_BYTES + ADMISSION_METADATA_BYTES)
                .expect("actual single-command payload allowance");
        let (sync_sender, sync_receiver) = sync_channel(1);
        let worker = thread::spawn(move || {
            run_worker(
                LaneStores {
                    primary,
                    tier: None,
                },
                receiver,
                Duration::ZERO,
                Duration::from_secs(60),
                sync_sender,
            )
        });
        Self {
            directory,
            sender: Some(sender),
            sync_receiver: Some(sync_receiver),
            worker: Some(worker),
            sequencer: TopicSequencer::new(
                TopicId::new(1),
                LogicalPartitionId::new(0),
                RingEpoch::new(1),
                vec![ShardId::new(0)],
            )
            .expect("actual reservation sequencer"),
        }
    }

    fn command(
        &mut self,
        admission: &Arc<FixedAdmission>,
    ) -> (
        ShardCommand,
        Receiver<Result<AppendDurability, AppendFailure>>,
    ) {
        let (response, receiver) = sync_channel(1);
        let reservation = self
            .sequencer
            .reserve(NonZeroU32::new(1).expect("record count"))
            .expect("actual next reservation");
        (
            ShardCommand::Append(AppendCommand {
                reservation,
                producer: None,
                atomic_group: None,
                payload: Bytes::from_static(b"ok"),
                object_durable: false,
                admission: Some(admission.clone()),
                _physical_guard: None,
                _byte_charge: None,
                response,
            }),
            receiver,
        )
    }

    fn send(&self, command: ShardCommand) {
        self.sender
            .as_ref()
            .expect("live shard sender")
            .try_send(
                command,
                2 + COMMAND_OVERHEAD_BYTES + ADMISSION_METADATA_BYTES,
            )
            .unwrap_or_else(|_| panic!("actual shard admission unexpectedly refused"));
    }

    fn request(&self) -> SyncRequest {
        self.sync_receiver
            .as_ref()
            .expect("live sync receiver")
            .recv_timeout(Duration::from_secs(3))
            .expect("actual WAL reached sync queue")
    }

    fn settle(request: SyncRequest) {
        let (sender, receiver) = sync_channel(1);
        sender
            .send(request)
            .unwrap_or_else(|_| panic!("actual sync dispatch"));
        drop(sender);
        run_sync_worker(receiver);
    }

    fn stop(&mut self) {
        self.sender.take();
        self.sync_receiver.take();
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .expect("actual shard thread survives admission errors");
        }
    }

    fn readback(&mut self) -> Vec<StoredBatch> {
        self.stop();
        let mut store = ShardStore::open(ShardStoreConfig {
            directory: self.directory.clone(),
            shard_id: ShardId::new(0),
            target_pack_bytes: 64 * 1024,
            max_batch_bytes: 64 * 1024,
        })
        .expect("reopen actual WAL");
        let planned = store
            .plan_fetch(
                TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0)),
                LogicalOffset::new(0),
                1024,
                8,
            )
            .expect("plan actual persisted reservations");
        store
            .read_fetch_plan(&planned)
            .expect("checksummed actual WAL")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop();
        let _ = fs::remove_dir_all(&self.directory);
    }
}

#[test]
fn physical_sync_retains_guard_and_charge_after_caller_cancellation() {
    let mut fixture = Fixture::new();
    let admission = Arc::new(FixedAdmission::default());
    let (command, response) = fixture.command(&admission);
    fixture.send(command);
    let request = fixture.request();
    drop(response);
    assert_eq!(admission.active.load(Ordering::Acquire), 1);
    let sender = fixture.sender.as_ref().expect("actual sender");
    assert_eq!(sender.stats().used_bytes, sender.stats().max_bytes);
    let (blocked, _) = fixture.command(&admission);
    assert!(matches!(
        fixture.sender.as_ref().expect("actual sender").try_send(
            blocked,
            2 + COMMAND_OVERHEAD_BYTES + ADMISSION_METADATA_BYTES,
        ),
        Err(TrySendError::ByteBudget { .. })
    ));
    assert_eq!(admission.active.load(Ordering::Acquire), 1);
    Fixture::settle(request);
    assert_eq!(admission.active.load(Ordering::Acquire), 0);
    assert_eq!(
        fixture
            .sender
            .as_ref()
            .expect("actual sender")
            .stats()
            .used_bytes,
        0
    );
    let (next, response) = fixture.command(&admission);
    fixture.send(next);
    Fixture::settle(fixture.request());
    response
        .recv_timeout(Duration::from_secs(3))
        .expect("settled response")
        .expect("actual later physical progress");
    let wal = fixture.readback();
    assert_eq!(wal.len(), 2);
    assert_eq!(wal[0].payload, Bytes::from_static(b"ok"));
    assert_eq!(wal[1].payload, Bytes::from_static(b"ok"));
    assert_eq!(wal[0].reservation.first_offset, LogicalOffset::new(0));
    assert_eq!(wal[1].reservation.first_offset, LogicalOffset::new(2));
    assert_eq!(admission.active.load(Ordering::Acquire), 0);
}

#[test]
fn physical_refusal_and_panic_release_capacity_without_stopping_real_shard() {
    let mut fixture = Fixture::new();
    let admission = Arc::new(FixedAdmission::default());
    for panic in [false, true] {
        admission.refuse.store(!panic, Ordering::Release);
        admission.panic_once.store(panic, Ordering::Release);
        let (command, response) = fixture.command(&admission);
        fixture.send(command);
        let refusal = response
            .recv_timeout(Duration::from_secs(3))
            .expect("explicit refusal")
            .expect_err("refused before physical mutation");
        assert!(refusal.definitely_not_written);
        assert!(matches!(refusal.error, EngineError::Fenced(_)));
        assert_eq!(admission.active.load(Ordering::Acquire), 0);
        assert_eq!(
            fixture
                .sender
                .as_ref()
                .expect("actual sender")
                .stats()
                .used_bytes,
            0,
            "refusal result must follow physical capacity release"
        );
    }
    admission.refuse.store(false, Ordering::Release);
    let (command, response) = fixture.command(&admission);
    fixture.send(command);
    Fixture::settle(fixture.request());
    response
        .recv_timeout(Duration::from_secs(3))
        .expect("later response")
        .expect("real shard survives admission panic");
    let wal = fixture.readback();
    assert_eq!(wal.len(), 1);
    assert_eq!(wal[0].reservation.first_offset, LogicalOffset::new(2));
    assert_eq!(admission.active.load(Ordering::Acquire), 0);
}

#[test]
fn physical_sync_dispatch_failure_is_ambiguous_and_releases_guard_and_charge() {
    let mut fixture = Fixture::new();
    fixture.sync_receiver.take();
    let admission = Arc::new(FixedAdmission::default());
    let (command, response) = fixture.command(&admission);
    fixture.send(command);
    let failure = response
        .recv_timeout(Duration::from_secs(3))
        .expect("physical sync failure")
        .expect_err("closed sync dispatcher cannot acknowledge durability");
    assert!(!failure.definitely_not_written);
    let wal = fixture.readback();
    assert_eq!(
        wal.len(),
        1,
        "real WAL mutation must not be classified as refused"
    );
    assert_eq!(admission.active.load(Ordering::Acquire), 0);
}
