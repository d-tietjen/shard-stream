use std::fs;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use shard_stream_core::{
    LogicalOffset, LogicalPartitionId, PlacementSequence, ShardId, TopicId, TopicPartition,
};
use shard_stream_engine::{
    DurableAppend, DurableAppendSink, DurableAppendSinkFactory, DurableSinkApply,
    DurableSinkCheckpoint, DurableSinkConfig, EngineConfig, EngineError, EngineResult,
    StreamEngine, TopicConfig,
};
use shard_stream_protocol::{AppendRequest, Durability};

const CHILD_ENV: &str = "SHARD_STREAM_DURABLE_SINK_CRASH_CHILD";
const DIRECTORY_ENV: &str = "SHARD_STREAM_DURABLE_SINK_CRASH_DIRECTORY";

#[derive(Debug)]
struct FileSinkFactory {
    state_path: PathBuf,
    entered_path: PathBuf,
    block_at_offset: Option<u64>,
    transaction: Arc<Mutex<()>>,
}

#[derive(Debug)]
struct FileSink {
    state_path: PathBuf,
    entered_path: PathBuf,
    block_at_offset: Option<u64>,
    transaction: Arc<Mutex<()>>,
}

impl DurableAppendSinkFactory for FileSinkFactory {
    fn validate_append(&self, _payload: &[u8], _record_count: NonZeroU32) -> EngineResult<()> {
        Ok(())
    }

    fn load_checkpoint(
        &self,
        topic_partition: TopicPartition,
    ) -> EngineResult<Option<DurableSinkCheckpoint>> {
        let _transaction = self
            .transaction
            .lock()
            .map_err(|_| EngineError::DurableSinkUnavailable("test sink lock poisoned".into()))?;
        read_state(&self.state_path, topic_partition)
            .map(|state| state.map(|(checkpoint, _)| checkpoint))
    }

    fn open_shard(&self, _shard_id: ShardId) -> EngineResult<Arc<dyn DurableAppendSink>> {
        Ok(Arc::new(FileSink {
            state_path: self.state_path.clone(),
            entered_path: self.entered_path.clone(),
            block_at_offset: self.block_at_offset,
            transaction: Arc::clone(&self.transaction),
        }))
    }
}

impl DurableAppendSink for FileSink {
    fn apply(
        &self,
        expected: DurableSinkCheckpoint,
        appends: &[DurableAppend],
        next: DurableSinkCheckpoint,
    ) -> EngineResult<DurableSinkApply> {
        if self.block_at_offset == Some(expected.next_offset.get()) {
            fs::write(&self.entered_path, b"entered").map_err(sink_io)?;
            loop {
                std::thread::park();
            }
        }
        let _transaction = self
            .transaction
            .lock()
            .map_err(|_| EngineError::DurableSinkUnavailable("test sink lock poisoned".into()))?;
        let (actual, mut payloads) = read_state(&self.state_path, expected.topic_partition)?
            .unwrap_or_else(|| {
                (
                    DurableSinkCheckpoint::initial(expected.topic_partition),
                    Vec::new(),
                )
            });
        if actual != expected {
            return Ok(DurableSinkApply::CheckpointConflict(actual));
        }
        payloads.extend(appends.iter().map(|append| append.payload.clone()));
        write_state(&self.state_path, next, &payloads)?;
        Ok(DurableSinkApply::Applied)
    }
}

#[test]
fn crash_after_notification_enqueue_replays_from_retained_wal() {
    if std::env::var_os(CHILD_ENV).is_some() {
        run_crashing_child();
    }

    let directory = temporary_directory();
    fs::create_dir_all(&directory).expect("create crash test directory");
    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("crash_after_notification_enqueue_replays_from_retained_wal")
        .arg("--test-threads=1")
        .env(CHILD_ENV, "1")
        .env(DIRECTORY_ENV, &directory)
        .status()
        .expect("spawn crash child");
    assert!(!status.success(), "crash child unexpectedly exited cleanly");

    let sink = sink_config(&directory, None);
    let engine =
        StreamEngine::open_with_durable_sink(engine_config(&directory), sink).expect("recover");
    let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
    let checkpoint = engine
        .durable_sink_checkpoint(partition)
        .expect("checkpoint query")
        .expect("sink checkpoint");
    assert_eq!(
        checkpoint.next_placement_sequence,
        PlacementSequence::new(1)
    );
    assert_eq!(checkpoint.next_offset, LogicalOffset::new(1));
    let (_, payloads) = read_state(&directory.join("sink.state"), partition)
        .expect("read sink")
        .expect("sink state");
    assert_eq!(
        payloads,
        vec![Bytes::from_static(b"survives-process-abort")]
    );
    drop(engine);
    fs::remove_dir_all(directory).expect("remove crash test directory");
}

#[test]
fn crash_after_checkpoint_does_not_duplicate_committed_sink_event() {
    if std::env::var_os(CHILD_ENV).is_some() {
        run_checkpoint_crashing_child();
    }

    let directory = temporary_directory();
    spawn_crash_child(
        &directory,
        "crash_after_checkpoint_does_not_duplicate_committed_sink_event",
    );
    let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
    let engine = StreamEngine::open_with_durable_sink(
        engine_config(&directory),
        sink_config(&directory, None),
    )
    .expect("recover after checkpoint");
    let checkpoint = engine
        .durable_sink_checkpoint(partition)
        .expect("checkpoint query")
        .expect("checkpoint");
    assert_eq!(checkpoint.next_offset, LogicalOffset::new(2));
    let (_, payloads) = read_state(&directory.join("sink.state"), partition)
        .expect("read sink")
        .expect("sink state");
    assert_eq!(
        payloads,
        vec![
            Bytes::from_static(b"checkpointed"),
            Bytes::from_static(b"after-checkpoint")
        ]
    );
    drop(engine);
    fs::remove_dir_all(directory).expect("remove crash test directory");
}

#[test]
fn crash_across_pack_rotation_replays_every_durable_batch() {
    if std::env::var_os(CHILD_ENV).is_some() {
        run_pack_rotation_crashing_child();
    }

    let directory = temporary_directory();
    spawn_crash_child(
        &directory,
        "crash_across_pack_rotation_replays_every_durable_batch",
    );
    let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
    let engine = StreamEngine::open_with_durable_sink(
        engine_config(&directory),
        sink_config(&directory, None),
    )
    .expect("recover rotated packs");
    let checkpoint = engine
        .durable_sink_checkpoint(partition)
        .expect("checkpoint query")
        .expect("checkpoint");
    assert_eq!(checkpoint.next_offset, LogicalOffset::new(12));
    let (_, payloads) = read_state(&directory.join("sink.state"), partition)
        .expect("read sink")
        .expect("sink state");
    assert_eq!(payloads.len(), 12);
    for (index, payload) in payloads.iter().enumerate() {
        assert!(payload.iter().all(|&byte| byte == index as u8));
    }
    drop(engine);
    fs::remove_dir_all(directory).expect("remove crash test directory");
}

fn run_crashing_child() -> ! {
    let directory =
        PathBuf::from(std::env::var_os(DIRECTORY_ENV).expect("crash test directory environment"));
    let engine = StreamEngine::open_with_durable_sink(
        engine_config(&directory),
        sink_config(&directory, Some(0)),
    )
    .expect("open child engine");
    engine
        .create_topic(TopicConfig {
            topic_id: TopicId::new(1),
            partitions: 1,
            shards: None,
        })
        .expect("create child topic");
    engine
        .append(AppendRequest {
            request_id: 1,
            topic_id: TopicId::new(1),
            partition_id: LogicalPartitionId::new(0),
            record_count: 1,
            payload: Bytes::from_static(b"survives-process-abort"),
            durability: Durability::Leader,
            producer: None,
            atomic_group: None,
            leader_epoch: None,
            extension_context: None,
        })
        .expect("append is acknowledged after WAL fsync");

    let entered = directory.join("sink.entered");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !entered.exists() {
        assert!(Instant::now() < deadline, "sink callback did not start");
        std::thread::sleep(Duration::from_millis(2));
    }
    std::process::abort();
}

fn run_checkpoint_crashing_child() -> ! {
    let directory =
        PathBuf::from(std::env::var_os(DIRECTORY_ENV).expect("crash test directory environment"));
    let engine = StreamEngine::open_with_durable_sink(
        engine_config(&directory),
        sink_config(&directory, Some(1)),
    )
    .expect("open child engine");
    engine
        .create_topic(TopicConfig {
            topic_id: TopicId::new(1),
            partitions: 1,
            shards: None,
        })
        .expect("create child topic");
    append_child(&engine, 1, Bytes::from_static(b"checkpointed"));

    let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if read_state(&directory.join("sink.state"), partition)
            .expect("read checkpoint")
            .is_some_and(|(checkpoint, _)| checkpoint.next_offset == LogicalOffset::new(1))
        {
            break;
        }
        assert!(Instant::now() < deadline, "first checkpoint did not commit");
        std::thread::sleep(Duration::from_millis(2));
    }
    append_child(&engine, 2, Bytes::from_static(b"after-checkpoint"));
    wait_for_entered(&directory);
    std::process::abort();
}

fn run_pack_rotation_crashing_child() -> ! {
    let directory =
        PathBuf::from(std::env::var_os(DIRECTORY_ENV).expect("crash test directory environment"));
    let engine = StreamEngine::open_with_durable_sink(
        engine_config(&directory),
        sink_config(&directory, Some(0)),
    )
    .expect("open child engine");
    engine
        .create_topic(TopicConfig {
            topic_id: TopicId::new(1),
            partitions: 1,
            shards: None,
        })
        .expect("create child topic");
    for index in 0..12_u8 {
        append_child(&engine, u128::from(index), Bytes::from(vec![index; 512]));
    }
    wait_for_entered(&directory);
    std::process::abort();
}

fn append_child(engine: &StreamEngine, request_id: u128, payload: Bytes) {
    engine
        .append(AppendRequest {
            request_id,
            topic_id: TopicId::new(1),
            partition_id: LogicalPartitionId::new(0),
            record_count: 1,
            payload,
            durability: Durability::Leader,
            producer: None,
            atomic_group: None,
            leader_epoch: None,
            extension_context: None,
        })
        .expect("append is acknowledged after WAL fsync");
}

fn wait_for_entered(directory: &Path) {
    let entered = directory.join("sink.entered");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !entered.exists() {
        assert!(Instant::now() < deadline, "sink callback did not start");
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn spawn_crash_child(directory: &Path, test_name: &str) {
    fs::create_dir_all(directory).expect("create crash test directory");
    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg(test_name)
        .arg("--test-threads=1")
        .env(CHILD_ENV, "1")
        .env(DIRECTORY_ENV, directory)
        .status()
        .expect("spawn crash child");
    assert!(!status.success(), "crash child unexpectedly exited cleanly");
}

fn sink_config(directory: &Path, block_at_offset: Option<u64>) -> DurableSinkConfig {
    DurableSinkConfig::new(Arc::new(FileSinkFactory {
        state_path: directory.join("sink.state"),
        entered_path: directory.join("sink.entered"),
        block_at_offset,
        transaction: Arc::new(Mutex::new(())),
    }))
}

fn engine_config(directory: &Path) -> EngineConfig {
    EngineConfig {
        data_dir: directory.join("engine"),
        object_store_dir: None,
        shard_count: 2,
        virtual_lane_count: 4,
        replication_factor: 1,
        min_in_sync_replicas: 1,
        queue_slots_per_shard: 64,
        queue_bytes_per_shard: 2 * 1024 * 1024,
        target_pack_bytes: 1024,
        max_pack_age: Duration::from_millis(20),
        max_batch_bytes: 64 * 1024,
        max_fetch_bytes: 1024 * 1024,
        append_linger: Duration::from_millis(1),
    }
}

fn temporary_directory() -> PathBuf {
    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "shard-stream-durable-sink-crash-{}-{nonce}-{sequence}",
        std::process::id(),
    ))
}

fn read_state(
    path: &Path,
    topic_partition: TopicPartition,
) -> EngineResult<Option<(DurableSinkCheckpoint, Vec<Bytes>)>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(sink_io(error)),
    };
    if bytes.len() < 20 {
        return Err(EngineError::DurableSinkCheckpoint(
            "test sink state is truncated".into(),
        ));
    }
    let sequence = u64::from_le_bytes(bytes[0..8].try_into().expect("sequence bytes"));
    let offset = u64::from_le_bytes(bytes[8..16].try_into().expect("offset bytes"));
    let count = u32::from_le_bytes(bytes[16..20].try_into().expect("count bytes"));
    let mut cursor = 20;
    let mut payloads = Vec::with_capacity(count as usize);
    for _ in 0..count {
        if bytes.len().saturating_sub(cursor) < 4 {
            return Err(EngineError::DurableSinkCheckpoint(
                "test sink payload length is truncated".into(),
            ));
        }
        let length = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().expect("length bytes"))
            as usize;
        cursor += 4;
        let end = cursor.checked_add(length).ok_or_else(|| {
            EngineError::DurableSinkCheckpoint("test sink payload length overflow".into())
        })?;
        let payload = bytes.get(cursor..end).ok_or_else(|| {
            EngineError::DurableSinkCheckpoint("test sink payload is truncated".into())
        })?;
        payloads.push(Bytes::copy_from_slice(payload));
        cursor = end;
    }
    if cursor != bytes.len() {
        return Err(EngineError::DurableSinkCheckpoint(
            "test sink state has trailing bytes".into(),
        ));
    }
    Ok(Some((
        DurableSinkCheckpoint {
            topic_partition,
            next_placement_sequence: PlacementSequence::new(sequence),
            next_offset: LogicalOffset::new(offset),
        },
        payloads,
    )))
}

fn write_state(
    path: &Path,
    checkpoint: DurableSinkCheckpoint,
    payloads: &[Bytes],
) -> EngineResult<()> {
    let count = u32::try_from(payloads.len())
        .map_err(|_| EngineError::DurableSinkUnavailable("too many test sink payloads".into()))?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&checkpoint.next_placement_sequence.get().to_le_bytes());
    encoded.extend_from_slice(&checkpoint.next_offset.get().to_le_bytes());
    encoded.extend_from_slice(&count.to_le_bytes());
    for payload in payloads {
        let length = u32::try_from(payload.len()).map_err(|_| {
            EngineError::DurableSinkUnavailable("test sink payload is too large".into())
        })?;
        encoded.extend_from_slice(&length.to_le_bytes());
        encoded.extend_from_slice(payload);
    }
    let temporary = path.with_extension("state.next");
    {
        let file = fs::File::create(&temporary).map_err(sink_io)?;
        use std::io::Write;
        let mut writer = std::io::BufWriter::new(file);
        writer.write_all(&encoded).map_err(sink_io)?;
        writer.flush().map_err(sink_io)?;
        writer.get_ref().sync_all().map_err(sink_io)?;
    }
    fs::rename(temporary, path).map_err(sink_io)?;
    if let Some(parent) = path.parent() {
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(sink_io)?;
    }
    Ok(())
}

fn sink_io(error: std::io::Error) -> EngineError {
    EngineError::DurableSinkUnavailable(format!("test sink I/O failed: {error}"))
}
