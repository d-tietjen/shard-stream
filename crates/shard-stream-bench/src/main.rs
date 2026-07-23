use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Parser, ValueEnum};
use serde::Serialize;
use shard_stream_core::{LogicalOffset, LogicalPartitionId, TopicId};
use shard_stream_engine::{EngineConfig, StreamEngine, TopicConfig};
use shard_stream_protocol::{AppendRequest, Durability};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum DurabilityArg {
    Leader,
    Quorum,
    Object,
}

impl DurabilityArg {
    const fn engine(self) -> Durability {
        match self {
            Self::Leader => Durability::Leader,
            Self::Quorum => Durability::Quorum,
            Self::Object => Durability::Object,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Leader => "leader-fsync",
            Self::Quorum => "quorum-fsync",
            Self::Object => "object-manifest",
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "shard-stream-bench", version, about)]
struct Args {
    #[arg(long, default_value_t = 10_000)]
    batches: u64,
    #[arg(long, default_value_t = 256)]
    records_per_batch: u32,
    #[arg(long, default_value_t = 256 * 1024)]
    payload_bytes: usize,
    #[arg(long, default_value_t = 8)]
    shards: u32,
    #[arg(long, default_value_t = 1)]
    replication_factor: u32,
    #[arg(long, default_value_t = 1)]
    min_in_sync_replicas: u32,
    #[arg(long, default_value_t = 1)]
    concurrency: usize,
    #[arg(long, value_enum, default_value_t = DurabilityArg::Leader)]
    durability: DurabilityArg,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    object_store_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    keep_data: bool,
    /// Fail the process when payload throughput is below this gate.
    #[arg(long)]
    min_mib_per_second: Option<f64>,
    /// Fail the process when append p99 exceeds this gate.
    #[arg(long)]
    max_p99_ms: Option<f64>,
}

#[derive(Debug, Serialize)]
struct LatencyDocument {
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    p999_ms: f64,
    max_ms: f64,
}

#[derive(Debug, Serialize)]
struct GateDocument {
    min_mib_per_second: Option<f64>,
    max_p99_ms: Option<f64>,
    passed: bool,
}

#[derive(Debug, Serialize)]
struct ResultDocument {
    format_version: u32,
    durability: &'static str,
    shards: u32,
    replication_factor: u32,
    min_in_sync_replicas: u32,
    concurrency: usize,
    batches: u64,
    records: u64,
    payload_bytes: u64,
    batch_payload_bytes: usize,
    records_per_batch: u32,
    elapsed_seconds: f64,
    batches_per_second: f64,
    records_per_second: f64,
    mebibytes_per_second: f64,
    latency: LatencyDocument,
    first_offset: String,
    allocated_end: String,
    gates: GateDocument,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    validate_args(&args)?;
    let generated_data_dir = args.data_dir.is_none();
    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(temporary_benchmark_dir);
    let generated_object_dir =
        matches!(args.durability, DurabilityArg::Object) && args.object_store_dir.is_none();
    let object_store_dir = match (args.durability, args.object_store_dir.clone()) {
        (DurabilityArg::Object, Some(directory)) => Some(directory),
        (DurabilityArg::Object, None) => Some(data_dir.with_extension("objects")),
        (_, directory) => directory,
    };
    let queue_bytes = args
        .payload_bytes
        .checked_mul(args.concurrency.max(64))
        .and_then(|bytes| bytes.checked_add(64 * 1024 * 1024))
        .ok_or("queue byte configuration overflow")?;
    let engine = Arc::new(StreamEngine::open(EngineConfig {
        data_dir: data_dir.clone(),
        object_store_dir: object_store_dir.clone(),
        shard_count: args.shards,
        replication_factor: args.replication_factor,
        min_in_sync_replicas: args.min_in_sync_replicas,
        queue_slots_per_shard: args.concurrency.saturating_mul(4).max(4096),
        queue_bytes_per_shard: queue_bytes,
        target_pack_bytes: 256 * 1024 * 1024,
        max_batch_bytes: args.payload_bytes,
        max_fetch_bytes: args.payload_bytes.max(1024 * 1024),
    })?);
    engine.create_topic(TopicConfig {
        topic_id: TopicId::new(1),
        partitions: 1,
        shards: None,
    })?;

    let payload = Arc::new(vec![0x5a; args.payload_bytes]);
    let next_batch = AtomicU64::new(0);
    let latencies = Mutex::new(Vec::with_capacity(
        usize::try_from(args.batches).unwrap_or(usize::MAX),
    ));
    let first_offset = Mutex::new(None::<LogicalOffset>);
    let failure = Mutex::new(None::<String>);
    let start = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..args.concurrency {
            let engine = Arc::clone(&engine);
            let payload = Arc::clone(&payload);
            let next_batch = &next_batch;
            let latencies = &latencies;
            let first_offset = &first_offset;
            let failure = &failure;
            scope.spawn(move || {
                let mut local_latencies = Vec::new();
                let mut local_first = None;
                loop {
                    if lock(failure).is_some() {
                        break;
                    }
                    let batch = next_batch.fetch_add(1, Ordering::Relaxed);
                    if batch >= args.batches {
                        break;
                    }
                    let append_start = Instant::now();
                    match engine.append(AppendRequest {
                        request_id: u128::from(batch),
                        topic_id: TopicId::new(1),
                        partition_id: LogicalPartitionId::new(0),
                        record_count: args.records_per_batch,
                        payload: payload.as_ref().clone(),
                        durability: args.durability.engine(),
                        producer: None,
                        leader_epoch: None,
                    }) {
                        Ok(response) => {
                            local_latencies.push(append_start.elapsed());
                            local_first = Some(
                                local_first
                                    .map_or(response.first_offset, |current: LogicalOffset| {
                                        current.min(response.first_offset)
                                    }),
                            );
                        }
                        Err(error) => {
                            *lock(failure) = Some(error.to_string());
                            break;
                        }
                    }
                }
                lock(latencies).extend(local_latencies);
                if let Some(local_first) = local_first {
                    let mut first = lock(first_offset);
                    *first = Some(first.map_or(local_first, |current| current.min(local_first)));
                }
            });
        }
    });
    if let Some(error) = lock(&failure).take() {
        return Err(error.into());
    }
    engine.sync()?;
    let elapsed = start.elapsed();
    let mut latencies = lock(&latencies);
    if latencies.len() != args.batches as usize {
        return Err(format!(
            "benchmark completed {} of {} batches",
            latencies.len(),
            args.batches
        )
        .into());
    }
    latencies.sort_unstable();
    let latency = LatencyDocument {
        p50_ms: duration_ms(quantile(&latencies, 0.50)),
        p95_ms: duration_ms(quantile(&latencies, 0.95)),
        p99_ms: duration_ms(quantile(&latencies, 0.99)),
        p999_ms: duration_ms(quantile(&latencies, 0.999)),
        max_ms: duration_ms(*latencies.last().expect("nonempty latencies")),
    };
    let records = args
        .batches
        .checked_mul(u64::from(args.records_per_batch))
        .ok_or("record count overflow")?;
    let payload_bytes = args
        .batches
        .checked_mul(args.payload_bytes as u64)
        .ok_or("payload byte count overflow")?;
    let seconds = elapsed.as_secs_f64();
    let mebibytes_per_second = payload_bytes as f64 / (1024.0 * 1024.0) / seconds;
    let gates_passed = args
        .min_mib_per_second
        .is_none_or(|minimum| mebibytes_per_second >= minimum)
        && args
            .max_p99_ms
            .is_none_or(|maximum| latency.p99_ms <= maximum);
    let watermarks = engine.watermarks(shard_stream_core::TopicPartition::new(
        TopicId::new(1),
        LogicalPartitionId::new(0),
    ))?;
    let result = ResultDocument {
        format_version: 1,
        durability: args.durability.name(),
        shards: args.shards,
        replication_factor: args.replication_factor,
        min_in_sync_replicas: args.min_in_sync_replicas,
        concurrency: args.concurrency,
        batches: args.batches,
        records,
        payload_bytes,
        batch_payload_bytes: args.payload_bytes,
        records_per_batch: args.records_per_batch,
        elapsed_seconds: seconds,
        batches_per_second: args.batches as f64 / seconds,
        records_per_second: records as f64 / seconds,
        mebibytes_per_second,
        latency,
        first_offset: lock(&first_offset).map_or_else(|| "0".into(), |offset| offset.to_string()),
        allocated_end: watermarks.allocated_end.to_string(),
        gates: GateDocument {
            min_mib_per_second: args.min_mib_per_second,
            max_p99_ms: args.max_p99_ms,
            passed: gates_passed,
        },
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    drop(latencies);
    drop(engine);
    if generated_data_dir && !args.keep_data {
        std::fs::remove_dir_all(&data_dir)?;
    }
    if generated_object_dir
        && !args.keep_data
        && let Some(object_store_dir) = object_store_dir
        && object_store_dir.exists()
    {
        std::fs::remove_dir_all(object_store_dir)?;
    }
    if !gates_passed {
        return Err("one or more benchmark gates failed".into());
    }
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    if args.batches == 0
        || args.records_per_batch == 0
        || args.payload_bytes == 0
        || args.shards == 0
        || args.replication_factor == 0
        || args.min_in_sync_replicas == 0
        || args.concurrency == 0
    {
        return Err(
            "benchmark counts, sizes, shards, replicas, ISR, and concurrency must be nonzero"
                .into(),
        );
    }
    if args.min_in_sync_replicas > args.replication_factor {
        return Err("min_in_sync_replicas cannot exceed replication_factor".into());
    }
    for value in [args.min_mib_per_second, args.max_p99_ms]
        .into_iter()
        .flatten()
    {
        if !value.is_finite() || value <= 0.0 {
            return Err("benchmark gates must be finite positive numbers".into());
        }
    }
    Ok(())
}

fn quantile(sorted: &[Duration], quantile: f64) -> Duration {
    let index = ((sorted.len() - 1) as f64 * quantile).ceil() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn temporary_benchmark_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    std::env::temp_dir().join(format!("shard-stream-bench-{}-{nonce}", std::process::id()))
}
