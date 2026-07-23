use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use serde::Serialize;
use shard_stream_core::{LogicalOffset, LogicalPartitionId, TopicId};
use shard_stream_engine::{EngineConfig, StreamEngine, TopicConfig};
use shard_stream_protocol::{AppendRequest, Durability, ProducerIdentity};

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
    /// Allow appending to an existing nonempty benchmark directory.
    #[arg(long, default_value_t = false)]
    reuse_data: bool,
    /// Give each benchmark worker an independent idempotent producer sequence.
    #[arg(long, default_value_t = false)]
    idempotent_producers: bool,
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
    idempotent_producers: bool,
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
    if !args.reuse_data {
        require_empty_directory(&data_dir, "--data-dir")?;
        if let Some(object_store_dir) = &object_store_dir {
            require_empty_directory(object_store_dir, "--object-store-dir")?;
        }
    }
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
        append_linger: std::time::Duration::from_millis(1),
    })?);
    engine.create_topic(TopicConfig {
        topic_id: TopicId::new(1),
        partitions: 1,
        shards: None,
    })?;

    let payload = Bytes::from(vec![0x5a; args.payload_bytes]);
    let next_batch = AtomicU64::new(0);
    let failed = AtomicBool::new(false);
    let start = Instant::now();
    let thread_results = std::thread::scope(|scope| {
        let mut workers = Vec::with_capacity(args.concurrency);
        for worker_index in 0..args.concurrency {
            let engine = Arc::clone(&engine);
            let payload = payload.clone();
            let next_batch = &next_batch;
            let failed = &failed;
            workers.push(scope.spawn(move || {
                let mut local_latencies = Vec::new();
                let mut local_first = None;
                let mut producer_sequence = 0u64;
                loop {
                    if failed.load(Ordering::Relaxed) {
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
                        payload: payload.clone(),
                        durability: args.durability.engine(),
                        producer: args.idempotent_producers.then_some(ProducerIdentity {
                            producer_id: worker_index as u128 + 1,
                            epoch: 0,
                            first_sequence: producer_sequence,
                        }),
                        leader_epoch: None,
                    }) {
                        Ok(response) => {
                            if args.idempotent_producers {
                                producer_sequence = producer_sequence
                                    .checked_add(u64::from(args.records_per_batch))
                                    .ok_or_else(|| "producer sequence overflow".to_owned())?;
                            }
                            local_latencies.push(append_start.elapsed());
                            local_first = Some(
                                local_first
                                    .map_or(response.first_offset, |current: LogicalOffset| {
                                        current.min(response.first_offset)
                                    }),
                            );
                        }
                        Err(error) => {
                            failed.store(true, Ordering::Relaxed);
                            return Err(error.to_string());
                        }
                    }
                }
                Ok((local_latencies, local_first))
            }));
        }
        workers
            .into_iter()
            .map(std::thread::ScopedJoinHandle::join)
            .collect::<Vec<_>>()
    });
    let mut latencies = Vec::with_capacity(usize::try_from(args.batches).unwrap_or(usize::MAX));
    let mut first_offset = None::<LogicalOffset>;
    for result in thread_results {
        let (mut local_latencies, local_first) =
            result
                .map_err(|_| "benchmark worker panicked")?
                .map_err(|error| format!("benchmark append failed: {error}"))?;
        latencies.append(&mut local_latencies);
        if let Some(local_first) = local_first {
            first_offset =
                Some(first_offset.map_or(local_first, |current| current.min(local_first)));
        }
    }
    engine.sync()?;
    let elapsed = start.elapsed();
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
        idempotent_producers: args.idempotent_producers,
        elapsed_seconds: seconds,
        batches_per_second: args.batches as f64 / seconds,
        records_per_second: records as f64 / seconds,
        mebibytes_per_second,
        latency,
        first_offset: first_offset.map_or_else(|| "0".into(), |offset| offset.to_string()),
        allocated_end: watermarks.allocated_end.to_string(),
        gates: GateDocument {
            min_mib_per_second: args.min_mib_per_second,
            max_p99_ms: args.max_p99_ms,
            passed: gates_passed,
        },
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
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

fn temporary_benchmark_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    std::env::temp_dir().join(format!("shard-stream-bench-{}-{nonce}", std::process::id()))
}

fn require_empty_directory(
    directory: &std::path::Path,
    argument: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if !directory.exists() {
        return Ok(());
    }
    if !directory.is_dir() {
        return Err(format!("{argument} must name a directory: {}", directory.display()).into());
    }
    if std::fs::read_dir(directory)?.next().transpose()?.is_some() {
        return Err(format!(
            "{argument} is nonempty; use a fresh directory or pass --reuse-data explicitly: {}",
            directory.display()
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_benchmark_directory_must_be_empty_by_default() {
        let directory = temporary_benchmark_dir();
        std::fs::create_dir_all(&directory).expect("create benchmark directory");
        require_empty_directory(&directory, "--data-dir").expect("empty directory");
        std::fs::write(directory.join("existing.sse"), b"existing").expect("write marker");
        let error = require_empty_directory(&directory, "--data-dir").expect_err("reject nonempty");
        assert!(error.to_string().contains("--reuse-data"));
        std::fs::remove_dir_all(directory).expect("remove benchmark directory");
    }
}
