use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use clap::Parser;
use serde::Serialize;
use shard_stream_core::{LogicalOffset, LogicalPartitionId, TopicId, TopicPartition};
use shard_stream_engine::{EngineConfig, StreamEngine, TopicConfig};
use shard_stream_protocol::{AppendRequest, Durability, FetchMode, FetchRequest};

#[derive(Debug, Parser)]
#[command(name = "shard-stream-fetch-bench", version, about)]
struct Args {
    #[arg(long, default_value_t = 16_384)]
    batches: u64,
    #[arg(long, default_value_t = 32 * 1024)]
    payload_bytes: usize,
    #[arg(long, default_value_t = 8)]
    shards: u32,
    #[arg(long, default_value_t = 64)]
    prepare_concurrency: usize,
    #[arg(long, default_value_t = 1024 * 1024)]
    fetch_bytes: usize,
    #[arg(long, default_value_t = 1)]
    warmup_passes: u32,
    #[arg(long, default_value_t = 1)]
    measured_passes: u32,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Reopen a prepared benchmark directory after verifying its logical end.
    #[arg(long, default_value_t = false)]
    reuse_data: bool,
    #[arg(long, default_value_t = false)]
    keep_data: bool,
    #[arg(long)]
    min_mib_per_second: Option<f64>,
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
struct ResultDocument {
    format_version: u32,
    workload: &'static str,
    shards: u32,
    batches_per_pass: u64,
    batch_payload_bytes: usize,
    fetch_max_bytes: usize,
    warmup_passes: u32,
    measured_passes: u32,
    reused_data: bool,
    fetches: u64,
    fetched_batches: u64,
    payload_bytes: u64,
    elapsed_seconds: f64,
    fetches_per_second: f64,
    batches_per_second: f64,
    mebibytes_per_second: f64,
    latency: LatencyDocument,
    min_mib_per_second: Option<f64>,
    max_p99_ms: Option<f64>,
    gates_passed: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    validate_args(&args)?;
    let generated_data_dir = args.data_dir.is_none();
    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(temporary_benchmark_dir);
    if !args.reuse_data {
        require_empty_directory(&data_dir)?;
    }

    let queue_bytes_per_shard = args
        .fetch_bytes
        .max(args.payload_bytes)
        .checked_add(64 * 1024 * 1024)
        .ok_or("queue byte configuration overflow")?;
    let engine = Arc::new(StreamEngine::open(EngineConfig {
        data_dir: data_dir.clone(),
        object_store_dir: None,
        shard_count: args.shards,
        replication_factor: 1,
        min_in_sync_replicas: 1,
        queue_slots_per_shard: args.prepare_concurrency.saturating_mul(4).max(4096),
        queue_bytes_per_shard,
        target_pack_bytes: 256 * 1024 * 1024,
        max_batch_bytes: args.payload_bytes,
        max_fetch_bytes: args.fetch_bytes,
        append_linger: std::time::Duration::from_millis(1),
    })?);
    let topic_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
    if args.reuse_data {
        let watermarks = engine.watermarks(topic_partition)?;
        if watermarks.allocated_end != LogicalOffset::new(u128::from(args.batches)) {
            return Err(format!(
                "prepared log end is {}, expected {}",
                watermarks.allocated_end, args.batches
            )
            .into());
        }
    } else {
        engine.create_topic(TopicConfig {
            topic_id: TopicId::new(1),
            partitions: 1,
            shards: None,
        })?;
        prepare(&engine, &args)?;
        engine.sync()?;
    }

    for _ in 0..args.warmup_passes {
        let _ = consume_pass(&engine, &args, None)?;
    }

    let mut latencies = Vec::new();
    let start = Instant::now();
    let mut fetches = 0u64;
    let mut fetched_batches = 0u64;
    let mut payload_bytes = 0u64;
    for _ in 0..args.measured_passes {
        let pass = consume_pass(&engine, &args, Some(&mut latencies))?;
        fetches = fetches
            .checked_add(pass.fetches)
            .ok_or("fetch count overflow")?;
        fetched_batches = fetched_batches
            .checked_add(pass.batches)
            .ok_or("batch count overflow")?;
        payload_bytes = payload_bytes
            .checked_add(pass.payload_bytes)
            .ok_or("payload byte count overflow")?;
    }
    let elapsed = start.elapsed();
    latencies.sort_unstable();
    let latency = LatencyDocument {
        p50_ms: duration_ms(quantile(&latencies, 0.50)),
        p95_ms: duration_ms(quantile(&latencies, 0.95)),
        p99_ms: duration_ms(quantile(&latencies, 0.99)),
        p999_ms: duration_ms(quantile(&latencies, 0.999)),
        max_ms: duration_ms(*latencies.last().expect("at least one fetch")),
    };
    let seconds = elapsed.as_secs_f64();
    let mebibytes_per_second = payload_bytes as f64 / (1024.0 * 1024.0) / seconds;
    let gates_passed = args
        .min_mib_per_second
        .is_none_or(|minimum| mebibytes_per_second >= minimum)
        && args
            .max_p99_ms
            .is_none_or(|maximum| latency.p99_ms <= maximum);
    let result = ResultDocument {
        format_version: 1,
        workload: "ordered-direct-engine-fetch",
        shards: args.shards,
        batches_per_pass: args.batches,
        batch_payload_bytes: args.payload_bytes,
        fetch_max_bytes: args.fetch_bytes,
        warmup_passes: args.warmup_passes,
        measured_passes: args.measured_passes,
        reused_data: args.reuse_data,
        fetches,
        fetched_batches,
        payload_bytes,
        elapsed_seconds: seconds,
        fetches_per_second: fetches as f64 / seconds,
        batches_per_second: fetched_batches as f64 / seconds,
        mebibytes_per_second,
        latency,
        min_mib_per_second: args.min_mib_per_second,
        max_p99_ms: args.max_p99_ms,
        gates_passed,
    };
    println!("{}", serde_json::to_string_pretty(&result)?);

    drop(engine);
    if generated_data_dir && !args.keep_data {
        std::fs::remove_dir_all(data_dir)?;
    }
    if !gates_passed {
        return Err("one or more benchmark gates failed".into());
    }
    Ok(())
}

fn prepare(engine: &Arc<StreamEngine>, args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let payload = Bytes::from(vec![0x5a; args.payload_bytes]);
    let next_batch = AtomicU64::new(0);
    let failed = AtomicBool::new(false);
    let results = std::thread::scope(|scope| {
        let mut workers = Vec::with_capacity(args.prepare_concurrency);
        for _ in 0..args.prepare_concurrency {
            let engine = Arc::clone(engine);
            let payload = payload.clone();
            let next_batch = &next_batch;
            let failed = &failed;
            workers.push(scope.spawn(move || {
                loop {
                    if failed.load(Ordering::Relaxed) {
                        break;
                    }
                    let batch = next_batch.fetch_add(1, Ordering::Relaxed);
                    if batch >= args.batches {
                        break;
                    }
                    if let Err(error) = engine.append(AppendRequest {
                        request_id: u128::from(batch),
                        topic_id: TopicId::new(1),
                        partition_id: LogicalPartitionId::new(0),
                        record_count: 1,
                        payload: payload.clone(),
                        durability: Durability::Leader,
                        producer: None,
                        leader_epoch: None,
                    }) {
                        failed.store(true, Ordering::Relaxed);
                        return Err(error.to_string());
                    }
                }
                Ok(())
            }));
        }
        workers
            .into_iter()
            .map(std::thread::ScopedJoinHandle::join)
            .collect::<Vec<_>>()
    });
    for result in results {
        result
            .map_err(|_| "prepare worker panicked")?
            .map_err(|error| format!("prepare append failed: {error}"))?;
    }
    Ok(())
}

#[derive(Debug)]
struct PassResult {
    fetches: u64,
    batches: u64,
    payload_bytes: u64,
}

fn consume_pass(
    engine: &StreamEngine,
    args: &Args,
    mut latencies: Option<&mut Vec<Duration>>,
) -> Result<PassResult, Box<dyn std::error::Error>> {
    let mut offset = LogicalOffset::new(0);
    let mut fetches = 0u64;
    let mut batches = 0u64;
    let mut payload_bytes = 0u64;
    while batches < args.batches {
        let fetch_start = Instant::now();
        let fetched = engine.fetch(FetchRequest {
            request_id: u128::from(fetches),
            topic_id: TopicId::new(1),
            partition_id: LogicalPartitionId::new(0),
            start_offset: offset,
            max_bytes: u32::try_from(args.fetch_bytes)
                .map_err(|_| "fetch_bytes exceeds the protocol limit")?,
            mode: FetchMode::Ordered,
        })?;
        if let Some(latencies) = &mut latencies {
            latencies.push(fetch_start.elapsed());
        }
        let Some(last) = fetched.last() else {
            return Err(format!("fetch at offset {offset} returned no batches").into());
        };
        offset = LogicalOffset::new(
            last.last_offset
                .get()
                .checked_add(1)
                .ok_or("logical offset overflow")?,
        );
        fetches = fetches.checked_add(1).ok_or("fetch count overflow")?;
        batches = batches
            .checked_add(u64::try_from(fetched.len())?)
            .ok_or("batch count overflow")?;
        for batch in fetched {
            payload_bytes = payload_bytes
                .checked_add(u64::try_from(batch.payload.len())?)
                .ok_or("payload byte count overflow")?;
        }
    }
    if batches != args.batches {
        return Err(format!(
            "fetch returned {batches} batches, expected {}",
            args.batches
        )
        .into());
    }
    Ok(PassResult {
        fetches,
        batches,
        payload_bytes,
    })
}

fn validate_args(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    if args.batches == 0
        || args.payload_bytes == 0
        || args.shards == 0
        || args.prepare_concurrency == 0
        || args.fetch_bytes == 0
        || args.measured_passes == 0
    {
        return Err("benchmark counts, sizes, shards, and passes must be nonzero".into());
    }
    if args.fetch_bytes > u32::MAX as usize {
        return Err("fetch_bytes exceeds the protocol limit".into());
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
    std::env::temp_dir().join(format!(
        "shard-stream-fetch-bench-{}-{nonce}",
        std::process::id()
    ))
}

fn require_empty_directory(directory: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    if !directory.exists() {
        return Ok(());
    }
    if !directory.is_dir() {
        return Err(format!("data_dir must name a directory: {}", directory.display()).into());
    }
    if std::fs::read_dir(directory)?.next().transpose()?.is_some() {
        return Err(format!("data_dir must be absent or empty: {}", directory.display()).into());
    }
    Ok(())
}
