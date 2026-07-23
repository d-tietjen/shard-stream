use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use serde::Serialize;
use shard_stream_core::{LogicalPartitionId, TopicId};
use shard_stream_engine::{EngineConfig, StreamEngine, TopicConfig};
use shard_stream_protocol::{AppendRequest, Durability};

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
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    keep_data: bool,
}

#[derive(Debug, Serialize)]
struct ResultDocument {
    durability: &'static str,
    shards: u32,
    batches: u64,
    records: u64,
    payload_bytes: u64,
    elapsed_seconds: f64,
    batches_per_second: f64,
    records_per_second: f64,
    mebibytes_per_second: f64,
    first_offset: String,
    allocated_end: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.batches == 0
        || args.records_per_batch == 0
        || args.payload_bytes == 0
        || args.shards == 0
    {
        return Err("batches, records_per_batch, payload_bytes, and shards must be nonzero".into());
    }
    let generated_dir = args.data_dir.is_none();
    let data_dir = args.data_dir.unwrap_or_else(temporary_benchmark_dir);
    let queue_bytes = args
        .payload_bytes
        .checked_mul(64)
        .and_then(|bytes| bytes.checked_add(64 * 1024 * 1024))
        .ok_or("queue byte configuration overflow")?;
    let engine = StreamEngine::open(EngineConfig {
        data_dir: data_dir.clone(),
        shard_count: args.shards,
        replication_factor: args.replication_factor,
        min_in_sync_replicas: args.min_in_sync_replicas,
        queue_slots_per_shard: 4096,
        queue_bytes_per_shard: queue_bytes,
        target_pack_bytes: 256 * 1024 * 1024,
        max_batch_bytes: args.payload_bytes,
        max_fetch_bytes: args.payload_bytes.max(1024 * 1024),
    })?;
    engine.create_topic(TopicConfig {
        topic_id: TopicId::new(1),
        partitions: 1,
        shards: None,
    })?;

    let payload = vec![0x5a; args.payload_bytes];
    let start = Instant::now();
    let mut first_offset = None;
    for batch in 0..args.batches {
        let response = engine.append(AppendRequest {
            request_id: u128::from(batch),
            topic_id: TopicId::new(1),
            partition_id: LogicalPartitionId::new(0),
            record_count: args.records_per_batch,
            payload: payload.clone(),
            durability: Durability::Leader,
            producer: None,
        })?;
        first_offset.get_or_insert(response.first_offset);
    }
    engine.sync()?;
    let elapsed = start.elapsed();
    let records = args
        .batches
        .checked_mul(u64::from(args.records_per_batch))
        .ok_or("record count overflow")?;
    let payload_bytes = args
        .batches
        .checked_mul(args.payload_bytes as u64)
        .ok_or("payload byte count overflow")?;
    let seconds = elapsed.as_secs_f64();
    let watermarks = engine.watermarks(shard_stream_core::TopicPartition::new(
        TopicId::new(1),
        LogicalPartitionId::new(0),
    ))?;
    let result = ResultDocument {
        durability: "leader-fsync",
        shards: args.shards,
        batches: args.batches,
        records,
        payload_bytes,
        elapsed_seconds: seconds,
        batches_per_second: args.batches as f64 / seconds,
        records_per_second: records as f64 / seconds,
        mebibytes_per_second: payload_bytes as f64 / (1024.0 * 1024.0) / seconds,
        first_offset: first_offset.map_or_else(|| "0".into(), |offset| offset.to_string()),
        allocated_end: watermarks.allocated_end.to_string(),
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    drop(engine);
    if generated_dir && !args.keep_data {
        std::fs::remove_dir_all(data_dir)?;
    }
    Ok(())
}

fn temporary_benchmark_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    std::env::temp_dir().join(format!("shard-stream-bench-{}-{nonce}", std::process::id()))
}
