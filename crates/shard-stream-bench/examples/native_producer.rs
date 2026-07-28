use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use serde::Serialize;
use shard_stream_client::{Client, ClientConfig, ProducerTuning};
use shard_stream_core::{LogicalPartitionId, TopicId};
use shard_stream_protocol::Durability;

#[derive(Debug, Clone, Copy, ValueEnum)]
#[clap(rename_all = "snake_case")]
enum DurabilityArg {
    Leader,
    Quorum,
    AllReplicas,
    Object,
}

impl DurabilityArg {
    const fn engine(self) -> Durability {
        match self {
            Self::Leader => Durability::Leader,
            Self::Quorum => Durability::Quorum,
            Self::AllReplicas => Durability::AllReplicas,
            Self::Object => Durability::Object,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Leader => "leader",
            Self::Quorum => "quorum",
            Self::AllReplicas => "all_replicas",
            Self::Object => "object",
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "End-to-end native producer benchmark for shard-stream")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:7420")]
    endpoint: String,
    #[arg(long, default_value_t = 1)]
    topic_id: u128,
    #[arg(long, default_value_t = 0)]
    partition_id: u32,
    #[arg(long, default_value_t = 4_096)]
    batches: u64,
    #[arg(long, default_value_t = 2 * 1024 * 1024)]
    payload_bytes: usize,
    #[arg(long, default_value_t = 2_048)]
    records_per_batch: u32,
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
    #[arg(long, value_enum, default_value_t = DurabilityArg::Leader)]
    durability: DurabilityArg,
    #[arg(long, default_value_t = 30)]
    timeout_seconds: u64,
    #[arg(long)]
    skip_create: bool,
    #[arg(long)]
    min_mib_per_second: Option<f64>,
    #[arg(long)]
    max_p99_ms: Option<f64>,
}

#[derive(Debug, Serialize)]
struct ResultDocument {
    format_version: u8,
    protocol: &'static str,
    response_encoding: &'static str,
    endpoint: String,
    topic_id: String,
    partition_id: u32,
    replication_factor: u32,
    durability: &'static str,
    concurrency: usize,
    batches: u64,
    records: u64,
    payload_bytes_per_batch: usize,
    records_per_batch: u32,
    application_payload_bytes: u64,
    elapsed_seconds: f64,
    batches_per_second: f64,
    records_per_second: f64,
    application_mib_per_second: f64,
    latency: LatencyDocument,
    gates: GateDocument,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    validate_args(&args)?;
    let topic_id = TopicId::new(args.topic_id);
    let partition_id = LogicalPartitionId::new(args.partition_id);
    let tuning = ProducerTuning {
        batch_size: args.payload_bytes,
        linger: Duration::ZERO,
        max_request_size: args.payload_bytes,
    };
    let client = Client::new(ClientConfig {
        endpoint: args.endpoint.clone(),
        request_timeout: Duration::from_secs(args.timeout_seconds),
        max_retries: 0,
        max_frame_bytes: args.payload_bytes.saturating_add(1024 * 1024),
        producer: tuning,
        admin_token: None,
    })?;
    if !args.skip_create {
        client.create_topic(topic_id, 1, None).await?;
    }

    let payload = Bytes::from(vec![0x5a; args.payload_bytes]);
    let next_batch = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    let mut workers = Vec::with_capacity(args.concurrency);
    for worker_index in 0..args.concurrency {
        let client = client.clone();
        let payload = payload.clone();
        let next_batch = Arc::clone(&next_batch);
        let failed = Arc::clone(&failed);
        let durability = args.durability.engine();
        workers.push(tokio::spawn(async move {
            let mut producer = client.producer(
                topic_id,
                partition_id,
                u128::from(u64::try_from(worker_index + 1).expect("worker index fits u64")),
                0,
            );
            let mut latencies = Vec::new();
            loop {
                if failed.load(Ordering::Relaxed) {
                    break;
                }
                let batch = next_batch.fetch_add(1, Ordering::Relaxed);
                if batch >= args.batches {
                    break;
                }
                let append_start = Instant::now();
                if let Err(error) = producer
                    .append(payload.clone(), args.records_per_batch, durability)
                    .await
                {
                    failed.store(true, Ordering::Relaxed);
                    return Err(format!("worker {worker_index} append failed: {error}"));
                }
                latencies.push(append_start.elapsed());
            }
            Ok::<Vec<Duration>, String>(latencies)
        }));
    }

    let mut latencies = Vec::new();
    for worker in workers {
        latencies.extend(worker.await??);
    }
    if latencies.len() != args.batches as usize {
        return Err(format!(
            "benchmark completed {} of {} batches",
            latencies.len(),
            args.batches
        )
        .into());
    }
    latencies.sort_unstable();
    let elapsed = start.elapsed().as_secs_f64();
    let records = args
        .batches
        .checked_mul(u64::from(args.records_per_batch))
        .ok_or("record count overflow")?;
    let application_payload_bytes = args
        .batches
        .checked_mul(args.payload_bytes as u64)
        .ok_or("payload byte count overflow")?;
    let application_mib_per_second = application_payload_bytes as f64 / (1024.0 * 1024.0) / elapsed;
    let latency = LatencyDocument {
        p50_ms: duration_ms(quantile(&latencies, 0.50)),
        p95_ms: duration_ms(quantile(&latencies, 0.95)),
        p99_ms: duration_ms(quantile(&latencies, 0.99)),
        p999_ms: duration_ms(quantile(&latencies, 0.999)),
        max_ms: duration_ms(*latencies.last().expect("nonempty latency list")),
    };
    let passed = args
        .min_mib_per_second
        .is_none_or(|minimum| application_mib_per_second >= minimum)
        && args
            .max_p99_ms
            .is_none_or(|maximum| latency.p99_ms <= maximum);
    println!(
        "{}",
        serde_json::to_string_pretty(&ResultDocument {
            format_version: 1,
            protocol: "native-rest",
            response_encoding: "fixed-width-binary",
            endpoint: args.endpoint,
            topic_id: args.topic_id.to_string(),
            partition_id: args.partition_id,
            replication_factor: 1,
            durability: args.durability.name(),
            concurrency: args.concurrency,
            batches: args.batches,
            records,
            payload_bytes_per_batch: args.payload_bytes,
            records_per_batch: args.records_per_batch,
            application_payload_bytes,
            elapsed_seconds: elapsed,
            batches_per_second: args.batches as f64 / elapsed,
            records_per_second: records as f64 / elapsed,
            application_mib_per_second,
            latency,
            gates: GateDocument {
                min_mib_per_second: args.min_mib_per_second,
                max_p99_ms: args.max_p99_ms,
                passed,
            },
        })?
    );
    if !passed {
        return Err("one or more benchmark gates failed".into());
    }
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if args.batches == 0
        || args.payload_bytes == 0
        || args.records_per_batch == 0
        || args.concurrency == 0
        || args.timeout_seconds == 0
    {
        return Err("batches, payload, records, concurrency, and timeout must be nonzero".into());
    }
    if args.payload_bytes > 128 * 1024 * 1024 {
        return Err("payload_bytes cannot exceed 128 MiB".into());
    }
    Ok(())
}

fn quantile(values: &[Duration], quantile: f64) -> Duration {
    let index = ((values.len() - 1) as f64 * quantile).round() as usize;
    values[index]
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
