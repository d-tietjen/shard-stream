use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use serde::Serialize;
use shard_stream_client::{Client, ClientConfig, ProducerTuning};
use shard_stream_core::{LogicalOffset, LogicalPartitionId, TopicId};
use shard_stream_protocol::FetchMode;
use tokio::sync::Barrier;

#[derive(Debug, Parser)]
#[command(about = "End-to-end ordered native consumer benchmark for shard-stream")]
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
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    fetch_bytes: usize,
    /// Independent ordered readers replaying the same partition.
    #[arg(long, default_value_t = 1)]
    consumers: usize,
    #[arg(long, default_value_t = 1)]
    warmup_passes: u32,
    #[arg(long, default_value_t = 1)]
    measured_passes: u32,
    #[arg(long, default_value_t = 30)]
    timeout_seconds: u64,
    #[arg(long)]
    min_mib_per_second: Option<f64>,
    #[arg(long)]
    max_p99_ms: Option<f64>,
}

#[derive(Debug, Serialize)]
struct ResultDocument {
    format_version: u8,
    workload: &'static str,
    protocol: &'static str,
    response_encoding: &'static str,
    endpoint: String,
    topic_id: String,
    partition_id: u32,
    replication_factor: u32,
    consumers: usize,
    warmup_passes: u32,
    measured_passes: u32,
    batches_per_consumer: u64,
    records_per_batch: u32,
    payload_bytes_per_batch: usize,
    fetch_max_bytes: usize,
    fetches: u64,
    fetched_batches: u64,
    fetched_records: u64,
    application_payload_bytes: u64,
    elapsed_seconds: f64,
    fetches_per_second: f64,
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

#[derive(Debug)]
struct PassResult {
    fetches: u64,
    batches: u64,
    records: u64,
    payload_bytes: u64,
    latencies: Vec<Duration>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Arc::new(Args::parse());
    validate_args(&args)?;
    let client = Client::new(ClientConfig {
        endpoint: args.endpoint.clone(),
        request_timeout: Duration::from_secs(args.timeout_seconds),
        max_retries: 0,
        max_frame_bytes: args.payload_bytes.saturating_add(1024 * 1024),
        producer: ProducerTuning::default(),
        admin_token: None,
    })?;

    for pass in 0..args.warmup_passes {
        let _ = consume_round(client.clone(), Arc::clone(&args), pass, false).await?;
    }

    let mut elapsed = Duration::ZERO;
    let mut fetches = 0u64;
    let mut fetched_batches = 0u64;
    let mut fetched_records = 0u64;
    let mut application_payload_bytes = 0u64;
    let mut latencies = Vec::new();
    for pass in 0..args.measured_passes {
        let (round_elapsed, results) =
            consume_round(client.clone(), Arc::clone(&args), pass, true).await?;
        elapsed = elapsed
            .checked_add(round_elapsed)
            .ok_or("consumer benchmark elapsed time overflow")?;
        for result in results {
            fetches = fetches
                .checked_add(result.fetches)
                .ok_or("fetch count overflow")?;
            fetched_batches = fetched_batches
                .checked_add(result.batches)
                .ok_or("batch count overflow")?;
            fetched_records = fetched_records
                .checked_add(result.records)
                .ok_or("record count overflow")?;
            application_payload_bytes = application_payload_bytes
                .checked_add(result.payload_bytes)
                .ok_or("payload byte count overflow")?;
            latencies.extend(result.latencies);
        }
    }
    latencies.sort_unstable();
    let seconds = elapsed.as_secs_f64();
    let application_mib_per_second = application_payload_bytes as f64 / (1024.0 * 1024.0) / seconds;
    let latency = LatencyDocument {
        p50_ms: duration_ms(quantile(&latencies, 0.50)),
        p95_ms: duration_ms(quantile(&latencies, 0.95)),
        p99_ms: duration_ms(quantile(&latencies, 0.99)),
        p999_ms: duration_ms(quantile(&latencies, 0.999)),
        max_ms: duration_ms(*latencies.last().expect("at least one measured fetch")),
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
            workload: "ordered-native-network-fetch",
            protocol: "native-rest",
            response_encoding: "fixed-width-binary",
            endpoint: args.endpoint.clone(),
            topic_id: args.topic_id.to_string(),
            partition_id: args.partition_id,
            replication_factor: 1,
            consumers: args.consumers,
            warmup_passes: args.warmup_passes,
            measured_passes: args.measured_passes,
            batches_per_consumer: args.batches,
            records_per_batch: args.records_per_batch,
            payload_bytes_per_batch: args.payload_bytes,
            fetch_max_bytes: args.fetch_bytes,
            fetches,
            fetched_batches,
            fetched_records,
            application_payload_bytes,
            elapsed_seconds: seconds,
            fetches_per_second: fetches as f64 / seconds,
            batches_per_second: fetched_batches as f64 / seconds,
            records_per_second: fetched_records as f64 / seconds,
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
        return Err("one or more consumer benchmark gates failed".into());
    }
    Ok(())
}

async fn consume_round(
    client: Client,
    args: Arc<Args>,
    pass: u32,
    collect_latencies: bool,
) -> Result<(Duration, Vec<PassResult>), Box<dyn std::error::Error + Send + Sync>> {
    let barrier = Arc::new(Barrier::new(args.consumers + 1));
    let mut workers = Vec::with_capacity(args.consumers);
    for consumer_index in 0..args.consumers {
        let client = client.clone();
        let args = Arc::clone(&args);
        let barrier = Arc::clone(&barrier);
        workers.push(tokio::spawn(async move {
            barrier.wait().await;
            consume_pass(client, &args, consumer_index, pass, collect_latencies).await
        }));
    }
    barrier.wait().await;
    let start = Instant::now();
    let mut results = Vec::with_capacity(args.consumers);
    for worker in workers {
        results.push(worker.await??);
    }
    Ok((start.elapsed(), results))
}

async fn consume_pass(
    client: Client,
    args: &Args,
    consumer_index: usize,
    pass: u32,
    collect_latencies: bool,
) -> Result<PassResult, String> {
    let consumer = client.consumer(
        TopicId::new(args.topic_id),
        LogicalPartitionId::new(args.partition_id),
    );
    let max_bytes = u32::try_from(args.fetch_bytes)
        .map_err(|_| "fetch_bytes exceeds the native protocol limit".to_owned())?;
    let mut offset = LogicalOffset::new(0);
    let mut fetches = 0u64;
    let mut batches = 0u64;
    let mut records = 0u64;
    let mut payload_bytes = 0u64;
    let mut latencies = Vec::new();
    while batches < args.batches {
        let fetch_start = Instant::now();
        let fetched = consumer
            .fetch(offset, max_bytes, FetchMode::Ordered)
            .await
            .map_err(|error| {
                format!(
                    "consumer {consumer_index} pass {pass} fetch at offset {offset} failed: {error}"
                )
            })?;
        let fetch_latency = fetch_start.elapsed();
        if collect_latencies {
            latencies.push(fetch_latency);
        }
        if fetched.is_empty() {
            return Err(format!(
                "consumer {consumer_index} pass {pass} fetch at offset {offset} returned no batches"
            ));
        }
        for batch in fetched {
            if batches >= args.batches {
                return Err(format!(
                    "consumer {consumer_index} pass {pass} returned more than {} batches",
                    args.batches
                ));
            }
            if batch.first_offset != offset {
                return Err(format!(
                    "consumer {consumer_index} pass {pass} expected offset {offset}, got {}",
                    batch.first_offset
                ));
            }
            if batch.record_count.get() != args.records_per_batch {
                return Err(format!(
                    "consumer {consumer_index} pass {pass} batch {} has {} records, expected {}",
                    batch.batch_id, batch.record_count, args.records_per_batch
                ));
            }
            if batch.payload.len() != args.payload_bytes {
                return Err(format!(
                    "consumer {consumer_index} pass {pass} batch {} has {} payload bytes, expected {}",
                    batch.batch_id,
                    batch.payload.len(),
                    args.payload_bytes
                ));
            }
            offset = LogicalOffset::new(
                batch
                    .last_offset
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| "logical offset overflow".to_owned())?,
            );
            batches = batches
                .checked_add(1)
                .ok_or_else(|| "batch count overflow".to_owned())?;
            records = records
                .checked_add(u64::from(batch.record_count.get()))
                .ok_or_else(|| "record count overflow".to_owned())?;
            payload_bytes = payload_bytes
                .checked_add(batch.payload.len() as u64)
                .ok_or_else(|| "payload byte count overflow".to_owned())?;
        }
        fetches = fetches
            .checked_add(1)
            .ok_or_else(|| "fetch count overflow".to_owned())?;
    }
    let expected_records = args
        .batches
        .checked_mul(u64::from(args.records_per_batch))
        .ok_or_else(|| "expected record count overflow".to_owned())?;
    let expected_payload_bytes = args
        .batches
        .checked_mul(args.payload_bytes as u64)
        .ok_or_else(|| "expected payload byte count overflow".to_owned())?;
    if records != expected_records
        || payload_bytes != expected_payload_bytes
        || offset.get() != expected_records
    {
        return Err(format!(
            "consumer {consumer_index} pass {pass} totals differ: \
             records={records}/{expected_records}, bytes={payload_bytes}/{expected_payload_bytes}, \
             end_offset={}/{expected_records}",
            offset.get()
        ));
    }
    Ok(PassResult {
        fetches,
        batches,
        records,
        payload_bytes,
        latencies,
    })
}

fn validate_args(args: &Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if args.batches == 0
        || args.payload_bytes == 0
        || args.records_per_batch == 0
        || args.fetch_bytes == 0
        || args.consumers == 0
        || args.measured_passes == 0
        || args.timeout_seconds == 0
    {
        return Err(
            "batches, payload, records, fetch size, consumers, passes, and timeout must be nonzero"
                .into(),
        );
    }
    if args.fetch_bytes > u32::MAX as usize {
        return Err("fetch_bytes exceeds the native protocol limit".into());
    }
    if args.payload_bytes > args.fetch_bytes {
        return Err("fetch_bytes must be at least payload_bytes".into());
    }
    args.batches
        .checked_mul(u64::from(args.records_per_batch))
        .ok_or("expected record count overflow")?;
    args.batches
        .checked_mul(args.payload_bytes as u64)
        .ok_or("expected payload byte count overflow")?;
    for gate in [args.min_mib_per_second, args.max_p99_ms]
        .into_iter()
        .flatten()
    {
        if !gate.is_finite() || gate <= 0.0 {
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
    duration.as_secs_f64() * 1_000.0
}
