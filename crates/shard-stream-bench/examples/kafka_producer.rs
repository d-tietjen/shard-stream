use std::error::Error;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use clap::Parser;
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use kafka_protocol::messages::metadata_request::{MetadataRequest, MetadataRequestTopic};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    ApiKey, RequestHeader, RequestKind, ResponseHeader, ResponseKind, TopicName,
};
use kafka_protocol::protocol::{Decodable, StrBytes, encode_request_header_into_buffer};
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_TIMESTAMP,
    Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use serde::Serialize;

const CREATE_TOPICS_VERSION: i16 = 4;
const METADATA_VERSION: i16 = 8;
const PRODUCE_VERSION: i16 = 8;
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(about = "End-to-end Kafka producer benchmark for shard-stream")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9092")]
    bootstrap: String,
    #[arg(long, default_value = "shard-stream-benchmark")]
    topic: String,
    #[arg(long, default_value_t = 4_096)]
    batches: u64,
    #[arg(long, default_value_t = 2_048)]
    records_per_batch: u32,
    #[arg(long, default_value_t = 1_024)]
    record_bytes: usize,
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
    #[arg(long, default_value_t = -1, allow_hyphen_values = true)]
    acks: i16,
    #[arg(long)]
    skip_create: bool,
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
    protocol: &'static str,
    request_encoding: &'static str,
    bootstrap: String,
    leader: String,
    topic: String,
    acks: i16,
    replication_factor: i16,
    concurrency: usize,
    batches: u64,
    records: u64,
    records_per_batch: u32,
    record_bytes: usize,
    encoded_batch_bytes: usize,
    record_batches_per_request: usize,
    application_payload_bytes: u64,
    encoded_record_batch_bytes: u64,
    elapsed_seconds: f64,
    batches_per_second: f64,
    records_per_second: f64,
    application_mib_per_second: f64,
    encoded_mib_per_second: f64,
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

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();
    validate_args(&args)?;
    let timeout = Duration::from_secs(args.timeout_seconds);
    let records = build_records(args.records_per_batch, args.record_bytes);
    let mut encoded_batch = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut encoded_batch,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )?;
    let encoded_batch = encoded_batch.freeze();
    let record_batches_per_request = count_record_batches(&encoded_batch)?;
    let produce_frame = Arc::new(encode_request_frame(
        ApiKey::Produce,
        PRODUCE_VERSION,
        0,
        RequestKind::Produce(
            kafka_protocol::messages::ProduceRequest::default()
                .with_acks(args.acks)
                .with_topic_data(vec![
                    TopicProduceData::default()
                        .with_name(topic_name(&args.topic))
                        .with_partition_data(vec![
                            PartitionProduceData::default()
                                .with_index(0)
                                .with_records(Some(encoded_batch.clone())),
                        ]),
                ]),
        ),
    )?);

    if !args.skip_create {
        create_topic(&args, timeout)?;
    }
    let leader = discover_leader(&args.bootstrap, &args.topic, timeout)?;

    let mut connections = Vec::with_capacity(args.concurrency);
    for _ in 0..args.concurrency {
        connections.push(connect(&leader, timeout)?);
    }

    let next_batch = AtomicU64::new(0);
    let failed = AtomicBool::new(false);
    let barrier = Arc::new(Barrier::new(args.concurrency + 1));
    let worker_results = std::thread::scope(|scope| {
        let mut workers = Vec::with_capacity(args.concurrency);
        for (worker_index, mut connection) in connections.into_iter().enumerate() {
            let next_batch = &next_batch;
            let failed = &failed;
            let barrier = Arc::clone(&barrier);
            let produce_frame = Arc::clone(&produce_frame);
            workers.push(scope.spawn(move || -> Result<Vec<Duration>, String> {
                let mut latencies = Vec::new();
                let mut produce_frame = produce_frame.as_ref().clone();
                barrier.wait();
                loop {
                    if failed.load(Ordering::Relaxed) {
                        break;
                    }
                    let batch = next_batch.fetch_add(1, Ordering::Relaxed);
                    if batch >= args.batches {
                        break;
                    }
                    let correlation_id = i32::try_from(batch)
                        .map_err(|_| "batch count exceeds Kafka correlation ID space")?;
                    let append_start = Instant::now();
                    let response =
                        produce_round_trip(&mut connection, &mut produce_frame, correlation_id)
                            .map_err(|error| {
                                failed.store(true, Ordering::Relaxed);
                                format!("worker {worker_index} produce failed: {error}")
                            })?;
                    let ResponseKind::Produce(response) = response else {
                        failed.store(true, Ordering::Relaxed);
                        return Err(format!(
                            "worker {worker_index} received a non-produce response"
                        ));
                    };
                    let partition = response
                        .responses
                        .first()
                        .and_then(|topic| topic.partition_responses.first())
                        .ok_or_else(|| "produce response omitted the partition".to_owned())?;
                    if partition.error_code != 0 {
                        failed.store(true, Ordering::Relaxed);
                        return Err(format!(
                            "worker {worker_index} produce returned Kafka error {}",
                            partition.error_code
                        ));
                    }
                    latencies.push(append_start.elapsed());
                }
                Ok(latencies)
            }));
        }
        barrier.wait();
        let started = Instant::now();
        let results = workers
            .into_iter()
            .map(std::thread::ScopedJoinHandle::join)
            .collect::<Vec<_>>();
        (started, results)
    });

    let (started, worker_results) = worker_results;
    let elapsed = started.elapsed();
    let mut latencies = Vec::with_capacity(usize::try_from(args.batches).unwrap_or(usize::MAX));
    for result in worker_results {
        let mut worker = result
            .map_err(|_| "Kafka benchmark worker panicked")?
            .map_err(|error| format!("Kafka benchmark failed: {error}"))?;
        latencies.append(&mut worker);
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

    let total_records = args
        .batches
        .checked_mul(u64::from(args.records_per_batch))
        .ok_or("record count overflow")?;
    let application_payload_bytes = total_records
        .checked_mul(args.record_bytes as u64)
        .ok_or("application payload byte count overflow")?;
    let encoded_record_batch_bytes = args
        .batches
        .checked_mul(encoded_batch.len() as u64)
        .ok_or("encoded byte count overflow")?;
    let seconds = elapsed.as_secs_f64();
    let application_mib_per_second = application_payload_bytes as f64 / (1024.0 * 1024.0) / seconds;
    let encoded_mib_per_second = encoded_record_batch_bytes as f64 / (1024.0 * 1024.0) / seconds;
    let latency = LatencyDocument {
        p50_ms: duration_ms(quantile(&latencies, 0.50)),
        p95_ms: duration_ms(quantile(&latencies, 0.95)),
        p99_ms: duration_ms(quantile(&latencies, 0.99)),
        p999_ms: duration_ms(quantile(&latencies, 0.999)),
        max_ms: duration_ms(*latencies.last().expect("nonempty latencies")),
    };
    let gates_passed = args
        .min_mib_per_second
        .is_none_or(|minimum| application_mib_per_second >= minimum)
        && args
            .max_p99_ms
            .is_none_or(|maximum| latency.p99_ms <= maximum);
    let result = ResultDocument {
        format_version: 2,
        protocol: "kafka-tcp",
        request_encoding: "preencoded-correlation-patch",
        bootstrap: args.bootstrap,
        leader,
        topic: args.topic,
        acks: args.acks,
        replication_factor: 1,
        concurrency: args.concurrency,
        batches: args.batches,
        records: total_records,
        records_per_batch: args.records_per_batch,
        record_bytes: args.record_bytes,
        encoded_batch_bytes: encoded_batch.len(),
        record_batches_per_request,
        application_payload_bytes,
        encoded_record_batch_bytes,
        elapsed_seconds: seconds,
        batches_per_second: args.batches as f64 / seconds,
        records_per_second: total_records as f64 / seconds,
        application_mib_per_second,
        encoded_mib_per_second,
        latency,
        gates: GateDocument {
            min_mib_per_second: args.min_mib_per_second,
            max_p99_ms: args.max_p99_ms,
            passed: gates_passed,
        },
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    if !gates_passed {
        return Err("one or more Kafka benchmark gates failed".into());
    }
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), Box<dyn Error + Send + Sync>> {
    if args.batches == 0
        || args.records_per_batch == 0
        || args.record_bytes == 0
        || args.concurrency == 0
        || args.timeout_seconds == 0
    {
        return Err("batch, record, concurrency, and timeout dimensions must be nonzero".into());
    }
    if args.batches > i32::MAX as u64 {
        return Err("batches cannot exceed the Kafka correlation ID space".into());
    }
    if args.records_per_batch > i32::MAX as u32 {
        return Err("records_per_batch cannot exceed the Kafka offset-delta range".into());
    }
    if !matches!(args.acks, -1..=1) {
        return Err("acks must be -1, 0, or 1".into());
    }
    if args.acks == 0 {
        return Err("acks=0 is excluded because this harness measures acknowledgements".into());
    }
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

fn create_topic(args: &Args, timeout: Duration) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut connection = connect(&args.bootstrap, timeout)?;
    let response = round_trip(
        &mut connection,
        ApiKey::CreateTopics,
        CREATE_TOPICS_VERSION,
        -1,
        RequestKind::CreateTopics(CreateTopicsRequest::default().with_topics(vec![
            CreatableTopic::default()
                .with_name(topic_name(&args.topic))
                .with_num_partitions(1)
                .with_replication_factor(1),
        ])),
    )?;
    let ResponseKind::CreateTopics(response) = response else {
        return Err("broker returned a non-create-topics response".into());
    };
    let topic = response
        .topics
        .first()
        .ok_or("create-topics response omitted the topic")?;
    if topic.error_code != 0 {
        return Err(format!("create-topics returned Kafka error {}", topic.error_code).into());
    }
    Ok(())
}

fn discover_leader(
    bootstrap: &str,
    topic: &str,
    timeout: Duration,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut connection = connect(bootstrap, timeout)?;
    let response = round_trip(
        &mut connection,
        ApiKey::Metadata,
        METADATA_VERSION,
        -2,
        RequestKind::Metadata(MetadataRequest::default().with_topics(Some(vec![
            MetadataRequestTopic::default().with_name(Some(topic_name(topic))),
        ]))),
    )?;
    let ResponseKind::Metadata(response) = response else {
        return Err("broker returned a non-metadata response".into());
    };
    let topic = response
        .topics
        .first()
        .ok_or("metadata response omitted the topic")?;
    if topic.error_code != 0 {
        return Err(format!("metadata returned topic error {}", topic.error_code).into());
    }
    let partition = topic
        .partitions
        .iter()
        .find(|partition| partition.partition_index == 0)
        .ok_or("metadata response omitted partition zero")?;
    if partition.error_code != 0 {
        return Err(format!("metadata returned partition error {}", partition.error_code).into());
    }
    let broker = response
        .brokers
        .iter()
        .find(|broker| broker.node_id == partition.leader_id)
        .ok_or("metadata response omitted the partition leader broker")?;
    let port = u16::try_from(broker.port).map_err(|_| "metadata returned an invalid port")?;
    Ok(format!("{}:{port}", broker.host))
}

fn connect(address: &str, timeout: Duration) -> std::io::Result<TcpStream> {
    let mut addresses = address.to_socket_addrs()?;
    let address = addresses.next().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "bootstrap address resolved to no endpoints",
        )
    })?;
    let stream = TcpStream::connect_timeout(&address, timeout)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    Ok(stream)
}

fn round_trip(
    stream: &mut TcpStream,
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    request: RequestKind,
) -> Result<ResponseKind, Box<dyn Error + Send + Sync>> {
    let frame = encode_request_frame(api_key, version, correlation_id, request)?;
    stream.write_all(&frame)?;
    read_response(stream, api_key, version, correlation_id)
}

fn encode_request_frame(
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    request: RequestKind,
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let header = RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("shard-stream-kafka-bench")));
    let mut body = BytesMut::new();
    encode_request_header_into_buffer(&mut body, &header)?;
    request.encode(&mut body, version)?;
    let length = i32::try_from(body.len()).map_err(|_| "Kafka request exceeds i32")?;
    let mut frame = Vec::with_capacity(body.len() + 4);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

fn produce_round_trip(
    stream: &mut TcpStream,
    frame: &mut [u8],
    correlation_id: i32,
) -> Result<ResponseKind, Box<dyn Error + Send + Sync>> {
    const FRAMED_CORRELATION_RANGE: std::ops::Range<usize> = 8..12;
    frame[FRAMED_CORRELATION_RANGE].copy_from_slice(&correlation_id.to_be_bytes());
    stream.write_all(frame)?;
    read_response(stream, ApiKey::Produce, PRODUCE_VERSION, correlation_id)
}

fn read_response(
    stream: &mut TcpStream,
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
) -> Result<ResponseKind, Box<dyn Error + Send + Sync>> {
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix)?;
    let response_length = i32::from_be_bytes(prefix);
    let response_length =
        usize::try_from(response_length).map_err(|_| "negative Kafka response length")?;
    if response_length > MAX_RESPONSE_BYTES {
        return Err("Kafka response exceeds benchmark safety bound".into());
    }
    let mut response = vec![0u8; response_length];
    stream.read_exact(&mut response)?;
    let mut response = Bytes::from(response);
    let header = ResponseHeader::decode(&mut response, api_key.response_header_version(version))?;
    if header.correlation_id != correlation_id {
        return Err(format!(
            "Kafka correlation mismatch: expected {correlation_id}, got {}",
            header.correlation_id
        )
        .into());
    }
    Ok(ResponseKind::decode(api_key, &mut response, version)?)
}

fn build_records(record_count: u32, record_bytes: usize) -> Vec<Record> {
    let value = Bytes::from(vec![0x5a; record_bytes]);
    (0..record_count)
        .map(|offset| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: i64::from(offset),
            sequence: i32::try_from(offset)
                .expect("record count was validated")
                .wrapping_sub(1),
            timestamp: NO_TIMESTAMP,
            key: None,
            value: Some(value.clone()),
            headers: Default::default(),
        })
        .collect()
}

fn count_record_batches(mut encoded: &[u8]) -> Result<usize, Box<dyn Error + Send + Sync>> {
    let mut batches = 0usize;
    while !encoded.is_empty() {
        if encoded.len() < 12 {
            return Err("encoded Kafka RecordBatch is truncated".into());
        }
        let batch_length = i32::from_be_bytes(encoded[8..12].try_into()?);
        let batch_length =
            usize::try_from(batch_length).map_err(|_| "encoded Kafka RecordBatch is negative")?;
        let total_length = 12usize
            .checked_add(batch_length)
            .ok_or("encoded Kafka RecordBatch length overflow")?;
        if total_length > encoded.len() {
            return Err("encoded Kafka RecordBatch exceeds the request".into());
        }
        encoded = &encoded[total_length..];
        batches = batches
            .checked_add(1)
            .ok_or("encoded Kafka RecordBatch count overflow")?;
    }
    Ok(batches)
}

fn topic_name(topic: &str) -> TopicName {
    StrBytes::from_string(topic.to_owned()).into()
}

fn quantile(sorted: &[Duration], quantile: f64) -> Duration {
    let index = ((sorted.len() - 1) as f64 * quantile).ceil() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use kafka_protocol::records::{Compression, RecordBatchEncoder, RecordEncodeOptions};

    use super::*;

    #[test]
    fn benchmark_encodes_one_realistic_non_idempotent_record_batch() {
        let records = build_records(2_048, 1_024);
        let mut encoded = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut encoded,
            &records,
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .expect("encode");
        assert_eq!(count_record_batches(&encoded).expect("count"), 1);
    }
}
