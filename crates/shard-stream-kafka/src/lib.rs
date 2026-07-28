//! Initial Kafka wire compatibility for the shard-stream engine.
//!
//! The adapter deliberately advertises only the versions and APIs implemented
//! here. Kafka topic names map deterministically to stream-native `u128` topic
//! identifiers with BLAKE3. Kafka record batches remain opaque engine payloads
//! and are decoded only to count records, enforce unsupported transaction
//! boundaries, derive idempotent producer metadata, and rewrite logical
//! offsets in fetch responses.

mod batch;

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytes_handoff::{HandoffBuffer, HandoffBufferConfig, WriteHandoff, WriteHandoffConfig};
use eden_logger::{LogAudience, LogContext, log_error, log_info};
use fast_telemetry::{Counter, Histogram, PrometheusExport};
use kafka_protocol::ResponseError;
use kafka_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use kafka_protocol::messages::create_topics_response::{
    CreatableTopicResult, CreateTopicsResponse,
};
use kafka_protocol::messages::fetch_response::{
    FetchResponse, FetchableTopicResponse, PartitionData,
};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::produce_response::{
    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};
use kafka_protocol::messages::{
    ApiKey, BrokerId, RequestKind, ResponseHeader, ResponseKind, TopicName,
};
use kafka_protocol::protocol::{Encodable, StrBytes, decode_request_header_from_buffer};
use kafka_protocol::records::{
    RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, RecordSet,
};
use serde::Serialize;
use shard_stream_core::{
    AssignmentProvider, LogicalOffset, LogicalPartitionId, TopicId, TopicPartition,
};
use shard_stream_engine::{EngineError, EngineResult, StreamEngine, TopicConfig};
use shard_stream_protocol::{AppendRequest, Durability, FetchMode, FetchRequest};
use tokio::io::AsyncRead;
#[cfg(test)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

const MAX_FRAME_HARD_LIMIT: usize = 128 * 1024 * 1024;
const TOPIC_REGISTRY_QUEUE_SLOTS: usize = 1_024;
const CONNECTION_WRITE_QUEUE_SLOTS: usize = 256;
const CONNECTION_READ_RESERVE: usize = 64 * 1024;
const CLUSTER_ID: &str = "shard-stream";
const API_VERSIONS: &[(ApiKey, i16, i16)] = &[
    (ApiKey::Produce, 3, 8),
    // Fetch sessions begin in v7; do not advertise them until session state is
    // implemented rather than treating incremental requests as full fetches.
    (ApiKey::Fetch, 4, 6),
    (ApiKey::ListOffsets, 1, 5),
    (ApiKey::Metadata, 0, 8),
    (ApiKey::ApiVersions, 0, 3),
    (ApiKey::CreateTopics, 2, 4),
];

const KAFKA_PROFILE_STAGE_COUNT: usize = 13;
const KAFKA_PROFILE_SHARDS: usize = 64;
const KAFKA_PROFILE_LATENCY_BOUNDS_NANOS: &[u64] = &[
    10_000,         // 10µs
    50_000,         // 50µs
    100_000,        // 100µs
    500_000,        // 500µs
    1_000_000,      // 1ms
    5_000_000,      // 5ms
    10_000_000,     // 10ms
    50_000_000,     // 50ms
    100_000_000,    // 100ms
    500_000_000,    // 500ms
    1_000_000_000,  // 1s
    5_000_000_000,  // 5s
    10_000_000_000, // 10s
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
enum KafkaProfileStage {
    FrameReceiveWait,
    RequestDecode,
    BlockingQueue,
    BlockingHandler,
    ProduceHandler,
    RecordValidation,
    EngineAppend,
    VisibilityWait,
    ResponseEncode,
    WriteQueue,
    WriteSyscall,
    WriteCompletion,
    RequestCompletion,
}

impl KafkaProfileStage {
    const ALL: [Self; KAFKA_PROFILE_STAGE_COUNT] = [
        Self::FrameReceiveWait,
        Self::RequestDecode,
        Self::BlockingQueue,
        Self::BlockingHandler,
        Self::ProduceHandler,
        Self::RecordValidation,
        Self::EngineAppend,
        Self::VisibilityWait,
        Self::ResponseEncode,
        Self::WriteQueue,
        Self::WriteSyscall,
        Self::WriteCompletion,
        Self::RequestCompletion,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::FrameReceiveWait => "frame_receive_wait",
            Self::RequestDecode => "request_decode",
            Self::BlockingQueue => "blocking_queue",
            Self::BlockingHandler => "blocking_handler",
            Self::ProduceHandler => "produce_handler",
            Self::RecordValidation => "record_validation",
            Self::EngineAppend => "engine_append",
            Self::VisibilityWait => "visibility_wait",
            Self::ResponseEncode => "response_encode",
            Self::WriteQueue => "write_queue",
            Self::WriteSyscall => "write_syscall",
            Self::WriteCompletion => "write_completion",
            Self::RequestCompletion => "request_completion",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct KafkaProfileStageSnapshot {
    pub stage: &'static str,
    pub samples: u64,
    pub total_nanos: u64,
    pub average_nanos: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct KafkaProfileSnapshot {
    pub sample_stride: u64,
    pub profiled_requests: u64,
    pub profiled_frame_bytes: u64,
    pub profiled_records: u64,
    pub stages: Vec<KafkaProfileStageSnapshot>,
}

/// Low-overhead, whole-request sampling for acknowledged Kafka Produce cost
/// attribution.
///
/// A stride of zero disables profiling. Nonzero strides are rounded up to a
/// power of two; one selected Produce request records every stage so stage
/// totals and averages remain comparable.
pub struct KafkaRequestMetrics {
    sample_stride: u64,
    sample_mask: u64,
    sequence: AtomicU64,
    profiled_requests: Counter,
    profiled_frame_bytes: Counter,
    profiled_records: Counter,
    stages: [Histogram; KAFKA_PROFILE_STAGE_COUNT],
}

impl fmt::Debug for KafkaRequestMetrics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaRequestMetrics")
            .field("sample_stride", &self.sample_stride)
            .field("profiled_requests", &self.profiled_requests.sum())
            .finish_non_exhaustive()
    }
}

impl Default for KafkaRequestMetrics {
    fn default() -> Self {
        Self::new(0)
    }
}

impl KafkaRequestMetrics {
    #[must_use]
    pub fn new(sample_stride: u64) -> Self {
        let sample_stride = if sample_stride == 0 {
            0
        } else {
            sample_stride
                .checked_next_power_of_two()
                .unwrap_or(1u64 << 63)
        };
        Self {
            sample_stride,
            sample_mask: sample_stride.saturating_sub(1),
            sequence: AtomicU64::new(0),
            profiled_requests: Counter::new(KAFKA_PROFILE_SHARDS),
            profiled_frame_bytes: Counter::new(KAFKA_PROFILE_SHARDS),
            profiled_records: Counter::new(KAFKA_PROFILE_SHARDS),
            stages: std::array::from_fn(|_| {
                Histogram::new(KAFKA_PROFILE_LATENCY_BOUNDS_NANOS, KAFKA_PROFILE_SHARDS)
            }),
        }
    }

    fn should_profile(&self) -> bool {
        self.sample_stride != 0
            && self.sequence.fetch_add(1, Ordering::Relaxed) & self.sample_mask == 0
    }

    fn enabled(&self) -> bool {
        self.sample_stride != 0
    }

    fn begin_request(&self, frame_bytes: usize) {
        self.profiled_requests.inc();
        self.profiled_frame_bytes
            .add(saturating_counter_value(frame_bytes as u64));
    }

    fn records(&self, records: u32) {
        self.profiled_records
            .add(saturating_counter_value(u64::from(records)));
    }

    fn record(&self, stage: KafkaProfileStage, elapsed: Duration) {
        self.record_nanos(stage, elapsed.as_nanos().min(u128::from(u64::MAX)) as u64);
    }

    fn record_nanos(&self, stage: KafkaProfileStage, nanos: u64) {
        self.stages[stage as usize].record(nanos);
    }

    #[must_use]
    pub fn snapshot(&self) -> KafkaProfileSnapshot {
        KafkaProfileSnapshot {
            sample_stride: self.sample_stride,
            profiled_requests: nonnegative_counter(self.profiled_requests.sum()),
            profiled_frame_bytes: nonnegative_counter(self.profiled_frame_bytes.sum()),
            profiled_records: nonnegative_counter(self.profiled_records.sum()),
            stages: KafkaProfileStage::ALL
                .into_iter()
                .map(|stage| {
                    let histogram = &self.stages[stage as usize];
                    let samples = histogram.count();
                    let total_nanos = histogram.sum();
                    KafkaProfileStageSnapshot {
                        stage: stage.name(),
                        samples,
                        total_nanos,
                        average_nanos: (samples != 0)
                            .then_some(total_nanos as f64 / samples as f64),
                    }
                })
                .collect(),
        }
    }

    pub fn export_prometheus(&self, output: &mut String) {
        self.profiled_requests.export_prometheus(
            output,
            "shard_stream_kafka_profiled_requests_total",
            "Kafka Produce requests selected for whole-request profiling",
        );
        self.profiled_frame_bytes.export_prometheus(
            output,
            "shard_stream_kafka_profiled_frame_bytes_total",
            "Kafka frame bytes selected for whole-request profiling",
        );
        self.profiled_records.export_prometheus(
            output,
            "shard_stream_kafka_profiled_records_total",
            "Kafka records selected for whole-request profiling",
        );
        for stage in KafkaProfileStage::ALL {
            self.stages[stage as usize].export_prometheus(
                output,
                &format!("shard_stream_kafka_profile_{}_nanos", stage.name()),
                &format!("Sampled Kafka {} latency in nanoseconds", stage.name()),
            );
        }
    }
}

fn saturating_counter_value(value: u64) -> isize {
    isize::try_from(value).unwrap_or(isize::MAX)
}

fn nonnegative_counter(value: isize) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub assignments: Arc<dyn AssignmentProvider>,
    pub max_frame_bytes: usize,
    pub max_fetch_bytes: usize,
    pub metrics: Arc<KafkaRequestMetrics>,
}

impl KafkaConfig {
    fn validate(&self) -> KafkaResult<()> {
        if self.max_frame_bytes == 0
            || self.max_frame_bytes > MAX_FRAME_HARD_LIMIT
            || self.max_fetch_bytes == 0
        {
            return Err(KafkaError::Protocol(
                "Kafka frame and fetch bounds are invalid".into(),
            ));
        }
        Ok(())
    }
}

pub trait TopicAdmin: Send + Sync {
    fn create_topic(
        &self,
        topic: TopicConfig,
    ) -> Pin<Box<dyn Future<Output = EngineResult<()>> + Send + '_>>;
}

struct LocalTopicAdmin {
    engine: Arc<StreamEngine>,
}

impl TopicAdmin for LocalTopicAdmin {
    fn create_topic(
        &self,
        topic: TopicConfig,
    ) -> Pin<Box<dyn Future<Output = EngineResult<()>> + Send + '_>> {
        let engine = Arc::clone(&self.engine);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || engine.create_topic(topic))
                .await
                .map_err(|error| {
                    EngineError::InvalidConfig(format!("Kafka topic admin task failed: {error}"))
                })?
        })
    }
}

struct Broker {
    engine: Arc<StreamEngine>,
    config: KafkaConfig,
    topic_admin: Arc<dyn TopicAdmin>,
    known_topics: KnownTopics,
    next_request_id: AtomicU64,
}

enum TopicRegistryCommand {
    Insert(String),
    List(SyncSender<Vec<String>>),
}

struct KnownTopics {
    sender: Option<SyncSender<TopicRegistryCommand>>,
    thread: Option<JoinHandle<()>>,
}

impl KnownTopics {
    fn spawn() -> KafkaResult<Self> {
        let (sender, receiver) = sync_channel(TOPIC_REGISTRY_QUEUE_SLOTS);
        let thread = thread::Builder::new()
            .name("shard-stream-kafka-topics".into())
            .spawn(move || run_topic_registry(receiver))?;
        Ok(Self {
            sender: Some(sender),
            thread: Some(thread),
        })
    }

    fn insert(&self, name: String) {
        if let Some(sender) = &self.sender {
            let _ = sender.send(TopicRegistryCommand::Insert(name));
        }
    }

    fn list(&self) -> Vec<String> {
        let (response, receiver) = sync_channel(1);
        let Some(sender) = &self.sender else {
            return Vec::new();
        };
        if sender.send(TopicRegistryCommand::List(response)).is_err() {
            return Vec::new();
        }
        receiver.recv().unwrap_or_default()
    }
}

impl Drop for KnownTopics {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn run_topic_registry(receiver: Receiver<TopicRegistryCommand>) {
    let mut topics = BTreeSet::new();
    while let Ok(command) = receiver.recv() {
        match command {
            TopicRegistryCommand::Insert(name) => {
                topics.insert(name);
            }
            TopicRegistryCommand::List(response) => {
                let _ = response.send(topics.iter().cloned().collect());
            }
        }
    }
}

pub async fn serve(
    listener: TcpListener,
    engine: Arc<StreamEngine>,
    config: KafkaConfig,
    shutdown: watch::Receiver<bool>,
) -> KafkaResult<()> {
    let topic_admin = Arc::new(LocalTopicAdmin {
        engine: Arc::clone(&engine),
    });
    serve_with_admin(listener, engine, config, topic_admin, shutdown).await
}

pub async fn serve_with_admin(
    listener: TcpListener,
    engine: Arc<StreamEngine>,
    config: KafkaConfig,
    topic_admin: Arc<dyn TopicAdmin>,
    mut shutdown: watch::Receiver<bool>,
) -> KafkaResult<()> {
    config.validate()?;
    let address = listener.local_addr()?;
    let broker = Arc::new(Broker {
        engine,
        config,
        topic_admin,
        known_topics: KnownTopics::spawn()?,
        next_request_id: AtomicU64::new(1),
    });
    log_info!(
        LogContext::<()>::new().with_feature("kafka"),
        "shard-stream Kafka listener ready",
        audience = LogAudience::Internal,
        address = address
    );
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let broker = Arc::clone(&broker);
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, broker).await {
                        log_error!(
                            LogContext::<()>::new().with_feature("kafka"),
                            "Kafka connection stopped",
                            audience = LogAudience::Internal,
                            peer = peer,
                            error = error
                        );
                    }
                });
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
    Ok(())
}

async fn handle_connection(stream: TcpStream, broker: Arc<Broker>) -> KafkaResult<()> {
    let max_buffered_bytes = broker
        .config
        .max_frame_bytes
        .checked_add(4)
        .ok_or_else(|| KafkaError::Protocol("Kafka frame bound overflow".into()))?;
    let mut input = HandoffBuffer::with_config(
        HandoffBufferConfig::new(max_buffered_bytes).with_read_reserve(CONNECTION_READ_RESERVE),
    );
    let (mut reader, writer) = stream.into_split();
    let output = WriteHandoff::spawn(
        writer,
        WriteHandoffConfig::new(
            CONNECTION_WRITE_QUEUE_SLOTS,
            broker
                .config
                .max_frame_bytes
                .saturating_mul(2)
                .max(CONNECTION_READ_RESERVE),
        ),
    );
    loop {
        let profiling_enabled = broker.config.metrics.enabled();
        let frame_receive_started = profiling_enabled.then(Instant::now);
        let Some(mut bytes) =
            read_kafka_frame(&mut reader, &mut input, broker.config.max_frame_bytes).await?
        else {
            output.drain().await.map_err(|error| {
                KafkaError::Protocol(format!("Kafka write drain failed: {error}"))
            })?;
            return Ok(());
        };
        let frame_received = profiling_enabled.then(Instant::now);
        let frame_bytes = bytes.len();
        let decode_started = profiling_enabled.then(Instant::now);
        let header = decode_request_header_from_buffer(&mut bytes)
            .map_err(|error| KafkaError::Protocol(error.to_string()))?;
        let api_key = ApiKey::try_from(header.request_api_key)
            .map_err(|_| KafkaError::Protocol("unsupported Kafka API key".into()))?;
        let version = header.request_api_version;
        let request = RequestKind::decode(api_key, &mut bytes, version)
            .map_err(|error| KafkaError::Protocol(error.to_string()))?;
        if bytes.has_remaining() {
            return Err(KafkaError::Protocol(
                "Kafka request contains trailing bytes".into(),
            ));
        }
        let profiled = matches!(&request, RequestKind::Produce(request) if request.acks != 0)
            && broker.config.metrics.should_profile();
        if profiled {
            broker.config.metrics.begin_request(frame_bytes);
            broker.config.metrics.record(
                KafkaProfileStage::FrameReceiveWait,
                frame_received
                    .expect("profiled frame has a receive timestamp")
                    .saturating_duration_since(
                        frame_receive_started.expect("profiled frame has a start timestamp"),
                    ),
            );
            broker.config.metrics.record(
                KafkaProfileStage::RequestDecode,
                decode_started
                    .expect("profiled frame has a decode timestamp")
                    .elapsed(),
            );
        }
        let response = Arc::clone(&broker)
            .handle(request, version, profiled)
            .await?;
        let Some(response) = response else {
            continue;
        };
        let response_encode_started = profiled.then(Instant::now);
        let encoded = encode_response(api_key, version, header.correlation_id, response)?;
        if profiled {
            broker.config.metrics.record(
                KafkaProfileStage::ResponseEncode,
                response_encode_started
                    .expect("profiled response has an encode timestamp")
                    .elapsed(),
            );
        }
        if encoded.len().saturating_sub(4) > broker.config.max_frame_bytes {
            return Err(KafkaError::Protocol(
                "Kafka response exceeds the configured frame bound".into(),
            ));
        }
        if profiled {
            let metrics = Arc::clone(&broker.config.metrics);
            let frame_received = frame_received.expect("profiled frame has a receive timestamp");
            output
                .write_with_completion_callback(encoded.freeze(), move |completion| {
                    let stats = completion.stats();
                    metrics.record_nanos(KafkaProfileStage::WriteQueue, stats.queue_nanos);
                    metrics.record_nanos(KafkaProfileStage::WriteSyscall, stats.write_nanos);
                    metrics.record_nanos(KafkaProfileStage::WriteCompletion, stats.e2e_nanos);
                    metrics.record(
                        KafkaProfileStage::RequestCompletion,
                        frame_received.elapsed(),
                    );
                })
                .await
        } else {
            output.write_fire_and_forget(encoded.freeze()).await
        }
        .map_err(|error| KafkaError::Protocol(format!("Kafka write handoff failed: {error}")))?;
    }
}

async fn read_kafka_frame<R>(
    reader: &mut R,
    input: &mut HandoffBuffer,
    max_frame_bytes: usize,
) -> KafkaResult<Option<Bytes>>
where
    R: AsyncRead + Unpin,
{
    loop {
        if input.len() >= 4 {
            let frame_len = i32::from_be_bytes(
                input.peek()[..4]
                    .try_into()
                    .expect("Kafka frame prefix width"),
            );
            let frame_len = usize::try_from(frame_len)
                .map_err(|_| KafkaError::Protocol("negative Kafka frame length".into()))?;
            if frame_len == 0 || frame_len > max_frame_bytes {
                return Err(KafkaError::Protocol(
                    "Kafka frame exceeds the configured bound".into(),
                ));
            }
            let framed_len = frame_len
                .checked_add(4)
                .ok_or_else(|| KafkaError::Protocol("Kafka frame length overflow".into()))?;
            if input.len() >= framed_len {
                return input
                    .split_prefix(framed_len)
                    .map(|frame| Some(frame.slice(4..)))
                    .map_err(|error| {
                        KafkaError::Protocol(format!("Kafka read handoff failed: {error}"))
                    });
            }
        }
        let read = input
            .read_available(reader)
            .await
            .map_err(|error| KafkaError::Protocol(format!("Kafka read failed: {error}")))?;
        if read == 0 {
            if input.is_empty() {
                return Ok(None);
            }
            return Err(KafkaError::Protocol(
                "Kafka connection closed with a partial frame".into(),
            ));
        }
    }
}

fn encode_response(
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    response: ResponseKind,
) -> KafkaResult<BytesMut> {
    let mut body = BytesMut::new();
    ResponseHeader::default()
        .with_correlation_id(correlation_id)
        .encode(&mut body, api_key.response_header_version(version))
        .map_err(|error| KafkaError::Protocol(error.to_string()))?;
    response
        .encode(&mut body, version)
        .map_err(|error| KafkaError::Protocol(error.to_string()))?;
    let length = i32::try_from(body.len())
        .map_err(|_| KafkaError::Protocol("Kafka response is too large".into()))?;
    let mut framed = BytesMut::with_capacity(body.len() + 4);
    framed.put_i32(length);
    framed.extend_from_slice(&body);
    Ok(framed)
}

impl Broker {
    async fn handle(
        self: Arc<Self>,
        request: RequestKind,
        version: i16,
        profiled: bool,
    ) -> KafkaResult<Option<ResponseKind>> {
        if !version_supported(request_key(&request), version) {
            return Err(KafkaError::Protocol(format!(
                "unsupported Kafka {:?} version {version}",
                request_key(&request)
            )));
        }
        if let RequestKind::CreateTopics(request) = request {
            return Ok(Some(ResponseKind::CreateTopics(
                self.create_topics(request).await,
            )));
        }
        let queued_at = profiled.then(Instant::now);
        tokio::task::spawn_blocking(move || {
            if profiled {
                self.config.metrics.record(
                    KafkaProfileStage::BlockingQueue,
                    queued_at
                        .expect("profiled task has a queue timestamp")
                        .elapsed(),
                );
            }
            let started = profiled.then(Instant::now);
            let response = self.handle_blocking(request, profiled);
            if profiled {
                self.config.metrics.record(
                    KafkaProfileStage::BlockingHandler,
                    started
                        .expect("profiled task has a handler timestamp")
                        .elapsed(),
                );
            }
            response
        })
        .await
        .map_err(|error| KafkaError::Protocol(format!("Kafka task failed: {error}")))?
    }

    fn handle_blocking(
        &self,
        request: RequestKind,
        profiled: bool,
    ) -> KafkaResult<Option<ResponseKind>> {
        match request {
            RequestKind::ApiVersions(_) => Ok(Some(ResponseKind::ApiVersions(self.api_versions()))),
            RequestKind::Metadata(request) => {
                Ok(Some(ResponseKind::Metadata(self.metadata(request))))
            }
            RequestKind::Produce(request) => {
                let send_response = request.acks != 0;
                let started = profiled.then(Instant::now);
                let response = self.produce(request, profiled);
                if profiled {
                    self.config.metrics.record(
                        KafkaProfileStage::ProduceHandler,
                        started
                            .expect("profiled Produce request has a handler timestamp")
                            .elapsed(),
                    );
                }
                Ok(send_response.then_some(ResponseKind::Produce(response)))
            }
            RequestKind::Fetch(request) => Ok(Some(ResponseKind::Fetch(self.fetch(request)))),
            RequestKind::ListOffsets(request) => {
                Ok(Some(ResponseKind::ListOffsets(self.list_offsets(request))))
            }
            _ => Err(KafkaError::Protocol(
                "Kafka API was decoded but is not advertised".into(),
            )),
        }
    }

    fn api_versions(&self) -> ApiVersionsResponse {
        ApiVersionsResponse::default().with_api_keys(
            API_VERSIONS
                .iter()
                .map(|(key, min, max)| {
                    ApiVersion::default()
                        .with_api_key(*key as i16)
                        .with_min_version(*min)
                        .with_max_version(*max)
                })
                .collect(),
        )
    }

    fn metadata(&self, request: kafka_protocol::messages::MetadataRequest) -> MetadataResponse {
        let names = match request.topics {
            Some(topics) => topics
                .into_iter()
                .filter_map(|topic| topic.name)
                .map(topic_string)
                .collect::<Vec<_>>(),
            None => self.known_topics.list(),
        };
        let topics = names
            .into_iter()
            .map(|name| self.metadata_topic(name))
            .collect();
        MetadataResponse::default()
            .with_brokers(
                self.config
                    .assignments
                    .brokers()
                    .into_iter()
                    .map(|node| {
                        MetadataResponseBroker::default()
                            .with_node_id(BrokerId(node.broker_id.get() as i32))
                            .with_host(StrBytes::from_string(node.kafka_host.clone()))
                            .with_port(i32::from(node.kafka_port))
                    })
                    .collect(),
            )
            .with_cluster_id(Some(StrBytes::from_static_str(CLUSTER_ID)))
            .with_controller_id(BrokerId(
                self.config.assignments.local_broker_id().get() as i32
            ))
            .with_topics(topics)
    }

    fn metadata_topic(&self, name: String) -> MetadataResponseTopic {
        self.known_topics.insert(name.clone());
        let topic_id = topic_id(&name);
        let partitions = self.engine.topic_partitions(topic_id);
        let error_code = if partitions.is_empty() {
            ResponseError::UnknownTopicOrPartition.code()
        } else {
            0
        };
        MetadataResponseTopic::default()
            .with_error_code(error_code)
            .with_name(Some(topic_name(name)))
            .with_partitions(
                partitions
                    .into_iter()
                    .map(|partition| {
                        let topic_partition = TopicPartition::new(topic_id, partition);
                        let assignment = self.config.assignments.assignment(topic_partition);
                        match assignment {
                            Some(assignment) => {
                                let leader = BrokerId(assignment.leader.get() as i32);
                                let replicas = assignment
                                    .replicas
                                    .iter()
                                    .map(|broker| BrokerId(broker.get() as i32))
                                    .collect::<Vec<_>>();
                                MetadataResponsePartition::default()
                                    .with_partition_index(partition.get() as i32)
                                    .with_leader_id(leader)
                                    .with_replica_nodes(replicas.clone())
                                    .with_isr_nodes(replicas)
                            }
                            None => MetadataResponsePartition::default()
                                .with_partition_index(partition.get() as i32)
                                .with_error_code(ResponseError::UnknownTopicOrPartition.code()),
                        }
                    })
                    .collect(),
            )
    }

    async fn create_topics(
        &self,
        request: kafka_protocol::messages::CreateTopicsRequest,
    ) -> CreateTopicsResponse {
        let mut topics = Vec::with_capacity(request.topics.len());
        for topic in request.topics {
            let name = topic_string(topic.name);
            let native_topic_id = topic_id(&name);
            let configured_rf = self
                .config
                .assignments
                .assignment(TopicPartition::new(
                    native_topic_id,
                    LogicalPartitionId::new(0),
                ))
                .map_or(1, |assignment| assignment.replicas.len() as i16);
            let requested_rf = if topic.replication_factor == -1 {
                configured_rf
            } else {
                topic.replication_factor
            };
            let result = if topic.num_partitions <= 0
                || !topic.assignments.is_empty()
                || !topic.configs.is_empty()
                || (topic.replication_factor != -1
                    && (topic.replication_factor <= 0
                        || topic.replication_factor as usize
                            > self.config.assignments.brokers().len()
                        || topic.replication_factor != configured_rf))
            {
                Err(ResponseError::InvalidRequest)
            } else if request.validate_only {
                Ok(())
            } else {
                self.topic_admin
                    .create_topic(TopicConfig {
                        topic_id: native_topic_id,
                        partitions: topic.num_partitions as u32,
                        shards: None,
                    })
                    .await
                    .map_err(engine_error)
            };
            if result.is_ok() {
                self.known_topics.insert(name.clone());
            }
            topics.push(
                CreatableTopicResult::default()
                    .with_name(topic_name(name))
                    .with_num_partitions(topic.num_partitions)
                    .with_replication_factor(requested_rf)
                    .with_error_code(result.err().map_or(0, |error| error.code())),
            );
        }
        CreateTopicsResponse::default().with_topics(topics)
    }

    fn produce(
        &self,
        request: kafka_protocol::messages::ProduceRequest,
        profiled: bool,
    ) -> ProduceResponse {
        let durability = match request.acks {
            -1 => Some(Durability::AllReplicas),
            0 | 1 => Some(Durability::Leader),
            _ => None,
        };
        let transactional = request.transactional_id.is_some();
        let responses = request
            .topic_data
            .into_iter()
            .map(|topic| {
                let name = topic_string(topic.name);
                self.known_topics.insert(name.clone());
                let topic_id = topic_id(&name);
                let partition_responses = topic
                    .partition_data
                    .into_iter()
                    .map(|partition| {
                        let result = if transactional {
                            Err(ResponseError::UnsupportedForMessageFormat)
                        } else if partition.index < 0 {
                            Err(ResponseError::UnknownTopicOrPartition)
                        } else if !self.config.assignments.is_local_leader(TopicPartition::new(
                            topic_id,
                            LogicalPartitionId::new(partition.index as u32),
                        )) {
                            Err(ResponseError::NotLeaderOrFollower)
                        } else if let Some(durability) = durability {
                            self.append_kafka(
                                topic_id,
                                partition.index as u32,
                                partition.records,
                                durability,
                                profiled,
                            )
                        } else {
                            Err(ResponseError::InvalidRequiredAcks)
                        };
                        let (error_code, base_offset) = match result {
                            Ok(offset) => (0, offset),
                            Err(error) => (error.code(), -1),
                        };
                        PartitionProduceResponse::default()
                            .with_index(partition.index)
                            .with_error_code(error_code)
                            .with_base_offset(base_offset)
                    })
                    .collect();
                TopicProduceResponse::default()
                    .with_name(topic_name(name))
                    .with_partition_responses(partition_responses)
            })
            .collect();
        ProduceResponse::default().with_responses(responses)
    }

    fn append_kafka(
        &self,
        topic_id: TopicId,
        partition: u32,
        records: Option<Bytes>,
        durability: Durability,
        profiled: bool,
    ) -> Result<i64, ResponseError> {
        let validation_started = profiled.then(Instant::now);
        let records = records.ok_or(ResponseError::InvalidRecord)?;
        let metadata = batch::inspect_record_batches(&records)?;
        let record_count = metadata.record_count;
        let producer = metadata.producer;
        if profiled {
            self.config.metrics.records(record_count);
            self.config.metrics.record(
                KafkaProfileStage::RecordValidation,
                validation_started
                    .expect("profiled Produce request has a validation timestamp")
                    .elapsed(),
            );
        }
        let append_started = profiled.then(Instant::now);
        let topic_partition = TopicPartition::new(topic_id, LogicalPartitionId::new(partition));
        let leader_epoch = self
            .config
            .assignments
            .assignment(topic_partition)
            .map(|assignment| assignment.leader_epoch.get())
            .ok_or(ResponseError::NotLeaderOrFollower)?;
        let response = self
            .engine
            .append(AppendRequest {
                request_id: u128::from(self.next_request_id.fetch_add(1, Ordering::Relaxed)),
                topic_id,
                partition_id: LogicalPartitionId::new(partition),
                record_count,
                payload: records,
                durability,
                producer,
                atomic_group: None,
                leader_epoch: Some(leader_epoch),
                extension_context: None,
            })
            .map_err(engine_error)?;
        if profiled {
            self.config.metrics.record(
                KafkaProfileStage::EngineAppend,
                append_started
                    .expect("profiled Produce request has an append timestamp")
                    .elapsed(),
            );
        }
        let through = response
            .last_offset
            .get()
            .checked_add(1)
            .map(LogicalOffset::new)
            .ok_or(ResponseError::OffsetOutOfRange)?;
        let visibility = if durability == Durability::Leader {
            FetchMode::Ordered
        } else {
            FetchMode::Replicated
        };
        let visibility_started = profiled.then(Instant::now);
        self.engine
            .wait_until_visible(topic_partition, through, visibility)
            .map_err(engine_error)?;
        if profiled {
            self.config.metrics.record(
                KafkaProfileStage::VisibilityWait,
                visibility_started
                    .expect("profiled Produce request has a visibility timestamp")
                    .elapsed(),
            );
        }
        offset_i64(response.first_offset)
    }

    fn fetch(&self, request: kafka_protocol::messages::FetchRequest) -> FetchResponse {
        let responses = request
            .topics
            .into_iter()
            .map(|topic| {
                let name = topic_string(topic.topic);
                self.known_topics.insert(name.clone());
                let topic_id = topic_id(&name);
                let partitions = topic
                    .partitions
                    .into_iter()
                    .map(|partition| {
                        self.fetch_partition(
                            topic_id,
                            partition.partition,
                            partition.fetch_offset,
                            partition.partition_max_bytes,
                        )
                    })
                    .collect();
                FetchableTopicResponse::default()
                    .with_topic(topic_name(name))
                    .with_partitions(partitions)
            })
            .collect();
        FetchResponse::default()
            .with_session_id(request.session_id)
            .with_responses(responses)
    }

    fn fetch_partition(
        &self,
        topic_id: TopicId,
        partition: i32,
        fetch_offset: i64,
        partition_max_bytes: i32,
    ) -> PartitionData {
        if partition < 0 || fetch_offset < 0 || partition_max_bytes <= 0 {
            return PartitionData::default()
                .with_partition_index(partition)
                .with_error_code(ResponseError::OffsetOutOfRange.code());
        }
        let topic_partition =
            TopicPartition::new(topic_id, LogicalPartitionId::new(partition as u32));
        if !self.config.assignments.is_local_leader(topic_partition) {
            return PartitionData::default()
                .with_partition_index(partition)
                .with_error_code(ResponseError::NotLeaderOrFollower.code());
        }
        let watermarks = match self.engine.watermarks(topic_partition) {
            Ok(watermarks) => watermarks,
            Err(error) => {
                return PartitionData::default()
                    .with_partition_index(partition)
                    .with_error_code(engine_error(error).code());
            }
        };
        let max_bytes = usize::try_from(partition_max_bytes)
            .unwrap_or(0)
            .min(self.config.max_fetch_bytes);
        let fetch = self.engine.fetch(FetchRequest {
            request_id: u128::from(self.next_request_id.fetch_add(1, Ordering::Relaxed)),
            topic_id,
            partition_id: LogicalPartitionId::new(partition as u32),
            start_offset: LogicalOffset::new(fetch_offset as u64),
            max_bytes: u32::try_from(max_bytes).unwrap_or(u32::MAX),
            mode: FetchMode::Replicated,
        });
        let (error_code, records) = match fetch.and_then(|batches| {
            encode_fetched_records(&batches)
                .map_err(|error| EngineError::InvalidConfig(error.to_string()))
        }) {
            Ok(records) => (0, Some(records)),
            Err(error) => (engine_error(error).code(), None),
        };
        PartitionData::default()
            .with_partition_index(partition)
            .with_error_code(error_code)
            .with_high_watermark(offset_i64_lossy(watermarks.replicated_high_watermark))
            .with_last_stable_offset(offset_i64_lossy(watermarks.replicated_high_watermark))
            .with_log_start_offset(offset_i64_lossy(watermarks.log_start))
            .with_records(records)
    }

    fn list_offsets(
        &self,
        request: kafka_protocol::messages::ListOffsetsRequest,
    ) -> ListOffsetsResponse {
        let topics = request
            .topics
            .into_iter()
            .map(|topic| {
                let name = topic_string(topic.name);
                self.known_topics.insert(name.clone());
                let topic_id = topic_id(&name);
                let partitions = topic
                    .partitions
                    .into_iter()
                    .map(|partition| {
                        let result = if partition.partition_index < 0 {
                            Err(ResponseError::UnknownTopicOrPartition)
                        } else if !self.config.assignments.is_local_leader(TopicPartition::new(
                            topic_id,
                            LogicalPartitionId::new(partition.partition_index as u32),
                        )) {
                            Err(ResponseError::NotLeaderOrFollower)
                        } else {
                            self.engine
                                .watermarks(TopicPartition::new(
                                    topic_id,
                                    LogicalPartitionId::new(partition.partition_index as u32),
                                ))
                                .map_err(engine_error)
                                .and_then(|watermarks| match partition.timestamp {
                                    -2 => offset_i64(watermarks.log_start),
                                    -1 => offset_i64(watermarks.replicated_high_watermark),
                                    _ => Err(ResponseError::UnsupportedVersion),
                                })
                        };
                        let (error_code, offset) = match result {
                            Ok(offset) => (0, offset),
                            Err(error) => (error.code(), -1),
                        };
                        ListOffsetsPartitionResponse::default()
                            .with_partition_index(partition.partition_index)
                            .with_error_code(error_code)
                            .with_offset(offset)
                    })
                    .collect();
                ListOffsetsTopicResponse::default()
                    .with_name(topic_name(name))
                    .with_partitions(partitions)
            })
            .collect();
        ListOffsetsResponse::default().with_topics(topics)
    }
}

fn request_key(request: &RequestKind) -> ApiKey {
    match request {
        RequestKind::Produce(_) => ApiKey::Produce,
        RequestKind::Fetch(_) => ApiKey::Fetch,
        RequestKind::ListOffsets(_) => ApiKey::ListOffsets,
        RequestKind::Metadata(_) => ApiKey::Metadata,
        RequestKind::ApiVersions(_) => ApiKey::ApiVersions,
        RequestKind::CreateTopics(_) => ApiKey::CreateTopics,
        _ => ApiKey::ApiVersions,
    }
}

fn version_supported(key: ApiKey, version: i16) -> bool {
    API_VERSIONS
        .iter()
        .any(|(candidate, min, max)| *candidate == key && (*min..=*max).contains(&version))
}

fn decode_record_sets(mut records: Bytes) -> Result<Vec<RecordSet>, ResponseError> {
    RecordBatchDecoder::decode_all(&mut records).map_err(|_| ResponseError::CorruptMessage)
}

fn encode_fetched_records(
    batches: &[shard_stream_engine::FetchedBatch],
) -> Result<Bytes, ResponseError> {
    let mut encoded = BytesMut::new();
    for batch in batches {
        let mut sets = decode_record_sets(batch.payload.clone())?;
        let mut next_offset = offset_i64(batch.first_offset)?;
        for set in &mut sets {
            for record in &mut set.records {
                record.offset = next_offset;
                next_offset = next_offset
                    .checked_add(1)
                    .ok_or(ResponseError::OffsetOutOfRange)?;
            }
            RecordBatchEncoder::encode(
                &mut encoded,
                &set.records,
                &RecordEncodeOptions {
                    version: 2,
                    compression: set.compression,
                },
            )
            .map_err(|_| ResponseError::CorruptMessage)?;
        }
    }
    Ok(encoded.freeze())
}

fn topic_id(name: &str) -> TopicId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"shard-stream.kafka.topic-name.v1");
    hasher.update(name.as_bytes());
    let digest = hasher.finalize();
    TopicId::new(u128::from_le_bytes(
        digest.as_bytes()[..16].try_into().expect("fixed digest"),
    ))
}

fn topic_name(name: String) -> TopicName {
    StrBytes::from_string(name).into()
}

fn topic_string(name: TopicName) -> String {
    name.to_string()
}

fn offset_i64(offset: LogicalOffset) -> Result<i64, ResponseError> {
    i64::try_from(offset.get()).map_err(|_| ResponseError::OffsetOutOfRange)
}

fn offset_i64_lossy(offset: LogicalOffset) -> i64 {
    i64::try_from(offset.get()).unwrap_or(i64::MAX)
}

fn engine_error(error: EngineError) -> ResponseError {
    match error {
        EngineError::UnknownPartition { .. } | EngineError::UnknownShard(_) => {
            ResponseError::UnknownTopicOrPartition
        }
        EngineError::AdmissionLimited { .. }
        | EngineError::ReplicaReorderLimited { .. }
        | EngineError::ReplicaBufferLimited { .. }
        | EngineError::ReplicaInFlight(_) => ResponseError::ThrottlingQuotaExceeded,
        EngineError::WorkerStopped(_) | EngineError::ObjectDurabilityUnavailable => {
            ResponseError::NotEnoughReplicas
        }
        EngineError::DurabilityUnavailable { .. } => ResponseError::NotEnoughReplicasAfterAppend,
        EngineError::OffsetOutOfRange { .. } => ResponseError::OffsetOutOfRange,
        EngineError::RetentionPinned { .. } => ResponseError::PolicyViolation,
        EngineError::StaleProducerEpoch { .. } => ResponseError::InvalidProducerEpoch,
        EngineError::ProducerSequenceOutOfOrder { .. }
        | EngineError::ProducerRequiresNewEpoch(_)
        | EngineError::DuplicateSequenceMismatch(_) => ResponseError::OutOfOrderSequenceNumber,
        EngineError::Fenced(_) => ResponseError::NotLeaderOrFollower,
        EngineError::UnsupportedDurability(_) => ResponseError::UnsupportedVersion,
        EngineError::TopicAlreadyExists(_) => ResponseError::TopicAlreadyExists,
        EngineError::InvalidConfig(_)
        | EngineError::UnknownAtomicGroup(_)
        | EngineError::AtomicGroupFinalized(_)
        | EngineError::AtomicGroupSequenceOutOfOrder { .. }
        | EngineError::AtomicGroupChunkMismatch(_)
        | EngineError::Sequencer(_) => ResponseError::InvalidRequest,
        EngineError::CorruptState(_) | EngineError::Storage(_) => ResponseError::UnknownServerError,
    }
}

pub type KafkaResult<T> = Result<T, KafkaError>;

#[derive(Debug)]
pub enum KafkaError {
    Io(std::io::Error),
    Protocol(String),
}

impl fmt::Display for KafkaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::Protocol(message) => write!(formatter, "Kafka protocol error: {message}"),
        }
    }
}

impl std::error::Error for KafkaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Protocol(_) => None,
        }
    }
}

impl From<std::io::Error> for KafkaError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use kafka_protocol::messages::RequestHeader;
    use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use kafka_protocol::protocol::{Decodable, encode_request_header_into_buffer};
    use kafka_protocol::records::{
        Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
        NO_TIMESTAMP, Record, TimestampType,
    };
    use shard_stream_core::{BrokerDescriptor, BrokerId, SingleNodeAssignment};
    use shard_stream_engine::EngineConfig;

    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let path = std::env::temp_dir()
                .join(format!("shard-stream-kafka-{}-{nonce}", std::process::id()));
            fs::create_dir_all(&path).expect("temp directory");
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn topic_mapping_is_stable_and_domain_separated() {
        assert_eq!(topic_id("orders"), topic_id("orders"));
        assert_ne!(topic_id("orders"), topic_id("payments"));
    }

    #[test]
    fn kafka_request_profiler_rounds_stride_and_stays_disabled_at_zero() {
        let disabled = KafkaRequestMetrics::default();
        assert!(!disabled.enabled());
        assert!(!disabled.should_profile());
        assert_eq!(disabled.snapshot().profiled_requests, 0);

        let sampled = KafkaRequestMetrics::new(3);
        assert_eq!(sampled.snapshot().sample_stride, 4);
        assert!(sampled.should_profile());
        assert!(!sampled.should_profile());
        assert!(!sampled.should_profile());
        assert!(!sampled.should_profile());
        assert!(sampled.should_profile());
    }

    #[test]
    fn fetched_record_offsets_are_rewritten_to_logical_offsets() {
        let records = test_records();
        let mut payload = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut payload,
            &records,
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .expect("encode");
        let fetched = shard_stream_engine::FetchedBatch {
            batch_id: shard_stream_core::BatchId::new(1),
            event_id: None,
            atomic_group: None,
            first_offset: LogicalOffset::new(11),
            last_offset: LogicalOffset::new(12),
            record_count: std::num::NonZeroU32::new(2).expect("count"),
            payload: payload.freeze(),
            placement: shard_stream_core::Placement {
                virtual_lane_id: shard_stream_core::VirtualLaneId::new(0),
                ring_epoch: shard_stream_core::RingEpoch::new(1),
                leader_epoch: shard_stream_core::LeaderEpoch::new(0),
                sequence: shard_stream_core::PlacementSequence::new(0),
            },
        };
        let encoded = encode_fetched_records(&[fetched]).expect("rewrite");
        let sets = decode_record_sets(encoded).expect("decode");
        assert_eq!(
            sets.into_iter()
                .flat_map(|set| set.records)
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![11, 12]
        );
    }

    #[tokio::test]
    async fn handoff_reader_preserves_fragmented_and_pipelined_frames() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let writer = tokio::spawn(async move {
            client.write_all(&[0, 0]).await.expect("fragment");
            tokio::task::yield_now().await;
            client
                .write_all(&[0, 3, b'a', b'b', b'c', 0, 0, 0, 2, b'd', b'e'])
                .await
                .expect("frames");
        });
        let mut input =
            HandoffBuffer::with_config(HandoffBufferConfig::new(64).with_read_reserve(8));
        let first = read_kafka_frame(&mut server, &mut input, 60)
            .await
            .expect("first read")
            .expect("first frame");
        let second = read_kafka_frame(&mut server, &mut input, 60)
            .await
            .expect("second read")
            .expect("second frame");
        assert_eq!(first, b"abc"[..]);
        assert_eq!(second, b"de"[..]);
        writer.await.expect("writer");
    }

    #[tokio::test]
    #[ignore = "requires local TCP sockets, which are unavailable in some sandboxes"]
    async fn kafka_wire_create_produce_and_fetch_round_trip() {
        let temp = TempDir::new();
        let engine = Arc::new(
            StreamEngine::open(EngineConfig {
                data_dir: temp.0.clone(),
                object_store_dir: None,
                shard_count: 2,
                virtual_lane_count: 2,
                replication_factor: 1,
                min_in_sync_replicas: 1,
                queue_slots_per_shard: 64,
                queue_bytes_per_shard: 2 * 1024 * 1024,
                target_pack_bytes: 1024 * 1024,
                max_pack_age: std::time::Duration::from_secs(1),
                max_batch_bytes: 1024 * 1024,
                max_fetch_bytes: 1024 * 1024,
                append_linger: std::time::Duration::from_millis(1),
            })
            .expect("engine"),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let server_engine = Arc::clone(&engine);
        let topology = Arc::new(
            SingleNodeAssignment::new(
                BrokerDescriptor {
                    broker_id: BrokerId::new(0),
                    incarnation: 1,
                    rack: String::new(),
                    zone: String::new(),
                    internal_rest: "http://127.0.0.1:7420".into(),
                    internal_replication: String::new(),
                    advertised_rest: "http://127.0.0.1:7420".into(),
                    advertised_grpc: "http://127.0.0.1:7421".into(),
                    kafka_host: "127.0.0.1".into(),
                    kafka_port: address.port(),
                    cpu_capacity: 1,
                    disk_capacity_bytes: 0,
                    network_capacity_bytes_per_sec: 0,
                    certificate_digest: [0; 32],
                },
                2,
            )
            .expect("assignment"),
        );
        let kafka_metrics = Arc::new(KafkaRequestMetrics::new(1));
        let server_metrics = Arc::clone(&kafka_metrics);
        let server = tokio::spawn(async move {
            serve(
                listener,
                server_engine,
                KafkaConfig {
                    assignments: topology,
                    max_frame_bytes: 4 * 1024 * 1024,
                    max_fetch_bytes: 1024 * 1024,
                    metrics: server_metrics,
                },
                shutdown_receiver,
            )
            .await
        });
        let mut socket = TcpStream::connect(address).await.expect("connect");

        let created = kafka_round_trip(
            &mut socket,
            ApiKey::CreateTopics,
            4,
            1,
            RequestKind::CreateTopics(CreateTopicsRequest::default().with_topics(vec![
                    CreatableTopic::default()
                        .with_name(topic_name("orders".into()))
                        .with_num_partitions(1)
                        .with_replication_factor(1),
                ])),
        )
        .await;
        let ResponseKind::CreateTopics(created) = created else {
            panic!("wrong create response");
        };
        assert_eq!(created.topics[0].error_code, 0);

        let records = test_records();
        let mut record_bytes = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut record_bytes,
            &records,
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .expect("records");
        let produced = kafka_round_trip(
            &mut socket,
            ApiKey::Produce,
            8,
            2,
            RequestKind::Produce(
                kafka_protocol::messages::ProduceRequest::default()
                    .with_acks(1)
                    .with_topic_data(vec![
                        TopicProduceData::default()
                            .with_name(topic_name("orders".into()))
                            .with_partition_data(vec![
                                PartitionProduceData::default()
                                    .with_index(0)
                                    .with_records(Some(record_bytes.freeze())),
                            ]),
                    ]),
            ),
        )
        .await;
        let ResponseKind::Produce(produced) = produced else {
            panic!("wrong produce response");
        };
        assert_eq!(produced.responses[0].partition_responses[0].error_code, 0);
        assert_eq!(produced.responses[0].partition_responses[0].base_offset, 0);

        let fetched = kafka_round_trip(
            &mut socket,
            ApiKey::Fetch,
            6,
            3,
            RequestKind::Fetch(
                kafka_protocol::messages::FetchRequest::default()
                    .with_max_bytes(1024 * 1024)
                    .with_topics(vec![
                        FetchTopic::default()
                            .with_topic(topic_name("orders".into()))
                            .with_partitions(vec![
                                FetchPartition::default()
                                    .with_partition(0)
                                    .with_fetch_offset(0)
                                    .with_partition_max_bytes(1024 * 1024),
                            ]),
                    ]),
            ),
        )
        .await;
        let ResponseKind::Fetch(fetched) = fetched else {
            panic!("wrong fetch response");
        };
        let partition = &fetched.responses[0].partitions[0];
        assert_eq!(partition.error_code, 0);
        assert_eq!(partition.high_watermark, 2);
        let sets = decode_record_sets(partition.records.clone().expect("records")).expect("decode");
        assert_eq!(
            sets.into_iter()
                .flat_map(|set| set.records)
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        let profile = kafka_metrics.snapshot();
        assert_eq!(profile.profiled_requests, 1);
        assert_eq!(profile.profiled_records, 2);
        assert_eq!(
            profile
                .stages
                .iter()
                .map(|stage| (stage.stage, stage.samples))
                .collect::<Vec<_>>(),
            KafkaProfileStage::ALL
                .into_iter()
                .map(|stage| (stage.name(), 1))
                .collect::<Vec<_>>()
        );

        let _ = shutdown_sender.send(true);
        server.await.expect("join").expect("serve");
    }

    async fn kafka_round_trip(
        socket: &mut TcpStream,
        api_key: ApiKey,
        version: i16,
        correlation_id: i32,
        request: RequestKind,
    ) -> ResponseKind {
        let header = RequestHeader::default()
            .with_request_api_key(api_key as i16)
            .with_request_api_version(version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("shard-stream-test")));
        let mut body = BytesMut::new();
        encode_request_header_into_buffer(&mut body, &header).expect("header");
        request.encode(&mut body, version).expect("request");
        socket.write_i32(body.len() as i32).await.expect("length");
        socket.write_all(&body).await.expect("body");

        let response_len = socket.read_i32().await.expect("response length");
        let mut response = vec![0; response_len as usize];
        socket.read_exact(&mut response).await.expect("response");
        let mut response = Bytes::from(response);
        let header =
            ResponseHeader::decode(&mut response, api_key.response_header_version(version))
                .expect("response header");
        assert_eq!(header.correlation_id, correlation_id);
        ResponseKind::decode(api_key, &mut response, version).expect("response body")
    }

    fn test_records() -> Vec<Record> {
        vec![
            Record {
                transactional: false,
                control: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: 0,
                sequence: NO_SEQUENCE,
                timestamp: NO_TIMESTAMP,
                key: None,
                value: Some(Bytes::from_static(b"one")),
                headers: Default::default(),
            },
            Record {
                offset: 1,
                sequence: 0,
                value: Some(Bytes::from_static(b"two")),
                ..Record {
                    transactional: false,
                    control: false,
                    partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                    producer_id: NO_PRODUCER_ID,
                    producer_epoch: NO_PRODUCER_EPOCH,
                    timestamp_type: TimestampType::Creation,
                    offset: 0,
                    sequence: NO_SEQUENCE,
                    timestamp: NO_TIMESTAMP,
                    key: None,
                    value: None,
                    headers: Default::default(),
                }
            },
        ]
    }
}
