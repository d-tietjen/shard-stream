//! Initial Kafka wire compatibility for the shard-stream engine.
//!
//! The adapter deliberately advertises only the versions and APIs implemented
//! here. Kafka topic names map deterministically to stream-native `u128` topic
//! identifiers with BLAKE3. Kafka record batches remain opaque engine payloads
//! and are decoded only to count records, enforce unsupported transaction
//! boundaries, derive idempotent producer metadata, and rewrite logical
//! offsets in fetch responses.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::{Buf, BufMut, Bytes, BytesMut};
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
    NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, RecordBatchDecoder, RecordBatchEncoder,
    RecordEncodeOptions, RecordSet,
};
use shard_stream_core::{LogicalOffset, LogicalPartitionId, TopicId, TopicPartition};
use shard_stream_engine::{EngineError, StreamEngine, TopicConfig};
use shard_stream_protocol::{AppendRequest, Durability, FetchMode, FetchRequest, ProducerIdentity};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{error, info};

const MAX_FRAME_HARD_LIMIT: usize = 128 * 1024 * 1024;
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

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub advertised_host: String,
    pub advertised_port: u16,
    pub node_id: i32,
    pub max_frame_bytes: usize,
    pub max_fetch_bytes: usize,
}

impl KafkaConfig {
    fn validate(&self) -> KafkaResult<()> {
        if self.advertised_host.is_empty() {
            return Err(KafkaError::Protocol(
                "Kafka advertised host cannot be empty".into(),
            ));
        }
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

struct Broker {
    engine: Arc<StreamEngine>,
    config: KafkaConfig,
    known_topics: Mutex<BTreeSet<String>>,
    next_request_id: AtomicU64,
}

pub async fn serve(
    listener: TcpListener,
    engine: Arc<StreamEngine>,
    config: KafkaConfig,
    mut shutdown: watch::Receiver<bool>,
) -> KafkaResult<()> {
    config.validate()?;
    let address = listener.local_addr()?;
    let broker = Arc::new(Broker {
        engine,
        config,
        known_topics: Mutex::new(BTreeSet::new()),
        next_request_id: AtomicU64::new(1),
    });
    info!(%address, "shard-stream Kafka listener ready");
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let broker = Arc::clone(&broker);
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, broker).await {
                        error!(%peer, %error, "Kafka connection stopped");
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

async fn handle_connection(mut stream: TcpStream, broker: Arc<Broker>) -> KafkaResult<()> {
    loop {
        let frame_len = match stream.read_i32().await {
            Ok(length) => length,
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let frame_len = usize::try_from(frame_len)
            .map_err(|_| KafkaError::Protocol("negative Kafka frame length".into()))?;
        if frame_len == 0 || frame_len > broker.config.max_frame_bytes {
            return Err(KafkaError::Protocol(
                "Kafka frame exceeds the configured bound".into(),
            ));
        }
        let mut frame = vec![0; frame_len];
        stream.read_exact(&mut frame).await?;
        let mut bytes = Bytes::from(frame);
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
        let broker_for_request = Arc::clone(&broker);
        let response =
            tokio::task::spawn_blocking(move || broker_for_request.handle(request, version))
                .await
                .map_err(|error| KafkaError::Protocol(format!("Kafka task failed: {error}")))??;
        let Some(response) = response else {
            continue;
        };
        let encoded = encode_response(api_key, version, header.correlation_id, response)?;
        if encoded.len().saturating_sub(4) > broker.config.max_frame_bytes {
            return Err(KafkaError::Protocol(
                "Kafka response exceeds the configured frame bound".into(),
            ));
        }
        stream.write_all(&encoded).await?;
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
    fn handle(&self, request: RequestKind, version: i16) -> KafkaResult<Option<ResponseKind>> {
        if !version_supported(request_key(&request), version) {
            return Err(KafkaError::Protocol(format!(
                "unsupported Kafka {:?} version {version}",
                request_key(&request)
            )));
        }
        match request {
            RequestKind::ApiVersions(_) => Ok(Some(ResponseKind::ApiVersions(self.api_versions()))),
            RequestKind::Metadata(request) => {
                Ok(Some(ResponseKind::Metadata(self.metadata(request))))
            }
            RequestKind::CreateTopics(request) => Ok(Some(ResponseKind::CreateTopics(
                self.create_topics(request),
            ))),
            RequestKind::Produce(request) => {
                let send_response = request.acks != 0;
                let response = self.produce(request);
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
            None => lock(&self.known_topics).iter().cloned().collect(),
        };
        let topics = names
            .into_iter()
            .map(|name| self.metadata_topic(name))
            .collect();
        MetadataResponse::default()
            .with_brokers(vec![
                MetadataResponseBroker::default()
                    .with_node_id(BrokerId(self.config.node_id))
                    .with_host(StrBytes::from_string(self.config.advertised_host.clone()))
                    .with_port(i32::from(self.config.advertised_port)),
            ])
            .with_cluster_id(Some(StrBytes::from_static_str(CLUSTER_ID)))
            .with_controller_id(BrokerId(self.config.node_id))
            .with_topics(topics)
    }

    fn metadata_topic(&self, name: String) -> MetadataResponseTopic {
        lock(&self.known_topics).insert(name.clone());
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
                        MetadataResponsePartition::default()
                            .with_partition_index(partition.get() as i32)
                            .with_leader_id(BrokerId(self.config.node_id))
                            .with_replica_nodes(vec![BrokerId(self.config.node_id)])
                            .with_isr_nodes(vec![BrokerId(self.config.node_id)])
                    })
                    .collect(),
            )
    }

    fn create_topics(
        &self,
        request: kafka_protocol::messages::CreateTopicsRequest,
    ) -> CreateTopicsResponse {
        let topics = request
            .topics
            .into_iter()
            .map(|topic| {
                let name = topic_string(topic.name);
                let result = if topic.num_partitions <= 0
                    || !topic.assignments.is_empty()
                    || !topic.configs.is_empty()
                    || !matches!(topic.replication_factor, -1 | 1)
                {
                    Err(ResponseError::InvalidRequest)
                } else if request.validate_only {
                    Ok(())
                } else {
                    self.engine
                        .create_topic(TopicConfig {
                            topic_id: topic_id(&name),
                            partitions: topic.num_partitions as u32,
                            shards: None,
                        })
                        .map_err(engine_error)
                };
                if result.is_ok() {
                    lock(&self.known_topics).insert(name.clone());
                }
                CreatableTopicResult::default()
                    .with_name(topic_name(name))
                    .with_num_partitions(topic.num_partitions)
                    .with_replication_factor(1)
                    .with_error_code(result.err().map_or(0, |error| error.code()))
            })
            .collect();
        CreateTopicsResponse::default().with_topics(topics)
    }

    fn produce(&self, request: kafka_protocol::messages::ProduceRequest) -> ProduceResponse {
        let durability = match request.acks {
            -1 => Some(Durability::Quorum),
            0 | 1 => Some(Durability::Leader),
            _ => None,
        };
        let transactional = request.transactional_id.is_some();
        let responses = request
            .topic_data
            .into_iter()
            .map(|topic| {
                let name = topic_string(topic.name);
                lock(&self.known_topics).insert(name.clone());
                let topic_id = topic_id(&name);
                let partition_responses = topic
                    .partition_data
                    .into_iter()
                    .map(|partition| {
                        let result = if transactional {
                            Err(ResponseError::UnsupportedForMessageFormat)
                        } else if partition.index < 0 {
                            Err(ResponseError::UnknownTopicOrPartition)
                        } else if let Some(durability) = durability {
                            self.append_kafka(
                                topic_id,
                                partition.index as u32,
                                partition.records,
                                durability,
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
    ) -> Result<i64, ResponseError> {
        let records = records.ok_or(ResponseError::InvalidRecord)?;
        let sets = decode_record_sets(records.clone())?;
        let flattened = sets.iter().flat_map(|set| &set.records).collect::<Vec<_>>();
        let record_count =
            u32::try_from(flattened.len()).map_err(|_| ResponseError::MessageTooLarge)?;
        if record_count == 0
            || flattened
                .iter()
                .any(|record| record.transactional || record.control)
        {
            return Err(ResponseError::UnsupportedForMessageFormat);
        }
        let producer = kafka_producer_identity(&flattened)?;
        let response = self
            .engine
            .append(AppendRequest {
                request_id: u128::from(self.next_request_id.fetch_add(1, Ordering::Relaxed)),
                topic_id,
                partition_id: LogicalPartitionId::new(partition),
                record_count,
                payload: records.to_vec(),
                durability,
                producer,
                leader_epoch: None,
            })
            .map_err(engine_error)?;
        offset_i64(response.first_offset)
    }

    fn fetch(&self, request: kafka_protocol::messages::FetchRequest) -> FetchResponse {
        let responses = request
            .topics
            .into_iter()
            .map(|topic| {
                let name = topic_string(topic.topic);
                lock(&self.known_topics).insert(name.clone());
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
            start_offset: LogicalOffset::new(fetch_offset as u128),
            max_bytes: u32::try_from(max_bytes).unwrap_or(u32::MAX),
            mode: FetchMode::Ordered,
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
            .with_high_watermark(offset_i64_lossy(watermarks.contiguous_log_end))
            .with_last_stable_offset(offset_i64_lossy(watermarks.last_stable_offset))
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
                lock(&self.known_topics).insert(name.clone());
                let topic_id = topic_id(&name);
                let partitions = topic
                    .partitions
                    .into_iter()
                    .map(|partition| {
                        let result = if partition.partition_index < 0 {
                            Err(ResponseError::UnknownTopicOrPartition)
                        } else {
                            self.engine
                                .watermarks(TopicPartition::new(
                                    topic_id,
                                    LogicalPartitionId::new(partition.partition_index as u32),
                                ))
                                .map_err(engine_error)
                                .and_then(|watermarks| match partition.timestamp {
                                    -2 => offset_i64(watermarks.log_start),
                                    -1 => offset_i64(watermarks.contiguous_log_end),
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

fn kafka_producer_identity(
    records: &[&kafka_protocol::records::Record],
) -> Result<Option<ProducerIdentity>, ResponseError> {
    let first = records.first().ok_or(ResponseError::InvalidRecord)?;
    if first.producer_id == NO_PRODUCER_ID {
        if records.iter().any(|record| {
            record.producer_id != NO_PRODUCER_ID
                || record.producer_epoch != NO_PRODUCER_EPOCH
                || record.sequence != NO_SEQUENCE
        }) {
            return Err(ResponseError::InvalidProducerEpoch);
        }
        return Ok(None);
    }
    if first.producer_id < 0
        || first.producer_epoch == NO_PRODUCER_EPOCH
        || first.producer_epoch < 0
        || first.sequence == NO_SEQUENCE
        || first.sequence < 0
        || records.iter().enumerate().any(|(index, record)| {
            record.producer_id != first.producer_id
                || record.producer_epoch != first.producer_epoch
                || i64::from(record.sequence) != i64::from(first.sequence) + index as i64
        })
    {
        return Err(ResponseError::InvalidProducerEpoch);
    }
    Ok(Some(ProducerIdentity {
        producer_id: first.producer_id as u128,
        epoch: first.producer_epoch as u32,
        first_sequence: first.sequence as u64,
    }))
}

fn encode_fetched_records(
    batches: &[shard_stream_engine::FetchedBatch],
) -> Result<Bytes, ResponseError> {
    let mut encoded = BytesMut::new();
    for batch in batches {
        let mut sets = decode_record_sets(Bytes::copy_from_slice(&batch.payload))?;
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
        EngineError::AdmissionLimited { .. } => ResponseError::ThrottlingQuotaExceeded,
        EngineError::WorkerStopped(_) | EngineError::ObjectDurabilityUnavailable => {
            ResponseError::NotEnoughReplicas
        }
        EngineError::DurabilityUnavailable { .. } => ResponseError::NotEnoughReplicasAfterAppend,
        EngineError::OffsetOutOfRange { .. } => ResponseError::OffsetOutOfRange,
        EngineError::StaleProducerEpoch { .. } => ResponseError::InvalidProducerEpoch,
        EngineError::ProducerSequenceOutOfOrder { .. }
        | EngineError::ProducerRequiresNewEpoch(_)
        | EngineError::DuplicateSequenceMismatch(_) => ResponseError::OutOfOrderSequenceNumber,
        EngineError::Fenced(_) => ResponseError::NotLeaderOrFollower,
        EngineError::UnsupportedDurability(_) => ResponseError::UnsupportedVersion,
        EngineError::TopicAlreadyExists(_) => ResponseError::TopicAlreadyExists,
        EngineError::InvalidConfig(_) | EngineError::Sequencer(_) => ResponseError::InvalidRequest,
        EngineError::CorruptState(_) | EngineError::Storage(_) => ResponseError::UnknownServerError,
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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
        Compression, NO_PARTITION_LEADER_EPOCH, NO_TIMESTAMP, Record, TimestampType,
    };
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
            first_offset: LogicalOffset::new(11),
            last_offset: LogicalOffset::new(12),
            record_count: std::num::NonZeroU32::new(2).expect("count"),
            payload: payload.to_vec(),
            placement: shard_stream_core::Placement {
                shard_id: shard_stream_core::ShardId::new(0),
                ring_epoch: shard_stream_core::RingEpoch::new(1),
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
    #[ignore = "requires local TCP sockets, which are unavailable in some sandboxes"]
    async fn kafka_wire_create_produce_and_fetch_round_trip() {
        let temp = TempDir::new();
        let engine = Arc::new(
            StreamEngine::open(EngineConfig {
                data_dir: temp.0.clone(),
                object_store_dir: None,
                shard_count: 2,
                replication_factor: 1,
                min_in_sync_replicas: 1,
                queue_slots_per_shard: 64,
                queue_bytes_per_shard: 2 * 1024 * 1024,
                target_pack_bytes: 1024 * 1024,
                max_batch_bytes: 1024 * 1024,
                max_fetch_bytes: 1024 * 1024,
            })
            .expect("engine"),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let server_engine = Arc::clone(&engine);
        let server = tokio::spawn(async move {
            serve(
                listener,
                server_engine,
                KafkaConfig {
                    advertised_host: "127.0.0.1".into(),
                    advertised_port: address.port(),
                    node_id: 0,
                    max_frame_bytes: 4 * 1024 * 1024,
                    max_fetch_bytes: 1024 * 1024,
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
