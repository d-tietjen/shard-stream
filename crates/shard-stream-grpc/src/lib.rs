use std::pin::Pin;
use std::sync::Arc;

use shard_stream_core::{
    AssignmentProvider, LogicalOffset, LogicalPartitionId, TopicId, TopicPartition,
};
#[cfg(test)]
use shard_stream_core::{BrokerDescriptor, BrokerId, SingleNodeAssignment};
use shard_stream_engine::{EngineError, FetchedBatch, StreamEngine};
use shard_stream_protocol::{
    AppendRequest as EngineAppendRequest, AtomicGroupIdentity, Durability as EngineDurability,
    FetchMode as EngineFetchMode, FetchRequest as EngineFetchRequest, LogicalTopicResolver,
    ProducerIdentity, TopicAccess, TopicRouteError, TopicRouteErrorCode,
};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio_stream::Stream;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status};

pub mod v1 {
    tonic::include_proto!("shardstream.v1");
}

use v1::consumer_service_server::{ConsumerService, ConsumerServiceServer};
use v1::producer_service_server::{ProducerService, ProducerServiceServer};

#[derive(Clone)]
struct GrpcService {
    engine: Arc<StreamEngine>,
    assignments: Arc<dyn AssignmentProvider>,
    topics: Option<Arc<dyn LogicalTopicResolver>>,
    stream_capacity: usize,
}

#[allow(clippy::too_many_arguments)]
pub async fn serve(
    listener: TcpListener,
    engine: Arc<StreamEngine>,
    assignments: Arc<dyn AssignmentProvider>,
    topics: Option<Arc<dyn LogicalTopicResolver>>,
    stream_capacity: usize,
    max_request_bytes: usize,
    max_response_bytes: usize,
    shutdown: watch::Receiver<bool>,
) -> Result<(), tonic::transport::Error> {
    let service = GrpcService {
        engine,
        assignments,
        topics,
        stream_capacity,
    };
    let producer = ProducerServiceServer::new(service.clone())
        .max_decoding_message_size(max_request_bytes)
        .max_encoding_message_size(max_response_bytes);
    let consumer = ConsumerServiceServer::new(service)
        .max_decoding_message_size(max_request_bytes)
        .max_encoding_message_size(max_response_bytes);
    tonic::transport::Server::builder()
        .add_service(producer)
        .add_service(consumer)
        .serve_with_incoming_shutdown(
            TcpListenerStream::new(listener),
            wait_for_shutdown(shutdown),
        )
        .await
}

type GrpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl ProducerService for GrpcService {
    type AppendStream = GrpcStream<v1::AppendResponse>;

    async fn append(
        &self,
        request: Request<tonic::Streaming<v1::AppendRequest>>,
    ) -> Result<Response<Self::AppendStream>, Status> {
        let mut inbound = request.into_inner();
        let (sender, receiver) = mpsc::channel(self.stream_capacity);
        let engine = Arc::clone(&self.engine);
        let assignments = Arc::clone(&self.assignments);
        let topics = self.topics.clone();
        tokio::spawn(async move {
            loop {
                let message = match inbound.message().await {
                    Ok(Some(message)) => message,
                    Ok(None) => break,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        break;
                    }
                };
                let mut request = match decode_append(message) {
                    Ok(request) => request,
                    Err(status) => {
                        let _ = sender.send(Err(status)).await;
                        break;
                    }
                };
                if let Err(status) = resolve_legacy_topic(
                    topics.as_deref(),
                    &mut request.topic_id,
                    request.partition_id,
                    TopicAccess::Append,
                ) {
                    let _ = sender.send(Err(status)).await;
                    break;
                }
                if let Err(status) = ensure_local_owner(assignments.as_ref(), &request) {
                    let _ = sender.send(Err(status)).await;
                    break;
                }
                if request.leader_epoch.is_none() {
                    let topic_partition =
                        TopicPartition::new(request.topic_id, request.partition_id);
                    request.leader_epoch = assignments
                        .assignment(topic_partition)
                        .map(|assignment| assignment.leader_epoch.get());
                }
                let engine = Arc::clone(&engine);
                let result = tokio::task::spawn_blocking(move || engine.append(request))
                    .await
                    .map_err(|error| Status::internal(format!("engine task failed: {error}")))
                    .and_then(|result| result.map(encode_append).map_err(engine_status));
                let failed = result.is_err();
                if sender.send(result).await.is_err() || failed {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

#[tonic::async_trait]
impl ConsumerService for GrpcService {
    type FetchStream = GrpcStream<v1::FetchBatch>;

    async fn fetch(
        &self,
        request: Request<v1::FetchRequest>,
    ) -> Result<Response<Self::FetchStream>, Status> {
        let mut request = decode_fetch(request.into_inner())?;
        resolve_legacy_topic(
            self.topics.as_deref(),
            &mut request.topic_id,
            request.partition_id,
            TopicAccess::Fetch,
        )?;
        ensure_local_fetch_owner(self.assignments.as_ref(), &request)?;
        let request_id = request.request_id;
        let engine = Arc::clone(&self.engine);
        let batches = tokio::task::spawn_blocking(move || engine.fetch(request))
            .await
            .map_err(|error| Status::internal(format!("engine task failed: {error}")))?
            .map_err(engine_status)?;
        let (sender, receiver) = mpsc::channel(self.stream_capacity);
        tokio::spawn(async move {
            for batch in batches {
                if sender
                    .send(Ok(encode_fetch_batch(request_id, batch)))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

fn resolve_legacy_topic(
    topics: Option<&dyn LogicalTopicResolver>,
    topic_id: &mut TopicId,
    partition_id: LogicalPartitionId,
    access: TopicAccess,
) -> Result<(), Status> {
    let Some(topics) = topics else {
        return Ok(());
    };
    if let Some(physical) = topics
        .resolve_legacy(TopicPartition::new(*topic_id, partition_id), access)
        .map_err(generation_status)?
    {
        *topic_id = physical.topic_id;
    }
    Ok(())
}

fn generation_status(error: TopicRouteError) -> Status {
    let message = error.to_string();
    match error.code {
        TopicRouteErrorCode::InvalidInput => Status::invalid_argument(message),
        TopicRouteErrorCode::NotFound => Status::not_found(message),
        TopicRouteErrorCode::WritePaused | TopicRouteErrorCode::Unavailable => {
            Status::unavailable(message)
        }
        TopicRouteErrorCode::SdkUpgradeRequired
        | TopicRouteErrorCode::StaleGeneration
        | TopicRouteErrorCode::Corrupt => Status::failed_precondition(message),
    }
}

fn ensure_local_owner(
    assignments: &dyn AssignmentProvider,
    request: &EngineAppendRequest,
) -> Result<(), Status> {
    ensure_local(
        assignments,
        TopicPartition::new(request.topic_id, request.partition_id),
    )
}

fn ensure_local_fetch_owner(
    assignments: &dyn AssignmentProvider,
    request: &EngineFetchRequest,
) -> Result<(), Status> {
    ensure_local(
        assignments,
        TopicPartition::new(request.topic_id, request.partition_id),
    )
}

fn ensure_local(
    assignments: &dyn AssignmentProvider,
    topic_partition: TopicPartition,
) -> Result<(), Status> {
    if assignments.is_local_leader(topic_partition) {
        return Ok(());
    }
    let owner = assignments
        .leader(topic_partition)
        .ok_or_else(|| Status::not_found("unknown topic partition assignment"))?;
    Err(Status::failed_precondition(format!(
        "not partition coordinator; retry {}",
        owner.advertised_grpc
    )))
}

fn decode_append(request: v1::AppendRequest) -> Result<EngineAppendRequest, Status> {
    let producer = request
        .producer
        .map(|producer| -> Result<ProducerIdentity, Status> {
            Ok(ProducerIdentity {
                producer_id: decode_u128(producer.producer_id, "producer_id")?,
                epoch: producer.epoch,
                first_sequence: producer.first_sequence,
                event_id: producer.event_id.map(|event_id| {
                    shard_stream_core::RecordId::new(
                        (u128::from(event_id.high) << 64) | u128::from(event_id.low),
                    )
                }),
            })
        })
        .transpose()?;
    Ok(EngineAppendRequest {
        request_id: decode_u128(request.request_id, "request_id")?,
        topic_id: TopicId::new(decode_u128(request.topic_id, "topic_id")?),
        partition_id: LogicalPartitionId::new(request.partition_id),
        record_count: request.record_count,
        payload: request.payload.into(),
        durability: match v1::Durability::try_from(request.durability) {
            Ok(v1::Durability::Leader) => EngineDurability::Leader,
            Ok(v1::Durability::Quorum) => EngineDurability::Quorum,
            Ok(v1::Durability::AllReplicas) => EngineDurability::AllReplicas,
            Ok(v1::Durability::Object) => EngineDurability::Object,
            _ => return Err(Status::invalid_argument("durability is required")),
        },
        producer,
        atomic_group: request
            .atomic_group
            .map(|group| -> Result<AtomicGroupIdentity, Status> {
                Ok(AtomicGroupIdentity {
                    transaction_id: decode_u128(group.transaction_id, "transaction_id")?,
                    chunk_sequence: group.chunk_sequence,
                })
            })
            .transpose()?,
        leader_epoch: request.leader_epoch,
        extension_context: None,
    })
}

fn decode_fetch(request: v1::FetchRequest) -> Result<EngineFetchRequest, Status> {
    Ok(EngineFetchRequest {
        request_id: decode_u128(request.request_id, "request_id")?,
        topic_id: TopicId::new(decode_u128(request.topic_id, "topic_id")?),
        partition_id: LogicalPartitionId::new(request.partition_id),
        start_offset: LogicalOffset::new(request.start_offset),
        max_bytes: request.max_bytes,
        mode: match v1::FetchMode::try_from(request.mode) {
            Ok(v1::FetchMode::Ordered) => EngineFetchMode::Ordered,
            Ok(v1::FetchMode::Replicated) => EngineFetchMode::Replicated,
            Ok(v1::FetchMode::AsAvailable) => EngineFetchMode::AsAvailable,
            _ => return Err(Status::invalid_argument("fetch mode is required")),
        },
    })
}

fn encode_append(response: shard_stream_protocol::AppendResponse) -> v1::AppendResponse {
    v1::AppendResponse {
        request_id: Some(encode_u128(response.request_id)),
        batch_id: Some(encode_u128(response.batch_id.get())),
        first_offset: response.first_offset.get(),
        last_offset: response.last_offset.get(),
        placement: Some(v1::Placement {
            virtual_lane_id: response.placement.virtual_lane_id.get(),
            ring_epoch: response.placement.ring_epoch.get(),
            sequence: response.placement.sequence.get(),
            leader_epoch: response.placement.leader_epoch.get(),
        }),
        record_id: Some(encode_u128(response.record_id.get())),
        visible_through: response.visible_through.get(),
    }
}

fn encode_fetch_batch(request_id: u128, batch: FetchedBatch) -> v1::FetchBatch {
    v1::FetchBatch {
        request_id: Some(encode_u128(request_id)),
        batch_id: Some(encode_u128(batch.batch_id.get())),
        first_offset: batch.first_offset.get(),
        last_offset: batch.last_offset.get(),
        payload: batch.payload.to_vec(),
        placement: Some(v1::Placement {
            virtual_lane_id: batch.placement.virtual_lane_id.get(),
            ring_epoch: batch.placement.ring_epoch.get(),
            sequence: batch.placement.sequence.get(),
            leader_epoch: batch.placement.leader_epoch.get(),
        }),
        record_count: batch.record_count.get(),
        event_id: batch.event_id.map(|event_id| encode_u128(event_id.get())),
        atomic_group: batch.atomic_group.map(|group| v1::AtomicGroupIdentity {
            transaction_id: Some(encode_u128(group.transaction_id)),
            chunk_sequence: group.chunk_sequence,
        }),
    }
}

fn decode_u128(value: Option<v1::UInt128>, field: &'static str) -> Result<u128, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument(format!("{field} is required")))?;
    Ok((u128::from(value.high) << 64) | u128::from(value.low))
}

fn encode_u128(value: u128) -> v1::UInt128 {
    v1::UInt128 {
        high: (value >> 64) as u64,
        low: value as u64,
    }
}

fn engine_status(error: EngineError) -> Status {
    match error {
        EngineError::InvalidConfig(_) => Status::invalid_argument(error.to_string()),
        EngineError::UnknownPartition { .. }
        | EngineError::UnknownShard(_)
        | EngineError::UnknownAtomicGroup(_) => Status::not_found(error.to_string()),
        EngineError::AdmissionLimited { .. }
        | EngineError::ReplicaReorderLimited { .. }
        | EngineError::ReplicaBufferLimited { .. }
        | EngineError::ReplicaInFlight(_)
        | EngineError::WorkerStopped(_)
        | EngineError::DurabilityUnavailable { .. }
        | EngineError::ObjectDurabilityUnavailable => Status::unavailable(error.to_string()),
        EngineError::OffsetOutOfRange { .. } => Status::out_of_range(error.to_string()),
        EngineError::RetentionPinned { .. } => Status::failed_precondition(error.to_string()),
        EngineError::UnsupportedDurability(_) => Status::unimplemented(error.to_string()),
        EngineError::TopicAlreadyExists(_)
        | EngineError::Fenced(_)
        | EngineError::StaleProducerEpoch { .. }
        | EngineError::ProducerSequenceOutOfOrder { .. }
        | EngineError::ProducerRequiresNewEpoch(_)
        | EngineError::DuplicateSequenceMismatch(_)
        | EngineError::AtomicGroupFinalized(_)
        | EngineError::AtomicGroupSequenceOutOfOrder { .. }
        | EngineError::AtomicGroupChunkMismatch(_)
        | EngineError::Sequencer(_) => Status::failed_precondition(error.to_string()),
        EngineError::CorruptState(_) | EngineError::Storage(_) => {
            Status::internal(error.to_string())
        }
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if !*shutdown.borrow() {
        let _ = shutdown.changed().await;
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use shard_stream_core::TopicId;
    use shard_stream_engine::{EngineConfig, TopicConfig};

    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let path = std::env::temp_dir()
                .join(format!("shard-stream-grpc-{}-{nonce}", std::process::id()));
            fs::create_dir_all(&path).expect("temp dir");
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn uint128_round_trip_preserves_full_width() {
        let value = u128::MAX - 7;
        assert_eq!(
            decode_u128(Some(encode_u128(value)), "value").expect("decode"),
            value
        );
    }

    #[tokio::test]
    #[ignore = "requires local TCP sockets, which are unavailable in some sandboxes"]
    async fn standard_grpc_clients_append_and_fetch() {
        let temp = TempDir::new();
        let engine = Arc::new(
            StreamEngine::open(EngineConfig {
                data_dir: temp.0.clone(),
                object_store_dir: None,
                shard_count: 2,
                virtual_lane_count: 2,
                replication_factor: 1,
                min_in_sync_replicas: 1,
                queue_slots_per_shard: 32,
                queue_bytes_per_shard: 2 * 1024 * 1024,
                target_pack_bytes: 1024,
                max_pack_age: std::time::Duration::from_secs(1),
                max_batch_bytes: 64 * 1024,
                max_fetch_bytes: 1024 * 1024,
                append_linger: std::time::Duration::from_millis(1),
            })
            .expect("engine"),
        );
        engine
            .create_topic(TopicConfig {
                topic_id: TopicId::new(9),
                partitions: 1,
                shards: None,
            })
            .expect("topic");

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
                    advertised_grpc: format!("http://{address}"),
                    kafka_host: "127.0.0.1".into(),
                    kafka_port: 9092,
                    cpu_capacity: 1,
                    disk_capacity_bytes: 0,
                    network_capacity_bytes_per_sec: 0,
                    certificate_digest: [0; 32],
                },
                2,
            )
            .expect("assignment"),
        );
        let server = tokio::spawn(async move {
            serve(
                listener,
                server_engine,
                topology,
                None,
                8,
                1024 * 1024,
                1024 * 1024,
                shutdown_receiver,
            )
            .await
        });

        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .expect("endpoint")
            .connect()
            .await
            .expect("connect");
        let mut producer = v1::producer_service_client::ProducerServiceClient::new(channel.clone());
        let request = v1::AppendRequest {
            request_id: Some(encode_u128(1)),
            topic_id: Some(encode_u128(9)),
            partition_id: 0,
            record_count: 1,
            payload: b"grpc".to_vec(),
            durability: v1::Durability::Leader as i32,
            producer: None,
            leader_epoch: None,
            atomic_group: None,
        };
        let mut appended = producer
            .append(tokio_stream::iter([request]))
            .await
            .expect("append RPC")
            .into_inner();
        assert_eq!(
            appended
                .message()
                .await
                .expect("stream")
                .expect("append response")
                .first_offset,
            0
        );

        let mut consumer = v1::consumer_service_client::ConsumerServiceClient::new(channel);
        let mut fetched = consumer
            .fetch(v1::FetchRequest {
                request_id: Some(encode_u128(2)),
                topic_id: Some(encode_u128(9)),
                partition_id: 0,
                start_offset: 0,
                max_bytes: 1024,
                mode: v1::FetchMode::Ordered as i32,
            })
            .await
            .expect("fetch RPC")
            .into_inner();
        let batch = fetched
            .message()
            .await
            .expect("stream")
            .expect("fetch batch");
        assert_eq!(batch.payload, b"grpc");
        assert_eq!(batch.record_count, 1);

        let _ = shutdown_sender.send(true);
        server.await.expect("server task").expect("server");
    }
}
