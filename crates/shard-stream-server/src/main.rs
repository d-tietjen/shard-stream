use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Path as FsPath, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::header::{ACCEPT, CONTENT_LENGTH, CONTENT_TYPE, HeaderName};
use axum::http::{HeaderMap, HeaderValue, Response, StatusCode};
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post, put};
use clap::Parser;
use eden_logger::{LogAudience, LogContext, WriterConfig, log_error, log_info};
use serde::{Deserialize, Serialize};
use shard_stream_core::{
    AssignmentProvider, BrokerDescriptor, BrokerId, LogicalOffset, LogicalPartitionId, RecordId,
    RingEpoch, ShardId, SingleNodeAssignment, TopicId, TopicPartition,
};
use shard_stream_engine::{EngineConfig, EngineError, StreamEngine, TopicConfig};
use shard_stream_protocol::{
    AppendRequest, Durability, FetchMode, FetchRequest, LogicalCursorPosition, LogicalTopicBinding,
    LogicalTopicCursor, NATIVE_APPEND_RESPONSE_CONTENT_TYPE, NativeFetchBatch, ProducerIdentity,
    TopicHaMode, WireError, encode_append_response, encode_fetch_batches_segmented,
};
use shard_stream_server::ServerBuilder;
use tokio::net::TcpListener;
use tokio::sync::watch;

const OPENAPI: &str = include_str!("../openapi/shard-stream-v1.json");
const NATIVE_BATCH_CONTENT_TYPE: &str = "application/vnd.shard-stream.batch.v1";
const ADMIN_TOKEN_HEADER: &str = "x-shard-stream-admin-token";

#[derive(Debug, Parser)]
#[command(name = "shard-stream-server", version, about)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:7420")]
    listen: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:7421")]
    grpc_listen: SocketAddr,
    #[cfg(feature = "kafka")]
    #[arg(long, default_value = "127.0.0.1:9092")]
    kafka_listen: SocketAddr,
    #[cfg(feature = "kafka")]
    #[arg(long, default_value = "127.0.0.1")]
    kafka_advertised_host: String,
    #[cfg(feature = "kafka")]
    #[arg(long, default_value_t = 9092)]
    kafka_advertised_port: u16,
    #[cfg(feature = "kafka")]
    #[arg(
        long,
        env = "SHARD_STREAM_KAFKA_PROFILE_SAMPLE_STRIDE",
        default_value_t = 0
    )]
    kafka_profile_sample_stride: u64,
    #[arg(long, env = "SHARD_STREAM_ADMIN_TOKEN", hide_env_values = true)]
    admin_token: Option<String>,
    #[arg(long)]
    h3_listen: Option<SocketAddr>,
    #[arg(long, requires = "h3_listen")]
    h3_certificate: Option<PathBuf>,
    #[arg(long, requires = "h3_listen")]
    h3_private_key: Option<PathBuf>,
    #[arg(long, default_value = "./var/shard-stream")]
    data_dir: PathBuf,
    #[arg(long)]
    object_store_dir: Option<PathBuf>,
    #[arg(long, default_value_t = 4)]
    shards: u32,
    #[arg(long, env = "SHARD_STREAM_VIRTUAL_LANES")]
    virtual_lanes: Option<u32>,
    #[arg(
        long,
        env = "SHARD_STREAM_MAX_IN_FLIGHT_BATCHES",
        default_value_t = 4096
    )]
    queue_slots_per_shard: usize,
    #[arg(
        long,
        env = "SHARD_STREAM_LANE_BUFFER_BYTES",
        default_value_t = 512 * 1024 * 1024
    )]
    queue_bytes_per_shard: usize,
    #[arg(
        long,
        env = "SHARD_STREAM_OBJECT_PACK_BYTES",
        default_value_t = 128 * 1024 * 1024
    )]
    target_pack_bytes: u64,
    #[arg(long, env = "SHARD_STREAM_MAX_PACK_AGE_MS", default_value_t = 1_000)]
    max_pack_age_ms: u64,
    #[arg(
        long,
        env = "SHARD_STREAM_MAX_BATCH_BYTES",
        default_value_t = 4 * 1024 * 1024
    )]
    max_batch_bytes: usize,
    #[arg(
        long,
        env = "SHARD_STREAM_MAX_REQUEST_BYTES",
        default_value_t = 32 * 1024 * 1024
    )]
    max_request_bytes: usize,
    #[arg(long, env = "SHARD_STREAM_APPEND_LINGER_MS", default_value_t = 2)]
    append_linger_ms: u64,
    #[arg(
        long,
        env = "SHARD_STREAM_MAX_FETCH_BYTES",
        default_value_t = 64 * 1024 * 1024
    )]
    max_fetch_bytes: usize,
}

#[derive(Clone)]
struct AppState {
    engine: Arc<StreamEngine>,
    topology: Arc<SingleNodeAssignment>,
    metrics: Arc<HttpMetrics>,
    #[cfg(feature = "kafka")]
    kafka_metrics: Arc<shard_stream_kafka::KafkaRequestMetrics>,
    next_request_id: Arc<AtomicU64>,
    max_fetch_bytes: usize,
    admin_token: Option<Arc<str>>,
}

#[derive(Debug, Default)]
struct HttpMetrics {
    requests: AtomicU64,
    errors: AtomicU64,
    appended_batches: AtomicU64,
    appended_bytes: AtomicU64,
    fetched_batches: AtomicU64,
    fetched_bytes: AtomicU64,
}

#[tokio::main]
async fn main() {
    eden_logger::init(WriterConfig::default());
    eden_logger::init_from_env();
    if let Err(error) = run(Args::parse()).await {
        log_error!(
            LogContext::<()>::new().with_feature("server"),
            "shard-stream server stopped",
            audience = LogAudience::Internal,
            error = error
        );
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), AppError> {
    validate_args(&args)?;
    reject_private_data(&args.data_dir)?;

    let h3_config = match (
        args.h3_listen,
        args.h3_certificate.clone(),
        args.h3_private_key.clone(),
    ) {
        (None, None, None) => None,
        (Some(listen), Some(certificate_path), Some(private_key_path)) => {
            Some(shard_stream_h3::H3Config {
                listen,
                certificate_path,
                private_key_path,
                max_batch_bytes: args.max_batch_bytes,
                max_fetch_bytes: args.max_fetch_bytes,
            })
        }
        _ => {
            return Err(AppError::bad_request(
                "h3_listen, h3_certificate, and h3_private_key must be supplied together",
            ));
        }
    };

    let virtual_lanes = args.virtual_lanes.unwrap_or(args.shards);
    let engine = Arc::new(open_public_engine(EngineConfig {
        data_dir: args.data_dir.clone(),
        object_store_dir: args.object_store_dir,
        shard_count: args.shards,
        virtual_lane_count: virtual_lanes,
        replication_factor: 1,
        min_in_sync_replicas: 1,
        queue_slots_per_shard: args.queue_slots_per_shard,
        queue_bytes_per_shard: args.queue_bytes_per_shard,
        target_pack_bytes: args.target_pack_bytes,
        max_pack_age: Duration::from_millis(args.max_pack_age_ms),
        max_batch_bytes: args.max_batch_bytes,
        max_fetch_bytes: args.max_fetch_bytes,
        append_linger: Duration::from_millis(args.append_linger_ms),
    })?);
    validate_rf1_state(&engine)?;

    #[cfg(feature = "kafka")]
    let (kafka_host, kafka_port) = (
        args.kafka_advertised_host.clone(),
        args.kafka_advertised_port,
    );
    #[cfg(not(feature = "kafka"))]
    let (kafka_host, kafka_port) = (String::new(), 0);
    let topology = Arc::new(
        SingleNodeAssignment::new(
            BrokerDescriptor {
                broker_id: BrokerId::new(0),
                incarnation: 1,
                rack: String::new(),
                zone: String::new(),
                internal_rest: format!("http://{}", args.listen),
                internal_replication: String::new(),
                advertised_rest: format!("http://{}", args.listen),
                advertised_grpc: format!("http://{}", args.grpc_listen),
                kafka_host,
                kafka_port,
                cpu_capacity: 1,
                disk_capacity_bytes: 0,
                network_capacity_bytes_per_sec: 0,
                certificate_digest: [0; 32],
            },
            virtual_lanes,
        )
        .map_err(AppError::bad_request)?,
    );
    #[cfg(feature = "kafka")]
    let kafka_metrics = Arc::new(shard_stream_kafka::KafkaRequestMetrics::new(
        args.kafka_profile_sample_stride,
    ));
    let state = AppState {
        engine,
        topology,
        metrics: Arc::new(HttpMetrics::default()),
        #[cfg(feature = "kafka")]
        kafka_metrics,
        next_request_id: Arc::new(AtomicU64::new(1)),
        max_fetch_bytes: args.max_fetch_bytes,
        admin_token: args.admin_token.map(Arc::from),
    };

    let routes = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(readiness))
        .route("/metrics", get(metrics))
        .route("/v1/topology", get(get_topology))
        .route("/openapi.json", get(openapi))
        .route("/v1/topics", post(create_topic))
        .route("/v1/topics/{topic_id}/binding", get(topic_binding))
        .route(
            "/v1/topics/{topic_id}/partitions/{partition_id}/cursor",
            get(initial_cursor),
        )
        .route(
            "/v1/topics/{topic_id}/partitions/{partition_id}/ring",
            put(reconfigure_ring),
        )
        .route(
            "/v1/topics/{topic_id}/partitions/{partition_id}/records",
            post(append).get(fetch),
        )
        .route(
            "/v1/topics/{topic_id}/partitions/{partition_id}/watermarks",
            get(watermarks),
        )
        .route(
            "/v1/topics/{topic_id}/partitions/{partition_id}/retained-bytes",
            get(retained_bytes),
        )
        .route(
            "/v1/topics/{topic_id}/partitions/{partition_id}/retention",
            put(update_retention),
        );
    #[cfg(feature = "kafka")]
    let routes = routes.route("/v1/diagnostics/kafka-profile", get(kafka_profile));
    let routes = routes
        .layer(DefaultBodyLimit::max(args.max_request_bytes))
        .with_state(state.clone());
    let builder = ServerBuilder::new(routes);
    let app = builder.build_routes();

    let listener = TcpListener::bind(args.listen)
        .await
        .map_err(|error| AppError::internal(format!("bind REST listener: {error}")))?;
    let grpc_listener = TcpListener::bind(args.grpc_listen)
        .await
        .map_err(|error| AppError::internal(format!("bind gRPC listener: {error}")))?;
    #[cfg(feature = "kafka")]
    let kafka_listener = TcpListener::bind(args.kafka_listen)
        .await
        .map_err(|error| AppError::internal(format!("bind Kafka listener: {error}")))?;

    log_info!(
        LogContext::<()>::new().with_feature("server"),
        "single-node RF1 shard-stream listeners ready",
        audience = LogAudience::Internal,
        rest = args.listen,
        grpc = args.grpc_listen
    );
    #[cfg(feature = "kafka")]
    log_info!(
        LogContext::<()>::new().with_feature("server"),
        "single-node RF1 shard-stream Kafka listener ready",
        audience = LogAudience::Internal,
        kafka = args.kafka_listen
    );

    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();
    let rest_shutdown = shutdown_receiver.clone();
    tasks.spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(wait_for_shutdown(rest_shutdown))
            .await
            .map_err(|error| AppError::internal(format!("HTTP server failed: {error}")))
    });

    let grpc_engine = Arc::clone(&state.engine);
    let grpc_assignments: Arc<dyn AssignmentProvider> = state.topology.clone();
    let grpc_shutdown = shutdown_receiver.clone();
    let grpc_max_response_bytes = args.max_fetch_bytes.saturating_add(1024 * 1024);
    tasks.spawn(async move {
        shard_stream_grpc::serve(
            grpc_listener,
            grpc_engine,
            grpc_assignments,
            None,
            128,
            args.max_request_bytes,
            grpc_max_response_bytes,
            grpc_shutdown,
        )
        .await
        .map_err(|error| AppError::internal(format!("gRPC server failed: {error}")))
    });

    #[cfg(feature = "kafka")]
    {
        let kafka_engine = Arc::clone(&state.engine);
        let kafka_shutdown = shutdown_receiver.clone();
        let kafka_config = shard_stream_kafka::KafkaConfig {
            assignments: state.topology.clone(),
            max_frame_bytes: args.max_request_bytes,
            max_fetch_bytes: args.max_fetch_bytes,
            metrics: Arc::clone(&state.kafka_metrics),
        };
        tasks.spawn(async move {
            shard_stream_kafka::serve(kafka_listener, kafka_engine, kafka_config, kafka_shutdown)
                .await
                .map_err(|error| AppError::internal(format!("Kafka server failed: {error}")))
        });
    }

    if let Some(config) = h3_config {
        let h3_engine = Arc::clone(&state.engine);
        let h3_shutdown = shutdown_receiver.clone();
        tasks.spawn(async move {
            shard_stream_h3::serve(config, h3_engine, h3_shutdown)
                .await
                .map_err(|error| AppError::internal(format!("HTTP/3 server failed: {error}")))
        });
    }

    for extension_task in builder.tasks(shutdown_receiver.clone()) {
        tasks.spawn(async move {
            extension_task
                .await
                .map_err(|error| AppError::internal(format!("server extension failed: {error}")))
        });
    }

    let first_result = tokio::select! {
        result = tasks.join_next() => result,
        () = shutdown_signal() => None,
    };
    let _ = shutdown_sender.send(true);
    let mut first_error = first_result.and_then(server_task_error);
    while let Some(result) = tasks.join_next().await {
        if let Some(error) = server_task_error(result)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    state.engine.sync()?;
    first_error.map_or(Ok(()), Err)
}

fn validate_args(args: &Args) -> Result<(), AppError> {
    if args.shards == 0 || args.virtual_lanes == Some(0) {
        return Err(AppError::bad_request(
            "shards and virtual_lanes must be nonzero",
        ));
    }
    if args.max_request_bytes < args.max_batch_bytes {
        return Err(AppError::bad_request(
            "max_request_bytes must be at least max_batch_bytes",
        ));
    }
    if args.max_request_bytes > 128 * 1024 * 1024 {
        return Err(AppError::bad_request(
            "max_request_bytes cannot exceed 128 MiB",
        ));
    }
    if args.append_linger_ms > 60_000 {
        return Err(AppError::bad_request(
            "append_linger_ms cannot exceed 60000",
        ));
    }
    if args.admin_token.as_deref() == Some("") {
        return Err(AppError::bad_request("admin token cannot be empty"));
    }
    Ok(())
}

fn reject_private_data(data_dir: &FsPath) -> Result<(), AppError> {
    for marker in ["ha-metadata", "active-active", "ha-leases.ssl"] {
        if data_dir.join(marker).exists() {
            return Err(AppError::private_data(format!(
                "{} contains {marker}; open this data directory with shard-stream-private",
                data_dir.display()
            )));
        }
    }
    Ok(())
}

fn open_public_engine(config: EngineConfig) -> Result<StreamEngine, AppError> {
    StreamEngine::open(config).map_err(|error| match error {
        EngineError::InvalidConfig(message)
            if message.contains("RF>1")
                || message.contains("replication_factor")
                || message.contains("replication factor")
                || message.contains("min_in_sync_replicas") =>
        {
            AppError::private_data(format!(
                "{message}; open this data directory with shard-stream-private"
            ))
        }
        other => AppError::from(other),
    })
}

fn validate_rf1_state(engine: &StreamEngine) -> Result<(), AppError> {
    for partition in engine.all_partitions() {
        let policy = engine.partition_replication_policy(partition)?;
        if policy.policy_generation != 1
            || policy.replication_factor != 1
            || policy.min_in_sync_replicas != 1
        {
            return Err(AppError::private_data(format!(
                "topic partition {}/{} has policy generation {}, RF {}, min ISR {}; open this data directory with shard-stream-private",
                partition.topic_id,
                partition.partition_id,
                policy.policy_generation,
                policy.replication_factor,
                policy.min_in_sync_replicas
            )));
        }
    }
    Ok(())
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "deployment": "single_node",
        "replication_factor": 1,
        "min_in_sync_replicas": 1
    }))
}

async fn readiness() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ready",
        "writable": true,
        "deployment": "single_node",
        "replication_factor": 1,
        "min_in_sync_replicas": 1
    }))
}

async fn get_topology(State(state): State<AppState>) -> impl IntoResponse {
    let local = state.topology.broker();
    let broker = serde_json::json!({
        "node_id": local.broker_id.get(),
        "advertised_rest": local.advertised_rest,
        "advertised_grpc": local.advertised_grpc
    });
    #[cfg(feature = "kafka")]
    let broker = {
        let mut broker = broker;
        if let Some(fields) = broker.as_object_mut() {
            fields.insert("kafka_host".into(), local.kafka_host.clone().into());
            fields.insert("kafka_port".into(), local.kafka_port.into());
        }
        broker
    };
    Json(serde_json::json!({
        "deployment": "single_node",
        "replication_factor": 1,
        "min_in_sync_replicas": 1,
        "broker": broker
    }))
}

#[cfg(feature = "kafka")]
async fn kafka_profile(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.kafka_metrics.snapshot())
}

fn openapi_document() -> serde_json::Value {
    let document: serde_json::Value =
        serde_json::from_str(OPENAPI).expect("embedded OpenAPI document must be valid JSON");
    #[cfg(feature = "kafka")]
    {
        document
    }
    #[cfg(not(feature = "kafka"))]
    {
        let mut document = document;
        document["paths"]
            .as_object_mut()
            .expect("OpenAPI paths must be an object")
            .remove("/v1/diagnostics/kafka-profile");
        document
    }
}

async fn openapi() -> impl IntoResponse {
    Json(openapi_document())
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let metrics = &state.metrics;
    let body = format!(
        concat!(
            "shard_stream_http_requests_total {}\n",
            "shard_stream_http_errors_total {}\n",
            "shard_stream_http_appended_batches_total {}\n",
            "shard_stream_http_appended_bytes_total {}\n",
            "shard_stream_http_fetched_batches_total {}\n",
            "shard_stream_http_fetched_bytes_total {}\n",
            "shard_stream_replication_factor 1\n",
            "shard_stream_min_in_sync_replicas 1\n"
        ),
        metrics.requests.load(Ordering::Relaxed),
        metrics.errors.load(Ordering::Relaxed),
        metrics.appended_batches.load(Ordering::Relaxed),
        metrics.appended_bytes.load(Ordering::Relaxed),
        metrics.fetched_batches.load(Ordering::Relaxed),
        metrics.fetched_bytes.load(Ordering::Relaxed),
    );
    #[cfg(feature = "kafka")]
    let body = {
        let mut body = body;
        state.kafka_metrics.export_prometheus(&mut body);
        body
    };
    (
        StatusCode::OK,
        [(CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
}

#[derive(Debug, Deserialize)]
struct CreateTopicBody {
    topic_id: String,
    partitions: u32,
    shards: Option<Vec<u32>>,
}

#[derive(Debug, Serialize)]
struct TopicResponse {
    topic_id: String,
    partitions: u32,
    replication_factor: u32,
    min_in_sync_replicas: u32,
}

async fn create_topic(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateTopicBody>,
) -> Result<impl IntoResponse, AppError> {
    request(&state);
    require_admin(&state, &headers)?;
    let topic_id = parse_topic_id(&body.topic_id)?;
    let topic = TopicConfig {
        topic_id,
        partitions: body.partitions,
        shards: body
            .shards
            .map(|shards| shards.into_iter().map(ShardId::new).collect()),
    };
    execute({
        let engine = Arc::clone(&state.engine);
        move || engine.create_topic(topic)
    })
    .await
    .inspect_err(|_| error(&state))?;
    Ok((
        StatusCode::CREATED,
        Json(TopicResponse {
            topic_id: topic_id.to_string(),
            partitions: body.partitions,
            replication_factor: 1,
            min_in_sync_replicas: 1,
        }),
    ))
}

async fn topic_binding(
    State(state): State<AppState>,
    Path(topic_id): Path<String>,
) -> Result<Json<LogicalTopicBinding>, AppError> {
    request(&state);
    let topic_id = parse_topic_id(&topic_id)?;
    let engine = Arc::clone(&state.engine);
    let binding = execute(move || {
        let partitions = engine.topic_partitions(topic_id);
        let Some(first) = partitions.first().copied() else {
            return Err(EngineError::UnknownPartition {
                topic_id,
                partition_id: LogicalPartitionId::new(0),
            });
        };
        let policy = engine.partition_replication_policy(TopicPartition::new(topic_id, first))?;
        if policy.policy_generation != 1
            || policy.replication_factor != 1
            || policy.min_in_sync_replicas != 1
        {
            return Err(EngineError::CorruptState(
                "non-RF1 topic requires shard-stream-private".into(),
            ));
        }
        Ok(LogicalTopicBinding {
            logical_topic_id: topic_id,
            physical_topic_id: topic_id,
            partitions: u32::try_from(partitions.len()).map_err(|_| {
                EngineError::CorruptState("topic partition count exceeds u32".into())
            })?,
            policy_generation: 1,
            data_generation: 1,
            mode: TopicHaMode::ActivePassive,
            profile: None,
            finalized_epoch_hash: None,
        })
    })
    .await
    .inspect_err(|_| error(&state))?;
    Ok(Json(binding))
}

async fn initial_cursor(
    State(state): State<AppState>,
    Path((topic_id, partition_id)): Path<(String, u32)>,
) -> Result<Json<LogicalTopicCursor>, AppError> {
    request(&state);
    let topic_id = parse_topic_id(&topic_id)?;
    let partition_id = LogicalPartitionId::new(partition_id);
    let engine = Arc::clone(&state.engine);
    execute(move || {
        if !engine.topic_partitions(topic_id).contains(&partition_id) {
            return Err(EngineError::UnknownPartition {
                topic_id,
                partition_id,
            });
        }
        Ok(())
    })
    .await
    .inspect_err(|_| error(&state))?;
    Ok(Json(LogicalTopicCursor {
        topic_partition: TopicPartition::new(topic_id, partition_id),
        data_generation: 1,
        position: LogicalCursorPosition::ActivePassive {
            next_offset: LogicalOffset::new(0),
        },
    }))
}

#[derive(Debug, Deserialize)]
struct RingBody {
    ring_epoch: u64,
    shards: Vec<u32>,
}

async fn reconfigure_ring(
    State(state): State<AppState>,
    Path((topic_id, partition_id)): Path<(String, u32)>,
    headers: HeaderMap,
    Json(body): Json<RingBody>,
) -> Result<StatusCode, AppError> {
    request(&state);
    require_admin(&state, &headers)?;
    let partition = TopicPartition::new(
        parse_topic_id(&topic_id)?,
        LogicalPartitionId::new(partition_id),
    );
    execute({
        let engine = Arc::clone(&state.engine);
        move || {
            engine.reconfigure_partition(
                partition,
                RingEpoch::new(body.ring_epoch),
                body.shards.into_iter().map(ShardId::new).collect(),
            )
        }
    })
    .await
    .inspect_err(|_| error(&state))?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
struct AppendQuery {
    request_id: Option<String>,
    #[serde(default = "default_record_count")]
    record_count: u32,
    #[serde(default = "default_durability")]
    durability: String,
    producer_id: Option<String>,
    producer_epoch: Option<u32>,
    first_sequence: Option<u64>,
    event_id: Option<String>,
    expected_data_generation: Option<u64>,
    leader_epoch: Option<u64>,
}

#[derive(Debug, Serialize)]
struct AppendResponseBody {
    request_id: String,
    batch_id: String,
    record_id: String,
    first_offset: String,
    last_offset: String,
    visible_through: String,
    shard_id: u32,
    ring_epoch: u64,
    leader_epoch: u64,
    placement_sequence: String,
    replication_factor: u32,
}

async fn append(
    State(state): State<AppState>,
    Path((topic_id, partition_id)): Path<(String, u32)>,
    Query(query): Query<AppendQuery>,
    headers: HeaderMap,
    payload: Bytes,
) -> Result<Response<Body>, AppError> {
    request(&state);
    if let Some(generation) = query.expected_data_generation
        && generation != 1
    {
        return Err(AppError::stale_generation(generation));
    }
    let producer = parse_producer(
        query.producer_id,
        query.producer_epoch,
        query.first_sequence,
        query.event_id,
    )?;
    if query.expected_data_generation.is_some()
        && producer.is_none_or(|producer| producer.event_id.is_none())
    {
        return Err(AppError::bad_request(
            "generation-aware append requires producer identity and event_id",
        ));
    }
    let payload_bytes = payload.len() as u64;
    let append = AppendRequest {
        request_id: query
            .request_id
            .as_deref()
            .map(|value| parse_u128("request_id", value))
            .transpose()?
            .unwrap_or_else(|| u128::from(state.next_request_id.fetch_add(1, Ordering::Relaxed))),
        topic_id: parse_topic_id(&topic_id)?,
        partition_id: LogicalPartitionId::new(partition_id),
        record_count: query.record_count,
        payload,
        durability: parse_durability(&query.durability)?,
        producer,
        atomic_group: None,
        leader_epoch: query.leader_epoch,
        extension_context: None,
    };
    let response = execute({
        let engine = Arc::clone(&state.engine);
        move || engine.append(append)
    })
    .await
    .inspect_err(|_| error(&state))?;
    state
        .metrics
        .appended_batches
        .fetch_add(1, Ordering::Relaxed);
    state
        .metrics
        .appended_bytes
        .fetch_add(payload_bytes, Ordering::Relaxed);
    render_append(response, &headers)
}

fn render_append(
    response: shard_stream_protocol::AppendResponse,
    headers: &HeaderMap,
) -> Result<Response<Body>, AppError> {
    if headers
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|media| media.trim() == NATIVE_APPEND_RESPONSE_CONTENT_TYPE)
        })
    {
        let mut rendered = Response::new(Body::from(encode_append_response(response)));
        rendered.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static(NATIVE_APPEND_RESPONSE_CONTENT_TYPE),
        );
        return Ok(rendered);
    }
    Ok(Json(AppendResponseBody {
        request_id: response.request_id.to_string(),
        batch_id: response.batch_id.to_string(),
        record_id: response.record_id.to_string(),
        first_offset: response.first_offset.to_string(),
        last_offset: response.last_offset.to_string(),
        visible_through: response.visible_through.to_string(),
        shard_id: response.placement.virtual_lane_id.get(),
        ring_epoch: response.placement.ring_epoch.get(),
        leader_epoch: response.placement.leader_epoch.get(),
        placement_sequence: response.placement.sequence.to_string(),
        replication_factor: 1,
    })
    .into_response())
}

#[derive(Debug, Deserialize)]
struct FetchQuery {
    #[serde(default = "default_offset")]
    offset: String,
    max_bytes: Option<u32>,
    #[serde(default = "default_fetch_mode")]
    mode: String,
    request_id: Option<String>,
    expected_data_generation: Option<u64>,
    cursor: Option<String>,
}

async fn fetch(
    State(state): State<AppState>,
    Path((topic_id, partition_id)): Path<(String, u32)>,
    Query(query): Query<FetchQuery>,
) -> Result<Response<Body>, AppError> {
    request(&state);
    if let Some(generation) = query.expected_data_generation
        && generation != 1
    {
        return Err(AppError::stale_generation(generation));
    }
    let topic_id = parse_topic_id(&topic_id)?;
    let partition_id = LogicalPartitionId::new(partition_id);
    let mut cursor = query
        .cursor
        .as_deref()
        .map(LogicalTopicCursor::decode_hex)
        .transpose()
        .map_err(|error| AppError::bad_request(format!("invalid logical cursor: {error}")))?;
    if let Some(value) = &cursor {
        if value.topic_partition != TopicPartition::new(topic_id, partition_id) {
            return Err(AppError::bad_request(
                "logical cursor belongs to another topic partition",
            ));
        }
        if value.data_generation != 1 || value.mode() != TopicHaMode::ActivePassive {
            return Err(AppError::stale_generation(value.data_generation));
        }
    }
    let scalar_offset = LogicalOffset::new(parse_u64("offset", &query.offset)?);
    let start_offset = match &cursor {
        Some(LogicalTopicCursor {
            position: LogicalCursorPosition::ActivePassive { next_offset },
            ..
        }) => *next_offset,
        Some(_) => return Err(AppError::bad_request("active-active cursor is unsupported")),
        None => scalar_offset,
    };
    let request_id = query
        .request_id
        .as_deref()
        .map(|value| parse_u128("request_id", value))
        .transpose()?
        .unwrap_or_else(|| u128::from(state.next_request_id.fetch_add(1, Ordering::Relaxed)));
    let batches = execute({
        let engine = Arc::clone(&state.engine);
        move || {
            engine.fetch(FetchRequest {
                request_id,
                topic_id,
                partition_id,
                start_offset,
                max_bytes: query
                    .max_bytes
                    .unwrap_or_else(|| u32::try_from(state.max_fetch_bytes).unwrap_or(u32::MAX)),
                mode: parse_fetch_mode(&query.mode)?,
            })
        }
    })
    .await
    .inspect_err(|_| error(&state))?;
    let payload_bytes = batches
        .iter()
        .map(|batch| batch.payload.len() as u64)
        .sum::<u64>();
    let native = batches
        .into_iter()
        .map(|batch| NativeFetchBatch {
            request_id,
            batch_id: batch.batch_id,
            event_id: batch.event_id,
            atomic_group: batch.atomic_group,
            first_offset: batch.first_offset,
            last_offset: batch.last_offset,
            record_count: batch.record_count,
            placement: batch.placement,
            payload: batch.payload,
        })
        .collect::<Vec<_>>();
    let next_cursor = if let Some(mut value) = cursor.take() {
        if let Some(last) = native.last() {
            value.position = LogicalCursorPosition::ActivePassive {
                next_offset: LogicalOffset::new(last.last_offset.get().saturating_add(1)),
            };
        }
        Some(
            value
                .encode_hex()
                .map_err(|error| AppError::internal(format!("encode cursor: {error}")))?,
        )
    } else {
        None
    };
    state
        .metrics
        .fetched_batches
        .fetch_add(native.len() as u64, Ordering::Relaxed);
    state
        .metrics
        .fetched_bytes
        .fetch_add(payload_bytes, Ordering::Relaxed);
    render_fetch(&state, native, next_cursor)
}

fn render_fetch(
    state: &AppState,
    batches: Vec<NativeFetchBatch>,
    next_cursor: Option<String>,
) -> Result<Response<Body>, AppError> {
    let encoded = encode_fetch_batches_segmented(
        &batches,
        state
            .max_fetch_bytes
            .saturating_add(batches.len().saturating_mul(256)),
    )?;
    let encoded_len = encoded.len();
    let body = Body::from_stream(tokio_stream::iter(
        encoded
            .into_segments()
            .into_iter()
            .map(Ok::<Bytes, Infallible>),
    ));
    let mut response = Response::new(body);
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static(NATIVE_BATCH_CONTENT_TYPE),
    );
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&encoded_len.to_string())
            .map_err(|error| AppError::internal(error.to_string()))?,
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shard-stream-batch-count"),
        HeaderValue::from_str(&batches.len().to_string())
            .map_err(|error| AppError::internal(error.to_string()))?,
    );
    if let Some(next_cursor) = next_cursor {
        response.headers_mut().insert(
            HeaderName::from_static("x-shard-stream-next-cursor"),
            HeaderValue::from_str(&next_cursor)
                .map_err(|error| AppError::internal(error.to_string()))?,
        );
    }
    Ok(response)
}

#[derive(Debug, Serialize)]
struct WatermarksResponse {
    log_start: String,
    allocated_end: String,
    contiguous_log_end: String,
    replicated_high_watermark: String,
    last_stable_offset: String,
    replication_factor: u32,
    min_in_sync_replicas: u32,
}

async fn watermarks(
    State(state): State<AppState>,
    Path((topic_id, partition_id)): Path<(String, u32)>,
) -> Result<Json<WatermarksResponse>, AppError> {
    request(&state);
    let partition = TopicPartition::new(
        parse_topic_id(&topic_id)?,
        LogicalPartitionId::new(partition_id),
    );
    let watermarks = execute({
        let engine = Arc::clone(&state.engine);
        move || engine.watermarks(partition)
    })
    .await
    .inspect_err(|_| error(&state))?;
    Ok(Json(watermarks_response(watermarks)))
}

#[derive(Debug, Deserialize)]
struct RetainedBytesQuery {
    start_offset: String,
}

async fn retained_bytes(
    State(state): State<AppState>,
    Path((topic_id, partition_id)): Path<(String, u32)>,
    Query(query): Query<RetainedBytesQuery>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AppError> {
    request(&state);
    require_admin(&state, &headers)?;
    let partition = TopicPartition::new(
        parse_topic_id(&topic_id)?,
        LogicalPartitionId::new(partition_id),
    );
    let start = LogicalOffset::new(parse_u64("start_offset", &query.start_offset)?);
    let bytes = execute({
        let engine = Arc::clone(&state.engine);
        move || engine.retained_payload_bytes(partition, start)
    })
    .await
    .inspect_err(|_| error(&state))?;
    Ok(Json(
        serde_json::json!({ "payload_bytes": bytes.to_string() }),
    ))
}

#[derive(Debug, Deserialize)]
struct RetentionBody {
    log_start: String,
}

async fn update_retention(
    State(state): State<AppState>,
    Path((topic_id, partition_id)): Path<(String, u32)>,
    headers: HeaderMap,
    Json(body): Json<RetentionBody>,
) -> Result<Json<WatermarksResponse>, AppError> {
    request(&state);
    require_admin(&state, &headers)?;
    let partition = TopicPartition::new(
        parse_topic_id(&topic_id)?,
        LogicalPartitionId::new(partition_id),
    );
    let start = LogicalOffset::new(parse_u64("log_start", &body.log_start)?);
    let watermarks = execute({
        let engine = Arc::clone(&state.engine);
        move || engine.truncate_partition(partition, start)
    })
    .await
    .inspect_err(|_| error(&state))?;
    Ok(Json(watermarks_response(watermarks)))
}

fn watermarks_response(watermarks: shard_stream_engine::PartitionWatermarks) -> WatermarksResponse {
    WatermarksResponse {
        log_start: watermarks.log_start.to_string(),
        allocated_end: watermarks.allocated_end.to_string(),
        contiguous_log_end: watermarks.contiguous_log_end.to_string(),
        replicated_high_watermark: watermarks.replicated_high_watermark.to_string(),
        last_stable_offset: watermarks.last_stable_offset.to_string(),
        replication_factor: 1,
        min_in_sync_replicas: 1,
    }
}

fn request(state: &AppState) {
    state.metrics.requests.fetch_add(1, Ordering::Relaxed);
}

fn error(state: &AppState) {
    state.metrics.errors.fetch_add(1, Ordering::Relaxed);
}

fn require_admin(state: &AppState, headers: &HeaderMap) -> Result<(), AppError> {
    let Some(expected) = &state.admin_token else {
        return Ok(());
    };
    let supplied = headers
        .get(ADMIN_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if constant_time_equal(expected, supplied) {
        Ok(())
    } else {
        Err(AppError::forbidden("invalid administrative credential"))
    }
}

fn constant_time_equal(expected: &str, supplied: &str) -> bool {
    if expected.len() != supplied.len() {
        return false;
    }
    expected
        .bytes()
        .zip(supplied.bytes())
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

async fn execute<T, F>(operation: F) -> Result<T, AppError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, EngineError> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| AppError::internal(format!("engine task failed: {error}")))?
        .map_err(Into::into)
}

fn parse_topic_id(value: &str) -> Result<TopicId, AppError> {
    Ok(TopicId::new(parse_u128("topic_id", value)?))
}

fn parse_u128(field: &str, value: &str) -> Result<u128, AppError> {
    u128::from_str(value)
        .map_err(|_| AppError::bad_request(format!("{field} must be an unsigned 128-bit integer")))
}

fn parse_u64(field: &str, value: &str) -> Result<u64, AppError> {
    value
        .parse::<u64>()
        .map_err(|_| AppError::bad_request(format!("{field} must be an unsigned u64")))
}

fn parse_durability(value: &str) -> Result<Durability, AppError> {
    match value {
        "leader" | "async" | "async_replicated" | "async-replicated" => Ok(Durability::Leader),
        "quorum" | "sync" | "sync_quorum" | "sync-quorum" => Ok(Durability::Quorum),
        "all_replicas" | "all-replicas" | "all" => Ok(Durability::AllReplicas),
        "object" => Ok(Durability::Object),
        _ => Err(AppError::bad_request(
            "durability must be leader, quorum, all_replicas, or object",
        )),
    }
}

fn parse_fetch_mode(value: &str) -> Result<FetchMode, EngineError> {
    match value {
        "ordered" => Ok(FetchMode::Ordered),
        "replicated" => Ok(FetchMode::Replicated),
        "as_available" => Ok(FetchMode::AsAvailable),
        _ => Err(EngineError::InvalidConfig(
            "mode must be ordered, replicated, or as_available".into(),
        )),
    }
}

fn parse_producer(
    producer_id: Option<String>,
    epoch: Option<u32>,
    first_sequence: Option<u64>,
    event_id: Option<String>,
) -> Result<Option<ProducerIdentity>, AppError> {
    match (producer_id, epoch, first_sequence, event_id) {
        (None, None, None, None) => Ok(None),
        (Some(producer_id), Some(epoch), Some(first_sequence), event_id) => {
            Ok(Some(ProducerIdentity {
                producer_id: parse_u128("producer_id", &producer_id)?,
                epoch,
                first_sequence,
                event_id: event_id
                    .map(|value| parse_u128("event_id", &value).map(RecordId::new))
                    .transpose()?,
            }))
        }
        _ => Err(AppError::bad_request(
            "producer_id, producer_epoch, and first_sequence must be supplied together",
        )),
    }
}

const fn default_record_count() -> u32 {
    1
}

fn default_durability() -> String {
    "leader".into()
}

fn default_offset() -> String {
    "0".into()
}

fn default_fetch_mode() -> String {
    "ordered".into()
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if !*shutdown.borrow() {
        let _ = shutdown.changed().await;
    }
}

fn server_task_error(
    result: Result<Result<(), AppError>, tokio::task::JoinError>,
) -> Option<AppError> {
    match result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error),
        Err(error) => Some(AppError::internal(format!("server task failed: {error}"))),
    }
}

#[derive(Debug, Serialize)]
struct ProblemBody {
    code: &'static str,
    message: String,
    retryable: bool,
}

#[derive(Debug)]
struct AppError {
    status: StatusCode,
    problem: ProblemBody,
}

impl AppError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "INVALID_REQUEST", message, false)
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            message,
            false,
        )
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "ADMIN_AUTHENTICATION_FAILED",
            message,
            false,
        )
    }

    fn private_data(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::PRECONDITION_FAILED,
            "SHARD_STREAM_PRIVATE_REQUIRED",
            message,
            false,
        )
    }

    fn stale_generation(expected: u64) -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "STALE_DATA_GENERATION",
            format!("expected data generation {expected}; current generation is 1"),
            true,
        )
    }

    fn new(
        status: StatusCode,
        code: &'static str,
        message: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self {
            status,
            problem: ProblemBody {
                code,
                message: message.into(),
                retryable,
            },
        }
    }
}

impl From<WireError> for AppError {
    fn from(error: WireError) -> Self {
        Self::internal(format!("encode native response: {error}"))
    }
}

impl From<EngineError> for AppError {
    fn from(error: EngineError) -> Self {
        let message = error.to_string();
        match error {
            EngineError::InvalidConfig(_) => Self::bad_request(message),
            EngineError::UnknownPartition { .. } => {
                Self::new(StatusCode::NOT_FOUND, "NOT_FOUND", message, false)
            }
            EngineError::TopicAlreadyExists(_) => {
                Self::new(StatusCode::CONFLICT, "TOPIC_EXISTS", message, false)
            }
            EngineError::AdmissionLimited { .. }
            | EngineError::WorkerStopped(_)
            | EngineError::DurabilityUnavailable { .. }
            | EngineError::ObjectDurabilityUnavailable
            | EngineError::DurableSinkUnavailable(_)
            | EngineError::ReplicaInFlight(_) => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "DURABILITY_UNAVAILABLE",
                message,
                true,
            ),
            EngineError::OffsetOutOfRange { .. } => Self::new(
                StatusCode::RANGE_NOT_SATISFIABLE,
                "OFFSET_OUT_OF_RANGE",
                message,
                false,
            ),
            EngineError::DurableSinkCheckpoint(_)
            | EngineError::CorruptState(_)
            | EngineError::Storage(_) => Self::internal(message),
            _ => Self::new(StatusCode::CONFLICT, "CONFLICT", message, false),
        }
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.problem.message)
    }
}

impl std::error::Error for AppError {}

impl IntoResponse for AppError {
    fn into_response(self) -> Response<Body> {
        (self.status, Json(self.problem)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use clap::CommandFactory;
    use shard_stream_engine::{EngineResult, ReplicationProgress, ReplicationTransport};
    use shard_stream_protocol::ReplicaAppend;

    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "shard-stream-public-server-{label}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("temporary directory");
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Debug)]
    struct NoopReplication;

    impl ReplicationTransport for NoopReplication {
        fn install_progress(&self, _progress: ReplicationProgress) -> EngineResult<()> {
            Ok(())
        }

        fn replicate(&self, _batch: ReplicaAppend, _required_followers: u32) -> EngineResult<u32> {
            Ok(0)
        }
    }

    fn engine_config(data_dir: PathBuf, replication_factor: u32) -> EngineConfig {
        EngineConfig {
            data_dir,
            object_store_dir: None,
            shard_count: 1,
            virtual_lane_count: 1,
            replication_factor,
            min_in_sync_replicas: replication_factor,
            queue_slots_per_shard: 16,
            queue_bytes_per_shard: 2 * 1024 * 1024,
            target_pack_bytes: 1024 * 1024,
            max_pack_age: Duration::from_secs(1),
            max_batch_bytes: 1024 * 1024,
            max_fetch_bytes: 1024 * 1024,
            append_linger: Duration::ZERO,
        }
    }

    #[test]
    fn every_native_durability_spelling_is_an_rf1_alias() {
        for spelling in [
            "leader",
            "async",
            "async_replicated",
            "quorum",
            "sync",
            "sync_quorum",
            "all_replicas",
            "all-replicas",
            "all",
        ] {
            assert!(parse_durability(spelling).is_ok(), "{spelling:?}");
        }
    }

    #[test]
    fn public_cli_contains_no_ha_or_multi_node_flags() {
        let command = Args::command();
        let argument_ids = command
            .get_arguments()
            .map(|argument| argument.get_id().as_str())
            .collect::<Vec<_>>();
        for forbidden in [
            "node_id",
            "cluster_nodes",
            "cluster_token",
            "replication_listen",
            "replication_factor",
            "min_in_sync_replicas",
            "ha_config",
            "control_listen",
        ] {
            assert!(!argument_ids.contains(&forbidden), "{forbidden}");
        }
        for kafka_argument in [
            "kafka_listen",
            "kafka_advertised_host",
            "kafka_advertised_port",
            "kafka_profile_sample_stride",
        ] {
            assert_eq!(
                argument_ids.contains(&kafka_argument),
                cfg!(feature = "kafka"),
                "{kafka_argument}"
            );
        }
    }

    #[test]
    fn openapi_matches_the_compiled_kafka_surface() {
        let document = openapi_document();
        assert_eq!(
            document
                .pointer("/paths/~1v1~1diagnostics~1kafka-profile")
                .is_some(),
            cfg!(feature = "kafka")
        );
    }

    #[test]
    fn private_markers_fail_closed_with_the_upgrade_code() {
        let directory = TempDir::new("private-marker");
        fs::create_dir_all(directory.0.join("ha-metadata")).expect("marker");
        let error = reject_private_data(&directory.0).expect_err("private data");
        assert_eq!(error.problem.code, "SHARD_STREAM_PRIVATE_REQUIRED");
    }

    #[test]
    fn rf1_data_reopens_but_rf2_policy_requires_private_server() {
        let rf1 = TempDir::new("rf1");
        {
            let engine = open_public_engine(engine_config(rf1.0.clone(), 1)).expect("RF1 open");
            engine
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(1),
                    partitions: 1,
                    shards: None,
                })
                .expect("RF1 topic");
        }
        let reopened = open_public_engine(engine_config(rf1.0.clone(), 1)).expect("RF1 reopen");
        validate_rf1_state(&reopened).expect("RF1 state");

        let rf2 = TempDir::new("rf2");
        {
            let engine = StreamEngine::open_with_replication(
                engine_config(rf2.0.clone(), 2),
                Arc::new(NoopReplication),
            )
            .expect("extended RF2 open");
            engine
                .create_topic(TopicConfig {
                    topic_id: TopicId::new(2),
                    partitions: 1,
                    shards: None,
                })
                .expect("RF2 topic");
        }
        let error = match open_public_engine(engine_config(rf2.0.clone(), 1)) {
            Ok(_) => panic!("RF2 data opened in the public server"),
            Err(error) => error,
        };
        assert_eq!(error.problem.code, "SHARD_STREAM_PRIVATE_REQUIRED");
    }
}
