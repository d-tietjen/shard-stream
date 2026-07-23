use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::header::{CONTENT_TYPE, HeaderName};
use axum::http::{HeaderValue, Response, StatusCode};
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post, put};
use clap::Parser;
use eden_logger::{LogAudience, LogContext, WriterConfig, log_error, log_info};
use fast_telemetry::{Counter, PrometheusExport};
use serde::{Deserialize, Serialize};
use shard_stream_core::{
    LogicalOffset, LogicalPartitionId, RingEpoch, ShardId, TopicId, TopicPartition,
};
use shard_stream_engine::{EngineConfig, EngineError, StreamEngine, TopicConfig};
use shard_stream_protocol::{
    AppendRequest, Durability, FetchMode, FetchRequest, NativeFetchBatch, ProducerIdentity,
    WireError, encode_fetch_batches,
};
use tokio::net::TcpListener;
use tokio::sync::watch;

const OPENAPI: &str = include_str!("../../../openapi/shard-stream-v1.json");
const NATIVE_BATCH_CONTENT_TYPE: &str = "application/vnd.shard-stream.batch.v1";

#[derive(Debug, Parser)]
#[command(name = "shard-stream-server", version, about)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:7420")]
    listen: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:7421")]
    grpc_listen: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:9092")]
    kafka_listen: SocketAddr,
    #[arg(long, default_value = "127.0.0.1")]
    kafka_advertised_host: String,
    #[arg(long, default_value_t = 9092)]
    kafka_advertised_port: u16,
    #[arg(long, default_value_t = 0)]
    kafka_node_id: i32,
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
    #[arg(long, default_value_t = 1)]
    replication_factor: u32,
    #[arg(long, default_value_t = 1)]
    min_in_sync_replicas: u32,
    #[arg(long, default_value_t = 4096)]
    queue_slots_per_shard: usize,
    #[arg(long, default_value_t = 512 * 1024 * 1024)]
    queue_bytes_per_shard: usize,
    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    target_pack_bytes: u64,
    #[arg(
        long,
        env = "SHARD_STREAM_MAX_BATCH_BYTES",
        default_value_t = 16 * 1024 * 1024
    )]
    max_batch_bytes: usize,
    #[arg(
        long,
        env = "SHARD_STREAM_MAX_REQUEST_BYTES",
        default_value_t = 32 * 1024 * 1024
    )]
    max_request_bytes: usize,
    #[arg(long, env = "SHARD_STREAM_APPEND_LINGER_MS", default_value_t = 1)]
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
    metrics: Arc<HttpMetrics>,
    next_request_id: Arc<AtomicU64>,
    max_fetch_bytes: usize,
}

#[derive(Debug)]
struct HttpMetrics {
    requests: Counter,
    errors: Counter,
    appended_batches: Counter,
    appended_bytes: Counter,
    fetched_batches: Counter,
    fetched_bytes: Counter,
}

impl Default for HttpMetrics {
    fn default() -> Self {
        Self {
            requests: Counter::new(64),
            errors: Counter::new(64),
            appended_batches: Counter::new(64),
            appended_bytes: Counter::new(64),
            fetched_batches: Counter::new(64),
            fetched_bytes: Counter::new(64),
        }
    }
}

impl HttpMetrics {
    fn request(&self) {
        self.requests.inc();
    }

    fn error(&self) {
        self.errors.inc();
    }

    fn appended(&self, bytes: u64) {
        self.appended_batches.inc();
        self.appended_bytes.add(counter_value(bytes));
    }

    fn fetched(&self, batches: usize, bytes: u64) {
        self.fetched_batches.add(counter_value(batches as u64));
        self.fetched_bytes.add(counter_value(bytes));
    }
}

#[tokio::main]
async fn main() {
    eden_logger::init(WriterConfig::default());
    eden_logger::init_from_env();
    let args = Args::parse();
    if let Err(error) = run(args).await {
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
    let config = EngineConfig {
        data_dir: args.data_dir,
        object_store_dir: args.object_store_dir,
        shard_count: args.shards,
        replication_factor: args.replication_factor,
        min_in_sync_replicas: args.min_in_sync_replicas,
        queue_slots_per_shard: args.queue_slots_per_shard,
        queue_bytes_per_shard: args.queue_bytes_per_shard,
        target_pack_bytes: args.target_pack_bytes,
        max_batch_bytes: args.max_batch_bytes,
        max_fetch_bytes: args.max_fetch_bytes,
        append_linger: Duration::from_millis(args.append_linger_ms),
    };
    let engine = Arc::new(StreamEngine::open(config).map_err(AppError::from)?);
    let state = AppState {
        engine,
        metrics: Arc::new(HttpMetrics::default()),
        next_request_id: Arc::new(AtomicU64::new(1)),
        max_fetch_bytes: args.max_fetch_bytes,
    };
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(health))
        .route("/metrics", get(metrics))
        .route("/openapi.json", get(openapi))
        .route("/v1/topics", post(create_topic))
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
            "/v1/topics/{topic_id}/partitions/{partition_id}/retention",
            put(update_retention),
        )
        .layer(DefaultBodyLimit::max(args.max_request_bytes))
        .with_state(state.clone());
    let listener = TcpListener::bind(args.listen)
        .await
        .map_err(|error| AppError::internal(format!("failed to bind {}: {error}", args.listen)))?;
    let grpc_listener = TcpListener::bind(args.grpc_listen).await.map_err(|error| {
        AppError::internal(format!(
            "failed to bind gRPC listener {}: {error}",
            args.grpc_listen
        ))
    })?;
    let kafka_listener = TcpListener::bind(args.kafka_listen)
        .await
        .map_err(|error| {
            AppError::internal(format!(
                "failed to bind Kafka listener {}: {error}",
                args.kafka_listen
            ))
        })?;
    let log_context = LogContext::<()>::new().with_feature("server");
    log_info!(
        log_context.clone(),
        "shard-stream REST listener ready",
        audience = LogAudience::Internal,
        address = args.listen
    );
    log_info!(
        log_context.clone(),
        "shard-stream gRPC listener ready",
        audience = LogAudience::Internal,
        address = args.grpc_listen
    );
    log_info!(
        log_context,
        "shard-stream Kafka listener ready",
        audience = LogAudience::Internal,
        address = args.kafka_listen
    );

    let grpc_engine = Arc::clone(&state.engine);
    let grpc_max_response_bytes = args.max_fetch_bytes.saturating_add(1024 * 1024);
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let rest_shutdown = shutdown_receiver.clone();
    let h3_shutdown = shutdown_receiver.clone();
    let kafka_shutdown = shutdown_receiver.clone();
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(wait_for_shutdown(rest_shutdown))
            .await
            .map_err(|error| AppError::internal(format!("HTTP server failed: {error}")))
    });
    tasks.spawn(async move {
        shard_stream_grpc::serve(
            grpc_listener,
            grpc_engine,
            128,
            args.max_request_bytes,
            grpc_max_response_bytes,
            shutdown_receiver,
        )
        .await
        .map_err(|error| AppError::internal(format!("gRPC server failed: {error}")))
    });
    let kafka_engine = Arc::clone(&state.engine);
    let kafka_config = shard_stream_kafka::KafkaConfig {
        advertised_host: args.kafka_advertised_host,
        advertised_port: args.kafka_advertised_port,
        node_id: args.kafka_node_id,
        max_frame_bytes: args.max_request_bytes,
        max_fetch_bytes: args.max_fetch_bytes,
    };
    tasks.spawn(async move {
        shard_stream_kafka::serve(kafka_listener, kafka_engine, kafka_config, kafka_shutdown)
            .await
            .map_err(|error| AppError::internal(format!("Kafka server failed: {error}")))
    });
    let h3_engine = Arc::clone(&state.engine);
    if let Some(config) = h3_config {
        tasks.spawn(async move {
            shard_stream_h3::serve(config, h3_engine, h3_shutdown)
                .await
                .map_err(|error| AppError::internal(format!("HTTP/3 server failed: {error}")))
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
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn validate_args(args: &Args) -> Result<(), AppError> {
    if args.max_request_bytes < args.max_batch_bytes {
        return Err(AppError::bad_request(
            "max_request_bytes must be at least max_batch_bytes",
        ));
    }
    if args.max_request_bytes > 128 * 1024 * 1024 {
        return Err(AppError::bad_request(
            "max_request_bytes cannot exceed the Kafka adapter's 128 MiB hard limit",
        ));
    }
    if args.append_linger_ms > 60_000 {
        return Err(AppError::bad_request(
            "append_linger_ms cannot exceed 60000",
        ));
    }
    Ok(())
}

async fn health() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(CONTENT_TYPE, "application/json")],
        r#"{"status":"ok"}"#,
    )
}

async fn openapi() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(CONTENT_TYPE, "application/json")],
        OPENAPI,
    )
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let metrics = &state.metrics;
    let mut body = String::new();
    metrics.requests.export_prometheus(
        &mut body,
        "shard_stream_http_requests_total",
        "HTTP requests received",
    );
    metrics.errors.export_prometheus(
        &mut body,
        "shard_stream_http_errors_total",
        "HTTP requests returning errors",
    );
    metrics.appended_batches.export_prometheus(
        &mut body,
        "shard_stream_appended_batches_total",
        "Successfully appended batches",
    );
    metrics.appended_bytes.export_prometheus(
        &mut body,
        "shard_stream_appended_bytes_total",
        "Successfully appended payload bytes",
    );
    metrics.fetched_batches.export_prometheus(
        &mut body,
        "shard_stream_fetched_batches_total",
        "Fetched batches",
    );
    metrics.fetched_bytes.export_prometheus(
        &mut body,
        "shard_stream_fetched_bytes_total",
        "Fetched payload bytes",
    );
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
}

async fn create_topic(
    State(state): State<AppState>,
    Json(body): Json<CreateTopicBody>,
) -> Result<impl IntoResponse, AppError> {
    state.metrics.request();
    let topic_id = parse_topic_id(&body.topic_id)?;
    let config = TopicConfig {
        topic_id,
        partitions: body.partitions,
        shards: body
            .shards
            .map(|shards| shards.into_iter().map(ShardId::new).collect()),
    };
    let engine = Arc::clone(&state.engine);
    execute(move || engine.create_topic(config))
        .await
        .inspect_err(|_| state.metrics.error())?;
    Ok((
        StatusCode::CREATED,
        Json(TopicResponse {
            topic_id: topic_id.to_string(),
            partitions: body.partitions,
        }),
    ))
}

#[derive(Debug, Deserialize)]
struct RingBody {
    ring_epoch: u64,
    shards: Vec<u32>,
}

async fn reconfigure_ring(
    State(state): State<AppState>,
    Path((topic_id, partition_id)): Path<(String, u32)>,
    Json(body): Json<RingBody>,
) -> Result<StatusCode, AppError> {
    state.metrics.request();
    let topic_partition = TopicPartition::new(
        parse_topic_id(&topic_id)?,
        LogicalPartitionId::new(partition_id),
    );
    let engine = Arc::clone(&state.engine);
    execute(move || {
        engine.reconfigure_partition(
            topic_partition,
            RingEpoch::new(body.ring_epoch),
            body.shards.into_iter().map(ShardId::new).collect(),
        )
    })
    .await
    .inspect_err(|_| state.metrics.error())?;
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
    leader_epoch: Option<u64>,
}

#[derive(Debug, Serialize)]
struct AppendResponseBody {
    request_id: String,
    batch_id: String,
    first_offset: String,
    last_offset: String,
    shard_id: u32,
    ring_epoch: u64,
    placement_sequence: String,
}

async fn append(
    State(state): State<AppState>,
    Path((topic_id, partition_id)): Path<(String, u32)>,
    Query(query): Query<AppendQuery>,
    payload: Bytes,
) -> Result<Json<AppendResponseBody>, AppError> {
    state.metrics.request();
    let request_id = match query.request_id {
        Some(request_id) => parse_u128("request_id", &request_id)?,
        None => u128::from(state.next_request_id.fetch_add(1, Ordering::Relaxed)),
    };
    let durability = parse_durability(&query.durability)?;
    let producer = parse_producer(
        query.producer_id,
        query.producer_epoch,
        query.first_sequence,
    )?;
    let payload_len = payload.len() as u64;
    let request = AppendRequest {
        request_id,
        topic_id: parse_topic_id(&topic_id)?,
        partition_id: LogicalPartitionId::new(partition_id),
        record_count: query.record_count,
        payload,
        durability,
        producer,
        leader_epoch: query.leader_epoch,
    };
    let engine = Arc::clone(&state.engine);
    let response = execute(move || engine.append(request))
        .await
        .inspect_err(|_| state.metrics.error())?;
    state.metrics.appended(payload_len);
    Ok(Json(AppendResponseBody {
        request_id: response.request_id.to_string(),
        batch_id: response.batch_id.to_string(),
        first_offset: response.first_offset.to_string(),
        last_offset: response.last_offset.to_string(),
        shard_id: response.placement.shard_id.get(),
        ring_epoch: response.placement.ring_epoch.get(),
        placement_sequence: response.placement.sequence.to_string(),
    }))
}

#[derive(Debug, Deserialize)]
struct FetchQuery {
    #[serde(default = "default_offset")]
    offset: String,
    max_bytes: Option<u32>,
    #[serde(default = "default_fetch_mode")]
    mode: String,
    request_id: Option<String>,
}

async fn fetch(
    State(state): State<AppState>,
    Path((topic_id, partition_id)): Path<(String, u32)>,
    Query(query): Query<FetchQuery>,
) -> Result<Response<Body>, AppError> {
    state.metrics.request();
    let request_id = match query.request_id {
        Some(request_id) => parse_u128("request_id", &request_id)?,
        None => u128::from(state.next_request_id.fetch_add(1, Ordering::Relaxed)),
    };
    let configured_max = u32::try_from(state.max_fetch_bytes).unwrap_or(u32::MAX);
    let max_bytes = query.max_bytes.unwrap_or(configured_max);
    let request = FetchRequest {
        request_id,
        topic_id: parse_topic_id(&topic_id)?,
        partition_id: LogicalPartitionId::new(partition_id),
        start_offset: LogicalOffset::new(parse_u128("offset", &query.offset)?),
        max_bytes,
        mode: parse_fetch_mode(&query.mode)?,
    };
    let engine = Arc::clone(&state.engine);
    let batches = execute(move || engine.fetch(request))
        .await
        .inspect_err(|_| state.metrics.error())?;
    let payload_bytes = batches
        .iter()
        .map(|batch| batch.payload.len() as u64)
        .sum::<u64>();
    let native = batches
        .into_iter()
        .map(|batch| NativeFetchBatch {
            request_id,
            batch_id: batch.batch_id,
            first_offset: batch.first_offset,
            last_offset: batch.last_offset,
            record_count: batch.record_count,
            placement: batch.placement,
            payload: batch.payload,
        })
        .collect::<Vec<_>>();
    let max_output = state
        .max_fetch_bytes
        .saturating_add(native.len().saturating_mul(256));
    let encoded = encode_fetch_batches(&native, max_output).map_err(AppError::from)?;
    state.metrics.fetched(native.len(), payload_bytes);

    let mut response = Response::new(Body::from(encoded));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static(NATIVE_BATCH_CONTENT_TYPE),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shard-stream-batch-count"),
        HeaderValue::from_str(&native.len().to_string())
            .map_err(|error| AppError::internal(format!("invalid count header: {error}")))?,
    );
    Ok(response)
}

#[derive(Debug, Serialize)]
struct WatermarksResponse {
    log_start: String,
    allocated_end: String,
    contiguous_log_end: String,
    replicated_high_watermark: String,
    last_stable_offset: String,
}

async fn watermarks(
    State(state): State<AppState>,
    Path((topic_id, partition_id)): Path<(String, u32)>,
) -> Result<Json<WatermarksResponse>, AppError> {
    state.metrics.request();
    let topic_partition = TopicPartition::new(
        parse_topic_id(&topic_id)?,
        LogicalPartitionId::new(partition_id),
    );
    let engine = Arc::clone(&state.engine);
    let watermarks = execute(move || engine.watermarks(topic_partition))
        .await
        .inspect_err(|_| state.metrics.error())?;
    Ok(Json(watermarks_response(watermarks)))
}

#[derive(Debug, Deserialize)]
struct RetentionBody {
    log_start: String,
}

async fn update_retention(
    State(state): State<AppState>,
    Path((topic_id, partition_id)): Path<(String, u32)>,
    Json(body): Json<RetentionBody>,
) -> Result<Json<WatermarksResponse>, AppError> {
    state.metrics.request();
    let topic_partition = TopicPartition::new(
        parse_topic_id(&topic_id)?,
        LogicalPartitionId::new(partition_id),
    );
    let log_start = LogicalOffset::new(parse_u128("log_start", &body.log_start)?);
    let engine = Arc::clone(&state.engine);
    let watermarks = execute(move || engine.truncate_partition(topic_partition, log_start))
        .await
        .inspect_err(|_| state.metrics.error())?;
    Ok(Json(watermarks_response(watermarks)))
}

fn watermarks_response(watermarks: shard_stream_engine::PartitionWatermarks) -> WatermarksResponse {
    WatermarksResponse {
        log_start: watermarks.log_start.to_string(),
        allocated_end: watermarks.allocated_end.to_string(),
        contiguous_log_end: watermarks.contiguous_log_end.to_string(),
        replicated_high_watermark: watermarks.replicated_high_watermark.to_string(),
        last_stable_offset: watermarks.last_stable_offset.to_string(),
    }
}

async fn execute<T, F>(operation: F) -> Result<T, AppError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, EngineError> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| AppError::internal(format!("engine task failed: {error}")))?
        .map_err(AppError::from)
}

fn parse_topic_id(value: &str) -> Result<TopicId, AppError> {
    parse_u128("topic_id", value).map(TopicId::new)
}

fn parse_u128(field: &str, value: &str) -> Result<u128, AppError> {
    u128::from_str(value)
        .map_err(|_| AppError::bad_request(format!("{field} must be an unsigned 128-bit integer")))
}

fn parse_durability(value: &str) -> Result<Durability, AppError> {
    match value {
        "leader" => Ok(Durability::Leader),
        "quorum" => Ok(Durability::Quorum),
        "object" => Ok(Durability::Object),
        _ => Err(AppError::bad_request(
            "durability must be leader, quorum, or object",
        )),
    }
}

fn parse_fetch_mode(value: &str) -> Result<FetchMode, AppError> {
    match value {
        "ordered" => Ok(FetchMode::Ordered),
        "as_available" => Ok(FetchMode::AsAvailable),
        _ => Err(AppError::bad_request(
            "mode must be ordered or as_available",
        )),
    }
}

fn parse_producer(
    producer_id: Option<String>,
    epoch: Option<u32>,
    first_sequence: Option<u64>,
) -> Result<Option<ProducerIdentity>, AppError> {
    match (producer_id, epoch, first_sequence) {
        (None, None, None) => Ok(None),
        (Some(producer_id), Some(epoch), Some(first_sequence)) => Ok(Some(ProducerIdentity {
            producer_id: parse_u128("producer_id", &producer_id)?,
            epoch,
            first_sequence,
        })),
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

fn counter_value(value: u64) -> isize {
    isize::try_from(value).unwrap_or(isize::MAX)
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    log_info!(
        LogContext::<()>::new().with_feature("server"),
        "shutdown signal received",
        audience = LogAudience::Internal
    );
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
    retry_after_seconds: Option<u64>,
}

impl AppError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            problem: ProblemBody {
                code: "INVALID_REQUEST",
                message: message.into(),
                retryable: false,
            },
            retry_after_seconds: None,
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            problem: ProblemBody {
                code: "INTERNAL",
                message: message.into(),
                retryable: false,
            },
            retry_after_seconds: None,
        }
    }
}

impl From<WireError> for AppError {
    fn from(error: WireError) -> Self {
        Self::internal(format!("failed to encode native response: {error}"))
    }
}

impl From<EngineError> for AppError {
    fn from(error: EngineError) -> Self {
        let message = error.to_string();
        match error {
            EngineError::InvalidConfig(_) => Self::bad_request(message),
            EngineError::TopicAlreadyExists(_) => Self {
                status: StatusCode::CONFLICT,
                problem: ProblemBody {
                    code: "TOPIC_EXISTS",
                    message,
                    retryable: false,
                },
                retry_after_seconds: None,
            },
            EngineError::UnknownPartition { .. } | EngineError::UnknownShard(_) => Self {
                status: StatusCode::NOT_FOUND,
                problem: ProblemBody {
                    code: "NOT_FOUND",
                    message,
                    retryable: false,
                },
                retry_after_seconds: None,
            },
            EngineError::AdmissionLimited { .. } => Self {
                status: StatusCode::TOO_MANY_REQUESTS,
                problem: ProblemBody {
                    code: "ADMISSION_LIMITED",
                    message,
                    retryable: true,
                },
                retry_after_seconds: Some(1),
            },
            EngineError::WorkerStopped(_) => Self {
                status: StatusCode::SERVICE_UNAVAILABLE,
                problem: ProblemBody {
                    code: "WORKER_UNAVAILABLE",
                    message,
                    retryable: true,
                },
                retry_after_seconds: Some(1),
            },
            EngineError::DurabilityUnavailable { .. }
            | EngineError::ObjectDurabilityUnavailable => Self {
                status: StatusCode::SERVICE_UNAVAILABLE,
                problem: ProblemBody {
                    code: "DURABILITY_UNAVAILABLE",
                    message,
                    retryable: true,
                },
                retry_after_seconds: Some(1),
            },
            EngineError::UnsupportedDurability(_) => Self {
                status: StatusCode::NOT_IMPLEMENTED,
                problem: ProblemBody {
                    code: "DURABILITY_UNAVAILABLE",
                    message,
                    retryable: false,
                },
                retry_after_seconds: None,
            },
            EngineError::Fenced(_) => Self {
                status: StatusCode::CONFLICT,
                problem: ProblemBody {
                    code: "FENCED",
                    message,
                    retryable: false,
                },
                retry_after_seconds: None,
            },
            EngineError::OffsetOutOfRange { .. } => Self {
                status: StatusCode::RANGE_NOT_SATISFIABLE,
                problem: ProblemBody {
                    code: "OFFSET_OUT_OF_RANGE",
                    message,
                    retryable: false,
                },
                retry_after_seconds: None,
            },
            EngineError::StaleProducerEpoch { .. }
            | EngineError::ProducerSequenceOutOfOrder { .. }
            | EngineError::ProducerRequiresNewEpoch(_)
            | EngineError::DuplicateSequenceMismatch(_)
            | EngineError::Sequencer(_) => Self {
                status: StatusCode::CONFLICT,
                problem: ProblemBody {
                    code: "PRODUCER_CONFLICT",
                    message,
                    retryable: false,
                },
                retry_after_seconds: None,
            },
            EngineError::CorruptState(_) | EngineError::Storage(_) => Self::internal(message),
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
    fn into_response(self) -> axum::response::Response {
        let mut response = (self.status, Json(self.problem)).into_response();
        if let Some(seconds) = self.retry_after_seconds
            && let Ok(value) = HeaderValue::from_str(&seconds.to_string())
        {
            response.headers_mut().insert("retry-after", value);
        }
        response
    }
}
