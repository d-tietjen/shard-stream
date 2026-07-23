use std::collections::HashMap;
use std::fmt;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Buf, Bytes};
use h3::server::RequestResolver;
use h3_quinn::quinn::{self, crypto::rustls::QuicServerConfig};
use http::{Method, Request, Response, StatusCode};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::Serialize;
use shard_stream_core::{LogicalOffset, LogicalPartitionId, TopicId};
use shard_stream_engine::{EngineError, StreamEngine};
use shard_stream_protocol::{
    AppendRequest, Durability, FetchMode, FetchRequest, NativeFetchBatch, ProducerIdentity,
    encode_fetch_batches,
};
use tokio::sync::watch;
use tracing::{error, info};

const ALPN: &[u8] = b"h3";
const NATIVE_BATCH_CONTENT_TYPE: &str = "application/vnd.shard-stream.batch.v1";
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct H3Config {
    pub listen: SocketAddr,
    pub certificate_path: PathBuf,
    pub private_key_path: PathBuf,
    pub max_batch_bytes: usize,
    pub max_fetch_bytes: usize,
}

#[derive(Debug)]
pub struct H3Error(String);

impl fmt::Display for H3Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for H3Error {}

pub async fn serve(
    config: H3Config,
    engine: Arc<StreamEngine>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), H3Error> {
    validate_config(&config)?;
    let certificates = load_certificates(&config.certificate_path)?;
    let key = load_private_key(&config.private_key_path)?;
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map_err(display_error("invalid HTTP/3 certificate or key"))?;
    // Appends mutate durable state, so accepting TLS 0-RTT would expose them to
    // transport-level replay. Keep early data disabled for the entire endpoint.
    tls.max_early_data_size = 0;
    tls.alpn_protocols = vec![ALPN.into()];
    let crypto = QuicServerConfig::try_from(tls)
        .map_err(display_error("invalid HTTP/3 QUIC TLS configuration"))?;
    let server_config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    let endpoint = quinn::Endpoint::server(server_config, config.listen)
        .map_err(display_error("failed to bind HTTP/3 endpoint"))?;
    info!(address = %config.listen, "shard-stream HTTP/3 listener ready");

    loop {
        tokio::select! {
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else {
                    break;
                };
                let engine = Arc::clone(&engine);
                let config = config.clone();
                tokio::spawn(async move {
                    match incoming.await {
                        Ok(connection) => {
                            if let Err(error) = handle_connection(connection, engine, config).await {
                                error!(%error, "HTTP/3 connection failed");
                            }
                        }
                        Err(error) => error!(%error, "HTTP/3 handshake failed"),
                    }
                });
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    endpoint.close(quinn::VarInt::from_u32(0), b"shutdown");
                    break;
                }
            }
        }
    }
    endpoint.wait_idle().await;
    Ok(())
}

async fn handle_connection(
    connection: quinn::Connection,
    engine: Arc<StreamEngine>,
    config: H3Config,
) -> Result<(), H3Error> {
    let mut connection = h3::server::Connection::new(h3_quinn::Connection::new(connection))
        .await
        .map_err(display_error("failed to establish HTTP/3 connection"))?;
    loop {
        match connection.accept().await {
            Ok(Some(resolver)) => {
                let engine = Arc::clone(&engine);
                let config = config.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_request(resolver, engine, config).await {
                        error!(%error, "HTTP/3 request failed");
                    }
                });
            }
            Ok(None) => return Ok(()),
            Err(error) => return Err(H3Error(format!("HTTP/3 accept failed: {error}"))),
        }
    }
}

async fn handle_request<C>(
    resolver: RequestResolver<C, Bytes>,
    engine: Arc<StreamEngine>,
    config: H3Config,
) -> Result<(), H3Error>
where
    C: h3::quic::Connection<Bytes>,
    <C as h3::quic::OpenStreams<Bytes>>::BidiStream: h3::quic::BidiStream<Bytes>,
{
    let (request, mut stream) = resolver
        .resolve_request()
        .await
        .map_err(display_error("failed to resolve HTTP/3 request"))?;
    let response = match route(request, &mut stream, engine, &config).await {
        Ok(response) => response,
        Err(error) => error_response(error),
    };
    let mut builder = Response::builder().status(response.status);
    for (name, value) in response.headers {
        builder = builder.header(name, value);
    }
    stream
        .send_response(
            builder
                .body(())
                .map_err(display_error("failed to build HTTP/3 response"))?,
        )
        .await
        .map_err(display_error("failed to send HTTP/3 response headers"))?;
    if !response.body.is_empty() {
        stream
            .send_data(Bytes::from(response.body))
            .await
            .map_err(display_error("failed to send HTTP/3 response body"))?;
    }
    stream
        .finish()
        .await
        .map_err(display_error("failed to finish HTTP/3 response"))
}

async fn route<C>(
    request: Request<()>,
    stream: &mut h3::server::RequestStream<C, Bytes>,
    engine: Arc<StreamEngine>,
    config: &H3Config,
) -> Result<H3Response, RouteError>
where
    C: h3::quic::BidiStream<Bytes>,
{
    if request.method() == Method::GET && request.uri().path() == "/healthz" {
        return Ok(H3Response::json(
            StatusCode::OK,
            br#"{"status":"ok"}"#.to_vec(),
        ));
    }
    let (topic_id, partition_id) = parse_records_path(request.uri().path())?;
    match *request.method() {
        Method::POST => {
            let query = parse_query(request.uri().query())?;
            let body = read_body(stream, config.max_batch_bytes).await?;
            let append = AppendRequest {
                request_id: query
                    .get("request_id")
                    .map(|value| parse_u128("request_id", value))
                    .transpose()?
                    .unwrap_or_else(next_request_id),
                topic_id,
                partition_id,
                record_count: query
                    .get("record_count")
                    .map(|value| parse_u32("record_count", value))
                    .transpose()?
                    .unwrap_or(1),
                payload: body,
                durability: parse_durability(query.get("durability").map(String::as_str))?,
                producer: parse_producer(&query)?,
            };
            let response = tokio::task::spawn_blocking(move || engine.append(append))
                .await
                .map_err(|error| RouteError::internal(format!("engine task failed: {error}")))?
                .map_err(RouteError::from)?;
            let body = serde_json::to_vec(&AppendResponseBody {
                request_id: response.request_id.to_string(),
                batch_id: response.batch_id.to_string(),
                first_offset: response.first_offset.to_string(),
                last_offset: response.last_offset.to_string(),
                shard_id: response.placement.shard_id.get(),
                ring_epoch: response.placement.ring_epoch.get(),
                placement_sequence: response.placement.sequence.to_string(),
            })
            .map_err(|error| RouteError::internal(format!("JSON encoding failed: {error}")))?;
            Ok(H3Response::json(StatusCode::OK, body))
        }
        Method::GET => {
            let query = parse_query(request.uri().query())?;
            let request_id = query
                .get("request_id")
                .map(|value| parse_u128("request_id", value))
                .transpose()?
                .unwrap_or_else(next_request_id);
            let max_bytes = query
                .get("max_bytes")
                .map(|value| parse_u32("max_bytes", value))
                .transpose()?
                .unwrap_or(u32::try_from(config.max_fetch_bytes).unwrap_or(u32::MAX));
            let fetch = FetchRequest {
                request_id,
                topic_id,
                partition_id,
                start_offset: LogicalOffset::new(
                    query
                        .get("offset")
                        .map(|value| parse_u128("offset", value))
                        .transpose()?
                        .unwrap_or(0),
                ),
                max_bytes,
                mode: parse_fetch_mode(query.get("mode").map(String::as_str))?,
            };
            let batches = tokio::task::spawn_blocking(move || engine.fetch(fetch))
                .await
                .map_err(|error| RouteError::internal(format!("engine task failed: {error}")))?
                .map_err(RouteError::from)?;
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
            let max_output = config
                .max_fetch_bytes
                .saturating_add(native.len().saturating_mul(256));
            let body = encode_fetch_batches(&native, max_output)
                .map_err(|error| RouteError::internal(error.to_string()))?;
            Ok(H3Response {
                status: StatusCode::OK,
                headers: vec![
                    ("content-type", NATIVE_BATCH_CONTENT_TYPE.to_string()),
                    ("x-shard-stream-batch-count", native.len().to_string()),
                ],
                body,
            })
        }
        _ => Err(RouteError::new(
            StatusCode::METHOD_NOT_ALLOWED,
            "METHOD_NOT_ALLOWED",
            "only GET and POST are supported",
            false,
        )),
    }
}

async fn read_body<C>(
    stream: &mut h3::server::RequestStream<C, Bytes>,
    max_bytes: usize,
) -> Result<Vec<u8>, RouteError>
where
    C: h3::quic::RecvStream,
{
    let mut body = Vec::new();
    while let Some(mut chunk) = stream
        .recv_data()
        .await
        .map_err(|error| RouteError::bad_request(format!("invalid request body: {error}")))?
    {
        let remaining = chunk.remaining();
        if body
            .len()
            .checked_add(remaining)
            .is_none_or(|length| length > max_bytes)
        {
            return Err(RouteError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "BATCH_TOO_LARGE",
                "request body exceeds the configured batch limit",
                false,
            ));
        }
        body.extend_from_slice(&chunk.copy_to_bytes(remaining));
    }
    Ok(body)
}

fn parse_records_path(path: &str) -> Result<(TopicId, LogicalPartitionId), RouteError> {
    let segments = path.trim_matches('/').split('/').collect::<Vec<_>>();
    if segments.len() != 6
        || segments[0] != "v1"
        || segments[1] != "topics"
        || segments[3] != "partitions"
        || segments[5] != "records"
    {
        return Err(RouteError::new(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "unknown HTTP/3 route",
            false,
        ));
    }
    Ok((
        TopicId::new(parse_u128("topic_id", segments[2])?),
        LogicalPartitionId::new(parse_u32("partition_id", segments[4])?),
    ))
}

fn parse_query(query: Option<&str>) -> Result<HashMap<String, String>, RouteError> {
    query
        .map(serde_urlencoded::from_str)
        .transpose()
        .map(|query| query.unwrap_or_default())
        .map_err(|error| RouteError::bad_request(format!("invalid query string: {error}")))
}

fn parse_u128(field: &str, value: &str) -> Result<u128, RouteError> {
    u128::from_str(value)
        .map_err(|_| RouteError::bad_request(format!("{field} must be an unsigned u128")))
}

fn parse_u32(field: &str, value: &str) -> Result<u32, RouteError> {
    u32::from_str(value)
        .map_err(|_| RouteError::bad_request(format!("{field} must be an unsigned u32")))
}

fn parse_durability(value: Option<&str>) -> Result<Durability, RouteError> {
    match value.unwrap_or("leader") {
        "leader" => Ok(Durability::Leader),
        "quorum" => Ok(Durability::Quorum),
        "object" => Ok(Durability::Object),
        _ => Err(RouteError::bad_request(
            "durability must be leader, quorum, or object",
        )),
    }
}

fn parse_fetch_mode(value: Option<&str>) -> Result<FetchMode, RouteError> {
    match value.unwrap_or("ordered") {
        "ordered" => Ok(FetchMode::Ordered),
        "as_available" => Ok(FetchMode::AsAvailable),
        _ => Err(RouteError::bad_request(
            "mode must be ordered or as_available",
        )),
    }
}

fn parse_producer(query: &HashMap<String, String>) -> Result<Option<ProducerIdentity>, RouteError> {
    match (
        query.get("producer_id"),
        query.get("producer_epoch"),
        query.get("first_sequence"),
    ) {
        (None, None, None) => Ok(None),
        (Some(id), Some(epoch), Some(sequence)) => Ok(Some(ProducerIdentity {
            producer_id: parse_u128("producer_id", id)?,
            epoch: parse_u32("producer_epoch", epoch)?,
            first_sequence: u64::from_str(sequence)
                .map_err(|_| RouteError::bad_request("first_sequence must be an unsigned u64"))?,
        })),
        _ => Err(RouteError::bad_request(
            "producer_id, producer_epoch, and first_sequence must be supplied together",
        )),
    }
}

fn next_request_id() -> u128 {
    u128::from(NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed))
}

#[derive(Debug)]
struct H3Response {
    status: StatusCode,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
}

impl H3Response {
    fn json(status: StatusCode, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: vec![("content-type", "application/json".into())],
            body,
        }
    }
}

#[derive(Debug)]
struct RouteError {
    status: StatusCode,
    code: &'static str,
    message: String,
    retryable: bool,
}

impl RouteError {
    fn new(
        status: StatusCode,
        code: &'static str,
        message: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            retryable,
        }
    }

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
}

impl From<EngineError> for RouteError {
    fn from(error: EngineError) -> Self {
        let message = error.to_string();
        match error {
            EngineError::InvalidConfig(_) => Self::bad_request(message),
            EngineError::UnknownPartition { .. } | EngineError::UnknownShard(_) => {
                Self::new(StatusCode::NOT_FOUND, "NOT_FOUND", message, false)
            }
            EngineError::AdmissionLimited { .. } => Self::new(
                StatusCode::TOO_MANY_REQUESTS,
                "ADMISSION_LIMITED",
                message,
                true,
            ),
            EngineError::WorkerStopped(_)
            | EngineError::DurabilityUnavailable { .. }
            | EngineError::ObjectDurabilityUnavailable => Self::new(
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
            EngineError::UnsupportedDurability(_) => Self::new(
                StatusCode::NOT_IMPLEMENTED,
                "DURABILITY_UNAVAILABLE",
                message,
                false,
            ),
            EngineError::TopicAlreadyExists(_)
            | EngineError::StaleProducerEpoch { .. }
            | EngineError::ProducerSequenceOutOfOrder { .. }
            | EngineError::ProducerRequiresNewEpoch(_)
            | EngineError::DuplicateSequenceMismatch(_)
            | EngineError::Sequencer(_) => {
                Self::new(StatusCode::CONFLICT, "CONFLICT", message, false)
            }
            EngineError::CorruptState(_) | EngineError::Storage(_) => Self::internal(message),
        }
    }
}

#[derive(Serialize)]
struct ProblemBody<'a> {
    code: &'a str,
    message: &'a str,
    retryable: bool,
}

fn error_response(error: RouteError) -> H3Response {
    let body = serde_json::to_vec(&ProblemBody {
        code: error.code,
        message: &error.message,
        retryable: error.retryable,
    })
    .unwrap_or_else(|_| {
        br#"{"code":"INTERNAL","message":"error encoding failed","retryable":false}"#.to_vec()
    });
    H3Response::json(error.status, body)
}

#[derive(Serialize)]
struct AppendResponseBody {
    request_id: String,
    batch_id: String,
    first_offset: String,
    last_offset: String,
    shard_id: u32,
    ring_epoch: u64,
    placement_sequence: String,
}

fn validate_config(config: &H3Config) -> Result<(), H3Error> {
    if config.max_batch_bytes == 0 || config.max_fetch_bytes == 0 {
        return Err(H3Error(
            "HTTP/3 batch and fetch byte limits must be nonzero".into(),
        ));
    }
    Ok(())
}

fn load_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, H3Error> {
    let bytes = std::fs::read(path).map_err(display_error("failed to read HTTP/3 certificate"))?;
    let mut reader = BufReader::new(bytes.as_slice());
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(display_error("failed to parse HTTP/3 certificate"))?;
    if certificates.is_empty() {
        Ok(vec![CertificateDer::from(bytes)])
    } else {
        Ok(certificates)
    }
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, H3Error> {
    let bytes = std::fs::read(path).map_err(display_error("failed to read HTTP/3 private key"))?;
    let mut reader = BufReader::new(bytes.as_slice());
    if let Some(key) = rustls_pemfile::private_key(&mut reader)
        .map_err(display_error("failed to parse HTTP/3 private key"))?
    {
        return Ok(key);
    }
    PrivateKeyDer::try_from(bytes)
        .map_err(|error| H3Error(format!("invalid DER HTTP/3 private key: {error}")))
}

fn display_error<E>(context: &'static str) -> impl FnOnce(E) -> H3Error
where
    E: fmt::Display,
{
    move |error| H3Error(format!("{context}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_and_query_parsing_are_strict() {
        assert_eq!(
            parse_records_path("/v1/topics/17/partitions/3/records").expect("path"),
            (TopicId::new(17), LogicalPartitionId::new(3))
        );
        assert!(parse_records_path("/v1/topics/17/records").is_err());
        let query = parse_query(Some("durability=quorum&record_count=2")).expect("query");
        assert_eq!(
            parse_durability(query.get("durability").map(String::as_str)).expect("durability"),
            Durability::Quorum
        );
    }
}
