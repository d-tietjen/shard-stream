//! Native Rust admin, producer, and consumer APIs.

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize};
use shard_stream_core::{
    BatchId, LogicalOffset, LogicalPartitionId, Placement, PlacementSequence, RingEpoch, ShardId,
    TopicId,
};
use shard_stream_protocol::{
    Durability, FetchMode, NativeFetchBatch, ProducerIdentity, WireError, decode_fetch_batches,
};
use tokio::sync::Mutex;

const DEFAULT_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024 + 4096;

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub endpoint: String,
    pub request_timeout: Duration,
    pub max_retries: u32,
    pub max_frame_bytes: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:7420".into(),
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    endpoint: Url,
    http: reqwest::Client,
    max_retries: u32,
    max_frame_bytes: usize,
    next_request_id: AtomicU64,
}

impl Client {
    pub fn new(config: ClientConfig) -> Result<Self, ClientError> {
        if config.max_frame_bytes == 0 {
            return Err(ClientError::InvalidConfig(
                "max_frame_bytes must be nonzero".into(),
            ));
        }
        let endpoint = Url::parse(config.endpoint.trim_end_matches('/'))
            .map_err(|error| ClientError::InvalidConfig(error.to_string()))?;
        if endpoint.scheme() != "http" && endpoint.scheme() != "https" {
            return Err(ClientError::InvalidConfig(
                "endpoint must use http or https".into(),
            ));
        }
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(ClientError::Transport)?;
        Ok(Self {
            inner: Arc::new(ClientInner {
                endpoint,
                http,
                max_retries: config.max_retries,
                max_frame_bytes: config.max_frame_bytes,
                next_request_id: AtomicU64::new(1),
            }),
        })
    }

    pub async fn create_topic(
        &self,
        topic_id: TopicId,
        partitions: u32,
        shards: Option<Vec<ShardId>>,
    ) -> Result<(), ClientError> {
        let url = self.url("v1/topics")?;
        let response = self
            .inner
            .http
            .post(url)
            .json(&CreateTopicRequest {
                topic_id: topic_id.to_string(),
                partitions,
                shards: shards
                    .map(|shards| shards.into_iter().map(ShardId::get).collect::<Vec<_>>()),
            })
            .send()
            .await
            .map_err(ClientError::Transport)?;
        ensure_success(response).await?;
        Ok(())
    }

    #[must_use]
    pub fn producer(
        &self,
        topic_id: TopicId,
        partition_id: LogicalPartitionId,
        producer_id: u128,
        epoch: u32,
    ) -> Producer {
        Producer {
            client: self.clone(),
            topic_id,
            partition_id,
            producer_id,
            epoch,
            next_sequence: Arc::new(Mutex::new(0)),
        }
    }

    #[must_use]
    pub fn consumer(&self, topic_id: TopicId, partition_id: LogicalPartitionId) -> Consumer {
        Consumer {
            client: self.clone(),
            topic_id,
            partition_id,
        }
    }

    fn next_request_id(&self) -> u128 {
        u128::from(self.inner.next_request_id.fetch_add(1, Ordering::Relaxed))
    }

    fn url(&self, path: &str) -> Result<Url, ClientError> {
        self.inner
            .endpoint
            .join(path)
            .map_err(|error| ClientError::InvalidConfig(error.to_string()))
    }
}

pub struct Producer {
    client: Client,
    topic_id: TopicId,
    partition_id: LogicalPartitionId,
    producer_id: u128,
    epoch: u32,
    next_sequence: Arc<Mutex<u64>>,
}

impl Producer {
    pub async fn append(
        &self,
        payload: impl Into<Vec<u8>>,
        record_count: u32,
        durability: Durability,
    ) -> Result<ProducerReceipt, ClientError> {
        if record_count == 0 {
            return Err(ClientError::InvalidConfig(
                "record_count must be nonzero".into(),
            ));
        }
        let payload = payload.into();
        let mut sequence = self.next_sequence.lock().await;
        let first_sequence = *sequence;
        let next_sequence = sequence
            .checked_add(u64::from(record_count))
            .ok_or_else(|| ClientError::InvalidConfig("producer sequence exhausted".into()))?;
        let request_id = self.client.next_request_id();
        let path = format!(
            "v1/topics/{}/partitions/{}/records",
            self.topic_id, self.partition_id
        );
        let url = self.client.url(&path)?;
        let query = [
            ("request_id", request_id.to_string()),
            ("record_count", record_count.to_string()),
            ("durability", durability_name(durability).into()),
            ("producer_id", self.producer_id.to_string()),
            ("producer_epoch", self.epoch.to_string()),
            ("first_sequence", first_sequence.to_string()),
        ];

        let mut attempt = 0u32;
        loop {
            let response = self
                .client
                .inner
                .http
                .post(url.clone())
                .query(&query)
                .header("content-type", "application/octet-stream")
                .body(payload.clone())
                .send()
                .await
                .map_err(ClientError::Transport)?;
            let status = response.status();
            if status.is_success() {
                let response = response
                    .json::<AppendResponseBody>()
                    .await
                    .map_err(ClientError::Transport)?;
                *sequence = next_sequence;
                return response.try_into();
            }
            if !matches!(
                status,
                StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
            ) || attempt >= self.client.inner.max_retries
            {
                return Err(api_error(response).await);
            }
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or_else(|| Duration::from_millis(10 * (1u64 << attempt.min(10))));
            tokio::time::sleep(retry_after).await;
            attempt += 1;
        }
    }

    pub async fn set_epoch(&mut self, epoch: u32) -> Result<(), ClientError> {
        if epoch <= self.epoch {
            return Err(ClientError::InvalidConfig(
                "producer epoch must increase".into(),
            ));
        }
        self.epoch = epoch;
        *self.next_sequence.lock().await = 0;
        Ok(())
    }

    #[must_use]
    pub fn identity(&self, first_sequence: u64) -> ProducerIdentity {
        ProducerIdentity {
            producer_id: self.producer_id,
            epoch: self.epoch,
            first_sequence,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerReceipt {
    pub request_id: u128,
    pub batch_id: BatchId,
    pub first_offset: LogicalOffset,
    pub last_offset: LogicalOffset,
    pub placement: Placement,
}

#[derive(Clone)]
pub struct Consumer {
    client: Client,
    topic_id: TopicId,
    partition_id: LogicalPartitionId,
}

impl Consumer {
    pub async fn fetch(
        &self,
        start_offset: LogicalOffset,
        max_bytes: u32,
        mode: FetchMode,
    ) -> Result<Vec<NativeFetchBatch>, ClientError> {
        if max_bytes == 0 {
            return Err(ClientError::InvalidConfig(
                "max_bytes must be nonzero".into(),
            ));
        }
        let request_id = self.client.next_request_id();
        let path = format!(
            "v1/topics/{}/partitions/{}/records",
            self.topic_id, self.partition_id
        );
        let response = self
            .client
            .inner
            .http
            .get(self.client.url(&path)?)
            .query(&[
                ("offset", start_offset.to_string()),
                ("max_bytes", max_bytes.to_string()),
                ("mode", fetch_mode_name(mode).into()),
                ("request_id", request_id.to_string()),
            ])
            .send()
            .await
            .map_err(ClientError::Transport)?;
        if !response.status().is_success() {
            return Err(api_error(response).await);
        }
        let bytes = response.bytes().await.map_err(ClientError::Transport)?;
        decode_fetch_batches(&bytes, self.client.inner.max_frame_bytes).map_err(ClientError::Wire)
    }
}

#[derive(Debug)]
pub enum ClientError {
    InvalidConfig(String),
    Transport(reqwest::Error),
    Api {
        status: StatusCode,
        code: String,
        message: String,
        retryable: bool,
    },
    InvalidResponse(String),
    Wire(WireError),
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "invalid client config: {message}"),
            Self::Transport(error) => write!(formatter, "transport error: {error}"),
            Self::Api {
                status,
                code,
                message,
                ..
            } => write!(formatter, "server error {status} {code}: {message}"),
            Self::InvalidResponse(message) => write!(formatter, "invalid response: {message}"),
            Self::Wire(error) => write!(formatter, "native protocol error: {error}"),
        }
    }
}

impl Error for ClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Wire(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize)]
struct CreateTopicRequest {
    topic_id: String,
    partitions: u32,
    shards: Option<Vec<u32>>,
}

#[derive(Debug, Deserialize)]
struct AppendResponseBody {
    request_id: String,
    batch_id: String,
    first_offset: String,
    last_offset: String,
    shard_id: u32,
    ring_epoch: u64,
    placement_sequence: String,
}

impl TryFrom<AppendResponseBody> for ProducerReceipt {
    type Error = ClientError;

    fn try_from(response: AppendResponseBody) -> Result<Self, Self::Error> {
        Ok(Self {
            request_id: parse_response_u128("request_id", &response.request_id)?,
            batch_id: BatchId::new(parse_response_u128("batch_id", &response.batch_id)?),
            first_offset: LogicalOffset::new(parse_response_u128(
                "first_offset",
                &response.first_offset,
            )?),
            last_offset: LogicalOffset::new(parse_response_u128(
                "last_offset",
                &response.last_offset,
            )?),
            placement: Placement {
                shard_id: ShardId::new(response.shard_id),
                ring_epoch: RingEpoch::new(response.ring_epoch),
                sequence: PlacementSequence::new(parse_response_u128(
                    "placement_sequence",
                    &response.placement_sequence,
                )?),
            },
        })
    }
}

#[derive(Debug, Deserialize)]
struct ProblemBody {
    code: String,
    message: String,
    retryable: bool,
}

async fn ensure_success(response: reqwest::Response) -> Result<reqwest::Response, ClientError> {
    if response.status().is_success() {
        Ok(response)
    } else {
        Err(api_error(response).await)
    }
}

async fn api_error(response: reqwest::Response) -> ClientError {
    let status = response.status();
    match response.json::<ProblemBody>().await {
        Ok(problem) => ClientError::Api {
            status,
            code: problem.code,
            message: problem.message,
            retryable: problem.retryable,
        },
        Err(error) => ClientError::InvalidResponse(format!(
            "server returned {status} with an unreadable error body: {error}"
        )),
    }
}

fn parse_response_u128(field: &str, value: &str) -> Result<u128, ClientError> {
    value.parse::<u128>().map_err(|_| {
        ClientError::InvalidResponse(format!("{field} is not an unsigned 128-bit integer"))
    })
}

const fn durability_name(durability: Durability) -> &'static str {
    match durability {
        Durability::Leader => "leader",
        Durability::Quorum => "quorum",
        Durability::Object => "object",
    }
}

const fn fetch_mode_name(mode: FetchMode) -> &'static str {
    match mode {
        FetchMode::Ordered => "ordered",
        FetchMode::AsAvailable => "as_available",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_endpoint_and_response_identifiers() {
        let config = ClientConfig {
            endpoint: "file:///tmp/socket".into(),
            ..ClientConfig::default()
        };
        assert!(matches!(
            Client::new(config),
            Err(ClientError::InvalidConfig(_))
        ));
        assert!(matches!(
            parse_response_u128("batch_id", "-1"),
            Err(ClientError::InvalidResponse(_))
        ));
    }

    #[test]
    fn response_conversion_preserves_u128_values() {
        let response = AppendResponseBody {
            request_id: u128::MAX.to_string(),
            batch_id: "2".into(),
            first_offset: "3".into(),
            last_offset: "4".into(),
            shard_id: 5,
            ring_epoch: 6,
            placement_sequence: "7".into(),
        };
        let receipt = ProducerReceipt::try_from(response).expect("receipt");
        assert_eq!(receipt.request_id, u128::MAX);
        assert_eq!(receipt.placement.sequence, PlacementSequence::new(7));
    }
}
