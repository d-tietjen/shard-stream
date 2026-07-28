//! Native Rust admin, producer, and consumer APIs.

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize};
use shard_stream_core::{
    BatchId, LeaderEpoch, LogicalOffset, LogicalPartitionId, Placement, PlacementSequence,
    RecordId, RingEpoch, ShardId, TopicId,
};
use shard_stream_protocol::{
    AppendResponse, Durability, FetchMode, LogicalCursorError, LogicalTopicBinding,
    LogicalTopicCursor, NATIVE_APPEND_RESPONSE_CONTENT_TYPE, NativeFetchBatch, ProducerIdentity,
    TopicHaMode, WireError, decode_append_response, decode_fetch_batches_owned,
};

const DEFAULT_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024 + 4096;
pub const BATCH_SIZE_ENV: &str = "SHARD_STREAM_BATCH_SIZE";
pub const LINGER_MS_ENV: &str = "SHARD_STREAM_LINGER_MS";
pub const MAX_REQUEST_SIZE_ENV: &str = "SHARD_STREAM_MAX_REQUEST_SIZE";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerTuning {
    pub batch_size: usize,
    pub linger: Duration,
    pub max_request_size: usize,
}

impl Default for ProducerTuning {
    fn default() -> Self {
        Self {
            batch_size: 2 * 1024 * 1024,
            linger: Duration::from_millis(20),
            max_request_size: 4 * 1024 * 1024,
        }
    }
}

impl ProducerTuning {
    pub fn from_env() -> Result<Self, ClientError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, ClientError> {
        let defaults = Self::default();
        let tuning = Self {
            batch_size: parse_env_value(BATCH_SIZE_ENV, lookup(BATCH_SIZE_ENV))?
                .unwrap_or(defaults.batch_size),
            linger: Duration::from_millis(
                parse_env_value(LINGER_MS_ENV, lookup(LINGER_MS_ENV))?
                    .unwrap_or(defaults.linger.as_millis() as u64),
            ),
            max_request_size: parse_env_value(MAX_REQUEST_SIZE_ENV, lookup(MAX_REQUEST_SIZE_ENV))?
                .unwrap_or(defaults.max_request_size),
        };
        tuning.validate()?;
        Ok(tuning)
    }

    fn validate(self) -> Result<(), ClientError> {
        if self.batch_size == 0 || self.max_request_size == 0 {
            return Err(ClientError::InvalidConfig(
                "producer batch and request sizes must be nonzero".into(),
            ));
        }
        if self.batch_size > self.max_request_size {
            return Err(ClientError::InvalidConfig(
                "producer batch_size cannot exceed max_request_size".into(),
            ));
        }
        if self.linger > Duration::from_secs(60) {
            return Err(ClientError::InvalidConfig(
                "producer linger cannot exceed 60 seconds".into(),
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn should_flush(self, buffered_payload_bytes: usize) -> bool {
        buffered_payload_bytes >= self.batch_size
    }
}

#[derive(Clone)]
pub struct ClientConfig {
    pub endpoint: String,
    pub request_timeout: Duration,
    pub max_retries: u32,
    pub max_frame_bytes: usize,
    pub producer: ProducerTuning,
    /// Administrative credential used for retention and ordered-log receipt
    /// endpoints. The client sends it through a dedicated header.
    pub admin_token: Option<String>,
}

impl fmt::Debug for ClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientConfig")
            .field("endpoint", &self.endpoint)
            .field("request_timeout", &self.request_timeout)
            .field("max_retries", &self.max_retries)
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("producer", &self.producer)
            .field(
                "admin_token",
                &self.admin_token.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:7420".into(),
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            producer: ProducerTuning::default(),
            admin_token: None,
        }
    }
}

impl ClientConfig {
    pub fn from_env() -> Result<Self, ClientError> {
        Ok(Self {
            producer: ProducerTuning::from_env()?,
            admin_token: std::env::var("SHARD_STREAM_ADMIN_TOKEN").ok(),
            ..Self::default()
        })
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
    producer: ProducerTuning,
    next_request_id: AtomicU64,
}

impl Client {
    pub fn new(config: ClientConfig) -> Result<Self, ClientError> {
        if config.max_frame_bytes == 0 {
            return Err(ClientError::InvalidConfig(
                "max_frame_bytes must be nonzero".into(),
            ));
        }
        config.producer.validate()?;
        let endpoint = Url::parse(config.endpoint.trim_end_matches('/'))
            .map_err(|error| ClientError::InvalidConfig(error.to_string()))?;
        if endpoint.scheme() != "http" && endpoint.scheme() != "https" {
            return Err(ClientError::InvalidConfig(
                "endpoint must use http or https".into(),
            ));
        }
        let mut default_headers = reqwest::header::HeaderMap::new();
        if let Some(token) = &config.admin_token {
            if token.is_empty() {
                return Err(ClientError::InvalidConfig(
                    "admin_token cannot be empty".into(),
                ));
            }
            default_headers.insert(
                "x-shard-stream-admin-token",
                reqwest::header::HeaderValue::from_str(token)
                    .map_err(|error| ClientError::InvalidConfig(error.to_string()))?,
            );
        }
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .default_headers(default_headers)
            .build()
            .map_err(ClientError::Transport)?;
        Ok(Self {
            inner: Arc::new(ClientInner {
                endpoint,
                http,
                max_retries: config.max_retries,
                max_frame_bytes: config.max_frame_bytes,
                producer: config.producer,
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

    /// Advances the physical retention floor for a partition.
    ///
    /// Callers must persist any external checkpoint before invoking this
    /// operation. Moving the floor backwards is rejected by the server.
    pub async fn advance_retention(
        &self,
        topic_id: TopicId,
        partition_id: LogicalPartitionId,
        log_start: LogicalOffset,
    ) -> Result<(), ClientError> {
        let path = format!("v1/topics/{topic_id}/partitions/{partition_id}/retention");
        let response = self
            .inner
            .http
            .put(self.url(&path)?)
            .json(&RetentionRequest {
                log_start: log_start.to_string(),
            })
            .send()
            .await
            .map_err(ClientError::Transport)?;
        ensure_success(response).await?;
        Ok(())
    }

    pub async fn partition_watermarks(
        &self,
        topic_id: TopicId,
        partition_id: LogicalPartitionId,
    ) -> Result<PartitionWatermarks, ClientError> {
        let path = format!("v1/topics/{topic_id}/partitions/{partition_id}/watermarks");
        let response = self
            .inner
            .http
            .get(self.url(&path)?)
            .send()
            .await
            .map_err(ClientError::Transport)?;
        let response = ensure_success(response)
            .await?
            .json::<WatermarksResponse>()
            .await
            .map_err(ClientError::Transport)?;
        response.try_into()
    }

    pub async fn retained_payload_bytes(
        &self,
        topic_id: TopicId,
        partition_id: LogicalPartitionId,
        start_offset: LogicalOffset,
    ) -> Result<u64, ClientError> {
        let path = format!("v1/topics/{topic_id}/partitions/{partition_id}/retained-bytes");
        let response = self
            .inner
            .http
            .get(self.url(&path)?)
            .query(&[("start_offset", start_offset.to_string())])
            .send()
            .await
            .map_err(ClientError::Transport)?;
        let response = ensure_success(response)
            .await?
            .json::<RetainedBytesResponse>()
            .await
            .map_err(ClientError::Transport)?;
        parse_response_u64("payload_bytes", &response.payload_bytes)
    }

    /// Loads the current logical-to-physical topic binding.
    ///
    /// HA-capable brokers return only bindings backed by verified topic
    /// finality. Legacy active-passive generation-one brokers return an
    /// explicit unfinalized compatibility binding.
    pub async fn topic_binding(
        &self,
        topic_id: TopicId,
    ) -> Result<LogicalTopicBinding, ClientError> {
        let path = format!("v1/topics/{topic_id}/binding");
        let binding = self
            .inner
            .http
            .get(self.url(&path)?)
            .send()
            .await
            .map_err(ClientError::Transport)?;
        let binding = ensure_success(binding)
            .await?
            .json::<LogicalTopicBinding>()
            .await
            .map_err(ClientError::Transport)?;
        binding.validate().map_err(ClientError::InvalidResponse)?;
        if binding.logical_topic_id != topic_id {
            return Err(ClientError::InvalidResponse(
                "topic binding belongs to another logical topic".into(),
            ));
        }
        Ok(binding)
    }

    pub async fn initial_cursor(
        &self,
        topic_id: TopicId,
        partition_id: LogicalPartitionId,
    ) -> Result<LogicalTopicCursor, ClientError> {
        let path = format!("v1/topics/{topic_id}/partitions/{partition_id}/cursor");
        let cursor = self
            .inner
            .http
            .get(self.url(&path)?)
            .send()
            .await
            .map_err(ClientError::Transport)?;
        let cursor = ensure_success(cursor)
            .await?
            .json::<LogicalTopicCursor>()
            .await
            .map_err(ClientError::Transport)?;
        cursor.validate().map_err(ClientError::Cursor)?;
        if cursor.topic_partition != shard_stream_core::TopicPartition::new(topic_id, partition_id)
        {
            return Err(ClientError::InvalidResponse(
                "initial cursor belongs to another topic partition".into(),
            ));
        }
        Ok(cursor)
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
            leader_epoch: None,
            next_sequence: 0,
            tuning: self.inner.producer,
            binding: None,
        }
    }

    #[must_use]
    pub fn consumer(&self, topic_id: TopicId, partition_id: LogicalPartitionId) -> Consumer {
        Consumer {
            client: self.clone(),
            topic_id,
            partition_id,
            binding: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    #[must_use]
    pub fn logical_consumer(
        &self,
        topic_id: TopicId,
        partition_id: LogicalPartitionId,
    ) -> LogicalConsumer {
        LogicalConsumer {
            client: self.clone(),
            topic_id,
            partition_id,
            cursor: tokio::sync::Mutex::new(None),
            binding: tokio::sync::Mutex::new(None),
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
    leader_epoch: Option<u64>,
    next_sequence: u64,
    tuning: ProducerTuning,
    binding: Option<LogicalTopicBinding>,
}

impl Producer {
    #[must_use]
    pub fn with_leader_epoch(mut self, leader_epoch: u64) -> Self {
        self.leader_epoch = Some(leader_epoch);
        self
    }

    /// Restores the next producer sequence from durable broker state.
    ///
    /// Callers must use the same producer ID and epoch. The broker still
    /// validates the sequence, so a stale cursor fails closed.
    #[must_use]
    pub fn resume_at(mut self, next_sequence: u64) -> Self {
        self.next_sequence = next_sequence;
        self
    }

    /// Reissues a known sequence for an idempotent recovery attempt.
    pub fn retry_at(&mut self, first_sequence: u64) {
        self.next_sequence = first_sequence;
    }

    #[must_use]
    pub const fn tuning(&self) -> ProducerTuning {
        self.tuning
    }

    pub async fn append(
        &mut self,
        payload: impl Into<Bytes>,
        record_count: u32,
        durability: Durability,
    ) -> Result<ProducerReceipt, ClientError> {
        if record_count == 0 {
            return Err(ClientError::InvalidConfig(
                "record_count must be nonzero".into(),
            ));
        }
        let payload: Bytes = payload.into();
        if payload.len() > self.tuning.max_request_size {
            return Err(ClientError::InvalidConfig(format!(
                "append payload has {} bytes, exceeding producer max_request_size {}",
                payload.len(),
                self.tuning.max_request_size
            )));
        }
        let first_sequence = self.next_sequence;
        let next_sequence = self
            .next_sequence
            .checked_add(u64::from(record_count))
            .ok_or_else(|| ClientError::InvalidConfig("producer sequence exhausted".into()))?;
        let request_id = self.client.next_request_id();
        let event_id = RecordId::for_producer_event(
            self.topic_id,
            self.partition_id,
            self.producer_id,
            self.epoch,
            first_sequence,
        );
        let mut binding = match &self.binding {
            Some(binding) => binding.clone(),
            None => self.client.topic_binding(self.topic_id).await?,
        };
        validate_binding_partition(&binding, self.partition_id)?;
        let path = format!(
            "v1/topics/{}/partitions/{}/records",
            self.topic_id, self.partition_id
        );
        let url = self.client.url(&path)?;

        let mut attempt = 0u32;
        loop {
            let mut query = vec![
                ("request_id", request_id.to_string()),
                ("record_count", record_count.to_string()),
                ("durability", durability_name(durability).into()),
                ("producer_id", self.producer_id.to_string()),
                ("producer_epoch", self.epoch.to_string()),
                ("first_sequence", first_sequence.to_string()),
                ("event_id", event_id.to_string()),
                (
                    "expected_data_generation",
                    binding.data_generation.to_string(),
                ),
            ];
            if let Some(leader_epoch) = self.leader_epoch {
                query.push(("leader_epoch", leader_epoch.to_string()));
            }
            let response = self
                .client
                .inner
                .http
                .post(url.clone())
                .query(&query)
                .header("content-type", "application/octet-stream")
                .header("accept", NATIVE_APPEND_RESPONSE_CONTENT_TYPE)
                .body(payload.clone())
                .send()
                .await
                .map_err(ClientError::Transport)?;
            let status = response.status();
            if status.is_success() {
                let is_native_response = response
                    .headers()
                    .get("content-type")
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| value.starts_with(NATIVE_APPEND_RESPONSE_CONTENT_TYPE));
                if is_native_response {
                    let body = response.bytes().await.map_err(ClientError::Transport)?;
                    let response = decode_append_response(&body).map_err(ClientError::Wire)?;
                    self.next_sequence = next_sequence;
                    self.binding = Some(binding);
                    return Ok(response.into());
                }
                let response = response
                    .json::<AppendResponseBody>()
                    .await
                    .map_err(ClientError::Transport)?;
                self.next_sequence = next_sequence;
                self.binding = Some(binding);
                return response.try_into();
            }
            if status == StatusCode::CONFLICT {
                let error = api_error(response).await;
                if error.api_code() == Some("STALE_DATA_GENERATION")
                    && attempt < self.client.inner.max_retries
                {
                    binding = self.client.topic_binding(self.topic_id).await?;
                    validate_binding_partition(&binding, self.partition_id)?;
                    attempt += 1;
                    continue;
                }
                return Err(error);
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
        self.next_sequence = 0;
        Ok(())
    }

    #[must_use]
    pub fn identity(&self, first_sequence: u64) -> ProducerIdentity {
        ProducerIdentity {
            producer_id: self.producer_id,
            epoch: self.epoch,
            first_sequence,
            event_id: Some(RecordId::for_producer_event(
                self.topic_id,
                self.partition_id,
                self.producer_id,
                self.epoch,
                first_sequence,
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerReceipt {
    pub request_id: u128,
    pub batch_id: BatchId,
    pub record_id: RecordId,
    pub first_offset: LogicalOffset,
    pub last_offset: LogicalOffset,
    pub visible_through: LogicalOffset,
    pub placement: Placement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionWatermarks {
    pub log_start: LogicalOffset,
    pub allocated_end: LogicalOffset,
    pub contiguous_log_end: LogicalOffset,
    pub replicated_high_watermark: LogicalOffset,
    pub last_stable_offset: LogicalOffset,
}

impl From<AppendResponse> for ProducerReceipt {
    fn from(response: AppendResponse) -> Self {
        Self {
            request_id: response.request_id,
            batch_id: response.batch_id,
            record_id: response.record_id,
            first_offset: response.first_offset,
            last_offset: response.last_offset,
            visible_through: response.visible_through,
            placement: response.placement,
        }
    }
}

#[derive(Clone)]
pub struct Consumer {
    client: Client,
    topic_id: TopicId,
    partition_id: LogicalPartitionId,
    binding: Arc<tokio::sync::Mutex<Option<LogicalTopicBinding>>>,
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
        let cached = self.binding.lock().await.clone();
        let mut binding = match cached {
            Some(binding) => binding,
            None => self.client.topic_binding(self.topic_id).await?,
        };
        validate_binding_partition(&binding, self.partition_id)?;
        if binding.mode != TopicHaMode::ActivePassive || binding.data_generation != 1 {
            return Err(ClientError::SdkUpgradeRequired(Box::new(binding)));
        }
        let path = format!(
            "v1/topics/{}/partitions/{}/records",
            self.topic_id, self.partition_id
        );
        let url = self.client.url(&path)?;
        let mut attempt = 0;
        loop {
            let request_id = self.client.next_request_id();
            let response = self
                .client
                .inner
                .http
                .get(url.clone())
                .query(&[
                    ("offset", start_offset.to_string()),
                    ("max_bytes", max_bytes.to_string()),
                    ("mode", fetch_mode_name(mode).into()),
                    ("request_id", request_id.to_string()),
                    (
                        "expected_data_generation",
                        binding.data_generation.to_string(),
                    ),
                ])
                .send()
                .await
                .map_err(ClientError::Transport)?;
            if response.status().is_success() {
                let bytes = response.bytes().await.map_err(ClientError::Transport)?;
                *self.binding.lock().await = Some(binding);
                return decode_fetch_batches_owned(bytes, self.client.inner.max_frame_bytes)
                    .map_err(ClientError::Wire);
            }
            let error = api_error(response).await;
            if error.api_code() == Some("STALE_DATA_GENERATION")
                && attempt < self.client.inner.max_retries
            {
                binding = self.client.topic_binding(self.topic_id).await?;
                validate_binding_partition(&binding, self.partition_id)?;
                if binding.mode != TopicHaMode::ActivePassive || binding.data_generation != 1 {
                    return Err(ClientError::SdkUpgradeRequired(Box::new(binding)));
                }
                attempt += 1;
                continue;
            }
            return Err(error);
        }
    }
}

pub struct LogicalConsumer {
    client: Client,
    topic_id: TopicId,
    partition_id: LogicalPartitionId,
    cursor: tokio::sync::Mutex<Option<LogicalTopicCursor>>,
    binding: tokio::sync::Mutex<Option<LogicalTopicBinding>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalFetchPage {
    pub batches: Vec<NativeFetchBatch>,
    pub next_cursor: LogicalTopicCursor,
}

impl LogicalConsumer {
    pub async fn fetch(
        &self,
        max_bytes: u32,
        mode: FetchMode,
    ) -> Result<LogicalFetchPage, ClientError> {
        if max_bytes == 0 {
            return Err(ClientError::InvalidConfig(
                "max_bytes must be nonzero".into(),
            ));
        }
        if mode == FetchMode::AsAvailable {
            return Err(ClientError::InvalidConfig(
                "logical cursor fetch requires a contiguous ordered or replicated mode".into(),
            ));
        }
        let mut binding = match self.binding.lock().await.clone() {
            Some(binding) => binding,
            None => self.client.topic_binding(self.topic_id).await?,
        };
        validate_binding_partition(&binding, self.partition_id)?;
        let mut cursor = match self.cursor.lock().await.clone() {
            Some(cursor) => cursor,
            None => {
                self.client
                    .initial_cursor(self.topic_id, self.partition_id)
                    .await?
            }
        };
        let path = format!(
            "v1/topics/{}/partitions/{}/records",
            self.topic_id, self.partition_id
        );
        let url = self.client.url(&path)?;
        let mut attempt = 0;
        loop {
            let request_id = self.client.next_request_id();
            let response = self
                .client
                .inner
                .http
                .get(url.clone())
                .query(&[
                    ("cursor", cursor.encode_hex().map_err(ClientError::Cursor)?),
                    ("max_bytes", max_bytes.to_string()),
                    ("mode", fetch_mode_name(mode).into()),
                    ("request_id", request_id.to_string()),
                    (
                        "expected_data_generation",
                        cursor.data_generation.to_string(),
                    ),
                ])
                .send()
                .await
                .map_err(ClientError::Transport)?;
            if response.status().is_success() {
                let next_cursor = response
                    .headers()
                    .get("x-shard-stream-next-cursor")
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| {
                        ClientError::InvalidResponse(
                            "logical fetch response omitted its next cursor".into(),
                        )
                    })
                    .and_then(|encoded| {
                        LogicalTopicCursor::decode_hex(encoded).map_err(ClientError::Cursor)
                    })?;
                if next_cursor.topic_partition != cursor.topic_partition {
                    return Err(ClientError::InvalidResponse(
                        "logical fetch returned a cursor for another partition".into(),
                    ));
                }
                let bytes = response.bytes().await.map_err(ClientError::Transport)?;
                let batches = decode_fetch_batches_owned(bytes, self.client.inner.max_frame_bytes)
                    .map_err(ClientError::Wire)?;
                cursor = next_cursor;
                *self.cursor.lock().await = Some(cursor.clone());
                *self.binding.lock().await = Some(binding);
                return Ok(LogicalFetchPage {
                    batches,
                    next_cursor: cursor,
                });
            }
            let error = api_error(response).await;
            if error.api_code() == Some("STALE_DATA_GENERATION")
                && attempt < self.client.inner.max_retries
            {
                binding = self.client.topic_binding(self.topic_id).await?;
                validate_binding_partition(&binding, self.partition_id)?;
                // The broker traverses the persisted cutover cursor. Keep the
                // same logical cursor and retry against the refreshed head.
                attempt += 1;
                continue;
            }
            return Err(error);
        }
    }

    pub async fn set_cursor(&self, cursor: LogicalTopicCursor) -> Result<(), ClientError> {
        if cursor.topic_partition.topic_id != self.topic_id
            || cursor.topic_partition.partition_id != self.partition_id
        {
            return Err(ClientError::InvalidConfig(
                "logical cursor belongs to another consumer".into(),
            ));
        }
        cursor.validate().map_err(ClientError::Cursor)?;
        *self.cursor.lock().await = Some(cursor);
        Ok(())
    }

    pub async fn cursor(&self) -> Option<LogicalTopicCursor> {
        self.cursor.lock().await.clone()
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
    Cursor(LogicalCursorError),
    SdkUpgradeRequired(Box<LogicalTopicBinding>),
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
            Self::Cursor(error) => write!(formatter, "logical cursor error: {error}"),
            Self::SdkUpgradeRequired(binding) => write!(
                formatter,
                "SDK upgrade required for {:?} data generation {}",
                binding.mode, binding.data_generation
            ),
        }
    }
}

impl Error for ClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Wire(error) => Some(error),
            Self::Cursor(error) => Some(error),
            _ => None,
        }
    }
}

impl ClientError {
    fn api_code(&self) -> Option<&str> {
        match self {
            Self::Api { code, .. } => Some(code),
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

#[derive(Debug, Serialize)]
struct RetentionRequest {
    log_start: String,
}

#[derive(Debug, Deserialize)]
struct WatermarksResponse {
    log_start: String,
    allocated_end: String,
    contiguous_log_end: String,
    replicated_high_watermark: String,
    last_stable_offset: String,
}

impl TryFrom<WatermarksResponse> for PartitionWatermarks {
    type Error = ClientError;

    fn try_from(response: WatermarksResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            log_start: LogicalOffset::new(parse_response_u64("log_start", &response.log_start)?),
            allocated_end: LogicalOffset::new(parse_response_u64(
                "allocated_end",
                &response.allocated_end,
            )?),
            contiguous_log_end: LogicalOffset::new(parse_response_u64(
                "contiguous_log_end",
                &response.contiguous_log_end,
            )?),
            replicated_high_watermark: LogicalOffset::new(parse_response_u64(
                "replicated_high_watermark",
                &response.replicated_high_watermark,
            )?),
            last_stable_offset: LogicalOffset::new(parse_response_u64(
                "last_stable_offset",
                &response.last_stable_offset,
            )?),
        })
    }
}

#[derive(Debug, Deserialize)]
struct RetainedBytesResponse {
    payload_bytes: String,
}

#[derive(Debug, Deserialize)]
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
}

impl TryFrom<AppendResponseBody> for ProducerReceipt {
    type Error = ClientError;

    fn try_from(response: AppendResponseBody) -> Result<Self, Self::Error> {
        Ok(Self {
            request_id: parse_response_u128("request_id", &response.request_id)?,
            batch_id: BatchId::new(parse_response_u128("batch_id", &response.batch_id)?),
            record_id: RecordId::new(parse_response_u128("record_id", &response.record_id)?),
            first_offset: LogicalOffset::new(parse_response_u64(
                "first_offset",
                &response.first_offset,
            )?),
            last_offset: LogicalOffset::new(parse_response_u64(
                "last_offset",
                &response.last_offset,
            )?),
            visible_through: LogicalOffset::new(parse_response_u64(
                "visible_through",
                &response.visible_through,
            )?),
            placement: Placement {
                virtual_lane_id: ShardId::new(response.shard_id),
                ring_epoch: RingEpoch::new(response.ring_epoch),
                leader_epoch: LeaderEpoch::new(response.leader_epoch),
                sequence: PlacementSequence::new(parse_response_u64(
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

fn parse_response_u64(field: &str, value: &str) -> Result<u64, ClientError> {
    value.parse::<u64>().map_err(|_| {
        ClientError::InvalidResponse(format!("{field} is not a valid unsigned 64-bit integer"))
    })
}

fn validate_binding_partition(
    binding: &LogicalTopicBinding,
    partition_id: LogicalPartitionId,
) -> Result<(), ClientError> {
    if partition_id.get() >= binding.partitions {
        return Err(ClientError::InvalidConfig(format!(
            "partition {} is outside topic partition count {}",
            partition_id.get(),
            binding.partitions
        )));
    }
    Ok(())
}

fn parse_env_value<T>(name: &str, value: Option<String>) -> Result<Option<T>, ClientError>
where
    T: std::str::FromStr,
{
    value
        .map(|value| {
            value.parse::<T>().map_err(|_| {
                ClientError::InvalidConfig(format!("{name} must be a non-negative base-10 integer"))
            })
        })
        .transpose()
}

const fn durability_name(durability: Durability) -> &'static str {
    match durability {
        Durability::Leader => "leader",
        Durability::Quorum => "quorum",
        Durability::AllReplicas => "all_replicas",
        Durability::Object => "object",
    }
}

const fn fetch_mode_name(mode: FetchMode) -> &'static str {
    match mode {
        FetchMode::Ordered => "ordered",
        FetchMode::Replicated => "replicated",
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
            record_id: "8".into(),
            first_offset: "3".into(),
            last_offset: "4".into(),
            visible_through: "5".into(),
            shard_id: 5,
            ring_epoch: 6,
            leader_epoch: 8,
            placement_sequence: "7".into(),
        };
        let receipt = ProducerReceipt::try_from(response).expect("receipt");
        assert_eq!(receipt.request_id, u128::MAX);
        assert_eq!(receipt.placement.sequence, PlacementSequence::new(7));
    }

    #[test]
    fn parses_partition_watermarks_without_precision_loss() {
        let watermarks = PartitionWatermarks::try_from(WatermarksResponse {
            log_start: "1".into(),
            allocated_end: "2".into(),
            contiguous_log_end: "3".into(),
            replicated_high_watermark: "4".into(),
            last_stable_offset: u64::MAX.to_string(),
        })
        .expect("watermarks");
        assert_eq!(watermarks.log_start, LogicalOffset::new(1));
        assert_eq!(watermarks.last_stable_offset, LogicalOffset::new(u64::MAX));
    }

    #[test]
    fn producer_tuning_uses_kafka_familiar_environment_names() {
        let tuning = ProducerTuning::from_lookup(|name| match name {
            BATCH_SIZE_ENV => Some("2097152".into()),
            LINGER_MS_ENV => Some("20".into()),
            MAX_REQUEST_SIZE_ENV => Some("4194304".into()),
            _ => None,
        })
        .expect("producer tuning");
        assert_eq!(tuning.batch_size, 2 * 1024 * 1024);
        assert_eq!(tuning.linger, Duration::from_millis(20));
        assert_eq!(tuning.max_request_size, 4 * 1024 * 1024);
        assert!(tuning.should_flush(2 * 1024 * 1024));
    }

    #[test]
    fn producer_tuning_rejects_a_batch_larger_than_the_request() {
        let error = ProducerTuning::from_lookup(|name| match name {
            BATCH_SIZE_ENV => Some("2097152".into()),
            MAX_REQUEST_SIZE_ENV => Some("1048576".into()),
            _ => None,
        })
        .expect_err("invalid producer tuning");
        assert!(matches!(error, ClientError::InvalidConfig(_)));
    }

    #[tokio::test]
    async fn producer_enforces_the_configured_request_limit_before_network_io() {
        let client = Client::new(ClientConfig {
            producer: ProducerTuning {
                batch_size: 4,
                linger: Duration::ZERO,
                max_request_size: 4,
            },
            ..ClientConfig::default()
        })
        .expect("client");
        let mut producer = client.producer(TopicId::new(1), LogicalPartitionId::new(0), 1, 0);
        let error = producer
            .append(vec![0; 5], 1, Durability::Leader)
            .await
            .expect_err("oversized request");
        assert!(matches!(error, ClientError::InvalidConfig(_)));
    }
}
