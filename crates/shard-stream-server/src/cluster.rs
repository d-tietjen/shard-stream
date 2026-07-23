use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::header::{CONTENT_LENGTH, HOST};
use axum::http::{HeaderMap, HeaderName, Response};
use axum::middleware::Next;
use reqwest::redirect::Policy;
use serde::{Deserialize, Serialize};
use shard_stream_core::{LogicalPartitionId, ShardId, StaticTopology, TopicId, TopicPartition};
use shard_stream_engine::{EngineError, EngineResult, StreamEngine, TopicConfig};
use shard_stream_kafka::TopicAdmin;

use super::AppError;

const CLUSTER_TOKEN_HEADER: &str = "x-shard-stream-cluster-token";
const TOPOLOGY_DIGEST_HEADER: &str = "x-shard-stream-topology-digest";
const PROXY_HOP_HEADER: &str = "x-shard-stream-proxy-hop";
const CLUSTER_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const CLUSTER_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ClusterTopicRequest {
    pub(crate) topic_id: String,
    pub(crate) partitions: u32,
    pub(crate) shards: Option<Vec<u32>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct TopologyResponse {
    cluster_mode: &'static str,
    local_node_id: u32,
    topology_digest: String,
    ownership: &'static str,
    nodes: Vec<TopologyNodeResponse>,
}

#[derive(Debug, Serialize)]
struct TopologyNodeResponse {
    node_id: u32,
    rest: String,
    grpc: String,
    kafka_host: String,
    kafka_port: u16,
}

pub(crate) struct ClusterRuntime {
    topology: Arc<StaticTopology>,
    token: Arc<str>,
    async_client: reqwest::Client,
    max_request_bytes: usize,
    max_response_bytes: usize,
}

impl ClusterRuntime {
    pub(crate) fn new(
        topology: Arc<StaticTopology>,
        token: String,
        max_request_bytes: usize,
        max_response_bytes: usize,
    ) -> Result<Self, AppError> {
        let async_client = reqwest::Client::builder()
            .redirect(Policy::none())
            .connect_timeout(CLUSTER_CONNECT_TIMEOUT)
            .timeout(CLUSTER_REQUEST_TIMEOUT)
            .build()
            .map_err(|error| {
                AppError::internal(format!("failed to build cluster HTTP client: {error}"))
            })?;
        Ok(Self {
            topology,
            token: Arc::from(token),
            async_client,
            max_request_bytes,
            max_response_bytes,
        })
    }

    pub(crate) fn snapshot(&self) -> TopologyResponse {
        TopologyResponse {
            cluster_mode: if self.topology.is_multi_node() {
                "static-active-active-rf1"
            } else {
                "single-node"
            },
            local_node_id: self.topology.local_node_id(),
            topology_digest: format!("{:016x}", self.topology.digest()),
            ownership: "xxh3-64(topic_id_le || partition_id_le) modulo sorted_node_count",
            nodes: self
                .topology
                .nodes()
                .iter()
                .map(|node| TopologyNodeResponse {
                    node_id: node.node_id,
                    rest: node.advertised_rest.clone(),
                    grpc: node.advertised_grpc.clone(),
                    kafka_host: node.kafka_host.clone(),
                    kafka_port: node.kafka_port,
                })
                .collect(),
        }
    }

    pub(crate) async fn fanout_topic(&self, request: &ClusterTopicRequest) -> Result<(), AppError> {
        for node in self
            .topology
            .nodes()
            .iter()
            .filter(|node| node.node_id != self.topology.local_node_id())
        {
            let url = format!("{}/internal/v1/topics", node.internal_rest);
            let response = self
                .async_client
                .post(url)
                .header(CLUSTER_TOKEN_HEADER, self.token.as_ref())
                .header(TOPOLOGY_DIGEST_HEADER, self.digest_header())
                .json(request)
                .send()
                .await
                .map_err(|error| {
                    AppError::cluster_unavailable(format!(
                        "node {} topic synchronization failed: {error}",
                        node.node_id
                    ))
                })?;
            if !response.status().is_success() {
                return Err(AppError::cluster_unavailable(format!(
                    "node {} rejected topic synchronization with status {}",
                    node.node_id,
                    response.status()
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn verify_internal_headers(&self, headers: &HeaderMap) -> Result<(), AppError> {
        let token = headers
            .get(CLUSTER_TOKEN_HEADER)
            .and_then(|value| value.to_str().ok());
        if token != Some(self.token.as_ref()) || self.token.is_empty() {
            return Err(AppError::forbidden(
                "invalid or missing internal cluster token",
            ));
        }
        self.verify_digest(headers)
    }

    fn verify_digest(&self, headers: &HeaderMap) -> Result<(), AppError> {
        if let Some(digest) = headers
            .get(TOPOLOGY_DIGEST_HEADER)
            .and_then(|value| value.to_str().ok())
            && digest != self.digest_header()
        {
            return Err(AppError::cluster_unavailable(
                "cluster topology digest mismatch",
            ));
        }
        Ok(())
    }

    fn digest_header(&self) -> String {
        format!("{:016x}", self.topology.digest())
    }

    async fn proxy(
        &self,
        request: Request,
        topic_partition: TopicPartition,
    ) -> Result<Response<Body>, AppError> {
        if request.headers().contains_key(PROXY_HOP_HEADER) {
            return Err(AppError::cluster_unavailable(
                "cluster request reached more than one proxy hop",
            ));
        }
        let owner = self.topology.owner(topic_partition);
        let path_and_query = request
            .uri()
            .path_and_query()
            .map_or_else(|| request.uri().path(), |value| value.as_str());
        let url = format!("{}{}", owner.internal_rest, path_and_query);
        let (parts, body) = request.into_parts();
        let body = to_bytes(body, self.max_request_bytes)
            .await
            .map_err(|error| AppError::bad_request(format!("request body failed: {error}")))?;
        let mut outbound = self
            .async_client
            .request(parts.method, url)
            .header(CLUSTER_TOKEN_HEADER, self.token.as_ref())
            .header(TOPOLOGY_DIGEST_HEADER, self.digest_header())
            .header(PROXY_HOP_HEADER, self.topology.local_node_id().to_string());
        for (name, value) in &parts.headers {
            if !is_hop_by_hop(name)
                && !is_internal_cluster_header(name)
                && *name != HOST
                && *name != CONTENT_LENGTH
            {
                outbound = outbound.header(name, value);
            }
        }
        let mut response = outbound.body(body).send().await.map_err(|error| {
            AppError::cluster_unavailable(format!(
                "partition coordinator {} is unavailable: {error}",
                owner.node_id
            ))
        })?;
        if response
            .content_length()
            .is_some_and(|length| length > self.max_response_bytes as u64)
        {
            return Err(AppError::cluster_unavailable(
                "proxied response exceeds the configured cluster bound",
            ));
        }
        let status = response.status();
        let headers = response.headers().clone();
        let initial_capacity = response
            .content_length()
            .unwrap_or_default()
            .min(self.max_response_bytes as u64) as usize;
        let mut body = Vec::with_capacity(initial_capacity);
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            AppError::cluster_unavailable(format!("failed to read proxied response: {error}"))
        })? {
            if chunk.len() > self.max_response_bytes.saturating_sub(body.len()) {
                return Err(AppError::cluster_unavailable(
                    "proxied response exceeds the configured cluster bound",
                ));
            }
            body.extend_from_slice(&chunk);
        }
        let mut proxied = Response::new(Body::from(body));
        *proxied.status_mut() = status;
        for (name, value) in &headers {
            if !is_hop_by_hop(name) && *name != CONTENT_LENGTH {
                proxied.headers_mut().insert(name, value.clone());
            }
        }
        Ok(proxied)
    }
}

pub(crate) async fn route_partition_owner(
    State(cluster): State<Arc<ClusterRuntime>>,
    request: Request,
    next: Next,
) -> Response<Body> {
    let Some(topic_partition) = partition_from_path(request.uri().path()) else {
        return next.run(request).await;
    };
    if request.headers().contains_key(PROXY_HOP_HEADER)
        && let Err(error) = cluster.verify_internal_headers(request.headers())
    {
        return axum::response::IntoResponse::into_response(error);
    }
    if cluster.topology.is_local_owner(topic_partition) {
        return next.run(request).await;
    }
    cluster
        .proxy(request, topic_partition)
        .await
        .unwrap_or_else(axum::response::IntoResponse::into_response)
}

pub(crate) fn create_topic_local(
    engine: &StreamEngine,
    request: &ClusterTopicRequest,
) -> EngineResult<()> {
    let topic_id = request
        .topic_id
        .parse::<u128>()
        .map(TopicId::new)
        .map_err(|_| EngineError::InvalidConfig("topic_id must be an unsigned u128".into()))?;
    let topic = TopicConfig {
        topic_id,
        partitions: request.partitions,
        shards: request
            .shards
            .clone()
            .map(|shards| shards.into_iter().map(ShardId::new).collect()),
    };
    match engine.create_topic(topic) {
        Ok(()) => Ok(()),
        Err(EngineError::TopicAlreadyExists(existing))
            if existing == topic_id
                && engine.topic_partitions(topic_id).len() == request.partitions as usize =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

pub(crate) struct ClusterTopicAdmin {
    engine: Arc<StreamEngine>,
    cluster: Arc<ClusterRuntime>,
}

impl ClusterTopicAdmin {
    pub(crate) fn new(engine: Arc<StreamEngine>, cluster: Arc<ClusterRuntime>) -> Self {
        Self { engine, cluster }
    }
}

impl TopicAdmin for ClusterTopicAdmin {
    fn create_topic(
        &self,
        topic: TopicConfig,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = EngineResult<()>> + Send + '_>> {
        let engine = Arc::clone(&self.engine);
        let cluster = Arc::clone(&self.cluster);
        let request = ClusterTopicRequest {
            topic_id: topic.topic_id.to_string(),
            partitions: topic.partitions,
            shards: topic
                .shards
                .map(|shards| shards.into_iter().map(ShardId::get).collect()),
        };
        Box::pin(async move {
            let local_request = request.clone();
            tokio::task::spawn_blocking(move || create_topic_local(&engine, &local_request))
                .await
                .map_err(|error| {
                    EngineError::InvalidConfig(format!(
                        "Kafka cluster topic admin task failed: {error}"
                    ))
                })??;
            let mut durable_nodes = 1u32;
            for node in cluster
                .topology
                .nodes()
                .iter()
                .filter(|node| node.node_id != cluster.topology.local_node_id())
            {
                let response = cluster
                    .async_client
                    .post(format!("{}/internal/v1/topics", node.internal_rest))
                    .header(CLUSTER_TOKEN_HEADER, cluster.token.as_ref())
                    .header(TOPOLOGY_DIGEST_HEADER, cluster.digest_header())
                    .json(&request)
                    .send()
                    .await;
                if response.is_ok_and(|response| response.status().is_success()) {
                    durable_nodes = durable_nodes.saturating_add(1);
                }
            }
            let required_nodes = cluster.topology.nodes().len() as u32;
            if durable_nodes != required_nodes {
                return Err(EngineError::DurabilityUnavailable {
                    required_replicas: required_nodes,
                    durable_replicas: durable_nodes,
                });
            }
            Ok(())
        })
    }
}

fn partition_from_path(path: &str) -> Option<TopicPartition> {
    let fields = path.trim_matches('/').split('/').collect::<Vec<_>>();
    if fields.len() < 6 || fields[0] != "v1" || fields[1] != "topics" || fields[3] != "partitions" {
        return None;
    }
    Some(TopicPartition::new(
        TopicId::new(fields[2].parse().ok()?),
        LogicalPartitionId::new(fields[4].parse().ok()?),
    ))
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn is_internal_cluster_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        CLUSTER_TOKEN_HEADER | TOPOLOGY_DIGEST_HEADER | PROXY_HOP_HEADER
    )
}

#[cfg(test)]
mod tests {
    use shard_stream_core::ClusterNode;

    use super::*;

    #[test]
    fn partition_paths_route_and_control_paths_do_not() {
        assert_eq!(
            partition_from_path("/v1/topics/42/partitions/7/records"),
            Some(TopicPartition::new(
                TopicId::new(42),
                LogicalPartitionId::new(7)
            ))
        );
        assert!(partition_from_path("/v1/topics").is_none());
        assert!(partition_from_path("/internal/v1/topics").is_none());
    }

    #[test]
    fn topology_snapshot_hides_internal_addresses_and_token() {
        let topology = Arc::new(
            StaticTopology::new(
                0,
                vec![ClusterNode {
                    node_id: 0,
                    internal_rest: "http://private:7420".into(),
                    advertised_rest: "http://public:7420".into(),
                    advertised_grpc: "http://public:7421".into(),
                    kafka_host: "public".into(),
                    kafka_port: 9092,
                }],
            )
            .expect("topology"),
        );
        let runtime =
            ClusterRuntime::new(topology, "secret".into(), 1024, 1024).expect("cluster runtime");
        let encoded = serde_json::to_string(&runtime.snapshot()).expect("snapshot");
        assert!(encoded.contains("http://public:7420"));
        assert!(!encoded.contains("private"));
        assert!(!encoded.contains("secret"));
    }
}
