use std::fmt;
use std::str::FromStr;

use xxhash_rust::xxh3::xxh3_64;

use crate::TopicPartition;

/// One member of a static shard-stream cluster.
///
/// The textual form is:
///
/// `node_id@internal_rest@advertised_rest@advertised_grpc@kafka_host:kafka_port`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNode {
    pub node_id: u32,
    pub internal_rest: String,
    pub advertised_rest: String,
    pub advertised_grpc: String,
    pub kafka_host: String,
    pub kafka_port: u16,
}

impl ClusterNode {
    fn validate(&self) -> Result<(), String> {
        if self.node_id > i32::MAX as u32 {
            return Err("cluster node_id must fit Kafka's signed 32-bit broker ID".into());
        }
        for (name, endpoint) in [
            ("internal REST", self.internal_rest.as_str()),
            ("advertised REST", self.advertised_rest.as_str()),
            ("advertised gRPC", self.advertised_grpc.as_str()),
        ] {
            if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
                return Err(format!("{name} endpoint must use http:// or https://"));
            }
        }
        if self.kafka_host.is_empty() {
            return Err("Kafka host must not be empty".into());
        }
        Ok(())
    }
}

impl FromStr for ClusterNode {
    type Err = String;

    fn from_str(spec: &str) -> Result<Self, Self::Err> {
        let mut fields = spec.split('@');
        let node_id = fields
            .next()
            .ok_or_else(|| node_spec_error(spec))?
            .parse::<u32>()
            .map_err(|_| node_spec_error(spec))?;
        let internal_rest = trim_endpoint(fields.next().ok_or_else(|| node_spec_error(spec))?);
        let advertised_rest = trim_endpoint(fields.next().ok_or_else(|| node_spec_error(spec))?);
        let advertised_grpc = trim_endpoint(fields.next().ok_or_else(|| node_spec_error(spec))?);
        let kafka = fields.next().ok_or_else(|| node_spec_error(spec))?;
        if fields.next().is_some() {
            return Err(node_spec_error(spec));
        }
        let (kafka_host, kafka_port) = kafka
            .rsplit_once(':')
            .ok_or_else(|| node_spec_error(spec))?;
        let node = Self {
            node_id,
            internal_rest,
            advertised_rest,
            advertised_grpc,
            kafka_host: kafka_host.to_owned(),
            kafka_port: kafka_port
                .parse::<u16>()
                .map_err(|_| node_spec_error(spec))?,
        };
        node.validate()?;
        Ok(node)
    }
}

impl fmt::Display for ClusterNode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}@{}@{}@{}@{}:{}",
            self.node_id,
            self.internal_rest,
            self.advertised_rest,
            self.advertised_grpc,
            self.kafka_host,
            self.kafka_port
        )
    }
}

#[derive(Debug, Clone)]
pub struct StaticTopology {
    local_node_id: u32,
    nodes: Vec<ClusterNode>,
    digest: u64,
}

impl StaticTopology {
    pub fn new(local_node_id: u32, mut nodes: Vec<ClusterNode>) -> Result<Self, String> {
        if nodes.is_empty() {
            return Err("cluster topology must contain at least one node".into());
        }
        for node in &nodes {
            node.validate()?;
        }
        nodes.sort_unstable_by_key(|node| node.node_id);
        if nodes
            .windows(2)
            .any(|pair| pair[0].node_id == pair[1].node_id)
        {
            return Err("cluster topology contains duplicate node IDs".into());
        }
        if !nodes.iter().any(|node| node.node_id == local_node_id) {
            return Err(format!(
                "local node {local_node_id} is missing from the cluster topology"
            ));
        }
        let digest = topology_digest(&nodes);
        Ok(Self {
            local_node_id,
            nodes,
            digest,
        })
    }

    #[must_use]
    pub const fn local_node_id(&self) -> u32 {
        self.local_node_id
    }

    #[must_use]
    pub fn local_node(&self) -> &ClusterNode {
        self.nodes
            .iter()
            .find(|node| node.node_id == self.local_node_id)
            .expect("validated topology contains local node")
    }

    #[must_use]
    pub fn nodes(&self) -> &[ClusterNode] {
        &self.nodes
    }

    #[must_use]
    pub fn owner(&self, topic_partition: TopicPartition) -> &ClusterNode {
        let mut key = [0u8; 20];
        key[..16].copy_from_slice(&topic_partition.topic_id.get().to_le_bytes());
        key[16..].copy_from_slice(&topic_partition.partition_id.get().to_le_bytes());
        let index = xxh3_64(&key) as usize % self.nodes.len();
        &self.nodes[index]
    }

    #[must_use]
    pub fn is_local_owner(&self, topic_partition: TopicPartition) -> bool {
        self.owner(topic_partition).node_id == self.local_node_id
    }

    #[must_use]
    pub const fn digest(&self) -> u64 {
        self.digest
    }

    #[must_use]
    pub fn is_multi_node(&self) -> bool {
        self.nodes.len() > 1
    }
}

fn trim_endpoint(endpoint: &str) -> String {
    endpoint.trim_end_matches('/').to_owned()
}

fn node_spec_error(spec: &str) -> String {
    format!(
        "invalid cluster node {spec:?}; expected \
         node_id@internal_rest@advertised_rest@advertised_grpc@kafka_host:kafka_port"
    )
}

fn topology_digest(nodes: &[ClusterNode]) -> u64 {
    let mut canonical = Vec::new();
    for node in nodes {
        canonical.extend_from_slice(&node.node_id.to_le_bytes());
        for value in [
            node.internal_rest.as_bytes(),
            node.advertised_rest.as_bytes(),
            node.advertised_grpc.as_bytes(),
            node.kafka_host.as_bytes(),
        ] {
            canonical.extend_from_slice(&(value.len() as u64).to_le_bytes());
            canonical.extend_from_slice(value);
        }
        canonical.extend_from_slice(&node.kafka_port.to_le_bytes());
    }
    xxh3_64(&canonical)
}

#[cfg(test)]
mod tests {
    use crate::{LogicalPartitionId, TopicId};

    use super::*;

    fn nodes() -> Vec<ClusterNode> {
        (0..3)
            .map(|node_id| ClusterNode {
                node_id,
                internal_rest: format!("http://node-{node_id}:7420"),
                advertised_rest: format!("http://localhost:{}7420", node_id + 1),
                advertised_grpc: format!("http://localhost:{}7421", node_id + 1),
                kafka_host: "localhost".into(),
                kafka_port: 19_092 + node_id as u16,
            })
            .collect()
    }

    #[test]
    fn textual_node_spec_round_trips() {
        let node = nodes().remove(0);
        assert_eq!(node.to_string().parse::<ClusterNode>().expect("node"), node);
    }

    #[test]
    fn ownership_is_stable_and_uses_every_node() {
        let topology = StaticTopology::new(1, nodes()).expect("topology");
        let second = StaticTopology::new(2, nodes().into_iter().rev().collect()).expect("topology");
        let mut counts = [0usize; 3];
        for partition in 0..1_000 {
            let topic_partition =
                TopicPartition::new(TopicId::new(42), LogicalPartitionId::new(partition));
            let owner = topology.owner(topic_partition).node_id;
            counts[owner as usize] += 1;
            assert_eq!(owner, second.owner(topic_partition).node_id);
        }
        assert!(counts.into_iter().all(|count| count > 250));
        assert_eq!(topology.digest(), second.digest());
    }

    #[test]
    fn topology_rejects_missing_and_duplicate_nodes() {
        assert!(StaticTopology::new(9, nodes()).is_err());
        let mut duplicate = nodes();
        duplicate[1].node_id = duplicate[0].node_id;
        assert!(StaticTopology::new(0, duplicate).is_err());
    }
}
