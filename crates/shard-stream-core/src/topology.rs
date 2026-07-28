use crate::{
    AssignmentEpoch, BrokerId, LeaderEpoch, LogicalOffset, RingEpoch, TopicPartition, VirtualLaneId,
};
use std::fmt;

/// Stable, transport-neutral broker metadata advertised by assignment
/// providers. Capacity values are relative weights, not hard limits.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BrokerDescriptor {
    pub broker_id: BrokerId,
    pub incarnation: u64,
    pub rack: String,
    pub zone: String,
    pub internal_rest: String,
    pub internal_replication: String,
    pub advertised_rest: String,
    pub advertised_grpc: String,
    pub kafka_host: String,
    pub kafka_port: u16,
    pub cpu_capacity: u32,
    pub disk_capacity_bytes: u64,
    pub network_capacity_bytes_per_sec: u64,
    pub certificate_digest: [u8; 32],
}

/// The explicit, finalized placement for one logical partition.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionAssignment {
    pub topic_partition: TopicPartition,
    pub replicas: Vec<BrokerId>,
    pub leader: BrokerId,
    pub preferred_follower: Option<BrokerId>,
    pub assignment_epoch: AssignmentEpoch,
    pub leader_epoch: LeaderEpoch,
    pub ring_epoch: RingEpoch,
    pub virtual_lane_count: u32,
    pub min_in_sync_replicas: u32,
}

impl PartitionAssignment {
    pub fn validate(&self) -> Result<(), String> {
        if self.replicas.is_empty() {
            return Err("partition assignment must contain a replica".into());
        }
        if self.virtual_lane_count == 0 {
            return Err("partition assignment must contain a virtual lane".into());
        }
        if self.min_in_sync_replicas == 0
            || self.min_in_sync_replicas as usize > self.replicas.len()
        {
            return Err("minimum ISR must be between one and the replica count".into());
        }
        if !self.replicas.contains(&self.leader) {
            return Err("partition leader must be a replica".into());
        }
        if self
            .preferred_follower
            .is_some_and(|broker| broker == self.leader || !self.replicas.contains(&broker))
        {
            return Err("preferred follower must be a non-leader replica".into());
        }
        let mut unique = self.replicas.clone();
        unique.sort_unstable();
        unique.dedup();
        if unique.len() != self.replicas.len() {
            return Err("partition assignment contains duplicate replicas".into());
        }
        Ok(())
    }

    #[must_use]
    pub fn virtual_lanes(&self) -> impl ExactSizeIterator<Item = VirtualLaneId> {
        (0..self.virtual_lane_count).map(VirtualLaneId::new)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplicaProgress {
    pub topic_partition: TopicPartition,
    pub broker_id: BrokerId,
    pub leader_epoch: LeaderEpoch,
    pub durable_end: LogicalOffset,
    pub visible_end: LogicalOffset,
    pub object_end: LogicalOffset,
    pub in_sync: bool,
    pub observed_at_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtentDescriptor {
    pub topic_partition: TopicPartition,
    pub first_offset: LogicalOffset,
    pub last_offset: LogicalOffset,
    pub virtual_lane_id: VirtualLaneId,
    pub ring_epoch: RingEpoch,
    pub leader_epoch: LeaderEpoch,
    pub checksum_xxh3_128: [u8; 16],
    pub object_key: Option<String>,
}

/// Read-only routing surface consumed by brokers and protocol adapters.
///
/// Implementations publish immutable snapshots so data-path reads never take a
/// cluster-global lock.
pub trait AssignmentProvider: Send + Sync + fmt::Debug {
    fn local_broker_id(&self) -> BrokerId;
    fn brokers(&self) -> Vec<BrokerDescriptor>;
    fn assignment(&self, topic_partition: TopicPartition) -> Option<PartitionAssignment>;
    fn assignment_epoch(&self) -> AssignmentEpoch;

    fn broker(&self, broker_id: BrokerId) -> Option<BrokerDescriptor> {
        self.brokers()
            .into_iter()
            .find(|broker| broker.broker_id == broker_id)
    }

    fn leader(&self, topic_partition: TopicPartition) -> Option<BrokerDescriptor> {
        self.assignment(topic_partition)
            .and_then(|assignment| self.broker(assignment.leader))
    }

    fn is_local_leader(&self, topic_partition: TopicPartition) -> bool {
        self.assignment(topic_partition)
            .is_some_and(|assignment| assignment.leader == self.local_broker_id())
    }
}

/// Fixed RF1 assignment used by the public server.
#[derive(Debug, Clone)]
pub struct SingleNodeAssignment {
    broker: BrokerDescriptor,
    virtual_lane_count: u32,
}

impl SingleNodeAssignment {
    pub fn new(broker: BrokerDescriptor, virtual_lane_count: u32) -> Result<Self, String> {
        if broker.broker_id.get() > i32::MAX as u32 {
            return Err("broker ID must fit Kafka's signed 32-bit broker ID".into());
        }
        if broker.kafka_host.is_empty() != (broker.kafka_port == 0) {
            return Err(
                "Kafka host and port must either both be configured or both be absent".into(),
            );
        }
        if virtual_lane_count == 0 {
            return Err("virtual lane count must be nonzero".into());
        }
        Ok(Self {
            broker,
            virtual_lane_count,
        })
    }

    #[must_use]
    pub const fn broker(&self) -> &BrokerDescriptor {
        &self.broker
    }
}

impl AssignmentProvider for SingleNodeAssignment {
    fn local_broker_id(&self) -> BrokerId {
        self.broker.broker_id
    }

    fn brokers(&self) -> Vec<BrokerDescriptor> {
        vec![self.broker.clone()]
    }

    fn assignment(&self, topic_partition: TopicPartition) -> Option<PartitionAssignment> {
        Some(PartitionAssignment {
            topic_partition,
            replicas: vec![self.broker.broker_id],
            leader: self.broker.broker_id,
            preferred_follower: None,
            assignment_epoch: AssignmentEpoch::new(1),
            leader_epoch: LeaderEpoch::new(0),
            ring_epoch: RingEpoch::new(1),
            virtual_lane_count: self.virtual_lane_count,
            min_in_sync_replicas: 1,
        })
    }

    fn assignment_epoch(&self) -> AssignmentEpoch {
        AssignmentEpoch::new(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker(kafka_host: &str, kafka_port: u16) -> BrokerDescriptor {
        BrokerDescriptor {
            broker_id: BrokerId::new(0),
            incarnation: 1,
            rack: String::new(),
            zone: String::new(),
            internal_rest: "http://127.0.0.1:7420".into(),
            internal_replication: String::new(),
            advertised_rest: "http://127.0.0.1:7420".into(),
            advertised_grpc: "http://127.0.0.1:7421".into(),
            kafka_host: kafka_host.into(),
            kafka_port,
            cpu_capacity: 1,
            disk_capacity_bytes: 0,
            network_capacity_bytes_per_sec: 0,
            certificate_digest: [0; 32],
        }
    }

    #[test]
    fn single_node_assignment_accepts_an_absent_optional_kafka_endpoint() {
        assert!(SingleNodeAssignment::new(broker("", 0), 1).is_ok());
        assert!(SingleNodeAssignment::new(broker("127.0.0.1", 9092), 1).is_ok());
    }

    #[test]
    fn single_node_assignment_rejects_a_partial_kafka_endpoint() {
        assert!(SingleNodeAssignment::new(broker("", 9092), 1).is_err());
        assert!(SingleNodeAssignment::new(broker("127.0.0.1", 0), 1).is_err());
    }
}
