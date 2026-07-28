use std::fmt;
use std::path::{Path, PathBuf};

use serde::Serialize;
use shard_stream_core::LogicalOffset;

use crate::LifecycleState;

/// Readiness thresholds applied to both embedded and listener-based brokers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthPolicy {
    /// Refuse producer readiness below this filesystem headroom.
    pub minimum_disk_free_bytes: u64,
    /// Require the assignment/lease control plane to report a quorum.
    pub require_quorum: bool,
    /// Require recovery replication to drain before becoming ready.
    pub require_replication_caught_up: bool,
}

impl Default for HealthPolicy {
    fn default() -> Self {
        Self {
            minimum_disk_free_bytes: 1024 * 1024 * 1024,
            require_quorum: true,
            require_replication_caught_up: true,
        }
    }
}

/// State supplied by storage and consumer coordination implementations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HealthProbeSnapshot {
    pub oldest_consumer_checkpoint: Option<LogicalOffset>,
    pub disk_free_bytes: u64,
    pub corruption_detected: bool,
}

/// Composable health dependency for embedded customers.
///
/// The default implementation measures the WAL filesystem. An Eden host can
/// wrap it to add its durable consumer-checkpoint minimum and external
/// corruption/scrubber state without replacing shard-stream's evaluator.
pub trait HealthProbe: Send + Sync + fmt::Debug {
    fn snapshot(&self) -> Result<HealthProbeSnapshot, String>;
}

#[derive(Debug, Clone)]
pub struct FilesystemHealthProbe {
    data_dir: PathBuf,
}

impl FilesystemHealthProbe {
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }

    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

impl HealthProbe for FilesystemHealthProbe {
    fn snapshot(&self) -> Result<HealthProbeSnapshot, String> {
        fs2::available_space(&self.data_dir)
            .map(|disk_free_bytes| HealthProbeSnapshot {
                oldest_consumer_checkpoint: None,
                disk_free_bytes,
                corruption_detected: false,
            })
            .map_err(|error| {
                format!(
                    "failed to inspect free space for {}: {error}",
                    self.data_dir.display()
                )
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionHealth {
    pub topic_id: u128,
    pub partition_id: u32,
    pub local_leader: bool,
    pub assignment_current: bool,
    pub leader_epoch: u64,
    pub in_sync_replicas: Vec<u32>,
    pub minimum_in_sync_replicas: u32,
    pub writable: bool,
    pub visible_end: u64,
    pub replicated_end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthObservation {
    pub lifecycle: LifecycleState,
    pub quorum_available: bool,
    pub assignment_current: bool,
    pub assignment_epoch: u64,
    pub recovery_replication_caught_up: bool,
    pub replication_backlog_bytes: u64,
    pub partitions: Vec<PartitionHealth>,
    pub probe: Result<HealthProbeSnapshot, String>,
    pub runtime_corruption_detected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum ReadinessBlocker {
    Lifecycle {
        lifecycle: LifecycleState,
    },
    AssignmentStale,
    QuorumUnavailable,
    InsufficientIsr {
        topic_id: String,
        partition_id: u32,
        required: u32,
        available: u32,
    },
    WriteFenced {
        topic_id: String,
        partition_id: u32,
        leader_epoch: u64,
    },
    ReplicationCatchUp {
        backlog_bytes: u64,
    },
    LowDiskSpace {
        available_bytes: u64,
        required_bytes: u64,
    },
    CorruptionDetected,
    HealthProbeFailed {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PartitionHealthSnapshot {
    pub topic_id: String,
    pub partition_id: u32,
    pub local_leader: bool,
    pub assignment_current: bool,
    pub leader_epoch: u64,
    pub isr: Vec<u32>,
    pub minimum_isr: u32,
    pub writable: bool,
    pub visible_end: u64,
    pub replicated_end: u64,
}

/// Conservative broker-level health with exact per-partition detail.
///
/// `visible_end` and `replicated_end` are the minima across hosted
/// partitions. `leader_epoch` is the maximum observed epoch and `isr` is the
/// sorted union. Consumers that need an exact answer should inspect
/// `partitions`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HealthSnapshot {
    pub lifecycle: LifecycleState,
    pub ready: bool,
    pub writable: bool,
    pub quorum_available: bool,
    pub assignment_epoch: u64,
    pub leader_epoch: u64,
    pub isr: Vec<u32>,
    pub visible_end: u64,
    pub replicated_end: u64,
    pub oldest_consumer_checkpoint: Option<u64>,
    pub replication_backlog_bytes: u64,
    pub disk_free_bytes: u64,
    pub corruption_detected: bool,
    pub blockers: Vec<ReadinessBlocker>,
    pub partitions: Vec<PartitionHealthSnapshot>,
}

impl HealthSnapshot {
    #[must_use]
    pub fn evaluate(observation: HealthObservation, policy: HealthPolicy) -> Self {
        let HealthObservation {
            lifecycle,
            quorum_available,
            assignment_current,
            assignment_epoch,
            recovery_replication_caught_up,
            replication_backlog_bytes,
            mut partitions,
            probe,
            runtime_corruption_detected,
        } = observation;
        partitions.sort_by_key(|partition| (partition.topic_id, partition.partition_id));

        let mut blockers = Vec::new();
        if lifecycle != LifecycleState::Ready {
            blockers.push(ReadinessBlocker::Lifecycle { lifecycle });
        }
        if !assignment_current {
            blockers.push(ReadinessBlocker::AssignmentStale);
        }
        if policy.require_quorum && !quorum_available {
            blockers.push(ReadinessBlocker::QuorumUnavailable);
        }
        for partition in &partitions {
            let available = u32::try_from(partition.in_sync_replicas.len()).unwrap_or(u32::MAX);
            if !partition.assignment_current || available < partition.minimum_in_sync_replicas {
                if !partition.assignment_current
                    && !blockers.contains(&ReadinessBlocker::AssignmentStale)
                {
                    blockers.push(ReadinessBlocker::AssignmentStale);
                }
                if available < partition.minimum_in_sync_replicas {
                    blockers.push(ReadinessBlocker::InsufficientIsr {
                        topic_id: partition.topic_id.to_string(),
                        partition_id: partition.partition_id,
                        required: partition.minimum_in_sync_replicas,
                        available,
                    });
                }
            }
            if partition.local_leader && !partition.writable {
                blockers.push(ReadinessBlocker::WriteFenced {
                    topic_id: partition.topic_id.to_string(),
                    partition_id: partition.partition_id,
                    leader_epoch: partition.leader_epoch,
                });
            }
        }
        if policy.require_replication_caught_up && !recovery_replication_caught_up {
            blockers.push(ReadinessBlocker::ReplicationCatchUp {
                backlog_bytes: replication_backlog_bytes,
            });
        }

        let (probe, probe_failed) = match probe {
            Ok(probe) => (probe, None),
            Err(message) => (HealthProbeSnapshot::default(), Some(message)),
        };
        if probe.disk_free_bytes < policy.minimum_disk_free_bytes {
            blockers.push(ReadinessBlocker::LowDiskSpace {
                available_bytes: probe.disk_free_bytes,
                required_bytes: policy.minimum_disk_free_bytes,
            });
        }
        let corruption_detected = runtime_corruption_detected || probe.corruption_detected;
        if corruption_detected {
            blockers.push(ReadinessBlocker::CorruptionDetected);
        }
        if let Some(message) = probe_failed {
            blockers.push(ReadinessBlocker::HealthProbeFailed { message });
        }

        let writable_partitions = partitions
            .iter()
            .filter(|partition| partition.local_leader)
            .all(|partition| partition.writable);
        let writable = lifecycle == LifecycleState::Ready
            && assignment_current
            && quorum_available
            && writable_partitions
            && !corruption_detected
            && probe.disk_free_bytes >= policy.minimum_disk_free_bytes;
        let leader_epoch = partitions
            .iter()
            .map(|partition| partition.leader_epoch)
            .max()
            .unwrap_or(0);
        let visible_end = partitions
            .iter()
            .map(|partition| partition.visible_end)
            .min()
            .unwrap_or(0);
        let replicated_end = partitions
            .iter()
            .map(|partition| partition.replicated_end)
            .min()
            .unwrap_or(0);
        let mut isr = partitions
            .iter()
            .flat_map(|partition| partition.in_sync_replicas.iter().copied())
            .collect::<Vec<_>>();
        isr.sort_unstable();
        isr.dedup();
        let partitions = partitions
            .into_iter()
            .map(|partition| PartitionHealthSnapshot {
                topic_id: partition.topic_id.to_string(),
                partition_id: partition.partition_id,
                local_leader: partition.local_leader,
                assignment_current: partition.assignment_current,
                leader_epoch: partition.leader_epoch,
                isr: partition.in_sync_replicas,
                minimum_isr: partition.minimum_in_sync_replicas,
                writable: partition.writable,
                visible_end: partition.visible_end,
                replicated_end: partition.replicated_end,
            })
            .collect();

        Self {
            lifecycle,
            ready: blockers.is_empty(),
            writable,
            quorum_available,
            assignment_epoch,
            leader_epoch,
            isr,
            visible_end,
            replicated_end,
            oldest_consumer_checkpoint: probe.oldest_consumer_checkpoint.map(LogicalOffset::get),
            replication_backlog_bytes,
            disk_free_bytes: probe.disk_free_bytes,
            corruption_detected,
            blockers,
            partitions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation() -> HealthObservation {
        HealthObservation {
            lifecycle: LifecycleState::Ready,
            quorum_available: true,
            assignment_current: true,
            assignment_epoch: 7,
            recovery_replication_caught_up: true,
            replication_backlog_bytes: 0,
            partitions: vec![PartitionHealth {
                topic_id: 9,
                partition_id: 0,
                local_leader: true,
                assignment_current: true,
                leader_epoch: 11,
                in_sync_replicas: vec![1, 2, 3],
                minimum_in_sync_replicas: 2,
                writable: true,
                visible_end: 50,
                replicated_end: 48,
            }],
            probe: Ok(HealthProbeSnapshot {
                oldest_consumer_checkpoint: Some(LogicalOffset::new(12)),
                disk_free_bytes: 10_000,
                corruption_detected: false,
            }),
            runtime_corruption_detected: false,
        }
    }

    #[test]
    fn healthy_observation_is_ready_and_writable() {
        let snapshot = HealthSnapshot::evaluate(
            observation(),
            HealthPolicy {
                minimum_disk_free_bytes: 1_000,
                ..HealthPolicy::default()
            },
        );
        assert!(snapshot.ready);
        assert!(snapshot.writable);
        assert_eq!(snapshot.oldest_consumer_checkpoint, Some(12));
        assert_eq!(snapshot.isr, vec![1, 2, 3]);
    }

    #[test]
    fn every_safety_precondition_blocks_readiness() {
        let mut observation = observation();
        observation.lifecycle = LifecycleState::CatchingUp;
        observation.quorum_available = false;
        observation.assignment_current = false;
        observation.recovery_replication_caught_up = false;
        observation.replication_backlog_bytes = 42;
        observation.partitions[0].in_sync_replicas = vec![1];
        observation.partitions[0].writable = false;
        observation.probe = Ok(HealthProbeSnapshot {
            oldest_consumer_checkpoint: None,
            disk_free_bytes: 10,
            corruption_detected: true,
        });
        let snapshot = HealthSnapshot::evaluate(
            observation,
            HealthPolicy {
                minimum_disk_free_bytes: 1_000,
                ..HealthPolicy::default()
            },
        );
        assert!(!snapshot.ready);
        assert!(!snapshot.writable);
        assert!(
            snapshot
                .blockers
                .iter()
                .any(|blocker| matches!(blocker, ReadinessBlocker::InsufficientIsr { .. }))
        );
        assert!(
            snapshot
                .blockers
                .contains(&ReadinessBlocker::QuorumUnavailable)
        );
        assert!(
            snapshot
                .blockers
                .contains(&ReadinessBlocker::CorruptionDetected)
        );
        assert!(
            snapshot
                .blockers
                .iter()
                .any(|blocker| matches!(blocker, ReadinessBlocker::WriteFenced { .. }))
        );
    }
}
