//! Shared effective native configuration recovery, without storage or workers.
//!
//! Factored from the native full recovery reducer; event order, defaults,
//! idempotence and errors are the same on both call paths.

use super::*;

pub(crate) fn apply_recovered_catalog_event(
    config: &EngineConfig,
    event: &JournalEvent,
    rings: &mut HashMap<TopicPartition, BTreeMap<RingEpoch, Vec<ShardId>>>,
    replication_policies: &mut HashMap<TopicPartition, PartitionReplicationPolicy>,
) -> EngineResult<()> {
    match event {
        JournalEvent::Configure {
            topic_partition,
            ring_epoch,
            shards,
        } => {
            validate_ring(config, shards)?;
            let history = rings.entry(*topic_partition).or_default();
            if history
                .last_key_value()
                .is_some_and(|(current, _)| ring_epoch <= current)
            {
                return Err(EngineError::CorruptState(format!(
                    "non-increasing ring epoch for {}/{}",
                    topic_partition.topic_id, topic_partition.partition_id
                )));
            }
            history.insert(*ring_epoch, shards.clone());
            replication_policies
                .entry(*topic_partition)
                .or_insert_with(|| config.default_replication_policy());
        }
        JournalEvent::CreatePartition {
            topic_partition,
            ring_epoch,
            shards,
            policy_generation,
            replication_factor,
            min_in_sync_replicas,
        } => {
            validate_ring(config, shards)?;
            if rings.contains_key(topic_partition) {
                return Err(EngineError::CorruptState(format!(
                    "duplicate partition creation for {}/{}",
                    topic_partition.topic_id, topic_partition.partition_id
                )));
            }
            let policy = PartitionReplicationPolicy {
                policy_generation: *policy_generation,
                replication_factor: *replication_factor,
                min_in_sync_replicas: *min_in_sync_replicas,
            };
            policy.validate(config.replication_factor)?;
            rings
                .entry(*topic_partition)
                .or_default()
                .insert(*ring_epoch, shards.clone());
            replication_policies.insert(*topic_partition, policy);
        }
        JournalEvent::UpdateReplicationPolicy {
            topic_partition,
            expected_policy_generation,
            policy_generation,
            replication_factor,
            min_in_sync_replicas,
        } => {
            if !rings.contains_key(topic_partition) {
                return Err(EngineError::CorruptState(format!(
                    "replication policy update for unconfigured partition {}/{}",
                    topic_partition.topic_id, topic_partition.partition_id
                )));
            }
            let policy = PartitionReplicationPolicy {
                policy_generation: *policy_generation,
                replication_factor: *replication_factor,
                min_in_sync_replicas: *min_in_sync_replicas,
            };
            policy.validate(config.replication_factor)?;
            let current = replication_policies
                .get(topic_partition)
                .copied()
                .unwrap_or_else(|| config.default_replication_policy());
            if current != policy {
                if current.policy_generation != *expected_policy_generation {
                    return Err(EngineError::CorruptState(format!(
                        "replication policy generation mismatch for {}/{}: expected {}, found {}",
                        topic_partition.topic_id,
                        topic_partition.partition_id,
                        expected_policy_generation,
                        current.policy_generation
                    )));
                }
                replication_policies.insert(*topic_partition, policy);
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn catalog_matches_full_typed_recovery_without_inventing_wire_acceptance() {
        let config = EngineConfig {
            data_dir: std::path::PathBuf::from("not-opened-by-this-pure-reducer-test"),
            object_store_dir: None,
            shard_count: 1,
            virtual_lane_count: 4,
            replication_factor: 3,
            min_in_sync_replicas: 2,
            queue_slots_per_shard: 1,
            queue_bytes_per_shard: 4096,
            target_pack_bytes: 4096,
            max_pack_age: Duration::from_secs(1),
            max_batch_bytes: 1024,
            max_fetch_bytes: 1024,
            append_linger: Duration::ZERO,
        };
        let tp = TopicPartition::new(TopicId::new(u128::MAX), LogicalPartitionId::new(0));
        let events = [
            JournalEvent::Configure {
                topic_partition: tp,
                ring_epoch: RingEpoch::new(1),
                shards: vec![ShardId::new(1), ShardId::new(3)],
            },
            JournalEvent::UpdateReplicationPolicy {
                topic_partition: tp,
                expected_policy_generation: 1,
                policy_generation: 5,
                replication_factor: 2,
                min_in_sync_replicas: 2,
            },
            // This exercises the original typed reducer contract ONLY. The
            // actual native SSJ3 encoder/decoder refuses such a nonadvancing
            // wire update; no fake signed/native disk history is asserted.
            JournalEvent::UpdateReplicationPolicy {
                topic_partition: tp,
                expected_policy_generation: 5,
                policy_generation: 2,
                replication_factor: 3,
                min_in_sync_replicas: 1,
            },
            JournalEvent::UpdateReplicationPolicy {
                topic_partition: tp,
                expected_policy_generation: 99,
                policy_generation: 2,
                replication_factor: 3,
                min_in_sync_replicas: 1,
            },
        ];
        let mut rings = HashMap::new();
        let mut policies = HashMap::new();
        for event in &events {
            apply_recovered_catalog_event(&config, event, &mut rings, &mut policies)
                .expect("unchanged native typed semantics");
        }
        let full = recover_partition_states(&config, &events, &HashMap::new(), 1)
            .expect("actual full native typed reducer");
        assert_eq!(full[&tp].replication_policy, policies[&tp]);
        assert_eq!(full[&tp].sequencer.ring_epoch(), RingEpoch::new(1));
        assert_eq!(policies[&tp].policy_generation, 2);
        assert_eq!(policies[&tp].min_in_sync_replicas, 1);
        assert_eq!(
            rings[&tp][&RingEpoch::new(1)],
            vec![ShardId::new(1), ShardId::new(3)]
        );
    }
}
