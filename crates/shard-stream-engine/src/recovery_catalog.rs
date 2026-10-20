//! Nonmutating effective catalog from the actual retained native journal.

use std::collections::{BTreeMap, HashMap};

use shard_stream_core::RingEpoch;

use super::*;
use crate::PartitionReplicationPolicy;

/// Privately constructed native configuration facts under the original custody.
///
/// This uses the SAME ordered Configure/CreatePartition/UpdateReplicationPolicy
/// reducer as full engine recovery. It does not validate all payload/truncation
/// state, authenticate an assignment, open storage or grant serving permission.
/// Native placement rings contain virtual-lane tokens. Resolve them through
/// [`EngineConfig::physical_shard_for_lane`]; they are distinct from physical
/// storage shards and a host's broker replica topology.
pub struct NativeRecoveryCatalog {
    rings: HashMap<TopicPartition, BTreeMap<RingEpoch, Vec<ShardId>>>,
    policies: HashMap<TopicPartition, PartitionReplicationPolicy>,
    virtual_lane_count: u32,
    expiry: Instant,
    usage: NativeRecoveryUsage,
    // LAST: all catalog values settle before this actual original allocation.
    owner: Arc<DirectoryOwnership>,
}

/// Borrowed effective policy and full native placement-ring history over virtual lanes.
///
/// There is no adoption or construction API; this view borrows a genuine native
/// census-derived catalog. Neither a policy nor a ring is write permission.
pub struct NativeRecoveryPartition<'catalog> {
    rings: &'catalog BTreeMap<RingEpoch, Vec<ShardId>>,
    policy: PartitionReplicationPolicy,
    virtual_lane_count: u32,
}

impl NativeRecoveryCatalog {
    /// Actual configured partition keys; iteration order is unspecified.
    pub fn partitions(&self) -> impl Iterator<Item = TopicPartition> + '_ {
        self.rings.keys().copied()
    }

    /// Exact native effective configuration, with no default for an absent TP.
    pub fn partition(&self, tp: TopicPartition) -> Option<NativeRecoveryPartition<'_>> {
        Some(NativeRecoveryPartition {
            rings: self.rings.get(&tp)?,
            policy: *self.policies.get(&tp)?,
            virtual_lane_count: self.virtual_lane_count,
        })
    }

    /// Actual original directory allocation, for a closed host's identity join.
    pub fn directory_ownership(&self) -> &Arc<DirectoryOwnership> {
        &self.owner
    }

    /// Actual saved native reader Instant, never a renewed relative deadline.
    pub const fn absolute_expiry(&self) -> Instant {
        self.expiry
    }

    /// Cumulative native census usage including this catalog, without a refund.
    pub const fn usage(&self) -> NativeRecoveryUsage {
        self.usage
    }
}

impl NativeRecoveryPartition<'_> {
    /// Every strictly ordered native placement ring over virtual lanes.
    ///
    /// Tokens retain their native `ShardId` representation; resolve physical
    /// storage with [`EngineConfig::physical_shard_for_lane`] on the original
    /// configuration. They are not physical shard or broker replica IDs.
    pub fn ring_history(&self) -> &BTreeMap<RingEpoch, Vec<ShardId>> {
        self.rings
    }

    /// Actual native effective replication policy, including legacy defaults.
    pub const fn replication_policy(&self) -> PartitionReplicationPolicy {
        self.policy
    }

    /// Actual engine lane count used by native ring validation.
    pub const fn virtual_lane_count(&self) -> u32 {
        self.virtual_lane_count
    }
}

impl NativeRecoveryCensus {
    /// Reduce a bounded native configuration catalog BEFORE mutable storage open.
    ///
    /// The original configuration, journal, owner, counters and Instant stay in
    /// this census. No caller event/map/policy is accepted. Full pre-admission
    /// covers native map/ring values, ring validation scratch, comparisons,
    /// clones and finite error text before the shared reducer runs. Repeated
    /// calls consume the SAME allowance and never renew its clock.
    pub fn catalog(&mut self) -> Result<NativeRecoveryCatalog> {
        self.attempt.clock()?;
        // Fixed output, simultaneously held containers, iterator/scalar values
        // and native diagnostic text. Native errors format only static text and
        // at most two full numeric TPs/generations or u32 counts (no paths).
        const ERROR_BYTES: usize = 256;
        let fixed = size_of::<NativeRecoveryCatalog>()
            + size_of::<NativeRecoveryPartition<'_>>()
            + size_of::<HashMap<TopicPartition, BTreeMap<RingEpoch, Vec<ShardId>>>>()
            + size_of::<HashMap<TopicPartition, PartitionReplicationPolicy>>()
            + size_of::<[usize; 8]>()
            + ERROR_BYTES;
        self.attempt.charge(0, 8, fixed, 0, fixed as u64)?;

        let add = |a: usize, b: usize| a.checked_add(b).ok_or_else(limited);
        let mul = |a: usize, b: usize| a.checked_mul(b).ok_or_else(limited);
        let count = self.attempt.journal.len();
        let scan = mul(add(count, 1)?, size_of::<JournalEvent>())?;
        self.attempt.charge(0, count, 0, 0, scan as u64)?;
        let mut shards = 0usize;
        for event in &self.attempt.journal {
            self.attempt.clock()?;
            if let JournalEvent::Configure { shards: ring, .. }
            | JournalEvent::CreatePartition { shards: ring, .. } = event
            {
                shards = add(shards, ring.len())?;
            }
        }
        let node = size_of::<(TopicPartition, BTreeMap<RingEpoch, Vec<ShardId>>)>()
            + size_of::<(TopicPartition, PartitionReplicationPolicy)>()
            + size_of::<(RingEpoch, Vec<ShardId>)>();
        // At most one distinct partition/ring/policy insertion per event. Each
        // ring clone and its native HashSet validation scratch are admitted.
        let bytes = add(mul(count, node)?, mul(shards, 2 * size_of::<ShardId>())?)?;
        let entries = add(mul(count, 3)?, mul(shards, 2)?)?;
        // Source-bound quadratic probes dominate both native hash-map collision
        // scans and BTree searches; ring validation/clone visits are separate.
        let event_work = mul(mul(add(count, 1)?, add(count, 1)?)?, node)?;
        let ring_work = mul(
            mul(add(shards, 1)?, add(shards, 1)?)?,
            2 * size_of::<ShardId>(),
        )?;
        let work = add(event_work, ring_work)?;
        self.attempt.charge(0, entries, bytes, 0, work as u64)?;
        let mut rings = HashMap::new();
        let mut policies = HashMap::new();
        for event in &self.attempt.journal {
            self.attempt.clock()?;
            crate::coordinator::apply_recovered_catalog_event(
                &self.attempt.config,
                event,
                &mut rings,
                &mut policies,
            )?;
            self.attempt.clock()?;
        }
        self.attempt.clock()?;
        Ok(NativeRecoveryCatalog {
            rings,
            policies,
            virtual_lane_count: self.attempt.config.virtual_lane_count,
            expiry: self.attempt.expiry,
            usage: self.attempt.usage,
            owner: Arc::clone(&self.attempt.owner),
        })
    }
}

#[cfg(test)]
#[path = "recovery_catalog_tests.rs"]
mod tests;
