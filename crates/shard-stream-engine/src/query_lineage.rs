//! Negative source-lineage observations, not indexed or serving authority.
//!
//! Mutators change one packed word before admitted effects. An entered batch
//! keeps its active unit in its actual coordinator record until the physical
//! result reaches complete/fail. Lost results leave that unit active. Readers
//! perform one load; no lock, RPC, retry, owner replacement or renewal occurs.

use std::fmt;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use shard_stream_core::TopicPartition;

use crate::{EngineHandle, EngineResult};

const ACTIVE_BITS: u32 = usize::BITS / 2;
const ACTIVE_MASK: usize = (1usize << ACTIVE_BITS) - 1;
const REVISION_STEP: usize = 1usize << ACTIVE_BITS;
const DENIED: usize = 1usize << (usize::BITS - 1);
const REVISION_MASK: usize = (DENIED - 1) & !ACTIVE_MASK;

/// Saved negative observation of the same partition and complete native owner.
///
/// This does not prove checkpoint/index completeness, current assignment,
/// authentication, write permission, HA replay, or permission to serve. A host
/// must retain its original independent evidence and recheck after producing a
/// real result. A later mutation invalidates this saved observation permanently.
/// No method refreshes its revision or extracts/replaces the engine owner.
pub struct EngineQueryLineage {
    partition: TopicPartition,
    revision: usize,
    source: Arc<Source>,
    // The actual ordinary/recovered owner drops after the borrowed source fact.
    owner: EngineHandle,
}

impl fmt::Debug for EngineQueryLineage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EngineQueryLineage")
            .field("partition", &self.partition)
            .finish_non_exhaustive()
    }
}

impl EngineQueryLineage {
    /// Checks this exact owner/partition and saved settled revision.
    ///
    /// One atomic load and allocation-identity comparisons only. Call before
    /// work and after retaining the complete real result, before success. A
    /// successful check can linearize before a later concurrent mutation; it
    /// does not retain publication permission through a later transport write.
    #[must_use]
    pub fn is_current_for(&self, owner: &EngineHandle, partition: TopicPartition) -> bool {
        self.partition == partition
            && self.owner.ptr_eq(owner)
            && self.source.word.load(Ordering::SeqCst) == self.revision
    }
}

impl EngineHandle {
    /// Captures one settled source observation from this actual coordinator.
    ///
    /// Blocking initial-capture operation only: this sends a real coordinator
    /// RPC and retains the complete engine owner. Do not call on async request
    /// threads or use it to renew a previously stored proof. Query checks use
    /// only `EngineQueryLineage::is_current_for`. The composing capture retains
    /// its original deadline, physical task and full checkpoint/assignment
    /// checks, then rechecks every saved observation before publishing a proof.
    /// This ordinary read-only API does not claim a strict native-work budget.
    ///
    /// # Errors
    /// Returns the actual unknown-partition/coordinator error. `None` means an
    /// active, uncertain or exhausted source; no replacement fact is created.
    pub fn observe_query_lineage(
        &self,
        partition: TopicPartition,
    ) -> EngineResult<Option<EngineQueryLineage>> {
        let source = self.engine().query_lineage_source(partition)?;
        let Some(revision) = source.capture() else {
            return Ok(None);
        };
        Ok(Some(EngineQueryLineage {
            partition,
            revision,
            source,
            owner: self.clone(),
        }))
    }
}

/// Real per-partition allocation, created with the actual coordinator state.
#[derive(Debug, Default)]
pub(crate) struct Source {
    word: AtomicUsize,
}

/// One Arc allocation: two native atomic reference counts and the packed word.
/// All have usize alignment; no hidden table, vector or callback is allocated.
pub(crate) const fn allocation_bytes() -> usize {
    size_of::<Source>() + 2 * size_of::<AtomicUsize>()
}

impl Source {
    pub(crate) fn capture(&self) -> Option<usize> {
        let word = self.word.load(Ordering::SeqCst);
        (word & (DENIED | ACTIVE_MASK) == 0).then_some(word)
    }

    pub(crate) fn deny(&self) {
        self.word.fetch_or(DENIED, Ordering::SeqCst);
    }

    /// One bounded CAS attempt. Contention/overflow denies observation, never
    /// the actual write/control operation. No transition can clear DENIED.
    pub(crate) fn begin(self: &Arc<Self>) -> Mutation {
        let before = self.word.load(Ordering::SeqCst);
        if before & DENIED == 0
            && (before & ACTIVE_MASK == ACTIVE_MASK
                || before & REVISION_MASK == REVISION_MASK
                || self
                    .word
                    .compare_exchange(
                        before,
                        before + REVISION_STEP + 1,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_err())
        {
            self.deny();
        }
        Mutation {
            source: Arc::clone(self),
            armed: true,
        }
    }

    /// Called once only by the actual settled owner or its retained batch bit.
    pub(crate) fn finish(&self) {
        let before = self.word.load(Ordering::SeqCst);
        if before & DENIED != 0 {
            return;
        }
        if before & ACTIVE_MASK == 0
            || self
                .word
                .compare_exchange(before, before - 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            self.deny();
        }
    }
}

/// Actual entered mutation; dropping an uncertain/unwinding owner denies it.
#[derive(Debug)]
pub(crate) struct Mutation {
    source: Arc<Source>,
    armed: bool,
}

impl Mutation {
    pub(crate) fn settle(mut self) {
        self.source.finish();
        self.armed = false;
    }

    /// The actual BatchRuntime bit takes the active unit, without allocation.
    /// Only its physical complete/definite-failure path may finish that unit.
    pub(crate) fn transfer_to_batch(mut self) {
        self.armed = false;
    }
}

impl Drop for Mutation {
    fn drop(&mut self) {
        if self.armed {
            self.source.deny();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_revision_never_renews_and_overlapping_units_stay_active() {
        let source = Arc::new(Source::default());
        let original = source.capture().expect("initial settled word");
        let first = source.begin();
        let second = source.begin();
        assert!(source.capture().is_none());
        first.settle();
        assert!(source.capture().is_none());
        second.settle();
        let later = source.capture().expect("both real owners settled");
        assert_ne!(original, later);
        assert_eq!(later & ACTIVE_MASK, 0);
    }

    #[test]
    fn abandoned_activity_and_revision_overflow_deny_without_wraparound() {
        let source = Arc::new(Source::default());
        drop(source.begin());
        assert!(source.capture().is_none());
        source.begin().settle();
        assert!(source.capture().is_none());
        let unwinding = Arc::new(Source::default());
        let caught = std::panic::catch_unwind(|| {
            let _actual_activity = unwinding.begin();
            panic!("actual entered activity unwinds");
        });
        assert!(caught.is_err());
        assert!(unwinding.capture().is_none());
        // Encoding boundary only, not a forged engine readiness/authority fact.
        let exhausted = Arc::new(Source {
            word: AtomicUsize::new(REVISION_MASK),
        });
        exhausted.begin().settle();
        assert!(exhausted.capture().is_none());
        assert_ne!(exhausted.word.load(Ordering::SeqCst) & DENIED, 0);
    }
}
