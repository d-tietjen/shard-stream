//! Public native log durability entry retaining the actual inner store.

use super::*;
use crate::{StorageDurabilityBudget, StorageDurabilityFailure};

impl ShardLog {
    /// Flushes and synchronizes the actual native file under the saved budget E.
    ///
    /// This borrows the same existing log; it does not open a file, replace an
    /// owner or reset counters. Late/native failures keep their original typed
    /// outcomes in `budget`. The composing owner retains both log and budget.
    /// This does not qualify opening, append, rotation, reading or compaction.
    pub fn sync_until(
        &mut self,
        budget: &mut StorageDurabilityBudget,
    ) -> Result<(), StorageDurabilityFailure> {
        self.inner.sync_until(budget)
    }
}
