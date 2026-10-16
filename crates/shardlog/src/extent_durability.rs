//! Actual active-file durability operations under one saved native budget.

use super::*;
use crate::{StorageDurabilityBudget, StorageDurabilityFailure, StorageDurabilityOperation};

impl ShardStore {
    /// Flushes and synchronizes the same actual active file under the original E.
    ///
    /// Each native call consumes a prepaid slot and saves its original typed
    /// result before the post-call clock. The caller keeps this same store and
    /// budget on every refusal; synchronous calls physically settle before return.
    pub fn sync_until(
        &mut self,
        budget: &mut StorageDurabilityBudget,
    ) -> Result<(), StorageDurabilityFailure> {
        self.sync_until_with_observer(budget, &mut |_| {})
    }

    pub(crate) fn sync_until_with_observer(
        &mut self,
        budget: &mut StorageDurabilityBudget,
        completed: &mut impl FnMut(StorageDurabilityOperation),
    ) -> Result<(), StorageDurabilityFailure> {
        budget.execute(
            StorageDurabilityOperation::Flush,
            || self.active.file.flush(),
            completed,
        )?;
        budget.execute(
            StorageDurabilityOperation::SyncData,
            || self.active.file.sync_data(),
            completed,
        )
    }
}
