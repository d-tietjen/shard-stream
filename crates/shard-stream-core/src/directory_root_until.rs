//! Bounded retained-root borrowing through the original native allocation.

use super::DirectoryOwnership;
use crate::{NativeDirectoryBudget, OwnedDirectoryHandle, RetainedDirectoryError};
use std::sync::Arc;

impl DirectoryOwnership {
    /// Borrow the SAME actual native root with finite original-E validation.
    ///
    /// Every nested native phase uses this budget's unchanged original E. A
    /// late/error outcome retains the original owner and any real cloned FD;
    /// no arbitrary file/path constructor or new root authority is exposed.
    pub fn root_handle_until(
        self: &Arc<Self>,
        budget: &NativeDirectoryBudget,
    ) -> Result<OwnedDirectoryHandle, RetainedDirectoryError> {
        crate::owned_directory_handle::operation_until::root_until(
            self,
            &self.canonical_directory,
            budget,
            || self._directory.try_clone(),
        )
    }
}
