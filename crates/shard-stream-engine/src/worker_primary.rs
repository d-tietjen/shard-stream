//! Keep the actual ordinary or whole Core store through lane and sync work.
//!
//! Only the owned variant has a descriptor-pinned directory. Borrowing its
//! ordinary native methods does not qualify those methods as bounded producers.

use super::*;
#[cfg(any(target_os = "linux", target_os = "android"))]
use shard_stream_core::{OwnedPinnedShardStore, OwnedShardStoreDirectory};

#[allow(
    clippy::large_enum_variant,
    reason = "retain the actual whole Core store inline under its existing owner and prepayment"
)]
pub(crate) enum PrimaryStore {
    Ordinary(ShardStore),
    #[cfg(any(target_os = "linux", target_os = "android"))]
    Owned(OwnedPinnedShardStore),
    // A consuming GC refusal owns the actual store in its typed result. Keep
    // this lane closed rather than rebuilding a store or reopening its path.
    Vacant,
}
impl PrimaryStore {
    pub(crate) fn native(&self) -> Option<&ShardStore> {
        match self {
            Self::Ordinary(store) => Some(store),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Self::Owned(store) => Some(store.store()),
            Self::Vacant => None,
        }
    }
    pub(crate) fn native_mut(&mut self) -> Option<&mut ShardStore> {
        match self {
            Self::Ordinary(store) => Some(store),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Self::Owned(store) => Some(store.store_mut()),
            Self::Vacant => None,
        }
    }
    pub(crate) fn is_owned(&self) -> bool {
        match self {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Self::Owned(_) => true,
            _ => false,
        }
    }
    pub(crate) fn retain_directory(&self) -> DirectoryRetention {
        DirectoryRetention {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            _directory: match self {
                Self::Owned(store) => Some(store.retain_directory()),
                _ => None,
            },
        }
    }
}

/// A physical loop keeps this outside all request/file/result stack frames.
/// The actual Core Arc is cloned, never reconstructed from a path or root.
pub(crate) struct DirectoryRetention {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    _directory: Option<OwnedShardStoreDirectory>,
}

pub(super) fn unavailable() -> StorageError {
    StorageError::Io(io::Error::new(
        io::ErrorKind::BrokenPipe,
        "consuming native GC retained this shard store in its refusal",
    ))
}
