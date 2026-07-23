//! Durable stream extent packs and coordinator metadata journal.

mod codec;
mod error;
mod extent;
mod journal;
mod tier;

pub use error::{StorageError, StorageResult};
pub use extent::{
    ExtentCatalogEntry, GarbageCollectionStats, SealedPack, ShardStore, ShardStoreConfig,
    StoredBatch,
};
pub use journal::{
    CoordinatorJournal, DIGEST_BLAKE3, JournalEvent, ProducerSequence, RecoveredJournal,
};
pub use tier::{LocalObjectTier, TierManifest, TierObject};
