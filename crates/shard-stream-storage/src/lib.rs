//! Durable stream extent packs and coordinator metadata journal.

mod codec;
mod error;
mod extent;
mod journal;

pub use error::{StorageError, StorageResult};
pub use extent::{ExtentCatalogEntry, ShardStore, ShardStoreConfig, StoredBatch};
pub use journal::{
    CoordinatorJournal, JournalEvent, ProducerSequence, RecoveredJournal, summarize_events,
};
