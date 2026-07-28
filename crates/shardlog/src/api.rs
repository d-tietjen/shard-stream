use std::collections::HashMap;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::{
    BatchId, LeaderEpoch, LogicalOffset, LogicalPartitionId, Placement, PlacementSequence,
    Reservation, RingEpoch, ShardId, ShardStore, ShardStoreConfig, StorageError, TopicId,
    TopicPartition,
};

const SHARDLOG_TOPIC_ID: TopicId = TopicId::new(0x5348_4152_444c_4f47_5f56_3100);
const SHARDLOG_PARTITION_ID: LogicalPartitionId = LogicalPartitionId::new(0);
const SHARDLOG_SHARD_ID: ShardId = ShardId::new(0);

/// Result type returned by ShardLog operations.
pub type ShardLogResult<T> = Result<T, StorageError>;

/// Configuration for an embedded [`ShardLog`].
#[derive(Debug, Clone)]
pub struct ShardLogConfig {
    /// Directory exclusively owned by this log while it is open.
    pub directory: PathBuf,
    /// Approximate byte threshold at which the active extent pack is sealed.
    pub target_pack_bytes: u64,
    /// Maximum payload size accepted for one record.
    pub max_record_bytes: usize,
}

impl ShardLogConfig {
    /// Creates a configuration with production-oriented pack and record limits.
    #[must_use]
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
            target_pack_bytes: 256 * 1024 * 1024,
            max_record_bytes: 64 * 1024 * 1024,
        }
    }
}

/// A durably appended contiguous record range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardLogAppendReceipt {
    /// Sequence assigned to the first record.
    pub first_sequence: u64,
    /// Sequence assigned to the final record.
    pub last_sequence: u64,
    /// Number of records in the append.
    pub record_count: usize,
    /// Sum of record payload bytes.
    pub payload_bytes: u64,
}

/// One recovered ShardLog record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardLogRecord {
    /// Monotonic sequence assigned by the caller at append time.
    pub sequence: u64,
    /// Immutable record payload.
    pub payload: Bytes,
}

/// Work completed by one local compaction attempt.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShardLogCompactionReceipt {
    /// Number of sealed pack files removed.
    pub packs_removed: u64,
    /// Number of indexed record extents removed.
    pub records_removed: u64,
    /// Total physical bytes removed.
    pub bytes_removed: u64,
}

/// Embedded crash-recoverable append log.
///
/// Each directory has one exclusive owner. Records are checksummed on disk;
/// recovery repairs an incomplete active tail and fails closed if committed
/// history is corrupt.
#[derive(Debug)]
pub struct ShardLog {
    inner: ShardStore,
}

impl ShardLog {
    /// Opens or creates a log and validates all retained extent packs.
    pub fn open(config: ShardLogConfig) -> ShardLogResult<Self> {
        let inner = ShardStore::open(ShardStoreConfig {
            directory: config.directory,
            shard_id: SHARDLOG_SHARD_ID,
            target_pack_bytes: config.target_pack_bytes,
            max_batch_bytes: config.max_record_bytes,
        })?;
        let log = Self { inner };
        log.bounds()?;
        Ok(log)
    }

    /// Returns the retained half-open sequence range.
    ///
    /// `None` means the log has no retained records. A non-empty log returns
    /// `(first_sequence, next_sequence)`.
    pub fn bounds(&self) -> ShardLogResult<Option<(u64, u64)>> {
        let mut sequences = self
            .inner
            .catalog()
            .into_iter()
            .map(|entry| validate_catalog_entry(entry.reservation))
            .collect::<ShardLogResult<Vec<_>>>()?;
        if sequences.is_empty() {
            return Ok(None);
        }
        sequences.sort_unstable();
        for pair in sequences.windows(2) {
            if pair[1] != pair[0].checked_add(1).ok_or_else(sequence_overflow)? {
                return Err(StorageError::InvalidInput(
                    "ShardLog retained sequence contains a gap".to_string(),
                ));
            }
        }
        let first = sequences[0];
        let next = sequences[sequences.len() - 1]
            .checked_add(1)
            .ok_or_else(sequence_overflow)?;
        Ok(Some((first, next)))
    }

    /// Appends a contiguous record group.
    ///
    /// The caller supplies the first sequence. When `durable` is true, success
    /// is returned only after the active pack is synchronized.
    pub fn append_group(
        &mut self,
        first_sequence: u64,
        records: &[Bytes],
        durable: bool,
    ) -> ShardLogResult<ShardLogAppendReceipt> {
        if records.is_empty() {
            return Err(StorageError::InvalidInput(
                "ShardLog append group cannot be empty".to_string(),
            ));
        }
        let last_sequence = first_sequence
            .checked_add(u64::try_from(records.len() - 1).map_err(|_| sequence_overflow())?)
            .ok_or_else(sequence_overflow)?;
        let mut payload_bytes = 0u64;
        let mut prepared = Vec::with_capacity(records.len());
        for (index, payload) in records.iter().enumerate() {
            let sequence = first_sequence
                .checked_add(u64::try_from(index).map_err(|_| sequence_overflow())?)
                .ok_or_else(sequence_overflow)?;
            payload_bytes = payload_bytes
                .checked_add(u64::try_from(payload.len()).map_err(|_| sequence_overflow())?)
                .ok_or_else(sequence_overflow)?;
            prepared.push(self.inner.prepare_append(
                reservation(sequence),
                None,
                payload.clone(),
            )?);
        }
        self.inner
            .append_prepared_group(&prepared.iter().collect::<Vec<_>>(), durable)?;
        Ok(ShardLogAppendReceipt {
            first_sequence,
            last_sequence,
            record_count: records.len(),
            payload_bytes,
        })
    }

    /// Reads retained records at or after `start_sequence` in sequence order.
    pub fn read_from(
        &mut self,
        start_sequence: u64,
        max_bytes: usize,
        max_records: usize,
    ) -> ShardLogResult<Vec<ShardLogRecord>> {
        let plan = self.inner.plan_fetch(
            topic_partition(),
            LogicalOffset::new(start_sequence),
            max_bytes,
            max_records,
        )?;
        self.inner
            .read_fetch_plan(&plan)?
            .into_iter()
            .map(|batch| {
                let sequence = validate_catalog_entry(batch.reservation)?;
                Ok(ShardLogRecord {
                    sequence,
                    payload: batch.payload,
                })
            })
            .collect()
    }

    /// Synchronizes all previously appended records.
    pub fn sync(&mut self) -> ShardLogResult<()> {
        self.inner.sync()
    }

    /// Seals the active pack when it contains records.
    pub fn seal(&mut self) -> ShardLogResult<()> {
        self.inner.seal_active().map(|_| ())
    }

    /// Seals the active pack and removes sealed packs wholly before `sequence`.
    ///
    /// Deletion is crash-safe. A failed cleanup is retried during a later
    /// open and does not invalidate committed history.
    pub fn compact_before(&mut self, sequence: u64) -> ShardLogResult<ShardLogCompactionReceipt> {
        self.inner.seal_active()?;
        let stats = self.inner.garbage_collect(&HashMap::from([(
            topic_partition(),
            LogicalOffset::new(sequence),
        )]))?;
        Ok(ShardLogCompactionReceipt {
            packs_removed: stats.packs_removed,
            records_removed: stats.extents_removed,
            bytes_removed: stats.bytes_removed,
        })
    }

    /// Returns the current active pack path for operational inspection.
    #[must_use]
    pub fn active_path(&self) -> &Path {
        self.inner.active_path()
    }
}

fn reservation(sequence: u64) -> Reservation {
    Reservation {
        topic_id: SHARDLOG_TOPIC_ID,
        partition_id: SHARDLOG_PARTITION_ID,
        batch_id: BatchId::new(u128::from(sequence)),
        first_offset: LogicalOffset::new(sequence),
        last_offset: LogicalOffset::new(sequence),
        record_count: NonZeroU32::new(1).expect("one is nonzero"),
        placement: Placement {
            virtual_lane_id: SHARDLOG_SHARD_ID,
            ring_epoch: RingEpoch::new(1),
            leader_epoch: LeaderEpoch::new(0),
            sequence: PlacementSequence::new(sequence),
        },
    }
}

fn topic_partition() -> TopicPartition {
    TopicPartition::new(SHARDLOG_TOPIC_ID, SHARDLOG_PARTITION_ID)
}

fn validate_catalog_entry(reservation: Reservation) -> ShardLogResult<u64> {
    if reservation.topic_id != SHARDLOG_TOPIC_ID
        || reservation.partition_id != SHARDLOG_PARTITION_ID
        || reservation.record_count.get() != 1
        || reservation.first_offset != reservation.last_offset
    {
        return Err(StorageError::InvalidInput(
            "ShardLog directory contains records outside its canonical stream".to_string(),
        ));
    }
    Ok(reservation.first_offset.get())
}

fn sequence_overflow() -> StorageError {
    StorageError::LimitExceeded("ShardLog sequence or byte count overflow".to_string())
}
