use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{IoSlice, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::time::{Duration, Instant};

use crate::{
    BatchId, LeaderEpoch, LogicalOffset, LogicalPartitionId, Placement, PlacementSequence,
    RecordId, Reservation, RingEpoch, ShardId, TopicId, TopicPartition, VirtualLaneId,
};
use bytes::Bytes;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

use crate::codec::{Decoder, push_u32, push_u64, push_u128};
use crate::journal::{AtomicGroupSequence, DIGEST_XXH3_128, ProducerSequence};
use crate::{StorageError, StorageResult};

#[path = "extent_durability.rs"]
mod durability;
#[cfg(test)]
#[path = "extent_durability_tests.rs"]
mod durability_tests;
#[path = "extent_fetch.rs"]
mod fetch;
#[cfg(unix)]
#[path = "pinned_directory.rs"]
mod pinned_directory;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use pinned_directory::{
    OpenedPinnedShardStore, PinnedStoreDirectoryEntry, PinnedStoreOpenChecks, PinnedStoreOpenCost,
    PinnedStoreOpenError, PinnedStoreOpenFailure, PinnedStoreOpenLimits, PinnedStoreOpenOutcome,
    PinnedStoreOpenPhase, PinnedStoreOpenUsage, pinned_store_open_cost,
};
#[cfg(unix)]
pub use pinned_directory::{
    PinnedShardDirectory, PinnedShardDirectoryCost, PinnedShardDirectoryError,
    PinnedShardDirectoryFailure, PinnedShardDirectoryLimits, PinnedShardDirectoryObservation,
    ShardDirectoryCheckFailure, ShardDirectoryChecks, pinned_shard_directory_cost,
};
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use pinned_directory::{
    PinnedShardGcChecks, PinnedShardGcCost, PinnedShardGcError, PinnedShardGcFailure,
    PinnedShardGcLimits, PinnedShardGcPack, PinnedShardGcPhase, PinnedShardGcReport,
    PinnedShardGcUsage, pinned_shard_gc_cost,
};
#[cfg(unix)]
#[path = "guard_acquire.rs"]
mod guard_acquire;
#[cfg(unix)]
pub use guard_acquire::{
    AcquiredShardRecoveryGuard, RetainedShardGuardAcquireError, ShardGuardAcquireCost,
    ShardGuardAcquireFailure, ShardGuardAcquireLimits, ShardGuardAcquireObservation,
    shard_guard_acquire_cost,
};
#[cfg(unix)]
#[path = "guard_validation.rs"]
mod guard_validation;
#[cfg(unix)]
pub use guard_validation::{
    ShardGuardValidation, ShardGuardValidationCost, ShardGuardValidationError,
    ShardGuardValidationFailure, ShardGuardValidationLimits, shard_guard_validation_cost,
};

const EXTENT_MAGIC: &[u8; 4] = b"SSE8";
const EXTENT_VERSION: u8 = 8;
const EXTENT_FLAGS: u8 = 0;
const PREFIX_LEN: usize = 4 + 1 + 1 + 2 + 8;
const PRODUCER_FIXED_LEN: usize = 1 + 3 + 16 + 4 + 8 + 1 + 3 + 16 + 1 + 3 + 16;
const GROUP_FIXED_LEN: usize = 1 + 3 + 16 + 8 + 16;
const BODY_FIXED_LEN: usize =
    16 + 4 + 16 + 8 + 8 + 4 + 4 + 8 + 8 + 8 + PRODUCER_FIXED_LEN + GROUP_FIXED_LEN + 8;
const HEADER_LEN: usize = PREFIX_LEN + BODY_FIXED_LEN;
const CHECKSUM_LEN: usize = 8;
const MAX_COALESCED_READ_GAP: u64 = 4 * 1024;
const FETCH_PLAN_INITIAL_CAPACITY: usize = 64;
const MAX_EXTENTS_PER_WRITE: usize = 128;
const READ_BUFFER_POOL_SLOTS: usize = 8;
const MAX_CACHED_READ_BUFFER_BYTES: usize = 8 * 1024 * 1024;

pub(crate) const fn recovery_header_bytes() -> usize {
    HEADER_LEN
}

pub(crate) const fn recovery_frame_minimum() -> usize {
    HEADER_LEN + CHECKSUM_LEN
}

pub(crate) fn recovery_frame_ceiling(max_batch_bytes: usize) -> StorageResult<usize> {
    recovery_frame_minimum()
        .checked_add(max_batch_bytes)
        .ok_or_else(|| StorageError::LimitExceeded("extent frame ceiling overflow".into()))
}
static NEXT_STORE_TOKEN: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct ShardStoreConfig {
    pub directory: PathBuf,
    pub shard_id: ShardId,
    pub target_pack_bytes: u64,
    pub max_batch_bytes: usize,
}

impl ShardStoreConfig {
    pub fn validate(&self) -> StorageResult<()> {
        if self.target_pack_bytes == 0 {
            return Err(StorageError::InvalidInput(
                "target_pack_bytes must be nonzero".into(),
            ));
        }
        if self.max_batch_bytes == 0 {
            return Err(StorageError::InvalidInput(
                "max_batch_bytes must be nonzero".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBatch {
    pub reservation: Reservation,
    pub producer: Option<ProducerSequence>,
    pub atomic_group: Option<AtomicGroupSequence>,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredBatchMetadata {
    reservation: Reservation,
    producer: Option<ProducerSequence>,
    atomic_group: Option<AtomicGroupSequence>,
    pack_sequence: u64,
    payload_offset: u64,
    payload_len: usize,
    store_token: u64,
    plan_epoch: u64,
}

impl StoredBatchMetadata {
    #[must_use]
    pub const fn reservation(self) -> Reservation {
        self.reservation
    }

    #[must_use]
    pub const fn payload_len(self) -> usize {
        self.payload_len
    }
}

#[derive(Debug)]
pub struct PreparedExtent {
    reservation: Reservation,
    producer: Option<ProducerSequence>,
    atomic_group: Option<AtomicGroupSequence>,
    payload: Bytes,
    encoded: EncodedExtent,
}

impl PreparedExtent {
    pub fn reservation(&self) -> Reservation {
        self.reservation
    }

    #[must_use]
    pub fn atomic_group(&self) -> Option<AtomicGroupSequence> {
        self.atomic_group
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtentCatalogEntry {
    pub reservation: Reservation,
    pub producer: Option<ProducerSequence>,
    pub atomic_group: Option<AtomicGroupSequence>,
    pub payload_bytes: u64,
    pub pack_sequence: u64,
    pub object_durable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedPack {
    pub sequence: u64,
    pub path: PathBuf,
    pub bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GarbageCollectionStats {
    pub packs_removed: u64,
    pub extents_removed: u64,
    pub bytes_removed: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ExtentLocation {
    pub(crate) reservation: Reservation,
    pub(crate) producer: Option<ProducerSequence>,
    pub(crate) atomic_group: Option<AtomicGroupSequence>,
    pack_sequence: u64,
    payload_offset: u64,
    payload_len: usize,
}

#[derive(Debug)]
struct ActivePack {
    sequence: u64,
    file: File,
    bytes: u64,
    opened_at: Instant,
}

#[derive(Debug)]
struct ReadBufferOwner {
    buffer: Option<Vec<u8>>,
    recycle: SyncSender<Vec<u8>>,
    cached_buffers: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for ReadBufferOwner {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_deref().expect("read buffer owner is live")
    }
}

impl Drop for ReadBufferOwner {
    fn drop(&mut self) {
        let Some(buffer) = self.buffer.take() else {
            return;
        };
        if buffer.capacity() <= MAX_CACHED_READ_BUFFER_BYTES
            && claim_read_buffer_slot(&self.cached_buffers)
            && self.recycle.try_send(buffer).is_err()
        {
            self.cached_buffers.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[derive(Debug)]
struct ReadBufferPool {
    available: Vec<Vec<u8>>,
    recycle: SyncSender<Vec<u8>>,
    recycled: Receiver<Vec<u8>>,
    cached_buffers: Arc<AtomicUsize>,
}

impl ReadBufferPool {
    fn new() -> Self {
        let (recycle, recycled) = sync_channel(READ_BUFFER_POOL_SLOTS);
        let cached_buffers = Arc::new(AtomicUsize::new(0));
        Self {
            available: Vec::with_capacity(READ_BUFFER_POOL_SLOTS),
            recycle,
            recycled,
            cached_buffers,
        }
    }

    fn take(&mut self, len: usize) -> Vec<u8> {
        self.drain_recycled();
        let selected = self
            .available
            .iter()
            .enumerate()
            .filter(|(_, buffer)| buffer.capacity() >= len)
            .min_by_key(|(_, buffer)| buffer.capacity())
            .map(|(index, _)| index)
            .or_else(|| {
                self.available
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, buffer)| buffer.capacity())
                    .map(|(index, _)| index)
            });
        let mut buffer = if let Some(index) = selected {
            let previous = self.cached_buffers.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(previous > 0, "read buffer cache slot underflow");
            self.available.swap_remove(index)
        } else {
            Vec::new()
        };
        buffer.resize(len, 0);
        buffer
    }

    fn recycle_now(&mut self, buffer: Vec<u8>) {
        if buffer.capacity() <= MAX_CACHED_READ_BUFFER_BYTES
            && claim_read_buffer_slot(&self.cached_buffers)
        {
            self.available.push(buffer);
        }
    }

    fn lease_bytes(&self, buffer: Vec<u8>) -> Bytes {
        Bytes::from_owner(ReadBufferOwner {
            buffer: Some(buffer),
            recycle: self.recycle.clone(),
            cached_buffers: Arc::clone(&self.cached_buffers),
        })
    }

    fn drain_recycled(&mut self) {
        while self.available.len() < READ_BUFFER_POOL_SLOTS {
            let Ok(buffer) = self.recycled.try_recv() else {
                break;
            };
            self.available.push(buffer);
        }
    }
}

fn claim_read_buffer_slot(cached_buffers: &AtomicUsize) -> bool {
    cached_buffers
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cached| {
            (cached < READ_BUFFER_POOL_SLOTS).then_some(cached + 1)
        })
        .is_ok()
}

/// Concrete native shard lease retaining the exact pre-open lock inode.
#[cfg(unix)]
pub struct ShardRecoveryGuard {
    file: File,
    directory: PathBuf,
}

#[cfg(unix)]
impl ShardRecoveryGuard {
    /// Nonblocking acquisition; no new timeout or skipped native lock.
    pub fn try_acquire(file: File, directory: PathBuf) -> StorageResult<Self> {
        let result = Self { file, directory };
        result.validate(&result.directory)?;
        fs2::FileExt::try_lock_exclusive(&result.file)?;
        result.validate(&result.directory)?;
        Ok(result)
    }

    /// Checks the same concrete lease against the native directory/lock path.
    pub fn validate(&self, directory: &Path) -> StorageResult<()> {
        let named = fs::symlink_metadata(directory.join(".shard-store.lock"))?;
        let actual = self.file.metadata()?;
        if guard_validation::identity_changed(self, directory, &named, &actual) {
            return Err(StorageError::InvalidInput(
                "shard recovery lease identity changed".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ShardStore {
    config: ShardStoreConfig,
    _lock_file: File,
    store_token: u64,
    plan_epoch: u64,
    active: ActivePack,
    index: HashMap<TopicPartition, Vec<ExtentLocation>>,
    by_batch: HashMap<(TopicPartition, BatchId), ExtentLocation>,
    pack_paths: BTreeMap<u64, PathBuf>,
    read_buffer_pool: ReadBufferPool,
}

impl ShardStore {
    /// Fixed logical recovery-open slots, including both native directory scans
    /// and the actual empty read-buffer queue. This is not allocator/RSS usage.
    pub const fn recovery_open_fixed_bytes() -> usize {
        size_of::<Self>()
            + size_of::<ReadBufferPool>()
            + READ_BUFFER_POOL_SLOTS * size_of::<Vec<u8>>()
            + 2 * 65_536
    }

    pub fn open(config: ShardStoreConfig) -> StorageResult<Self> {
        Self::open_inner(config, None)
    }

    /// Continues with the SAME native lock held during a pre-open census.
    #[cfg(unix)]
    pub fn open_after_census(
        config: ShardStoreConfig,
        guard: ShardRecoveryGuard,
    ) -> StorageResult<Self> {
        guard.validate(&config.directory)?;
        Self::open_inner(config, Some(guard.file))
    }

    /// Continues the same native lease while the recovery owner retains it.
    ///
    /// Cloning this descriptor preserves the same open-file-description lock;
    /// it neither opens the pathname nor acquires a second lease. The recovery
    /// owner retains its original guard even if native repair refuses.
    #[cfg(unix)]
    pub fn open_under_census(
        config: ShardStoreConfig,
        guard: &ShardRecoveryGuard,
    ) -> StorageResult<Self> {
        guard.validate(&config.directory)?;
        Self::open_inner(config, Some(guard.file.try_clone()?))
    }

    fn open_inner(config: ShardStoreConfig, held_lock: Option<File>) -> StorageResult<Self> {
        config.validate()?;
        let directory_existed = config.directory.exists();
        fs::create_dir_all(&config.directory)?;
        if !directory_existed && let Some(parent) = config.directory.parent() {
            sync_directory(parent)?;
        }
        let lock_path = config.directory.join(".shard-store.lock");
        let lock_existed = lock_path.exists();
        let lock_file = match held_lock {
            Some(file) => file,
            None => {
                let file = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(lock_path)?;
                fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
                    StorageError::InvalidInput(format!(
                        "shard store directory is already open: {error}"
                    ))
                })?;
                file
            }
        };
        if !lock_existed {
            lock_file.sync_all()?;
            sync_directory(&config.directory)?;
        }
        cleanup_pending_pack_deletions(&config.directory)?;
        let mut pack_paths = discover_packs(&config.directory)?;
        if pack_paths.is_empty() {
            let path = pack_path(&config.directory, 0);
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)?;
            pack_paths.insert(0, path);
            sync_directory(&config.directory)?;
        }

        let last_sequence = *pack_paths.last_key_value().expect("pack map is nonempty").0;
        let mut index: HashMap<TopicPartition, Vec<ExtentLocation>> = HashMap::new();
        let mut by_batch = HashMap::new();
        for (&sequence, path) in &pack_paths {
            let permit_tail_repair = sequence == last_sequence;
            scan_pack(
                path,
                sequence,
                config.shard_id,
                config.max_batch_bytes,
                permit_tail_repair,
                &mut index,
                &mut by_batch,
            )?;
        }
        for locations in index.values_mut() {
            locations.sort_by_key(|location| location.reservation.first_offset);
        }

        let active_path = pack_paths
            .get(&last_sequence)
            .expect("last pack exists")
            .clone();
        let active_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&active_path)?;
        let active_bytes = active_file.metadata()?.len();
        let store_token = NEXT_STORE_TOKEN
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |token| {
                token.checked_add(1)
            })
            .map_err(|_| StorageError::LimitExceeded("store token space exhausted".into()))?;
        Ok(Self {
            config,
            _lock_file: lock_file,
            store_token,
            plan_epoch: 0,
            active: ActivePack {
                sequence: last_sequence,
                file: active_file,
                bytes: active_bytes,
                opened_at: Instant::now(),
            },
            index,
            by_batch,
            pack_paths,
            read_buffer_pool: ReadBufferPool::new(),
        })
    }

    /// Native primary opening under an already-admitted absolute recovery expiry.
    /// No clock, ownership or serving permission is created by this storage API.
    #[cfg(unix)]
    pub fn open_until(config: ShardStoreConfig, expiry: Instant) -> StorageResult<Self> {
        Self::open_inner_until(config, None, expiry)
    }

    /// Continues the SAME native lease, checking E across pack scan/repair phases.
    #[cfg(unix)]
    pub fn open_under_census_until(
        config: ShardStoreConfig,
        guard: &ShardRecoveryGuard,
        expiry: Instant,
    ) -> StorageResult<Self> {
        recovery_gc_clock(expiry)?;
        guard.validate(&config.directory)?;
        recovery_gc_clock(expiry)?;
        Self::open_inner_until(config, Some(guard.file.try_clone()?), expiry)
    }

    #[cfg(unix)]
    fn open_inner_until(
        config: ShardStoreConfig,
        held_lock: Option<File>,
        expiry: Instant,
    ) -> StorageResult<Self> {
        recovery_gc_clock(expiry)?;
        config.validate()?;
        recovery_gc_clock(expiry)?;
        let directory_existed = config.directory.exists();
        recovery_gc_clock(expiry)?;
        fs::create_dir_all(&config.directory)?;
        if !directory_existed && let Some(parent) = config.directory.parent() {
            recovery_gc_clock(expiry)?;
            sync_directory(parent)?;
        }
        recovery_gc_clock(expiry)?;
        let lock_path = config.directory.join(".shard-store.lock");
        let lock_existed = lock_path.exists();
        recovery_gc_clock(expiry)?;
        let lock_file = match held_lock {
            Some(file) => file,
            None => {
                recovery_gc_clock(expiry)?;
                let file = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(lock_path)?;
                recovery_gc_clock(expiry)?;
                fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
                    StorageError::InvalidInput(format!(
                        "shard store directory is already open: {error}"
                    ))
                })?;
                file
            }
        };
        if !lock_existed {
            recovery_gc_clock(expiry)?;
            lock_file.sync_all()?;
            recovery_gc_clock(expiry)?;
            sync_directory(&config.directory)?;
        }
        cleanup_pending_pack_deletions_until(&config.directory, expiry)?;
        let mut pack_paths = discover_packs_until(&config.directory, expiry)?;
        if pack_paths.is_empty() {
            let path = pack_path(&config.directory, 0);
            recovery_gc_clock(expiry)?;
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)?;
            pack_paths.insert(0, path);
            recovery_gc_clock(expiry)?;
            sync_directory(&config.directory)?;
        }

        let last_sequence = *pack_paths.last_key_value().expect("pack map is nonempty").0;
        let mut index: HashMap<TopicPartition, Vec<ExtentLocation>> = HashMap::new();
        let mut by_batch = HashMap::new();
        for (&sequence, path) in &pack_paths {
            recovery_gc_clock(expiry)?;
            let permit_tail_repair = sequence == last_sequence;
            scan_pack_until(
                path,
                sequence,
                config.shard_id,
                config.max_batch_bytes,
                permit_tail_repair,
                &mut index,
                &mut by_batch,
                expiry,
            )?;
        }
        for locations in index.values_mut() {
            recovery_gc_clock(expiry)?;
            locations.sort_by_key(|location| location.reservation.first_offset);
        }

        recovery_gc_clock(expiry)?;
        let active_path = pack_paths
            .get(&last_sequence)
            .expect("last pack exists")
            .clone();
        recovery_gc_clock(expiry)?;
        let active_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&active_path)?;
        recovery_gc_clock(expiry)?;
        let active_bytes = active_file.metadata()?.len();
        recovery_gc_clock(expiry)?;
        let store_token = NEXT_STORE_TOKEN
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |token| {
                token.checked_add(1)
            })
            .map_err(|_| StorageError::LimitExceeded("store token space exhausted".into()))?;
        recovery_gc_clock(expiry)?;
        let result = Self {
            config,
            _lock_file: lock_file,
            store_token,
            plan_epoch: 0,
            active: ActivePack {
                sequence: last_sequence,
                file: active_file,
                bytes: active_bytes,
                opened_at: Instant::now(),
            },
            index,
            by_batch,
            pack_paths,
            read_buffer_pool: ReadBufferPool::new(),
        };
        recovery_gc_clock(expiry)?;
        Ok(result)
    }

    pub fn append(
        &mut self,
        reservation: Reservation,
        producer: Option<ProducerSequence>,
        payload: &[u8],
        sync: bool,
    ) -> StorageResult<()> {
        self.append_atomic(reservation, producer, None, payload, sync)
    }

    pub fn append_atomic(
        &mut self,
        reservation: Reservation,
        producer: Option<ProducerSequence>,
        atomic_group: Option<AtomicGroupSequence>,
        payload: &[u8],
        sync: bool,
    ) -> StorageResult<()> {
        self.validate_append(reservation, payload)?;
        let encoded = encode_extent(reservation, producer, atomic_group, payload)?;
        self.append_encoded(reservation, producer, atomic_group, payload, &encoded, sync)
    }

    pub fn append_and_prepare(
        &mut self,
        reservation: Reservation,
        producer: Option<ProducerSequence>,
        payload: Bytes,
        sync: bool,
    ) -> StorageResult<PreparedExtent> {
        self.append_and_prepare_atomic(reservation, producer, None, payload, sync)
    }

    pub fn append_and_prepare_atomic(
        &mut self,
        reservation: Reservation,
        producer: Option<ProducerSequence>,
        atomic_group: Option<AtomicGroupSequence>,
        payload: Bytes,
        sync: bool,
    ) -> StorageResult<PreparedExtent> {
        let prepared = self.prepare_append_atomic(reservation, producer, atomic_group, payload)?;
        self.append_prepared(&prepared, sync)?;
        Ok(prepared)
    }

    pub fn prepare_append(
        &self,
        reservation: Reservation,
        producer: Option<ProducerSequence>,
        payload: Bytes,
    ) -> StorageResult<PreparedExtent> {
        self.prepare_append_atomic(reservation, producer, None, payload)
    }

    pub fn prepare_append_atomic(
        &self,
        reservation: Reservation,
        producer: Option<ProducerSequence>,
        atomic_group: Option<AtomicGroupSequence>,
        payload: Bytes,
    ) -> StorageResult<PreparedExtent> {
        self.validate_append(reservation, &payload)?;
        let encoded = encode_extent(reservation, producer, atomic_group, &payload)?;
        Ok(PreparedExtent {
            reservation,
            producer,
            atomic_group,
            payload,
            encoded,
        })
    }

    pub fn append_prepared(&mut self, prepared: &PreparedExtent, sync: bool) -> StorageResult<()> {
        self.append_prepared_group(&[prepared], sync)
    }

    pub fn append_prepared_group(
        &mut self,
        prepared: &[&PreparedExtent],
        sync: bool,
    ) -> StorageResult<()> {
        let mut group_keys = HashSet::with_capacity(prepared.len());
        for extent in prepared {
            self.validate_append(extent.reservation, &extent.payload)?;
            let key = (
                TopicPartition::new(extent.reservation.topic_id, extent.reservation.partition_id),
                extent.reservation.batch_id,
            );
            if !group_keys.insert(key) {
                return Err(StorageError::InvalidInput(format!(
                    "batch {} appears more than once in an append group",
                    extent.reservation.batch_id
                )));
            }
        }

        let mut first = 0usize;
        while first < prepared.len() {
            let first_frame_len = u64::try_from(prepared[first].encoded.frame_len)
                .map_err(|_| StorageError::LimitExceeded("extent frame exceeds u64".into()))?;
            if self.active.bytes > 0
                && self
                    .active
                    .bytes
                    .checked_add(first_frame_len)
                    .is_none_or(|next| next > self.config.target_pack_bytes)
            {
                self.rotate()?;
            }

            let mut end = first;
            let mut pack_bytes = self.active.bytes;
            while end < prepared.len() && end - first < MAX_EXTENTS_PER_WRITE {
                let frame_len = u64::try_from(prepared[end].encoded.frame_len)
                    .map_err(|_| StorageError::LimitExceeded("extent frame exceeds u64".into()))?;
                let next = pack_bytes
                    .checked_add(frame_len)
                    .ok_or_else(|| StorageError::LimitExceeded("pack length overflow".into()))?;
                if pack_bytes > 0 && next > self.config.target_pack_bytes {
                    break;
                }
                pack_bytes = next;
                end += 1;
            }
            debug_assert!(end > first);
            self.append_prepared_segment(&prepared[first..end])?;
            first = end;
        }
        if sync && !prepared.is_empty() {
            self.sync()?;
        }
        Ok(())
    }

    fn validate_append(&self, reservation: Reservation, payload: &[u8]) -> StorageResult<()> {
        if payload.len() > self.config.max_batch_bytes {
            return Err(StorageError::LimitExceeded(format!(
                "batch has {} bytes, maximum is {}",
                payload.len(),
                self.config.max_batch_bytes
            )));
        }
        let key = (
            TopicPartition::new(reservation.topic_id, reservation.partition_id),
            reservation.batch_id,
        );
        if self.by_batch.contains_key(&key) {
            return Err(StorageError::InvalidInput(format!(
                "batch {} already exists",
                reservation.batch_id
            )));
        }
        Ok(())
    }

    fn append_encoded(
        &mut self,
        reservation: Reservation,
        producer: Option<ProducerSequence>,
        atomic_group: Option<AtomicGroupSequence>,
        payload: &[u8],
        encoded: &EncodedExtent,
        sync: bool,
    ) -> StorageResult<()> {
        let key = (
            TopicPartition::new(reservation.topic_id, reservation.partition_id),
            reservation.batch_id,
        );
        if self.active.bytes > 0
            && self
                .active
                .bytes
                .checked_add(encoded.frame_len as u64)
                .is_none_or(|next| next > self.config.target_pack_bytes)
        {
            self.rotate()?;
        }

        let frame_offset = self.active.bytes;
        self.active.file.seek(SeekFrom::Start(frame_offset))?;
        if let Err(error) = write_extent_frame(
            &mut self.active.file,
            &encoded.header,
            payload,
            &encoded.checksum,
        ) {
            let _ = self.active.file.set_len(frame_offset);
            return Err(error.into());
        }
        self.active.file.flush()?;
        if sync {
            self.active.file.sync_data()?;
        }
        self.active.bytes = self
            .active
            .bytes
            .checked_add(encoded.frame_len as u64)
            .ok_or_else(|| StorageError::LimitExceeded("pack length overflow".into()))?;

        let location = ExtentLocation {
            reservation,
            producer,
            atomic_group,
            pack_sequence: self.active.sequence,
            payload_offset: frame_offset + HEADER_LEN as u64,
            payload_len: payload.len(),
        };
        let locations = self.index.entry(key.0).or_default();
        if locations
            .last()
            .is_none_or(|last| last.reservation.first_offset < reservation.first_offset)
        {
            locations.push(location.clone());
        } else {
            let insertion = locations.partition_point(|existing| {
                existing.reservation.first_offset < reservation.first_offset
            });
            locations.insert(insertion, location.clone());
        }
        self.by_batch.insert(key, location);
        Ok(())
    }

    fn append_prepared_segment(&mut self, prepared: &[&PreparedExtent]) -> StorageResult<()> {
        let frame_offset = self.active.bytes;
        self.active.file.seek(SeekFrom::Start(frame_offset))?;
        let mut slices = Vec::with_capacity(prepared.len().saturating_mul(3));
        for extent in prepared {
            slices.push(IoSlice::new(&extent.encoded.header));
            slices.push(IoSlice::new(&extent.payload));
            slices.push(IoSlice::new(&extent.encoded.checksum));
        }
        if let Err(error) = write_all_vectored(&mut self.active.file, &mut slices) {
            let _ = self.active.file.set_len(frame_offset);
            return Err(error.into());
        }
        self.active.file.flush()?;

        let mut next_frame_offset = frame_offset;
        for extent in prepared {
            let location = ExtentLocation {
                reservation: extent.reservation,
                producer: extent.producer,
                atomic_group: extent.atomic_group,
                pack_sequence: self.active.sequence,
                payload_offset: next_frame_offset + HEADER_LEN as u64,
                payload_len: extent.payload.len(),
            };
            self.install_location(location);
            next_frame_offset = next_frame_offset
                .checked_add(extent.encoded.frame_len as u64)
                .ok_or_else(|| StorageError::LimitExceeded("pack length overflow".into()))?;
        }
        self.active.bytes = next_frame_offset;
        Ok(())
    }

    fn install_location(&mut self, location: ExtentLocation) {
        let key = (
            TopicPartition::new(
                location.reservation.topic_id,
                location.reservation.partition_id,
            ),
            location.reservation.batch_id,
        );
        let locations = self.index.entry(key.0).or_default();
        if locations
            .last()
            .is_none_or(|last| last.reservation.first_offset < location.reservation.first_offset)
        {
            locations.push(location.clone());
        } else {
            let insertion = locations.partition_point(|existing| {
                existing.reservation.first_offset < location.reservation.first_offset
            });
            locations.insert(insertion, location.clone());
        }
        self.by_batch.insert(key, location);
    }

    pub fn contains(&self, reservation: &Reservation) -> bool {
        self.by_batch.contains_key(&(
            TopicPartition::new(reservation.topic_id, reservation.partition_id),
            reservation.batch_id,
        ))
    }

    pub fn read_batch(&mut self, reservation: &Reservation) -> StorageResult<Option<StoredBatch>> {
        let Some(location) = self.by_batch.get(&(
            TopicPartition::new(reservation.topic_id, reservation.partition_id),
            reservation.batch_id,
        )) else {
            return Ok(None);
        };
        if location.reservation != *reservation {
            return Err(StorageError::InvalidInput(format!(
                "batch {} metadata does not match indexed extent",
                reservation.batch_id
            )));
        }
        let planned = self.metadata_for(location);
        Ok(self.read_fetch_plan(&[planned])?.pop())
    }

    pub fn fetch(
        &mut self,
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
        max_bytes: usize,
    ) -> StorageResult<Vec<StoredBatch>> {
        let planned = self.plan_fetch(topic_partition, start_offset, max_bytes, usize::MAX)?;
        self.read_fetch_plan(&planned)
    }

    /// Actual native catalog count before a bounded caller materializes a copy.
    #[must_use]
    pub fn recovery_catalog_len(&self) -> usize {
        self.by_batch.len()
    }

    #[must_use]
    pub fn catalog(&self) -> Vec<ExtentCatalogEntry> {
        let mut entries = self
            .by_batch
            .values()
            .map(|location| ExtentCatalogEntry {
                reservation: location.reservation,
                producer: location.producer,
                atomic_group: location.atomic_group,
                payload_bytes: location.payload_len as u64,
                pack_sequence: location.pack_sequence,
                object_durable: false,
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| {
            (
                entry.reservation.topic_id,
                entry.reservation.partition_id,
                entry.reservation.first_offset,
            )
        });
        entries
    }

    /// Materializes the native catalog under the original admitted recovery E.
    #[cfg(unix)]
    pub fn catalog_until(&self, expiry: Instant) -> StorageResult<Vec<ExtentCatalogEntry>> {
        recovery_gc_clock(expiry)?;
        let mut entries = Vec::with_capacity(self.by_batch.len());
        for location in self.by_batch.values() {
            recovery_gc_clock(expiry)?;
            entries.push(ExtentCatalogEntry {
                reservation: location.reservation,
                producer: location.producer,
                atomic_group: location.atomic_group,
                payload_bytes: location.payload_len as u64,
                pack_sequence: location.pack_sequence,
                object_durable: false,
            });
        }
        recovery_gc_clock(expiry)?;
        entries.sort_by_key(|entry| {
            (
                entry.reservation.topic_id,
                entry.reservation.partition_id,
                entry.reservation.first_offset,
            )
        });
        recovery_gc_clock(expiry)?;
        Ok(entries)
    }

    /// Discovers actual sealed pack metadata without renewing recovery expiry.
    #[cfg(unix)]
    pub fn sealed_packs_until(&self, expiry: Instant) -> StorageResult<Vec<SealedPack>> {
        recovery_gc_clock(expiry)?;
        let mut packs = Vec::new();
        for (&sequence, path) in self.pack_paths.range(..self.active.sequence) {
            recovery_gc_clock(expiry)?;
            let bytes = path.metadata()?.len();
            recovery_gc_clock(expiry)?;
            if bytes > 0 {
                packs.push(SealedPack {
                    sequence,
                    path: path.clone(),
                    bytes,
                });
            }
        }
        recovery_gc_clock(expiry)?;
        Ok(packs)
    }

    pub fn sync(&mut self) -> StorageResult<()> {
        self.active.file.flush()?;
        self.active.file.sync_data()?;
        Ok(())
    }

    pub fn sealed_packs(&self) -> StorageResult<Vec<SealedPack>> {
        let mut packs = Vec::new();
        for (&sequence, path) in self.pack_paths.range(..self.active.sequence) {
            let bytes = path.metadata()?.len();
            if bytes > 0 {
                packs.push(SealedPack {
                    sequence,
                    path: path.clone(),
                    bytes,
                });
            }
        }
        Ok(packs)
    }

    pub fn seal_active(&mut self) -> StorageResult<Option<SealedPack>> {
        if self.active.bytes == 0 {
            return Ok(None);
        }
        let sealed = SealedPack {
            sequence: self.active.sequence,
            path: self.active_path().to_path_buf(),
            bytes: self.active.bytes,
        };
        self.rotate()?;
        Ok(Some(sealed))
    }

    pub fn seal_active_if_older(&mut self, max_age: Duration) -> StorageResult<Option<SealedPack>> {
        if self.active.opened_at.elapsed() < max_age {
            return Ok(None);
        }
        self.seal_active()
    }

    pub fn garbage_collect(
        &mut self,
        log_starts: &HashMap<TopicPartition, LogicalOffset>,
    ) -> StorageResult<GarbageCollectionStats> {
        let removable = self
            .pack_paths
            .range(..self.active.sequence)
            .filter_map(|(&sequence, path)| {
                let locations = self
                    .by_batch
                    .values()
                    .filter(|location| location.pack_sequence == sequence)
                    .collect::<Vec<_>>();
                (!locations.is_empty()
                    && locations
                        .iter()
                        .all(|location| gc_location_before_start(location, log_starts)))
                .then(|| (sequence, path.clone(), locations.len() as u64))
            })
            .collect::<Vec<_>>();

        if !removable.is_empty() {
            self.plan_epoch = self
                .plan_epoch
                .checked_add(1)
                .ok_or_else(|| StorageError::LimitExceeded("fetch plan epoch exhausted".into()))?;
        }
        let mut stats = GarbageCollectionStats::default();
        for (sequence, path, extents) in removable {
            let bytes = path.metadata()?.len();
            let tombstone = pending_pack_deletion_path(&path);
            fs::rename(&path, &tombstone)?;
            sync_directory(&self.config.directory)?;
            self.pack_paths.remove(&sequence);
            self.by_batch
                .retain(|_, location| location.pack_sequence != sequence);
            for locations in self.index.values_mut() {
                locations.retain(|location| location.pack_sequence != sequence);
            }
            self.index.retain(|_, locations| !locations.is_empty());
            fs::remove_file(tombstone)?;
            sync_directory(&self.config.directory)?;
            stats.packs_removed = stats.packs_removed.saturating_add(1);
            stats.extents_removed = stats.extents_removed.saturating_add(extents);
            stats.bytes_removed = stats.bytes_removed.saturating_add(bytes);
        }
        Ok(stats)
    }

    /// Applies native pack GC under a retained operation's original Instant.
    ///
    /// This changes no retention policy. A late error may leave a real native
    /// gc-pending tombstone; normal recovery keeps its existing cleanup rules.
    #[cfg(unix)]
    pub fn garbage_collect_until(
        &mut self,
        log_starts: &HashMap<TopicPartition, LogicalOffset>,
        expiry: Instant,
    ) -> StorageResult<GarbageCollectionStats> {
        recovery_gc_clock(expiry)?;
        let mut removable = Vec::new();
        for (&sequence, path) in self.pack_paths.range(..self.active.sequence) {
            recovery_gc_clock(expiry)?;
            let locations = self
                .by_batch
                .values()
                .filter(|location| location.pack_sequence == sequence)
                .collect::<Vec<_>>();
            recovery_gc_clock(expiry)?;
            if !locations.is_empty()
                && locations
                    .iter()
                    .all(|location| gc_location_before_start(location, log_starts))
            {
                recovery_gc_clock(expiry)?;
                removable.push((sequence, path.clone(), locations.len() as u64));
            }
        }
        recovery_gc_clock(expiry)?;
        if !removable.is_empty() {
            self.plan_epoch = self
                .plan_epoch
                .checked_add(1)
                .ok_or_else(|| StorageError::LimitExceeded("fetch plan epoch exhausted".into()))?;
        }
        let mut stats = GarbageCollectionStats::default();
        for (sequence, path, extents) in removable {
            recovery_gc_clock(expiry)?;
            let bytes = path.metadata()?.len();
            recovery_gc_clock(expiry)?;
            let tombstone = pending_pack_deletion_path(&path);
            recovery_gc_clock(expiry)?;
            fs::rename(&path, &tombstone)?;
            #[cfg(test)]
            recovery_gc_test_step("renamed");
            recovery_gc_clock(expiry)?;
            sync_directory(&self.config.directory)?;
            recovery_gc_clock(expiry)?;
            self.pack_paths.remove(&sequence);
            self.by_batch
                .retain(|_, location| location.pack_sequence != sequence);
            for locations in self.index.values_mut() {
                recovery_gc_clock(expiry)?;
                locations.retain(|location| location.pack_sequence != sequence);
            }
            self.index.retain(|_, locations| !locations.is_empty());
            recovery_gc_clock(expiry)?;
            fs::remove_file(tombstone)?;
            recovery_gc_clock(expiry)?;
            sync_directory(&self.config.directory)?;
            recovery_gc_clock(expiry)?;
            stats.packs_removed = stats.packs_removed.saturating_add(1);
            stats.extents_removed = stats.extents_removed.saturating_add(extents);
            stats.bytes_removed = stats.bytes_removed.saturating_add(bytes);
        }
        recovery_gc_clock(expiry)?;
        Ok(stats)
    }

    #[must_use]
    pub fn active_path(&self) -> &Path {
        self.pack_paths
            .get(&self.active.sequence)
            .expect("active pack path exists")
    }

    /// Clone the active pack descriptor for a thread-local durability worker.
    ///
    /// The append worker retains ownership of the `ShardStore` and its indexes;
    /// the clone is only used for `sync_data`, so no mutable storage state is
    /// shared across workers.
    pub fn active_file_clone(&self) -> StorageResult<File> {
        Ok(self.active.file.try_clone()?)
    }

    fn rotate(&mut self) -> StorageResult<()> {
        self.active.file.flush()?;
        self.active.file.sync_data()?;
        let sequence = self
            .active
            .sequence
            .checked_add(1)
            .ok_or_else(|| StorageError::LimitExceeded("pack sequence exhausted".into()))?;
        let path = pack_path(&self.config.directory, sequence);
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        self.pack_paths.insert(sequence, path.clone());
        sync_directory(&self.config.directory)?;
        self.active = ActivePack {
            sequence,
            file,
            bytes: 0,
            opened_at: Instant::now(),
        };
        Ok(())
    }

    fn metadata_for(&self, location: &ExtentLocation) -> StoredBatchMetadata {
        StoredBatchMetadata {
            reservation: location.reservation,
            producer: location.producer,
            atomic_group: location.atomic_group,
            pack_sequence: location.pack_sequence,
            payload_offset: location.payload_offset,
            payload_len: location.payload_len,
            store_token: self.store_token,
            plan_epoch: self.plan_epoch,
        }
    }

    fn open_pack_reader(&self, sequence: u64) -> StorageResult<File> {
        let path = self
            .pack_paths
            .get(&sequence)
            .ok_or_else(|| StorageError::InvalidInput("planned pack is missing".into()))?;
        Ok(OpenOptions::new().read(true).open(path)?)
    }
}

#[derive(Debug)]
struct EncodedExtent {
    header: Vec<u8>,
    checksum: [u8; CHECKSUM_LEN],
    frame_len: usize,
}

fn encode_extent(
    reservation: Reservation,
    producer: Option<ProducerSequence>,
    atomic_group: Option<AtomicGroupSequence>,
    payload: &[u8],
) -> StorageResult<EncodedExtent> {
    let frame_len = HEADER_LEN
        .checked_add(payload.len())
        .and_then(|value| value.checked_add(CHECKSUM_LEN))
        .ok_or_else(|| StorageError::LimitExceeded("extent frame length overflow".into()))?;
    let frame_len_u64 = u64::try_from(frame_len)
        .map_err(|_| StorageError::LimitExceeded("extent frame exceeds u64".into()))?;
    let payload_len_u64 = u64::try_from(payload.len())
        .map_err(|_| StorageError::LimitExceeded("payload exceeds u64".into()))?;

    let mut header = Vec::with_capacity(HEADER_LEN);
    header.extend_from_slice(EXTENT_MAGIC);
    header.push(EXTENT_VERSION);
    header.push(EXTENT_FLAGS);
    header.extend_from_slice(&(HEADER_LEN as u16).to_le_bytes());
    push_u64(&mut header, frame_len_u64);
    push_u128(&mut header, reservation.topic_id.get());
    push_u32(&mut header, reservation.partition_id.get());
    push_u128(&mut header, reservation.batch_id.get());
    push_u64(&mut header, reservation.first_offset.get());
    push_u64(&mut header, reservation.last_offset.get());
    push_u32(&mut header, reservation.record_count.get());
    push_u32(&mut header, reservation.placement.virtual_lane_id.get());
    push_u64(&mut header, reservation.placement.ring_epoch.get());
    push_u64(&mut header, reservation.placement.leader_epoch.get());
    push_u64(&mut header, reservation.placement.sequence.get());
    match producer {
        Some(producer) => {
            if producer.digest_algorithm != DIGEST_XXH3_128 {
                return Err(StorageError::InvalidInput(
                    "unsupported producer payload digest algorithm".into(),
                ));
            }
            header.push(1);
            header.extend_from_slice(&[0; 3]);
            push_u128(&mut header, producer.producer_id);
            push_u32(&mut header, producer.epoch);
            push_u64(&mut header, producer.first_sequence);
            header.push(producer.digest_algorithm);
            header.extend_from_slice(&[0; 3]);
            header.extend_from_slice(&producer.payload_digest);
            match producer.event_id {
                Some(event_id) => {
                    header.push(1);
                    header.extend_from_slice(&[0; 3]);
                    push_u128(&mut header, event_id.get());
                }
                None => header.extend_from_slice(&[0; 20]),
            }
        }
        None => header.extend_from_slice(&[0; PRODUCER_FIXED_LEN]),
    }
    match atomic_group {
        Some(group) => {
            header.push(1);
            header.extend_from_slice(&[0; 3]);
            push_u128(&mut header, group.transaction_id);
            push_u64(&mut header, group.chunk_sequence);
            header.extend_from_slice(&group.payload_digest);
        }
        None => header.extend_from_slice(&[0; GROUP_FIXED_LEN]),
    }
    push_u64(&mut header, payload_len_u64);
    debug_assert_eq!(header.len(), HEADER_LEN);
    Ok(EncodedExtent {
        checksum: extent_checksum(&header, payload).to_le_bytes(),
        header,
        frame_len,
    })
}

fn extent_checksum(header: &[u8], payload: &[u8]) -> u64 {
    xxh3_64_with_seed(payload, xxh3_64(header))
}

fn write_extent_frame(
    writer: &mut impl Write,
    header: &[u8],
    payload: &[u8],
    checksum: &[u8; CHECKSUM_LEN],
) -> std::io::Result<()> {
    let parts = [header, payload, checksum.as_slice()];
    let slices = [
        IoSlice::new(parts[0]),
        IoSlice::new(parts[1]),
        IoSlice::new(parts[2]),
    ];
    let written = writer.write_vectored(&slices)?;
    if written == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            "failed to write extent frame",
        ));
    }
    let mut remaining_skip = written;
    for part in parts {
        if remaining_skip >= part.len() {
            remaining_skip -= part.len();
        } else {
            writer.write_all(&part[remaining_skip..])?;
            remaining_skip = 0;
        }
    }
    Ok(())
}

fn write_all_vectored(writer: &mut impl Write, buffers: &mut [IoSlice<'_>]) -> std::io::Result<()> {
    let mut remaining = buffers;
    while !remaining.is_empty() {
        let written = writer.write_vectored(remaining)?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write grouped extent frames",
            ));
        }
        IoSlice::advance_slices(&mut remaining, written);
    }
    Ok(())
}

pub(crate) fn decode_extent(
    path: &Path,
    frame_offset: u64,
    frame: &[u8],
    _physical_shard: ShardId,
) -> StorageResult<ExtentLocation> {
    if frame.len() < HEADER_LEN + CHECKSUM_LEN {
        return Err(StorageError::corrupt(path, frame_offset, "frame too short"));
    }
    let expected_checksum = u64::from_le_bytes(
        frame[frame.len() - CHECKSUM_LEN..]
            .try_into()
            .expect("checksum width"),
    );
    let actual_checksum = extent_checksum(
        &frame[..HEADER_LEN],
        &frame[HEADER_LEN..frame.len() - CHECKSUM_LEN],
    );
    if actual_checksum != expected_checksum {
        return Err(StorageError::corrupt(
            path,
            frame_offset,
            "extent checksum mismatch",
        ));
    }

    let mut decoder = Decoder::new(&frame[PREFIX_LEN..frame.len() - CHECKSUM_LEN]);
    let header = decode_extent_header(path, frame_offset, &mut decoder)?;
    if decoder.take(header.payload_len).is_err() || !decoder.is_empty() {
        return Err(StorageError::corrupt(
            path,
            frame_offset,
            "payload length does not match frame",
        ));
    }

    Ok(ExtentLocation {
        reservation: header.reservation,
        producer: header.producer,
        atomic_group: header.atomic_group,
        pack_sequence: 0,
        payload_offset: frame_offset + HEADER_LEN as u64,
        payload_len: header.payload_len,
    })
}

// Complete borrowed inspection reuses the unchanged native decoder above.
// The payload range belongs to this codec, rather than an external SSE8 layout.
pub(crate) fn decode_extent_payload<'a>(
    path: &Path,
    frame_offset: u64,
    frame: &'a [u8],
) -> StorageResult<(ExtentLocation, &'a [u8])> {
    let location = decode_extent(path, frame_offset, frame, ShardId::new(0))?;
    let mut payload = Decoder::new(&frame[HEADER_LEN..frame.len() - CHECKSUM_LEN]);
    let bytes = payload.take(location.payload_len)?;
    debug_assert!(
        payload.is_empty(),
        "native decode consumed the complete payload"
    );
    Ok((location, bytes))
}

pub(crate) struct DecodedExtentHeader {
    pub(crate) reservation: Reservation,
    pub(crate) producer: Option<ProducerSequence>,
    pub(crate) atomic_group: Option<AtomicGroupSequence>,
    pub(crate) payload_len: usize,
}

// One native fixed-header decoder, shared with incomplete-tail observation.
// Complete decode calls this only AFTER its original checksum validation.
pub(crate) fn decode_extent_header(
    path: &Path,
    frame_offset: u64,
    decoder: &mut Decoder<'_>,
) -> StorageResult<DecodedExtentHeader> {
    let topic_id = TopicId::new(decoder.u128()?);
    let partition_id = LogicalPartitionId::new(decoder.u32()?);
    let batch_id = BatchId::new(decoder.u128()?);
    let first_offset = LogicalOffset::new(decoder.u64()?);
    let last_offset = LogicalOffset::new(decoder.u64()?);
    let record_count = std::num::NonZeroU32::new(decoder.u32()?)
        .ok_or_else(|| StorageError::corrupt(path, frame_offset, "zero record count"))?;
    let virtual_lane_id = VirtualLaneId::new(decoder.u32()?);
    let ring_epoch = RingEpoch::new(decoder.u64()?);
    let leader_epoch = LeaderEpoch::new(decoder.u64()?);
    let sequence = PlacementSequence::new(decoder.u64()?);
    let producer_present = decoder.u8()?;
    let producer_reserved = decoder.take(3)?;
    let producer_id = decoder.u128()?;
    let producer_epoch = decoder.u32()?;
    let producer_first_sequence = decoder.u64()?;
    let digest_algorithm = decoder.u8()?;
    let digest_reserved = decoder.take(3)?;
    let payload_digest: [u8; 16] = decoder.take(16)?.try_into().expect("producer digest width");
    let event_present = decoder.u8()?;
    let event_reserved = decoder.take(3)?;
    let event_id = decoder.u128()?;
    if producer_reserved != [0; 3] || digest_reserved != [0; 3] || event_reserved != [0; 3] {
        return Err(StorageError::corrupt(
            path,
            frame_offset,
            "producer reserved bytes are nonzero",
        ));
    }
    let producer = match producer_present {
        0 => {
            if producer_id != 0
                || producer_epoch != 0
                || producer_first_sequence != 0
                || digest_algorithm != 0
                || payload_digest != [0; 16]
                || event_present != 0
                || event_id != 0
            {
                return Err(StorageError::corrupt(
                    path,
                    frame_offset,
                    "absent producer metadata is nonzero",
                ));
            }
            None
        }
        1 if digest_algorithm == DIGEST_XXH3_128 => {
            let event_id = match event_present {
                0 if event_id == 0 => None,
                1 => Some(RecordId::new(event_id)),
                _ => {
                    return Err(StorageError::corrupt(
                        path,
                        frame_offset,
                        "invalid producer event metadata",
                    ));
                }
            };
            Some(ProducerSequence {
                producer_id,
                epoch: producer_epoch,
                first_sequence: producer_first_sequence,
                event_id,
                digest_algorithm,
                payload_digest,
            })
        }
        1 => {
            return Err(StorageError::corrupt(
                path,
                frame_offset,
                "unsupported producer digest algorithm",
            ));
        }
        _ => {
            return Err(StorageError::corrupt(
                path,
                frame_offset,
                "invalid producer metadata flag",
            ));
        }
    };
    let group_present = decoder.u8()?;
    let group_reserved = decoder.take(3)?;
    let transaction_id = decoder.u128()?;
    let chunk_sequence = decoder.u64()?;
    let group_payload_digest: [u8; 16] = decoder.take(16)?.try_into().expect("group digest width");
    if group_reserved != [0; 3] {
        return Err(StorageError::corrupt(
            path,
            frame_offset,
            "atomic group reserved bytes are nonzero",
        ));
    }
    let atomic_group = match group_present {
        0 if transaction_id == 0 && chunk_sequence == 0 && group_payload_digest == [0; 16] => None,
        1 => Some(AtomicGroupSequence {
            transaction_id,
            chunk_sequence,
            payload_digest: group_payload_digest,
        }),
        _ => {
            return Err(StorageError::corrupt(
                path,
                frame_offset,
                "invalid atomic group metadata",
            ));
        }
    };
    let payload_len = usize::try_from(decoder.u64()?)
        .map_err(|_| StorageError::corrupt(path, frame_offset, "payload length overflow"))?;
    if first_offset
        .get()
        .checked_add(u64::from(record_count.get()) - 1)
        != Some(last_offset.get())
    {
        return Err(StorageError::corrupt(
            path,
            frame_offset,
            "record range does not match record count",
        ));
    }
    Ok(DecodedExtentHeader {
        reservation: Reservation {
            topic_id,
            partition_id,
            batch_id,
            first_offset,
            last_offset,
            record_count,
            placement: Placement {
                virtual_lane_id,
                ring_epoch,
                leader_epoch,
                sequence,
            },
        },
        producer,
        atomic_group,
        payload_len,
    })
}

fn scan_pack(
    path: &Path,
    pack_sequence: u64,
    expected_shard: ShardId,
    max_batch_bytes: usize,
    permit_tail_repair: bool,
    index: &mut HashMap<TopicPartition, Vec<ExtentLocation>>,
    by_batch: &mut HashMap<(TopicPartition, BatchId), ExtentLocation>,
) -> StorageResult<()> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let file_len = file.metadata()?.len();
    let mut offset = 0u64;
    while offset < file_len {
        let remaining = file_len - offset;
        if remaining < PREFIX_LEN as u64 {
            if permit_tail_repair {
                file.set_len(offset)?;
                file.sync_data()?;
                break;
            }
            return Err(StorageError::corrupt(
                path,
                offset,
                "truncated prefix in sealed pack",
            ));
        }
        file.seek(SeekFrom::Start(offset))?;
        let mut prefix = [0u8; PREFIX_LEN];
        file.read_exact(&mut prefix)?;
        validate_prefix(path, offset, &prefix)?;
        let frame_len = u64::from_le_bytes(prefix[8..16].try_into().expect("frame length"));
        let max_frame_len = (HEADER_LEN as u64)
            .checked_add(max_batch_bytes as u64)
            .and_then(|value| value.checked_add(CHECKSUM_LEN as u64))
            .ok_or_else(|| StorageError::LimitExceeded("configured frame limit overflow".into()))?;
        if frame_len < (HEADER_LEN + CHECKSUM_LEN) as u64 || frame_len > max_frame_len {
            return Err(StorageError::corrupt(
                path,
                offset,
                format!("invalid frame length {frame_len}"),
            ));
        }
        if offset
            .checked_add(frame_len)
            .is_none_or(|end| end > file_len)
        {
            if permit_tail_repair {
                file.set_len(offset)?;
                file.sync_data()?;
                break;
            }
            return Err(StorageError::corrupt(
                path,
                offset,
                "truncated frame in sealed pack",
            ));
        }
        let frame_len_usize = usize::try_from(frame_len)
            .map_err(|_| StorageError::corrupt(path, offset, "frame length exceeds usize"))?;
        let mut frame = vec![0u8; frame_len_usize];
        frame[..PREFIX_LEN].copy_from_slice(&prefix);
        file.read_exact(&mut frame[PREFIX_LEN..])?;
        let location = decode_extent(path, offset, &frame, expected_shard)?;
        index_recovered_extent(
            path,
            offset,
            pack_sequence,
            location,
            index,
            by_batch,
            &mut None,
        )?;
        offset += frame_len;
    }
    Ok(())
}

// Bounded callers reserve the native maps and existing topic Vec first, and
// retain an optional pre-reserved new topic Vec outside their unwind boundary.
// Ordinary callers pass None and retain the original allocation behavior.
// Preserve the native duplicate overwrite/refusal BEFORE any index insertion.
fn index_recovered_extent(
    path: &Path,
    offset: u64,
    pack_sequence: u64,
    mut location: ExtentLocation,
    index: &mut HashMap<TopicPartition, Vec<ExtentLocation>>,
    by_batch: &mut HashMap<(TopicPartition, BatchId), ExtentLocation>,
    spare_locations: &mut Option<Vec<ExtentLocation>>,
) -> StorageResult<()> {
    location.pack_sequence = pack_sequence;
    let topic_partition = TopicPartition::new(
        location.reservation.topic_id,
        location.reservation.partition_id,
    );
    let key = (topic_partition, location.reservation.batch_id);
    if by_batch.insert(key, location.clone()).is_some() {
        return Err(StorageError::corrupt(path, offset, "duplicate batch ID"));
    }
    index
        .entry(topic_partition)
        .or_insert_with(|| spare_locations.take().unwrap_or_default())
        .push(location);
    Ok(())
}

pub(crate) fn validate_prefix(
    path: &Path,
    offset: u64,
    prefix: &[u8; PREFIX_LEN],
) -> StorageResult<()> {
    if &prefix[..4] != EXTENT_MAGIC {
        return Err(StorageError::corrupt(path, offset, "invalid extent magic"));
    }
    if prefix[4] != EXTENT_VERSION {
        return Err(StorageError::corrupt(
            path,
            offset,
            format!("unsupported extent version {}", prefix[4]),
        ));
    }
    if prefix[5] != EXTENT_FLAGS {
        return Err(StorageError::corrupt(path, offset, "unsupported flags"));
    }
    let header_len = u16::from_le_bytes(prefix[6..8].try_into().expect("header length"));
    if usize::from(header_len) != HEADER_LEN {
        return Err(StorageError::corrupt(
            path,
            offset,
            "unexpected header length",
        ));
    }
    Ok(())
}

// Keep the existing native accepted digit spellings and pending-name role.
fn pack_sequence_from_name(name: &str) -> Option<u64> {
    name.strip_prefix("pack-")
        .and_then(|name| name.strip_suffix(".sse"))
        .and_then(|digits| digits.parse::<u64>().ok())
}

fn is_pending_pack_deletion_name(name: &str) -> bool {
    name.starts_with("pack-") && name.ends_with(".sse.gc-pending")
}

fn discover_packs(directory: &Path) -> StorageResult<BTreeMap<u64, PathBuf>> {
    let mut packs = BTreeMap::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(sequence) = pack_sequence_from_name(name) else {
            continue;
        };
        if packs.insert(sequence, path).is_some() {
            return Err(StorageError::InvalidInput(format!(
                "duplicate pack sequence {sequence}"
            )));
        }
    }
    Ok(packs)
}

fn pack_path(directory: &Path, sequence: u64) -> PathBuf {
    directory.join(format!("pack-{sequence:020}.sse"))
}

#[cfg(unix)]
fn scan_pack_until(
    path: &Path,
    pack_sequence: u64,
    expected_shard: ShardId,
    max_batch_bytes: usize,
    permit_tail_repair: bool,
    index: &mut HashMap<TopicPartition, Vec<ExtentLocation>>,
    by_batch: &mut HashMap<(TopicPartition, BatchId), ExtentLocation>,
    expiry: Instant,
) -> StorageResult<()> {
    recovery_gc_clock(expiry)?;
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    recovery_gc_clock(expiry)?;
    let file_len = file.metadata()?.len();
    let mut offset = 0u64;
    while offset < file_len {
        recovery_gc_clock(expiry)?;
        let remaining = file_len - offset;
        if remaining < PREFIX_LEN as u64 {
            if permit_tail_repair {
                recovery_gc_clock(expiry)?;
                file.set_len(offset)?;
                recovery_gc_clock(expiry)?;
                file.sync_data()?;
                recovery_gc_clock(expiry)?;
                break;
            }
            return Err(StorageError::corrupt(
                path,
                offset,
                "truncated prefix in sealed pack",
            ));
        }
        recovery_gc_clock(expiry)?;
        file.seek(SeekFrom::Start(offset))?;
        recovery_gc_clock(expiry)?;
        let mut prefix = [0u8; PREFIX_LEN];
        file.read_exact(&mut prefix)?;
        recovery_gc_clock(expiry)?;
        validate_prefix(path, offset, &prefix)?;
        let frame_len = u64::from_le_bytes(prefix[8..16].try_into().expect("frame length"));
        let max_frame_len = (HEADER_LEN as u64)
            .checked_add(max_batch_bytes as u64)
            .and_then(|value| value.checked_add(CHECKSUM_LEN as u64))
            .ok_or_else(|| StorageError::LimitExceeded("configured frame limit overflow".into()))?;
        if frame_len < (HEADER_LEN + CHECKSUM_LEN) as u64 || frame_len > max_frame_len {
            return Err(StorageError::corrupt(
                path,
                offset,
                format!("invalid frame length {frame_len}"),
            ));
        }
        if offset
            .checked_add(frame_len)
            .is_none_or(|end| end > file_len)
        {
            if permit_tail_repair {
                recovery_gc_clock(expiry)?;
                file.set_len(offset)?;
                recovery_gc_clock(expiry)?;
                file.sync_data()?;
                recovery_gc_clock(expiry)?;
                break;
            }
            return Err(StorageError::corrupt(
                path,
                offset,
                "truncated frame in sealed pack",
            ));
        }
        let frame_len_usize = usize::try_from(frame_len)
            .map_err(|_| StorageError::corrupt(path, offset, "frame length exceeds usize"))?;
        recovery_gc_clock(expiry)?;
        let mut frame = vec![0u8; frame_len_usize];
        frame[..PREFIX_LEN].copy_from_slice(&prefix);
        file.read_exact(&mut frame[PREFIX_LEN..])?;
        recovery_gc_clock(expiry)?;
        let location = decode_extent(path, offset, &frame, expected_shard)?;
        recovery_gc_clock(expiry)?;
        index_recovered_extent(
            path,
            offset,
            pack_sequence,
            location,
            index,
            by_batch,
            &mut None,
        )?;
        offset += frame_len;
    }
    recovery_gc_clock(expiry)?;
    Ok(())
}

#[cfg(unix)]
fn discover_packs_until(
    directory: &Path,
    expiry: Instant,
) -> StorageResult<BTreeMap<u64, PathBuf>> {
    recovery_gc_clock(expiry)?;
    let mut packs = BTreeMap::new();
    let mut entries = fs::read_dir(directory)?;
    loop {
        recovery_gc_clock(expiry)?;
        #[cfg(test)]
        recovery_directory_test_step("discover-next-start");
        recovery_gc_clock(expiry)?;
        let entry = entries.next();
        #[cfg(test)]
        recovery_directory_test_step("discover-next-complete");
        // EOF and errors are physical visits too, never a renewed clock.
        recovery_gc_clock(expiry)?;
        let Some(entry) = entry else { break };
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            #[cfg(test)]
            recovery_directory_test_step("discover-entry-complete");
            continue;
        };
        let Some(sequence) = pack_sequence_from_name(name) else {
            #[cfg(test)]
            recovery_directory_test_step("discover-entry-complete");
            continue;
        };
        if packs.insert(sequence, path).is_some() {
            return Err(StorageError::InvalidInput(format!(
                "duplicate pack sequence {sequence}"
            )));
        }
        #[cfg(test)]
        recovery_directory_test_step("discover-entry-complete");
    }
    recovery_gc_clock(expiry)?;
    Ok(packs)
}

#[cfg(unix)]
fn cleanup_pending_pack_deletions_until(directory: &Path, expiry: Instant) -> StorageResult<()> {
    recovery_gc_clock(expiry)?;
    let mut pending = Vec::new();
    let mut entries = fs::read_dir(directory)?;
    loop {
        recovery_gc_clock(expiry)?;
        #[cfg(test)]
        recovery_directory_test_step("cleanup-next-start");
        recovery_gc_clock(expiry)?;
        let entry = entries.next();
        #[cfg(test)]
        recovery_directory_test_step("cleanup-next-complete");
        // EOF and errors are physical visits too, never a renewed clock.
        recovery_gc_clock(expiry)?;
        let Some(entry) = entry else { break };
        if let Ok(entry) = entry {
            let path = entry.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(is_pending_pack_deletion_name)
            {
                pending.push(path);
            }
        }
        #[cfg(test)]
        recovery_directory_test_step("cleanup-entry-complete");
    }
    for path in &pending {
        recovery_gc_clock(expiry)?;
        fs::remove_file(path)?;
    }
    recovery_gc_clock(expiry)?;
    if !pending.is_empty() {
        sync_directory(directory)?;
    }
    recovery_gc_clock(expiry)?;
    Ok(())
}

#[cfg(unix)]
fn recovery_gc_clock(expiry: Instant) -> StorageResult<()> {
    if Instant::now() >= expiry {
        return Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "native recovery original expiry elapsed",
        )
        .into());
    }
    Ok(())
}
#[cfg(all(unix, test))]
thread_local! {
    static RECOVERY_DIRECTORY_STEP: std::cell::RefCell<Option<Box<dyn FnMut(&str)>>> =
        const { std::cell::RefCell::new(None) };
}
#[cfg(all(unix, test))]
fn recovery_directory_test_step(step: &str) {
    RECOVERY_DIRECTORY_STEP.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook(step);
        }
    });
}

#[cfg(all(unix, test))]
thread_local! {
    static RECOVERY_GC_STEP: std::cell::RefCell<Option<Box<dyn FnMut(&str)>>> =
        const { std::cell::RefCell::new(None) };
}
#[cfg(all(unix, test))]
fn recovery_gc_test_step(step: &str) {
    RECOVERY_GC_STEP.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook(step);
        }
    });
}

fn gc_location_before_start(
    location: &ExtentLocation,
    log_starts: &HashMap<TopicPartition, LogicalOffset>,
) -> bool {
    log_starts
        .get(&TopicPartition::new(
            location.reservation.topic_id,
            location.reservation.partition_id,
        ))
        .is_some_and(|start| location.reservation.last_offset < *start)
}

fn pending_pack_deletion_path(path: &Path) -> PathBuf {
    path.with_extension("sse.gc-pending")
}

fn cleanup_pending_pack_deletions(directory: &Path) -> StorageResult<()> {
    let pending = fs::read_dir(directory)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(is_pending_pack_deletion_name)
        })
        .collect::<Vec<_>>();
    for path in &pending {
        fs::remove_file(path)?;
    }
    if !pending.is_empty() {
        sync_directory(directory)?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> StorageResult<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::num::NonZeroU32;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    struct ShortWriter {
        bytes: Vec<u8>,
        max_write: usize,
    }

    impl Write for ShortWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let written = bytes.len().min(self.max_write);
            self.bytes.extend_from_slice(&bytes[..written]);
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn write_vectored(&mut self, buffers: &[IoSlice<'_>]) -> io::Result<usize> {
            let Some(first) = buffers.iter().find(|buffer| !buffer.is_empty()) else {
                return Ok(0);
            };
            self.write(first)
        }
    }

    #[test]
    fn vectored_extent_write_completes_after_a_short_first_write() {
        let mut writer = ShortWriter {
            bytes: Vec::new(),
            max_write: 3,
        };
        write_extent_frame(&mut writer, b"header", b"payload", &123u64.to_le_bytes())
            .expect("write frame");
        assert_eq!(writer.bytes, b"headerpayload{\0\0\0\0\0\0\0");
    }

    #[test]
    fn grouped_vectored_write_completes_across_short_writes_and_slices() {
        let mut writer = ShortWriter {
            bytes: Vec::new(),
            max_write: 3,
        };
        let mut slices = [
            IoSlice::new(b"header"),
            IoSlice::new(b"payload"),
            IoSlice::new(b"checksum"),
            IoSlice::new(b"next"),
        ];
        write_all_vectored(&mut writer, &mut slices).expect("write grouped frames");
        assert_eq!(writer.bytes, b"headerpayloadchecksumnext");
    }

    #[test]
    fn read_buffer_is_recycled_after_the_last_bytes_slice_is_dropped() {
        let mut pool = ReadBufferPool::new();
        let mut buffer = Vec::with_capacity(1024);
        buffer.resize(1024, 7);
        let original_pointer = buffer.as_ptr();

        let bytes = pool.lease_bytes(buffer);
        let slice = bytes.slice(1..);
        drop(bytes);
        pool.drain_recycled();
        assert!(pool.available.is_empty());
        assert_eq!(pool.cached_buffers.load(Ordering::Acquire), 0);

        drop(slice);
        let reused = pool.take(512);
        assert_eq!(reused.as_ptr(), original_pointer);
        assert_eq!(reused.len(), 512);
        assert_eq!(pool.cached_buffers.load(Ordering::Acquire), 0);
    }

    #[test]
    fn read_buffer_cache_limit_includes_ready_and_returned_buffers() {
        let mut pool = ReadBufferPool::new();
        let first_wave = (0..READ_BUFFER_POOL_SLOTS)
            .map(|_| pool.lease_bytes(vec![0; 64]))
            .collect::<Vec<_>>();
        drop(first_wave);
        pool.drain_recycled();
        assert_eq!(pool.available.len(), READ_BUFFER_POOL_SLOTS);
        assert_eq!(
            pool.cached_buffers.load(Ordering::Acquire),
            READ_BUFFER_POOL_SLOTS
        );

        let second_wave = (0..READ_BUFFER_POOL_SLOTS)
            .map(|_| pool.lease_bytes(vec![0; 64]))
            .collect::<Vec<_>>();
        drop(second_wave);
        pool.drain_recycled();
        assert_eq!(pool.available.len(), READ_BUFFER_POOL_SLOTS);
        assert_eq!(
            pool.cached_buffers.load(Ordering::Acquire),
            READ_BUFFER_POOL_SLOTS
        );
    }

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "shard-stream-storage-{name}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("temp dir");
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn reservation(batch: u128, first: u64, shard: u32) -> Reservation {
        Reservation {
            topic_id: TopicId::new(1),
            partition_id: LogicalPartitionId::new(0),
            batch_id: BatchId::new(batch),
            first_offset: LogicalOffset::new(first),
            last_offset: LogicalOffset::new(first + 1),
            record_count: NonZeroU32::new(2).expect("nonzero"),
            placement: Placement {
                virtual_lane_id: ShardId::new(shard),
                ring_epoch: RingEpoch::new(1),
                leader_epoch: LeaderEpoch::new(0),
                sequence: PlacementSequence::new(
                    u64::try_from(batch).expect("test batch fits u64"),
                ),
            },
        }
    }

    fn config(directory: &Path, target_pack_bytes: u64) -> ShardStoreConfig {
        ShardStoreConfig {
            directory: directory.to_path_buf(),
            shard_id: ShardId::new(3),
            target_pack_bytes,
            max_batch_bytes: 1024,
        }
    }

    #[test]
    fn directory_has_one_exclusive_owner() {
        let temp = TempDir::new("exclusive-owner");
        let first = ShardStore::open(config(&temp.0, 4096)).expect("first owner");
        assert!(matches!(
            ShardStore::open(config(&temp.0, 4096)),
            Err(StorageError::InvalidInput(message))
                if message.contains("already open")
        ));
        drop(first);
        ShardStore::open(config(&temp.0, 4096)).expect("owner after close");
    }

    #[test]
    fn append_fetch_rotate_and_recover() {
        let temp = TempDir::new("recover");
        {
            let mut store = ShardStore::open(config(&temp.0, 150)).expect("open");
            store
                .append(reservation(0, 0, 3), None, b"first", true)
                .expect("append first");
            store
                .append(reservation(1, 2, 3), None, b"second", true)
                .expect("append second");
            assert!(store.pack_paths.len() >= 2);
        }

        let mut store = ShardStore::open(config(&temp.0, 150)).expect("reopen");
        let batches = store
            .fetch(
                TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0)),
                LogicalOffset::new(0),
                1024,
            )
            .expect("fetch");
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.payload.as_ref())
                .collect::<Vec<_>>(),
            vec![b"first".as_slice(), b"second".as_slice()]
        );

        store
            .append(reservation(2, 4, 3), None, b"third", true)
            .expect("append after reopen");
        let batches = store
            .fetch(
                TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0)),
                LogicalOffset::new(0),
                1024,
            )
            .expect("fetch after append");
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.payload.as_ref())
                .collect::<Vec<_>>(),
            vec![
                b"first".as_slice(),
                b"second".as_slice(),
                b"third".as_slice()
            ]
        );
    }

    #[test]
    fn atomic_group_identity_and_digest_survive_extent_recovery() {
        let temp = TempDir::new("atomic-group");
        let expected = AtomicGroupSequence {
            transaction_id: 99,
            chunk_sequence: 3,
            payload_digest: [7; 16],
        };
        {
            let mut store = ShardStore::open(config(&temp.0, 1024)).expect("open");
            store
                .append_atomic(reservation(0, 0, 3), None, Some(expected), b"atomic", true)
                .expect("append atomic extent");
        }

        let mut store = ShardStore::open(config(&temp.0, 1024)).expect("reopen");
        assert_eq!(store.catalog()[0].atomic_group, Some(expected));
        let batches = store
            .fetch(
                TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0)),
                LogicalOffset::new(0),
                1024,
            )
            .expect("fetch");
        assert_eq!(batches[0].atomic_group, Some(expected));
        assert_eq!(batches[0].payload, Bytes::from_static(b"atomic"));
    }

    #[test]
    fn metadata_fetch_plan_is_bounded_and_materializes_only_selected_extents() {
        let temp = TempDir::new("planned-fetch");
        let mut store = ShardStore::open(config(&temp.0, 1)).expect("open");
        for (batch, payload) in [
            (0, b"zero".as_slice()),
            (1, b"one".as_slice()),
            (2, b"two".as_slice()),
        ] {
            store
                .append(
                    reservation(
                        batch,
                        u64::try_from(batch * 2).expect("test offset fits u64"),
                        3,
                    ),
                    None,
                    payload,
                    false,
                )
                .expect("append");
        }
        let topic_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));

        let planned = store
            .plan_fetch(topic_partition, LogicalOffset::new(0), 1024, 2)
            .expect("plan");
        assert_eq!(planned.len(), 2);
        assert_eq!(
            planned
                .iter()
                .map(|metadata| metadata.reservation.batch_id)
                .collect::<Vec<_>>(),
            vec![BatchId::new(0), BatchId::new(1)]
        );

        let batches = store
            .read_fetch_plan(&planned[..1])
            .expect("read selection");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].payload.as_ref(), b"zero");

        store
            .garbage_collect(&HashMap::from([(topic_partition, LogicalOffset::new(2))]))
            .expect("garbage collect first planned extent");
        assert!(matches!(
            store.read_fetch_plan(&planned[..1]),
            Err(StorageError::InvalidInput(_))
        ));
    }

    #[test]
    fn pending_pack_deletion_is_finished_before_recovery() {
        let temp = TempDir::new("pending-pack-deletion");
        let store_config = config(&temp.0, 1);
        {
            let mut store = ShardStore::open(store_config.clone()).expect("open");
            store
                .append(reservation(0, 0, 2), None, b"old", true)
                .expect("append old pack");
            store
                .append(reservation(1, 1, 2), None, b"retained", true)
                .expect("append retained pack");
        }
        let old_pack = pack_path(&temp.0, 0);
        let tombstone = pending_pack_deletion_path(&old_pack);
        fs::rename(&old_pack, &tombstone).expect("stage pack deletion");
        sync_directory(&temp.0).expect("commit staged deletion");

        let store = ShardStore::open(store_config).expect("reopen");
        assert!(!tombstone.exists());
        assert_eq!(store.catalog().len(), 1);
        assert_eq!(store.catalog()[0].reservation.batch_id, BatchId::new(1));
    }

    #[test]
    fn many_pack_recovery_and_fetch_open_readers_on_demand() {
        const PACKS: u128 = 81;
        let temp = TempDir::new("on-demand-pack-readers");
        {
            let mut store = ShardStore::open(config(&temp.0, 1)).expect("open");
            for batch in 0..PACKS {
                store
                    .append(
                        reservation(batch, u64::try_from(batch * 2).expect("offset fits"), 3),
                        None,
                        b"payload",
                        false,
                    )
                    .expect("append");
            }
            assert_eq!(store.pack_paths.len(), PACKS as usize);
        }

        let mut store = ShardStore::open(config(&temp.0, 1)).expect("reopen");
        assert_eq!(store.pack_paths.len(), PACKS as usize);
        let batches = store
            .fetch(
                TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0)),
                LogicalOffset::new(0),
                usize::MAX,
            )
            .expect("fetch every pack");
        assert_eq!(batches.len(), PACKS as usize);
        assert!(batches.iter().all(|batch| batch.payload == "payload"));
    }

    #[test]
    fn grouped_prepared_append_rotates_and_recovers_every_extent() {
        let temp = TempDir::new("grouped-append");
        let store_config = config(&temp.0, 300);
        let mut store = ShardStore::open(store_config.clone()).expect("open");
        let prepared = [
            (0, 0, b"zero".as_slice()),
            (1, 2, b"one".as_slice()),
            (2, 4, b"two".as_slice()),
        ]
        .map(|(batch, first, payload)| {
            store
                .prepare_append(
                    reservation(batch, first, 3),
                    None,
                    Bytes::copy_from_slice(payload),
                )
                .expect("prepare")
        });
        store
            .append_prepared_group(&prepared.iter().collect::<Vec<_>>(), true)
            .expect("append group");
        assert_eq!(store.catalog().len(), 3);
        assert_eq!(store.pack_paths.len(), 3);
        drop(store);

        let mut store = ShardStore::open(store_config).expect("reopen");
        let batches = store
            .fetch(
                TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0)),
                LogicalOffset::new(0),
                1024,
            )
            .expect("fetch");
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.payload.as_ref())
                .collect::<Vec<_>>(),
            vec![b"zero".as_slice(), b"one".as_slice(), b"two".as_slice()]
        );
    }

    #[test]
    fn grouped_prepared_append_rejects_duplicate_batch_before_writing() {
        let temp = TempDir::new("grouped-duplicate");
        let mut store = ShardStore::open(config(&temp.0, 4096)).expect("open");
        let prepared = store
            .prepare_append(reservation(0, 0, 3), None, Bytes::from_static(b"duplicate"))
            .expect("prepare");

        assert!(matches!(
            store.append_prepared_group(&[&prepared, &prepared], false),
            Err(StorageError::InvalidInput(_))
        ));
        assert_eq!(store.active.bytes, 0);
        assert!(store.catalog().is_empty());
    }

    #[test]
    fn fetch_is_ordered_before_reopen_when_appends_arrive_out_of_order() {
        let temp = TempDir::new("out-of-order-arrival");
        let mut store = ShardStore::open(config(&temp.0, 4096)).expect("open");
        for (batch, first, payload) in [
            (2, 4, b"two".as_slice()),
            (0, 0, b"zero".as_slice()),
            (1, 2, b"one".as_slice()),
        ] {
            store
                .append(reservation(batch, first, 3), None, payload, false)
                .expect("append");
        }

        let batches = store
            .fetch(
                TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0)),
                LogicalOffset::new(0),
                1024,
            )
            .expect("fetch");
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.reservation.batch_id)
                .collect::<Vec<_>>(),
            vec![BatchId::new(0), BatchId::new(1), BatchId::new(2)]
        );
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.payload.as_ref())
                .collect::<Vec<_>>(),
            vec![b"zero".as_slice(), b"one".as_slice(), b"two".as_slice()]
        );
    }

    #[test]
    fn repairs_only_a_partial_active_tail() {
        let temp = TempDir::new("tail");
        let active_path;
        {
            let mut store = ShardStore::open(config(&temp.0, 4096)).expect("open");
            store
                .append(reservation(0, 0, 3), None, b"complete", true)
                .expect("append");
            active_path = store.active_path().to_path_buf();
        }
        let valid_len = fs::metadata(&active_path).expect("metadata").len();
        OpenOptions::new()
            .append(true)
            .open(&active_path)
            .expect("open append")
            .write_all(b"SSE")
            .expect("partial tail");

        let store = ShardStore::open(config(&temp.0, 4096)).expect("repair");
        assert_eq!(
            fs::metadata(&active_path).expect("metadata").len(),
            valid_len
        );
        assert_eq!(store.catalog().len(), 1);
    }

    #[test]
    fn checksum_corruption_is_not_silently_truncated() {
        let temp = TempDir::new("crc");
        let active_path;
        {
            let mut store = ShardStore::open(config(&temp.0, 4096)).expect("open");
            store
                .append(reservation(0, 0, 3), None, b"payload", true)
                .expect("append");
            active_path = store.active_path().to_path_buf();
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&active_path)
            .expect("open corrupt");
        file.seek(SeekFrom::Start(HEADER_LEN as u64)).expect("seek");
        file.write_all(b"X").expect("corrupt");
        file.sync_data().expect("sync");

        assert!(matches!(
            ShardStore::open(config(&temp.0, 4096)),
            Err(StorageError::Corrupt { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn owned_gc_late_first_rename_keeps_tombstone_and_starts_no_second_pack_work() {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                RECOVERY_GC_STEP.with(|hook| *hook.borrow_mut() = None);
            }
        }
        let temp = TempDir::new("owned-gc-expiry");
        let cfg = config(&temp.0, 1);
        let mut store = ShardStore::open(cfg.clone()).expect("actual native store");
        for n in 0..3 {
            store
                .append(
                    reservation(u128::from(n), n * 2, 3),
                    None,
                    b"actual native pack",
                    true,
                )
                .expect("native append");
        }
        let first = pack_path(&temp.0, 0);
        let second = pack_path(&temp.0, 1);
        let second_bytes = fs::read(&second).expect("native second pack bytes");
        let expiry = Instant::now() + Duration::from_millis(50);
        let steps = std::rc::Rc::new(std::cell::Cell::new(0));
        let seen = steps.clone();
        RECOVERY_GC_STEP.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |step| {
                assert_eq!(step, "renamed");
                seen.set(seen.get() + 1);
                std::thread::sleep(
                    expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
                );
            }))
        });
        let reset = Reset;
        let starts = HashMap::from([(
            TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0)),
            LogicalOffset::new(4),
        )]);
        assert!(store.garbage_collect_until(&starts, expiry).is_err());
        assert_eq!(steps.get(), 1);
        drop(reset);
        assert!(!first.exists());
        assert!(
            pending_pack_deletion_path(&first).exists(),
            "truthful native ambiguous directory tail remains"
        );
        assert_eq!(
            fs::read(&second).expect("second pack never renamed/unlinked"),
            second_bytes
        );
        assert!(!pending_pack_deletion_path(&second).exists());
        drop(store);
        let mut reopened = ShardStore::open(cfg).expect("ordinary native pending cleanup");
        assert!(!pending_pack_deletion_path(&first).exists());
        assert_eq!(reopened.catalog().len(), 2);
        reopened
            .garbage_collect(&starts)
            .expect("ordinary native GC preserves its established policy");
        assert_eq!(reopened.catalog().len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn owned_directory_iteration_checks_original_expiry_before_each_next_and_after_eof() {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                RECOVERY_DIRECTORY_STEP.with(|hook| *hook.borrow_mut() = None);
            }
        }
        for cleanup in [false, true] {
            let temp = TempDir::new("owned-directory-next-expiry");
            let cfg = config(&temp.0, 1);
            let mut store = ShardStore::open(cfg.clone()).expect("actual native packs");
            for n in 0..3 {
                store
                    .append(
                        reservation(u128::from(n), n * 2, 3),
                        None,
                        b"actual native directory entry",
                        true,
                    )
                    .expect("real native frame");
            }
            drop(store);
            let first = pack_path(&temp.0, 0);
            let pending = pending_pack_deletion_path(&first);
            fs::rename(&first, &pending).expect("actual interrupted native GC");
            let expected = fs::read(&pending).expect("retained interrupted bytes");
            let expiry = Instant::now() + Duration::from_millis(50);
            let visits = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
            let observed = visits.clone();
            RECOVERY_DIRECTORY_STEP.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |step| {
                    observed.borrow_mut().push(step.to_owned());
                    if step.ends_with("-entry-complete") {
                        std::thread::sleep(
                            expiry.saturating_duration_since(Instant::now())
                                + Duration::from_millis(5),
                        );
                    }
                }))
            });
            let reset = Reset;
            if cleanup {
                assert!(cleanup_pending_pack_deletions_until(&temp.0, expiry).is_err());
                assert_eq!(
                    &*visits.borrow(),
                    &[
                        "cleanup-next-start",
                        "cleanup-next-complete",
                        "cleanup-entry-complete"
                    ]
                );
            } else {
                assert!(discover_packs_until(&temp.0, expiry).is_err());
                assert_eq!(
                    &*visits.borrow(),
                    &[
                        "discover-next-start",
                        "discover-next-complete",
                        "discover-entry-complete"
                    ]
                );
            }
            drop(reset);
            assert_eq!(
                fs::read(&pending).expect("late entry work starts no later read/unlink"),
                expected
            );
            assert!(
                discover_packs_until(&temp.0, Instant::now() + Duration::from_secs(2))
                    .expect("fitting real traversal")
                    .len()
                    >= 2
            );
            cleanup_pending_pack_deletions_until(&temp.0, Instant::now() + Duration::from_secs(2))
                .expect("fitting real cleanup");
            assert!(!pending.exists());
        }
        // An actual empty native directory still performs one EOF next; its
        // late completion must be checked before returning a successful scan.
        let empty = TempDir::new("owned-directory-eof-expiry");
        let expiry = Instant::now() + Duration::from_millis(50);
        let visits = std::rc::Rc::new(std::cell::Cell::new(0));
        let observed = visits.clone();
        RECOVERY_DIRECTORY_STEP.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |step| {
                if step == "discover-next-complete" {
                    observed.set(observed.get() + 1);
                    std::thread::sleep(
                        expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
                    );
                }
            }))
        });
        let reset = Reset;
        assert!(discover_packs_until(&empty.0, expiry).is_err());
        assert_eq!(visits.get(), 1);
        drop(reset);
        assert!(
            discover_packs_until(&empty.0, Instant::now() + Duration::from_secs(2))
                .expect("normal empty native scan")
                .is_empty()
        );
    }
}

#[cfg(test)]
#[path = "extent_header_tests.rs"]
mod header_tests;
