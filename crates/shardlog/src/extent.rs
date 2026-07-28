use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{IoSlice, Read, Seek, SeekFrom, Write};
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
struct ExtentLocation {
    reservation: Reservation,
    producer: Option<ProducerSequence>,
    atomic_group: Option<AtomicGroupSequence>,
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
    pack_readers: BTreeMap<u64, File>,
    read_buffer_pool: ReadBufferPool,
}

impl ShardStore {
    pub fn open(config: ShardStoreConfig) -> StorageResult<Self> {
        config.validate()?;
        let directory_existed = config.directory.exists();
        fs::create_dir_all(&config.directory)?;
        if !directory_existed && let Some(parent) = config.directory.parent() {
            sync_directory(parent)?;
        }
        let lock_path = config.directory.join(".shard-store.lock");
        let lock_existed = lock_path.exists();
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        fs2::FileExt::try_lock_exclusive(&lock_file).map_err(|error| {
            StorageError::InvalidInput(format!("shard store directory is already open: {error}"))
        })?;
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
        let pack_readers = pack_paths
            .iter()
            .map(|(&sequence, path)| {
                OpenOptions::new()
                    .read(true)
                    .open(path)
                    .map(|file| (sequence, file))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
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
            pack_readers,
            read_buffer_pool: ReadBufferPool::new(),
        })
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

    pub fn plan_fetch(
        &self,
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
        max_bytes: usize,
        max_batches: usize,
    ) -> StorageResult<Vec<StoredBatchMetadata>> {
        if max_bytes == 0 || max_batches == 0 {
            return Err(StorageError::InvalidInput(
                "fetch byte and batch limits must be nonzero".into(),
            ));
        }
        let Some(locations) = self.index.get(&topic_partition) else {
            return Ok(Vec::new());
        };

        let first =
            locations.partition_point(|location| location.reservation.last_offset < start_offset);
        let capacity = FETCH_PLAN_INITIAL_CAPACITY
            .min(max_batches)
            .min(locations.len().saturating_sub(first));
        let mut result = Vec::with_capacity(capacity);
        let mut used = 0usize;
        for location in &locations[first..] {
            if result.len() == max_batches {
                break;
            }
            let next = used
                .checked_add(location.payload_len)
                .ok_or_else(|| StorageError::LimitExceeded("fetch byte count overflow".into()))?;
            if !result.is_empty() && next > max_bytes {
                break;
            }
            result.push(self.metadata_for(location));
            used = next;
        }
        Ok(result)
    }

    pub fn read_fetch_plan(
        &mut self,
        planned: &[StoredBatchMetadata],
    ) -> StorageResult<Vec<StoredBatch>> {
        if planned.iter().any(|metadata| {
            metadata.store_token != self.store_token || metadata.plan_epoch != self.plan_epoch
        }) {
            return Err(StorageError::InvalidInput(
                "fetch plan does not belong to the current store generation".into(),
            ));
        }
        let mut result = Vec::with_capacity(planned.len());
        let mut first = 0usize;
        while first < planned.len() {
            let mut end = first + 1;
            let mut range_end = payload_end(&planned[first])?;
            while end < planned.len() {
                let next = planned[end];
                if next.pack_sequence != planned[first].pack_sequence
                    || next.payload_offset < range_end
                    || next.payload_offset - range_end > MAX_COALESCED_READ_GAP
                {
                    break;
                }
                range_end = payload_end(&next)?;
                end += 1;
            }

            let range_start = planned[first].payload_offset;
            let range_len = usize::try_from(range_end - range_start)
                .map_err(|_| StorageError::LimitExceeded("fetch range exceeds usize".into()))?;
            let reader = self
                .pack_readers
                .get(&planned[first].pack_sequence)
                .ok_or_else(|| StorageError::InvalidInput("planned pack is missing".into()))?;
            let mut buffer = self.read_buffer_pool.take(range_len);
            if let Err(error) = read_exact_at(reader, &mut buffer, range_start) {
                self.read_buffer_pool.recycle_now(buffer);
                return Err(error.into());
            }
            let buffer = self.read_buffer_pool.lease_bytes(buffer);

            for metadata in &planned[first..end] {
                let payload_start = usize::try_from(metadata.payload_offset - range_start)
                    .map_err(|_| {
                        StorageError::LimitExceeded("payload offset exceeds usize".into())
                    })?;
                let payload_end = payload_start
                    .checked_add(metadata.payload_len)
                    .ok_or_else(|| StorageError::LimitExceeded("payload range overflow".into()))?;
                result.push(StoredBatch {
                    reservation: metadata.reservation,
                    producer: metadata.producer,
                    atomic_group: metadata.atomic_group,
                    payload: buffer.slice(payload_start..payload_end),
                });
            }
            first = end;
        }
        Ok(result)
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
                    && locations.iter().all(|location| {
                        log_starts
                            .get(&TopicPartition::new(
                                location.reservation.topic_id,
                                location.reservation.partition_id,
                            ))
                            .is_some_and(|log_start| location.reservation.last_offset < *log_start)
                    }))
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
            self.pack_readers.remove(&sequence);
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
        let reader = OpenOptions::new().read(true).open(&path)?;
        self.pack_paths.insert(sequence, path.clone());
        self.pack_readers.insert(sequence, reader);
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
}

fn payload_end(metadata: &StoredBatchMetadata) -> StorageResult<u64> {
    metadata
        .payload_offset
        .checked_add(
            u64::try_from(metadata.payload_len)
                .map_err(|_| StorageError::LimitExceeded("payload length exceeds u64".into()))?,
        )
        .ok_or_else(|| StorageError::LimitExceeded("payload end overflow".into()))
}

fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    while !buffer.is_empty() {
        match file.read_at(buffer, offset)? {
            0 => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "planned extent ended before its payload",
                ));
            }
            read => {
                offset = offset
                    .checked_add(read as u64)
                    .ok_or_else(|| std::io::Error::other("read offset overflow"))?;
                buffer = &mut buffer[read..];
            }
        }
    }
    Ok(())
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

fn decode_extent(
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
    if decoder.take(payload_len).is_err() || !decoder.is_empty() {
        return Err(StorageError::corrupt(
            path,
            frame_offset,
            "payload length does not match frame",
        ));
    }

    Ok(ExtentLocation {
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
        pack_sequence: 0,
        payload_offset: frame_offset + HEADER_LEN as u64,
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
        let mut location = decode_extent(path, offset, &frame, expected_shard)?;
        location.pack_sequence = pack_sequence;
        let topic_partition = TopicPartition::new(
            location.reservation.topic_id,
            location.reservation.partition_id,
        );
        let key = (topic_partition, location.reservation.batch_id);
        if by_batch.insert(key, location.clone()).is_some() {
            return Err(StorageError::corrupt(path, offset, "duplicate batch ID"));
        }
        index.entry(topic_partition).or_default().push(location);
        offset += frame_len;
    }
    Ok(())
}

fn validate_prefix(path: &Path, offset: u64, prefix: &[u8; PREFIX_LEN]) -> StorageResult<()> {
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

fn discover_packs(directory: &Path) -> StorageResult<BTreeMap<u64, PathBuf>> {
    let mut packs = BTreeMap::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(sequence) = name
            .strip_prefix("pack-")
            .and_then(|name| name.strip_suffix(".sse"))
            .and_then(|digits| digits.parse::<u64>().ok())
        else {
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

fn pending_pack_deletion_path(path: &Path) -> PathBuf {
    path.with_extension("sse.gc-pending")
}

fn cleanup_pending_pack_deletions(directory: &Path) -> StorageResult<()> {
    let pending = fs::read_dir(directory)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("pack-") && name.ends_with(".sse.gc-pending"))
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
}
