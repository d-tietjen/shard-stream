use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use shard_stream_core::{
    BatchId, LogicalOffset, LogicalPartitionId, Placement, PlacementSequence, Reservation,
    RingEpoch, ShardId, TopicId, TopicPartition,
};

use crate::codec::{Decoder, push_u32, push_u64, push_u128};
use crate::{StorageError, StorageResult};

const EXTENT_MAGIC: &[u8; 4] = b"SSE1";
const EXTENT_VERSION: u8 = 1;
const EXTENT_FLAGS: u8 = 0;
const PREFIX_LEN: usize = 4 + 1 + 1 + 2 + 8;
const BODY_FIXED_LEN: usize = 16 + 4 + 16 + 16 + 16 + 4 + 4 + 8 + 16 + 8;
const HEADER_LEN: usize = PREFIX_LEN + BODY_FIXED_LEN;
const CRC_LEN: usize = 4;

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
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtentCatalogEntry {
    pub reservation: Reservation,
    pub payload_bytes: u64,
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
    pack_sequence: u64,
    payload_offset: u64,
    payload_len: usize,
}

#[derive(Debug)]
struct ActivePack {
    sequence: u64,
    file: File,
    bytes: u64,
}

#[derive(Debug)]
pub struct ShardStore {
    config: ShardStoreConfig,
    active: ActivePack,
    index: HashMap<TopicPartition, Vec<ExtentLocation>>,
    by_batch: HashMap<(TopicPartition, BatchId), ExtentLocation>,
    pack_paths: BTreeMap<u64, PathBuf>,
}

impl ShardStore {
    pub fn open(config: ShardStoreConfig) -> StorageResult<Self> {
        config.validate()?;
        fs::create_dir_all(&config.directory)?;
        let mut pack_paths = discover_packs(&config.directory)?;
        if pack_paths.is_empty() {
            let path = pack_path(&config.directory, 0);
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)?;
            pack_paths.insert(0, path);
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
        Ok(Self {
            config,
            active: ActivePack {
                sequence: last_sequence,
                file: active_file,
                bytes: active_bytes,
            },
            index,
            by_batch,
            pack_paths,
        })
    }

    pub fn append(
        &mut self,
        reservation: Reservation,
        payload: &[u8],
        sync: bool,
    ) -> StorageResult<()> {
        if reservation.placement.shard_id != self.config.shard_id {
            return Err(StorageError::InvalidInput(format!(
                "reservation targets shard {}, store owns {}",
                reservation.placement.shard_id, self.config.shard_id
            )));
        }
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

        let encoded = encode_extent(reservation, payload)?;
        if self.active.bytes > 0
            && self
                .active
                .bytes
                .checked_add(encoded.len() as u64)
                .is_none_or(|next| next > self.config.target_pack_bytes)
        {
            self.rotate()?;
        }

        let frame_offset = self.active.bytes;
        self.active.file.seek(SeekFrom::Start(frame_offset))?;
        if let Err(error) = self.active.file.write_all(&encoded) {
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
            .checked_add(encoded.len() as u64)
            .ok_or_else(|| StorageError::LimitExceeded("pack length overflow".into()))?;

        let location = ExtentLocation {
            reservation,
            pack_sequence: self.active.sequence,
            payload_offset: frame_offset + HEADER_LEN as u64,
            payload_len: payload.len(),
        };
        self.index.entry(key.0).or_default().push(location.clone());
        self.by_batch.insert(key, location);
        Ok(())
    }

    pub fn contains(&self, reservation: &Reservation) -> bool {
        self.by_batch.contains_key(&(
            TopicPartition::new(reservation.topic_id, reservation.partition_id),
            reservation.batch_id,
        ))
    }

    pub fn read_batch(&self, reservation: &Reservation) -> StorageResult<Option<StoredBatch>> {
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
        let path = self
            .pack_paths
            .get(&location.pack_sequence)
            .ok_or_else(|| StorageError::InvalidInput("indexed pack is missing".into()))?;
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(location.payload_offset))?;
        let mut payload = vec![0; location.payload_len];
        file.read_exact(&mut payload)?;
        Ok(Some(StoredBatch {
            reservation: *reservation,
            payload,
        }))
    }

    pub fn fetch(
        &self,
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
        max_bytes: usize,
    ) -> StorageResult<Vec<StoredBatch>> {
        if max_bytes == 0 {
            return Err(StorageError::InvalidInput(
                "fetch max_bytes must be nonzero".into(),
            ));
        }
        let Some(locations) = self.index.get(&topic_partition) else {
            return Ok(Vec::new());
        };

        let mut result = Vec::new();
        let mut used = 0usize;
        for location in locations {
            if location.reservation.last_offset < start_offset {
                continue;
            }
            let next = used
                .checked_add(location.payload_len)
                .ok_or_else(|| StorageError::LimitExceeded("fetch byte count overflow".into()))?;
            if !result.is_empty() && next > max_bytes {
                break;
            }
            let path = self
                .pack_paths
                .get(&location.pack_sequence)
                .ok_or_else(|| StorageError::InvalidInput("indexed pack is missing".into()))?;
            let mut file = File::open(path)?;
            file.seek(SeekFrom::Start(location.payload_offset))?;
            let mut payload = vec![0; location.payload_len];
            file.read_exact(&mut payload)?;
            result.push(StoredBatch {
                reservation: location.reservation,
                payload,
            });
            used = next;
        }
        Ok(result)
    }

    #[must_use]
    pub fn catalog(&self) -> Vec<ExtentCatalogEntry> {
        let mut entries = self
            .by_batch
            .values()
            .map(|location| ExtentCatalogEntry {
                reservation: location.reservation,
                payload_bytes: location.payload_len as u64,
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

        let mut stats = GarbageCollectionStats::default();
        for (sequence, path, extents) in removable {
            let bytes = path.metadata()?.len();
            fs::remove_file(&path)?;
            sync_directory(&self.config.directory)?;
            self.pack_paths.remove(&sequence);
            self.by_batch
                .retain(|_, location| location.pack_sequence != sequence);
            for locations in self.index.values_mut() {
                locations.retain(|location| location.pack_sequence != sequence);
            }
            self.index.retain(|_, locations| !locations.is_empty());
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
        self.active = ActivePack {
            sequence,
            file,
            bytes: 0,
        };
        Ok(())
    }
}

fn encode_extent(reservation: Reservation, payload: &[u8]) -> StorageResult<Vec<u8>> {
    let frame_len = HEADER_LEN
        .checked_add(payload.len())
        .and_then(|value| value.checked_add(CRC_LEN))
        .ok_or_else(|| StorageError::LimitExceeded("extent frame length overflow".into()))?;
    let frame_len_u64 = u64::try_from(frame_len)
        .map_err(|_| StorageError::LimitExceeded("extent frame exceeds u64".into()))?;
    let payload_len_u64 = u64::try_from(payload.len())
        .map_err(|_| StorageError::LimitExceeded("payload exceeds u64".into()))?;

    let mut bytes = Vec::with_capacity(frame_len);
    bytes.extend_from_slice(EXTENT_MAGIC);
    bytes.push(EXTENT_VERSION);
    bytes.push(EXTENT_FLAGS);
    bytes.extend_from_slice(&(HEADER_LEN as u16).to_le_bytes());
    push_u64(&mut bytes, frame_len_u64);
    push_u128(&mut bytes, reservation.topic_id.get());
    push_u32(&mut bytes, reservation.partition_id.get());
    push_u128(&mut bytes, reservation.batch_id.get());
    push_u128(&mut bytes, reservation.first_offset.get());
    push_u128(&mut bytes, reservation.last_offset.get());
    push_u32(&mut bytes, reservation.record_count.get());
    push_u32(&mut bytes, reservation.placement.shard_id.get());
    push_u64(&mut bytes, reservation.placement.ring_epoch.get());
    push_u128(&mut bytes, reservation.placement.sequence.get());
    push_u64(&mut bytes, payload_len_u64);
    debug_assert_eq!(bytes.len(), HEADER_LEN);
    bytes.extend_from_slice(payload);
    let checksum = crc32fast::hash(&bytes);
    push_u32(&mut bytes, checksum);
    Ok(bytes)
}

fn decode_extent(
    path: &Path,
    frame_offset: u64,
    frame: &[u8],
    expected_shard: ShardId,
) -> StorageResult<ExtentLocation> {
    if frame.len() < HEADER_LEN + CRC_LEN {
        return Err(StorageError::corrupt(path, frame_offset, "frame too short"));
    }
    let expected_crc = u32::from_le_bytes(
        frame[frame.len() - CRC_LEN..]
            .try_into()
            .expect("CRC width"),
    );
    let actual_crc = crc32fast::hash(&frame[..frame.len() - CRC_LEN]);
    if actual_crc != expected_crc {
        return Err(StorageError::corrupt(
            path,
            frame_offset,
            "extent checksum mismatch",
        ));
    }

    let mut decoder = Decoder::new(&frame[PREFIX_LEN..frame.len() - CRC_LEN]);
    let topic_id = TopicId::new(decoder.u128()?);
    let partition_id = LogicalPartitionId::new(decoder.u32()?);
    let batch_id = BatchId::new(decoder.u128()?);
    let first_offset = LogicalOffset::new(decoder.u128()?);
    let last_offset = LogicalOffset::new(decoder.u128()?);
    let record_count = std::num::NonZeroU32::new(decoder.u32()?)
        .ok_or_else(|| StorageError::corrupt(path, frame_offset, "zero record count"))?;
    let shard_id = ShardId::new(decoder.u32()?);
    let ring_epoch = RingEpoch::new(decoder.u64()?);
    let sequence = PlacementSequence::new(decoder.u128()?);
    let payload_len = usize::try_from(decoder.u64()?)
        .map_err(|_| StorageError::corrupt(path, frame_offset, "payload length overflow"))?;
    if shard_id != expected_shard {
        return Err(StorageError::corrupt(
            path,
            frame_offset,
            "extent is stored on the wrong shard",
        ));
    }
    if first_offset
        .get()
        .checked_add(u128::from(record_count.get()) - 1)
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
                shard_id,
                ring_epoch,
                sequence,
            },
        },
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
            .and_then(|value| value.checked_add(CRC_LEN as u64))
            .ok_or_else(|| StorageError::LimitExceeded("configured frame limit overflow".into()))?;
        if frame_len < (HEADER_LEN + CRC_LEN) as u64 || frame_len > max_frame_len {
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

fn sync_directory(path: &Path) -> StorageResult<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

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

    fn reservation(batch: u128, first: u128, shard: u32) -> Reservation {
        Reservation {
            topic_id: TopicId::new(1),
            partition_id: LogicalPartitionId::new(0),
            batch_id: BatchId::new(batch),
            first_offset: LogicalOffset::new(first),
            last_offset: LogicalOffset::new(first + 1),
            record_count: NonZeroU32::new(2).expect("nonzero"),
            placement: Placement {
                shard_id: ShardId::new(shard),
                ring_epoch: RingEpoch::new(1),
                sequence: PlacementSequence::new(batch),
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
    fn append_fetch_rotate_and_recover() {
        let temp = TempDir::new("recover");
        {
            let mut store = ShardStore::open(config(&temp.0, 150)).expect("open");
            store
                .append(reservation(0, 0, 3), b"first", true)
                .expect("append first");
            store
                .append(reservation(1, 2, 3), b"second", true)
                .expect("append second");
            assert!(store.pack_paths.len() >= 2);
        }

        let store = ShardStore::open(config(&temp.0, 150)).expect("reopen");
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
                .map(|batch| batch.payload.as_slice())
                .collect::<Vec<_>>(),
            vec![b"first".as_slice(), b"second".as_slice()]
        );
    }

    #[test]
    fn repairs_only_a_partial_active_tail() {
        let temp = TempDir::new("tail");
        let active_path;
        {
            let mut store = ShardStore::open(config(&temp.0, 4096)).expect("open");
            store
                .append(reservation(0, 0, 3), b"complete", true)
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
                .append(reservation(0, 0, 3), b"payload", true)
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
