use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{LogicalOffset, Reservation, ShardId, TopicPartition};
use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::{StorageError, StorageResult};

const MANIFEST_VERSION: u8 = 3;
const CHECKSUM_BLAKE3: &str = "blake3";
const COPY_BUFFER_BYTES: usize = 1024 * 1024;
const MAX_MANIFEST_BYTES: u64 = 64 * 1024 * 1024;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierObject {
    pub pack_sequence: u64,
    pub object_key: String,
    pub bytes: u64,
    pub checksum_algorithm: String,
    pub checksum: String,
    pub created_unix_ms: u64,
    pub extents: Vec<TierExtent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierExtent {
    pub topic_id: String,
    pub partition_id: u32,
    pub first_offset: u64,
    pub last_offset: u64,
    pub virtual_lane_id: u32,
    pub ring_epoch: u64,
    pub leader_epoch: u64,
}

impl TierExtent {
    #[must_use]
    pub fn from_reservation(reservation: Reservation) -> Self {
        Self {
            topic_id: reservation.topic_id.to_string(),
            partition_id: reservation.partition_id.get(),
            first_offset: reservation.first_offset.get(),
            last_offset: reservation.last_offset.get(),
            virtual_lane_id: reservation.placement.virtual_lane_id.get(),
            ring_epoch: reservation.placement.ring_epoch.get(),
            leader_epoch: reservation.placement.leader_epoch.get(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierManifest {
    pub format_version: u8,
    pub generation: u64,
    pub shard_id: u32,
    pub objects: Vec<TierObject>,
}

impl TierManifest {
    fn empty(shard_id: ShardId) -> Self {
        Self {
            format_version: MANIFEST_VERSION,
            generation: 0,
            shard_id: shard_id.get(),
            objects: Vec::new(),
        }
    }

    fn validate(&self, shard_id: ShardId) -> StorageResult<()> {
        if self.format_version != MANIFEST_VERSION {
            return Err(tier_corrupt(format!(
                "unsupported tier manifest version {}",
                self.format_version
            )));
        }
        if self.shard_id != shard_id.get() {
            return Err(tier_corrupt(format!(
                "tier manifest belongs to shard {}, expected {}",
                self.shard_id, shard_id
            )));
        }
        let mut previous = None;
        for object in &self.objects {
            validate_object_key(&object.object_key)?;
            if object.bytes == 0
                || object.checksum.len() != 64
                || object.checksum_algorithm != CHECKSUM_BLAKE3
                || object.extents.is_empty()
            {
                return Err(tier_corrupt(
                    "tier manifest contains invalid object metadata",
                ));
            }
            for extent in &object.extents {
                if extent.topic_id.parse::<u128>().is_err()
                    || extent.first_offset > extent.last_offset
                {
                    return Err(tier_corrupt(
                        "tier manifest contains invalid extent coverage",
                    ));
                }
            }
            if previous.is_some_and(|sequence| sequence >= object.pack_sequence) {
                return Err(tier_corrupt(
                    "tier manifest pack sequences are not strictly increasing",
                ));
            }
            previous = Some(object.pack_sequence);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct LocalObjectTier {
    root: PathBuf,
    shard_id: ShardId,
    manifest: TierManifest,
}

impl LocalObjectTier {
    pub fn open(root: impl AsRef<Path>, shard_id: ShardId) -> StorageResult<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("objects").join(shard_directory(shard_id)))?;
        fs::create_dir_all(root.join("manifests").join(shard_directory(shard_id)))?;
        let manifest = load_current_manifest(&root, shard_id)?;
        manifest.validate(shard_id)?;
        for object in &manifest.objects {
            verify_object(&root, object)?;
        }
        Ok(Self {
            root,
            shard_id,
            manifest,
        })
    }

    #[must_use]
    pub fn manifest(&self) -> &TierManifest {
        &self.manifest
    }

    pub fn offload_pack(
        &mut self,
        pack_sequence: u64,
        source: impl AsRef<Path>,
        extents: Vec<TierExtent>,
    ) -> StorageResult<TierObject> {
        if extents.is_empty() {
            return Err(StorageError::InvalidInput(
                "object manifest coverage cannot be empty".into(),
            ));
        }
        if let Some(existing) = self
            .manifest
            .objects
            .iter()
            .find(|object| object.pack_sequence == pack_sequence)
        {
            let observed = hash_file(source.as_ref())?;
            if existing.bytes != observed.0 || existing.checksum != observed.1 {
                return Err(tier_corrupt(format!(
                    "sealed pack {pack_sequence} differs from its authoritative tier manifest"
                )));
            }
            verify_object(&self.root, existing)?;
            if existing.extents != extents {
                return Err(tier_corrupt(format!(
                    "sealed pack {pack_sequence} has different extent coverage"
                )));
            }
            return Ok(existing.clone());
        }
        if self
            .manifest
            .objects
            .last()
            .is_some_and(|object| object.pack_sequence >= pack_sequence)
        {
            return Err(StorageError::InvalidInput(
                "tier packs must be published in increasing sequence order".into(),
            ));
        }

        let source = source.as_ref();
        let metadata = source.metadata()?;
        if !metadata.is_file() || metadata.len() == 0 {
            return Err(StorageError::InvalidInput(
                "only nonempty sealed pack files can be offloaded".into(),
            ));
        }
        let object_key = format!(
            "objects/{}/pack-{pack_sequence:020}.sse",
            shard_directory(self.shard_id)
        );
        let destination = object_path(&self.root, &object_key)?;
        let (bytes, checksum) = copy_object_atomically(source, &destination)?;
        let object = TierObject {
            pack_sequence,
            object_key,
            bytes,
            checksum_algorithm: CHECKSUM_BLAKE3.into(),
            checksum,
            created_unix_ms: now_unix_ms()?,
            extents,
        };
        verify_object(&self.root, &object)?;

        let mut next = self.manifest.clone();
        next.generation = next.generation.checked_add(1).ok_or_else(|| {
            StorageError::LimitExceeded("tier manifest generation exhausted".into())
        })?;
        next.objects.push(object.clone());
        publish_manifest(&self.root, self.shard_id, self.manifest.generation, &next)?;
        self.manifest = next;
        Ok(object)
    }

    /// Returns the exclusive object-backed high watermark for a partition
    /// within this physical-lane manifest. Callers combine all lane manifests
    /// before using the result as a partition-wide watermark.
    #[must_use]
    pub fn covered_through(&self, topic_partition: TopicPartition) -> LogicalOffset {
        let mut ranges = self
            .manifest
            .objects
            .iter()
            .flat_map(|object| &object.extents)
            .filter(|extent| {
                extent.topic_id == topic_partition.topic_id.to_string()
                    && extent.partition_id == topic_partition.partition_id.get()
            })
            .map(|extent| (extent.first_offset, extent.last_offset))
            .collect::<Vec<_>>();
        ranges.sort_unstable();
        let mut end = 0u64;
        for (first, last) in ranges {
            if first > end {
                break;
            }
            end = end.max(last.saturating_add(1));
        }
        LogicalOffset::new(end)
    }
}

fn load_current_manifest(root: &Path, shard_id: ShardId) -> StorageResult<TierManifest> {
    let current = current_path(root, shard_id);
    let generation = match fs::read_to_string(&current) {
        Ok(value) => value
            .trim()
            .parse::<u64>()
            .map_err(|_| tier_corrupt("invalid tier CURRENT generation"))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(TierManifest::empty(shard_id));
        }
        Err(error) => return Err(error.into()),
    };
    let path = manifest_path(root, shard_id, generation);
    let metadata = path.metadata()?;
    if metadata.len() > MAX_MANIFEST_BYTES {
        return Err(StorageError::LimitExceeded(
            "tier manifest exceeds the read limit".into(),
        ));
    }
    let bytes = fs::read(path)?;
    let manifest: TierManifest = serde_json::from_slice(&bytes)
        .map_err(|error| tier_corrupt(format!("invalid tier manifest: {error}")))?;
    if manifest.generation != generation {
        return Err(tier_corrupt(
            "tier CURRENT and manifest generation disagree",
        ));
    }
    Ok(manifest)
}

fn publish_manifest(
    root: &Path,
    shard_id: ShardId,
    expected_generation: u64,
    manifest: &TierManifest,
) -> StorageResult<()> {
    manifest.validate(shard_id)?;
    let _lock = ManifestUpdateLock::acquire(root, shard_id)?;
    let observed = load_current_manifest(root, shard_id)?;
    if observed.generation != expected_generation
        || manifest.generation != expected_generation.saturating_add(1)
    {
        return Err(StorageError::InvalidInput(format!(
            "conditional manifest update failed: expected generation {expected_generation}, observed {}",
            observed.generation
        )));
    }
    let encoded = serde_json::to_vec(manifest).map_err(|error| {
        StorageError::InvalidInput(format!("manifest encoding failed: {error}"))
    })?;
    if encoded.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(StorageError::LimitExceeded(
            "tier manifest exceeds the write limit".into(),
        ));
    }
    let path = manifest_path(root, shard_id, manifest.generation);
    match OpenOptions::new().create_new(true).write(true).open(&path) {
        Ok(mut file) => {
            file.write_all(&encoded)?;
            file.sync_all()?;
            sync_directory(path.parent().expect("manifest has parent"))?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if fs::read(&path)? != encoded {
                return Err(StorageError::corrupt(
                    &path,
                    0,
                    "immutable manifest generation has different content",
                ));
            }
        }
        Err(error) => return Err(error.into()),
    }

    let current = current_path(root, shard_id);
    let temporary = temporary_path(&current);
    let mut pointer = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    writeln!(pointer, "{}", manifest.generation)?;
    pointer.sync_all()?;
    fs::rename(&temporary, &current)?;
    sync_directory(current.parent().expect("CURRENT has parent"))?;
    Ok(())
}

struct ManifestUpdateLock {
    file: File,
}

impl ManifestUpdateLock {
    fn acquire(root: &Path, shard_id: ShardId) -> StorageResult<Self> {
        let path = root
            .join("manifests")
            .join(shard_directory(shard_id))
            .join("UPDATE.lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        file.lock_exclusive()?;
        Ok(Self { file })
    }
}

impl Drop for ManifestUpdateLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn copy_object_atomically(source: &Path, destination: &Path) -> StorageResult<(u64, String)> {
    if destination.exists() {
        let source_digest = hash_file(source)?;
        let destination_digest = hash_file(destination)?;
        if source_digest != destination_digest {
            return Err(StorageError::corrupt(
                destination,
                0,
                "immutable object already exists with different content",
            ));
        }
        return Ok(destination_digest);
    }
    let temporary = temporary_path(destination);
    let result = (|| {
        let mut input = File::open(source)?;
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        let mut digest = blake3::Hasher::new();
        let mut bytes = 0u64;
        let mut buffer = vec![0u8; COPY_BUFFER_BYTES];
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            output.write_all(&buffer[..read])?;
            digest.update(&buffer[..read]);
            bytes = bytes
                .checked_add(read as u64)
                .ok_or_else(|| StorageError::LimitExceeded("object size overflow".into()))?;
        }
        output.sync_all()?;
        fs::rename(&temporary, destination)?;
        sync_directory(destination.parent().expect("object has parent"))?;
        Ok((bytes, digest.finalize().to_hex().to_string()))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn verify_object(root: &Path, object: &TierObject) -> StorageResult<()> {
    let path = object_path(root, &object.object_key)?;
    let (bytes, checksum) = hash_file(&path)?;
    if bytes != object.bytes || checksum != object.checksum {
        return Err(StorageError::corrupt(
            &path,
            0,
            "tier object length or checksum mismatch",
        ));
    }
    Ok(())
}

fn hash_file(path: &Path) -> StorageResult<(u64, String)> {
    let mut file = File::open(path)?;
    let mut digest = blake3::Hasher::new();
    let mut bytes = 0u64;
    let mut buffer = vec![0u8; COPY_BUFFER_BYTES];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        bytes = checked_object_bytes(bytes, read)?;
    }
    Ok((bytes, digest.finalize().to_hex().to_string()))
}

fn checked_object_bytes(current: u64, read: usize) -> StorageResult<u64> {
    current
        .checked_add(read as u64)
        .ok_or_else(|| StorageError::LimitExceeded("object size overflow".into()))
}

fn object_path(root: &Path, key: &str) -> StorageResult<PathBuf> {
    validate_object_key(key)?;
    Ok(root.join(key))
}

fn validate_object_key(key: &str) -> StorageResult<()> {
    if key.is_empty()
        || Path::new(key)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(tier_corrupt("tier manifest contains an unsafe object key"));
    }
    Ok(())
}

fn manifest_path(root: &Path, shard_id: ShardId, generation: u64) -> PathBuf {
    root.join("manifests")
        .join(shard_directory(shard_id))
        .join(format!("manifest-{generation:020}.json"))
}

fn current_path(root: &Path, shard_id: ShardId) -> PathBuf {
    root.join("manifests")
        .join(shard_directory(shard_id))
        .join("CURRENT")
}

fn shard_directory(shard_id: ShardId) -> String {
    format!("shard-{}", shard_id.get())
}

fn temporary_path(path: &Path) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("tier");
    path.with_file_name(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        sequence
    ))
}

fn sync_directory(path: &Path) -> StorageResult<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn now_unix_ms() -> StorageResult<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| StorageError::InvalidInput(format!("system clock before epoch: {error}")))?
        .as_millis();
    u64::try_from(millis)
        .map_err(|_| StorageError::LimitExceeded("system time exceeds u64 milliseconds".into()))
}

fn tier_corrupt(reason: impl Into<String>) -> StorageError {
    StorageError::Corrupt {
        path: "tier-manifest".into(),
        offset: 0,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn coverage() -> Vec<TierExtent> {
        vec![TierExtent {
            topic_id: "1".into(),
            partition_id: 0,
            first_offset: 0,
            last_offset: 0,
            virtual_lane_id: 0,
            ring_epoch: 1,
            leader_epoch: 1,
        }]
    }

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "shard-stream-tier-{}-{nonce}-{sequence}",
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

    #[test]
    fn publishes_immutable_objects_and_recovers_from_current_manifest() {
        let temp = TempDir::new();
        let source = temp.0.join("source.sse");
        fs::write(&source, b"sealed pack bytes").expect("source");
        let root = temp.0.join("tier");
        let mut tier = LocalObjectTier::open(&root, ShardId::new(2)).expect("open");
        let object = tier.offload_pack(7, &source, coverage()).expect("offload");
        assert_eq!(object.bytes, 17);
        assert_eq!(tier.manifest().generation, 1);

        let reopened = LocalObjectTier::open(&root, ShardId::new(2)).expect("reopen");
        assert_eq!(reopened.manifest(), tier.manifest());
        assert_eq!(reopened.manifest().objects, vec![object]);
    }

    #[test]
    fn rejects_object_corruption_on_recovery() {
        let temp = TempDir::new();
        let source = temp.0.join("source.sse");
        fs::write(&source, b"sealed pack bytes").expect("source");
        let root = temp.0.join("tier");
        let mut tier = LocalObjectTier::open(&root, ShardId::new(0)).expect("open");
        let object = tier.offload_pack(0, &source, coverage()).expect("offload");
        fs::write(root.join(object.object_key), b"corrupt").expect("corrupt");
        assert!(matches!(
            LocalObjectTier::open(&root, ShardId::new(0)),
            Err(StorageError::Corrupt { .. })
        ));
    }

    #[test]
    fn conditional_manifest_publish_rejects_a_stale_writer() {
        let temp = TempDir::new();
        let root = temp.0.join("tier");
        let first_source = temp.0.join("first.sse");
        let second_source = temp.0.join("second.sse");
        fs::write(&first_source, b"first sealed pack").expect("first");
        fs::write(&second_source, b"second sealed pack").expect("second");
        let mut first = LocalObjectTier::open(&root, ShardId::new(0)).expect("first open");
        let mut stale = LocalObjectTier::open(&root, ShardId::new(0)).expect("stale open");
        first
            .offload_pack(0, &first_source, coverage())
            .expect("first publish");
        assert!(matches!(
            stale.offload_pack(1, &second_source, coverage()),
            Err(StorageError::InvalidInput(_))
        ));
    }
}
