use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::mem::size_of;
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

#[path = "tier_recovery_paths.rs"]
mod recovery_paths;
pub use recovery_paths::{
    RecoveryTierPathCost, RecoveryTierPathError, RecoveryTierPathLimits,
    recovery_tier_manifest_path_matches, recovery_tier_object_path_matches,
    recovery_tier_path_inspection_cost,
};

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

    /// Validates the native version, shard, object order and extent encoding.
    pub fn validate(&self, shard_id: ShardId) -> StorageResult<()> {
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
    // A concrete pre-open lease may move here; never reacquire its own flock.
    #[cfg(unix)]
    recovery_guard: Option<TierRecoveryGuard>,
}

impl LocalObjectTier {
    /// Fixed native value slots before recovery open; paths/JSON/object values
    /// and actual verification reads must be admitted separately by the owner.
    pub const fn recovery_open_fixed_bytes() -> usize {
        size_of::<Self>() + size_of::<TierManifest>()
    }

    /// Actual native verification scratch allocated by `hash_file` per object.
    pub const fn recovery_hash_buffer_bytes() -> usize {
        COPY_BUFFER_BYTES
    }

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
            #[cfg(unix)]
            recovery_guard: None,
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
        // The real native writer lease spans object copy AND publication.
        // Older writers that lock publication only are not made quiescent by
        // this change; a recovery snapshot must still retain that distinction.
        #[cfg(unix)]
        let temporary;
        #[cfg(unix)]
        let publication: &ManifestUpdateLock = match &self.recovery_guard {
            Some(guard) => &guard.publication,
            None => {
                temporary = ManifestUpdateLock::acquire(&self.root, self.shard_id)?;
                &temporary
            }
        };
        #[cfg(not(unix))]
        let temporary = ManifestUpdateLock::acquire(&self.root, self.shard_id)?;
        #[cfg(not(unix))]
        let publication = &temporary;
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
        publish_manifest(
            &self.root,
            self.shard_id,
            self.manifest.generation,
            &next,
            publication,
        )?;
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
        Ok(value) => parse_recovery_tier_current(&value)
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
    let manifest = decode_recovery_tier_manifest(&bytes)
        .map_err(|error| tier_corrupt(format!("invalid tier manifest: {error}")))?;
    if manifest.generation != generation {
        return Err(tier_corrupt(
            "tier CURRENT and manifest generation disagree",
        ));
    }
    Ok(manifest)
}

/// Parses native CURRENT text without reading a path or inferring selection.
///
/// This is the ordinary loader's exact trim/u64 grammar. A byte caller must
/// separately validate UTF-8, bound input/error work and retain its original
/// expiry. No missing-file or empty-manifest fallback is provided here.
pub fn parse_recovery_tier_current(value: &str) -> Result<u64, std::num::ParseIntError> {
    value.trim().parse::<u64>()
}

/// Decodes the original native manifest type, without I/O or validation policy.
///
/// Admit bounded JSON shape/native values before calling, then call the actual
/// `TierManifest::validate` and join CURRENT/generation/source/object coverage.
/// The ordinary loader's error policy is unchanged by this shared decoder.
pub fn decode_recovery_tier_manifest(bytes: &[u8]) -> Result<TierManifest, serde_json::Error> {
    serde_json::from_slice(bytes)
}

fn publish_manifest(
    root: &Path,
    shard_id: ShardId,
    expected_generation: u64,
    manifest: &TierManifest,
    _publication: &ManifestUpdateLock,
) -> StorageResult<()> {
    manifest.validate(shard_id)?;
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

#[derive(Debug)]
struct ManifestUpdateLock {
    file: File,
}

/// Actual retained native tier writer lease acquired before a recovery census.
///
/// This excludes cooperating copy+publication writers in this protocol only.
/// Older publication-only writers may still create objects before this lease.
#[cfg(unix)]
#[derive(Debug)]
pub struct TierRecoveryGuard {
    publication: std::sync::Arc<ManifestUpdateLock>,
    root: PathBuf,
    shard_id: ShardId,
}

#[cfg(unix)]
impl TierRecoveryGuard {
    /// Acquires the actual native lease without blocking or starting a new clock.
    ///
    /// `file` must be the pinned regular native UPDATE.lock; its inode is checked
    /// against the exact native path. This is a lock, not directory authority.
    pub fn try_acquire(file: File, root: PathBuf, shard_id: ShardId) -> StorageResult<Self> {
        validate_recovery_lock(&file, &root, shard_id)?;
        file.try_lock_exclusive()?;
        // Construct the unlocking RAII value only after actual acquisition.
        let result = Self {
            publication: std::sync::Arc::new(ManifestUpdateLock { file }),
            root,
            shard_id,
        };
        result.validate()?;
        Ok(result)
    }

    /// Checks that the real lease still names the original native lock inode.
    pub fn validate(&self) -> StorageResult<()> {
        validate_recovery_lock(&self.publication.file, &self.root, self.shard_id)
    }
}

#[cfg(unix)]
fn validate_recovery_lock(file: &File, root: &Path, shard_id: ShardId) -> StorageResult<()> {
    use std::os::unix::fs::MetadataExt;
    let path = root
        .join("manifests")
        .join(shard_directory(shard_id))
        .join("UPDATE.lock");
    let named = fs::symlink_metadata(path)?;
    let actual = file.metadata()?;
    if !named.is_file()
        || named.file_type().is_symlink()
        || named.dev() != actual.dev()
        || named.ino() != actual.ino()
    {
        return Err(tier_corrupt("tier recovery lease identity changed"));
    }
    Ok(())
}

#[cfg(unix)]
impl LocalObjectTier {
    /// Opens while the concrete recovery owner retains the SAME native lease.
    ///
    /// A private Arc keeps the original unlocking value alive through both the
    /// storage owner and normal worker. No raw-file clone can silently unlock
    /// this publication lease when an opening error destroys an intermediate.
    pub fn open_under_census(
        root: PathBuf,
        shard_id: ShardId,
        guard: &TierRecoveryGuard,
    ) -> StorageResult<Self> {
        if root != guard.root || shard_id != guard.shard_id {
            return Err(tier_corrupt("tier recovery lease has another root/shard"));
        }
        guard.validate()?;
        let mut result = Self::open(root, shard_id)?;
        result.recovery_guard = Some(TierRecoveryGuard {
            publication: std::sync::Arc::clone(&guard.publication),
            root: guard.root.clone(),
            shard_id,
        });
        Ok(result)
    }

    /// Opens using the SAME concrete lease held by the native recovery census.
    ///
    /// This still verifies native CURRENT and every referenced object. It does
    /// not restore object-only batches into the engine's primary catalog.
    pub fn open_after_census(
        root: PathBuf,
        shard_id: ShardId,
        guard: TierRecoveryGuard,
    ) -> StorageResult<Self> {
        if root != guard.root || shard_id != guard.shard_id {
            return Err(tier_corrupt("tier recovery lease has another root/shard"));
        }
        guard.validate()?;
        let mut result = Self::open(root, shard_id)?;
        result.recovery_guard = Some(guard);
        Ok(result)
    }
}

#[cfg(unix)]
impl LocalObjectTier {
    /// Native tier opening bounded by an existing absolute recovery expiry.
    ///
    /// This creates no clock, directory authority or serving permission. The
    /// owning recovery operation must preadmit all native materialization.
    pub fn open_until(
        root: impl AsRef<Path>,
        shard_id: ShardId,
        expiry: std::time::Instant,
    ) -> StorageResult<Self> {
        tier_recovery_clock(expiry)?;
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("objects").join(shard_directory(shard_id)))?;
        tier_recovery_clock(expiry)?;
        fs::create_dir_all(root.join("manifests").join(shard_directory(shard_id)))?;
        tier_recovery_clock(expiry)?;
        let manifest = load_current_manifest_until(&root, shard_id, expiry)?;
        manifest.validate(shard_id)?;
        for object in &manifest.objects {
            tier_recovery_clock(expiry)?;
            verify_object_until(&root, object, expiry)?;
        }
        tier_recovery_clock(expiry)?;
        Ok(Self {
            root,
            shard_id,
            manifest,
            recovery_guard: None,
        })
    }

    /// Verifies CURRENT objects under the SAME retained native lease and Instant.
    pub fn open_under_census_until(
        root: PathBuf,
        shard_id: ShardId,
        guard: &TierRecoveryGuard,
        expiry: std::time::Instant,
    ) -> StorageResult<Self> {
        tier_recovery_clock(expiry)?;
        if root != guard.root || shard_id != guard.shard_id {
            return Err(tier_corrupt("tier recovery lease has another root/shard"));
        }
        guard.validate()?;
        tier_recovery_clock(expiry)?;
        let mut result = Self::open_until(root, shard_id, expiry)?;
        result.recovery_guard = Some(TierRecoveryGuard {
            publication: std::sync::Arc::clone(&guard.publication),
            root: guard.root.clone(),
            shard_id,
        });
        tier_recovery_clock(expiry)?;
        Ok(result)
    }

    /// Offloads using native hash/coverage/CURRENT rules under an original Instant.
    ///
    /// A started syscall may settle late. No subsequent copy, verification,
    /// manifest publication or pack operation starts after the observed expiry;
    /// a late error may follow durable native publication and is never unchanged.
    pub fn offload_pack_until(
        &mut self,
        pack_sequence: u64,
        source: impl AsRef<Path>,
        extents: Vec<TierExtent>,
        expiry: std::time::Instant,
    ) -> StorageResult<TierObject> {
        tier_recovery_clock(expiry)?;
        if extents.is_empty() {
            return Err(StorageError::InvalidInput(
                "object manifest coverage cannot be empty".into(),
            ));
        }
        // The real native writer lease spans object copy AND publication.
        // Older writers that lock publication only are not made quiescent by
        // this change; a recovery snapshot must still retain that distinction.
        #[cfg(unix)]
        let temporary;
        #[cfg(unix)]
        let publication: &ManifestUpdateLock = match &self.recovery_guard {
            Some(guard) => &guard.publication,
            None => {
                temporary = ManifestUpdateLock::acquire(&self.root, self.shard_id)?;
                &temporary
            }
        };
        #[cfg(not(unix))]
        let temporary = ManifestUpdateLock::acquire(&self.root, self.shard_id)?;
        #[cfg(not(unix))]
        let publication = &temporary;
        if let Some(existing) = self
            .manifest
            .objects
            .iter()
            .find(|object| object.pack_sequence == pack_sequence)
        {
            let observed = hash_file_until(source.as_ref(), expiry)?;
            if existing.bytes != observed.0 || existing.checksum != observed.1 {
                return Err(tier_corrupt(format!(
                    "sealed pack {pack_sequence} differs from its authoritative tier manifest"
                )));
            }
            verify_object_until(&self.root, existing, expiry)?;
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

        tier_recovery_clock(expiry)?;
        let source = source.as_ref();
        let metadata = source.metadata()?;
        if !metadata.is_file() || metadata.len() == 0 {
            return Err(StorageError::InvalidInput(
                "only nonempty sealed pack files can be offloaded".into(),
            ));
        }
        tier_recovery_clock(expiry)?;
        let object_key = format!(
            "objects/{}/pack-{pack_sequence:020}.sse",
            shard_directory(self.shard_id)
        );
        let destination = object_path(&self.root, &object_key)?;
        let (bytes, checksum) = copy_object_atomically_until(source, &destination, expiry)?;
        tier_recovery_clock(expiry)?;
        let object = TierObject {
            pack_sequence,
            object_key,
            bytes,
            checksum_algorithm: CHECKSUM_BLAKE3.into(),
            checksum,
            created_unix_ms: now_unix_ms()?,
            extents,
        };
        verify_object_until(&self.root, &object, expiry)?;

        tier_recovery_clock(expiry)?;
        let mut next = self.manifest.clone();
        next.generation = next.generation.checked_add(1).ok_or_else(|| {
            StorageError::LimitExceeded("tier manifest generation exhausted".into())
        })?;
        next.objects.push(object.clone());
        tier_recovery_clock(expiry)?;
        publish_manifest_until(
            &self.root,
            self.shard_id,
            self.manifest.generation,
            &next,
            publication,
            expiry,
        )?;
        // If the final native directory sync settled late, retain the old
        // in-memory view and return a truthful refusal; never renew the clock.
        tier_recovery_clock(expiry)?;
        self.manifest = next;
        Ok(object)
    }
}

#[cfg(unix)]
fn tier_recovery_clock(expiry: std::time::Instant) -> StorageResult<()> {
    if std::time::Instant::now() >= expiry {
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
    static TIER_RECOVERY_STEP: std::cell::RefCell<Option<Box<dyn FnMut(&str)>>> =
        const { std::cell::RefCell::new(None) };
}
#[cfg(all(unix, test))]
fn tier_recovery_test_step(step: &str) {
    TIER_RECOVERY_STEP.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook(step);
        }
    });
}

#[cfg(all(unix, test))]
thread_local! {
    static TIER_COPY_STEP: std::cell::RefCell<Option<Box<dyn FnMut(&str, &Path)>>> =
        const { std::cell::RefCell::new(None) };
}
#[cfg(all(unix, test))]
fn tier_copy_test_step(step: &str, path: &Path) {
    TIER_COPY_STEP.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook(step, path);
        }
    });
}

#[cfg(unix)]
fn load_current_manifest_until(
    root: &Path,
    shard_id: ShardId,
    expiry: std::time::Instant,
) -> StorageResult<TierManifest> {
    tier_recovery_clock(expiry)?;
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
    tier_recovery_clock(expiry)?;
    let path = manifest_path(root, shard_id, generation);
    let metadata = path.metadata()?;
    if metadata.len() > MAX_MANIFEST_BYTES {
        return Err(StorageError::LimitExceeded(
            "tier manifest exceeds the read limit".into(),
        ));
    }
    tier_recovery_clock(expiry)?;
    let bytes = fs::read(path)?;
    tier_recovery_clock(expiry)?;
    let manifest: TierManifest = serde_json::from_slice(&bytes)
        .map_err(|error| tier_corrupt(format!("invalid tier manifest: {error}")))?;
    if manifest.generation != generation {
        return Err(tier_corrupt(
            "tier CURRENT and manifest generation disagree",
        ));
    }
    tier_recovery_clock(expiry)?;
    Ok(manifest)
}

#[cfg(unix)]
fn publish_manifest_until(
    root: &Path,
    shard_id: ShardId,
    expected_generation: u64,
    manifest: &TierManifest,
    _publication: &ManifestUpdateLock,
    expiry: std::time::Instant,
) -> StorageResult<()> {
    tier_recovery_clock(expiry)?;
    manifest.validate(shard_id)?;
    tier_recovery_clock(expiry)?;
    let observed = load_current_manifest_until(root, shard_id, expiry)?;
    if observed.generation != expected_generation
        || manifest.generation != expected_generation.saturating_add(1)
    {
        return Err(StorageError::InvalidInput(format!(
            "conditional manifest update failed: expected generation {expected_generation}, observed {}",
            observed.generation
        )));
    }
    tier_recovery_clock(expiry)?;
    let encoded = serde_json::to_vec(manifest).map_err(|error| {
        StorageError::InvalidInput(format!("manifest encoding failed: {error}"))
    })?;
    if encoded.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(StorageError::LimitExceeded(
            "tier manifest exceeds the write limit".into(),
        ));
    }
    tier_recovery_clock(expiry)?;
    let path = manifest_path(root, shard_id, manifest.generation);
    match OpenOptions::new().create_new(true).write(true).open(&path) {
        Ok(mut file) => {
            tier_recovery_clock(expiry)?;
            file.write_all(&encoded)?;
            tier_recovery_clock(expiry)?;
            file.sync_all()?;
            tier_recovery_clock(expiry)?;
            sync_directory(path.parent().expect("manifest has parent"))?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            tier_recovery_clock(expiry)?;
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

    tier_recovery_clock(expiry)?;
    let current = current_path(root, shard_id);
    let temporary = temporary_path(&current);
    let mut pointer = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    tier_recovery_clock(expiry)?;
    writeln!(pointer, "{}", manifest.generation)?;
    tier_recovery_clock(expiry)?;
    pointer.sync_all()?;
    tier_recovery_clock(expiry)?;
    fs::rename(&temporary, &current)?;
    tier_recovery_clock(expiry)?;
    sync_directory(current.parent().expect("CURRENT has parent"))?;
    tier_recovery_clock(expiry)?;
    Ok(())
}

#[cfg(unix)]
fn copy_object_atomically_until(
    source: &Path,
    destination: &Path,
    expiry: std::time::Instant,
) -> StorageResult<(u64, String)> {
    tier_recovery_clock(expiry)?;
    if destination.exists() {
        let source_digest = hash_file_until(source, expiry)?;
        let destination_digest = hash_file_until(destination, expiry)?;
        if source_digest != destination_digest {
            return Err(StorageError::corrupt(
                destination,
                0,
                "immutable object already exists with different content",
            ));
        }
        return Ok(destination_digest);
    }
    tier_recovery_clock(expiry)?;
    let temporary = temporary_path(destination);
    #[cfg(test)]
    tier_copy_test_step("temporary-chosen", &temporary);
    // Refusal preserves staging evidence. No pathname cleanup is safe here:
    // source open/create_new may have failed before this call created a file,
    // or a late rename error may leave a different file at the old temp name.
    tier_recovery_clock(expiry)?;
    let mut input = File::open(source)?;
    tier_recovery_clock(expiry)?;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    tier_recovery_clock(expiry)?;
    let mut digest = blake3::Hasher::new();
    let mut bytes = 0u64;
    let mut buffer = vec![0u8; COPY_BUFFER_BYTES];
    loop {
        tier_recovery_clock(expiry)?;
        let read = input.read(&mut buffer)?;
        tier_recovery_clock(expiry)?;
        if read == 0 {
            break;
        }
        output.write_all(&buffer[..read])?;
        digest.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .ok_or_else(|| StorageError::LimitExceeded("object size overflow".into()))?;
    }
    tier_recovery_clock(expiry)?;
    output.sync_all()?;
    tier_recovery_clock(expiry)?;
    fs::rename(&temporary, destination)?;
    #[cfg(test)]
    tier_copy_test_step("renamed", &temporary);
    tier_recovery_clock(expiry)?;
    sync_directory(destination.parent().expect("object has parent"))?;
    tier_recovery_clock(expiry)?;
    Ok((bytes, digest.finalize().to_hex().to_string()))
}

#[cfg(unix)]
fn verify_object_until(
    root: &Path,
    object: &TierObject,
    expiry: std::time::Instant,
) -> StorageResult<()> {
    tier_recovery_clock(expiry)?;
    let path = object_path(root, &object.object_key)?;
    let (bytes, checksum) = hash_file_until(&path, expiry)?;
    if bytes != object.bytes || checksum != object.checksum {
        return Err(StorageError::corrupt(
            &path,
            0,
            "tier object length or checksum mismatch",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn hash_file_until(path: &Path, expiry: std::time::Instant) -> StorageResult<(u64, String)> {
    tier_recovery_clock(expiry)?;
    #[cfg(test)]
    tier_recovery_test_step("hash-start");
    tier_recovery_clock(expiry)?;
    let mut file = File::open(path)?;
    let mut digest = blake3::Hasher::new();
    let mut bytes = 0u64;
    let mut buffer = vec![0u8; COPY_BUFFER_BYTES];
    loop {
        tier_recovery_clock(expiry)?;
        let read = file.read(&mut buffer)?;
        tier_recovery_clock(expiry)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        bytes = checked_object_bytes(bytes, read)?;
    }
    #[cfg(test)]
    tier_recovery_test_step("hash-complete");
    tier_recovery_clock(expiry)?;
    Ok((bytes, digest.finalize().to_hex().to_string()))
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

#[cfg(all(unix, test))]
mod owned_expiry_tests {
    use super::*;
    use crate::{
        BatchId, LeaderEpoch, LogicalPartitionId, Placement, PlacementSequence, RingEpoch,
        ShardStore, ShardStoreConfig, TopicId, VirtualLaneId,
    };
    use std::num::NonZeroU32;
    use std::time::{Duration, Instant};

    struct ActualRoot(PathBuf);
    impl Drop for ActualRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            TIER_RECOVERY_STEP.with(|hook| *hook.borrow_mut() = None);
        }
    }
    fn delay_first_complete(expiry: Instant) -> std::rc::Rc<std::cell::Cell<usize>> {
        let reads = std::rc::Rc::new(std::cell::Cell::new(0));
        let observed = reads.clone();
        TIER_RECOVERY_STEP.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |step| {
                if step == "hash-start" {
                    observed.set(observed.get() + 1);
                }
                if step == "hash-complete" {
                    std::thread::sleep(
                        expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
                    );
                }
            }))
        });
        reads
    }
    #[test]
    fn owned_native_open_and_offload_stop_after_first_late_hash_before_later_verification_or_current()
     {
        let root = ActualRoot(std::env::temp_dir().join(format!(
            "tier-owned-expiry-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        )));
        fs::create_dir_all(&root.0).expect("actual native root");
        let tier_root = root.0.join("tier");
        let mut primary = ShardStore::open(ShardStoreConfig {
            directory: root.0.join("primary"),
            shard_id: ShardId::new(0),
            target_pack_bytes: 4096,
            max_batch_bytes: 4096,
        })
        .expect("actual native primary");
        let mut native =
            LocalObjectTier::open(&tier_root, ShardId::new(0)).expect("actual native tier");
        let mut last = None;
        for sequence in 0..3 {
            let reservation = Reservation {
                topic_id: TopicId::new(11),
                partition_id: LogicalPartitionId::new(0),
                batch_id: BatchId::new(u128::from(sequence)),
                first_offset: LogicalOffset::new(sequence),
                last_offset: LogicalOffset::new(sequence),
                record_count: NonZeroU32::new(1).expect("one"),
                placement: Placement {
                    virtual_lane_id: VirtualLaneId::new(0),
                    ring_epoch: RingEpoch::new(1),
                    leader_epoch: LeaderEpoch::new(91),
                    sequence: PlacementSequence::new(sequence),
                },
            };
            primary
                .append(reservation, None, b"actual native encoded pack", true)
                .expect("native SSE8 writer");
            let pack = primary
                .seal_active()
                .expect("seal")
                .expect("actual nonempty pack");
            let coverage: Vec<_> = primary
                .catalog()
                .into_iter()
                .filter(|entry| entry.pack_sequence == pack.sequence)
                .map(|entry| TierExtent::from_reservation(entry.reservation))
                .collect();
            if sequence < 2 {
                native
                    .offload_pack(pack.sequence, &pack.path, coverage)
                    .expect("actual native CURRENT producer");
            } else {
                last = Some((pack, coverage));
            }
        }
        drop(primary);
        drop(native);
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(tier_root.join("manifests/shard-0/UPDATE.lock"))
            .expect("actual native lock inode");
        let guard = TierRecoveryGuard::try_acquire(lock, tier_root.clone(), ShardId::new(0))
            .expect("same actual publication lease");
        let expiry = Instant::now() + Duration::from_millis(50);
        let reads = delay_first_complete(expiry);
        let reset = Reset;
        assert!(
            LocalObjectTier::open_under_census_until(
                tier_root.clone(),
                ShardId::new(0),
                &guard,
                expiry
            )
            .is_err()
        );
        assert_eq!(
            reads.get(),
            1,
            "second actual CURRENT object was never opened after E"
        );
        drop(reset);
        guard.validate().expect("original native lease remains");
        let mut owned = LocalObjectTier::open_under_census_until(
            tier_root.clone(),
            ShardId::new(0),
            &guard,
            Instant::now() + Duration::from_secs(2),
        )
        .expect("actual fresh native component opening");
        let (pack, coverage) = last.expect("third actual pack");
        let expiry = Instant::now() + Duration::from_millis(50);
        let reads = delay_first_complete(expiry);
        let reset = Reset;
        assert!(
            owned
                .offload_pack_until(pack.sequence, &pack.path, coverage.clone(), expiry)
                .is_err()
        );
        assert_eq!(reads.get(), 1);
        drop(reset);
        assert_eq!(
            load_current_manifest(&tier_root, ShardId::new(0))
                .expect("actual durable pointer")
                .generation,
            2,
            "late verification did not begin manifest publication"
        );
        assert!(
            tier_root
                .join(format!("objects/shard-0/pack-{:020}.sse", pack.sequence))
                .exists(),
            "completed native copy remains truthful evidence"
        );
        drop(owned);
        drop(guard);
        let mut ordinary =
            LocalObjectTier::open(&tier_root, ShardId::new(0)).expect("ordinary native reopen");
        ordinary
            .offload_pack(pack.sequence, pack.path, coverage)
            .expect("ordinary native rejoin validates retained exact copied bytes");
        assert_eq!(ordinary.manifest().generation, 3);
    }

    #[test]
    fn owned_native_copy_refusal_never_unlinks_stale_temp_source_failure_or_postrename_replacement()
    {
        struct CopyReset;
        impl Drop for CopyReset {
            fn drop(&mut self) {
                TIER_COPY_STEP.with(|hook| *hook.borrow_mut() = None);
            }
        }
        let root = ActualRoot(std::env::temp_dir().join(format!(
            "tier-copy-evidence-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        )));
        fs::create_dir_all(&root.0).expect("actual root");
        let mut primary = ShardStore::open(ShardStoreConfig {
            directory: root.0.join("primary"),
            shard_id: ShardId::new(0),
            target_pack_bytes: 4096,
            max_batch_bytes: 4096,
        })
        .expect("actual native source");
        let reservation = Reservation {
            topic_id: TopicId::new(11),
            partition_id: LogicalPartitionId::new(0),
            batch_id: BatchId::new(0),
            first_offset: LogicalOffset::new(0),
            last_offset: LogicalOffset::new(0),
            record_count: NonZeroU32::new(1).expect("one"),
            placement: Placement {
                virtual_lane_id: VirtualLaneId::new(0),
                ring_epoch: RingEpoch::new(1),
                leader_epoch: LeaderEpoch::new(91),
                sequence: PlacementSequence::new(0),
            },
        };
        primary
            .append(reservation, None, b"actual native immutable source", true)
            .expect("real SSE8 writer");
        let pack = primary
            .seal_active()
            .expect("seal")
            .expect("real native pack");
        let coverage: Vec<_> = primary
            .catalog()
            .into_iter()
            .filter(|entry| entry.pack_sequence == pack.sequence)
            .map(|entry| TierExtent::from_reservation(entry.reservation))
            .collect();
        drop(primary);
        let expected = fs::read(&pack.path).expect("real source bytes");
        let mut tier =
            LocalObjectTier::open(root.0.join("tier"), ShardId::new(0)).expect("real native tier");
        let unknown = b"preexisting unknown evidence";
        for source_failure in [false, true] {
            let observed = std::rc::Rc::new(std::cell::RefCell::new(None));
            let saved = observed.clone();
            let source = pack.path.clone();
            let moved_source = root.0.join("original-source-evidence");
            let moved = moved_source.clone();
            TIER_COPY_STEP.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |step, path| {
                    if step == "temporary-chosen" {
                        let mut old = OpenOptions::new()
                            .create_new(true)
                            .write(true)
                            .open(path)
                            .expect("genuine stale PID/sequence-name collision");
                        old.write_all(unknown).expect("unknown prior bytes");
                        old.sync_all().expect("actual old evidence");
                        *saved.borrow_mut() = Some(path.to_owned());
                        if source_failure {
                            fs::rename(&source, &moved).expect(
                                "preserve real source while its original open becomes NotFound",
                            );
                        }
                    }
                }))
            });
            let reset = CopyReset;
            assert!(
                tier.offload_pack_until(
                    pack.sequence,
                    &pack.path,
                    coverage.clone(),
                    Instant::now() + Duration::from_secs(2)
                )
                .is_err()
            );
            drop(reset);
            let actual = observed
                .borrow()
                .clone()
                .expect("helper actually selected its native temporary");
            assert_eq!(
                fs::read(actual).expect("failed native copy cannot delete unknown bytes"),
                unknown
            );
            assert_eq!(tier.manifest().generation, 0);
            if source_failure {
                assert_eq!(
                    fs::read(&moved_source).expect("source evidence retained"),
                    expected
                );
                fs::rename(&moved_source, &pack.path)
                    .expect("restore exact original fixture source");
            }
        }
        let expiry = Instant::now() + Duration::from_secs(2);
        let replacement = std::rc::Rc::new(std::cell::RefCell::new(None));
        let saved = replacement.clone();
        TIER_COPY_STEP.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |step, path| {
                if step == "renamed" {
                    let mut old = OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(path)
                        .expect("real replacement at vacated temp name");
                    old.write_all(unknown).expect("replacement evidence");
                    old.sync_all().expect("actual replacement file");
                    *saved.borrow_mut() = Some(path.to_owned());
                    std::thread::sleep(
                        expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
                    );
                }
            }))
        });
        let reset = CopyReset;
        assert!(
            tier.offload_pack_until(pack.sequence, &pack.path, coverage, expiry)
                .is_err()
        );
        drop(reset);
        let actual = replacement
            .borrow()
            .clone()
            .expect("real rename occurred before expiry refusal");
        assert_eq!(
            fs::read(actual).expect("late error cannot remove another file at old temp path"),
            unknown
        );
        assert_eq!(
            fs::read(&pack.path).expect("original native source unchanged"),
            expected
        );
        assert_eq!(
            fs::read(
                tier.root
                    .join(format!("objects/shard-0/pack-{:020}.sse", pack.sequence))
            )
            .expect("completed native copy is retained truthful evidence"),
            expected
        );
        assert_eq!(
            load_current_manifest(&tier.root, ShardId::new(0))
                .expect("actual CURRENT")
                .generation,
            0
        );
    }
}
