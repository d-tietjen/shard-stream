//! Owned observations before native repair, worker startup, offload or GC.
//!
//! These values preserve local bytes and maxima, not authenticated assignment,
//! global completeness, receipt validity, available catchup or write permission.

use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::mem::size_of;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use shard_stream_core::{
    DirectoryOwnership, DirectoryOwnershipError, OwnedDirectoryHandle, OwnedFileAccess,
    OwnedFileHandle, ShardId, TopicPartition,
};
use shardlog::{
    ExtentCatalogEntry, JournalEvent, LocalObjectTier, RECOVERY_EXTENT_HEADER_BYTES,
    RECOVERY_EXTENT_PREFIX_BYTES, RECOVERY_JOURNAL_PREFIX_BYTES, RecoveryExtentHeader,
    RecoveryExtentTail, ShardRecoveryGuard, ShardStore, ShardStoreConfig, StorageError, TierExtent,
    TierManifest, TierRecoveryGuard, inspect_recovery_extent, inspect_recovery_extent_tail,
    inspect_recovery_journal, recovery_extent_frame_bytes, recovery_journal_frame_bytes,
};

use crate::{EngineConfig, EngineError};

#[path = "recovery_restore.rs"]
mod restore;
pub(crate) use restore::OwnedEngineCustody;

#[path = "recovery_catalog.rs"]
mod catalog;
pub use catalog::{NativeRecoveryCatalog, NativeRecoveryPartition};

/// Explicit finite logical allowances; the original Instant is separate.
#[derive(Clone, Copy, Debug)]
pub struct NativeRecoveryLimits {
    /// Files and retained physical file descriptors, including native leases.
    pub max_files: usize,
    /// Directory visits, decoded records, JSON values and retained slots.
    pub max_entries: usize,
    /// Total actual file read bytes, including verification passes.
    pub max_read_bytes: u64,
    /// Cumulative logical buffers/native values, not allocator/RSS accounting.
    pub max_declared_bytes: usize,
    /// Cumulative logical decode/hash/comparison/copy work, not elapsed CPU.
    pub max_work_bytes: u64,
    /// One complete SSE8/SSJ3 frame before allocation.
    pub max_frame_bytes: usize,
    /// One CURRENT/manifest body before allocation.
    pub max_manifest_bytes: usize,
    /// Retained raw incomplete tail, never silently truncated by this reader.
    pub max_partial_tail_bytes: usize,
    /// One generated/returned diagnostic path before cloning.
    pub max_path_bytes: usize,
}

/// Actual cumulative reservations, never caller-supplied carry or authority.
#[derive(Clone, Copy, Debug, Default)]
pub struct NativeRecoveryUsage {
    pub files: usize,
    pub entries: usize,
    pub read_bytes: u64,
    pub declared_bytes: usize,
    pub work_bytes: u64,
}

/// Native storage role observed under the retained handles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoverySourceKind {
    Coordinator,
    PrimaryPack,
    PendingDeletion,
    TierCurrent,
    TierManifest,
    UnreferencedTierManifest,
    TierObject,
    UnreferencedTierObject,
}

/// Physical identity and exact bytes observed; no signature/freshness claim.
#[derive(Debug)]
pub struct RecoverySourceFact {
    pub path: PathBuf,
    pub kind: RecoverySourceKind,
    pub device: u64,
    pub inode: u64,
    pub bytes: u64,
    pub blake3: [u8; 32],
    /// Exact incomplete native tail preserved before any subsequent repair.
    pub partial_tail: Vec<u8>,
    /// Actual incomplete SSE8 header observation, never checksum validity.
    /// Coordinator SSJ3 tails have no encoded leader term and leave this None.
    pub extent_tail: Option<RecoveryExtentTailObservation>,
}

/// Original source coordinates plus native incomplete-header/unknown evidence.
#[derive(Debug)]
pub struct RecoveryExtentTailObservation {
    /// Index of the source retaining the exact bytes and real descriptor.
    pub source_index: usize,
    /// Offset of the original incomplete frame within that source.
    pub file_offset: u64,
    /// Actual physical source shard, independent of the claimed placement.
    pub physical_shard: ShardId,
    /// Native header fields or explicit unknown; no payload/CRC validity claim.
    pub native: RecoveryExtentTail,
}

/// One complete native frame, including nonlatest and duplicate-source facts.
#[derive(Debug)]
pub struct RecoveryExtentObservation {
    pub source_index: usize,
    pub file_offset: u64,
    pub physical_shard: ShardId,
    pub native: ExtentCatalogEntry,
    /// Actual current configuration mapping; historical ring joins remain due.
    pub configured_shard: ShardId,
}

/// Native retained manifest bytes, distinct from verified payload availability.
#[derive(Debug)]
pub struct RecoveryTierManifestObservation {
    pub source_index: usize,
    pub physical_shard: ShardId,
    /// Whether the captured CURRENT selected this exact generation.
    pub selected_by_current: bool,
    pub native: TierManifest,
}

struct Source {
    handle: OwnedFileHandle,
    fact: RecoverySourceFact,
}

struct Attempt {
    config: EngineConfig,
    limits: NativeRecoveryLimits,
    expiry: Instant,
    usage: NativeRecoveryUsage,
    writable_journal: bool,
    restore_started: bool,
    storage_open_prepaid: bool,
    validation_prepaid: Option<NativeRecoveryUsage>,
    validation_credit: Option<NativeRecoveryUsage>,
    restored_parents: Vec<OwnedDirectoryHandle>,
    restored_files: Vec<OwnedFileHandle>,
    engine_facts: Vec<RecoverySourceFact>,
    native_discovery_visits: usize,
    native_discovery_path_bytes: usize,
    sources: Vec<Source>,
    extents: Vec<RecoveryExtentObservation>,
    tier_manifests: Vec<RecoveryTierManifestObservation>,
    journal: Vec<JournalEvent>,
    missing_paths: Vec<PathBuf>,
    directories: Vec<OwnedDirectoryHandle>,
    shards: Vec<(ShardId, ShardRecoveryGuard)>,
    tiers: Vec<(ShardId, TierRecoveryGuard)>,
    opened: Vec<(ShardId, ShardStore, Option<LocalObjectTier>)>,
    root: Option<OwnedDirectoryHandle>,
    tier_root: Option<OwnedDirectoryHandle>,
    // Last: every opened file/guard/value settles before original OS custody.
    owner: Arc<DirectoryOwnership>,
}

/// Failure retains actual partial observations and concrete storage custody.
pub struct NativeRecoveryError {
    error: EngineError,
    attempt: Box<Attempt>,
}

impl std::fmt::Debug for NativeRecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeRecoveryError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for NativeRecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for NativeRecoveryError {}

impl NativeRecoveryError {
    /// Actual refusal; this cannot recycle any process-start grant.
    pub fn error(&self) -> &EngineError {
        &self.error
    }
    /// Reservations already consumed before refusal.
    pub fn usage(&self) -> NativeRecoveryUsage {
        self.attempt.usage
    }
    /// The unchanged original deadline still owned by the failed attempt.
    pub fn absolute_expiry(&self) -> Instant {
        self.attempt.expiry
    }
}

/// Privately constructed full native observation retaining real storage leases.
pub struct NativeRecoveryCensus {
    attempt: Attempt,
}

/// Native stores opened by consuming the actual pre-open leases.
///
/// `open_storage` opens native stores without object rehydration; the explicit
/// `restore_storage` path first copies verified CURRENT objects into primary
/// storage. The consuming engine constructor can then install these real stores
/// in normal workers. None of these steps authenticates currentness, resumes
/// sinks or authorizes serving. Pre-open facts remain historical observations
/// after repair/cleanup, not claims that their old paths remain intact.
pub struct OwnedRecoveryStorage {
    attempt: Attempt,
}

impl std::fmt::Debug for OwnedRecoveryStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedRecoveryStorage")
            .field("usage", &self.attempt.usage)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for NativeRecoveryCensus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeRecoveryCensus")
            .field("usage", &self.attempt.usage)
            .finish_non_exhaustive()
    }
}

impl OwnedRecoveryStorage {
    /// Cumulative admissions, including actual repeated native-open work.
    pub fn usage(&self) -> NativeRecoveryUsage {
        self.attempt.usage
    }
    /// Original census expiry, never reconstructed from elapsed duration.
    pub fn absolute_expiry(&self) -> Instant {
        self.attempt.expiry
    }
    /// Real primary store for a configured shard, retaining original custody.
    pub fn primary(&mut self, shard: ShardId) -> Option<&mut ShardStore> {
        self.attempt
            .opened
            .iter_mut()
            .find(|(id, _, _)| *id == shard)
            .map(|(_, store, _)| store)
    }
    /// Real object tier, retaining the transferred native copy/publication lease.
    pub fn tier(&mut self, shard: ShardId) -> Option<&mut LocalObjectTier> {
        self.attempt
            .opened
            .iter_mut()
            .find(|(id, _, _)| *id == shard)
            .and_then(|(_, _, tier)| tier.as_mut())
    }
    /// Exact observations made before mutable native open, including raw tails.
    pub fn prior_sources(&self) -> impl Iterator<Item = &RecoverySourceFact> {
        self.attempt.sources.iter().map(|source| &source.fact)
    }
}

impl NativeRecoveryCensus {
    /// Reads actual native storage before any mutable opener/worker/GC.
    ///
    /// A separately configured tier requires its actual separately acquired
    /// owner. Neither argument authenticates history or authorizes serving.
    pub fn read(
        config: EngineConfig,
        owner: Arc<DirectoryOwnership>,
        tier_root: Option<OwnedDirectoryHandle>,
        limits: NativeRecoveryLimits,
        expiry: Instant,
    ) -> std::result::Result<Self, NativeRecoveryError> {
        Self::read_inner(config, owner, tier_root, limits, expiry, false)
    }

    /// Same nonmutating census, retaining a writable coordinator descriptor.
    ///
    /// Use this before `restore_storage` and the consuming engine constructor.
    /// Opening RDWR does not repair or write; the actual journal remains pinned
    /// and no engine path later reopens it or reacquires the native leases.
    pub fn read_for_engine(
        config: EngineConfig,
        owner: Arc<DirectoryOwnership>,
        tier_root: Option<OwnedDirectoryHandle>,
        limits: NativeRecoveryLimits,
        expiry: Instant,
    ) -> std::result::Result<Self, NativeRecoveryError> {
        Self::read_inner(config, owner, tier_root, limits, expiry, true)
    }

    fn read_inner(
        config: EngineConfig,
        owner: Arc<DirectoryOwnership>,
        tier_root: Option<OwnedDirectoryHandle>,
        limits: NativeRecoveryLimits,
        expiry: Instant,
        writable_journal: bool,
    ) -> std::result::Result<Self, NativeRecoveryError> {
        let mut attempt = Attempt {
            config,
            limits,
            expiry,
            usage: NativeRecoveryUsage::default(),
            writable_journal,
            restore_started: false,
            storage_open_prepaid: false,
            validation_prepaid: None,
            validation_credit: None,
            restored_parents: Vec::new(),
            restored_files: Vec::new(),
            engine_facts: Vec::new(),
            native_discovery_visits: 0,
            native_discovery_path_bytes: 0,
            sources: Vec::new(),
            extents: Vec::new(),
            tier_manifests: Vec::new(),
            journal: Vec::new(),
            missing_paths: Vec::new(),
            directories: Vec::new(),
            shards: Vec::new(),
            tiers: Vec::new(),
            opened: Vec::new(),
            root: None,
            tier_root,
            owner,
        };
        let result = (|| {
            if writable_journal {
                attempt.clock()?;
                let held = attempt
                    .config
                    .data_dir
                    .as_os_str()
                    .len()
                    .checked_add(
                        attempt
                            .config
                            .object_store_dir
                            .as_ref()
                            .map_or(0, |p| p.as_os_str().len()),
                    )
                    .and_then(|n| n.checked_add(size_of::<EngineConfig>()))
                    .ok_or_else(limited)?;
                attempt.charge(0, 1, held, 0, held as u64)?;
            }
            attempt.read()
        })();
        match result {
            Ok(()) => Ok(Self { attempt }),
            Err(error) => Err(NativeRecoveryError {
                error,
                attempt: Box::new(attempt),
            }),
        }
    }

    /// Every native complete frame, without retention/latest-only filtering.
    pub fn extents(&self) -> &[RecoveryExtentObservation] {
        &self.attempt.extents
    }
    /// Every retained incomplete SSE8 header, including explicit unknowns.
    /// These borrow the source facts that retain the original raw tail bytes.
    pub fn extent_tails(&self) -> impl Iterator<Item = &RecoveryExtentTailObservation> {
        self.attempt
            .sources
            .iter()
            .filter_map(|source| source.fact.extent_tail.as_ref())
    }
    /// All retained native manifests, including generations beyond CURRENT.
    ///
    /// Their recorded terms conservatively raise an observed floor; this does
    /// not certify that their objects exist, are valid or can be served.
    pub fn tier_manifests(&self) -> &[RecoveryTierManifestObservation] {
        &self.attempt.tier_manifests
    }
    /// Actual root owner retained by this census, for exact Arc identity joins.
    /// This borrow creates no owner, recovery authority or serving permission.
    pub fn directory_ownership(&self) -> &Arc<DirectoryOwnership> {
        &self.attempt.owner
    }
    /// Actual native coordinator events; they contain no leader-term maximum.
    pub fn journal(&self) -> &[JournalEvent] {
        &self.attempt.journal
    }
    /// Actually absent paths, not an empty-history/unused-storage certificate.
    pub fn missing_paths(&self) -> &[PathBuf] {
        &self.attempt.missing_paths
    }
    /// Borrow source facts without cloning path/tail data.
    pub fn sources(&self) -> impl Iterator<Item = &RecoverySourceFact> {
        self.attempt.sources.iter().map(|source| &source.fact)
    }
    /// Actual reservations already made by this reader.
    pub fn usage(&self) -> NativeRecoveryUsage {
        self.attempt.usage
    }
    /// SAME original expiry, never a relative timeout reconstructed on return.
    pub fn absolute_expiry(&self) -> Instant {
        self.attempt.expiry
    }
    /// Copies bounded original bytes from a retained source before mutable open.
    ///
    /// This debits actual read/copy work under the SAME saved expiry and keeps
    /// the real source lease; no caller can extract or replace its raw handle.
    pub fn read_source_chunk(
        &mut self,
        index: usize,
        offset: u64,
        maximum: usize,
    ) -> Result<Vec<u8>> {
        if maximum == 0 || maximum > self.attempt.limits.max_frame_bytes {
            return Err(limited());
        }
        self.attempt.charge(1, 1, 256, 0, 256)?;
        let source = self.attempt.sources.get(index).ok_or_else(limited)?;
        source.handle.validate().map_err(identity)?;
        let length = source
            .fact
            .bytes
            .checked_sub(offset)
            .ok_or_else(limited)?
            .min(maximum as u64);
        let file = source
            .handle
            .file()
            .try_clone()
            .map_err(StorageError::from)?;
        let mut bytes = self
            .attempt
            .buffer(usize::try_from(length).map_err(|_| limited())?)?;
        self.attempt.read_at(&file, &mut bytes, offset)?;
        self.attempt.sources[index]
            .handle
            .validate()
            .map_err(identity)?;
        Ok(bytes)
    }
    /// Maximum of complete frames, complete tail headers and manifest claims.
    ///
    /// This is a conservative local observation, never all-time authority or
    /// proof of payload availability. Unknown tails remain in `extent_tails`;
    /// this numeric maximum cannot prove their absence or safe term reuse.
    pub fn observed_leader_epoch(&self, tp: TopicPartition) -> Option<u64> {
        self.attempt
            .extents
            .iter()
            .filter(|entry| {
                entry.native.reservation.topic_id == tp.topic_id
                    && entry.native.reservation.partition_id == tp.partition_id
            })
            .map(|entry| entry.native.reservation.placement.leader_epoch.get())
            .chain(self.extent_tails().filter_map(|entry| match &entry.native {
                RecoveryExtentTail::Header(header)
                    if header.reservation.topic_id == tp.topic_id
                        && header.reservation.partition_id == tp.partition_id =>
                {
                    Some(header.reservation.placement.leader_epoch.get())
                }
                _ => None,
            }))
            .chain(
                self.attempt
                    .tier_manifests
                    .iter()
                    .flat_map(move |manifest| {
                        manifest.native.objects.iter().flat_map(move |object| {
                            object.extents.iter().filter_map(move |extent| {
                                (extent.topic_id.parse::<u128>().ok() == Some(tp.topic_id.get())
                                    && extent.partition_id == tp.partition_id.get())
                                .then_some(extent.leader_epoch)
                            })
                        })
                    }),
            )
            .max()
    }

    /// Rehydrates verified CURRENT objects before any mutable primary scan.
    ///
    /// Only the actual retained native manifest/object snapshot is used. An
    /// existing primary must match exactly; publication never replaces a name.
    /// All old source facts, incomplete tails and uncertain copy results remain
    /// retained on refusal. Persist their lineage/floors BEFORE this mutation.
    pub fn restore_storage(
        mut self,
    ) -> std::result::Result<OwnedRecoveryStorage, NativeRecoveryError> {
        let result = self
            .attempt
            .restore_objects()
            .and_then(|()| self.attempt.open_storage_inner(true));
        match result {
            Ok(()) => Ok(OwnedRecoveryStorage {
                attempt: self.attempt,
            }),
            Err(error) => Err(NativeRecoveryError {
                error,
                attempt: Box::new(self.attempt),
            }),
        }
    }

    /// Consumes concrete native leases into the actual storage openers.
    ///
    /// The caller must persist required floor/raw lineage before invoking any
    /// mutable repair. This is storage, not a private startup/permission gate.
    /// Actual opened resources and old observations remain owned on failure.
    pub fn open_storage(
        mut self,
    ) -> std::result::Result<OwnedRecoveryStorage, NativeRecoveryError> {
        let result = self.attempt.open_storage();
        match result {
            Ok(()) => Ok(OwnedRecoveryStorage {
                attempt: self.attempt,
            }),
            Err(error) => Err(NativeRecoveryError {
                error,
                attempt: Box::new(self.attempt),
            }),
        }
    }
}

type Result<T> = std::result::Result<T, EngineError>;

fn limited() -> EngineError {
    EngineError::InvalidConfig("native recovery allowance exhausted".into())
}
fn identity(error: impl std::fmt::Display) -> EngineError {
    EngineError::InvalidConfig(error.to_string())
}

impl Attempt {
    // Reserve the whole configured set BEFORE the first mutable native open.
    // Empty stores still create locks/packs/directories and fixed maps/queues.
    // Counts conservatively include missing-path opens and temporary sync FDs;
    // each existing source adds native scan/hash/path slots. These are logical
    // cumulative reservations, not claims about malloc or the kernel's RSS.
    fn storage_open_admission(&self) -> Result<NativeRecoveryUsage> {
        let add = |a: usize, b: usize| a.checked_add(b).ok_or_else(limited);
        let mul = |a: usize, b: usize| a.checked_mul(b).ok_or_else(limited);
        let count = usize::try_from(self.config.shard_count).map_err(|_| limited())?;
        let source_bytes = self.sources.iter().try_fold(0usize, |sum, source| {
            self.clock()?;
            add(
                sum,
                usize::try_from(source.fact.bytes).map_err(|_| limited())?,
            )
        })?;
        let fixed_primary = add(
            add(
                ShardStore::recovery_open_fixed_bytes(),
                mul(
                    size_of::<(ShardId, ShardStore, Option<LocalObjectTier>)>(),
                    2,
                )?,
            )?,
            add(mul(self.limits.max_path_bytes, 32)?, 4096)?,
        )?;
        let mut bytes = add(
            mul(count, fixed_primary)?,
            add(mul(source_bytes, 2)?, mul(self.extents.len(), 2048)?)?,
        )?;
        // Both real native directory scans call entry.path() BEFORE filtering,
        // including ignored/non-UTF8 names. Reserve complete measured parent+
        // name copies, iterator values, pending Vec capacity and native map
        // slots; source/extents-only costs cannot cover these visits.
        bytes = add(
            bytes,
            add(
                mul(self.native_discovery_path_bytes, 4)?,
                mul(
                    self.native_discovery_visits,
                    size_of::<PathBuf>() * 8 + size_of::<u64>() * 8 + 128,
                )?,
            )?,
        )?;
        // Native cleanup/discovery repeats all directory visits, not just packs.
        let mut entries = add(
            mul(count, 32)?,
            add(
                mul(self.usage.entries, 2)?,
                add(mul(self.sources.len(), 8)?, mul(self.extents.len(), 8)?)?,
            )?,
        )?;
        let mut files = add(mul(count, 16)?, mul(self.sources.len(), 4)?)?;
        if self.tier_root.is_some() {
            bytes = add(
                bytes,
                mul(
                    count,
                    add(
                        LocalObjectTier::recovery_open_fixed_bytes(),
                        add(mul(self.limits.max_path_bytes, 16)?, 4096)?,
                    )?,
                )?,
            )?;
            files = add(files, mul(count, 8)?)?;
            entries = add(entries, mul(count, 16)?)?;
            for manifest in &self.tier_manifests {
                self.clock()?;
                if !manifest.selected_by_current {
                    continue;
                }
                // Native open rereads/deserializes CURRENT's manifest and
                // validates/hashes EVERY referenced object. Pre-admit native
                // JSON value/error-copy bounds and its actual 1 MiB scratch.
                let raw = self
                    .sources
                    .get(manifest.source_index)
                    .ok_or_else(limited)?
                    .fact
                    .bytes;
                bytes = add(
                    bytes,
                    mul(usize::try_from(raw).map_err(|_| limited())?, 1024)?,
                )?;
                let objects = manifest.native.objects.len();
                bytes = add(
                    bytes,
                    mul(
                        objects,
                        add(
                            LocalObjectTier::recovery_hash_buffer_bytes(),
                            add(
                                mul(self.limits.max_path_bytes, 4)?,
                                size_of::<shardlog::TierObject>() * 2,
                            )?,
                        )?,
                    )?,
                )?;
                files = add(files, objects)?;
                entries = add(entries, mul(objects, 8)?)?;
            }
        }
        let read = u64::try_from(source_bytes)
            .map_err(|_| limited())?
            .checked_mul(4)
            .ok_or_else(limited)?;
        let work = read
            .checked_mul(8)
            .and_then(|n| n.checked_add(u64::try_from(bytes).ok()?.checked_mul(4)?))
            .and_then(|n| n.checked_add(u64::try_from(entries).ok()?.checked_mul(32)?))
            .ok_or_else(limited)?;
        Ok(NativeRecoveryUsage {
            files,
            entries,
            declared_bytes: bytes,
            read_bytes: read,
            work_bytes: work,
        })
    }

    fn open_storage(&mut self) -> Result<()> {
        self.open_storage_inner(false)
    }

    fn open_storage_inner(&mut self, retained: bool) -> Result<()> {
        if let Some(credit) = self.validation_prepaid.take() {
            self.validation_credit = Some(credit);
            self.validate_sources()?;
            let left = self.validation_credit.take().ok_or_else(limited)?;
            if (
                left.files,
                left.entries,
                left.declared_bytes,
                left.read_bytes,
                left.work_bytes,
            ) != (0, 0, 0, 0, 0)
            {
                return Err(limited());
            }
        } else {
            self.validate_sources()?;
        }
        for path in &self.missing_paths {
            if self.restore_started {
                if let Some(parent) = self
                    .restored_parents
                    .iter()
                    .find(|held| held.path() == path)
                {
                    parent.validate().map_err(identity)?;
                    continue;
                }
            }
            match std::fs::symlink_metadata(path) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                _ => {
                    return Err(identity(
                        "an absent native path changed before storage open",
                    ));
                }
            }
        }
        if !self.storage_open_prepaid {
            let admitted = self.storage_open_admission()?;
            self.charge(
                admitted.files,
                admitted.entries,
                admitted.declared_bytes,
                admitted.read_bytes,
                admitted.work_bytes,
            )?;
        }
        // Reserve actual output slots up front, before any native mutation.
        self.opened
            .try_reserve_exact(usize::try_from(self.config.shard_count).map_err(|_| limited())?)
            .map_err(|_| limited())?;
        for number in 0..self.config.shard_count {
            self.clock()?;
            let shard = ShardId::new(number);
            let root = self.root.as_ref().ok_or_else(limited)?;
            root.validate().map_err(identity)?;
            let directory = root.path().join("shards").join(format!("shard-{shard}"));
            let config = ShardStoreConfig {
                directory,
                shard_id: shard,
                target_pack_bytes: self.config.target_pack_bytes,
                max_batch_bytes: self.config.max_batch_bytes,
            };
            let store = if let Some(index) = self.shards.iter().position(|(id, _)| *id == shard) {
                if retained {
                    ShardStore::open_under_census_until(config, &self.shards[index].1, self.expiry)?
                } else {
                    let (_, guard) = self.shards.swap_remove(index);
                    ShardStore::open_after_census(config, guard)?
                }
            } else {
                // No pre-open shard existed. A native new store acquires its
                // own concrete lease; absence never supplied a permission.
                if retained {
                    ShardStore::open_until(config, self.expiry)?
                } else {
                    ShardStore::open(config)?
                }
            };
            self.opened.push((shard, store, None));
            if retained {
                self.clock()?;
            }
            if let Some(root) = self.tier_root.as_ref() {
                root.validate().map_err(identity)?;
                let path = root.path().to_owned();
                let tier = if let Some(index) = self.tiers.iter().position(|(id, _)| *id == shard) {
                    if retained {
                        LocalObjectTier::open_under_census_until(
                            path,
                            shard,
                            &self.tiers[index].1,
                            self.expiry,
                        )?
                    } else {
                        let (_, guard) = self.tiers.swap_remove(index);
                        LocalObjectTier::open_after_census(path, shard, guard)?
                    }
                } else if retained {
                    LocalObjectTier::open_until(path, shard, self.expiry)?
                } else {
                    LocalObjectTier::open(path, shard)?
                };
                self.opened.last_mut().ok_or_else(limited)?.2 = Some(tier);
            }
            self.clock()?;
        }
        Ok(())
    }
    fn read(&mut self) -> Result<()> {
        self.clock()?;
        self.config.validate()?;
        if [
            self.limits.max_files,
            self.limits.max_entries,
            self.limits.max_declared_bytes,
            self.limits.max_frame_bytes,
            self.limits.max_manifest_bytes,
            self.limits.max_partial_tail_bytes,
            self.limits.max_path_bytes,
        ]
        .contains(&0)
            || self.limits.max_read_bytes == 0
            || self.limits.max_work_bytes == 0
        {
            return Err(limited());
        }
        self.owner
            .validate_directory(&self.config.data_dir)
            .map_err(identity)?;
        self.charge(1, 1, size_of::<Self>() + 65_536, 0, 0)?;
        self.root = Some(self.owner.root_handle().map_err(identity)?);
        let root = self.root.as_ref().ok_or_else(limited)?.retain();
        if self.writable_journal {
            let bytes = root.path().as_os_str().len();
            if bytes > self.limits.max_path_bytes {
                return Err(limited());
            }
            self.charge(0, 1, bytes, 0, bytes as u64)?;
            self.config.data_dir = root.path().to_owned();
        }
        if let Some(journal) = self.optional_file(&root, OsStr::new("coordinator.ssj"))? {
            self.scan_source(journal, RecoverySourceKind::Coordinator, ShardId::new(0), 0)?;
        }
        if let Some(shards) = self.optional_directory(&root, OsStr::new("shards"))? {
            let ids = self.shard_names(&shards)?;
            for number in 0..self.config.shard_count {
                self.charge(0, 1, 128, 0, ids.len() as u64)?;
                let shard = ShardId::new(number);
                if !ids.contains(&shard) {
                    self.missing(&shards, OsStr::new(&format!("shard-{shard}")))?;
                }
            }
            for shard in ids {
                self.charge(0, 1, 128, 0, 128)?;
                let name = format!("shard-{shard}");
                let directory = self
                    .optional_directory(&shards, OsStr::new(&name))?
                    .ok_or_else(|| identity("native shard directory disappeared during census"))?;
                let lease_file = self.open_file(
                    &directory,
                    OsStr::new(".shard-store.lock"),
                    OwnedFileAccess::ReadWriteCreate,
                )?;
                self.charge(1, 1, 256, 0, 256)?;
                let lease = ShardRecoveryGuard::try_acquire(
                    lease_file.file().try_clone().map_err(StorageError::from)?,
                    directory.path().to_owned(),
                )?;
                self.charge(0, 1, size_of::<(ShardId, ShardRecoveryGuard)>(), 0, 0)?;
                self.shards.try_reserve(1).map_err(|_| limited())?;
                self.shards.push((shard, lease));
                self.scan_primary(&directory, shard)?;
            }
        }
        if let Some(path) = &self.config.object_store_dir {
            let held = self.tier_root.as_ref().ok_or_else(|| {
                identity("configured object tier has no real retained directory handle")
            })?;
            held.validate().map_err(identity)?;
            if std::fs::canonicalize(path).map_err(StorageError::from)? != held.path() {
                return Err(identity(
                    "configured tier root differs from actual retained directory",
                ));
            }
            let tier_root = self.tier_root.as_ref().ok_or_else(limited)?.retain();
            let mut ids = Vec::new();
            for number in 0..self.config.shard_count {
                self.charge(0, 1, size_of::<ShardId>() * 2, 0, 4)?;
                ids.try_reserve(1).map_err(|_| limited())?;
                ids.push(ShardId::new(number));
            }
            for name in ["manifests", "objects"] {
                if let Some(directory) = self.optional_directory(&tier_root, OsStr::new(name))? {
                    for shard in self.shard_names(&directory)? {
                        self.charge(0, 1, size_of::<ShardId>() * 2, 0, ids.len() as u64)?;
                        if !ids.contains(&shard) {
                            ids.try_reserve(1).map_err(|_| limited())?;
                            ids.push(shard);
                        }
                    }
                }
            }
            for shard in ids {
                self.scan_tier(&tier_root, shard)?;
            }
        } else if self.tier_root.is_some() {
            return Err(identity(
                "object owner supplied without configured object tier",
            ));
        }
        self.validate_sources()?;
        self.clock()
    }

    fn missing(&mut self, root: &OwnedDirectoryHandle, name: &OsStr) -> Result<()> {
        self.path(root, name)?;
        self.charge(0, 1, size_of::<PathBuf>(), 0, 0)?;
        self.missing_paths.try_reserve(1).map_err(|_| limited())?;
        self.missing_paths.push(root.path().join(name));
        Ok(())
    }

    fn optional_file(
        &mut self,
        root: &OwnedDirectoryHandle,
        name: &OsStr,
    ) -> Result<Option<OwnedFileHandle>> {
        self.path(root, name)?;
        self.charge(1, 1, size_of::<OwnedFileHandle>() + 256, 0, 256)?;
        let access = if self.writable_journal && name == OsStr::new("coordinator.ssj") {
            OwnedFileAccess::ReadWrite
        } else {
            OwnedFileAccess::ReadOnly
        };
        match root.open_file(name, access) {
            Ok(file) => {
                self.clock()?;
                Ok(Some(file))
            }
            Err(DirectoryOwnershipError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                self.missing(root, name)?;
                Ok(None)
            }
            Err(error) => Err(identity(error)),
        }
    }

    fn optional_directory(
        &mut self,
        root: &OwnedDirectoryHandle,
        name: &OsStr,
    ) -> Result<Option<OwnedDirectoryHandle>> {
        self.path(root, name)?;
        self.charge(1, 1, size_of::<OwnedDirectoryHandle>() + 256, 0, 256)?;
        match root.open_directory(name, false) {
            Ok(directory) => {
                self.clock()?;
                self.charge(0, 1, size_of::<OwnedDirectoryHandle>() * 2, 0, 0)?;
                self.directories.try_reserve(1).map_err(|_| limited())?;
                self.directories.push(directory.retain());
                Ok(Some(directory))
            }
            Err(DirectoryOwnershipError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                self.missing(root, name)?;
                Ok(None)
            }
            Err(error) => Err(identity(error)),
        }
    }

    // Discover actual native physical directories, including an older ring's
    // shards outside today's configured count. No latest/config-only filter.
    fn shard_names(&mut self, directory: &OwnedDirectoryHandle) -> Result<Vec<ShardId>> {
        self.charge(1, 1, 65_536, 0, 0)?;
        let mut names = directory.entries().map_err(identity)?;
        let mut ids = Vec::new();
        loop {
            self.charge(0, 1, 512, 0, 512)?;
            let Some(name) = names.next() else {
                break;
            };
            let name = name.map_err(identity)?;
            let Some(text) = name.to_str() else {
                continue;
            };
            let Some(digits) = text.strip_prefix("shard-") else {
                continue;
            };
            let number = digits
                .parse::<u32>()
                .map_err(|_| identity("invalid native shard name"))?;
            self.charge(0, 1, 128, 0, 128)?;
            if text != format!("shard-{number}") {
                return Err(identity("noncanonical native shard name"));
            }
            ids.try_reserve(1).map_err(|_| limited())?;
            ids.push(ShardId::new(number));
        }
        directory.validate().map_err(identity)?;
        Ok(ids)
    }

    fn scan_primary(&mut self, directory: &OwnedDirectoryHandle, shard: ShardId) -> Result<()> {
        self.charge(1, 1, 65_536, 0, 0)?;
        let mut entries = directory.entries().map_err(identity)?;
        loop {
            self.charge(0, 1, 256, 0, 256)?;
            let Some(name) = entries.next() else {
                break;
            };
            let name = name.map_err(identity)?;
            self.measure_native_discovery_path(directory, &name)?;
            let Some(text) = name.to_str() else {
                continue;
            };
            let (sequence, kind) = if let Some(digits) = text
                .strip_prefix("pack-")
                .and_then(|p| p.strip_suffix(".sse.gc-pending"))
            {
                (digits, RecoverySourceKind::PendingDeletion)
            } else if let Some(digits) = text
                .strip_prefix("pack-")
                .and_then(|p| p.strip_suffix(".sse"))
            {
                (digits, RecoverySourceKind::PrimaryPack)
            } else {
                continue;
            };
            let sequence = sequence
                .parse::<u64>()
                .map_err(|_| identity("invalid recognizable native pack sequence"))?;
            let file = self.open_file(directory, &name, OwnedFileAccess::ReadOnly)?;
            self.scan_source(file, kind, shard, sequence)?;
        }
        directory.validate().map_err(identity)
    }

    // Measure native discovery BEFORE format filtering, without manufacturing
    // PathBufs or accepting caller-provided visit/path counts. The existing
    // generated-path ceiling also applies to native ignored-entry paths.
    fn measure_native_discovery_path(
        &mut self,
        directory: &OwnedDirectoryHandle,
        name: &OsStr,
    ) -> Result<()> {
        let length = directory
            .path()
            .as_os_str()
            .len()
            .checked_add(name.len())
            .and_then(|n| n.checked_add(1))
            .ok_or_else(limited)?;
        if length > self.limits.max_path_bytes {
            return Err(limited());
        }
        let visits = self
            .native_discovery_visits
            .checked_add(1)
            .ok_or_else(limited)?;
        let bytes = self
            .native_discovery_path_bytes
            .checked_add(length)
            .ok_or_else(limited)?;
        self.charge(0, 0, 0, 0, u64::try_from(length).map_err(|_| limited())?)?;
        self.native_discovery_visits = visits;
        self.native_discovery_path_bytes = bytes;
        Ok(())
    }

    fn blob(&mut self, handle: OwnedFileHandle, kind: RecoverySourceKind) -> Result<Vec<u8>> {
        handle.validate().map_err(identity)?;
        let metadata = handle.file().metadata().map_err(StorageError::from)?;
        let length = usize::try_from(metadata.len()).map_err(|_| limited())?;
        if length > self.limits.max_manifest_bytes {
            return Err(limited());
        }
        let mut raw = self.buffer(length)?;
        self.read_at(handle.file(), &mut raw, 0)?;
        self.charge(
            0,
            1,
            size_of::<Source>() + self.limits.max_path_bytes,
            0,
            length as u64,
        )?;
        let digest = *blake3::hash(&raw).as_bytes();
        let path = handle.path();
        handle.validate().map_err(identity)?;
        self.sources.try_reserve(1).map_err(|_| limited())?;
        self.sources.push(Source {
            handle,
            fact: RecoverySourceFact {
                path,
                kind,
                device: metadata.dev(),
                inode: metadata.ino(),
                bytes: metadata.len(),
                blake3: digest,
                partial_tail: Vec::new(),
                extent_tail: None,
            },
        });
        Ok(raw)
    }

    fn descendant(&mut self, root: &OwnedDirectoryHandle, path: &Path) -> Result<OwnedFileHandle> {
        let mut directory = root.retain();
        let mut parts = path.components().peekable();
        while let Some(part) = parts.next() {
            let Component::Normal(name) = part else {
                return Err(identity("native object key escapes retained root"));
            };
            self.path(&directory, name)?;
            if parts.peek().is_none() {
                return self.open_file(&directory, name, OwnedFileAccess::ReadOnly);
            }
            directory = self
                .optional_directory(&directory, name)?
                .ok_or_else(|| identity("native object parent disappeared"))?;
            self.clock()?;
        }
        Err(identity("empty native object key"))
    }

    fn tier_manifest(
        &mut self,
        root: &OwnedDirectoryHandle,
        shard: ShardId,
        shard_name: &str,
    ) -> Result<Option<TierManifest>> {
        let Some(manifests) = self.optional_directory(root, OsStr::new("manifests"))? else {
            return Ok(None);
        };
        let Some(directory) = self.optional_directory(&manifests, OsStr::new(shard_name))? else {
            return Ok(None);
        };
        let lease_file = self.open_file(
            &directory,
            OsStr::new("UPDATE.lock"),
            OwnedFileAccess::ReadWriteCreate,
        )?;
        self.charge(1, 1, 256, 0, 256)?;
        let lease = TierRecoveryGuard::try_acquire(
            lease_file.file().try_clone().map_err(StorageError::from)?,
            root.path().to_owned(),
            shard,
        )?;
        self.charge(0, 1, size_of::<(ShardId, TierRecoveryGuard)>(), 0, 0)?;
        self.tiers.try_reserve(1).map_err(|_| limited())?;
        self.tiers.push((shard, lease));
        let generation =
            if let Some(current) = self.optional_file(&directory, OsStr::new("CURRENT"))? {
                let raw = self.blob(current, RecoverySourceKind::TierCurrent)?;
                Some(
                    std::str::from_utf8(&raw)
                        .ok()
                        .and_then(|s| s.trim().parse::<u64>().ok())
                        .ok_or_else(|| identity("invalid native tier CURRENT"))?,
                )
            } else {
                None
            };
        self.charge(1, 1, 65_536, 0, 0)?;
        let mut names = directory.entries().map_err(identity)?;
        let mut current = None;
        loop {
            self.charge(0, 1, 512, 0, 512)?;
            let Some(name) = names.next() else {
                break;
            };
            let name = name.map_err(identity)?;
            let Some(text) = name.to_str() else {
                continue;
            };
            let Some(digits) = text
                .strip_prefix("manifest-")
                .and_then(|s| s.strip_suffix(".json"))
            else {
                continue;
            };
            let observed = digits
                .parse::<u64>()
                .map_err(|_| identity("invalid recognizable tier manifest generation"))?;
            self.charge(0, 1, 128, 0, 128)?;
            if text != format!("manifest-{observed:020}.json") {
                return Err(identity("noncanonical native tier manifest name"));
            }
            let selected = generation == Some(observed);
            let kind = if selected {
                RecoverySourceKind::TierManifest
            } else {
                RecoverySourceKind::UnreferencedTierManifest
            };
            let file = self.open_file(&directory, &name, OwnedFileAccess::ReadOnly)?;
            let raw = self.blob(file, kind)?;
            let source_index = self.sources.len().checked_sub(1).ok_or_else(limited)?;
            let manifest = self.decode_manifest(&raw, shard, observed)?;
            if selected {
                // The retained observation and the verified-current pass each
                // own a native value; admit that second copy before cloning.
                self.charge(
                    0,
                    1,
                    raw.len().checked_mul(1024).ok_or_else(limited)?,
                    0,
                    (raw.len() as u64).checked_mul(16).ok_or_else(limited)?,
                )?;
                current = Some(manifest.clone());
            }
            self.charge(0, 1, size_of::<RecoveryTierManifestObservation>(), 0, 0)?;
            self.tier_manifests.try_reserve(1).map_err(|_| limited())?;
            self.tier_manifests.push(RecoveryTierManifestObservation {
                source_index,
                physical_shard: shard,
                selected_by_current: selected,
                native: manifest,
            });
        }
        if generation.is_some() && current.is_none() {
            return Err(identity("native tier CURRENT selects a missing manifest"));
        }
        directory.validate().map_err(identity)?;
        Ok(current)
    }

    fn decode_manifest(
        &mut self,
        raw: &[u8],
        shard: ShardId,
        generation: u64,
    ) -> Result<TierManifest> {
        // Pre-admit parser scratch before generic token inspection; typed
        // native Vec/String materialization follows its measured shape charge.
        self.charge(
            0,
            1,
            raw.len().checked_mul(4).ok_or_else(limited)?,
            0,
            (raw.len() as u64).checked_mul(8).ok_or_else(limited)?,
        )?;
        // The visitor debits this SAME attempt before every actual node visit,
        // including a visit that later fails native JSON shape validation.
        let nodes = json_shape::inspect(raw, self)?;
        self.charge(
            0,
            0,
            nodes
                .checked_mul(1024)
                .and_then(|n| n.checked_add(raw.len().checked_mul(4)?))
                .ok_or_else(limited)?,
            0,
            (raw.len() as u64).checked_mul(16).ok_or_else(limited)?,
        )?;
        let manifest: TierManifest = serde_json::from_slice(raw)
            .map_err(|_| identity("invalid native tier manifest JSON"))?;
        manifest.validate(shard)?;
        if manifest.generation != generation {
            return Err(identity(
                "tier CURRENT and native manifest generation disagree",
            ));
        }
        Ok(manifest)
    }

    fn scan_tier(&mut self, root: &OwnedDirectoryHandle, shard: ShardId) -> Result<()> {
        self.charge(0, 1, 256, 0, 256)?;
        let shard_name = format!("shard-{shard}");
        let manifest = self.tier_manifest(root, shard, &shard_name)?;
        let objects = manifest
            .as_ref()
            .map_or(&[][..], |manifest| manifest.objects.as_slice());
        self.charge(
            0,
            objects.len(),
            objects.len().checked_mul(128).ok_or_else(limited)?,
            0,
            0,
        )?;
        let mut referenced = HashSet::new();
        referenced
            .try_reserve(objects.len())
            .map_err(|_| limited())?;
        for object in objects {
            self.clock()?;
            self.charge(
                0,
                1,
                object.object_key.len(),
                0,
                object.object_key.len() as u64,
            )?;
            if !referenced.insert(object.object_key.clone()) {
                return Err(identity("duplicate native tier object key"));
            }
            let file = self.descendant(root, Path::new(&object.object_key))?;
            let first = self.extents.len();
            let source = self.scan_source(
                file,
                RecoverySourceKind::TierObject,
                shard,
                object.pack_sequence,
            )?;
            let facts = &self.sources[source].fact;
            if !facts.partial_tail.is_empty()
                || facts.bytes != object.bytes
                || blake3::Hash::from_bytes(facts.blake3).to_hex().as_str() != object.checksum
            {
                return Err(identity("native tier object length/hash/tail disagrees"));
            }
            self.charge(
                0,
                self.extents.len() - first,
                (self.extents.len() - first)
                    .checked_mul(size_of::<TierExtent>() + size_of::<shardlog::Reservation>() + 128)
                    .ok_or_else(limited)?,
                0,
                ((self.extents.len() - first) as u64)
                    .checked_mul(256)
                    .ok_or_else(limited)?,
            )?;
            let mut ordered = self.extents[first..]
                .iter()
                .map(|entry| entry.native.reservation)
                .collect::<Vec<_>>();
            ordered.sort_by_key(|entry| (entry.topic_id, entry.partition_id, entry.first_offset));
            let coverage = ordered
                .into_iter()
                .map(TierExtent::from_reservation)
                .collect::<Vec<_>>();
            if coverage != object.extents {
                return Err(identity(
                    "native tier manifest extent coverage disagrees with actual pack",
                ));
            }
        }
        // CURRENT is a referenced snapshot, not proof that older writer
        // protocols stopped copying. Preserve recognizable unreferenced files.
        let Some(objects_dir) = self.optional_directory(root, OsStr::new("objects"))? else {
            return Ok(());
        };
        let Some(directory) = self.optional_directory(&objects_dir, OsStr::new(&shard_name))?
        else {
            return Ok(());
        };
        self.charge(1, 1, 65_536, 0, 0)?;
        let mut names = directory.entries().map_err(identity)?;
        loop {
            self.charge(0, 1, 512, 0, 512)?;
            let Some(name) = names.next() else {
                break;
            };
            let name = name.map_err(identity)?;
            let Some(text) = name.to_str() else {
                continue;
            };
            let Some(digits) = text
                .trim_start_matches('.')
                .strip_prefix("pack-")
                .and_then(|p| p.split('.').next())
            else {
                continue;
            };
            let sequence = digits
                .parse::<u64>()
                .map_err(|_| identity("unclassified recognizable tier object name"))?;
            let key = format!("objects/{shard_name}/{text}");
            if referenced.contains(&key) {
                continue;
            }
            let file = self.open_file(&directory, &name, OwnedFileAccess::ReadOnly)?;
            self.scan_source(
                file,
                RecoverySourceKind::UnreferencedTierObject,
                shard,
                sequence,
            )?;
        }
        root.validate().map_err(identity)
    }

    fn hash_file(&mut self, file: &File, length: u64) -> Result<[u8; 32]> {
        let size = usize::try_from(length.min(65_536)).map_err(|_| limited())?;
        let mut buffer = self.buffer(size)?;
        let mut hash = blake3::Hasher::new();
        let mut offset = 0u64;
        while offset < length {
            let size = usize::try_from((length - offset).min(buffer.len() as u64))
                .map_err(|_| limited())?;
            self.read_at(file, &mut buffer[..size], offset)?;
            self.charge(0, 0, 0, 0, size as u64)?;
            hash.update(&buffer[..size]);
            offset = offset.checked_add(size as u64).ok_or_else(limited)?;
        }
        Ok(*hash.finalize().as_bytes())
    }

    fn scan_source(
        &mut self,
        handle: OwnedFileHandle,
        kind: RecoverySourceKind,
        shard: ShardId,
        sequence: u64,
    ) -> Result<usize> {
        handle.validate().map_err(identity)?;
        let metadata = handle.file().metadata().map_err(StorageError::from)?;
        let digest = self.hash_file(handle.file(), metadata.len())?;
        self.charge(0, 1, size_of::<Source>() + self.limits.max_path_bytes, 0, 0)?;
        let source_index = self.sources.len();
        self.sources.try_reserve(1).map_err(|_| limited())?;
        let path = handle.path();
        self.sources.push(Source {
            fact: RecoverySourceFact {
                path,
                kind,
                device: metadata.dev(),
                inode: metadata.ino(),
                bytes: metadata.len(),
                blake3: digest,
                partial_tail: Vec::new(),
                extent_tail: None,
            },
            handle,
        });
        // Retain the actual opened handle in state before any parser failure.
        // A duplicate descriptor retains the same open object, never a path reopen.
        self.charge(1, 1, self.limits.max_path_bytes, 0, 256)?;
        let file = self.sources[source_index]
            .handle
            .file()
            .try_clone()
            .map_err(StorageError::from)?;
        let path = self.sources[source_index].fact.path.clone();
        let mut offset = 0u64;
        let first_extent = self.extents.len();
        while offset < metadata.len() {
            let journal = kind == RecoverySourceKind::Coordinator;
            let prefix_bytes = if journal {
                RECOVERY_JOURNAL_PREFIX_BYTES
            } else {
                RECOVERY_EXTENT_PREFIX_BYTES
            };
            if metadata.len() - offset < prefix_bytes as u64 {
                self.tail(source_index, &file, offset, metadata.len(), kind, shard)?;
                break;
            }
            let mut prefix = [0u8; RECOVERY_EXTENT_PREFIX_BYTES];
            self.read_at(&file, &mut prefix[..prefix_bytes], offset)?;
            let length = if journal {
                recovery_journal_frame_bytes(
                    &path,
                    offset,
                    prefix[..prefix_bytes].try_into().map_err(|_| limited())?,
                )?
            } else {
                recovery_extent_frame_bytes(&path, offset, &prefix, self.config.max_batch_bytes)?
            };
            if length > self.limits.max_frame_bytes {
                return Err(limited());
            }
            if length as u64 > metadata.len() - offset {
                self.tail(source_index, &file, offset, metadata.len(), kind, shard)?;
                break;
            }
            let mut frame = self.buffer(length)?;
            self.read_at(&file, &mut frame, offset)?;
            // Native shard vectors/sets are bounded by their encoded body. This
            // conservative logical reservation precedes the real decoder.
            let declared = if journal {
                length.checked_mul(64)
            } else {
                Some(size_of::<RecoveryExtentObservation>() * 2)
            }
            .ok_or_else(limited)?;
            self.charge(
                0,
                if journal { length } else { 2 },
                declared,
                0,
                (length as u64).checked_mul(8).ok_or_else(limited)?,
            )?;
            if journal {
                let event = inspect_recovery_journal(&path, offset, &frame)?;
                self.journal.try_reserve(1).map_err(|_| limited())?;
                self.journal.push(event);
            } else {
                let native = inspect_recovery_extent(
                    &path,
                    offset,
                    &frame,
                    self.config.max_batch_bytes,
                    sequence,
                )?;
                self.charge(
                    0,
                    0,
                    0,
                    0,
                    ((self.extents.len() - first_extent) as u64)
                        .checked_mul(size_of::<RecoveryExtentObservation>() as u64)
                        .ok_or_else(limited)?,
                )?;
                if self.extents[first_extent..].iter().any(|old| {
                    old.native.reservation.topic_id == native.reservation.topic_id
                        && old.native.reservation.partition_id == native.reservation.partition_id
                        && old.native.reservation.batch_id == native.reservation.batch_id
                }) {
                    return Err(EngineError::CorruptState(
                        "duplicate native batch within a physical source".into(),
                    ));
                }
                let configured_shard = self.config.physical_shard_for_lane(ShardId::new(
                    native.reservation.placement.virtual_lane_id.get(),
                ));
                self.extents.try_reserve(1).map_err(|_| limited())?;
                self.extents.push(RecoveryExtentObservation {
                    source_index,
                    file_offset: offset,
                    physical_shard: shard,
                    configured_shard,
                    native,
                });
            }
            offset = offset.checked_add(length as u64).ok_or_else(limited)?;
        }
        if self.hash_file(&file, metadata.len())? != digest {
            return Err(identity("native source changed during census"));
        }
        let after = file.metadata().map_err(StorageError::from)?;
        if after.len() != metadata.len()
            || after.mtime() != metadata.mtime()
            || after.mtime_nsec() != metadata.mtime_nsec()
        {
            return Err(identity(
                "native source identity/length changed during census",
            ));
        }
        self.sources[source_index]
            .handle
            .validate()
            .map_err(identity)?;
        self.clock()?;
        Ok(source_index)
    }

    fn tail(
        &mut self,
        index: usize,
        file: &File,
        offset: u64,
        length: u64,
        kind: RecoverySourceKind,
        shard: ShardId,
    ) -> Result<()> {
        let remaining = usize::try_from(length - offset).map_err(|_| limited())?;
        if remaining > self.limits.max_partial_tail_bytes {
            return Err(limited());
        }
        let mut raw = self.buffer(remaining)?;
        self.read_at(file, &mut raw, offset)?;
        // Retain original raw evidence before deriving any interpretation.
        self.sources[index].fact.partial_tail = raw;
        if kind != RecoverySourceKind::Coordinator {
            self.admit_tail_header()?;
            let fact = &self.sources[index].fact;
            #[cfg(test)]
            TAIL_HEADER_INSPECTIONS.with(|count| count.set(count.get() + 1));
            let native = inspect_recovery_extent_tail(
                &fact.path,
                offset,
                &fact.partial_tail,
                self.config.max_batch_bytes,
            );
            self.sources[index].fact.extent_tail = Some(RecoveryExtentTailObservation {
                source_index: index,
                file_offset: offset,
                physical_shard: shard,
                native,
            });
            self.clock()?;
        }
        Ok(())
    }

    // Admission precedes every fixed-header probe/parser/result construction.
    // Native error paths allocate at most one copied path plus static text;
    // reserve four path/message copies conservatively, without cloning raw data.
    fn admit_tail_header(&mut self) -> Result<()> {
        let paths = self
            .limits
            .max_path_bytes
            .checked_mul(4)
            .ok_or_else(limited)?;
        let fixed = size_of::<RecoveryExtentHeader>()
            .checked_mul(3)
            .and_then(|n| n.checked_add(2 * size_of::<RecoveryExtentTailObservation>()))
            .and_then(|n| n.checked_add(size_of::<RecoveryExtentTail>()))
            .and_then(|n| n.checked_add(RECOVERY_EXTENT_HEADER_BYTES))
            .and_then(|n| n.checked_add(paths))
            .and_then(|n| n.checked_add(512))
            .ok_or_else(limited)?;
        let work = RECOVERY_EXTENT_HEADER_BYTES
            .checked_mul(4)
            .and_then(|n| n.checked_add(fixed))
            .ok_or_else(limited)?;
        self.charge(0, 3, fixed, 0, u64::try_from(work).map_err(|_| limited())?)
    }

    fn validate_sources(&mut self) -> Result<()> {
        for directory in &self.directories {
            self.clock()?;
            directory.validate().map_err(identity)?;
        }
        for index in 0..self.sources.len() {
            self.clock()?;
            self.charge(1, 1, 256, 0, 256)?;
            let source = &self.sources[index];
            source.handle.validate().map_err(identity)?;
            let file = source
                .handle
                .file()
                .try_clone()
                .map_err(StorageError::from)?;
            let length = source.fact.bytes;
            let expected = source.fact.blake3;
            if file.metadata().map_err(StorageError::from)?.len() != length
                || self.hash_file(&file, length)? != expected
            {
                return Err(identity(
                    "native source changed before final census closure",
                ));
            }
        }
        Ok(())
    }

    fn clock(&self) -> Result<()> {
        if Instant::now() >= self.expiry {
            Err(EngineError::InvalidConfig(
                "native recovery original expiry elapsed".into(),
            ))
        } else {
            Ok(())
        }
    }

    fn charge(
        &mut self,
        files: usize,
        entries: usize,
        bytes: usize,
        read: u64,
        work: u64,
    ) -> Result<()> {
        self.clock()?;
        // Closed prepayment for this exact repeated source-validation pass.
        // It comes only from actual retained source sizes, is never refunded,
        // and cannot be used by a caller or a different native operation.
        if let Some(credit) = &mut self.validation_credit {
            *credit = NativeRecoveryUsage {
                files: credit.files.checked_sub(files).ok_or_else(limited)?,
                entries: credit.entries.checked_sub(entries).ok_or_else(limited)?,
                declared_bytes: credit
                    .declared_bytes
                    .checked_sub(bytes)
                    .ok_or_else(limited)?,
                read_bytes: credit.read_bytes.checked_sub(read).ok_or_else(limited)?,
                work_bytes: credit.work_bytes.checked_sub(work).ok_or_else(limited)?,
            };
            return Ok(());
        }
        let add = |a: usize, b: usize, max: usize| {
            a.checked_add(b).filter(|n| *n <= max).ok_or_else(limited)
        };
        let wide =
            |a: u64, b: u64, max: u64| a.checked_add(b).filter(|n| *n <= max).ok_or_else(limited);
        self.usage = NativeRecoveryUsage {
            files: add(self.usage.files, files, self.limits.max_files)?,
            entries: add(self.usage.entries, entries, self.limits.max_entries)?,
            declared_bytes: add(
                self.usage.declared_bytes,
                bytes,
                self.limits.max_declared_bytes,
            )?,
            read_bytes: wide(self.usage.read_bytes, read, self.limits.max_read_bytes)?,
            work_bytes: wide(self.usage.work_bytes, work, self.limits.max_work_bytes)?,
        };
        Ok(())
    }

    fn path(&mut self, root: &OwnedDirectoryHandle, name: &OsStr) -> Result<()> {
        let length = root
            .path()
            .as_os_str()
            .as_encoded_bytes()
            .len()
            .checked_add(name.as_encoded_bytes().len())
            .and_then(|n| n.checked_add(1))
            .ok_or_else(limited)?;
        if length > self.limits.max_path_bytes {
            return Err(limited());
        }
        self.charge(
            0,
            1,
            length.checked_mul(4).ok_or_else(limited)?,
            0,
            length as u64,
        )
    }

    fn open_file(
        &mut self,
        directory: &OwnedDirectoryHandle,
        name: &OsStr,
        access: OwnedFileAccess,
    ) -> Result<OwnedFileHandle> {
        self.path(directory, name)?;
        self.charge(1, 1, size_of::<OwnedFileHandle>() + 256, 0, 256)?;
        let file = directory.open_file(name, access).map_err(identity)?;
        self.clock()?;
        Ok(file)
    }

    fn read_at(&mut self, file: &File, bytes: &mut [u8], offset: u64) -> Result<()> {
        self.charge(0, 0, 0, bytes.len() as u64, bytes.len() as u64)?;
        file.read_exact_at(bytes, offset)
            .map_err(StorageError::from)?;
        self.clock()
    }

    fn buffer(&mut self, length: usize) -> Result<Vec<u8>> {
        self.charge(0, 1, length, 0, length as u64)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(|_| limited())?;
        bytes.resize(length, 0);
        Ok(bytes)
    }
}

mod json_shape {
    use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
    use std::fmt;

    use super::{Attempt, Result, identity};

    struct Shape<'a> {
        attempt: &'a mut Attempt,
        depth: usize,
    }

    impl<'de> DeserializeSeed<'de> for Shape<'_> {
        type Value = ();
        fn deserialize<D: serde::Deserializer<'de>>(
            self,
            deserializer: D,
        ) -> std::result::Result<(), D::Error> {
            self.attempt
                .charge(0, 1, 0, 0, 0)
                .map_err(|_| serde::de::Error::custom("JSON cumulative node limit or expiry"))?;
            if self.depth > 32 {
                return Err(serde::de::Error::custom("JSON depth limit"));
            }
            deserializer.deserialize_any(self)
        }
    }

    impl<'de> Visitor<'de> for Shape<'_> {
        type Value = ();
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("bounded native JSON")
        }
        fn visit_bool<E: serde::de::Error>(self, _: bool) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_i64<E: serde::de::Error>(self, _: i64) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_u64<E: serde::de::Error>(self, _: u64) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_f64<E: serde::de::Error>(self, _: f64) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_str<E: serde::de::Error>(self, _: &str) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> std::result::Result<(), S::Error> {
            while seq
                .next_element_seed(Shape {
                    attempt: &mut *self.attempt,
                    depth: self.depth + 1,
                })?
                .is_some()
            {}
            Ok(())
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> std::result::Result<(), M::Error> {
            while map
                .next_key_seed(Shape {
                    attempt: &mut *self.attempt,
                    depth: self.depth + 1,
                })?
                .is_some()
            {
                map.next_value_seed(Shape {
                    attempt: &mut *self.attempt,
                    depth: self.depth + 1,
                })?;
            }
            Ok(())
        }
    }

    pub(super) fn inspect(bytes: &[u8], attempt: &mut Attempt) -> Result<usize> {
        let before = attempt.usage.entries;
        let mut decoder = serde_json::Deserializer::from_slice(bytes);
        Shape {
            attempt: &mut *attempt,
            depth: 0,
        }
        .deserialize(&mut decoder)
        .map_err(|_| identity("native manifest JSON shape refused"))?;
        decoder
            .end()
            .map_err(|_| identity("native manifest JSON has trailing bytes"))?;
        Ok(attempt.usage.entries - before)
    }
}

#[cfg(test)]
#[path = "recovery_census_tests.rs"]
mod tests;

#[cfg(test)]
std::thread_local! {
    static TAIL_HEADER_INSPECTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
