//! Actual guarded object restoration and native worker handoff, without authority.

use super::*;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_RESTORE: AtomicU64 = AtomicU64::new(0);
const COPY_BYTES: usize = 65_536;

struct CopyPlan {
    shard: ShardId,
    sequence: u64,
    source: usize,
    present: bool,
}

impl Attempt {
    fn restore_objects(&mut self) -> Result<()> {
        self.clock()?;
        let headers = self
            .tier_manifests
            .len()
            .checked_add(self.sources.len())
            .and_then(|n| n.checked_add(self.missing_paths.len()))
            .and_then(|n| n.checked_add(1))
            .ok_or_else(limited)?;
        let per_header = self
            .limits
            .max_path_bytes
            .checked_add(size_of::<RecoveryTierManifestObservation>())
            .and_then(|n| n.checked_add(size_of::<Source>()))
            .ok_or_else(limited)?;
        let visits = headers.checked_mul(per_header).ok_or_else(limited)?;
        self.charge(
            self.missing_paths.len(),
            headers,
            0,
            0,
            u64::try_from(visits).map_err(|_| limited())?,
        )?;
        self.validate_sources()?;
        for path in &self.missing_paths {
            self.clock()?;
            match std::fs::symlink_metadata(path) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                _ => {
                    return Err(identity(
                        "original absent native path changed before restore",
                    ));
                }
            }
        }
        // Admit all manifest/source probes, plan/name slots and complete native
        // reads/writes/hash/scratch BEFORE inspecting objects or mutating names.
        let objects = self
            .tier_manifests
            .iter()
            .filter(|m| m.selected_by_current)
            .try_fold(0usize, |sum, m| {
                sum.checked_add(m.native.objects.len()).ok_or_else(limited)
            })?;
        let add = |a: usize, b: usize| a.checked_add(b).ok_or_else(limited);
        let mul = |a: usize, b: usize| a.checked_mul(b).ok_or_else(limited);
        let configured = usize::try_from(self.config.shard_count).map_err(|_| limited())?;
        let units = add(add(objects, configured)?, 1)?;
        let manifest_slots = add(self.tier_manifests.len(), 1)?;
        let probes = mul(
            units,
            add(add(self.sources.len(), objects)?, manifest_slots)?,
        )?;
        let slots = mul(
            units,
            size_of::<CopyPlan>()
                + 8 * size_of::<OwnedFileHandle>()
                + 8 * size_of::<OwnedDirectoryHandle>()
                + 2 * size_of::<ShardRecoveryGuard>(),
        )?;
        let paths = mul(units, mul(self.limits.max_path_bytes, 16)?)?;
        self.charge(
            mul(units, 16)?,
            add(probes, mul(units, 32)?)?,
            add(slots, add(paths, COPY_BYTES)?)?,
            0,
            u64::try_from(add(
                mul(
                    probes,
                    add(size_of::<Source>(), self.limits.max_path_bytes)?,
                )?,
                paths,
            )?)
            .map_err(|_| limited())?,
        )?;
        let mut plans = Vec::new();
        plans.try_reserve_exact(objects).map_err(|_| limited())?;
        let mut total = 0u64;
        for manifest in &self.tier_manifests {
            self.clock()?;
            if !manifest.selected_by_current
                || manifest.physical_shard.get() >= self.config.shard_count
            {
                continue;
            }
            let root = self.tier_root.as_ref().ok_or_else(limited)?;
            for object in &manifest.native.objects {
                self.clock()?;
                let path = root.path().join(&object.object_key);
                let source = self
                    .sources
                    .iter()
                    .position(|s| {
                        s.fact.kind == RecoverySourceKind::TierObject && s.fact.path == path
                    })
                    .ok_or_else(|| identity("CURRENT object has no retained verified source"))?;
                let fact = &self.sources[source].fact;
                total = total.checked_add(fact.bytes).ok_or_else(limited)?;
                let primary = self
                    .config
                    .data_dir
                    .join("shards")
                    .join(format!("shard-{}", manifest.physical_shard))
                    .join(format!("pack-{:020}.sse", object.pack_sequence));
                if primary
                    .as_os_str()
                    .len()
                    .checked_add(128)
                    .is_none_or(|n| n > self.limits.max_path_bytes)
                {
                    return Err(limited());
                }
                for old in &self.sources {
                    if !matches!(
                        old.fact.kind,
                        RecoverySourceKind::PrimaryPack | RecoverySourceKind::PendingDeletion
                    ) || old.fact.path.parent() != primary.parent()
                    {
                        continue;
                    }
                    let same = old
                        .fact
                        .path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .and_then(|n| n.strip_prefix("pack-"))
                        .and_then(|n| {
                            n.strip_suffix(".sse")
                                .or_else(|| n.strip_suffix(".sse.gc-pending"))
                        })
                        .and_then(|n| n.parse::<u64>().ok())
                        == Some(object.pack_sequence);
                    if same
                        && (old.fact.bytes != fact.bytes
                            || old.fact.blake3 != fact.blake3
                            || !old.fact.partial_tail.is_empty())
                    {
                        return Err(identity(
                            "recognizable primary/pending pack conflicts with CURRENT object",
                        ));
                    }
                }
                let existing = self.sources.iter().find(|s| {
                    s.fact.kind == RecoverySourceKind::PrimaryPack && s.fact.path == primary
                });
                let present = if let Some(old) = existing {
                    if old.fact.bytes != fact.bytes
                        || old.fact.blake3 != fact.blake3
                        || !old.fact.partial_tail.is_empty()
                    {
                        return Err(identity(
                            "primary differs from CURRENT object; refusing replacement",
                        ));
                    }
                    true
                } else {
                    match std::fs::symlink_metadata(&primary) {
                        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
                        _ => {
                            return Err(identity(
                                "uncensused destination exists before object restore",
                            ));
                        }
                    }
                };
                plans.push(CopyPlan {
                    shard: manifest.physical_shard,
                    sequence: object.pack_sequence,
                    source,
                    present,
                });
            }
        }
        // CURRENT packs remain immutable. If all newer local active files are
        // absent, create the native empty successor BEFORE primary discovery;
        // otherwise native open would append into the last restored object.
        let mut active = Vec::new();
        active
            .try_reserve_exact(configured)
            .map_err(|_| limited())?;
        for number in 0..self.config.shard_count {
            self.clock()?;
            let shard = ShardId::new(number);
            let directory = self
                .config
                .data_dir
                .join("shards")
                .join(format!("shard-{shard}"));
            let mut highest = None;
            for source in &self.sources {
                if source.fact.kind != RecoverySourceKind::PrimaryPack
                    || source.fact.path.parent() != Some(directory.as_path())
                {
                    continue;
                }
                let sequence = source
                    .fact
                    .path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|n| n.strip_prefix("pack-"))
                    .and_then(|n| n.strip_suffix(".sse"))
                    .and_then(|n| n.parse::<u64>().ok())
                    .ok_or_else(limited)?;
                highest = Some(highest.map_or(sequence, |old: u64| old.max(sequence)));
            }
            let current = self
                .tier_manifests
                .iter()
                .filter(|m| m.selected_by_current && m.physical_shard == shard)
                .flat_map(|m| m.native.objects.iter())
                .map(|o| o.pack_sequence)
                .max();
            if let Some(last) = current {
                if highest.is_none_or(|primary| primary <= last) {
                    active.push((shard, last.checked_add(1).ok_or_else(limited)?));
                }
            }
        }

        // Both source verification passes, copy reads/writes and final native
        // hash work are prepaid. Actual per-chunk operations still check E.
        self.charge(
            0,
            0,
            0,
            total.checked_mul(2).ok_or_else(limited)?,
            total.checked_mul(8).ok_or_else(limited)?,
        )?;
        self.restored_files
            .try_reserve_exact(units)
            .map_err(|_| limited())?;
        self.restored_parents
            .try_reserve_exact(units.checked_mul(2).ok_or_else(limited)?)
            .map_err(|_| limited())?;
        self.shards.try_reserve(configured).map_err(|_| limited())?;
        let admitted = self.storage_open_admission()?;
        self.charge(
            admitted.files,
            admitted.entries,
            admitted.declared_bytes,
            admitted.read_bytes,
            admitted.work_bytes,
        )?;
        self.storage_open_prepaid = true;
        // The existing open_storage closure revalidates every original source
        // AFTER copying. Prepay that exact pass before any publication rather
        // than discover a final one-less allowance after canonical mutation.
        let mut validation = NativeRecoveryUsage::default();
        for source in &self.sources {
            self.clock()?;
            let scratch = usize::try_from(source.fact.bytes.min(65_536)).map_err(|_| limited())?;
            validation.files = add(validation.files, 1)?;
            validation.entries = add(validation.entries, 2)?;
            validation.declared_bytes = add(validation.declared_bytes, add(256, scratch)?)?;
            validation.read_bytes = validation
                .read_bytes
                .checked_add(source.fact.bytes)
                .ok_or_else(limited)?;
            validation.work_bytes = validation
                .work_bytes
                .checked_add(source.fact.bytes.checked_mul(2).ok_or_else(limited)?)
                .and_then(|n| n.checked_add(scratch as u64 + 256))
                .ok_or_else(limited)?;
        }
        self.charge(
            validation.files,
            validation.entries,
            validation.declared_bytes,
            validation.read_bytes,
            validation.work_bytes,
        )?;
        self.validation_prepaid = Some(validation);
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(COPY_BYTES)
            .map_err(|_| limited())?;
        buffer.resize(COPY_BYTES, 0);
        self.restore_started = true;
        for number in 0..self.config.shard_count {
            self.clock()?;
            let root = self.root.as_ref().ok_or_else(limited)?.retain();
            if root
                .path()
                .as_os_str()
                .len()
                .checked_add(192)
                .is_none_or(|n| n > self.limits.max_path_bytes)
            {
                return Err(limited());
            }
            let shards = root
                .open_directory(OsStr::new("shards"), true)
                .map_err(identity)?;
            self.restored_parents.push(shards.retain());
            self.clock()?;
            root.sync().map_err(identity)?;
            self.clock()?;
            let shard = ShardId::new(number);
            let directory = shards
                .open_directory(OsStr::new(&format!("shard-{shard}")), true)
                .map_err(identity)?;
            self.restored_parents.push(directory.retain());
            self.clock()?;
            shards.sync().map_err(identity)?;
            self.clock()?;
            if !self.shards.iter().any(|(id, _)| *id == shard) {
                let lock = directory
                    .open_file(
                        OsStr::new(".shard-store.lock"),
                        OwnedFileAccess::ReadWriteCreate,
                    )
                    .map_err(identity)?;
                self.clock()?;
                lock.file().sync_all().map_err(StorageError::from)?;
                self.clock()?;
                directory.sync().map_err(identity)?;
                self.clock()?;
                let guard = ShardRecoveryGuard::try_acquire(
                    lock.file().try_clone().map_err(StorageError::from)?,
                    directory.path().to_owned(),
                )?;
                self.shards.push((shard, guard));
            }
        }
        for plan in plans {
            self.clock()?;
            if plan.present {
                continue;
            }
            let root = self.root.as_ref().ok_or_else(limited)?.retain();
            let shards = root
                .open_directory(OsStr::new("shards"), true)
                .map_err(identity)?;
            self.clock()?;
            let directory = shards
                .open_directory(OsStr::new(&format!("shard-{}", plan.shard)), false)
                .map_err(identity)?;
            self.clock()?;
            for (_, guard) in &self.tiers {
                self.clock()?;
                guard.validate()?;
            }
            let name = format!("pack-{:020}.sse", plan.sequence);
            let temporary = format!(
                ".{name}.{}.{}.owned-restore",
                std::process::id(),
                NEXT_RESTORE.fetch_add(1, Ordering::Relaxed)
            );
            let output = directory
                .open_file(OsStr::new(&temporary), OwnedFileAccess::CreateNew)
                .map_err(identity)?;
            // Retain the physical temporary BEFORE fallible copying or hashing.
            self.restored_files.push(output);
            let output = self.restored_files.last().ok_or_else(limited)?;
            let source = &self.sources[plan.source];
            source.handle.validate().map_err(identity)?;
            let mut offset = 0u64;
            let mut hash = blake3::Hasher::new();
            let mut file = output.file();
            while offset < source.fact.bytes {
                self.clock()?;
                let n = usize::try_from((source.fact.bytes - offset).min(COPY_BYTES as u64))
                    .map_err(|_| limited())?;
                source
                    .handle
                    .file()
                    .read_exact_at(&mut buffer[..n], offset)
                    .map_err(StorageError::from)?;
                self.clock()?;
                file.write_all(&buffer[..n]).map_err(StorageError::from)?;
                self.clock()?;
                hash.update(&buffer[..n]);
                offset = offset.checked_add(n as u64).ok_or_else(limited)?;
            }
            self.clock()?;
            if *hash.finalize().as_bytes() != source.fact.blake3
                || source
                    .handle
                    .file()
                    .metadata()
                    .map_err(StorageError::from)?
                    .len()
                    != source.fact.bytes
            {
                return Err(identity("CURRENT source changed during object restore"));
            }
            self.clock()?;
            source.handle.validate().map_err(identity)?;
            self.clock()?;
            // linkat is atomic NO-REPLACE: even a last-moment name conflict
            // refuses, and no old primary bytes or unknown tail are overwritten.
            directory
                .publish_new_file_until(output, OsStr::new(&name), self.expiry)
                .map_err(identity)?;
            // Definite successful publication removed ONLY our known temp.
            // Keep its FD through preparation failure; the final successful
            // engine handoff retires it before normal GC is exposed.
            self.clock()?;
        }
        for (shard, sequence) in active {
            self.clock()?;
            let shard_name = format!("shard-{shard}");
            let directory = self
                .restored_parents
                .iter()
                .find(|d| d.path().file_name() == Some(OsStr::new(&shard_name)))
                .ok_or_else(limited)?
                .retain();
            let file = directory
                .open_file(
                    OsStr::new(&format!("pack-{sequence:020}.sse")),
                    OwnedFileAccess::CreateNew,
                )
                .map_err(identity)?;
            self.restored_files.push(file);
            self.clock()?;
            self.restored_files
                .last()
                .ok_or_else(limited)?
                .file()
                .sync_all()
                .map_err(StorageError::from)?;
            self.clock()?;
            directory.sync().map_err(identity)?;
            self.clock()?;
        }
        self.clock()
    }
}

impl OwnedRecoveryStorage {
    pub(crate) fn recovery_clock(&self) -> Result<()> {
        self.attempt.clock()
    }
    pub(crate) fn recovery_owner(&self) -> &Arc<DirectoryOwnership> {
        &self.attempt.owner
    }
    pub(crate) fn recovery_config(&self) -> &EngineConfig {
        &self.attempt.config
    }
    pub(crate) fn recovery_charge(&mut self, bytes: usize, work: u64) -> Result<()> {
        self.attempt.charge(0, 0, bytes, 0, work)
    }
    pub(crate) fn recovery_take_shard(
        &mut self,
        shard: ShardId,
    ) -> Result<(ShardStore, Option<LocalObjectTier>)> {
        self.attempt.clock()?;
        let index = self
            .attempt
            .opened
            .iter()
            .position(|(id, _, _)| *id == shard)
            .ok_or_else(limited)?;
        let (_, primary, tier) = self.attempt.opened.swap_remove(index);
        Ok((primary, tier))
    }
    pub(crate) fn admit_engine(&mut self) -> Result<()> {
        self.attempt.clock()?;
        if !self.attempt.writable_journal || !self.attempt.restore_started {
            return Err(identity(
                "consuming engine requires read_for_engine and restored storage",
            ));
        }
        let a = &mut self.attempt;
        let add = |x: usize, y: usize| x.checked_add(y).ok_or_else(limited);
        let mul = |x: usize, y: usize| x.checked_mul(y).ok_or_else(limited);
        let configured = usize::try_from(a.config.shard_count).map_err(|_| limited())?;
        let sizing_visits = mul(add(add(a.sources.len(), a.opened.len())?, 1)?, 4)?;
        a.charge(
            0,
            sizing_visits,
            mul(add(a.opened.len(), 1)?, size_of::<ShardStore>())?,
            0,
            u64::try_from(mul(
                sizing_visits,
                size_of::<ShardStore>() + size_of::<Source>(),
            )?)
            .map_err(|_| limited())?,
        )?;
        a.charge(
            0,
            a.sources.len(),
            a.sources
                .len()
                .checked_mul(size_of::<RecoverySourceFact>())
                .ok_or_else(limited)?,
            0,
            0,
        )?;
        a.engine_facts
            .try_reserve_exact(a.sources.len())
            .map_err(|_| limited())?;
        let entries = a.opened.iter().try_fold(0usize, |sum, (_, store, _)| {
            add(sum, store.recovery_catalog_len())
        })?;
        let sources = add(a.sources.len(), entries)?;
        let source_bytes = a.sources.iter().try_fold(0u64, |sum, s| {
            sum.checked_add(s.fact.bytes).ok_or_else(limited)
        })?;
        let per_object = add(
            size_of::<shardlog::TierObject>() * 4,
            add(mul(a.limits.max_path_bytes, 4)?, 1024)?,
        )?;
        let native_tier = add(
            mul(add(sources, 1)?, per_object)?,
            mul(add(entries, 1)?, size_of::<TierExtent>() * 4 + 1024)?,
        )?;
        let worker = crate::worker::recovery_worker_bytes(&a.config).ok_or_else(limited)?;
        let fixed = mul(
            configured,
            add(
                worker,
                add(
                    size_of::<crate::worker::ShardHandle>() * 4,
                    mul(a.limits.max_path_bytes, 16)?,
                )?,
            )?,
        )?;
        // Native offload clones/encodes a complete manifest per sealed pack and
        // repeatedly obtains/sorts its catalog. Reserve those actual product
        // bounds, 1MiB native hash scratch per source, and both native JSON/error
        // phases before any worker/offload construction.
        let journal_bytes = a
            .sources
            .iter()
            .filter(|s| s.fact.kind == RecoverySourceKind::Coordinator)
            .try_fold(0usize, |n, s| {
                add(n, usize::try_from(s.fact.bytes).map_err(|_| limited())?)
            })?;
        let fixed = add(fixed, mul(journal_bytes, 64)?)?;
        let bytes = add(
            fixed,
            add(
                mul(native_tier, add(sources, 8)?)?,
                add(
                    mul(
                        add(sources, 1)?,
                        LocalObjectTier::recovery_hash_buffer_bytes(),
                    )?,
                    mul(add(entries, 1)?, size_of::<ExtentCatalogEntry>() * 8)?,
                )?,
            )?,
        )?;
        let work = source_bytes
            .checked_mul(16)
            .and_then(|n| n.checked_add(u64::try_from(bytes).ok()?.checked_mul(8)?))
            .and_then(|n| {
                n.checked_add(
                    u64::try_from(
                        entries
                            .checked_add(1)?
                            .checked_mul(sources.checked_add(1)?)?,
                    )
                    .ok()?
                    .checked_mul(256)?,
                )
            })
            .ok_or_else(limited)?;
        a.charge(
            mul(add(sources, 1)?, 8)?,
            mul(add(entries, 1)?, add(sources, 8)?)?,
            bytes,
            source_bytes.checked_mul(8).ok_or_else(limited)?,
            work,
        )?;
        Ok(())
    }
    pub(crate) fn recovery_open_journal(
        &mut self,
    ) -> Result<(shardlog::CoordinatorJournal, shardlog::RecoveredJournal)> {
        self.attempt.clock()?;
        let a = &mut self.attempt;
        a.charge(
            2,
            2,
            a.limits
                .max_path_bytes
                .checked_mul(4)
                .and_then(|n| n.checked_add(1024))
                .ok_or_else(limited)?,
            0,
            1024,
        )?;
        let file = if let Some(source) = a
            .sources
            .iter()
            .find(|s| s.fact.kind == RecoverySourceKind::Coordinator)
        {
            source.handle.validate().map_err(identity)?;
            a.clock()?;
            (
                source.fact.path.clone(),
                source
                    .handle
                    .file()
                    .try_clone()
                    .map_err(StorageError::from)?,
            )
        } else {
            a.restored_files.try_reserve(1).map_err(|_| limited())?;
            let root = a.root.as_ref().ok_or_else(limited)?;
            let file = root
                .open_file(OsStr::new("coordinator.ssj"), OwnedFileAccess::CreateNew)
                .map_err(identity)?;
            a.restored_files.push(file);
            a.clock()?;
            let file = a.restored_files.last().ok_or_else(limited)?;
            file.file().sync_all().map_err(StorageError::from)?;
            a.clock()?;
            root.sync().map_err(identity)?;
            a.clock()?;
            (
                file.path(),
                file.file().try_clone().map_err(StorageError::from)?,
            )
        };
        let result =
            shardlog::CoordinatorJournal::open_existing_file_until(file.0, file.1, a.expiry)?;
        a.clock()?;
        Ok(result)
    }
}

// This final success-only owner contains observations and leases, not retired
// payload descriptors. Keeping those descriptors past normal native GC would
// retain unlinked pack bytes outside its pack/capacity account.
pub(crate) struct OwnedEngineCustody {
    usage: NativeRecoveryUsage,
    expiry: Instant,
    facts: Vec<RecoverySourceFact>,
    _directories: Vec<OwnedDirectoryHandle>,
    _restored_parents: Vec<OwnedDirectoryHandle>,
    _extents: Vec<RecoveryExtentObservation>,
    _manifests: Vec<RecoveryTierManifestObservation>,
    _journal: Vec<JournalEvent>,
    _missing_paths: Vec<PathBuf>,
    _shards: Vec<(ShardId, ShardRecoveryGuard)>,
    _tiers: Vec<(ShardId, TierRecoveryGuard)>,
    _root: Option<OwnedDirectoryHandle>,
    _tier_root: Option<OwnedDirectoryHandle>,
    owner: Arc<DirectoryOwnership>,
}
impl OwnedEngineCustody {
    pub(crate) fn usage(&self) -> NativeRecoveryUsage {
        self.usage
    }
    pub(crate) fn absolute_expiry(&self) -> Instant {
        self.expiry
    }
    pub(crate) fn facts(&self) -> impl Iterator<Item = &RecoverySourceFact> {
        self.facts.iter()
    }
    pub(crate) fn owner(&self) -> &Arc<DirectoryOwnership> {
        &self.owner
    }
}
impl OwnedRecoveryStorage {
    // Called only after actual construction/worker setup and its last clock
    // check have completed. No allocation, callback, I/O or fallible operation
    // remains. Failed/unwinding construction never invokes this retirement.
    pub(crate) fn finish_engine_custody(self) -> OwnedEngineCustody {
        let mut a = self.attempt;
        for Source { handle, fact } in a.sources.drain(..) {
            a.engine_facts.push(fact); // Capacity admitted/reserved before setup.
            drop(handle);
        }
        // Known new active/coordinator/copy snapshot FDs are now represented by
        // actual live native stores/journal. Keep no hidden retired payload FD.
        a.restored_files.clear();
        OwnedEngineCustody {
            usage: a.usage,
            expiry: a.expiry,
            facts: a.engine_facts,
            _directories: a.directories,
            _restored_parents: a.restored_parents,
            _extents: a.extents,
            _manifests: a.tier_manifests,
            _journal: a.journal,
            _missing_paths: a.missing_paths,
            _shards: a.shards,
            _tiers: a.tiers,
            _root: a.root,
            _tier_root: a.tier_root,
            owner: a.owner,
        }
    }
}
