//! Retained physical root validation through the original private native files.

use super::validation_until::{
    validate_ancestor_identity, validate_lock_stat_identity, validate_root_named_identity,
    validate_root_spelling, validate_root_stat_identity,
};
use super::{DirectoryOwnership, DirectoryOwnershipError, LOCK_NAME};
use crate::owned_directory_handle::checked_validation::{
    CheckedDirectoryValidationFailure, CheckedDirectoryValidationPhase as Phase, Effects, Step,
    Value, Work,
};
use rustix::fs::{AtFlags, CWD, Mode, OFlags, Stat, fstat, openat, statat};
use std::{
    ffi::{CString, NulError},
    io,
    path::{Component, Path},
};

impl DirectoryOwnership {
    pub(crate) fn validate_retained_checked(
        &self,
        directory: &Path,
        effects: &mut Effects,
        work: &Work<'_>,
    ) -> Step<()> {
        let bytes = directory.as_os_str().as_encoded_bytes().len();
        let canonical = self
            .canonical_directory
            .as_os_str()
            .as_encoded_bytes()
            .len();
        effects.scan(work, bytes, 8)?;
        effects.scan(work, canonical, 4)?;
        effects.prepare_typed::<(), DirectoryOwnershipError>(work, Phase::Identity, 0, 0)?;
        effects.identity(
            work,
            validate_root_spelling(directory, &self.canonical_directory),
        )?;
        let Some(path_buffer) = bytes.checked_add(1) else {
            return effects.capacity();
        };
        effects.prepare_typed::<std::fs::Metadata, io::Error>(
            work,
            Phase::RootPathMetadata,
            1,
            path_buffer,
        )?;
        let initial = effects.completed(
            work,
            Phase::RootPathMetadata,
            Value::Metadata(std::fs::symlink_metadata(directory)),
        )?;
        for ancestor in directory.ancestors().filter(|p| !p.as_os_str().is_empty()) {
            let Some(path_buffer) = ancestor.as_os_str().as_encoded_bytes().len().checked_add(1)
            else {
                return effects.capacity();
            };
            effects.prepare_typed::<std::fs::Metadata, io::Error>(
                work,
                Phase::AncestorMetadata,
                1,
                path_buffer,
            )?;
            let metadata = effects.optional_metadata_completed(
                work,
                Phase::AncestorMetadata,
                std::fs::symlink_metadata(ancestor),
            )?;
            if let Some(index) = metadata {
                effects.prepare_typed::<(), DirectoryOwnershipError>(
                    work,
                    Phase::Identity,
                    0,
                    0,
                )?;
                effects.identity(work, validate_ancestor_identity(effects.metadata(index)?))?;
            }
        }
        effects.prepare_typed::<(), DirectoryOwnershipError>(work, Phase::Identity, 0, 0)?;
        effects.identity(
            work,
            validate_root_named_identity(effects.metadata(initial)?, self.directory_identity),
        )?;
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        effects.prepare_typed::<rustix::fd::OwnedFd, rustix::io::Errno>(
            work,
            Phase::RootOpened,
            1,
            0,
        )?;
        let mut resolved = effects.completed(
            work,
            Phase::RootOpened,
            Value::Open(openat(CWD, c"/", flags, Mode::empty())),
        )?;
        for component in directory.components() {
            let name = match component {
                Component::RootDir => continue,
                Component::Normal(n) => n,
                _ => return Err(CheckedDirectoryValidationFailure::Ownership),
            };
            if name.as_encoded_bytes().len() > work.core.limits().max_component_bytes {
                return effects.capacity();
            }
            effects.scan(work, name.as_encoded_bytes().len(), 4)?;
            let Some(strings) = name.as_encoded_bytes().len().checked_add(1) else {
                return effects.capacity();
            };
            effects.prepare_typed::<CString, NulError>(work, Phase::ComponentName, 0, strings)?;
            let name_index = effects.completed(
                work,
                Phase::ComponentName,
                Value::Name(CString::new(name.as_encoded_bytes())),
            )?;
            effects.prepare_typed::<rustix::fd::OwnedFd, rustix::io::Errno>(
                work,
                Phase::ComponentOpened,
                1,
                0,
            )?;
            let observed = openat(
                effects.fd(resolved)?,
                effects.name(name_index)?,
                flags,
                Mode::empty(),
            );
            resolved = effects.completed(work, Phase::ComponentOpened, Value::Open(observed))?;
        }
        effects.prepare_typed::<Stat, rustix::io::Errno>(work, Phase::ResolvedRootStat, 1, 0)?;
        let raw = fstat(effects.fd(resolved)?);
        let opened = effects.completed(work, Phase::ResolvedRootStat, Value::Stat(raw))?;
        effects.prepare_typed::<Stat, rustix::io::Errno>(work, Phase::RetainedRootStat, 1, 0)?;
        let retained = effects.completed(
            work,
            Phase::RetainedRootStat,
            Value::Stat(fstat(&self._directory)),
        )?;
        effects.prepare_typed::<(), DirectoryOwnershipError>(work, Phase::Identity, 0, 0)?;
        effects.identity(
            work,
            validate_root_stat_identity(effects.stat(opened)?, effects.stat(retained)?),
        )?;
        effects.prepare_typed::<CString, NulError>(
            work,
            Phase::ComponentName,
            0,
            LOCK_NAME.len() + 1,
        )?;
        let lock_index = effects.completed(
            work,
            Phase::ComponentName,
            Value::Name(CString::new(LOCK_NAME.as_bytes())),
        )?;
        effects.prepare_typed::<Stat, rustix::io::Errno>(work, Phase::NamedLockStat, 1, 0)?;
        let raw = statat(
            effects.fd(resolved)?,
            effects.name(lock_index)?,
            AtFlags::SYMLINK_NOFOLLOW,
        );
        let named = effects.completed(work, Phase::NamedLockStat, Value::Stat(raw))?;
        effects.prepare_typed::<Stat, rustix::io::Errno>(work, Phase::RetainedLockStat, 1, 0)?;
        let retained = effects.completed(
            work,
            Phase::RetainedLockStat,
            Value::Stat(fstat(&self._lock)),
        )?;
        effects.prepare_typed::<(), DirectoryOwnershipError>(work, Phase::Identity, 0, 0)?;
        effects.identity(
            work,
            validate_lock_stat_identity(effects.stat(named)?, effects.stat(retained)?),
        )?;
        effects.checkpoint(work)
    }
}
