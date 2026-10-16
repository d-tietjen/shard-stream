//! Same native recovery-lease identity policy with bounded original-E observations.

use super::*;
use std::collections::TryReserveError;
use std::os::unix::fs::MetadataExt;

const LOCK_NAME: &str = ".shard-store.lock";

/// Explicit source limits for one native recovery-guard validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardGuardValidationLimits {
    /// Maximum requested complete lock-path bytes, including its separator.
    pub max_path_bytes: usize,
    /// Maximum reserved metadata call slots, including refused late entry.
    pub max_io_calls: usize,
}

/// Logical source sizes and native phase slots; no allocator/OS/RSS claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardGuardValidationCost {
    /// Fixed result/error values plus the requested pathname ceiling.
    pub declared_bytes: usize,
    /// Requested pathname bytes, a subset of declared bytes.
    pub string_bytes: usize,
    /// Fixed observation value and two native metadata result slots.
    pub entries: usize,
    /// Fixed source bytes, inspected/copied path bytes and two metadata calls.
    pub work_bytes: usize,
}

/// Derives checked costs before the composing owner admits this observation.
pub fn shard_guard_validation_cost(
    limits: ShardGuardValidationLimits,
) -> Option<ShardGuardValidationCost> {
    let declared_bytes = size_of::<ShardGuardValidationError>()
        .checked_add(size_of::<ShardGuardValidation>())?
        .checked_add(limits.max_path_bytes)?;
    Some(ShardGuardValidationCost {
        declared_bytes,
        string_bytes: limits.max_path_bytes,
        entries: 3,
        work_bytes: declared_bytes
            .checked_add(limits.max_path_bytes.checked_mul(2)?)?
            .checked_add(2)?,
    })
}

/// Refusal of one original-expiry native identity observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardGuardValidationFailure {
    /// Original absolute expiry reached, possibly after a completed native phase.
    Expired,
    /// A path or native-call allowance refused before the next physical phase.
    Capacity,
    /// Checked source path or cost arithmetic overflowed.
    Arithmetic,
    /// Actual pathname reservation failed; the original error is retained.
    Allocation,
    /// Actual native metadata operation failed; its original error is retained.
    Io,
    /// The existing native named-file/descriptor identity predicate refused.
    Identity,
}

/// Actual native path/metadata outcomes; this contains no lease or serving grant.
///
/// The caller keeps the same actual `ShardRecoveryGuard` while this synchronous
/// observation runs and through refusal handling. This does not validate parent
/// directories, acquire a lock, or qualify a later open/repair operation.
#[derive(Debug)]
pub struct ShardGuardValidation {
    expiry: Instant,
    lock_path: PathBuf,
    requested_path_bytes: usize,
    io_calls: usize,
    entered_io_calls: usize,
    named: Option<fs::Metadata>,
    actual: Option<fs::Metadata>,
    identity_changed: Option<bool>,
    io_error: Option<std::io::Error>,
    allocation_error: Option<TryReserveError>,
}

impl ShardGuardValidation {
    /// Original caller-supplied absolute expiry.
    pub fn absolute_expiry(&self) -> Instant {
        self.expiry
    }
    /// Actual retained pathname, possibly partial on refusal.
    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }
    /// Logical pathname reservation requested, including a failed request.
    pub fn requested_path_bytes(&self) -> usize {
        self.requested_path_bytes
    }
    /// Reserved native metadata slots, including a refused late entry.
    pub fn io_calls(&self) -> usize {
        self.io_calls
    }
    /// Actual synchronous native metadata calls entered after their last E check.
    pub fn entered_io_calls(&self) -> usize {
        self.entered_io_calls
    }
    /// Actual named-file metadata saved before the post-call clock.
    pub fn named_metadata(&self) -> Option<&fs::Metadata> {
        self.named.as_ref()
    }
    /// Actual held-descriptor metadata saved before the post-call clock.
    pub fn descriptor_metadata(&self) -> Option<&fs::Metadata> {
        self.actual.as_ref()
    }
    /// Native policy decision, saved before the final clock; no freshness grant.
    pub fn identity_changed(&self) -> Option<bool> {
        self.identity_changed
    }
    /// Original native I/O failure, even if expiry subsequently took precedence.
    pub fn io_error(&self) -> Option<&std::io::Error> {
        self.io_error.as_ref()
    }
    /// Original allocator refusal, even if expiry subsequently took precedence.
    pub fn allocation_error(&self) -> Option<&TryReserveError> {
        self.allocation_error.as_ref()
    }

    fn clock(&self) -> Result<(), ShardGuardValidationFailure> {
        if Instant::now() >= self.expiry {
            Err(ShardGuardValidationFailure::Expired)
        } else {
            Ok(())
        }
    }
    fn enter(
        &mut self,
        limits: ShardGuardValidationLimits,
    ) -> Result<(), ShardGuardValidationFailure> {
        self.clock()?;
        if self.io_calls == limits.max_io_calls {
            return Err(ShardGuardValidationFailure::Capacity);
        }
        self.io_calls += 1;
        self.clock()
    }
}

/// Refused observation retaining actual raw metadata and original native errors.
#[derive(Debug)]
pub struct ShardGuardValidationError {
    failure: ShardGuardValidationFailure,
    observation: ShardGuardValidation,
}
impl ShardGuardValidationError {
    /// Actual terminal refusal reason.
    pub fn failure(&self) -> ShardGuardValidationFailure {
        self.failure
    }
    /// Complete actual partial path, metadata, decision and native failures.
    pub fn observation(&self) -> &ShardGuardValidation {
        &self.observation
    }
}
impl std::fmt::Display for ShardGuardValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "native recovery guard observation refused: {:?}",
            self.failure
        )
    }
}
impl std::error::Error for ShardGuardValidationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let Some(e) = &self.observation.io_error {
            return Some(e);
        }
        self.observation
            .allocation_error
            .as_ref()
            .map(|e| e as &dyn std::error::Error)
    }
}

#[derive(Clone, Copy)]
enum Phase {
    PathReserved,
    NamedMetadata,
    DescriptorMetadata,
    Identity,
}

// The ordinary validator and the bounded producer use this exact native policy.
pub(super) fn identity_changed(
    guard: &ShardRecoveryGuard,
    directory: &Path,
    named: &fs::Metadata,
    actual: &fs::Metadata,
) -> bool {
    directory != guard.directory
        || !named.is_file()
        || named.file_type().is_symlink()
        || actual.dev() != named.dev()
        || actual.ino() != named.ino()
}

#[allow(
    clippy::result_large_err,
    reason = "retain actual metadata and errors without allocating on refusal"
)]
impl ShardRecoveryGuard {
    /// Observes this same native guard using its unchanged named-file/FD policy.
    ///
    /// Admit the plain cost before entry. Path allocation and each actual native
    /// metadata call settle before their post-E observation. Late success/error
    /// facts remain in the refusal. The actual guard stays with the caller.
    /// Existing ordinary validation remains unchanged.
    pub fn validate_until(
        &self,
        directory: &Path,
        limits: ShardGuardValidationLimits,
        original_expiry: Instant,
    ) -> Result<ShardGuardValidation, ShardGuardValidationError> {
        self.validate_until_observed(directory, limits, original_expiry, |_| {})
    }

    fn validate_until_observed(
        &self,
        directory: &Path,
        limits: ShardGuardValidationLimits,
        original_expiry: Instant,
        mut completed: impl FnMut(Phase),
    ) -> Result<ShardGuardValidation, ShardGuardValidationError> {
        let mut observation = ShardGuardValidation {
            expiry: original_expiry,
            lock_path: PathBuf::new(),
            requested_path_bytes: 0,
            io_calls: 0,
            entered_io_calls: 0,
            named: None,
            actual: None,
            identity_changed: None,
            io_error: None,
            allocation_error: None,
        };
        let result = (|| {
            observation.clock()?;
            shard_guard_validation_cost(limits).ok_or(ShardGuardValidationFailure::Arithmetic)?;
            let path_bytes = directory
                .as_os_str()
                .len()
                .checked_add(1)
                .and_then(|n| n.checked_add(LOCK_NAME.len()))
                .ok_or(ShardGuardValidationFailure::Arithmetic)?;
            let held_path_bytes = self
                .directory
                .as_os_str()
                .len()
                .checked_add(1)
                .and_then(|n| n.checked_add(LOCK_NAME.len()))
                .ok_or(ShardGuardValidationFailure::Arithmetic)?;
            if path_bytes > limits.max_path_bytes || held_path_bytes > limits.max_path_bytes {
                return Err(ShardGuardValidationFailure::Capacity);
            }
            observation.requested_path_bytes = path_bytes;
            observation.clock()?;
            observation.allocation_error =
                observation.lock_path.try_reserve_exact(path_bytes).err();
            completed(Phase::PathReserved);
            observation.clock()?;
            if observation.allocation_error.is_some() {
                return Err(ShardGuardValidationFailure::Allocation);
            }
            observation.lock_path.push(directory);
            observation.lock_path.push(LOCK_NAME);
            observation.clock()?;
            observation.enter(limits)?;
            observation.entered_io_calls += 1;
            match fs::symlink_metadata(&observation.lock_path) {
                Ok(metadata) => observation.named = Some(metadata),
                Err(error) => observation.io_error = Some(error),
            }
            completed(Phase::NamedMetadata);
            observation.clock()?;
            if observation.io_error.is_some() {
                return Err(ShardGuardValidationFailure::Io);
            }
            observation.enter(limits)?;
            observation.entered_io_calls += 1;
            match self.file.metadata() {
                Ok(metadata) => observation.actual = Some(metadata),
                Err(error) => observation.io_error = Some(error),
            }
            completed(Phase::DescriptorMetadata);
            observation.clock()?;
            if observation.io_error.is_some() {
                return Err(ShardGuardValidationFailure::Io);
            }
            if let (Some(named), Some(actual)) = (&observation.named, &observation.actual) {
                observation.identity_changed =
                    Some(identity_changed(self, directory, named, actual));
            }
            completed(Phase::Identity);
            observation.clock()?;
            if observation.identity_changed != Some(false) {
                return Err(ShardGuardValidationFailure::Identity);
            }
            Ok(())
        })();
        match result {
            Ok(()) => Ok(observation),
            Err(failure) => Err(ShardGuardValidationError {
                failure,
                observation,
            }),
        }
    }
}

#[cfg(test)]
#[path = "guard_validation_tests.rs"]
mod tests;
