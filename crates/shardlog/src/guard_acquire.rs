//! Same-descriptor native lease acquisition with retained original-E outcomes.

use super::*;

/// Finite source allowances for two native identity checks and one lock attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardGuardAcquireLimits {
    /// Maximum complete named lock-path bytes in either validation.
    pub max_path_bytes: usize,
    /// Cumulative metadata/lock call slots, including refused late entry.
    pub max_io_calls: usize,
}

/// Checked logical source costs; allocator/kernel/RSS bounds are separate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardGuardAcquireCost {
    /// Fixed owner/error values, incoming path bytes and both validation costs.
    pub declared_bytes: usize,
    /// Incoming path and both validation path ceilings, already in declared bytes.
    pub string_bytes: usize,
    /// Fixed values and both native validation slot ceilings.
    pub entries: usize,
    /// Logical setup/path work and the two validations plus one native lock call.
    pub work_bytes: usize,
    /// At most four metadata calls and one actual nonblocking lock call.
    pub native_call_slots: usize,
}

/// Derive plain costs from the actual borrowed input before transferring it.
///
/// This performs no filesystem operation, allocation or pathname normalization.
/// The input's existing allocation is owned by the caller before this producer;
/// these logical sizes do not measure its allocator capacity or native FD state.
pub fn shard_guard_acquire_cost(
    directory: &Path,
    limits: ShardGuardAcquireLimits,
) -> Option<ShardGuardAcquireCost> {
    let input_bytes = directory.as_os_str().len();
    let lock_bytes = input_bytes
        .checked_add(1)?
        .checked_add(".shard-store.lock".len())?;
    if lock_bytes > limits.max_path_bytes {
        return None;
    }
    let validation = shard_guard_validation_cost(ShardGuardValidationLimits {
        max_path_bytes: limits.max_path_bytes,
        max_io_calls: 2,
    })?;
    let values = [
        size_of::<Attempt>(),
        size_of::<AcquiredShardRecoveryGuard>(),
        size_of::<RetainedShardGuardAcquireError>(),
        size_of::<ShardGuardAcquireCost>(),
    ];
    let fixed = values.into_iter().try_fold(0usize, usize::checked_add)?;
    let mut cost = ShardGuardAcquireCost {
        declared_bytes: fixed.checked_add(input_bytes)?,
        string_bytes: input_bytes,
        entries: values.len(),
        work_bytes: fixed.checked_add(input_bytes)?,
        native_call_slots: 1,
    };
    // These are the two actual native validation invocations, before/after lock.
    for phase in [validation, validation] {
        cost.declared_bytes = cost.declared_bytes.checked_add(phase.declared_bytes)?;
        cost.string_bytes = cost.string_bytes.checked_add(phase.string_bytes)?;
        cost.entries = cost.entries.checked_add(phase.entries)?;
        cost.work_bytes = cost.work_bytes.checked_add(phase.work_bytes)?;
        cost.native_call_slots = cost.native_call_slots.checked_add(2)?;
    }
    cost.work_bytes = cost.work_bytes.checked_add(1)?;
    Some(cost)
}

/// Terminal refusal; exact completed native outcomes remain in the observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardGuardAcquireFailure {
    /// The original expiry elapsed, possibly after acquiring the real lock.
    Expired,
    /// The input path or cumulative native-call allowance refused.
    Capacity,
    /// Checked source-size arithmetic overflowed.
    Arithmetic,
    /// A real bounded identity validation refused; its full result is retained.
    Validation,
    /// The actual native nonblocking lock operation failed.
    Lock,
}

/// Actual results of this attempt; this value grants no storage or serving access.
#[derive(Debug)]
pub struct ShardGuardAcquireObservation {
    expiry: Instant,
    limits: ShardGuardAcquireLimits,
    io_calls: usize,
    entered_io_calls: usize,
    before: Option<Result<ShardGuardValidation, ShardGuardValidationError>>,
    lock: Option<std::io::Result<()>>,
    after: Option<Result<ShardGuardValidation, ShardGuardValidationError>>,
}
impl ShardGuardAcquireObservation {
    /// The caller's unchanged original absolute expiry.
    pub fn absolute_expiry(&self) -> Instant {
        self.expiry
    }
    /// The unchanged finite limits of this attempt.
    pub fn limits(&self) -> ShardGuardAcquireLimits {
        self.limits
    }
    /// Cumulative reserved metadata/lock slots, including refused late entry.
    pub fn io_calls(&self) -> usize {
        self.io_calls
    }
    /// Actual native calls entered after their final entry clock checks.
    pub fn entered_io_calls(&self) -> usize {
        self.entered_io_calls
    }
    /// Complete actual pre-lock validation, including partial results on refusal.
    pub fn before_validation(
        &self,
    ) -> Option<&Result<ShardGuardValidation, ShardGuardValidationError>> {
        self.before.as_ref()
    }
    /// Exact native lock result, saved before observing the post-lock expiry.
    pub fn lock_result(&self) -> Option<&std::io::Result<()>> {
        self.lock.as_ref()
    }
    /// Complete actual post-lock validation, including partial results on refusal.
    pub fn after_validation(
        &self,
    ) -> Option<&Result<ShardGuardValidation, ShardGuardValidationError>> {
        self.after.as_ref()
    }
    fn clock(&self) -> Result<(), ShardGuardAcquireFailure> {
        if Instant::now() >= self.expiry {
            Err(ShardGuardAcquireFailure::Expired)
        } else {
            Ok(())
        }
    }
}

struct Attempt {
    observation: ShardGuardAcquireObservation,
    // Last: actual observations settle before the exact input FD/lease is dropped.
    guard: ShardRecoveryGuard,
}
impl Attempt {
    fn validation(
        &mut self,
        before: bool,
        completed: &mut impl FnMut(Phase),
    ) -> Result<(), ShardGuardAcquireFailure> {
        self.observation.clock()?;
        let remaining = self
            .observation
            .limits
            .max_io_calls
            .checked_sub(self.observation.io_calls)
            .ok_or(ShardGuardAcquireFailure::Capacity)?;
        let observed = self.guard.validate_until(
            &self.guard.directory,
            ShardGuardValidationLimits {
                max_path_bytes: self.observation.limits.max_path_bytes,
                max_io_calls: remaining.min(2),
            },
            self.observation.expiry,
        );
        let slot = if before {
            &mut self.observation.before
        } else {
            &mut self.observation.after
        };
        *slot = Some(observed);
        let (calls, entered, failure) = match slot.as_ref() {
            Some(Ok(value)) => (value.io_calls(), value.entered_io_calls(), None),
            Some(Err(error)) => (
                error.observation().io_calls(),
                error.observation().entered_io_calls(),
                Some(error.failure()),
            ),
            None => return Err(ShardGuardAcquireFailure::Validation),
        };
        self.observation.io_calls = self
            .observation
            .io_calls
            .checked_add(calls)
            .ok_or(ShardGuardAcquireFailure::Arithmetic)?;
        self.observation.entered_io_calls = self
            .observation
            .entered_io_calls
            .checked_add(entered)
            .ok_or(ShardGuardAcquireFailure::Arithmetic)?;
        completed(if before {
            Phase::BeforeValidation
        } else {
            Phase::AfterValidation
        });
        self.observation.clock()?;
        match failure {
            None => Ok(()),
            Some(ShardGuardValidationFailure::Expired) => Err(ShardGuardAcquireFailure::Expired),
            Some(ShardGuardValidationFailure::Capacity) => Err(ShardGuardAcquireFailure::Capacity),
            Some(ShardGuardValidationFailure::Arithmetic) => {
                Err(ShardGuardAcquireFailure::Arithmetic)
            }
            Some(_) => Err(ShardGuardAcquireFailure::Validation),
        }
    }
}

/// A successfully acquired actual native guard, with both original observations.
///
/// This borrows the same guard for subsequent native work. It provides no raw
/// descriptor/parts extraction, parent-directory validation, bounded storage
/// opening or current serving permission. Those producers require their own
/// original-E checks and retained partial outcomes.
pub struct AcquiredShardRecoveryGuard {
    attempt: Attempt,
}
impl std::fmt::Debug for AcquiredShardRecoveryGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcquiredShardRecoveryGuard")
            .field("observation", &self.observation())
            .finish_non_exhaustive()
    }
}
impl AcquiredShardRecoveryGuard {
    /// Borrow the same successfully locked and validated native guard.
    pub fn guard(&self) -> &ShardRecoveryGuard {
        &self.attempt.guard
    }
    /// Both actual validations and the saved native lock result.
    pub fn observation(&self) -> &ShardGuardAcquireObservation {
        &self.attempt.observation
    }
}

/// Refusal retaining the exact input FD and any lock acquired before refusal.
///
/// The guard is deliberately inaccessible here: an early, contended or invalid
/// attempt must not provide an unacquired guard to an existing native open API.
/// Keep this owner through refusal handling; dropping it releases its actual FD.
pub struct RetainedShardGuardAcquireError {
    failure: ShardGuardAcquireFailure,
    attempt: Attempt,
}
impl RetainedShardGuardAcquireError {
    /// Terminal refusal, without discarding any actual native outcome.
    pub fn failure(&self) -> ShardGuardAcquireFailure {
        self.failure
    }
    /// Borrow actual diagnostic observations; no guard or descriptor is exposed.
    pub fn observation(&self) -> &ShardGuardAcquireObservation {
        &self.attempt.observation
    }
}
impl std::fmt::Debug for RetainedShardGuardAcquireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetainedShardGuardAcquireError")
            .field("failure", &self.failure)
            .field("observation", &self.observation())
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for RetainedShardGuardAcquireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "native recovery guard acquisition refused: {:?}",
            self.failure
        )
    }
}
impl std::error::Error for RetainedShardGuardAcquireError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        let observation = self.observation();
        if let Some(Err(error)) = &observation.lock {
            return Some(error);
        }
        for result in [&observation.after, &observation.before] {
            if let Some(Err(error)) = result {
                return Some(error);
            }
        }
        None
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    BeforeValidation,
    Lock,
    AfterValidation,
}

#[allow(
    clippy::result_large_err,
    reason = "retain the same native FD, lock and exact outcomes without allocating on refusal"
)]
impl ShardRecoveryGuard {
    /// Acquire the exact supplied native descriptor with the existing lock policy.
    ///
    /// Admit the plain cost using the actual directory before entry. This first
    /// retains the supplied FD/path, then performs the native identity check,
    /// actual nonblocking `fs2` lock and final identity check in the original
    /// order. Each typed outcome is saved before post-E. A lock acquired late
    /// stays in the error owner; no unlock, pathname reopen, retry, clone or new
    /// deadline is hidden in this producer. Ordinary acquisition is unchanged.
    pub fn try_acquire_until(
        file: File,
        directory: PathBuf,
        limits: ShardGuardAcquireLimits,
        original_expiry: Instant,
    ) -> Result<AcquiredShardRecoveryGuard, RetainedShardGuardAcquireError> {
        Self::acquire_until_observed(file, directory, limits, original_expiry, |_| {})
    }

    fn acquire_until_observed(
        file: File,
        directory: PathBuf,
        limits: ShardGuardAcquireLimits,
        original_expiry: Instant,
        mut completed: impl FnMut(Phase),
    ) -> Result<AcquiredShardRecoveryGuard, RetainedShardGuardAcquireError> {
        let mut attempt = Attempt {
            guard: Self { file, directory },
            observation: ShardGuardAcquireObservation {
                expiry: original_expiry,
                limits,
                io_calls: 0,
                entered_io_calls: 0,
                before: None,
                lock: None,
                after: None,
            },
        };
        let result = (|| {
            attempt.observation.clock()?;
            let path_bytes = attempt
                .guard
                .directory
                .as_os_str()
                .len()
                .checked_add(1)
                .and_then(|n| n.checked_add(".shard-store.lock".len()))
                .ok_or(ShardGuardAcquireFailure::Arithmetic)?;
            if path_bytes > limits.max_path_bytes {
                return Err(ShardGuardAcquireFailure::Capacity);
            }
            shard_guard_acquire_cost(&attempt.guard.directory, limits)
                .ok_or(ShardGuardAcquireFailure::Arithmetic)?;
            attempt.observation.clock()?;
            attempt.validation(true, &mut completed)?;
            attempt.observation.clock()?;
            if attempt.observation.io_calls >= limits.max_io_calls {
                return Err(ShardGuardAcquireFailure::Capacity);
            }
            attempt.observation.io_calls = attempt
                .observation
                .io_calls
                .checked_add(1)
                .ok_or(ShardGuardAcquireFailure::Arithmetic)?;
            attempt.observation.clock()?;
            attempt.observation.entered_io_calls = attempt
                .observation
                .entered_io_calls
                .checked_add(1)
                .ok_or(ShardGuardAcquireFailure::Arithmetic)?;
            attempt.observation.lock = Some(fs2::FileExt::try_lock_exclusive(&attempt.guard.file));
            completed(Phase::Lock);
            attempt.observation.clock()?;
            if !matches!(attempt.observation.lock.as_ref(), Some(Ok(()))) {
                return Err(ShardGuardAcquireFailure::Lock);
            }
            attempt.validation(false, &mut completed)?;
            attempt.observation.clock()
        })();
        match result {
            Ok(()) => Ok(AcquiredShardRecoveryGuard { attempt }),
            Err(failure) => Err(RetainedShardGuardAcquireError { failure, attempt }),
        }
    }
}

#[cfg(test)]
#[path = "guard_acquire_tests.rs"]
mod tests;
