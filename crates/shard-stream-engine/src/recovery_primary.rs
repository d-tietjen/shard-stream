//! Actual acquired-guard, Core pin and whole-store continuation for a census.
//!
//! The optional route changes primary acquisition/opening only. Other census,
//! tier, coordinator and worker catalog producers retain their separate bounds.

use super::*;
use crate::worker::PrimaryStore;
use shard_stream_core::{
    NativeDirectoryBudget, NativeDirectoryFailure, OwnedPinnedShardStore, OwnedShardDirectory,
    RetainedDirectoryError, RetainedOwnedPinnedShardStoreError, RetainedOwnedShardDirectoryError,
};
use shardlog::{
    AcquiredShardRecoveryGuard, PinnedShardDirectoryLimits, PinnedShardGcChecks,
    PinnedShardGcLimits, PinnedStoreOpenChecks, PinnedStoreOpenLimits,
    RetainedShardGuardAcquireError, ShardDirectoryCheckFailure, ShardGuardAcquireLimits,
    shard_guard_acquire_cost,
};
use std::any::Any;
use std::collections::TryReserveError;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Ordinary finite policy and the SAME actual directory work for primary open.
///
/// These fields grant no history, currentness, health or serving permission.
/// The composing host must supply checks backed by its SAME actual captured
/// health and work. Both callbacks must deny that health before native cleanup.
#[derive(Clone)]
pub struct NativePrimaryRecovery {
    /// Original cumulative directory work, also retained through worker GC.
    pub budget: Arc<NativeDirectoryBudget>,
    /// Two real native validations and the actual nonblocking lease attempt.
    pub guard_limits: ShardGuardAcquireLimits,
    /// Actual descriptor-relative directory/lease identity continuation.
    pub pin_limits: PinnedShardDirectoryLimits,
    /// Actual bounded native discovery/read/index/repair ceilings.
    pub open_limits: PinnedStoreOpenLimits,
    /// Actual bounded primary collection ceilings used at bootstrap.
    pub gc_limits: PinnedShardGcLimits,
    /// SAME real composing check/denial for native opening and repair cleanup.
    pub open_checks: Arc<dyn PinnedStoreOpenChecks + Send + Sync>,
    /// SAME real composing check/denial for entered collection and settlement.
    pub gc_checks: Arc<dyn PinnedShardGcChecks + Send + Sync>,
}

/// Actual primary-route outcomes retained on refusal, without input extraction.
#[derive(Default)]
pub struct NativePrimaryOutcomes {
    path: Option<PathBuf>,
    lease: Option<OwnedFileHandle>,
    config: Option<ShardStoreConfig>,
    clone_result: Option<io::Result<File>>,
    acquired:
        Option<std::result::Result<AcquiredShardRecoveryGuard, RetainedShardGuardAcquireError>>,
    pinned: Option<std::result::Result<OwnedShardDirectory, RetainedOwnedShardDirectoryError>>,
    opened: Option<std::result::Result<OwnedPinnedShardStore, RetainedOwnedPinnedShardStoreError>>,
    directory_error: Option<RetainedDirectoryError>,
    budget_error: Option<NativeDirectoryFailure>,
    allocation_error: Option<TryReserveError>,
    checkpoint_error: Option<ShardDirectoryCheckFailure>,
    panic: Option<Box<dyn Any + Send>>,
    deny_panic: Option<Box<dyn Any + Send>>,
}
impl NativePrimaryOutcomes {
    /// Actual clone error; a successful late descriptor is retained privately.
    pub fn clone_error(&self) -> Option<&io::Error> {
        self.clone_result.as_ref().and_then(|v| v.as_ref().err())
    }
    /// Actual native lease refusal, retaining any acquired lock.
    pub fn guard_error(&self) -> Option<&RetainedShardGuardAcquireError> {
        self.acquired.as_ref().and_then(|v| v.as_ref().err())
    }
    /// Actual Core/native directory continuation refusal and input custody.
    pub fn pin_error(&self) -> Option<&RetainedOwnedShardDirectoryError> {
        self.pinned.as_ref().and_then(|v| v.as_ref().err())
    }
    /// Actual native open/repair refusal and partial whole store.
    pub fn open_error(&self) -> Option<&RetainedOwnedPinnedShardStoreError> {
        self.opened.as_ref().and_then(|v| v.as_ref().err())
    }
    /// Actual recursive directory phase refusal and retained outcomes.
    pub fn directory_error(&self) -> Option<&RetainedDirectoryError> {
        self.directory_error.as_ref()
    }
    /// Actual cumulative budget refusal.
    pub fn budget_error(&self) -> Option<&NativeDirectoryFailure> {
        self.budget_error.as_ref()
    }
    /// Actual checkpoint refusal before or after a completed native phase.
    pub fn checkpoint_error(&self) -> Option<ShardDirectoryCheckFailure> {
        self.checkpoint_error
    }
    /// Actual allocation refusal, without replacing it with a new allocation.
    pub fn allocation_error(&self) -> Option<&TryReserveError> {
        self.allocation_error.as_ref()
    }
    /// Actual route/checkpoint unwind retained before denial.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.panic.as_deref()
    }
    /// Actual composing denial unwind retained separately.
    pub fn deny_panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.deny_panic.as_deref()
    }
}

pub(super) struct PrimaryRecovery {
    pub(super) context: NativePrimaryRecovery,
    pub(super) outcomes: NativePrimaryOutcomes,
    guards: Vec<(ShardId, AcquiredShardRecoveryGuard)>,
    directories: Vec<(ShardId, OwnedDirectoryHandle)>,
    denial_attempted: bool,
}
impl PrimaryRecovery {
    pub(super) fn new(context: NativePrimaryRecovery) -> Self {
        Self {
            context,
            outcomes: NativePrimaryOutcomes::default(),
            guards: Vec::new(),
            directories: Vec::new(),
            denial_attempted: false,
        }
    }
    pub(super) fn check(&mut self, expiry: Instant) -> Result<()> {
        if let Err(error) = self
            .context
            .budget
            .cap_expiry(expiry)
            .and_then(|()| self.context.budget.check())
        {
            if self.outcomes.budget_error.is_none() {
                self.outcomes.budget_error = Some(error);
            }
            return Err(limited());
        }
        let checked = self.context.open_checks.check();
        if let Err(error) = checked {
            if self.outcomes.checkpoint_error.is_none() {
                self.outcomes.checkpoint_error = Some(error);
            }
        }
        let after = self.context.budget.check();
        if let Err(error) = after {
            if self.outcomes.budget_error.is_none() {
                self.outcomes.budget_error = Some(error);
            }
            return Err(limited());
        }
        checked.map_err(|_| identity("actual primary composing checkpoint refused"))
    }
    pub(super) fn deny(&mut self) {
        self.context.budget.deny();
        if self.denial_attempted {
            return;
        }
        self.denial_attempted = true;
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.context.open_checks.deny())) {
            if self.outcomes.deny_panic.is_none() {
                self.outcomes.deny_panic = Some(payload);
            }
        }
    }
    pub(super) fn unwind(&mut self, payload: Box<dyn Any + Send>) {
        self.outcomes.panic = Some(payload);
        self.deny();
    }
    pub(super) fn has_guard(&self, shard: ShardId) -> bool {
        self.guards.iter().any(|(id, _)| *id == shard)
    }
    fn budget_result(
        &mut self,
        observed: std::result::Result<(), NativeDirectoryFailure>,
    ) -> Result<()> {
        if let Err(error) = observed {
            self.outcomes.budget_error = Some(error);
            return Err(limited());
        }
        Ok(())
    }
    pub(super) fn acquire(
        &mut self,
        shard: ShardId,
        directory: &OwnedDirectoryHandle,
        file: OwnedFileHandle,
        expiry: Instant,
    ) -> Result<()> {
        self.outcomes.lease = Some(file);
        self.check(expiry)?;
        let cost = shard_guard_acquire_cost(directory.path(), self.context.guard_limits)
            .ok_or_else(limited)?;
        let guards = self
            .guards
            .len()
            .checked_add(1)
            .and_then(|n| n.checked_mul(size_of::<(ShardId, AcquiredShardRecoveryGuard)>()))
            .ok_or_else(limited)?;
        let directories = self
            .directories
            .len()
            .checked_add(1)
            .and_then(|n| n.checked_mul(size_of::<(ShardId, OwnedDirectoryHandle)>()))
            .ok_or_else(limited)?;
        let extra = [
            size_of::<io::Result<File>>(),
            guards,
            directories,
            directory.path().as_os_str().len(),
        ]
        .into_iter()
        .try_fold(0usize, usize::checked_add)
        .ok_or_else(limited)?;
        let declared = cost.declared_bytes.checked_add(extra).ok_or_else(limited)?;
        let paid = self.context.budget.reserve(
            1,
            cost.native_call_slots.checked_add(1).ok_or_else(limited)?,
            declared,
            cost.string_bytes
                .checked_add(directory.path().as_os_str().len())
                .ok_or_else(limited)?,
            cost.entries.checked_add(4).ok_or_else(limited)?,
            cost.work_bytes.checked_add(extra).ok_or_else(limited)?,
        );
        self.budget_result(paid)?;
        self.outcomes.allocation_error = self.guards.try_reserve_exact(1).err();
        self.check(expiry)?;
        if self.outcomes.allocation_error.is_some() {
            return Err(limited());
        }
        self.outcomes.allocation_error = self.directories.try_reserve_exact(1).err();
        self.check(expiry)?;
        if self.outcomes.allocation_error.is_some() {
            return Err(limited());
        }
        self.directories.push((shard, directory.retain()));
        self.outcomes.path = Some(directory.path().to_owned());
        self.check(expiry)?;
        self.outcomes.clone_result = Some(
            self.outcomes
                .lease
                .as_ref()
                .ok_or_else(limited)?
                .file()
                .try_clone(),
        );
        self.check(expiry)?;
        if matches!(self.outcomes.clone_result, Some(Err(_))) {
            return Err(identity("actual primary lease descriptor clone refused"));
        }
        let Some(Ok(file)) = self.outcomes.clone_result.take() else {
            return Err(limited());
        };
        let path = self.outcomes.path.take().ok_or_else(limited)?;
        self.outcomes.acquired = Some(ShardRecoveryGuard::try_acquire_until(
            file,
            path,
            self.context.guard_limits,
            self.context.budget.absolute_expiry(),
        ));
        self.check(expiry)?;
        if !matches!(self.outcomes.acquired, Some(Ok(_))) {
            return Err(identity("actual bounded primary lease acquisition refused"));
        }
        let Some(Ok(guard)) = self.outcomes.acquired.take() else {
            return Err(limited());
        };
        self.guards.push((shard, guard));
        self.check(expiry)
    }
    pub(super) fn open(
        &mut self,
        shard: ShardId,
        config: ShardStoreConfig,
        expiry: Instant,
    ) -> Result<PrimaryStore> {
        self.outcomes.config = Some(config);
        self.check(expiry)?;
        let visits = self
            .guards
            .len()
            .checked_add(self.directories.len())
            .ok_or_else(limited)?;
        let paid = self.context.budget.reserve(
            0,
            0,
            0,
            0,
            visits,
            visits
                .checked_mul(size_of::<ShardId>())
                .ok_or_else(limited)?,
        );
        self.budget_result(paid)?;
        let index = self
            .guards
            .iter()
            .position(|(id, _)| *id == shard)
            .ok_or_else(|| identity("owned primary has no actually acquired native guard"))?;
        let directory = self
            .directories
            .iter()
            .find(|(id, _)| *id == shard)
            .map(|(_, v)| v)
            .ok_or_else(limited)?;
        let (_, guard) = self.guards.swap_remove(index);
        self.outcomes.pinned = Some(directory.pin_shard_directory_until(
            guard,
            self.context.pin_limits,
            &self.context.budget,
        ));
        self.check(expiry)?;
        if !matches!(self.outcomes.pinned, Some(Ok(_))) {
            return Err(identity("actual primary Core pin refused"));
        }
        let Some(Ok(directory)) = self.outcomes.pinned.take() else {
            return Err(limited());
        };
        self.outcomes.opened = Some(directory.open_store_until(
            self.outcomes.config.take().ok_or_else(limited)?,
            self.context.open_limits,
            &self.context.budget,
            self.context.open_checks.as_ref(),
        ));
        self.check(expiry)?;
        let Some(Ok(store)) = self.outcomes.opened.as_ref() else {
            return Err(identity("actual bounded primary open refused"));
        };
        let _ = store;
        let Some(Ok(store)) = self.outcomes.opened.take() else {
            return Err(limited());
        };
        Ok(PrimaryStore::Owned(store))
    }
}

pub(super) struct PrimaryCustody {
    _outcomes: NativePrimaryOutcomes,
    _guards: Vec<(ShardId, AcquiredShardRecoveryGuard)>,
    _directories: Vec<(ShardId, OwnedDirectoryHandle)>,
}
impl NativePrimaryOutcomes {
    pub(super) fn record_budget_error(&mut self, error: NativeDirectoryFailure) {
        if self.budget_error.is_none() {
            self.budget_error = Some(error);
        }
    }
}
impl PrimaryRecovery {
    pub(super) fn finish(self) -> PrimaryCustody {
        PrimaryCustody {
            _outcomes: self.outcomes,
            _guards: self.guards,
            _directories: self.directories,
        }
    }
    pub(super) fn directory_result<T>(
        &mut self,
        result: std::result::Result<T, RetainedDirectoryError>,
    ) -> Result<T> {
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                self.outcomes.directory_error = Some(error);
                Err(identity("actual bounded primary directory phase refused"))
            }
        }
    }
    pub(super) fn enter(&mut self, expiry: Instant) -> Result<()> {
        self.check(expiry)?;
        let fixed = [
            size_of::<Self>(),
            size_of::<NativePrimaryOutcomes>(),
            size_of::<Box<dyn Any + Send>>(),
            size_of::<Box<dyn Any + Send>>(),
        ]
        .into_iter()
        .try_fold(0usize, usize::checked_add)
        .ok_or_else(limited)?;
        let paid = self.context.budget.reserve(0, 0, fixed, 0, 4, fixed);
        self.budget_result(paid)?;
        self.check(expiry)
    }
}

impl Attempt {
    pub(super) fn primary_operation<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        if matches!(self.primary_setup, PrimarySetup::Ordinary) {
            return operation(self);
        }
        let observed = catch_unwind(AssertUnwindSafe(|| operation(self)));
        let result = match observed {
            Ok(result) => result,
            Err(payload) => {
                if let PrimarySetup::Owned(primary) = &mut self.primary_setup {
                    primary.unwind(payload);
                }
                Err(identity("actual primary operation unwound"))
            }
        };
        if result.is_err() {
            if let PrimarySetup::Owned(primary) = &mut self.primary_setup {
                primary.deny();
            }
        }
        result
    }
}
