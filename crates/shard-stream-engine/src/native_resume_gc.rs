//! The SAME resume counters and actual captured-health checks around Core GC.

use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) struct Early {
    budget_error: Option<shard_stream_core::NativeDirectoryFailure>,
    checks: Option<Arc<super::DirectoryFetchChecks>>,
    deny_panic: Option<Box<dyn Any + Send>>,
    _policy: NativeResumeGcPolicy,
}

struct Checks {
    work: NativeResumeWork,
    outer: Arc<dyn PinnedShardGcChecks + Send + Sync>,
    denial_attempted: AtomicBool,
}
impl ShardDirectoryChecks for Checks {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        self.work
            .check()
            .map_err(|_| ShardDirectoryCheckFailure::Refused)?;
        let observed = self.outer.check();
        self.work
            .check()
            .map_err(|_| ShardDirectoryCheckFailure::Refused)?;
        observed
    }
}
impl PinnedShardGcChecks for Checks {
    fn deny(&self) {
        // Native/Core invokes this BEFORE already-prepaid durability cleanup.
        self.work.deny();
        if !self.denial_attempted.swap(true, Ordering::AcqRel) {
            self.outer.deny();
        }
    }
}
impl Scope {
    pub(super) fn deny_gc(&mut self) {
        self.work.deny();
        if let Some(policy) = &self.gc_actual_policy {
            policy.budget.deny();
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| policy.checks.deny()))
                && self.gc_deny_panic.is_none()
            {
                self.gc_deny_panic = Some(payload);
            }
        } else if !self.gc_preentry_denial_attempted
            && let Some(policy) = &self.gc_policy
        {
            self.gc_preentry_denial_attempted = true;
            policy.budget.deny();
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| policy.checks.deny()))
                && self.gc_deny_panic.is_none()
            {
                self.gc_deny_panic = Some(payload);
            }
        }
    }

    pub(super) fn drain_gc(&mut self) {
        let Some((shard, ticket)) = self.pending_gc.as_mut() else {
            return;
        };
        let shard = *shard;
        let result = ticket.settle();
        self.outcomes.push(NativeResumeOutcome::GarbageCollected {
            shard,
            result,
            refusal: None,
        });
        self.pending_gc.take();
        let refusal = self.work.check().err();
        if let Some(NativeResumeOutcome::GarbageCollected { refusal: saved, .. }) =
            self.outcomes.last_mut()
        {
            *saved = refusal;
        }
    }
    pub(super) fn collect_owned(
        &mut self,
        partition: TopicPartition,
        start: LogicalOffset,
    ) -> Result<(), NativeResumeFailure> {
        let policy = self
            .gc_policy
            .as_ref()
            .ok_or(NativeResumeFailure::Callback)?;
        let cost =
            owned_pinned_shard_gc_cost(policy.limits).ok_or(NativeResumeRefusal::Capacity)?;
        let command =
            crate::worker::owned_gc_command_cost().ok_or(NativeResumeRefusal::Capacity)?;
        // Independent native directory work is prepaid on its SAME budget by
        // the actual Core producer; these are the composing resume fees.
        let fixed = add(size_of::<Checks>(), size_of::<[usize; 2]>())?;
        let declared = add(cost.declared_bytes, add(command, fixed)?)?;
        let strings = add(
            cost.string_bytes,
            crate::worker::OWNED_GC_COMMAND_DIAGNOSTIC_BYTES,
        )?;
        for lane in 0..self.owner.shards.len() {
            self.work.reserve_outcomes(1)?;
            self.work.reserve_command(
                declared,
                strings,
                add(
                    cost.entries,
                    add(crate::worker::OWNED_GC_COMMAND_ENTRIES, 2)?,
                )?,
                1,
            )?;
            self.work.reserve_values(0, 0, 0, cost.work_bytes)?;
            self.work.check()?;
            let policy = self
                .gc_policy
                .as_ref()
                .ok_or(NativeResumeFailure::Callback)?;
            self.gc_actual_policy = Some(NativeResumeGcPolicy {
                budget: Arc::clone(&policy.budget),
                limits: policy.limits,
                checks: Arc::new(Checks {
                    work: self.work.clone(),
                    outer: Arc::clone(&policy.checks),
                    denial_attempted: AtomicBool::new(false),
                }),
            });
            self.work.check()?;
            let shard = self.owner.shards[lane].shard_id;
            let actual = self
                .gc_actual_policy
                .as_ref()
                .ok_or(NativeResumeFailure::Callback)?;
            match self.owner.shards[lane].begin_owned_gc(
                partition,
                start,
                actual,
                self.work.absolute_expiry(),
            ) {
                Ok(ticket) => self.pending_gc = Some((shard, ticket)),
                Err(outcome) => {
                    self.outcomes.push(NativeResumeOutcome::GarbageCollected {
                        shard,
                        result: Ok(outcome),
                        refusal: None,
                    });
                    return Err(NativeResumeFailure::Callback);
                }
            }
            // Pending ticket is already owned even when this post-E refuses.
            let after = self.work.check();
            if after.is_err() {
                self.deny_gc();
            }
            self.drain_gc();
            after?;
            if !matches!(self.outcomes.last(),Some(NativeResumeOutcome::GarbageCollected {result:Ok(outcome),refusal:None,..}) if outcome.accepted())
            {
                return Err(NativeResumeFailure::Callback);
            }
        }
        self.work.check()?;
        Ok(())
    }
}
#[allow(
    clippy::result_large_err,
    reason = "retain original engine, policy and actual early refusal custody inline"
)]
pub(super) fn start_owned(
    owner: &EngineHandle,
    limits: NativeResumeLimits,
    original_expiry: Instant,
    policy: NativeResumeGcPolicy,
) -> Result<Box<Scope>, RetainedNativeResumeError> {
    let expiry = original_expiry.min(policy.budget.absolute_expiry());
    let capped = policy.budget.cap_expiry(expiry);
    if let Err(budget_error) = capped {
        policy.budget.deny();
        let deny_panic = catch_unwind(AssertUnwindSafe(|| policy.checks.deny())).err();
        return Err(RetainedNativeResumeError {
            failure: NativeResumeFailure::Callback,
            gc_early: Some(Early {
                budget_error: Some(budget_error),
                checks: None,
                deny_panic,
                _policy: policy,
            }),
            custody: Custody::Early(owner.clone()),
        });
    }
    let checks = match super::DirectoryFetchChecks::new(Arc::clone(&policy.budget)) {
        Ok(checks) => checks,
        Err(budget_error) => {
            policy.budget.deny();
            let deny_panic = catch_unwind(AssertUnwindSafe(|| policy.checks.deny())).err();
            return Err(RetainedNativeResumeError {
                failure: NativeResumeFailure::Callback,
                gc_early: Some(Early {
                    budget_error: Some(budget_error),
                    checks: None,
                    deny_panic,
                    _policy: policy,
                }),
                custody: Custody::Early(owner.clone()),
            });
        }
    };
    let mut scope = match super::start_inner(owner, limits, expiry, Some(Arc::clone(&checks))) {
        Ok(scope) => scope,
        Err(mut error) => {
            // Observe through the SAME concrete checker; a direct budget.check
            // fallback here could escape the first-error escrow and later be
            // superseded by a generic denial from an escaped work clone.
            let _ = checks.check_native();
            let budget_error = None;
            policy.budget.deny();
            let deny_panic = catch_unwind(AssertUnwindSafe(|| policy.checks.deny())).err();
            error.gc_early = Some(Early {
                budget_error,
                checks: Some(checks),
                deny_panic,
                _policy: policy,
            });
            return Err(error);
        }
    };
    scope.gc_policy = Some(policy);
    Ok(scope)
}

impl EngineHandle {
    /// Collect real owned lane stores under one actual resume/directory budget.
    ///
    /// The actual supplied checks must deny the SAME captured C1 health. Every
    /// entered command is retained and drained even if E elapses or a callback
    /// unwinds. Success is collection data, never currentness or serving proof.
    /// Ordinary `StreamEngine::collect_garbage` keeps its existing behavior.
    ///
    /// # Errors
    /// Retains this same engine, actual Core report/refused whole store, pending
    /// command settlement and typed work/checkpoint/unwind outcomes.
    #[allow(
        clippy::result_large_err,
        reason = "retain actual entered GC results and the same engine without a refusal allocation"
    )]
    pub fn collect_garbage_until(
        &self,
        partition: TopicPartition,
        start: LogicalOffset,
        policy: NativeResumeGcPolicy,
        limits: NativeResumeLimits,
        original_expiry: Instant,
    ) -> Result<NativeResumeReport, RetainedNativeResumeError> {
        let mut scope = start_owned(self, limits, original_expiry, policy)?;
        let observed = catch_unwind(AssertUnwindSafe(|| scope.collect_owned(partition, start)));
        let observed = match observed {
            Ok(result) => result,
            Err(payload) => {
                scope.work.deny();
                scope.panic_payload = Some(payload);
                Err(NativeResumeFailure::Unwind)
            }
        };
        if let Err(error) = observed {
            return Err(scope.fail(error));
        }
        scope.finish()
    }
}

impl RetainedNativeResumeError {
    /// Actual composing GC denial unwind, independently of producer unwinds.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn gc_deny_panic_payload(&self) -> Option<&(dyn Any + Send)> {
        if let Some(early) = &self.gc_early {
            return early.deny_panic.as_deref();
        }
        match &self.custody {
            Custody::Early(_) => None,
            Custody::Entered(scope) => scope.gc_deny_panic.as_deref(),
        }
    }
}

impl RetainedNativeResumeError {
    /// Actual SAME-directory-budget refusal retained independently of native reports.
    /// The first recorded checked refusal survives later sticky denial; its
    /// immutable borrowed value is never taken, reset or replaced.
    pub fn gc_budget_error(&self) -> Option<&shard_stream_core::NativeDirectoryFailure> {
        if let Some(early) = &self.gc_early {
            return early
                .checks
                .as_ref()
                .and_then(|checks| checks.first_failure())
                .or(early.budget_error.as_ref());
        }
        match &self.custody {
            Custody::Early(_) => None,
            Custody::Entered(scope) => scope
                .work
                .inner
                .directory
                .as_ref()
                .and_then(|checks| checks.first_failure())
                .or(scope.gc_budget_error.as_ref()),
        }
    }
}
