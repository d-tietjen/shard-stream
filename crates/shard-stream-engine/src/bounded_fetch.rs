//! Finite native fetch work retaining the same actual engine through settlement.

use super::*;
use crate::EngineHandle;
use crate::coordinator::BoundedFetchCoordinatorFailure;
use crate::worker::{
    BoundedFetchCompletion, BoundedFetchDispatchError, BoundedFetchPlanInput,
    bounded_fetch_command_bytes,
};
use shardlog::{
    StorageFetchBudget, StorageFetchFailure, StorageFetchLimits, StorageFetchUsage,
    storage_fetch_cost,
};
use std::{error::Error, mem::size_of, sync::mpsc::Sender, time::Instant};

#[cfg(test)]
#[path = "bounded_fetch_tests.rs"]
mod tests;

/// Finite native plan, selection, bitmap, path, read and logical-work limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeFetchLimits {
    /// Metadata entries requested from each actual physical shard worker.
    pub max_plan_batches_per_shard: usize,
    /// Maximum batches returned in one native prefix, including zero payloads.
    pub max_selected_batches: usize,
    /// Maximum inclusive native batch-ID bitmap span before allocation.
    pub max_candidate_window: usize,
    /// Cumulative explicitly admitted metadata/channel/value slots.
    pub max_entries: usize,
    /// Bytes in an actual pack name before its native open.
    pub max_path_bytes: usize,
    /// Cumulative prepaid actual read attempts, including short/error/EOF calls.
    pub max_read_attempts: usize,
    /// Cumulative requested read bytes, including retries and coalesced gaps.
    pub max_read_bytes: usize,
    /// Cumulative logical native allocation requests, including all output data.
    pub max_declared_bytes: usize,
    /// Cumulative source-visible native work and entry fees.
    pub max_work_bytes: usize,
}
impl NativeFetchLimits {
    fn storage(self) -> StorageFetchLimits {
        StorageFetchLimits {
            max_plan_batches: self.max_plan_batches_per_shard,
            max_entries: self.max_entries,
            max_path_bytes: self.max_path_bytes,
            max_read_attempts: self.max_read_attempts,
            max_read_bytes: self.max_read_bytes,
            max_declared_bytes: self.max_declared_bytes,
            max_work_bytes: self.max_work_bytes,
        }
    }
}

/// Plain conservative logical costs, not admission or allocator/RSS accounting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeFetchCost {
    /// Fixed source-visible values plus cumulative native allocation ceiling.
    pub declared_bytes: usize,
    /// Fixed values/channels plus cumulative native entry ceiling.
    pub entries: usize,
    /// Cumulative path-conversion subset already in declared bytes.
    pub string_bytes: usize,
    /// Source-visible setup plus cumulative native work ceiling.
    pub work_bytes: usize,
}

/// Actual native fetch refusals, preserving original native error information.
#[derive(Debug)]
pub enum NativeFetchFailure {
    /// An actual native operation/counter/deadline refusal.
    Budget(StorageFetchFailure),
    /// An actual engine/coordinator/queue refusal.
    Engine(EngineError),
    /// An actual engine refusal completed after original E.
    LateEngine(EngineError),
    /// An actual shard plan/read refused; retained child errors supply detail.
    Shard(ShardId),
    /// A real queued command was destroyed or its worker stopped.
    WorkerStopped(ShardId),
}
impl From<StorageFetchFailure> for NativeFetchFailure {
    fn from(error: StorageFetchFailure) -> Self {
        Self::Budget(error)
    }
}
impl From<EngineError> for NativeFetchFailure {
    fn from(error: EngineError) -> Self {
        Self::Engine(error)
    }
}
impl From<BoundedFetchCoordinatorFailure> for NativeFetchFailure {
    fn from(error: BoundedFetchCoordinatorFailure) -> Self {
        match error {
            BoundedFetchCoordinatorFailure::Budget(error) => Self::Budget(error),
            BoundedFetchCoordinatorFailure::Engine(error) => Self::Engine(error),
            BoundedFetchCoordinatorFailure::LateEngine(error) => Self::LateEngine(error),
        }
    }
}
impl fmt::Display for NativeFetchFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Budget(error) => fmt::Display::fmt(error, f),
            Self::Engine(error) | Self::LateEngine(error) => fmt::Display::fmt(error, f),
            Self::Shard(_) => f.write_str("native fetch shard refused"),
            Self::WorkerStopped(_) => f.write_str("native fetch worker stopped"),
        }
    }
}
impl Error for NativeFetchFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Budget(error) => Some(error),
            Self::Engine(error) | Self::LateEngine(error) => Some(error),
            _ => None,
        }
    }
}

/// Completed ordinary native data with the actual cumulative debit snapshot.
#[derive(Debug)]
pub struct NativeFetchResult {
    batches: Vec<FetchedBatch>,
    usage: StorageFetchUsage,
}
impl NativeFetchResult {
    /// Borrow actual native batches; these are data, not serving authority.
    pub fn batches(&self) -> &[FetchedBatch] {
        &self.batches
    }
    /// Move only already-completed payload data into a response serializer.
    pub fn into_batches(self) -> Vec<FetchedBatch> {
        self.batches
    }
    /// Actual cumulative native fees, including paid refused queue/read entry.
    pub const fn usage(&self) -> StorageFetchUsage {
        self.usage
    }
}

enum Custody {
    Early(EngineHandle),
    Entered(Box<Scope>),
}
/// A native refusal retaining the same actual engine and all partial data.
///
/// Entered commands are drained before this is returned. A stopped-worker
/// outcome remains a failure, never a healthy completion; the same engine owner
/// retains its actual worker destruction/join obligations.
pub struct RetainedNativeFetchError {
    failure: NativeFetchFailure,
    custody: Custody,
}
impl fmt::Debug for RetainedNativeFetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedNativeFetchError")
            .field("failure", &self.failure)
            .field("pending", &self.scope().map_or(0, |scope| scope.pending))
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedNativeFetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.failure, f)
    }
}
impl Error for RetainedNativeFetchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.failure)
    }
}
impl RetainedNativeFetchError {
    fn early(owner: &EngineHandle, failure: NativeFetchFailure) -> Self {
        Self {
            failure,
            custody: Custody::Early(owner.clone()),
        }
    }
    fn scope(&self) -> Option<&Scope> {
        match &self.custody {
            Custody::Early(_) => None,
            Custody::Entered(scope) => Some(scope),
        }
    }
    /// Actual native/engine/worker refusal.
    pub fn failure(&self) -> &NativeFetchFailure {
        &self.failure
    }
    /// Borrow the same actual strong engine, without bare Arc extraction.
    pub fn engine_handle(&self) -> &EngineHandle {
        match &self.custody {
            Custody::Early(owner) => owner,
            Custody::Entered(scope) => &scope.owner,
        }
    }
    /// Actual cumulative native counters, or their retained poison refusal.
    pub fn usage(&self) -> Result<StorageFetchUsage, StorageFetchFailure> {
        self.scope()
            .and_then(|scope| scope.budget.as_ref())
            .map_or(Ok(StorageFetchUsage::default()), StorageFetchBudget::usage)
    }
    /// Borrow every real refused native plan/read cause after physical drain.
    pub fn storage_failures(&self) -> impl Iterator<Item = &StorageFetchFailure> {
        self.scope()
            .into_iter()
            .flat_map(|scope| &scope.child_errors)
            .filter_map(|completion| match completion {
                BoundedFetchCompletion::Plan {
                    result: Err(error), ..
                } => Some(error.failure()),
                BoundedFetchCompletion::Read {
                    result: Err(error), ..
                } => Some(error.failure()),
                _ => None,
            })
    }
    /// Retained actual queue errors that also completed after a budget refusal.
    pub fn queue_errors(&self) -> &[EngineError] {
        self.scope()
            .map_or(&[], |scope| scope.queue_errors.as_slice())
    }
    /// Already-completed payload bytes still retained on this refusal.
    pub fn completed_payloads(&self) -> impl Iterator<Item = &[u8]> {
        self.scope().into_iter().flat_map(|scope| {
            scope
                .batches
                .iter()
                .flatten()
                .chain(scope.materialized.iter())
                .map(|batch| batch.payload.as_ref())
                .chain(scope.converted.iter().map(|batch| batch.payload.as_ref()))
                .chain(
                    scope
                        .child_errors
                        .iter()
                        .flat_map(|completion| match completion {
                            BoundedFetchCompletion::Read {
                                result: Err(error), ..
                            } => error.batches(),
                            _ => &[],
                        })
                        .map(|batch| batch.payload.as_ref()),
                )
        })
    }
}

struct Scope {
    sender: Option<Sender<BoundedFetchCompletion>>,
    receiver: Option<Receiver<BoundedFetchCompletion>>,
    pending: usize,
    plans: Vec<Vec<StoredBatchMetadata>>,
    batches: Vec<VecDeque<StoredBatch>>,
    plan_seen: Vec<bool>,
    read_seen: Vec<bool>,
    materialized: VecDeque<StoredBatch>,
    converted: Vec<FetchedBatch>,
    child_errors: Vec<BoundedFetchCompletion>,
    queue_errors: Vec<EngineError>,
    budget: Option<StorageFetchBudget>,
    // Last: all task results, payload owners and command cleanup precede root.
    owner: EngineHandle,
}
impl Scope {
    fn retained(owner: EngineHandle) -> Self {
        Self {
            sender: None,
            receiver: None,
            pending: 0,
            plans: Vec::new(),
            batches: Vec::new(),
            plan_seen: Vec::new(),
            read_seen: Vec::new(),
            materialized: VecDeque::new(),
            converted: Vec::new(),
            child_errors: Vec::new(),
            queue_errors: Vec::new(),
            budget: None,
            owner,
        }
    }
    fn fail(mut self: Box<Self>, failure: NativeFetchFailure) -> RetainedNativeFetchError {
        if let Some(budget) = &self.budget {
            budget.deny();
        }
        self.sender.take();
        let _ = self.drain();
        RetainedNativeFetchError {
            failure,
            custody: Custody::Entered(self),
        }
    }
    fn drain(&mut self) -> Result<(), NativeFetchFailure> {
        let mut first_error = None;
        while self.pending > 0 {
            let received = self
                .receiver
                .as_ref()
                .ok_or_else(|| {
                    NativeFetchFailure::Engine(EngineError::CorruptState(
                        "bounded fetch has no actual completion receiver".into(),
                    ))
                })?
                .recv();
            let received = match received {
                Ok(received) => received,
                Err(_) => {
                    self.pending = 0;
                    return Err(NativeFetchFailure::Engine(EngineError::CorruptState(
                        "bounded fetch completion channel disconnected".into(),
                    )));
                }
            };
            self.pending -= 1;
            let error = match received {
                BoundedFetchCompletion::Plan { lane, result } => {
                    if lane >= self.plans.len() || self.plan_seen[lane] {
                        Some(NativeFetchFailure::Engine(EngineError::CorruptState(
                            "bounded fetch duplicate/invalid plan worker".into(),
                        )))
                    } else {
                        self.plan_seen[lane] = true;
                        match result {
                            Ok(planned) => {
                                self.plans[lane] = planned;
                                None
                            }
                            Err(error) => {
                                self.child_errors.push(BoundedFetchCompletion::Plan {
                                    lane,
                                    result: Err(error),
                                });
                                Some(NativeFetchFailure::Shard(self.owner.shards[lane].shard_id))
                            }
                        }
                    }
                }
                BoundedFetchCompletion::Read { lane, result } => {
                    if lane >= self.batches.len() || self.read_seen[lane] {
                        Some(NativeFetchFailure::Engine(EngineError::CorruptState(
                            "bounded fetch duplicate/invalid read worker".into(),
                        )))
                    } else {
                        self.read_seen[lane] = true;
                        match result {
                            Ok(batches) => {
                                self.batches[lane] = VecDeque::from(batches);
                                None
                            }
                            Err(error) => {
                                self.child_errors.push(BoundedFetchCompletion::Read {
                                    lane,
                                    result: Err(error),
                                });
                                Some(NativeFetchFailure::Shard(self.owner.shards[lane].shard_id))
                            }
                        }
                    }
                }
                BoundedFetchCompletion::Stopped { lane, shard } => {
                    self.child_errors
                        .push(BoundedFetchCompletion::Stopped { lane, shard });
                    Some(NativeFetchFailure::WorkerStopped(shard))
                }
            };
            if let Some(error) = error {
                if first_error.is_none() {
                    first_error = Some(error);
                }
                if let Some(budget) = &self.budget {
                    budget.deny();
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        if self.pending != 0 {
            if let Some(budget) = &self.budget {
                budget.deny();
            }
            self.sender.take();
            let _ = self.drain();
        }
    }
}

fn sum(a: usize, b: usize) -> Result<usize, StorageFetchFailure> {
    a.checked_add(b).ok_or(StorageFetchFailure::Capacity)
}
fn product(a: usize, b: usize) -> Result<usize, StorageFetchFailure> {
    a.checked_mul(b).ok_or(StorageFetchFailure::Capacity)
}

/// Derive checked plain native costs from the actual engine configuration.
///
/// Fixed source-visible scope/channel/value slots and cumulative declared/work
/// ceilings are included. Actual stored read/plan data, bitmap and final output
/// allocations are charged to those same ceilings before work. Private library,
/// allocator/OS layout and total process RSS are separate measured gates.
pub fn native_fetch_cost(
    config: &EngineConfig,
    limits: NativeFetchLimits,
) -> Result<NativeFetchCost, NativeFetchFailure> {
    config.validate()?;
    if limits.max_selected_batches == 0 || limits.max_candidate_window == 0 {
        return Err(StorageFetchFailure::Capacity.into());
    }
    let shards = usize::try_from(config.shard_count).map_err(|_| StorageFetchFailure::Capacity)?;
    let storage = storage_fetch_cost(limits.storage())?;
    let slots = product(shards, 2)?;
    let fixed = sum(
        sum(size_of::<Scope>(), size_of::<RetainedNativeFetchError>())?,
        sum(
            size_of::<NativeFetchCost>(),
            product(slots, bounded_fetch_command_bytes())?,
        )?,
    )?;
    Ok(NativeFetchCost {
        declared_bytes: sum(storage.declared_bytes, fixed)?,
        entries: sum(storage.entries, sum(slots, 8)?)?,
        string_bytes: storage.string_bytes,
        work_bytes: sum(storage.work_bytes, fixed)?,
    })
}

impl EngineHandle {
    /// Fetch a finite native prefix using one original operation E and counters.
    ///
    /// Metadata, zero-payload batches and sparse batch-ID bitmap spans have
    /// explicit caps. Every admitted command is physically settled on error or
    /// unwind; this retains the same engine and never grants serving authority.
    pub fn fetch_until(
        &self,
        request: FetchRequest,
        limits: NativeFetchLimits,
        original_expiry: Instant,
    ) -> Result<NativeFetchResult, RetainedNativeFetchError> {
        let early = |failure| RetainedNativeFetchError::early(self, failure);
        if Instant::now() >= original_expiry {
            return Err(early(NativeFetchFailure::Budget(
                StorageFetchFailure::Expired(None),
            )));
        }
        let measured = native_fetch_cost(&self.config, limits);
        if Instant::now() >= original_expiry {
            return Err(early(NativeFetchFailure::Budget(
                StorageFetchFailure::Expired(None),
            )));
        }
        measured.map_err(&early)?;
        let budget = StorageFetchBudget::new(limits.storage(), original_expiry)
            .map_err(|error| early(error.into()))?;
        budget.check().map_err(|error| early(error.into()))?;
        // Scope bytes are in the precharged fixed native cost. No scope heap
        // allocation occurs for an already-expired/refused initial entry.
        let mut scope = Box::new(Scope::retained(self.clone()));
        scope.budget = Some(budget.clone());
        let prepared = (|| {
            budget.check()?;
            run(self, request, false, limits, &budget, &mut scope)?;
            budget.check()?;
            let count = scope.materialized.len();
            let bytes = product(count, size_of::<FetchedBatch>())
                .and_then(|value| sum(value, size_of::<Vec<FetchedBatch>>()))?;
            budget.charge_values(bytes, sum(count, 1)?, bytes)?;
            budget.check()?;
            Ok(budget)
        })();
        let budget = match prepared {
            Ok(budget) => budget,
            Err(error) => return Err(scope.fail(error)),
        };
        let converted = (|| {
            budget.check()?;
            scope.converted = Vec::with_capacity(scope.materialized.len());
            budget.check()?;
            while !scope.materialized.is_empty() {
                budget.check()?;
                if let Some(batch) = scope.materialized.pop_front() {
                    scope.converted.push(super::fetched_batch(batch));
                }
                budget.check()?;
            }
            let usage = budget.usage();
            budget.check()?;
            usage
        })();
        let usage = match converted {
            Ok(usage) => usage,
            Err(error) => return Err(scope.fail(error.into())),
        };
        let batches = std::mem::take(&mut scope.converted);
        Ok(NativeFetchResult { batches, usage })
    }
}

fn run(
    owner: &EngineHandle,
    request: FetchRequest,
    include_unresolved: bool,
    limits: NativeFetchLimits,
    budget: &StorageFetchBudget,
    scope: &mut Scope,
) -> Result<(), NativeFetchFailure> {
    budget.check()?;
    request
        .validate()
        .map_err(|error| EngineError::InvalidConfig(format!("invalid fetch request: {error}")))?;
    let max_bytes = usize::try_from(request.max_bytes)
        .map_err(|_| EngineError::InvalidConfig("max_bytes exceeds usize".into()))?;
    if max_bytes > owner.config.max_fetch_bytes {
        return Err(EngineError::InvalidConfig(format!(
            "fetch max_bytes {max_bytes} exceeds configured maximum {}",
            owner.config.max_fetch_bytes
        ))
        .into());
    }
    let shards = owner.shards.len();
    let value_bytes = product(
        shards,
        size_of::<Vec<StoredBatchMetadata>>()
            + size_of::<VecDeque<StoredBatch>>()
            + 2 * size_of::<bool>(),
    )?;
    let result_slots = product(shards, 2)?;
    let retained_bytes = product(
        result_slots,
        size_of::<BoundedFetchCompletion>() + size_of::<EngineError>(),
    )?;
    // Real completion messages can reside in the channel while previously
    // received failures remain in the retained vectors. Pay both capacities.
    let channel_bytes = sum(
        size_of::<(
            Sender<BoundedFetchCompletion>,
            Receiver<BoundedFetchCompletion>,
        )>(),
        product(result_slots, size_of::<BoundedFetchCompletion>())?,
    )?;
    let bytes = sum(sum(value_bytes, retained_bytes)?, channel_bytes)?;
    let entries = sum(sum(product(shards, 4)?, product(result_slots, 3)?)?, 2)?;
    budget.charge_values(bytes, entries, bytes)?;
    budget.check()?;
    scope.plans = (0..shards).map(|_| Vec::new()).collect();
    budget.check()?;
    scope.batches = (0..shards).map(|_| VecDeque::new()).collect();
    budget.check()?;
    scope.plan_seen = vec![false; shards];
    scope.read_seen = vec![false; shards];
    budget.check()?;
    scope.child_errors = Vec::with_capacity(result_slots);
    scope.queue_errors = Vec::with_capacity(result_slots);
    budget.check()?;
    let (sender, receiver) = channel();
    scope.sender = Some(sender);
    scope.receiver = Some(receiver);
    budget.check()?;
    let topic_partition = TopicPartition::new(request.topic_id, request.partition_id);
    let bounds =
        owner
            .coordinators
            .fetch_snapshot_until(topic_partition, request.start_offset, budget)?;
    budget.check()?;
    let ordered_end = if include_unresolved {
        bounds.physical_end
    } else {
        match request.mode {
            FetchMode::Replicated => bounds.replicated_end,
            FetchMode::Ordered | FetchMode::AsAvailable => bounds.ordered_end,
        }
    };
    for (lane, shard) in owner.shards.iter().enumerate() {
        budget.check()?;
        let completion = scope
            .sender
            .as_ref()
            .ok_or_else(|| {
                EngineError::CorruptState("bounded fetch lost completion sender".into())
            })?
            .clone();
        match shard.begin_bounded_fetch_plan(
            BoundedFetchPlanInput {
                lane,
                topic_partition,
                start_offset: request.start_offset,
                max_bytes,
                max_batches: limits.max_plan_batches_per_shard,
            },
            budget,
            completion,
        ) {
            Ok(()) => scope.pending += 1,
            Err(error) => return Err(dispatch(error, scope)),
        }
        budget.check()?;
    }
    scope.drain()?;
    budget.check()?;
    let candidates_len = scope
        .plans
        .iter()
        .try_fold(0usize, |total, planned| sum(total, planned.len()))?;
    let bytes = sum(
        product(candidates_len, size_of::<BatchId>())?,
        size_of::<Vec<BatchId>>(),
    )?;
    budget.charge_values(bytes, sum(candidates_len, 1)?, bytes)?;
    budget.check()?;
    let mut candidates = Vec::with_capacity(candidates_len);
    for planned in &scope.plans {
        for metadata in planned {
            budget.check()?;
            candidates.push(metadata.reservation().batch_id);
        }
    }
    budget.check()?;
    let committed = owner.coordinators.committed_candidates_until(
        topic_partition,
        candidates,
        include_unresolved,
        limits.max_candidate_window,
        budget,
    )?;
    budget.check()?;
    let selection = super::select_fetch_plan_bounded(
        &scope.plans,
        request.mode,
        ordered_end,
        Some(&committed),
        max_bytes,
        limits.max_selected_batches,
        Some(budget),
    )?;
    budget.check()?;
    for (lane, (shard, planned)) in owner.shards.iter().zip(selection.by_shard).enumerate() {
        budget.check()?;
        if !planned.is_empty() {
            let completion = scope
                .sender
                .as_ref()
                .ok_or_else(|| EngineError::CorruptState("bounded fetch lost read sender".into()))?
                .clone();
            match shard.begin_bounded_fetch_read(lane, planned, budget, completion) {
                Ok(()) => scope.pending += 1,
                Err(error) => return Err(dispatch(error, scope)),
            }
        }
        budget.check()?;
    }
    scope.drain()?;
    budget.check()?;
    let bytes = sum(
        product(selection.merge_order.len(), size_of::<StoredBatch>())?,
        size_of::<Vec<StoredBatch>>(),
    )?;
    budget.charge_values(bytes, sum(selection.merge_order.len(), 1)?, bytes)?;
    budget.check()?;
    scope.materialized = VecDeque::with_capacity(selection.merge_order.len());
    budget.check()?;
    for &lane in &selection.merge_order {
        budget.check()?;
        budget.charge_values(0, 0, size_of::<StoredBatch>())?;
        let batch = super::pop_materialized_batch(&mut scope.batches, lane)?;
        scope.materialized.push_back(batch);
        budget.check()?;
    }
    Ok(())
}
fn dispatch(error: BoundedFetchDispatchError, scope: &mut Scope) -> NativeFetchFailure {
    match error {
        BoundedFetchDispatchError::Queue(error) => NativeFetchFailure::Engine(error),
        BoundedFetchDispatchError::Budget {
            failure,
            queue_error,
        } => {
            if let Some(error) = queue_error {
                scope.queue_errors.push(error);
            }
            NativeFetchFailure::Budget(failure)
        }
    }
}
