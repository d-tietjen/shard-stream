//! Bounded producer progress and the actual last visible origin-log batch.

use super::*;
use crate::EngineHandle;
use shardlog::{StorageFetchBudget, StorageFetchFailure, StorageFetchUsage, storage_fetch_cost};
use std::any::Any;
use std::error::Error;
use std::sync::Mutex;

/// Finite cumulative work for all origin queries in one composing command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeOriginProgressLimits {
    /// Query attempts, including real absent-producer observations.
    pub max_queries: usize,
    /// Prepaid coordinator/lane command slots across every query.
    pub max_commands: usize,
    /// SAME cumulative native plan/read/value/path/work limits for every query.
    pub fetch: NativeFetchLimits,
}

/// Plain complete logical ceiling; no allocation, admission or authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeOriginProgressCost {
    /// Fixed setup, finite query frames and cumulative native value ceiling.
    pub declared_bytes: usize,
    /// Source-visible values and command/outcome slots.
    pub entries: usize,
    /// Path/diagnostic subset already included in declared bytes.
    pub string_bytes: usize,
    /// Complete explicit native setup, entry and work ceiling.
    pub work_bytes: usize,
}

/// Actual cumulative fees; reserved command slots need not all enter workers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NativeOriginProgressUsage {
    /// Paid query attempts, never reset between partitions/origins.
    pub queries: usize,
    /// Paid command slots, including worst-case lane read slots before fetch.
    pub commands: usize,
    /// SAME cumulative real native allocation, read-attempt/byte and work fees.
    pub storage: StorageFetchUsage,
}

#[derive(Default)]
struct Counters {
    queries: usize,
    commands: usize,
}
struct Inner {
    limits: NativeOriginProgressLimits,
    counters: Mutex<Counters>,
    storage: StorageFetchBudget,
}

/// Non-clone ordinary work budget shared by reference across one real command.
///
/// Prepay `native_origin_progress_cost` into the composing command's actual
/// ledger before construction. This owns no resource capability or permission;
/// fresh work cannot replace the host's original deadline/admission policy.
pub struct NativeOriginProgressBudget {
    inner: Arc<Inner>,
}
impl fmt::Debug for NativeOriginProgressBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeOriginProgressBudget")
            .field("expiry", &self.original_expiry())
            .field("limits", &self.inner.limits)
            .finish_non_exhaustive()
    }
}
impl NativeOriginProgressBudget {
    /// Constructs finite counters at the caller's original absolute minimum E.
    ///
    /// # Errors
    /// Returns actual expiry, checked limit/cost or allocation-policy refusal.
    pub fn new(
        limits: NativeOriginProgressLimits,
        original_expiry: Instant,
    ) -> Result<Self, StorageFetchFailure> {
        let storage = StorageFetchBudget::new(limits.fetch.storage(), original_expiry)?;
        let budget = Self {
            inner: Arc::new(Inner {
                limits,
                counters: Mutex::new(Counters::default()),
                storage,
            }),
        };
        budget.check()?;
        Ok(budget)
    }
    /// The SAME saved absolute expiry, never a new relative timeout.
    pub fn original_expiry(&self) -> Instant {
        self.inner.storage.absolute_expiry()
    }
    /// The original immutable cumulative limits.
    pub fn limits(&self) -> NativeOriginProgressLimits {
        self.inner.limits
    }
    /// Checks E and sticky native refusal without replacing any counters.
    pub fn check(&self) -> Result<(), StorageFetchFailure> {
        self.inner.storage.check()
    }
    /// Observes actual cumulative counters without claiming completion.
    pub fn usage(&self) -> Result<NativeOriginProgressUsage, StorageFetchFailure> {
        let counters = self
            .inner
            .counters
            .lock()
            .map_err(|_| StorageFetchFailure::Poisoned)?;
        let queries = counters.queries;
        let commands = counters.commands;
        drop(counters);
        Ok(NativeOriginProgressUsage {
            queries,
            commands,
            storage: self.inner.storage.usage()?,
        })
    }
    pub(crate) fn share(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
    pub(crate) fn storage(&self) -> &StorageFetchBudget {
        &self.inner.storage
    }
    pub(crate) fn deny(&self) {
        self.inner.storage.deny();
    }
    fn reserve(&self, queries: usize, commands: usize) -> Result<(), StorageFetchFailure> {
        self.check()?;
        let mut counters = self
            .inner
            .counters
            .lock()
            .map_err(|_| StorageFetchFailure::Poisoned)?;
        let next = counters
            .queries
            .checked_add(queries)
            .zip(counters.commands.checked_add(commands));
        match next {
            Some((queries, commands))
                if queries <= self.inner.limits.max_queries
                    && commands <= self.inner.limits.max_commands =>
            {
                counters.queries = queries;
                counters.commands = commands;
            }
            _ => {
                drop(counters);
                self.deny();
                return Err(StorageFetchFailure::Capacity);
            }
        }
        drop(counters);
        self.check()
    }
}

/// Actual same-worker producer lookup refusal; no empty state is fabricated.
pub enum NativeOriginProgressQueryError {
    /// SAME native budget refused before the next lookup phase.
    Budget(StorageFetchFailure),
    /// Original native engine error, including an unknown partition.
    Engine(EngineError),
    /// The actual finite coordinator queue was full before entry.
    QueueFull,
    /// The actual entered command/worker stopped without a result.
    WorkerStopped,
    /// Actual native lookup frame unwound; its original payload is retained.
    Unwind(Box<dyn Any + Send>),
}
impl fmt::Debug for NativeOriginProgressQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Budget(e) => f.debug_tuple("Budget").field(e).finish(),
            Self::Engine(e) => f.debug_tuple("Engine").field(e).finish(),
            Self::QueueFull => f.write_str("QueueFull"),
            Self::WorkerStopped => f.write_str("WorkerStopped"),
            Self::Unwind(_) => f.write_str("Unwind(actual native payload)"),
        }
    }
}
impl fmt::Display for NativeOriginProgressQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "origin producer lookup refused: {self:?}")
    }
}
impl Error for NativeOriginProgressQueryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Budget(e) => Some(e),
            Self::Engine(e) => Some(e),
            _ => None,
        }
    }
}

/// Actual producer result retained before a separate post-operation refusal.
pub struct NativeOriginProgressObservation {
    pub(crate) result: Result<Option<ProducerProgress>, NativeOriginProgressQueryError>,
    pub(crate) refusal: Option<StorageFetchFailure>,
    pub(crate) post_panic: Option<Box<dyn Any + Send>>,
}
impl fmt::Debug for NativeOriginProgressObservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeOriginProgressObservation")
            .field("result", &self.result)
            .field("refusal", &self.refusal)
            .field("post_panic", &self.post_panic.is_some())
            .finish()
    }
}
impl NativeOriginProgressObservation {
    pub(crate) fn completed(
        result: Result<Option<ProducerProgress>, NativeOriginProgressQueryError>,
        budget: &NativeOriginProgressBudget,
    ) -> Self {
        let mut observed = Self {
            result,
            refusal: None,
            post_panic: None,
        };
        observed.refusal = budget.check().err();
        observed
    }
    /// Actual optional producer cursor; `Some(None)` is a real queried absence.
    pub fn producer_progress(&self) -> Option<Option<ProducerProgress>> {
        self.result.as_ref().ok().copied()
    }
    /// Original typed native/queue/unwind cause.
    pub fn error(&self) -> Option<&NativeOriginProgressQueryError> {
        self.result.as_ref().err()
    }
    /// SAME-work refusal observed after the actual result was saved.
    pub fn refusal(&self) -> Option<&StorageFetchFailure> {
        self.refusal.as_ref()
    }
    /// Original post-observation frame unwind, independently of its real data.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.post_panic.as_deref()
    }
    fn accepted(&self) -> bool {
        self.result.is_ok() && self.refusal.is_none() && self.post_panic.is_none()
    }
}

/// Actual progress continuation refusal; typed observations remain in custody.
#[derive(Debug)]
pub enum NativeOriginProgressFailure {
    /// Original/native cumulative limits, counters or E refused.
    Budget(StorageFetchFailure),
    /// Actual configuration/cost derivation refused before entering work.
    Admission(NativeFetchFailure),
    /// Actual same-worker producer observation refused.
    Query,
    /// Actual bounded visible native fetch refused; the whole child is retained.
    Fetch,
    /// Actual owning continuation frame unwound; original payload is retained.
    Unwind,
}
impl From<StorageFetchFailure> for NativeOriginProgressFailure {
    fn from(e: StorageFetchFailure) -> Self {
        Self::Budget(e)
    }
}
impl fmt::Display for NativeOriginProgressFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "origin progress refused: {self:?}")
    }
}
impl Error for NativeOriginProgressFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Budget(e) => Some(e),
            Self::Admission(e) => Some(e),
            _ => None,
        }
    }
}

struct Scope {
    budget: NativeOriginProgressBudget,
    partition: TopicPartition,
    producer_id: u128,
    observation: Option<NativeOriginProgressObservation>,
    batches: VecDeque<StoredBatch>,
    tail: Option<FetchedBatch>,
    fetch_error: Option<RetainedNativeFetchError>,
    pending: Option<crate::coordinator::OriginProgressTicket>,
    entered: bool,
    panic: Option<Box<dyn Any + Send>>,
    // LAST: all actual command inputs/results/body owners settle before engine.
    owner: EngineHandle,
}
impl Scope {
    fn drain(&mut self) {
        if let Some(ticket) = &mut self.pending {
            self.observation = Some(ticket.settle());
            self.pending.take();
            // Actual returned cursor/error is stored before this post-receive E.
            if let Some(observed) = &mut self.observation
                && observed.refusal.is_none()
            {
                observed.refusal = self.budget.check().err();
            }
        }
    }
    fn fail(
        mut self: Box<Self>,
        failure: NativeOriginProgressFailure,
    ) -> RetainedNativeOriginProgressError {
        self.budget.deny();
        self.drain();
        RetainedNativeOriginProgressError {
            failure,
            custody: Custody::Entered(self),
        }
    }
    fn run(&mut self) -> Result<(), NativeOriginProgressFailure> {
        self.budget.check()?;
        match self.owner.coordinators.begin_origin_progress(
            self.partition,
            self.producer_id,
            &self.budget,
        ) {
            Ok(ticket) => {
                self.pending = Some(ticket);
                self.entered = true;
            }
            Err(observed) => {
                self.observation = Some(observed);
                return Err(NativeOriginProgressFailure::Query);
            }
        }
        let after = self.budget.check();
        if after.is_err() {
            self.budget.deny();
        }
        self.drain();
        after?;
        let observed = self
            .observation
            .as_ref()
            .ok_or(NativeOriginProgressFailure::Query)?;
        if !observed.accepted() {
            return Err(NativeOriginProgressFailure::Query);
        }
        let Some(Some(progress)) = observed.producer_progress() else {
            return Ok(());
        };
        if progress.next_sequence == 0 {
            return Ok(());
        }
        self.budget.reserve(
            0,
            super::bounded_fetch::command_slots(self.owner.shards.len())?,
        )?;
        match super::bounded_fetch::visible_with_budget(
            &self.owner,
            FetchRequest {
                request_id: 0,
                topic_id: self.partition.topic_id,
                partition_id: self.partition.partition_id,
                start_offset: LogicalOffset::new(progress.next_sequence - 1),
                max_bytes: 1,
                mode: FetchMode::Ordered,
            },
            self.budget.limits().fetch,
            self.budget.storage(),
        ) {
            Ok(batches) => self.batches = batches,
            Err(error) => {
                self.fetch_error = Some(error);
                return Err(NativeOriginProgressFailure::Fetch);
            }
        }
        self.budget.check()?;
        if let Some(batch) = self.batches.front() {
            // Keep the original native body and metadata outside catch while
            // exposing only this separately charged completed data clone.
            self.budget.storage().charge_values(
                size_of::<FetchedBatch>() + size_of::<StoredBatch>(),
                2,
                size_of::<FetchedBatch>() + size_of::<StoredBatch>(),
            )?;
            self.budget.check()?;
            self.tail = Some(fetched_batch(batch.clone()));
        }
        self.budget.check()?;
        Ok(())
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        if self.pending.is_some() {
            self.budget.deny();
            self.drain();
        }
    }
}
struct Early {
    budget: NativeOriginProgressBudget,
    owner: EngineHandle,
}
enum Custody {
    Early(Early),
    Entered(Box<Scope>),
}

/// Completed native producer observation and actual optional visible tail.
///
/// This keeps the SAME actual engine allocation. It does not decode an AA
/// envelope, identify an origin/writer, create a topic or grant authority.
pub struct NativeOriginProgressReport {
    scope: Box<Scope>,
    usage: NativeOriginProgressUsage,
}
impl fmt::Debug for NativeOriginProgressReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeOriginProgressReport")
            .field("usage", &self.usage)
            .field("observation", &self.scope.observation)
            .finish_non_exhaustive()
    }
}
impl NativeOriginProgressReport {
    /// Genuine successful lookup, including real absence on an existing topic.
    pub fn producer_progress(&self) -> Option<ProducerProgress> {
        self.scope
            .observation
            .as_ref()
            .and_then(NativeOriginProgressObservation::producer_progress)
            .flatten()
    }
    /// First actual Ordered batch returned at `next_sequence - 1`, when read.
    /// Missing visible data does not replace a known nonzero producer cursor.
    pub fn last_batch(&self) -> Option<&FetchedBatch> {
        self.scope.tail.as_ref()
    }
    /// Actual producer metadata from the SAME first visible native batch.
    /// This copies fixed fields from the retained stored frame, without another
    /// query, decode or allocation; no queried identity is substituted.
    pub fn last_producer_identity(&self) -> Option<shard_stream_protocol::ProducerIdentity> {
        self.scope.batches.front()?.producer.map(|producer| {
            shard_stream_protocol::ProducerIdentity {
                producer_id: producer.producer_id,
                epoch: producer.epoch,
                first_sequence: producer.first_sequence,
                event_id: producer.event_id,
            }
        })
    }
    /// Actual native digest algorithm identifier and digest for that batch.
    /// These are retained source facts, not an envelope or authority decision.
    pub fn last_producer_digest(&self) -> Option<(u8, [u8; 16])> {
        self.scope
            .batches
            .front()?
            .producer
            .map(|producer| (producer.digest_algorithm, producer.payload_digest))
    }
    /// SAME engine owner retained through completed native observations.
    pub fn engine_handle(&self) -> &EngineHandle {
        &self.scope.owner
    }
    /// Actual cumulative fees at this completed query, not a refreshed budget.
    pub fn usage(&self) -> NativeOriginProgressUsage {
        self.usage
    }
    /// Original saved E for this same cumulative command.
    pub fn original_expiry(&self) -> Instant {
        self.scope.budget.original_expiry()
    }
}

/// Whole actual engine, pending/result/body custody through genuine refusal.
pub struct RetainedNativeOriginProgressError {
    failure: NativeOriginProgressFailure,
    custody: Custody,
}
impl fmt::Debug for RetainedNativeOriginProgressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedNativeOriginProgressError")
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedNativeOriginProgressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.failure, f)
    }
}
impl Error for RetainedNativeOriginProgressError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.failure)
    }
}
impl RetainedNativeOriginProgressError {
    /// Original continuation refusal without replacing actual native causes.
    pub fn failure(&self) -> &NativeOriginProgressFailure {
        &self.failure
    }
    /// Same actual strong engine; no raw Arc/parts escape.
    pub fn engine_handle(&self) -> &EngineHandle {
        match &self.custody {
            Custody::Early(e) => &e.owner,
            Custody::Entered(s) => &s.owner,
        }
    }
    /// Actual producer result/error saved before any post-operation refusal.
    pub fn observation(&self) -> Option<&NativeOriginProgressObservation> {
        match &self.custody {
            Custody::Early(_) => None,
            Custody::Entered(s) => s.observation.as_ref(),
        }
    }
    /// Whether this continuation actually enqueued the producer lookup.
    pub fn query_entered(&self) -> bool {
        match &self.custody {
            Custody::Early(_) => false,
            Custody::Entered(s) => s.entered,
        }
    }
    /// Whole actual bounded native read error and every partial native owner.
    pub fn fetch_error(&self) -> Option<&RetainedNativeFetchError> {
        match &self.custody {
            Custody::Early(_) => None,
            Custody::Entered(s) => s.fetch_error.as_ref(),
        }
    }
    /// Already completed native bodies retained outside the owning catch frame.
    pub fn completed_payloads(&self) -> impl Iterator<Item = &[u8]> {
        let scope = match &self.custody {
            Custody::Early(_) => None,
            Custody::Entered(s) => Some(s),
        };
        scope
            .into_iter()
            .flat_map(|s| s.batches.iter().map(|b| b.payload.as_ref()))
    }
    /// Original owning frame unwind payload, independently of native errors.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        match &self.custody {
            Custody::Early(_) => None,
            Custody::Entered(s) => s.panic.as_deref(),
        }
    }
    /// SAME cumulative counters, including paid refused attempts/command slots.
    pub fn usage(&self) -> Result<NativeOriginProgressUsage, StorageFetchFailure> {
        match &self.custody {
            Custody::Early(e) => e.budget.usage(),
            Custody::Entered(s) => s.budget.usage(),
        }
    }
}

fn add(a: usize, b: usize) -> Result<usize, NativeOriginProgressFailure> {
    a.checked_add(b)
        .ok_or_else(|| StorageFetchFailure::Capacity.into())
}
fn mul(a: usize, b: usize) -> Result<usize, NativeOriginProgressFailure> {
    a.checked_mul(b)
        .ok_or_else(|| StorageFetchFailure::Capacity.into())
}

/// Derives the complete finite logical query/fetch/frame ceiling before setup.
///
/// # Errors
/// Preserves actual native configuration/cost refusal and checked overflow.
pub fn native_origin_progress_cost(
    config: &EngineConfig,
    limits: NativeOriginProgressLimits,
) -> Result<NativeOriginProgressCost, NativeOriginProgressFailure> {
    let native =
        native_fetch_cost(config, limits.fetch).map_err(NativeOriginProgressFailure::Admission)?;
    let storage = storage_fetch_cost(limits.fetch.storage())?;
    let (command_bytes, command_entries) =
        crate::coordinator::origin_progress_command_cost().ok_or(StorageFetchFailure::Capacity)?;
    let fixed = [
        size_of::<Inner>(),
        size_of::<NativeOriginProgressBudget>(),
        size_of::<NativeOriginProgressCost>(),
        size_of::<Counters>(),
        size_of::<[usize; 2]>(),
    ]
    .into_iter()
    .try_fold(0usize, add)?;
    let frame = [
        size_of::<Scope>(),
        size_of::<NativeOriginProgressReport>(),
        size_of::<RetainedNativeOriginProgressError>(),
        size_of::<NativeOriginProgressObservation>(),
        size_of::<FetchedBatch>(),
        size_of::<StoredBatch>(),
        command_bytes,
    ]
    .into_iter()
    .try_fold(0usize, add)?;
    let fetch_fixed = native
        .declared_bytes
        .checked_sub(storage.declared_bytes)
        .ok_or(StorageFetchFailure::Capacity)?;
    let declared = add(
        add(storage.declared_bytes, fixed)?,
        mul(limits.max_queries, add(frame, fetch_fixed)?)?,
    )?;
    let entries = add(
        add(storage.entries, 7)?,
        mul(
            limits.max_queries,
            add(
                command_entries,
                add(
                    native
                        .entries
                        .checked_sub(storage.entries)
                        .ok_or(StorageFetchFailure::Capacity)?,
                    6,
                )?,
            )?,
        )?,
    )?;
    Ok(NativeOriginProgressCost {
        declared_bytes: declared,
        entries,
        string_bytes: native.string_bytes,
        work_bytes: add(add(storage.work_bytes, declared)?, entries)?,
    })
}

impl EngineHandle {
    /// Derives the full plain ceiling from this actual engine configuration.
    ///
    /// Prepay these facts in the composing command before creating its shared
    /// budget. This does no lookup, I/O, allocation or authority admission.
    ///
    /// # Errors
    /// Preserves checked native configuration/cost overflow and refusal.
    pub fn origin_progress_cost(
        &self,
        limits: NativeOriginProgressLimits,
    ) -> Result<NativeOriginProgressCost, NativeOriginProgressFailure> {
        native_origin_progress_cost(&self.config, limits)
    }

    /// Reads a real existing origin partition's producer and last visible data.
    ///
    /// The caller supplies its exact native origin partition/producer identity.
    /// This never creates a topic or falls back to ordinary progress/fetch.
    /// All origins in one composing command must share the SAME pre-admitted
    /// budget and original E. A real absent producer returns `None`; an unknown
    /// partition refuses. Known nonzero progress is retained even if its last
    /// event is not visible; callers must validate that observation and decode
    /// their own bounded envelope before using timestamps or writer epochs.
    ///
    /// # Errors
    /// Retains this SAME engine, actual native lookup/read/unwind results and
    /// partial data. Every entered command physically settles, even after E.
    #[allow(
        clippy::result_large_err,
        reason = "retain the same engine and original budget inline without allocating on refusal"
    )]
    pub fn origin_progress_until(
        &self,
        partition: TopicPartition,
        producer_id: u128,
        budget: &NativeOriginProgressBudget,
    ) -> Result<NativeOriginProgressReport, RetainedNativeOriginProgressError> {
        let early = |failure| {
            budget.deny();
            RetainedNativeOriginProgressError {
                failure,
                custody: Custody::Early(Early {
                    budget: budget.share(),
                    owner: self.clone(),
                }),
            }
        };
        budget.check().map_err(|e| early(e.into()))?;
        native_origin_progress_cost(&self.config, budget.limits()).map_err(&early)?;
        budget.reserve(1, 1).map_err(|e| early(e.into()))?;
        let bytes = size_of::<Scope>()
            + size_of::<NativeOriginProgressReport>()
            + size_of::<RetainedNativeOriginProgressError>();
        budget
            .storage()
            .charge_values(bytes, 3, bytes)
            .map_err(|e| early(e.into()))?;
        budget.check().map_err(|e| early(e.into()))?;
        let mut scope = Box::new(Scope {
            budget: budget.share(),
            partition,
            producer_id,
            observation: None,
            batches: VecDeque::new(),
            tail: None,
            fetch_error: None,
            pending: None,
            entered: false,
            panic: None,
            owner: self.clone(),
        });
        let run = catch_unwind(AssertUnwindSafe(|| scope.run()));
        let result = match run {
            Ok(result) => result,
            Err(payload) => {
                scope.panic = Some(payload);
                Err(NativeOriginProgressFailure::Unwind)
            }
        };
        if let Err(error) = result {
            return Err(scope.fail(error));
        }
        let usage = match scope.budget.usage() {
            Ok(usage) => usage,
            Err(error) => return Err(scope.fail(error.into())),
        };
        if let Err(error) = scope.budget.check() {
            return Err(scope.fail(error.into()));
        }
        Ok(NativeOriginProgressReport { scope, usage })
    }
}

#[cfg(all(test, unix))]
#[path = "origin_progress_tests.rs"]
mod tests;
