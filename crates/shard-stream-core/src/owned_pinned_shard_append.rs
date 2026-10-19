//! Consume a whole Core store through genuine pinned append and rotation.

use super::{OwnedPinnedShardStore, OwnedShardStoreDirectory};
use crate::{
    DirectoryOwnership, NativeDirectoryBudget, NativeDirectoryFailure, OwnedDirectoryHandle,
    OwnedShardDirectory, RetainedDirectoryError,
};
use shardlog::{
    PinnedShardAppendBudget, PinnedShardAppendChecks, PinnedShardAppendError,
    PinnedShardAppendFailure, PinnedShardAppendInput, PinnedShardAppendLimits,
    PinnedShardAppendPhase, PinnedShardAppendReport, PinnedShardAppendUsage, PreparedExtent,
    SealedPack, ShardDirectoryCheckFailure, ShardDirectoryChecks, StorageError,
    pinned_shard_append_cost,
};
use std::any::Any;
use std::cell::{Cell, RefCell};
use std::error::Error;
use std::fmt;
use std::io;
use std::mem::size_of;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, MutexGuard, TryLockError, TryLockResult};
use std::time::Instant;

type PinBorrow<'a> = Option<TryLockResult<MutexGuard<'a, OwnedShardDirectory>>>;

/// Borrowed actual append observations with no producer FD/native-owner access.
///
/// The raw native report/error and its created/retired Files remain private in
/// the Core owner. This view intentionally has no Deref, native report/error,
/// file/descriptor extractor or constructor accepting caller-claimed custody.
pub struct OwnedPinnedShardAppendObservation<'a> {
    native: &'a PinnedShardAppendReport,
}
impl OwnedPinnedShardAppendObservation<'_> {
    /// Original actual input values and payloads.
    pub fn inputs(&self) -> &[PinnedShardAppendInput] {
        self.native.inputs()
    }
    /// Actual prepared native receipt/header data, containing no File handles.
    pub fn prepared(&self) -> &[PreparedExtent] {
        self.native.prepared()
    }
    /// Actual complete or partial installed native index count.
    pub fn installed_frames(&self) -> usize {
        self.native.installed_frames()
    }
    /// Actual terminal cumulative native fees.
    pub fn usage(&self) -> PinnedShardAppendUsage {
        self.native.usage()
    }
    /// Actual completed sealed metadata, containing paths/scalars only.
    pub fn sealed_pack(&self) -> Option<&SealedPack> {
        self.native.sealed_pack()
    }
    /// Actual proposed sealed metadata, also on refused rotation.
    pub fn sealed_candidate(&self) -> Option<&SealedPack> {
        self.native.sealed_candidate()
    }
    /// Actual original active sequence.
    pub fn original_active_sequence(&self) -> Option<u64> {
        self.native.original_active_sequence()
    }
    /// Actual original active basename, not a reconstructed canonical name.
    pub fn original_active_name(&self) -> Option<&std::ffi::OsStr> {
        self.native.original_active_name()
    }
    /// Actual complete active pathname byte count.
    pub fn active_path_bytes(&self) -> Option<usize> {
        self.native.active_path_bytes()
    }
    /// Actual retained native C-name bytes, containing no descriptor.
    pub fn active_c_name(&self) -> Option<&std::ffi::CStr> {
        self.native.active_c_name()
    }
    /// Actual prepared native header bytes.
    pub fn prepared_headers(&self) -> impl Iterator<Item = &[u8]> {
        self.native.prepared_headers()
    }
    /// Actual unfinished native phase, including the original unwound phase.
    pub fn unfinished_phase(&self) -> Option<PinnedShardAppendPhase> {
        self.native.unfinished_phase()
    }
    /// Actual last completed phase, without promoting it to a permission.
    pub fn last_completed_phase(&self) -> Option<PinnedShardAppendPhase> {
        self.native.last_completed_phase()
    }
    /// Original native codec/validation error; no report/FD-bearing wrapper.
    pub fn native_error(&self) -> Option<&StorageError> {
        self.native.native_error()
    }
    /// Actual typed physical write results in entered order.
    pub fn write_results(&self) -> impl Iterator<Item = &io::Result<usize>> {
        self.native.write_results()
    }
    /// Sequence/frame/part/offset/requested length and actual typed write result.
    pub fn write_observations(
        &self,
    ) -> impl Iterator<Item = (u64, usize, usize, usize, usize, &io::Result<usize>)> {
        self.native.write_observations()
    }
    /// Actual native seek results.
    pub fn seek_results(&self) -> impl Iterator<Item = &io::Result<u64>> {
        self.native.seek_results()
    }
    /// Actual flush/sync/truncate and already-prepaid settlement results.
    pub fn unit_results(&self) -> impl Iterator<Item = (PinnedShardAppendPhase, &io::Result<()>)> {
        self.native.unit_results()
    }
    /// Actual descriptor/name metadata values/errors, without descriptors.
    pub fn metadata_results(
        &self,
    ) -> impl Iterator<Item = (PinnedShardAppendPhase, &io::Result<rustix::fs::Stat>)> {
        self.native.metadata_results()
    }
    /// Original actual CreateNew errors.
    pub fn create_errors(&self) -> impl Iterator<Item = &io::Error> {
        self.native.create_errors()
    }
    /// Original actual native IO error, if any.
    pub fn io_error(&self) -> Option<&io::Error> {
        self.native.io_error()
    }
    /// Actual retained name conversion failure and its original bytes.
    pub fn name_error(&self) -> Option<&std::ffi::NulError> {
        self.native.name_error()
    }
    /// Actual native rotation names without the created/retired File handles.
    pub fn rotation_names(&self) -> impl Iterator<Item = &std::ffi::OsStr> {
        self.native.rotation_names()
    }
    /// Sequence/name and actual creation/publication facts, without Files.
    pub fn rotation_observations(
        &self,
    ) -> impl Iterator<Item = (u64, &std::ffi::OsStr, bool, bool)> {
        self.native.rotation_observations()
    }
    /// Actual native fallible container reservation error.
    pub fn allocation_error(&self) -> Option<&std::collections::TryReserveError> {
        self.native.allocation_error()
    }
    /// Actual native phase panic, retained verbatim. Composing callbacks can
    /// supply their own payloads; this exposes no producer File by construction.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.native.panic_payload()
    }
    /// Actual composing denial panic retained by the native producer.
    pub fn denial_panic(&self) -> Option<&(dyn Any + Send)> {
        self.native.denial_panic()
    }
    /// Actual settlement panic retained by the native producer.
    pub fn settlement_panic(&self) -> Option<&(dyn Any + Send)> {
        self.native.settlement_panic()
    }
    /// Actual independently retained settlement phase panics.
    pub fn settlement_phase_panics(&self) -> impl Iterator<Item = &(dyn Any + Send)> {
        self.native.settlement_phase_panics()
    }
    /// Actual native settlement failure category.
    pub fn settlement_failure(&self) -> Option<PinnedShardAppendFailure> {
        self.native.settlement_failure()
    }
    /// Scalar actual file-settlement progress, never a File borrow.
    pub fn pending_file_settlement(&self) -> Option<(u64, bool, bool, bool)> {
        self.native.pending_file_settlement()
    }
    /// Whether actual prepaid parent sync remains pending.
    pub fn pending_directory_sync(&self) -> bool {
        self.native.pending_directory_sync()
    }
}
impl fmt::Debug for OwnedPinnedShardAppendObservation<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedPinnedShardAppendObservation")
            .field("usage", &self.usage())
            .field("installed_frames", &self.installed_frames())
            .finish_non_exhaustive()
    }
}

/// Actual retained-input and native ceilings for one consuming append or seal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnedPinnedShardAppendLimits {
    /// Maximum actual input Vec capacity, independently of its length.
    pub max_input_capacity: usize,
    /// Genuine native cumulative frame, segment, rotation, IO and work limits.
    pub native: PinnedShardAppendLimits,
}

/// Plain checked logical Core/native ceilings, without admission or RSS claims.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnedPinnedShardAppendCost {
    /// Native maxima, retained input capacity/payload and named Core values.
    pub declared_bytes: usize,
    /// Native path/name/error subset already included in declared bytes.
    pub string_bytes: usize,
    /// Retained Core input payload ceiling, also included in declared bytes.
    pub payload_bytes: usize,
    /// Native configured slots plus Core values and actual input-capacity slots.
    pub entries: usize,
    /// Native maxima and Core setup/input scans, excluding Core validation.
    pub work_bytes: usize,
    /// Native IO/settlement ceilings; Core validation pays its own calls.
    pub native_call_slots: usize,
}

/// Actual terminal Core refusal; all original inputs and effects remain owned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnedPinnedShardAppendFailure {
    /// SAME cumulative counters, input ceilings or original/minimum E refused.
    Budget,
    /// Actual recursive Core parent/root validation refused.
    Directory,
    /// Actual composing work/health checkpoint refused.
    Checkpoint,
    /// Immediate borrowing of the SAME private pin refused or was poisoned.
    PinBorrow,
    /// Genuine native append/seal refused with its actual partial-effect report.
    Native,
    /// An actual bridge/checkpoint unwound; its original payload is retained.
    Unwind,
    /// A private carrier transition refused without reconstructing inputs.
    Invariant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BorrowFailure {
    WouldBlock,
    Poisoned,
}

struct Effects {
    native: Option<Result<PinnedShardAppendReport, PinnedShardAppendError>>,
    native_budget: Option<PinnedShardAppendBudget>,
    inputs: Option<Vec<PinnedShardAppendInput>>,
    input_capacity: usize,
    measured_payload_bytes: usize,
    directory_error: Option<RetainedDirectoryError>,
    budget_error: Option<NativeDirectoryFailure>,
    checkpoint_error: Option<ShardDirectoryCheckFailure>,
    panic_payload: Option<Box<dyn Any + Send>>,
    deny_panic_payload: Option<Box<dyn Any + Send>>,
    denial_completed: bool,
    pin_borrowed: bool,
    borrow_failure: Option<BorrowFailure>,
}
impl Effects {
    fn new(inputs: Vec<PinnedShardAppendInput>) -> Self {
        let input_capacity = inputs.capacity();
        Self {
            native: None,
            native_budget: None,
            inputs: Some(inputs),
            input_capacity,
            measured_payload_bytes: 0,
            directory_error: None,
            budget_error: None,
            checkpoint_error: None,
            panic_payload: None,
            deny_panic_payload: None,
            denial_completed: false,
            pin_borrowed: false,
            borrow_failure: None,
        }
    }
    fn check(&self, checks: &Checks<'_>) -> Result<(), OwnedPinnedShardAppendFailure> {
        checks.check().map_err(|_| {
            if checks.budget_error.borrow().is_some() {
                OwnedPinnedShardAppendFailure::Budget
            } else {
                OwnedPinnedShardAppendFailure::Checkpoint
            }
        })
    }
    fn validate(
        &mut self,
        parent: &OwnedDirectoryHandle,
        budget: &NativeDirectoryBudget,
    ) -> Result<(), OwnedPinnedShardAppendFailure> {
        match parent.validate_until(budget) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.directory_error = Some(error);
                Err(OwnedPinnedShardAppendFailure::Directory)
            }
        }
    }
    fn report(&self) -> Option<&PinnedShardAppendReport> {
        match self.native.as_ref()? {
            Ok(report) => Some(report),
            Err(error) => Some(error.report()),
        }
    }
}

struct Checks<'a> {
    budget: &'a NativeDirectoryBudget,
    outer: &'a dyn PinnedShardAppendChecks,
    budget_error: RefCell<Option<NativeDirectoryFailure>>,
    checkpoint_error: Cell<Option<ShardDirectoryCheckFailure>>,
    denial_attempted: Cell<bool>,
    denial_completed: Cell<bool>,
}
impl Checks<'_> {
    fn check_budget(&self) -> Result<(), ShardDirectoryCheckFailure> {
        match self.budget.check() {
            Ok(()) => Ok(()),
            Err(error) => {
                let mut saved = self.budget_error.borrow_mut();
                if saved.is_none() {
                    *saved = Some(error);
                }
                Err(ShardDirectoryCheckFailure::Refused)
            }
        }
    }
}
impl ShardDirectoryChecks for Checks<'_> {
    fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
        self.check_budget()?;
        let observed = self.outer.check();
        if let Err(error) = observed
            && self.checkpoint_error.get().is_none()
        {
            self.checkpoint_error.set(Some(error));
        }
        self.check_budget()?;
        observed
    }
}
impl PinnedShardAppendChecks for Checks<'_> {
    fn deny(&self) {
        // This executes inside native failure handling BEFORE its prepaid
        // rollback/file/directory settlement, while the actual pin is held.
        self.budget.deny();
        if !self.denial_attempted.replace(true) {
            self.outer.deny();
            self.denial_completed.set(true);
        }
    }
}

/// Actual completed append/seal report with the whole Core owner retained last.
pub struct OwnedPinnedShardAppendComplete {
    report: PinnedShardAppendReport,
    store: OwnedPinnedShardStore,
}
/// Actual native report retaining its SAME pin and original Core root last.
pub struct OwnedPinnedShardAppendReport {
    report: PinnedShardAppendReport,
    directory: OwnedShardStoreDirectory,
}
impl OwnedPinnedShardAppendReport {
    /// Borrow original raw/typed native results; no native ownership extraction.
    pub fn report(&self) -> OwnedPinnedShardAppendObservation<'_> {
        OwnedPinnedShardAppendObservation {
            native: &self.report,
        }
    }
    /// SAME original physical root allocation.
    pub fn owner(&self) -> &Arc<DirectoryOwnership> {
        self.directory.owner()
    }
}
impl fmt::Debug for OwnedPinnedShardAppendReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedPinnedShardAppendReport")
            .field("report", &self.report)
            .field("directory", &self.directory)
            .finish_non_exhaustive()
    }
}
impl OwnedPinnedShardAppendComplete {
    /// Original native report, including actual inputs and physical results.
    pub fn report(&self) -> OwnedPinnedShardAppendObservation<'_> {
        OwnedPinnedShardAppendObservation {
            native: &self.report,
        }
    }
    /// Borrow the whole genuine Core owner after native settlement.
    pub fn store(&self) -> &OwnedPinnedShardStore {
        &self.store
    }
    /// Drop the report first and return the WHOLE Core store, never bare parts.
    pub fn into_store(self) -> OwnedPinnedShardStore {
        let Self { report, store } = self;
        drop(report);
        store
    }
    /// Separate whole store and opaque report retaining SAME directory last.
    /// One Arc increment is prepaid by this operation's ordinary cost.
    pub fn into_store_and_report(self) -> (OwnedPinnedShardStore, OwnedPinnedShardAppendReport) {
        let Self { report, store } = self;
        let directory = store.retain_directory();
        (store, OwnedPinnedShardAppendReport { report, directory })
    }
}
impl fmt::Debug for OwnedPinnedShardAppendComplete {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedPinnedShardAppendComplete")
            .field("report", &self.report)
            .finish_non_exhaustive()
    }
}

/// Refusal owns original input/results and the whole possibly uncertain store.
/// No retry, store, pin, descriptor or raw-report extraction is provided.
pub struct RetainedOwnedPinnedShardAppendError {
    failure: OwnedPinnedShardAppendFailure,
    effects: Effects,
    // Last: every input/native result/panic drops before whole store/root.
    store: OwnedPinnedShardStore,
}
impl RetainedOwnedPinnedShardAppendError {
    /// Actual terminal Core category.
    pub fn failure(&self) -> OwnedPinnedShardAppendFailure {
        self.failure
    }
    /// Actual first SAME-budget refusal, including a later minimum E.
    pub fn budget_error(&self) -> Option<&NativeDirectoryFailure> {
        self.effects.budget_error.as_ref()
    }
    /// Actual recursive Core validation refusal and its original effects.
    pub fn directory_error(&self) -> Option<&RetainedDirectoryError> {
        self.effects.directory_error.as_ref()
    }
    /// Actual returned composing work/health refusal.
    pub fn checkpoint_error(&self) -> Option<ShardDirectoryCheckFailure> {
        self.effects.checkpoint_error
    }
    /// Actual native failure category; the full FD-bearing error stays private.
    pub fn native_failure(&self) -> Option<PinnedShardAppendFailure> {
        self.effects
            .native
            .as_ref()?
            .as_ref()
            .err()
            .map(PinnedShardAppendError::failure)
    }
    /// Safe original observations on native success or refusal, without Files.
    pub fn native_report(&self) -> Option<OwnedPinnedShardAppendObservation<'_>> {
        self.effects
            .report()
            .map(|native| OwnedPinnedShardAppendObservation { native })
    }
    /// Original actual input slice, whether retained early or in native report.
    pub fn inputs(&self) -> &[PinnedShardAppendInput] {
        if let Some(inputs) = &self.effects.inputs {
            inputs
        } else {
            match self.effects.report() {
                Some(report) => report.inputs(),
                None => &[],
            }
        }
    }
    /// Actual original Vec capacity, independently of length.
    pub fn input_capacity(&self) -> usize {
        self.effects.input_capacity
    }
    /// Actual native cumulative budget usage, never refreshed per segment.
    pub fn native_usage(&self) -> Option<PinnedShardAppendUsage> {
        self.effects
            .native_budget
            .as_ref()
            .map(PinnedShardAppendBudget::usage)
    }
    /// Actual native original/minimum E, if its budget was entered.
    pub fn native_expiry(&self) -> Option<Instant> {
        self.effects
            .native_budget
            .as_ref()
            .map(PinnedShardAppendBudget::absolute_expiry)
    }
    /// Actual bridge unwind; native phase payloads remain in native_report().
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.effects.panic_payload.as_deref()
    }
    /// Actual independent bridge-denial unwind.
    pub fn deny_panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.effects.deny_panic_payload.as_deref()
    }
    /// Whether mandatory composing denial actually returned normally.
    pub fn denial_completed(&self) -> bool {
        self.effects.denial_completed
    }
    /// Whether the actual private pin was successfully borrowed.
    pub fn pin_borrowed(&self) -> bool {
        self.effects.pin_borrowed
    }
    /// The actual immediate pin borrow returned WouldBlock.
    pub fn pin_would_block(&self) -> bool {
        self.effects.borrow_failure == Some(BorrowFailure::WouldBlock)
    }
    /// The actual pin mutex returned its poisoned result.
    pub fn pin_poisoned(&self) -> bool {
        self.effects.borrow_failure == Some(BorrowFailure::Poisoned)
    }
    /// SAME original physical root held with the whole store until final drop.
    pub fn owner(&self) -> &Arc<DirectoryOwnership> {
        self.store.owner()
    }
}
impl fmt::Debug for RetainedOwnedPinnedShardAppendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedOwnedPinnedShardAppendError")
            .field("failure", &self.failure)
            .field("native_report", &self.native_report())
            .field("input_capacity", &self.input_capacity())
            .field("pin_borrowed", &self.pin_borrowed())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedOwnedPinnedShardAppendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "owned pinned append refused: {:?}", self.failure)
    }
}
impl Error for RetainedOwnedPinnedShardAppendError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Never return PinnedShardAppendError as dyn Error: downcasting that
        // wrapper would expose its raw native report and producer File borrows.
        if let Some(error) = self
            .effects
            .report()
            .and_then(PinnedShardAppendReport::native_error)
        {
            return Some(error);
        }
        if let Some(error) = self
            .effects
            .report()
            .and_then(PinnedShardAppendReport::io_error)
        {
            return Some(error);
        }
        if let Some(error) = self.directory_error() {
            return Some(error);
        }
        self.budget_error().map(|error| error as &dyn Error)
    }
}

/// Checked pre-entry logical ceiling over actual configured native maxima.
///
/// This includes the Core retained Vec capacity and original payload ceiling
/// separately from native prepared header/receipt/error/result capacities. The
/// consuming call checks actual capacity before scanning and uses that capacity
/// (never len) for its retained-input fee. It prepays the full payload ceiling
/// before inspecting each actual payload length. Recursive parent validation
/// additionally charges its real phases on the SAME directory budget.
/// Hidden Bytes allocator/alias capacity and RSS are not observable guarantees.
pub fn owned_pinned_shard_append_cost(
    limits: OwnedPinnedShardAppendLimits,
) -> Option<OwnedPinnedShardAppendCost> {
    let native = pinned_shard_append_cost(limits.native)?;
    let values = [
        size_of::<Effects>(),
        size_of::<OwnedPinnedShardAppendComplete>(),
        size_of::<OwnedPinnedShardAppendReport>(),
        size_of::<OwnedShardStoreDirectory>(),
        size_of::<RetainedOwnedPinnedShardAppendError>(),
        size_of::<OwnedPinnedShardAppendCost>(),
        size_of::<Checks<'static>>(),
        size_of::<PinBorrow<'static>>(),
        size_of::<Result<PinnedShardAppendReport, PinnedShardAppendError>>(),
        size_of::<PinnedShardAppendBudget>(),
        size_of::<OwnedPinnedShardAppendLimits>(),
        size_of::<OwnedPinnedShardAppendObservation<'static>>(),
        size_of::<Box<dyn Any + Send>>(),
        size_of::<Box<dyn Any + Send>>(),
    ];
    let fixed = values.into_iter().try_fold(0usize, usize::checked_add)?;
    let input_slots = limits
        .max_input_capacity
        .checked_mul(size_of::<PinnedShardAppendInput>())?;
    let retained = input_slots.checked_add(limits.native.max_payload_bytes)?;
    let entries = native
        .entries
        .checked_add(values.len())?
        .checked_add(limits.max_input_capacity)?;
    Some(OwnedPinnedShardAppendCost {
        declared_bytes: native
            .declared_bytes
            .checked_add(fixed)?
            .checked_add(retained)?,
        string_bytes: native.string_bytes,
        payload_bytes: native
            .payload_bytes
            .checked_add(limits.native.max_payload_bytes)?,
        entries,
        work_bytes: native
            .work_bytes
            .checked_add(fixed)?
            .checked_add(retained)?
            .checked_add(values.len())?
            .checked_add(limits.max_input_capacity)?
            .checked_add(1)?,
        native_call_slots: limits.native.max_io_calls,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    InputsAdmitted,
    ParentValidated,
    PinBorrowed,
    NativeCompleted,
    FinalParentValidated,
}
fn completed(phase: Phase) {
    #[cfg(test)]
    tests::completed(phase);
    #[cfg(not(test))]
    let _ = phase;
}

#[allow(
    clippy::result_large_err,
    reason = "keep actual input Vec and whole Core owner without pre-entry allocation"
)]
impl OwnedPinnedShardStore {
    /// Consume this whole store through genuine native grouped append/rotation.
    ///
    /// Inputs are retained before any cost/check/catch. Actual Vec capacity is
    /// capped and prepaid independently of length; native configured maxima and
    /// every segment share one native budget and SAME directory counters/minE.
    /// `checks.deny()` must deny the SAME actual composing work/captured health
    /// before native rollback/flush/sync settlement. No callback, Session or
    /// permanently expired startup budget is retained in a successful store.
    ///
    /// # Errors
    /// Retains original inputs, partial native results and the WHOLE uncertain
    /// store/root. There is no retry or bare owner/report/descriptor extraction.
    pub fn append_group_until(
        self,
        inputs: Vec<PinnedShardAppendInput>,
        sync: bool,
        limits: OwnedPinnedShardAppendLimits,
        budget: &NativeDirectoryBudget,
        checks: &dyn PinnedShardAppendChecks,
    ) -> Result<OwnedPinnedShardAppendComplete, RetainedOwnedPinnedShardAppendError> {
        self.append_inner(inputs, sync, false, limits, budget, checks)
    }
    /// Consume this whole store through the same genuine native seal path.
    /// Empty native packs retain the native no-op semantics and actual report.
    ///
    /// # Errors
    /// Retains the whole store and actual late/error/unwind/native outcomes.
    pub fn seal_active_until(
        self,
        limits: OwnedPinnedShardAppendLimits,
        budget: &NativeDirectoryBudget,
        checks: &dyn PinnedShardAppendChecks,
    ) -> Result<OwnedPinnedShardAppendComplete, RetainedOwnedPinnedShardAppendError> {
        self.append_inner(Vec::new(), false, true, limits, budget, checks)
    }
    fn append_inner(
        self,
        inputs: Vec<PinnedShardAppendInput>,
        sync: bool,
        seal: bool,
        limits: OwnedPinnedShardAppendLimits,
        budget: &NativeDirectoryBudget,
        outer: &dyn PinnedShardAppendChecks,
    ) -> Result<OwnedPinnedShardAppendComplete, RetainedOwnedPinnedShardAppendError> {
        // Both actual inputs are OUTSIDE every catch and every fallible phase.
        let mut store = self;
        let mut effects = Effects::new(inputs);
        let admitted = (|| {
            budget.check()?;
            if effects.input_capacity > limits.max_input_capacity
                || effects.inputs.as_ref().map_or(0, Vec::len) > limits.native.max_frames
                || limits.native.max_path_bytes > budget.limits().max_path_bytes
            {
                return Err(NativeDirectoryFailure::Capacity);
            }
            let cost = owned_pinned_shard_append_cost(OwnedPinnedShardAppendLimits {
                max_input_capacity: effects.input_capacity,
                native: limits.native,
            })
            .ok_or(NativeDirectoryFailure::Capacity)?;
            budget.charge(
                1,
                cost.native_call_slots,
                cost.declared_bytes,
                cost.string_bytes,
                cost.entries,
                cost.work_bytes,
            )?;
            budget.check()
        })();
        if let Err(error) = admitted {
            effects.budget_error = Some(error);
            budget.deny();
            let denied = catch_unwind(AssertUnwindSafe(|| outer.deny()));
            effects.denial_completed = denied.is_ok();
            effects.deny_panic_payload = denied.err();
            return Err(RetainedOwnedPinnedShardAppendError {
                failure: OwnedPinnedShardAppendFailure::Budget,
                effects,
                store,
            });
        }
        let checks = Checks {
            budget,
            outer,
            budget_error: RefCell::new(None),
            checkpoint_error: Cell::new(None),
            denial_attempted: Cell::new(false),
            denial_completed: Cell::new(false),
        };
        let observed = {
            let shared = &store.directory.inner;
            // Actual typed result/guard survives unwinds through denial and
            // all native prepaid settlement; never reconstruct a pin on error.
            let mut borrowed: PinBorrow<'_> = None;
            let observed = catch_unwind(AssertUnwindSafe(|| {
                completed(Phase::InputsAdmitted);
                effects.check(&checks)?;
                for input in effects
                    .inputs
                    .as_ref()
                    .ok_or(OwnedPinnedShardAppendFailure::Invariant)?
                {
                    effects.check(&checks)?;
                    let next = effects
                        .measured_payload_bytes
                        .checked_add(input.payload.len());
                    let Some(next) = next.filter(|n| *n <= limits.native.max_payload_bytes) else {
                        *checks.budget_error.borrow_mut() = Some(NativeDirectoryFailure::Capacity);
                        return Err(OwnedPinnedShardAppendFailure::Budget);
                    };
                    effects.measured_payload_bytes = next;
                    effects.check(&checks)?;
                }
                effects.native_budget =
                    PinnedShardAppendBudget::new(limits.native, budget.absolute_expiry());
                effects.check(&checks)?;
                if effects.native_budget.is_none() {
                    return Err(OwnedPinnedShardAppendFailure::Invariant);
                }
                effects.validate(&shared.parent, budget)?;
                completed(Phase::ParentValidated);
                effects.check(&checks)?;
                borrowed = Some(shared.native.try_lock());
                match borrowed.as_ref() {
                    Some(Ok(_)) => effects.pin_borrowed = true,
                    Some(Err(TryLockError::WouldBlock)) => {
                        effects.borrow_failure = Some(BorrowFailure::WouldBlock)
                    }
                    Some(Err(TryLockError::Poisoned(_))) => {
                        effects.borrow_failure = Some(BorrowFailure::Poisoned)
                    }
                    None => return Err(OwnedPinnedShardAppendFailure::Invariant),
                }
                completed(Phase::PinBorrowed);
                effects.check(&checks)?;
                let pin = match borrowed.as_ref() {
                    Some(Ok(pin)) => pin,
                    Some(Err(_)) => return Err(OwnedPinnedShardAppendFailure::PinBorrow),
                    None => return Err(OwnedPinnedShardAppendFailure::Invariant),
                };
                let native_budget = effects
                    .native_budget
                    .as_mut()
                    .ok_or(OwnedPinnedShardAppendFailure::Invariant)?;
                native_budget.cap_expiry(budget.absolute_expiry());
                if seal {
                    effects.native = Some(store.opened.store_mut().seal_active_pinned_until(
                        &pin.native,
                        native_budget,
                        &checks,
                    ));
                } else {
                    let inputs = effects
                        .inputs
                        .take()
                        .ok_or(OwnedPinnedShardAppendFailure::Invariant)?;
                    effects.native = Some(store.opened.store_mut().append_group_pinned_until(
                        &pin.native,
                        inputs,
                        sync,
                        native_budget,
                        &checks,
                    ));
                }
                completed(Phase::NativeCompleted);
                let checkpoint = effects.check(&checks);
                if matches!(effects.native.as_ref(), Some(Err(_))) {
                    return Err(OwnedPinnedShardAppendFailure::Native);
                }
                checkpoint?;
                if !matches!(effects.native.as_ref(), Some(Ok(_))) {
                    return Err(OwnedPinnedShardAppendFailure::Invariant);
                }
                effects.validate(&shared.parent, budget)?;
                completed(Phase::FinalParentValidated);
                effects.check(&checks)
            }));
            let observed = match observed {
                Ok(result) => result,
                Err(payload) => {
                    effects.panic_payload = Some(payload);
                    Err(OwnedPinnedShardAppendFailure::Unwind)
                }
            };
            if observed.is_err()
                && let Err(payload) = catch_unwind(AssertUnwindSafe(|| checks.deny()))
            {
                effects.deny_panic_payload = Some(payload);
            }
            drop(borrowed);
            observed
        };
        let failure = match observed {
            Ok(()) => match effects.native.take() {
                Some(Ok(report)) => return Ok(OwnedPinnedShardAppendComplete { report, store }),
                native => {
                    effects.native = native;
                    OwnedPinnedShardAppendFailure::Invariant
                }
            },
            Err(failure) => failure,
        };
        if !checks.denial_attempted.get()
            && let Err(payload) = catch_unwind(AssertUnwindSafe(|| checks.deny()))
        {
            effects.deny_panic_payload = Some(payload);
        }
        if effects.budget_error.is_none() {
            effects.budget_error = checks.budget_error.into_inner();
        }
        effects.checkpoint_error = checks.checkpoint_error.get();
        effects.denial_completed = checks.denial_completed.get();
        Err(RetainedOwnedPinnedShardAppendError {
            failure,
            effects,
            store,
        })
    }
}

#[cfg(test)]
#[path = "owned_pinned_shard_append_tests.rs"]
mod tests;
