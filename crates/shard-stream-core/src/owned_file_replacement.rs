//! Bounded same-parent atomic replacement retaining actual files and outcomes.
//!
//! The shared directory budget covers native validation and every physical
//! phase. A successful rename creates a prepaid directory-sync obligation;
//! settling it never revives the deadline or grants publication permission.

use super::*;
use crate::{NativeDirectoryBudget, NativeDirectoryFailure};
use rustix::fs::renameat;
use std::any::Any;
use std::collections::TryReserveError;
use std::error::Error;
use std::fmt;
use std::io::Write;
use std::mem::size_of;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Finite work allowed for one actual replacement, in addition to shared fees.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeFileReplacementLimits {
    /// Maximum retained encoded body capacity, checked before inspecting it.
    pub max_body_bytes: usize,
    /// Maximum actual write attempts, including Interrupted and short writes.
    pub max_write_calls: usize,
    /// Maximum bytes requested by any one physical write.
    pub max_write_bytes: usize,
}

/// Plain producer allocation ceiling; no admission or filesystem authority.
///
/// Recursive native open/validation fees are charged separately to the SAME
/// directory budget. The retained input body is included, even though ownership
/// of that existing allocation is transferred rather than copied here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeFileReplacementCost {
    /// Producer values, names, encoded body and configured raw outcome slots.
    pub declared_bytes: usize,
    /// Explicit name buffers and native rename conversions, within declared.
    pub string_bytes: usize,
    /// Producer values, names and configured raw outcome slots.
    pub entries: usize,
    /// Configured actual write attempts plus six fixed physical phases.
    pub call_slots: usize,
}

/// Actual physical phase, recorded before observing the clock afterwards.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeFileReplacementPhase {
    /// Metadata from the actual newly opened temporary descriptor.
    TemporaryMetadata,
    /// A bounded physical write, including partial and Interrupted outcomes.
    Write,
    /// Flush on the actual temporary file.
    Flush,
    /// Data synchronization of the actual temporary file.
    SyncData,
    /// Descriptor-relative replacement in the SAME retained parent.
    Rename,
    /// Physical settlement of the prepaid parent-directory synchronization.
    SyncDirectory,
    /// Metadata from the same actual descriptor after replacement.
    FinalMetadata,
}

/// Unmodified result of an actual native call; errors are never reconstructed.
#[derive(Debug)]
pub enum NativeFileReplacementOutcome {
    /// Actual write range and native returned count/error.
    Write {
        /// Actual offset within the retained encoded body.
        offset: usize,
        /// Bytes requested by this call.
        requested: usize,
        /// Actual physical write result.
        result: io::Result<usize>,
    },
    /// Actual flush, data sync, rename or directory sync result.
    Unit {
        /// The physical operation that produced this result.
        phase: NativeFileReplacementPhase,
        /// Actual native result.
        result: io::Result<()>,
    },
    /// Actual metadata result from the retained file descriptor.
    Metadata {
        /// The physical operation that produced this result.
        phase: NativeFileReplacementPhase,
        /// Actual native metadata/error, including its identity facts.
        result: io::Result<std::fs::Metadata>,
    },
}
impl NativeFileReplacementOutcome {
    /// Physical operation represented by this exact retained outcome.
    pub fn phase(&self) -> NativeFileReplacementPhase {
        match self {
            Self::Write { .. } => NativeFileReplacementPhase::Write,
            Self::Unit { phase, .. } | Self::Metadata { phase, .. } => *phase,
        }
    }
    /// Borrow the actual native error, including its original OS error code.
    pub fn io_error(&self) -> Option<&io::Error> {
        match self {
            Self::Write { result, .. } => result.as_ref().err(),
            Self::Unit { result, .. } => result.as_ref().err(),
            Self::Metadata { result, .. } => result.as_ref().err(),
        }
    }
}

/// Ordinary native inode facts; these grant no history or serving authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeFileIdentity {
    /// Actual descriptor's device number.
    pub device: u64,
    /// Actual descriptor's inode number.
    pub inode: u64,
}

/// Reason the actual replacement cannot be accepted as timely and complete.
#[derive(Debug)]
pub enum NativeFileReplacementFailure {
    /// Same shared counters, minimum E or previous refusal denied work.
    Budget(NativeDirectoryFailure),
    /// Literal names or checked finite producer limits refused before work.
    InvalidInput,
    /// A real fallible name/outcome allocation refused.
    Allocation(TryReserveError),
    /// A native open/identity validation refused; its actual error is retained.
    Directory,
    /// Index of the original failing physical result in `outcomes()`.
    Io(usize),
    /// Index of a real zero-byte write with an unfinished encoded body.
    WriteZero(usize),
    /// Actual descriptor identity did not match the same retained file.
    Identity,
    /// An actual native phase unwound; its payload and partial custody remain.
    Unwind,
}
impl fmt::Display for NativeFileReplacementFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Budget(error) => fmt::Display::fmt(error, f),
            Self::InvalidInput => f.write_str("native replacement input exceeds its limits"),
            Self::Allocation(error) => fmt::Display::fmt(error, f),
            Self::Directory => f.write_str("native replacement directory validation refused"),
            Self::Io(index) => write!(f, "native replacement I/O outcome {index} failed"),
            Self::WriteZero(index) => {
                write!(f, "native replacement write {index} made no progress")
            }
            Self::Identity => f.write_str("native replacement file identity changed"),
            Self::Unwind => f.write_str("native replacement unwound with retained custody"),
        }
    }
}
impl Error for NativeFileReplacementFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Budget(error) => Some(error),
            Self::Allocation(error) => Some(error),
            _ => None,
        }
    }
}
impl From<NativeDirectoryFailure> for NativeFileReplacementFailure {
    fn from(error: NativeDirectoryFailure) -> Self {
        Self::Budget(error)
    }
}
impl From<TryReserveError> for NativeFileReplacementFailure {
    fn from(error: TryReserveError) -> Self {
        Self::Allocation(error)
    }
}
type ReplacementResult<T> = std::result::Result<T, NativeFileReplacementFailure>;

struct Scope {
    file: Option<OwnedFileHandle>,
    directory_error: Option<RetainedDirectoryError>,
    outcomes: Vec<NativeFileReplacementOutcome>,
    written: usize,
    unfinished_phase: Option<NativeFileReplacementPhase>,
    unwind_phase: Option<NativeFileReplacementPhase>,
    rename_attempted: bool,
    renamed: bool,
    directory_sync_attempted: bool,
    directory_synced: bool,
    panic_payload: Option<Box<dyn Any + Send>>,
    settlement_panic_payload: Option<Box<dyn Any + Send>>,
    temporary_identity: Option<NativeFileIdentity>,
    final_identity: Option<NativeFileIdentity>,
    temporary: OsString,
    destination: OsString,
    descriptor_name: OsString,
    body: Vec<u8>,
    budget: NativeDirectoryBudget,
    // Last: real descriptor/results/body settle before original root custody.
    parent: OwnedDirectoryHandle,
}

/// Actual completed or partially completed replacement and its native custody.
///
/// No file/root extraction, serving permission or automatic unlink is provided.
/// The same encoded input, original names, descriptor and typed results survive
/// a late/error decision. Serialization and a caller's content/census checks
/// are separate operations, and must themselves share the original deadline.
pub struct NativeFileReplacement {
    scope: Box<Scope>,
}
impl fmt::Debug for NativeFileReplacement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeFileReplacement")
            .field("temporary", &self.scope.temporary)
            .field("destination", &self.scope.destination)
            .field("written", &self.scope.written)
            .field("unfinished_phase", &self.scope.unfinished_phase)
            .field("renamed", &self.scope.renamed)
            .field("directory_synced", &self.scope.directory_synced)
            .field("outcomes", &self.scope.outcomes)
            .finish_non_exhaustive()
    }
}
impl NativeFileReplacement {
    /// Exact encoded input retained without reconstruction.
    pub fn body(&self) -> &[u8] {
        &self.scope.body
    }
    /// Original literal temporary name, even after a successful rename.
    pub fn temporary_name(&self) -> &OsStr {
        &self.scope.temporary
    }
    /// Literal destination in the SAME actual parent.
    pub fn destination_name(&self) -> &OsStr {
        &self.scope.destination
    }
    /// Same actual file, when native opening completed its validation.
    pub fn file_handle(&self) -> Option<&OwnedFileHandle> {
        self.scope.file.as_ref()
    }
    /// Actual native open/validation refusal and any partial opened descriptor.
    pub fn directory_error(&self) -> Option<&RetainedDirectoryError> {
        self.scope.directory_error.as_ref()
    }
    /// Every entered write/flush/sync/rename/metadata result in native order.
    pub fn outcomes(&self) -> &[NativeFileReplacementOutcome] {
        &self.scope.outcomes
    }
    /// Bytes whose actual write returned success, including late writes.
    pub fn written_bytes(&self) -> usize {
        self.scope.written
    }
    /// Whether actual renameat returned success; not a timely success claim.
    pub fn renamed(&self) -> bool {
        self.scope.renamed
    }
    /// Whether the prepaid actual parent fsync returned success, even if late.
    pub fn directory_synced(&self) -> bool {
        self.scope.directory_synced
    }
    /// Actual temporary descriptor identity observed before writing.
    pub fn temporary_identity(&self) -> Option<NativeFileIdentity> {
        self.scope.temporary_identity
    }
    /// Actual same descriptor identity observed after timely namespace settlement.
    pub fn final_identity(&self) -> Option<NativeFileIdentity> {
        self.scope.final_identity
    }
    /// SAME original actual root owner; no reconstruction or authority.
    pub fn owner(&self) -> &Arc<DirectoryOwnership> {
        self.scope.parent.owner()
    }
    /// SAME ordinary cumulative counters and minimum E, without new authority.
    pub fn budget(&self) -> &NativeDirectoryBudget {
        &self.scope.budget
    }
    /// Prepaid physical phase for which no returned native result was saved.
    ///
    /// This records uncertainty after unwind; it does not fabricate an outcome.
    pub fn unfinished_phase(&self) -> Option<NativeFileReplacementPhase> {
        self.scope.unfinished_phase
    }
    /// Original unfinished phase saved before any unwind settlement began.
    /// A completed cleanup does not erase an uncertain original rename.
    pub fn unwind_phase(&self) -> Option<NativeFileReplacementPhase> {
        self.scope.unwind_phase
    }
    /// Whether the already-prepaid parent synchronization was attempted.
    /// An unwind during that attempt never causes a second physical sync.
    pub fn directory_sync_attempted(&self) -> bool {
        self.scope.directory_sync_attempted
    }
    /// Original actual unwind payload, without formatting or reconstruction.
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.scope.panic_payload.as_deref()
    }
    /// Actual additional cleanup unwind, if physical settlement itself unwound.
    pub fn settlement_panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.scope.settlement_panic_payload.as_deref()
    }
}

enum Custody {
    Early {
        body: Vec<u8>,
        budget: NativeDirectoryBudget,
        parent: OwnedDirectoryHandle,
    },
    Entered(NativeFileReplacement),
}
/// Actual refusal retaining raw input and every entered native resource/result.
pub struct RetainedNativeFileReplacementError {
    failure: NativeFileReplacementFailure,
    custody: Custody,
}
impl fmt::Debug for RetainedNativeFileReplacementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedNativeFileReplacementError")
            .field("failure", &self.failure)
            .field("replacement", &self.replacement())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedNativeFileReplacementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.failure, f)
    }
}
impl Error for RetainedNativeFileReplacementError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(replacement) = self.replacement() {
            if let Some(error) = replacement.directory_error() {
                return Some(error);
            }
            if let Some(error) = replacement
                .outcomes()
                .iter()
                .rev()
                .find_map(NativeFileReplacementOutcome::io_error)
            {
                return Some(error);
            }
        }
        Some(&self.failure)
    }
}
impl RetainedNativeFileReplacementError {
    /// Actual refusal, with raw native errors retained separately in custody.
    pub fn failure(&self) -> &NativeFileReplacementFailure {
        &self.failure
    }
    /// Entered producer and its actual outcomes; absent for pre-entry refusal.
    pub fn replacement(&self) -> Option<&NativeFileReplacement> {
        match &self.custody {
            Custody::Early { .. } => None,
            Custody::Entered(value) => Some(value),
        }
    }
    /// Exact original encoded body, including an already-expired entry.
    pub fn body(&self) -> &[u8] {
        match &self.custody {
            Custody::Early { body, .. } => body,
            Custody::Entered(value) => value.body(),
        }
    }
    /// SAME original root retained until all actual producer custody drops.
    pub fn owner(&self) -> &Arc<DirectoryOwnership> {
        match &self.custody {
            Custody::Early { parent, .. } => parent.owner(),
            Custody::Entered(value) => value.owner(),
        }
    }
    /// SAME ordinary counters, including entry refusal and all retained fees.
    pub fn budget(&self) -> &NativeDirectoryBudget {
        match &self.custody {
            Custody::Early { budget, .. } => budget,
            Custody::Entered(value) => value.budget(),
        }
    }
}

/// Checked producer allocation facts before entry, excluding shared validation.
pub fn native_file_replacement_cost(
    temporary: &OsStr,
    destination: &OsStr,
    body_capacity: usize,
    limits: NativeFileReplacementLimits,
) -> Option<NativeFileReplacementCost> {
    if body_capacity > limits.max_body_bytes
        || limits.max_write_calls == 0
        || limits.max_write_bytes == 0
        || temporary == destination
    {
        return None;
    }
    if [temporary, destination].into_iter().any(|name| {
        component(name).is_err() || name.as_bytes().contains(&0) || name.as_bytes().contains(&b'/')
    }) {
        return None;
    }
    let call_slots = limits.max_write_calls.checked_add(6)?;
    // Three retained names plus two bounded native rename path conversions.
    let strings = temporary
        .as_bytes()
        .len()
        .checked_add(1)?
        .checked_mul(2)?
        .checked_add(
            destination
                .as_bytes()
                .len()
                .checked_add(1)?
                .checked_mul(3)?,
        )?;
    let declared = size_of::<Scope>()
        .checked_add(size_of::<NativeFileReplacement>())?
        .checked_add(size_of::<RetainedNativeFileReplacementError>())?
        .checked_add(strings)?
        .checked_add(body_capacity)?
        .checked_add(call_slots.checked_mul(size_of::<NativeFileReplacementOutcome>())?)?;
    Some(NativeFileReplacementCost {
        declared_bytes: declared,
        string_bytes: strings,
        // Producer/name/input values plus the two retained unwind payload slots.
        entries: call_slots.checked_add(10)?,
        call_slots,
    })
}

fn copy_name(target: &mut OsString, source: &OsStr) -> ReplacementResult<()> {
    target.try_reserve_exact(source.as_bytes().len())?;
    target.push(source);
    Ok(())
}
fn completed(phase: NativeFileReplacementPhase) {
    #[cfg(test)]
    tests::completed(phase);
    #[cfg(not(test))]
    let _ = phase;
}
impl Scope {
    fn file(&self) -> ReplacementResult<&OwnedFileHandle> {
        self.file
            .as_ref()
            .ok_or(NativeFileReplacementFailure::Identity)
    }
    fn admit_call(&self, work: usize) -> ReplacementResult<()> {
        self.budget.charge(0, 1, 0, 0, 0, work)?;
        self.budget.check()?;
        Ok(())
    }
    fn unit(
        &mut self,
        phase: NativeFileReplacementPhase,
        observed: io::Result<()>,
    ) -> ReplacementResult<()> {
        let failed = observed.is_err();
        let index = self.outcomes.len();
        self.outcomes.push(NativeFileReplacementOutcome::Unit {
            phase,
            result: observed,
        });
        self.unfinished_phase = None;
        completed(phase);
        self.budget.check()?;
        if failed {
            return Err(NativeFileReplacementFailure::Io(index));
        }
        Ok(())
    }
    fn metadata(
        &mut self,
        phase: NativeFileReplacementPhase,
    ) -> ReplacementResult<NativeFileIdentity> {
        self.admit_call(size_of::<std::fs::Metadata>())?;
        self.unfinished_phase = Some(phase);
        let observed = self.file()?.file.metadata();
        let identity = observed
            .as_ref()
            .ok()
            .filter(|metadata| metadata.is_file())
            .map(|metadata| NativeFileIdentity {
                device: metadata.dev(),
                inode: metadata.ino(),
            });
        let index = self.outcomes.len();
        let failed = observed.is_err();
        self.outcomes.push(NativeFileReplacementOutcome::Metadata {
            phase,
            result: observed,
        });
        self.unfinished_phase = None;
        // Keep actual identity facts before the first post-call clock check.
        match phase {
            NativeFileReplacementPhase::TemporaryMetadata => self.temporary_identity = identity,
            NativeFileReplacementPhase::FinalMetadata => self.final_identity = identity,
            _ => {}
        }
        completed(phase);
        self.budget.check()?;
        if failed {
            return Err(NativeFileReplacementFailure::Io(index));
        }
        identity.ok_or(NativeFileReplacementFailure::Identity)
    }
    fn validate_file(&mut self) -> ReplacementResult<()> {
        let observed = self.file()?.validate_until(&self.budget);
        if let Err(error) = observed {
            self.directory_error = Some(error);
            return Err(NativeFileReplacementFailure::Directory);
        }
        self.budget.check()?;
        Ok(())
    }
    fn sync_parent(&mut self) -> usize {
        self.directory_sync_attempted = true;
        self.unfinished_phase = Some(NativeFileReplacementPhase::SyncDirectory);
        let observed = self.parent.inner.file.sync_all();
        self.directory_synced = observed.is_ok();
        let index = self.outcomes.len();
        self.outcomes.push(NativeFileReplacementOutcome::Unit {
            phase: NativeFileReplacementPhase::SyncDirectory,
            result: observed,
        });
        self.unfinished_phase = None;
        index
    }
    fn retain_unwind(&mut self, payload: Box<dyn Any + Send>) {
        // Deny FIRST, while this same outside carrier still holds every actual
        // descriptor/result/body. No native owner is dropped by catch_unwind.
        self.budget.deny();
        self.panic_payload = Some(payload);
        self.unwind_phase = self.unfinished_phase;
        if self.rename_attempted && !self.directory_sync_attempted {
            // Both slots were admitted before rename. Settle once without a
            // callback, new admission, deadline revival or retry. A rename
            // without a saved result is uncertain, never assumed unchanged.
            let settled = catch_unwind(AssertUnwindSafe(|| self.sync_parent()));
            if let Err(payload) = settled {
                self.settlement_panic_payload = Some(payload);
            }
        }
    }
    fn run(&mut self, limits: NativeFileReplacementLimits) -> ReplacementResult<()> {
        let observed =
            self.parent
                .open_file_until(&self.temporary, OwnedFileAccess::CreateNew, &self.budget);
        match observed {
            Ok(file) => self.file = Some(file),
            Err(error) => {
                self.directory_error = Some(error);
                return Err(NativeFileReplacementFailure::Directory);
            }
        }
        self.budget.check()?;
        self.metadata(NativeFileReplacementPhase::TemporaryMetadata)?;
        let mut calls = 0usize;
        while self.written < self.body.len() {
            if calls >= limits.max_write_calls {
                return Err(NativeDirectoryFailure::Capacity.into());
            }
            let requested = (self.body.len() - self.written).min(limits.max_write_bytes);
            self.admit_call(requested)?;
            let offset = self.written;
            self.unfinished_phase = Some(NativeFileReplacementPhase::Write);
            let observed = {
                let mut file = self.file()?.file();
                file.write(&self.body[offset..offset + requested])
            };
            let index = self.outcomes.len();
            if let Ok(written) = &observed {
                self.written += *written;
            }
            self.outcomes.push(NativeFileReplacementOutcome::Write {
                offset,
                requested,
                result: observed,
            });
            self.unfinished_phase = None;
            calls += 1;
            completed(NativeFileReplacementPhase::Write);
            self.budget.check()?;
            match &self.outcomes[index] {
                NativeFileReplacementOutcome::Write { result: Ok(0), .. } => {
                    return Err(NativeFileReplacementFailure::WriteZero(index));
                }
                NativeFileReplacementOutcome::Write {
                    result: Err(error), ..
                } if error.kind() == io::ErrorKind::Interrupted => {}
                NativeFileReplacementOutcome::Write { result: Err(_), .. } => {
                    return Err(NativeFileReplacementFailure::Io(index));
                }
                _ => {}
            }
        }
        self.admit_call(0)?;
        self.unfinished_phase = Some(NativeFileReplacementPhase::Flush);
        let observed = {
            let mut file = self.file()?.file();
            file.flush()
        };
        self.unit(NativeFileReplacementPhase::Flush, observed)?;
        self.admit_call(0)?;
        self.unfinished_phase = Some(NativeFileReplacementPhase::SyncData);
        let observed = self.file()?.file.sync_data();
        self.unit(NativeFileReplacementPhase::SyncData, observed)?;
        self.validate_file()?;
        // Prepay BOTH mutation and mandatory physical namespace settlement.
        let rename_work = self
            .temporary
            .as_bytes()
            .len()
            .checked_add(self.destination.as_bytes().len())
            .ok_or(NativeFileReplacementFailure::InvalidInput)?;
        self.budget.charge(0, 2, 0, 0, 0, rename_work)?;
        self.budget.check()?;
        self.rename_attempted = true;
        self.unfinished_phase = Some(NativeFileReplacementPhase::Rename);
        let observed = renameat(
            &self.parent.inner.file,
            &self.temporary,
            &self.parent.inner.file,
            &self.destination,
        )
        .map_err(io::Error::from);
        self.renamed = observed.is_ok();
        let rename_index = self.outcomes.len();
        self.outcomes.push(NativeFileReplacementOutcome::Unit {
            phase: NativeFileReplacementPhase::Rename,
            result: observed,
        });
        self.unfinished_phase = None;
        if self.renamed {
            let name = std::mem::take(&mut self.descriptor_name);
            if let Some(file) = &mut self.file {
                file.name = name;
            }
        }
        completed(NativeFileReplacementPhase::Rename);
        if !self.renamed {
            self.budget.check()?;
            return Err(NativeFileReplacementFailure::Io(rename_index));
        }
        // Physical obligation already admitted before rename. No intervening
        // clock/admission can abandon a successfully changed namespace.
        let sync_index = self.sync_parent();
        completed(NativeFileReplacementPhase::SyncDirectory);
        self.budget.check()?;
        if self.outcomes[sync_index].io_error().is_some() {
            return Err(NativeFileReplacementFailure::Io(sync_index));
        }
        self.validate_file()?;
        let identity = self.metadata(NativeFileReplacementPhase::FinalMetadata)?;
        if Some(identity) != self.temporary_identity {
            return Err(NativeFileReplacementFailure::Identity);
        }
        self.budget.check()?;
        Ok(())
    }
}

impl OwnedDirectoryHandle {
    /// Replace one literal file through a new temporary in this SAME parent.
    ///
    /// The caller supplies an already-encoded body and a bounded unique ordinary
    /// temporary name. No existing temporary is opened/truncated/unlinked. Each
    /// actual write, flush, sync and rename has a finite prepaid outcome slot;
    /// all native validation uses the SAME directory budget and minimum E.
    /// Once rename succeeds, its prepaid parent fsync settles even if rename
    /// returned late. A late/error outcome refuses success and retains actual
    /// namespace effects; there is no rollback or automatic deletion.
    ///
    /// # Errors
    /// Returns retained input/root custody on pre-entry refusal and the actual
    /// file, raw phase outcomes and partial effects after entry. Callers must
    /// retain this custody and deny publication on every error.
    pub fn replace_file_until(
        &self,
        temporary: &OsStr,
        destination: &OsStr,
        body: Vec<u8>,
        limits: NativeFileReplacementLimits,
        budget: &NativeDirectoryBudget,
    ) -> std::result::Result<NativeFileReplacement, RetainedNativeFileReplacementError> {
        let prepare = (|| {
            budget.check()?;
            if temporary.as_bytes().len() > budget.limits().max_component_bytes
                || destination.as_bytes().len() > budget.limits().max_component_bytes
            {
                return Err(NativeFileReplacementFailure::InvalidInput);
            }
            for name in [temporary, destination] {
                let path_bytes = self
                    .path()
                    .as_os_str()
                    .as_bytes()
                    .len()
                    .checked_add(1)
                    .and_then(|value| value.checked_add(name.as_bytes().len()))
                    .ok_or(NativeFileReplacementFailure::InvalidInput)?;
                if path_bytes > budget.limits().max_path_bytes {
                    return Err(NativeFileReplacementFailure::InvalidInput);
                }
            }
            let cost =
                native_file_replacement_cost(temporary, destination, body.capacity(), limits)
                    .ok_or(NativeFileReplacementFailure::InvalidInput)?;
            budget.charge(
                1,
                0,
                cost.declared_bytes,
                cost.string_bytes,
                cost.entries,
                cost.declared_bytes,
            )?;
            budget.check()?;
            Ok(cost)
        })();
        let cost = match prepare {
            Ok(cost) => cost,
            Err(failure) => {
                budget.deny();
                return Err(RetainedNativeFileReplacementError {
                    failure,
                    custody: Custody::Early {
                        body,
                        budget: budget.clone(),
                        parent: self.retain(),
                    },
                });
            }
        };
        let mut replacement = NativeFileReplacement {
            scope: Box::new(Scope {
                file: None,
                directory_error: None,
                outcomes: Vec::new(),
                written: 0,
                unfinished_phase: None,
                unwind_phase: None,
                rename_attempted: false,
                renamed: false,
                directory_sync_attempted: false,
                directory_synced: false,
                panic_payload: None,
                settlement_panic_payload: None,
                temporary_identity: None,
                final_identity: None,
                temporary: OsString::new(),
                destination: OsString::new(),
                descriptor_name: OsString::new(),
                body,
                budget: budget.clone(),
                parent: self.retain(),
            }),
        };
        // The actual carrier stays OUTSIDE the unwinding producer. Unwind may
        // change namespace state; it must never destroy retained custody or
        // bypass sticky denial and the prepaid settlement obligation.
        let observed = catch_unwind(AssertUnwindSafe(|| {
            budget.check()?;
            let scope = &mut replacement.scope;
            scope.outcomes.try_reserve_exact(cost.call_slots)?;
            budget.check()?;
            copy_name(&mut scope.temporary, temporary)?;
            budget.check()?;
            copy_name(&mut scope.destination, destination)?;
            budget.check()?;
            copy_name(&mut scope.descriptor_name, destination)?;
            budget.check()?;
            scope.run(limits)
        }));
        let observed = match observed {
            Ok(observed) => observed,
            Err(payload) => {
                replacement.scope.retain_unwind(payload);
                Err(NativeFileReplacementFailure::Unwind)
            }
        };
        match observed {
            Ok(()) => Ok(replacement),
            Err(failure) => {
                budget.deny();
                Err(RetainedNativeFileReplacementError {
                    failure,
                    custody: Custody::Entered(replacement),
                })
            }
        }
    }
}

#[cfg(test)]
#[path = "owned_file_replacement_tests.rs"]
mod tests;
