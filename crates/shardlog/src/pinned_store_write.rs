//! Private tracking of genuine retired ActivePack ownership after bounded writes.
//! No pathname open, FD clone, parts factory or public native report extraction.
use super::super::append::RetiredTransferFailure;
use super::*;
use crate::{
    PinnedShardAppendBudget, PinnedShardAppendReport, PinnedShardFetchChecks, StorageFetchBudget,
    StorageFetchFailure,
};
use std::os::unix::ffi::OsStringExt;

/// Immutable ceilings carried by the same actual opened owner across operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrackedWriteLimits {
    /// All native Files owned by this open, including lease/scan companions,
    /// sealed and retired packs, active and worst-case entered create results.
    /// The caller's external pin/Core parent Files have separate prepaid costs.
    pub max_live_files: usize,
    /// Original plus newly created actual tracked pack identities.
    pub max_tracked_packs: usize,
    /// Retained retired ActivePack slots; actual capacity is capped too.
    pub max_retired_slots: usize,
    /// Actual input Vec capacity, including excess caller-reserved slots.
    pub max_input_slots: usize,
}

pub(super) struct RetiredPack {
    pub(super) pack: Option<ActivePack>,
}
pub(super) struct TrackedWriteCatalog {
    limits: TrackedWriteLimits,
    pub(super) retired: Vec<RetiredPack>,
}
impl Drop for TrackedWriteCatalog {
    fn drop(&mut self) {
        // The real retained Vec/Files drop immediately after this body, before
        // the following original store/scope fields. No raw close status is
        // claimed. Only the genuine filesystem fixture observes this boundary.
        #[cfg(test)]
        crate::api::pinned::write::tests::catalog_drop_started();
    }
}
#[derive(Debug)]
pub(crate) enum TrackedWriteFailure {
    Budget(StorageFetchFailure),
    Capacity,
    Allocation,
    Stale,
    Identity,
    Transfer,
    Io,
    Invariant,
}
struct Transfer {
    source: usize,
    destination: usize,
    result: Option<Result<(), RetiredTransferFailure>>,
    sequence: Option<u64>,
    path: PathBuf,
    path_bytes: Vec<u8>,
    path_allocation: Option<Result<(), TryReserveError>>,
    name_bytes: Vec<u8>,
    name_allocation: Option<Result<(), TryReserveError>>,
    name: Option<CString>,
    name_error: Option<NulError>,
    held: Option<io::Result<Stat>>,
    named: Option<io::Result<Stat>>,
}
impl Transfer {
    fn new(source: usize, destination: usize) -> Self {
        Self {
            source,
            destination,
            result: None,
            sequence: None,
            path: PathBuf::new(),
            path_bytes: Vec::new(),
            path_allocation: None,
            name_bytes: Vec::new(),
            name_allocation: None,
            name: None,
            name_error: None,
            held: None,
            named: None,
        }
    }
}
/// Actual allocations, transfer Results, direct native stats and partial moves.
/// All Files remain in the actual report or same opened owner, never this view.
pub(crate) struct TrackedWriteObservation {
    allocations: [Option<Result<(), TryReserveError>>; 3],
    pin_observation: fetch::PinnedShardFetchObservation,
    pin_result: Option<Result<(), fetch::PinnedShardFetchFailure>>,
    transfers: Vec<Transfer>,
    active: Transfer,
    initial_live_files: Option<usize>,
    remaining_rotations: Option<usize>,
    destination_start: Option<usize>,
}
impl TrackedWriteObservation {
    pub(crate) fn new() -> Self {
        Self {
            allocations: [None, None, None],
            pin_observation: fetch::PinnedShardFetchObservation::new(),
            pin_result: None,
            transfers: Vec::new(),
            active: Transfer::new(0, 0),
            initial_live_files: None,
            remaining_rotations: None,
            destination_start: None,
        }
    }
    pub(crate) fn transfer_observations(
        &self,
    ) -> impl Iterator<Item = (usize, usize, Option<bool>)> + '_ {
        self.transfers.iter().map(|v| {
            (
                v.source,
                v.destination,
                v.result.as_ref().map(Result::is_ok),
            )
        })
    }
    pub(crate) fn transferred_count(&self) -> usize {
        self.transfers
            .iter()
            .filter(|v| matches!(v.result, Some(Ok(()))))
            .count()
    }
    pub(crate) fn stats(&self) -> impl Iterator<Item = &io::Result<Stat>> {
        self.transfers
            .iter()
            .chain(std::iter::once(&self.active))
            .flat_map(|v| [v.held.as_ref(), v.named.as_ref()])
            .flatten()
    }
    pub(crate) fn allocation_error(&self) -> Option<&TryReserveError> {
        self.allocations
            .iter()
            .filter_map(Option::as_ref)
            .find_map(|r| r.as_ref().err())
    }
}
fn add(a: usize, b: usize) -> Result<usize, TrackedWriteFailure> {
    a.checked_add(b).ok_or(TrackedWriteFailure::Capacity)
}
fn mul(a: usize, b: usize) -> Result<usize, TrackedWriteFailure> {
    a.checked_mul(b).ok_or(TrackedWriteFailure::Capacity)
}
fn pay(
    b: &StorageFetchBudget,
    bytes: usize,
    entries: usize,
    work: usize,
) -> Result<(), TrackedWriteFailure> {
    b.charge_values(bytes, entries, work)
        .map_err(TrackedWriteFailure::Budget)
}
fn check(
    b: &StorageFetchBudget,
    c: &dyn PinnedShardFetchChecks,
) -> Result<(), TrackedWriteFailure> {
    b.check().map_err(TrackedWriteFailure::Budget)?;
    // The composing error is Copy; the producer's caller also retains the
    // actual callback result in its borrowed adapter before returning here.
    let actual = c.check();
    let post = b.check();
    post.map_err(TrackedWriteFailure::Budget)?;
    actual.map_err(|_| TrackedWriteFailure::Stale)
}
/// Every separate value/header is explicit; a larger carrier is no slot fee.
pub(crate) fn tracking_fixed_fee() -> Option<(usize, usize)> {
    let values = [
        std::mem::size_of::<TrackedWriteCatalog>(),
        std::mem::size_of::<TrackedWriteObservation>(),
        std::mem::size_of::<Vec<RetiredPack>>(),
        std::mem::size_of::<Vec<Transfer>>(),
        std::mem::size_of::<RetiredPack>(),
        std::mem::size_of::<Transfer>(),
        std::mem::size_of::<Option<ActivePack>>(),
        std::mem::size_of::<Option<Result<(), RetiredTransferFailure>>>(),
        std::mem::size_of::<Option<Result<(), TryReserveError>>>(),
        std::mem::size_of::<Option<Result<(), TryReserveError>>>(),
        std::mem::size_of::<Option<Result<(), TryReserveError>>>(),
        std::mem::size_of::<Option<io::Result<Stat>>>(),
        std::mem::size_of::<Option<io::Result<Stat>>>(),
        std::mem::size_of::<PathBuf>(),
        std::mem::size_of::<CString>(),
        std::mem::size_of::<NulError>(),
        std::mem::size_of::<Option<NulError>>(),
        std::mem::size_of::<Result<CString, NulError>>(),
        std::mem::size_of::<TrackedWriteFailure>(),
        std::mem::size_of::<Option<Result<(), fetch::PinnedShardFetchFailure>>>(),
        std::mem::size_of::<ScannedPackSource>(),
        std::mem::size_of::<u64>(),
        std::mem::size_of::<u64>(),
        std::mem::size_of::<Vec<u8>>(),
        std::mem::size_of::<Vec<u8>>(),
        std::mem::size_of::<Option<Result<(), TryReserveError>>>(),
        std::mem::size_of::<Option<Result<(), TryReserveError>>>(),
    ];
    let native = fetch::fixed_fee().ok()?;
    Some((
        values.iter().try_fold(native.0, |n, v| n.checked_add(*v))?,
        values.len().checked_add(native.1)?,
    ))
}
// Per actual dynamic ledger entry: separate owning values/results, including
// both genuine path/name reservation outcomes and the descriptor source tag.
fn transfer_value_fee() -> Option<(usize, usize)> {
    let fields = [
        std::mem::size_of::<Transfer>(),
        std::mem::size_of::<RetiredPack>(),
        std::mem::size_of::<Option<ActivePack>>(),
        std::mem::size_of::<ScannedPack>(),
        std::mem::size_of::<ScannedPackSource>(),
        std::mem::size_of::<PathBuf>(),
        std::mem::size_of::<Vec<u8>>(),
        std::mem::size_of::<Vec<u8>>(),
        std::mem::size_of::<Option<Result<(), TryReserveError>>>(),
        std::mem::size_of::<Option<Result<(), TryReserveError>>>(),
        std::mem::size_of::<Option<Result<(), RetiredTransferFailure>>>(),
        std::mem::size_of::<Option<io::Result<Stat>>>(),
        std::mem::size_of::<Option<io::Result<Stat>>>(),
        std::mem::size_of::<Option<CString>>(),
        std::mem::size_of::<Option<NulError>>(),
        std::mem::size_of::<Result<CString, NulError>>(),
    ];
    Some((
        fields.iter().try_fold(0usize, |n, v| n.checked_add(*v))?,
        fields.len(),
    ))
}
fn reserve<T>(
    target: &mut Vec<T>,
    additional: usize,
    result: &mut Option<Result<(), TryReserveError>>,
    maximum: usize,
    b: &StorageFetchBudget,
    c: &dyn PinnedShardFetchChecks,
) -> Result<(), TrackedWriteFailure> {
    let wanted = add(target.len(), additional)?;
    if wanted > maximum {
        return Err(TrackedWriteFailure::Capacity);
    }
    let old = target.capacity();
    if old > maximum {
        return Err(TrackedWriteFailure::Capacity);
    }
    if wanted <= old {
        return check(b, c);
    }
    let requested = wanted - old;
    pay(
        b,
        mul(requested, std::mem::size_of::<T>())?,
        requested,
        mul(add(old, requested)?, std::mem::size_of::<T>())?,
    )?;
    check(b, c)?;
    *result = Some(target.try_reserve_exact(wanted - target.len()));
    check(b, c)?;
    if result.as_ref().is_some_and(Result::is_err) {
        return Err(TrackedWriteFailure::Allocation);
    }
    let excess = target.capacity().saturating_sub(wanted);
    pay(
        b,
        mul(excess, std::mem::size_of::<T>())?,
        excess,
        mul(excess, std::mem::size_of::<T>())?,
    )?;
    if target.capacity() > maximum {
        return Err(TrackedWriteFailure::Capacity);
    }
    check(b, c)
}
fn regular(stat: &Stat) -> bool {
    FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile && stat.st_nlink == 1
}
fn identity(stat: &Stat) -> (u64, u64) {
    (stat.st_dev, stat.st_ino)
}

impl OpenedPinnedShardStore {
    /// Reserve genuine empty destinations BEFORE native append/create entry.
    pub(crate) fn prepare_write_tracking(
        &mut self,
        pin: &PinnedShardDirectory,
        policy: TrackedWriteLimits,
        native: &PinnedShardAppendBudget,
        b: &StorageFetchBudget,
        c: &dyn PinnedShardFetchChecks,
        o: &mut TrackedWriteObservation,
    ) -> Result<(), TrackedWriteFailure> {
        check(b, c)?;
        let (fixed, entries) = tracking_fixed_fee().ok_or(TrackedWriteFailure::Capacity)?;
        pay(b, fixed, entries, fixed)?;
        o.pin_result = Some(o.pin_observation.validate_pin(self, pin, b, c));
        check(b, c)?;
        if !matches!(o.pin_result, Some(Ok(()))) {
            return Err(TrackedWriteFailure::Identity);
        }
        if self.store.plan_epoch != self.tracked_epoch
            || self.store.active.sequence != self.tracked_active_sequence
            || self.store.active.bytes != self.scope.active_bytes
        {
            return Err(TrackedWriteFailure::Stale);
        }
        let remaining = native
            .limits()
            .max_rotations
            .checked_sub(native.usage().rotations)
            .ok_or(TrackedWriteFailure::Stale)?;
        o.remaining_rotations = Some(remaining);
        if let Some(catalog) = &self.write_catalog {
            if catalog.limits != policy {
                return Err(TrackedWriteFailure::Stale);
            }
        } else {
            self.write_catalog = Some(TrackedWriteCatalog {
                limits: policy,
                retired: Vec::new(),
            });
        }
        // Pay sparse traversal before examining retained File or catalog slots.
        pay(
            b,
            0,
            0,
            add(
                mul(
                    add(self.scope.files.capacity(), 1)?,
                    std::mem::size_of::<FileSlot>(),
                )?,
                mul(
                    add(
                        self.write_catalog
                            .as_ref()
                            .ok_or(TrackedWriteFailure::Invariant)?
                            .retired
                            .capacity(),
                        1,
                    )?,
                    std::mem::size_of::<RetiredPack>(),
                )?,
            )?,
        )?;
        check(b, c)?;
        let catalog = self
            .write_catalog
            .as_mut()
            .ok_or(TrackedWriteFailure::Invariant)?;
        let live = add(
            add(
                self.scope
                    .files
                    .iter()
                    .filter(|s| matches!(s.result, Some(Ok(_))))
                    .count(),
                2,
            )?,
            catalog.retired.iter().filter(|r| r.pack.is_some()).count(),
        )?;
        o.initial_live_files = Some(live);
        if add(live, remaining)? > policy.max_live_files {
            return Err(TrackedWriteFailure::Capacity);
        }
        o.destination_start = Some(catalog.retired.len());
        reserve(
            &mut catalog.retired,
            remaining,
            &mut o.allocations[0],
            policy.max_retired_slots,
            b,
            c,
        )?;
        reserve(
            &mut self.scope.scanned,
            remaining,
            &mut o.allocations[1],
            policy.max_tracked_packs,
            b,
            c,
        )?;
        reserve(
            &mut o.transfers,
            remaining,
            &mut o.allocations[2],
            policy.max_retired_slots,
            b,
            c,
        )?;
        check(b, c)
    }
    pub(crate) fn finish_write_tracking(
        &mut self,
        pin: &PinnedShardDirectory,
        report: &mut PinnedShardAppendReport,
        b: &StorageFetchBudget,
        c: &dyn PinnedShardFetchChecks,
        o: &mut TrackedWriteObservation,
    ) -> Result<(), TrackedWriteFailure> {
        check(b, c)?;
        let (count, inspection) = report
            .transfer_shape()
            .ok_or(TrackedWriteFailure::Capacity)?;
        pay(b, 0, 0, inspection)?;
        check(b, c)?;
        if report.has_created_file()
            || count
                > o.remaining_rotations
                    .ok_or(TrackedWriteFailure::Invariant)?
        {
            return Err(TrackedWriteFailure::Invariant);
        }
        let start = o.destination_start.ok_or(TrackedWriteFailure::Invariant)?;
        for source in 0..count {
            let nested = transfer_value_fee().ok_or(TrackedWriteFailure::Capacity)?;
            pay(b, nested.0, nested.1, nested.0)?;
            check(b, c)?;
            let destination = add(start, source)?;
            // Both Vecs were reserved/capped before physical entry. Materialize
            // every empty destination and actual ledger BEFORE the safe move.
            self.write_catalog
                .as_mut()
                .ok_or(TrackedWriteFailure::Invariant)?
                .retired
                .push(RetiredPack { pack: None });
            o.transfers.push(Transfer::new(source, destination));
            pay(
                b,
                0,
                0,
                std::mem::size_of::<ActivePack>() + std::mem::size_of::<Transfer>(),
            )?;
            check(b, c)?;
            let entry = self
                .write_catalog
                .as_mut()
                .and_then(|v| v.retired.get_mut(destination))
                .ok_or(TrackedWriteFailure::Invariant)?;
            o.transfers[source].result =
                Some(report.transfer_retired_into(source, &mut entry.pack));
            completed(WriteTrackingPhase::Transferred);
            check(b, c)?;
            if !matches!(o.transfers[source].result, Some(Ok(()))) {
                return Err(TrackedWriteFailure::Transfer);
            }
            let pack = self
                .write_catalog
                .as_ref()
                .and_then(|v| v.retired.get(destination))
                .and_then(|v| v.pack.as_ref())
                .ok_or(TrackedWriteFailure::Invariant)?;
            o.transfers[source].sequence = Some(pack.sequence);
            inspect_file(
                &self.store,
                pin,
                &pack.file,
                pack.sequence,
                &mut o.transfers[source],
                b,
                c,
            )?;
            let id = identity(
                o.transfers[source]
                    .held
                    .as_ref()
                    .and_then(|r| r.as_ref().ok())
                    .ok_or(TrackedWriteFailure::Io)?,
            );
            pay(
                b,
                0,
                0,
                mul(
                    add(self.scope.scanned.capacity(), 1)?,
                    std::mem::size_of::<ScannedPack>(),
                )?,
            )?;
            check(b, c)?;
            if let Some(existing) = self
                .scope
                .scanned
                .iter_mut()
                .find(|p| p.sequence == pack.sequence)
            {
                if existing.identity != id {
                    return Err(TrackedWriteFailure::Identity);
                }
                existing.source = ScannedPackSource::Retired(destination);
            } else {
                self.scope.scanned.push(ScannedPack {
                    sequence: pack.sequence,
                    slot: 0,
                    identity: id,
                    source: ScannedPackSource::Retired(destination),
                });
            }
            completed(WriteTrackingPhase::Registered);
            check(b, c)?;
        }
        // The exact new active File lives in the SAME store. Never reopen it.
        o.active.sequence = Some(self.store.active.sequence);
        inspect_file(
            &self.store,
            pin,
            &self.store.active.file,
            self.store.active.sequence,
            &mut o.active,
            b,
            c,
        )?;
        let id = identity(
            o.active
                .held
                .as_ref()
                .and_then(|r| r.as_ref().ok())
                .ok_or(TrackedWriteFailure::Io)?,
        );
        pay(
            b,
            0,
            0,
            mul(
                add(self.scope.scanned.capacity(), 1)?,
                std::mem::size_of::<ScannedPack>(),
            )?,
        )?;
        check(b, c)?;
        if let Some(existing) = self
            .scope
            .scanned
            .iter_mut()
            .find(|p| p.sequence == self.store.active.sequence)
        {
            if existing.identity != id {
                return Err(TrackedWriteFailure::Identity);
            }
            existing.source = ScannedPackSource::Active;
        } else {
            self.scope.scanned.push(ScannedPack {
                sequence: self.store.active.sequence,
                slot: 0,
                identity: id,
                source: ScannedPackSource::Active,
            });
        }
        // Only this completed controlled transition advances the tracked epoch.
        self.scope.active_bytes = self.store.active.bytes;
        self.tracked_epoch = self.store.plan_epoch;
        self.tracked_active_sequence = self.store.active.sequence;
        o.pin_result = Some(o.pin_observation.validate_pin(self, pin, b, c));
        check(b, c)?;
        if !matches!(o.pin_result, Some(Ok(()))) {
            return Err(TrackedWriteFailure::Identity);
        }
        completed(WriteTrackingPhase::Registered);
        check(b, c)
    }
}
fn inspect_file(
    store: &ShardStore,
    pin: &PinnedShardDirectory,
    file: &File,
    sequence: u64,
    slot: &mut Transfer,
    b: &StorageFetchBudget,
    c: &dyn PinnedShardFetchChecks,
) -> Result<(), TrackedWriteFailure> {
    let n = store.pack_paths.len();
    pay(
        b,
        0,
        0,
        mul(add(n, 1)?, std::mem::size_of::<(u64, PathBuf)>())?,
    )?;
    check(b, c)?;
    let path = store
        .pack_paths
        .get(&sequence)
        .ok_or(TrackedWriteFailure::Stale)?;
    let len = path.as_os_str().as_bytes().len();
    if len > b.limits().max_path_bytes {
        return Err(TrackedWriteFailure::Capacity);
    }
    // Actual requested and excess capacity is retained outside catch, then the
    // same bytes move into the path without a pathname reopen or local owner.
    reserve(
        &mut slot.path_bytes,
        len,
        &mut slot.path_allocation,
        b.limits().max_path_bytes,
        b,
        c,
    )?;
    pay(b, len, 1, mul(len, 9)?)?;
    check(b, c)?;
    slot.path_bytes
        .extend_from_slice(path.as_os_str().as_bytes());
    slot.path = PathBuf::from(OsString::from_vec(std::mem::take(&mut slot.path_bytes)));
    completed(WriteTrackingPhase::PathCopied);
    check(b, c)?;
    if slot.path.parent() != Some(pin.directory()) {
        return Err(TrackedWriteFailure::Identity);
    }
    let name = slot.path.file_name().ok_or(TrackedWriteFailure::Identity)?;
    let size = name.as_bytes().len();
    pay(b, add(size, 1)?, 2, mul(add(size, 1)?, 4)?)?;
    check(b, c)?;
    if name.to_str().and_then(pack_sequence_from_name) != Some(sequence) {
        return Err(TrackedWriteFailure::Identity);
    }
    // Include NUL capacity before CString::new so conversion cannot grow an
    // uncharged allocation. Both the allocation and actual conversion outcome
    // are in this transfer ledger before the next callback/check.
    reserve(
        &mut slot.name_bytes,
        add(size, 1)?,
        &mut slot.name_allocation,
        add(b.limits().max_path_bytes, 1)?,
        b,
        c,
    )?;
    slot.name_bytes.extend_from_slice(name.as_bytes());
    match CString::new(std::mem::take(&mut slot.name_bytes)) {
        Ok(name) => slot.name = Some(name),
        Err(error) => slot.name_error = Some(error),
    };
    completed(WriteTrackingPhase::NameConverted);
    check(b, c)?;
    if slot.name_error.is_some() {
        return Err(TrackedWriteFailure::Identity);
    }
    pay(
        b,
        std::mem::size_of::<io::Result<Stat>>(),
        2,
        std::mem::size_of::<Stat>(),
    )?;
    check(b, c)?;
    slot.held = Some(fstat(file).map_err(io::Error::from));
    completed(WriteTrackingPhase::Stat);
    check(b, c)?;
    pay(
        b,
        std::mem::size_of::<io::Result<Stat>>(),
        2,
        std::mem::size_of::<Stat>(),
    )?;
    check(b, c)?;
    slot.named = Some(
        statat(
            &pin.scope.directory,
            slot.name.as_deref().ok_or(TrackedWriteFailure::Invariant)?,
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(io::Error::from),
    );
    completed(WriteTrackingPhase::Stat);
    check(b, c)?;
    let held = slot
        .held
        .as_ref()
        .and_then(|r| r.as_ref().ok())
        .ok_or(TrackedWriteFailure::Io)?;
    let named = slot
        .named
        .as_ref()
        .and_then(|r| r.as_ref().ok())
        .ok_or(TrackedWriteFailure::Io)?;
    if !regular(held) || !regular(named) || identity(held) != identity(named) {
        return Err(TrackedWriteFailure::Identity);
    }
    Ok(())
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteTrackingPhase {
    Transferred,
    Registered,
    PathCopied,
    NameConverted,
    Stat,
}
fn completed(phase: WriteTrackingPhase) {
    #[cfg(test)]
    crate::api::pinned::write::tests::tracking_completed(phase);
    #[cfg(not(test))]
    let _ = phase;
}
