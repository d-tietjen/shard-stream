//! Private tracked GC source identities and actual descriptor quarantine.
//! No opened File, descriptor or fabricated tracked source is publicly returned.
use super::super::gc::{PinnedShardGcFailure as Failure, PinnedShardGcLimits, PinnedShardGcReport};
use super::write::{RetiredPack, TrackedWriteCatalog};
use super::*;
use crate::{StorageFetchBudget, StorageFetchFailure};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(
    clippy::enum_variant_names,
    reason = "every stage denotes a checkpoint before mutation, rather than a completed effect"
)]
pub(crate) enum TrackedGcStage {
    BeforeEpoch,
    BeforeIndex,
    BeforeUnlink,
}
impl TrackedGcStage {
    fn index(self) -> usize {
        match self {
            Self::BeforeEpoch => 0,
            Self::BeforeIndex => 1,
            Self::BeforeUnlink => 2,
        }
    }
}
struct Target {
    sequence: u64,
    source: Option<ScannedPack>,
    lookup: Option<Result<(), Failure>>,
    stats: [[Option<io::Result<Stat>>; 2]; 3],
    matched: [Option<bool>; 3],
}
impl Target {
    fn new(sequence: u64) -> Self {
        Self {
            sequence,
            source: None,
            lookup: None,
            stats: std::array::from_fn(|_| [None, None]),
            matched: [None, None, None],
        }
    }
}
enum Quarantine {
    Original(Option<io::Result<File>>),
    Retired(Option<ActivePack>),
}
struct Move {
    source: ScannedPack,
    destination: usize,
    result: Option<Result<(), Failure>>,
    removed_scanned: Option<ScannedPack>,
}
struct Retag {
    from: usize,
    to: usize,
    sequence: Option<u64>,
    result: Option<Result<(), Failure>>,
    popped: Option<RetiredPack>,
}
/// Retained real native stats, allocation/results, actual partial moves and Files.
/// It has no public File/result-wrapper extractor and is before whole log LAST.
pub(crate) struct TrackedGcTrace {
    targets: Vec<Target>,
    quarantine: Vec<Quarantine>,
    moves: Vec<Move>,
    retags: Vec<Retag>,
    allocations: [Option<Result<(), TryReserveError>>; 4],
    budget_result: Option<Result<(), StorageFetchFailure>>,
    selected: Option<usize>,
    checkpoint: Option<Result<(), ShardDirectoryCheckFailure>>,
    phase: Option<TrackedGcStage>,
    native_capacities: Option<[(usize, usize, usize); 5]>,
}
impl TrackedGcTrace {
    pub(crate) fn new() -> Self {
        Self {
            targets: Vec::new(),
            quarantine: Vec::new(),
            moves: Vec::new(),
            retags: Vec::new(),
            allocations: [None, None, None, None],
            budget_result: None,
            selected: None,
            checkpoint: None,
            phase: None,
            native_capacities: None,
        }
    }
    pub(crate) fn stats(&self) -> impl Iterator<Item = &io::Result<Stat>> {
        self.targets
            .iter()
            .flat_map(|t| t.stats.iter())
            .flat_map(|pair| pair.iter())
            .filter_map(Option::as_ref)
    }
    pub(crate) fn moved(&self) -> usize {
        self.moves
            .iter()
            .filter(|m| matches!(m.result, Some(Ok(()))))
            .count()
    }
    pub(crate) fn retagged(&self) -> usize {
        self.retags
            .iter()
            .filter(|m| matches!(m.result, Some(Ok(()))))
            .count()
    }
    pub(crate) fn budget_error(&self) -> Option<&StorageFetchFailure> {
        self.budget_result.as_ref()?.as_ref().err()
    }
    pub(crate) fn allocation_error(&self) -> Option<&TryReserveError> {
        self.allocations
            .iter()
            .filter_map(Option::as_ref)
            .find_map(|r| r.as_ref().err())
    }
    pub(crate) fn phase(&self) -> Option<usize> {
        self.phase.map(TrackedGcStage::index)
    }
    pub(crate) fn selected(&self) -> Option<usize> {
        self.selected
    }
    pub(crate) fn native_capacities(&self) -> Option<[(usize, usize, usize); 5]> {
        self.native_capacities
    }
    pub(crate) fn transfers(
        &self,
    ) -> impl Iterator<Item = (u64, usize, Option<bool>, Option<u64>)> + '_ {
        self.moves.iter().map(|m| {
            (
                m.source.sequence,
                m.destination,
                m.result.as_ref().map(Result::is_ok),
                m.removed_scanned.map(|p| p.sequence),
            )
        })
    }
    pub(crate) fn retags(
        &self,
    ) -> impl Iterator<Item = (usize, usize, Option<u64>, Option<bool>, bool)> + '_ {
        self.retags.iter().map(|r| {
            (
                r.from,
                r.to,
                r.sequence,
                r.result.as_ref().map(Result::is_ok),
                r.popped.as_ref().is_some_and(|p| p.pack.is_none()),
            )
        })
    }
    fn pay(
        &mut self,
        b: &StorageFetchBudget,
        bytes: usize,
        entries: usize,
        work: usize,
    ) -> Result<(), Failure> {
        self.budget_result = Some(b.charge_values(bytes, entries, work));
        if self.budget_result.as_ref().is_some_and(Result::is_err) {
            Err(Failure::Capacity)
        } else {
            Ok(())
        }
    }
    fn check(
        &mut self,
        b: &StorageFetchBudget,
        c: &dyn super::super::gc::PinnedShardGcChecks,
    ) -> Result<(), Failure> {
        check_saved(b, c, &mut self.budget_result, &mut self.checkpoint)
    }
    pub(crate) fn prepare(
        &mut self,
        l: PinnedShardGcLimits,
        b: &StorageFetchBudget,
        c: &dyn super::super::gc::PinnedShardGcChecks,
    ) -> Result<(), Failure> {
        self.check(b, c)?;
        let (bytes, entries) = tracking_gc_fixed_fee().ok_or(Failure::Arithmetic)?;
        self.pay(b, bytes, entries, bytes)?;
        // Independently name every nested value in all prepaid empty slots.
        // Real Vec storage is charged separately by reserve, never implicit.
        for (n, fee) in [
            (l.max_plan_packs, target_fee()),
            (l.max_plan_packs, move_fee()),
            (l.max_packs, retag_fee()),
        ] {
            let (v, e) = fee.ok_or(Failure::Arithmetic)?;
            self.pay(b, mul(n, v)?, mul(n, e)?, mul(n, v)?)?;
            self.check(b, c)?;
        }
        // Reserve real empty destinations/ledgers before any native GC effects.
        reserve(
            &mut self.targets,
            l.max_plan_packs,
            &mut self.allocations[0],
            &mut self.budget_result,
            &mut self.checkpoint,
            b,
            c,
        )?;
        reserve(
            &mut self.quarantine,
            l.max_plan_packs,
            &mut self.allocations[1],
            &mut self.budget_result,
            &mut self.checkpoint,
            b,
            c,
        )?;
        reserve(
            &mut self.moves,
            l.max_plan_packs,
            &mut self.allocations[2],
            &mut self.budget_result,
            &mut self.checkpoint,
            b,
            c,
        )?;
        reserve(
            &mut self.retags,
            l.max_packs,
            &mut self.allocations[3],
            &mut self.budget_result,
            &mut self.checkpoint,
            b,
            c,
        )
    }
}
fn add(a: usize, b: usize) -> Result<usize, Failure> {
    a.checked_add(b).ok_or(Failure::Arithmetic)
}
fn mul(a: usize, b: usize) -> Result<usize, Failure> {
    a.checked_mul(b).ok_or(Failure::Arithmetic)
}
fn check_saved(
    b: &StorageFetchBudget,
    c: &dyn super::super::gc::PinnedShardGcChecks,
    paid: &mut Option<Result<(), StorageFetchFailure>>,
    checkpoint: &mut Option<Result<(), ShardDirectoryCheckFailure>>,
) -> Result<(), Failure> {
    *paid = Some(b.check());
    if paid.as_ref().is_some_and(Result::is_err) {
        return Err(Failure::Capacity);
    }
    *checkpoint = Some(c.check());
    *paid = Some(b.check());
    if paid.as_ref().is_some_and(Result::is_err) {
        return Err(Failure::Capacity);
    }
    if matches!(checkpoint, Some(Err(_))) {
        return Err(Failure::Checkpoint);
    }
    Ok(())
}
fn reserve<T>(
    v: &mut Vec<T>,
    n: usize,
    result: &mut Option<Result<(), TryReserveError>>,
    paid: &mut Option<Result<(), StorageFetchFailure>>,
    checkpoint: &mut Option<Result<(), ShardDirectoryCheckFailure>>,
    b: &StorageFetchBudget,
    c: &dyn super::super::gc::PinnedShardGcChecks,
) -> Result<(), Failure> {
    let requested = mul(n, std::mem::size_of::<T>())?;
    *paid = Some(b.charge_values(requested, n, requested));
    if paid.as_ref().is_some_and(Result::is_err) {
        return Err(Failure::Capacity);
    }
    check_saved(b, c, paid, checkpoint)?;
    // Both original typed results remain in the outside tracking scope.
    *result = Some(v.try_reserve_exact(n));
    check_saved(b, c, paid, checkpoint)?;
    if result.as_ref().is_some_and(Result::is_err) {
        return Err(Failure::Allocation);
    }
    let excess = v.capacity().saturating_sub(n);
    let bytes = mul(excess, std::mem::size_of::<T>())?;
    *paid = Some(b.charge_values(bytes, excess, bytes));
    if paid.as_ref().is_some_and(Result::is_err) {
        return Err(Failure::Capacity);
    }
    if v.capacity() > n {
        return Err(Failure::Capacity);
    }
    Ok(())
}
/// Every independently retained value/header is named, including empty slots.
pub(crate) fn tracking_gc_fixed_fee() -> Option<(usize, usize)> {
    let fields = [
        std::mem::size_of::<TrackedGcTrace>(),
        std::mem::size_of::<Vec<Target>>(),
        std::mem::size_of::<Vec<Quarantine>>(),
        std::mem::size_of::<Vec<Move>>(),
        std::mem::size_of::<Vec<Retag>>(),
        std::mem::size_of::<Target>(),
        std::mem::size_of::<Quarantine>(),
        std::mem::size_of::<Move>(),
        std::mem::size_of::<Retag>(),
        std::mem::size_of::<ScannedPack>(),
        std::mem::size_of::<ScannedPackSource>(),
        std::mem::size_of::<Option<ScannedPack>>(),
        std::mem::size_of::<Option<Result<(), Failure>>>(),
        std::mem::size_of::<Option<Result<(), StorageFetchFailure>>>(),
        std::mem::size_of::<Option<Result<(), TryReserveError>>>(),
        std::mem::size_of::<Option<Result<(), TryReserveError>>>(),
        std::mem::size_of::<Option<Result<(), TryReserveError>>>(),
        std::mem::size_of::<Option<Result<(), TryReserveError>>>(),
        std::mem::size_of::<Option<io::Result<File>>>(),
        std::mem::size_of::<Option<ActivePack>>(),
        std::mem::size_of::<Option<RetiredPack>>(),
        std::mem::size_of::<Option<usize>>(),
        std::mem::size_of::<Option<Result<(), ShardDirectoryCheckFailure>>>(),
        std::mem::size_of::<Option<TrackedGcStage>>(),
        std::mem::size_of::<TrackedGcSources<'static>>(),
        std::mem::size_of::<Option<&mut TrackedGcSources<'static>>>(),
        std::mem::size_of::<Result<(), Failure>>(),
        std::mem::size_of::<Option<[(usize, usize, usize); 5]>>(),
        std::mem::size_of::<[(usize, usize, usize); 5]>(),
        std::mem::size_of::<(usize, usize, usize)>(),
        std::mem::size_of::<(usize, usize, usize)>(),
        std::mem::size_of::<(usize, usize, usize)>(),
        std::mem::size_of::<(usize, usize, usize)>(),
        std::mem::size_of::<(usize, usize, usize)>(),
    ];
    Some((
        fields.iter().try_fold(0usize, |n, v| n.checked_add(*v))?,
        fields.len(),
    ))
}
fn move_fee() -> Option<(usize, usize)> {
    let values = [
        std::mem::size_of::<Quarantine>(),
        std::mem::size_of::<Move>(),
        std::mem::size_of::<ScannedPack>(),
        std::mem::size_of::<ScannedPackSource>(),
        std::mem::size_of::<usize>(),
        std::mem::size_of::<Option<Result<(), Failure>>>(),
        std::mem::size_of::<Result<(), Failure>>(),
        std::mem::size_of::<Option<ScannedPack>>(),
        std::mem::size_of::<Option<io::Result<File>>>(),
        std::mem::size_of::<io::Result<File>>(),
        std::mem::size_of::<File>(),
        std::mem::size_of::<Option<ActivePack>>(),
        std::mem::size_of::<ActivePack>(),
    ];
    Some((
        values.iter().try_fold(0usize, |n, v| n.checked_add(*v))?,
        values.len(),
    ))
}
fn retag_fee() -> Option<(usize, usize)> {
    let values = [
        std::mem::size_of::<Retag>(),
        std::mem::size_of::<usize>(),
        std::mem::size_of::<usize>(),
        std::mem::size_of::<Option<u64>>(),
        std::mem::size_of::<Option<Result<(), Failure>>>(),
        std::mem::size_of::<Option<RetiredPack>>(),
        std::mem::size_of::<RetiredPack>(),
        std::mem::size_of::<Option<ActivePack>>(),
    ];
    Some((
        values.iter().try_fold(0usize, |n, v| n.checked_add(*v))?,
        values.len(),
    ))
}
/// Full strict call ceiling is derived from the genuine shared producer:
/// five identity calls; each pack has old stat/rename/sync/unlink/sync (5)
/// plus held+named stats at each of three strict cuts (6): 5 + 11*N.
pub(crate) fn tracking_gc_cost(l: PinnedShardGcLimits) -> Option<crate::PinnedShardGcCost> {
    let mut cost = crate::pinned_shard_gc_cost(l)?;
    let fixed = tracking_gc_fixed_fee()?;
    let mut bytes = fixed.0;
    let mut entries = fixed.1;
    for (n, width) in [
        (l.max_plan_packs, std::mem::size_of::<Target>()),
        (l.max_plan_packs, std::mem::size_of::<Quarantine>()),
        (l.max_plan_packs, std::mem::size_of::<Move>()),
        (l.max_packs, std::mem::size_of::<Retag>()),
    ] {
        bytes = bytes.checked_add(n.checked_mul(width)?)?;
        entries = entries.checked_add(n)?;
    }
    for (n, fee) in [
        (l.max_plan_packs, target_fee()),
        (l.max_plan_packs, move_fee()),
        (l.max_packs, retag_fee()),
    ] {
        let (v, e) = fee?;
        bytes = bytes.checked_add(n.checked_mul(v)?)?;
        entries = entries.checked_add(n.checked_mul(e)?)?;
    }
    // Each independently entered actual scan/probe/move is debited at entry.
    // Strict source capacities are capped by max_packs before native effects.
    let scanned = l
        .max_packs
        .checked_add(1)?
        .checked_mul(std::mem::size_of::<ScannedPack>())?;
    let targets = l
        .max_plan_packs
        .checked_add(1)?
        .checked_mul(std::mem::size_of::<Target>())?;
    let held = (std::mem::size_of::<ScannedPack>())
        .checked_add(std::mem::size_of::<FileSlot>())?
        .checked_add(std::mem::size_of::<RetiredPack>())?
        .checked_mul(3)?;
    let movement = (std::mem::size_of::<Quarantine>())
        .checked_add(std::mem::size_of::<Move>())?
        .checked_add(std::mem::size_of::<FileSlot>())?
        .checked_add(std::mem::size_of::<RetiredPack>())?;
    let per_target = targets
        .checked_add(scanned.checked_mul(2)?)?
        .checked_add(held)?
        .checked_add(movement)?;
    let dense = l
        .max_packs
        .checked_add(1)?
        .checked_mul(std::mem::size_of::<RetiredPack>())?
        .checked_add(
            l.max_packs.checked_mul(
                (std::mem::size_of::<RetiredPack>())
                    .checked_add(std::mem::size_of::<Retag>())?
                    .checked_add(scanned)?,
            )?,
        )?;
    let work = per_target
        .checked_mul(l.max_plan_packs)?
        .checked_add(dense)?;
    cost.declared_bytes = cost.declared_bytes.checked_add(bytes)?;
    cost.entries = cost.entries.checked_add(entries)?;
    cost.work_bytes = cost
        .work_bytes
        .checked_add(bytes)?
        .checked_add(entries)?
        .checked_add(work)?;
    cost.native_call_slots = l.max_plan_packs.checked_mul(11)?.checked_add(5)?;
    Some(cost)
}
fn target_fee() -> Option<(usize, usize)> {
    let fields = [
        std::mem::size_of::<Target>(),
        std::mem::size_of::<Option<ScannedPack>>(),
        std::mem::size_of::<Option<Result<(), Failure>>>(),
        std::mem::size_of::<Option<io::Result<Stat>>>(),
        std::mem::size_of::<Option<io::Result<Stat>>>(),
        std::mem::size_of::<Option<io::Result<Stat>>>(),
        std::mem::size_of::<Option<io::Result<Stat>>>(),
        std::mem::size_of::<Option<io::Result<Stat>>>(),
        std::mem::size_of::<Option<io::Result<Stat>>>(),
        std::mem::size_of::<Option<bool>>(),
        std::mem::size_of::<Option<bool>>(),
        std::mem::size_of::<Option<bool>>(),
        std::mem::size_of::<io::Result<Stat>>(),
        std::mem::size_of::<io::Result<Stat>>(),
        std::mem::size_of::<io::Result<Stat>>(),
        std::mem::size_of::<io::Result<Stat>>(),
        std::mem::size_of::<io::Result<Stat>>(),
        std::mem::size_of::<io::Result<Stat>>(),
        std::mem::size_of::<Stat>(),
        std::mem::size_of::<Stat>(),
        std::mem::size_of::<Stat>(),
        std::mem::size_of::<Stat>(),
        std::mem::size_of::<Stat>(),
        std::mem::size_of::<Stat>(),
        std::mem::size_of::<Result<&File, Failure>>(),
        std::mem::size_of::<Result<&File, Failure>>(),
        std::mem::size_of::<Result<&File, Failure>>(),
    ];
    Some((
        fields.iter().try_fold(0usize, |n, v| n.checked_add(*v))?,
        fields.len(),
    ))
}
/// Concrete opaque borrowed actual sources. Only this child constructs it from
/// the genuine opened owner; callers cannot manufacture a tracked identity.
pub(in crate::extent::pinned_directory) struct TrackedGcSources<'a> {
    scope: &'a Scope,
    catalog: Option<&'a TrackedWriteCatalog>,
    active: u64,
    tracked_epoch: u64,
    active_bytes: u64,
    trace: &'a mut TrackedGcTrace,
    budget: &'a StorageFetchBudget,
}
impl TrackedGcSources<'_> {
    pub(in crate::extent::pinned_directory) fn native_capacities(
        &mut self,
        actual: [(usize, usize, usize); 5],
        c: &dyn super::super::gc::PinnedShardGcChecks,
    ) -> Result<(), Failure> {
        self.trace.native_capacities = Some(actual);
        for (capacity, requested, width) in actual {
            let extra = capacity.saturating_sub(requested);
            let bytes = mul(extra, width)?;
            self.trace.pay(self.budget, bytes, extra, bytes)?;
            self.trace.check(self.budget, c)?;
            if capacity > requested {
                return Err(Failure::Capacity);
            }
        }
        Ok(())
    }
    pub(in crate::extent::pinned_directory) fn notify(&mut self, stage: TrackedGcStage) {
        self.trace.phase = Some(stage);
        #[cfg(test)]
        crate::api::pinned::gc::tests::identity(stage);
    }
    pub(in crate::extent::pinned_directory) fn validate_store(
        &self,
        store: &ShardStore,
        limits: PinnedShardGcLimits,
    ) -> Result<(), Failure> {
        if self.scope.scanned.capacity() > limits.max_packs
            || self
                .catalog
                .is_some_and(|c| c.retired.capacity() > limits.max_packs)
        {
            return Err(Failure::Capacity);
        }
        if store.plan_epoch != self.tracked_epoch
            || store.active.sequence != self.active
            || store.active.bytes != self.active_bytes
        {
            Err(Failure::Identity)
        } else {
            Ok(())
        }
    }
    pub(in crate::extent::pinned_directory) fn select(
        &mut self,
        index: usize,
        sequence: u64,
        c: &dyn super::super::gc::PinnedShardGcChecks,
    ) -> Result<(), Failure> {
        self.trace.check(self.budget, c)?;
        if index != self.trace.targets.len() || index >= self.trace.targets.capacity() {
            return Err(Failure::Capacity);
        }
        self.trace.pay(
            self.budget,
            0,
            0,
            mul(
                add(self.trace.targets.capacity(), 1)?,
                std::mem::size_of::<Target>(),
            )?,
        )?;
        self.trace.check(self.budget, c)?;
        if self.trace.targets.iter().any(|p| p.sequence == sequence) {
            return Err(Failure::Invariant);
        }
        self.trace.targets.push(Target::new(sequence));
        self.trace.pay(
            self.budget,
            0,
            0,
            mul(
                add(self.scope.scanned.capacity(), 1)?,
                std::mem::size_of::<ScannedPack>(),
            )?,
        )?;
        self.trace.check(self.budget, c)?;
        let source = self
            .scope
            .scanned
            .iter()
            .find(|p| p.sequence == sequence)
            .copied();
        self.trace.targets[index].source = source;
        self.trace.targets[index].lookup = Some(match source {
            Some(p)
                if sequence < self.active
                    && !matches!(p.source, ScannedPackSource::Active)
                    && !(matches!(p.source, ScannedPackSource::Original)
                        && self.scope.active_slot == Some(p.slot)) =>
            {
                Ok(())
            }
            _ => Err(Failure::Identity),
        });
        if !matches!(self.trace.targets[index].lookup, Some(Ok(()))) {
            return Err(Failure::Identity);
        }
        Ok(())
    }
    fn file(&self, index: usize) -> Result<&File, Failure> {
        let p = self
            .trace
            .targets
            .get(index)
            .and_then(|t| t.source)
            .ok_or(Failure::Invariant)?;
        match p.source {
            ScannedPackSource::Original => self.scope.file(p.slot).map_err(|_| Failure::Identity),
            ScannedPackSource::Retired(slot) => self
                .catalog
                .and_then(|v| v.retired.get(slot))
                .and_then(|v| v.pack.as_ref())
                .filter(|v| v.sequence == p.sequence)
                .map(|v| &v.file)
                .ok_or(Failure::Identity),
            ScannedPackSource::Active => Err(Failure::Identity),
        }
    }
    pub(in crate::extent::pinned_directory) fn prepare_held(
        &mut self,
        index: usize,
        c: &dyn super::super::gc::PinnedShardGcChecks,
    ) -> Result<(), Failure> {
        self.trace.pay(
            self.budget,
            0,
            0,
            std::mem::size_of::<ScannedPack>()
                + std::mem::size_of::<FileSlot>()
                + std::mem::size_of::<RetiredPack>(),
        )?;
        self.trace.check(self.budget, c)?;
        let actual = self.file(index).map(|_| ());
        self.trace.targets[index].lookup = Some(actual);
        self.trace.targets[index].lookup.ok_or(Failure::Invariant)?
    }
    pub(in crate::extent::pinned_directory) fn held(
        &mut self,
        index: usize,
        stage: TrackedGcStage,
    ) -> Result<(), Failure> {
        let result = fstat(self.file(index)?).map_err(io::Error::from);
        self.trace.targets[index].stats[stage.index()][0] = Some(result);
        Ok(())
    }
    pub(in crate::extent::pinned_directory) fn named(
        &mut self,
        index: usize,
        stage: TrackedGcStage,
        pin: &PinnedShardDirectory,
        name: &std::ffi::CStr,
    ) {
        self.trace.targets[index].stats[stage.index()][1] = Some(
            statat(&pin.scope.directory, name, AtFlags::SYMLINK_NOFOLLOW).map_err(io::Error::from),
        );
    }
    pub(in crate::extent::pinned_directory) fn compare(
        &mut self,
        index: usize,
        stage: TrackedGcStage,
    ) -> Result<(), Failure> {
        let target = &mut self.trace.targets[index];
        let held = target.stats[stage.index()][0]
            .as_ref()
            .and_then(|v| v.as_ref().ok());
        let named = target.stats[stage.index()][1]
            .as_ref()
            .and_then(|v| v.as_ref().ok());
        let Some((held, named)) = held.zip(named) else {
            return Err(Failure::Io);
        };
        let identity = target.source.ok_or(Failure::Invariant)?.identity;
        let matched = [held, named].into_iter().all(|v| {
            FileType::from_raw_mode(v.st_mode) == FileType::RegularFile
                && v.st_nlink == 1
                && (v.st_dev, v.st_ino) == identity
        });
        target.matched[stage.index()] = Some(matched);
        if matched {
            Ok(())
        } else {
            Err(Failure::Identity)
        }
    }
    pub(in crate::extent::pinned_directory) fn compare_original(
        &self,
        index: usize,
        stat: &Stat,
    ) -> Result<(), Failure> {
        let expected = self
            .trace
            .targets
            .get(index)
            .and_then(|t| t.source)
            .ok_or(Failure::Invariant)?
            .identity;
        if (stat.st_dev, stat.st_ino) == expected {
            Ok(())
        } else {
            Err(Failure::Identity)
        }
    }
}
impl OpenedPinnedShardStore {
    /// Internal actual owner path; native producer never sees an arbitrary
    /// descriptor plus a claimed owner. Split fields borrow SAME actual store.
    #[allow(
        clippy::result_large_err,
        reason = "retain actual native collection plans and outcomes without a failure allocation"
    )]
    #[allow(
        clippy::too_many_arguments,
        reason = "keep the actual pin, retention floors, original budgets/checks and trace explicit"
    )]
    pub(crate) fn run_tracked_gc(
        &mut self,
        pin: &PinnedShardDirectory,
        starts: &HashMap<TopicPartition, LogicalOffset>,
        limits: PinnedShardGcLimits,
        expiry: Instant,
        checks: &dyn super::super::gc::PinnedShardGcChecks,
        budget: &StorageFetchBudget,
        trace: &mut TrackedGcTrace,
    ) -> Result<super::super::gc::PinnedShardGcReport, super::super::gc::PinnedShardGcError> {
        let mut sources = TrackedGcSources {
            scope: &self.scope,
            catalog: self.write_catalog.as_ref(),
            active: self.tracked_active_sequence,
            tracked_epoch: self.tracked_epoch,
            active_bytes: self.scope.active_bytes,
            trace,
            budget,
        };
        super::super::gc::collect_tracked(
            &mut self.store,
            pin,
            starts,
            limits,
            expiry,
            checks,
            &mut sources,
        )
    }
    /// Only the canonical caller's saved ACTUAL NativeOk reaches this private
    /// path. Every real source moves into an already-paid empty destination.
    pub(crate) fn retire_completed_gc(
        &mut self,
        report: &PinnedShardGcReport,
        b: &StorageFetchBudget,
        trace: &mut TrackedGcTrace,
        checks: &dyn super::super::gc::PinnedShardGcChecks,
    ) -> Result<(), Failure> {
        trace.check(b, checks)?;
        let count = report.selected_packs();
        trace.selected = Some(count);
        if count != trace.targets.len()
            || count > trace.quarantine.capacity()
            || count > trace.moves.capacity()
        {
            return Err(Failure::Invariant);
        }
        for index in 0..count {
            trace.check(b, checks)?;
            let pack = report.pack(index).ok_or(Failure::Invariant)?;
            if !matches!(pack.unlink_result(), Some(Ok(())))
                || !matches!(pack.unlink_sync_result(), Some(Ok(())))
            {
                return Err(Failure::Invariant);
            }
            let source = trace.targets[index].source.ok_or(Failure::Invariant)?;
            if source.sequence != pack.sequence() {
                return Err(Failure::Identity);
            }
            trace.pay(
                b,
                0,
                0,
                std::mem::size_of::<Quarantine>()
                    + std::mem::size_of::<Move>()
                    + std::mem::size_of::<FileSlot>()
                    + std::mem::size_of::<RetiredPack>(),
            )?;
            trace.check(b, checks)?;
            trace.quarantine.push(match source.source {
                ScannedPackSource::Original => Quarantine::Original(None),
                ScannedPackSource::Retired(_) => Quarantine::Retired(None),
                ScannedPackSource::Active => return Err(Failure::Identity),
            });
            trace.moves.push(Move {
                source,
                destination: index,
                result: None,
                removed_scanned: None,
            });
            let result = match (&mut trace.quarantine[index], source.source) {
                (Quarantine::Original(destination), ScannedPackSource::Original) => {
                    let slot = self
                        .scope
                        .files
                        .get_mut(source.slot)
                        .ok_or(Failure::Invariant)?;
                    if destination.is_some() || !matches!(slot.result, Some(Ok(_))) {
                        Err(Failure::Invariant)
                    } else {
                        *destination = slot.result.take();
                        Ok(())
                    }
                }
                (Quarantine::Retired(destination), ScannedPackSource::Retired(slot)) => {
                    let actual = self
                        .write_catalog
                        .as_mut()
                        .and_then(|v| v.retired.get_mut(slot))
                        .ok_or(Failure::Invariant)?;
                    if destination.is_some() || actual.pack.is_none() {
                        Err(Failure::Invariant)
                    } else {
                        *destination = actual.pack.take();
                        Ok(())
                    }
                }
                _ => Err(Failure::Invariant),
            };
            trace.moves[index].result = Some(result);
            completed(TrackedGcTransferPhase::Quarantined);
            trace.check(b, checks)?;
            if !matches!(trace.moves[index].result, Some(Ok(()))) {
                return Err(Failure::Invariant);
            }
            trace.pay(
                b,
                0,
                0,
                mul(
                    add(self.scope.scanned.capacity(), 1)?,
                    std::mem::size_of::<ScannedPack>(),
                )?,
            )?;
            trace.check(b, checks)?;
            let at = self
                .scope
                .scanned
                .iter()
                .position(|p| p.sequence == source.sequence && p.identity == source.identity)
                .ok_or(Failure::Invariant)?;
            trace.moves[index].removed_scanned = Some(self.scope.scanned.swap_remove(at));
            completed(TrackedGcTransferPhase::RegistrationRemoved);
            trace.check(b, checks)?;
        }
        if let Some(catalog) = &mut self.write_catalog {
            trace.pay(
                b,
                0,
                0,
                mul(
                    add(catalog.retired.capacity(), 1)?,
                    std::mem::size_of::<RetiredPack>(),
                )?,
            )?;
            trace.check(b, checks)?;
            let mut hole = 0;
            while hole < catalog.retired.len() {
                trace.check(b, checks)?;
                trace.pay(b, 0, 0, std::mem::size_of::<RetiredPack>())?;
                trace.check(b, checks)?;
                if catalog.retired[hole].pack.is_some() {
                    hole += 1;
                    continue;
                }
                let tail = catalog.retired.len() - 1;
                if trace.retags.len() >= trace.retags.capacity() {
                    return Err(Failure::Capacity);
                }
                trace.pay(
                    b,
                    0,
                    0,
                    add(
                        std::mem::size_of::<Retag>(),
                        mul(
                            add(self.scope.scanned.capacity(), 1)?,
                            std::mem::size_of::<ScannedPack>(),
                        )?,
                    )?,
                )?;
                trace.check(b, checks)?;
                let seq = catalog.retired[tail].pack.as_ref().map(|v| v.sequence);
                trace.retags.push(Retag {
                    from: tail,
                    to: hole,
                    sequence: seq,
                    result: None,
                    popped: None,
                });
                let n = trace.retags.len() - 1;
                if hole < tail {
                    let (front, back) = catalog.retired.split_at_mut(tail);
                    front[hole].pack = back[0].pack.take();
                }
                trace.retags[n].popped = catalog.retired.pop();
                trace.retags[n].result = Some(Ok(()));
                completed(TrackedGcTransferPhase::CatalogMoved);
                trace.check(b, checks)?;
                if let Some(sequence) = seq {
                    let actual = self
                        .scope
                        .scanned
                        .iter_mut()
                        .find(|p| p.sequence == sequence)
                        .ok_or(Failure::Invariant)?;
                    if !matches!(actual.source,ScannedPackSource::Retired(slot)if slot==tail) {
                        return Err(Failure::Identity);
                    }
                    actual.source = ScannedPackSource::Retired(hole);
                    completed(TrackedGcTransferPhase::Retagged);
                    trace.check(b, checks)?;
                }
            }
        }
        let (_, epoch) = report.epochs();
        if self.store.plan_epoch != epoch {
            return Err(Failure::Identity);
        }
        self.tracked_epoch = epoch;
        trace.check(b, checks)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TrackedGcTransferPhase {
    Quarantined,
    RegistrationRemoved,
    CatalogMoved,
    Retagged,
}
fn completed(phase: TrackedGcTransferPhase) {
    #[cfg(test)]
    crate::api::pinned::gc::tests::transferred(phase);
    #[cfg(not(test))]
    let _ = phase;
}
