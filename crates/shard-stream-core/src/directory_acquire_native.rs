//! Genuine finite nofollow acquisition; entered mutation obligations settle.

use super::super::{FileIdentity, LOCK_NAME};
use super::*;
use rustix::fs::{AtFlags, CWD, Mode, OFlags, Stat, fstat, mkdirat, openat, statat};
use std::{
    any::Any,
    collections::TryReserveError,
    ffi::OsStr,
    fs::{self, File, TryLockError},
    io,
    mem::size_of,
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    panic::{AssertUnwindSafe, catch_unwind},
    path::PathBuf,
};

fn add(a: usize, b: usize) -> std::result::Result<usize, DirectoryOwnershipUntilError> {
    a.checked_add(b)
        .ok_or(DirectoryOwnershipUntilError::Capacity)
}
fn mul(a: usize, b: usize) -> std::result::Result<usize, DirectoryOwnershipUntilError> {
    a.checked_mul(b)
        .ok_or(DirectoryOwnershipUntilError::Capacity)
}
fn spelling(
    path: &Path,
    l: DirectoryAcquireLimits,
) -> std::result::Result<usize, DirectoryOwnershipUntilError> {
    let bytes = path.as_os_str().as_bytes();
    if l.max_path_bytes == 0 || bytes.len() > l.max_path_bytes {
        return Err(DirectoryOwnershipUntilError::Capacity);
    }
    if bytes.first() != Some(&b'/') || bytes.contains(&0) {
        return Err(DirectoryOwnershipError::IdentityMismatch.into());
    }
    if bytes == b"/" {
        return Ok(0);
    }
    let mut n = 0usize;
    for component in bytes[1..].split(|b| *b == b'/') {
        if component.is_empty() || component == b"." || component == b".." {
            return Err(DirectoryOwnershipError::IdentityMismatch.into());
        }
        n = add(n, 1)?;
        if n > l.max_components {
            return Err(DirectoryOwnershipUntilError::Capacity);
        }
    }
    Ok(n)
}
pub(super) fn cost(
    path: &Path,
    l: DirectoryAcquireLimits,
) -> std::result::Result<DirectoryAcquireCost, DirectoryOwnershipUntilError> {
    let components = spelling(path, l)?;
    let bytes = path.as_os_str().as_bytes().len();
    // Includes ENOENT -> raced mkdir EEXIST -> reopened child, both walks,
    // lock revalidation and every future retained descriptor destruction.
    let calls = add(mul(components, 12)?, 40)?;
    let files = add(mul(add(components, 1)?, 2)?, 1)?;
    let arrays = add(
        mul(calls, size_of::<Observation>())?,
        add(
            mul(files, size_of::<FileSlot>())?,
            mul(add(components, 1)?, size_of::<usize>())?,
        )?,
    )?;
    // Finite slack admits measured native Vec/OsString excess before use.
    // Each call also prepays its own temporary native NUL-conversion scratch.
    let strings = add(
        mul(add(bytes, 1)?, add(calls, 2)?)?,
        mul(calls, LOCK_NAME.len() + 1)?,
    )?;
    let fixed = add(
        size_of::<RetainedDirectoryAcquireError>(),
        add(size_of::<DirectoryOwnership>(), size_of::<[usize; 2]>())?,
    )?;
    let declared = add(fixed, add(mul(arrays, 2)?, strings)?)?;
    let entries = add(
        add(calls, add(files, add(components, 1)?)?)?,
        add(mul(calls, 4)?, 32)?,
    )?;
    let work = add(
        mul(declared, 8)?,
        add(mul(calls, add(mul(bytes, 4)?, 4096)?)?, entries)?,
    )?;
    Ok(DirectoryAcquireCost {
        declared,
        entries,
        strings,
        work,
        calls,
        files,
        components,
    })
}

#[derive(Clone, Copy)]
enum Opened {
    Directory(usize),
    Lock,
}
enum Actual {
    Open(io::Result<Opened>),
    Metadata(io::Result<fs::Metadata>),
    Stat(io::Result<Stat>),
    Unit(io::Result<()>),
    Lock(std::result::Result<(), TryLockError>),
}
pub(super) struct Observation {
    pub(super) phase: DirectoryAcquirePhase,
    actual: Actual,
}
impl Observation {
    pub(super) fn succeeded(&self) -> bool {
        match &self.actual {
            Actual::Open(r) => r.is_ok(),
            Actual::Metadata(r) => r.is_ok(),
            Actual::Stat(r) => r.is_ok(),
            Actual::Unit(r) => r.is_ok(),
            Actual::Lock(r) => r.is_ok(),
        }
    }
    pub(super) fn io_error(&self) -> Option<&io::Error> {
        match &self.actual {
            Actual::Open(r) => r.as_ref().err(),
            Actual::Metadata(r) => r.as_ref().err(),
            Actual::Stat(r) => r.as_ref().err(),
            Actual::Unit(r) => r.as_ref().err(),
            Actual::Lock(Err(TryLockError::Error(e))) => Some(e),
            _ => None,
        }
    }
    pub(super) fn contended(&self) -> bool {
        matches!(self.actual, Actual::Lock(Err(TryLockError::WouldBlock)))
    }
    fn kind(&self, kind: io::ErrorKind) -> bool {
        self.io_error().is_some_and(|e| e.kind() == kind)
    }
}
struct FileSlot {
    file: Option<File>,
    identity: Option<FileIdentity>,
}
#[derive(Clone, Copy)]
enum Creation {
    Directory,
    Lock,
}
#[derive(Clone, Copy)]
struct Obligation {
    parent: usize,
    component: usize,
    kind: Creation,
    child: Option<usize>,
    created: bool,
}

pub(super) struct Attempt {
    path: PathBuf,
    outcomes: Vec<Observation>,
    directories: Vec<FileSlot>,
    first_walk: Vec<usize>,
    allocation_error: Option<TryReserveError>,
    panics: [Option<Box<dyn Any + Send>>; 4],
    obligation: Option<Obligation>,
    limits: DirectoryAcquireLimits,
    ceiling: Option<DirectoryAcquireCost>,
    usage: DirectoryAcquireUsage,
    expiry: Instant,
    denied: bool,
    lock_held: bool,
    // LAST: all partial directory descriptors, observations and panic payloads
    // are destroyed before either the raw locked file or completed native owner.
    lock: Option<File>,
    owner: Option<Arc<DirectoryOwnership>>,
}
impl Attempt {
    fn new(limits: DirectoryAcquireLimits, expiry: Instant) -> Self {
        Self {
            path: PathBuf::new(),
            outcomes: Vec::new(),
            directories: Vec::new(),
            first_walk: Vec::new(),
            allocation_error: None,
            panics: std::array::from_fn(|_| None),
            obligation: None,
            limits,
            ceiling: None,
            usage: DirectoryAcquireUsage::default(),
            expiry,
            denied: false,
            lock_held: false,
            lock: None,
            owner: None,
        }
    }
    pub(super) fn usage(&self) -> DirectoryAcquireUsage {
        self.usage
    }
    pub(super) fn observations(&self) -> &[Observation] {
        &self.outcomes
    }
    pub(super) fn allocation_error(&self) -> Option<&TryReserveError> {
        self.allocation_error.as_ref()
    }
    pub(super) fn locked(&self) -> bool {
        self.lock_held
    }
    pub(super) fn panic_payloads(&self) -> impl Iterator<Item = &(dyn Any + Send)> {
        self.panics.iter().filter_map(|p| p.as_deref())
    }
    fn clock(&self) -> Result<()> {
        if Instant::now() >= self.expiry {
            return Err(DirectoryOwnershipUntilError::Expired.into());
        }
        if self.denied {
            return Err(DirectoryAcquireFailure::Capacity);
        }
        Ok(())
    }
    fn fee(
        &mut self,
        calls: usize,
        bytes: usize,
        strings: usize,
        entries: usize,
        work: usize,
    ) -> Result<()> {
        self.clock()?;
        let c = self.ceiling.ok_or(DirectoryAcquireFailure::Invariant)?;
        let next = (|| {
            let next = DirectoryAcquireUsage {
                calls: add(self.usage.calls, calls)?,
                declared_bytes: add(self.usage.declared_bytes, bytes)?,
                string_bytes: add(self.usage.string_bytes, strings)?,
                entries: add(self.usage.entries, entries)?,
                work_bytes: add(self.usage.work_bytes, add(work, entries)?)?,
                ..self.usage
            };
            if next.calls > c.calls
                || next.declared_bytes > c.declared
                || next.entries > c.entries
                || next.string_bytes > c.strings
                || next.work_bytes > c.work
            {
                return Err(DirectoryOwnershipUntilError::Capacity);
            }
            Ok(next)
        })();
        match next {
            Ok(next) => {
                self.usage = next;
                Ok(())
            }
            Err(e) => {
                self.denied = true;
                Err(e.into())
            }
        }
    }
    fn entry(&mut self, calls: usize, name_bytes: usize) -> Result<()> {
        let scratch = mul(calls, add(name_bytes, 1)?)?;
        let work = mul(calls, add(mul(name_bytes, 4)?, 4096)?)?;
        self.fee(calls, scratch, scratch, mul(calls, 4)?, work)
    }
    fn prepare(&mut self, path: &Path) -> Result<()> {
        self.clock()?;
        let c = cost(path, self.limits)?;
        self.ceiling = Some(c);
        let fixed = add(
            size_of::<RetainedDirectoryAcquireError>(),
            add(size_of::<DirectoryOwnership>(), size_of::<[usize; 2]>())?,
        )?;
        self.fee(0, fixed, 0, 16, fixed)?;
        let bytes = path.as_os_str().as_bytes().len();
        // Component selection uses bounded repeated borrowed scans. Pay the
        // complete first/second/creation-settlement walk ceiling before entry.
        let scans = mul(4, mul(add(c.components, 1)?, bytes)?)?;
        self.fee(0, 0, 0, 0, scans)?;
        // Closing the maximum retained descriptor set is prepaid even when
        // custody escapes in a completed owner or retained late error.
        self.entry(add(c.files, 1)?, 0)?;
        self.fee(0, bytes, bytes, 1, mul(bytes, 2)?)?;
        self.allocation_error = self.path.as_mut_os_string().try_reserve_exact(bytes).err();
        self.clock()?;
        if self.allocation_error.is_some() {
            return Err(DirectoryAcquireFailure::Allocation);
        }
        let excess = self.path.capacity().saturating_sub(bytes);
        self.fee(0, excess, excess, 0, excess)?;
        self.path.push(path);
        self.clock()?;
        self.reserve_vectors(c)?;
        self.clock()
    }
    fn reserve_vectors(&mut self, c: DirectoryAcquireCost) -> Result<()> {
        // The buffers remain in this attempt before any post-allocation check.
        for (count, unit) in [
            (c.calls, size_of::<Observation>()),
            (c.files, size_of::<FileSlot>()),
            (c.components + 1, size_of::<usize>()),
        ] {
            let bytes = mul(count, unit)?;
            self.fee(0, bytes, 0, count, bytes)?;
        }
        self.allocation_error = self.outcomes.try_reserve_exact(c.calls).err();
        self.clock()?;
        if self.allocation_error.is_some() {
            return Err(DirectoryAcquireFailure::Allocation);
        }
        let slots = self.outcomes.capacity().saturating_sub(c.calls);
        let bytes = mul(slots, size_of::<Observation>())?;
        self.fee(0, bytes, 0, slots, bytes)?;
        self.allocation_error = self.directories.try_reserve_exact(c.files).err();
        self.clock()?;
        if self.allocation_error.is_some() {
            return Err(DirectoryAcquireFailure::Allocation);
        }
        let slots = self.directories.capacity().saturating_sub(c.files);
        let bytes = mul(slots, size_of::<FileSlot>())?;
        self.fee(0, bytes, 0, slots, bytes)?;
        self.allocation_error = self.first_walk.try_reserve_exact(c.components + 1).err();
        self.clock()?;
        if self.allocation_error.is_some() {
            return Err(DirectoryAcquireFailure::Allocation);
        }
        let slots = self.first_walk.capacity().saturating_sub(c.components + 1);
        let bytes = mul(slots, size_of::<usize>())?;
        self.fee(0, bytes, 0, slots, bytes)
    }
    fn file(&self, index: usize) -> Result<&File> {
        self.directories
            .get(index)
            .and_then(|s| s.file.as_ref())
            .ok_or(DirectoryAcquireFailure::Invariant)
    }
    fn record(&mut self, phase: DirectoryAcquirePhase, actual: Actual) -> usize {
        // All actual call slots were prepaid, and the complete outcome buffer
        // was reserved/measured before the first physical call.
        let i = self.outcomes.len();
        self.outcomes.push(Observation { phase, actual });
        i
    }
    fn native_result(&self, i: usize) -> Result<()> {
        if self.outcomes[i].succeeded() {
            Ok(())
        } else {
            Err(DirectoryAcquireFailure::Native(i))
        }
    }
    fn physical_open(
        &mut self,
        parent: Option<usize>,
        name: &OsStr,
        lock: bool,
        create: bool,
    ) -> Result<(usize, Option<Opened>)> {
        let flags = if lock {
            OFlags::RDWR
                | OFlags::NOFOLLOW
                | OFlags::NONBLOCK
                | OFlags::CLOEXEC
                | if create {
                    OFlags::CREATE | OFlags::EXCL
                } else {
                    OFlags::empty()
                }
        } else {
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC
        };
        let mode = if create {
            Mode::from_raw_mode(0o600)
        } else {
            Mode::empty()
        };
        let actual = match parent {
            Some(parent) => openat(self.file(parent)?, name, flags, mode),
            None => openat(CWD, name, flags, mode),
        }
        .map(File::from)
        .map_err(io::Error::from);
        let phase = if !lock {
            DirectoryAcquirePhase::DirectoryOpened
        } else if create {
            DirectoryAcquirePhase::LockCreated
        } else {
            DirectoryAcquirePhase::LockOpened
        };
        let mut opened = None;
        let actual = actual.map(|file| {
            let slot = if lock {
                self.lock = Some(file);
                Opened::Lock
            } else {
                let index = self.directories.len();
                self.directories.push(FileSlot {
                    file: Some(file),
                    identity: None,
                });
                Opened::Directory(index)
            };
            opened = Some(slot);
            slot
        });
        let index = self.record(phase, Actual::Open(actual));
        Ok((index, opened))
    }
    fn open(
        &mut self,
        parent: Option<usize>,
        name: &OsStr,
        hook: &mut impl FnMut(DirectoryAcquirePhase, &Attempt),
    ) -> Result<(usize, Option<usize>)> {
        self.entry(1, name.as_bytes().len())?;
        let (i, opened) = self.physical_open(parent, name, false, false)?;
        hook(DirectoryAcquirePhase::DirectoryOpened, self);
        self.clock()?;
        Ok((
            i,
            opened.and_then(|v| match v {
                Opened::Directory(i) => Some(i),
                Opened::Lock => None,
            }),
        ))
    }
    fn metadata(
        &mut self,
        directory: Option<usize>,
        hook: &mut impl FnMut(DirectoryAcquirePhase, &Attempt),
    ) -> Result<FileIdentity> {
        self.entry(1, 0)?;
        let actual = match directory {
            Some(i) => self.file(i)?.metadata(),
            None => self
                .lock
                .as_ref()
                .ok_or(DirectoryAcquireFailure::Invariant)?
                .metadata(),
        };
        let phase = if directory.is_some() {
            DirectoryAcquirePhase::DirectoryMetadata
        } else {
            DirectoryAcquirePhase::LockMetadata
        };
        let i = self.record(phase, Actual::Metadata(actual));
        hook(phase, self);
        self.clock()?;
        self.native_result(i)?;
        let Actual::Metadata(Ok(metadata)) = &self.outcomes[i].actual else {
            return Err(DirectoryAcquireFailure::Invariant);
        };
        if directory.is_some() && !metadata.is_dir()
            || directory.is_none() && (!metadata.is_file() || metadata.nlink() != 1)
        {
            return Err(DirectoryAcquireFailure::Identity);
        }
        let identity = FileIdentity::of(metadata);
        if let Some(dir) = directory {
            self.directories[dir].identity = Some(identity);
        }
        Ok(identity)
    }
    fn save_panic(&mut self, payload: Box<dyn Any + Send>) {
        if let Some(slot) = self.panics.iter_mut().find(|p| p.is_none()) {
            *slot = Some(payload);
        }
    }
    fn settle(
        &mut self,
        path: &Path,
        hook: &mut impl FnMut(DirectoryAcquirePhase, &Attempt),
    ) -> Result<()> {
        let Some(mut obligation) = self.obligation else {
            return Ok(());
        };
        if !obligation.created {
            self.obligation = None;
            return Ok(());
        }
        let mut failed = None;
        if matches!(obligation.kind, Creation::Directory) && obligation.child.is_none() {
            let actual = catch_unwind(AssertUnwindSafe(|| -> Result<()> {
                let name = component(path, obligation.component)?;
                let (i, opened) =
                    self.physical_open(Some(obligation.parent), name, false, false)?;
                if let Some(Opened::Directory(child)) = opened {
                    obligation.child = Some(child);
                    self.obligation = Some(obligation);
                }
                hook(DirectoryAcquirePhase::DirectoryOpened, self);
                self.native_result(i)
            }));
            match actual {
                Ok(Err(e)) => failed = Some(e),
                Err(p) => {
                    self.save_panic(p);
                    failed = Some(DirectoryAcquireFailure::Unwind);
                }
                _ => {}
            }
        }
        // Both independent syncs are already paid BEFORE mutation. An E failure
        // or observer panic cannot suppress parent settlement after child work.
        for parent in [false, true] {
            let actual = catch_unwind(AssertUnwindSafe(|| -> Result<()> {
                let file = if parent {
                    Some(self.file(obligation.parent)?)
                } else {
                    match obligation.kind {
                        Creation::Directory => {
                            obligation.child.map(|i| self.file(i)).transpose()?
                        }
                        Creation::Lock => self.lock.as_ref(),
                    }
                };
                let Some(file) = file else {
                    return Ok(());
                };
                let observed = file.sync_all();
                let phase = if parent {
                    DirectoryAcquirePhase::ParentSynced
                } else {
                    DirectoryAcquirePhase::ChildSynced
                };
                let i = self.record(phase, Actual::Unit(observed));
                hook(phase, self);
                self.native_result(i)
            }));
            match actual {
                Ok(Err(e)) if failed.is_none() => failed = Some(e),
                Err(p) => {
                    self.save_panic(p);
                    failed = Some(DirectoryAcquireFailure::Unwind);
                }
                _ => {}
            }
        }
        self.obligation = None;
        if let Some(error) = failed {
            return Err(error);
        }
        Ok(())
    }
    fn create_directory(
        &mut self,
        parent: usize,
        ordinal: usize,
        path: &Path,
        hook: &mut impl FnMut(DirectoryAcquirePhase, &Attempt),
    ) -> Result<usize> {
        if self.usage.creation_attempts >= self.limits.max_created_directories {
            return Err(DirectoryAcquireFailure::Capacity);
        }
        let name = component(path, ordinal)?;
        self.entry(4, name.as_bytes().len())?;
        self.usage.creation_attempts += 1;
        self.obligation = Some(Obligation {
            parent,
            component: ordinal,
            kind: Creation::Directory,
            child: None,
            created: false,
        });
        let actual =
            mkdirat(self.file(parent)?, name, Mode::from_raw_mode(0o700)).map_err(io::Error::from);
        let created = actual.is_ok();
        let i = self.record(
            DirectoryAcquirePhase::DirectoryCreated,
            Actual::Unit(actual),
        );
        if created {
            self.usage.created_directories += 1;
            if let Some(o) = self.obligation.as_mut() {
                o.created = true;
            }
        }
        hook(DirectoryAcquirePhase::DirectoryCreated, self);
        if created {
            self.settle(path, hook)?;
            self.clock()?;
            // The actual newly opened child is retained by the settlement.
            return self
                .directories
                .len()
                .checked_sub(1)
                .ok_or(DirectoryAcquireFailure::Invariant);
        }
        self.obligation = None;
        self.clock()?;
        if self.outcomes[i].kind(io::ErrorKind::AlreadyExists) {
            let (i, opened) = self.open(Some(parent), name, hook)?;
            self.native_result(i)?;
            return opened.ok_or(DirectoryAcquireFailure::Invariant);
        }
        Err(DirectoryAcquireFailure::Native(i))
    }
    fn acquire_lock(
        &mut self,
        root: usize,
        path: &Path,
        hook: &mut impl FnMut(DirectoryAcquirePhase, &Attempt),
    ) -> Result<FileIdentity> {
        // Exclusive creation distinguishes an actual creation from EEXIST.
        self.entry(3, LOCK_NAME.len())?;
        self.obligation = Some(Obligation {
            parent: root,
            component: 0,
            kind: Creation::Lock,
            child: None,
            created: false,
        });
        let (i, opened) = self.physical_open(Some(root), OsStr::new(LOCK_NAME), true, true)?;
        let created = opened.is_some();
        if let Some(o) = self.obligation.as_mut() {
            o.created = created;
        }
        hook(DirectoryAcquirePhase::LockCreated, self);
        if created {
            self.settle(path, hook)?;
            self.clock()?;
        } else {
            self.obligation = None;
            self.clock()?;
            if !self.outcomes[i].kind(io::ErrorKind::AlreadyExists) {
                return Err(DirectoryAcquireFailure::Native(i));
            }
            self.entry(1, LOCK_NAME.len())?;
            let (i, _) = self.physical_open(Some(root), OsStr::new(LOCK_NAME), true, false)?;
            hook(DirectoryAcquirePhase::LockOpened, self);
            self.clock()?;
            self.native_result(i)?;
        }
        let identity = self.metadata(None, hook)?;
        self.entry(1, 0)?;
        let actual = self
            .lock
            .as_ref()
            .ok_or(DirectoryAcquireFailure::Invariant)?
            .try_lock();
        self.lock_held = actual.is_ok();
        let i = self.record(DirectoryAcquirePhase::AdvisoryLock, Actual::Lock(actual));
        hook(DirectoryAcquirePhase::AdvisoryLock, self);
        self.clock()?;
        self.native_result(i)?;
        Ok(identity)
    }
    fn run(
        &mut self,
        path: &Path,
        hook: &mut impl FnMut(DirectoryAcquirePhase, &Attempt),
    ) -> Result<()> {
        self.prepare(path)?;
        let n = self
            .ceiling
            .ok_or(DirectoryAcquireFailure::Invariant)?
            .components;
        let (i, first) = self.open(None, OsStr::new("/"), hook)?;
        self.native_result(i)?;
        let mut parent = first.ok_or(DirectoryAcquireFailure::Invariant)?;
        self.metadata(Some(parent), hook)?;
        self.first_walk.push(parent);
        for ordinal in 0..n {
            let name = component(path, ordinal)?;
            let (i, opened) = self.open(Some(parent), name, hook)?;
            let child = match opened {
                Some(child) => child,
                None if self.outcomes[i].kind(io::ErrorKind::NotFound) => {
                    self.create_directory(parent, ordinal, path, hook)?
                }
                None => return Err(DirectoryAcquireFailure::Native(i)),
            };
            self.metadata(Some(child), hook)?;
            self.first_walk.push(child);
            parent = child;
        }
        let root = parent;
        let lock_identity = self.acquire_lock(root, path, hook)?;
        // Second literal nofollow walk compares EVERY original opened ancestor.
        let (i, opened) = self.open(None, OsStr::new("/"), hook)?;
        self.native_result(i)?;
        let mut current = opened.ok_or(DirectoryAcquireFailure::Invariant)?;
        for ordinal in 0..=n {
            let actual = self.metadata(Some(current), hook)?;
            let expected = self.directories[self.first_walk[ordinal]]
                .identity
                .ok_or(DirectoryAcquireFailure::Invariant)?;
            if actual != expected {
                return Err(DirectoryAcquireFailure::Identity);
            }
            if ordinal < n {
                let (i, opened) = self.open(Some(current), component(path, ordinal)?, hook)?;
                self.native_result(i)?;
                current = opened.ok_or(DirectoryAcquireFailure::Invariant)?;
            }
        }
        self.entry(1, LOCK_NAME.len())?;
        let actual = statat(self.file(current)?, LOCK_NAME, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(io::Error::from);
        let i = self.record(
            DirectoryAcquirePhase::NamedLockMetadata,
            Actual::Stat(actual),
        );
        hook(DirectoryAcquirePhase::NamedLockMetadata, self);
        self.clock()?;
        self.native_result(i)?;
        self.entry(1, 0)?;
        let actual = fstat(
            self.lock
                .as_ref()
                .ok_or(DirectoryAcquireFailure::Invariant)?,
        )
        .map_err(io::Error::from);
        let retained = self.record(DirectoryAcquirePhase::LockMetadata, Actual::Stat(actual));
        hook(DirectoryAcquirePhase::LockMetadata, self);
        self.clock()?;
        self.native_result(retained)?;
        let Actual::Stat(Ok(named)) = &self.outcomes[i].actual else {
            return Err(DirectoryAcquireFailure::Invariant);
        };
        let Actual::Stat(Ok(retained)) = &self.outcomes[retained].actual else {
            return Err(DirectoryAcquireFailure::Invariant);
        };
        // Reuse the native identity policy on SAME-platform Stat values; Unix
        // dev_t need not have the std MetadataExt u64 representation.
        super::super::validation_until::validate_lock_stat_identity(named, retained)
            .map_err(|_| DirectoryAcquireFailure::Identity)?;
        if self.metadata(None, hook)? != lock_identity {
            return Err(DirectoryAcquireFailure::Identity);
        }
        let root_identity = self.metadata(Some(root), hook)?;
        self.clock()?;
        let directory_file = self.directories[root]
            .file
            .take()
            .ok_or(DirectoryAcquireFailure::Invariant)?;
        let lock = self.lock.take().ok_or(DirectoryAcquireFailure::Invariant)?;
        self.owner = Some(Arc::new(DirectoryOwnership {
            canonical_directory: std::mem::take(&mut self.path),
            directory_identity: root_identity,
            lock_identity,
            _directory: directory_file,
            _lock: lock,
        }));
        hook(DirectoryAcquirePhase::OwnerCompleted, self);
        self.clock()
    }
}
fn component(path: &Path, ordinal: usize) -> Result<&OsStr> {
    path.as_os_str().as_bytes()[1..]
        .split(|b| *b == b'/')
        .nth(ordinal)
        .map(OsStr::from_bytes)
        .ok_or(DirectoryAcquireFailure::Invariant)
}

#[allow(
    clippy::result_large_err,
    reason = "retain actual acquisition inputs, descriptors and settlement without a failure allocation"
)]
pub(super) fn acquire(
    path: &Path,
    limits: DirectoryAcquireLimits,
    expiry: Instant,
    mut hook: impl FnMut(DirectoryAcquirePhase, &Attempt),
) -> std::result::Result<Arc<DirectoryOwnership>, RetainedDirectoryAcquireError> {
    let mut attempt = Attempt::new(limits, expiry);
    let actual = catch_unwind(AssertUnwindSafe(|| attempt.run(path, &mut hook)));
    let mut failure = match actual {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(e),
        Err(payload) => {
            attempt.save_panic(payload);
            Some(DirectoryAcquireFailure::Unwind)
        }
    };
    // A real successful creation can outlive its interrupted construction.
    // Settle only its already prepaid physical obligation, never later work.
    if let Err(error) = attempt.settle(path, &mut hook)
        && failure.is_none()
    {
        failure = Some(error);
    }
    if let Some(failure) = failure {
        attempt.denied = true;
        return Err(RetainedDirectoryAcquireError { failure, attempt });
    }
    match attempt.owner.take() {
        Some(owner) => Ok(owner),
        None => Err(RetainedDirectoryAcquireError {
            failure: DirectoryAcquireFailure::Invariant,
            attempt,
        }),
    }
}

#[cfg(test)]
#[path = "directory_acquire_tests.rs"]
mod tests;
