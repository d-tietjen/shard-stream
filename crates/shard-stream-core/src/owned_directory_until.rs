//! Original-E operations on actual descriptor-relative native owners.

use super::*;
use crate::{NativeDirectoryBudget, NativeDirectoryFailure};
use std::error::Error;
use std::fmt;
use std::mem::size_of;

type Bounded<T> = std::result::Result<T, NativeDirectoryFailure>;

/// Known actual effects of one bounded native operation; no rollback is implied.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NativeDirectoryProgress {
    /// An actual mkdirat was entered, including uncertain/error outcomes.
    pub mkdir_attempted: bool,
    /// The actual mkdirat returned success before the next clock observation.
    pub directory_created: bool,
    /// The requested child/root-handle producer open was entered.
    /// Transient nested validation opens have separate prepaid native fees.
    pub open_attempted: bool,
    /// That producer call returned a descriptor, now retained or completed.
    pub descriptor_opened: bool,
    /// A real native call observed an absent name; final success is separate.
    pub absence_observed: bool,
    /// Actual non-symlink kind observed by a native probe before later refusal.
    pub entry_kind_observed: Option<OwnedEntryKind>,
    /// An actual native no-follow probe observed a symlink and refused it.
    pub symlink_observed: bool,
}

/// Ordinary no-follow native kind of one actually observed named entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnedEntryKind {
    /// A native regular file.
    RegularFile,
    /// A native directory.
    Directory,
    /// Another real native object kind; this is still an observed trace.
    Other,
}

struct Scope {
    directory: Option<OwnedDirectoryHandle>,
    file: Option<OwnedFileHandle>,
    raw_file: Option<File>,
    attempted_name: Option<OsString>,
    progress: NativeDirectoryProgress,
    parent: Option<OwnedDirectoryHandle>,
    // Last: any partial actual descriptor and parent settle before root custody.
    owner: Arc<DirectoryOwnership>,
}
impl Scope {
    fn root(owner: Arc<DirectoryOwnership>) -> Self {
        Self {
            directory: None,
            file: None,
            raw_file: None,
            attempted_name: None,
            progress: NativeDirectoryProgress::default(),
            parent: None,
            owner,
        }
    }
    fn child(parent: &OwnedDirectoryHandle) -> Self {
        let mut scope = Self::root(Arc::clone(parent.owner()));
        scope.parent = Some(parent.retain());
        scope
    }
    fn fail(
        self: Box<Self>,
        failure: NativeDirectoryFailure,
        budget: &NativeDirectoryBudget,
    ) -> RetainedDirectoryError {
        budget.deny();
        RetainedDirectoryError {
            failure,
            custody: Custody::Entered(self),
        }
    }
    fn enter(
        mut self,
        budget: &NativeDirectoryBudget,
    ) -> std::result::Result<Box<Self>, RetainedDirectoryError> {
        if let Err(failure) = self.begin(budget) {
            budget.deny();
            let custody = match self.parent.take() {
                Some(parent) => Custody::EarlyParent(parent),
                None => Custody::EarlyRoot(self.owner),
            };
            return Err(RetainedDirectoryError { failure, custody });
        }
        // The actual scope allocation is paid and E-checked before entry. An
        // already-expired/refused call retains only its real owner/parent.
        let scope = Box::new(self);
        match budget.check() {
            Ok(()) => Ok(scope),
            Err(error) => Err(scope.fail(error, budget)),
        }
    }
    fn begin(&self, budget: &NativeDirectoryBudget) -> Bounded<()> {
        budget.charge(
            1,
            0,
            size_of::<Scope>() + size_of::<RetainedDirectoryError>(),
            0,
            2,
            size_of::<Scope>(),
        )
    }
}

enum Custody {
    EarlyRoot(Arc<DirectoryOwnership>),
    EarlyParent(OwnedDirectoryHandle),
    Entered(Box<Scope>),
}
/// Actual native refusal retaining the SAME root/parent and new partial handles.
///
/// No newly created name is removed. A borrowed validation target remains owned
/// by its caller; any descriptor newly opened by this operation is retained here
/// before observing a late/error phase. This grants no serving permission.
pub struct RetainedDirectoryError {
    failure: NativeDirectoryFailure,
    custody: Custody,
}
pub(crate) const fn retained_scope_bytes() -> usize {
    size_of::<Scope>()
}
impl fmt::Debug for RetainedDirectoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedDirectoryError")
            .field("failure", &self.failure)
            .field("progress", &self.progress())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedDirectoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.failure, f)
    }
}
impl Error for RetainedDirectoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.failure)
    }
}
impl RetainedDirectoryError {
    fn scope(&self) -> Option<&Scope> {
        match &self.custody {
            Custody::Entered(scope) => Some(scope),
            _ => None,
        }
    }
    fn from_parent(
        parent: &OwnedDirectoryHandle,
        failure: NativeDirectoryFailure,
        budget: &NativeDirectoryBudget,
    ) -> Self {
        budget.deny();
        Self {
            failure,
            custody: Custody::EarlyParent(parent.retain()),
        }
    }
    /// Actual native resource/deadline/identity/I/O refusal.
    pub fn failure(&self) -> &NativeDirectoryFailure {
        &self.failure
    }
    /// The original actual owner, without extraction or reconstruction.
    pub fn owner(&self) -> &Arc<DirectoryOwnership> {
        match &self.custody {
            Custody::EarlyRoot(owner) => owner,
            Custody::EarlyParent(parent) => parent.owner(),
            Custody::Entered(scope) => &scope.owner,
        }
    }
    /// Actual known effects, including entered but uncertain mkdir/open calls.
    pub fn progress(&self) -> NativeDirectoryProgress {
        self.scope()
            .map_or_else(NativeDirectoryProgress::default, |scope| scope.progress)
    }
    /// Literal attempted name, when its bounded conversion completed.
    pub fn attempted_name(&self) -> Option<&OsStr> {
        self.scope()
            .and_then(|scope| scope.attempted_name.as_deref())
    }
    /// Any actual newly constructed directory still retained on refusal.
    pub fn directory_handle(&self) -> Option<&OwnedDirectoryHandle> {
        self.scope().and_then(|scope| scope.directory.as_ref())
    }
    /// Any actual newly constructed file still retained on refusal.
    pub fn file_handle(&self) -> Option<&OwnedFileHandle> {
        self.scope().and_then(|scope| scope.file.as_ref())
    }
    /// Whether an earlier actual open descriptor is retained before construction.
    pub fn retains_partial_descriptor(&self) -> bool {
        self.scope().is_some_and(|scope| scope.raw_file.is_some())
    }
}

fn call<T>(
    budget: &NativeDirectoryBudget,
    phase: &str,
    entered: impl FnOnce() -> Result<T>,
) -> Bounded<T> {
    let bytes = size_of::<T>()
        .checked_add(size_of::<Result<T>>())
        .ok_or(NativeDirectoryFailure::Capacity)?;
    budget.charge(0, 1, bytes, 0, 2, bytes)?;
    budget.check()?;
    let observed = entered();
    #[cfg(test)]
    tests::completed(phase);
    #[cfg(not(test))]
    let _ = phase;
    budget.completed(observed)
}

fn path(budget: &NativeDirectoryBudget, path: &Path) -> Bounded<()> {
    if path.as_os_str().as_bytes().len() > budget.limits().max_path_bytes {
        return Err(NativeDirectoryFailure::Capacity);
    }
    budget.check()
}
fn prepare_name(
    scope: &mut Scope,
    parent: &OwnedDirectoryHandle,
    name: &OsStr,
    budget: &NativeDirectoryBudget,
) -> Bounded<()> {
    budget.check()?;
    component(name)?;
    let name_bytes = name.as_bytes().len();
    if name_bytes > budget.limits().max_component_bytes {
        return Err(NativeDirectoryFailure::Capacity);
    }
    let path_bytes = parent
        .path()
        .as_os_str()
        .as_bytes()
        .len()
        .checked_add(name_bytes)
        .and_then(|value| value.checked_add(1))
        .ok_or(NativeDirectoryFailure::Capacity)?;
    if path_bytes > budget.limits().max_path_bytes {
        return Err(NativeDirectoryFailure::Capacity);
    }
    // Attempted name, retained parent component and native pathname conversion;
    // the diagnostic joined path is paid separately. No alias resolver runs.
    let strings = name_bytes
        .checked_add(1)
        .and_then(|value| value.checked_mul(3))
        .and_then(|value| value.checked_add(path_bytes))
        .ok_or(NativeDirectoryFailure::Capacity)?;
    let bytes = strings
        .checked_add(size_of::<Directory>())
        .and_then(|value| value.checked_add(size_of::<OwnedFileHandle>()))
        .and_then(|value| value.checked_add(size_of::<[usize; 2]>()))
        .ok_or(NativeDirectoryFailure::Capacity)?;
    budget.charge(0, 0, bytes, strings, 7, bytes)?;
    budget.check()?;
    scope.attempted_name = Some(name.to_owned());
    budget.check()
}

fn validate_directory(
    handle: &OwnedDirectoryHandle,
    budget: &NativeDirectoryBudget,
    depth: usize,
) -> Bounded<()> {
    budget.check()?;
    if depth > budget.limits().max_depth {
        return Err(NativeDirectoryFailure::Capacity);
    }
    path(budget, &handle.inner.path)?;
    let frame = size_of::<(&OwnedDirectoryHandle, usize, Option<&OwnedDirectoryHandle>)>();
    budget.charge(0, 0, frame, 0, 3, frame)?;
    if let Some((parent, name)) = &handle.inner.parent {
        validate_directory(
            parent,
            budget,
            depth
                .checked_add(1)
                .ok_or(NativeDirectoryFailure::Capacity)?,
        )?;
        return validate_entry_until(&parent.inner.file, name, &handle.inner.file, true, budget);
    }
    // The exact native root validator owns all of its internal ancestor/open/
    // lock phases. Charge its actual plain ceiling before that native call.
    let measured = handle
        .inner
        .owner
        .directory_validation_cost(&handle.inner.path);
    budget.check()?;
    let measured = measured?;
    budget.charge(
        0,
        measured.entries(),
        measured.declared_bytes(),
        measured.string_bytes(),
        measured.entries(),
        measured.work_bytes(),
    )?;
    budget.check()?;
    let observed = handle
        .inner
        .owner
        .validate_directory_with_budget(&handle.inner.path, budget);
    budget.finish(observed)?;
    let observed = call(budget, "root_name_metadata", || {
        std::fs::symlink_metadata(&handle.inner.path).map_err(DirectoryOwnershipError::from)
    })?;
    let actual = call(budget, "root_fd_metadata", || {
        handle
            .inner
            .file
            .metadata()
            .map_err(DirectoryOwnershipError::from)
    })?;
    if !observed.is_dir()
        || observed.file_type().is_symlink()
        || observed.dev() != actual.dev()
        || observed.ino() != actual.ino()
    {
        return Err(DirectoryOwnershipError::IdentityMismatch.into());
    }
    budget.check()
}
fn validate_entry_until(
    parent: &File,
    name: &OsStr,
    file: &File,
    directory: bool,
    budget: &NativeDirectoryBudget,
) -> Bounded<()> {
    budget.check()?;
    component(name)?;
    if name.as_bytes().len() > budget.limits().max_component_bytes {
        return Err(NativeDirectoryFailure::Capacity);
    }
    let strings = name
        .as_bytes()
        .len()
        .checked_add(1)
        .ok_or(NativeDirectoryFailure::Capacity)?;
    let bytes = strings
        .checked_add(size_of::<(&File, &OsStr, &File, bool)>())
        .ok_or(NativeDirectoryFailure::Capacity)?;
    budget.charge(0, 0, bytes, strings, 5, bytes)?;
    let actual = call(budget, "entry_metadata", || {
        file.metadata().map_err(DirectoryOwnershipError::from)
    })?;
    let opened = call(budget, "entry_fstat", || {
        fstat(file)
            .map_err(io::Error::from)
            .map_err(DirectoryOwnershipError::from)
    })?;
    let named = call(budget, "entry_statat", || {
        statat(parent, name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(io::Error::from)
            .map_err(DirectoryOwnershipError::from)
    })?;
    // The ordinary native entry validator shares this exact identity predicate.
    validate_entry_identity(&actual, &opened, &named, directory)?;
    budget.check()
}

pub(crate) fn root_until(
    owner: &Arc<DirectoryOwnership>,
    name: &Path,
    budget: &NativeDirectoryBudget,
    open: impl FnOnce() -> io::Result<File>,
) -> std::result::Result<OwnedDirectoryHandle, RetainedDirectoryError> {
    let mut scope = Scope::root(Arc::clone(owner)).enter(budget)?;
    let observed = (|| {
        path(budget, name)?;
        let measured = owner.directory_validation_cost(name);
        budget.check()?;
        let measured = measured?;
        budget.charge(
            0,
            measured.entries(),
            measured.declared_bytes(),
            measured.string_bytes(),
            measured.entries(),
            measured.work_bytes(),
        )?;
        budget.check()?;
        let validated = owner.validate_directory_with_budget(name, budget);
        budget.finish(validated)?;
        let strings = name.as_os_str().as_bytes().len();
        let bytes = strings
            .checked_add(size_of::<Directory>() + size_of::<[usize; 2]>())
            .ok_or(NativeDirectoryFailure::Capacity)?;
        budget.charge(0, 1, bytes, strings, 4, bytes)?;
        budget.check()?;
        scope.progress.open_attempted = true;
        let opened = open();
        let opened = match opened {
            Ok(file) => {
                scope.raw_file = Some(file);
                scope.progress.descriptor_opened = true;
                Ok(())
            }
            Err(error) => Err(DirectoryOwnershipError::from(error)),
        };
        #[cfg(test)]
        tests::completed("root_clone");
        budget.completed(opened)?;
        budget.check()?;
        let file = scope
            .raw_file
            .take()
            .ok_or(DirectoryOwnershipError::IdentityMismatch)?;
        scope.directory = Some(OwnedDirectoryHandle {
            inner: Arc::new(Directory {
                file,
                path: name.to_owned(),
                parent: None,
                owner: Arc::clone(owner),
            }),
        });
        budget.check()?;
        validate_directory(
            scope
                .directory
                .as_ref()
                .ok_or(DirectoryOwnershipError::IdentityMismatch)?,
            budget,
            0,
        )
    })();
    match budget.finish(observed) {
        Ok(()) => match scope.directory.take() {
            Some(directory) => Ok(directory),
            None => Err(scope.fail(DirectoryOwnershipError::IdentityMismatch.into(), budget)),
        },
        Err(error) => Err(scope.fail(error, budget)),
    }
}

impl OwnedDirectoryHandle {
    /// Validate this actual root/parent chain under one finite original budget.
    pub fn validate_until(
        &self,
        budget: &NativeDirectoryBudget,
    ) -> std::result::Result<(), RetainedDirectoryError> {
        let scope = Scope::child(self).enter(budget)?;
        let observed = validate_directory(self, budget, 0);
        budget
            .finish(observed)
            .map_err(|error| scope.fail(error, budget))
    }
    /// Open one literal child under the SAME owner/counters/E, optionally mkdir.
    ///
    /// Every new real descriptor is retained before post-call clock observation.
    /// Partial/uncertain names remain evidence; no cleanup unlinks them.
    pub fn open_directory_until(
        &self,
        name: &OsStr,
        create: bool,
        budget: &NativeDirectoryBudget,
    ) -> std::result::Result<Self, RetainedDirectoryError> {
        self.open_directory_inner_until(name, create, false, budget)
            .and_then(|directory| {
                directory.ok_or_else(|| {
                    RetainedDirectoryError::from_parent(
                        self,
                        DirectoryOwnershipError::IdentityMismatch.into(),
                        budget,
                    )
                })
            })
    }
    /// Open an existing literal directory, preserving fitting absence as None.
    ///
    /// No directory is created. Only fitting native NotFound is absence; actual
    /// symlink/wrong-kind/other errors refuse, and parent validation completes
    /// under the SAME original/min-only E and cumulative counters.
    pub fn open_optional_directory_until(
        &self,
        name: &OsStr,
        budget: &NativeDirectoryBudget,
    ) -> std::result::Result<Option<Self>, RetainedDirectoryError> {
        self.open_directory_inner_until(name, false, true, budget)
    }
    fn open_directory_inner_until(
        &self,
        name: &OsStr,
        create: bool,
        optional: bool,
        budget: &NativeDirectoryBudget,
    ) -> std::result::Result<Option<Self>, RetainedDirectoryError> {
        let mut scope = Scope::child(self).enter(budget)?;
        let observed = (|| {
            prepare_name(&mut scope, self, name, budget)?;
            validate_directory(self, budget, 0)?;
            if create {
                budget.charge(0, 1, size_of::<Result<()>>(), 0, 1, size_of::<Result<()>>())?;
                budget.check()?;
                scope.progress.mkdir_attempted = true;
                let mkdir = mkdirat(&self.inner.file, name, Mode::RWXU);
                if mkdir.is_ok() {
                    scope.progress.directory_created = true;
                }
                #[cfg(test)]
                tests::completed("mkdir");
                let mkdir = match mkdir {
                    Ok(()) | Err(rustix::io::Errno::EXIST) => Ok(()),
                    Err(error) => Err(DirectoryOwnershipError::from(io::Error::from(error))),
                };
                budget.completed(mkdir)?;
            }
            budget.charge(0, 1, size_of::<Result<File>>(), 0, 1, size_of::<File>())?;
            budget.check()?;
            scope.progress.open_attempted = true;
            let opened = openat(
                &self.inner.file,
                name,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map(File::from)
            .map_err(io::Error::from);
            let opened = match opened {
                Ok(file) => {
                    scope.raw_file = Some(file);
                    scope.progress.descriptor_opened = true;
                    Ok(())
                }
                Err(error) => Err(DirectoryOwnershipError::from(error)),
            };
            #[cfg(test)]
            tests::completed("directory_open");
            let present = if optional {
                budget.completed_optional(opened)?.is_some()
            } else {
                budget.completed(opened)?;
                true
            };
            if !present {
                scope.progress.absence_observed = true;
                validate_directory(self, budget, 0)?;
                return Ok(());
            }
            budget.check()?;
            validate_entry_until(
                &self.inner.file,
                name,
                scope
                    .raw_file
                    .as_ref()
                    .ok_or(DirectoryOwnershipError::IdentityMismatch)?,
                true,
                budget,
            )?;
            let file = scope
                .raw_file
                .take()
                .ok_or(DirectoryOwnershipError::IdentityMismatch)?;
            scope.directory = Some(Self {
                inner: Arc::new(Directory {
                    file,
                    path: self.inner.path.join(name),
                    parent: Some((self.retain(), name.to_owned())),
                    owner: Arc::clone(self.owner()),
                }),
            });
            budget.check()?;
            validate_directory(self, budget, 0)?;
            validate_directory(
                scope
                    .directory
                    .as_ref()
                    .ok_or(DirectoryOwnershipError::IdentityMismatch)?,
                budget,
                0,
            )
        })();
        match budget.finish(observed) {
            Ok(()) => match scope.directory.take() {
                Some(directory) => Ok(Some(directory)),
                None if optional && scope.progress.absence_observed => Ok(None),
                None => Err(scope.fail(DirectoryOwnershipError::IdentityMismatch.into(), budget)),
            },
            Err(error) => Err(scope.fail(error, budget)),
        }
    }
    /// Open one literal regular file under the SAME owner/counters/original E.
    ///
    /// Creating modes preserve native nontruncating regular-file behavior.
    /// NONBLOCK avoids waiting on a substituted special file before the native
    /// regular-file identity check rejects it. A late/error result
    /// retains the actual parent and any newly opened file, without unlinking.
    pub fn open_file_until(
        &self,
        name: &OsStr,
        access: OwnedFileAccess,
        budget: &NativeDirectoryBudget,
    ) -> std::result::Result<OwnedFileHandle, RetainedDirectoryError> {
        self.open_file_inner_until(name, access, false, budget)
            .and_then(|file| {
                file.ok_or_else(|| {
                    RetainedDirectoryError::from_parent(
                        self,
                        DirectoryOwnershipError::IdentityMismatch.into(),
                        budget,
                    )
                })
            })
    }
    /// Observe an absent literal file as `None`, without denying this budget.
    ///
    /// Only native NotFound from a ReadOnly open is an expected absence. All
    /// late, wrong-kind, symlink, identity and other errors retain their actual
    /// outcomes and deny later work. Parent validation still completes under E.
    pub fn open_optional_file_until(
        &self,
        name: &OsStr,
        budget: &NativeDirectoryBudget,
    ) -> std::result::Result<Option<OwnedFileHandle>, RetainedDirectoryError> {
        self.open_file_inner_until(name, OwnedFileAccess::ReadOnly, true, budget)
    }
    fn open_file_inner_until(
        &self,
        name: &OsStr,
        access: OwnedFileAccess,
        optional: bool,
        budget: &NativeDirectoryBudget,
    ) -> std::result::Result<Option<OwnedFileHandle>, RetainedDirectoryError> {
        let mut scope = Scope::child(self).enter(budget)?;
        let observed = (|| {
            prepare_name(&mut scope, self, name, budget)?;
            validate_directory(self, budget, 0)?;
            let access = match access {
                OwnedFileAccess::ReadOnly => OFlags::RDONLY,
                OwnedFileAccess::ReadWrite => OFlags::RDWR,
                OwnedFileAccess::ReadWriteCreate => OFlags::RDWR | OFlags::CREATE,
                OwnedFileAccess::CreateNew => OFlags::RDWR | OFlags::CREATE | OFlags::EXCL,
            };
            budget.charge(0, 1, size_of::<Result<File>>(), 0, 1, size_of::<File>())?;
            budget.check()?;
            scope.progress.open_attempted = true;
            let opened = openat(
                &self.inner.file,
                name,
                access | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
                Mode::RUSR | Mode::WUSR,
            )
            .map(File::from)
            .map_err(io::Error::from);
            let opened = match opened {
                Ok(file) => {
                    scope.raw_file = Some(file);
                    scope.progress.descriptor_opened = true;
                    Ok(())
                }
                Err(error) => Err(DirectoryOwnershipError::from(error)),
            };
            #[cfg(test)]
            tests::completed("file_open");
            let present = if optional {
                budget.completed_optional(opened)?.is_some()
            } else {
                budget.completed(opened)?;
                true
            };
            if !present {
                scope.progress.absence_observed = true;
                validate_directory(self, budget, 0)?;
                return Ok(());
            }
            budget.check()?;
            validate_entry_until(
                &self.inner.file,
                name,
                scope
                    .raw_file
                    .as_ref()
                    .ok_or(DirectoryOwnershipError::IdentityMismatch)?,
                false,
                budget,
            )?;
            validate_directory(self, budget, 0)?;
            let file = scope
                .raw_file
                .take()
                .ok_or(DirectoryOwnershipError::IdentityMismatch)?;
            scope.file = Some(OwnedFileHandle {
                file,
                name: name.to_owned(),
                parent: self.retain(),
            });
            budget.check()
        })();
        match budget.finish(observed) {
            Ok(()) => match scope.file.take() {
                Some(file) => Ok(Some(file)),
                None if optional && scope.progress.absence_observed => Ok(None),
                None => Err(scope.fail(DirectoryOwnershipError::IdentityMismatch.into(), budget)),
            },
            Err(error) => Err(scope.fail(error, budget)),
        }
    }
    /// Inspect one real literal native entry with no-follow statat and E.
    ///
    /// Expected native absence is `None`; every non-symlink real kind is an
    /// ordinary observed trace. This grants no native/source/serving authority.
    pub fn entry_kind_until(
        &self,
        name: &OsStr,
        budget: &NativeDirectoryBudget,
    ) -> std::result::Result<Option<OwnedEntryKind>, RetainedDirectoryError> {
        let mut scope = Scope::child(self).enter(budget)?;
        let mut result = None;
        let observed = (|| {
            prepare_name(&mut scope, self, name, budget)?;
            validate_directory(self, budget, 0)?;
            let bytes = size_of::<rustix::fs::Stat>() + size_of::<Result<rustix::fs::Stat>>();
            budget.charge(0, 1, bytes, 0, 2, bytes)?;
            budget.check()?;
            let observed = statat(&self.inner.file, name, AtFlags::SYMLINK_NOFOLLOW)
                .map_err(io::Error::from)
                .map_err(DirectoryOwnershipError::from);
            match &observed {
                Ok(stat) => match rustix::fs::FileType::from_raw_mode(stat.st_mode) {
                    rustix::fs::FileType::Symlink => scope.progress.symlink_observed = true,
                    rustix::fs::FileType::Directory => {
                        scope.progress.entry_kind_observed = Some(OwnedEntryKind::Directory)
                    }
                    rustix::fs::FileType::RegularFile => {
                        scope.progress.entry_kind_observed = Some(OwnedEntryKind::RegularFile)
                    }
                    _ => scope.progress.entry_kind_observed = Some(OwnedEntryKind::Other),
                },
                Err(DirectoryOwnershipError::Io(error))
                    if error.kind() == io::ErrorKind::NotFound =>
                {
                    scope.progress.absence_observed = true
                }
                _ => {}
            }
            #[cfg(test)]
            tests::completed("entry_probe");
            let observed = budget.completed_optional(observed)?;
            result = match observed {
                None => {
                    scope.progress.absence_observed = true;
                    None
                }
                Some(stat) => Some(match rustix::fs::FileType::from_raw_mode(stat.st_mode) {
                    rustix::fs::FileType::Symlink => {
                        return Err(DirectoryOwnershipError::IdentityMismatch.into());
                    }
                    rustix::fs::FileType::Directory => OwnedEntryKind::Directory,
                    rustix::fs::FileType::RegularFile => OwnedEntryKind::RegularFile,
                    _ => OwnedEntryKind::Other,
                }),
            };
            validate_directory(self, budget, 0)
        })();
        match budget.finish(observed) {
            Ok(()) => Ok(result),
            Err(error) => Err(scope.fail(error, budget)),
        }
    }
    /// Synchronize the actual pinned directory with recursive original-E checks.
    pub fn sync_until(
        &self,
        budget: &NativeDirectoryBudget,
    ) -> std::result::Result<(), RetainedDirectoryError> {
        let scope = Scope::child(self).enter(budget)?;
        let observed = (|| {
            validate_directory(self, budget, 0)?;
            call(budget, "directory_sync", || {
                self.inner
                    .file
                    .sync_all()
                    .map_err(DirectoryOwnershipError::from)
            })?;
            validate_directory(self, budget, 0)
        })();
        budget
            .finish(observed)
            .map_err(|error| scope.fail(error, budget))
    }
}
impl OwnedFileHandle {
    /// Revalidate the genuine parent/name/opened FD with E between every phase.
    ///
    /// This borrows the caller's actual file; errors retain its same parent/root.
    pub fn validate_until(
        &self,
        budget: &NativeDirectoryBudget,
    ) -> std::result::Result<(), RetainedDirectoryError> {
        let scope = Scope::child(&self.parent).enter(budget)?;
        let observed = (|| {
            validate_directory(&self.parent, budget, 0)?;
            validate_entry_until(
                &self.parent.inner.file,
                &self.name,
                &self.file,
                false,
                budget,
            )
        })();
        budget
            .finish(observed)
            .map_err(|error| scope.fail(error, budget))
    }
}

#[cfg(test)]
#[path = "owned_directory_until_tests.rs"]
mod tests;

#[cfg(test)]
pub(crate) fn completed_native_root_phase(phase: &str) {
    tests::completed(phase);
}
