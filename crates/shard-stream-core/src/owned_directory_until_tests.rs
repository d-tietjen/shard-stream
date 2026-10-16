use super::*;
use crate::{NativeDirectoryLimits, native_directory_cost};
use std::cell::RefCell;
use std::fs;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

type Observer = Box<dyn FnMut(&str)>;
thread_local! { static OBSERVER: RefCell<Option<Observer>> = RefCell::new(None); }
pub(super) fn completed(phase: &str) {
    OBSERVER.with(|observer| {
        if let Some(observer) = &mut *observer.borrow_mut() {
            observer(phase);
        }
    });
}
struct Observe;
impl Drop for Observe {
    fn drop(&mut self) {
        OBSERVER.with(|observer| *observer.borrow_mut() = None);
    }
}
fn observe(callback: impl FnMut(&str) + 'static) -> Observe {
    OBSERVER.with(|observer| {
        assert!(observer.borrow().is_none());
        *observer.borrow_mut() = Some(Box::new(callback));
    });
    Observe
}
struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "native-handles-original-e-{}-{now}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("real native directory");
        Self(path)
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn limits() -> NativeDirectoryLimits {
    NativeDirectoryLimits {
        max_depth: 16,
        max_component_bytes: 4096,
        max_path_bytes: 8192,
        max_operations: 1024,
        max_calls: 1_000_000,
        max_entries: 1_000_000,
        max_declared_bytes: 1 << 26,
        max_work_bytes: 1 << 30,
    }
}
fn budget() -> NativeDirectoryBudget {
    NativeDirectoryBudget::new(limits(), Instant::now() + Duration::from_secs(30))
        .expect("actual finite native counter owner")
}
fn late(expiry: Instant) {
    std::thread::sleep(expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(2));
}
fn contended(path: &Path) {
    assert!(matches!(
        DirectoryOwnership::acquire(path),
        Err(DirectoryOwnershipError::Contended)
    ));
}

#[test]
fn actual_root_child_file_chain_keeps_exact_owner_and_native_nontruncating_behavior() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual original owner");
    let weak = Arc::downgrade(&owner);
    let paid = budget();
    let root = owner
        .root_handle_until(&paid)
        .expect("actual original root clone");
    let child = root
        .open_directory_until(OsStr::new("ha-metadata"), true, &paid)
        .expect("actual child mkdir/open");
    let file = child
        .open_file_until(
            OsStr::new("metadata.ssm"),
            OwnedFileAccess::ReadWriteCreate,
            &paid,
        )
        .expect("actual metadata file");
    assert!(Arc::ptr_eq(file.owner(), &owner));
    assert!(Arc::ptr_eq(child.owner(), &owner));
    (&file.file)
        .write_all(b"actual native metadata bytes")
        .expect("real fixture writer");
    file.file.sync_all().expect("real fixture durability");
    file.validate_until(&paid)
        .expect("recursive original-E native file checks");
    child
        .sync_until(&paid)
        .expect("real native directory synchronization");
    let reopened = child
        .open_file_until(
            OsStr::new("metadata.ssm"),
            OwnedFileAccess::ReadWriteCreate,
            &paid,
        )
        .expect("native opening does not truncate");
    let mut bytes = Vec::new();
    (&reopened.file)
        .read_to_end(&mut bytes)
        .expect("actual existing bytes");
    assert_eq!(bytes, b"actual native metadata bytes");
    drop(owner);
    drop(root);
    drop(child);
    drop(file);
    contended(&path.0);
    assert!(weak.upgrade().is_some());
    drop(reopened);
    assert!(weak.upgrade().is_none());
    DirectoryOwnership::acquire(&path.0).expect("actual last root released");
}

#[test]
fn actual_cumulative_native_chain_exact_fees_fit_and_one_less_call_refuses_stickily() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual owner");
    let run = |budget: &NativeDirectoryBudget| {
        let root = owner.root_handle_until(budget)?;
        let child = root.open_directory_until(OsStr::new("ha-metadata"), true, budget)?;
        child.open_file_until(
            OsStr::new("metadata.ssm"),
            OwnedFileAccess::ReadWriteCreate,
            budget,
        )
    };
    let generous = budget();
    drop(run(&generous).expect("real native chain"));
    let paid = generous.usage().expect("actual cumulative native fees");
    let mut exact = limits();
    exact.max_calls = paid.calls;
    exact.max_entries = paid.entries;
    exact.max_declared_bytes = paid.declared_bytes;
    exact.max_work_bytes = paid.work_bytes;
    exact.max_operations = paid.operations;
    let fitting = NativeDirectoryBudget::new(exact, Instant::now() + Duration::from_secs(30))
        .expect("exact actual ceilings");
    drop(run(&fitting).expect("all actual exact logical fees fit"));
    assert_eq!(fitting.usage().expect("same native fees"), paid);
    exact.max_calls -= 1;
    let short = NativeDirectoryBudget::new(exact, Instant::now() + Duration::from_secs(30))
        .expect("one fewer actual call allowance");
    let error = match run(&short) {
        Ok(_) => panic!("one fewer native call must refuse"),
        Err(error) => error,
    };
    assert!(matches!(error.failure(), NativeDirectoryFailure::Capacity));
    assert!(Arc::ptr_eq(error.owner(), &owner));
    assert!(matches!(short.check(), Err(NativeDirectoryFailure::Denied)));
    assert_eq!(
        fs::read(path.0.join("ha-metadata/metadata.ssm")).expect("actual retained name"),
        b""
    );
    let cost = native_directory_cost(exact).expect("plain actual domain ceiling");
    assert!(
        cost.declared_bytes >= exact.max_declared_bytes && cost.work_bytes >= exact.max_work_bytes
    );
}

#[test]
fn late_actual_mkdir_retains_known_effect_and_starts_no_directory_open() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual owner");
    let root = owner.root_handle().expect("actual pinned root fixture");
    let expiry = Instant::now() + Duration::from_secs(1);
    let paid = NativeDirectoryBudget::new(limits(), expiry).expect("original E");
    let phases = std::rc::Rc::new(RefCell::new(Vec::new()));
    let recorded = phases.clone();
    let _observer = observe(move |phase| {
        recorded.borrow_mut().push(phase.to_owned());
        if phase == "mkdir" {
            late(expiry);
        }
    });
    let error = match root.open_directory_until(OsStr::new("new-directory"), true, &paid) {
        Ok(_) => panic!("actual mkdir completed late"),
        Err(error) => error,
    };
    assert!(matches!(
        error.failure(),
        NativeDirectoryFailure::Expired(_)
    ));
    assert!(error.progress().mkdir_attempted && error.progress().directory_created);
    assert!(!error.progress().open_attempted);
    assert_eq!(
        phases
            .borrow()
            .last()
            .expect("actual completed native phase"),
        "mkdir"
    );
    assert!(
        path.0.join("new-directory").is_dir(),
        "actual partial mkdir remains evidence"
    );
    drop(root);
    drop(owner);
    contended(&path.0);
    drop(error);
    DirectoryOwnership::acquire(&path.0).expect("real error owner was last");
}

#[test]
fn late_actual_root_clone_and_file_open_retain_real_descriptor_without_later_stat_or_unlink() {
    for root_clone in [true, false] {
        let path = Temporary::new();
        let owner = DirectoryOwnership::acquire(&path.0).expect("actual owner");
        let root = owner.root_handle().expect("actual root fixture");
        fs::write(path.0.join("known.ssm"), b"unknown original bytes")
            .expect("actual preexisting native evidence");
        let expiry = Instant::now() + Duration::from_secs(1);
        let paid = NativeDirectoryBudget::new(limits(), expiry).expect("saved E");
        let phases = std::rc::Rc::new(RefCell::new(Vec::new()));
        let recorded = phases.clone();
        let wanted = if root_clone {
            "root_clone"
        } else {
            "file_open"
        };
        let _observer = observe(move |phase| {
            recorded.borrow_mut().push(phase.to_owned());
            if phase == wanted {
                late(expiry);
            }
        });
        let error = if root_clone {
            match owner.root_handle_until(&paid) {
                Ok(_) => panic!("real root clone late"),
                Err(error) => error,
            }
        } else {
            match root.open_file_until(
                OsStr::new("known.ssm"),
                OwnedFileAccess::ReadWriteCreate,
                &paid,
            ) {
                Ok(_) => panic!("real file open late"),
                Err(error) => error,
            }
        };
        assert!(matches!(
            error.failure(),
            NativeDirectoryFailure::Expired(_)
        ));
        assert!(error.progress().open_attempted && error.progress().descriptor_opened);
        assert!(error.retains_partial_descriptor());
        assert_eq!(
            phases.borrow().last().expect("actual completed phase"),
            wanted
        );
        assert_eq!(
            fs::read(path.0.join("known.ssm")).expect("unknown bytes preserved"),
            b"unknown original bytes"
        );
        drop(root);
        drop(owner);
        contended(&path.0);
        drop(error);
        DirectoryOwnership::acquire(&path.0)
            .expect("actual partial descriptor and root physically dropped");
    }
}

#[test]
fn native_name_replacement_after_open_refuses_identity_and_preserves_both_real_names() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("real owner");
    let root = owner.root_handle().expect("real root");
    fs::write(path.0.join("known.ssm"), b"original native inode").expect("real original");
    let named = path.0.join("known.ssm");
    let old = path.0.join("retained.ssm");
    let _observer = observe(move |phase| {
        if phase == "file_open" {
            fs::rename(&named, &old).expect("actual rename after native open");
            std::os::unix::fs::symlink("retained.ssm", &named).expect("actual symlink replacement");
        }
    });
    let error = match root.open_file_until(
        OsStr::new("known.ssm"),
        OwnedFileAccess::ReadOnly,
        &budget(),
    ) {
        Ok(_) => panic!("replacement native name cannot pass identity"),
        Err(error) => error,
    };
    assert!(matches!(
        error.failure(),
        NativeDirectoryFailure::Ownership(DirectoryOwnershipError::IdentityMismatch)
    ));
    assert!(error.retains_partial_descriptor());
    assert_eq!(
        fs::read(path.0.join("retained.ssm")).expect("original retained inode"),
        b"original native inode"
    );
    assert!(
        fs::symlink_metadata(path.0.join("known.ssm"))
            .expect("unknown replacement remains")
            .file_type()
            .is_symlink()
    );
}

#[test]
fn recursive_actual_parent_validation_stops_between_metadata_phases_and_depth_refuses_before_io() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("real owner");
    let root = owner.root_handle().expect("real root");
    let child = root
        .open_directory(OsStr::new("a"), true)
        .expect("actual child a");
    let child = child
        .open_directory(OsStr::new("b"), true)
        .expect("actual child b");
    let file = child
        .open_file(OsStr::new("known.ssm"), OwnedFileAccess::CreateNew)
        .expect("actual target file");
    let mut shallow = limits();
    shallow.max_depth = 1;
    let paid = NativeDirectoryBudget::new(shallow, Instant::now() + Duration::from_secs(30))
        .expect("actual shallow cap");
    let error = file
        .validate_until(&paid)
        .expect_err("actual retained depth exceeds one");
    assert!(matches!(error.failure(), NativeDirectoryFailure::Capacity));
    assert_eq!(
        paid.usage()
            .expect("no native IO before depth refusal")
            .calls,
        0
    );
    let expiry = Instant::now() + Duration::from_secs(1);
    let paid = NativeDirectoryBudget::new(limits(), expiry).expect("original E");
    let phases = std::rc::Rc::new(RefCell::new(Vec::new()));
    let recorded = phases.clone();
    let _observer = observe(move |phase| {
        recorded.borrow_mut().push(phase.to_owned());
        if phase == "entry_metadata" {
            late(expiry);
        }
    });
    let error = file
        .validate_until(&paid)
        .expect_err("actual ancestor entry metadata completed late");
    assert!(matches!(
        error.failure(),
        NativeDirectoryFailure::Expired(_)
    ));
    assert_eq!(
        phases.borrow().last().expect("real last phase"),
        "entry_metadata"
    );
    assert!(!phases.borrow().iter().any(|phase| phase == "entry_fstat"));
    assert!(Arc::ptr_eq(error.owner(), &owner));
    assert_eq!(paid.absolute_expiry(), expiry);
}

#[test]
fn late_actual_open_error_preserves_real_notfound_and_does_not_enter_later_native_stage() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual owner");
    let root = owner.root_handle().expect("actual root");
    let expiry = Instant::now() + Duration::from_secs(1);
    let paid = NativeDirectoryBudget::new(limits(), expiry).expect("saved E");
    let _observer = observe(move |phase| {
        if phase == "file_open" {
            late(expiry);
        }
    });
    let error =
        match root.open_file_until(OsStr::new("absent.ssm"), OwnedFileAccess::ReadOnly, &paid) {
            Ok(_) => panic!("actual absent file"),
            Err(error) => error,
        };
    assert!(
        matches!(error.failure(), NativeDirectoryFailure::Expired(Some(DirectoryOwnershipError::Io(error))) if error.kind() == io::ErrorKind::NotFound)
    );
    assert!(error.progress().open_attempted && !error.progress().descriptor_opened);
    assert!(!path.0.join("absent.ssm").exists());
}

#[test]
fn same_budget_min_only_cap_survives_clones_and_stops_after_real_completed_file_open() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual native owner");
    let root = owner.root_handle().expect("actual native root");
    fs::write(
        path.0.join("known.ssm"),
        b"actual retained before common minimum",
    )
    .expect("real original bytes");
    let original = Instant::now() + Duration::from_secs(30);
    let earlier = Instant::now() + Duration::from_secs(1);
    let paid = NativeDirectoryBudget::new(limits(), original).expect("actual original E");
    let shared = paid.clone();
    let before = paid.usage().expect("actual unchanged usage");
    shared
        .cap_expiry(earlier)
        .expect("real earlier minimum stored");
    paid.cap_expiry(original + Duration::from_secs(30))
        .expect("larger cap cannot extend actual E");
    assert_eq!(paid.absolute_expiry(), earlier);
    assert_eq!(paid.limits(), limits());
    assert_eq!(paid.usage().expect("no reset or new fees"), before);
    let phases = std::rc::Rc::new(RefCell::new(Vec::new()));
    let recorded = phases.clone();
    let _observer = observe(move |phase| {
        recorded.borrow_mut().push(phase.to_owned());
        if phase == "file_open" {
            late(earlier);
        }
    });
    let error =
        match root.open_file_until(OsStr::new("known.ssm"), OwnedFileAccess::ReadOnly, &paid) {
            Ok(_) => panic!("actual opened result completed after common min"),
            Err(error) => error,
        };
    assert!(
        Instant::now() < original,
        "the original larger E still fits"
    );
    assert!(matches!(
        error.failure(),
        NativeDirectoryFailure::Expired(_)
    ));
    assert_eq!(
        phases.borrow().last().expect("actual real native phase"),
        "file_open"
    );
    assert!(error.retains_partial_descriptor());
    let spent = paid.usage().expect("actual spent counters");
    assert!(matches!(
        shared.cap_expiry(original),
        Err(NativeDirectoryFailure::Expired(_))
    ));
    assert_eq!(paid.absolute_expiry(), earlier);
    assert_eq!(
        shared
            .usage()
            .expect("same actual counters after refused revival"),
        spent
    );
    assert_eq!(
        fs::read(path.0.join("known.ssm")).expect("original bytes untouched"),
        b"actual retained before common minimum"
    );
}

#[test]
fn real_optional_absence_and_native_trace_kinds_do_not_deny_fitting_first_boot_budget() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual first-boot owner");
    let paid = budget();
    let root = owner
        .root_handle_until(&paid)
        .expect("actual bounded original root");
    assert!(
        root.open_optional_file_until(OsStr::new("cluster.identity"), &paid)
            .expect("actual absent native file")
            .is_none()
    );
    assert_eq!(
        root.entry_kind_until(OsStr::new("coordinator.ssj"), &paid)
            .expect("actual absent native journal"),
        None
    );
    paid.check()
        .expect("expected absences did not poison carried budget");
    fs::write(path.0.join("coordinator.ssj"), b"actual journal trace")
        .expect("real native trace fixture");
    assert_eq!(
        root.entry_kind_until(OsStr::new("coordinator.ssj"), &paid)
            .expect("actual native nofollow probe"),
        Some(OwnedEntryKind::RegularFile)
    );
    let child = root
        .open_directory_until(OsStr::new("shards"), true, &paid)
        .expect("real native directory trace");
    assert_eq!(
        root.entry_kind_until(OsStr::new("shards"), &paid)
            .expect("actual native kind"),
        Some(OwnedEntryKind::Directory)
    );
    let socket = std::os::unix::net::UnixListener::bind(path.0.join("socket-trace"))
        .expect("another real native trace kind");
    assert_eq!(
        root.entry_kind_until(OsStr::new("socket-trace"), &paid)
            .expect("other native kind still observed"),
        Some(OwnedEntryKind::Other)
    );
    let file = root
        .open_optional_file_until(OsStr::new("coordinator.ssj"), &paid)
        .expect("same carried budget progresses")
        .expect("actual present file");
    file.validate_until(&paid).expect("real file validation");
    let wrong = match root.open_optional_file_until(OsStr::new("shards"), &budget()) {
        Ok(_) => panic!("a real directory cannot be optional regular data"),
        Err(error) => error,
    };
    assert!(matches!(
        wrong.failure(),
        NativeDirectoryFailure::Ownership(DirectoryOwnershipError::IdentityMismatch)
    ));
    std::os::unix::fs::symlink("coordinator.ssj", path.0.join("alias"))
        .expect("real symlink trace");
    let rejected = root
        .entry_kind_until(OsStr::new("alias"), &budget())
        .expect_err("native symlink refusal");
    assert!(matches!(
        rejected.failure(),
        NativeDirectoryFailure::Ownership(DirectoryOwnershipError::IdentityMismatch)
    ));
    assert!(rejected.progress().symlink_observed);
    drop(socket);
    drop(child);
}

#[test]
fn actual_native_optional_notfound_and_probe_kind_survive_late_phase_without_later_work() {
    for probe in [false, true] {
        let path = Temporary::new();
        let owner = DirectoryOwnership::acquire(&path.0).expect("actual owner");
        let root = owner.root_handle().expect("actual root");
        fs::write(
            path.0.join("coordinator.ssj"),
            b"real retained journal trace",
        )
        .expect("actual native bytes");
        let expiry = Instant::now() + Duration::from_secs(1);
        let paid = NativeDirectoryBudget::new(limits(), expiry).expect("same real E");
        let phases = std::rc::Rc::new(RefCell::new(Vec::new()));
        let recorded = phases.clone();
        let wanted = if probe { "entry_probe" } else { "file_open" };
        let _observer = observe(move |phase| {
            recorded.borrow_mut().push(phase.to_owned());
            if phase == wanted {
                late(expiry);
            }
        });
        let error = if probe {
            root.entry_kind_until(OsStr::new("coordinator.ssj"), &paid)
                .expect_err("real probe completed late")
        } else {
            match root.open_optional_file_until(OsStr::new("absent.ssm"), &paid) {
                Ok(_) => panic!("late expected absence cannot succeed"),
                Err(error) => error,
            }
        };
        if probe {
            assert_eq!(
                error.progress().entry_kind_observed,
                Some(OwnedEntryKind::RegularFile)
            );
        } else {
            assert!(
                matches!(error.failure(), NativeDirectoryFailure::Expired(Some(DirectoryOwnershipError::Io(error))) if error.kind() == io::ErrorKind::NotFound)
            );
        }
        assert_eq!(
            phases.borrow().last().expect("actual last native phase"),
            wanted
        );
        assert_eq!(
            fs::read(path.0.join("coordinator.ssj")).expect("actual source preserved"),
            b"real retained journal trace"
        );
    }
}

#[test]
fn tightening_same_min_inside_actual_native_root_phase_starts_no_later_ancestor_io() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual root owner");
    let original = Instant::now() + Duration::from_secs(30);
    let paid = NativeDirectoryBudget::new(limits(), original).expect("actual shared original E");
    let capped = paid.clone();
    let phases = std::rc::Rc::new(RefCell::new(Vec::new()));
    let recorded = phases.clone();
    let _observer = observe(move |phase| {
        recorded.borrow_mut().push(phase.to_owned());
        if phase == "PathMetadata" {
            assert!(matches!(
                capped.cap_expiry(Instant::now()),
                Err(NativeDirectoryFailure::Expired(_))
            ));
        }
    });
    let error = match owner.root_handle_until(&paid) {
        Ok(_) => panic!("real native root phase tightened earlier E"),
        Err(error) => error,
    };
    assert!(matches!(
        error.failure(),
        NativeDirectoryFailure::Expired(_)
    ));
    assert_eq!(phases.borrow().as_slice(), &["PathMetadata"]);
    assert!(!error.progress().open_attempted);
    assert!(Instant::now() < original);
    assert!(paid.absolute_expiry() < original);
    assert!(Arc::ptr_eq(error.owner(), &owner));
}

#[test]
fn real_optional_native_directory_absence_preserves_carried_budget_without_mkdir() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual owner");
    let paid = budget();
    let root = owner.root_handle_until(&paid).expect("actual root");
    assert!(
        root.open_optional_directory_until(OsStr::new("new-generation"), &paid)
            .expect("fitting actual absent directory")
            .is_none()
    );
    assert!(
        !path.0.join("new-generation").exists(),
        "optional producer did not mkdir"
    );
    paid.check().expect("real absence preserves shared budget");
    let child = root
        .open_directory_until(OsStr::new("new-generation"), true, &paid)
        .expect("later actual creation uses same counters");
    let found = root
        .open_optional_directory_until(OsStr::new("new-generation"), &paid)
        .expect("actual present directory")
        .expect("real native directory");
    assert!(Arc::ptr_eq(found.owner(), &owner));
    assert_eq!(found.path(), child.path());
    fs::write(path.0.join("wrong-kind"), b"actual file bytes").expect("real wrong-kind evidence");
    let wrong = budget();
    assert!(
        root.open_optional_directory_until(OsStr::new("wrong-kind"), &wrong)
            .is_err()
    );
    assert!(matches!(wrong.check(), Err(NativeDirectoryFailure::Denied)));
    assert_eq!(
        fs::read(path.0.join("wrong-kind")).expect("actual unknown file kept"),
        b"actual file bytes"
    );
}

#[test]
fn actual_missing_retained_root_keeps_native_notfound_before_and_after_shared_min_expiry() {
    for expires_after_result in [false, true] {
        let path = Temporary::new();
        let owner = DirectoryOwnership::acquire(&path.0).expect("actual original directory owner");
        let weak = Arc::downgrade(&owner);
        fs::write(
            path.0.join("metadata.ssm"),
            b"actual retained metadata source",
        )
        .expect("actual source before physical rename");
        let moved = path.0.with_extension("actual-moved-owner");
        fs::rename(&path.0, &moved).expect("actual retained directory inode renamed");
        let original = Instant::now() + Duration::from_secs(30);
        let paid = NativeDirectoryBudget::new(limits(), original).expect("same real original E");
        let capped = paid.clone();
        let phases = std::rc::Rc::new(RefCell::new(Vec::new()));
        let recorded = phases.clone();
        let _observer = observe(move |phase| {
            recorded.borrow_mut().push(phase.to_owned());
            if expires_after_result && phase == "PathMetadata" {
                assert!(matches!(
                    capped.cap_expiry(Instant::now()),
                    Err(NativeDirectoryFailure::Expired(_))
                ));
            }
        });
        let error = match owner.root_handle_until(&paid) {
            Ok(_) => panic!("actual retained canonical name is absent"),
            Err(error) => error,
        };
        if expires_after_result {
            assert!(
                matches!(error.failure(), NativeDirectoryFailure::Expired(Some(DirectoryOwnershipError::Io(error))) if error.kind() == io::ErrorKind::NotFound && error.raw_os_error().is_some())
            );
            assert!(paid.absolute_expiry() < original);
        } else {
            assert!(
                matches!(error.failure(), NativeDirectoryFailure::Ownership(DirectoryOwnershipError::Io(error)) if error.kind() == io::ErrorKind::NotFound && error.raw_os_error().is_some())
            );
            assert_eq!(paid.absolute_expiry(), original);
            assert!(matches!(paid.check(), Err(NativeDirectoryFailure::Denied)));
        }
        assert_eq!(phases.borrow().as_slice(), &["PathMetadata"]);
        assert!(!error.progress().open_attempted);
        assert!(Arc::ptr_eq(error.owner(), &owner));
        assert!(Instant::now() < original);
        contended(&moved);
        fs::rename(&moved, &path.0).expect("restore only the real test directory name");
        assert_eq!(
            fs::read(path.0.join("metadata.ssm")).expect("same actual source bytes"),
            b"actual retained metadata source"
        );
        drop(owner);
        assert!(
            weak.upgrade().is_some(),
            "actual refused owner retains the same root"
        );
        contended(&path.0);
        drop(error);
        assert!(weak.upgrade().is_none());
        DirectoryOwnership::acquire(&path.0)
            .expect("last actual refused owner physically released");
    }
}
