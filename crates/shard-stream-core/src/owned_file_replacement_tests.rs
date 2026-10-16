//! Genuine filesystem regressions for retained bounded atomic replacement.

use super::*;
use crate::NativeDirectoryLimits;
use std::cell::RefCell;
use std::fs;
use std::os::unix::fs::symlink;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

type Observer = Box<dyn FnMut(NativeFileReplacementPhase)>;
thread_local! { static OBSERVER: RefCell<Option<Observer>> = RefCell::new(None); }
pub(super) fn completed(phase: NativeFileReplacementPhase) {
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
fn observe(callback: impl FnMut(NativeFileReplacementPhase) + 'static) -> Observe {
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
        let path = std::env::temp_dir().join(format!(
            "native-file-replacement-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual temporary directory");
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
        .expect("same finite native budget")
}
fn replacement_limits() -> NativeFileReplacementLimits {
    NativeFileReplacementLimits {
        max_body_bytes: 4096,
        max_write_calls: 8,
        max_write_bytes: 2,
    }
}
fn replace(
    root: &OwnedDirectoryHandle,
    paid: &NativeDirectoryBudget,
    body: &[u8],
) -> std::result::Result<NativeFileReplacement, RetainedNativeFileReplacementError> {
    root.replace_file_until(
        OsStr::new(".hlc.tmp"),
        OsStr::new("hlc.json"),
        body.to_vec(),
        replacement_limits(),
        paid,
    )
}
fn contended(path: &Path) {
    assert!(matches!(
        DirectoryOwnership::acquire(path),
        Err(DirectoryOwnershipError::Contended)
    ));
}

#[test]
fn actual_chunked_replacement_retains_body_results_identity_and_original_root() {
    let path = Temporary::new();
    fs::write(path.0.join("hlc.json"), b"old").expect("actual old HLC body");
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual original owner");
    let weak = Arc::downgrade(&owner);
    let root = owner.root_handle().expect("actual pinned root");
    let paid = budget();
    let result = replace(&root, &paid, b"new-HLC").expect("actual bounded replacement");
    assert!(Arc::ptr_eq(result.owner(), &owner));
    assert_eq!(result.body(), b"new-HLC");
    assert_eq!(result.temporary_name(), OsStr::new(".hlc.tmp"));
    assert_eq!(result.destination_name(), OsStr::new("hlc.json"));
    assert_eq!(
        result.file_handle().expect("same actual file").name(),
        OsStr::new("hlc.json")
    );
    assert!(result.renamed() && result.directory_synced());
    assert_eq!(result.temporary_identity(), result.final_identity());
    assert!(result.temporary_identity().is_some());
    assert_eq!(result.written_bytes(), 7);
    assert_eq!(
        fs::read(path.0.join("hlc.json")).expect("actual final body"),
        b"new-HLC"
    );
    assert!(!path.0.join(".hlc.tmp").exists());
    let writes: Vec<_> = result
        .outcomes()
        .iter()
        .filter_map(|outcome| match outcome {
            NativeFileReplacementOutcome::Write {
                offset,
                requested,
                result: Ok(written),
            } => Some((*offset, *requested, *written)),
            _ => None,
        })
        .collect();
    assert_eq!(writes, [(0, 2, 2), (2, 2, 2), (4, 2, 2), (6, 1, 1)]);
    assert_eq!(result.outcomes().len(), 10);
    assert!(
        result
            .outcomes()
            .iter()
            .all(|outcome| outcome.io_error().is_none())
    );
    paid.check().expect("success preserves same shared budget");
    drop(root);
    drop(owner);
    contended(&path.0);
    assert!(weak.upgrade().is_some());
    drop(result);
    assert!(weak.upgrade().is_none());
    DirectoryOwnership::acquire(&path.0).expect("root releases only after real last custody");
}

#[test]
fn every_actual_late_phase_keeps_raw_outcomes_and_rename_sync_settles_before_refusal() {
    for target in [
        NativeFileReplacementPhase::TemporaryMetadata,
        NativeFileReplacementPhase::Write,
        NativeFileReplacementPhase::Flush,
        NativeFileReplacementPhase::SyncData,
        NativeFileReplacementPhase::Rename,
        NativeFileReplacementPhase::SyncDirectory,
        NativeFileReplacementPhase::FinalMetadata,
    ] {
        let path = Temporary::new();
        fs::write(path.0.join("hlc.json"), b"old").expect("old actual body");
        let owner = DirectoryOwnership::acquire(&path.0).expect("actual original owner");
        let root = owner.root_handle().expect("actual root");
        let paid = budget();
        let shared = paid.clone();
        let _observe = observe(move |phase| {
            if phase == target {
                assert!(matches!(
                    shared.cap_expiry(Instant::now()),
                    Err(NativeDirectoryFailure::Expired(_))
                ));
            }
        });
        let error = replace(&root, &paid, b"new").expect_err("same minimum E must refuse");
        assert!(matches!(
            error.failure(),
            NativeFileReplacementFailure::Budget(NativeDirectoryFailure::Expired(_))
        ));
        assert!(Arc::ptr_eq(error.owner(), &owner));
        assert_eq!(error.body(), b"new");
        let actual = error.replacement().expect("actual entered custody");
        assert!(actual.temporary_identity().is_some());
        let phases: Vec<_> = actual
            .outcomes()
            .iter()
            .map(NativeFileReplacementOutcome::phase)
            .collect();
        assert!(phases.contains(&target));
        let renamed = matches!(
            target,
            NativeFileReplacementPhase::Rename
                | NativeFileReplacementPhase::SyncDirectory
                | NativeFileReplacementPhase::FinalMetadata
        );
        assert_eq!(actual.renamed(), renamed);
        assert_eq!(actual.directory_synced(), renamed);
        assert_eq!(
            phases.contains(&NativeFileReplacementPhase::SyncDirectory),
            renamed
        );
        if renamed {
            assert_eq!(
                fs::read(path.0.join("hlc.json")).expect("truthful actual changed namespace"),
                b"new"
            );
            assert!(!path.0.join(".hlc.tmp").exists());
            assert_eq!(
                actual
                    .file_handle()
                    .expect("actual renamed descriptor")
                    .name(),
                OsStr::new("hlc.json")
            );
        } else {
            assert_eq!(
                fs::read(path.0.join("hlc.json")).expect("old final unchanged"),
                b"old"
            );
            assert!(path.0.join(".hlc.tmp").exists());
            assert!(!phases.contains(&NativeFileReplacementPhase::Rename));
        }
        assert_eq!(
            phases.contains(&NativeFileReplacementPhase::FinalMetadata),
            target == NativeFileReplacementPhase::FinalMetadata
        );
        assert!(matches!(
            paid.check(),
            Err(NativeDirectoryFailure::Expired(_))
        ));
        drop(root);
        drop(owner);
        contended(&path.0);
        drop(error);
        DirectoryOwnership::acquire(&path.0).expect("retained error was actual last native owner");
    }
}

#[test]
fn actual_write_attempt_cap_keeps_partial_body_and_enters_no_later_mutation() {
    let path = Temporary::new();
    fs::write(path.0.join("hlc.json"), b"old").expect("actual original body");
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual owner");
    let root = owner.root_handle().expect("actual root");
    let paid = budget();
    let mut cap = replacement_limits();
    cap.max_write_calls = 1;
    let error = root
        .replace_file_until(
            OsStr::new(".hlc.tmp"),
            OsStr::new("hlc.json"),
            b"new".to_vec(),
            cap,
            &paid,
        )
        .expect_err("one real write cannot finish body");
    assert!(matches!(
        error.failure(),
        NativeFileReplacementFailure::Budget(NativeDirectoryFailure::Capacity)
    ));
    let actual = error.replacement().expect("actual partial producer");
    assert_eq!(actual.written_bytes(), 2);
    assert_eq!(
        fs::read(path.0.join(".hlc.tmp")).expect("retained partial actual file"),
        b"ne"
    );
    assert_eq!(
        fs::read(path.0.join("hlc.json")).expect("actual final unchanged"),
        b"old"
    );
    assert!(!actual.renamed());
    assert!(actual.outcomes().iter().all(|outcome| matches!(
        outcome.phase(),
        NativeFileReplacementPhase::TemporaryMetadata | NativeFileReplacementPhase::Write
    )));
    assert!(matches!(paid.check(), Err(NativeDirectoryFailure::Denied)));
}

#[test]
fn actual_existing_temp_is_never_opened_truncated_or_unlinked() {
    let path = Temporary::new();
    fs::write(path.0.join(".hlc.tmp"), b"unrecognized existing temporary")
        .expect("real old temporary");
    fs::write(path.0.join("hlc.json"), b"old").expect("real old final");
    let before = fs::metadata(path.0.join(".hlc.tmp")).expect("old actual identity");
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual owner");
    let root = owner.root_handle().expect("actual root");
    let paid = budget();
    let error = replace(&root, &paid, b"new").expect_err("CreateNew refuses unrecognized name");
    assert!(matches!(
        error.failure(),
        NativeFileReplacementFailure::Directory
    ));
    let actual = error.replacement().expect("entered producer");
    let native = actual
        .directory_error()
        .expect("retained original native error");
    assert!(
        matches!(native.failure(), NativeDirectoryFailure::Ownership(DirectoryOwnershipError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists)
    );
    assert!(actual.outcomes().is_empty());
    assert!(actual.file_handle().is_none());
    assert_eq!(
        fs::read(path.0.join(".hlc.tmp")).expect("original temp preserved"),
        b"unrecognized existing temporary"
    );
    let after = fs::metadata(path.0.join(".hlc.tmp")).expect("same original temp");
    assert_eq!((before.dev(), before.ino()), (after.dev(), after.ino()));
    assert_eq!(
        fs::read(path.0.join("hlc.json")).expect("final preserved"),
        b"old"
    );
}

#[test]
fn actual_late_rename_error_keeps_original_errno_and_never_syncs_or_unlinks_other_names() {
    let path = Temporary::new();
    fs::create_dir(path.0.join("hlc.json")).expect("actual conflicting destination directory");
    fs::write(path.0.join("hlc.json/retained"), b"keep").expect("actual foreign contents");
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual owner");
    let root = owner.root_handle().expect("actual root");
    let paid = budget();
    let shared = paid.clone();
    let _observe = observe(move |phase| {
        if phase == NativeFileReplacementPhase::Rename {
            assert!(shared.cap_expiry(Instant::now()).is_err());
        }
    });
    let error =
        replace(&root, &paid, b"new").expect_err("real conflicting rename and minimum E refuse");
    assert!(matches!(
        error.failure(),
        NativeFileReplacementFailure::Budget(NativeDirectoryFailure::Expired(_))
    ));
    let actual = error.replacement().expect("actual producer custody");
    let raw = actual.outcomes().last().expect("real completed rename");
    assert_eq!(raw.phase(), NativeFileReplacementPhase::Rename);
    assert!(
        raw.io_error()
            .expect("actual rename error")
            .raw_os_error()
            .is_some()
    );
    assert!(!actual.renamed() && !actual.directory_synced());
    assert_eq!(
        fs::read(path.0.join(".hlc.tmp")).expect("actual retained temporary"),
        b"new"
    );
    assert_eq!(
        fs::read(path.0.join("hlc.json/retained")).expect("actual foreign name preserved"),
        b"keep"
    );
    assert!(error.source().is_some());
}

#[test]
fn actual_temporary_symlink_substitution_refuses_before_rename_and_keeps_original_descriptor() {
    let path = Temporary::new();
    fs::write(path.0.join("hlc.json"), b"old").expect("actual old final");
    fs::write(path.0.join("victim"), b"do not change").expect("actual other file");
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual owner");
    let root = owner.root_handle().expect("actual root");
    let paid = budget();
    let directory = path.0.clone();
    let _observe = observe(move |phase| {
        if phase == NativeFileReplacementPhase::SyncData {
            fs::rename(
                directory.join(".hlc.tmp"),
                directory.join("retained-original-temp"),
            )
            .expect("move actual temporary");
            symlink("victim", directory.join(".hlc.tmp")).expect("actual substituted symlink");
        }
    });
    let error = replace(&root, &paid, b"new").expect_err("actual inode mismatch must refuse");
    assert!(matches!(
        error.failure(),
        NativeFileReplacementFailure::Directory
    ));
    let actual = error.replacement().expect("actual partial producer");
    assert!(!actual.renamed());
    assert!(
        !actual
            .outcomes()
            .iter()
            .any(|outcome| outcome.phase() == NativeFileReplacementPhase::Rename)
    );
    let retained = actual
        .file_handle()
        .expect("same original file descriptor")
        .file()
        .metadata()
        .expect("actual retained descriptor identity");
    assert_eq!(
        actual.temporary_identity(),
        Some(NativeFileIdentity {
            device: retained.dev(),
            inode: retained.ino()
        })
    );
    assert_eq!(
        fs::read(path.0.join("retained-original-temp"))
            .expect("original written temporary retained"),
        b"new"
    );
    assert_eq!(
        fs::read(path.0.join("victim")).expect("other file unchanged"),
        b"do not change"
    );
    assert_eq!(
        fs::read(path.0.join("hlc.json")).expect("final unchanged"),
        b"old"
    );
    assert!(
        fs::symlink_metadata(path.0.join(".hlc.tmp"))
            .expect("substituted name retained")
            .file_type()
            .is_symlink()
    );
}

#[test]
fn actual_cumulative_fees_fit_exactly_and_one_less_call_retains_changed_namespace() {
    let path = Temporary::new();
    let owner = DirectoryOwnership::acquire(&path.0).expect("actual owner");
    let root = owner.root_handle().expect("same actual root");
    let generous = budget();
    drop(replace(&root, &generous, b"new").expect("actual first physical replacement"));
    let used = generous.usage().expect("actual prepaid native fees");
    let mut exact = limits();
    exact.max_operations = used.operations;
    exact.max_calls = used.calls;
    exact.max_entries = used.entries;
    exact.max_declared_bytes = used.declared_bytes;
    exact.max_work_bytes = used.work_bytes;
    let fitting = NativeDirectoryBudget::new(exact, Instant::now() + Duration::from_secs(30))
        .expect("same exact finite allowance");
    let actual = replace(&root, &fitting, b"new").expect("all actual exact fees fit");
    assert_eq!(fitting.usage().expect("same fees"), used);
    assert_eq!(
        actual
            .budget()
            .usage()
            .expect("retained SAME counter values"),
        used
    );
    drop(actual);
    exact.max_calls -= 1;
    let short = NativeDirectoryBudget::new(exact, Instant::now() + Duration::from_secs(30))
        .expect("one fewer physical call allowance");
    let error =
        replace(&root, &short, b"new").expect_err("final physical observation cannot enter");
    assert!(matches!(
        error.failure(),
        NativeFileReplacementFailure::Budget(NativeDirectoryFailure::Capacity)
    ));
    let actual = error
        .replacement()
        .expect("truthful partial native custody");
    assert!(actual.renamed() && actual.directory_synced());
    assert!(actual.final_identity().is_none());
    assert_eq!(
        actual
            .outcomes()
            .last()
            .expect("last actual entered phase")
            .phase(),
        NativeFileReplacementPhase::SyncDirectory
    );
    assert_eq!(
        fs::read(path.0.join("hlc.json")).expect("actual namespace was changed and settled"),
        b"new"
    );
    assert!(matches!(short.check(), Err(NativeDirectoryFailure::Denied)));
}

#[test]
fn pre_entry_expiry_name_capacity_and_body_capacity_refusals_keep_input_and_root() {
    for kind in 0..5 {
        let path = Temporary::new();
        let owner = DirectoryOwnership::acquire(&path.0).expect("actual owner");
        let weak = Arc::downgrade(&owner);
        let root = owner.root_handle().expect("actual root");
        let paid = budget();
        let mut body = Vec::with_capacity(32);
        body.extend_from_slice(b"new");
        let mut cap = replacement_limits();
        let (temporary, destination) = match kind {
            0 => {
                assert!(paid.cap_expiry(Instant::now()).is_err());
                (OsStr::new(".hlc.tmp"), OsStr::new("hlc.json"))
            }
            1 => (OsStr::new("../outside"), OsStr::new("hlc.json")),
            2 => (OsStr::new("hlc.json"), OsStr::new("hlc.json")),
            3 => (OsStr::new("nested/"), OsStr::new("hlc.json")),
            _ => {
                cap.max_body_bytes = 3;
                (OsStr::new(".hlc.tmp"), OsStr::new("hlc.json"))
            }
        };
        let error = root
            .replace_file_until(temporary, destination, body, cap, &paid)
            .expect_err("invalid/expired entry refuses before producer allocation");
        assert!(error.replacement().is_none());
        assert_eq!(error.body(), b"new");
        assert!(Arc::ptr_eq(error.owner(), &owner));
        assert_eq!(
            error.budget().usage().expect("no entry fees spent"),
            Default::default()
        );
        assert!(!path.0.join(".hlc.tmp").exists() && !path.0.join("hlc.json").exists());
        drop(root);
        drop(owner);
        contended(&path.0);
        assert!(weak.upgrade().is_some());
        drop(error);
        assert!(weak.upgrade().is_none());
        DirectoryOwnership::acquire(&path.0)
            .expect("actual root released after retained early refusal");
    }
}

#[test]
fn actual_phase_unwind_denies_first_retains_custody_and_settles_rename_sync_once() {
    for target in [
        NativeFileReplacementPhase::TemporaryMetadata,
        NativeFileReplacementPhase::Write,
        NativeFileReplacementPhase::Flush,
        NativeFileReplacementPhase::SyncData,
        NativeFileReplacementPhase::Rename,
        NativeFileReplacementPhase::SyncDirectory,
        NativeFileReplacementPhase::FinalMetadata,
    ] {
        let path = Temporary::new();
        fs::write(path.0.join("hlc.json"), b"old").expect("actual old final");
        let owner = DirectoryOwnership::acquire(&path.0).expect("actual owner");
        let weak = Arc::downgrade(&owner);
        let root = owner.root_handle().expect("actual root");
        let paid = budget();
        let _observe = observe(move |phase| {
            if phase == target {
                panic!("actual completed replacement phase unwound");
            }
        });
        let error =
            replace(&root, &paid, b"new").expect_err("actual unwind returns retained denial");
        assert!(matches!(
            error.failure(),
            NativeFileReplacementFailure::Unwind
        ));
        assert!(matches!(paid.check(), Err(NativeDirectoryFailure::Denied)));
        assert_eq!(error.body(), b"new");
        let actual = error
            .replacement()
            .expect("outside carrier survives real catch_unwind");
        assert_eq!(
            actual
                .panic_payload()
                .expect("original panic payload")
                .downcast_ref::<&'static str>(),
            Some(&"actual completed replacement phase unwound")
        );
        assert!(actual.settlement_panic_payload().is_none());
        assert!(actual.unfinished_phase().is_none());
        assert!(
            actual
                .outcomes()
                .iter()
                .any(|outcome| outcome.phase() == target)
        );
        let renamed = matches!(
            target,
            NativeFileReplacementPhase::Rename
                | NativeFileReplacementPhase::SyncDirectory
                | NativeFileReplacementPhase::FinalMetadata
        );
        assert_eq!(actual.renamed(), renamed);
        assert_eq!(actual.directory_sync_attempted(), renamed);
        assert_eq!(actual.directory_synced(), renamed);
        assert_eq!(
            actual
                .outcomes()
                .iter()
                .filter(|outcome| outcome.phase() == NativeFileReplacementPhase::SyncDirectory)
                .count(),
            usize::from(renamed)
        );
        assert_eq!(
            fs::read(path.0.join("hlc.json")).expect("actual namespace outcome"),
            if renamed { b"new" } else { b"old" }
        );
        if renamed {
            assert!(!path.0.join(".hlc.tmp").exists());
            assert_eq!(
                actual
                    .file_handle()
                    .expect("same actual renamed file")
                    .name(),
                OsStr::new("hlc.json")
            );
        } else {
            assert!(path.0.join(".hlc.tmp").exists());
        }
        drop(root);
        drop(owner);
        assert!(weak.upgrade().is_some());
        contended(&path.0);
        drop(error);
        assert!(weak.upgrade().is_none());
        DirectoryOwnership::acquire(&path.0)
            .expect("actual custody physically releases after error drop");
    }
}
