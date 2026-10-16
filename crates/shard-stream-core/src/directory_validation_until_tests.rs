//! Genuine native directory validation, physical phase clocks and lock custody.

use super::*;
use std::{
    fs,
    os::unix::fs::{MetadataExt, symlink},
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

static SERIAL: Mutex<()> = Mutex::new(());
static PROBE: Mutex<Option<Arc<Probe>>> = Mutex::new(None);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    PathMetadata,
    AncestorMetadata,
    AncestorsValidated,
    DirectoryOpened,
    OpenedMetadata,
    RetainedDirectoryMetadata,
    LockMetadata,
    RetainedLockMetadata,
}

struct Probe {
    path: PathBuf,
    phase: Phase,
    events: Mutex<Vec<Phase>>,
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl Probe {
    fn wait(&self) {
        let state = self.state.lock().expect("actual phase gate");
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(5), |s| !s.0)
            .expect("actual completed native phase");
        assert!(state.0 && !timeout.timed_out());
    }
    fn release(&self) {
        self.state.lock().expect("actual phase gate").1 = true;
        self.changed.notify_all();
    }
}

pub(super) fn completed(owner: &DirectoryOwnership, phase: Phase) {
    let probe = PROBE
        .lock()
        .expect("test observation")
        .as_ref()
        .filter(|p| p.path == owner.canonical_directory)
        .cloned();
    let Some(probe) = probe else { return };
    probe
        .events
        .lock()
        .expect("actual completed calls")
        .push(phase);
    if phase != probe.phase {
        return;
    }
    let mut state = probe.state.lock().expect("actual phase gate");
    state.0 = true;
    probe.changed.notify_all();
    while !state.1 {
        state = probe
            .changed
            .wait(state)
            .expect("physical validation remains entered");
    }
}

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "native-directory-until-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("unique actual native root");
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Waiting {
    probe: Arc<Probe>,
    worker: Option<JoinHandle<Result<(), DirectoryOwnershipUntilError>>>,
}
impl Drop for Waiting {
    fn drop(&mut self) {
        self.probe.release();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        *PROBE.lock().expect("test observation cleanup") = None;
    }
}

#[test]
fn saved_expired_entry_and_fitting_native_validation_preserve_ordinary_owner() {
    let _serial = SERIAL.lock().expect("test phase serialization");
    let tmp = Temp::new();
    let owner = DirectoryOwnership::acquire(&tmp.0).expect("actual native owner");
    let before = fs::metadata(tmp.0.join(super::LOCK_NAME)).expect("actual lock");
    assert!(matches!(
        owner.validate_directory_until(&tmp.0, Instant::now()),
        Err(DirectoryOwnershipUntilError::Expired)
    ));
    assert!(matches!(
        DirectoryOwnership::acquire(&tmp.0),
        Err(DirectoryOwnershipError::Contended)
    ));
    let path = owner.canonical_directory.clone();
    let cost = owner
        .directory_validation_cost(&path)
        .expect("ordinary source cost");
    assert!(
        cost.declared_bytes() >= cost.string_bytes() && cost.work_bytes() >= cost.declared_bytes()
    );
    owner
        .validate_directory_until(&path, Instant::now() + Duration::from_secs(5))
        .expect("actual native policy fits");
    owner
        .validate_directory(&path)
        .expect("unchanged ordinary method");
    let after = fs::metadata(tmp.0.join(super::LOCK_NAME)).expect("same lock");
    assert_eq!(
        (before.dev(), before.ino(), before.len()),
        (after.dev(), after.ino(), after.len())
    );
    assert_eq!(after.len(), 0);
}

#[test]
fn real_deadline_after_each_completed_native_phase_prevents_later_calls() {
    let _serial = SERIAL.lock().expect("test phase serialization");
    for phase in [
        Phase::PathMetadata,
        Phase::AncestorMetadata,
        Phase::AncestorsValidated,
        Phase::DirectoryOpened,
        Phase::OpenedMetadata,
        Phase::RetainedDirectoryMetadata,
        Phase::LockMetadata,
        Phase::RetainedLockMetadata,
    ] {
        let tmp = Temp::new();
        let owner = DirectoryOwnership::acquire(&tmp.0).expect("actual native owner");
        let probe = Arc::new(Probe {
            path: owner.canonical_directory.clone(),
            phase,
            events: Mutex::new(Vec::new()),
            state: Mutex::new((false, false)),
            changed: Condvar::new(),
        });
        *PROBE.lock().expect("test observation") = Some(Arc::clone(&probe));
        let expiry = Instant::now() + Duration::from_secs(2);
        let retained = Arc::clone(&owner);
        let path = tmp.0.clone();
        let mut waiting = Waiting {
            probe: Arc::clone(&probe),
            worker: Some(thread::spawn(move || {
                retained.validate_directory_until(&path, expiry)
            })),
        };
        probe.wait();
        let before = probe.events.lock().expect("actual phase log").clone();
        assert_eq!(before.last(), Some(&phase));
        thread::sleep(expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(10));
        assert!(
            !waiting
                .worker
                .as_ref()
                .expect("actual entered call")
                .is_finished()
        );
        assert!(matches!(
            DirectoryOwnership::acquire(&tmp.0),
            Err(DirectoryOwnershipError::Contended)
        ));
        probe.release();
        let result = waiting
            .worker
            .take()
            .expect("actual worker")
            .join()
            .expect("physically joined");
        assert!(matches!(result, Err(DirectoryOwnershipUntilError::Expired)));
        assert_eq!(
            *probe.events.lock().expect("actual completed phases"),
            before,
            "no new native phase after original expiry"
        );
        owner
            .validate_directory(&tmp.0)
            .expect("same native owner and unchanged source");
        drop(waiting);
    }
}

#[test]
fn actual_last_owner_stays_contended_through_entered_late_validation_cleanup() {
    let _serial = SERIAL.lock().expect("test phase serialization");
    let tmp = Temp::new();
    let owner = DirectoryOwnership::acquire(&tmp.0).expect("actual native owner");
    let probe = Arc::new(Probe {
        path: owner.canonical_directory.clone(),
        phase: Phase::OpenedMetadata,
        events: Mutex::new(Vec::new()),
        state: Mutex::new((false, false)),
        changed: Condvar::new(),
    });
    *PROBE.lock().expect("test observation") = Some(Arc::clone(&probe));
    let expiry = Instant::now() + Duration::from_secs(2);
    let path = tmp.0.clone();
    let mut waiting = Waiting {
        probe: Arc::clone(&probe),
        worker: Some(thread::spawn(move || {
            owner.validate_directory_until(&path, expiry)
        })),
    };
    probe.wait();
    thread::sleep(expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(10));
    assert!(matches!(
        DirectoryOwnership::acquire(&tmp.0),
        Err(DirectoryOwnershipError::Contended)
    ));
    probe.release();
    assert!(matches!(
        waiting
            .worker
            .take()
            .expect("actual worker")
            .join()
            .expect("physically joined"),
        Err(DirectoryOwnershipUntilError::Expired)
    ));
    drop(waiting);
    drop(DirectoryOwnership::acquire(&tmp.0).expect("real last owner released after completion"));
}

#[test]
fn native_identity_symlink_and_lock_link_refusals_match_ordinary_policy() {
    let _serial = SERIAL.lock().expect("test phase serialization");
    let tmp = Temp::new();
    let owned = tmp.0.join("owned");
    let owner = DirectoryOwnership::acquire(&owned).expect("actual native owner");
    let alias = tmp.0.join("alias");
    symlink(&owned, &alias).expect("actual native path symlink");
    for path in [&tmp.0, &alias] {
        assert!(matches!(
            owner.validate_directory(path),
            Err(DirectoryOwnershipError::IdentityMismatch)
        ));
        assert!(matches!(
            owner.validate_directory_until(path, Instant::now() + Duration::from_secs(5)),
            Err(DirectoryOwnershipUntilError::Ownership(
                DirectoryOwnershipError::IdentityMismatch
            ))
        ));
    }
    let lock = owned.join(super::LOCK_NAME);
    fs::hard_link(&lock, tmp.0.join("other-lock-name")).expect("real extra link");
    assert!(matches!(
        owner.validate_directory(&owned),
        Err(DirectoryOwnershipError::IdentityMismatch)
    ));
    assert!(matches!(
        owner.validate_directory_until(&owned, Instant::now() + Duration::from_secs(5)),
        Err(DirectoryOwnershipUntilError::Ownership(
            DirectoryOwnershipError::IdentityMismatch
        ))
    ));
}

#[test]
fn exact_name_refuses_relative_aliases_and_real_renames_before_native_io() {
    let _serial = SERIAL.lock().expect("test phase serialization");
    let tmp = Temp::new();
    let owned = tmp.0.join("owned");
    let owner = DirectoryOwnership::acquire(&owned).expect("actual native owner");
    let probe = Arc::new(Probe {
        path: owner.canonical_directory.clone(),
        phase: Phase::OpenedMetadata,
        events: Mutex::new(Vec::new()),
        state: Mutex::new((false, true)),
        changed: Condvar::new(),
    });
    *PROBE.lock().expect("test observation") = Some(Arc::clone(&probe));
    let _cleanup = Waiting {
        probe: Arc::clone(&probe),
        worker: None,
    };
    let alias = owner.canonical_directory.join(".");
    owner
        .validate_directory(&alias)
        .expect("ordinary spelling remains supported");
    let longer = tmp.0.join("longer-real-parent-".repeat(6));
    fs::create_dir(&longer).expect("actual longer ancestor");
    let renamed = longer.join("owned");
    let before = fs::metadata(&owned).expect("actual owned directory");
    fs::rename(&owned, &renamed).expect("same real directory at longer name");
    let after = fs::metadata(&renamed).expect("actual renamed directory");
    assert_eq!((before.dev(), before.ino()), (after.dev(), after.ino()));
    for path in [Path::new("."), alias.as_path(), renamed.as_path()] {
        assert!(matches!(
            owner.directory_validation_cost(path),
            Err(DirectoryOwnershipUntilError::Ownership(
                DirectoryOwnershipError::IdentityMismatch
            ))
        ));
        assert!(matches!(
            owner.validate_directory_until(path, Instant::now() + Duration::from_secs(5)),
            Err(DirectoryOwnershipUntilError::Ownership(
                DirectoryOwnershipError::IdentityMismatch
            ))
        ));
    }
    assert!(
        probe
            .events
            .lock()
            .expect("actual native phase log")
            .is_empty()
    );
    assert!(matches!(
        DirectoryOwnership::acquire(&renamed),
        Err(DirectoryOwnershipError::Contended)
    ));
    fs::rename(&renamed, &owned).expect("restore the actual retained name");
    owner
        .validate_directory(&owned)
        .expect("ordinary owner still valid");
}

#[test]
fn real_late_ancestor_symlink_cannot_redirect_descriptor_validation() {
    let _serial = SERIAL.lock().expect("test phase serialization");
    let tmp = Temp::new();
    let owned = tmp.0.join("owned");
    let owner = DirectoryOwnership::acquire(&owned).expect("actual native owner");
    let probe = Arc::new(Probe {
        path: owner.canonical_directory.clone(),
        phase: Phase::AncestorsValidated,
        events: Mutex::new(Vec::new()),
        state: Mutex::new((false, false)),
        changed: Condvar::new(),
    });
    *PROBE.lock().expect("test observation") = Some(Arc::clone(&probe));
    let retained = Arc::clone(&owner);
    let path = owner.canonical_directory.clone();
    let mut waiting = Waiting {
        probe: Arc::clone(&probe),
        worker: Some(thread::spawn(move || {
            retained.validate_directory_until(&path, Instant::now() + Duration::from_secs(5))
        })),
    };
    probe.wait();
    let renamed = tmp.0.join("actual-renamed-owner");
    fs::rename(&owned, &renamed).expect("real rename after all ancestor observations");
    symlink(&renamed, &owned).expect("actual late symlink to the SAME owned directory");
    assert!(matches!(
        owner.validate_directory(&owned),
        Err(DirectoryOwnershipError::IdentityMismatch)
    ));
    probe.release();
    assert!(matches!(
        waiting
            .worker
            .take()
            .expect("actual native worker")
            .join()
            .expect("physically joined"),
        Err(DirectoryOwnershipUntilError::Ownership(_))
    ));
    assert!(
        !probe
            .events
            .lock()
            .expect("actual native phase log")
            .contains(&Phase::OpenedMetadata)
    );
    fs::remove_file(&owned).expect("remove only known test symlink");
    fs::rename(&renamed, &owned).expect("restore SAME real native directory");
    owner
        .validate_directory(&owned)
        .expect("same native lock and directory remain owned");
}
