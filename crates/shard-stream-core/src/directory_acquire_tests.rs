//! Actual filesystem/lock acquisition and retained settlement regressions.
//! Hooks observe captured native resources/results; they supply no authority.

use super::*;
use std::{
    ffi::OsString,
    os::{
        fd::AsRawFd,
        unix::{ffi::OsStringExt, fs::symlink},
    },
    sync::{
        Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

struct Temp(PathBuf);
impl Temp {
    fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("actual fixture clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "native-acquire-{label}-{}-{stamp}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual unique fixture directory");
        // The API deliberately refuses aliases. Resolve a platform's temp-root
        // alias in fixture setup, rather than weakening the producer's policy.
        Self(fs::canonicalize(path).expect("actual canonical fixture root"))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn limits(created: usize) -> DirectoryAcquireLimits {
    DirectoryAcquireLimits {
        max_path_bytes: 4096,
        max_components: 64,
        max_created_directories: created,
    }
}
fn expiry() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
fn results(error: &RetainedDirectoryAcquireError, phase: DirectoryAcquirePhase) -> Vec<bool> {
    error
        .observations()
        .filter(|o| o.phase() == phase)
        .map(|o| o.succeeded())
        .collect()
}
fn assert_contended(path: &Path) {
    let actual = DirectoryOwnership::acquire_until(path, limits(0), expiry())
        .expect_err("SAME real lock remains held");
    assert!(!actual.owns_lock());
    assert!(
        actual
            .observations()
            .any(|o| o.phase() == DirectoryAcquirePhase::AdvisoryLock && o.contended()),
        "actual nonblocking WouldBlock observation, without a timeout worker"
    );
}
fn pause_to(expiry: Instant) {
    while Instant::now() < expiry {
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn expired(error: &RetainedDirectoryAcquireError) -> bool {
    matches!(
        error.failure(),
        DirectoryAcquireFailure::Until(DirectoryOwnershipUntilError::Expired)
    )
}

#[test]
fn normalized_absolute_policy_and_finite_path_caps_precede_native_work() {
    let root = Temp::new("policy");
    let normal = root.0.join("not-created");
    let absolute = normal.to_str().expect("ASCII fixture path");
    let mut nul = normal.as_os_str().as_bytes().to_vec();
    nul.push(0);
    let invalid = [
        PathBuf::from("relative/not-created"),
        PathBuf::from(format!("{absolute}/")),
        PathBuf::from(format!("{}//not-created", root.0.display())),
        PathBuf::from(format!("{}/./not-created", root.0.display())),
        PathBuf::from(format!("{}/../not-created", root.0.display())),
        PathBuf::from(OsString::from_vec(nul)),
    ];
    for path in invalid {
        assert!(matches!(
            cost(&path, limits(1)),
            Err(DirectoryOwnershipUntilError::Ownership(
                DirectoryOwnershipError::IdentityMismatch
            ))
        ));
        let error = acquire(&path, limits(1), expiry(), |_, _| {})
            .expect_err("literal normalized absolute path required");
        assert_eq!(error.observations().len(), 0);
        assert_eq!(error.usage(), DirectoryAcquireUsage::default());
        assert!(!error.owns_lock());
    }
    let components = normal.components().count() - 1;
    for bound in [
        DirectoryAcquireLimits {
            max_path_bytes: 0,
            ..limits(1)
        },
        DirectoryAcquireLimits {
            max_path_bytes: normal.as_os_str().as_bytes().len() - 1,
            ..limits(1)
        },
        DirectoryAcquireLimits {
            max_components: components - 1,
            ..limits(1)
        },
    ] {
        let error = acquire(&normal, bound, expiry(), |_, _| {})
            .expect_err("finite path/component ceiling before allocation or I/O");
        assert!(matches!(
            error.failure(),
            DirectoryAcquireFailure::Until(DirectoryOwnershipUntilError::Capacity)
        ));
        assert_eq!(error.observations().len(), 0);
        assert_eq!(error.usage(), DirectoryAcquireUsage::default());
    }
    let exact = DirectoryAcquireLimits {
        max_path_bytes: normal.as_os_str().as_bytes().len(),
        max_components: components,
        ..limits(1)
    };
    assert!(
        cost(&normal, exact).is_ok(),
        "exact literal length/component allowance fits"
    );
    let error = acquire(&normal, exact, Instant::now(), |_, _| {})
        .expect_err("saved original expiry precedes filesystem entry");
    assert!(expired(&error));
    assert_eq!(error.observations().len(), 0);
    assert!(!normal.exists());
    assert!(!root.0.join(LOCK_NAME).exists());

    let late_e = Instant::now() + Duration::from_secs(1);
    let mut original_error = None;
    let late = acquire(&normal, exact, late_e, |phase, attempt| {
        if phase != DirectoryAcquirePhase::DirectoryOpened || original_error.is_some() {
            return;
        }
        let native = attempt
            .outcomes
            .last()
            .expect("actual completed native open")
            .io_error();
        if let Some(native) = native.filter(|e| e.kind() == io::ErrorKind::NotFound) {
            original_error = Some((
                native.kind(),
                native.raw_os_error(),
                native as *const io::Error as usize,
            ));
            pause_to(late_e);
        }
    })
    .expect_err("actual failed open completed before original expiry observation");
    assert!(expired(&late));
    let retained = late
        .observations()
        .filter(|o| o.phase() == DirectoryAcquirePhase::DirectoryOpened)
        .find_map(|o| {
            o.io_error()
                .filter(|e| e.kind() == io::ErrorKind::NotFound)
                .map(|native| {
                    (
                        native.kind(),
                        native.raw_os_error(),
                        native as *const io::Error as usize,
                    )
                })
        })
        .expect("same captured actual ENOENT remains distinct from deadline refusal");
    assert_eq!(
        Some(retained),
        original_error,
        "original native error object, kind and OS code survive late E"
    );
    assert!(late.usage().calls > 0);
    assert_eq!(late.usage().creation_attempts, 0);
    assert_eq!(late.usage().created_directories, 0);
    assert!(!late.owns_lock());
    assert!(!normal.exists());
    assert!(!root.0.join(LOCK_NAME).exists());
}

#[test]
fn real_nested_creation_honors_attempt_cap_and_prepays_independent_syncs() {
    let root = Temp::new("creation");
    let target = root.0.join("one/two");
    let none = acquire(&target, limits(0), expiry(), |_, _| {})
        .expect_err("missing child cannot bypass zero creation policy");
    assert!(matches!(none.failure(), DirectoryAcquireFailure::Capacity));
    assert_eq!(none.usage().creation_attempts, 0);
    assert_eq!(none.usage().created_directories, 0);
    assert!(!root.0.join("one").exists());
    assert!(none.observations().any(|o| {
        o.phase() == DirectoryAcquirePhase::DirectoryOpened
            && o.io_error()
                .is_some_and(|e| e.kind() == io::ErrorKind::NotFound)
    }));
    drop(none);
    let partial = acquire(&target, limits(1), expiry(), |_, _| {})
        .expect_err("second creation is refused, first actual child is retained");
    assert!(matches!(
        partial.failure(),
        DirectoryAcquireFailure::Capacity
    ));
    assert_eq!(partial.usage().creation_attempts, 1);
    assert_eq!(partial.usage().created_directories, 1);
    assert!(root.0.join("one").is_dir());
    assert!(!target.exists());
    assert_eq!(
        results(&partial, DirectoryAcquirePhase::DirectoryCreated),
        vec![true]
    );
    assert_eq!(
        results(&partial, DirectoryAcquirePhase::ChildSynced),
        vec![true]
    );
    assert_eq!(
        results(&partial, DirectoryAcquirePhase::ParentSynced),
        vec![true]
    );
    assert!(!partial.owns_lock());
    drop(partial);
    let mut completed = None;
    let mut child_syncs = 0;
    let mut parent_syncs = 0;
    let owner = acquire(&target, limits(1), expiry(), |phase, attempt| match phase {
        DirectoryAcquirePhase::ChildSynced => {
            child_syncs += usize::from(
                attempt
                    .outcomes
                    .last()
                    .expect("actual child sync result")
                    .succeeded(),
            )
        }
        DirectoryAcquirePhase::ParentSynced => {
            parent_syncs += usize::from(
                attempt
                    .outcomes
                    .last()
                    .expect("actual independent parent result")
                    .succeeded(),
            )
        }
        DirectoryAcquirePhase::OwnerCompleted => {
            assert!(attempt.owner.is_some());
            assert!(attempt.locked());
            completed = Some(attempt.usage());
        }
        _ => {}
    })
    .expect("actual remaining child and lock creation");
    let paid = completed.expect("real native owner completion was observed");
    assert_eq!(paid.creation_attempts, 1);
    assert_eq!(paid.created_directories, 1);
    assert_eq!(
        (child_syncs, parent_syncs),
        (2, 2),
        "one directory plus literal lock file each settle both actual syncs"
    );
    let ceiling = cost(&target, limits(1)).expect("plain cost for same actual path");
    assert!(paid.declared_bytes <= ceiling.declared_bytes());
    assert!(paid.entries <= ceiling.entries());
    assert!(paid.string_bytes <= ceiling.string_bytes());
    assert!(paid.work_bytes <= ceiling.work_bytes());
    owner
        .validate_directory(&target)
        .expect("real complete owner validates exact target");
}

#[test]
fn real_nonblocking_contention_waits_for_last_owner_and_reuses_native_lock_inode() {
    let root = Temp::new("contention");
    let owner = DirectoryOwnership::acquire_until(&root.0, limits(0), expiry())
        .expect("actual first bounded owner");
    let metadata = fs::metadata(root.0.join(LOCK_NAME)).expect("actual original lock metadata");
    let identity = (metadata.dev(), metadata.ino());
    let retained = Arc::clone(&owner);
    assert_contended(&root.0);
    drop(owner);
    assert_contended(&root.0);
    drop(retained);
    let next = DirectoryOwnership::acquire_until(&root.0, limits(0), expiry())
        .expect("actual lock becomes available after last owner drops");
    let metadata = next
        ._lock
        .metadata()
        .expect("actual reused descriptor metadata");
    assert_eq!((metadata.dev(), metadata.ino()), identity);
}

#[test]
fn real_symlink_hardlink_and_fifo_names_refuse_without_following_or_locking_special_files() {
    let root = Temp::new("wrong-types");
    let target = root.0.join("target");
    fs::create_dir(&target).expect("actual target");
    let alias = root.0.join("alias");
    symlink(&target, &alias).expect("actual directory alias");
    for path in [alias.clone(), alias.join("must-not-create")] {
        let error = DirectoryOwnership::acquire_until(&path, limits(2), expiry())
            .expect_err("actual nofollow ancestor open refuses symlink");
        assert!(!error.owns_lock());
        assert!(
            error
                .observations()
                .any(|o| o.phase() == DirectoryAcquirePhase::DirectoryOpened
                    && o.io_error().is_some())
        );
    }
    assert!(!target.join("must-not-create").exists());
    assert!(!target.join(LOCK_NAME).exists());
    let external = root.0.join("unchanged");
    fs::write(&external, b"unaltered native bytes").expect("actual unrelated file");
    let lock = target.join(LOCK_NAME);
    symlink(&external, &lock).expect("actual lock symlink");
    let error = DirectoryOwnership::acquire_until(&target, limits(0), expiry())
        .expect_err("actual existing symlink lock must not open");
    assert!(!error.owns_lock());
    assert_eq!(
        results(&error, DirectoryAcquirePhase::LockOpened),
        vec![false]
    );
    assert_eq!(
        fs::read(&external).expect("actual original file remains"),
        b"unaltered native bytes"
    );
    drop(error);
    fs::remove_file(&lock).expect("remove fixture symlink");
    fs::write(&lock, b"").expect("actual ordinary lock file");
    let linked = root.0.join("hardlink");
    fs::hard_link(&lock, &linked).expect("actual extra native link");
    let error = DirectoryOwnership::acquire_until(&target, limits(0), expiry())
        .expect_err("opened two-link lock is refused before advisory entry");
    assert!(matches!(error.failure(), DirectoryAcquireFailure::Identity));
    assert_eq!(
        results(&error, DirectoryAcquirePhase::LockMetadata),
        vec![true]
    );
    assert_eq!(
        results(&error, DirectoryAcquirePhase::AdvisoryLock),
        Vec::<bool>::new()
    );
    assert!(
        error.attempt.lock.is_some(),
        "actual opened wrong-identity resource remains retained"
    );
    assert!(!error.owns_lock());
    drop(error);
    fs::remove_file(&linked).expect("remove fixture extra link");
    fs::remove_file(&lock).expect("remove fixture regular lock");
    rustix::fs::mkfifoat(CWD, &lock, Mode::from_raw_mode(0o600))
        .expect("actual FIFO, without a helper process or fake native operation");
    let error = DirectoryOwnership::acquire_until(&target, limits(0), expiry())
        .expect_err("actual nonblocking FIFO open must fail regular-file validation");
    assert!(matches!(error.failure(), DirectoryAcquireFailure::Identity));
    assert_eq!(
        results(&error, DirectoryAcquirePhase::LockOpened),
        vec![true]
    );
    assert_eq!(
        results(&error, DirectoryAcquirePhase::LockMetadata),
        vec![true]
    );
    assert_eq!(
        results(&error, DirectoryAcquirePhase::AdvisoryLock),
        Vec::<bool>::new()
    );
    assert!(!error.owns_lock());
}

#[test]
fn actual_ancestor_replacement_refuses_and_retains_the_original_locked_inode() {
    let root = Temp::new("ancestor");
    let ancestor = root.0.join("ancestor");
    let retired = root.0.join("retired");
    let target = ancestor.join("owned");
    fs::create_dir_all(&target).expect("actual initial ancestors");
    let mut replaced = false;
    let error = acquire(&target, limits(0), expiry(), |phase, attempt| {
        if phase == DirectoryAcquirePhase::AdvisoryLock && attempt.locked() && !replaced {
            assert!(
                attempt
                    .outcomes
                    .last()
                    .expect("actual advisory result")
                    .succeeded()
            );
            fs::rename(&ancestor, &retired).expect("move actual opened ancestor");
            fs::create_dir_all(&target).expect("different actual ancestor/leaf");
            replaced = true;
        }
    })
    .expect_err("second physical nofollow walk must compare original ancestor identity");
    assert!(replaced);
    assert!(matches!(error.failure(), DirectoryAcquireFailure::Identity));
    assert!(error.owns_lock());
    assert!(
        error.attempt.owner.is_none(),
        "refusal precedes owner publication"
    );
    assert!(
        !target.join(LOCK_NAME).exists(),
        "replacement path never receives a lock"
    );
    assert_contended(&retired.join("owned"));
    fs::remove_dir_all(&ancestor).expect("remove empty replacement fixture");
    fs::rename(&retired, &ancestor).expect("restore SAME original native path");
    assert_contended(&target);
    let retained_fd = error
        .attempt
        .lock
        .as_ref()
        .expect("actual retained raw lock")
        .as_raw_fd();
    assert!(retained_fd >= 0);
    drop(error);
    let next = DirectoryOwnership::acquire_until(&target, limits(0), expiry())
        .expect("original actual lock is released by error Drop");
    next.validate_directory(&target)
        .expect("same restored directory");
}

#[test]
fn actual_raw_lock_and_complete_owner_survive_original_expiry_or_observer_unwind() {
    for phase in [
        DirectoryAcquirePhase::AdvisoryLock,
        DirectoryAcquirePhase::OwnerCompleted,
    ] {
        for panic in [false, true] {
            let root = Temp::new("retained-owner");
            let e = if panic {
                expiry()
            } else {
                Instant::now() + Duration::from_secs(1)
            };
            let mut observed = false;
            let mut descriptor = None;
            let mut owner: Option<Weak<DirectoryOwnership>> = None;
            let error = acquire(&root.0, limits(0), e, |actual, attempt| {
                if actual != phase || observed {
                    return;
                }
                observed = true;
                assert!(attempt.locked());
                let file = match attempt.owner.as_ref() {
                    Some(complete) => {
                        owner = Some(Arc::downgrade(complete));
                        &complete._lock
                    }
                    None => attempt.lock.as_ref().expect("captured actual raw lock"),
                };
                descriptor = Some(file.as_raw_fd());
                if panic {
                    std::panic::panic_any("actual locked observer unwind");
                }
                pause_to(e);
            })
            .expect_err("completed actual lock/owner must not become late or panicked success");
            assert!(observed, "actual target physical phase was reached");
            assert!(error.owns_lock());
            assert_eq!(error.panic_payloads().count(), usize::from(panic));
            if panic {
                assert!(matches!(error.failure(), DirectoryAcquireFailure::Unwind));
            } else {
                assert!(expired(&error));
            }
            let actual = match error.attempt.owner.as_ref() {
                Some(complete) => &complete._lock,
                None => error
                    .attempt
                    .lock
                    .as_ref()
                    .expect("same raw native lock retained"),
            };
            assert_eq!(
                Some(actual.as_raw_fd()),
                descriptor,
                "SAME descriptor was retained, without a pathname reopen"
            );
            if phase == DirectoryAcquirePhase::OwnerCompleted {
                let upgraded = owner
                    .as_ref()
                    .expect("actual original owner allocation")
                    .upgrade()
                    .expect("same owner alive in returned error");
                assert!(Arc::ptr_eq(
                    &upgraded,
                    error
                        .attempt
                        .owner
                        .as_ref()
                        .expect("complete original owner")
                ));
                drop(upgraded);
            } else {
                assert!(error.attempt.owner.is_none());
            }
            assert_contended(&root.0);
            drop(error);
            if let Some(owner) = owner {
                assert!(
                    owner.upgrade().is_none(),
                    "no escaped strong owner outlives the error"
                );
            }
            let next = DirectoryOwnership::acquire_until(&root.0, limits(0), expiry())
                .expect("later real acquisition succeeds only after error Drop");
            next.validate_directory(&root.0)
                .expect("actual retained directory");
        }
    }
}

#[test]
fn created_child_and_parent_syncs_physically_settle_after_expiry_and_independent_panics() {
    for directory in [false, true] {
        for panic in [false, true] {
            let root = Temp::new("settlement");
            let target = if directory {
                root.0.join("created")
            } else {
                root.0.clone()
            };
            let creation = if directory {
                DirectoryAcquirePhase::DirectoryCreated
            } else {
                DirectoryAcquirePhase::LockCreated
            };
            let e = if panic {
                expiry()
            } else {
                Instant::now() + Duration::from_secs(1)
            };
            let mut created = false;
            let error = acquire(
                &target,
                limits(usize::from(directory)),
                e,
                |phase, attempt| {
                    if phase == creation && !created {
                        assert!(
                            attempt
                                .outcomes
                                .last()
                                .expect("actual successful creation")
                                .succeeded()
                        );
                        created = true;
                        if panic {
                            std::panic::panic_any("original creation observer");
                        }
                        pause_to(e);
                    } else if created && panic {
                        match phase {
                            DirectoryAcquirePhase::DirectoryOpened => {
                                std::panic::panic_any("retained child-open observer")
                            }
                            DirectoryAcquirePhase::ChildSynced => {
                                assert!(
                                    attempt
                                        .outcomes
                                        .last()
                                        .expect("actual child sync")
                                        .succeeded()
                                );
                                std::panic::panic_any("independent child-sync observer");
                            }
                            DirectoryAcquirePhase::ParentSynced => {
                                assert!(
                                    attempt
                                        .outcomes
                                        .last()
                                        .expect("actual independent parent sync")
                                        .succeeded()
                                );
                                std::panic::panic_any("independent parent-sync observer");
                            }
                            _ => {}
                        }
                    }
                },
            )
            .expect_err("entered creation obligation settles despite original error/E");
            assert!(created, "physical native creation actually completed");
            assert!(
                !error.owns_lock(),
                "settlement does not invent an advisory-lock result"
            );
            assert_eq!(results(&error, creation), vec![true]);
            assert_eq!(
                results(&error, DirectoryAcquirePhase::ChildSynced),
                vec![true]
            );
            assert_eq!(
                results(&error, DirectoryAcquirePhase::ParentSynced),
                vec![true]
            );
            assert!(
                error.attempt.obligation.is_none(),
                "both prepaid physical sync obligations finished"
            );
            assert_eq!(error.usage().created_directories, usize::from(directory));
            if panic {
                assert!(matches!(error.failure(), DirectoryAcquireFailure::Unwind));
                let payloads = error
                    .panic_payloads()
                    .map(|p| {
                        p.downcast_ref::<&'static str>()
                            .copied()
                            .expect("same original typed panic payload")
                    })
                    .collect::<Vec<_>>();
                let expected = if directory {
                    vec![
                        "original creation observer",
                        "retained child-open observer",
                        "independent child-sync observer",
                        "independent parent-sync observer",
                    ]
                } else {
                    vec![
                        "original creation observer",
                        "independent child-sync observer",
                        "independent parent-sync observer",
                    ]
                };
                assert_eq!(
                    payloads, expected,
                    "all independent physical-settlement observer panics retained"
                );
            } else {
                assert!(expired(&error));
                assert_eq!(error.panic_payloads().count(), 0);
            }
            if directory {
                assert!(target.is_dir());
                assert!(!target.join(LOCK_NAME).exists());
            } else {
                assert!(target.join(LOCK_NAME).is_file());
            }
            drop(error);
            let next = DirectoryOwnership::acquire_until(&target, limits(0), expiry())
                .expect("physical created source remains usable after refusal owner drops");
            next.validate_directory(&target)
                .expect("actual final directory identity");
        }
    }
}
