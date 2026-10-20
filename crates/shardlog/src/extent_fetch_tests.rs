use super::*;
use crate::{StorageFetchBudget, StorageFetchFailure, StorageFetchLimits};
use std::cell::RefCell;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

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
            "native-bounded-storage-fetch-{}-{now}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual directory");
        Self(path)
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn config(path: &Path) -> ShardStoreConfig {
    ShardStoreConfig {
        directory: path.to_owned(),
        shard_id: ShardId::new(0),
        target_pack_bytes: 128,
        max_batch_bytes: 8192,
    }
}
fn partition() -> TopicPartition {
    TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0))
}
fn reservation(id: u128) -> Reservation {
    Reservation {
        topic_id: TopicId::new(1),
        partition_id: LogicalPartitionId::new(0),
        batch_id: BatchId::new(id),
        first_offset: LogicalOffset::new(id as u64),
        last_offset: LogicalOffset::new(id as u64),
        record_count: NonZeroU32::new(1).expect("one"),
        placement: Placement {
            virtual_lane_id: ShardId::new(0),
            ring_epoch: RingEpoch::new(1),
            leader_epoch: LeaderEpoch::new(1),
            sequence: PlacementSequence::new(id as u64),
        },
    }
}
fn limits() -> StorageFetchLimits {
    StorageFetchLimits {
        max_plan_batches: 128,
        max_entries: 100_000,
        max_path_bytes: 8192,
        max_read_attempts: 1024,
        max_read_bytes: 1 << 20,
        max_declared_bytes: 1 << 22,
        max_work_bytes: 1 << 26,
    }
}
fn budget(limits: StorageFetchLimits) -> StorageFetchBudget {
    StorageFetchBudget::new(limits, Instant::now() + Duration::from_secs(10))
        .expect("real finite ordinary counters")
}

#[test]
fn real_zero_payload_history_has_finite_metadata_prefix_and_exact_cumulative_entry_cap() {
    let root = Temporary::new();
    let mut store = ShardStore::open(config(&root.0)).expect("real native store");
    for id in 0..64 {
        store
            .append(reservation(id), None, b"", true)
            .expect("real zero-payload extent");
    }
    let ordinary = store
        .plan_fetch(partition(), LogicalOffset::new(0), 4096, 128)
        .expect("ordinary native plan");
    assert_eq!(ordinary.len(), 64);
    let generous = budget(limits());
    let bounded = store
        .plan_fetch_until(partition(), LogicalOffset::new(0), 4096, 7, &generous)
        .expect("finite actual prefix");
    assert_eq!(bounded, ordinary[..7]);
    let paid = generous.usage().expect("actual cumulative fees");
    let mut exact = limits();
    exact.max_entries = paid.entries;
    let exact_budget = budget(exact);
    assert_eq!(
        store
            .plan_fetch_until(partition(), LogicalOffset::new(0), 4096, 7, &exact_budget)
            .expect("exact real entry ceiling"),
        bounded
    );
    assert_eq!(exact_budget.usage().expect("same fees"), paid);
    exact.max_entries -= 1;
    let short = budget(exact);
    assert!(matches!(
        store
            .plan_fetch_until(partition(), LogicalOffset::new(0), 4096, 7, &short)
            .expect_err("one fewer entry")
            .failure(),
        StorageFetchFailure::Capacity
    ));
    assert!(matches!(short.check(), Err(StorageFetchFailure::Denied)));
    assert_eq!(
        store
            .plan_fetch(partition(), LogicalOffset::new(0), 4096, 128)
            .expect("ordinary policy unchanged"),
        ordinary
    );
}

#[test]
fn real_separate_pack_reads_retain_first_payload_when_next_physical_attempt_is_refused() {
    let root = Temporary::new();
    let mut store = ShardStore::open(config(&root.0)).expect("real store");
    for id in 0..3 {
        store
            .append(reservation(id), None, b"retained payload", true)
            .expect("real pack");
    }
    let planned = store
        .plan_fetch(partition(), LogicalOffset::new(0), 4096, 3)
        .expect("native plan");
    assert_eq!(
        planned
            .iter()
            .map(|metadata| metadata.pack_sequence)
            .collect::<HashSet<_>>()
            .len(),
        3
    );
    let mut cap = limits();
    cap.max_read_attempts = 1;
    let limited = budget(cap);
    let error = store
        .read_fetch_plan_until(&planned, &limited)
        .expect_err("second actual read denied");
    assert!(matches!(error.failure(), StorageFetchFailure::Capacity));
    assert_eq!(error.batches().len(), 1);
    assert_eq!(error.batches()[0].payload.as_ref(), b"retained payload");
    assert_eq!(limited.usage().expect("actual attempts").read_attempts, 1);
    assert!(matches!(limited.check(), Err(StorageFetchFailure::Denied)));
    assert_eq!(
        store
            .read_fetch_plan(&planned)
            .expect("ordinary reader still progresses")
            .len(),
        3
    );
}

#[test]
fn actual_short_file_read_and_eof_attempt_preserve_native_io_error_and_prepaid_attempts() {
    let root = Temporary::new();
    let mut store = ShardStore::open(config(&root.0)).expect("real store");
    store
        .append(reservation(0), None, b"actual truncated payload", true)
        .expect("real extent");
    let planned = store
        .plan_fetch(partition(), LogicalOffset::new(0), 4096, 1)
        .expect("native plan");
    let metadata = planned[0];
    let file = OpenOptions::new()
        .write(true)
        .open(&store.pack_paths[&metadata.pack_sequence])
        .expect("actual same native file");
    file.set_len(metadata.payload_offset + 2)
        .expect("actual short physical file");
    let actual = budget(limits());
    let error = store
        .read_fetch_plan_until(&planned, &actual)
        .expect_err("actual short then EOF");
    assert!(
        matches!(error.failure(), StorageFetchFailure::Storage(StorageError::Io(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof)
    );
    let usage = actual.usage().expect("real fees");
    assert_eq!(usage.read_attempts, 2);
    assert_eq!(
        usage.read_bytes,
        metadata.payload_len + metadata.payload_len - 2
    );
    assert!(error.batches().is_empty());
}

#[test]
fn original_e_after_actual_open_or_read_starts_no_later_physical_phase() {
    for late_phase in ["open", "read"] {
        let root = Temporary::new();
        let mut store = ShardStore::open(config(&root.0)).expect("real store");
        for id in 0..3 {
            store
                .append(reservation(id), None, b"native actual phase", true)
                .expect("real pack");
        }
        let planned = store
            .plan_fetch(partition(), LogicalOffset::new(0), 4096, 3)
            .expect("native plan");
        let expiry = Instant::now() + Duration::from_secs(1);
        let actual = StorageFetchBudget::new(limits(), expiry).expect("saved original E");
        let phases = std::rc::Rc::new(RefCell::new(Vec::new()));
        let recorded = phases.clone();
        let _observer = observe(move |phase| {
            recorded.borrow_mut().push(phase.to_owned());
            if phase == late_phase {
                std::thread::sleep(
                    expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(2),
                );
            }
        });
        let error = store
            .read_fetch_plan_until(&planned, &actual)
            .expect_err("actual phase returned past original E");
        assert!(matches!(error.failure(), StorageFetchFailure::Expired(_)));
        let expected: &[&str] = if late_phase == "open" {
            &["open"]
        } else {
            &["open", "read"]
        };
        assert_eq!(phases.borrow().as_slice(), expected);
        assert_eq!(actual.absolute_expiry(), expiry);
        assert_eq!(
            actual.usage().expect("paid attempts").read_attempts,
            usize::from(late_phase == "read")
        );
        assert!(error.batches().is_empty());
    }
}

#[test]
fn shared_native_storage_algorithms_match_complete_original_writer_plan_and_read_oracles() {
    let root = Temporary::new();
    let mut store = ShardStore::open(config(&root.0)).expect("actual native writer");
    for id in 0..8 {
        store
            .append(
                reservation(id),
                None,
                if id % 2 == 0 {
                    b""
                } else {
                    b"actual oracle payload"
                },
                true,
            )
            .expect("real native mixed payload extent");
    }
    for bytes in [1, 17, 4096] {
        for start in [0, 3, 7, 8] {
            let expected = store
                .original_plan_fetch_oracle(partition(), LogicalOffset::new(start), bytes, 7)
                .expect("complete original native metadata oracle");
            let ordinary = store
                .plan_fetch(partition(), LogicalOffset::new(start), bytes, 7)
                .expect("same ordinary native entry");
            let actual = budget(limits());
            let bounded = store
                .plan_fetch_until(partition(), LogicalOffset::new(start), bytes, 7, &actual)
                .expect("same bounded native plan");
            assert_eq!(ordinary, expected);
            assert_eq!(bounded, expected);
            let expected_data = store
                .original_read_fetch_plan_oracle(&expected)
                .expect("complete original native read oracle");
            assert_eq!(
                store
                    .read_fetch_plan(&ordinary)
                    .expect("ordinary shared reader"),
                expected_data
            );
            assert_eq!(
                store
                    .read_fetch_plan_until(&bounded, &actual)
                    .expect("bounded shared reader"),
                expected_data
            );
        }
    }
}

// Entire original native functions are source oracles; only their names and
// the original read-helper reference change. No wire parser is duplicated.
impl ShardStore {
    fn original_plan_fetch_oracle(
        &self,
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
        max_bytes: usize,
        max_batches: usize,
    ) -> StorageResult<Vec<StoredBatchMetadata>> {
        if max_bytes == 0 || max_batches == 0 {
            return Err(StorageError::InvalidInput(
                "fetch byte and batch limits must be nonzero".into(),
            ));
        }
        let Some(locations) = self.index.get(&topic_partition) else {
            return Ok(Vec::new());
        };

        let first =
            locations.partition_point(|location| location.reservation.last_offset < start_offset);
        let capacity = FETCH_PLAN_INITIAL_CAPACITY
            .min(max_batches)
            .min(locations.len().saturating_sub(first));
        let mut result = Vec::with_capacity(capacity);
        let mut used = 0usize;
        for location in &locations[first..] {
            if result.len() == max_batches {
                break;
            }
            let next = used
                .checked_add(location.payload_len)
                .ok_or_else(|| StorageError::LimitExceeded("fetch byte count overflow".into()))?;
            if !result.is_empty() && next > max_bytes {
                break;
            }
            result.push(self.metadata_for(location));
            used = next;
        }
        Ok(result)
    }

    fn original_read_fetch_plan_oracle(
        &mut self,
        planned: &[StoredBatchMetadata],
    ) -> StorageResult<Vec<StoredBatch>> {
        if planned.iter().any(|metadata| {
            metadata.store_token != self.store_token || metadata.plan_epoch != self.plan_epoch
        }) {
            return Err(StorageError::InvalidInput(
                "fetch plan does not belong to the current store generation".into(),
            ));
        }
        let mut result = Vec::with_capacity(planned.len());
        let mut first = 0usize;
        while first < planned.len() {
            let mut end = first + 1;
            let mut range_end = payload_end(&planned[first])?;
            while end < planned.len() {
                let next = planned[end];
                if next.pack_sequence != planned[first].pack_sequence
                    || next.payload_offset < range_end
                    || next.payload_offset - range_end > MAX_COALESCED_READ_GAP
                {
                    break;
                }
                range_end = payload_end(&next)?;
                end += 1;
            }

            let range_start = planned[first].payload_offset;
            let range_len = usize::try_from(range_end - range_start)
                .map_err(|_| StorageError::LimitExceeded("fetch range exceeds usize".into()))?;
            let reader = self.open_pack_reader(planned[first].pack_sequence)?;
            let mut buffer = self.read_buffer_pool.take(range_len);
            if let Err(error) = original_read_exact_at_oracle(&reader, &mut buffer, range_start) {
                self.read_buffer_pool.recycle_now(buffer);
                return Err(error.into());
            }
            let buffer = self.read_buffer_pool.lease_bytes(buffer);

            for metadata in &planned[first..end] {
                let payload_start = usize::try_from(metadata.payload_offset - range_start)
                    .map_err(|_| {
                        StorageError::LimitExceeded("payload offset exceeds usize".into())
                    })?;
                let payload_end = payload_start
                    .checked_add(metadata.payload_len)
                    .ok_or_else(|| StorageError::LimitExceeded("payload range overflow".into()))?;
                result.push(StoredBatch {
                    reservation: metadata.reservation,
                    producer: metadata.producer,
                    atomic_group: metadata.atomic_group,
                    payload: buffer.slice(payload_start..payload_end),
                });
            }
            first = end;
        }
        Ok(result)
    }
}

fn original_read_exact_at_oracle(
    file: &File,
    mut buffer: &mut [u8],
    mut offset: u64,
) -> std::io::Result<()> {
    while !buffer.is_empty() {
        match file.read_at(buffer, offset)? {
            0 => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "planned extent ended before its payload",
                ));
            }
            read => {
                offset = offset
                    .checked_add(read as u64)
                    .ok_or_else(|| std::io::Error::other("read offset overflow"))?;
                buffer = &mut buffer[read..];
            }
        }
    }
    Ok(())
}

// Separate from the original completion observer: old native test hooks and
// their exact phase sequences remain byte-for-byte unchanged.
type OwnerObserver = Box<dyn FnMut(&str, &crate::fetch_limits::ReadCustody)>;
thread_local! { static OWNER_OBSERVER: RefCell<Option<OwnerObserver>> = RefCell::new(None); }
pub(super) fn owner_completed(phase: &str, owner: &crate::fetch_limits::ReadCustody) {
    OWNER_OBSERVER.with(|slot| {
        if let Some(observer) = &mut *slot.borrow_mut() {
            observer(phase, owner);
        }
    });
}
struct ObserveOwner;
impl Drop for ObserveOwner {
    fn drop(&mut self) {
        OWNER_OBSERVER.with(|slot| *slot.borrow_mut() = None);
    }
}
fn observe_owner(
    callback: impl FnMut(&str, &crate::fetch_limits::ReadCustody) + 'static,
) -> ObserveOwner {
    OWNER_OBSERVER.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(callback));
    });
    ObserveOwner
}
fn owned_reader(owner: &crate::fetch_limits::ReadCustody) -> &File {
    owner
        .reader
        .as_ref()
        .or_else(|| {
            owner
                .open_result
                .as_ref()
                .and_then(|result| result.as_ref().ok())
        })
        .expect("the exact actual returned native File remains owned")
}
fn reader_identity(file: &File) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    let actual = file.metadata().expect("actual retained FD metadata");
    (actual.dev(), actual.ino())
}
fn read_fixture() -> (Temporary, ShardStore, Vec<StoredBatchMetadata>) {
    let root = Temporary::new();
    let mut store = ShardStore::open(config(&root.0)).expect("actual native writer");
    store
        .append(
            reservation(0),
            None,
            b"actual unpublished native bytes",
            true,
        )
        .expect("real complete native extent");
    let planned = store
        .plan_fetch(partition(), LogicalOffset::new(0), 4096, 1)
        .expect("same native plan");
    (root, store, planned)
}

#[test]
fn retained_read_open_late_keeps_actual_inode_and_starts_no_read() {
    let (_root, mut store, planned) = read_fixture();
    let path = store.pack_paths[&planned[0].pack_sequence].clone();
    let expected = reader_identity(&File::open(&path).expect("independent actual inode oracle"));
    let expiry = Instant::now() + Duration::from_secs(1);
    let budget = StorageFetchBudget::new(limits(), expiry).expect("original absolute E");
    let seen = std::rc::Rc::new(RefCell::new(Vec::new()));
    let record = seen.clone();
    let _observe = observe_owner(move |phase, owner| {
        record.borrow_mut().push(phase.to_owned());
        if phase == "open" {
            assert_eq!(reader_identity(owned_reader(owner)), expected);
            std::thread::sleep(
                expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(2),
            );
        }
    });
    let error = store
        .read_fetch_plan_until(&planned, &budget)
        .expect_err("actual File returned after original E");
    assert!(matches!(
        error.failure(),
        StorageFetchFailure::Expired(None)
    ));
    assert_eq!(*seen.borrow(), ["open"]);
    assert!(error.has_native_reader());
    assert!(error.unpublished_bytes().is_none());
    assert!(!error.read_entered() && !error.read_returned());
    assert_eq!(
        budget
            .usage()
            .expect("same original counters")
            .read_attempts,
        0
    );
    assert_eq!(budget.absolute_expiry(), expiry);
    assert!(error.batches().is_empty());
    assert_eq!(
        reader_identity(owned_reader(error.current.as_ref().expect("real range"))),
        expected
    );
    drop(store);
    fs::remove_file(&path).expect("remove original name while retained File is live");
    use std::os::unix::fs::MetadataExt;
    let retained = owned_reader(error.current.as_ref().expect("same returned owner"));
    assert_eq!(reader_identity(retained), expected);
    assert_eq!(
        retained.metadata().expect("actual unnamed inode").nlink(),
        0
    );
}

#[test]
fn retained_read_short_eof_keeps_actual_partial_buffer_file_and_attempts() {
    let (_root, mut store, planned) = read_fixture();
    let metadata = planned[0];
    let path = &store.pack_paths[&metadata.pack_sequence];
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .expect("actual writer FD");
    let identity = reader_identity(&file);
    file.set_len(metadata.payload_offset + 2)
        .expect("real short physical input");
    drop(file);
    let pointer = std::rc::Rc::new(RefCell::new(None));
    let saved = pointer.clone();
    let _observe = observe_owner(move |phase, owner| {
        if phase == "read" {
            let actual = owner
                .raw
                .as_ref()
                .expect("actual initialized raw buffer")
                .as_ref()
                .as_ptr();
            if let Some(previous) = *saved.borrow() {
                assert_eq!(actual, previous);
            }
            *saved.borrow_mut() = Some(actual);
        }
    });
    let original = budget(limits());
    let error = store
        .read_fetch_plan_until(&planned, &original)
        .expect_err("actual short read followed by actual Ok(0)");
    assert!(matches!(error.failure(),
        StorageFetchFailure::Storage(StorageError::Io(error))
            if error.kind() == std::io::ErrorKind::UnexpectedEof));
    let bytes = error
        .unpublished_bytes()
        .expect("same partly modified allocation");
    assert_eq!(
        bytes.as_ptr(),
        pointer.borrow().expect("actual read destination pointer")
    );
    assert_eq!(bytes.len(), metadata.payload_len);
    assert_eq!(&bytes[..2], b"ac");
    assert!(bytes[2..].iter().all(|byte| *byte == 0));
    assert_eq!(
        error.last_read_bytes(),
        Some(0),
        "EOF was native Ok(0), not native Err"
    );
    assert_eq!(error.confirmed_read_bytes(), 2);
    assert_eq!(
        error.read_range(),
        Some((metadata.payload_offset, metadata.payload_len))
    );
    assert!(error.read_returned() && error.has_native_reader());
    assert!(!error.native_error_returned());
    assert_eq!(
        reader_identity(owned_reader(error.current.as_ref().expect("real owner"))),
        identity
    );
    assert_eq!(
        original
            .usage()
            .expect("same prepaid attempts")
            .read_attempts,
        2
    );
    assert_eq!(
        original.usage().expect("same requested bytes").read_bytes,
        metadata.payload_len + metadata.payload_len - 2
    );
    assert!(error.batches().is_empty());
    assert_eq!(
        store
            .read_buffer_pool
            .cached_buffers
            .load(Ordering::Acquire),
        0,
        "the refused raw allocation has not been recycled into the store"
    );
}

#[test]
fn retained_read_native_error_and_first_directory_refusal_both_survive() {
    use crate::{ShardDirectoryCheckFailure, ShardDirectoryChecks};
    use std::sync::atomic::AtomicUsize;
    struct RealChecks(AtomicUsize);
    impl ShardDirectoryChecks for RealChecks {
        fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
            match self.0.load(Ordering::Acquire) {
                1 => Err(ShardDirectoryCheckFailure::Refused),
                2 => std::panic::panic_any(417usize),
                _ => Ok(()),
            }
        }
    }
    for mode in [1, 2] {
        let (root, mut store, planned) = read_fixture();
        // The actual OS directory read supplies an independent real I/O error.
        let mut probe = [0u8; 1];
        let reference = File::open(&root.0)
            .expect("genuine directory File")
            .read_at(&mut probe, 0)
            .expect_err("actual OS refuses directory payload reads");
        store
            .pack_paths
            .insert(planned[0].pack_sequence, root.0.clone());
        let checks = Arc::new(RealChecks(AtomicUsize::new(0)));
        let observed = checks.clone();
        let _observe = observe_owner(move |phase, owner| {
            if phase == "read" {
                assert!(owner.read_result.as_ref().is_some_and(Result::is_err));
                observed.0.store(mode, Ordering::Release);
            }
        });
        let expiry = Instant::now() + Duration::from_secs(30);
        let budget = StorageFetchBudget::new_with_checks(limits(), expiry, checks)
            .expect("same actual original checked budget");
        let error = store
            .read_fetch_plan_until(&planned, &budget)
            .expect_err("real returned native error and actual checkpoint refusal");
        match error.failure() {
            StorageFetchFailure::Checkpoint {
                failure: ShardDirectoryCheckFailure::Refused,
                storage_error: Some(StorageError::Io(actual)),
                late: false,
            } => {
                assert_eq!(mode, 1);
                assert_eq!(actual.raw_os_error(), reference.raw_os_error());
            }
            StorageFetchFailure::CheckpointUnwind {
                payload,
                storage_error: Some(StorageError::Io(actual)),
                late: false,
            } => {
                assert_eq!(mode, 2);
                assert_eq!(payload.downcast_ref::<usize>(), Some(&417));
                assert_eq!(actual.raw_os_error(), reference.raw_os_error());
            }
            other => panic!("actual independent native/checkpoint outcomes were lost: {other:?}"),
        }
        assert!(error.native_error_returned() && error.read_returned());
        assert!(error.has_native_reader());
        assert_eq!(
            error
                .unpublished_bytes()
                .expect("real initialized buffer")
                .len(),
            planned[0].payload_len
        );
        assert!(error.last_read_bytes().is_none());
        assert_eq!(budget.absolute_expiry(), expiry);
        assert!(matches!(budget.check(), Err(StorageFetchFailure::Denied)));
        assert_eq!(
            budget
                .usage()
                .expect("exact actual IO attempts")
                .read_attempts,
            1
        );
    }
}

#[test]
fn retained_read_unwind_keeps_actual_open_read_or_promoted_owner() {
    for target in ["open", "read", "lease"] {
        let (_root, mut store, planned) = read_fixture();
        let token = Arc::new(913usize);
        let saved = token.clone();
        let observed_pointer = std::rc::Rc::new(RefCell::new(None));
        let pointer = observed_pointer.clone();
        let _observe = observe_owner(move |phase, owner| {
            if phase == target {
                *pointer.borrow_mut() = owner
                    .raw
                    .as_ref()
                    .map(AsRef::as_ref)
                    .or(owner.promoted.as_deref())
                    .map(<[u8]>::as_ptr);
                std::panic::panic_any(saved.clone());
            }
        });
        let budget = budget(limits());
        let error = store
            .read_fetch_plan_until(&planned, &budget)
            .expect_err("actual entered source frame unwound");
        assert!(matches!(
            error.failure(),
            StorageFetchFailure::NativeReadUnwind { late: false, .. }
        ));
        assert!(Arc::ptr_eq(
            error
                .read_panic_payload()
                .expect("original payload")
                .downcast_ref::<Arc<usize>>()
                .expect("actual typed token"),
            &token
        ));
        assert!(error.has_native_reader());
        if target == "open" {
            assert!(error.unpublished_bytes().is_none());
            assert!(!error.read_entered());
        } else {
            assert_eq!(
                error
                    .unpublished_bytes()
                    .expect("same returned buffer")
                    .as_ptr(),
                observed_pointer.borrow().expect("actual native pointer")
            );
            assert_eq!(
                error.unpublished_bytes(),
                Some(b"actual unpublished native bytes".as_slice())
            );
            assert_eq!(error.last_read_bytes(), Some(planned[0].payload_len));
            assert!(error.read_entered() && error.read_returned());
            assert_eq!(
                error.confirmed_read_bytes(),
                if target == "lease" {
                    planned[0].payload_len
                } else {
                    0
                }
            );
        }
        assert!(error.batches().is_empty());
        assert_eq!(
            store
                .read_buffer_pool
                .cached_buffers
                .load(Ordering::Acquire),
            0
        );
        // The exact actual unwind also survives the common late transformation.
        let crate::StorageFetchReadError {
            failure,
            batches,
            current,
        } = error;
        let late = crate::StorageFetchReadError {
            failure: failure.late(),
            batches,
            current,
        };
        assert!(matches!(
            late.failure(),
            StorageFetchFailure::NativeReadUnwind { late: true, .. }
        ));
        assert!(Arc::ptr_eq(
            late.read_panic_payload()
                .expect("same late payload")
                .downcast_ref::<Arc<usize>>()
                .expect("actual token"),
            &token
        ));
        assert!(late.has_native_reader());
        drop(late);
        assert_eq!(
            store
                .read_buffer_pool
                .cached_buffers
                .load(Ordering::Acquire),
            usize::from(target != "open")
        );
    }
}

#[test]
fn retained_read_exact_original_framing_and_one_less_refuse_before_next_phase() {
    let (_root, mut store, planned) = read_fixture();
    let expiry = Instant::now() + Duration::from_secs(30);
    let measured = StorageFetchBudget::new(limits(), expiry).expect("original measurement budget");
    drop(
        store
            .read_fetch_plan_until(&planned, &measured)
            .expect("real successful measurement"),
    );
    let paid = measured.usage().expect("actual complete same-path usage");
    for axis in ["declared", "entries", "work"] {
        for one_less in [false, true] {
            let mut allowed = limits();
            match axis {
                "declared" => {
                    allowed.max_declared_bytes = paid.declared_bytes - usize::from(one_less)
                }
                "entries" => allowed.max_entries = paid.entries - usize::from(one_less),
                "work" => allowed.max_work_bytes = paid.work_bytes - usize::from(one_less),
                _ => unreachable!(),
            }
            let original = StorageFetchBudget::new(allowed, expiry)
                .expect("independent original ledger configured before actual read");
            let result = store.read_fetch_plan_until(&planned, &original);
            assert_eq!(original.absolute_expiry(), expiry);
            assert_eq!(original.limits(), allowed);
            if !one_less {
                assert_eq!(result.expect("exact actual original limit fits").len(), 1);
                assert_eq!(original.usage().expect("same exact fees"), paid);
            } else {
                let error = result.expect_err("one fewer actual original unit");
                assert!(matches!(error.failure(), StorageFetchFailure::Capacity));
                assert!(error.has_native_reader());
                assert!(error.batches().is_empty());
                match axis {
                    "declared" => {
                        assert!(error.unpublished_bytes().is_none());
                        assert!(!error.read_entered());
                    }
                    "entries" => {
                        assert!(
                            error
                                .unpublished_bytes()
                                .expect("real pre-read allocation")
                                .iter()
                                .all(|b| *b == 0)
                        );
                        assert!(!error.read_entered());
                    }
                    "work" => {
                        assert_eq!(
                            error.unpublished_bytes(),
                            Some(b"actual unpublished native bytes".as_slice())
                        );
                        assert_eq!(error.last_read_bytes(), Some(planned[0].payload_len));
                    }
                    _ => unreachable!(),
                }
                assert!(matches!(original.check(), Err(StorageFetchFailure::Denied)));
            }
        }
    }
    let (bytes, entries) =
        crate::fetch_limits::read_custody_cost().expect("source-derived fixed framing");
    let mut before_entry = limits();
    before_entry.max_declared_bytes = bytes - 1;
    before_entry.max_entries = entries;
    let original = StorageFetchBudget::new(before_entry, expiry).expect("original framing cutoff");
    let error = store
        .read_fetch_plan_until(&planned, &original)
        .expect_err("unpaid framing enters no File");
    assert!(matches!(error.failure(), StorageFetchFailure::Capacity));
    assert!(!error.has_native_reader());
    assert!(error.unpublished_bytes().is_none());
    assert_eq!(original.usage().expect("no entered debit").read_attempts, 0);
}

#[test]
fn retained_read_success_matches_native_oracle_without_extra_buffer_or_file_leak() {
    use std::os::fd::AsRawFd;
    for coalesced in [false, true] {
        let root = Temporary::new();
        let mut cfg = config(&root.0);
        if coalesced {
            cfg.target_pack_bytes = 1 << 20;
        }
        let mut store = ShardStore::open(cfg).expect("same genuine producer");
        for id in 0..3 {
            store
                .append(reservation(id), None, b"same real reader oracle", true)
                .expect("real append");
        }
        let planned = store
            .plan_fetch(partition(), LogicalOffset::new(0), 4096, 3)
            .expect("actual plan");
        let expected = store
            .original_read_fetch_plan_oracle(&planned)
            .expect("unchanged original full reader");
        let handles = std::rc::Rc::new(RefCell::new(Vec::new()));
        let saved = handles.clone();
        let _observe = observe_owner(move |phase, owner| {
            if phase == "open" {
                let actual = owned_reader(owner);
                saved
                    .borrow_mut()
                    .push((actual.as_raw_fd(), reader_identity(actual)));
            }
        });
        let budget = budget(limits());
        let actual = store
            .read_fetch_plan_until(&planned, &budget)
            .expect("real bounded success");
        assert_eq!(actual, expected);
        assert_eq!(
            store
                .read_fetch_plan(&planned)
                .expect("ordinary None path unchanged"),
            expected
        );
        assert_eq!(handles.borrow().len(), if coalesced { 1 } else { 3 });
        for (fd, identity) in handles.borrow().iter() {
            use std::os::unix::fs::MetadataExt;
            let current = fs::metadata(format!("/proc/self/fd/{fd}"));
            assert!(
                !current.is_ok_and(|meta| (meta.dev(), meta.ino()) == *identity),
                "this exact native read FD must no longer retain its inode"
            );
        }
        assert!(budget.usage().expect("real requested reads").read_attempts > 0);
    }
}

#[test]
fn retained_read_interleaved_unselected_record_stays_owned_but_not_publicly_visible() {
    use crate::{ShardDirectoryCheckFailure, ShardDirectoryChecks};
    use std::sync::atomic::AtomicUsize;

    struct RefuseAfterCapture(AtomicUsize);
    impl ShardDirectoryChecks for RefuseAfterCapture {
        fn check(&self) -> Result<(), ShardDirectoryCheckFailure> {
            if self.0.load(Ordering::Acquire) == 0 {
                Ok(())
            } else {
                Err(ShardDirectoryCheckFailure::Refused)
            }
        }
    }

    let secret = b"UNSELECTED_OTHER_PARTITION_SECRET";
    for refuse_after in ["read", "lease"] {
        let root = Temporary::new();
        let mut cfg = config(&root.0);
        // Keep these three genuine frames together; operation caps are unchanged.
        cfg.target_pack_bytes = 1 << 20;
        let mut store = ShardStore::open(cfg).expect("actual shared native pack");
        let first = reservation(0);
        let mut other = reservation(1);
        other.topic_id = TopicId::new(2);
        let last = reservation(2);
        for (item, payload) in [
            (first, b"selected first".as_slice()),
            (other, secret.as_slice()),
            (last, b"selected second".as_slice()),
        ] {
            store
                .append(item, None, payload, true)
                .expect("real interleaved native producer");
        }
        let planned = store
            .plan_fetch(partition(), LogicalOffset::new(0), 4096, 2)
            .expect("native target-only plan");
        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].reservation(), first);
        assert_eq!(planned[1].reservation(), last);
        let other_plan = store
            .plan_fetch(
                TopicPartition::new(other.topic_id, other.partition_id),
                LogicalOffset::new(0),
                4096,
                1,
            )
            .expect("independent unselected native metadata");
        assert_eq!(other_plan.len(), 1);
        assert_eq!(other_plan[0].reservation(), other);
        assert_eq!(planned[0].pack_sequence, other_plan[0].pack_sequence);
        assert_eq!(planned[0].pack_sequence, planned[1].pack_sequence);
        assert!(coalesces(
            &planned[0],
            &planned[1],
            payload_end(&planned[0]).expect("actual first payload end")
        ));
        let start = planned[0].payload_offset;
        let end = payload_end(&planned[1]).expect("actual last payload end");
        let range_len = usize::try_from(end - start).expect("finite actual range");
        let other_start = usize::try_from(other_plan[0].payload_offset - start)
            .expect("other record actually inside the target range");
        let checks = Arc::new(RefuseAfterCapture(AtomicUsize::new(0)));
        let captured_checks = checks.clone();
        let capture = std::rc::Rc::new(RefCell::new(None));
        let captured = capture.clone();
        let _observe = observe_owner(move |phase, owner| {
            if phase == refuse_after {
                assert!(matches!(owner.read_result.as_ref(), Some(Ok(n)) if *n == range_len));
                let bytes = if phase == "read" {
                    owner
                        .raw
                        .as_ref()
                        .expect("actual filled raw owner")
                        .as_ref()
                } else {
                    owner
                        .promoted
                        .as_ref()
                        .expect("actual promoted owner")
                        .as_ref()
                };
                assert_eq!(&bytes[other_start..other_start + secret.len()], secret);
                *captured.borrow_mut() =
                    Some((bytes.as_ptr(), reader_identity(owned_reader(owner))));
                // Only after actual native read/promotion has returned do the
                // existing directory checks produce this genuine refusal.
                captured_checks.0.store(1, Ordering::Release);
            }
        });
        let expiry = Instant::now() + Duration::from_secs(10);
        let original = StorageFetchBudget::new_with_checks(limits(), expiry, checks)
            .expect("same finite original counters and E");
        let error = store
            .read_fetch_plan_until(&planned, &original)
            .expect_err("real post-capture checkpoint refusal");
        assert!(matches!(
            error.failure(),
            StorageFetchFailure::Checkpoint {
                failure: ShardDirectoryCheckFailure::Refused,
                storage_error: None,
                late: false,
            }
        ));
        assert!(error.batches().is_empty());
        assert!(error.has_native_reader() && error.read_entered() && error.read_returned());
        assert!(!error.native_error_returned());
        assert_eq!(error.last_read_bytes(), Some(range_len));
        assert_eq!(error.read_range(), Some((start, range_len)));
        let bytes = error
            .unpublished_bytes()
            .expect("internal complete physical owner");
        let (pointer, identity) = capture.borrow().expect("actual capture before refusal");
        assert_eq!(bytes.as_ptr(), pointer);
        assert_eq!(&bytes[other_start..other_start + secret.len()], secret);
        assert_eq!(error.unpublished_buffer_len(), Some(range_len));
        assert_eq!(
            reader_identity(owned_reader(
                error.current.as_ref().expect("retained range")
            )),
            identity
        );
        let current = error.current.as_ref().expect("same current range");
        assert_eq!(current.raw.is_some(), refuse_after == "read");
        assert_eq!(current.promoted.is_some(), refuse_after == "lease");
        let debug = format!("{error:?}");
        let secret_text = std::str::from_utf8(secret).expect("fixed ASCII sentinel");
        let secret_numbers = secret
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        assert!(
            !debug.contains(secret_text),
            "no promoted range contents in Debug"
        );
        assert!(
            !debug.contains(&secret_numbers),
            "no raw Vec contents in Debug"
        );
        assert!(debug.contains("ReadCustody") && debug.contains("range_len"));
        assert!(debug.contains("has_raw_buffer") && debug.contains("has_promoted_buffer"));
        assert_eq!(original.absolute_expiry(), expiry);
        let usage = original.usage().expect("same original paid read counters");
        assert_eq!(usage.read_attempts, 1);
        assert_eq!(usage.read_bytes, range_len);
        assert!(matches!(original.check(), Err(StorageFetchFailure::Denied)));
        let pool = Arc::clone(&store.read_buffer_pool.cached_buffers);
        assert_eq!(pool.load(Ordering::Acquire), 0);
        drop(store);
        assert_eq!(
            reader_identity(owned_reader(
                error.current.as_ref().expect("same File after store drop")
            )),
            identity
        );
        drop(error);
        assert_eq!(
            pool.load(Ordering::Acquire),
            0,
            "original recycler receiver is gone"
        );
    }
}
