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
