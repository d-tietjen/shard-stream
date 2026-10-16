use std::num::NonZeroU32;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use super::*;
use crate::{
    BatchId, DIGEST_XXH3_128, LeaderEpoch, LogicalOffset, LogicalPartitionId, Placement,
    PlacementSequence, RecordId, RingEpoch, ShardId, ShardStore, ShardStoreConfig, TopicId,
    VirtualLaneId, inspect_recovery_extent,
};

fn limits() -> RecoveryExtentInspectionLimits {
    RecoveryExtentInspectionLimits {
        max_batch_bytes: 4096,
        max_frame_bytes: 8192,
        max_path_bytes: 4096,
        max_declared_bytes: 64 * 1024,
        max_work_bytes: 64 * 1024,
        max_entries: 64,
    }
}

fn reservation(batch: u128) -> Reservation {
    Reservation {
        topic_id: TopicId::new(u128::MAX),
        partition_id: LogicalPartitionId::new(u32::MAX),
        batch_id: BatchId::new(batch),
        first_offset: LogicalOffset::new(19),
        last_offset: LogicalOffset::new(20),
        record_count: NonZeroU32::new(2).expect("native nonzero record count"),
        placement: Placement {
            virtual_lane_id: VirtualLaneId::new(3),
            ring_epoch: RingEpoch::new(u64::MAX),
            leader_epoch: LeaderEpoch::new(u64::MAX),
            sequence: PlacementSequence::new(u64::MAX),
        },
    }
}

fn writer(directory: &Path) -> ShardStore {
    ShardStore::open(ShardStoreConfig {
        directory: directory.to_path_buf(),
        shard_id: ShardId::new(3),
        target_pack_bytes: 1024 * 1024,
        max_batch_bytes: limits().max_batch_bytes,
    })
    .expect("actual native store")
}

fn native_frame(
    value: Reservation,
    producer: Option<ProducerSequence>,
    group: Option<AtomicGroupSequence>,
    payload: &[u8],
) -> (tempfile::TempDir, PathBuf, Vec<u8>) {
    let directory = tempfile::tempdir().expect("native source directory");
    let mut store = writer(directory.path());
    store
        .append_atomic(value, producer, group, payload, true)
        .expect("actual native append and sync");
    let path = store.active_path().to_path_buf();
    drop(store);
    let bytes = std::fs::read(&path).expect("actual native frame bytes");
    (directory, path, bytes)
}

fn native_reason(error: &StorageError) -> &str {
    match error {
        StorageError::Corrupt { reason, .. } => reason,
        other => panic!("expected native corruption, got {other}"),
    }
}

#[test]
fn native_writer_payload_and_full_metadata_are_borrowed_from_original_frame() {
    let payload = b"\0actual native payload\xff\0";
    let producer = ProducerSequence {
        producer_id: u128::MAX,
        epoch: u32::MAX,
        first_sequence: u64::MAX - 1,
        event_id: Some(RecordId::new(u128::MAX)),
        digest_algorithm: DIGEST_XXH3_128,
        payload_digest: xxhash_rust::xxh3::xxh3_128(payload).to_le_bytes(),
    };
    let group = AtomicGroupSequence {
        transaction_id: u128::MAX,
        chunk_sequence: u64::MAX,
        payload_digest: producer.payload_digest,
    };
    let value = reservation(u128::MAX);
    let (_directory, path, frame) = native_frame(value, Some(producer), Some(group), payload);
    let before = frame.clone();
    let expiry = Instant::now() + Duration::from_secs(5);
    let view = inspect_recovery_extent_payload(&path, 0, &frame, limits(), expiry)
        .expect("native borrowed inspection");
    assert_eq!(view.reservation(), value);
    assert_eq!(view.producer(), Some(producer));
    assert_eq!(view.atomic_group(), Some(group));
    assert_eq!(view.payload(), payload);
    assert!(std::ptr::eq(view.frame().as_ptr(), frame.as_ptr()));
    let actual_payload_at = frame
        .windows(payload.len())
        .position(|window| window == payload)
        .expect("actual native body occurrence");
    assert!(std::ptr::eq(
        view.payload().as_ptr(),
        frame[actual_payload_at..].as_ptr()
    ));
    let catalog = inspect_recovery_extent(&path, 0, &frame, limits().max_batch_bytes, 27)
        .expect("ordinary native catalog inspector");
    assert_eq!(catalog.reservation, view.reservation());
    assert_eq!(catalog.producer, view.producer());
    assert_eq!(catalog.atomic_group, view.atomic_group());
    assert_eq!(catalog.payload_bytes, payload.len() as u64);
    assert_eq!(frame, before);
    assert_eq!(
        std::fs::read(&path).expect("native source remains exact"),
        before
    );
}

#[test]
fn actual_consecutive_native_frames_support_empty_body_and_nonzero_source_offset() {
    let directory = tempfile::tempdir().expect("actual native source");
    let mut store = writer(directory.path());
    store
        .append(reservation(1), None, b"first native body", true)
        .expect("first actual frame");
    store
        .append(reservation(2), None, b"", true)
        .expect("native empty payload");
    let path = store.active_path().to_path_buf();
    drop(store);
    let bytes = std::fs::read(&path).expect("actual complete pack");
    let prefix = bytes[..RECOVERY_EXTENT_PREFIX_BYTES]
        .try_into()
        .expect("native prefix");
    let first_len = recovery_extent_frame_bytes(&path, 0, prefix, limits().max_batch_bytes)
        .expect("native frame size");
    let first = inspect_recovery_extent_payload(
        &path,
        0,
        &bytes[..first_len],
        limits(),
        Instant::now() + Duration::from_secs(5),
    )
    .expect("first native body");
    assert_eq!(first.payload(), b"first native body");
    let second = inspect_recovery_extent_payload(
        &path,
        first_len as u64,
        &bytes[first_len..],
        limits(),
        Instant::now() + Duration::from_secs(5),
    )
    .expect("empty native body at actual nonzero offset");
    assert_eq!(second.reservation(), reservation(2));
    assert!(second.payload().is_empty());
    assert!(second.producer().is_none() && second.atomic_group().is_none());
    assert_eq!(second.frame().as_ptr(), bytes[first_len..].as_ptr());
}

#[test]
fn checksum_truncation_and_real_writer_invalid_range_match_native_acceptance() {
    let (_directory, path, frame) = native_frame(reservation(9), None, None, b"actual body");
    let mut bad_checksum = frame.clone();
    *bad_checksum.last_mut().expect("native checksum byte") ^= 1;
    let mut bad_magic = frame.clone();
    bad_magic[0] ^= 1;
    let mut extra = frame.clone();
    extra.push(0);
    for damaged in [
        bad_checksum,
        bad_magic,
        frame[..frame.len() - 1].to_vec(),
        extra,
    ] {
        let ordinary = inspect_recovery_extent(&path, 0, &damaged, limits().max_batch_bytes, 0)
            .expect_err("ordinary native refusal");
        let error = inspect_recovery_extent_payload(
            &path,
            0,
            &damaged,
            limits(),
            Instant::now() + Duration::from_secs(5),
        )
        .expect_err("same complete native refusal");
        let RecoveryExtentInspectionError::Native(actual) = error else {
            panic!("expected actual native error")
        };
        assert_eq!(native_reason(&actual), native_reason(&ordinary));
    }
    // The ordinary writer encodes the actual typed reservation without a range
    // check. Both recovery APIs must apply the actual decoder's range check.
    let mut inconsistent = reservation(10);
    inconsistent.last_offset = LogicalOffset::new(999);
    let (_bad_directory, bad_path, encoded) =
        native_frame(inconsistent, None, None, b"native invalid range");
    let ordinary = inspect_recovery_extent(&bad_path, 0, &encoded, limits().max_batch_bytes, 0)
        .expect_err("native range validation");
    let error = inspect_recovery_extent_payload(
        &bad_path,
        0,
        &encoded,
        limits(),
        Instant::now() + Duration::from_secs(5),
    )
    .expect_err("same native range validation");
    let RecoveryExtentInspectionError::Native(actual) = error else {
        panic!("actual decoder expected")
    };
    assert_eq!(
        native_reason(&actual),
        "record range does not match record count"
    );
    assert_eq!(native_reason(&actual), native_reason(&ordinary));
    assert!(NonZeroU32::new(0).is_none());
}

#[test]
fn actual_cost_exact_fit_and_each_one_less_refuse_before_native_checksum() {
    let (_directory, path, frame) =
        native_frame(reservation(11), None, None, b"bounded actual native body");
    let path_bytes = path.as_os_str().as_encoded_bytes().len();
    let cost = recovery_extent_inspection_cost(frame.len(), path_bytes, limits())
        .expect("plain native facts");
    assert_eq!(cost.input_bytes(), frame.len());
    let mut exact = limits();
    exact.max_frame_bytes = frame.len();
    exact.max_path_bytes = path_bytes;
    exact.max_declared_bytes = cost.declared_bytes();
    exact.max_work_bytes = cost.work_bytes();
    exact.max_entries = cost.entries();
    assert_eq!(
        recovery_extent_inspection_cost(frame.len(), path_bytes, exact).expect("exact finite cost"),
        cost
    );
    assert_eq!(
        inspect_recovery_extent_payload(
            &path,
            0,
            &frame,
            exact,
            Instant::now() + Duration::from_secs(5)
        )
        .expect("exact fit real native input")
        .payload(),
        b"bounded actual native body"
    );
    let mut corrupt = frame.clone();
    *corrupt.last_mut().expect("actual checksum") ^= 1;
    for axis in 0..5 {
        let mut short = exact;
        match axis {
            0 => short.max_frame_bytes -= 1,
            1 => short.max_path_bytes -= 1,
            2 => short.max_declared_bytes -= 1,
            3 => short.max_work_bytes -= 1,
            4 => short.max_entries -= 1,
            _ => unreachable!(),
        }
        assert!(matches!(
            inspect_recovery_extent_payload(
                &path,
                0,
                &corrupt,
                short,
                Instant::now() + Duration::from_secs(5)
            ),
            Err(RecoveryExtentInspectionError::Capacity)
        ));
    }
    assert!(matches!(
        inspect_recovery_extent_payload(
            &path,
            0,
            &corrupt,
            exact,
            Instant::now() + Duration::from_secs(5)
        ),
        Err(RecoveryExtentInspectionError::Native(_))
    ));
}

#[test]
fn original_absolute_expiry_precedes_limits_and_cannot_be_renewed_by_sizing() {
    let (_directory, path, frame) =
        native_frame(reservation(12), None, None, b"saved expiry native body");
    let expiry = Instant::now() + Duration::from_millis(250);
    let cost = recovery_extent_inspection_cost(
        frame.len(),
        path.as_os_str().as_encoded_bytes().len(),
        limits(),
    )
    .expect("cost has no new clock");
    assert_eq!(
        inspect_recovery_extent_payload(&path, 0, &frame, limits(), expiry)
            .expect("initial original-E call")
            .payload(),
        b"saved expiry native body"
    );
    std::thread::sleep(expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(1));
    assert!(matches!(
        inspect_recovery_extent_payload(&path, 0, &frame, limits(), expiry),
        Err(RecoveryExtentInspectionError::Deadline)
    ));
    let mut invalid = limits();
    invalid.max_entries = 0;
    assert!(matches!(
        inspect_recovery_extent_payload(&path, 0, &frame, invalid, expiry),
        Err(RecoveryExtentInspectionError::Deadline)
    ));
    assert_eq!(
        recovery_extent_inspection_cost(
            frame.len(),
            path.as_os_str().as_encoded_bytes().len(),
            limits()
        )
        .expect("sizing never renews E"),
        cost
    );
    assert_eq!(
        std::fs::read(&path).expect("same original source retained"),
        frame
    );
}

#[test]
fn native_payload_ceiling_offset_overflow_and_invalid_limits_remain_finite() {
    let (_directory, path, frame) = native_frame(reservation(13), None, None, b"four");
    let mut smaller = limits();
    smaller.max_batch_bytes = 3;
    let error = inspect_recovery_extent_payload(
        &path,
        0,
        &frame,
        smaller,
        Instant::now() + Duration::from_secs(5),
    )
    .expect_err("actual native ceiling");
    let RecoveryExtentInspectionError::Native(error) = error else {
        panic!("native frame ceiling expected")
    };
    assert_eq!(native_reason(&error), "invalid extent frame length");
    assert!(matches!(
        inspect_recovery_extent_payload(
            &path,
            u64::MAX,
            &frame,
            limits(),
            Instant::now() + Duration::from_secs(5)
        ),
        Err(RecoveryExtentInspectionError::Capacity)
    ));
    let mut invalid = limits();
    invalid.max_batch_bytes = 0;
    assert!(matches!(
        recovery_extent_inspection_cost(frame.len(), 1, invalid),
        Err(RecoveryExtentInspectionError::InvalidLimits)
    ));
    assert!(matches!(
        recovery_extent_inspection_cost(usize::MAX, 1, limits()),
        Err(RecoveryExtentInspectionError::Capacity)
    ));
}
