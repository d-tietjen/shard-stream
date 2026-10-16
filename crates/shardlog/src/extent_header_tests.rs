//! Exact pre-factoring native decoder differential and incomplete-header boundaries.
//! Invalid byte mutations below are codec probes, never signed/current proofs.
use super::*;
use crate::{RecoveryExtentTail, RecoveryExtentTailUnknown, inspect_recovery_extent_tail};

// The exact original native complete decoder, renamed only for differential.
fn original_decode_extent(
    path: &Path,
    frame_offset: u64,
    frame: &[u8],
    _physical_shard: ShardId,
) -> StorageResult<ExtentLocation> {
    if frame.len() < HEADER_LEN + CHECKSUM_LEN {
        return Err(StorageError::corrupt(path, frame_offset, "frame too short"));
    }
    let expected_checksum = u64::from_le_bytes(
        frame[frame.len() - CHECKSUM_LEN..]
            .try_into()
            .expect("checksum width"),
    );
    let actual_checksum = extent_checksum(
        &frame[..HEADER_LEN],
        &frame[HEADER_LEN..frame.len() - CHECKSUM_LEN],
    );
    if actual_checksum != expected_checksum {
        return Err(StorageError::corrupt(
            path,
            frame_offset,
            "extent checksum mismatch",
        ));
    }

    let mut decoder = Decoder::new(&frame[PREFIX_LEN..frame.len() - CHECKSUM_LEN]);
    let topic_id = TopicId::new(decoder.u128()?);
    let partition_id = LogicalPartitionId::new(decoder.u32()?);
    let batch_id = BatchId::new(decoder.u128()?);
    let first_offset = LogicalOffset::new(decoder.u64()?);
    let last_offset = LogicalOffset::new(decoder.u64()?);
    let record_count = std::num::NonZeroU32::new(decoder.u32()?)
        .ok_or_else(|| StorageError::corrupt(path, frame_offset, "zero record count"))?;
    let virtual_lane_id = VirtualLaneId::new(decoder.u32()?);
    let ring_epoch = RingEpoch::new(decoder.u64()?);
    let leader_epoch = LeaderEpoch::new(decoder.u64()?);
    let sequence = PlacementSequence::new(decoder.u64()?);
    let producer_present = decoder.u8()?;
    let producer_reserved = decoder.take(3)?;
    let producer_id = decoder.u128()?;
    let producer_epoch = decoder.u32()?;
    let producer_first_sequence = decoder.u64()?;
    let digest_algorithm = decoder.u8()?;
    let digest_reserved = decoder.take(3)?;
    let payload_digest: [u8; 16] = decoder.take(16)?.try_into().expect("producer digest width");
    let event_present = decoder.u8()?;
    let event_reserved = decoder.take(3)?;
    let event_id = decoder.u128()?;
    if producer_reserved != [0; 3] || digest_reserved != [0; 3] || event_reserved != [0; 3] {
        return Err(StorageError::corrupt(
            path,
            frame_offset,
            "producer reserved bytes are nonzero",
        ));
    }
    let producer = match producer_present {
        0 => {
            if producer_id != 0
                || producer_epoch != 0
                || producer_first_sequence != 0
                || digest_algorithm != 0
                || payload_digest != [0; 16]
                || event_present != 0
                || event_id != 0
            {
                return Err(StorageError::corrupt(
                    path,
                    frame_offset,
                    "absent producer metadata is nonzero",
                ));
            }
            None
        }
        1 if digest_algorithm == DIGEST_XXH3_128 => {
            let event_id = match event_present {
                0 if event_id == 0 => None,
                1 => Some(RecordId::new(event_id)),
                _ => {
                    return Err(StorageError::corrupt(
                        path,
                        frame_offset,
                        "invalid producer event metadata",
                    ));
                }
            };
            Some(ProducerSequence {
                producer_id,
                epoch: producer_epoch,
                first_sequence: producer_first_sequence,
                event_id,
                digest_algorithm,
                payload_digest,
            })
        }
        1 => {
            return Err(StorageError::corrupt(
                path,
                frame_offset,
                "unsupported producer digest algorithm",
            ));
        }
        _ => {
            return Err(StorageError::corrupt(
                path,
                frame_offset,
                "invalid producer metadata flag",
            ));
        }
    };
    let group_present = decoder.u8()?;
    let group_reserved = decoder.take(3)?;
    let transaction_id = decoder.u128()?;
    let chunk_sequence = decoder.u64()?;
    let group_payload_digest: [u8; 16] = decoder.take(16)?.try_into().expect("group digest width");
    if group_reserved != [0; 3] {
        return Err(StorageError::corrupt(
            path,
            frame_offset,
            "atomic group reserved bytes are nonzero",
        ));
    }
    let atomic_group = match group_present {
        0 if transaction_id == 0 && chunk_sequence == 0 && group_payload_digest == [0; 16] => None,
        1 => Some(AtomicGroupSequence {
            transaction_id,
            chunk_sequence,
            payload_digest: group_payload_digest,
        }),
        _ => {
            return Err(StorageError::corrupt(
                path,
                frame_offset,
                "invalid atomic group metadata",
            ));
        }
    };
    let payload_len = usize::try_from(decoder.u64()?)
        .map_err(|_| StorageError::corrupt(path, frame_offset, "payload length overflow"))?;
    if first_offset
        .get()
        .checked_add(u64::from(record_count.get()) - 1)
        != Some(last_offset.get())
    {
        return Err(StorageError::corrupt(
            path,
            frame_offset,
            "record range does not match record count",
        ));
    }
    if decoder.take(payload_len).is_err() || !decoder.is_empty() {
        return Err(StorageError::corrupt(
            path,
            frame_offset,
            "payload length does not match frame",
        ));
    }

    Ok(ExtentLocation {
        reservation: Reservation {
            topic_id,
            partition_id,
            batch_id,
            first_offset,
            last_offset,
            record_count,
            placement: Placement {
                virtual_lane_id,
                ring_epoch,
                leader_epoch,
                sequence,
            },
        },
        producer,
        atomic_group,
        pack_sequence: 0,
        payload_offset: frame_offset + HEADER_LEN as u64,
        payload_len,
    })
}

fn native_frame(producer: Option<ProducerSequence>, group: Option<AtomicGroupSequence>) -> Vec<u8> {
    let payload = b"genuine native encoder payload";
    let reservation = Reservation {
        topic_id: TopicId::new(11),
        partition_id: LogicalPartitionId::new(3),
        batch_id: BatchId::new(7),
        first_offset: LogicalOffset::new(9),
        last_offset: LogicalOffset::new(10),
        record_count: std::num::NonZeroU32::new(2).expect("native count"),
        placement: Placement {
            virtual_lane_id: VirtualLaneId::new(3),
            ring_epoch: RingEpoch::new(2),
            leader_epoch: LeaderEpoch::new(u64::MAX),
            sequence: PlacementSequence::new(7),
        },
    };
    let encoded =
        encode_extent(reservation, producer, group, payload).expect("actual native encoder");
    let mut frame = encoded.header;
    frame.extend_from_slice(payload);
    frame.extend_from_slice(&encoded.checksum);
    frame
}
fn compare(frame: &[u8]) {
    let path = Path::new("native-codec-differential.sse");
    let current = decode_extent(path, 17, frame, ShardId::new(0))
        .map(|v| format!("{v:?}"))
        .map_err(|e| e.to_string());
    let original = original_decode_extent(path, 17, frame, ShardId::new(0))
        .map(|v| format!("{v:?}"))
        .map_err(|e| e.to_string());
    assert_eq!(
        current, original,
        "complete native acceptance/error ordering"
    );
}
#[test]
fn shared_header_preserves_complete_native_decode_and_checksum_first_errors() {
    let producer = ProducerSequence {
        producer_id: 7,
        epoch: 3,
        first_sequence: 9,
        event_id: Some(RecordId::new(11)),
        digest_algorithm: DIGEST_XXH3_128,
        payload_digest: [4; 16],
    };
    let group = AtomicGroupSequence {
        transaction_id: 9,
        chunk_sequence: 2,
        payload_digest: [5; 16],
    };
    for producer in [None, Some(producer)] {
        for group in [None, Some(group)] {
            let frame = native_frame(producer, group);
            compare(&frame);
            let RecoveryExtentTail::Header(header) = inspect_recovery_extent_tail(
                Path::new("native-optional-header.sse"),
                17,
                &frame[..HEADER_LEN],
                1024,
            ) else {
                panic!("actual native optional metadata must share header acceptance");
            };
            assert_eq!(header.producer, producer);
            assert_eq!(header.atomic_group, group);
            // All truncations keep the native too-short/checksum order too.
            for length in 0..frame.len() {
                compare(&frame[..length]);
            }
            // Each actual native header byte exercises range, flags, reserved,
            // optional producer/group and length acceptance against old decode.
            for at in PREFIX_LEN..HEADER_LEN {
                let mut invalid = frame.clone();
                invalid[at] ^= 1;
                compare(&invalid);
                assert!(
                    decode_extent(Path::new("codec.sse"), 17, &invalid, ShardId::new(0))
                        .expect_err("bad checksum precedes header interpretation")
                        .to_string()
                        .contains("extent checksum mismatch")
                );
                // This deliberate native-checksum recomputation is ONLY an
                // invalid-codec boundary probe, never a recovery input proof.
                let checksum = extent_checksum(
                    &invalid[..HEADER_LEN],
                    &invalid[HEADER_LEN..invalid.len() - CHECKSUM_LEN],
                );
                let end = invalid.len() - CHECKSUM_LEN;
                invalid[end..].copy_from_slice(&checksum.to_le_bytes());
                compare(&invalid);
            }
        }
    }
}
#[test]
fn native_tail_header_maximum_is_conservative_and_short_or_invalid_stays_unknown() {
    let frame = native_frame(None, None);
    let path = Path::new("actual-native-header.sse");
    for length in 0..HEADER_LEN {
        assert_eq!(
            inspect_recovery_extent_tail(path, 0, &frame[..length], 1024),
            RecoveryExtentTail::Unknown(RecoveryExtentTailUnknown::ShortHeader)
        );
    }
    for length in HEADER_LEN..frame.len() {
        let RecoveryExtentTail::Header(header) =
            inspect_recovery_extent_tail(path, 0, &frame[..length], 1024)
        else {
            panic!("every complete valid fixed header contributes its conservative observation");
        };
        assert_eq!(header.reservation.placement.leader_epoch.get(), u64::MAX);
        assert!(
            header
                .reservation
                .placement
                .leader_epoch
                .get()
                .checked_add(1)
                .is_none()
        );
        assert_eq!(header.declared_frame_bytes, frame.len());
        assert_eq!(
            header.payload_bytes,
            frame.len() - HEADER_LEN - CHECKSUM_LEN
        );
    }
    assert_eq!(
        inspect_recovery_extent_tail(path, 0, &frame, 1024),
        RecoveryExtentTail::Unknown(RecoveryExtentTailUnknown::NotIncomplete)
    );
    let mut invalid = frame[..HEADER_LEN].to_vec();
    invalid[0] ^= 1;
    assert_eq!(
        inspect_recovery_extent_tail(path, 0, &invalid, 1024),
        RecoveryExtentTail::Unknown(RecoveryExtentTailUnknown::InvalidHeader)
    );
    let mut invalid = frame[..HEADER_LEN].to_vec();
    // Last native field is payload_len; it must agree with declared frame size.
    invalid[HEADER_LEN - 8] ^= 1;
    assert_eq!(
        inspect_recovery_extent_tail(path, 0, &invalid, 1024),
        RecoveryExtentTail::Unknown(RecoveryExtentTailUnknown::InvalidHeader)
    );
}
