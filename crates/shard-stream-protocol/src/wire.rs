use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;

use bytes::Bytes;
use shard_stream_core::{
    BatchId, LeaderEpoch, LogicalOffset, Placement, PlacementSequence, RecordId, RingEpoch,
    VirtualLaneId,
};

use crate::{AppendResponse, AtomicGroupIdentity};

const MAGIC: &[u8; 4] = b"SSB5";
const VERSION: u8 = 5;
const PREFIX_LEN: usize = 4 + 1 + 1 + 2 + 8;
const BODY_FIXED_LEN: usize = 16 + 16 + 8 + 8 + 4 + 4 + 8 + 8 + 8 + 1 + 3 + 16 + 1 + 3 + 16 + 8 + 8;
const HEADER_LEN: usize = PREFIX_LEN + BODY_FIXED_LEN;
const CRC_LEN: usize = 4;

const APPEND_RESPONSE_MAGIC: &[u8; 4] = b"SAR1";
const APPEND_RESPONSE_VERSION: u8 = 1;
const APPEND_RESPONSE_LEN: usize = 8 + 16 + 16 + 16 + 8 + 8 + 8 + 4 + 8 + 8 + 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeFetchBatch {
    pub request_id: u128,
    pub batch_id: BatchId,
    pub event_id: Option<RecordId>,
    pub atomic_group: Option<AtomicGroupIdentity>,
    pub first_offset: LogicalOffset,
    pub last_offset: LogicalOffset,
    pub record_count: NonZeroU32,
    pub placement: Placement,
    pub payload: Bytes,
}

#[derive(Debug)]
pub struct EncodedFetchResponse {
    encoded_len: usize,
    segments: Vec<Bytes>,
}

impl EncodedFetchResponse {
    #[must_use]
    pub const fn len(&self) -> usize {
        self.encoded_len
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.encoded_len == 0
    }

    #[must_use]
    pub fn into_segments(self) -> Vec<Bytes> {
        self.segments
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
    FrameTooLarge,
    Truncated,
    InvalidMagic,
    UnsupportedVersion(u8),
    UnsupportedFlags(u8),
    InvalidHeaderLength,
    InvalidFrameLength,
    ChecksumMismatch,
    InvalidRecordRange,
    TrailingBytes,
}

impl fmt::Display for WireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooLarge => formatter.write_str("native frame exceeds configured limit"),
            Self::Truncated => formatter.write_str("truncated native frame"),
            Self::InvalidMagic => formatter.write_str("invalid native frame magic"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported native frame version {version}")
            }
            Self::UnsupportedFlags(flags) => {
                write!(formatter, "unsupported native frame flags {flags}")
            }
            Self::InvalidHeaderLength => formatter.write_str("invalid native header length"),
            Self::InvalidFrameLength => formatter.write_str("invalid native frame length"),
            Self::ChecksumMismatch => formatter.write_str("native frame checksum mismatch"),
            Self::InvalidRecordRange => formatter.write_str("invalid native record range"),
            Self::TrailingBytes => formatter.write_str("trailing bytes after native frames"),
        }
    }
}

impl Error for WireError {}

/// Content type used by native producers when they want a fixed-width response
/// instead of the compatibility JSON representation.
pub const NATIVE_APPEND_RESPONSE_CONTENT_TYPE: &str =
    "application/vnd.shard-stream.append-response.v1";

/// Encodes an append receipt without decimal string conversion or JSON
/// allocation. The response is intentionally fixed width so a native client
/// can validate it with one length check and decode it in place.
pub fn encode_append_response(response: AppendResponse) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(APPEND_RESPONSE_LEN);
    encoded.extend_from_slice(APPEND_RESPONSE_MAGIC);
    encoded.push(APPEND_RESPONSE_VERSION);
    encoded.push(0);
    encoded.extend_from_slice(&(APPEND_RESPONSE_LEN as u16).to_le_bytes());
    encoded.extend_from_slice(&response.request_id.to_le_bytes());
    encoded.extend_from_slice(&response.batch_id.get().to_le_bytes());
    encoded.extend_from_slice(&response.record_id.get().to_le_bytes());
    encoded.extend_from_slice(&response.first_offset.get().to_le_bytes());
    encoded.extend_from_slice(&response.last_offset.get().to_le_bytes());
    encoded.extend_from_slice(&response.visible_through.get().to_le_bytes());
    encoded.extend_from_slice(&response.placement.virtual_lane_id.get().to_le_bytes());
    encoded.extend_from_slice(&response.placement.ring_epoch.get().to_le_bytes());
    encoded.extend_from_slice(&response.placement.leader_epoch.get().to_le_bytes());
    encoded.extend_from_slice(&response.placement.sequence.get().to_le_bytes());
    debug_assert_eq!(encoded.len(), APPEND_RESPONSE_LEN);
    encoded
}

/// Decodes the fixed-width native append receipt.
pub fn decode_append_response(bytes: &[u8]) -> Result<AppendResponse, WireError> {
    if bytes.len() < APPEND_RESPONSE_LEN {
        return Err(WireError::Truncated);
    }
    if bytes.len() != APPEND_RESPONSE_LEN {
        return Err(WireError::InvalidFrameLength);
    }
    if &bytes[..4] != APPEND_RESPONSE_MAGIC {
        return Err(WireError::InvalidMagic);
    }
    if bytes[4] != APPEND_RESPONSE_VERSION {
        return Err(WireError::UnsupportedVersion(bytes[4]));
    }
    if bytes[5] != 0 {
        return Err(WireError::UnsupportedFlags(bytes[5]));
    }
    if usize::from(u16::from_le_bytes(
        bytes[6..8].try_into().expect("response length"),
    )) != APPEND_RESPONSE_LEN
    {
        return Err(WireError::InvalidHeaderLength);
    }
    Ok(AppendResponse {
        request_id: read_u128(bytes, 8)?,
        batch_id: BatchId::new(read_u128(bytes, 24)?),
        record_id: RecordId::new(read_u128(bytes, 40)?),
        first_offset: LogicalOffset::new(read_u64(bytes, 56)?),
        last_offset: LogicalOffset::new(read_u64(bytes, 64)?),
        visible_through: LogicalOffset::new(read_u64(bytes, 72)?),
        placement: Placement {
            virtual_lane_id: VirtualLaneId::new(read_u32(bytes, 80)?),
            ring_epoch: RingEpoch::new(read_u64(bytes, 84)?),
            leader_epoch: LeaderEpoch::new(read_u64(bytes, 92)?),
            sequence: PlacementSequence::new(read_u64(bytes, 100)?),
        },
    })
}

pub fn encode_fetch_batches(
    batches: &[NativeFetchBatch],
    max_output_bytes: usize,
) -> Result<Vec<u8>, WireError> {
    let response = encode_fetch_batches_segmented(batches, max_output_bytes)?;
    let mut output = Vec::with_capacity(response.len());
    for segment in response.into_segments() {
        output.extend_from_slice(&segment);
    }
    Ok(output)
}

/// Encodes native fetch frames as immutable header, payload, and checksum
/// segments. The payload segment retains the storage buffer instead of copying
/// it into one monolithic response allocation.
pub fn encode_fetch_batches_segmented(
    batches: &[NativeFetchBatch],
    max_output_bytes: usize,
) -> Result<EncodedFetchResponse, WireError> {
    let mut encoded_len = 0usize;
    let mut segments = Vec::with_capacity(batches.len().saturating_mul(3));
    for batch in batches {
        let frame_len = HEADER_LEN
            .checked_add(batch.payload.len())
            .and_then(|value| value.checked_add(CRC_LEN))
            .ok_or(WireError::FrameTooLarge)?;
        encoded_len = encoded_len
            .checked_add(frame_len)
            .ok_or(WireError::FrameTooLarge)?;
        if encoded_len > max_output_bytes {
            return Err(WireError::FrameTooLarge);
        }
        let frame_len_u64 = u64::try_from(frame_len).map_err(|_| WireError::FrameTooLarge)?;
        let payload_len_u64 =
            u64::try_from(batch.payload.len()).map_err(|_| WireError::FrameTooLarge)?;
        let mut header = Vec::with_capacity(HEADER_LEN);
        header.extend_from_slice(MAGIC);
        header.push(VERSION);
        header.push(0);
        header.extend_from_slice(&(HEADER_LEN as u16).to_le_bytes());
        header.extend_from_slice(&frame_len_u64.to_le_bytes());
        header.extend_from_slice(&batch.request_id.to_le_bytes());
        header.extend_from_slice(&batch.batch_id.get().to_le_bytes());
        header.extend_from_slice(&batch.first_offset.get().to_le_bytes());
        header.extend_from_slice(&batch.last_offset.get().to_le_bytes());
        header.extend_from_slice(&batch.record_count.get().to_le_bytes());
        header.extend_from_slice(&batch.placement.virtual_lane_id.get().to_le_bytes());
        header.extend_from_slice(&batch.placement.ring_epoch.get().to_le_bytes());
        header.extend_from_slice(&batch.placement.leader_epoch.get().to_le_bytes());
        header.extend_from_slice(&batch.placement.sequence.get().to_le_bytes());
        match batch.event_id {
            Some(event_id) => {
                header.push(1);
                header.extend_from_slice(&[0; 3]);
                header.extend_from_slice(&event_id.get().to_le_bytes());
            }
            None => header.extend_from_slice(&[0; 20]),
        }
        match batch.atomic_group {
            Some(group) => {
                header.push(1);
                header.extend_from_slice(&[0; 3]);
                header.extend_from_slice(&group.transaction_id.to_le_bytes());
                header.extend_from_slice(&group.chunk_sequence.to_le_bytes());
            }
            None => header.extend_from_slice(&[0; 28]),
        }
        header.extend_from_slice(&payload_len_u64.to_le_bytes());
        debug_assert_eq!(header.len(), HEADER_LEN);
        let mut checksum = crc32fast::Hasher::new();
        checksum.update(&header);
        checksum.update(&batch.payload);
        let checksum = checksum.finalize();
        segments.push(Bytes::from(header));
        segments.push(batch.payload.clone());
        segments.push(Bytes::copy_from_slice(&checksum.to_le_bytes()));
    }
    Ok(EncodedFetchResponse {
        encoded_len,
        segments,
    })
}

pub fn decode_fetch_batches(
    bytes: &[u8],
    max_frame_bytes: usize,
) -> Result<Vec<NativeFetchBatch>, WireError> {
    decode_fetch_batches_owned(Bytes::copy_from_slice(bytes), max_frame_bytes)
}

/// Decodes a native fetch response while retaining payloads as slices of the
/// owned response buffer. Network clients should prefer this path so decoding
/// does not copy the application payload a second time.
pub fn decode_fetch_batches_owned(
    bytes: Bytes,
    max_frame_bytes: usize,
) -> Result<Vec<NativeFetchBatch>, WireError> {
    let mut batches = Vec::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        let prefix_end = cursor
            .checked_add(PREFIX_LEN)
            .ok_or(WireError::FrameTooLarge)?;
        let prefix = bytes.get(cursor..prefix_end).ok_or(WireError::Truncated)?;
        if &prefix[..4] != MAGIC {
            return Err(WireError::InvalidMagic);
        }
        if prefix[4] != VERSION {
            return Err(WireError::UnsupportedVersion(prefix[4]));
        }
        if prefix[5] != 0 {
            return Err(WireError::UnsupportedFlags(prefix[5]));
        }
        if usize::from(u16::from_le_bytes(
            prefix[6..8].try_into().expect("header width"),
        )) != HEADER_LEN
        {
            return Err(WireError::InvalidHeaderLength);
        }
        let frame_len_u64 = u64::from_le_bytes(prefix[8..16].try_into().expect("frame width"));
        let frame_len = usize::try_from(frame_len_u64).map_err(|_| WireError::FrameTooLarge)?;
        if frame_len < HEADER_LEN + CRC_LEN || frame_len > max_frame_bytes {
            return Err(WireError::InvalidFrameLength);
        }
        let frame_end = cursor
            .checked_add(frame_len)
            .ok_or(WireError::FrameTooLarge)?;
        let frame = bytes.get(cursor..frame_end).ok_or(WireError::Truncated)?;
        let expected_crc = u32::from_le_bytes(
            frame[frame.len() - CRC_LEN..]
                .try_into()
                .expect("CRC width"),
        );
        if crc32fast::hash(&frame[..frame.len() - CRC_LEN]) != expected_crc {
            return Err(WireError::ChecksumMismatch);
        }

        let body_start = cursor
            .checked_add(PREFIX_LEN)
            .ok_or(WireError::FrameTooLarge)?;
        let body_end = frame_end
            .checked_sub(CRC_LEN)
            .ok_or(WireError::InvalidFrameLength)?;
        let body = bytes
            .get(body_start..body_end)
            .ok_or(WireError::Truncated)?;
        if body.len() < BODY_FIXED_LEN {
            return Err(WireError::Truncated);
        }
        let request_id = read_u128(body, 0)?;
        let batch_id = BatchId::new(read_u128(body, 16)?);
        let first_offset = LogicalOffset::new(read_u64(body, 32)?);
        let last_offset = LogicalOffset::new(read_u64(body, 40)?);
        let record_count =
            NonZeroU32::new(read_u32(body, 48)?).ok_or(WireError::InvalidRecordRange)?;
        let virtual_lane_id = VirtualLaneId::new(read_u32(body, 52)?);
        let ring_epoch = RingEpoch::new(read_u64(body, 56)?);
        let leader_epoch = LeaderEpoch::new(read_u64(body, 64)?);
        let sequence = PlacementSequence::new(read_u64(body, 72)?);
        let event_present = body[80];
        if body[81..84] != [0; 3] {
            return Err(WireError::InvalidFrameLength);
        }
        let event_id = read_u128(body, 84)?;
        let event_id = match event_present {
            0 if event_id == 0 => None,
            1 => Some(RecordId::new(event_id)),
            _ => return Err(WireError::InvalidFrameLength),
        };
        let group_present = body[100];
        if body[101..104] != [0; 3] {
            return Err(WireError::InvalidFrameLength);
        }
        let transaction_id = read_u128(body, 104)?;
        let chunk_sequence = read_u64(body, 120)?;
        let atomic_group = match group_present {
            0 if transaction_id == 0 && chunk_sequence == 0 => None,
            1 => Some(AtomicGroupIdentity {
                transaction_id,
                chunk_sequence,
            }),
            _ => return Err(WireError::InvalidFrameLength),
        };
        let payload_len =
            usize::try_from(read_u64(body, 128)?).map_err(|_| WireError::FrameTooLarge)?;
        if first_offset
            .get()
            .checked_add(u64::from(record_count.get()) - 1)
            != Some(last_offset.get())
        {
            return Err(WireError::InvalidRecordRange);
        }
        let payload_start = body_start
            .checked_add(BODY_FIXED_LEN)
            .ok_or(WireError::FrameTooLarge)?;
        let _payload = bytes
            .get(payload_start..body_end)
            .filter(|payload| payload.len() == payload_len)
            .ok_or(WireError::InvalidFrameLength)?;
        batches.push(NativeFetchBatch {
            request_id,
            batch_id,
            event_id,
            atomic_group,
            first_offset,
            last_offset,
            record_count,
            placement: Placement {
                virtual_lane_id,
                ring_epoch,
                leader_epoch,
                sequence,
            },
            payload: bytes.slice(payload_start..body_end),
        });
        cursor = frame_end;
    }
    if cursor != bytes.len() {
        return Err(WireError::TrailingBytes);
    }
    Ok(batches)
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, WireError> {
    let end = offset.checked_add(4).ok_or(WireError::Truncated)?;
    Ok(u32::from_le_bytes(
        bytes
            .get(offset..end)
            .ok_or(WireError::Truncated)?
            .try_into()
            .expect("u32 width"),
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, WireError> {
    let end = offset.checked_add(8).ok_or(WireError::Truncated)?;
    Ok(u64::from_le_bytes(
        bytes
            .get(offset..end)
            .ok_or(WireError::Truncated)?
            .try_into()
            .expect("u64 width"),
    ))
}

fn read_u128(bytes: &[u8], offset: usize) -> Result<u128, WireError> {
    let end = offset.checked_add(16).ok_or(WireError::Truncated)?;
    Ok(u128::from_le_bytes(
        bytes
            .get(offset..end)
            .ok_or(WireError::Truncated)?
            .try_into()
            .expect("u128 width"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(payload: &[u8]) -> NativeFetchBatch {
        NativeFetchBatch {
            request_id: 1,
            batch_id: BatchId::new(2),
            event_id: Some(RecordId::new(9)),
            atomic_group: Some(AtomicGroupIdentity {
                transaction_id: 7,
                chunk_sequence: 1,
            }),
            first_offset: LogicalOffset::new(10),
            last_offset: LogicalOffset::new(11),
            record_count: NonZeroU32::new(2).expect("nonzero"),
            placement: Placement {
                virtual_lane_id: VirtualLaneId::new(3),
                ring_epoch: RingEpoch::new(4),
                leader_epoch: LeaderEpoch::new(6),
                sequence: PlacementSequence::new(5),
            },
            payload: Bytes::copy_from_slice(payload),
        }
    }

    #[test]
    fn round_trips_multiple_batches() {
        let expected = vec![batch(b"first"), batch(b"second")];
        let encoded = encode_fetch_batches(&expected, 4096).expect("encode");
        assert_eq!(
            decode_fetch_batches(&encoded, 4096).expect("decode"),
            expected
        );
    }

    #[test]
    fn owned_decode_slices_payload_without_copying() {
        let encoded =
            Bytes::from(encode_fetch_batches(&[batch(b"payload")], 4096).expect("encode"));
        let payload_pointer = encoded[HEADER_LEN..].as_ptr();
        let decoded = decode_fetch_batches_owned(encoded, 4096).expect("decode");
        assert_eq!(decoded[0].payload.as_ptr(), payload_pointer);
        assert_eq!(decoded[0].payload, Bytes::from_static(b"payload"));
    }

    #[test]
    fn segmented_encoding_retains_payload_storage_and_round_trips() {
        let expected = batch(b"payload");
        let payload_pointer = expected.payload.as_ptr();
        let response =
            encode_fetch_batches_segmented(std::slice::from_ref(&expected), 4096).expect("encode");
        assert_eq!(
            response.len(),
            HEADER_LEN + expected.payload.len() + CRC_LEN
        );
        let segments = response.into_segments();
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[1].as_ptr(), payload_pointer);
        let encoded = segments
            .into_iter()
            .fold(Vec::new(), |mut output, segment| {
                output.extend_from_slice(&segment);
                output
            });
        assert_eq!(
            decode_fetch_batches(&encoded, 4096).expect("decode"),
            vec![expected]
        );
    }

    #[test]
    fn rejects_checksum_corruption_and_output_overflow() {
        let mut encoded = encode_fetch_batches(&[batch(b"payload")], 4096).expect("encode");
        encoded[HEADER_LEN] ^= 1;
        assert_eq!(
            decode_fetch_batches(&encoded, 4096),
            Err(WireError::ChecksumMismatch)
        );
        assert_eq!(
            encode_fetch_batches(&[batch(b"payload")], 4),
            Err(WireError::FrameTooLarge)
        );
    }

    #[test]
    fn round_trips_fixed_width_append_response() {
        let expected = AppendResponse {
            request_id: u128::MAX,
            batch_id: BatchId::new(2),
            record_id: RecordId::new(3),
            first_offset: LogicalOffset::new(4),
            last_offset: LogicalOffset::new(5),
            visible_through: LogicalOffset::new(6),
            placement: Placement {
                virtual_lane_id: VirtualLaneId::new(7),
                ring_epoch: RingEpoch::new(8),
                leader_epoch: LeaderEpoch::new(9),
                sequence: PlacementSequence::new(10),
            },
        };
        let encoded = encode_append_response(expected);
        assert_eq!(encoded.len(), APPEND_RESPONSE_LEN);
        assert_eq!(decode_append_response(&encoded).expect("decode"), expected);
    }

    #[test]
    fn rejects_fixed_width_append_response_with_trailing_bytes() {
        let mut encoded = encode_append_response(AppendResponse {
            request_id: 1,
            batch_id: BatchId::new(2),
            record_id: RecordId::new(3),
            first_offset: LogicalOffset::new(4),
            last_offset: LogicalOffset::new(4),
            visible_through: LogicalOffset::new(5),
            placement: Placement {
                virtual_lane_id: VirtualLaneId::new(6),
                ring_epoch: RingEpoch::new(7),
                leader_epoch: LeaderEpoch::new(8),
                sequence: PlacementSequence::new(9),
            },
        });
        encoded.push(0);
        assert_eq!(
            decode_append_response(&encoded),
            Err(WireError::InvalidFrameLength)
        );
    }
}
