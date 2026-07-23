use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;

use bytes::Bytes;
use shard_stream_core::{BatchId, LogicalOffset, Placement, PlacementSequence, RingEpoch, ShardId};

const MAGIC: &[u8; 4] = b"SSB1";
const VERSION: u8 = 1;
const PREFIX_LEN: usize = 4 + 1 + 1 + 2 + 8;
const BODY_FIXED_LEN: usize = 16 + 16 + 16 + 16 + 4 + 4 + 8 + 16 + 8;
const HEADER_LEN: usize = PREFIX_LEN + BODY_FIXED_LEN;
const CRC_LEN: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeFetchBatch {
    pub request_id: u128,
    pub batch_id: BatchId,
    pub first_offset: LogicalOffset,
    pub last_offset: LogicalOffset,
    pub record_count: NonZeroU32,
    pub placement: Placement,
    pub payload: Bytes,
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

pub fn encode_fetch_batches(
    batches: &[NativeFetchBatch],
    max_output_bytes: usize,
) -> Result<Vec<u8>, WireError> {
    let mut output = Vec::new();
    for batch in batches {
        let frame_len = HEADER_LEN
            .checked_add(batch.payload.len())
            .and_then(|value| value.checked_add(CRC_LEN))
            .ok_or(WireError::FrameTooLarge)?;
        if output
            .len()
            .checked_add(frame_len)
            .is_none_or(|length| length > max_output_bytes)
        {
            return Err(WireError::FrameTooLarge);
        }
        let frame_len_u64 = u64::try_from(frame_len).map_err(|_| WireError::FrameTooLarge)?;
        let payload_len_u64 =
            u64::try_from(batch.payload.len()).map_err(|_| WireError::FrameTooLarge)?;
        let frame_start = output.len();
        output.extend_from_slice(MAGIC);
        output.push(VERSION);
        output.push(0);
        output.extend_from_slice(&(HEADER_LEN as u16).to_le_bytes());
        output.extend_from_slice(&frame_len_u64.to_le_bytes());
        output.extend_from_slice(&batch.request_id.to_le_bytes());
        output.extend_from_slice(&batch.batch_id.get().to_le_bytes());
        output.extend_from_slice(&batch.first_offset.get().to_le_bytes());
        output.extend_from_slice(&batch.last_offset.get().to_le_bytes());
        output.extend_from_slice(&batch.record_count.get().to_le_bytes());
        output.extend_from_slice(&batch.placement.shard_id.get().to_le_bytes());
        output.extend_from_slice(&batch.placement.ring_epoch.get().to_le_bytes());
        output.extend_from_slice(&batch.placement.sequence.get().to_le_bytes());
        output.extend_from_slice(&payload_len_u64.to_le_bytes());
        debug_assert_eq!(output.len() - frame_start, HEADER_LEN);
        output.extend_from_slice(&batch.payload);
        let checksum = crc32fast::hash(&output[frame_start..]);
        output.extend_from_slice(&checksum.to_le_bytes());
    }
    Ok(output)
}

pub fn decode_fetch_batches(
    bytes: &[u8],
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

        let body = &frame[PREFIX_LEN..frame.len() - CRC_LEN];
        if body.len() < BODY_FIXED_LEN {
            return Err(WireError::Truncated);
        }
        let request_id = read_u128(body, 0)?;
        let batch_id = BatchId::new(read_u128(body, 16)?);
        let first_offset = LogicalOffset::new(read_u128(body, 32)?);
        let last_offset = LogicalOffset::new(read_u128(body, 48)?);
        let record_count =
            NonZeroU32::new(read_u32(body, 64)?).ok_or(WireError::InvalidRecordRange)?;
        let shard_id = ShardId::new(read_u32(body, 68)?);
        let ring_epoch = RingEpoch::new(read_u64(body, 72)?);
        let sequence = PlacementSequence::new(read_u128(body, 80)?);
        let payload_len =
            usize::try_from(read_u64(body, 96)?).map_err(|_| WireError::FrameTooLarge)?;
        if first_offset
            .get()
            .checked_add(u128::from(record_count.get()) - 1)
            != Some(last_offset.get())
        {
            return Err(WireError::InvalidRecordRange);
        }
        let payload = Bytes::copy_from_slice(
            body.get(BODY_FIXED_LEN..)
                .filter(|payload| payload.len() == payload_len)
                .ok_or(WireError::InvalidFrameLength)?,
        );
        batches.push(NativeFetchBatch {
            request_id,
            batch_id,
            first_offset,
            last_offset,
            record_count,
            placement: Placement {
                shard_id,
                ring_epoch,
                sequence,
            },
            payload,
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
            first_offset: LogicalOffset::new(10),
            last_offset: LogicalOffset::new(11),
            record_count: NonZeroU32::new(2).expect("nonzero"),
            placement: Placement {
                shard_id: ShardId::new(3),
                ring_epoch: RingEpoch::new(4),
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
}
