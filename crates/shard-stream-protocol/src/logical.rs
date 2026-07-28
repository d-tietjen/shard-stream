use std::error::Error;
use std::fmt;

use shard_stream_core::{LogicalOffset, LogicalPartitionId, OriginId, TopicId, TopicPartition};

const CURSOR_MAGIC: &[u8; 8] = b"SSCURS01";
const CURSOR_VERSION: u16 = 1;
const CURSOR_CHECKSUM_BYTES: usize = size_of::<u32>();
const MAX_CURSOR_ORIGINS: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum TopicHaMode {
    ActivePassive = 1,
    ActiveActive = 2,
}

impl TopicHaMode {
    fn decode(value: u8) -> Result<Self, LogicalCursorError> {
        match value {
            1 => Ok(Self::ActivePassive),
            2 => Ok(Self::ActiveActive),
            _ => Err(LogicalCursorError::InvalidMode(value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum TopicCompatibility {
    NativeFlexible = 1,
    KafkaCompatible = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct HaProfileRef {
    pub profile_id: String,
    pub version: u64,
}

/// Verified routing snapshot for a stable logical topic identity.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LogicalTopicBinding {
    pub logical_topic_id: TopicId,
    pub physical_topic_id: TopicId,
    pub partitions: u32,
    pub policy_generation: u64,
    pub data_generation: u64,
    pub mode: TopicHaMode,
    pub profile: Option<HaProfileRef>,
    /// Verified finality hash supplied by a generation-aware server.
    /// Generation-one bindings may omit this value.
    pub finalized_epoch_hash: Option<[u8; 32]>,
}

impl LogicalTopicBinding {
    pub fn validate(&self) -> Result<(), String> {
        if self.partitions == 0
            || self.policy_generation == 0
            || self.data_generation == 0
            || self
                .finalized_epoch_hash
                .is_some_and(|hash| hash == [0; 32])
            || ((self.data_generation > 1 || self.mode == TopicHaMode::ActiveActive)
                && self.finalized_epoch_hash.is_none())
        {
            return Err("logical topic binding has invalid generation or finality metadata".into());
        }
        if let Some(profile) = &self.profile {
            profile.validate()?;
        }
        Ok(())
    }
}

impl HaProfileRef {
    pub fn validate(&self) -> Result<(), String> {
        if self.version == 0 {
            return Err("profile version must be nonzero".into());
        }
        if self.profile_id.is_empty()
            || self.profile_id.len() > 128
            || !self
                .profile_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(
                "profile ID must contain 1-128 ASCII letters, digits, '.', '_', or '-'".into(),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ActiveActiveCursorPosition {
    pub origin_id: OriginId,
    pub writer_epoch: u32,
    pub next_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LogicalCursorPosition {
    ActivePassive {
        next_offset: LogicalOffset,
    },
    ActiveActive {
        membership_epoch: u64,
        positions: Vec<ActiveActiveCursorPosition>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LogicalTopicCursor {
    pub topic_partition: TopicPartition,
    pub data_generation: u64,
    pub position: LogicalCursorPosition,
}

impl LogicalTopicCursor {
    #[must_use]
    pub const fn mode(&self) -> TopicHaMode {
        match self.position {
            LogicalCursorPosition::ActivePassive { .. } => TopicHaMode::ActivePassive,
            LogicalCursorPosition::ActiveActive { .. } => TopicHaMode::ActiveActive,
        }
    }

    pub fn validate(&self) -> Result<(), LogicalCursorError> {
        if self.data_generation == 0 {
            return Err(LogicalCursorError::InvalidGeneration);
        }
        if let LogicalCursorPosition::ActiveActive {
            membership_epoch,
            positions,
        } = &self.position
        {
            if *membership_epoch == 0 {
                return Err(LogicalCursorError::InvalidMembershipEpoch);
            }
            if positions.is_empty() {
                return Err(LogicalCursorError::EmptyOrigins);
            }
            if positions.len() > MAX_CURSOR_ORIGINS {
                return Err(LogicalCursorError::TooManyOrigins);
            }
            if positions
                .windows(2)
                .any(|pair| pair[0].origin_id >= pair[1].origin_id)
            {
                return Err(LogicalCursorError::OriginsNotCanonical);
            }
            if positions.iter().any(|position| position.writer_epoch == 0) {
                return Err(LogicalCursorError::InvalidWriterEpoch);
            }
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, LogicalCursorError> {
        self.validate()?;
        let origin_bytes = match &self.position {
            LogicalCursorPosition::ActivePassive { .. } => 0,
            LogicalCursorPosition::ActiveActive { positions, .. } => positions
                .len()
                .checked_mul(16)
                .ok_or(LogicalCursorError::LengthOverflow)?,
        };
        let mut bytes = Vec::with_capacity(
            8 + 2 + 1 + 1 + 16 + 4 + 8 + 12 + origin_bytes + CURSOR_CHECKSUM_BYTES,
        );
        bytes.extend_from_slice(CURSOR_MAGIC);
        bytes.extend_from_slice(&CURSOR_VERSION.to_le_bytes());
        bytes.push(self.mode() as u8);
        bytes.push(0);
        bytes.extend_from_slice(&self.topic_partition.topic_id.get().to_le_bytes());
        bytes.extend_from_slice(&self.topic_partition.partition_id.get().to_le_bytes());
        bytes.extend_from_slice(&self.data_generation.to_le_bytes());
        match &self.position {
            LogicalCursorPosition::ActivePassive { next_offset } => {
                bytes.extend_from_slice(&next_offset.get().to_le_bytes());
            }
            LogicalCursorPosition::ActiveActive {
                membership_epoch,
                positions,
            } => {
                bytes.extend_from_slice(&membership_epoch.to_le_bytes());
                let count = u32::try_from(positions.len())
                    .map_err(|_| LogicalCursorError::TooManyOrigins)?;
                bytes.extend_from_slice(&count.to_le_bytes());
                for position in positions {
                    bytes.extend_from_slice(&position.origin_id.get().to_le_bytes());
                    bytes.extend_from_slice(&position.writer_epoch.to_le_bytes());
                    bytes.extend_from_slice(&position.next_sequence.to_le_bytes());
                }
            }
        }
        let checksum = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        Ok(bytes)
    }

    pub fn encode_hex(&self) -> Result<String, LogicalCursorError> {
        let bytes = self.encode()?;
        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write as _;
            write!(encoded, "{byte:02x}").map_err(|_| LogicalCursorError::LengthOverflow)?;
        }
        Ok(encoded)
    }

    pub fn decode_hex(encoded: &str) -> Result<Self, LogicalCursorError> {
        if !encoded.len().is_multiple_of(2) {
            return Err(LogicalCursorError::InvalidHex);
        }
        let mut bytes = Vec::with_capacity(encoded.len() / 2);
        for pair in encoded.as_bytes().chunks_exact(2) {
            let high = decode_hex_nibble(pair[0]).ok_or(LogicalCursorError::InvalidHex)?;
            let low = decode_hex_nibble(pair[1]).ok_or(LogicalCursorError::InvalidHex)?;
            bytes.push((high << 4) | low);
        }
        Self::decode(&bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, LogicalCursorError> {
        let minimum = CURSOR_MAGIC.len()
            + size_of::<u16>()
            + 2
            + size_of::<u128>()
            + size_of::<u32>()
            + size_of::<u64>()
            + size_of::<u64>()
            + CURSOR_CHECKSUM_BYTES;
        if bytes.len() < minimum {
            return Err(LogicalCursorError::Truncated);
        }
        let body_len = bytes
            .len()
            .checked_sub(CURSOR_CHECKSUM_BYTES)
            .ok_or(LogicalCursorError::Truncated)?;
        let expected = u32::from_le_bytes(
            bytes[body_len..]
                .try_into()
                .map_err(|_| LogicalCursorError::Truncated)?,
        );
        if crc32fast::hash(&bytes[..body_len]) != expected {
            return Err(LogicalCursorError::ChecksumMismatch);
        }
        let mut decoder = Decoder::new(&bytes[..body_len]);
        if decoder.take(CURSOR_MAGIC.len())? != CURSOR_MAGIC {
            return Err(LogicalCursorError::InvalidMagic);
        }
        let version = decoder.u16()?;
        if version != CURSOR_VERSION {
            return Err(LogicalCursorError::UnsupportedVersion(version));
        }
        let mode = TopicHaMode::decode(decoder.u8()?)?;
        if decoder.u8()? != 0 {
            return Err(LogicalCursorError::ReservedBits);
        }
        let topic_partition = TopicPartition::new(
            TopicId::new(decoder.u128()?),
            LogicalPartitionId::new(decoder.u32()?),
        );
        let data_generation = decoder.u64()?;
        let position = match mode {
            TopicHaMode::ActivePassive => LogicalCursorPosition::ActivePassive {
                next_offset: LogicalOffset::new(decoder.u64()?),
            },
            TopicHaMode::ActiveActive => {
                let membership_epoch = decoder.u64()?;
                let count = usize::try_from(decoder.u32()?)
                    .map_err(|_| LogicalCursorError::LengthOverflow)?;
                if count > MAX_CURSOR_ORIGINS {
                    return Err(LogicalCursorError::TooManyOrigins);
                }
                let mut positions = Vec::with_capacity(count);
                for _ in 0..count {
                    positions.push(ActiveActiveCursorPosition {
                        origin_id: OriginId::new(decoder.u32()?),
                        writer_epoch: decoder.u32()?,
                        next_sequence: decoder.u64()?,
                    });
                }
                LogicalCursorPosition::ActiveActive {
                    membership_epoch,
                    positions,
                }
            }
        };
        if !decoder.is_empty() {
            return Err(LogicalCursorError::TrailingBytes);
        }
        let cursor = Self {
            topic_partition,
            data_generation,
            position,
        };
        cursor.validate()?;
        Ok(cursor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalCursorError {
    Truncated,
    InvalidMagic,
    UnsupportedVersion(u16),
    InvalidMode(u8),
    ReservedBits,
    ChecksumMismatch,
    InvalidGeneration,
    InvalidMembershipEpoch,
    EmptyOrigins,
    TooManyOrigins,
    OriginsNotCanonical,
    InvalidWriterEpoch,
    LengthOverflow,
    TrailingBytes,
    InvalidHex,
}

impl fmt::Display for LogicalCursorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("logical cursor is truncated"),
            Self::InvalidMagic => formatter.write_str("logical cursor magic is invalid"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "logical cursor version {version} is unsupported")
            }
            Self::InvalidMode(mode) => write!(formatter, "logical cursor mode {mode} is invalid"),
            Self::ReservedBits => formatter.write_str("logical cursor reserved bits are nonzero"),
            Self::ChecksumMismatch => formatter.write_str("logical cursor checksum is invalid"),
            Self::InvalidGeneration => {
                formatter.write_str("logical cursor data generation must be nonzero")
            }
            Self::InvalidMembershipEpoch => {
                formatter.write_str("active-active cursor membership epoch must be nonzero")
            }
            Self::EmptyOrigins => {
                formatter.write_str("active-active cursor must contain at least one origin")
            }
            Self::TooManyOrigins => formatter.write_str("logical cursor has too many origins"),
            Self::OriginsNotCanonical => {
                formatter.write_str("logical cursor origins are not sorted and unique")
            }
            Self::InvalidWriterEpoch => {
                formatter.write_str("active-active cursor writer epochs must be nonzero")
            }
            Self::LengthOverflow => formatter.write_str("logical cursor length overflowed"),
            Self::TrailingBytes => formatter.write_str("logical cursor has trailing bytes"),
            Self::InvalidHex => formatter.write_str("logical cursor hex encoding is invalid"),
        }
    }
}

impl Error for LogicalCursorError {}

const fn decode_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    const fn is_empty(&self) -> bool {
        self.cursor == self.bytes.len()
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], LogicalCursorError> {
        let end = self
            .cursor
            .checked_add(len)
            .ok_or(LogicalCursorError::LengthOverflow)?;
        let bytes = self
            .bytes
            .get(self.cursor..end)
            .ok_or(LogicalCursorError::Truncated)?;
        self.cursor = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, LogicalCursorError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, LogicalCursorError> {
        Ok(u16::from_le_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| LogicalCursorError::Truncated)?,
        ))
    }

    fn u32(&mut self) -> Result<u32, LogicalCursorError> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| LogicalCursorError::Truncated)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, LogicalCursorError> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| LogicalCursorError::Truncated)?,
        ))
    }

    fn u128(&mut self) -> Result<u128, LogicalCursorError> {
        Ok(u128::from_le_bytes(
            self.take(16)?
                .try_into()
                .map_err(|_| LogicalCursorError::Truncated)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_passive_cursor_round_trips() {
        let cursor = LogicalTopicCursor {
            topic_partition: TopicPartition::new(TopicId::new(42), LogicalPartitionId::new(3)),
            data_generation: 7,
            position: LogicalCursorPosition::ActivePassive {
                next_offset: LogicalOffset::new(99),
            },
        };
        assert_eq!(
            LogicalTopicCursor::decode(&cursor.encode().expect("encode")).expect("decode"),
            cursor
        );
        assert_eq!(
            LogicalTopicCursor::decode_hex(&cursor.encode_hex().expect("hex")).expect("hex decode"),
            cursor
        );
    }

    #[test]
    fn active_active_cursor_round_trips_and_requires_canonical_origins() {
        let cursor = LogicalTopicCursor {
            topic_partition: TopicPartition::new(TopicId::new(42), LogicalPartitionId::new(3)),
            data_generation: 8,
            position: LogicalCursorPosition::ActiveActive {
                membership_epoch: 11,
                positions: vec![
                    ActiveActiveCursorPosition {
                        origin_id: OriginId::new(1),
                        writer_epoch: 2,
                        next_sequence: 10,
                    },
                    ActiveActiveCursorPosition {
                        origin_id: OriginId::new(4),
                        writer_epoch: 3,
                        next_sequence: 20,
                    },
                ],
            },
        };
        assert_eq!(
            LogicalTopicCursor::decode(&cursor.encode().expect("encode")).expect("decode"),
            cursor
        );

        let mut invalid = cursor;
        let LogicalCursorPosition::ActiveActive { positions, .. } = &mut invalid.position else {
            unreachable!();
        };
        positions.swap(0, 1);
        assert_eq!(
            invalid.encode(),
            Err(LogicalCursorError::OriginsNotCanonical)
        );
    }

    #[test]
    fn cursor_checksum_rejects_mutation() {
        let cursor = LogicalTopicCursor {
            topic_partition: TopicPartition::new(TopicId::new(42), LogicalPartitionId::new(3)),
            data_generation: 7,
            position: LogicalCursorPosition::ActivePassive {
                next_offset: LogicalOffset::new(99),
            },
        };
        let mut bytes = cursor.encode().expect("encode");
        bytes[20] ^= 1;
        assert_eq!(
            LogicalTopicCursor::decode(&bytes),
            Err(LogicalCursorError::ChecksumMismatch)
        );
    }
}
