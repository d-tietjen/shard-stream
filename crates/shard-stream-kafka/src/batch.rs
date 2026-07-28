use anyhow::Result as AnyResult;
use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::compression::{Decompressor, Gzip, Lz4, Snappy, Zstd};
use kafka_protocol::records::{Compression, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE};
use shard_stream_protocol::ProducerIdentity;

const OUTER_HEADER_BYTES: usize = 12;
const BATCH_BODY_HEADER_BYTES: usize = 49;
const RECORD_BATCH_HEADER_BYTES: usize = OUTER_HEADER_BYTES + BATCH_BODY_HEADER_BYTES;
const MAGIC_V2: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InspectedRecordBatches {
    pub record_count: u32,
    pub producer: Option<ProducerIdentity>,
}

#[derive(Debug, Clone, Copy)]
struct BatchHeader {
    compression: Compression,
    last_offset_delta: i32,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    record_count: u32,
}

#[derive(Debug, Clone, Copy)]
enum ProducerMode {
    Unknown,
    NonIdempotent,
    Idempotent {
        producer_id: i64,
        producer_epoch: i16,
        first_sequence: i32,
        next_sequence: i64,
    },
}

pub(crate) fn inspect_record_batches(
    records: &Bytes,
) -> Result<InspectedRecordBatches, ResponseError> {
    let mut cursor = 0usize;
    let mut record_count = 0u32;
    let mut producer = ProducerMode::Unknown;

    while cursor < records.len() {
        let remaining = records
            .len()
            .checked_sub(cursor)
            .ok_or(ResponseError::CorruptMessage)?;
        if remaining < RECORD_BATCH_HEADER_BYTES {
            return Err(ResponseError::CorruptMessage);
        }
        let outer = &records[cursor..];
        let batch_length = read_i32(&outer[8..12]);
        let batch_length =
            usize::try_from(batch_length).map_err(|_| ResponseError::CorruptMessage)?;
        if batch_length < BATCH_BODY_HEADER_BYTES {
            return Err(ResponseError::CorruptMessage);
        }
        let total_length = OUTER_HEADER_BYTES
            .checked_add(batch_length)
            .ok_or(ResponseError::CorruptMessage)?;
        if total_length > remaining {
            return Err(ResponseError::CorruptMessage);
        }

        let batch_end = cursor
            .checked_add(total_length)
            .ok_or(ResponseError::CorruptMessage)?;
        let body = &records[cursor + OUTER_HEADER_BYTES..batch_end];
        let header = inspect_header(body)?;
        let record_data = records.slice(cursor + RECORD_BATCH_HEADER_BYTES..batch_end);
        validate_record_data(header, record_data)?;
        update_producer(&mut producer, header)?;
        record_count = record_count
            .checked_add(header.record_count)
            .ok_or(ResponseError::MessageTooLarge)?;
        cursor = batch_end;
    }

    if record_count == 0 {
        return Err(ResponseError::UnsupportedForMessageFormat);
    }
    let producer = match producer {
        ProducerMode::Idempotent {
            producer_id,
            producer_epoch,
            first_sequence,
            ..
        } => Some(ProducerIdentity {
            producer_id: producer_id as u128,
            epoch: producer_epoch as u32,
            first_sequence: first_sequence as u64,
            event_id: None,
        }),
        ProducerMode::NonIdempotent => None,
        ProducerMode::Unknown => return Err(ResponseError::InvalidRecord),
    };
    Ok(InspectedRecordBatches {
        record_count,
        producer,
    })
}

fn inspect_header(body: &[u8]) -> Result<BatchHeader, ResponseError> {
    if body.len() < BATCH_BODY_HEADER_BYTES || body[4] != MAGIC_V2 {
        return Err(ResponseError::CorruptMessage);
    }
    let supplied_crc = read_u32(&body[5..9]);
    if crc32c::crc32c(&body[9..]) != supplied_crc {
        return Err(ResponseError::CorruptMessage);
    }

    let attributes = read_i16(&body[9..11]);
    if attributes & (1 << 4) != 0 || attributes & (1 << 5) != 0 {
        return Err(ResponseError::UnsupportedForMessageFormat);
    }
    let compression = match attributes & 0x7 {
        0 => Compression::None,
        1 => Compression::Gzip,
        2 => Compression::Snappy,
        3 => Compression::Lz4,
        4 => Compression::Zstd,
        _ => return Err(ResponseError::CorruptMessage),
    };
    let last_offset_delta = read_i32(&body[11..15]);
    let producer_id = read_i64(&body[31..39]);
    let producer_epoch = read_i16(&body[39..41]);
    let base_sequence = read_i32(&body[41..45]);
    let record_count =
        u32::try_from(read_i32(&body[45..49])).map_err(|_| ResponseError::CorruptMessage)?;
    if record_count == 0
        || last_offset_delta < 0
        || u32::try_from(last_offset_delta).ok() != record_count.checked_sub(1)
    {
        return Err(ResponseError::CorruptMessage);
    }

    Ok(BatchHeader {
        compression,
        last_offset_delta,
        producer_id,
        producer_epoch,
        base_sequence,
        record_count,
    })
}

fn validate_record_data(header: BatchHeader, mut data: Bytes) -> Result<(), ResponseError> {
    if header.compression == Compression::None {
        return validate_record_frames(&data, header);
    }
    let result = match header.compression {
        Compression::None => unreachable!("uncompressed batches return above"),
        Compression::Gzip => {
            Gzip::decompress(&mut data, |decoded| validate_decompressed(decoded, header))
        }
        Compression::Snappy => {
            Snappy::decompress(&mut data, |decoded| validate_decompressed(decoded, header))
        }
        Compression::Lz4 => {
            Lz4::decompress(&mut data, |decoded| validate_decompressed(decoded, header))
        }
        Compression::Zstd => {
            Zstd::decompress(&mut data, |decoded| validate_decompressed(decoded, header))
        }
    };
    result.map_err(|_| ResponseError::CorruptMessage)
}

fn validate_decompressed(data: &Bytes, header: BatchHeader) -> AnyResult<()> {
    validate_record_frames(data, header)
        .map_err(|error| anyhow::anyhow!("invalid Kafka record framing: {error:?}"))
}

fn validate_record_frames(mut data: &[u8], header: BatchHeader) -> Result<(), ResponseError> {
    for expected_delta in 0..header.record_count {
        let size = read_varint(&mut data)?;
        let size = usize::try_from(size).map_err(|_| ResponseError::CorruptMessage)?;
        if data.len() < size {
            return Err(ResponseError::CorruptMessage);
        }
        let (record, remaining) = data.split_at(size);
        inspect_record(record, expected_delta)?;
        data = remaining;
    }
    if !data.is_empty()
        || u32::try_from(header.last_offset_delta).ok() != header.record_count.checked_sub(1)
    {
        return Err(ResponseError::CorruptMessage);
    }
    Ok(())
}

fn inspect_record(mut record: &[u8], expected_delta: u32) -> Result<(), ResponseError> {
    take(&mut record, 1)?;
    read_varlong(&mut record)?;
    let offset_delta = read_varint(&mut record)?;
    if u32::try_from(offset_delta).ok() != Some(expected_delta) {
        return Err(ResponseError::CorruptMessage);
    }

    skip_nullable_bytes(&mut record)?;
    skip_nullable_bytes(&mut record)?;
    let header_count = read_varint(&mut record)?;
    let header_count = u32::try_from(header_count).map_err(|_| ResponseError::CorruptMessage)?;
    for _ in 0..header_count {
        let key_length = read_varint(&mut record)?;
        let key_length = usize::try_from(key_length).map_err(|_| ResponseError::CorruptMessage)?;
        take(&mut record, key_length)?;
        skip_nullable_bytes(&mut record)?;
    }
    if !record.is_empty() {
        return Err(ResponseError::CorruptMessage);
    }
    Ok(())
}

fn skip_nullable_bytes(input: &mut &[u8]) -> Result<(), ResponseError> {
    let length = read_varint(input)?;
    if length < -1 {
        return Err(ResponseError::CorruptMessage);
    }
    if length >= 0 {
        let length = usize::try_from(length).map_err(|_| ResponseError::CorruptMessage)?;
        take(input, length)?;
    }
    Ok(())
}

fn read_varint(input: &mut &[u8]) -> Result<i32, ResponseError> {
    let mut value = 0u32;
    for shift in [0, 7, 14, 21, 28] {
        let byte = *take(input, 1)?
            .first()
            .ok_or(ResponseError::CorruptMessage)?;
        if shift == 28 && byte & 0xf0 != 0 {
            return Err(ResponseError::CorruptMessage);
        }
        value |= u32::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(((value >> 1) as i32) ^ (-((value & 1) as i32)));
        }
    }
    Err(ResponseError::CorruptMessage)
}

fn read_varlong(input: &mut &[u8]) -> Result<i64, ResponseError> {
    let mut value = 0u64;
    for shift in [0, 7, 14, 21, 28, 35, 42, 49, 56, 63] {
        let byte = *take(input, 1)?
            .first()
            .ok_or(ResponseError::CorruptMessage)?;
        if shift == 63 && byte & 0xfe != 0 {
            return Err(ResponseError::CorruptMessage);
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(((value >> 1) as i64) ^ (-((value & 1) as i64)));
        }
    }
    Err(ResponseError::CorruptMessage)
}

fn take<'a>(input: &mut &'a [u8], length: usize) -> Result<&'a [u8], ResponseError> {
    if input.len() < length {
        return Err(ResponseError::CorruptMessage);
    }
    let (taken, remaining) = input.split_at(length);
    *input = remaining;
    Ok(taken)
}

fn update_producer(aggregate: &mut ProducerMode, header: BatchHeader) -> Result<(), ResponseError> {
    let batch_mode = if header.producer_id == NO_PRODUCER_ID {
        if header.producer_epoch != NO_PRODUCER_EPOCH || header.base_sequence != NO_SEQUENCE {
            return Err(ResponseError::InvalidProducerEpoch);
        }
        ProducerMode::NonIdempotent
    } else {
        if header.producer_id < 0 || header.producer_epoch < 0 || header.base_sequence < 0 {
            return Err(ResponseError::InvalidProducerEpoch);
        }
        let last_sequence = i64::from(header.base_sequence)
            .checked_add(i64::from(header.record_count) - 1)
            .ok_or(ResponseError::InvalidProducerEpoch)?;
        if last_sequence > i64::from(i32::MAX) {
            return Err(ResponseError::InvalidProducerEpoch);
        }
        ProducerMode::Idempotent {
            producer_id: header.producer_id,
            producer_epoch: header.producer_epoch,
            first_sequence: header.base_sequence,
            next_sequence: last_sequence + 1,
        }
    };

    match (*aggregate, batch_mode) {
        (ProducerMode::Unknown, mode) => *aggregate = mode,
        (ProducerMode::NonIdempotent, ProducerMode::NonIdempotent) => {}
        (
            ProducerMode::Idempotent {
                producer_id,
                producer_epoch,
                first_sequence,
                next_sequence,
            },
            ProducerMode::Idempotent {
                producer_id: next_producer_id,
                producer_epoch: next_producer_epoch,
                first_sequence: next_first_sequence,
                next_sequence: next_next_sequence,
            },
        ) if producer_id == next_producer_id
            && producer_epoch == next_producer_epoch
            && next_sequence == i64::from(next_first_sequence) =>
        {
            *aggregate = ProducerMode::Idempotent {
                producer_id,
                producer_epoch,
                first_sequence,
                next_sequence: next_next_sequence,
            };
        }
        _ => return Err(ResponseError::InvalidProducerEpoch),
    }
    Ok(())
}

fn read_i16(bytes: &[u8]) -> i16 {
    i16::from_be_bytes(bytes[..2].try_into().expect("validated Kafka header width"))
}

fn read_i32(bytes: &[u8]) -> i32 {
    i32::from_be_bytes(bytes[..4].try_into().expect("validated Kafka header width"))
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes[..4].try_into().expect("validated Kafka header width"))
}

fn read_i64(bytes: &[u8]) -> i64 {
    i64::from_be_bytes(bytes[..8].try_into().expect("validated Kafka header width"))
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::records::{
        Compression, NO_PARTITION_LEADER_EPOCH, NO_TIMESTAMP, Record, RecordBatchEncoder,
        RecordEncodeOptions, TimestampType,
    };

    use super::*;

    #[test]
    fn standard_non_idempotent_batch_is_scanned_without_record_materialization() {
        let encoded = encode_records(
            &[
                test_record(0, NO_PRODUCER_ID, NO_PRODUCER_EPOCH, NO_SEQUENCE),
                test_record(1, NO_PRODUCER_ID, NO_PRODUCER_EPOCH, 0),
            ],
            Compression::None,
        );
        assert_eq!(encoded.len(), batch_total_length(&encoded));
        assert_eq!(
            inspect_record_batches(&encoded),
            Ok(InspectedRecordBatches {
                record_count: 2,
                producer: None,
            })
        );
    }

    #[test]
    fn idempotent_batches_require_contiguous_header_sequences() {
        let first = encode_records(
            &[test_record(0, 7, 2, 100), test_record(1, 7, 2, 101)],
            Compression::None,
        );
        let second = encode_records(
            &[test_record(0, 7, 2, 102), test_record(1, 7, 2, 103)],
            Compression::None,
        );
        let mut joined = BytesMut::with_capacity(first.len() + second.len());
        joined.extend_from_slice(&first);
        joined.extend_from_slice(&second);
        assert_eq!(
            inspect_record_batches(&joined.freeze()),
            Ok(InspectedRecordBatches {
                record_count: 4,
                producer: Some(ProducerIdentity {
                    producer_id: 7,
                    epoch: 2,
                    first_sequence: 100,
                    event_id: None,
                }),
            })
        );

        let invalid = encode_records(
            &[test_record(0, 7, 2, 104), test_record(1, 7, 2, 105)],
            Compression::None,
        );
        let mut joined = BytesMut::with_capacity(first.len() + invalid.len());
        joined.extend_from_slice(&first);
        joined.extend_from_slice(&invalid);
        assert_eq!(
            inspect_record_batches(&joined.freeze()),
            Err(ResponseError::InvalidProducerEpoch)
        );
    }

    #[test]
    fn compressed_batch_uses_the_same_metadata_scanner() {
        let encoded = encode_records(
            &[
                test_record(0, NO_PRODUCER_ID, NO_PRODUCER_EPOCH, NO_SEQUENCE),
                test_record(1, NO_PRODUCER_ID, NO_PRODUCER_EPOCH, 0),
            ],
            Compression::Zstd,
        );
        assert_eq!(
            inspect_record_batches(&encoded),
            Ok(InspectedRecordBatches {
                record_count: 2,
                producer: None,
            })
        );
    }

    #[test]
    fn timestamp_delta_parser_uses_kafka_varlong_framing() {
        let expected = i64::from(i32::MAX) + 10;
        let mut value = (expected as u64) << 1;
        let mut encoded = Vec::new();
        while value >= 0x80 {
            encoded.push((value as u8) | 0x80);
            value >>= 7;
        }
        encoded.push(value as u8);

        let mut input = encoded.as_slice();
        assert_eq!(read_varlong(&mut input), Ok(expected));
        assert!(input.is_empty());
    }

    #[test]
    fn crc_and_record_count_corruption_are_rejected() {
        let encoded = encode_records(
            &[
                test_record(0, NO_PRODUCER_ID, NO_PRODUCER_EPOCH, NO_SEQUENCE),
                test_record(1, NO_PRODUCER_ID, NO_PRODUCER_EPOCH, 0),
            ],
            Compression::None,
        );
        let mut crc_corrupt = encoded.to_vec();
        *crc_corrupt.last_mut().expect("encoded byte") ^= 1;
        assert_eq!(
            inspect_record_batches(&Bytes::from(crc_corrupt)),
            Err(ResponseError::CorruptMessage)
        );

        let mut count_corrupt = encoded.to_vec();
        count_corrupt[57..61].copy_from_slice(&3i32.to_be_bytes());
        let crc = crc32c::crc32c(&count_corrupt[21..]);
        count_corrupt[17..21].copy_from_slice(&crc.to_be_bytes());
        assert_eq!(
            inspect_record_batches(&Bytes::from(count_corrupt)),
            Err(ResponseError::CorruptMessage)
        );
    }

    #[test]
    fn transactional_batches_remain_unsupported() {
        let mut record = test_record(0, 7, 2, 0);
        record.transactional = true;
        let encoded = encode_records(&[record], Compression::None);
        assert_eq!(
            inspect_record_batches(&encoded),
            Err(ResponseError::UnsupportedForMessageFormat)
        );
    }

    fn encode_records(records: &[Record], compression: Compression) -> Bytes {
        let mut encoded = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut encoded,
            records,
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )
        .expect("encode records");
        encoded.freeze()
    }

    fn test_record(offset: i64, producer_id: i64, producer_epoch: i16, sequence: i32) -> Record {
        Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id,
            producer_epoch,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence,
            timestamp: NO_TIMESTAMP,
            key: None,
            value: Some(Bytes::from_static(b"value")),
            headers: Default::default(),
        }
    }

    fn batch_total_length(encoded: &[u8]) -> usize {
        OUTER_HEADER_BYTES + usize::try_from(read_i32(&encoded[8..12])).expect("batch length")
    }
}
