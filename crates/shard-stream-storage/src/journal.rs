use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use shard_stream_core::{
    BatchId, LogicalOffset, LogicalPartitionId, Placement, PlacementSequence, Reservation,
    RingEpoch, ShardId, TopicId, TopicPartition,
};

use crate::codec::{Decoder, push_u8, push_u32, push_u64, push_u128};
use crate::{StorageError, StorageResult};

const JOURNAL_MAGIC: &[u8; 4] = b"SSJ1";
const JOURNAL_VERSION: u8 = 1;
const PREFIX_LEN: usize = 4 + 1 + 1 + 2 + 4;
const CRC_LEN: usize = 4;
const MAX_EVENT_BYTES: usize = 1024 * 1024;

const CONFIGURE_KIND: u8 = 1;
const RESERVE_KIND: u8 = 2;
const COMMIT_KIND: u8 = 3;
const ABORT_KIND: u8 = 4;
const COMMIT_V2_KIND: u8 = 5;
const TRUNCATE_KIND: u8 = 6;

pub const DIGEST_BLAKE3: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerSequence {
    pub producer_id: u128,
    pub epoch: u32,
    pub first_sequence: u64,
    pub digest_algorithm: u8,
    pub payload_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalEvent {
    Configure {
        topic_partition: TopicPartition,
        ring_epoch: RingEpoch,
        shards: Vec<ShardId>,
    },
    Reserve {
        reservation: Reservation,
        producer: Option<ProducerSequence>,
    },
    Commit {
        topic_partition: TopicPartition,
        batch_id: BatchId,
        durable_replicas: u32,
        object_durable: bool,
    },
    Abort {
        topic_partition: TopicPartition,
        batch_id: BatchId,
    },
    Truncate {
        topic_partition: TopicPartition,
        log_start: LogicalOffset,
    },
}

#[derive(Debug, Clone, Default)]
pub struct RecoveredJournal {
    pub events: Vec<JournalEvent>,
    pub repaired_partial_tail: bool,
}

#[derive(Debug)]
pub struct CoordinatorJournal {
    path: PathBuf,
    file: File,
}

impl CoordinatorJournal {
    pub fn open(path: impl AsRef<Path>) -> StorageResult<(Self, RecoveredJournal)> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        let recovered = scan_and_repair(&path, &mut file)?;
        file.seek(SeekFrom::End(0))?;
        Ok((Self { path, file }, recovered))
    }

    pub fn append(&mut self, event: &JournalEvent, sync: bool) -> StorageResult<()> {
        let encoded = encode_event(event)?;
        let start = self.file.seek(SeekFrom::End(0))?;
        if let Err(error) = self.file.write_all(&encoded) {
            let _ = self.file.set_len(start);
            return Err(error.into());
        }
        self.file.flush()?;
        if sync {
            self.file.sync_data()?;
        }
        Ok(())
    }

    pub fn sync(&mut self) -> StorageResult<()> {
        self.file.flush()?;
        self.file.sync_data()?;
        Ok(())
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn scan_and_repair(path: &Path, file: &mut File) -> StorageResult<RecoveredJournal> {
    let file_len = file.metadata()?.len();
    let mut offset = 0u64;
    let mut events = Vec::new();
    let mut repaired_partial_tail = false;
    while offset < file_len {
        if file_len - offset < PREFIX_LEN as u64 {
            file.set_len(offset)?;
            repaired_partial_tail = true;
            break;
        }
        file.seek(SeekFrom::Start(offset))?;
        let mut prefix = [0u8; PREFIX_LEN];
        file.read_exact(&mut prefix)?;
        validate_prefix(path, offset, &prefix)?;
        let body_len = u32::from_le_bytes(prefix[8..12].try_into().expect("body length")) as usize;
        if body_len > MAX_EVENT_BYTES {
            return Err(StorageError::corrupt(
                path,
                offset,
                "journal event exceeds maximum size",
            ));
        }
        let frame_len = PREFIX_LEN
            .checked_add(body_len)
            .and_then(|value| value.checked_add(CRC_LEN))
            .ok_or_else(|| StorageError::corrupt(path, offset, "journal length overflow"))?;
        if offset
            .checked_add(frame_len as u64)
            .is_none_or(|end| end > file_len)
        {
            file.set_len(offset)?;
            repaired_partial_tail = true;
            break;
        }
        let mut frame = vec![0u8; frame_len];
        frame[..PREFIX_LEN].copy_from_slice(&prefix);
        file.read_exact(&mut frame[PREFIX_LEN..])?;
        let expected_crc =
            u32::from_le_bytes(frame[frame_len - CRC_LEN..].try_into().expect("CRC width"));
        if crc32fast::hash(&frame[..frame_len - CRC_LEN]) != expected_crc {
            return Err(StorageError::corrupt(
                path,
                offset,
                "journal checksum mismatch",
            ));
        }
        let kind = prefix[5];
        events.push(decode_event(
            path,
            offset,
            kind,
            &frame[PREFIX_LEN..frame_len - CRC_LEN],
        )?);
        offset += frame_len as u64;
    }
    Ok(RecoveredJournal {
        events,
        repaired_partial_tail,
    })
}

fn encode_event(event: &JournalEvent) -> StorageResult<Vec<u8>> {
    let (kind, body) = match event {
        JournalEvent::Configure {
            topic_partition,
            ring_epoch,
            shards,
        } => {
            if shards.is_empty() {
                return Err(StorageError::InvalidInput(
                    "configured shard ring cannot be empty".into(),
                ));
            }
            let shard_count = u32::try_from(shards.len())
                .map_err(|_| StorageError::LimitExceeded("too many shards".into()))?;
            let mut body = Vec::with_capacity(16 + 4 + 8 + 4 + shards.len() * 4);
            push_topic_partition(&mut body, *topic_partition);
            push_u64(&mut body, ring_epoch.get());
            push_u32(&mut body, shard_count);
            for shard in shards {
                push_u32(&mut body, shard.get());
            }
            (CONFIGURE_KIND, body)
        }
        JournalEvent::Reserve {
            reservation,
            producer,
        } => {
            let mut body = Vec::with_capacity(128);
            push_reservation(&mut body, *reservation);
            match producer {
                Some(producer) => {
                    push_u8(&mut body, 1);
                    push_u128(&mut body, producer.producer_id);
                    push_u32(&mut body, producer.epoch);
                    push_u64(&mut body, producer.first_sequence);
                    if producer.digest_algorithm != DIGEST_BLAKE3 {
                        return Err(StorageError::InvalidInput(
                            "unsupported producer payload digest algorithm".into(),
                        ));
                    }
                    push_u8(&mut body, producer.digest_algorithm);
                    body.extend_from_slice(&producer.payload_digest);
                }
                None => push_u8(&mut body, 0),
            }
            (RESERVE_KIND, body)
        }
        JournalEvent::Commit {
            topic_partition,
            batch_id,
            durable_replicas,
            object_durable,
        } => {
            if *durable_replicas == 0 {
                return Err(StorageError::InvalidInput(
                    "committed batch must have a durable replica".into(),
                ));
            }
            let mut body = Vec::with_capacity(40);
            push_topic_partition(&mut body, *topic_partition);
            push_u128(&mut body, batch_id.get());
            push_u32(&mut body, *durable_replicas);
            push_u8(&mut body, u8::from(*object_durable));
            (COMMIT_V2_KIND, body)
        }
        JournalEvent::Abort {
            topic_partition,
            batch_id,
        } => {
            let mut body = Vec::with_capacity(36);
            push_topic_partition(&mut body, *topic_partition);
            push_u128(&mut body, batch_id.get());
            (ABORT_KIND, body)
        }
        JournalEvent::Truncate {
            topic_partition,
            log_start,
        } => {
            let mut body = Vec::with_capacity(36);
            push_topic_partition(&mut body, *topic_partition);
            push_u128(&mut body, log_start.get());
            (TRUNCATE_KIND, body)
        }
    };
    if body.len() > MAX_EVENT_BYTES {
        return Err(StorageError::LimitExceeded(
            "journal event exceeds maximum size".into(),
        ));
    }
    let body_len = u32::try_from(body.len())
        .map_err(|_| StorageError::LimitExceeded("journal body exceeds u32".into()))?;
    let mut frame = Vec::with_capacity(PREFIX_LEN + body.len() + CRC_LEN);
    frame.extend_from_slice(JOURNAL_MAGIC);
    push_u8(&mut frame, JOURNAL_VERSION);
    push_u8(&mut frame, kind);
    frame.extend_from_slice(&0u16.to_le_bytes());
    push_u32(&mut frame, body_len);
    frame.extend_from_slice(&body);
    let checksum = crc32fast::hash(&frame);
    push_u32(&mut frame, checksum);
    Ok(frame)
}

fn decode_event(path: &Path, offset: u64, kind: u8, body: &[u8]) -> StorageResult<JournalEvent> {
    let mut decoder = Decoder::new(body);
    let event = match kind {
        CONFIGURE_KIND => {
            let topic_partition = decode_topic_partition(&mut decoder)?;
            let ring_epoch = RingEpoch::new(decoder.u64()?);
            let shard_count = usize::try_from(decoder.u32()?)
                .map_err(|_| StorageError::corrupt(path, offset, "shard count overflow"))?;
            if shard_count == 0 {
                return Err(StorageError::corrupt(
                    path,
                    offset,
                    "empty configured shard ring",
                ));
            }
            let mut shards = Vec::with_capacity(shard_count);
            let mut unique = HashSet::with_capacity(shard_count);
            for _ in 0..shard_count {
                let shard = ShardId::new(decoder.u32()?);
                if !unique.insert(shard) {
                    return Err(StorageError::corrupt(
                        path,
                        offset,
                        "duplicate configured shard",
                    ));
                }
                shards.push(shard);
            }
            JournalEvent::Configure {
                topic_partition,
                ring_epoch,
                shards,
            }
        }
        RESERVE_KIND => {
            let reservation = decode_reservation(&mut decoder)?;
            let producer = match decoder.u8()? {
                0 => None,
                1 => {
                    let producer_id = decoder.u128()?;
                    let epoch = decoder.u32()?;
                    let first_sequence = decoder.u64()?;
                    let digest_algorithm = decoder.u8()?;
                    if digest_algorithm != DIGEST_BLAKE3 {
                        return Err(StorageError::corrupt(
                            path,
                            offset,
                            "unsupported producer payload digest algorithm",
                        ));
                    }
                    Some(ProducerSequence {
                        producer_id,
                        epoch,
                        first_sequence,
                        digest_algorithm,
                        payload_digest: decoder.take(32)?.try_into().map_err(|_| {
                            StorageError::corrupt(path, offset, "invalid payload digest")
                        })?,
                    })
                }
                _ => {
                    return Err(StorageError::corrupt(
                        path,
                        offset,
                        "invalid producer presence flag",
                    ));
                }
            };
            JournalEvent::Reserve {
                reservation,
                producer,
            }
        }
        COMMIT_KIND | COMMIT_V2_KIND | ABORT_KIND => {
            let topic_partition = decode_topic_partition(&mut decoder)?;
            let batch_id = BatchId::new(decoder.u128()?);
            if kind == COMMIT_KIND || kind == COMMIT_V2_KIND {
                let durable_replicas = decoder.u32()?;
                if durable_replicas == 0 {
                    return Err(StorageError::corrupt(
                        path,
                        offset,
                        "committed batch has zero durable replicas",
                    ));
                }
                let object_durable = if kind == COMMIT_V2_KIND {
                    match decoder.u8()? {
                        0 => false,
                        1 => true,
                        _ => {
                            return Err(StorageError::corrupt(
                                path,
                                offset,
                                "invalid object durability flag",
                            ));
                        }
                    }
                } else {
                    false
                };
                JournalEvent::Commit {
                    topic_partition,
                    batch_id,
                    durable_replicas,
                    object_durable,
                }
            } else {
                JournalEvent::Abort {
                    topic_partition,
                    batch_id,
                }
            }
        }
        TRUNCATE_KIND => JournalEvent::Truncate {
            topic_partition: decode_topic_partition(&mut decoder)?,
            log_start: LogicalOffset::new(decoder.u128()?),
        },
        _ => {
            return Err(StorageError::corrupt(
                path,
                offset,
                format!("unknown journal event kind {kind}"),
            ));
        }
    };
    if !decoder.is_empty() {
        return Err(StorageError::corrupt(
            path,
            offset,
            "journal event has trailing bytes",
        ));
    }
    Ok(event)
}

fn push_topic_partition(bytes: &mut Vec<u8>, topic_partition: TopicPartition) {
    push_u128(bytes, topic_partition.topic_id.get());
    push_u32(bytes, topic_partition.partition_id.get());
}

fn decode_topic_partition(decoder: &mut Decoder<'_>) -> StorageResult<TopicPartition> {
    Ok(TopicPartition::new(
        TopicId::new(decoder.u128()?),
        LogicalPartitionId::new(decoder.u32()?),
    ))
}

fn push_reservation(bytes: &mut Vec<u8>, reservation: Reservation) {
    push_topic_partition(
        bytes,
        TopicPartition::new(reservation.topic_id, reservation.partition_id),
    );
    push_u128(bytes, reservation.batch_id.get());
    push_u128(bytes, reservation.first_offset.get());
    push_u128(bytes, reservation.last_offset.get());
    push_u32(bytes, reservation.record_count.get());
    push_u32(bytes, reservation.placement.shard_id.get());
    push_u64(bytes, reservation.placement.ring_epoch.get());
    push_u128(bytes, reservation.placement.sequence.get());
}

fn decode_reservation(decoder: &mut Decoder<'_>) -> StorageResult<Reservation> {
    let topic_partition = decode_topic_partition(decoder)?;
    let batch_id = BatchId::new(decoder.u128()?);
    let first_offset = LogicalOffset::new(decoder.u128()?);
    let last_offset = LogicalOffset::new(decoder.u128()?);
    let record_count = NonZeroU32::new(decoder.u32()?)
        .ok_or_else(|| StorageError::InvalidInput("zero journal record count".into()))?;
    let shard_id = ShardId::new(decoder.u32()?);
    let ring_epoch = RingEpoch::new(decoder.u64()?);
    let sequence = PlacementSequence::new(decoder.u128()?);
    if first_offset
        .get()
        .checked_add(u128::from(record_count.get()) - 1)
        != Some(last_offset.get())
    {
        return Err(StorageError::InvalidInput(
            "journal reservation range does not match count".into(),
        ));
    }
    Ok(Reservation {
        topic_id: topic_partition.topic_id,
        partition_id: topic_partition.partition_id,
        batch_id,
        first_offset,
        last_offset,
        record_count,
        placement: Placement {
            shard_id,
            ring_epoch,
            sequence,
        },
    })
}

fn validate_prefix(path: &Path, offset: u64, prefix: &[u8; PREFIX_LEN]) -> StorageResult<()> {
    if &prefix[..4] != JOURNAL_MAGIC {
        return Err(StorageError::corrupt(path, offset, "invalid journal magic"));
    }
    if prefix[4] != JOURNAL_VERSION {
        return Err(StorageError::corrupt(
            path,
            offset,
            format!("unsupported journal version {}", prefix[4]),
        ));
    }
    if prefix[6..8] != [0, 0] {
        return Err(StorageError::corrupt(
            path,
            offset,
            "journal reserved bytes are nonzero",
        ));
    }
    Ok(())
}

pub fn summarize_events(
    events: &[JournalEvent],
) -> HashMap<(TopicPartition, BatchId), &'static str> {
    let mut states = HashMap::new();
    for event in events {
        match event {
            JournalEvent::Reserve { reservation, .. } => {
                states.insert(
                    (
                        TopicPartition::new(reservation.topic_id, reservation.partition_id),
                        reservation.batch_id,
                    ),
                    "reserved",
                );
            }
            JournalEvent::Commit {
                topic_partition,
                batch_id,
                ..
            } => {
                states.insert((*topic_partition, *batch_id), "committed");
            }
            JournalEvent::Abort {
                topic_partition,
                batch_id,
            } => {
                states.insert((*topic_partition, *batch_id), "aborted");
            }
            JournalEvent::Configure { .. } | JournalEvent::Truncate { .. } => {}
        }
    }
    states
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    struct TempFile(PathBuf);

    impl TempFile {
        fn new(name: &str) -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            Self(std::env::temp_dir().join(format!(
                "shard-stream-journal-{name}-{}-{nonce}.ssj",
                std::process::id()
            )))
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn reservation() -> Reservation {
        Reservation {
            topic_id: TopicId::new(9),
            partition_id: LogicalPartitionId::new(2),
            batch_id: BatchId::new(4),
            first_offset: LogicalOffset::new(10),
            last_offset: LogicalOffset::new(12),
            record_count: NonZeroU32::new(3).expect("nonzero"),
            placement: Placement {
                shard_id: ShardId::new(1),
                ring_epoch: RingEpoch::new(7),
                sequence: PlacementSequence::new(4),
            },
        }
    }

    #[test]
    fn round_trips_every_event_and_repairs_partial_tail() {
        let temp = TempFile::new("roundtrip");
        let events = vec![
            JournalEvent::Configure {
                topic_partition: TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(2)),
                ring_epoch: RingEpoch::new(7),
                shards: vec![ShardId::new(1), ShardId::new(2)],
            },
            JournalEvent::Reserve {
                reservation: reservation(),
                producer: Some(ProducerSequence {
                    producer_id: 3,
                    epoch: 2,
                    first_sequence: 99,
                    digest_algorithm: DIGEST_BLAKE3,
                    payload_digest: [17; 32],
                }),
            },
            JournalEvent::Commit {
                topic_partition: TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(2)),
                batch_id: BatchId::new(4),
                durable_replicas: 2,
                object_durable: true,
            },
            JournalEvent::Truncate {
                topic_partition: TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(2)),
                log_start: LogicalOffset::new(13),
            },
        ];
        {
            let (mut journal, recovered) = CoordinatorJournal::open(&temp.0).expect("open journal");
            assert!(recovered.events.is_empty());
            for event in &events {
                journal.append(event, true).expect("append");
            }
        }
        OpenOptions::new()
            .append(true)
            .open(&temp.0)
            .expect("open tail")
            .write_all(b"SS")
            .expect("partial tail");

        let (_, recovered) = CoordinatorJournal::open(&temp.0).expect("recover");
        assert_eq!(recovered.events, events);
        assert!(recovered.repaired_partial_tail);
    }

    #[test]
    fn checksum_corruption_is_an_error() {
        let temp = TempFile::new("crc");
        {
            let (mut journal, _) = CoordinatorJournal::open(&temp.0).expect("open");
            journal
                .append(
                    &JournalEvent::Reserve {
                        reservation: reservation(),
                        producer: None,
                    },
                    true,
                )
                .expect("append");
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&temp.0)
            .expect("open corrupt");
        file.seek(SeekFrom::Start(PREFIX_LEN as u64 + 1))
            .expect("seek");
        file.write_all(b"X").expect("corrupt");
        file.sync_data().expect("sync");

        assert!(matches!(
            CoordinatorJournal::open(&temp.0),
            Err(StorageError::Corrupt { .. })
        ));
    }
}
