//! Nonmutating native frame inspection; physical custody belongs to the caller.

use std::path::Path;

use crate::{
    AtomicGroupSequence, ExtentCatalogEntry, JournalEvent, ProducerSequence, Reservation, ShardId,
    StorageError, StorageResult,
};

/// Fixed native SSE8 prefix; read/admit this before allocating the whole frame.
pub const RECOVERY_EXTENT_PREFIX_BYTES: usize = 16;
/// Actual fixed SSE8 header, before payload and checksum.
pub const RECOVERY_EXTENT_HEADER_BYTES: usize = crate::extent::recovery_header_bytes();
/// Fixed native SSJ3 prefix; read/admit this before allocating the whole frame.
pub const RECOVERY_JOURNAL_PREFIX_BYTES: usize = 12;

/// Native header fields observed without a complete payload/checksum.
///
/// These are conservative local byte observations, not a valid extent, payload
/// availability, authenticated history or permission to append at a term.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryExtentHeader {
    /// Actual native reservation, including its placement leader epoch.
    pub reservation: Reservation,
    /// Actual native producer metadata, when present.
    pub producer: Option<ProducerSequence>,
    /// Actual native atomic-group metadata, when present.
    pub atomic_group: Option<AtomicGroupSequence>,
    /// Header-declared complete frame length, including the absent checksum.
    pub declared_frame_bytes: usize,
    /// Header-declared payload length; missing bytes are never manufactured.
    pub payload_bytes: usize,
}

/// Why an incomplete byte slice cannot supply a usable native header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryExtentTailUnknown {
    /// Fewer than the actual native fixed header bytes were retained.
    ShortHeader,
    /// Native prefix/header/range/length validation refused the retained bytes.
    InvalidHeader,
    /// The slice is not shorter than its native declared complete frame.
    NotIncomplete,
}

/// Conservative header observation or explicit unknown; never full validity.
#[derive(Debug, PartialEq, Eq)]
pub enum RecoveryExtentTail {
    /// Every fixed-header field passed the actual native header parser.
    /// The fixed native output allocation must be admitted before inspection.
    Header(Box<RecoveryExtentHeader>),
    /// No safe numeric floor follows from this tail alone.
    Unknown(RecoveryExtentTailUnknown),
}

/// Inspects retained incomplete SSE8 bytes through the native header decoder.
///
/// Admit fixed header/result slots, fixed scan work and one native diagnostic
/// path/message allocation before calling. This borrows the original raw bytes,
/// performs no I/O, and never pads, re-encodes or verifies a missing checksum.
/// A caller must retain Unknown and its raw provenance when choosing floors.
pub fn inspect_recovery_extent_tail(
    path: &Path,
    offset: u64,
    tail: &[u8],
    max_batch_bytes: usize,
) -> RecoveryExtentTail {
    use RecoveryExtentTailUnknown::{InvalidHeader, NotIncomplete, ShortHeader};
    if tail.len() < RECOVERY_EXTENT_HEADER_BYTES {
        return RecoveryExtentTail::Unknown(ShortHeader);
    }
    let prefix = tail[..RECOVERY_EXTENT_PREFIX_BYTES]
        .try_into()
        .expect("complete native header includes prefix");
    let Ok(length) = recovery_extent_frame_bytes(path, offset, prefix, max_batch_bytes) else {
        return RecoveryExtentTail::Unknown(InvalidHeader);
    };
    if tail.len() >= length {
        return RecoveryExtentTail::Unknown(NotIncomplete);
    }
    let mut decoder = crate::codec::Decoder::new(
        &tail[RECOVERY_EXTENT_PREFIX_BYTES..RECOVERY_EXTENT_HEADER_BYTES],
    );
    let Ok(header) = crate::extent::decode_extent_header(path, offset, &mut decoder) else {
        return RecoveryExtentTail::Unknown(InvalidHeader);
    };
    if !decoder.is_empty()
        || crate::extent::recovery_frame_minimum().checked_add(header.payload_len) != Some(length)
    {
        return RecoveryExtentTail::Unknown(InvalidHeader);
    }
    RecoveryExtentTail::Header(Box::new(RecoveryExtentHeader {
        reservation: header.reservation,
        producer: header.producer,
        atomic_group: header.atomic_group,
        declared_frame_bytes: length,
        payload_bytes: header.payload_len,
    }))
}

/// Validates a native SSE8 prefix and computes the bounded complete frame size.
pub fn recovery_extent_frame_bytes(
    path: &Path,
    offset: u64,
    prefix: &[u8; RECOVERY_EXTENT_PREFIX_BYTES],
    max_batch_bytes: usize,
) -> StorageResult<usize> {
    crate::extent::validate_prefix(path, offset, prefix)?;
    let length = u64::from_le_bytes(prefix[8..16].try_into().expect("native length width"));
    // This is the native header/body/checksum layout, exported by its owner.
    let maximum = crate::extent::recovery_frame_ceiling(max_batch_bytes)?;
    let length = usize::try_from(length)
        .map_err(|_| StorageError::corrupt(path, offset, "extent frame exceeds usize"))?;
    if length < crate::extent::recovery_frame_minimum() || length > maximum {
        return Err(StorageError::corrupt(
            path,
            offset,
            "invalid extent frame length",
        ));
    }
    Ok(length)
}

/// Inspects a complete native extent without storing, repairing or filtering it.
///
/// The caller must admit this frame, native decoded result and checksum work
/// before reading/materializing it. Physical shard is not an encoded field;
/// actual lane-to-shard joins belong to the engine configuration layer.
pub fn inspect_recovery_extent(
    path: &Path,
    offset: u64,
    frame: &[u8],
    max_batch_bytes: usize,
    pack_sequence: u64,
) -> StorageResult<ExtentCatalogEntry> {
    let prefix = frame
        .get(..RECOVERY_EXTENT_PREFIX_BYTES)
        .and_then(|p| p.try_into().ok())
        .ok_or_else(|| StorageError::corrupt(path, offset, "extent prefix is incomplete"))?;
    if recovery_extent_frame_bytes(path, offset, prefix, max_batch_bytes)? != frame.len() {
        return Err(StorageError::corrupt(
            path,
            offset,
            "extent frame length disagrees",
        ));
    }
    let decoded = crate::extent::decode_extent(path, offset, frame, ShardId::new(0))?;
    Ok(ExtentCatalogEntry {
        reservation: decoded.reservation,
        producer: decoded.producer,
        atomic_group: decoded.atomic_group,
        payload_bytes: (frame.len() - crate::extent::recovery_frame_minimum()) as u64,
        pack_sequence,
        object_durable: false,
    })
}

/// Validates an SSJ3 prefix and its actual native maximum before allocation.
pub fn recovery_journal_frame_bytes(
    path: &Path,
    offset: u64,
    prefix: &[u8; RECOVERY_JOURNAL_PREFIX_BYTES],
) -> StorageResult<usize> {
    crate::journal::validate_prefix(path, offset, prefix)?;
    let body = u32::from_le_bytes(prefix[8..12].try_into().expect("native length width")) as usize;
    crate::journal::recovery_frame_length(path, offset, body)
}

/// Named fixed native SSJ3 inspection slots, before any decoder work.
///
/// These ordinary logical facts exclude held frame/path bytes and dynamic
/// shard values. Admit those separately: each encoded shard occupies at most
/// one Vec value and one uniqueness-set value (at most frame bytes / u32).
/// This creates no budget, storage owner or authenticated source observation.
pub fn recovery_journal_fixed_inspection_cost() -> (usize, usize) {
    use std::collections::HashSet;
    use std::mem::size_of;
    let slots = [
        size_of::<crate::codec::Decoder<'_>>(),
        size_of::<JournalEvent>(),
        size_of::<Vec<ShardId>>(),
        size_of::<HashSet<ShardId>>(),
        size_of::<&[u8; RECOVERY_JOURNAL_PREFIX_BYTES]>(),
        size_of::<&[u8]>(),
        size_of::<&Path>(),
        size_of::<StorageError>(),
        size_of::<u64>(),
        size_of::<usize>(),
        size_of::<u32>(),
    ];
    (slots.into_iter().sum(), slots.len())
}

/// Validates native SSJ3 framing/CRC/body without a journal open or tail repair.
///
/// Admit decoded shard-vector/hash-set upper bounds from the frame length
/// before calling. Native decoding also checks a shard count fits the body.
pub fn inspect_recovery_journal(
    path: &Path,
    offset: u64,
    frame: &[u8],
) -> StorageResult<JournalEvent> {
    let prefix = frame
        .get(..RECOVERY_JOURNAL_PREFIX_BYTES)
        .and_then(|p| p.try_into().ok())
        .ok_or_else(|| StorageError::corrupt(path, offset, "journal prefix is incomplete"))?;
    if recovery_journal_frame_bytes(path, offset, prefix)? != frame.len() {
        return Err(StorageError::corrupt(
            path,
            offset,
            "journal frame length disagrees",
        ));
    }
    let checksum_at = frame.len() - 4;
    let expected = u32::from_le_bytes(frame[checksum_at..].try_into().expect("native CRC width"));
    if crc32fast::hash(&frame[..checksum_at]) != expected {
        return Err(StorageError::corrupt(
            path,
            offset,
            "journal checksum mismatch",
        ));
    }
    crate::journal::decode_event(
        path,
        offset,
        prefix[5],
        &frame[RECOVERY_JOURNAL_PREFIX_BYTES..checksum_at],
    )
}
