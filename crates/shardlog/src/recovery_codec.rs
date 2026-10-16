//! Nonmutating native frame inspection; physical custody belongs to the caller.

use std::path::Path;

use crate::{ExtentCatalogEntry, JournalEvent, ShardId, StorageError, StorageResult};

/// Fixed native SSE8 prefix; read/admit this before allocating the whole frame.
pub const RECOVERY_EXTENT_PREFIX_BYTES: usize = 16;
/// Fixed native SSJ3 prefix; read/admit this before allocating the whole frame.
pub const RECOVERY_JOURNAL_PREFIX_BYTES: usize = 12;

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
