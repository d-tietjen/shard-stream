//! Bounded complete native extent inspection borrowing the original payload.

use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::path::Path;
use std::time::Instant;

use crate::codec::Decoder;
use crate::extent::{DecodedExtentHeader, ExtentLocation};
use crate::{
    AtomicGroupSequence, ProducerSequence, RECOVERY_EXTENT_HEADER_BYTES,
    RECOVERY_EXTENT_PREFIX_BYTES, Reservation, StorageError, recovery_extent_frame_bytes,
};

/// Explicit finite per-call limits; the composing owner keeps cumulative usage.
#[derive(Clone, Copy, Debug)]
pub struct RecoveryExtentInspectionLimits {
    /// Actual native maximum payload size; must be nonzero.
    pub max_batch_bytes: usize,
    /// Maximum original complete frame, including header and checksum.
    pub max_frame_bytes: usize,
    /// Maximum original encoded diagnostic path before native formatting.
    pub max_path_bytes: usize,
    /// Logical held input, native values, result and diagnostic bytes.
    pub max_declared_bytes: usize,
    /// Logical checksum, fixed decode, value-copy and diagnostic byte work.
    pub max_work_bytes: usize,
    /// Cumulative fixed value/buffer slots for this invocation.
    pub max_entries: usize,
}

/// Ordinary refusal or the actual native decoder error, without storage changes.
#[derive(Debug)]
pub enum RecoveryExtentInspectionError {
    /// The original absolute expiry has elapsed; no replacement clock is made.
    Deadline,
    /// A required finite limit is zero, unrepresentable or inconsistent.
    InvalidLimits,
    /// A checked input/cost bound or source-offset range was exceeded.
    Capacity,
    /// The unchanged native prefix/checksum/header/payload decoder refused.
    Native(StorageError),
}

impl fmt::Display for RecoveryExtentInspectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Deadline => f.write_str("native extent inspection expired"),
            Self::InvalidLimits => f.write_str("invalid native extent inspection limits"),
            Self::Capacity => f.write_str("native extent inspection capacity exceeded"),
            Self::Native(error) => fmt::Display::fmt(error, f),
        }
    }
}

impl Error for RecoveryExtentInspectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Native(error) => Some(error),
            Self::Deadline | Self::InvalidLimits | Self::Capacity => None,
        }
    }
}

/// Exact native fields and original checksum-validated payload, borrowed in place.
///
/// This establishes native frame validity only. It supplies no authenticated
/// origin, child/body lineage, completeness, currentness or write permission.
#[derive(PartialEq, Eq)]
pub struct BorrowedRecoveryExtent<'a> {
    reservation: Reservation,
    producer: Option<ProducerSequence>,
    atomic_group: Option<AtomicGroupSequence>,
    frame: &'a [u8],
    payload: &'a [u8],
}

impl<'a> BorrowedRecoveryExtent<'a> {
    /// Actual native reservation, including its complete leader epoch.
    pub const fn reservation(&self) -> Reservation {
        self.reservation
    }
    /// Actual native producer fields, without producer/lineage authority.
    pub const fn producer(&self) -> Option<ProducerSequence> {
        self.producer
    }
    /// Actual native atomic-group fields, without a commit decision.
    pub const fn atomic_group(&self) -> Option<AtomicGroupSequence> {
        self.atomic_group
    }
    /// Original complete raw extent, without copying or canonical re-encoding.
    pub const fn frame(&self) -> &'a [u8] {
        self.frame
    }
    /// Original payload slice selected by the actual native decoder.
    pub const fn payload(&self) -> &'a [u8] {
        self.payload
    }
}

impl fmt::Debug for BorrowedRecoveryExtent<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BorrowedRecoveryExtent")
            .field("reservation", &self.reservation)
            .field("producer", &self.producer)
            .field("atomic_group", &self.atomic_group)
            .field("frame_bytes", &self.frame.len())
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}

/// Plain source-derived upper bounds to admit before native inspection.
///
/// These facts create no budget, proof or process capability. They include the
/// already-held input/path once, the named native/result/scratch slots, one
/// diagnostic's worst-case lossy UTF-8 path and native reason. Logical bytes
/// and byte work are separate from allocator overhead, RSS or elapsed CPU.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryExtentInspectionCost {
    input_bytes: usize,
    declared_bytes: usize,
    work_bytes: usize,
    entries: usize,
}

impl RecoveryExtentInspectionCost {
    /// Original held frame bytes included in declared bytes.
    pub const fn input_bytes(self) -> usize {
        self.input_bytes
    }
    /// Complete logical input/native/result/diagnostic byte bound.
    pub const fn declared_bytes(self) -> usize {
        self.declared_bytes
    }
    /// Complete logical checksum/decode/copy/diagnostic byte work bound.
    pub const fn work_bytes(self) -> usize {
        self.work_bytes
    }
    /// Named cumulative value/buffer slots, including both decoder invocations.
    pub const fn entries(self) -> usize {
        self.entries
    }
}

/// Sizes one call from lengths and explicit configuration, without reading bytes.
///
/// The caller must charge this ordinary fact into its SAME existing cumulative
/// owner before inspection. Calling this function does not claim admission.
pub fn recovery_extent_inspection_cost(
    frame_bytes: usize,
    path_bytes: usize,
    limits: RecoveryExtentInspectionLimits,
) -> Result<RecoveryExtentInspectionCost, RecoveryExtentInspectionError> {
    use RecoveryExtentInspectionError::{Capacity, InvalidLimits};
    if [
        limits.max_batch_bytes,
        limits.max_frame_bytes,
        limits.max_path_bytes,
        limits.max_declared_bytes,
        limits.max_work_bytes,
        limits.max_entries,
    ]
    .into_iter()
    .any(|n| n == 0 || n > isize::MAX as usize)
        || limits.max_frame_bytes < crate::extent::recovery_frame_minimum()
        || crate::extent::recovery_frame_minimum()
            .checked_add(limits.max_batch_bytes)
            .is_none()
    {
        return Err(InvalidLimits);
    }
    if frame_bytes > limits.max_frame_bytes || path_bytes > limits.max_path_bytes {
        return Err(Capacity);
    }
    // All complete-decoder reason literals fit the actual longest native one.
    // Prefix version formatting adds at most the three decimal digits of u8.
    const REASON_BYTES: usize = "record range does not match record count".len();
    let slots = [
        size_of::<&[u8]>(),
        size_of::<&Path>(),
        size_of::<BorrowedRecoveryExtent<'_>>(),
        size_of::<ExtentLocation>(),
        size_of::<DecodedExtentHeader>(),
        size_of::<Decoder<'_>>(),
        size_of::<Decoder<'_>>(),
        size_of::<&[u8; RECOVERY_EXTENT_PREFIX_BYTES]>(),
        size_of::<u64>(),
        size_of::<u64>(),
        size_of::<RecoveryExtentInspectionLimits>(),
        size_of::<RecoveryExtentInspectionCost>(),
        size_of::<StorageError>(),
        size_of::<RecoveryExtentInspectionError>(),
    ];
    let fixed = slots
        .into_iter()
        .try_fold(0usize, |sum, n| sum.checked_add(n))
        .ok_or(Capacity)?;
    // Native corrupt() formats at most three UTF-8 bytes per original path byte.
    // Reserve both error buffers even on success, before invoking native code.
    let diagnostic = path_bytes
        .checked_mul(3)
        .and_then(|n| n.checked_add(REASON_BYTES))
        .ok_or(Capacity)?;
    let declared_bytes = frame_bytes
        .checked_add(path_bytes)
        .and_then(|n| n.checked_add(fixed))
        .and_then(|n| n.checked_add(diagnostic))
        .ok_or(Capacity)?;
    // One full native checksum pass, the fixed prefix/header pass, both fixed
    // decoder/value-copy frames and the bounded diagnostic formatting/copy pass.
    let work_bytes = frame_bytes
        .checked_add(RECOVERY_EXTENT_PREFIX_BYTES)
        .and_then(|n| n.checked_add(RECOVERY_EXTENT_HEADER_BYTES))
        .and_then(|n| n.checked_add(fixed))
        .and_then(|n| diagnostic.checked_mul(2).and_then(|d| n.checked_add(d)))
        .ok_or(Capacity)?;
    let entries = slots.len() + 2; // Named fixed slots plus native path/reason buffers.
    if declared_bytes > limits.max_declared_bytes
        || work_bytes > limits.max_work_bytes
        || entries > limits.max_entries
    {
        return Err(Capacity);
    }
    Ok(RecoveryExtentInspectionCost {
        input_bytes: frame_bytes,
        declared_bytes,
        work_bytes,
        entries,
    })
}

fn clock(expiry: Instant) -> Result<(), RecoveryExtentInspectionError> {
    if Instant::now() >= expiry {
        Err(RecoveryExtentInspectionError::Deadline)
    } else {
        Ok(())
    }
}

/// Validates one complete native extent and borrows its original payload.
///
/// Check/charge `recovery_extent_inspection_cost` in the composing caller before
/// invoking this function. Per-call limits are checked again before any native
/// checksum, decode, diagnostic allocation or payload exposure. The exact saved
/// Instant is checked before every native phase and after it settles; an elapsed
/// call returns Deadline even when native validation completed. No I/O, mutation,
/// worker, re-encoding, payload clone, new timeout or authority is introduced.
pub fn inspect_recovery_extent_payload<'a>(
    path: &Path,
    offset: u64,
    frame: &'a [u8],
    limits: RecoveryExtentInspectionLimits,
    expiry: Instant,
) -> Result<BorrowedRecoveryExtent<'a>, RecoveryExtentInspectionError> {
    use RecoveryExtentInspectionError::{Capacity, Native};
    clock(expiry)?;
    let cost = recovery_extent_inspection_cost(
        frame.len(),
        path.as_os_str().as_encoded_bytes().len(),
        limits,
    );
    clock(expiry)?;
    cost?;
    let length = u64::try_from(frame.len()).map_err(|_| Capacity)?;
    offset.checked_add(length).ok_or(Capacity)?;
    clock(expiry)?;
    let prefix = frame
        .get(..RECOVERY_EXTENT_PREFIX_BYTES)
        .and_then(|p| p.try_into().ok())
        .ok_or_else(|| {
            Native(StorageError::corrupt(
                path,
                offset,
                "extent prefix is incomplete",
            ))
        });
    clock(expiry)?;
    let prefix = prefix?;
    let native_length = recovery_extent_frame_bytes(path, offset, prefix, limits.max_batch_bytes);
    clock(expiry)?;
    if native_length.map_err(Native)? != frame.len() {
        let error = Native(StorageError::corrupt(
            path,
            offset,
            "extent frame length disagrees",
        ));
        clock(expiry)?;
        return Err(error);
    }
    clock(expiry)?;
    let decoded = crate::extent::decode_extent_payload(path, offset, frame);
    clock(expiry)?;
    let (native, payload) = decoded.map_err(Native)?;
    Ok(BorrowedRecoveryExtent {
        reservation: native.reservation,
        producer: native.producer,
        atomic_group: native.atomic_group,
        frame,
        payload,
    })
}

#[cfg(test)]
#[path = "recovery_payload_tests.rs"]
mod tests;
