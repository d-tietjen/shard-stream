//! Native byte interpretation only; archived coordinates confer no custody.

use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::path::Path;
use std::time::Instant;

use shard_stream_core::ShardId;
use shardlog::{
    BorrowedRecoveryExtent, JournalEvent, RECOVERY_EXTENT_HEADER_BYTES,
    RECOVERY_EXTENT_PREFIX_BYTES, RECOVERY_JOURNAL_PREFIX_BYTES, RecoveryExtentHeader,
    RecoveryExtentInspectionError, RecoveryExtentInspectionLimits, RecoveryExtentTail,
    StorageError, TierExtent, TierManifest, TierObject, decode_recovery_tier_manifest,
    inspect_recovery_extent_payload, inspect_recovery_extent_tail, inspect_recovery_journal,
    parse_recovery_tier_current, recovery_extent_frame_bytes, recovery_extent_inspection_cost,
    recovery_journal_fixed_inspection_cost, recovery_journal_frame_bytes,
};

use crate::RecoverySourceKind;
use crate::recovery_json::{self, Budget};

/// Finite per-call bounds, tightened by the actual cumulative resource owner.
#[derive(Clone, Copy, Debug)]
pub struct NativeArchivedSourceLimits {
    /// Original complete source bytes held by the caller.
    pub max_source_bytes: usize,
    /// One complete native frame before decoding it.
    pub max_frame_bytes: usize,
    /// Complete frames retained from one physical source.
    pub max_frames: usize,
    /// Complete native CURRENT or manifest input.
    pub max_manifest_bytes: usize,
    /// Generic JSON keys/values visited before native typed materialization.
    pub max_json_nodes: usize,
    /// Generic JSON nesting; may tighten the native depth32 bound.
    pub max_json_depth: usize,
    /// Original encoded diagnostic path before native error formatting.
    pub max_path_bytes: usize,
    /// Native SSE8 payload ceiling, including for incomplete headers.
    pub max_batch_bytes: usize,
    /// Logical input/result/native-value/error bytes for this invocation.
    pub max_declared_bytes: usize,
    /// Logical decode/hash/validation/comparison/copy work for this invocation.
    pub max_work_bytes: usize,
    /// Cumulative logical value/buffer/JSON/native slots for this invocation.
    pub max_entries: usize,
}

/// Ordinary byte refusal; no process or storage permission can be recovered.
#[derive(Debug)]
pub enum NativeArchivedSourceError {
    /// The supplied original absolute expiry elapsed.
    Deadline,
    /// Finite limits are invalid or loosen native maximum JSON depth.
    InvalidLimits,
    /// Checked source/native materialization costs exceed their limits.
    Capacity,
    /// The shared generic native JSON shape parser refused.
    JsonShape,
    /// CURRENT bytes are not UTF-8, before the original native text parser.
    CurrentUtf8,
    /// The original native trim/u64 CURRENT parser refused.
    CurrentValue,
    /// The actual native typed manifest deserializer refused after shape admission.
    ManifestDecode,
    /// A complete native codec or native manifest validator refused.
    Native(StorageError),
}
impl fmt::Display for NativeArchivedSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Deadline => f.write_str("archived native source expired"),
            Self::InvalidLimits => f.write_str("invalid archived native source limits"),
            Self::Capacity => f.write_str("archived native source capacity exceeded"),
            Self::JsonShape => f.write_str("archived native JSON shape refused"),
            Self::CurrentUtf8 => f.write_str("archived native CURRENT is not UTF-8"),
            Self::CurrentValue => f.write_str("archived native CURRENT generation is invalid"),
            Self::ManifestDecode => f.write_str("archived native manifest decode refused"),
            Self::Native(error) => fmt::Display::fmt(error, f),
        }
    }
}
impl Error for NativeArchivedSourceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Native(error) => Some(error),
            _ => None,
        }
    }
}
type Result<T> = std::result::Result<T, NativeArchivedSourceError>;
use NativeArchivedSourceError::{Capacity, Deadline, InvalidLimits, Native};

/// Actual native event decoded from one original complete SSJ3 frame.
#[derive(Debug)]
pub struct ArchivedNativeJournalEvent<'a> {
    offset: u64,
    raw: &'a [u8],
    native: JournalEvent,
}
impl<'a> ArchivedNativeJournalEvent<'a> {
    /// Original source byte offset.
    pub const fn file_offset(&self) -> u64 {
        self.offset
    }
    /// Original complete frame, with no re-encoding.
    pub const fn raw_frame(&self) -> &'a [u8] {
        self.raw
    }
    /// Actual native event, not a latest-state or term projection.
    pub const fn native(&self) -> &JournalEvent {
        &self.native
    }
}

/// Complete checksum-validated native extent borrowing its original body.
#[derive(Debug)]
pub struct ArchivedNativeExtent<'a> {
    offset: u64,
    native: BorrowedRecoveryExtent<'a>,
}
impl<'a> ArchivedNativeExtent<'a> {
    /// Original source byte offset.
    pub const fn file_offset(&self) -> u64 {
        self.offset
    }
    /// The native validated view; its payload still borrows the original raw.
    pub const fn native(&self) -> &BorrowedRecoveryExtent<'a> {
        &self.native
    }
}

/// Exact incomplete terminal suffix, never a complete-payload validity claim.
#[derive(Debug)]
pub struct ArchivedNativeTail<'a> {
    offset: u64,
    raw: &'a [u8],
    extent: Option<RecoveryExtentTail>,
}
impl<'a> ArchivedNativeTail<'a> {
    /// Original terminal suffix offset.
    pub const fn file_offset(&self) -> u64 {
        self.offset
    }
    /// Exact retained incomplete bytes.
    pub const fn raw_bytes(&self) -> &'a [u8] {
        self.raw
    }
    /// Native SSE8 header/Unknown; SSJ3 encodes no leader term and returns None.
    pub const fn extent(&self) -> Option<&RecoveryExtentTail> {
        self.extent.as_ref()
    }
}

/// Ordinary native observations from untrusted captured bytes, privately built.
///
/// Kind, path and shard supplied to inspection do not prove physical origin.
/// This cannot install a census/engine, choose a floor or grant currentness.
#[derive(Debug)]
pub struct UnverifiedNativeArchivedSource<'a> {
    kind: RecoverySourceKind,
    raw: &'a [u8],
    journal: Vec<ArchivedNativeJournalEvent<'a>>,
    extents: Vec<ArchivedNativeExtent<'a>>,
    tail: Option<ArchivedNativeTail<'a>>,
    current: Option<u64>,
    manifest: Option<TierManifest>,
}
impl<'a> UnverifiedNativeArchivedSource<'a> {
    /// Original ordinary captured kind, not authenticated provenance.
    pub const fn kind(&self) -> RecoverySourceKind {
        self.kind
    }
    /// Entire original bytes, including any incomplete suffix.
    pub const fn raw_bytes(&self) -> &'a [u8] {
        self.raw
    }
    /// Every native journal event, in physical order; no retention filtering.
    pub fn journal(&self) -> &[ArchivedNativeJournalEvent<'a>] {
        &self.journal
    }
    /// Every complete native extent, in physical order; no latest-only filter.
    pub fn extents(&self) -> &[ArchivedNativeExtent<'a>] {
        &self.extents
    }
    /// Original incomplete suffix and native conservative observation if any.
    pub const fn tail(&self) -> Option<&ArchivedNativeTail<'a>> {
        self.tail.as_ref()
    }
    /// Native CURRENT text value only, not a current-manifest/source join.
    pub const fn current_generation(&self) -> Option<u64> {
        self.current
    }
    /// The original native manifest; CURRENT/hash/coverage joins remain due.
    pub const fn manifest(&self) -> Option<&TierManifest> {
        self.manifest.as_ref()
    }
}

/// Plain logical facts for pre-admission, not a budget, owner or permission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeArchivedSourceCost {
    input_bytes: usize,
    declared_bytes: usize,
    work_bytes: usize,
    entries: usize,
    string_bytes: usize,
    payload_bytes: usize,
}
impl NativeArchivedSourceCost {
    /// Original raw held input bytes, counted in declared bytes.
    pub const fn input_bytes(self) -> usize {
        self.input_bytes
    }
    /// Logical input/native/results/error bound, separate from allocator RSS.
    pub const fn declared_bytes(self) -> usize {
        self.declared_bytes
    }
    /// Logical hash/decode/scan/comparison/copy byte work.
    pub const fn work_bytes(self) -> usize {
        self.work_bytes
    }
    /// Native/frame/JSON/value/buffer slots, cumulative per invocation.
    pub const fn entries(self) -> usize {
        self.entries
    }
    /// Original path/native JSON/error-string upper bound.
    pub const fn string_bytes(self) -> usize {
        self.string_bytes
    }
    /// Original payload/native-string bytes upper bound.
    pub const fn payload_bytes(self) -> usize {
        self.payload_bytes
    }
}

fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or(Capacity)
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or(Capacity)
}
fn clock(expiry: Instant) -> Result<()> {
    if Instant::now() >= expiry {
        Err(Deadline)
    } else {
        Ok(())
    }
}
fn validate(limits: NativeArchivedSourceLimits) -> Result<()> {
    if [
        limits.max_source_bytes,
        limits.max_frame_bytes,
        limits.max_frames,
        limits.max_manifest_bytes,
        limits.max_json_nodes,
        limits.max_json_depth,
        limits.max_path_bytes,
        limits.max_batch_bytes,
        limits.max_declared_bytes,
        limits.max_work_bytes,
        limits.max_entries,
    ]
    .into_iter()
    .any(|n| n == 0 || n > isize::MAX as usize)
        || limits.max_json_depth > 32
        || limits.max_frame_bytes < RECOVERY_EXTENT_HEADER_BYTES
    {
        return Err(InvalidLimits);
    }
    Ok(())
}
fn frame_slots(
    bytes: usize,
    kind: RecoverySourceKind,
    limits: NativeArchivedSourceLimits,
) -> usize {
    let prefix = if kind == RecoverySourceKind::Coordinator {
        RECOVERY_JOURNAL_PREFIX_BYTES
    } else {
        RECOVERY_EXTENT_PREFIX_BYTES
    };
    (bytes / prefix).min(limits.max_frames)
}
fn payload_limits(limits: NativeArchivedSourceLimits) -> RecoveryExtentInspectionLimits {
    RecoveryExtentInspectionLimits {
        max_batch_bytes: limits.max_batch_bytes,
        max_frame_bytes: limits.max_frame_bytes,
        max_path_bytes: limits.max_path_bytes,
        max_declared_bytes: limits.max_declared_bytes,
        max_work_bytes: limits.max_work_bytes,
        max_entries: limits.max_entries,
    }
}

/// Length-only facts before reading any native byte or allocating a result.
///
/// A host must charge these into its SAME cumulative Session before calling
/// inspection. Concatenating original archive chunks is separate charged work.
/// These bounds cover both successful values and a failing native diagnostic;
/// they do not describe allocator/hash-table overhead or process memory limits.
pub fn native_archived_source_inspection_cost(
    kind: RecoverySourceKind,
    bytes: usize,
    path_bytes: usize,
    limits: NativeArchivedSourceLimits,
) -> Result<NativeArchivedSourceCost> {
    validate(limits)?;
    if bytes > limits.max_source_bytes || path_bytes > limits.max_path_bytes {
        return Err(Capacity);
    }
    let fixed_slots = [
        size_of::<&[u8]>(),
        size_of::<&Path>(),
        size_of::<Instant>(),
        size_of::<UnverifiedNativeArchivedSource<'_>>(),
        size_of::<NativeArchivedSourceCost>(),
        size_of::<NativeArchivedSourceLimits>(),
        size_of::<NativeArchivedSourceError>(),
        size_of::<StorageError>(),
        size_of::<ArchivedNativeTail<'_>>(),
        size_of::<RecoveryExtentHeader>(),
        size_of::<RecoveryExtentTail>(),
        size_of::<serde_json::Deserializer<serde_json::de::SliceRead<'_>>>(),
        size_of::<serde_json::Error>(),
        size_of::<JsonBudget>(),
        size_of::<shardlog::RecoveryExtentInspectionCost>(),
        size_of::<usize>(),
        size_of::<usize>(),
    ];
    let fixed = fixed_slots
        .into_iter()
        .try_fold(0usize, |n, slot| n.checked_add(slot))
        .ok_or(Capacity)?;
    // Native errors may include a lossy UTF8 path and serde's bounded input
    // description copied through the native error wrapper. Input string bytes
    // and two diagnostic copies are reserved even on successful inspection.
    let diagnostics = add(mul(path_bytes, 3)?, add(mul(bytes, 2)?, 512)?)?;
    let mut declared = add(add(bytes, path_bytes)?, add(fixed, mul(diagnostics, 2)?)?)?;
    let mut work = add(mul(add(bytes, diagnostics)?, 8)?, fixed)?;
    let mut entries = add(fixed_slots.len(), 2)?; // Two native diagnostic buffers.
    match kind {
        RecoverySourceKind::TierCurrent => {
            if bytes > limits.max_manifest_bytes {
                return Err(Capacity);
            }
        }
        RecoverySourceKind::TierManifest | RecoverySourceKind::UnreferencedTierManifest => {
            if bytes > limits.max_manifest_bytes {
                return Err(Capacity);
            }
            let nodes = add(bytes.min(limits.max_json_nodes), 1)?;
            let native = size_of::<TierManifest>()
                + size_of::<TierObject>()
                + size_of::<TierExtent>()
                + 4 * size_of::<String>()
                + 4 * size_of::<Vec<usize>>()
                + 2 * size_of::<usize>();
            declared = add(declared, add(mul(nodes, mul(native, 2)?)?, mul(bytes, 4)?)?)?;
            work = add(work, add(mul(bytes, 8)?, mul(nodes, mul(native, 4)?)?)?)?;
            // The byte bound names thirteen possible native value/container
            // slots per JSON node, with both materialization/copy values paid.
            entries = add(entries, mul(nodes, 2 * (3 + 4 + 4 + 2))?)?;
        }
        RecoverySourceKind::Coordinator => {
            let frames = frame_slots(bytes, kind, limits);
            // Native decode_shards validates count <= encoded bytes/4 before
            // allocating both its Vec and uniqueness set. Count each value in
            // both containers plus fixed event/frame slots before any decode.
            let shards = bytes / size_of::<u32>();
            let shard_slot = 2 * size_of::<ShardId>() + 2 * size_of::<usize>();
            let (native_bytes, native_slots) = recovery_journal_fixed_inspection_cost();
            let fixed_frame = add(native_bytes, size_of::<ArchivedNativeJournalEvent<'_>>())?;
            declared = add(
                declared,
                add(mul(frames, fixed_frame)?, mul(shards, shard_slot)?)?,
            )?;
            entries = add(
                entries,
                add(mul(frames, add(native_slots, 1)?)?, mul(shards, 2)?)?,
            )?;
            work = add(
                work,
                add(
                    mul(bytes, 8)?,
                    add(mul(frames, fixed_frame)?, mul(shards, shard_slot)?)?,
                )?,
            )?;
        }
        _ => {
            let frames = frame_slots(bytes, kind, limits);
            let native = recovery_extent_inspection_cost(0, path_bytes, payload_limits(limits))
                .map_err(map_payload)?;
            declared = add(
                declared,
                mul(
                    frames,
                    add(
                        size_of::<ArchivedNativeExtent<'_>>(),
                        native.declared_bytes(),
                    )?,
                )?,
            )?;
            entries = add(entries, mul(frames, add(1, native.entries())?)?)?;
            work = add(
                work,
                add(mul(bytes, 2)?, mul(frames, native.work_bytes())?)?,
            )?;
        }
    }
    if declared > limits.max_declared_bytes
        || work > limits.max_work_bytes
        || entries > limits.max_entries
    {
        return Err(Capacity);
    }
    Ok(NativeArchivedSourceCost {
        input_bytes: bytes,
        declared_bytes: declared,
        work_bytes: work,
        entries,
        string_bytes: add(path_bytes, mul(diagnostics, 2)?)?,
        payload_bytes: mul(bytes, 2)?,
    })
}

fn map_payload(error: RecoveryExtentInspectionError) -> NativeArchivedSourceError {
    match error {
        RecoveryExtentInspectionError::Deadline => Deadline,
        RecoveryExtentInspectionError::InvalidLimits => InvalidLimits,
        RecoveryExtentInspectionError::Capacity => Capacity,
        RecoveryExtentInspectionError::Native(error) => Native(error),
    }
}
struct JsonBudget {
    expiry: Instant,
    limit: usize,
    depth: usize,
    used: usize,
    refused: bool,
}
impl Budget for JsonBudget {
    fn node(&mut self) -> bool {
        if Instant::now() >= self.expiry || self.used >= self.limit {
            self.refused = true;
            return false;
        }
        self.used += 1;
        true
    }
    fn maximum_depth(&self) -> usize {
        self.depth
    }
}

/// Interprets one ordinary archived source using the actual native codecs.
///
/// Callers retain original chunks, physical/signed context and cumulative
/// admission. This function borrows bytes, performs no I/O/repair/open/worker,
/// and creates no physical census, floor, source-authentication or permission.
/// All incomplete suffixes remain explicit; complete corrupt frames refuse.
/// An expected shard checks the native manifest against that ordinary value.
/// None validates its own decoded shard only and establishes no placement join.
pub fn inspect_archived_native_source<'a>(
    kind: RecoverySourceKind,
    path: &Path,
    expected_shard: Option<ShardId>,
    raw: &'a [u8],
    limits: NativeArchivedSourceLimits,
    expiry: Instant,
) -> Result<UnverifiedNativeArchivedSource<'a>> {
    clock(expiry)?;
    let admitted = native_archived_source_inspection_cost(
        kind,
        raw.len(),
        path.as_os_str().as_encoded_bytes().len(),
        limits,
    );
    clock(expiry)?;
    admitted?;
    let mut result = UnverifiedNativeArchivedSource {
        kind,
        raw,
        journal: Vec::new(),
        extents: Vec::new(),
        tail: None,
        current: None,
        manifest: None,
    };
    match kind {
        RecoverySourceKind::TierCurrent => {
            let text = std::str::from_utf8(raw);
            clock(expiry)?;
            let value = parse_recovery_tier_current(
                text.map_err(|_| NativeArchivedSourceError::CurrentUtf8)?,
            );
            clock(expiry)?;
            result.current = Some(value.map_err(|_| NativeArchivedSourceError::CurrentValue)?);
        }
        RecoverySourceKind::TierManifest | RecoverySourceKind::UnreferencedTierManifest => {
            let mut budget = JsonBudget {
                expiry,
                limit: limits.max_json_nodes,
                depth: limits.max_json_depth,
                used: 0,
                refused: false,
            };
            clock(expiry)?;
            let shape = recovery_json::inspect(raw, &mut budget);
            clock(expiry)?;
            if budget.refused {
                return Err(Capacity);
            }
            shape.map_err(|_| NativeArchivedSourceError::JsonShape)?;
            #[cfg(test)]
            tests::phase("after-shape", expiry);
            clock(expiry)?;
            let manifest = decode_recovery_tier_manifest(raw);
            clock(expiry)?;
            let manifest = manifest.map_err(|_| NativeArchivedSourceError::ManifestDecode)?;
            let valid =
                manifest.validate(expected_shard.unwrap_or(ShardId::new(manifest.shard_id)));
            clock(expiry)?;
            valid.map_err(Native)?;
            result.manifest = Some(manifest);
        }
        _ => {
            let journal = kind == RecoverySourceKind::Coordinator;
            let slots = frame_slots(raw.len(), kind, limits);
            clock(expiry)?;
            let reserved = if journal {
                result.journal.try_reserve_exact(slots)
            } else {
                result.extents.try_reserve_exact(slots)
            };
            clock(expiry)?;
            reserved.map_err(|_| Capacity)?;
            let mut offset = 0usize;
            while offset < raw.len() {
                clock(expiry)?;
                let prefix = if journal {
                    RECOVERY_JOURNAL_PREFIX_BYTES
                } else {
                    RECOVERY_EXTENT_PREFIX_BYTES
                };
                let rest = &raw[offset..];
                let at = u64::try_from(offset).map_err(|_| Capacity)?;
                let length = if rest.len() < prefix {
                    None
                } else {
                    let length = if journal {
                        recovery_journal_frame_bytes(
                            path,
                            at,
                            rest[..prefix].try_into().expect("native journal prefix"),
                        )
                    } else {
                        recovery_extent_frame_bytes(
                            path,
                            at,
                            rest[..prefix].try_into().expect("native extent prefix"),
                            limits.max_batch_bytes,
                        )
                    };
                    clock(expiry)?;
                    let length = length.map_err(Native)?;
                    if length > limits.max_frame_bytes {
                        return Err(Capacity);
                    }
                    (length <= rest.len()).then_some(length)
                };
                let Some(length) = length else {
                    clock(expiry)?;
                    let extent = if journal {
                        None
                    } else {
                        Some(inspect_recovery_extent_tail(
                            path,
                            at,
                            rest,
                            limits.max_batch_bytes,
                        ))
                    };
                    clock(expiry)?;
                    result.tail = Some(ArchivedNativeTail {
                        offset: at,
                        raw: rest,
                        extent,
                    });
                    break;
                };
                if result.journal.len().max(result.extents.len()) >= limits.max_frames {
                    return Err(Capacity);
                }
                let frame = &rest[..length];
                clock(expiry)?;
                if journal {
                    let event = inspect_recovery_journal(path, at, frame);
                    clock(expiry)?;
                    result.journal.push(ArchivedNativeJournalEvent {
                        offset: at,
                        raw: frame,
                        native: event.map_err(Native)?,
                    });
                } else {
                    let native = inspect_recovery_extent_payload(
                        path,
                        at,
                        frame,
                        payload_limits(limits),
                        expiry,
                    )
                    .map_err(map_payload)?;
                    result
                        .extents
                        .push(ArchivedNativeExtent { offset: at, native });
                }
                #[cfg(test)]
                tests::phase("after-frame", expiry);
                clock(expiry)?;
                offset = offset.checked_add(length).ok_or(Capacity)?;
            }
        }
    }
    clock(expiry)?;
    Ok(result)
}

#[cfg(test)]
#[path = "recovery_archived_tests.rs"]
mod tests;
