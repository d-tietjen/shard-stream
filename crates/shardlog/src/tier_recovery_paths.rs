//! Ordinary exact native tier path joins, without opening or adopting storage.

use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::{ShardId, StorageError};

use super::{current_path, manifest_path, object_path};

/// Additional finite per-invocation ceilings; the host retains cumulative admission.
#[derive(Clone, Copy, Debug)]
pub struct RecoveryTierPathLimits {
    /// Each original encoded path or object key, before native path construction.
    pub max_path_bytes: usize,
    /// Logical held inputs, native path temporaries and diagnostics.
    pub max_declared_bytes: usize,
    /// Logical path-component, native value and diagnostic slots.
    pub max_entries: usize,
    /// Logical path construction, comparison and component-scan work.
    pub max_work_bytes: usize,
}

/// Static ordinary refusal, or the unchanged native object-key refusal.
#[derive(Debug)]
pub enum RecoveryTierPathError {
    /// A finite positive ceiling is invalid.
    InvalidLimits,
    /// A checked input or logical operation cost exceeds its ceiling.
    Capacity,
    /// The supplied original absolute Instant elapsed.
    Deadline,
    /// The actual native object-key validator refused.
    Native(StorageError),
}
impl fmt::Display for RecoveryTierPathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits => f.write_str("invalid native tier path limits"),
            Self::Capacity => f.write_str("native tier path capacity exceeded"),
            Self::Deadline => f.write_str("native tier path inspection expired"),
            Self::Native(error) => fmt::Display::fmt(error, f),
        }
    }
}
impl Error for RecoveryTierPathError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Native(error) => Some(error),
            _ => None,
        }
    }
}
type Result<T> = std::result::Result<T, RecoveryTierPathError>;
use RecoveryTierPathError::{Capacity, Deadline, InvalidLimits, Native};

/// Plain length-derived costs, without a budget, source identity or permission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryTierPathCost {
    declared_bytes: usize,
    entries: usize,
    string_bytes: usize,
    work_bytes: usize,
}
impl RecoveryTierPathCost {
    /// Logical held input and native temporary/error bytes.
    pub const fn declared_bytes(self) -> usize {
        self.declared_bytes
    }
    /// Logical path-component/native value/diagnostic slots.
    pub const fn entries(self) -> usize {
        self.entries
    }
    /// Conservative native path/format/diagnostic string bytes.
    pub const fn string_bytes(self) -> usize {
        self.string_bytes
    }
    /// Logical native path construction, comparison and component visits.
    pub const fn work_bytes(self) -> usize {
        self.work_bytes
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

/// Admits lengths before any native path construction or object-key inspection.
///
/// Use key_bytes=0 for a manifest join. The real host must predebit this plain
/// fact through its SAME cumulative owner before calling either path helper.
/// Logical costs exclude allocator/RSS/CPU measurements; component visits are
/// bounded by encoded input bytes and native fixed name/number widths.
pub fn recovery_tier_path_inspection_cost(
    current_bytes: usize,
    other_path_bytes: usize,
    key_bytes: usize,
    limits: RecoveryTierPathLimits,
) -> Result<RecoveryTierPathCost> {
    if [
        limits.max_path_bytes,
        limits.max_declared_bytes,
        limits.max_entries,
        limits.max_work_bytes,
    ]
    .into_iter()
    .any(|n| n == 0 || n > isize::MAX as usize)
    {
        return Err(InvalidLimits);
    }
    if [current_bytes, other_path_bytes, key_bytes]
        .into_iter()
        .any(|n| n > limits.max_path_bytes)
    {
        return Err(Capacity);
    }
    let inputs = add(add(current_bytes, other_path_bytes)?, key_bytes)?;
    // Both native path producers join three components; root inference also
    // builds the actual relative CURRENT. Reserve their intermediate owned
    // paths/formatted numbers, native validation error, and fixed scratch.
    let fixed = 10 * size_of::<PathBuf>()
        + 6 * size_of::<String>()
        + size_of::<RecoveryTierPathCost>()
        + size_of::<RecoveryTierPathLimits>()
        + size_of::<RecoveryTierPathError>()
        + size_of::<StorageError>()
        + 2048;
    let declared_bytes = add(mul(inputs, 8)?, fixed)?;
    let entries = add(inputs, 64)?;
    let string_bytes = add(mul(inputs, 4)?, 1024)?;
    let work_bytes = add(mul(inputs, 16)?, mul(fixed, 2)?)?;
    if declared_bytes > limits.max_declared_bytes
        || entries > limits.max_entries
        || work_bytes > limits.max_work_bytes
    {
        return Err(Capacity);
    }
    Ok(RecoveryTierPathCost {
        declared_bytes,
        entries,
        string_bytes,
        work_bytes,
    })
}

fn exact(a: &Path, b: &Path) -> bool {
    a.as_os_str().as_encoded_bytes() == b.as_os_str().as_encoded_bytes()
}

// Infer only an ordinary lexical root from the ACTUAL native path producer.
// No copied directory depth, shard-name parser, filesystem read or identity
// assertion is involved. Reconstructed CURRENT must match original raw bytes.
fn root(current: &Path, shard: ShardId, expiry: Instant) -> Result<Option<&Path>> {
    clock(expiry)?;
    let relative = current_path(Path::new(""), shard);
    clock(expiry)?;
    let Some(root) = current.ancestors().nth(relative.components().count()) else {
        return Ok(None);
    };
    clock(expiry)?;
    let expected = current_path(root, shard);
    clock(expiry)?;
    let matches = exact(current, &expected);
    clock(expiry)?;
    Ok(matches.then_some(root))
}

/// Compare CURRENT and a selected manifest using the actual native path layout.
///
/// The shard/generation must come from the actual decoded manifest and CURRENT
/// must separately select that generation. This byte-path match authenticates
/// no source, placement, completeness or permission and performs no I/O.
pub fn recovery_tier_manifest_path_matches(
    current: &Path,
    manifest: &Path,
    decoded_shard: ShardId,
    decoded_generation: u64,
    limits: RecoveryTierPathLimits,
    original_expiry: Instant,
) -> Result<bool> {
    clock(original_expiry)?;
    let cost = recovery_tier_path_inspection_cost(
        current.as_os_str().as_encoded_bytes().len(),
        manifest.as_os_str().as_encoded_bytes().len(),
        0,
        limits,
    );
    clock(original_expiry)?;
    cost?;
    let root = root(current, decoded_shard, original_expiry)?;
    clock(original_expiry)?;
    let Some(root) = root else {
        return Ok(false);
    };
    #[cfg(test)]
    tests::after_root(original_expiry);
    clock(original_expiry)?;
    let expected = manifest_path(root, decoded_shard, decoded_generation);
    clock(original_expiry)?;
    let matches = exact(manifest, &expected);
    clock(original_expiry)?;
    Ok(matches)
}

/// Compare an object to the selected tier root and unchanged native key policy.
///
/// Input key/shard must come from the actual native manifest. The host retains
/// same-set source, full hash/length and native extent-coverage joins; matching
/// a lexical path supplies none of those facts or any storage ownership.
pub fn recovery_tier_object_path_matches(
    current: &Path,
    decoded_shard: ShardId,
    native_object_key: &str,
    object: &Path,
    limits: RecoveryTierPathLimits,
    original_expiry: Instant,
) -> Result<bool> {
    clock(original_expiry)?;
    let cost = recovery_tier_path_inspection_cost(
        current.as_os_str().as_encoded_bytes().len(),
        object.as_os_str().as_encoded_bytes().len(),
        native_object_key.len(),
        limits,
    );
    clock(original_expiry)?;
    cost?;
    let root = root(current, decoded_shard, original_expiry)?;
    clock(original_expiry)?;
    let Some(root) = root else {
        return Ok(false);
    };
    #[cfg(test)]
    tests::after_root(original_expiry);
    clock(original_expiry)?;
    let expected = object_path(root, native_object_key);
    clock(original_expiry)?;
    let expected = expected.map_err(Native)?;
    let matches = exact(object, &expected);
    clock(original_expiry)?;
    Ok(matches)
}

#[cfg(test)]
#[path = "tier_recovery_paths_tests.rs"]
mod tests;
