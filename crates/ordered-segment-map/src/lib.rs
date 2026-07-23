//! Append-ordered segmented indexes.
//!
//! [`DenseOrderedMap`] avoids hashing when keys are dense monotonic ordinals.
//! [`OrderedAppendMap`] adds an IndexMap-style prehashed lookup table for
//! genuinely sparse keys while keeping values in insertion order.

mod dense;
mod sparse;

pub use dense::{DenseInsertError, DenseOrderedMap};
pub use sparse::OrderedAppendMap;

/// Default number of entries retained in one allocation.
pub const DEFAULT_SEGMENT_CAPACITY: usize = 1_024;
