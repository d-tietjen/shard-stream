use std::collections::VecDeque;
use std::fmt;

use crate::DEFAULT_SEGMENT_CAPACITY;

#[derive(Debug)]
struct Segment<T> {
    first_key: u128,
    values: VecDeque<T>,
}

impl<T> Segment<T> {
    fn end_key(&self) -> u128 {
        self.first_key + self.values.len() as u128
    }
}

/// Failure returned when a dense insertion is not the next expected key.
#[derive(Debug, PartialEq, Eq)]
pub enum DenseInsertError<T> {
    /// The supplied key would create a gap or overwrite an existing entry.
    OutOfOrder {
        /// Next key accepted by the map.
        expected: u128,
        /// Key supplied by the caller.
        actual: u128,
        /// Value that was not inserted.
        value: T,
    },
    /// Inserting the key would exhaust the `u128` key space.
    KeySpaceExhausted {
        /// Value that was not inserted.
        value: T,
    },
    /// The process cannot represent another live entry in `usize`.
    CapacityExhausted {
        /// Value that was not inserted.
        value: T,
    },
}

impl<T> DenseInsertError<T> {
    /// Returns ownership of the value that was not inserted.
    pub fn into_value(self) -> T {
        match self {
            Self::OutOfOrder { value, .. }
            | Self::KeySpaceExhausted { value }
            | Self::CapacityExhausted { value } => value,
        }
    }
}

impl<T> fmt::Display for DenseInsertError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfOrder {
                expected, actual, ..
            } => write!(
                formatter,
                "dense key {actual} is out of order; expected {expected}"
            ),
            Self::KeySpaceExhausted { .. } => formatter.write_str("dense key space exhausted"),
            Self::CapacityExhausted { .. } => formatter.write_str("dense index capacity exhausted"),
        }
    }
}

impl<T: fmt::Debug> std::error::Error for DenseInsertError<T> {}

/// Segmented insertion-ordered storage for dense monotonic `u128` keys.
///
/// Exact lookup is arithmetic and does not hash. Prefix truncation drops whole
/// segments and performs at most one bounded partial-segment drain.
#[derive(Debug)]
pub struct DenseOrderedMap<T, const SEGMENT_CAPACITY: usize = DEFAULT_SEGMENT_CAPACITY> {
    first_key: u128,
    next_key: u128,
    len: usize,
    segments: VecDeque<Segment<T>>,
}

impl<T, const SEGMENT_CAPACITY: usize> DenseOrderedMap<T, SEGMENT_CAPACITY> {
    /// Creates an empty map whose first insertion must use `first_key`.
    #[must_use]
    pub fn new(first_key: u128) -> Self {
        assert!(
            SEGMENT_CAPACITY > 0,
            "segment capacity must be greater than zero"
        );
        Self {
            first_key,
            next_key: first_key,
            len: 0,
            segments: VecDeque::new(),
        }
    }

    /// Number of retained entries.
    #[inline]
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the map contains no retained entries.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// First retained key, or the next insertion key when empty.
    #[inline]
    #[must_use]
    pub const fn first_key(&self) -> u128 {
        self.first_key
    }

    /// Key required by the next insertion.
    #[inline]
    #[must_use]
    pub const fn next_key(&self) -> u128 {
        self.next_key
    }

    /// Appends the next dense key and value.
    pub fn push(&mut self, key: u128, value: T) -> Result<(), DenseInsertError<T>> {
        if key != self.next_key {
            return Err(DenseInsertError::OutOfOrder {
                expected: self.next_key,
                actual: key,
                value,
            });
        }
        let Some(next_key) = key.checked_add(1) else {
            return Err(DenseInsertError::KeySpaceExhausted { value });
        };
        let Some(next_len) = self.len.checked_add(1) else {
            return Err(DenseInsertError::CapacityExhausted { value });
        };

        let segment_number = key / SEGMENT_CAPACITY as u128;
        match self.segments.back_mut() {
            Some(segment) if segment.first_key / SEGMENT_CAPACITY as u128 == segment_number => {
                debug_assert_eq!(segment.end_key(), key);
                segment.values.push_back(value);
            }
            _ => {
                let mut values = VecDeque::with_capacity(SEGMENT_CAPACITY);
                values.push_back(value);
                self.segments.push_back(Segment {
                    first_key: key,
                    values,
                });
            }
        }
        self.next_key = next_key;
        self.len = next_len;
        Ok(())
    }

    /// Returns a shared reference for an exact dense key.
    #[inline]
    #[must_use]
    pub fn get(&self, key: u128) -> Option<&T> {
        let (segment_index, value_index) = self.position(key)?;
        self.segments.get(segment_index)?.values.get(value_index)
    }

    /// Returns a mutable reference for an exact dense key.
    #[inline]
    pub fn get_mut(&mut self, key: u128) -> Option<&mut T> {
        let (segment_index, value_index) = self.position(key)?;
        self.segments
            .get_mut(segment_index)?
            .values
            .get_mut(value_index)
    }

    /// Returns whether an exact dense key is retained.
    #[inline]
    #[must_use]
    pub fn contains_key(&self, key: u128) -> bool {
        self.position(key).is_some()
    }

    /// Removes and returns the first retained entry.
    pub fn pop_front(&mut self) -> Option<(u128, T)> {
        let key = self.first_key;
        let segment = self.segments.front_mut()?;
        let value = segment.values.pop_front()?;
        segment.first_key += 1;
        self.first_key += 1;
        self.len -= 1;
        if segment.values.is_empty() {
            self.segments.pop_front();
        }
        if self.len == 0 {
            self.first_key = self.next_key;
        }
        Some((key, value))
    }

    /// Removes every retained entry with a key below `first_retained`.
    ///
    /// Returns the number of removed entries.
    pub fn truncate_before(&mut self, first_retained: u128) -> usize {
        let target = first_retained.clamp(self.first_key, self.next_key);
        let mut removed = 0usize;

        while self
            .segments
            .front()
            .is_some_and(|segment| segment.end_key() <= target)
        {
            let segment = self.segments.pop_front().expect("front segment exists");
            removed += segment.values.len();
            self.len -= segment.values.len();
            self.first_key = segment.end_key();
        }

        if let Some(segment) = self.segments.front_mut()
            && segment.first_key < target
        {
            let count =
                usize::try_from(target - segment.first_key).expect("target lies in one segment");
            segment.values.drain(..count);
            segment.first_key = target;
            removed += count;
            self.len -= count;
            self.first_key = target;
        }

        if self.len == 0 {
            self.first_key = self.next_key;
        }
        removed
    }

    /// Iterates over keys and values in insertion order.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (u128, &T)> + '_ {
        self.segments.iter().flat_map(|segment| {
            segment
                .values
                .iter()
                .enumerate()
                .map(move |(index, value)| (segment.first_key + index as u128, value))
        })
    }

    /// Iterates mutably over keys and values in insertion order.
    pub fn iter_mut(&mut self) -> impl DoubleEndedIterator<Item = (u128, &mut T)> + '_ {
        self.segments.iter_mut().flat_map(|segment| {
            let first_key = segment.first_key;
            segment
                .values
                .iter_mut()
                .enumerate()
                .map(move |(index, value)| (first_key + index as u128, value))
        })
    }

    /// Iterates over retained values in insertion order.
    pub fn values(&self) -> impl DoubleEndedIterator<Item = &T> + '_ {
        self.segments
            .iter()
            .flat_map(|segment| segment.values.iter())
    }

    /// Iterates mutably over retained values in insertion order.
    pub fn values_mut(&mut self) -> impl DoubleEndedIterator<Item = &mut T> + '_ {
        self.segments
            .iter_mut()
            .flat_map(|segment| segment.values.iter_mut())
    }

    /// Iterates from an exact retained key, or from the next insertion point.
    pub fn iter_from(&self, key: u128) -> impl DoubleEndedIterator<Item = (u128, &T)> + '_ {
        let start_key = key.clamp(self.first_key, self.next_key);
        let (segment_index, value_index) = if start_key == self.next_key {
            (self.segments.len(), 0)
        } else {
            self.position(start_key)
                .expect("clamped non-terminal key is retained")
        };
        self.segments
            .iter()
            .skip(segment_index)
            .enumerate()
            .flat_map(move |(relative_segment, segment)| {
                let skip = if relative_segment == 0 {
                    value_index
                } else {
                    0
                };
                segment
                    .values
                    .iter()
                    .enumerate()
                    .skip(skip)
                    .map(move |(index, value)| (segment.first_key + index as u128, value))
            })
    }

    /// Iterates over values from an exact retained key.
    pub fn values_from(&self, key: u128) -> impl DoubleEndedIterator<Item = &T> + '_ {
        self.iter_from(key).map(|(_, value)| value)
    }

    /// Binary-searches retained values while preserving dense direct indexing.
    pub fn binary_search_by<F>(&self, mut compare: F) -> Result<u128, u128>
    where
        F: FnMut(u128, &T) -> std::cmp::Ordering,
    {
        let mut left = 0usize;
        let mut right = self.len;
        while left < right {
            let middle = left + (right - left) / 2;
            let key = self.first_key + middle as u128;
            let value = self.get(key).expect("middle key is retained");
            match compare(key, value) {
                std::cmp::Ordering::Less => left = middle + 1,
                std::cmp::Ordering::Equal => return Ok(key),
                std::cmp::Ordering::Greater => right = middle,
            }
        }
        Err(self.first_key + left as u128)
    }

    fn position(&self, key: u128) -> Option<(usize, usize)> {
        if key < self.first_key || key >= self.next_key {
            return None;
        }
        let first_segment = self.segments.front()?;
        let segment_number = key / SEGMENT_CAPACITY as u128;
        let first_segment_number = first_segment.first_key / SEGMENT_CAPACITY as u128;
        let segment_index = usize::try_from(segment_number - first_segment_number).ok()?;
        let segment = self.segments.get(segment_index)?;
        let value_index = usize::try_from(key.checked_sub(segment.first_key)?).ok()?;
        (value_index < segment.values.len()).then_some((segment_index, value_index))
    }
}

impl<T, const SEGMENT_CAPACITY: usize> Default for DenseOrderedMap<T, SEGMENT_CAPACITY> {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::{DenseInsertError, DenseOrderedMap};

    #[test]
    fn lookup_and_iteration_cross_segment_boundaries() {
        let mut map = DenseOrderedMap::<_, 4>::new(2);
        for key in 2..12 {
            map.push(key, key * 10).expect("dense insertion");
        }

        assert_eq!(map.get(1), None);
        assert_eq!(map.get(2), Some(&20));
        assert_eq!(map.get(11), Some(&110));
        assert_eq!(
            map.iter()
                .map(|(key, value)| (key, *value))
                .collect::<Vec<_>>(),
            (2..12).map(|key| (key, key * 10)).collect::<Vec<_>>()
        );
    }

    #[test]
    fn rejects_gaps_without_losing_the_value() {
        let mut map = DenseOrderedMap::<_, 4>::new(7);
        let error = map.push(8, "eight").expect_err("gap must fail");
        assert_eq!(
            error,
            DenseInsertError::OutOfOrder {
                expected: 7,
                actual: 8,
                value: "eight",
            }
        );
        assert_eq!(map.next_key(), 7);
    }

    #[test]
    fn prefix_truncation_is_bounded_and_preserves_direct_lookup() {
        let mut map = DenseOrderedMap::<_, 4>::new(0);
        for key in 0..14 {
            map.push(key, key).expect("dense insertion");
        }

        assert_eq!(map.truncate_before(9), 9);
        assert_eq!(map.first_key(), 9);
        assert_eq!(map.get(8), None);
        assert_eq!(map.get(9), Some(&9));
        assert_eq!(
            map.iter().map(|(key, _)| key).collect::<Vec<_>>(),
            (9..14).collect::<Vec<_>>()
        );
        assert_eq!(map.truncate_before(100), 5);
        assert!(map.is_empty());
        assert_eq!(map.first_key(), 14);
        map.push(14, 14).expect("append after complete truncation");
    }

    #[test]
    fn binary_search_returns_key_or_insertion_key() {
        let mut map = DenseOrderedMap::<_, 3>::new(10);
        for key in 10..20 {
            map.push(key, key * 2).expect("dense insertion");
        }
        assert_eq!(map.binary_search_by(|_, value| value.cmp(&28)), Ok(14));
        assert_eq!(
            map.binary_search_by(|_, value| {
                if *value < 29 {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }),
            Err(15)
        );
    }

    #[test]
    fn mutable_iteration_and_pop_front_preserve_keys() {
        let mut map = DenseOrderedMap::<_, 2>::new(5);
        for key in 5..10 {
            map.push(key, key).expect("dense insertion");
        }
        for (_, value) in map.iter_mut() {
            *value += 1;
        }
        assert_eq!(map.pop_front(), Some((5, 6)));
        assert_eq!(map.get(6), Some(&7));
        assert_eq!(map.len(), 4);
    }

    #[test]
    fn iteration_can_start_without_walking_the_retained_prefix() {
        let mut map = DenseOrderedMap::<_, 3>::new(10);
        for key in 10..20 {
            map.push(key, key).expect("dense insertion");
        }
        assert_eq!(
            map.iter_from(16).map(|(key, _)| key).collect::<Vec<_>>(),
            vec![16, 17, 18, 19]
        );
        assert_eq!(map.iter_from(20).next(), None);
    }
}
