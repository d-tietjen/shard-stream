use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash};

use hashbrown::HashTable;
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

use crate::{DEFAULT_SEGMENT_CAPACITY, DenseOrderedMap};

#[derive(Debug, Clone, Copy)]
struct LookupSlot {
    hash: u64,
    ordinal: u128,
}

#[derive(Debug)]
struct OrderedEntry<K, V> {
    hash: u64,
    key: K,
    value: V,
}

/// Append-ordered map for sparse keys.
///
/// The hash table stores only a precomputed hash and stable ordinal. Keys and
/// values remain in segmented insertion order.
#[derive(Debug)]
pub struct OrderedAppendMap<
    K,
    V,
    S = Xxh3DefaultBuilder,
    const SEGMENT_CAPACITY: usize = DEFAULT_SEGMENT_CAPACITY,
> {
    entries: DenseOrderedMap<OrderedEntry<K, V>, SEGMENT_CAPACITY>,
    lookup: HashTable<LookupSlot>,
    hash_builder: S,
}

impl<K, V, S, const SEGMENT_CAPACITY: usize> OrderedAppendMap<K, V, S, SEGMENT_CAPACITY>
where
    S: BuildHasher,
{
    /// Creates an empty ordered map with an explicit hash builder.
    #[must_use]
    pub fn with_hasher(hash_builder: S) -> Self {
        Self::with_capacity_and_hasher(0, hash_builder)
    }

    /// Creates an empty ordered map with a lookup-table capacity hint.
    #[must_use]
    pub fn with_capacity_and_hasher(capacity: usize, hash_builder: S) -> Self {
        Self {
            entries: DenseOrderedMap::new(0),
            lookup: HashTable::with_capacity(capacity),
            hash_builder,
        }
    }

    /// Number of retained entries.
    #[inline]
    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the map contains no entries.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Inserts a sparse key, preserving its original ordinal when replaced.
    pub fn insert(&mut self, key: K, value: V) -> Option<V>
    where
        K: Eq + Hash,
    {
        let hash = self.hash_builder.hash_one(&key);
        self.insert_prehashed(hash, key, value)
    }

    /// Inserts using a caller-provided hash.
    ///
    /// Callers must use the same hash for subsequent prehashed lookups.
    pub fn insert_prehashed(&mut self, hash: u64, key: K, value: V) -> Option<V>
    where
        K: Eq,
    {
        let existing = {
            let entries = &self.entries;
            self.lookup
                .find(hash, |slot| {
                    entries
                        .get(slot.ordinal)
                        .is_some_and(|entry| entry.key == key)
                })
                .map(|slot| slot.ordinal)
        };
        if let Some(ordinal) = existing {
            let entry = self
                .entries
                .get_mut(ordinal)
                .expect("lookup ordinal references a retained entry");
            return Some(std::mem::replace(&mut entry.value, value));
        }

        let ordinal = self.entries.next_key();
        if self
            .entries
            .push(ordinal, OrderedEntry { hash, key, value })
            .is_err()
        {
            unreachable!("ordered map ordinal is generated internally");
        }
        self.lookup
            .insert_unique(hash, LookupSlot { hash, ordinal }, |slot| slot.hash);
        None
    }

    /// Returns a value for a borrowed sparse key.
    #[must_use]
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.get_prehashed(self.hash_builder.hash_one(key), key)
    }

    /// Returns a value using a caller-provided hash.
    #[must_use]
    pub fn get_prehashed<Q>(&self, hash: u64, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let ordinal = self
            .lookup
            .find(hash, |slot| {
                self.entries
                    .get(slot.ordinal)
                    .is_some_and(|entry| entry.key.borrow() == key)
            })?
            .ordinal;
        self.entries.get(ordinal).map(|entry| &entry.value)
    }

    /// Returns a mutable value for a borrowed sparse key.
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hash_builder.hash_one(key);
        let ordinal = {
            let entries = &self.entries;
            self.lookup
                .find(hash, |slot| {
                    entries
                        .get(slot.ordinal)
                        .is_some_and(|entry| entry.key.borrow() == key)
                })?
                .ordinal
        };
        self.entries.get_mut(ordinal).map(|entry| &mut entry.value)
    }

    /// Removes the oldest retained key and value.
    pub fn pop_front(&mut self) -> Option<(K, V)> {
        let ordinal = self.entries.first_key();
        let hash = self.entries.get(ordinal)?.hash;
        self.lookup
            .find_entry(hash, |slot| slot.ordinal == ordinal)
            .expect("lookup contains the first retained ordinal")
            .remove();
        let (_, entry) = self
            .entries
            .pop_front()
            .expect("first retained entry exists");
        Some((entry.key, entry.value))
    }

    /// Removes at most `count` entries from the insertion-order prefix.
    pub fn truncate_front(&mut self, count: usize) -> usize {
        let mut removed = 0usize;
        while removed < count && self.pop_front().is_some() {
            removed += 1;
        }
        removed
    }

    /// Iterates over sparse keys and values in insertion order.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&K, &V)> + '_ {
        self.entries
            .values()
            .map(|entry| (&entry.key, &entry.value))
    }
}

impl<K, V, S, const SEGMENT_CAPACITY: usize> OrderedAppendMap<K, V, S, SEGMENT_CAPACITY>
where
    S: BuildHasher + Default,
{
    /// Creates an empty ordered map.
    #[must_use]
    pub fn new() -> Self {
        Self::with_hasher(S::default())
    }

    /// Creates an empty ordered map with a lookup-table capacity hint.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, S::default())
    }
}

impl<K, V, S, const SEGMENT_CAPACITY: usize> Default for OrderedAppendMap<K, V, S, SEGMENT_CAPACITY>
where
    S: BuildHasher + Default,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, S, const SEGMENT_CAPACITY: usize> FromIterator<(K, V)>
    for OrderedAppendMap<K, V, S, SEGMENT_CAPACITY>
where
    K: Eq + Hash,
    S: BuildHasher + Default,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (K, V)>,
    {
        let iterator = iter.into_iter();
        let mut map = Self::with_capacity(iterator.size_hint().0);
        map.extend(iterator);
        map
    }
}

impl<K, V, S, const SEGMENT_CAPACITY: usize> Extend<(K, V)>
    for OrderedAppendMap<K, V, S, SEGMENT_CAPACITY>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = (K, V)>,
    {
        for (key, value) in iter {
            self.insert(key, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use xxhash_rust::xxh3::Xxh3DefaultBuilder;

    use super::OrderedAppendMap;

    #[test]
    fn replaces_in_place_and_iterates_in_insertion_order() {
        let mut map = OrderedAppendMap::<String, u32>::new();
        assert_eq!(map.insert("beta".into(), 2), None);
        assert_eq!(map.insert("alpha".into(), 1), None);
        assert_eq!(map.insert("beta".into(), 20), Some(2));

        assert_eq!(map.get("alpha"), Some(&1));
        assert_eq!(map.get("beta"), Some(&20));
        assert_eq!(
            map.iter()
                .map(|(key, value)| (key.as_str(), *value))
                .collect::<Vec<_>>(),
            vec![("beta", 20), ("alpha", 1)]
        );
    }

    #[test]
    fn prehashed_lookup_validates_full_keys_on_collision() {
        let mut map = OrderedAppendMap::<String, u32>::new();
        assert_eq!(map.insert_prehashed(7, "one".into(), 1), None);
        assert_eq!(map.insert_prehashed(7, "two".into(), 2), None);
        assert_eq!(map.get_prehashed(7, "one"), Some(&1));
        assert_eq!(map.get_prehashed(7, "two"), Some(&2));
        assert_eq!(map.get_prehashed(7, "three"), None);
    }

    #[test]
    fn prefix_removal_preserves_remaining_ordinals_and_order() {
        let mut map = OrderedAppendMap::<u64, u64, Xxh3DefaultBuilder, 2>::new();
        for key in 0..6 {
            map.insert(key, key * 10);
        }
        assert_eq!(map.truncate_front(3), 3);
        assert_eq!(map.get(&2), None);
        assert_eq!(map.get(&3), Some(&30));
        assert_eq!(
            map.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
    }
}
