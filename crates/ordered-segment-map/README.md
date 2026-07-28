# ordered-segment-map

`ordered-segment-map` provides append-only indexes for workloads that need
fast exact lookup and insertion-order iteration without moving retained
entries.

- `DenseOrderedMap<T>` uses arithmetic indexing for dense, monotonic `u128`
  keys. It performs no hashing.
- `OrderedAppendMap<K, V>` combines the dense segmented store with a compact
  `hashbrown::HashTable` from precomputed XXH3 hashes to support sparse keys.

Both structures support prefix removal for log retention. Arbitrary removal is
deliberately excluded because it would either create holes or make ordered
iteration and stable ordinals more expensive.

```rust
use ordered_segment_map::DenseOrderedMap;

let mut batches = DenseOrderedMap::<&str>::new(40);
batches.push(40, "first").expect("append batch 40");
batches.push(41, "second").expect("append batch 41");

assert_eq!(batches.get(41), Some(&"second"));
assert_eq!(
    batches.iter().collect::<Vec<_>>(),
    vec![(40, &"first"), (41, &"second")]
);

batches.truncate_before(41);
assert_eq!(batches.first_key(), 41);
```

Run the comparative benchmark with:

```shell
cargo bench -p ordered-segment-map --bench ordered_index
```

The crate is licensed under the MIT License.
