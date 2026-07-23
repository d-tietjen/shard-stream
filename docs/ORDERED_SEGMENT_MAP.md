# Ordered segment map

`ordered-segment-map` is a standalone MIT-licensed crate for the append-ordered
indexes used by shard-stream. It is independently implemented for dense stream
identifiers rather than copied from `IndexMap`.

## Data layout

`DenseOrderedMap<T>` stores dense monotonic `u128` keys in fixed-capacity
segments:

```text
key  ── subtract retained base ── quotient/remainder ──► segment + slot

segments
├── [batch 1024 … batch 2047]
├── [batch 2048 … batch 3071]
└── [batch 3072 … next]
```

No hash is calculated and no comparison tree is traversed. Segments are
append-only. Retention drops complete front segments and drains at most one
bounded partial segment. Logical ordinals remain stable.

`OrderedAppendMap<K, V>` is the sparse-key companion. A
`hashbrown::HashTable` stores only an XXH3 hash and stable ordinal; full keys
and values remain in insertion order in the segmented store. Full-key equality
is always checked after a hash match. Sparse prefix removal also preserves the
remaining ordinals.

Arbitrary middle removal is intentionally not provided. Stream retention is a
prefix operation, and supporting arbitrary holes would make direct indexing,
iteration, or memory reclamation more expensive.

## Complexity

| Operation | Dense map | Sparse ordered map |
|---|---:|---:|
| Append | amortized O(1) | average O(1) |
| Exact lookup | O(1), no hash | average O(1) |
| Ordered iteration | O(k) | O(k) |
| Start from known batch ID | O(1) | average O(1) |
| Prefix retention | O(segments + one bounded drain) | O(removed entries) |

Logical record offsets are variable-width ranges, so an arbitrary offset uses
binary search over dense batch metadata. Sequential consumer cursors advance
to the next batch ordinal directly.

## Engine integration

The coordinator's former `BTreeMap<LogicalOffset, BatchRuntime>` is now a
`DenseOrderedMap<BatchRuntime>` indexed by dense `BatchId`. Completion and
replica acknowledgements therefore update an exact batch with arithmetic O(1)
lookup. Watermark and committed-range scans start at a binary-searched batch
and then iterate contiguous metadata.

The fetch snapshot uses a dense bitset instead of allocating a hash set of
committed batch IDs. Storage offset vectors use `slice::partition_point` rather
than scanning from the first retained extent, and ordered results use a
shard-count heap merge instead of a global batch sort.

## Adam calibration — 2026-07-23

The standalone benchmark used one million `u128` keys, four million randomized
exact lookups, eight million ordered iteration visits, and three repetitions
pinned to Adam CPUs 16–31. Values below are medians.

| Implementation | Insert ns/op | Lookup ns/op | Ordered iteration ns/item |
|---|---:|---:|---:|
| `DenseOrderedMap` | 10.423 | 14.562 | 1.098 |
| `OrderedAppendMap` | 138.756 | 130.446 | 1.903 |
| `IndexMap` | 81.313 | 137.020 | 1.324 |
| `std::HashMap` | 79.718 | 92.165 | not measured |
| `BTreeMap` | 113.182 | 192.575 | 4.412 |

For the dense batch-ID workload, direct lookup was 6.3x faster than
`std::HashMap`, 9.4x faster than `IndexMap`, and 13.2x faster than `BTreeMap`.
Dense insertion was 7.6x, 7.8x, and 10.9x faster respectively. Dense ordered
iteration was 20.6% faster than `IndexMap` and 4.0x faster than `BTreeMap`.

Removing a 500,000-entry prefix took a median 13.491 microseconds in the dense
map, versus 10.443 milliseconds for `IndexMap::drain` and 2.286 milliseconds
for `BTreeMap::split_off`.

The sparse map is retained for identities that cannot use ordinal arithmetic.
Its median lookup was modestly faster than `IndexMap`, but insertion and
iteration were slower. It is not used for dense coordinator batches.

An interleaved A-B-B-A RF1 tmpfs engine comparison used 16,384 256-KiB batches,
eight shards, concurrency 64, and the previous optimized binary as baseline:

| Metric | Previous index | Dense index/fetch path | Change |
|---|---:|---:|---:|
| Throughput | 8,941.98 MiB/s | 9,191.90 MiB/s | +2.80% |
| p50 | 1.600 ms | 1.598 ms | -0.15% |
| p95 | 3.329 ms | 3.003 ms | -9.79% |
| p99 | 4.725 ms | 3.804 ms | -19.47% |

Same-binary throughput spread was 9.65% for the baseline and 6.43% for the
candidate, both inside the 10% calibration gate. This is an in-process RF1
tmpfs result, not a networked or durable-device product claim.
