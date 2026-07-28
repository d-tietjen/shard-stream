use std::collections::{BTreeMap, HashMap};
use std::hint::black_box;
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use ordered_segment_map::{DenseOrderedMap, OrderedAppendMap};

const DEFAULT_ENTRIES: usize = 1_000_000;
const LOOKUP_ROUNDS: usize = 4;
const ITERATION_ROUNDS: usize = 8;

fn main() {
    let entries = std::env::var("ORDERED_SEGMENT_BENCH_ENTRIES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_ENTRIES);
    assert!(entries > 0, "benchmark entry count must be nonzero");
    let keys = lookup_keys(entries);

    bench_insert(entries);
    bench_lookup(entries, &keys);
    bench_iteration(entries);
    bench_prefix_retention(entries);
}

fn bench_insert(entries: usize) {
    report(
        "insert",
        "dense_ordered_map",
        entries,
        elapsed(|| {
            let mut map = DenseOrderedMap::<u64>::new(0);
            for key in 0..entries {
                map.push(key as u128, key as u64).expect("dense insert");
            }
            black_box(map.len());
        }),
    );
    report(
        "insert",
        "ordered_append_map",
        entries,
        elapsed(|| {
            let mut map = OrderedAppendMap::<u128, u64>::with_capacity(entries);
            for key in 0..entries {
                map.insert(key as u128, key as u64);
            }
            black_box(map.len());
        }),
    );
    report(
        "insert",
        "indexmap",
        entries,
        elapsed(|| {
            let mut map = IndexMap::with_capacity(entries);
            for key in 0..entries {
                map.insert(key as u128, key as u64);
            }
            black_box(map.len());
        }),
    );
    report(
        "insert",
        "std_hash_map",
        entries,
        elapsed(|| {
            let mut map = HashMap::with_capacity(entries);
            for key in 0..entries {
                map.insert(key as u128, key as u64);
            }
            black_box(map.len());
        }),
    );
    report(
        "insert",
        "btree_map",
        entries,
        elapsed(|| {
            let mut map = BTreeMap::new();
            for key in 0..entries {
                map.insert(key as u128, key as u64);
            }
            black_box(map.len());
        }),
    );
}

fn bench_lookup(entries: usize, keys: &[u128]) {
    let operations = keys.len();

    let mut dense = DenseOrderedMap::<u64>::new(0);
    for key in 0..entries {
        dense.push(key as u128, key as u64).expect("dense insert");
    }
    report(
        "lookup",
        "dense_ordered_map",
        operations,
        elapsed(|| {
            let mut sum = 0u64;
            for key in keys {
                sum = sum.wrapping_add(*dense.get(*key).expect("dense key"));
            }
            black_box(sum);
        }),
    );
    drop(dense);

    let mut ordered = OrderedAppendMap::<u128, u64>::new();
    for key in 0..entries {
        ordered.insert(key as u128, key as u64);
    }
    report(
        "lookup",
        "ordered_append_map",
        operations,
        elapsed(|| {
            let mut sum = 0u64;
            for key in keys {
                sum = sum.wrapping_add(*ordered.get(key).expect("ordered key"));
            }
            black_box(sum);
        }),
    );
    drop(ordered);

    let index = (0..entries)
        .map(|key| (key as u128, key as u64))
        .collect::<IndexMap<_, _>>();
    report(
        "lookup",
        "indexmap",
        operations,
        elapsed(|| {
            let mut sum = 0u64;
            for key in keys {
                sum = sum.wrapping_add(*index.get(key).expect("index key"));
            }
            black_box(sum);
        }),
    );
    drop(index);

    let hash = (0..entries)
        .map(|key| (key as u128, key as u64))
        .collect::<HashMap<_, _>>();
    report(
        "lookup",
        "std_hash_map",
        operations,
        elapsed(|| {
            let mut sum = 0u64;
            for key in keys {
                sum = sum.wrapping_add(*hash.get(key).expect("hash key"));
            }
            black_box(sum);
        }),
    );
    drop(hash);

    let tree = (0..entries)
        .map(|key| (key as u128, key as u64))
        .collect::<BTreeMap<_, _>>();
    report(
        "lookup",
        "btree_map",
        operations,
        elapsed(|| {
            let mut sum = 0u64;
            for key in keys {
                sum = sum.wrapping_add(*tree.get(key).expect("tree key"));
            }
            black_box(sum);
        }),
    );
}

fn bench_iteration(entries: usize) {
    let operations = entries.saturating_mul(ITERATION_ROUNDS);

    let mut dense = DenseOrderedMap::<u64>::new(0);
    for key in 0..entries {
        dense.push(key as u128, key as u64).expect("dense insert");
    }
    report(
        "ordered_iteration",
        "dense_ordered_map",
        operations,
        elapsed(|| {
            let mut sum = 0u64;
            for _ in 0..ITERATION_ROUNDS {
                for value in dense.values() {
                    sum = sum.wrapping_add(*value);
                }
            }
            black_box(sum);
        }),
    );
    drop(dense);

    let ordered = (0..entries)
        .map(|key| (key as u128, key as u64))
        .collect::<OrderedAppendMap<_, _>>();
    report(
        "ordered_iteration",
        "ordered_append_map",
        operations,
        elapsed(|| {
            let mut sum = 0u64;
            for _ in 0..ITERATION_ROUNDS {
                for (_, value) in ordered.iter() {
                    sum = sum.wrapping_add(*value);
                }
            }
            black_box(sum);
        }),
    );
    drop(ordered);

    let index = (0..entries)
        .map(|key| (key as u128, key as u64))
        .collect::<IndexMap<_, _>>();
    report(
        "ordered_iteration",
        "indexmap",
        operations,
        elapsed(|| {
            let mut sum = 0u64;
            for _ in 0..ITERATION_ROUNDS {
                for value in index.values() {
                    sum = sum.wrapping_add(*value);
                }
            }
            black_box(sum);
        }),
    );
    drop(index);

    let tree = (0..entries)
        .map(|key| (key as u128, key as u64))
        .collect::<BTreeMap<_, _>>();
    report(
        "ordered_iteration",
        "btree_map",
        operations,
        elapsed(|| {
            let mut sum = 0u64;
            for _ in 0..ITERATION_ROUNDS {
                for value in tree.values() {
                    sum = sum.wrapping_add(*value);
                }
            }
            black_box(sum);
        }),
    );
}

fn bench_prefix_retention(entries: usize) {
    let retained = entries / 2;

    let mut dense = DenseOrderedMap::<u64>::new(0);
    for key in 0..entries {
        dense.push(key as u128, key as u64).expect("dense insert");
    }
    report(
        "prefix_retention",
        "dense_ordered_map",
        retained,
        elapsed(|| {
            black_box(dense.truncate_before(retained as u128));
        }),
    );

    let mut ordered = (0..entries)
        .map(|key| (key as u128, key as u64))
        .collect::<OrderedAppendMap<_, _>>();
    report(
        "prefix_retention",
        "ordered_append_map",
        retained,
        elapsed(|| {
            black_box(ordered.truncate_front(retained));
        }),
    );

    let mut index = (0..entries)
        .map(|key| (key as u128, key as u64))
        .collect::<IndexMap<_, _>>();
    report(
        "prefix_retention",
        "indexmap_drain",
        retained,
        elapsed(|| {
            drop(index.drain(..retained));
            black_box(index.len());
        }),
    );

    let mut tree = (0..entries)
        .map(|key| (key as u128, key as u64))
        .collect::<BTreeMap<_, _>>();
    report(
        "prefix_retention",
        "btree_split_off",
        retained,
        elapsed(|| {
            let retained_tree = tree.split_off(&(retained as u128));
            black_box(retained_tree.len());
        }),
    );
}

fn lookup_keys(entries: usize) -> Vec<u128> {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut keys = Vec::with_capacity(entries.saturating_mul(LOOKUP_ROUNDS));
    for _ in 0..entries.saturating_mul(LOOKUP_ROUNDS) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        keys.push((state as usize % entries) as u128);
    }
    keys
}

fn elapsed(run: impl FnOnce()) -> Duration {
    let started = Instant::now();
    run();
    started.elapsed()
}

fn report(workload: &str, implementation: &str, operations: usize, duration: Duration) {
    let seconds = duration.as_secs_f64();
    let ns_per_operation = duration.as_nanos() as f64 / operations.max(1) as f64;
    let million_operations_per_second = operations as f64 / seconds / 1_000_000.0;
    println!(
        "{{\"workload\":\"{workload}\",\"implementation\":\"{implementation}\",\
         \"operations\":{operations},\"seconds\":{seconds:.9},\
         \"ns_per_operation\":{ns_per_operation:.3},\
         \"million_operations_per_second\":{million_operations_per_second:.3}}}"
    );
}
