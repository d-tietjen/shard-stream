# shard-stream

`shard-stream` is an experimental shard-striped durable stream engine. Its
central design is that successive append batches for one logical partition can
be placed round-robin across independently owned shard lanes while consumers
retain an ordered logical view.

The repository is independent from
[`shard-kv`](https://github.com/d-tietjen/shard-kv). Selected implementation
patterns will be copied and adapted with attribution, but storage formats,
protocols, releases, and compatibility contracts belong to `shard-stream`.

## Status

The repository is in its bootstrap phase and is not ready for production use.
The first code establishes:

- stream-native `u128` identifiers and logical offsets;
- deterministic batch-level round-robin placement;
- checked offset and placement reservation;
- bounded MPSC channels accounting for both slots and bytes;
- shared producer/consumer protocol types; and
- the initial native Protobuf service contract.

The reviewed architecture and performance contract are in
[`docs/SHARD_STREAM_PLAN.md`](docs/SHARD_STREAM_PLAN.md).

## Build

```shell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Workspace

- `shard-stream-core`: identifiers, sequencing, placement, and bounded
  execution primitives.
- `shard-stream-protocol`: transport-independent producer/consumer types.
- `shard-stream-server`: eventual broker binary; currently a bootstrap
  executable.
- `proto/shardstream/v1`: versioned native service schema.

## License

Apache-2.0. See [`UPSTREAM.md`](UPSTREAM.md) for reused-source tracking.
