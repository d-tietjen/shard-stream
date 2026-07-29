# shard-stream

`shard-stream` is a single-node, RF1 durable streaming engine. It stripes
successive append batches across independently owned shard lanes while exposing
one ordered logical stream to producers and consumers.

This public distribution includes:

- restart-durable extent-pack storage with checksums and partial-tail recovery;
- bounded, shard-owned append queues and grouped shard-local fsync;
- ordered and completion-order fetch;
- idempotent producer epochs, sequences, and stable event IDs;
- partition-scoped atomic append groups and durable retention checkpoints;
- an optional crash-safe, partition-checkpointed durable append sink for
  embedded compositions;
- immutable object-tier packs;
- REST, gRPC/HTTP2, optional HTTP/3, and an optional conservative Kafka
  protocol adapter;
- a Rust client with stable logical topic IDs and generation-aware cursors; and
- compile-time extension contracts for assignment, replication transport,
  write fencing, retention, lifecycle, routes, readiness, and background tasks.

The shipped `shard-stream-server` binary is deliberately single-node. Its
replication factor and minimum ISR are always `1`; it does not provide
failover, follower replication, active-active operation, membership,
interbroker transport, or topic-generation transitions.

## Durability contract

The public server accepts the native durability spellings `leader`, `quorum`,
and `all_replicas`, plus Kafka `acks=1` and `acks=all`. Because the server is
RF1, all of those spellings resolve to the same local durable boundary and
never claim replica or failover guarantees. Object durability additionally
requires `--object-store-dir`.

Logical topic bindings remain compatible with generation-aware clients:
logical and physical topic IDs are identical, policy and data generation are
both `1`, and the mode is represented as single-writer
`active_passive`. Existing RF1 directories open in place. A directory
containing RF>1 policy, transitioned generations, active-active state, or
private HA metadata fails startup with `SHARD_STREAM_PRIVATE_REQUIRED`.

## Build and test

```shell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo check -p shard-stream-server --no-default-features
# Runs local TCP/UDP interoperability tests for gRPC, Kafka, and HTTP/3.
cargo test --workspace -- --ignored
./scripts/check-public-surface.sh
```

The `shard-stream-server` crate enables its `kafka` feature by default to
preserve the standard multi-protocol binary. Build a native REST/gRPC/HTTP3
server without the Kafka dependency, listener, CLI flags, metrics, or
diagnostic route with:

```shell
cargo build -p shard-stream-server --no-default-features
```

## Run

```shell
cargo run -p shard-stream-server -- \
  --listen 127.0.0.1:7420 \
  --grpc-listen 127.0.0.1:7421 \
  --kafka-listen 127.0.0.1:9092 \
  --data-dir ./var/shard-stream \
  --shards 4
```

Set `SHARD_STREAM_ADMIN_TOKEN` to require the
`x-shard-stream-admin-token` header for topic creation, ring changes, and
retention administration. Without a token those local administrative routes
are available.

Create a topic and append one opaque native batch:

```shell
curl -X POST \
  -H 'content-type: application/json' \
  -d '{"topic_id":"42","partitions":1}' \
  http://127.0.0.1:7420/v1/topics

printf 'hello' > /tmp/shard-stream-batch.bin
curl -X POST --data-binary @/tmp/shard-stream-batch.bin \
  'http://127.0.0.1:7420/v1/topics/42/partitions/0/records?record_count=1&durability=leader'
```

The complete REST contract is in
[`crates/shard-stream-server/openapi/shard-stream-v1.json`](crates/shard-stream-server/openapi/shard-stream-v1.json). See
[`docs/KAFKA_COMPATIBILITY.md`](docs/KAFKA_COMPATIBILITY.md),
[`docs/EMBEDDED_RUNTIME.md`](docs/EMBEDDED_RUNTIME.md), and
[`docs/EXTENSIONS.md`](docs/EXTENSIONS.md) for protocol and embedding details.

## Configuration

| Variable | Default | Effect |
| --- | ---: | --- |
| `SHARD_STREAM_MAX_BATCH_BYTES` | 4 MiB | Maximum encoded append batch |
| `SHARD_STREAM_MAX_REQUEST_BYTES` | 32 MiB | Maximum protocol request |
| `SHARD_STREAM_MAX_FETCH_BYTES` | 64 MiB | Maximum fetch response |
| `SHARD_STREAM_APPEND_LINGER_MS` | 2 ms | Maximum shard-local group-fsync wait |
| `SHARD_STREAM_MAX_IN_FLIGHT_BATCHES` | 4096 | Queue slots per physical shard |
| `SHARD_STREAM_LANE_BUFFER_BYTES` | 512 MiB | Queue byte bound per physical shard |
| `SHARD_STREAM_VIRTUAL_LANES` | physical shard count | Stable logical append lanes |
| `SHARD_STREAM_KAFKA_PROFILE_SAMPLE_STRIDE` | 0 | Kafka request profiling stride when the `kafka` feature is enabled |
| `SHARD_STREAM_ADMIN_TOKEN` | unset | Optional local administration credential |

The Rust client also reads `SHARD_STREAM_BATCH_SIZE`,
`SHARD_STREAM_LINGER_MS`, and `SHARD_STREAM_MAX_REQUEST_SIZE`.
See [`.env.example`](.env.example) for a complete local example.

## Workspace

- `shard-stream-core`: identifiers and neutral extension traits.
- `shardlog`: the published durable extent and journal storage engine.
- `shard-stream-storage`: a non-published compatibility facade for internal
  source compatibility.
- `shard-stream-engine`: RF1 storage, ordering, and generic durability hooks.
- `shard-stream-runtime`: listener-free embedded lifecycle composition.
- `shard-stream-protocol` and `shard-stream-client`: stable public contracts.
- `shard-stream-grpc`, `shard-stream-h3`, and `shard-stream-kafka`: protocol
  adapters.
- `shard-stream-server`: the RF1 multi-protocol binary and extension builder.
- `shard-stream-bench`: the RF1 benchmark harness.

Public crate packaging and dependency order are documented in
[`docs/RELEASING.md`](docs/RELEASING.md).

## License

The shard-stream product is Apache-2.0. The standalone
`ordered-segment-map` crate is MIT licensed. See [`UPSTREAM.md`](UPSTREAM.md)
for reused-source tracking.
