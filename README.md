# shard-stream

`shard-stream` is an experimental shard-striped durable stream engine. Its
central design is that successive append batches for one logical partition are
placed round-robin across independently owned shard lanes while consumers
retain an ordered logical view.

The repository is independent from
[`shard-kv`](https://github.com/d-tietjen/shard-kv). Selected implementation
patterns will be copied and adapted with attribution, but storage formats,
protocols, releases, and compatibility contracts belong to `shard-stream`.

## Implemented product slice

The repository now contains a runnable, restart-durable broker rather than a
protocol mock:

- stream-native `u128` identifiers, offsets, and request IDs;
- deterministic batch-level round-robin placement across shard-owned workers;
- bounded MPSC admission accounting for queue slots and bytes;
- a checksummed, self-describing extent-pack format that acts as the payload
  WAL;
- a separate checksummed coordinator journal for durable reservations,
  commit/abort decisions, ring revisions, and producer sequence recovery;
- ordered cross-shard fetch and an explicit completion-order fetch mode;
- idempotent producer epoch and sequence validation using BLAKE3 content
  identity;
- synchronous local replica sets, quorum acknowledgement, replica catch-up,
  and replicated high watermarks;
- stream-native Blossom lease claims, finality-certificate validation,
  persistent leader epochs, and an engine write-fencing hook;
- immutable BLAKE3-verified object-tier packs with atomic manifest generations;
- durable, monotonic, batch-aligned retention and safe sealed-pack reclamation;
- a versioned binary batch response format with CRC validation;
- REST, standard gRPC/HTTP2 streaming, and optional REST-compatible HTTP/3
  listeners;
- a conservative Kafka wire adapter for topic creation, metadata, produce,
  fetch, offset lookup, and API negotiation;
- an OpenAPI document, Prometheus endpoint, and Rust client;
- Docker and Compose packaging; and
- unit, restart-recovery, corruption, retention, object-tier, quorum, catch-up,
  and gRPC interoperability tests.

This is still pre-production. Replica copies currently model the replication
contract in one process and on one host; distributed replica transport,
The concrete Blossom network/validator adapter, distributed replica transport,
remote object-storage providers, Kafka consumer groups/transactions, and the
broader Kafka administrative surface are not complete yet. The HTTP/3 adapter
is experimental and disables TLS 0-RTT to prevent mutation replay. The server
defaults to replication factor one so it never implies multi-node HA.

The on-disk format has no SHA compatibility mode: immutable content identity
uses BLAKE3 exclusively. CRC32 frame checks remain a separate, non-identity
corruption-detection mechanism.

The reviewed architecture and performance contract are in
[`docs/SHARD_STREAM_PLAN.md`](docs/SHARD_STREAM_PLAN.md).
The executable benchmark and gate workflow is documented in
[`docs/BENCHMARKING.md`](docs/BENCHMARKING.md).

## Build

```shell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
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

Create a topic and append one raw native batch:

```shell
curl -X POST \
  -H 'content-type: application/json' \
  -d '{"topic_id":"42","partitions":1}' \
  http://127.0.0.1:7420/v1/topics

curl -X POST --data-binary @batch.bin \
  'http://127.0.0.1:7420/v1/topics/42/partitions/0/records?record_count=1&durability=leader'
```

Fetch responses use
`application/vnd.shard-stream.batch.v1`; the Rust client decodes this framing.
See [`openapi/shard-stream-v1.json`](openapi/shard-stream-v1.json) for the REST
surface and [`proto/shardstream/v1/stream.proto`](proto/shardstream/v1/stream.proto)
for the gRPC service contract. Enable HTTP/3 with `--h3-listen`,
`--h3-certificate`, and `--h3-private-key`; it exposes the native append/fetch
paths over QUIC.

The Kafka listener maps names to stable BLAKE3-derived native topic IDs and
advertises only the APIs and versions it implements. See
[`docs/KAFKA_COMPATIBILITY.md`](docs/KAFKA_COMPATIBILITY.md) for the exact
contract and current exclusions.

## Workspace

- `shard-stream-core`: identifiers, sequencing, placement, and bounded
  execution primitives.
- `shard-stream-control`: Blossom-finalized leases and write fencing.
- `shard-stream-storage`: extent packs, coordinator journal, and object tier.
- `shard-stream-engine`: shard workers, recovery, ordering, and replication.
- `shard-stream-protocol`: transport-independent and binary wire types.
- `shard-stream-client`: async native Rust producer and consumer.
- `shard-stream-grpc`: standard gRPC producer and consumer streaming.
- `shard-stream-h3`: native REST-compatible HTTP/3 data path.
- `shard-stream-kafka`: Kafka wire compatibility and record-batch translation.
- `shard-stream-server`: runnable multi-protocol broker.
- `shard-stream-bench`: local append-throughput harness.
- `proto/shardstream/v1`: versioned native service schema.

## License

Apache-2.0. See [`UPSTREAM.md`](UPSTREAM.md) for reused-source tracking.
