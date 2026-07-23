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
- a self-describing `SSE4` extent-pack WAL with XXH3-64 header-and-payload
  corruption checks;
- hot-path coordination through bounded MPSC-owned partition state with no
  shared global mutex;
- an MIT-licensed `ordered-segment-map` crate providing hash-free O(1) dense
  batch lookup, segmented insertion-order iteration, and bounded prefix
  retention;
- a checksummed control journal for ring revisions and retention checkpoints,
  with no globally ordered per-append fsync;
- ordered cross-shard fetch and an explicit completion-order fetch mode;
- idempotent producer epoch and sequence validation using an XXH3-128 retry
  fingerprint, with no payload hash on non-idempotent appends;
- grouped shard fsync, parallel local replica workers, quorum acknowledgement,
  replica catch-up, and replicated high watermarks;
- stream-native Blossom lease claims, finality-certificate validation,
  persistent leader epochs, and partition-hashed MPSC lease/fencing workers;
- immutable BLAKE3-verified object-tier packs with atomic manifest generations;
- durable, monotonic, batch-aligned retention and safe sealed-pack reclamation;
- a versioned binary batch response format with CRC validation;
- REST, standard gRPC/HTTP2 streaming, and optional REST-compatible HTTP/3
  listeners;
- a conservative Kafka wire adapter for topic creation, metadata, produce,
  fetch, offset lookup, and API negotiation, with incremental
  `bytes-handoff` framing and bounded asynchronous response writes;
- immutable `Bytes` payload ownership from protocol ingress through shard and
  replica dispatch;
- an OpenAPI document, a `fast-telemetry`-backed Prometheus endpoint,
  `eden_logger` lifecycle/error logging, and a Rust client;
- MPSC-owned Kafka topic discovery and a single-owner native producer sequence,
  leaving no shared runtime `Mutex` or `RwLock`;
- Docker and Compose packaging; and
- unit, restart-recovery, corruption, retention, object-tier, quorum, catch-up,
  and gRPC interoperability tests.

This is still pre-production. Replica copies currently model the replication
contract in one process and on one host; the concrete Blossom
network/validator adapter, distributed replica transport, remote object-storage
providers, Kafka consumer groups/transactions, and the broader Kafka
administrative surface are not complete yet. The HTTP/3 adapter is experimental
and disables TLS 0-RTT to prevent mutation replay. The server defaults to
replication factor one so it never implies multi-node HA.

The on-disk format has no SHA compatibility mode. Hot-path idempotent-producer
retry fingerprints use XXH3-128, immutable object-tier content identity uses
BLAKE3, on-disk extent checks use XXH3-64, and control-journal/native-wire
frames retain CRC32 corruption checks.

The reviewed architecture and performance contract are in
[`docs/SHARD_STREAM_PLAN.md`](docs/SHARD_STREAM_PLAN.md).
The implemented lease/fencing boundary and remaining distributed-HA work are
in [`docs/HA.md`](docs/HA.md).
The executable benchmark and gate workflow is documented in
[`docs/BENCHMARKING.md`](docs/BENCHMARKING.md).

## Build

```shell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
# Runs real local TCP/UDP interoperability tests for gRPC, Kafka, and HTTP/3.
cargo test --workspace -- --ignored
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

The broker accepts environment variables for the corresponding deployment
knobs. Explicit command-line flags take precedence:

| Environment variable | Default | Effect |
| --- | ---: | --- |
| `SHARD_STREAM_MAX_BATCH_BYTES` | 16 MiB | Hard limit for one admitted batch |
| `SHARD_STREAM_MAX_REQUEST_BYTES` | 32 MiB | Hard limit for a protocol request, which may contain multiple Kafka batches |
| `SHARD_STREAM_MAX_FETCH_BYTES` | 64 MiB | Hard limit for one fetch response |
| `SHARD_STREAM_APPEND_LINGER_MS` | 1 ms | Maximum adaptive wait for shard-local group fsync; `0` disables waiting |

The native Rust client exposes the Kafka-familiar producer profile through
`ClientConfig::from_env()`:

| Environment variable | Default | Kafka analogue |
| --- | ---: | --- |
| `SHARD_STREAM_BATCH_SIZE` | 16 KiB | `batch.size` |
| `SHARD_STREAM_LINGER_MS` | 5 ms | `linger.ms` |
| `SHARD_STREAM_MAX_REQUEST_SIZE` | 1 MiB | `max.request.size` |

`SHARD_STREAM_BATCH_SIZE` is a soft flush threshold: a single record may exceed
it, but no append may exceed `SHARD_STREAM_MAX_REQUEST_SIZE`. The current native
`Producer::append` API accepts an already assembled batch; applications can
read `Producer::tuning()` and use `ProducerTuning::should_flush()` in their
batching layer. Kafka-protocol applications continue setting `batch.size`,
`linger.ms`, and `max.request.size` directly in their Kafka client.

For a tuned 2 MiB producer batch, use:

```shell
export SHARD_STREAM_BATCH_SIZE=2097152
export SHARD_STREAM_LINGER_MS=20
export SHARD_STREAM_MAX_REQUEST_SIZE=4194304

export SHARD_STREAM_MAX_BATCH_BYTES=2097152
export SHARD_STREAM_MAX_REQUEST_BYTES=4194304
```

Docker Compose reads the broker variables from the shell or a local `.env`
file. [`.env.example`](.env.example) contains the complete producer and broker
set.

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
- `ordered-segment-map`: reusable MIT-licensed dense and sparse append-ordered
  indexes.
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

The shard-stream product is Apache-2.0. The standalone
`ordered-segment-map` crate is MIT licensed. See [`UPSTREAM.md`](UPSTREAM.md)
for reused-source tracking.
