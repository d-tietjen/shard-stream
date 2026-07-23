# Benchmarking and release gates

The architecture's absolute goals and comparative win criteria are defined in
[`SHARD_STREAM_PLAN.md`](SHARD_STREAM_PLAN.md#15-performance-goals-win-criteria-and-validation).
Raw benchmark output is evidence, not a product claim.

The local harness emits versioned JSON with workload dimensions, payload and
record throughput, append p50/p95/p99/p99.9/max latency, logical watermarks,
and optional pass/fail gates:

```shell
cargo run --release -p shard-stream-bench -- \
  --batches 10000 \
  --records-per-batch 256 \
  --payload-bytes 262144 \
  --shards 8 \
  --concurrency 32 \
  --durability quorum \
  --replication-factor 3 \
  --min-in-sync-replicas 2 \
  --min-mib-per-second 1024 \
  --max-p99-ms 10
```

The process exits unsuccessfully when an explicit gate fails. The default run
has no hidden threshold and therefore cannot be mistaken for a release
qualification.

Benchmark the coordinator's reusable ordered-index data structures separately
with:

```shell
cargo bench -p ordered-segment-map --bench ordered_index
```

This compares dense arithmetic lookup and the sparse prehashed ordered map with
`IndexMap`, `std::HashMap`, and `BTreeMap`. It reports JSON lines for insertion,
random exact lookup, insertion-order iteration, and prefix retention. See
[`ORDERED_SEGMENT_MAP.md`](ORDERED_SEGMENT_MAP.md) for the data layout and the
latest Adam calibration.

An explicit `--data-dir` or `--object-store-dir` must be empty by default so
repeated trials cannot silently append to prior data. Pass `--reuse-data` only
for intentional recovery or log-growth tests. Use `--idempotent-producers` to
measure producer retry-fingerprint and sequence-tracking overhead; each harness
worker then owns an independent producer ID and ordered sequence.

The in-process harness shares one immutable `Bytes` payload among append
requests. This deliberately measures the engine and storage ceiling after
protocol ingress ownership has been established; it excludes socket reads,
protocol decoding, and client-to-broker payload copies. Use the Kafka, REST,
gRPC, or HTTP/3 network harness before making end-to-end copy-cost or throughput
claims.

Measure ordered consumer throughput independently with the checked-in fetch
example:

```shell
cargo run --release -p shard-stream-bench --example fetch -- \
  --batches 16384 \
  --payload-bytes 32768 \
  --shards 8 \
  --fetch-bytes 1048576 \
  --warmup-passes 1 \
  --measured-passes 3 \
  --min-mib-per-second 4096 \
  --max-p99-ms 1
```

The preparation phase creates an RF1 log, then the timed phase repeatedly
consumes it from offset zero and reports fetch, batch, payload, and latency
rates. For profiler runs, pass an explicit `--data-dir` on the preparation run,
then reopen it with the same dimensions and `--reuse-data`. Reuse verifies that
the recovered logical end exactly matches `--batches` before timing reads.

Run the standard shard-scaling matrix with:

```shell
./bench/run-bench-matrix.sh bench/results
```

Run the bounded first-pass Linux shard and concurrency calibration with:

```shell
SHARD_STREAM_BENCH_BATCHES=4096 \
  ./bench/run-linux-calibration.sh bench/results/linux-calibration
```

It records the exact commit and host environment beside per-case JSON and
`/usr/bin/time -v` output. Increase `SHARD_STREAM_BENCH_REPETITIONS` to three
for reportable results; the default single repetition is only a calibration.

The first single-node Linux calibration must sweep:

- 1, 2, 4, 8, and 16 shards at RF1/leader durability and RF3/minimum-ISR-2
  quorum durability;
- 100 B, 1 KiB, 10 KiB, and 1 MiB records, with 16 KiB, 32 KiB, 64 KiB,
  256 KiB, 1 MiB, and 2 MiB target batches where the record size permits;
- concurrency from 1 through saturation, reporting the full throughput/latency
  curve rather than only its peak;
- uncompressed, LZ4, and Zstandard Kafka RecordBatch profiles in the wire
  benchmark; and
- producer linger of 0, 1, 5, 20, and 50 ms plus matching consumer fetch-size
  sweeps in the protocol benchmark.

The 16 KiB profile matches Kafka's default `batch.size`. Kafka's default
`max.request.size` is 1 MiB, so the 2 MiB profile is explicitly tuned and must
raise the producer request cap plus compatible broker and topic limits. A Kafka
request may carry multiple per-partition record batches; request size and
record-batch size must therefore be recorded separately. Shard-stream's server
batch limit is configurable and defaults to 16 MiB.

For native-client comparisons, record `SHARD_STREAM_BATCH_SIZE`,
`SHARD_STREAM_LINGER_MS`, and `SHARD_STREAM_MAX_REQUEST_SIZE`. For broker-side
admission and flush behavior, separately record
`SHARD_STREAM_MAX_BATCH_BYTES`, `SHARD_STREAM_MAX_REQUEST_BYTES`, and
`SHARD_STREAM_APPEND_LINGER_MS`. Do not present broker group-fsync linger as
producer linger; they act at different stages of the append path.

Record payload rate, message rate, batch rate, CPU, disk bandwidth/IOPS, and
network traffic separately. Before interpreting a ceiling, record NVMe
sequential throughput and fsync latency, NIC speed, filesystem/mount options,
CPU governor, TLS/SASL state, and whether reads are served by page cache. A
same-process or same-host RF3 run measures code-path overhead only and must not
be described as fault-tolerant replication.

For storage comparisons, require at least 10% free space, record an idle
`iostat` window before every sample, and use fresh data directories. Interleave
baseline and candidate runs (for example A-B-B-A) instead of running all
baselines first. Treat the comparison as inconclusive if same-binary samples
differ by more than 10% or device throughput degrades monotonically with run
order; do not average away thermal, write-cache, background-workload, or
filesystem-state drift.

The local harness is useful for regression detection. Comparative Kafka,
Redpanda, AutoMQ, WarpStream, and other market results require the hardware,
durability, warmup, duration, client, filesystem, network, and failure
controls from the plan. Never compare this single-process harness to a
multi-node competitor result without normalizing those conditions.
