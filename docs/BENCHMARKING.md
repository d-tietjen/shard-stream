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

Run the standard shard-scaling matrix with:

```shell
./bench/run-bench-matrix.sh bench/results
```

The first single-node Linux calibration must sweep:

- 1, 2, 4, 8, and 16 shards at RF1/leader durability and RF3/minimum-ISR-2
  quorum durability;
- 100 B, 1 KiB, 10 KiB, and 1 MiB records, with 32 KiB, 64 KiB, 256 KiB,
  and 1 MiB target batches where the record size permits;
- concurrency from 1 through saturation, reporting the full throughput/latency
  curve rather than only its peak;
- uncompressed, LZ4, and Zstandard Kafka RecordBatch profiles in the wire
  benchmark; and
- producer linger of 0, 1, 5, 20, and 50 ms plus matching consumer fetch-size
  sweeps in the protocol benchmark.

Record payload rate, message rate, batch rate, CPU, disk bandwidth/IOPS, and
network traffic separately. Before interpreting a ceiling, record NVMe
sequential throughput and fsync latency, NIC speed, filesystem/mount options,
CPU governor, TLS/SASL state, and whether reads are served by page cache. A
same-process or same-host RF3 run measures code-path overhead only and must not
be described as fault-tolerant replication.

The local harness is useful for regression detection. Comparative Kafka,
Redpanda, AutoMQ, WarpStream, and other market results require the hardware,
durability, warmup, duration, client, filesystem, network, and failure
controls from the plan. Never compare this single-process harness to a
multi-node competitor result without normalizing those conditions.
