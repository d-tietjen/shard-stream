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

The local harness is useful for regression detection. Comparative Kafka,
Redpanda, AutoMQ, WarpStream, and other market results require the hardware,
durability, warmup, duration, client, filesystem, network, and failure
controls from the plan. Never compare this single-process harness to a
multi-node competitor result without normalizing those conditions.
