# Benchmarking

The public benchmark measures one RF1 engine. It never creates followers or
reports failover durability.

Run the default workload:

```shell
cargo run --release -p shard-stream-bench -- \
  --batches 10000 \
  --payload-bytes 262144 \
  --records-per-batch 256 \
  --partitions 16 \
  --shards 8 \
  --concurrency 32 \
  --durability leader
```

`leader`, `quorum`, and `all-replicas` are useful compatibility test cases but
are the same local RF1 durable boundary in this distribution. `object` also
waits for immutable object-manifest publication.

The result is JSON and records RF1/min-ISR 1, workload dimensions, throughput,
latency percentiles, the final durable sync, and optional pass/fail gates.
Never compare runs without recording the filesystem, hardware, batch size,
record count, concurrency, shard count, and durability spelling.

The harness also accepts `--sink-mode none|noop|slow|failed`.
`--sink-delay-us` controls the slow callback. Failed mode uses availability
policy so it measures bounded notification dropping rather than intentionally
blocking forever at the lag limit. Sink backlog, checkpoint age, retries,
failures, and dirty partitions are included in the JSON.

Run the five-pair regression gate against the checked-out `origin/main` with:

```shell
./bench/run-durable-sink-bench.sh
```

The median disabled path must retain at least 98% of main throughput and stay
within 5% of main p99 append latency. The enabled no-op sink must retain at
least 95% of throughput and stay within 10% of p99. Override workload sizes
with the `SHARD_STREAM_DURABLE_SINK_BENCH_*` environment variables.

Run the shard-count matrix with:

```shell
./bench/run-bench-matrix.sh
```

Linux hosts can capture machine metadata and repeated RF1 calibration with:

```shell
./bench/run-linux-calibration.sh
```

Historical multi-node and HA benchmark assets are not part of this public
distribution.
