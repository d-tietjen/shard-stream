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
