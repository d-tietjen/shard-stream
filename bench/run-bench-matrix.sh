#!/usr/bin/env bash
set -euo pipefail

results_dir="${1:-bench/results}"
matrix_batches="${SHARD_STREAM_BENCH_BATCHES:-10000}"
matrix_concurrency="${SHARD_STREAM_BENCH_CONCURRENCY:-32}"
matrix_payload_bytes="${SHARD_STREAM_BENCH_PAYLOAD_BYTES:-262144}"
matrix_records_per_batch="${SHARD_STREAM_BENCH_RECORDS_PER_BATCH:-256}"

mkdir -p "${results_dir}"

for matrix_shards in 1 2 4 8 16; do
  cargo run --release --quiet -p shard-stream-bench -- \
    --batches "${matrix_batches}" \
    --concurrency "${matrix_concurrency}" \
    --payload-bytes "${matrix_payload_bytes}" \
    --records-per-batch "${matrix_records_per_batch}" \
    --shards "${matrix_shards}" \
    > "${results_dir}/leader-${matrix_shards}-shards.json"
done
