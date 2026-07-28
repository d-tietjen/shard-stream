#!/usr/bin/env bash
set -euo pipefail

results_dir="${1:-bench/results/linux-calibration}"
calibration_batches="${SHARD_STREAM_BENCH_BATCHES:-4096}"
calibration_payload_bytes="${SHARD_STREAM_BENCH_PAYLOAD_BYTES:-262144}"
calibration_records_per_batch="${SHARD_STREAM_BENCH_RECORDS_PER_BATCH:-256}"
calibration_repetitions="${SHARD_STREAM_BENCH_REPETITIONS:-1}"

mkdir -p "${results_dir}"

{
  date --iso-8601=seconds
  uname -a
  cargo --version
  rustc --version
  git rev-parse HEAD
  if command -v lscpu >/dev/null; then
    lscpu
  fi
  if command -v lsblk >/dev/null; then
    lsblk -o NAME,TYPE,SIZE,ROTA,MODEL,FSTYPE,MOUNTPOINTS
  fi
  df -h .
  mount
  if command -v ip >/dev/null; then
    ip -brief link
  fi
} > "${results_dir}/environment.txt"

run_case() {
  local name="$1"
  shift
  /usr/bin/time -v -o "${results_dir}/${name}.time.txt" \
    cargo run --release --quiet -p shard-stream-bench -- \
      --batches "${calibration_batches}" \
      --payload-bytes "${calibration_payload_bytes}" \
      --records-per-batch "${calibration_records_per_batch}" \
      "$@" \
      > "${results_dir}/${name}.json"
}

for repetition in $(seq 1 "${calibration_repetitions}"); do
  for shards in 1 2 4 8 16; do
    run_case \
      "r${repetition}-leader-${shards}shards-c32" \
      --shards "${shards}" \
      --concurrency 32 \
      --durability leader
  done

  for concurrency in 32 64 128 256; do
    run_case \
      "r${repetition}-leader-8shards-c${concurrency}" \
      --shards 8 \
      --concurrency "${concurrency}" \
      --durability leader
    run_case \
      "r${repetition}-all-replicas-alias-8shards-c${concurrency}" \
      --shards 8 \
      --concurrency "${concurrency}" \
      --durability all-replicas
  done
done
