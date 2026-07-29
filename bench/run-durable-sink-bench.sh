#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
results_dir="${1:-$repo_root/bench/results/durable-sink}"
bench_batches="${SHARD_STREAM_DURABLE_SINK_BENCH_BATCHES:-5000}"
bench_concurrency="${SHARD_STREAM_DURABLE_SINK_BENCH_CONCURRENCY:-32}"
bench_payload_bytes="${SHARD_STREAM_DURABLE_SINK_BENCH_PAYLOAD_BYTES:-65536}"
bench_records_per_batch="${SHARD_STREAM_DURABLE_SINK_BENCH_RECORDS_PER_BATCH:-64}"
bench_partitions="${SHARD_STREAM_DURABLE_SINK_BENCH_PARTITIONS:-16}"
bench_shards="${SHARD_STREAM_DURABLE_SINK_BENCH_SHARDS:-8}"

command -v jq >/dev/null || {
  echo "durable sink benchmark requires jq" >&2
  exit 1
}
git -C "$repo_root" rev-parse --verify origin/main >/dev/null

scratch="$(mktemp -d "${TMPDIR:-/tmp}/shard-stream-durable-sink-bench.XXXXXX")"
baseline_source="$scratch/main"
baseline_target="$scratch/main-target"
feature_target="$scratch/feature-target"
mkdir -p "$baseline_source" "$results_dir"
trap 'rm -rf "$scratch"' EXIT

git -C "$repo_root" archive origin/main | tar -x -C "$baseline_source"

(
  cd "$baseline_source"
  CARGO_TARGET_DIR="$baseline_target" cargo build --release --quiet -p shard-stream-bench
)
(
  cd "$repo_root"
  CARGO_TARGET_DIR="$feature_target" cargo build --release --quiet -p shard-stream-bench
)

common_args=(
  --batches "$bench_batches"
  --concurrency "$bench_concurrency"
  --payload-bytes "$bench_payload_bytes"
  --records-per-batch "$bench_records_per_batch"
  --partitions "$bench_partitions"
  --shards "$bench_shards"
)

run_main() {
  local run="$1"
  "$baseline_target/release/shard-stream-bench" "${common_args[@]}" \
    > "$results_dir/main-$run.json"
}

run_none() {
  local run="$1"
  "$feature_target/release/shard-stream-bench" "${common_args[@]}" \
    --sink-mode none \
    > "$results_dir/none-$run.json"
}

run_noop() {
  local run="$1"
  "$feature_target/release/shard-stream-bench" "${common_args[@]}" \
    --sink-mode noop \
    > "$results_dir/noop-$run.json"
}

for run in 1 2 3 4 5; do
  case "$((run % 3))" in
    1)
      run_main "$run"
      run_none "$run"
      run_noop "$run"
      ;;
    2)
      run_none "$run"
      run_noop "$run"
      run_main "$run"
      ;;
    0)
      run_noop "$run"
      run_main "$run"
      run_none "$run"
      ;;
  esac
done

median_field() {
  local field="$1"
  shift
  jq -s --arg field "$field" \
    'map(getpath($field | split("."))) | sort | .[length / 2 | floor]' "$@"
}

main_throughput="$(median_field mebibytes_per_second "$results_dir"/main-*.json)"
none_throughput="$(median_field mebibytes_per_second "$results_dir"/none-*.json)"
noop_throughput="$(median_field mebibytes_per_second "$results_dir"/noop-*.json)"
main_p99="$(median_field latency.p99_ms "$results_dir"/main-*.json)"
none_p99="$(median_field latency.p99_ms "$results_dir"/none-*.json)"
noop_p99="$(median_field latency.p99_ms "$results_dir"/noop-*.json)"

jq -n \
  --argjson main_throughput "$main_throughput" \
  --argjson none_throughput "$none_throughput" \
  --argjson noop_throughput "$noop_throughput" \
  --argjson main_p99 "$main_p99" \
  --argjson none_p99 "$none_p99" \
  --argjson noop_p99 "$noop_p99" \
  '{
    runs: 5,
    main: {
      median_mib_per_second: $main_throughput,
      median_p99_ms: $main_p99
    },
    no_sink: {
      median_mib_per_second: $none_throughput,
      throughput_ratio: ($none_throughput / $main_throughput),
      median_p99_ms: $none_p99,
      p99_ratio: ($none_p99 / $main_p99),
      passed: (
        $none_throughput >= $main_throughput * 0.98
        and $none_p99 <= $main_p99 * 1.05
      )
    },
    noop_sink: {
      median_mib_per_second: $noop_throughput,
      throughput_ratio: ($noop_throughput / $main_throughput),
      median_p99_ms: $noop_p99,
      p99_ratio: ($noop_p99 / $main_p99),
      passed: (
        $noop_throughput >= $main_throughput * 0.95
        and $noop_p99 <= $main_p99 * 1.10
      )
    }
  }' | tee "$results_dir/summary.json"

jq -e '.no_sink.passed and .noop_sink.passed' \
  "$results_dir/summary.json" >/dev/null || {
  echo "durable sink benchmark regression gate failed" >&2
  exit 1
}
