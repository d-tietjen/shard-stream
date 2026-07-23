#!/usr/bin/env bash
set -euo pipefail

compose_file="${SHARD_STREAM_COMPOSE_FILE:-compose.multi-node.yaml}"
topic_id="${SHARD_STREAM_SMOKE_TOPIC_ID:-$(date +%s)}"
partitions="${SHARD_STREAM_SMOKE_PARTITIONS:-24}"

if [[ "${SHARD_STREAM_SKIP_COMPOSE:-0}" != "1" ]]; then
  docker compose -f "${compose_file}" up --build --detach --wait
fi

curl --fail --silent --show-error \
  -H 'content-type: application/json' \
  -d "{\"topic_id\":\"${topic_id}\",\"partitions\":${partitions}}" \
  http://127.0.0.1:17420/v1/topics >/dev/null

for ((partition = 0; partition < partitions; partition++)); do
  curl --fail --silent --show-error \
    --data-binary "partition-${partition}" \
    "http://127.0.0.1:17420/v1/topics/${topic_id}/partitions/${partition}/records?record_count=1&durability=leader" \
    >/dev/null
done

for ((partition = 0; partition < partitions; partition++)); do
  batch_count="$(
    curl --fail --silent --show-error --dump-header - --output /dev/null \
      "http://127.0.0.1:37420/v1/topics/${topic_id}/partitions/${partition}/records?offset=0&max_bytes=1048576" \
      | tr -d '\r' \
      | awk -F ': ' 'tolower($1) == "x-shard-stream-batch-count" { print $2 }'
  )"
  if [[ "${batch_count}" != "1" ]]; then
    echo "partition ${partition} returned ${batch_count:-no} batches" >&2
    exit 1
  fi
done

curl --fail --silent --show-error http://127.0.0.1:17420/v1/topology
echo
for port in 17420 27420 37420; do
  curl --fail --silent --show-error "http://127.0.0.1:${port}/metrics" \
    | awk '/shard_stream_appended_batches_total/ { print }'
done
