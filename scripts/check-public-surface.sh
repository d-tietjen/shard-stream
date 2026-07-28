#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
  echo "public-source guard failed: $*" >&2
  exit 1
}

search_files() {
  local pattern="$1"
  shift
  if command -v rg >/dev/null 2>&1; then
    rg -n --hidden --glob '!target/**' --glob '!.git/**' \
      --glob '!scripts/check-public-surface.sh' "$pattern" "$@"
  else
    grep -ERn --exclude-dir=target --exclude-dir=.git \
      --exclude=check-public-surface.sh -- "$pattern" "$@"
  fi
}

search_stdin() {
  local pattern="$1"
  if command -v rg >/dev/null 2>&1; then
    rg -n "$pattern"
  else
    grep -En -- "$pattern"
  fi
}

search_quiet() {
  local pattern="$1"
  shift
  if command -v rg >/dev/null 2>&1; then
    rg -q "$pattern" "$@"
  else
    grep -Eq -- "$pattern" "$@"
  fi
}

reject_match() {
  local description="$1"
  local pattern="$2"
  shift 2
  if search_files "$pattern" "$@"; then
    fail "$description"
  fi
}

for removed_path in \
  crates/shard-stream-control \
  crates/shard-stream-coordination \
  crates/shard-stream-metadata \
  crates/shard-stream-replication \
  compose.multi-node.yaml \
  simulation-run.json \
  simulation/profiles/ha-upgrade.json \
  scripts/generate-dev-mtls.sh \
  scripts/test-multi-node.sh \
  scripts/test-multi-process-recovery.sh
do
  if test -d "$removed_path"; then
    if find "$removed_path" -type f -print -quit | grep -q .; then
      fail "private implementation or deployment asset remains: $removed_path"
    fi
  elif test -e "$removed_path"; then
    fail "private implementation or deployment asset remains: $removed_path"
  fi
done

reject_match \
  "Blossom must not be referenced by public source or dependency metadata" \
  '(^|[^[:alnum:]_])([Bb][Ll][Oo][Ss][Ss][Oo][Mm])([^[:alnum:]_]|$)' \
  Cargo.toml Cargo.lock crates proto scripts .github

reject_match \
  "private crate imports or dependencies must not enter the public workspace" \
  '(shard_stream_private|shard-stream-private\s*=)' \
  Cargo.toml Cargo.lock crates proto scripts .github

reject_match \
  "private implementation packages must not enter public dependency metadata" \
  'shard-stream-(control|coordination|metadata|replication)([[:space:]]|=|"|\})' \
  Cargo.toml Cargo.lock crates

if sed '/^#\[cfg(test)\]/,$d' crates/shard-stream-server/src/main.rs \
  | search_stdin '(/v1/ha|/ha/|/internal/v1/(replicas|metadata)|active_active|dynamic_ha|cluster_token|control_listen|replication_listen)'
then
  fail "the public server exposes HA, replication, metadata-quorum, or interbroker routes"
fi

if sed '/^#\[cfg(test)\]/,$d' crates/shard-stream-server/src/main.rs \
  | search_stdin '(cluster_nodes|cluster_token|replication_listen|replication_queue|replica_admission|control_listen|control_tls)'
then
  fail "the public server exposes multi-node configuration flags"
fi

test -s crates/shard-stream-core/src/fencing.rs \
  || fail "neutral WriteFence contract is missing"
test -s crates/shard-stream-protocol/src/logical.rs \
  || fail "generation-aware public cursor contract is missing"
test -s crates/shard-stream-protocol/src/routing.rs \
  || fail "stable logical-topic resolver contract is missing"
test -s crates/shard-stream-server/src/lib.rs \
  || fail "ServerBuilder/ServerExtension contract is missing"
test -s crates/shard-stream-grpc/proto/shardstream/v1/stream.proto \
  || fail "the packaged gRPC schema is missing"
test -s crates/shard-stream-server/openapi/shard-stream-v1.json \
  || fail "the packaged OpenAPI document is missing"

if ! search_quiet '^default = \["kafka"\]$' crates/shard-stream-server/Cargo.toml; then
  fail "public server must preserve Kafka compatibility as a default feature"
fi
if ! search_quiet '^kafka = \["dep:shard-stream-kafka"\]$' crates/shard-stream-server/Cargo.toml; then
  fail "public server Kafka feature does not own the adapter dependency"
fi
if ! search_quiet 'shard-stream-kafka = \{ workspace = true, optional = true \}' \
  crates/shard-stream-server/Cargo.toml
then
  fail "public server Kafka adapter dependency is not optional"
fi

if ! search_quiet '"replication_factor": 1' crates/shard-stream-server/src/main.rs; then
  fail "public server does not explicitly report RF1"
fi
if ! search_quiet '"min_in_sync_replicas": 1' crates/shard-stream-server/src/main.rs; then
  fail "public server does not explicitly report min ISR 1"
fi

echo "public-source guard passed"
