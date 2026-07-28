#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
  echo "publish-artifact check failed: $*" >&2
  exit 1
}

release_packages=(
  ordered-segment-map
  shardlog
  shard-stream-core
  shard-stream-protocol
  shard-stream-engine
  shard-stream-client
  shard-stream-runtime
  shard-stream-grpc
  shard-stream-h3
  shard-stream-kafka
  shard-stream-server
)

package_args=(--locked)
if test "${SHARD_STREAM_ALLOW_DIRTY_PACKAGES:-0}" = "1"; then
  package_args+=(--allow-dirty)
fi
if test "${SHARD_STREAM_OFFLINE_PACKAGES:-0}" = "1"; then
  package_args+=(--offline)
fi

metadata_file="$(mktemp "${TMPDIR:-/tmp}/shard-stream-release-metadata.XXXXXX")"
package_lists="$(mktemp -d "${TMPDIR:-/tmp}/shard-stream-package-lists.XXXXXX")"
trap 'rm -f "$metadata_file"; rm -rf "$package_lists"' EXIT

cargo metadata --locked --no-deps --format-version 1 > "$metadata_file"
release_version="$(
  jq -r '.packages[] | select(.name == "shard-stream-server") | .version' \
    "$metadata_file"
)"
test -n "$release_version" && test "$release_version" != "null" \
  || fail "could not resolve the workspace release version"

is_release_package() {
  local candidate="$1"
  local package
  for package in "${release_packages[@]}"; do
    if test "$candidate" = "$package"; then
      return 0
    fi
  done
  return 1
}

for package in "${release_packages[@]}"; do
  manifest="$(
    jq -r --arg package "$package" \
      '.packages[] | select(.name == $package) | .manifest_path' \
      "$metadata_file"
  )"
  test -n "$manifest" && test "$manifest" != "null" \
    || fail "release package is missing from the workspace: $package"

  package_version="$(
    jq -r --arg package "$package" \
      '.packages[] | select(.name == $package) | .version' \
      "$metadata_file"
  )"
  test "$package_version" = "$release_version" \
    || fail "$package has version $package_version instead of $release_version"

  publish_policy="$(
    jq -c --arg package "$package" \
      '.packages[] | select(.name == $package) | .publish' \
      "$metadata_file"
  )"
  test "$publish_policy" != "[]" \
    || fail "$package is marked publish = false"

  while IFS=$'\t' read -r dependency requirement; do
    test -n "$dependency" || continue
    if ! is_release_package "$dependency"; then
      fail "$package has a path dependency on non-published package $dependency"
    fi
    case "$requirement" in
      *"$release_version"*) ;;
      *) fail "$package path dependency $dependency does not require $release_version" ;;
    esac
  done < <(
    jq -r --arg package "$package" \
      '.packages[]
       | select(.name == $package)
       | .dependencies[]
       | select(.path != null)
       | [.name, .req]
       | @tsv' \
      "$metadata_file"
  )

  cargo package "${package_args[@]}" --list -p "$package" \
    > "$package_lists/$package"
  grep -Fxq 'LICENSE' "$package_lists/$package" \
    || fail "$package archive does not include its license text"
done

grep -Fxq 'proto/shardstream/v1/stream.proto' \
  "$package_lists/shard-stream-grpc" \
  || fail "the gRPC package does not contain its protobuf schema"
grep -Fxq 'openapi/shard-stream-v1.json' \
  "$package_lists/shard-stream-server" \
  || fail "the server package does not contain its OpenAPI document"
grep -Fxq 'src/main.rs' "$package_lists/shard-stream-server" \
  || fail "the server package does not contain the public binary"

for private_package in shard-stream-storage shard-stream-bench; do
  publish_policy="$(
    jq -c --arg package "$private_package" \
      '.packages[] | select(.name == $package) | .publish' \
      "$metadata_file"
  )"
  test "$publish_policy" = "[]" \
    || fail "$private_package must remain non-published"
done

# These leaf packages have no unpublished shard-stream dependencies, so Cargo
# can build their exact registry archives before the staged release starts.
cargo package "${package_args[@]}" -p ordered-segment-map
cargo package "${package_args[@]}" -p shardlog

printf 'Public package manifests and archives are ready for shard-stream %s.\n' \
  "$release_version"
printf 'Publish in the dependency order documented in docs/RELEASING.md.\n'
