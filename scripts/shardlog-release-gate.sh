#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

cd -- "$repo_root"

cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo test --locked -p shardlog
cargo clippy --locked -p shardlog --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc --locked -p shardlog --no-deps
cargo package --locked -p shardlog

printf 'ShardLog package is release-ready; no registry upload was performed.\n'
