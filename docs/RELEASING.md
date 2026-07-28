# Public crate release and private consumption

The public workspace publishes one lockstep version across eleven crates.
`shard-stream-storage` is a source-compatibility facade and
`shard-stream-bench` is an internal harness; neither is published.

## Public release gate

From a clean checkout of the approved public commit, run:

```sh
bash scripts/check-public-surface.sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --no-deps
bash scripts/check-publish-artifacts.sh
```

The artifact check verifies the publish allowlist, lockstep versions, path
dependency closure, and packaged protobuf/OpenAPI inputs. It builds the two
leaf archives but does not upload anything.

For local inspection before the release commit is clean, set
`SHARD_STREAM_ALLOW_DIRTY_PACKAGES=1`. CI and the final release gate must run
without that override. A checkout with a complete Cargo cache can additionally
set `SHARD_STREAM_OFFLINE_PACKAGES=1`; the final gate should use the registry.

## Publish order

Publish one level at a time. Wait for every crate in a level to resolve from
the crates.io index before continuing, because Cargo verifies downstream
packages against registry dependencies rather than workspace paths.

1. `ordered-segment-map`, `shardlog`
2. `shard-stream-core`
3. `shard-stream-protocol`
4. `shard-stream-engine`, `shard-stream-client`
5. `shard-stream-runtime`, `shard-stream-grpc`, `shard-stream-h3`,
   `shard-stream-kafka`
6. `shard-stream-server`

For each crate, run a dry run immediately before its upload:

```sh
cargo publish --locked --dry-run -p CRATE
cargo publish --locked -p CRATE
```

Do not publish the server until both default features and
`--no-default-features` have passed CI. Kafka compatibility is a default
server feature but remains an optional dependency for embedders.

After the final upload, verify docs.rs and install the exact server version in
a clean directory. The existing `v0.1.0` product tag is immutable and predates
crate publication; use a distinct `crates-v0.1.0` tag for the publication
commit. Publishing and tagging are deliberately manual external actions.

## Private update

`shard-stream-private` first validates against the exact public Git commit.
After all public crates are visible on crates.io, its checked-in update helper
can switch the six direct public dependencies to exact registry versions
without changing private package names or the deployed binary name:

```sh
scripts/update-public-dependencies.sh git PUBLIC_COMMIT PUBLIC_VERSION
scripts/update-public-dependencies.sh registry PUBLIC_VERSION
```

Commit the resulting private `Cargo.toml` and `Cargo.lock` together. The
private integration check must pass against the corresponding public checkout
before the private release is tagged.
