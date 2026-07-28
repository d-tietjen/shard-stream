# ShardLog release and deployment

ShardLog is the standalone embedded durable-log crate in
`crates/shardlog`. It owns the extent-pack, recovery, checkpoint, and local
garbage-collection implementation used by shard-stream and compatible embedded
applications.

The crates.io package and Rust crate are both named `shardlog`; the public
product and primary type are `ShardLog`.

## Package boundary

`shardlog` has no dependency on another shard-stream crate. Durable-log
identifiers and append reservations live in ShardLog so downstream
applications do not need shard-stream's broker, protocol, or coordination
graph.

`shard-stream-core` re-exports the shared identifier types, and
`shard-stream-storage` remains a non-published compatibility facade for
existing internal imports. The published engine depends directly on
`shardlog`; new external consumers must do the same.

## Release gate

Run the checked-in gate from a clean shard-stream checkout:

```sh
scripts/shardlog-release-gate.sh
```

The gate checks formatting, the full workspace dependency direction,
ShardLog's tests and lints, warnings-as-errors documentation, and the exact
crate archive Cargo would upload. It creates a package locally but does not
publish it.

Before the `0.1.0` publication:

1. confirm `main` and `origin/main` identify the approved commit;
2. run `scripts/shardlog-release-gate.sh` on Rust 1.93;
3. inspect `cargo package --locked -p shardlog --list`;
4. create and push an annotated `shardlog-crates-v0.1.0` tag without moving
   the existing `v0.1.0` product tag;
5. publish with `cargo publish --locked -p shardlog`; and
6. verify the registry package and docs.rs build before updating consumers.

Publishing is intentionally a separate manual action. The release gate never
uploads to a registry.

## Consumer rollout

ShardLog is embedded and has no independently deployed process. Deployment
means rolling out a consumer binary linked to an approved ShardLog version.

For each shard-stream or embedded-application rollout:

1. stop the old process cleanly and retain a filesystem-level backup or volume
   snapshot of its data directory;
2. verify no second process owns the directory;
3. start one canary using the same directory and wait for recovery validation;
4. exercise an acknowledged append, restart, replay, checkpoint, and bounded
   reclamation;
5. compare durability and recovery metrics with the previous binary; and
6. expand only after the canary remains healthy.

Existing shard-stream extent packs remain byte-for-byte compatible because the
extraction does not change the `SSE8` extent or `SSJ3` journal formats.

## Rollback

For this extraction, rollback is a binary rollback:

1. stop the new consumer;
2. preserve the data directory before attempting recovery;
3. restore the previous binary against the same directory; and
4. if recovery rejects committed history, fail closed and restore the
   pre-rollout snapshot rather than deleting or truncating files manually.

Future format changes must introduce a new readable format version and document
their downgrade boundary here before release. Pack deletion is asynchronous;
failed cleanup is safe to retry and must not be treated as committed-history
loss.
