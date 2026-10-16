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

## Owned pre-open observations on Unix

`DirectoryOwnership::root_handle` produces opaque descriptor-relative handles.
Every literal child retains its actual parent chain and the same root owner;
validation checks each parent entry with `NOFOLLOW` against its opened inode.
File handles expose borrowed parent/name facts so a caller can admit path
copies before allocating them. These are cooperating storage custody, not
process identity, history authentication or write permission. They do not
prevent a noncooperating actor from replacing paths after a validation.

`NativeRecoveryCensus::read` inspects storage before ordinary native repair,
pending-deletion cleanup, workers, offload or GC. It retains the original
absolute `Instant`, cumulative file/entry/read/declared-buffer/work allowances,
actual root handles and nonblocking native shard/UPDATE leases. Its bounded
reader reuses the native SSE8/SSJ3 decoders and verifies complete frames; exact
incomplete tails remain observations rather than silently repaired bytes.
It visits recognizable retained physical shard directories, including shards
outside the current ring count. Current lane-to-shard mapping is recorded; the
SSE8 decoder does not encode a physical-shard equality check.

The census retains all native manifest generations, CURRENT and recognizable
referenced/unreferenced object files. CURRENT objects must match their actual
length, BLAKE3 and native extent coverage. Retained manifest claims also raise
the conservative observed epoch maximum, even when CURRENT is older; that is
not proof that the claimed payload remains available. SSJ3 coordinator events
contain no leader term. Missing paths are explicit absences, never evidence of
unused storage or permission to choose epoch zero. Signed history, metadata
journals, historical routing and authority joins remain the consumer's work.

Native `offload_pack` now holds the real UPDATE lease through hash/copy,
verification and conditional manifest publication. Publication uses that same
lease without reacquiring it. This serializes cooperating writers implementing
this protocol. Older or external publication-only writers can still create
objects outside it. CURRENT plus its referenced immutable objects is a captured
snapshot, not an all-version claim of root quiescence or an orphan deletion
permission. Files are revalidated before custody handoff; sources stay pinned
until physical destruction, including on refusal.

After the caller has durably preserved required raw observations and floors,
`open_storage` consumes the same concrete leases into native store openers.
Original observations then describe pre-open bytes, since normal native open
may repair tails or remove pending deletions. The returned storage retains the
original owners. It starts no engine/worker/sink and grants no serving rights.
In particular, object-only floor observation does **not** implement operational
object-only restore: the existing engine catalog/fetch path remains primary
based, and real tier-backed fetch or rehydration is still required before such
a consumer can serve. Existing unowned constructors retain their behavior.

Allowances are finite cumulative logical admissions, not RSS/allocator or CPU
metering. They cover native directory buffers, file opens/duplicates, parser
shapes, hashes and repeated reads. Filesystem calls remain synchronous; an
expiry observed after physical I/O settles refuses the result without dropping
its owners early. This API does not claim cancellation of a blocked kernel call
or that an observed payload/manifest grants authenticated recovery authority.

The mutable handoff admits the entire configured native-open set before its
first store call, including an empty root's new locks/packs/directories, fixed
native states, output slots and path copies. It additionally admits repeated
pack/index scans and CURRENT's native manifest parse/object-hash scratch from
the captured facts. These conservative logical costs remain distinct from
actual allocator accounting. A short admission leaves the native directories
uncreated. JSON shape inspection debits the original cumulative entry counter
before every visit, including unknown values, under the same original Instant;
it never starts a second per-parser node allowance.

The native-open allowance also includes measured complete parent/name path bytes
for every primary-directory visit before filename filtering. Both native
cleanup and pack discovery materialize those paths, even for ignored names;
their copied paths, iterator values and pending/map slots are reserved before
mutation. Those generated paths obey the same explicit path ceiling. The
long-root fixture uses real ignored files and an interrupted native pack to
check fitting admission and that one-less capacity does not repair its tail or
create a new shard.
