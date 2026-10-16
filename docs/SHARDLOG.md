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
Object-only floor observation alone does **not** restore an engine catalog.
The explicit consuming restore path below rehydrates verified CURRENT objects
before the native primary scan. Existing unowned constructors retain their
behavior.

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


### Consuming owned engine and object-only native restoration

`NativeRecoveryCensus::read_for_engine` performs the same original bounded
census, opening the actual coordinator descriptor writable without mutating it.
It retains configured paths and normalizes the primary path to the actual
anchored root only after admission. `restore_storage` then admits the complete
CURRENT-object/primary conflict scan, file/handle/path/plan/scratch values,
copy/read/hash/write passes, native store-open costs and repeated final source
validation **before** publishing any payload. The latter is a closed exact
prepayment from actual source sizes, consumed solely by that same validation
pass; it creates no new allowance or refunded work.

The operation accepts only the actual retained CURRENT manifest and objects
already validated by native framing, BLAKE3, length and exact native extent
coverage. Older manifests, orphan objects, primary/pending records and raw
incomplete tails remain observations; they are not selected as substitutes.
Every recognizable primary/pending copy of a CURRENT sequence must match its
actual bytes/hash/tail. A conflict refuses before primary repair or GC and is
never overwritten. Missing objects are copied from their pinned native source
into an anchored exclusive temporary, hashed while copying, synchronized and
published with descriptor-relative atomic `linkat` that cannot replace an
existing name. After validating and synchronizing that final link, the exact
known temporary name is unlinked and the same parent synchronized again.
No unknown/source/final name is deleted. A failed or ambiguous cleanup retains
its actual descriptor and refuses, without claiming no mutation. This path
enables no unknown temporary or orphan pruning. Ancestor/file identities are checked; this is cooperating
custody, not protection against malicious filesystem replacement.

Missing primary directories and native lock files are created and synchronized
under the SAME actual root and native leases. If the highest remaining primary
would be a CURRENT object, a real empty native successor is created before
primary discovery so later normal append cannot modify an immutable object.
Sequence MAX without a successor refuses before payload mutation; it never
wraps to zero. Parent directories, output files and publication directories
are synchronized. Original current/unknown bytes are not fabricated or reset.
The caller must already have durably preserved full raw lineage/floors before
this mutable handoff; the storage API cannot attest that private startup step.

`StreamEngine::open_recovered_storage` consumes the opaque storage rather than
accepting maps or reopening/relocking stores. It uses the actual already-open
primary/tier instances, the pinned writable coordinator descriptor and the
normal native codecs, partition recovery, catalog, workers and fetch path.
The original `JournalEvent` enum is re-exported for native-profile joins; it is
not a second model or decoder. An opaque `OwnedRecoveredEngine` permits only a
borrow of its normal engine. Original observations, root/tier handles and the
SAME leases remain retained through errors, unwind and physical worker cleanup.
After actual engine construction and worker setup have settled successfully,
a final allocation-free handoff retains complete ordinary source/frame/manifest/
journal/raw-tail facts plus root/tier/native guards, while closing retired
payload snapshots and known copy descriptors. They must not pin unlinked pack
bytes beyond the native GC/capacity account. Error/unwind before completed setup retains the original snapshot/copy handles
until physical cleanup. If E elapses while the definite final retirement itself
settles, refusal instead retains the completed opaque engine, ordinary facts
and all guards; it never exposes that engine as successful startup.
The shard continuation duplicates its SAME open-file-description lease; the
tier continuation shares its actual unlocking value, avoiding both reacquisition
and early explicit unlock. The ordinary constructors and unowned paths keep
their existing behavior.

Startup admission includes actual native channel slots, catalog copies,
manifest/offload clone and JSON/error bounds, file hashes, coordinator maps and
dense-segment floors. Synthetic recovered abort slots are bounded from actual
borrowed journal checkpoints and retained batch gaps, with their quadratic
sizing visits prepaid before native recovery. Ordered legacy `Truncate` events
clear an earlier checkpoint's sequencer floor, exactly as native recovery does;
sizing includes the aborts native recovery builds before final truncation.
Component callbacks keep their existing external contracts; their arbitrary
allocations/CPU are not claimed as native logical usage.

The owned continuation carries the SAME absolute E into retained primary
cleanup/discovery/frame-scan and coordinator scan/repair, catalog and native
recovery/abort loops, tier CURRENT/object verification, chunked hashing/copy, conditional manifest publication, each pack
offload and worker/channel start. A late first sink callback cannot start later
callbacks or dispatchers. Control/coordinator starts also check that same E.
Owned GC carries E through its real queue and checks again after dequeue,
before each later pack and metadata/rename/dirsync/unlink phase. A late refusal
can leave an actual native `.gc-pending` tombstone; it never claims unchanged
storage, and normal native recovery retains its existing cleanup policy.
A started kernel call or callback may settle late; it is never cancelled or
detached to manufacture success, and no later independent work starts after
observed expiry. Ordinary constructors, offload, GC and sink policies keep their
existing behavior. The new owned path joins staged workers, including optional
sink workers, through physical cleanup. Ordinary sink destruction remains
bounded as before. A late construction refuses without granting a new timeout.

These are custody/storage APIs. They do not authenticate signed assignment,
lease/currentness, receipt keys or payload authority, consume an issuer start,
write PREPARED/COMMITTED, manufacture a fence, or enable a server/follower.
The closed consumer still must join actual full signed history, route aliases,
metadata/AA child bodies, conservative local/native maxima and durable evidence
under its original retained control Session before operational activation.
Native tier storage is in scope; Pluribus and Mooncake are excluded.

The new source fixtures use an actual native engine object ACK/offload, real
missing-primary/whole-shard filesystems, the consuming path and normal native
fetch/append/restart. Other source regressions cover conflicts/pending bytes,
source/name races, all five exact storage caps and one-less boundaries, original
expiry, real failed/unwinding component cleanup with blocked Drop, raw partial
tails/opaque files, native MAX sequence and anchored no-replacement publication.
The GC regression also exercises actual restore, native truncate/GC and ordinary
restart; on Linux it checks the actual device/inode against `/proc/self/fd` to
exclude both hidden links and retired payload descriptors after real unlink.
They have not been compiled or executed here; source review and formatting are
not runtime, restoration or serving qualification.

Additional source regressions use genuine ordered checkpoint/legacy-truncate
journal events and native pack recovery with exact/one-less admission. Actual
first-pack publication, CURRENT verification, sink opening and GC rename are
allowed to settle past E; observations require no later publication/callback/
worker/pack phase. A real recovered worker queue barrier separately verifies
that queue residence never renews E and leaves native bytes unchanged when the
command expires before native GC begins. These remain source fixtures until
actual compilation and execution; no operational startup claim follows.

Owned primary discovery and pending-deletion cleanup check the same E before
and after every actual directory `next`, including EOF and error visits.
Owned tier-copy refusal leaves failed staging names as evidence. It never
unlinks a predicted temporary name when source open or exclusive creation
failed, nor a replacement at that name after rename. This grants no permission
to prune unknown staging objects; ordinary native helpers remain unchanged.
The additional source regressions use actual native packs, late directory
visits/EOF, genuine stale-name collisions, source-open loss and a real
post-rename replacement. They remain uncompiled and unexecuted.
