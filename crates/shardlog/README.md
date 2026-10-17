# ShardLog

ShardLog is an embedded, crash-recoverable append log for Rust applications.
It stores checksummed records in immutable extent packs, repairs only
uncommitted torn tails, fails closed on committed corruption, and makes durable
appends visible only after the active pack is synchronized.

ShardLog is the persistence engine used by shard-stream. It is a storage
primitive rather than a consensus protocol or application database.

## Status

The crate is being prepared for its first public release and is not yet
published on crates.io.

## Durability properties

- exclusive ownership of each data directory;
- checksummed records and fail-closed committed-history recovery;
- durable pack creation, rotation, tail repair, and garbage collection;
- grouped prepared appends for atomic higher-level commit protocols; and
- ordered fetch planning over immutable sealed packs and the active pack.

The stable public entry points are `ShardLog`, `ShardLogConfig`,
`PreparedExtent`, and `StoredBatch`.
## Pinned native append and rotation (source implementation)

On Linux and Android, `ShardStore::append_group_pinned_until` and
`seal_active_pinned_until` borrow the actual store and `PinnedShardDirectory`.
The composing owner prepays `pinned_shard_append_cost`, passes one cumulative
`PinnedShardAppendBudget` with its original absolute `Instant`, and retains the
same Core owner, store and pin through report/error cleanup. The mandatory
`PinnedShardAppendChecks::deny` acts on that same operation before failed or late
physical settlement. These ordinary interfaces provide no currentness or
serving authority.

The bounded path shares the native frame lengths, header/producer acceptance,
checksum, group segmentation, location construction, sorted index insertion and
pack-name producer with ordinary append. It admits finite direct write attempts,
retains each actual result before checking time, and reserves rollback/flush/
data-sync before entering a segment. Failed unindexed segments can truncate the
same FD; index-entry refusal instead retains the partial index and physical
frames under denial. An entered flush/data-sync is never retried during failure
settlement. Independent unwind custody keeps a rollback or flush panic from
skipping a still-unentered prepaid data-sync. Direct native data-sync/truncate
calls have no hidden `Interrupted` retry loop.

Rotation uses the actual pinned directory descriptor and CreateNew native pack
name. The old pack is flushed and synced before creation. The actual pack-map,
parent-sync and active-swap order is preserved; replaced old FDs remain in the
report. After an entered CreateNew, a prepaid parent fsync settles even when the
operation is late or unwinds. Unknown existing names are never overwritten or
unlinked. Every refusal sticks to the same budget and composing denial; the
partially modified store must not be reused.

The cost describes checked source-visible values, requested container slots,
retained input/header/name/result capacities and explicit cumulative inner work.
It is not an allocator, kernel or RSS bound. A separate report can be retained
for every allowed invocation, so the cost includes each invocation's requested
report capacities. Genuine filesystem, ordinary-frame differential, rotation,
reopen, partial/late/error/unwind, exact-ceiling and one-less fixtures are source
only here: they have not been compiled or executed. This unit does not implement
the composing Core/ShardLog handoff, higher-level transaction/journal producers,
dependency integration or whole owned-server startup qualification.

The active identity check and strict seal metadata use the native opener's
actual retained pack-map path, including accepted extra leading-zero decimal
spellings. `max_path_bytes` bounds the complete actual stored path before its
component scan; `max_name_bytes` separately bounds its literal basename before
copying or C-name conversion. The cost covers those actual name slots. Newly
created rotation names still come from the same canonical native name producer.

Preparation's topic lookup and each segment's pre-index topic lookup separately
prepay the actual current map-capacity work before probing the map. An earlier
full scan or later sorted-location movement fee does not pay for a point lookup.
Two additional genuine native append/seal/GC sparse-map groups cover an immutable
generous-budget append/reopen and exact/one-less point-fee fault injection. The
test-only hook tightens the same work ceiling without changing spent usage or E;
it is not an originally configured immutable-ceiling or full startup proof. All
19 groups remain source-only, uncompiled and unexecuted.

Index installation separately pays the missing-topic insertion and both native
`install_location` probes (`index.entry` and `by_batch.insert`) from their actual
current capacities before entry. These fees are independent of validation,
contains/search and location-vector movement. Two further real sparse-history
groups cover existing and newly installed topics, immutable generous policies,
and labeled downward-only exact/one-less fee injection with actual written
frames, partial index effects and same-FD settlement retained. All 21 groups
remain source only, uncompiled and unexecuted.

## Consuming canonical log inspection (source implementation)

On Linux and Android, `OpenedPinnedShardStore::into_shard_log_until` consumes
the actual opened native carrier. It returns an `OpenedPinnedShardLog` with the
observed canonical half-open sequence range. A refused conversion retains the
whole original store, validated sequence prefix, last reservation, actual
allocation/canonical results and unwind payloads. Neither result exposes the
original store, pin, raw parts or file descriptors. The caller keeps the genuine
native pin and composing directory owner until all returned values are dropped.

The composing owner prepays `pinned_shard_log_open_cost` before constructing the
supplied cumulative `StorageFetchBudget`. The conversion pays the actual table
capacity scan and selected sequence/sort work before traversal or allocation.
Its canonical stream and contiguity rules are shared with ordinary `ShardLog`.
Actual results are retained before postchecks; refusal denies the same native
counters and mandatory composing checks before releasing the store. Budget and
checkpoint callbacks are borrowed for this conversion and are not stored for
later operations. Costs describe source-visible/requested storage and logical
work, excluding allocator internals, kernel storage and RSS.

Eleven genuine filesystem regression groups cover ordinary bounds/bytes parity,
foreign and gapped history, exact and one-less cumulative fees, sparse table
capacity, late completed bounds, unwind/denial and actual lease custody. They
remain uncompiled and unexecuted. This constructor provides no bounded log
read, append, seal, sync or compaction API, Core handoff, complete recovery or
serving authority; those composed operations require separate implementation
and validation.

The fixed native debit separately names each retained typed result, sequence
header, three panic payload headers, the diagnostic String header and its
separate buffer entry. The plain composing cost and the same inner native
counters use this coherent inventory. An actual sixteen-record history succeeds
with a generous finite policy and refuses an independently configured 22-entry
policy before catalog allocation or inspection. This is a small accounting
regression fixture; it provides no whole-startup or production-cap qualification.

## Pinned descriptor reads

On Linux and Android, `OpenedPinnedShardStore::plan_fetch_pinned_until` and
`read_fetch_plan_pinned_until` select the ordinary metadata policy and read the
actual descriptors retained by bounded open. Open records each scanned pack's
sequence, file slot and validated identity in a separately prepaid bounded
vector. The active descriptor is the exact file moved into the ordinary store;
sealed descriptors remain in their original successful file slots. No pack is
reopened or reconstructed from a pathname or a claimed owner.

The read validates the supplied pin against the actual retained directory and
lease, then checks each retained pack name and held descriptor before direct
`pread` attempts. Original leading-zero filenames remain unchanged. Hardlinks,
symlinks and regular inode replacements refuse. A native plan retains its real
store token and epoch. Untracked ordinary active appends or rotations refuse;
this read producer does not qualify ordinary mutation, GC or later operational
distribution. The composing Core owner must retain its same parent and check
that parent's recursive identity through the borrowed checkpoints.

Each plan, range, stat, allocation, short/EOF/error/EINTR read and record mapping
uses the SAME supplied `StorageFetchBudget` and original deadline. The seven
existing limit axes remain unchanged. Every physical attempt consumes finite
cumulative entries; each `pread` additionally consumes requested read bytes and
a read-attempt slot. Hash-table capacity and conservative complete BTree entry
traversals are charged before actual probes. Full pathname scans and bounded
native name conversion are prepaid before parsing or syscalls. The logical
cost helper counts source-visible values, requested buffers, actual metadata
capacities and independent result/panic/header slots; it makes no allocator,
private library-layout, kernel or whole-process RSS/performance claim.

Retained results precede postchecks and completion observers. Refusal first
latches the same native counters and attempts the actual composing health
callback before partial buffers or physical owners can drop. No new sync or
write settlement is invented for reads. Partial bytes, native errors, actual
allocation results and independent unwind payloads remain observable without
exposing files, raw descriptors, parts constructors or storage factories.

`OpenedPinnedShardLog::read_from_until` consumes the whole canonical log and
keeps it last in both success and refusal. It shares ordinary canonical record
mapping and the same native budget across planning, physical reading, mapping
and later pages; `into_log` destroys read results before returning that whole
same log. Plain costs must be prepaid once before the shared budget is created,
with each actual composing bridge's additional costs paid separately. Success
stores no startup budget, Session or borrowed callback.

The new regression source exercises real ordinary-written histories, empty and
zero-payload reads, multiple packs, leading-zero names, sparse native indexes,
real namespace/identity changes, actual partial reads and EOF, original-E and
unwind boundaries, immutable low-entry policies, cumulative remaining budgets
and real lease drop order. These groups remain uncompiled and unexecuted at
this source checkpoint. Existing native open and canonical-constructor groups
remain preserved. Core owning-log adoption, bounded writes/seal/compaction,
Blossom recovery, C1/census integration, configured 64/256 MiB positives and full
host/currentness/serving qualification remain separate required gates.

Future write integration must retain real append reports and explicitly extend
this tracked identity set. Retired active file descriptors in those reports
and the exact new active store file must stay with their physical owners;
safe canonical reports must not expose those raw file-bearing wrappers. This
initial-history read unit provides no such write or rotation handoff.
