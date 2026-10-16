# Extension contracts

The public crates expose compile-time contracts so another distribution can
compose additional behavior without patching storage internals. The contracts
do not provide distributed behavior by themselves.

## Engine and runtime

- `ReplicationTransport` receives immutable reserved batches, reports bounded
  asynchronous progress, and supplies synchronous durable-copy counts.
- `WriteFence` validates a writer epoch before the engine reserves durable
  state. Each new fenced reservation carries that accepted epoch into its
  placement, WAL, replication batch and durable sink event. The coordinator
  rejects a new reservation below its already observed epoch. Exact producer
  and atomic-chunk retries keep their original receipt and placement, including
  after restart. Without a fence, a supplied request epoch keeps its existing
  non-authoritative behavior and does not advance the placement epoch.
- `AssignmentProvider` supplies an immutable assignment view.
- `RetentionGuard` can reject a retention advance that would violate an
  external reader or migration invariant.
- `DurableAppendSinkFactory` installs a cluster-shared transactional sink
  without changing the shardlog format.
- `HealthProbe` contributes local observations to embedded readiness.

Opening an `EngineConfig` with RF>1 requires an injected
`ReplicationTransport`. `StreamEngine::open` and the public server never
manufacture local follower copies. A transport implementation must bound
admission by bytes and work items, make retries idempotent, and only report
copies that reached its durable boundary.

`DirectoryOwnership::acquire` provides nonblocking, advisory exclusive custody
of a local engine directory on Unix. Acquire it before advancing a process
identity or opening mutable recovery state. Its sealed `Arc` owns actual
directory/lock handles and checks file identities, symlinks, lock hardlinks and
same-directory installation. The lock inode is reused and is never deleted or
truncated by this API. Filesystem paths must remain unchanged during custody;
an advisory lock cannot fence an actor unlinking/replacing paths or a process
using an unowned constructor.

`StreamEngine::open_with_owned_components` validates that exact directory before
storage opens and retains the owner through the engine's full destruction.
Escaped engine `Arc`s keep custody. Actual lane/sync and durable-sink workers
also retain it outside their physical frames, covering constructor failure and
bounded dispatcher destruction that detaches a blocked sink callback. A worker
releases custody only after its callback, sink and factory destruction finish.
A permanently blocked detached callback therefore keeps the directory locked;
a host must report that physical shutdown is still incomplete. Existing
constructors keep their unowned behavior and explicit sink-replay contracts.
`has_directory_ownership` checks the exact installed owner allocation without
filesystem I/O. It establishes no process incarnation, authenticated recovery,
lease, replay or write authority. A composing host must retain the same owner in
its own escaped state, tickets, guards and persistence workers. Arbitrary
third-party background work is covered only if it explicitly retains that
owner; this API does not discover it. Authenticated identity/cache handoff and
current authority recovery remain separate required contracts.

`TopicSequencer::propose_reservation` computes a fixed-size
`ReservationProposal` without allocating offsets or observing a prospective
leader epoch. Its typed coordinates are not a WAL or replication reservation.
`commit_reservation` compares the proposal with the current allocation, ring
and prior leader epoch before changing state; a conflict consumes nothing.
The caller must serialize commit with other sequencer mutations and separately
retain genuine physical authority. Proposals establish no authority themselves.
For a local-leader append with a physical ticket, the coordinator first
prepares the proposal and the relevant producer/atomic-group counters without
mutating them. The engine charges and queues input on the proposed lane. The
worker obtains genuine permission through `try_admit_proposal`, then commits
only if the proposal, prior epoch, ring, dense index, replication policy and
relevant producer/atomic-group state still agree. A refusal, callback panic,
queue refusal, cancellation or compare conflict before commit allocates no
coordinates, creates no abort and does not fail the producer. Only a committed
reservation can enter WAL, replication or durable-sink publication. The
unfenced/check-only path preserves its existing parallel append flow.

The accepted epoch is reservation identity. The default `WriteFence`
preparation calls `check_write` and returns no pending ticket, preserving
existing implementations. This check returns before sink preflight,
backpressure and reservation; an additional unsynchronized check cannot fence
a later queued WAL mutation against authority revocation.

A host requiring that boundary declares the immutable
`requires_physical_admission` contract and implements `prepare_write` with a
`PendingWriteAdmission`. A pending ticket owns no physical permission.
`validate_before_reserve` rejects authority already revoked during preflight
or capacity waits. For local leaders, the shard calls `try_admit_proposal`
with typed prospective coordinates, without constructing a reservation. An
existing strict ticket's default proposal method refuses until its host
implements this contract. Guarded incoming replicas first verify the exact
received reservation against pure receiver sequencing and producer/atomic
snapshots. They charge and queue input on the actual receiver shard, obtain
`try_admit_physical` permission for those immutable remote coordinates, and
only then compare/commit receiver allocation and epoch. This uses the existing
reservation callback without forging or restamping coordinates. Positive
production sender/issuer/lease installation is a separate required stage. The host must
join current partition, epoch, ring, role and genuine lease validity to its
shared transition state and return an owned `PhysicalWriteGuard`. Pending
revocation must stop new admissions, and conflicting effective ownership must
not publish while an admitted operation's guard remains live. Admission and
guard destruction must use bounded local state without I/O or synchronous
transition waits. This is a local linearization boundary, not a wall-clock
guarantee that fsync finishes before lease expiry or a distributed finality
proof.

`StreamEngine::has_guarded_replica_fence` checks the actual guarded mode captured
at construction and exact allocation identity of the installed fence. A clone
of that installed `Arc<dyn WriteFence>` qualifies; a different allocation,
check-only fence or unfenced engine does not. An alias wrapper must be checked
as the actual installed allocation, rather than substituting its inner fence.
This check calls no authority callback and supplies no TLS, issuer, lease or
current write permission proof. Hosts still need the reviewed receiver source
and dependency pins plus genuine authority and delivery qualification.

The engine moves the guard and byte charge with the physical append command,
including batching and the sync queue, until physical settlement. Dropping a
producer response cannot release them early. Guard ownership ends before a
subsequent remote or durable-index acknowledgement wait. Guarded leader and
replica physical refusal or panic precedes allocation and is returned separately from
failure of an allocated reservation. Storage or sync-dispatch failure after
commit retains the existing failure/recovery semantics and remains ambiguous.
Before storage opens, a guarded constructor verifies that both configured
request ceilings plus command and fixed admission metadata fit the shard byte
budget. A guarded operation checks its actual charge before reservation, and
a ticket inconsistent with the installation contract is refused. The reported
maximum payload remains the configured maximum when that guarded
configuration is valid. These are command/input budgets, not process RAM
limits; a host must bound its own ticket and guard allocations.

Preparation replies and guarded replica plans box their fixed metadata. Shard
admission moves that metadata into the existing inline charged command, so the
queued proposal retains the same metadata representation and byte charge.
The transient preparation/plan allocations and allocator overhead remain
outside the shard command budget and need process resource and performance
qualification.

Proposal RPCs use the existing bounded coordinator queue with nonblocking
queue admission and one preparation plus one commit attempt. Both have a
one-second response interval. Preparation has no mutations. A commit still in
`Pending` may atomically become `Cancelled`, preventing later allocation;
once `Committing` wins, its physical worker retains capacity and permission
until the actual reply or destruction of its reply sender after settlement or
coordinator failure. Only a successful `Pending` cancellation or an actual
returned precommit refusal proves no allocation. A missing reply after commit
wins is `Unknown`: it cannot create an abort or index delivery, nor prove that
sequencing, producer or group state stayed unchanged. There is no detached
replacement job or automatic proposal reroute. The new handlers perform no journal or host
metadata I/O, but the shared coordinator still executes existing journal
control operations. Therefore this interval is not a strict wall-clock bound
on an already committing operation, storage I/O or engine destruction.

For guarded new local writes, replication dispatch follows successful local
WAL/sync settlement; the local physical guard ends before peer acknowledgement
waiting. Each follower must independently verify its current credential,
incarnation, remote lease, role and physical permission. This ordering does
not provide that missing positive follower/HA contract. A sufficiently durable
cached receipt keeps its exact identity. The strict path first uses a
read-only current-authority check plus full producer/chunk duplicate validation;
it returns an eligible immutable ACK before ticket acquisition, sink validation
or physical charge. That read-only check grants no new write permission. Only
new prepared work acquires owned capacity and its physical guard. A guarded
historical retry that
would dispatch new peer WAL is refused until a separate authenticated replay
lineage contract exists; its old placement must not be restamped.

`DurableApplyQueue::append_from_peer` carries `ReplicaSourceIdentity` obtained
from a verified transport to the installed fence. Identity fields alone
are not authentication or leader authority. The host must verify credential
binding, sender membership, current remote-leader authority and the exact
reservation lineage. Replica entry points without verified context call
`prepare_unbound_replica_write`, whose default preserves existing write
preparation. A host requiring authenticated replica authority must explicitly
refuse this path even while it has local-leader permission. The default
context preparation also uses this unbound path because it does not verify
the supplied fields. Existing check-only implementations remain compatible.
For an actually guarded bound receiver, `check_replica_write` first verifies
current replica authority without acquiring host tickets. Its default refuses
unsupported strong authority; it never borrows local-leader permission. Pure
receiver preparation then validates the exact reservation, producer and atomic
identities, payload length and supplied digest. Before returning a durable
duplicate, the engine rechecks current read-only authority after that RPC and
returns the original coordinates without a ticket, physical guard or new WAL.
New input alone calls `prepare_replica_write` and follows the charged physical
guard/compare-commit path. The read-only result cannot authorize a new write.
This transport-neutral engine boundary retains the composing distribution's
framing/checksum/authentication responsibility; it does not authenticate a
caller-constructed source or supply TLS, issuer or lease proof.
A retransmitted old placement must
be explicitly authorized as historical lineage or refused; it must never be
restamped as a new current-term append. An exact duplicate acquires no new
physical guard and retains its original persisted coordinates. Current replica
source authorization still runs before an immutable ACK; the existing replica
read-only hook does not consume physical ticket capacity. This preserves an
eligible cached ACK even while a real host ticket/guard owns that capacity.

For a guarded receiver, `DurableApplyQueue` advances its ordering cursor only
after the actual committed append result, before draining the next buffered
batch. It does not advertise an unallocated predecessor as reserved. Queue
refusal, Pending cancellation, callback refusal/panic or compare conflict
changes no receiver epoch, offset, producer/group state, data WAL or index.
Missing replies after commit wins remain `Unknown` under the same owned
arbitration as leader proposals; they cannot create a speculative abort or
claim unchanged receiver allocation. Legacy/check-only receiver planning and
parallel dispatch preserve their existing behavior. Exact guarded retries of
an existing aborted batch remain refused without authenticated replay lineage;
legacy aborted revival is unchanged. Successful follower WAL installation is
still index-silent; retained WAL replay through `resume_durable_sink` is the
existing separate leader-activation contract.

These contracts alone do not supply a host authority implementation, HA quorum
or indexed replay. Guarded leader or replica precommit refusal does not create
an allocated gap. Already allocated legacy replica or storage failures retain the
existing abort algorithm: an allocated slot can become an in-memory abort
without rewinding the normal sequencer. A later durable extent permits recovery of an interior gap;
an unpersisted tail can disappear during restart. In-memory aborts are not
durable sink checkpoint evidence. The current sink does not advance across
such a gap, and a safe checkpoint-only advancement requires durable abort
lineage restored before checkpoint trust. Neither an aborted frontier nor a
guarded WAL append proves source completeness.

The response's visibility watermark remains conservative under the existing
recovery policy; it is separate from persisted batch, record, offset and
placement identity and need not equal its earlier live response value.

## Durable append sink

`DurableSinkConfig` adds asynchronous, at-least-once delivery after the local
WAL fsync succeeds. Normal appends publish a cheap `Bytes` reference only after
durability; replica installation is silent. A new leader must call
`StreamEngine::resume_durable_sink` with its assigned partitions. The embedded
runtime does this from its `AssignmentProvider` during `recover`. The
standalone `StreamEngine::open_with_durable_sink` constructor treats every
recovered RF1 partition as locally led.

Progress is cluster-shared and partition-scoped:

- `DurableSinkCheckpoint` identifies the next `PlacementSequence` and next
  logical offset.
- `DurableAppend::event_id` is the stable `RecordId::for_batch` identity.
- `DurableAppendSink::apply` receives the expected checkpoint, one or more
  events, and the next checkpoint.
- The sink must compare the expected checkpoint and atomically commit event
  effects plus the next checkpoint. A conflict returns the actual checkpoint.

Every shard handle returned by one factory must use the same cluster checkpoint
namespace and transactional backend. A process-local checkpoint per shard is
incorrect: placement can move between physical shards, and a follower can be
promoted after storing records that it intentionally did not publish.

The engine guarantees at-least-once attempts. Exactly-once effects require the
sink transaction to make event IDs, output, and checkpoint advancement atomic.
Sink failures or panics are retried and never replace an append result that was
already made durable. Third-party callbacks run only on partition-striped sink
workers; callback code is never invoked by shard, coordinator, or fsync
threads. `validate_append` is the one exception: it runs before offset
reservation on the producer caller and therefore must be bounded and must not
perform sink I/O.

`DurableSinkOptions` controls:

- blocking or background startup recovery;
- bounded backpressure or availability-first lag handling;
- all-durable publication or committed-only atomic visibility;
- callback worker count (two partition-striped workers by default) plus
  pending item and byte limits; and
- the recovery/checkpoint timeout.

Backpressure is the default and happens before coordinator reservation.
Availability mode acknowledges the durable WAL append when the notification
budget is exhausted, drops only the in-memory notification, marks the
partition dirty, and relies on `resume_durable_sink` to rescan retained WAL.
Hosts should invoke that method from their assignment/maintenance loop for
dirty locally led partitions. `DurableSinkStats` exposes pending items, pending
bytes, checkpoint age, retries, failures, dropped notifications, and dirty
partitions. The sink checkpoint automatically pins retention, so availability
mode can increase disk usage until catch-up.

In `CommittedOnly` mode, unresolved atomic chunks are delivered as
`DurableAppendDelivery::Stage`. The shared sink must durably stage them,
declare `supports_committed_only`, implement `pending_atomic_groups`, and make
`finalize_atomic_group` idempotently apply commit or abort decisions. A
decision can arrive before a retried stage operation, so the sink backend must
retain an idempotent decision record. Restart reconciliation compares pending
sink groups with recovered coordinator state.

Worker destruction is bounded: a permanently stuck sink callback is detached,
and uncheckpointed work remains in WAL for restart. This means sink
implementations must not assume engine destruction waits for their callbacks.
The public `shard-stream-server` installs no concrete sink and exposes no sink
configuration.

## Logical topics and clients

`LogicalTopicBinding`, `LogicalTopicCursor`, and `LogicalTopicResolver` keep a
stable client-facing `TopicId` while allowing a composing distribution to
resolve another physical identity. The public server resolves every topic to
itself at policy/data generation `1`.

## Server

`ServerBuilder` accepts `ServerExtension` implementations at compile time.
Extensions may add routes, readiness observations, and shutdown-aware
background tasks. The public binary installs none. Cargo features and runtime
dynamic loading are not privacy or isolation boundaries.

## Bounded catalog before owned recovery

On Unix, `NativeRecoveryCensus::catalog(&mut self)` derives an immutable
`NativeRecoveryCatalog` from the actual retained coordinator journal and the
census's original `EngineConfig`. It shares the ordinary full recovery
Configure/CreatePartition/UpdateReplicationPolicy reducer. Legacy Configure
keeps the native configured default policy, exact policy repeats keep native
idempotence, and native validation/error ordering is preserved. The existing
SSJ3 writer/parser still enforce their own wire validity, including advancing
policy updates. A pure typed-reducer differential is not a disk acceptance
claim.

`partition(tp)` borrows the effective policy, complete ordered native placement
ring history over virtual lanes and configured virtual lane count. Ring tokens
retain their native `ShardId` representation; resolve physical storage shards
through the original `EngineConfig::physical_shard_for_lane`. These tokens are
distinct from physical storage shards and broker replica IDs. An absent TP has
no synthesized policy or empty ring. The
privately constructed catalog exposes its actual original owner allocation,
Instant and cumulative native usage. It accepts no caller event, map, usage or
authority flag. The census and its source leases must remain owned while the
host joins these observations with signed history and physical evidence.

Catalog fixed values, all borrowed event sizing, native map/ring values,
validation scratch, clones, probes and finite error buffers are admitted through
the SAME native reader caps before materialization. Repeated calls accumulate
cost and never renew the saved Instant. Catalog construction reads no files,
repairs no tail, opens no storage and starts no worker. It validates the native
configuration event subset; full payload, truncation, atomic state and engine
recovery remain required before operational use. No assignment, lease,
currentness, serving or write-fence permission is conferred.

The new source fixtures use actual RF1 engine creation/reconfiguration/policy
updates, real SSJ3 journal production and ordinary full restart, native invalid
configuration differentials, exact/one-less original caps, cumulative refusal
and actual expiry/custody release. A separate explicitly pure typed reducer
fixture preserves its broader in-memory semantics without fabricating native
wire acceptance. They remain uncompiled and unexecuted in this source unit;
canonical formatting alone does not qualify the full recovery graph.

## Original expiry during retained source reads

`NativeRecoveryCensus::read_source_chunk` keeps the original census resource
allowance, source descriptors and absolute expiry. It checks that expiry before
source identity validation, before and after descriptor cloning, before the
final identity validation, and after that validation before returning bytes.
Elapsed work cannot start another physical source phase or return a successful
late copy. Allocation and read admission continue to use the existing cumulative
native counters. Refusal retains the census and its actual directory and shard
leases; these bytes grant no authority or serving permission.

The source regressions use real native writers, retained file identities and
leases. Test-only scheduling pauses completed operations against the same real
saved expiry; it supplies no clock or source replacement to the reader. Exact
and one-less copy/read caps and real inode replacement are also covered. These
new cases remain uncompiled and unexecuted until the original native dependency
graph and required full checks run.

## Borrowed complete native extent inspection

`inspect_recovery_extent_payload` borrows a complete retained extent and returns
the actual native reservation, producer and atomic-group fields together with
the original frame and payload slices. It calls the unchanged native complete
decoder: native prefix/length, checksum, nonzero record count, range, reserved
fields and payload-length acceptance stay with shardlog. Payload boundaries are
selected inside that codec, with no host-side SSE8 offset or replacement parser.
An incomplete tail is refused here; the separate incomplete-header inspector
retains its conservative observation/Unknown contract.

Before invocation, the composing owner must charge the ordinary
`recovery_extent_inspection_cost` facts against its existing cumulative budget.
Explicit finite per-call limits are checked again before native work. The facts
include already-held frame/path bytes, named fixed native/result/decoder slots,
native diagnostic buffers, checksum and bounded decode/copy work. They are
logical bounds, not allocator or process RSS limits. No payload is cloned or
canonicalized. A borrowed view remains valid only while its original frame is
owned by the caller; storage leases and authenticated source/body joins remain
the caller's responsibility.

The original absolute Instant is checked before limits/native phases and after
native settlement. Sizing does not create a clock or carry consumed usage.
These APIs perform no filesystem I/O, repair, mutation, worker creation or
authority admission. The engine re-exports the same native types and functions
for compositions without a direct shardlog dependency. Native validity alone
does not prove child lineage, signatures, source completeness, currentness,
availability, commit or serving permission.

The new source fixtures use real native append/sync output, original borrowed
pointer identity, complete producer/atomic metadata, consecutive/empty payloads,
the ordinary decoder's checksum/range refusals, exact/one-less cost limits and
the original saved expiry. They have not been compiled or executed in this
source unit; formatting, type checks and full composition qualification remain
separate required work.

## Native extent sizing through the engine

The engine also re-exports shardlog's actual `RECOVERY_EXTENT_PREFIX_BYTES` and
`recovery_extent_frame_bytes`. A composing host can read that fixed prefix from
its retained `NativeRecoveryCensus`, use the native helper to validate and size
the complete frame, then read exactly that frame through the same census before
calling `inspect_recovery_extent_payload`. It need not infer native header or
checksum offsets from payload length.

The composing host must admit prefix inspection and any bounded native
diagnostic allocation into its existing cumulative allowance, check its saved
absolute expiry before and after the helper, and bound the returned size before
the complete read. The census separately charges both actual reads. These are
the original native constant and function, with unchanged acceptance; they do
not acquire storage custody, establish body lineage or completeness, renew a
budget or expiry, or confer permission to publish a leader term. Full native and
composition qualification remains required.


### Ordinary archived native source inspection

On Unix, `shard-stream-engine::inspect_archived_native_source` interprets one
complete, already-retained native source through the same native parsers used by
recovery. It performs no filesystem I/O, repair, engine open, worker start or
storage-owner construction. The caller retains the original chunks and their
source context; assembling chunks is separate work that must be admitted before
copying. The returned `UnverifiedNativeArchivedSource` borrows those original
bytes. It does not authenticate their physical origin or supply completeness,
currentness, a floor decision, child lineage or permission to serve or append.

Admit `native_archived_source_inspection_cost` through the caller's existing
cumulative resource owner before inspection. Its plain facts cover held input,
result/native slots, dynamic SSJ3 shard values, JSON/native strings, bounded
native diagnostics, and scan/hash/decode/validation work. These are logical
value and byte bounds, separate from allocator capacity, RSS or a CPU-time
promise. The inspector independently checks the explicit finite limits before
native work and retains the caller's original absolute `Instant`; it never
creates a replacement duration or cumulative budget. Limits apply to whole
source/frame/manifest/path/payload lengths, complete frame count, JSON depth and
nodes, declared bytes, work and entries. JSON depth can only tighten the native
32-level walk. The shared shape visitor admits every key/value visit before
native manifest Vec/String materialization, including ignored JSON fields.

For a coordinator source, every complete SSJ3 event and its original raw frame
is returned in physical order. An incomplete terminal suffix remains explicit;
SSJ3 has no encoded leader term. Pack and object roles expose every complete
SSE8 frame through the actual checksum/header decoder, including native
reservation, producer, atomic-group fields and the borrowed payload. An
incomplete SSE8 suffix retains its exact bytes plus the native conservative
header or explicit Unknown. Complete corrupt frames refuse; they are never
silently converted to an empty source or a valid payload.

CURRENT uses the native trim/u64 text grammar. Selected and unselected
manifests use the native typed JSON decoder and native manifest validation,
retaining every object and extent. The optional expected shard is an ordinary
context check: `Some(actual_shard)` checks a manifest against that value;
`None` validates only its own decoded shard and remains physically unbound.
Neither path guesses a shard from an archive path. Native ordinary loaders
retain their existing `read_to_string`, missing-CURRENT and invalid-UTF8 I/O
behavior. A raw inspector reports its separate ordinary byte refusal instead.

Cross-source CURRENT/generation/hash/coverage joins, effective journal/catalog
reduction, native placement-to-physical-shard mapping, duplicate/source identity,
full current and historical floors, authenticated child/body linkage and the
actual retained-owner handoff remain the consuming recovery path's work. Raw
capture context never makes an otherwise possible local term foreign. Native
frame validity alone cannot establish an active-active commit or activation.
The archive source fixtures are source regressions until the normal dependency,
typecheck and test workflow has actually run.
