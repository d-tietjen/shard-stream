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

### Exact native tier path joins

Archived CURRENT, manifest and object sources must agree on the same native
tier root. A matching object-key suffix under another root cannot complete
that join. `recovery_tier_manifest_path_matches` and
`recovery_tier_object_path_matches` reuse the ordinary native CURRENT,
manifest and object path producers. Root inference uses the actual relative
CURRENT path's component count, followed by exact encoded-byte comparison;
there is no separate directory-depth or shard-name parser. The shard and
generation come from the native decoded manifest; CURRENT must independently
select that generation.

Before either helper, predebit `recovery_tier_path_inspection_cost` into the
same cumulative recovery owner. Explicit `RecoveryTierPathLimits` bound every
input path/key and the native temporary, diagnostic, entry and work costs.
Both helpers preserve the supplied absolute Instant before and after native
phases. They read no files and return only an ordinary lexical match. Matching
paths still require same-set source membership, original source retention,
full object length/hash and native extent coverage; they authenticate no
physical root, placement, currentness or serving permission. Existing ordinary
tier loaders and native path/key policies keep their behavior.


### Tightening a retained native recovery deadline

`NativeRecoveryCensus::cap_expiry` takes an absolute `Instant` and retains the
earlier of that value and the census deadline. A closed host must propagate
each tighter common deadline to its retained census before starting another
native read or restore operation. An outer precheck alone cannot make a native
operation use a shorter deadline between its physical phases. The method
changes no resource limits, cumulative reservations, original sources, locks or
recovery authority, and a later call cannot renew an elapsed deadline.

The added source fixtures use actual native storage, the existing physical
source-read observers and real directory/shard lock contention to cover an
elapsed entry and shorter deadlines after identity validation, payload reading
and final validation. They also check that tightening does not reset charged
usage or release custody. These fixtures remain uncompiled and unexecuted;
full dependency, lint, test and startup qualification remain required.


### Shared engine custody in native adapters

`EngineHandle` retains either the exact existing `Arc<StreamEngine>` or, on
Unix, an actual `Arc<OwnedRecoveredEngine>`. The variants remain private. Clones
retain that same allocation; `engine()` and `Deref` expose a borrow only. There
is no bare-engine extraction, recreated directory owner, reopen, claimed digest
or completion flag. This is ordinary resource custody, not a capability to
serve, a currentness observation, or a substitute for a host's full recovery
and durable activation sequence.

`downgrade()` produces an opaque `WeakEngineHandle`. Its `upgrade()` retains
the entire original strong owner for a callback or task. Strong and weak
`ptr_eq` compare the actual underlying allocation within the same variant,
including dead weak references; independently wrapping one ordinary/recovered
Arc does not introduce a second outer identity. The last recovered strong
handle runs the native wrapper's actual owned sink join, normal engine worker
cleanup and then its retained native/root/tier custody destruction.

The existing ordinary Arc APIs retain their signatures and delegate to the
same implementations used by these parallel custody entries:

- `DurableApplyQueue::spawn_with_engine_handle`;
- `DurableApplyQueue::spawn_with_engine_handle_until`;
- `shard_stream_grpc::serve_with_engine_handle`;
- `shard_stream_h3::serve_with_engine_handle`;
- `shard_stream_kafka::serve_with_engine_handle`; and
- `shard_stream_kafka::serve_with_admin_and_engine_handle`.

Replica admission/append workers, gRPC service/stream/blocking callbacks,
HTTP/3 connection/request callbacks and Kafka broker/native topic-admin work
clone the complete handle. Existing queue bounds, framing, RF1 durability,
validation, fences, shutdown and custom topic-admin contracts remain in effect.
An adapter does not authenticate a supplied assignment provider or prove that
an independently supplied topic admin uses the same storage. Hosts must still
join those actual components. The public RF1 REST binary remains unchanged;
this unit introduces no alternate REST or startup factory.

An owned startup uses `spawn_with_engine_handle_until` with its saved original
absolute `Instant`. The constructor checks it before each channel and physical
worker spawn, after each started worker has entered synchronous cleanup custody,
and before success. Expiry can stop further work; an already-started spawn or
cleanup may settle late. Refusal and unwind physically join all started workers
before releasing the complete engine owner. The ordinary Arc and handle entries
retain their existing construction behavior without an added expiry. Successful
queues retain normal operation and shutdown behavior; a startup expiry is not a
new per-request permission or a replacement duration clock.

The genuine native custody fixtures produce object data, remove its primary
pack, restore through the actual census and consuming native constructor, then
exercise weak upgrades, real replica writes, blocked sink cleanup and unwind,
and later normal restart. Additional adapter component fixtures exercise a real
gRPC fetch, HTTP/3 finite configuration refusal without network, and a Kafka
native topic-admin future. Queue expiry fixtures use the actual recovered engine
and real spawns: expired entry starts no queue worker, and a paused first append
or admission worker crosses the same original expiry without starting a later
worker. Both directory owners remain contended until construction and worker
cleanup settle. These authored groups remain source regressions
until their normal dependency/typecheck and execution gates actually run.


### Original-deadline directory validation

`DirectoryOwnership::validate_directory_until` checks one supplied absolute
`Instant` before and after every explicit native path metadata, ancestor
metadata, descriptor open and native stat phase. This bounded API accepts only
the byte-exact retained canonical absolute directory name. Relative names,
alternate spellings and renamed paths fail before filesystem I/O. It walks
borrowed components from `/` using `DIRECTORY | NOFOLLOW | CLOEXEC`, keeping each
parent open until its child open settles. Ancestors must permit read access.
The terminal directory and its nofollow relative lock entry are compared with
the actual retained directory and lock descriptors using native stat types.
It allocates no OS-derived canonical pathname. This is a deliberately narrower
contract than the ordinary validator, which still accepts its existing path
spellings and performs its existing canonicalization. Neither API establishes
an atomic or permanent namespace against concurrent path replacement. An
already entered filesystem call may settle late while the actual owner remains
held. The validator then returns `DirectoryOwnershipUntilError::Expired` without
starting another phase. The existing ordinary validator and its exhaustive
error enum keep their signatures and policy. The additive error wraps
canonical-name, identity and filesystem refusals as `Ownership`; `Capacity`
reports checked cost overflow.

Before this method, charge `directory_validation_cost` into the same composing
resource owner. Its checked scalar facts use the borrowed input path and this
actual owner's retained canonical path, bounded component/NUL conversion
scratch, two temporary descriptor slots, native results and cleanup work.
`declared_bytes` includes its separate
`string_bytes` subset; `work_bytes` includes native entry work. These are ordinary
logical native costs, without a new budget, clock, directory owner, currentness
observation or serving authority. The host still owns the full startup and
durable installation sequence.

The authored fixtures use genuine native directories and OS locks, completed
physical-phase gates, a real elapsed deadline, fitting identity/lock policy and
last owner contention through blocked cleanup. Additional cases perform a real
same-inode rename and install a real symlink after ancestor observations;
alternate names start no native phase and the descriptor walk refuses the late
symlink. Ordinary alias validation remains supported. They remain uncompiled and unexecuted
until the normal native dependency, type and test gates run.

### Native active-file durability under an original expiry

`ShardLog::sync_until` and `ShardStore::sync_until` use the same actual active
file as ordinary synchronization. The composing owner first admits
`storage_durability_cost`, constructs a `StorageDurabilityBudget` with finite
`max_io_calls` and its original absolute expiry, and retains that budget with
the same native log/store. A successful synchronization consumes two slots:
the actual `File::flush` and `File::sync_data`. Slots never refund or recycle.

The producer saves each original typed I/O result before checking the clock
after that call. A late success or native error therefore remains visible in
`outcomes`; an entered slot without a result identifies an unwind before
normal completion. A late reservation can remain unentered. Once refused,
the budget cannot enter another native call. Synchronous calls settle before
return, and callers must retain the actual store and budget on every refusal.
The counters supply no ownership, admission, currentness or serving permission.

Costs describe declared source values and requested outcome-vector slots,
excluding allocator, kernel, device and process RSS overhead. This additive
surface covers active-file synchronization only. Native opening, appending,
reading, rotation and compaction need their own original-expiry producers;
calling an ordinary operation inside an outer clock check does not qualify it.
The ordinary synchronization APIs keep their existing behavior. The genuine
sync/reopen, lock-retention, one-less-slot, late-phase and actual Linux device
error regression source remains uncompiled and unexecuted pending the normal
native dependency and validation gates.

### Finite native fetch and retained directory operations

`EngineHandle::fetch_until` uses one original operation deadline and one shared
`NativeFetchLimits` budget across real coordinator selection, physical shard
planning, reads and result construction. Metadata batch counts, including zero
payload batches, and the committed batch-ID bitmap span have explicit limits
before allocation. Read retries, errors and EOF attempts spend the same counters.
A selected batch cap returns an ordinary native prefix for pagination. The old
fetch API retains its existing byte and visibility policy through the same
native selector and storage algorithms.

`RetainedNativeFetchError` holds the same `EngineHandle`, native child refusals,
queue errors and completed payload owners. Every entered bounded command is
drained before return or unwind. A stopped worker is a failure; it does not claim
that the whole engine has been destroyed. The same engine retains its actual
worker join and root-custody obligations until its last strong owner is dropped.

`NativeDirectoryBudget` carries one original deadline, cumulative finite counters
and min-only `cap_expiry` across the actual root, child directories and files.
`root_handle_until`, recursive `validate_until`, child `open_directory_until`,
`open_file_until` and directory `sync_until` check that same minimum inside every
native physical phase. Root validation shares the existing native canonical-name
and descriptor-walk algorithm. Child validation shares the ordinary native
identity predicate. New file opens use `NOFOLLOW` and `NONBLOCK`, validate the
actual regular-file identity and preserve nontruncating regular-file behavior.
Existing ordinary entry points retain their behavior.

`open_optional_directory_until` and `open_optional_file_until` use the same
physical read-only openers and treat only
fitting native NotFound as `None`, with the actual parent revalidated before
success. It leaves the carried budget usable for genuine first boot.
`entry_kind_until` performs a native no-follow probe: regular files, directories
and other real kinds are ordinary traces; symlinks refuse. These observations
provide neither source completeness nor startup permission.

`RetainedDirectoryError` keeps the same native root/parent, known mkdir/probe
outcomes and any newly opened producer descriptor before post-call deadline
observation. It never unlinks a failed/uncertain name. Borrowed validation targets
remain owned by the caller. Transient native validator descriptors settle during
the synchronous call; the error retains newly opened child/root-handle producer
outcomes. A late fitting absence or native error is a refusal, with the completed
native error retained when available.

The plain cost helpers describe source-visible logical setup values, retained
slots, path buffers and configured cumulative ceilings. String costs are a
subset of declared bytes. Their counters are prepaid conservative fees, including
unused fees when a later entry check refuses. Allocator/library/kernel internals
and whole-process RSS/performance remain separate measured gates. These ordinary
budgets grant no process, currentness, fencing or serving authority. Native sink
and replication resumes, topic creation, ShardLog open/append/rotation/compaction
and the final consuming startup remain separate required continuations.

### Native recovery lock acquisition under an original expiry

`ShardRecoveryGuard::try_acquire_until` consumes the actual supplied lock file
and directory. After admitting `shard_guard_acquire_cost` for that same path,
the caller supplies finite `ShardGuardAcquireLimits` and its original absolute
expiry. The producer performs the existing native identity policy, actual
nonblocking `fs2` lock, then the same identity policy again. It keeps both typed
validation results and the exact lock result before their post-call clocks.
Five cumulative native-call slots cover the fitting path; a smaller allowance
can refuse partway through the final validation while retaining the acquired
lock and actual metadata observations.

Success returns `AcquiredShardRecoveryGuard`, which borrows only the same
successfully acquired native guard and retains all observations. Refusal returns
`RetainedShardGuardAcquireError`, which keeps the actual input file and any lock
acquired before expiry or a final identity refusal. Its diagnostic observation
cannot expose an unacquired guard or raw descriptor to another native open API.
No retry, pathname reopen, descriptor clone, explicit unlock or fresh expiry is
hidden in acquisition. The ordinary acquisition API remains unchanged.

These costs describe source values and logical path bytes; callers already own
the input allocation and descriptor before entry. Allocator/kernel/RSS bounds,
parent-directory validation, native store opening/repair and serving permission
remain separate requirements. The genuine lock contention, late native phases,
one-less-call, changed-name and public cleanup test sources have not been
compiled or executed.

### Retained native atomic file replacement

`OwnedDirectoryHandle::replace_file_until` creates a new literal temporary file
in the same retained parent, writes an already-encoded body in finite physical
attempts, flushes and synchronizes that file, validates its actual identity,
performs descriptor-relative replacement `renameat`, then synchronizes the same
actual parent. The existing `NativeDirectoryBudget` supplies cumulative native
fees and its original/minimum deadline inside recursive validation and each
physical phase. No pathname-based reopen or ordinary unbounded file writer is
hidden inside the producer. A caller supplies a bounded unique temporary name;
an unrecognized existing temporary is refused and preserved.

Every actual write/metadata/flush/sync/rename result is retained before the next
clock check. Write attempt limits include short writes and Interrupted results;
one write requests at most the configured chunk size. The encoded input body,
original names, observed device/inode facts, actual file and same original root
survive in `NativeFileReplacement` or `RetainedNativeFileReplacementError`.
The descriptor's retained name changes only when actual rename returns success.
No failed temporary or destination is automatically unlinked, and an error does
not claim rollback. Pre-entry refusal retains the input and root without
allocating the entered producer or entering filesystem work.

Before rename, both rename and parent synchronization are prepaid. If actual
rename succeeds after the minimum deadline, the producer physically completes
that already-admitted parent synchronization before reporting refusal. Its raw
rename and synchronization outcomes remain available, including known changed
namespace effects on late/error paths. This physical settlement supplies no
fresh deadline or permission to publish a recovered state.

The actual carrier remains outside the producer's unwind boundary. A native
phase panic denies the same budget first and retains its original panic payload,
file, raw outcomes and encoded body. If a rename was entered without a saved
result, its namespace effect remains uncertain. The already-prepaid parent sync
settles once after a successful or uncertain entered rename, without a cleanup
callback or a new clock/admission. An already-entered parent sync is never
retried. A cleanup unwind retains its own payload and unfinished-phase fact;
it cannot be presented as a completed native result.

`native_file_replacement_cost` covers producer values, retained input capacity,
name buffers/native conversions and configured raw outcome slots. Recursive
native open/validation and each entered call spend the same directory budget
separately. These are source-visible logical costs; allocator/kernel/RSS and
performance measurements remain required. Serialization, content validation,
source census comparison and consuming HLC recovery remain separate required
work under the original deadline. The nine genuine filesystem regression
groups are uncompiled and unexecuted pending normal native validation.

### Retained native shard-directory handoff

`OwnedDirectoryHandle::pin_shard_directory_until` derives a descriptor clone
from the actual retained Core directory and joins it to an already acquired
`AcquiredShardRecoveryGuard`. Recursive Core validation and the native
`PinnedShardDirectory` producer use the SAME cumulative directory budget and
minimum original deadline. The native producer compares real directory
metadata and the acquired lock's descriptor identity with the no-follow lock
name observed relative to that actual directory FD. It enters at most four
native metadata/stat calls. No arbitrary descriptor and claimed Core owner pair
is accepted by the Core bridge.

Each actual clone/metadata/stat result is saved before its post-call checkpoint.
Early refusal retains the original input guard and Core parent without entered
allocation or I/O. Late, identity, I/O and unwind refusals retain all actual
partial results, descriptors, lock, original root and original panic payload;
the Core bridge denies the same budget before returning cleanup custody. Neither
success nor refusal exposes a raw descriptor or guard extraction API. Successful
custody contains no Session or permanently retained startup work budget.

The cost helpers describe fixed source values, configured native-call slots
and pathname conversion ceilings. The Core bridge prepays its descriptor clone
and native observation values, including an independent retained bridge panic
slot alongside the native producer's panic slot. Recursive Core validations
spend their own fees on that same budget. Allocator/kernel/RSS measurements remain
separate gates.
This handoff establishes physical directory custody only. Actual native store
opening, repair, codec work, appending and garbage collection still require
their bounded producers. Ten genuine filesystem regression groups are saved
for identity substitution, exact and one-less fees, real native errors, tighter
checkpoints, phase unwinds and final-owner cleanup; they remain uncompiled and
unexecuted.

### Bounded native garbage collection through the pinned descriptor

On Linux and Android, `ShardStore::garbage_collect_pinned_until` uses the actual
retained `PinnedShardDirectory` for native stat, no-replace rename, parent sync
and unlink. It first joins the actual directory and named path, acquired lock,
descriptor-relative named lock and store lock identities. The selected pack
must be an actual no-follow single-link regular file. Existing pending names
are preserved by native `NOREPLACE`; there is no pathname reopen or fallback
that replaces an unrecognized tombstone.

The collector shares the ordinary native retention predicate. A finite plan
caps existing map/vector capacities, literal path/name bytes, selected packs,
native calls and cumulative loop/copy/index-movement work. Raw pathname bytes
are capped and component/role scans are prepaid before parsing. The shared
parser preserves ordinary accepted leading-zero digit spellings. Planned batch
and topic keys are retained before mutation. It removes actual native index
values in finite steps and keeps those values, removed paths and original topic
vector allocations in the result before each post-operation checkpoint. Partial
epoch/index/namespace effects remain visible on refusal; no rollback is claimed.

Every actual stat/rename/sync/unlink result and reservation refusal precedes its
post-call deadline check. Actual normal sync-helper results are separately held
before the next checkpoint. Bounded native C names and conversion errors are
retained before final entry checks; native calls receive those prepared C names.
Rename and unlink each prepay their parent sync. On
late/error/unwind, the required scoped composing denial runs before cleanup,
then an already-admitted pending parent sync physically settles once through a
direct native syscall. An entered sync is never retried. Producer, denial and
settlement panics retain distinct actual payloads; an entered call with no
result keeps its unfinished phase. A denial panic does not assert that external
health was successfully denied. The composing owner supplies its SAME actual
counters and captured health, retaining the SAME store, pin and original Core
parent outside the operation through physical cleanup. The native report is
ordinary data and contains no Session, permanent startup budget or serving grant.

`pinned_shard_gc_cost` describes fixed source values, configured requested vector
and name slots, native conversion ceilings, native outcomes and three independent
panic slots before entry. Existing store/pin/floor allocations are already owned
inputs. Allocator/library/kernel/RSS and performance measurements remain separate
required gates. Twelve genuine native filesystem regression groups are saved for
actual retention, identity changes, partial index effects, tighter deadlines,
phase/denial unwinds, unknown pending names, real native errors and exact/one-less
limits. These tests remain uncompiled and unexecuted. Native open/repair and GC
still require normal compilation and validation. Later pack reads,
append/rotation, Core/worker distribution and strict consuming resume require
their actual producers and integration.

`ShardStore::open_pinned_until` separately discovers, scans and repairs through
the same actual pinned directory. Its Linux/Android `RawDir` uses a finite
caller buffer and exposes individual refill attempts; native reads, truncation
and synchronization use bounded direct attempts. Native framing, duplicate
batch rejection and index insertion use the existing native codec/reducer.
Actual opened files, raw reads, decoded values, index state and repair outcomes
remain held before post-call checks. The active store uses the actual scanned
descriptor. `OpenedPinnedShardStore` offers borrowed store access and retains
observations; it supplies no raw descriptor extraction or ownership/serving
witness. The caller must retain the SAME pin/Core parent through this owner and
all later jobs. Fifteen genuine opener groups and the actual bounded-open/GC
composition group remain uncompiled and unexecuted. Later ordinary store methods
do not become bounded by being called on a successfully opened store.

### Core owner for pinned native open

On Linux and Android, `OwnedShardDirectory::open_store_until` consumes the
actual Core-derived pinned directory into `OwnedPinnedShardStore`. It uses the
same cumulative `NativeDirectoryBudget` and minimum original deadline for
recursive Core validation and native discovery/read/decode/repair. Its required
composing checks deny the same work and captured health before entered native
repair settlement. Late failure retains the actual partial or opened store,
typed outcomes and independent panic payloads ahead of the original Core parent.
`owned_pinned_shard_store_cost` includes the native configured ceilings, bridge
values and actual shared-directory allocation; Core validation charges its own
actual phases to those same counters.

`retain_directory` returns opaque strong custody of the same pin, acquired lock
and Core parent for a physical worker. Internal synchronization retains the
complete native state, including any Send-only observation/error values; clones
expose only the original parent diagnostics. Each entered job must prepay the
fixed clone value/entry/work and keep that custody outside its actual file/request/
result frames until they are destroyed. This is necessary for a sync worker
that outlives the primary store. There is no pin, guard, descriptor or store
extraction, fresh startup budget or serving permission. Borrowed ordinary store
methods keep their existing behavior; bounded GC, append/rotation and actual
engine worker distribution require their own composed producers and validation.

### Core custody through native garbage collection

`OwnedPinnedShardStore::garbage_collect_until` consumes the whole actual Core
store and calls the native pinned collector using its same private directory.
The original/minimum `NativeDirectoryBudget` and mandatory composing checks
cover recursive Core validation and the real native operation. The actual typed
pin-borrow result remains outside catch boundaries until composing work/health
denial and physical native settlement finish. The private pin is never extracted
or paired with a caller-claimed owner.

Success returns `OwnedPinnedShardGcComplete`: its report precedes the whole Core
owner in destruction order. `into_store` drops that actual report and returns the
whole `OwnedPinnedShardStore`, preserving native pin/lock/Core-parent custody.
Every refusal retains the actual store, including its partial mutable indexes,
native report, typed errors and independent unwind payloads. A successful native
collection followed by a final Core identity/deadline refusal remains an error
with both actual outcomes retained. No store/pin reconstruction or retry witness
is provided. A physical worker must still retain its opaque directory custody
outside all file/request/result frames through physical joining.

`owned_pinned_shard_gc_cost` includes native configured ceilings, fixed Core
carriers, the actual pin-borrow result slot and independent bridge panic slots.
Core validation charges its own actual phases to the same counters. Existing
store/pin/floor allocations are already owned inputs; logical costs do not
establish allocator/kernel/RSS or performance bounds. Seven genuine Core/native
filesystem regression groups are saved and remain uncompiled and unexecuted.
Actual consuming engine/census/worker adoption, bounded later producers and full
host/resource/authority qualification remain required.

A worker that must restore its whole store and retain the actual native report
uses `OwnedPinnedShardGcComplete::into_store_and_report`. It returns the whole
`OwnedPinnedShardStore` plus `OwnedPinnedShardGcReport`; the latter retains the
original report followed by a clone of the same opaque directory custody. Either
value can drop first while the other retains the original pin and Core parent.
The report exposes only a borrow of the actual native results and the original
owner; it has no bare-report, descriptor or permission extractor. The GC cost
prepays the fixed carrier, local directory value and one Arc increment before
entry. No callback, Session or startup budget persists in the returned report.
One additional genuine filesystem group covers both drop orders and actual lock
custody, for eight Core GC groups in total, all uncompiled and unexecuted.

Composing workers prepay their actual local values, clones, queues and explicit
calls with `NativeDirectoryBudget::reserve`. This delegates to the same counters
used by Core's native operations; entry fees also add to work, string bytes stay
within declared bytes, and checked capacity refusal remains sticky across every
clone. `deny` exposes the existing irreversible refusal so a worker can deny the
same budget before its captured-health callback and physical settlement. Neither
method constructs custody or serving permission, changes limits, refunds fees or
renews the original/minimum deadline. Three additional genuine filesystem groups
cover shared reservations, exact/one-over admission and denial/minimum-expiry
refusal before native reentry; they remain uncompiled and unexecuted.

### Consuming Core append and seal

`OwnedPinnedShardStore::append_group_until` and `seal_active_until` consume the
whole actual Core store and call the genuine pinned native append/rotation
producer. `OwnedPinnedShardAppendLimits` bounds the original input Vec capacity
separately from its length and carries the actual native configured maxima.
The store and actual input Vec are retained before any cost, clock or unwind
boundary. The Core bridge checks capacity in constant time, prepays the actual
capacity times the input-slot size plus the full retained payload ceiling, and
then checks each original payload length under that same prepaid work. Native
header, receipt, container, error, physical-result and panic maxima are also
prepaid. Hidden payload allocator/alias capacity and RSS are not claimed.

One consuming call owns one genuine `PinnedShardAppendBudget`; every frame,
segment, rotation, short/Interrupted write and paid settlement shares its
counters and original/minimum E. Each later consuming call is a separate native
operation, while any supplied `NativeDirectoryBudget` continues to spend its
same cumulative counters and minimum deadline. No per-segment reset or refund
exists. A caller composing several operations must prepay their total actual
limits; the bridge does not revive an expired startup budget or Session.

The actual private-pin try-lock result and native append Result remain outside
the bridge catch boundary. The mandatory adapter denies the same directory
budget first and invokes the actual composing work/captured-health denial
before native rollback, file sync or directory settlement. Original native
errors, successful-but-late reports and independent bridge/denial panics remain
retained with the whole possibly uncertain store; refusal exposes no retry,
bare store, pin, descriptor or raw-report extractor.

`OwnedPinnedShardAppendComplete::into_store` drops the native report before
returning the whole Core owner. `into_store_and_report` instead returns that
whole owner plus an opaque `OwnedPinnedShardAppendReport` retaining the actual
native report and the same directory/root last. Either drop order preserves
physical lock custody. The optional Arc increment and output/observation carriers are
prepaid. Public report access returns a private-field
`OwnedPinnedShardAppendObservation` containing safe scalar/receipt and typed
IO/metadata observations. It exposes no native report/error wrapper, created or
retired File, descriptor, Deref or caller-claimed constructor. Error sources
cannot downcast to the FD-bearing native report/error. The original native
reports, partial errors, created/retired files and panic payloads stay privately
retained through actual cleanup. Caller-supplied panic payloads remain original
observations; this adds no new producer descriptor extractor. Existing ordinary writers and the Core GC return methods retain their
current behavior.

Twelve genuine filesystem source groups cover Core open, grouped append,
rotation, seal and reopen, real leading-zero ACTIVE names, original Vec capacity
greater than length, exact/one-less bridge and complete actual fees, raw writes
followed by actual elapsed-E or native panic settlement, real named-file IO
errors, typed private-pin refusals and both output drop orders. They remain
uncompiled and unexecuted. Native algorithm, Core composition and source
coverage do not establish configured C1/issuer health, serving authority,
64 MiB/256 MiB startup fit, full host/runtime or performance qualification.

## Consuming retained directory validation with shared fetch work

On Unix, `OwnedDirectoryHandle::validate_for_storage_fetch_until` consumes a
whole actual handle and borrows its existing `NativeDirectoryBudget`, the
caller's existing shared `StorageFetchBudget`, and mandatory
`CheckedDirectoryValidationChecks`. It tightens the Core minimum to the
native budget's original absolute deadline once, then checks both shared
states and the captured composing work before and after every explicit
allocation, conversion, identity, metadata, open and stat phase. A sibling
refusal or a later Core minimum applies to the next real phase. Neither
counter is reconstructed or reset, and no startup budget is stored in the
returned owner.

The additive producer walks the actual private root descriptors and the
original retained parent chain. It shares the existing pure spelling,
ancestor, root, lock and child identity predicates. The ordinary `validate`,
`validate_until`, `root_handle` and native budget behavior keep their original
physical/check order; their old result-consumption behavior is not advertised
as this retained contract. The new entry performs no canonicalize, alias
resolution, owner reconstruction, lock-descriptor clone or public FD export.

The outside-catch carrier owns every actual entered Metadata, Stat, native
open Result and transient OwnedFd, CString/NulError, allocation Result,
checkpoint refusal and independent unwind payload. An actual result is stored
before a callback or post-call deadline check. Native pathname metadata pays
its bounded path-conversion ceiling and four paid buffer passes before entry; explicit open/stat names
use retained C strings. Result-buffer growth prepays the requested delta and
movement of its existing capacity, retains the real allocation Result, and
charges any actual excess capacity before another physical producer. No
source-visible request substitutes vector length for capacity. The consumed
whole input is the last field, so its actual parent/root remains through all
raw result, transient descriptor and panic cleanup.

Every refusal denies the same Core counters, then the same native counters,
then attempts the captured composing denial exactly once for that validation.
Those independent panic payloads remain retained. An adapter used across
several validations must itself record the actual outer work/health denial
attempt before calling that health, so the composing operation also denies
once if that callback unwinds. This trait carries no permission or witness.
A successful `RetainedCheckedDirectoryValidation` exposes safe observations
and can return only the whole original handle through `into_handle`. The
retained error exposes actual safe errors and scalar observations, with no
handle, native owning Result, File, FD, panic downcast or retry extraction.

`checked_directory_validation_cost` is a plain checked setup/cumulative Core
ceiling. The caller prepays that ceiling and its existing native fetch setup
once before constructing counters, and prepays an actual `retain` before
passing a second whole handle. Each validation then spends its own fixed
retention values and the incremental real work from the same finite Core
policy. The fixed inventory names separate cap/fee/rule and checkpoint
results, four independent panic slots, allocation/result headers and ordinary
bookkeeping. Per-result admission separately includes its actual value,
error, Result header, enum/record and actual vector capacity. These are
source-visible logical costs; allocator/kernel internals and process RSS
remain separate gates. No credits, refunds, deadline renewal or larger cap
are introduced.

Twelve new genuine filesystem regression groups cover root/deep-child
acceptance and ordinary identity parity; every real completion and allocation
boundary under sibling native denial, health refusal and unwind; shared
minimum deadlines; an actual missing lock with retained late errno; independent
panic payloads; true last-owner contention; immutable fixed-slot exact and
one-less admission; actual deep-walk capacity; shared remaining counters and
zero-new-Arc/heap pre-entry refusal; real child replacement; and safe Send
carriers without an artificial Sync upgrade. These groups are source only,
uncompiled and unexecuted at this boundary. This entry is a generic custody
and observation producer. Core owning-log integration, native bounded read,
configured 64 MiB/256 MiB aggregate startup, C1 and whole-host qualification
remain open; no serving or source-completeness authority is created.
