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

`TopicSequencer::propose_reservation` computes a fixed-size
`ReservationProposal` without allocating offsets or observing a prospective
leader epoch. Its typed coordinates are not a WAL or replication reservation.
`commit_reservation` compares the proposal with the current allocation, ring
and prior leader epoch before changing state; a conflict consumes nothing.
The caller must serialize commit with other sequencer mutations and separately
retain genuine physical authority. Proposals establish no authority themselves.
The engine's existing append path still reserves before its physical queue;
this sequencer primitive alone does not remove late-refusal abort gaps or
establish durable-index progress after a refused queued append.

The accepted epoch is reservation identity. The default `WriteFence`
preparation calls `check_write` and returns no pending ticket, preserving
existing implementations. This check returns before sink preflight,
backpressure and reservation; an additional unsynchronized check cannot fence
a later queued WAL mutation against authority revocation.

A host requiring that boundary declares the immutable
`requires_physical_admission` contract and implements `prepare_write` with a
`PendingWriteAdmission`. A pending ticket owns no physical permission.
`validate_before_reserve` rejects authority already revoked during preflight
or capacity waits. The shard calls `try_admit_physical` with the actual
reservation before any append preparation or data WAL mutation. The host must
join current partition, epoch, ring, role and genuine lease validity to its
shared transition state and return an owned `PhysicalWriteGuard`. Pending
revocation must stop new admissions, and conflicting effective ownership must
not publish while an admitted operation's guard remains live. Admission and
guard destruction must use bounded local state without I/O or synchronous
transition waits. This is a local linearization boundary, not a wall-clock
guarantee that fsync finishes before lease expiry or a distributed finality
proof.

The engine moves the guard and byte charge with the physical append command,
including batching and the sync queue, until physical settlement. Dropping a
producer response cannot release them early. Guard ownership ends before a
subsequent durable-index acknowledgement wait. Physical admission refusal or
panic occurs before data WAL mutation and uses the existing definitely-not-
written failure path; a storage or sync-dispatch failure remains ambiguous.
Before storage opens, a guarded constructor verifies that both configured
request ceilings plus command and fixed admission metadata fit the shard byte
budget. A guarded operation checks its actual charge before reservation, and
a ticket inconsistent with the installation contract is refused. The reported
maximum payload remains the configured maximum when that guarded
configuration is valid. These are command/input budgets, not process RAM
limits; a host must bound its own ticket and guard allocations.

`DurableApplyQueue::append_from_peer` carries `ReplicaSourceIdentity` obtained
from a verified transport to `prepare_replica_write`. Identity fields alone
are not authentication or leader authority. The host must verify credential
binding, sender membership, current remote-leader authority and the exact
reservation lineage. Replica entry points without verified context call
`prepare_unbound_replica_write`, whose default preserves existing write
preparation. A host requiring authenticated replica authority must explicitly
refuse this path even while it has local-leader permission. The default
context preparation also uses this unbound path because it does not verify
the supplied fields. Existing check-only implementations remain compatible.
A retransmitted old placement must
be explicitly authorized as historical lineage or refused; it must never be
restamped as a new current-term append. An exact duplicate acquires no new
physical guard and retains its original persisted coordinates.

These contracts alone do not supply a host authority implementation, HA quorum
or indexed replay. A late physical refusal retains the existing abort
algorithm: an allocated slot becomes an in-memory abort without rewinding the
normal sequencer. A later durable extent permits recovery of an interior gap;
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
