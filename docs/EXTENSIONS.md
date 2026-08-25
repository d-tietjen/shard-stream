# Extension contracts

The public crates expose compile-time contracts so another distribution can
compose additional behavior without patching storage internals. The contracts
do not provide distributed behavior by themselves.

## Engine and runtime

- `ReplicationTransport` receives immutable reserved batches, reports bounded
  asynchronous progress, and supplies synchronous durable-copy counts.
- `WriteFence` validates a writer epoch before the engine reserves durable
  state.
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
