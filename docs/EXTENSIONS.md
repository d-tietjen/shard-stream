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
- `HealthProbe` contributes local observations to embedded readiness.

Opening an `EngineConfig` with RF>1 requires an injected
`ReplicationTransport`. `StreamEngine::open` and the public server never
manufacture local follower copies. A transport implementation must bound
admission by bytes and work items, make retries idempotent, and only report
copies that reached its durable boundary.

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
