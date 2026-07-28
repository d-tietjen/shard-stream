# Health and readiness

The public server exposes:

- `GET /healthz`: process liveness and the fixed RF1 durability profile.
- `GET /readyz`: local traffic readiness and the fixed RF1 profile.
- `GET /metrics`: process-local HTTP/Kafka counters plus explicit
  `shard_stream_replication_factor 1` and
  `shard_stream_min_in_sync_replicas 1`.
- `GET /v1/topology`: exactly one local broker.

None of these endpoints claims quorum, replica, promotion, or failover health.
The public binary completes storage recovery and validates the data directory
before binding listeners. RF>1 policies or HA-managed state fail startup with
`SHARD_STREAM_PRIVATE_REQUIRED`.

Embedded applications can use `EmbeddedRuntime::health_snapshot()` and inject
a `HealthProbe`. The default filesystem probe measures local free space.
Extension-provided observations remain the responsibility of the composing
distribution and do not change the guarantees of the public server.
