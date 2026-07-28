# Kafka compatibility contract

`shard-stream` exposes a deliberately conservative, single-broker Kafka
listener. `ApiVersions` advertises only operations implemented and tested by
this repository.

Kafka support is the default-enabled `kafka` feature of
`shard-stream-server`. A `--no-default-features` server build contains no Kafka
adapter dependency, listener, CLI configuration, metrics, diagnostics route,
or topology advertisement.

| API | Versions | Public RF1 behavior |
| --- | ---: | --- |
| Produce | 3–8 | `acks=0/1/-1`, record-batch validation, idempotent producer metadata; acknowledged durability is local RF1 |
| Fetch | 4–6 | Ordered reads with rewritten Kafka offsets; fetch sessions are not advertised |
| ListOffsets | 1–5 | Earliest (`-2`) and latest (`-1`) offsets |
| Metadata | 0–8 | One broker, one leader, and a one-member replica set |
| ApiVersions | 0–3 | Exact capability negotiation |
| CreateTopics | 2–4 | RF1 topics only; manual assignments and custom configs rejected |

Kafka topic names map deterministically to native `u128` topic IDs using
domain-separated BLAKE3. Kafka record batches remain immutable payloads.
Produce validates record count, rejects transactional/control batches, maps
producer identity to native idempotency, and preserves compressed bytes.
Fetch rewrites logical offsets into Kafka offsets.

`acks=all` is accepted for client compatibility but cannot provide failover
durability in a one-broker deployment. A create-topics request asking for a
replication factor other than `1` is rejected.

Not advertised:

- consumer groups and committed group offsets;
- transactions and read-committed filtering;
- ACLs, quotas, SCRAM/SASL, or Kafka TLS configuration;
- topic deletion, partition expansion, reassignment, and config mutation;
- follower fetch; and
- newer topic UUID APIs.

Clients must use API-version negotiation. Unsupported APIs and versions are
not silently emulated.
