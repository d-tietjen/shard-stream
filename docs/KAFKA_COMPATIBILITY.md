# Kafka compatibility contract

`shard-stream` exposes a deliberately conservative Kafka listener. It does not
claim the full Kafka feature matrix: `ApiVersions` advertises only operations
implemented and tested by this repository.

## Implemented APIs

| API | Versions | Current behavior |
| --- | ---: | --- |
| Produce | 3–8 | `acks=0/1/-1`, record-batch validation, idempotent producer metadata, leader/quorum durability |
| Fetch | 4–6 | Ordered reads with rewritten Kafka offsets and stream-engine watermarks; fetch sessions are not advertised |
| ListOffsets | 1–5 | Earliest (`-2`) and latest (`-1`) offsets |
| Metadata | 0–8 | Single-broker metadata for named topics |
| ApiVersions | 0–3 | Exact capability negotiation |
| CreateTopics | 2–4 | Automatic partition placement; manual assignments and custom configs rejected |

Kafka topic names are mapped to native `u128` topic IDs using
domain-separated BLAKE3. The mapping is deterministic across restart. Topic
names observed through the Kafka endpoint are retained in memory for
all-topics metadata responses; named metadata requests remain deterministic
after restart.

Kafka record batches are stored as immutable shard-stream payloads. Produce
decodes them to:

- determine the exact logical record count;
- reject malformed, transactional, and control batches;
- map Kafka producer ID, epoch, and sequence to native idempotency; and
- preserve the original compressed bytes for durable identity.

Fetch decodes and re-encodes each stored batch so Kafka offsets match the
stream engine's ordered logical offsets. Gzip, Snappy, LZ4, and Zstandard are
enabled through the protocol library.

## Explicitly not advertised

- consumer-group coordination and committed group offsets;
- transactions and read-committed transaction filtering;
- ACLs, quotas, SCRAM/SASL, and Kafka TLS configuration;
- topic deletion, partition expansion, reassignment, and config mutation;
- follower fetch, tier-aware Kafka extensions, and multi-broker metadata; and
- Kafka topic UUID APIs introduced by newer request versions.

Clients must use API-version negotiation. Unsupported APIs or versions are not
silently emulated.
