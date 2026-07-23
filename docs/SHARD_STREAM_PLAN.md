# shard-stream Plan

Status: reviewed architecture plan
Market and community audit snapshot: 2026-07-23

This document describes a proposed independent repository named `shard-stream`.
The repository will reuse selected storage, persistence, transport, and
operational code from `shard-kv`, but it will have its own crate boundaries,
data model, protocol, release cadence, and compatibility contract.

## 1. Product direction

`shard-stream` is a tiered, shard-striped append log intended to provide a
Kafka-compatible adoption path and a more efficient native protocol. Its main
architectural bet is that a single logical partition can place successive
append batches on different shard-owned lanes, gaining physical parallelism
without exposing more Kafka partitions.

The core design separates physical placement from logical stream semantics:

```text
 Kafka clients | REST/HTTP/3 clients | gRPC/HTTP/2 clients
                         |
              protocol adapters and admission
            |
 topic sequencers / partition coordinators
            |
  bounded per-shard MPSC queues
            |
 shard-local extent-pack writers ------ direct replica streams
       |                         |
 local SSD / page cache     object storage

 Blossom finality
       |
 ownership, epochs, ring revisions, and promotion certificates only
```

The physical engine distributes admitted append batches across shard lanes
using a per-topic round-robin placement ring. Placement sequence determines
where a batch lives; it does not determine when a pipelined batch finishes.
The logical partition layer builds ordered, Kafka-compatible views over those
physically striped batches.

There are two deliberately separate paths:

- The data plane writes and replicates append batches directly between shard
  lanes.
- The control plane uses adapted `shard-kv` Blossom bridge logic to finalize
  rare ownership conflicts, monotonically increasing epochs, ring revisions,
  and failover promotions.

Blossom is not a payload replication path, a second WAL, or per-append
consensus.

## 2. Goals

- Provide a durable append-log engine with shard-parallel writes and reads.
- Keep hot records in memory while progressively moving sealed data to SSD and
  object storage.
- Allow one ordered logical partition to use multiple physical shard lanes.
- Change physical shard membership without changing the client-visible logical
  partition count or copying already offloaded cold data.
- Reuse proven shard ownership, WAL framing, checksums, compression, TLS,
  replication, metrics, and test infrastructure from `shard-kv`.
- Support existing Kafka producer and consumer clients through the Kafka
  request/response protocol.
- Expose an HTTP/3 native protocol that can use independent QUIC streams,
  direct shard routing, larger batches, zero-copy buffers, `u128` identifiers,
  and tier-aware fetches.
- Generate standard gRPC clients from the same Protobuf service model as an
  HTTP/2 compatibility and UDP-blocked-network fallback.
- Support active-passive failover per shard or topic ownership domain.
- Support active-active deployments for disjoint logical partition ownership;
  retain a single fenced writer for any one ordered partition.
- Keep fixed memory per idle topic and logical partition near zero through
  shared queues, coordinators, packs, and indexes.
- Make the physical engine independent of Kafka-specific protocol assumptions.

## 3. Non-goals for the first release

- Reimplement every Kafka broker feature before basic producer/consumer
  interoperability works.
- Preserve Redis or general key-value semantics.
- Treat the existing Redis Streams implementation as the storage foundation.
- Claim global ordering when the placement engine only provides sequencing for
  physical placement.
- Use eventual active-active conflict resolution as a substitute for a single
  ordered writer or leader fence.
- Send stream payloads or per-append decisions through Blossom.
- Equate Kafka `acks=all` with object-storage durability.
- Claim full Kafka compatibility without a tested API-version, client, codec,
  authentication, and semantic compatibility matrix.
- Call the HTTP/3 protocol "gRPC" unless it passes standard gRPC client
  interoperability; standard gRPC remains a separate HTTP/2 adapter.
- Force hot producer or consumer payloads through JSON, issue one HTTP request
  per record, or let REST and gRPC adapters develop different semantics.

## 4. Repository boundary

`shard-stream` should be a new repository rather than a feature inside
`shard-kv`. The initial repository can be bootstrapped from a pinned
`shard-kv` commit, then pruned and renamed.

Suggested workspace layout:

```text
shard-stream/
  crates/
    shard-stream-core/
    shard-stream-protocol/
    shard-stream-storage/
    shard-stream-server/
    shard-stream-client-rs/
  proto/
  docs/
  benchmarks/
  examples/
```

### Code to reuse and adapt

- Shard worker ownership, direct shard routing, and single-owner state
  mutation.
- WAL frame headers, CRC validation, compression, segment rotation, and
  recovery mechanics, adapted to an append-only extent-pack format.
- Local filesystem and S3-compatible object-store adapters.
- Bounded queues, backpressure, shutdown, metrics, and health snapshots.
- Direct-shard transport, TLS/mTLS, authentication, and connection lifecycle.
- SCNP's bounded length-prefixed framing, incremental decode, route validation,
  and malformed-input test patterns, adapted to stream batches rather than KV
  opcodes. The SCNP wire format itself is not copied.
- Replication framing, snapshot transfer, and transport hardening where the
  record model fits.
- Blossom bridge mechanics for canonical claims, supermajority finality,
  certificate verification, fencing, bounded state, deadlines, checksummed
  persistence, and restart recovery.
- Deterministic test helpers and benchmark harness patterns.

The stream implementation should preserve the useful pattern already present
in `shard-kv`: asynchronous ingress at the edge, bounded multi-producer queues
at shard boundaries, one owner consuming each shard queue, and one-shot
responses for individual operations. It must not copy unbounded queue choices
into the new data path.

### Code to leave behind

- `FlatMap` point-key mutation semantics.
- TTL, LRU/LFU eviction, and KV overflow behavior.
- Redis/RESP command families.
- Vector, semantic-cache, and Redis module subsystems.
- Point-value active-active conflict handling.

The Blossom-derived module must also leave behind KV-specific conflict ranks
and values. It should finalize stream ownership claims and epochs, not resolve
concurrent writes to a partition.

Every copied module should be tracked in an `UPSTREAM.md` file with its source
commit, original path, destination path, and whether future synchronization is
expected. Both repositories should retain Apache-2.0 attribution and license
information.

## 5. Correctness invariants

These invariants are part of the storage contract and should be encoded in
types, assertions, recovery tests, and the on-disk format:

1. An admitted producer append batch is the indivisible placement unit. A Kafka
   `RecordBatch` is never split across shard lanes.
2. One admitted batch advances the topic ring once. Network requests containing
   several batches advance it once per batch, not once per request or record.
3. The active shard extent pack is the payload WAL. Payload bytes are not first
   written to one WAL and then copied into a second segment log.
4. Logical identifiers are tentative until a complete extent reaches the
   requested durability. A failed unpersisted tail allocation may be reused.
5. Every durable allocation is represented by a self-describing extent.
   Recovery merges extents by logical offset and infers interior abort ranges
   from later batch, placement, and offset sequences, so later ordered data
   cannot remain hidden behind an unexplained gap.
6. A committed data extent, including optional producer identity and XXH3-128
   payload retry fingerprint, is sufficient to rebuild all
   derived indexes after a crash.
7. Ordered visibility advances only through contiguous committed or aborted
   ranges. Pipelined completion order is never confused with logical order.
8. Every append carries immutable ring and leader epochs. A lane rejects an
   append whose ownership certificate or epoch is stale.
9. Immutable manifest generations, not object-store listing, are the authority
   for remote data.
10. Blossom finalizes control-plane ownership and fencing decisions only.
    Direct lane replication determines data durability.

The internal model uses `u128` reservation, placement, and native logical
identifiers. A partition's per-record offset is represented by a batch base
offset plus its record delta; it is not stored as a separate KV key. The Kafka
adapter exposes signed 64-bit offsets and must refuse new Kafka appends or
migrate the partition before that representation is exhausted.

## 6. Placement and append protocol

### 6.1 Roles and ring placement

Each topic has a durable ring revision:

```text
placement_seq = next per-topic placement sequence
target_shard  = ring[placement_seq % ring.len()]
```

A lightweight MPSC-owned topic sequencer owns `placement_seq` and the ring epoch. A
logical partition coordinator owns that partition's offset allocator, producer
sequence table, watermarks, and ordered visibility. Version 1 should colocate
the topic sequencer and all of that topic's partition-control state on one
coordinator worker so reservation is a single-owner operation, while payload
work is distributed to every shard. Coordinator workers are spread across
cores and multiplex many topics; there is not one thread or queue per
partition.

The sequencer determines placement, not completion order. A producer can
pipeline batches to shards 0, 1, 2, and 3; shard 2 may finish first. The ordered
Kafka view still waits for every earlier partition range to be committed or
explicitly aborted. The native API may opt into completion-order delivery.

Ring changes create a new immutable `ring_epoch`. New batches use the new ring;
old batches remain addressed by the epoch and shard recorded in their extent.
Scale-out therefore does not require rewriting cold packs merely to rebalance
new writes.

For exact round-robin placement, the sequencer peeks at its next sequence to
select a candidate lane, reserves that lane's capacity, and only then commits
and advances the sequence. If that lane has no capacity, the topic is
backpressured; version 1 does not silently skip to a less-loaded lane because
that would change the placement contract. The scheduler must not hold a worker
thread while waiting for the asynchronous permit.

### 6.2 Canonical append envelope

Each lane stores one canonical `AppendBatch` extent containing enough metadata
to recover without a separate exact-record index:

```text
format_version
extent_type: DATA | ABORT | CONTROL
cluster_id
topic_id
logical_partition_id
reservation_id: u128
first_logical_offset: u128
record_count
placement_seq: u128
ring_epoch
target_shard
shard_local_sequence
leader_epoch
ownership_certificate_digest
producer_id / producer_epoch / base_sequence   # optional by protocol
first_timestamp / max_timestamp
attributes / codec / transactional flags
payload_length
header_checksum / payload_checksum
payload
```

Leader, ring, and producer epoch fields belong in format version 1 even if some
features ship later. That avoids an on-disk rewrite when HA, idempotence, or
transactions are enabled.

For Kafka, the payload remains an intact Kafka `RecordBatch` wherever possible.
This preserves its CRC, compression unit, producer sequencing, and
transactional boundaries and enables zero-copy handoff. Native batches use an
equivalent immutable framing contract.

Use `Bytes` ownership through protocol, engine, storage, and replica dispatch
so an admitted immutable batch is not recopied between layers. At asynchronous
socket boundaries, `bytes-handoff` may retain incomplete input tails and queue
bounded output frames while I/O progresses. It does not replace the
byte-accounted shard command channels or their single-owner sequencing.

### 6.3 Admission and atomic recovery

The append path is:

1. Decode and validate the batch, including size, codec, producer epoch, and
   sequence.
2. Peek at the next placement sequence, select its candidate lane, and
   asynchronously reserve both a queue slot and byte-budget permit there.
   Oversized batches fail before identifier allocation.
3. The partition's single MPSC owner assigns the placement sequence, logical
   offset range, batch ID, candidate lane, and ring epoch without a shared
   mutex or disk operation.
4. Enqueue an immutable, reference-counted `AppendBatch` to the lane's bounded
   MPSC append queue.

Partition batch metadata uses the standalone MIT-licensed
`ordered-segment-map` crate. Dense monotonic batch IDs map directly to
segmented ordinals without hashing; the segments retain insertion/logical
order and allow retention to discard complete prefixes without shifting live
entries. Sparse identities use a separate prehashed `HashTable` to stable
ordinals. The coordinator must not put dense batch IDs in a general-purpose
`HashMap` or pointer-heavy ordered tree.
5. The single lane owner drains a bounded group, appends each self-describing
   extent once, and performs one shard-local fsync for the group.
6. Persistent replica workers copy the same reference-counted payload and
   flush followers concurrently. Leader durability replies after the lane
   fsync; quorum durability replies after the configured number of distinct
   replica fsyncs, without waiting for surplus followers. Later follower
   completions update replicated watermarks through partition-routed MPSC
   progress messages. No payload or completion waits for a global journal
   order.
7. Completion returns to the partition owner, advances any contiguous watermarks,
   updates derived in-memory directories, and resolves the producer reply when
   its requested durability has been met.

The checksummed coordinator journal is control-only: it persists ring revisions
and retention/checkpoint state, not reservations or append completions. There is
therefore no global per-append mutex, journal write, or fsync generation.

Recovery merges scanned data extents from all shard lanes:

| Crash point | Recovery result |
| --- | --- |
| Before a complete data extent and with no later extent | No durable append existed; the trailing tentative allocation may be reused |
| Missing extent followed by a later valid extent | Infer an explicit abort range from the later batch/offset/placement sequence |
| After extent durability, before completion callback | Extent scan proves commit |
| During a torn extent write | Checksum/length validation truncates the suffix; a later extent implies an abort, otherwise the tail disappears |
| After an ambiguous client timeout | Producer identity and sequence deduplicate the retry |

There is no distributed payload/index transaction: the canonical extent proves
data commit and all indexes are rebuildable. Batch IDs and placement sequences
advance together, allowing recovery to reject inconsistent or non-round-robin
extents. A bounded gap window limits outstanding tentative ranges. If an early
lane stalls, the coordinator stops admitting later ranges rather than allowing
unbounded memory growth or visibility lag.

## 7. Bounded MPSC execution model

The network edge is asynchronous, but every mutable shard state has one owner.
MPSC channels provide the cross-shard handoff:

```text
producer connections
        |
 shared topic/partition coordinators
        |
 reserve lane slot + bytes
        |
 per-shard bounded append MPSC ---> one shard writer ---> replica MPSC

fetch requests ---> planner ---> per-shard bounded fetch MPSC
                              ---> one-shot / bounded response channels
                              ---> ordered reassembly
```

Required queue behavior:

- Separate append, fetch, replication, and control/maintenance queues so remote
  reads, compaction, or replication cannot starve producer appends.
- Enforce limits in both batch count and bytes. A small number of huge batches
  must not bypass backpressure.
- Use weighted fair service with reserved control capacity; shutdown, fencing,
  and lease renewal must remain schedulable under overload.
- Transfer payloads as immutable reference-counted byte buffers. Do not copy
  when moving from protocol ingress to a lane or replica.
- Use one-shot replies for ordinary append/fetch calls and bounded response
  MPSC channels for streaming native fetches.
- Attach deadlines and cancellation tokens. A disconnected or slow consumer
  must not block the shard owner.
- Record queue depth, used bytes, admission wait, rejected admissions, service
  time, cancellations, and per-class starvation.

The channel crate is not part of the public API. Benchmark bounded
`crossbeam_channel`, `flume`, and Tokio MPSC under the actual dedicated-worker
and async-edge workloads. A likely starting point is bounded crossbeam channels
for dedicated shard threads, bounded flume or Tokio channels at async edges,
and Tokio one-shots for replies, matching the useful patterns already present
in `shard-kv`.

A native client may carry a ring routing hint, but it cannot allocate arbitrary
offsets. It still needs a coordinator-issued reservation or lease and must be
fenced by ring and leader epoch.

## 8. Storage, indexing, and tiering

### 8.1 Shard-local extent packs

Each shard owner appends all admitted batches to a shard-local active extent
pack. The pack is the WAL and becomes immutable when sealed. Memory is a write
buffer and read cache; local SSD contains the active durable log. This avoids
double-writing payloads while retaining sequential per-shard I/O.

Residency and acknowledgement are separate. Hot pages and directories remain
in memory; pressure evicts sealed data to SSD and, after verified upload, to
object storage. The native engine distinguishes `accepted`, `memory`,
`local_append`, `local_fsync`, `replicated`, and `object_durable`. Memory-only
or admission-only acknowledgement is explicitly lossy across failure and is
never presented as Kafka `acks=all`.

A pack may contain extents from many topics and logical partitions. Each sealed
pack has a checksummed footer or immutable sidecar directory mapping:

```text
(topic, partition, logical offset range, placement range)
    -> (pack_id, byte range, extent checksum)
```

This is a batch/range directory, not a contradictory "sparse exact-record
index." Within a batch, the Kafka or native batch framing locates records.
Sparse timestamp and offset checkpoints accelerate seeks without creating one
heavy index entry per record.

Co-packing is allowed only inside a compatible policy domain. Topics requiring
different replica sets, encryption keys, storage regions, tenant isolation, or
deletion guarantees use separate lazy pack streams even when they share a
physical shard. Otherwise a shared pack could silently weaken a topic's
durability or security policy. The number of simultaneously active policy
domains is bounded and measured because excessive domains would recreate
per-partition segment overhead.

On restart, scan the valid extent prefix, truncate a torn suffix, rebuild the
directory, and reconcile reservations. Periodic checksummed checkpoints bound
scan time but are never the sole source of truth.

### 8.2 Retention and pack garbage collection

Retention is logical by topic and partition but physical packs are shared:

- Expiring a logical range removes its extent reference in a new manifest
  generation.
- Delete a local or remote pack only when no live manifest references remain
  and the garbage-collection grace period has elapsed.
- Repack a low-live-ratio pack with bounded background I/O, write and verify the
  replacement, publish the new manifest generation, and only then retire the
  old pack.
- Keep compaction and repacking below explicit CPU, disk, network, and queue
  budgets so foreground work remains isolated.

Policies include maximum age, maximum logical bytes, minimum retained offset,
and optional local-tier byte/age limits. Consumer group offset commits are
metadata and may point below the log start. The commit remains stored; a later
fetch returns the Kafka-compatible out-of-range/reset behavior.

### 8.3 Object storage and manifests

When a pack seals, the engine:

1. finalizes and checksums its footer;
2. fsyncs the local pack;
3. uploads an immutable object;
4. verifies object length and checksum;
5. publishes a new manifest generation with compare-and-swap or an equivalent
   single-writer conditional update;
6. reclaims local bytes only after policy and durability requirements permit.

If the remote tier is unavailable and local SSD reaches its protected
high-water mark, admission is backpressured or rejected. The engine never
overwrites a live local pack or marks an unverified remote object durable to
stay available.

The manifest records pack and extent ranges, ring and leader epochs, checksums,
codec, retention state, object generation, and storage backend. Bucket listing
is never recovery authority. Unreferenced objects are collected after a grace
period so a crashed publisher or stale reader cannot cause premature deletion.

Storage-provider or bucket migration is a supported state machine:

1. mark the destination and migration generation;
2. dual-write new sealed packs;
3. copy old live packs;
4. verify destination checksums;
5. atomically cut the authoritative manifest to the destination;
6. retain the source through a rollback grace period.

Remote fetch groups needed extents by object, issues bounded parallel range
reads across objects and partitions, and reassembles in logical offset order.
One slow remote partition must not serialize all fetches in the request.

### 8.4 Log compaction and tombstones

Keyed log compaction is a design target, not a first-storage shortcut. A
compactor reads eligible extents, preserves Kafka offset/tombstone semantics,
writes replacement packs, and atomically swaps references through a manifest
generation. The same process works for local and remote tiers. Compacted gaps
remain valid offsets; they do not renumber the logical log.

## 9. Logical partitions and visibility

Logical partitions are ordered views over batch extents distributed across
physical lanes. A partition directory maps an offset range to its placement
sequence, shard, pack, and byte range. Fetch planning groups ranges by shard or
remote object, reads them in parallel, then reassembles batches by logical
offset.

Each partition publishes distinct watermarks:

- `log_start_offset`: earliest retained logical offset;
- `allocated_end_offset`: internal end of all tentative in-flight and durable
  ranges;
- `log_end_offset`: end of the highest contiguous locally committed or aborted
  range;
- `high_watermark`: end of the highest contiguous range satisfying the
  configured replica/ISR durability;
- `last_stable_offset`: transactionally stable read boundary.

An out-of-order completion bitmap tracks the bounded window between the first
unresolved reservation and later completions. It is checkpointed but
rebuildable. Ordered Kafka fetch stops at the appropriate contiguous
watermark. Native `AS_AVAILABLE` fetch is a separate contract that may return
completed batches beyond a gap together with their logical identifiers.

Lag should be observable in records, bytes, and time, with physical lane lag,
replica lag, and remote-read wait separated from consumer logical lag.

## 10. Replication and HA with Blossom coordination

### 10.1 Data-plane replication

Shard lanes replicate canonical extents directly to their followers. Followers
validate checksums, reservation identity, ring epoch, leader epoch, and
ownership-certificate digest before append. Replica acknowledgements and ISR
membership determine replicated durability; Blossom does not.

Control-plane checkpoints are replicated or snapshotted to the coordinator
standby. Producer identity is embedded in canonical extents, which remain the
final evidence of committed payload and allow a stale derived checkpoint to be
reconciled by scanning later extents.

The initial profile is active-passive for each ordered logical partition.
Active-active means different topics or partitions can have different active
nodes at the same time. One partition still has one fenced coordinator/leader.

### 10.2 Blossom control plane

Adapt the bounded `shardmap-blossom-bridge` coordination pattern to finalize
rare ambiguous control-plane claims:

- logical partition coordinator ownership;
- monotonically increasing leader epochs and fencing;
- topic ring membership and `ring_epoch` revisions;
- replica membership and promotion;
- optionally, manifest-authority transfer.

A canonical `StreamOwnershipClaim` should include:

```text
cluster_configuration_digest
topic_id / logical_partition_id
candidate_leader
previous_leader_epoch / proposed_leader_epoch
ring_epoch / replica_membership_revision
caught_up_high_watermark_digest
leadership_lease_term
claim_nonce
```

The finalized certificate must be verified for the expected group, nonce,
validator generation, claim inclusion, supermajority, hash chain, signatures,
and monotonic epoch. Persist accepted ranks/certificates atomically with
checksums and bounded history, following the existing bridge's defensive
mechanics. The caught-up watermark must be supported by replica receipts; a
candidate's self-reported digest is not sufficient proof of safe promotion.

Failover is ordered:

1. A candidate catches up data and proves its durable watermark.
2. It submits an ownership/promotion claim.
3. The cluster verifies a finalized Blossom certificate.
4. The new leader publishes the higher epoch and certificate digest.
5. Every lane fences the old epoch before accepting new appends.
6. The promoted coordinator resumes reservations and materializes abort ranges
   for any prior reserved-but-missing batches.

No producer payload, consumer fetch, or per-batch append waits for Blossom
finality. Version 1 fails closed for writes: admission requires an unexpired
quorum-backed leadership lease tied to the finalized certificate. A leader
that cannot renew before the deadline, including the allowed clock-skew margin,
stops new reservations; reads may continue. Blossom unavailability blocks
ownership changes and ring revisions and eventually stops writes when the
current lease expires. A simpler manually assigned single-site mode may omit
HA, but must not advertise split-brain protection.

## 11. Kafka compatibility

The Kafka listener should be an adapter over the logical partition layer, not
the storage engine. Kafka clients negotiate API versions using `ApiVersions` and
then use versioned request/response APIs. Compatibility means matching request
versions, error codes, offset and watermark behavior, record-batch validation,
producer sequencing, group state transitions, authentication, and timeouts—not
merely accepting Kafka-shaped frames. The official protocol documents the core
APIs and version negotiation at
<https://kafka.apache.org/41/design/protocol/>.

### Compatibility phases

#### Phase A: producer and consumer baseline

- `ApiVersions`
- `Metadata`
- `Produce`
- `Fetch`
- `ListOffsets`
- `FindCoordinator`
- `JoinGroup`
- `SyncGroup`
- `Heartbeat`
- `LeaveGroup`
- `OffsetCommit`
- `OffsetFetch`
- `InitProducerId`

`InitProducerId`, producer epochs, and per-partition sequence validation belong
in the baseline because modern Kafka producers enable idempotence by default
when their settings permit it. Transaction coordination can remain later, but
a producer should not have to discover that it must disable its default safety
behavior to connect. See the official
[producer configuration](https://kafka.apache.org/43/configuration/producer-configs/).

Phase A stores intact Kafka RecordBatch v2 batches and supports the common
`none`, gzip, Snappy, LZ4, and Zstandard codecs. Codec support is part of the
published matrix. The broker should avoid decompression on the hot append path
unless validation, quotas, or compaction requires it.

#### Phase B: operational compatibility

- `CreateTopics`, `DeleteTopics`, and topic descriptions;
- broker, topic, group, and configuration description APIs selected by the
  client matrix;
- broker and partition error mapping;
- TLS and SASL profiles used by supported clients;
- configurable metadata and coordinator behavior;
- multiple SCRAM mechanisms where required, per-principal quotas,
  authorization, and administrative behavior required by target deployments;
- the newer incremental consumer group protocol when justified by the selected
  clients.

#### Phase C: transactions and broader compatibility

- transactions and transactional offset commits;
- transaction-marker recovery and `last_stable_offset`;
- additional admin APIs, group versions, authentication profiles, and
  ecosystem integrations based on measured demand.

Before claiming a Kafka-compatible release, check in a machine-readable matrix
covering:

- every API key and min/max version;
- tested Java, librdkafka, Go, and Rust client versions;
- RecordBatch versions and codecs;
- producer idempotence and retry cases;
- classic and newer consumer-group protocols;
- TLS/SASL mechanisms and authorization;
- error-code, timeout, retention, and offset-reset behavior;
- explicitly unsupported transactions, quotas, or admin operations.

### Kafka acknowledgement and offset semantics

The adapter maps acknowledgements explicitly:

- `acks=0`: no response; admission or later failure is unreported.
- `acks=1`: respond after the selected shard leader has appended the complete
  extent according to the documented leader-local flush policy.
- `acks=all`/`-1`: respond only after the extent satisfies the configured
  in-sync replica policy and `min.insync.replicas`.

Object-store upload is not part of `acks=all`. `object_durable` is a separate
native durability option and may later be exposed through a documented
extension.

Committed consumer offsets are independent metadata. An offset commit below
`log_start_offset` is retained rather than rejected. A subsequent fetch gets
the compatible out-of-range response, allowing the client's configured reset
policy to choose earliest, latest, or failure.

Kafka exposes signed 64-bit offsets. Native clients may use full `u128`
identifiers, while the adapter provides an `i64` view and rejects new Kafka
appends or migrates a logical partition before that view is exhausted.

## 12. Native engine API and protocol

A typed internal native API and benchmark harness should exist from Milestone
1, before Kafka parsing shapes the engine. The networked native protocol can
follow after the storage and compatibility model stabilizes.

### 12.1 Protocol decision: REST over HTTP/3 plus gRPC

Support both surfaces, with one semantic API and distinct roles:

| Surface | Transport and encoding | Role |
| --- | --- | --- |
| Native REST/streaming HTTP API | HTTP/3 over QUIC and TLS 1.3; Protobuf binary frames on the hot path; JSON for control and diagnostics | Primary producer, consumer, direct-routing, and admin interface |
| Standard gRPC adapter | Standard gRPC over HTTP/2; the same Protobuf messages and operation semantics | Generated multi-language clients, service integration, and fallback when UDP/HTTP/3 is unavailable |
| Kafka adapter | Kafka protocol over TCP/TLS | Existing Kafka ecosystem compatibility |

This is intentionally not described as "gRPC over HTTP/3." The gRPC abstract
model has streaming RPCs, but its interoperable transport and common runtimes
remain HTTP/2-based. The Rust tonic implementation likewise identifies itself
as gRPC over HTTP/2. A private HTTP/3 mapping would lose the main reason to
offer gRPC: standard clients. Revisit native gRPC-over-HTTP/3 only after the
official protocol and target runtimes provide cross-language interoperability.

References:

- HTTP/3 supplies secure QUIC transport, independent request streams, and
  per-stream flow control:
  <https://www.rfc-editor.org/rfc/rfc9114.html>.
- gRPC documents its bidirectional streaming transport as HTTP/2-based:
  <https://grpc.io/about/>.
- tonic currently describes itself as a gRPC-over-HTTP/2 implementation:
  <https://github.com/hyperium/tonic>.
- OpenAPI 3.2 can describe HTTP APIs and sequential streaming media types:
  <https://spec.openapis.org/oas/latest.html>.

### 12.2 One schema and semantics

The repository should contain versioned Protobuf messages and services for:

- append batches and batch acknowledgements;
- ordered and `AS_AVAILABLE` fetches;
- consumer sessions and offset commits;
- topic, logical partition, ring, retention, and tier administration;
- health, topology, queue pressure, and failover metadata.

The `.proto` package is the canonical field-number and binary-compatibility
contract. HTTP annotations define REST resource mappings, and generated
OpenAPI 3.2 output is checked in for documentation, REST client generation, and
conformance tests. CI must fail if generated Protobuf descriptors, OpenAPI, or
gRPC bindings differ from committed artifacts.

The REST and gRPC handlers translate into the same internal typed commands.
They do not independently implement offset allocation, validation, admission,
or error policy. Every engine error has one stable typed error code plus
documented HTTP status, gRPC status, retryability, and Kafka error mappings.

Protocol evolution rules:

- never reuse Protobuf field numbers or enum values;
- reserve removed fields and values;
- add capabilities through a version/capability handshake rather than
  connection heuristics;
- bound unknown-field, nesting, repeated-field, decompression, and frame sizes;
- publish minimum client/server versions and downgrade behavior;
- preserve unknown fields where a proxy or gateway promises transparent
  forwarding.

### 12.3 REST-shaped HTTP/3 API

Resource-oriented unary operations cover ordinary use and administration:

```text
POST   /v1/topics
GET    /v1/topics/{topic}
DELETE /v1/topics/{topic}
POST   /v1/topics/{topic}/partitions/{partition}/records
GET    /v1/topics/{topic}/partitions/{partition}/records?offset=...
POST   /v1/consumer-groups/{group}/offsets:commit
GET    /v1/consumer-groups/{group}/offsets
GET    /v1/topology
GET    /v1/health
```

Control and diagnostic calls accept and return `application/json`. Producer and
fetch calls default to `application/shard-stream+protobuf`. One REST append
contains one complete native append batch, not one record. Kafka RecordBatch
bytes may be carried opaquely when the caller intentionally uses Kafka batch
framing.

An explicitly bounded JSON convenience profile may be offered for low-volume
append and fetch debugging, with binary payloads base64-encoded. It is not the
SDK default and is excluded from performance claims because its allocations
and wire expansion are intentional tradeoffs.

High-throughput clients use long-lived streaming operations:

```text
POST /v1/streams/produce
POST /v1/streams/fetch
```

Their bodies use
`application/shard-stream-seq+protobuf`: a bounded length-delimited sequence of
typed frames. A producer stream carries many append batches, and its response
body carries acknowledgements identified by `request_id`, allowing pipelined
requests and out-of-completion-order replies. A fetch stream carries records,
watermarks, rebalance notices, and terminal errors without creating one HTTP
request per batch.

Use separate HTTP/3 request streams for independent producer sessions, fetch
sessions, and control work. QUIC prevents a lost packet for one stream from
blocking delivery on an unrelated stream, but connection congestion control is
still shared. QUIC and HTTP flow-control windows are therefore bounded and
coordinated with the application queue byte permits; transport buffering must
not become an unaccounted second queue.

Within one reliable QUIC stream, a lost byte still delays later bytes on that
stream. The SDK may maintain a small bounded pool of producer streams so one
loss event or oversized batch does not stall every pipelined batch. Pooling
must preserve producer sequence validation and cannot reorder records inside a
logical append batch.

The native surface provides:

- direct coordinator, shard, or lane routing hints;
- `u128` placement and logical identifiers;
- append/fetch batches with bounded byte budgets;
- zero-copy payload handoff after frame validation;
- explicit durability and tier hints;
- `AS_AVAILABLE` versus ordered fetch modes;
- per-batch acknowledgements, retry hints, and backpressure state;
- topology, ring epoch, ownership certificate, and failover metadata.

### 12.4 HTTP/3 safety and operations

- Require TLS 1.3 and negotiate HTTP/3 with standard ALPN. Support mTLS and
  bearer-token authentication without binding identity to the peer's UDP
  four-tuple, because QUIC connection migration can change addresses.
- Reject mutating requests received as QUIC 0-RTT/HTTP early data with HTTP
  `425 Too Early`. Producer sequence deduplication is still required, but it is
  not a substitute for transport replay protection.
- Do not use unreliable QUIC or HTTP datagrams for records, acknowledgements,
  leases, commits, or fencing. Correctness-bearing messages use reliable
  streams.
- Map admission pressure to typed errors and HTTP `429` with a bounded
  `Retry-After`; use `413` for oversized batches, `409` for stale epochs or
  conflicting state, and `503` for unavailable durability. The typed error
  body, not the status phrase, is authoritative.
- Set per-connection and per-stream limits for concurrent streams, bytes,
  header sizes, idle time, acknowledgement backlog, and unread consumer data.
- A slow or abandoned response stream triggers cancellation and releases
  application queue permits without cancelling an append whose canonical
  extent already crossed its requested durability boundary.
- Dynamic fetch and topology responses use explicit no-store cache policy.
  Mutating or streaming calls do not rely on automatic HTTP redirects: a
  `NOT_COORDINATOR` error returns the signed topology epoch and endpoint, and
  the SDK retries with the same idempotency identity.
- Deployment conformance must detect intermediaries that buffer request or
  response bodies, terminate H3 incorrectly, cap long-lived streams, or strip
  trailers and authentication metadata.
- Advertise HTTP/3 using the deployment's supported discovery mechanism and
  expose metrics for H3 negotiation, handshake latency, 0-RTT rejection,
  stream resets, connection migration, flow-control stalls, and fallback.

The official native SDK first attempts HTTP/3. If UDP is blocked or H3
negotiation fails within a bounded policy, it may use the standard gRPC/HTTP/2
adapter while preserving request IDs, producer identity, retries, durability,
and error semantics. Fallback must be observable and configurable; it must not
silently weaken durability or authentication.

### 12.5 gRPC adapter

Generate unary, client-streaming, server-streaming, and bidirectional-streaming
gRPC methods from the same Protobuf package. The adapter is useful for broad
language support, infrastructure integration, reflection, health checking, and
networks that do not pass QUIC.

The gRPC path is not a second engine and should not sit in front of the native
HTTP/3 path as an internal proxy. Both terminate into the same admission and
coordinator APIs. Benchmark gRPC independently; do not assume its HTTP/2
connection behavior, flow control, batching, or allocation profile matches
HTTP/3.

The native Rust client should expose typed producer, consumer, admin, and health
APIs and hide transport selection unless the caller requests strict H3. A
direct client may use coordinator-issued placement leases in a future version,
but leader and ring fencing remain mandatory. Kafka translation must not
constrain the native types or completion-order mode.

## 13. Product opportunities and competitor lessons

The differentiator is not simply "Kafka-compatible but faster." It is the
decoupling of logical partitions from physical execution and storage
placement:

| Existing operational complaint | `shard-stream` design response | Required proof |
| --- | --- | --- |
| Kafka scale-out commonly involves partition reassignment and data movement | Advance the topic ring epoch for new batches; retain old extent locations and avoid moving cold object-backed data | Scale from N to 2N shards while serving old and new epochs |
| Kafka partition count cannot be reduced and increasing it changes default keyed routing | Keep client-visible logical partitions stable while independently changing the physical lane ring | Stable key-to-logical-partition behavior across ring changes |
| A partition traditionally maps to one leader execution path | Stripe successive batches from one logical partition across shard lanes | One-partition throughput scaling without violating ordered fetch |
| Per-partition replicas carry meaningful fixed memory and core overhead | Multiplex idle partitions over shared coordinators, bounded queues, packs, and range indexes | Memory slope for 10K, 100K, and 1M idle partitions |
| Tiered storage can be vendor- or backend-constrained and difficult to migrate | Ship an open object-store interface, immutable portable manifests, verified migration, and rollback generations | Live bucket/provider migration with fault injection |
| Remote fetch can serialize work per remote partition | Plan extent reads across shards/objects concurrently under byte and request budgets | Mixed hot/cold multi-partition fetch tail latency |
| Kafka-compatible brokers can differ on authentication, quotas, transactions, and edge-case errors | Publish exact conformance matrices and prioritize multiple SCRAM mechanisms, per-principal quotas, idempotence, and transaction parity | Cross-client protocol and failure suites |
| Lag is often reduced to an offset count | Expose lag in records, bytes, time, physical lane, replica, queue, and remote tier | Operators can attribute a slow consumer to the correct layer |

### 13.1 Broader market audit

The final product audit covers both self-managed engines and managed or
object-first services. It does not imply that every product serves the same
workload. It identifies mature features, recurring operational friction, and
benchmarks that `shard-stream` should either adopt or deliberately decline.

| Product or family | Strength worth preserving | Friction or request seen in documentation/community | `shard-stream` response |
| --- | --- | --- | --- |
| Apache Pulsar | Multi-tenancy, delayed delivery, retry/DLQ, geo-replication, and very high topic counts | Brokers, BookKeeper, and metadata services increase deployment and upgrade surface; delayed records can pin later ledgers; per-topic metrics can produce extreme cardinality | Keep one broker/storage binary plus Blossom coordination; store timer state separately from log packs; aggregate metrics by default with budgeted drill-down |
| NATS JetStream | Simple subject model, work-queue and interest retention, fast fan-out | Operators request server-side ingress rate limits; compressed versus logical storage accounting is surprising; acknowledgement and wildcard edge cases recur | Add hierarchical admission control, explicit physical/logical byte accounting, and model-based acknowledgement/filter tests |
| RabbitMQ Streams | Super streams, producer deduplication, filtering, and familiar queue semantics | Read-ahead needs workload tuning; deduplication based only on producer name/highest ID is unsafe for concurrent producers; protocol conversion can add hot-path work | Use adaptive byte-bounded prefetch, producer epochs/fencing, and opaque payloads with no format conversion in the storage path |
| Apache RocketMQ | Delayed delivery, retry policy, DLQ, per-message-group FIFO, filtering, transactions, and tracing | Same-time delayed messages can create a thundering herd; retry policy and transactional callbacks add operational state | Add a smoothed scheduler, explicit retry/DLQ state, per-key sequence lanes, and end-to-end trace phases; defer application transactions |
| Redis Streams | Very simple append/range API, consumer groups, claims, replay, and approximate trimming | Memory and pending-entry accounting can be hard to reason about; multi-group deletion semantics needed later commands; active-active reads and consumer metadata have documented caveats | Make resident, retained, pending, and remote bytes exact; make group retention references explicit; fence geo checkpoints and avoid promising unfenced global order |
| Pravega | Stream cuts, retention references, transactions, and automatic segment scaling | A controller/segment-store abstraction is powerful but adds a separate control architecture | Adopt immutable stream cuts and retention pins while keeping ring epochs and Blossom as the existing control model |
| Kinesis, Google Pub/Sub, and Azure Event Hubs | Managed elasticity, integrated identity, and low-operations consumption | Throughput units, shard/partition limits, per-ordering-key limits, retention ceilings, and fan-out quotas leak into application design | Expose natural-unit quotas, decouple physical capacity from logical partitions, allow object-backed long retention, and make fan-out a resource policy rather than a topology change |
| Confluent Cloud | Complete Kafka ecosystem and mature managed operations | Hot partitions still require application partitioning or more provisioned capacity; capacity changes and region/provider moves are not instantaneous | Stripe a hot logical partition across lanes, admit new ring capacity without rewriting history, and keep manifests provider-portable |
| AutoMQ | Shared object-backed streams, WAL coalescing, dynamic sharding, and Kafka compatibility | Shared object packs and dynamic sharding are already market features, not unique claims | Compete on single-log striping, predictable hot latency, portable manifests, open self-hosting, and failure correctness |
| WarpStream | Stateless object-storage-first agents and operational simplicity | Standard object-storage durability trades latency for simplicity; published standard-object p99 is much higher than local replicated logs | Preserve a low-latency replicated hot path while asynchronously offloading immutable packs |
| Bufstream | Kafka compatibility, any-broker partition intake, schema validation, and Iceberg integration | Broker-side contracts and direct analytical access raise the expected product baseline | Add schema contracts before arbitrary transformations and provide an optional governed lakehouse export |

The audit leads to four product principles:

1. Physical elasticity must not force users to choose more logical partitions,
   change keys, or buy opaque throughput units.
2. Advanced delivery features must be separate indexed views or policy layers.
   Delayed delivery, retries, pending entries, filters, and checkpoints must not
   pin unrelated packs or corrupt base-log retention.
3. Every resource limit must be enforceable and explainable in bytes, records,
   operations, time, or currency. A compressed byte, logical byte, resident
   byte, replicated byte, and remotely retained byte are different facts.
4. Compatibility and convenience features share one engine contract. Kafka,
   HTTP/3, gRPC, future queue adapters, and lakehouse exports must not acquire
   conflicting durability or ordering semantics.

### 13.2 Community-driven requirements

The following requirements are promoted into the architecture because they
address repeated requests or failure modes across multiple products.

#### P0: design into the core formats and control plane

- Add tenant and namespace policy domains above topics. Authentication
  principals map to one or more policy domains rather than directly to queues.
- Enforce hierarchical cluster, tenant, namespace, topic, producer, and
  consumer limits for ingress bytes/records, egress bytes/records, in-flight
  bytes, retained bytes, connections, streams, and object requests.
- Apply overload as bounded backpressure with typed retry hints. Never silently
  drop an admitted durable append, and never allow an overloaded data class to
  starve fencing, replication acknowledgements, or lease renewal.
- Track logical payload bytes, compressed bytes, wire bytes, resident bytes,
  SSD bytes, object bytes, replication bytes, read amplification, write
  amplification, and object request counts separately. Quotas and billing
  views state which measurement they use.
- Export low-cardinality cluster/tenant/policy-class metrics by default.
  Per-topic, partition, producer, consumer, and lane drill-down is opt-in,
  time-bounded, and protected by a cardinality budget.
- Back aggregate hot-path counters with sharded `fast-telemetry` cells and
  export snapshots out of band. Metrics must not introduce a contended global
  atomic or lock into append, replication, or fetch.
- Route lifecycle, control-plane, and sampled error events through
  `eden_logger` with compile-time level gating. Never synchronously log every
  record or append; detailed hot-path visibility comes from bounded metrics and
  sampling.
- Add immutable named stream cuts: a consistent set of partition offsets and
  ring epochs that can reproduce a read, seed a consumer checkpoint, or pin
  retention. Pins consume an explicit storage quota and expire unless renewed.
- Define per-key or per-message-group ordering as a versioned logical sequence
  lane above physical placement. A routing-policy change creates a new policy
  epoch and cannot silently reorder an existing key.
- Treat consumer checkpoints as fenced state. Cross-region replication states
  whether it is synchronous or asynchronous and exposes its replication lag;
  failover may redeliver but must not silently skip committed input.
- Include an end-to-end trace identity and timestamps for admission,
  reservation, lane append, replication, visibility, tier upload, fetch, and
  consumer delivery without requiring payload inspection. Propagate that
  identity into structured logs and protocol responses where safe.

#### P1: delivery and contract features after the durable log

- Implement scheduled delivery through a durable timer index and independently
  reclaimable schedule packs. A far-future record must not retain every later
  base-log pack. Scheduling supports cancellation, rescheduling, bounded
  lateness, and burst smoothing.
- Add retry policies with exponential backoff, jitter, maximum attempts, and a
  dead-letter topic. Preserve original topic/partition/offset, producer
  identity, headers, and trace identity; do not create uncontrolled reappend
  loops.
- Offer a work-queue subscription profile in which one group member claims each
  record. Keep it distinct from Kafka log retention. Interest-based retention
  may follow only after multi-group deletion and outage semantics are proven.
- Add server-side header/tag equality filters with pack- and extent-level
  summaries. Report filter selectivity, false-positive reads, and read
  amplification. Payload SQL, user code, and arbitrary transformations are not
  part of the first implementation.
- Add a schema/contract service for Protobuf, JSON Schema, and Avro identifiers,
  compatibility policies, and optional broker-side validation. Validation is
  policy-controlled, budgeted, observable, and never requires a second payload
  encoding in storage.
- Make fetch prefetch adaptive to batch size, lag, tier, observed object
  latency, and consumer processing rate while retaining strict per-consumer
  byte limits.
- Make acknowledgement, pending-entry, retry, and DLQ counts exact or explicitly
  estimated. User records, control records, and checkpoint records use
  separate counters.

#### P2: ecosystem expansion

- Validate Kafka Connect compatibility against the published Kafka protocol
  matrix before building a proprietary connector runtime.
- Provide an optional Iceberg/Parquet table projection with schema evolution,
  deterministic offsets, and snapshot lineage. It is an export/index contract,
  not a second source of truth, until compaction and transaction semantics are
  formally aligned.
- Consider MQTT, AMQP, webhook/push, and serverless scale-to-zero modes only
  when concrete users require them. Each adapter must map to the same producer,
  consumer, retry, and durability contracts.
- Consider a transactional-outbox helper after Kafka transactions, but do not
  embed a general application transaction coordinator or stream-processing
  runtime.

### 13.3 Guardrails against product sprawl

- `shard-stream` is a durable stream engine, not a replacement for Flink,
  Materialize, RisingWave, or arbitrary user-code execution.
- Exactly-once storage effects, producer idempotence, and transactional reads
  must not be marketed as exactly-once application processing.
- Active-active operation means disjoint fenced ownership or an explicitly
  relaxed merge order. One strictly ordered partition never has two unfenced
  writers, and the normal consumer API never skips late-arriving geo records.
- Scheduled delivery and retry state never change the immutable identity of the
  source record or hold unrelated data past retention.
- Compression savings never hide logical quota use or physical storage use.
- Shared object packs, tiered storage, and Kafka compatibility are market
  expectations. Product claims must focus on measured differentiation.
- Per-topic metric labels, unbounded pending-entry lists, unbounded filters,
  and unbounded retention pins are prohibited by design.

### 13.4 Evidence and claim discipline

The claims above should stay precise:

- Kafka's documented operations explain reassignment and that partition counts
  cannot be reduced; adding partitions can affect keyed consumers:
  <https://kafka.apache.org/42/operations/basic-kafka-operations/>.
- Kafka's current tiered-storage documentation lists limitations including the
  lack of a built-in production `RemoteStorageManager`, compacted-topic
  limitations, and remote fetch bottlenecks:
  <https://kafka.apache.org/41/operations/tiered-storage/>.
- Redpanda's sizing guidance documents per-partition-replica memory and
  partition-per-core guidance, which makes low fixed idle-partition overhead a
  measurable target:
  <https://docs.redpanda.com/streaming/current/deploy/redpanda/manual/sizing/>.
- Redpanda's tiered-storage documentation describes licensing and migration
  constraints that portable manifests can address:
  <https://docs.redpanda.com/streaming/current/manage/tiered-storage/>.
- Redpanda already provides cluster balancing and tiered compaction, so neither
  should be marketed as absent there:
  <https://docs.redpanda.com/streaming/current/manage/cluster-maintenance/cluster-balancing/>.
- Kafka's newer consumer protocol improves incremental rebalancing, so avoid
  the stale claim that all Kafka rebalances are full stop-the-world events:
  <https://kafka.apache.org/42/operations/consumer-rebalance-protocol/>.
- Redpanda documents specific Kafka-client differences such as SCRAM, quota,
  and transaction behavior. `shard-stream` should compete with a tested
  compatibility matrix rather than a blanket claim:
  <https://docs.redpanda.com/streaming/current/develop/kafka-clients/>.
- Pulsar documents its broker, BookKeeper, and metadata-store architecture and
  the fact that delayed messages can prevent deletion of later ledger data:
  <https://pulsar.apache.org/docs/4.0.x/concepts-architecture-overview/> and
  <https://pulsar.apache.org/docs/4.2.x/concepts-messaging/>.
- Pulsar's upgrade procedure demonstrates the operational consequence of a
  multi-component deployment:
  <https://pulsar.apache.org/docs/next/administration-upgrade/>.
- A Pulsar/OpenTelemetry issue quantifies how per-topic metrics can grow to
  millions of time series:
  <https://github.com/open-telemetry/opentelemetry-java/issues/5105>.
- NATS documents limits, work-queue retention, interest retention, and subject
  transforms; community requests call out per-stream ingress limiting and
  surprising compressed-storage accounting:
  <https://docs.nats.io/nats-concepts/jetstream>,
  <https://github.com/nats-io/nats-server/issues/7591>, and
  <https://github.com/nats-io/nats-server/issues/6258>.
- NATS acknowledgement, pending-count, and wildcard-filter reports justify
  deterministic delivery-state and filter model testing:
  <https://github.com/nats-io/nats-server/issues/5576>,
  <https://github.com/nats-io/nats-server/issues/6093>, and
  <https://github.com/nats-io/nats-server/issues/6846>.
- RabbitMQ documents stream filtering, deduplication, super streams, and offset
  tracking; its read-ahead discussion motivates adaptive byte-bounded fetch:
  <https://www.rabbitmq.com/docs/next/streams> and
  <https://github.com/rabbitmq/rabbitmq-server/discussions/14877>.
- RocketMQ documents scheduled delivery, retry/DLQ, FIFO message groups,
  filtering, transactions, and the same-deadline burst risk:
  <https://rocketmq.apache.org/docs/featureBehavior/02delaymessage/>,
  <https://rocketmq.apache.org/docs/featureBehavior/03fifomessage/>,
  <https://rocketmq.apache.org/docs/featureBehavior/04transactionmessage/>,
  <https://rocketmq.apache.org/docs/featureBehavior/07messagefilter/>, and
  <https://rocketmq.apache.org/docs/featureBehavior/10consumerretrypolicy/>.
- Redis documents append-only streams, consumer groups, pending entries,
  replay, trimming, and newer multi-group deletion controls:
  <https://redis.io/docs/latest/develop/data-types/streams/>.
- Redis's active-active documentation explicitly describes skipped plain reads,
  possible redelivery, and consumer metadata that is not replicated. That
  supports fenced checkpoints and conservative geo claims:
  <https://redis.io/docs/latest/operate/rs/7.4/databases/active-active/develop/data-types/streams/>.
- Redis community reports around stream/consumer memory and lag visibility
  support exact pending-state accounting:
  <https://github.com/redis/redis/issues/8635> and
  <https://github.com/redis/redis/issues/8392>.
- Pravega's controller and retention documentation motivates stream cuts,
  retention references, and policy-driven scaling:
  <https://cncf.pravega.io/docs/latest/controller-service/> and
  <https://cncf.pravega.io/docs/latest/retention/>.
- Managed-service quotas make physical topology and throughput units visible to
  applications:
  <https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html>,
  <https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html>,
  <https://docs.cloud.google.com/pubsub/quotas>, and
  <https://learn.microsoft.com/azure/event-hubs/event-hubs-quotas>.
- Confluent documents hot-partition remedies, cluster capacity limits, resize
  times, and region/provider migration constraints:
  <https://docs.confluent.io/cloud/current/monitoring/monitor-performance.html>,
  <https://docs.confluent.io/cloud/current/clusters/cluster-types.html>, and
  <https://docs.confluent.io/cloud/current/clusters/cluster-faq.html>.
- AutoMQ, WarpStream, and Bufstream already document object-backed shared
  streams, stateless or any-broker intake, schema validation, and lakehouse
  integration. They belong in every relevant comparison:
  <https://docs.automq.com/automq/architecture/s3stream-shared-streaming-storage/overview>,
  <https://docs.warpstream.com/warpstream/overview/architecture>,
  <https://docs.warpstream.com/warpstream/kafka/advanced-agent-deployment-options/low-latency-clusters>,
  <https://buf.build/docs/bufstream/architecture/kafka-flow/>, and
  <https://buf.build/docs/bufstream/iceberg/>.

## 14. Implementation milestones

### Milestone 0: repository bootstrap

- Create the independent `shard-stream` repository.
- Pin the source `shard-kv` commit.
- Copy only the approved infrastructure modules.
- Add `UPSTREAM.md`, license attribution, and independent CI.
- Create the protocol crate, versioned `proto/` package, generated-artifact
  checks, and an HTTP/3 client/server feasibility spike.
- Remove KV, Redis, and unrelated feature gates.

### Milestone 1: core types, channels, and in-memory engine

- Define `AppendBatch`, reservation, ring, watermark, and ownership-certificate
  types with format-version tests.
- Expose typed internal producer, fetch, and admin APIs plus a benchmark
  harness.
- Define canonical Protobuf messages, HTTP annotations, typed error mappings,
  capability negotiation, and generated OpenAPI 3.2 artifacts.
- Implement per-topic ring configuration and ring epochs.
- Implement topic sequencing, partition coordination, and bounded per-shard
  append/fetch/control MPSC queues.
- Define tenant/namespace policy domains, hierarchical admission hooks, and
  exact logical/compressed/resident/SSD/object/replication byte counters.
- Add deterministic round-robin tests.
- Enforce slot and byte permits, cancellation, fair scheduling, and shutdown.
- Benchmark channel implementations under producer, fetch, and mixed workloads.

### Milestone 2: atomic local extent log

- Implement self-describing extents and a control-only coordinator journal.
- Define and implement immutable extent-pack framing and sealing.
- Add local SSD pack rotation, batch-range directories, checksums, and
  checkpoints.
- Add append and fetch APIs over placement identifiers.
- Validate every tentative-allocation/extent crash point, partial-pack truncation,
  deduplication, abort materialization, and index rebuild.

### Milestone 3: logical partitions and producer safety

- Add partition offsets and partition indexes.
- Add the five watermarks, bounded completion bitmap, ordered range fetch, and
  native `AS_AVAILABLE` fetch.
- Add producer IDs, epochs, sequence validation, and durable deduplication.
- Add consumer-group offset storage and commit semantics.
- Add versioned per-key sequence lanes and immutable stream cuts with
  quota-accounted retention pins.
- Benchmark parallel physical fetch plus logical reassembly.

### Milestone 4: replication, fencing, and Blossom control plane

- Add direct lane replication, ISR tracking, and minimum-replica durability.
- Adapt Blossom claim canonicalization, certificate verification, bounded
  persistence, and deadline behavior.
- Implement catch-up, promotion, higher-epoch publication, and stale-writer
  rejection.
- Test partitions, stale certificates, competing promotions, clock skew,
  Blossom unavailability, and failover with unresolved reservations.

### Milestone 5: Kafka baseline

- Implement and test the Phase A protocol APIs, including non-transactional
  idempotence.
- Preserve Kafka RecordBatch framing and common compression codecs.
- Publish protocol-version, client, codec, security, and semantics matrices.
- Run producer retry, consumer group, retention reset, and rolling-failure
  suites with supported clients.

### Milestone 6: tiering, retention, and compaction

- Add SSD retention and asynchronous object-storage offload.
- Add conditional immutable manifest generations, reference-aware pack GC, and
  bounded repacking.
- Add parallel remote fetch, retries, cache policy, and degraded-mode health.
- Implement and fault-test live backend migration.
- Add keyed compaction and tombstones locally and remotely.

### Milestone 7: HTTP/3 REST, gRPC, and native optimization

- Add the HTTP/3 REST listener, unary resource API, and length-delimited
  producer/fetch streams.
- Add standard gRPC/HTTP/2 unary and streaming adapters over the same internal
  commands.
- Add zero-copy binary batches, tier-aware fetch hints, and direct-shard
  routing.
- Add native client libraries with explicit H3-only mode and observable,
  bounded gRPC fallback.
- Add TLS 1.3, mTLS/bearer authentication, early-data rejection, topology
  redirection, stream cancellation, and transport resource limits.
- Run generated-schema, OpenAPI, REST/gRPC semantic-parity, and cross-language
  compatibility suites.
- Evaluate coordinator-issued routing leases without weakening fencing.
- Benchmark against Kafka and Redpanda using equivalent durability and
  retention settings.

### Milestone 8: elasticity and active-active ownership

- Ship a static RF1 active-active profile first: deterministically assign
  disjoint logical partitions across a sorted broker membership, proxy native
  REST requests by owner, advertise Kafka leaders, and synchronize topic
  creation. Label it capacity scaling rather than HA.
- Change ring membership without rewriting historical packs.
- Add disjoint partition/topic active-active placement.
- Add fenced, observable replication of consumer checkpoints and stream-cut
  metadata.
- Test mixed ring epochs, node replacement, rollback, and remote-only history.
- Decide whether the colocated per-topic sequencer needs safe range leasing or
  hierarchical sequencing based on measured bottlenecks.

### Milestone 9: delivery policies and ecosystem

- Add separately stored scheduled delivery with cancellation, rescheduling,
  burst smoothing, and retention-independent timer indexes.
- Add retry/backoff policies, dead-letter topics, and exact pending-state
  accounting.
- Add the work-queue subscription profile and adaptive byte-bounded prefetch.
- Add header/tag filtering with pack summaries and measured read amplification.
- Add schema registration, compatibility policy, and optional validation for
  Protobuf, JSON Schema, and Avro.
- Validate Kafka Connect before considering a proprietary connector runtime.
- Prototype an Iceberg/Parquet projection with offset and stream-cut lineage.
- Evaluate additional adapters only against demonstrated user demand and the
  common engine contract.

## 15. Performance goals, win criteria, and validation

Performance claims are release contracts, not aspirational benchmark peaks.
Correctness, durability, and isolation gates are evaluated before throughput.
Targets may be raised as prototypes provide evidence, but they may not be
weakened silently to declare a win.

### 15.1 Reference benchmark profiles

The primary reproducible profile, `RPC-25`, is:

- three broker nodes, each with 32 dedicated vCPUs, 128 GiB RAM, one durable
  NVMe device capable of at least 3 GB/s sequential writes, and 25 GbE;
- separate load-generator machines so client CPU and network are not counted
  as broker capacity;
- Linux, CPU frequency policy, IRQ placement, filesystem, cloud instance type,
  object-store region, and every software version recorded in the result;
- replication factor 3, minimum ISR 2, `acks=all`, TLS enabled, no transactions,
  and object offload enabled unless a workload explicitly says otherwise;
- 1 KiB records in 256 KiB target batches for the standard throughput profile,
  plus 100 B, 10 KiB, and 1 MiB record sweeps;
- uncompressed and Zstandard profiles reported separately, never combined;
- at least five minutes of warm-up, a 30-minute measured run, and a 24-hour
  soak for release candidates; and
- latency measured from the client with coordinated-omission correction and
  synchronized clocks where one-way phases are reported.

`RPC-100` repeats the scale tests on a disclosed 100 GbE/NVMe profile.
`EDGE-8` uses three 8-vCPU nodes to reveal fixed overhead and avoid optimizing
only for large servers.

Before those distributed release profiles, `SINGLE-NVME` calibrates one Linux
broker and one dedicated NVMe device. It sweeps RF1/leader and RF3/minimum-ISR-2
quorum paths, but same-host RF3 is labeled replication-overhead calibration,
not HA. The matrix crosses 100 B, 1 KiB, 10 KiB, and 1 MiB records with 16 KiB,
32 KiB, 64 KiB, 256 KiB, 1 MiB, and 2 MiB target batches where valid; 1, 2, 4,
8, and 16 lanes; concurrency from one through saturation; producer linger of
0, 1, 5, 20, and 50 ms; matching consumer fetch sizes; and uncompressed, LZ4,
and Zstandard Kafka RecordBatch profiles. The 16 KiB case represents Kafka's
default `batch.size`; 2 MiB is a tuned case requiring compatible producer,
broker, and topic request limits. Direct-engine results and Kafka, gRPC, and
HTTP/3 wire results are separate.

The calibration records sequential device bandwidth, fsync latency, NIC link
speed, filesystem and mount options, CPU governor, page-cache state, TLS/SASL,
and per-role CPU. It reports messages/s, batches/s, payload bytes/s, disk and
network bytes/s, and latency together: a small-message message-rate win cannot
be substituted for a large-batch byte-rate win.

Steady-state latency gates are measured at 70% of the empirically observed
maximum sustainable rate for that exact durability/workload profile.
Sustainable means queues and memory remain bounded and no backlog grows over
the 30-minute interval.

### 15.2 Correctness and durability gates

Any failure here blocks a performance claim:

- zero acknowledged record loss with replication factor 3/minimum ISR 2 across
  process crashes, power-loss simulation, partial writes, replica isolation,
  coordinator failover, ring changes, and object-store faults;
- zero reused logical offsets and zero duplicate committed producer sequences
  for idempotent producers;
- no read above the requested isolation watermark and no silent skip in an
  ordered consumer;
- no stale writer accepted after a higher ownership epoch is visible;
- no corrupt extent, manifest, or checkpoint returned as a valid record; and
- deterministic recovery of every reserved range as committed data or an
  explicit abort range.

The long-running fault suite must reach at least 100,000 injected failures and
one billion acknowledged records without violating these invariants before a
release candidate is called durable.

### 15.3 Absolute initial-GA targets

#### Hot replicated path

- One logical partition striped over at least eight shard lanes sustains
  1,000,000 1 KiB records/s (about 0.95 GiB/s of payload) with
  replication factor 3, minimum ISR 2, and `acks=all`.
- `RPC-25` sustains at least 2 GiB/s aggregate producer payload and 4 GiB/s
  aggregate consumer payload for balanced multi-partition workloads.
- At 70% of sustainable load, replicated append latency is at most 10 ms p99
  and 25 ms p99.9; hot end-to-end fetch latency is at most 15 ms p99 and
  40 ms p99.9.
- Aggregate `acks=all` producer efficiency is at least 25 MiB of user payload
  per broker CPU-second. Report broker, replication, TLS, compression, and
  client CPU separately.
- Stalling one data lane does not reduce admission or fetch throughput on
  healthy lanes by more than 20%, and cannot starve control or replication
  traffic.

#### Density, connections, and control-plane scale

- One million idle logical partitions add at most 2 GiB of broker resident
  memory above the fixed process baseline and do not create one metric time
  series per partition by default.
- `RPC-25` supports 100,000 idle native client connections cluster-wide and
  10,000 active fetch streams per node while remaining inside configured
  connection and in-flight-byte budgets.
- Creating, deleting, or applying policy to 10,000 topics completes at least
  1,000 operations/s without increasing hot-path p99 by more than 20%.
- A cold start with one million partition descriptors and 10 TiB of remote
  history reaches coordinator readiness within 60 seconds without listing the
  object bucket.

#### Tiered storage and storage efficiency

- A same-region standard-object-store cache miss has at most 150 ms p99 and
  500 ms p99.9 time to first record after connection pools are warm.
- Each broker sustains at least 1 GiB/s sequential remote payload fetch, subject
  to a disclosed object-service limit, with at most 1.25x remote read
  amplification for sequential reads and 2x for random range reads.
- Local payload write amplification before configured replication is at most
  1.15x; sealed-object framing and indexes add at most 5% space before
  compression.
- Background upload, garbage collection, and repacking stay within their byte,
  request, and CPU budgets. At the default 20% background-bandwidth budget,
  foreground p99 degrades by no more than 20%.

#### Availability, elasticity, and isolation

- A hard active-leader failure has RPO 0 for acknowledged `acks=all` records
  and restores successful writes within 5 seconds at p99. Stale leaders remain
  fenced even when clients retry old routing information.
- Expanding a live topic ring from N to 2N lanes admits work to the new lanes
  within 30 seconds, copies no historical payload solely for the expansion,
  preserves at least 80% of pre-change throughput, and keeps p99 below 2x its
  pre-change value.
- When an untrusted tenant offers twice its quota while a protected tenant uses
  50% of provisioned cluster capacity, the protected tenant receives at least
  95% of its entitlement and its p99 degrades by no more than 20%.
- Loss of the object store does not block hot replicated appends until the
  configured local-retention safety threshold is reached; health and remaining
  time/bytes are explicit before admission is stopped.

#### Native protocol

- On a 30 ms RTT path with 1% random packet loss, native HTTP/3 binary streaming
  sustains at least 1.25x the payload throughput of the project's gRPC/HTTP/2
  adapter at equal batching and CPU, or lowers p99.9 latency by at least 25%.
- H3-to-gRPC fallback completes within two seconds of the configured
  negotiation deadline, preserves the producer identity and retry token, and
  causes no duplicate committed sequence.
- JSON REST control operations are excluded from hot-path throughput claims;
  binary REST/H3, gRPC, and Kafka comparisons use identical engine batches.

### 15.4 Feature-specific acceptance targets

These gates apply when the corresponding P1/P2 feature is released:

- scheduled records are delivered within 100 ms of their deadline at p99 under
  steady load; one million records sharing a deadline drain within five seconds
  without violating hot-append isolation;
- a scheduled population of ten million records consumes bounded memory and
  does not pin unrelated base-log packs;
- schema validation adds no more than 15% throughput cost and 1 ms p99 append
  latency in its declared standard schema profile;
- a selective header/tag filter reduces delivered bytes in proportion to
  selectivity while keeping measured remote read amplification below 2x; and
- a checkpoint/stream cut can be resolved across all ring epochs without
  scanning payload packs, and an expired retention pin becomes reclaimable
  within one garbage-collection generation.

### 15.5 Comparative win criteria

Comparisons are grouped by workload:

- Kafka-compatible log: Apache Kafka, Redpanda, AutoMQ, WarpStream, and
  Bufstream;
- durable pub/sub, queue, retry, and fan-out: Pulsar, NATS JetStream, RabbitMQ
  Streams, RocketMQ, and Redis Streams;
- stream cuts and physical elasticity: Pravega plus the Kafka-compatible set;
  and
- managed economics and published limits: Confluent Cloud, Kinesis, Google
  Pub/Sub, and Azure Event Hubs in the same provider/region where practical.

The OpenMessaging Benchmark framework is the common baseline where it has a
maintained driver:
<https://github.com/openmessaging/benchmark>. Custom workloads are required for
single-log striping, ring expansion, tier migration, stream cuts, HTTP/3, and
fault injection. A custom harness supplements rather than replaces the common
workloads.

A launch benchmark is a product win only if all correctness gates and absolute
GA targets pass, and at least four of these six same-hardware wins hold against
the fastest eligible alternative:

1. at least 2x single-logical-partition producer throughput at equal durability;
2. at least 5x more idle logical partitions per GiB of broker memory;
3. capacity expansion reaches useful new throughput in at most 30 seconds with
   no historical-data rewrite and at least 10x less moved data;
4. at least 2x tiered sequential fetch throughput or at least 50% lower cold
   p99 time to first record on the same object-storage class;
5. at least 1.5x payload throughput per utilized broker vCPU or at least 30%
   lower fully disclosed three-year infrastructure/object-request cost for the
   same workload; and
6. p99.9 hot replicated append latency no worse than 10% above the fastest
   eligible system while delivering the benchmark's target throughput.

No result counts as a win if `shard-stream` has more than a 20% regression in
the paired workload's other primary dimension, weakens durability, omits TLS,
uses a different record/batch size, excludes required replication traffic, or
allows an unbounded queue. Managed-service cost comparisons include provisioned
capacity, storage, replication, API/object requests, cross-zone traffic,
egress, and required fan-out capacity.

### 15.6 Benchmark integrity and required reporting

Every result includes:

- the full saturation curve rather than one selected operating point;
- achieved records/s and payload bytes/s, p50/p95/p99/p99.9/max latency, queue
  depth, and consumer lag;
- broker and client CPU, resident memory, allocator use, disk IOPS/bandwidth,
  network traffic by purpose, replication bytes, object requests, cache hit
  rate, and read/write amplification;
- p99 values in one-minute windows to reveal pauses hidden by a run-wide
  percentile;
- all server, kernel, JVM/runtime, client, compression, batching, linger,
  durability, retention, and tier settings in version control;
- at least three independent runs, confidence intervals, raw result artifacts,
  and checksums; and
- the newest stable competitor versions available when the benchmark campaign
  begins, with tuning reviewed against each project's official guidance.

### 15.7 Required benchmark and validation suites

- Single-topic round-robin write throughput across 1, 2, 4, 8, and 16 shards.
- One logical partition versus many logical partitions at the same shard count.
- Many-topic placement skew and ring balance.
- Pipelined append latency for `acks=1`, minimum ISR, and native durability
  levels, with equivalent settings for comparisons.
- Head-of-line behavior when one shard stalls; verify bounded gaps and
  backpressure.
- Queue saturation by oversized batches, slow consumers, remote reads,
  replication, and compaction; verify control traffic cannot starve.
- Ordered logical fetch with parallel shard reads.
- `AS_AVAILABLE` native fetch throughput.
- REST/HTTP/3 binary streaming versus gRPC/HTTP/2, Kafka, REST/HTTP/3 unary
  batching, and JSON control calls with identical engine workloads.
- HTTP/3 behavior under packet loss, reordering, connection migration, stream
  resets, flow-control exhaustion, slow readers, and bounded producer-stream
  pools.
- Mutation replay attempts through QUIC 0-RTT; verify `425` rejection and no
  offset allocation.
- UDP-blocked H3 negotiation and gRPC fallback; verify bounded failover time,
  identical idempotency, durability, authentication, and typed errors.
- REST/gRPC conformance for every operation, error, retry hint, capability, and
  protocol-version downgrade.
- End-to-end H3 deployment tests through supported load balancers and proxies,
  detecting buffering, trailer loss, idle termination, and header limits.
- Producer idempotence across timeout, retry, failover, and epoch change.
- Consumer-group commits below retention and restart/reset behavior.
- SSD-only, object-only, and mixed-tier reads.
- Multi-partition remote fetch concurrency and tail latency.
- Extent-pack recovery after a crash at every byte boundary around header,
  payload, trailer, reservation, and checkpoint writes.
- Manifest publish, upload, garbage collection, repack, and provider-migration
  fault injection.
- Active-passive promotion, conflicting claims, and stale-writer fencing with
  and without Blossom availability.
- Ring expansion without cold-data rewrite and reads spanning many ring epochs.
- Memory and recovery-time slopes for 10K, 100K, and 1M idle partitions.
- Kafka client compatibility, error codes, codecs, idempotence, and group
  behavior against the checked-in matrix.
- Native protocol comparison against the Kafka adapter on identical workloads.
- Long-running Jepsen-style or deterministic model tests for acknowledged-write
  loss, duplicate committed producer sequences, offset reuse, watermark
  regression, and split-brain writes.
- Hierarchical quota, noisy-neighbor, cardinality-budget, and exact
  logical/physical byte-accounting tests.
- Scheduled-delivery retention independence, same-deadline burst, cancellation,
  retry/backoff, poison-message, and DLQ recovery tests.
- Work-queue claim/ack/reclaim tests and multi-group retention-reference tests.
- Filter correctness and amplification tests across local, remote, compacted,
  and mixed-schema packs.
- Stream-cut reproducibility and retention-pin tests across ring expansion,
  failover, compaction, migration, and garbage collection.
- OpenMessaging Benchmark workloads for every maintained applicable driver,
  with checked-in deployment and tuning profiles.

Every published comparison must disclose record size, batch size, compression,
replication factor, minimum ISR, flush policy, retention tier, client settings,
hardware, and whether acknowledgements include local, replicated, or object
durability.

### 15.8 Architecture go/no-go gates

- At Milestone 1, eight in-memory lanes must deliver at least 4x the throughput
  of one lane for one logical partition with 256 KiB batches. Coordination,
  range allocation, and ordered completion together must consume no more than
  15% of broker CPU at that point.
- At Milestone 2, the crash-safe local extent path must preserve at least 70%
  of in-memory throughput and keep p99 below 2x the in-memory result. Otherwise
  revisit journal batching, extent framing, or flush policy before adding
  protocols.
- At Milestone 4, replication factor 3/minimum ISR 2 must preserve at least 50%
  of local durable ingress and pass the failure invariants. Otherwise revisit
  replica placement, acknowledgement aggregation, and network copies before
  claiming Kafka parity.
- At Milestone 6, remote reads must meet the amplification and cold-latency
  targets before object storage is advertised as a transparent tier.
- At Milestone 7, HTTP/3 must meet the native-protocol comparative target. If
  it does not, it remains an optional transport and gRPC/HTTP/2 becomes the
  default native endpoint until a measured H3 advantage exists.
- Missing a gate in two documented optimization cycles triggers an
  architecture review. The team either changes the design or publishes a
  dated target revision with raw evidence; it does not benchmark around the
  failed case.

## 16. Decisions resolved by this review

- Repository: independent `shard-stream`, with selected source copied and
  attributed from a pinned `shard-kv` commit.
- Physical placement: per-topic round-robin ring, advanced once per admitted
  append batch.
- Logical semantics: efficient Kafka-compatible partitions are ordered views
  over physically striped batch extents.
- Atomicity: one canonical self-describing data extent; interior missing
  extents recover as inferred abort ranges and indexes rebuild from extents.
- Storage: shard-local shared extent packs are the WAL; there is no second
  payload WAL.
- Indexing: exact batch/range directory plus sparse seek checkpoints, not an
  exact per-record sparse index.
- Concurrency: bounded byte- and slot-accounted MPSC channels with one owner per
  mutable shard and separate traffic classes.
- Visibility: explicit log start, allocated end, contiguous log end, replicated
  high watermark, and last stable offset.
- HA: direct lane replication plus Blossom-finalized control-plane ownership,
  ring revisions, epochs, and promotion fencing.
- Active-active: allowed across disjoint ownership domains, never concurrent
  unfenced writers to one ordered partition.
- Kafka baseline: idempotent non-transactional producer support and common
  RecordBatch codecs are required.
- Tiering: immutable generation manifests are authoritative; migration,
  reference-aware GC, parallel fetch, and eventual tiered compaction are design
  requirements.
- Native API: typed engine APIs and schemas arrive before the Kafka adapter.
  REST-shaped HTTP/3 with binary Protobuf streaming is the primary native
  protocol; standard gRPC/HTTP/2 is the generated compatibility and
  UDP-blocked-network fallback over identical semantics.
- Protocol safety: mutation early data is rejected, correctness messages use
  reliable streams, transport buffers count toward byte budgets, and automatic
  redirects never replay producer writes.
- Multi-tenancy: tenant and namespace policy domains, hierarchical
  backpressure, bounded metric cardinality, and separate logical/physical
  resource accounting are core requirements rather than later control-plane
  additions.
- Delivery features: scheduling, retry/DLQ, work-queue consumption, filtering,
  and schema validation are policy/index layers over the immutable base log;
  they cannot pin unrelated history or fork engine semantics.
- Replay: immutable stream cuts span partitions and ring epochs, and retention
  pins are explicit, quota-accounted, and expiring.
- Product boundary: connectors, lakehouse projection, and extra protocol
  adapters follow validated demand; arbitrary stream processing and
  application transactions stay outside the broker.
- Performance contract: initial GA must pass the correctness gates and absolute
  `RPC-25` targets; a public market win additionally requires at least four of
  the six same-hardware comparative criteria.

## 17. Remaining design decisions

Before implementation begins, settle:

1. What coordinator sharding and batching limit prevents a hot topic sequencer
   from becoming the new bottleneck?
2. What exact leader-local flush policy backs Kafka `acks=1`, and which native
   durability is the default?
3. What replication factor, minimum ISR, lag threshold, and ISR eviction rules
   define `acks=all`?
4. Which Kafka API versions, clients, SASL mechanisms, quotas, and group
   protocols constitute the first supported matrix?
5. What conditional-write primitive implements manifest publication on each
   supported object store?
6. How long are object and source-backend rollback grace periods?
7. What Blossom lease-expiry policy permits an existing leader to continue,
   including renewal quorum, duration, and maximum clock-skew allowance?
8. How are new certificates and revocations distributed so every lane fences a
   stale coordinator before the new leader admits writes?
9. At what dead/live ratio does pack repacking beat retaining sparse packs?
10. Which transaction APIs and isolation levels define the release after
    non-transactional idempotence?
11. Which Rust HTTP/3/QUIC stack meets the required zero-copy, cancellation,
    connection-migration, observability, and maintenance criteria?
12. What producer-stream pool size, stream lifetime, and rotation policy gives
    loss isolation without excessive QUIC or server state?
13. Which H3 discovery, load-balancer, proxy, and certificate deployments are
    in the supported production matrix?
14. When does the official SDK fall back to gRPC, and which deployments require
    strict H3 with fallback disabled?
15. Which tenant/namespace hierarchy, fairness algorithm, quota burst window,
    and retry-hint contract form the first multi-tenant policy version?
16. How are stream cuts atomically captured when partitions span coordinators,
    and what authorization and quota rules govern retention pins?
17. Which P1 delivery features ship together, and which consumer-state records
    are replicated synchronously during regional failover?
18. Is schema metadata embedded in `shard-stream`, exposed through a compatible
    registry API, or delegated while broker validation caches an authoritative
    contract?
19. Which object/table format supports an Iceberg projection without making
    analytical layout choices part of the canonical log format?
20. Which exact machines and object-storage classes become the permanent
    `RPC-25`, `RPC-100`, and `EDGE-8` reference environments?
21. Which competitor versions and official tuning profiles are frozen for the
    first reproducible market benchmark campaign?

These should be answered with prototypes and failure tests where possible,
rather than interface speculation alone.

## 18. Initial architectural decision

The core decision for `shard-stream` is:

> Admit immutable append batches through bounded MPSC queues, place successive
> batches round-robin across shard-owned extent logs, and build fenced,
> recoverable logical partitions and protocol compatibility above that physical
> engine.

> Expose that engine primarily through REST-shaped HTTP/3 with binary Protobuf
> streaming, with standard gRPC/HTTP/2 generated from the same schema as a
> compatibility and fallback surface.

This preserves `shard-kv`'s efficient single-owner shard execution, persistence,
tiering adapters, direct replication, and Blossom coordination lessons while
giving the new repository an independent format and compatibility contract.
