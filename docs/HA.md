# HA and fencing contract

`shard-stream` separates ownership consensus from payload replication.
Blossom finalizes infrequent leadership claims; producers, fetches, and extent
replication do not send payload data through Blossom.

## Implemented control-plane boundary

The `shard-stream-control` crate provides:

- domain-separated BLAKE3 identifiers for partition lease groups and canonical
  lease claims;
- verification that a certificate finalizes the exact claim and forms a
  monotonic, hash-linked lease sequence;
- durable, checksummed, bounded lease state that is persisted before a lease
  becomes active;
- refresh from the latest trusted finality so a former leader learns about a
  transfer before its local lease expires;
- monotonically increasing leader epochs for leadership transfer; and
- a `WriteFence` installed in the stream engine. When installed, every append
  must carry the exact current, unexpired leader epoch.

Lease state is partitioned across a fixed pool of MPSC-owned workers using a
stable topic/partition hash. The append-side fence check therefore has no
shared map lock and unrelated partitions can validate concurrently. A separate
control-plane owner serializes the infrequent durable lease-state replacement;
payload appends never wait for a global append journal or Blossom finality.

The `BlossomLeaseConsensus` implementation is a security boundary. Its
`latest_lease` and `commit_lease` methods must verify validator signatures,
validator-generation transitions, the trusted contiguous epoch chain, and
claim inclusion before returning a certificate. The control crate does not
accept raw votes or unverified network responses.

The coordinator's normal loop is:

1. Read and verify the latest finalized lease.
2. Propose a hash-linked renewal, or a strictly higher epoch for a new leader.
3. Verify and persist the finalized certificate.
4. Activate the epoch for producer admission.
5. Refresh continuously and stop writes when the lease expires or another
   leader is observed.

## Data-plane requirements

A production distributed replica transport must send the leader epoch and
ownership-certificate digest with each replicated extent. Every replica must
persist its highest accepted epoch and reject lower epochs independently; a
leader-side check alone is not sufficient split-brain protection. Promotion
must also prove that the candidate has caught up through the safe replicated
high watermark before Blossom finalizes ownership.

Active-passive applies within one ordered logical partition. Active-active
operation means independent topics or partitions may have different active
leaders concurrently; two nodes must never lead the same partition at the
same epoch.

## Current status

The lease state machine, certificate boundary, refresh behavior, engine fence,
restart persistence, transfer fencing, and fail-closed checks are implemented
and tested. Replica copies currently run inside one broker process, so the
repository does not yet claim multi-node HA. A concrete Blossom
network/validator adapter and a remote replica protocol with replica-side epoch
enforcement are required before enabling that claim.
