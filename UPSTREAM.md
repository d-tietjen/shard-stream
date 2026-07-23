# Upstream source tracking

`shard-stream` is an independent repository. Reused implementation must be
copied into this repository, adapted to stream-native interfaces, and recorded
here. Runtime dependencies on `shard-kv` crates are not allowed.

## Bootstrap source revision

- Repository: <https://github.com/d-tietjen/shard-kv>
- Revision: `23811c3f1b17d61caaefd77907b2e4f65b6021bc`
- License: Apache-2.0

## Reuse ledger

| `shard-stream` path | Upstream path | Status |
| --- | --- | --- |
| `LICENSE` | `LICENSE` | Copied unchanged |
| `Cargo.toml` release profiles | `Cargo.toml` release profiles | Adapted for the new workspace |
| `.github/workflows/ci.yml` | `.github/workflows/ci.yml` | Reduced and adapted; Redis/KV jobs removed |
| `docs/SHARD_STREAM_PLAN.md` | `docs/SHARD_STREAM_PLAN.md` on branch `shard-stream-plan` | Copied architecture plan |

No `shard-kv` runtime implementation is copied in the bootstrap commit. The KV
WAL encodes mutation records and the existing Blossom bridge imports
`shardmap` ownership types directly. Copying either unchanged would couple the
new repository to the old data model.

## Candidate ports

Future ports must add a row to the reuse ledger with the exact upstream path
and revision:

- WAL frame checksums, partial-tail recovery, segment rotation, and sync policy;
- direct replication framing, batching, backlog bounds, and transport metrics;
- object-storage adapters, immutable manifest publication, and migration;
- TLS configuration and certificate reload patterns;
- Blossom claim canonicalization, quorum verification, persistence bounds, and
  deadline handling; and
- deterministic fault-injection utilities.

The Redis command layer, key-value mutation types, cache APIs, language
bindings, and GPU runtime are explicitly outside the reuse boundary.
