# Adam profiling report — 2026-07-23

This report records an optimization calibration, not a Kafka or Redpanda
product claim. The harness embeds the engine in-process and Adam was running
other services during the test.

## Host and cleanup

- 32 logical CPUs.
- Samsung SSD 970 EVO 1 TB behind ext4/LVM.
- Root filesystem initially had roughly 3 GB free of 914 GB.
- Docker reported 602 inactive build-cache entries and no active build.
- `docker builder prune -af` removed only inactive build cache and reported
  156 GB reclaimed. Containers, images, volumes, source trees, models, and
  databases were not removed.
- Root free space increased to 91 GB. One orphaned 36-day-old build-monitoring
  shell was also stopped.
- Adam still had 110 active containers and several active Eden/database
  processes, so physical-device results retain background-noise risk.

## Baseline hotspots

The symbolized RF1 tmpfs profile sustained 5.46 GiB/s at p99 7.23 ms. Its top
application costs were:

- BLAKE3 payload hashing: 12.66% of CPU-cycle samples;
- CRC32 extent checksumming: 4.41%; and
- payload allocation/copy work around extent writes.

The benchmark used non-idempotent producers, so the BLAKE3 digest was discarded.
The extent encoder also copied each complete payload into a second
payload-sized frame buffer before writing it.

The symbolized RF1 NVMe baseline sustained 375.31 MiB/s at p99 114.15 ms. The
filesystem reached approximately 99% utilization, with write/flush latency
commonly around 11–15 ms. CPU was not saturated.

## Implemented changes

- Do not hash payloads for appends without an idempotent producer identity.
- Use XXH3-128 for idempotent-producer retry fingerprints.
- Store owned payloads as `Arc<Vec<u8>>`, avoiding the `Vec` to `Arc<[u8]>`
  payload copy while retaining cheap sharing across local replica workers.
- Encode only the fixed extent header, calculate CRC32 over header plus the
  original payload, and write header/payload/checksum with scatter/gather I/O.
- Bump the pre-launch extent format to `SSE3`; no legacy format path was added.
- Add `--idempotent-producers` to measure producer sequencing overhead.
- Reject nonempty explicit benchmark directories unless `--reuse-data` is
  supplied, preventing accidental log-growth contamination.

Object-tier immutable-content verification remained BLAKE3. In this first
pass, CRC32 remained the extent and wire corruption check.

## Results

All rates use 256 KiB batches, 256 records per batch, eight shards, and
concurrency 64.

| Mode | Baseline | Optimized | Change |
|---|---:|---:|---:|
| RF1 tmpfs CPU ceiling | 5.46 GiB/s | 7.24 GiB/s | +32.6% |
| RF1 tmpfs p99 | 7.23 ms | 4.69 ms | -35.1% |
| RF1 NVMe symbolized | 375.31 MiB/s | 435.09 MiB/s | +15.9% |
| RF1 NVMe A-B-B-A average | 390.71 MiB/s | 751.82 MiB/s | +92.4% |
| RF1 NVMe A-B-B-A average p99 | 88.16 ms | 48.70 ms | -44.8% |
| RF3 tmpfs A-B-B-A average | 2.18 GiB/s | 3.20 GiB/s | +46.7% |
| RF3 tmpfs A-B-B-A average p99 | 15.50 ms | 9.40 ms | -39.4% |

The final idempotent RF1 tmpfs sample reached 7.56 GiB/s, within ordinary
run-to-run variance of the 7.24 GiB/s non-idempotent sample.

The RF3 physical-NVMe A-B-B-A sequence was invalid: throughput degraded
monotonically from 231.35 to 66.31 MiB/s across the four runs regardless of
binary order. No RF3 NVMe performance conclusion is drawn from it.

## Final profile and next bottleneck

The optimized profile contains no BLAKE3 hot-path samples for non-idempotent
appends. CRC32 is now the largest named application function (5.36% in the
final NVMe profile). The rest of the leading samples are Linux page allocation,
ext4 delayed allocation/writeback, dirty-page throttling, and filesystem data
movement.

The next trustworthy qualification step is a quiet dedicated NVMe host (or an
exclusive benchmark window on Adam), followed by real network producer and
consumer tests. RF3 claims require three fault domains and network replication;
same-host replica workers measure only local code-path overhead.

## Second optimization pass

The second pass removed payload duplication across the remaining engine and
replica boundaries and moved repeatable extent preparation to the leader:

- Protocol adapters, engine commands, storage, and the benchmark now pass
  immutable `Bytes` ownership. REST and Kafka retain ingress buffers directly,
  gRPC transfers ownership of its decoded vector, and HTTP/3 freezes its
  completed receive buffer.
- The primary storage append produces one `PreparedExtent` containing the
  encoded header, payload, checksum, and reservation. Local replicas write that
  exact prepared extent instead of re-encoding and checksumming it.
- The pre-launch extent format advanced to `SSE4` and replaced its CRC32 with
  seeded one-shot XXH3-64 over the header and payload. The control journal and
  native wire format retain CRC32; object-tier identity retains BLAKE3.
- The Kafka TCP adapter uses `bytes-handoff` to preserve fragmented frame tails
  and pipeline complete input frames. A byte- and item-bounded output handoff
  overlaps response writes with subsequent request handling while preserving
  response order.
- Server metrics use sharded `fast-telemetry` counters and Prometheus snapshot
  export. Server, Kafka, and HTTP/3 lifecycle/error logs use `eden_logger`.
  Neither dependency adds per-record synchronous logging.

`bytes-handoff` is intentionally limited to the asynchronous Kafka socket
boundary. The engine's byte-accounted MPSC channels already encode admission,
ownership, and per-lane sequencing; substituting an I/O buffer there would
weaken those guarantees without addressing a measured hotspot.

### Checksum selection

An Adam single-thread checksum microbenchmark over the representative 256 KiB
payload measured:

| Algorithm | GiB/s |
|---|---:|
| CRC32 streaming | 12.66 |
| XXH3 streaming state | 21.33 |
| XXH3-64 seeded one-shot | 24.76 |
| XXH3-64 split/xor | 24.93 |
| XXH3-128 split/xor | 24.96 |

The seeded one-shot XXH3-64 construction was retained because it protects both
header and payload, provides a 64-bit corruption check, and was faster than the
streaming state. A checksum-only interleaved engine comparison was effectively
neutral at +0.33%, confirming that page allocation—not checksum throughput
alone—limits the complete RF1 run.

### Interleaved tmpfs results

All second-pass rates use 256 KiB batches, 256 records per batch, eight shards,
and concurrency 64. Each comparison used an A-B-B-A order against the previous
optimized binary.

| Mode | Previous optimized | Second pass | Throughput | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|
| RF1/leader | 7,280.36 MiB/s | 9,826.42 MiB/s | +34.97% | -30.17% | -13.88% | +5.32% |
| RF3/quorum-2 | 3,150.97 MiB/s | 4,522.03 MiB/s | +43.51% | -30.50% | -30.77% | -29.80% |

The RF1 p99 regression was not stable across samples: the symbolized final
profile reached 9,911.66 MiB/s at p99 3.56 ms, compared with the interleaved
second-pass mean of 4.82 ms. It is reported rather than averaged away.

The harness now shares one immutable payload between requests, so these results
are an engine/storage ceiling after ingress ownership. They do not include
socket transport, request parsing, or client-to-broker copies.

### New profile

The final RF1 symbolized profile sustained 9,911.66 MiB/s. Its largest samples
were Linux page clearing (10.54%) and XXH3's long-input loop (5.76%), followed
by tmpfs shared-memory allocation and write paths. The former benchmark
payload-clone/copy hotspot disappeared.

The RF3 symbolized profile sustained 4,731.85 MiB/s. Replica workers consumed
61.18% of sampled cycles and lane workers 32.34%. XXH3 appeared only in the
leader lane (2.10%) and not in replicas, confirming that prepared extents are
reused. Linux page clearing across leader and replica workers accounted for
15.12%. No application-wide lock, `Bytes`, MPSC handoff, telemetry, or logging
hotspot appeared.

Physical NVMe was not rerun in this pass. Adam's root filesystem remained at
approximately 90% used—at the benchmark policy's minimum free-space
boundary—and the prior RF3 physical sequence had already failed the run-order
drift gate. The next optimization target is therefore buffer/page reuse or
vectored direct-I/O experimentation on a quiet device, followed by end-to-end
network qualification.

## Ordered consumer pass

The first ordered-fetch profile used 16,384 32 KiB batches striped over eight
shards and a 1 MiB response budget. It returned only 1.54–1.58 GiB/s. A traced
warm-plus-measured pass opened pack files 260,382 times for 32,768 returned
batches and read 7.95 times the returned payload because every shard
materialized up to the complete response budget before the engine performed
its global merge. Coordinator fetch snapshots also scanned retained batch
prefixes and committed suffixes.

The optimized path now:

- caches ordered and replicated watermarks and advances each cursor
  amortized O(1), while `AS_AVAILABLE` visibility checks only the bounded
  candidate window;
- requests metadata from each shard first, performs one global byte-budgeted
  k-way selection, and reads only selected extents;
- retains persistent pack readers and coalesces nearby positional reads into
  one `Bytes` backing allocation whose payload slices are shared through the
  engine and native protocol;
- protects metadata plans with opaque per-store generation tokens, invalidated
  by retention, instead of rehashing every selected batch; and
- records the already-known global placement order so materialization does not
  run a second heap merge.

On the same prepared tmpfs dataset, the exact final path sustained 9,206.81
MiB/s (294,618 batches/s) with p50 0.106 ms, p99 0.140 ms, and p99.9 0.187 ms
over ten passes. This is roughly 5.8 times the original ordered-fetch rate.
The result is still a direct-engine, page-cache ceiling rather than a network
consumer claim.

A per-thread syscall trace covering one warmup and one measured pass returned
1,073,741,824 payload bytes and issued 1,077,982,480 bytes of positional reads,
or 1.004x read amplification. No pack-file `openat` occurred in the fetch
workers; startup retained the file handles. The exact-current-code CPU profile
was dominated by tmpfs-to-userspace copying: `rep_movs_alternative` was 26.80%,
`_copy_to_iter` 6.02%, and `shmem_file_read_iter` 3.45%. The former per-batch
default-hasher validation and second merge heap were absent from the reported
hot symbols. Eliminating the remaining copy requires a transport-aware
zero-copy or mapped-file design; `bytes-handoff` cannot remove the kernel file
read into an owned engine response.
