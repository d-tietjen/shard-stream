# Adam Kafka Produce request profile — 2026-07-24

> Follow-up: this run revealed that the original benchmark encoded 2,048
> one-record RecordBatches per request rather than one normal multi-record
> batch. The attribution below remains valid for that historical request shape,
> but current performance and the corrected harness are documented in
> [`ADAM_KAFKA_FAST_SCAN_2026-07-24.md`](ADAM_KAFKA_FAST_SCAN_2026-07-24.md).

This report attributes shard-stream's Kafka RF1 Produce cost by following the
same sampled request through every broker stage. It is diagnostic evidence, not
a Kafka or Redpanda comparison and not an NVMe release qualification.

## Environment and workload

- Host: Adam, AMD Ryzen 9 3950X, 16 physical cores / 32 logical CPUs, Linux
  6.8.
- Broker CPUs: 0–15; client CPUs: 16–31.
- Storage: a fresh directory in `/dev/shm`.
- Adam's root filesystem was full and its load average was approximately
  8–12, so compiler temporaries and all benchmark artifacts were isolated in
  tmpfs.
- One logical partition, RF1, minimum ISR 1, Kafka `acks=1`.
- Eight client connections with one request in flight per connection.
- 4,096 batches, each containing 2,048 records with 1 KiB application values.
- Each request carried 2,240,512 encoded RecordBatch bytes and 2 MiB of
  application payload.
- No compression, TLS, or remote replication.

The server was built in release mode. The CPU profile used the same optimized
binary with symbols retained. It captured 4,071 cycle samples with no lost
samples.

## Profiling overhead control

The control and fully sampled runs used fresh data directories and otherwise
identical dimensions.

| Sample stride | Application MiB/s | Records/s | p50 | p99 | p99.9 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0, disabled | 2,388.55 | 2.446M | 6.568 ms | 9.059 ms | 26.307 ms |
| 1, every request | 2,370.64 | 2.428M | 6.634 ms | 9.262 ms | 19.175 ms |

The sampled run was 0.75% slower. This is below the report's 10% run-to-run
variability threshold, so profiling overhead was not distinguishable from
noise in this pair. Production deployments should leave the default stride of
zero or use a sparse power-of-two stride.

## Per-request stage attribution

All 4,096 Produce requests populated every stage. The mean frame was 2,240,600
bytes and contained 2,048 records.

| Stage | Mean | Interpretation |
| --- | ---: | --- |
| `frame_receive_wait` | 2.901 ms | Inclusive interval before the next full frame; under one-in-flight pacing this includes client turnaround and upload |
| `request_decode` | 0.005 ms | Kafka request envelope decode after the frame is resident |
| `blocking_queue` | 0.033 ms | Tokio blocking-pool admission delay |
| `blocking_handler` | 3.757 ms | Inclusive blocking handler |
| `produce_handler` | 3.755 ms | Inclusive Produce handler |
| `record_validation` | 1.174 ms | RecordBatch CRC/decode, record materialization, validation, and producer identity extraction |
| `engine_append` | 2.318 ms | Engine admission, XXH3, lane dispatch, and durable RF1 append |
| `visibility_wait` | 0.091 ms | Wait for the contiguous ordered watermark |
| `response_encode` | 0.005 ms | Kafka Produce response encode |
| `write_queue` | 0.005 ms | Response wait in `bytes-handoff` |
| `write_syscall` | 0.033 ms | Socket write operation |
| `write_completion` | 0.038 ms | Inclusive queue-through-write completion |
| `request_completion` | 3.881 ms | Full frame resident through response socket completion |

The inclusive `produce_handler` consumed 96.8% of post-ingress server time.
Within it, engine append was 59.7% of request completion, record validation was
30.2%, and visibility waiting was 2.3%. The remaining handler work was 4.5%;
blocking admission, response handling, and scheduling account for the small
remainder. Aggregate rows overlap their child rows and must not be summed.

At 1,185.3 batches/s with eight requests in flight, Little's Law gives about
6.75 ms of average client-observed residence per request. The 3.88 ms
post-ingress server interval plus the 2.90 ms paced receive interval explains
that end-to-end value. Kafka envelope and response encoding are not meaningful
bottlenecks.

## CPU hotspots

The flat cycle profile identified:

- 16.43% in the kernel's primary memory-copy routine on Tokio ingress workers;
- 13.55% in directly named Kafka RecordBatch CRC and decode functions;
- 6.51% in XXH3 on storage-lane workers;
- 5.49% in kernel page clearing, followed by additional tmpfs page allocation
  and write-accounting work; and
- 1.20% in nftables evaluation on the loopback path.

The most prominent named Kafka functions were
`shard_stream_kafka::decode_record_sets` (4.28%),
`RecordBatchDecoder::decode_new_records` (3.12%), and CRC32C routines (6.15%
combined). A generic iterator fold used by validation accounted for another
4.10%, and dropping decoded record vectors accounted for 0.57%. Those latter
samples align with the current validation implementation and put its likely
CPU footprint near 18%, although a flat profile cannot prove the generic
fold's caller.

## Finding and next optimization

The engine append itself remains close to the earlier direct-engine
concurrency-eight service time. The Kafka-specific server penalty is primarily
the adapter's full RecordBatch materialization, not the Kafka request envelope
or response.

The next optimization should replace `decode_all` plus `Vec<Record>` creation
with a bounded metadata scanner that:

1. validates the batch header, size, magic, attributes, and CRC exactly once;
2. reads record count and producer identity without allocating every record;
3. rejects transactional/control batches from batch metadata;
4. verifies record framing and limits without retaining decoded values; and
5. passes the original immutable RecordBatch bytes directly to the engine.

CRC validation must remain enabled for Kafka correctness. XXH3 is an
engine-wide corruption check and affects native and Kafka appends, so it should
be optimized independently rather than counted as Kafka protocol overhead.
After the metadata scanner, profile the receive path to determine whether
`HandoffBuffer` can transfer ownership of the filled ingress allocation
without another large copy.
