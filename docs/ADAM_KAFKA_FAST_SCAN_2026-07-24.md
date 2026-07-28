# Adam Kafka fast RecordBatch admission — 2026-07-24

This report measures the Kafka Produce optimization that replaced full
`Vec<Record>` materialization with a framing scanner. It also separates the
cost of the Kafka/TCP path from the direct-engine harness. This is an RF1
tmpfs calibration, not an NVMe, RF3, or cross-host release qualification.

## Environment and workload

- Host: Adam, AMD Ryzen 9 3950X, 16 physical cores / 32 logical CPUs.
- Broker CPUs: 0–15; Kafka client CPUs: 16–31.
- Storage: a fresh directory in `/dev/shm` for every repetition.
- Adam's root filesystem was full and its load average was approximately 7–8,
  so builds and benchmark data were isolated in tmpfs.
- One logical partition, RF1, minimum ISR 1, Kafka `acks=1`.
- Eight Kafka connections, with one request in flight per connection.
- 4,096 requests, each containing 2,048 records with 1 KiB application values.
- Each request contained one 2,117,629-byte uncompressed RecordBatch carrying
  2 MiB of application payload.
- No TLS, compression, or remote replication.

The benchmark now pre-encodes the complete Produce frame once per connection
and patches only its correlation ID. This removes a benchmark-client request
construction and 2 MiB copy from every measured round trip.

## Correctness and implementation

The adapter now:

1. bounds every outer RecordBatch before reading its header;
2. validates magic v2, attributes, CRC32C, record count, and offset deltas;
3. scans Kafka varint and varlong record framing without retaining records;
4. validates non-idempotent and idempotent producer-header semantics across
   multiple RecordBatches; and
5. passes the original immutable bytes directly to the engine.

Uncompressed admission performs no per-record allocation. Compressed batches
still require a temporary decompression buffer, but use the same scanner after
decompression. Transactional and control batches remain explicitly rejected.

This work also corrected a benchmark construction bug. The previous harness
gave every non-idempotent record sequence `-1`, causing the encoder to emit
2,048 one-record RecordBatches per request. A normal multi-record
non-idempotent batch has base sequence `-1`, followed by reconstructed record
sequences `-1, 0, 1, ...`. The adapter now accepts that standard representation
and the harness verifies that every request contains exactly one RecordBatch.

## Repeatable throughput

The table reports the median of three repetitions. Spread is
`(maximum - minimum) / median`.

| Path | Concurrency | Median MiB/s | Records/s | p50 | p99 | Spread |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Direct engine, RF1 leader | 8 | 6,598.82 | 6.76M | 2.311 ms | 3.795 ms | 4.69% |
| Kafka/TCP, RF1 `acks=1` | 8 | 2,810.89 | 2.88M | 5.597 ms | 7.748 ms | 1.85% |

The Kafka samples were 2,841.56, 2,810.89, and 2,789.64 MiB/s. Compared with
the previous unsampled 2,388.55 MiB/s Kafka control, the optimized path is
17.7% faster in application-payload throughput. This is an end-to-end
before/after comparison: it includes the scanner, the corrected one-batch
request shape, and removal of the benchmark client's per-request frame copy.

A diagnostic Kafka concurrency sweep produced 2,193 MiB/s at 4 connections,
2,853 MiB/s at 16, 2,719 MiB/s at 32, and 2,564 MiB/s at 64. These are single
samples with 2,048 requests each, so they are saturation guidance rather than
publishable medians. They put saturation near 16 connections; the repeatable
eight-connection result is already 98.5% of that observed peak with much lower
latency.

For context, one current direct-engine concurrency-64 sample reached
11,392 MiB/s (11.13 GiB/s) at 23.71 ms p99. It confirms the historical
11+ GiB/s engine ceiling, but it is not directly comparable to Kafka at eight
connections and is not a three-run result.

## Sampled request attribution

Profiling every request reduced Kafka throughput by less than one percent
relative to the repeatable median. All 4,096 requests populated every stage.

| Stage | Mean | Interpretation |
| --- | ---: | --- |
| `frame_receive_wait` | 2.291 ms | Client turnaround, upload, and receive until a complete frame was resident |
| `request_decode` | 0.004 ms | Kafka envelope decode |
| `blocking_queue` | 0.030 ms | Blocking-pool admission |
| `produce_handler` | 3.291 ms | Inclusive Produce handler |
| `record_validation` | 0.463 ms | CRC32C plus allocation-free RecordBatch and record-framing scan |
| `engine_append` | 2.724 ms | Engine admission, XXH3, lane dispatch, and durable RF1 append |
| `visibility_wait` | 0.095 ms | Ordered contiguous-watermark wait |
| `response_encode` | 0.004 ms | Produce response encode |
| `write_completion` | 0.035 ms | Response queue and socket write |
| `request_completion` | 3.408 ms | Full frame resident through response socket completion |

The earlier decoder/materializer spent 1.174 ms in `record_validation`; the new
scanner spends 0.463 ms, a 60.5% reduction. Envelope and response encoding are
still immaterial.

At 1,412 requests/s with eight requests in flight, Little's Law gives
approximately 5.666 ms average client residence. The measured 2.291 ms
receive interval plus 3.408 ms post-ingress completion interval accounts for
that end-to-end duration within 0.6%.

## Why direct is cheaper

At concurrency eight, the direct median implies approximately 2.425 ms average
residence per append. Kafka requires approximately 5.69 ms. The additional
3.27 ms is accounted for by:

- 2.291 ms of paced TCP upload and receive;
- 0.463 ms of mandatory Kafka CRC and record-framing validation;
- approximately 0.299 ms between the profiled Kafka engine append and the
  direct average, including the slightly larger encoded payload and scheduling;
- 0.095 ms waiting for Kafka-visible contiguous ordering; and
- approximately 0.12 ms of queues, decoding, response work, and residual
  handler overhead.

The direct harness shares one immutable 2 MiB `Bytes` value among in-process
append calls. It has no client socket, kernel TCP copies, Kafka framing, Kafka
CRC, response, or ordered-visibility acknowledgement. It measures the storage
engine ceiling after payload ownership has already been established.

The CPU profile agrees with the stage data. TCP receive copying alone consumed
18.6% of broker cycles on Tokio workers. XXH3 consumed 6.75%, while tmpfs page
clearing consumed 6.07%. The large-frame `bytes-handoff` path already transfers
its filled allocation into immutable `Bytes` without another user-space payload
copy; the prominent copy is the kernel's required socket-to-user transfer.

No application global lock was found. A first exploratory profile showed
kernel spin-lock time, but its call graph was entirely tmpfs page reclaim
because two 8 GiB datasets were resident simultaneously. Repeating with one
fresh dataset at a time removed that hotspot.

## Next optimization boundary

The largest remaining Kafka-specific opportunity is bounded connection
read-ahead paired with a pipelined producer harness. That can overlap the next
frame's TCP receive with the current append while preserving per-connection
admission order. It must be byte-bounded, and idempotent producer sequences
must remain ordered.

After that, the dominant costs are fundamental data movement and the shared
engine path. Any attempt to skip Kafka CRC validation or acknowledge before
the contiguous visibility watermark would change Kafka correctness semantics
and must not be reported as a protocol optimization.
