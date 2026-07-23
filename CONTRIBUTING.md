# Contributing

The project is in architecture-first bootstrap.

Before submitting a change:

1. preserve the correctness invariants in `docs/SHARD_STREAM_PLAN.md`;
2. keep every queue bounded by slots and bytes;
3. use checked arithmetic for offsets, ranges, lengths, and budgets;
4. add deterministic tests for failure and boundary behavior;
5. record copied or substantially adapted `shard-kv` code in `UPSTREAM.md`;
6. run formatting, Clippy with warnings denied, and the full test workspace.

Performance changes must report durability, record and batch sizes,
compression, hardware, queue bounds, and latency percentiles. A faster result
with weaker durability or unbounded buffering is not a valid improvement.
