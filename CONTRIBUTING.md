# Contributing

The public project is a single-node RF1 product with stable extension
contracts.

Before submitting a change:

1. preserve the RF1 durability and extension-boundary contracts;
2. keep every queue bounded by slots and bytes;
3. use checked arithmetic for offsets, ranges, lengths, and budgets;
4. add deterministic tests for recovery and boundary behavior;
5. record copied or substantially adapted `shard-kv` code in `UPSTREAM.md`;
6. run formatting, Clippy with warnings denied, and the full test workspace.

Performance changes must report durability spelling, RF1, record and batch
sizes, compression, hardware, queue bounds, and latency percentiles. A faster
result with weaker local durability or unbounded buffering is not valid.
