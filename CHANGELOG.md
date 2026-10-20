# Changelog

## 0.3.0 — Unreleased

This candidate aligns all eleven public crates at `0.3.0`. It is not yet
published; the release gates and manual publication in
[`docs/RELEASING.md`](docs/RELEASING.md) remain required.

### Compatibility

- `SequencerError::ReservationProposalChanged` and
  `EngineError::OperationBusy` add variants to previously exhaustive public
  enums. Downstream exhaustive matches must handle these variants. The new
  `0.3` version line separates this Rust API change from compatible `0.2`
  dependency updates.
- The storage facade `shard-stream-storage` and benchmark harness
  `shard-stream-bench` inherit the workspace version but remain unpublished.

### Embedded recovery and queries

- Add owned recovery and bounded replay continuation APIs retaining the
  original directory ownership, operation expiry, cumulative work limits and
  entered-work cleanup. See [`docs/SHARDLOG.md`](docs/SHARDLOG.md) and
  [`docs/BOUNDED-RESUME.md`](docs/BOUNDED-RESUME.md) for their scope and
  remaining consumer responsibilities.
- Add retained negative query-source observations so a composing host can
  reject an invalidated source. These observations do not establish indexed
  replay, query completeness, authenticated placement or serving permission;
  see [`docs/QUERY-LINEAGE.md`](docs/QUERY-LINEAGE.md).
- Preserve the public single-node RF1 product boundary. The new recovery and
  observation APIs do not add a distributed host or establish private HA
  qualification.
