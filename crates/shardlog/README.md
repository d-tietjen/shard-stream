# ShardLog

ShardLog is an embedded, crash-recoverable append log for Rust applications.
It stores checksummed records in immutable extent packs, repairs only
uncommitted torn tails, fails closed on committed corruption, and makes durable
appends visible only after the active pack is synchronized.

ShardLog is the persistence engine used by shard-stream. It is a storage
primitive rather than a consensus protocol or application database.

## Status

The crate is being prepared for its first public release and is not yet
published on crates.io.

## Durability properties

- exclusive ownership of each data directory;
- checksummed records and fail-closed committed-history recovery;
- durable pack creation, rotation, tail repair, and garbage collection;
- grouped prepared appends for atomic higher-level commit protocols; and
- ordered fetch planning over immutable sealed packs and the active pack.

The stable public entry points are `ShardLog`, `ShardLogConfig`,
`PreparedExtent`, and `StoredBatch`.
