# Embedded runtime

`shard-stream-runtime` provides listener-free lifecycle management around the
public storage engine. The default composition is RF1. A separately distributed
crate may inject implementations of the public assignment, replication,
write-fence, retention, durable-sink, and health contracts at compile time.

```rust
use shard_stream_core::BrokerId;
use shard_stream_runtime::EmbeddedRuntime;

let runtime = EmbeddedRuntime::builder()
    .storage(engine_config)
    .broker_id(BrokerId::new(0))
    .open()?;

runtime.recover()?;
runtime.ready()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The lifecycle is `Recovering → CatchingUp → Ready → Draining → Stopped`.
`Failed` is terminal for the process. A write fence may additionally place the
runtime in `Fenced` while reads remain available.

The runtime owns recovery, admission, drain, durable sync, and shutdown. It
starts no REST, gRPC, Kafka, HTTP/3, interbroker, or administrative listener.
Host applications must assign bounded task and queue budgets and must stop
admission before shutdown.

Install a sink through
`EmbeddedRuntimeBuilder::durable_sink(DurableSinkConfig)`. During `recover`,
the runtime filters recovered WAL partitions through the assignment provider
and calls `StreamEngine::resume_durable_sink` only for partitions led locally.
Blocking sink recovery must finish before the runtime enters `CatchingUp`;
background recovery continues on dedicated sink workers. The public server
does not install a sink.

See [`EXTENSIONS.md`](EXTENSIONS.md) for the extension contracts. The public
`shard-stream-server` does not install any distributed implementation.
