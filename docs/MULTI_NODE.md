# Static multi-node operation

Shard-stream can run a static RF1 active-active cluster in which different
logical topic partitions are owned by different broker nodes. Every owning
broker still stripes successive batches across its local shard lanes. The
cluster layer adds a second, coarser level of parallelism without introducing a
global append lock.

This mode is for parallel capacity, not HA. Each partition has one durable
owner and no remote replica. Losing that node makes its partitions unavailable
until its volume and process return. Multi-node RF2/RF3, promotion, and
split-brain-safe failover remain gated on remote extent replication and
replica-side leader-epoch enforcement.

## Start three nodes

```shell
export SHARD_STREAM_CLUSTER_TOKEN="$(openssl rand -hex 32)"
docker compose -f compose.multi-node.yaml up --build --detach --wait
```

The profile exposes:

| Node | REST | gRPC | Kafka |
| ---: | --- | --- | --- |
| 0 | `http://127.0.0.1:17420` | `http://127.0.0.1:17421` | `127.0.0.1:19092` |
| 1 | `http://127.0.0.1:27420` | `http://127.0.0.1:27421` | `127.0.0.1:29092` |
| 2 | `http://127.0.0.1:37420` | `http://127.0.0.1:37421` | `127.0.0.1:39092` |

Create a topic through any REST node:

```shell
curl --fail \
  -H 'content-type: application/json' \
  -d '{"topic_id":"42","partitions":24}' \
  http://127.0.0.1:17420/v1/topics
```

Topic creation is synchronously installed on every configured node before the
request succeeds. Retrying the same topic definition is idempotent on internal
cluster synchronization paths.

Run the checked-in smoke test with:

```shell
./scripts/test-multi-node.sh
```

The script writes all partitions through node 0, reads all partitions through
node 2, and relies on one-hop owner routing in both directions. It then prints
per-node append metrics so physical distribution is visible.

## Ownership and routing

All nodes sort the same immutable membership list by node ID. A partition owner
is:

```text
xxh3_64(topic_id_le || partition_id_le) modulo node_count
```

`GET /v1/topology` returns the membership, topology digest, advertised
endpoints, and ownership algorithm. Internal addresses and the cluster token
are never returned.

- REST requests may enter any node. A non-owner proxies the complete request to
  the owner in one bounded hop and returns the owner's status, headers, and
  bounded response to the caller.
- Kafka metadata advertises every broker and the deterministic leader for every
  partition. Requests sent to a stale or incorrect broker receive
  `NOT_LEADER_OR_FOLLOWER`, causing compatible clients to refresh metadata.
- gRPC checks ownership before engine admission. A non-owner returns
  `FAILED_PRECONDITION` with the advertised owner endpoint. Generated clients
  should cache `/v1/topology` and open producer streams directly to owners.
- HTTP/3 is rejected at startup in multi-node mode until its adapter supports
  equivalent coordinator routing.

There is no global fsync or global data-path lock. Topic creation is a rare
cluster-wide control operation; append and fetch remain node-local after
routing.

## Static membership configuration

Each node receives the identical comma-separated
`SHARD_STREAM_CLUSTER_NODES` value. One node entry is:

```text
node_id@internal_rest@advertised_rest@advertised_grpc@kafka_host:kafka_port
```

For example:

```shell
export SHARD_STREAM_NODE_ID=0
export SHARD_STREAM_CLUSTER_TOKEN='replace-with-a-secret'
export SHARD_STREAM_CLUSTER_NODES='0@http://node-0:7420@https://node-0.example:7420@https://node-0.example:7421@node-0.example:9092,1@http://node-1:7420@https://node-1.example:7420@https://node-1.example:7421@node-1.example:9092'
```

The internal REST endpoints must be reachable between brokers. Advertised
REST, gRPC, and Kafka endpoints must be reachable by clients. Node IDs,
membership order after sorting, and endpoint strings participate in the
topology digest; a proxied request with a different digest fails closed.

The internal topic-synchronization endpoint requires
`SHARD_STREAM_CLUSTER_TOKEN`. Put internal REST endpoints on a private network
and use a high-entropy token outside local development.

Static membership changes alter ownership and are not online-safe yet. Stop
writes and migrate partition data before changing the list. Dynamic,
certificate-fenced membership belongs to the remote-replication milestone.
