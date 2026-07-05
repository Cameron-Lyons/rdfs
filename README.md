# RDFS

RDFS is now a gRPC-based distributed file store with Raft-backed metadata, immutable chunk replicas, and manifest-based atomic commits.

## Components

- `rdfs-meta`: metadata node with the public `MetadataService` plus internal `RaftService`
- `rdfs-chunk`: chunkserver with streamed chunk upload/download and replica forwarding
- `rdfs-client`: CLI for `mkdir`, `ls`, `put`, `cat`, `rm`, `stat`, and `demo`
- `rdfs-local`: 3-meta / 3-chunk local cluster harness

## Design

- Metadata uses `openraft` with a RocksDB-backed log. The state machine lives in memory and recovers from the latest snapshot plus committed-log replay, so applies stay O(entry) instead of re-serializing the namespace.
- The state machine stores the namespace, file manifests, upload leases, chunk refcounts, tombstones, repair intents, and chunkserver heartbeats.
- Chunkservers store immutable chunk files on disk with a RocksDB index.
- Chunk transfers are framed streams (256 KiB messages), so chunks are not bounded by the gRPC message limit and are never buffered whole in memory on the serving path.
- Writers upload chunks to a primary replica; replicas form a chain that forwards frames downstream while writing, and the primary acknowledges only after the whole chain has verified the chunk.
- `CommitUpload` atomically swaps the visible file manifest, so uncommitted uploads remain invisible.
- Reads use leader leases before serving metadata; readers fetch multiple chunks concurrently and verify each against the manifest checksum.
- Readers report corrupt or unreadable chunk replicas back to metadata so the repair loop can restore replication.
- Chunkserver heartbeats carry an inventory digest and include the full inventory only when it changes, so steady-state heartbeats are constant-size; reconciliation lets restarted replicas rejoin fresh manifests automatically.
- All peers talk over cached, lazily-connected gRPC channels instead of dialing per RPC.
- Metadata membership can be changed online by adding learners, promoting voters, removing dead nodes, and replacing failed voters.

## Client API

The Rust client surface is high-level:

- `Client::create_writer(path, options)`
- `Client::overwrite_writer(path, options)`
- `Client::open_reader(path)`
- `Client::stat/list/mkdir/delete`

There is no public per-block mutation API.

## Quick Start

Launch a local cluster:

```bash
cargo run --bin rdfs-local
```

Run the demo client:

```bash
cargo run --bin rdfs-client -- 127.0.0.1:9500,127.0.0.1:9501,127.0.0.1:9502 demo
```

Manual metadata startup:

```bash
cargo run --bin rdfs-meta -- 1 127.0.0.1:9500 /tmp/rdfs/meta-1 1=127.0.0.1:9500,2=127.0.0.1:9501,3=127.0.0.1:9502 --bootstrap
cargo run --bin rdfs-meta -- 2 127.0.0.1:9501 /tmp/rdfs/meta-2 1=127.0.0.1:9500,2=127.0.0.1:9501,3=127.0.0.1:9502
cargo run --bin rdfs-meta -- 3 127.0.0.1:9502 /tmp/rdfs/meta-3 1=127.0.0.1:9500,2=127.0.0.1:9501,3=127.0.0.1:9502
```

Manual chunkserver startup:

```bash
cargo run --bin rdfs-chunk -- chunk-1 127.0.0.1:9510 /tmp/rdfs/chunk-1 127.0.0.1:9500,127.0.0.1:9501,127.0.0.1:9502
cargo run --bin rdfs-chunk -- chunk-2 127.0.0.1:9511 /tmp/rdfs/chunk-2 127.0.0.1:9500,127.0.0.1:9501,127.0.0.1:9502
cargo run --bin rdfs-chunk -- chunk-3 127.0.0.1:9512 /tmp/rdfs/chunk-3 127.0.0.1:9500,127.0.0.1:9501,127.0.0.1:9502
```

CLI examples:

```bash
cargo run --bin rdfs-client -- 127.0.0.1:9500,127.0.0.1:9501,127.0.0.1:9502 mkdir /docs
cargo run --bin rdfs-client -- 127.0.0.1:9500,127.0.0.1:9501,127.0.0.1:9502 put /docs/file.txt hello
cargo run --bin rdfs-client -- 127.0.0.1:9500,127.0.0.1:9501,127.0.0.1:9502 cat /docs/file.txt
cargo run --bin rdfs-client -- 127.0.0.1:9500,127.0.0.1:9501,127.0.0.1:9502 meta-membership
cargo run --bin rdfs-client -- 127.0.0.1:9500,127.0.0.1:9501,127.0.0.1:9502 meta-add 4 127.0.0.1:9503 --voter
cargo run --bin rdfs-client -- 127.0.0.1:9500,127.0.0.1:9501,127.0.0.1:9502 meta-replace 1 4 127.0.0.1:9503
```

## Testing

```bash
cargo test
```

Current test coverage includes:

- path normalization
- atomic create/write/read/overwrite/delete on a 3-meta / 3-chunk cluster
- multi-chunk round trips with chunks larger than the gRPC message limit
- invisibility of uncommitted uploads
- single-writer lease enforcement
- committed reads survive loss of one chunkserver
- corrupt chunk replicas are detected on read and repaired in the background
- fresh manifests drop dead replicas after failure reports and pick them back up after chunkserver restart heartbeats
- uploads fail cleanly when a required chunk replica disappears before replication completes
- metadata leader failover after a committed write
- metadata node restart recovering its state from snapshot and log replay
- metadata voter replacement restoring quorum after losing a node

## Not In Scope

- POSIX/FUSE
- append or random in-place writes
- follower-served linearizable reads
- metadata sharding
