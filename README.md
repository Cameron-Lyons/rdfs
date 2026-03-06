# RDFS v3

RDFS is now a gRPC-based distributed file store with Raft-backed metadata, immutable chunk replicas, and manifest-based atomic commits.

This rewrite intentionally breaks compatibility with the previous JSON/TCP prototype. The old wire protocol, public client API, and checked-in metadata/block artifacts are not part of v3.

## Components

- `rdfs-meta`: metadata node with the public `MetadataService` plus internal `RaftService`
- `rdfs-chunk`: chunkserver with streamed chunk upload/download and replica forwarding
- `rdfs-client`: CLI for `mkdir`, `ls`, `put`, `cat`, `rm`, `stat`, and `demo`
- `rdfs-local` / `xtask`: 3-meta / 3-chunk local cluster harness

## Design

- Metadata uses `openraft` with RocksDB-backed log/state persistence.
- The state machine stores the namespace, file manifests, upload leases, chunk refcounts, tombstones, repair intents, and chunkserver heartbeats.
- Chunkservers store immutable chunk files on disk with a RocksDB index.
- Writers upload chunks to a primary replica, which stores locally and forwards to secondaries before acknowledging.
- `CommitUpload` atomically swaps the visible file manifest, so uncommitted uploads remain invisible.
- Reads are leader-only in v1 and use leader-lease reads before serving metadata.
- Readers report corrupt or unreadable chunk replicas back to metadata so the repair loop can restore replication.
- Chunkserver heartbeats reconcile on-disk inventory back into metadata, so restarted replicas rejoin fresh manifests automatically.
- Metadata membership can be changed online by adding learners, promoting voters, removing dead nodes, and replacing failed voters.

## Client API

The Rust client surface is now high-level:

- `Client::create_writer(path, options)`
- `Client::overwrite_writer(path, options)`
- `Client::open_reader(path)`
- `Client::stat/list/mkdir/rename/delete`

There is no public per-block mutation API in v3.

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
- invisibility of uncommitted uploads
- single-writer lease enforcement
- committed reads survive loss of one chunkserver
- corrupt chunk replicas are detected on read and repaired in the background
- fresh manifests drop dead replicas after failure reports and pick them back up after chunkserver restart heartbeats
- uploads fail cleanly when a required chunk replica disappears before replication completes
- metadata leader failover after a committed write
- metadata voter replacement restoring quorum after losing a node

## Not In Scope

- POSIX/FUSE
- append or random in-place writes
- follower-served linearizable reads
- metadata sharding
- migration from the old prototype’s protocol or disk format
