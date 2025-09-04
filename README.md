# RDFS - Rust Distributed File System

A simple distributed file system implementation in Rust with replication, fault tolerance, and consistency mechanisms.

## Architecture

The system consists of three main components:

1. **Master Server**: Manages file metadata, coordinates operations, and monitors node health
2. **Storage Nodes**: Store actual data blocks with replication
3. **Client Library**: Provides API for file operations

### Key Features

- Block-based storage (4MB blocks by default)
- Configurable replication factor (default: 3)
- Automatic failover and re-replication
- Heartbeat monitoring for node health
- Simple consistency model

## Building

```bash
cargo build --release
```

## Running the System

### 1. Start the Master Server

```bash
cargo run --bin master -- [master_addr]
# Example:
cargo run --bin master -- 127.0.0.1:9000
```

### 2. Start Storage Nodes

```bash
cargo run --bin storage -- <node_id> <listen_addr> [master_addr] [data_dir]
# Examples:
cargo run --bin storage -- node1 127.0.0.1:9001 127.0.0.1:9000 ./data/node1
cargo run --bin storage -- node2 127.0.0.1:9002 127.0.0.1:9000 ./data/node2
cargo run --bin storage -- node3 127.0.0.1:9003 127.0.0.1:9000 ./data/node3
```

### 3. Run Client Operations

```bash
cargo run --bin client
```

## API Usage

```rust
use rdfs::client::api::DfsClient;

// Connect to master
let client = DfsClient::new("127.0.0.1:9000").await?;

// Open or create a file
let file = client.open("/my_file.txt").await?;

// Write data to blocks
file.write_block(0, b"Hello, DFS!").await?;

// Read data from blocks
let data = file.read_block(0).await?;

// Delete file
client.delete("/my_file.txt").await?;
```

## Testing

Run integration tests:

```bash
cargo test
```

## Implementation Details

### Metadata Management
- Files are tracked with path, size, blocks, and replication info
- Block allocation is managed by the master server
- Node registry maintains storage node information

### Replication Strategy
- Write operations replicate to N nodes (configurable)
- Reads can be served from any replica
- Failed nodes trigger re-replication to maintain replication factor

### Consistency Model
- Simple primary-backup replication
- Eventual consistency with versioning
- Write operations require majority acknowledgment

### Fault Tolerance
- Heartbeat monitoring (10-second intervals)
- Automatic node failure detection (60-second timeout)
- Re-replication on node failure
- Graceful handling of network partitions

## Architecture Diagram

```
┌─────────┐     ┌──────────────┐     ┌─────────────┐
│ Client  │────▶│ Master Server│────▶│  Metadata   │
└─────────┘     └──────────────┘     │    Store    │
     │                 │              └─────────────┘
     │                 │
     ▼                 ▼
┌─────────────────────────────────────────────────┐
│              Storage Nodes                      │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐        │
│  │ Node 1  │  │ Node 2  │  │ Node 3  │  ...   │
│  └─────────┘  └─────────┘  └─────────┘        │
└─────────────────────────────────────────────────┘
```

## Future Improvements

- Implement proper consensus algorithm (Raft/Paxos)
- Add data compression and deduplication
- Support for directories and file permissions
- Implement caching layer
- Add encryption for data at rest and in transit
- Support for large file streaming
- Implement proper transaction support
- Add monitoring and metrics collection