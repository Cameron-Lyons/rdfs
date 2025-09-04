#!/bin/bash

# Demo script for RDFS - Rust Distributed File System

echo "RDFS Demo - Starting distributed file system..."
echo "============================================="

# Clean up any existing data
rm -rf ./data/

# Build the project
echo "Building project..."
cargo build --release

# Start master server in background
echo "Starting master server on port 9000..."
cargo run --release --bin master -- 127.0.0.1:9000 &
MASTER_PID=$!
sleep 2

# Start 3 storage nodes
echo "Starting storage nodes..."
cargo run --release --bin storage -- node1 127.0.0.1:9001 127.0.0.1:9000 ./data/node1 &
NODE1_PID=$!
sleep 1

cargo run --release --bin storage -- node2 127.0.0.1:9002 127.0.0.1:9000 ./data/node2 &
NODE2_PID=$!
sleep 1

cargo run --release --bin storage -- node3 127.0.0.1:9003 127.0.0.1:9000 ./data/node3 &
NODE3_PID=$!
sleep 2

echo "============================================="
echo "System is running!"
echo "Master PID: $MASTER_PID"
echo "Node1 PID: $NODE1_PID"
echo "Node2 PID: $NODE2_PID"
echo "Node3 PID: $NODE3_PID"
echo "============================================="

# Run client operations
echo "Running client operations..."
cargo run --release --bin client

echo "============================================="
echo "Demo complete. Press Enter to shutdown..."
read

# Cleanup
echo "Shutting down..."
kill $MASTER_PID $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null
rm -rf ./data/
echo "Cleanup complete."