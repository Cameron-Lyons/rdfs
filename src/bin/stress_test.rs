use rdfs::client::api::DfsClient;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Barrier;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    let master_addr = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:9000".to_string());
    let num_writers: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);
    let num_readers: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(4);
    let ops_per_worker: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(50);
    let block_size: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(4096);

    println!("RDFS Stress Test");
    println!("  master:      {}", master_addr);
    println!("  writers:     {}", num_writers);
    println!("  readers:     {}", num_readers);
    println!("  ops/worker:  {}", ops_per_worker);
    println!("  block_size:  {} bytes", block_size);
    println!();

    let client = match DfsClient::new(&master_addr).await {
        Ok(c) => Arc::new(c),
        Err(e) => {
            eprintln!("Failed to connect to master: {}", e);
            std::process::exit(1);
        }
    };

    let barrier = Arc::new(Barrier::new(num_writers));
    let mut write_handles = Vec::new();
    let total_write_start = Instant::now();

    for worker_id in 0..num_writers {
        let client = Arc::clone(&client);
        let barrier = Arc::clone(&barrier);
        let data = vec![0xABu8; block_size];

        let handle = tokio::spawn(async move {
            barrier.wait().await;
            let mut latencies = Vec::with_capacity(ops_per_worker);

            for op in 0..ops_per_worker {
                let path = format!("/stress_w{}_op{}.dat", worker_id, op);
                let start = Instant::now();
                let result = async {
                    let file = client.open(&path).await?;
                    file.write_block(0, &data).await?;
                    Ok::<_, rdfs::client::error::DfsError>(())
                }
                .await;
                let elapsed = start.elapsed();

                match result {
                    Ok(()) => latencies.push(elapsed.as_micros() as u64),
                    Err(e) => eprintln!("  writer {} op {} failed: {}", worker_id, op, e),
                }
            }

            latencies
        });

        write_handles.push(handle);
    }

    let mut all_write_latencies: Vec<u64> = Vec::new();
    for handle in write_handles {
        match handle.await {
            Ok(lats) => all_write_latencies.extend(lats),
            Err(e) => eprintln!("Writer task panicked: {}", e),
        }
    }
    let total_write_duration = total_write_start.elapsed();

    println!("=== Write Phase ===");
    print_stats(
        &all_write_latencies,
        total_write_duration,
        block_size,
        "write",
    );

    let read_barrier = Arc::new(Barrier::new(num_readers));
    let mut read_handles = Vec::new();
    let total_read_start = Instant::now();

    for reader_id in 0..num_readers {
        let client = Arc::clone(&client);
        let read_barrier = Arc::clone(&read_barrier);
        let writer_idx = reader_id % num_writers;

        let handle = tokio::spawn(async move {
            read_barrier.wait().await;
            let mut latencies = Vec::with_capacity(ops_per_worker);

            for op in 0..ops_per_worker {
                let path = format!("/stress_w{}_op{}.dat", writer_idx, op);
                let start = Instant::now();
                let result = async {
                    let file = client.open(&path).await?;
                    let _data = file.read_block(0).await?;
                    Ok::<_, rdfs::client::error::DfsError>(())
                }
                .await;
                let elapsed = start.elapsed();

                match result {
                    Ok(()) => latencies.push(elapsed.as_micros() as u64),
                    Err(e) => eprintln!("  reader {} op {} failed: {}", reader_id, op, e),
                }
            }

            latencies
        });

        read_handles.push(handle);
    }

    let mut all_read_latencies: Vec<u64> = Vec::new();
    for handle in read_handles {
        match handle.await {
            Ok(lats) => all_read_latencies.extend(lats),
            Err(e) => eprintln!("Reader task panicked: {}", e),
        }
    }
    let total_read_duration = total_read_start.elapsed();

    println!("\n=== Read Phase ===");
    print_stats(&all_read_latencies, total_read_duration, block_size, "read");

    println!("\n=== Cleanup ===");
    let mut deleted = 0u64;
    for worker_id in 0..num_writers {
        for op in 0..ops_per_worker {
            let path = format!("/stress_w{}_op{}.dat", worker_id, op);
            if client.delete(&path).await.is_ok() {
                deleted += 1;
            }
        }
    }
    println!("Deleted {} test files", deleted);

    println!("\n=== Connection Stats ===");
    let stats = client.get_connection_stats().await;
    println!("  total_requests:  {}", stats.total_requests);
    println!("  failed_requests: {}", stats.failed_requests);
    println!("  bytes_sent:      {}", stats.bytes_sent);
    println!("  bytes_received:  {}", stats.bytes_received);
    if let Some(ref err) = stats.last_error {
        println!("  last_error:      {}", err);
    }
}

fn print_stats(latencies: &[u64], wall_time: std::time::Duration, block_size: usize, label: &str) {
    if latencies.is_empty() {
        println!("  No successful {} operations", label);
        return;
    }

    let mut sorted = latencies.to_vec();
    sorted.sort();

    let total_ops = sorted.len();
    let p50 = sorted[total_ops * 50 / 100];
    let p95 = sorted[total_ops * 95 / 100];
    let p99 = sorted[(total_ops * 99 / 100).min(total_ops - 1)];
    let mean: u64 = sorted.iter().sum::<u64>() / total_ops as u64;

    let wall_secs = wall_time.as_secs_f64();
    let ops_per_sec = total_ops as f64 / wall_secs;
    let mb_per_sec = (total_ops as f64 * block_size as f64) / (wall_secs * 1024.0 * 1024.0);

    println!("  successful ops: {}", total_ops);
    println!("  wall time:      {:.2}s", wall_secs);
    println!(
        "  throughput:     {:.1} ops/s, {:.2} MB/s",
        ops_per_sec, mb_per_sec
    );
    println!(
        "  latency (us):   mean={}, p50={}, p95={}, p99={}",
        mean, p50, p95, p99
    );
}
