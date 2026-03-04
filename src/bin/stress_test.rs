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
    let num_workers: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);
    let ops_per_worker: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(25);
    let chunk_size: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(4096);

    println!("RDFS v2 stress test");
    println!("  master: {}", master_addr);
    println!("  workers: {}", num_workers);
    println!("  ops/worker: {}", ops_per_worker);
    println!("  chunk_size: {}", chunk_size);

    let client = match DfsClient::new(&master_addr).await {
        Ok(client) => Arc::new(client),
        Err(err) => {
            eprintln!("Failed to connect: {}", err);
            return;
        }
    };

    client.mkdir("/stress").await.ok();

    let barrier = Arc::new(Barrier::new(num_workers));
    let start = Instant::now();
    let mut handles = Vec::new();

    for worker_id in 0..num_workers {
        let client = Arc::clone(&client);
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let payload = vec![0xABu8; chunk_size];
            let mut ok = 0usize;

            for op in 0..ops_per_worker {
                let path = format!("/stress/w{}_op{}.dat", worker_id, op);
                let result = async {
                    let file = client.create(&path).await?;
                    let mut write = file.begin_write().await?;
                    write.write_chunk(&payload).await?;
                    write.commit().await?;
                    let data = file.read_all().await?;
                    if data != payload {
                        return Err(rdfs::client::error::DfsError::Protocol(
                            "readback mismatch".to_string(),
                        ));
                    }
                    Ok::<_, rdfs::client::error::DfsError>(())
                }
                .await;

                if result.is_ok() {
                    ok += 1;
                }
            }

            ok
        }));
    }

    let mut total_ok = 0usize;
    for handle in handles {
        match handle.await {
            Ok(ok) => total_ok += ok,
            Err(err) => eprintln!("Worker panicked: {}", err),
        }
    }

    let elapsed = start.elapsed();
    let total_ops = num_workers * ops_per_worker;
    println!("Successful ops: {}/{}", total_ok, total_ops);
    println!("Elapsed: {:.2}s", elapsed.as_secs_f64());
    println!(
        "Throughput: {:.1} ops/s",
        total_ok as f64 / elapsed.as_secs_f64()
    );

    let stats = client.get_connection_stats().await;
    println!("Requests: {}", stats.total_requests);
    println!("Failed requests: {}", stats.failed_requests);
}
