use rdfs::client::api::DfsClient;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let master_addr = "127.0.0.1:9000";

    println!("Connecting to DFS master at {}...", master_addr);
    let client = match DfsClient::new(master_addr).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect to master: {}", e);
            return;
        }
    };

    println!("Connected successfully!\n");

    println!("=== Test 1: Basic file operations ===");
    let file_path = "/test_file.txt";
    let test_data = b"Hello, Distributed File System with improved connection management!";

    let file = client.open(file_path).await.expect("Failed to open file");

    let start = Instant::now();
    file.write_block(0, test_data)
        .await
        .expect("Failed to write block");
    println!("Write completed in {:?}", start.elapsed());

    let start = Instant::now();
    let read_data = file.read_block(0).await.expect("Failed to read block");
    println!("Read completed in {:?}", start.elapsed());

    assert_eq!(read_data, test_data);
    println!("Data integrity verified ✓\n");

    println!("=== Test 2: Multiple blocks ===");
    let multi_file = client
        .open("/multi_block.txt")
        .await
        .expect("Failed to open file");

    let block_data: Vec<&[u8]> = vec![
        b"Block 0: The quick brown fox",
        b"Block 1: jumps over the lazy dog",
        b"Block 2: and runs through the forest",
    ];

    for (i, data) in block_data.iter().enumerate() {
        multi_file
            .write_block(i as u64, data)
            .await
            .unwrap_or_else(|_| panic!("Failed to write block {}", i));
        println!("Wrote block {}: {} bytes", i, data.len());
    }

    for (i, expected) in block_data.iter().enumerate() {
        let actual = multi_file
            .read_block(i as u64)
            .await
            .unwrap_or_else(|_| panic!("Failed to read block {}", i));
        assert_eq!(&actual[..], *expected);
        println!("Verified block {}: {} bytes", i, actual.len());
    }
    println!("Multi-block test passed ✓\n");

    println!("=== Connection Statistics ===");
    let stats = client.get_connection_stats().await;
    println!("Total requests: {}", stats.total_requests);
    println!("Failed requests: {}", stats.failed_requests);
    println!("Bytes sent: {}", stats.bytes_sent);
    println!("Bytes received: {}", stats.bytes_received);
    if let Some(err) = stats.last_error {
        println!("Last error: {}", err);
    }

    let success_rate = if stats.total_requests > 0 {
        ((stats.total_requests - stats.failed_requests) as f64 / stats.total_requests as f64)
            * 100.0
    } else {
        100.0
    };
    println!("Success rate: {:.2}%\n", success_rate);

    println!("=== Test 4: Concurrent operations ===");
    let mut tasks = Vec::new();
    let num_concurrent = 10;

    for i in 0..num_concurrent {
        let client_clone = client.clone();
        let task = tokio::spawn(async move {
            let path = format!("/concurrent_file_{}.txt", i);
            let data = format!("Concurrent data from task {}", i);

            let file = client_clone.open(&path).await?;
            file.write_block(0, data.as_bytes()).await?;
            let read = file.read_block(0).await?;

            Ok::<_, rdfs::client::error::DfsError>((i, read))
        });
        tasks.push(task);
    }

    let mut successful = 0;
    for task in tasks {
        match task.await {
            Ok(Ok((i, _data))) => {
                successful += 1;
                println!("Task {} completed successfully", i);
            }
            Ok(Err(e)) => println!("Task failed: {}", e),
            Err(e) => println!("Task panicked: {}", e),
        }
    }
    println!(
        "Concurrent test: {}/{} successful\n",
        successful, num_concurrent
    );

    println!("=== Final Statistics ===");
    let final_stats = client.get_connection_stats().await;
    println!("Total operations: {}", final_stats.total_requests);
    println!(
        "Total data transferred: {} bytes",
        final_stats.bytes_sent + final_stats.bytes_received
    );

    println!("\n=== Cleanup ===");
    client
        .delete(file_path)
        .await
        .expect("Failed to delete test file");
    client
        .delete("/multi_block.txt")
        .await
        .expect("Failed to delete multi-block file");
    for i in 0..num_concurrent {
        let path = format!("/concurrent_file_{}.txt", i);
        client.delete(&path).await.ok();
    }
    println!("Cleanup completed ✓");

    println!("\nAll tests completed successfully!");
}

