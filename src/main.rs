use rdfs::client::api::DfsClient;

#[tokio::main]
async fn main() {
    let client = DfsClient::new("127.0.0.1:9000")
        .await
        .expect("Failed to connect");

    println!("Creating new file /hello.txt");
    let file = client.create("/hello.txt").await.expect("Failed to create file");
    println!("File created at: {}", file.get_path());

    println!("\nWriting data to block 0");
    file.write_block(0, b"Hello, DFS!").await.expect("Write failed");

    println!("Reading data from block 0");
    let data = file.read_block(0).await.expect("Read failed");
    println!("Read block: {}", String::from_utf8_lossy(&data));

    println!("\nFile metadata:");
    println!("  Size: {} bytes", file.get_size());
    println!("  Blocks: {}", file.get_block_count());
    println!("  Replicas: {}", file.get_replica_count());

    println!("\nListing files in /");
    match client.list("/").await {
        Ok(files) => {
            for f in &files {
                println!("  {} - {} bytes", f.path, f.size);
            }
        }
        Err(e) => println!("List failed: {}", e),
    }

    println!("\nRenaming /hello.txt to /goodbye.txt");
    client.rename("/hello.txt", "/goodbye.txt").await.expect("Rename failed");

    println!("Opening renamed file");
    let renamed_file = client.open("/goodbye.txt").await.expect("Failed to open renamed file");
    let data = renamed_file.read_block(0).await.expect("Read failed");
    println!("Read from renamed file: {}", String::from_utf8_lossy(&data));

    println!("\nDeleting /goodbye.txt");
    client.delete("/goodbye.txt").await.expect("Delete failed");
    
    println!("\nConnection statistics:");
    let stats = client.get_connection_stats().await;
    println!("  Total requests: {}", stats.total_requests);
    println!("  Failed requests: {}", stats.failed_requests);
    println!("  Bytes sent: {}", stats.bytes_sent);
    println!("  Bytes received: {}", stats.bytes_received);
    if let Some(error) = stats.last_error {
        println!("  Last error: {}", error);
    }
}
