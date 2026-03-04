use crate::client::api::DfsClient;
use crate::client::error::DfsError;
use std::time::Instant;

pub async fn run(master_addr: &str, print_stats: bool) -> Result<(), DfsError> {
    println!("Connecting to {master_addr}");
    let client = DfsClient::new(master_addr).await?;

    client.mkdir("/demo").await.ok();

    let file = client.create("/demo/demo.txt").await?;

    println!("=== Transactional write ===");
    let mut session = file.begin_write().await?;
    let chunks: [&[u8]; 3] = [
        b"Block 0: hello ",
        b"Block 1: distributed ",
        b"Block 2: filesystem",
    ];

    let start = Instant::now();
    for chunk in chunks {
        if let Err(err) = session.write_chunk(chunk).await {
            let _ = session.abort().await;
            return Err(err);
        }
    }
    session.commit().await?;
    println!("Committed in {:?}", start.elapsed());

    println!("=== Read back ===");
    let data = file.read_all().await?;
    println!("{}", String::from_utf8_lossy(&data));

    println!("=== Directory list ===");
    if let Ok(entries) = client.list("/demo").await {
        for entry in entries {
            println!(
                "{} {}",
                if entry.is_dir { "dir " } else { "file" },
                entry.path
            );
        }
    }

    if print_stats {
        let stats = client.get_connection_stats().await;
        println!("=== Stats ===");
        println!("total_requests: {}", stats.total_requests);
        println!("failed_requests: {}", stats.failed_requests);
        println!("bytes_sent: {}", stats.bytes_sent);
        println!("bytes_received: {}", stats.bytes_received);
        if let Some(err) = stats.last_error {
            println!("last_error: {err}");
        }
    }

    Ok(())
}
