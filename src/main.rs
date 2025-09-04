use dfs_client::client::DfsClient;

#[tokio::main]
async fn main() {
    let client = DfsClient::new("127.0.0.1:9000")
        .await
        .expect("Failed to connect");

    let file = client.open("/hello.txt").await.expect("Failed to open file");

    file.write_block(0, b"Hello, DFS!").await.expect("Write failed");

    let data = file.read_block(0).await.expect("Read failed");
    println!("Read block: {}", String::from_utf8_lossy(&data));

    client.delete("/hello.txt").await.expect("Delete failed");
}
