use rdfs::client::api::DfsClient;
use rdfs::server::{master::MasterServer, storage::StorageNode};
use std::path::PathBuf;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_basic_file_operations() {
    let master_addr = "127.0.0.1:19000";
    let master = MasterServer::new(master_addr.to_string());
    
    tokio::spawn(async move {
        let _ = master.start().await;
    });

    sleep(Duration::from_millis(500)).await;

    for i in 1..=3 {
        let node_id = format!("test_node_{}", i);
        let addr = format!("127.0.0.1:1900{}", i);
        let data_dir = PathBuf::from(format!("./test_data/{}", node_id));
        
        let node = StorageNode::new(
            node_id,
            addr,
            master_addr.to_string(),
            data_dir,
        );

        tokio::spawn(async move {
            let _ = node.start().await;
        });
    }

    sleep(Duration::from_secs(2)).await;

    let client = DfsClient::new(master_addr)
        .await
        .expect("Failed to connect to master");

    let test_data = b"Hello, Distributed File System!";
    let file = client
        .open("/test_file.txt")
        .await
        .expect("Failed to open file");

    file.write_block(0, test_data)
        .await
        .expect("Failed to write block");

    let read_data = file.read_block(0).await.expect("Failed to read block");
    assert_eq!(read_data, test_data);

    client
        .delete("/test_file.txt")
        .await
        .expect("Failed to delete file");

    tokio::fs::remove_dir_all("./test_data").await.ok();
}

#[tokio::test]
async fn test_replication() {
    let master_addr = "127.0.0.1:19010";
    let master = MasterServer::new(master_addr.to_string());
    
    tokio::spawn(async move {
        let _ = master.start().await;
    });

    sleep(Duration::from_millis(500)).await;

    let mut node_handles = Vec::new();
    for i in 1..=5 {
        let node_id = format!("rep_node_{}", i);
        let addr = format!("127.0.0.1:1901{}", i);
        let data_dir = PathBuf::from(format!("./test_rep_data/{}", node_id));
        
        let node = StorageNode::new(
            node_id,
            addr,
            master_addr.to_string(),
            data_dir,
        );

        let handle = tokio::spawn(async move {
            let _ = node.start().await;
        });
        node_handles.push(handle);
    }

    sleep(Duration::from_secs(2)).await;

    let client = DfsClient::new(master_addr)
        .await
        .expect("Failed to connect to master");

    let test_data = b"Testing replication!";
    let file = client
        .open("/replicated_file.txt")
        .await
        .expect("Failed to open file");

    file.write_block(0, test_data)
        .await
        .expect("Failed to write block");

    node_handles[0].abort();
    sleep(Duration::from_secs(1)).await;

    let read_data = file.read_block(0).await.expect("Failed to read from replica");
    assert_eq!(read_data, test_data);

    client
        .delete("/replicated_file.txt")
        .await
        .expect("Failed to delete file");

    tokio::fs::remove_dir_all("./test_rep_data").await.ok();
}

#[tokio::test]
async fn test_multiple_blocks() {
    let master_addr = "127.0.0.1:19020";
    let master = MasterServer::new(master_addr.to_string());
    
    tokio::spawn(async move {
        let _ = master.start().await;
    });

    sleep(Duration::from_millis(500)).await;

    for i in 1..=3 {
        let node_id = format!("mb_node_{}", i);
        let addr = format!("127.0.0.1:1902{}", i);
        let data_dir = PathBuf::from(format!("./test_mb_data/{}", node_id));
        
        let node = StorageNode::new(
            node_id,
            addr,
            master_addr.to_string(),
            data_dir,
        );

        tokio::spawn(async move {
            let _ = node.start().await;
        });
    }

    sleep(Duration::from_secs(2)).await;

    let client = DfsClient::new(master_addr)
        .await
        .expect("Failed to connect to master");

    let file = client
        .open("/multi_block_file.txt")
        .await
        .expect("Failed to open file");

    let block1 = b"First block data";
    let block2 = b"Second block data";
    let block3 = b"Third block data";

    file.write_block(0, block1).await.expect("Failed to write block 0");
    file.write_block(1, block2).await.expect("Failed to write block 1");
    file.write_block(2, block3).await.expect("Failed to write block 2");

    assert_eq!(file.read_block(0).await.unwrap(), block1);
    assert_eq!(file.read_block(1).await.unwrap(), block2);
    assert_eq!(file.read_block(2).await.unwrap(), block3);

    client
        .delete("/multi_block_file.txt")
        .await
        .expect("Failed to delete file");

    tokio::fs::remove_dir_all("./test_mb_data").await.ok();
}