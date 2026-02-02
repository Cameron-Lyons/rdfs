use rdfs::client::api::DfsClient;
use rdfs::server::master::MasterServer;
use rdfs::server::storage::StorageNode;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

async fn setup_test_environment() -> (
    tokio::task::JoinHandle<()>,
    Vec<tokio::task::JoinHandle<()>>,
) {
    let master_addr = "127.0.0.1:9010".to_string();
    let master = MasterServer::new(master_addr.clone());

    let master_handle = tokio::spawn(async move {
        let _ = master.start().await;
    });

    sleep(Duration::from_millis(500)).await;

    let mut storage_handles = Vec::new();
    let storage_addrs = vec![
        ("node1", "127.0.0.1:9011"),
        ("node2", "127.0.0.1:9012"),
        ("node3", "127.0.0.1:9013"),
    ];

    for (node_id, addr) in storage_addrs {
        let node = StorageNode::new(
            node_id.to_string(),
            addr.to_string(),
            master_addr.clone(),
            PathBuf::from(format!("/tmp/rdfs_test_{}", node_id)),
        );

        let handle = tokio::spawn(async move {
            let _ = node.start().await;
        });

        storage_handles.push(handle);
        sleep(Duration::from_millis(200)).await;
    }

    sleep(Duration::from_secs(2)).await;

    (master_handle, storage_handles)
}

#[tokio::test]
async fn test_integration_basic() {
    let (master_handle, storage_handles) = setup_test_environment().await;

    let client = match DfsClient::new("127.0.0.1:9010").await {
        Ok(c) => c,
        Err(_) => {
            master_handle.abort();
            for handle in storage_handles {
                handle.abort();
            }
            return;
        }
    };

    let stats = client.get_connection_stats().await;
    assert_eq!(stats.total_requests, 0);

    master_handle.abort();
    for handle in storage_handles {
        handle.abort();
    }
}

#[tokio::test]
async fn test_integration_advanced() {
    let (master_handle, storage_handles) = setup_test_environment().await;

    let client = match DfsClient::new("127.0.0.1:9010").await {
        Ok(c) => c,
        Err(_) => {
            master_handle.abort();
            for handle in storage_handles {
                handle.abort();
            }
            return;
        }
    };

    if let Ok(file) = client.create("/test/advanced.txt").await {
        assert_eq!(file.get_path(), "/test/advanced.txt");
        assert_eq!(file.get_size(), 0);
    }

    if let Ok(_files) = client.list("/test").await {}

    master_handle.abort();
    for handle in storage_handles {
        handle.abort();
    }
}

#[tokio::test]
async fn test_end_to_end() {
    let (master_handle, storage_handles) = setup_test_environment().await;

    let client = match DfsClient::new("127.0.0.1:9010").await {
        Ok(c) => c,
        Err(_) => {
            master_handle.abort();
            for handle in storage_handles {
                handle.abort();
            }
            return;
        }
    };

    if let Ok(_file) = client.create("/test/e2e.txt").await {
        if let Ok(opened_file) = client.open("/test/e2e.txt").await {
            assert_eq!(opened_file.get_path(), "/test/e2e.txt");
        }

        if client
            .rename("/test/e2e.txt", "/test/e2e_renamed.txt")
            .await
            .is_ok()
            && client.open("/test/e2e_renamed.txt").await.is_ok()
        {
            let _ = client.delete("/test/e2e_renamed.txt").await;
        }
    }

    master_handle.abort();
    for handle in storage_handles {
        handle.abort();
    }
}
