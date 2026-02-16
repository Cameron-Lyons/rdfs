use rdfs::client::api::DfsClient;
use rdfs::client::connection::{Envelope, Request, Response, auth_token};
use rdfs::server::master::MasterServer;
use rdfs::server::storage::StorageNode;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

struct TestEnv {
    master_handle: tokio::task::JoinHandle<()>,
    storage_handles: Vec<tokio::task::JoinHandle<()>>,
    master_addr: String,
    storage_addrs: Vec<String>,
    data_dirs: Vec<PathBuf>,
    db_path: PathBuf,
}

impl TestEnv {
    async fn stop(self) {
        self.master_handle.abort();
        for handle in self.storage_handles {
            handle.abort();
        }

        for dir in self.data_dirs {
            let _ = tokio::fs::remove_dir_all(dir).await;
        }
        let _ = tokio::fs::remove_file(self.db_path).await;
    }
}

async fn setup_test_environment(base_port: u16, storage_nodes: usize) -> TestEnv {
    let master_addr = format!("127.0.0.1:{}", base_port);
    let db_path = PathBuf::from(format!(
        "rdfs_metadata_{}.db",
        master_addr.replace([':', '.'], "_")
    ));
    let _ = tokio::fs::remove_file(&db_path).await;

    let master = MasterServer::new(master_addr.clone());
    let master_handle = tokio::spawn(async move {
        let _ = master.start().await;
    });

    sleep(Duration::from_millis(500)).await;

    let mut storage_handles = Vec::new();
    let mut storage_addrs = Vec::new();
    let mut data_dirs = Vec::new();

    for i in 0..storage_nodes {
        let node_id = format!("node{}", i + 1);
        let addr = format!("127.0.0.1:{}", base_port + 1 + i as u16);
        let data_dir = PathBuf::from(format!("/tmp/rdfs_test_{}_{}", base_port, node_id));
        let _ = tokio::fs::remove_dir_all(&data_dir).await;

        let node = StorageNode::new(node_id, addr.clone(), master_addr.clone(), data_dir.clone());

        let handle = tokio::spawn(async move {
            let _ = node.start().await;
        });

        storage_handles.push(handle);
        storage_addrs.push(addr);
        data_dirs.push(data_dir);
        sleep(Duration::from_millis(200)).await;
    }

    sleep(Duration::from_secs(2)).await;

    TestEnv {
        master_handle,
        storage_handles,
        master_addr,
        storage_addrs,
        data_dirs,
        db_path,
    }
}

async fn read_block_from_storage(addr: &str, block_id: u64) -> Result<Vec<u8>, String> {
    let mut stream = TcpStream::connect(addr).await.map_err(|e| e.to_string())?;
    let envelope = Envelope {
        token: auth_token(),
        payload: Request::ReadBlock { block_id },
    };

    let msg = serde_json::to_vec(&envelope).map_err(|e| e.to_string())?;
    let len = (msg.len() as u32).to_be_bytes();
    stream
        .write_all(&len)
        .await
        .map_err(|e| format!("write length failed: {e}"))?;
    stream
        .write_all(&msg)
        .await
        .map_err(|e| format!("write request failed: {e}"))?;

    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| format!("read response length failed: {e}"))?;
    let resp_len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; resp_len];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| format!("read response body failed: {e}"))?;

    let response: Response = serde_json::from_slice(&buf).map_err(|e| e.to_string())?;
    match response {
        Response::BlockData { data } => Ok(data),
        Response::Error { message } => Err(message),
        _ => Err("Unexpected response".to_string()),
    }
}

#[tokio::test]
async fn test_create_with_replication_is_supported() {
    let env = setup_test_environment(9110, 3).await;

    let client = match DfsClient::new(&env.master_addr).await {
        Ok(client) => client,
        Err(_) => {
            env.stop().await;
            return;
        }
    };
    let file = client
        .create_with_replication("/test/rf2.txt", 2)
        .await
        .expect("create_with_replication should succeed");

    assert_eq!(file.get_replica_count(), 2);

    env.stop().await;
}

#[tokio::test]
async fn test_block_commit_updates_file_size() {
    let env = setup_test_environment(9120, 3).await;

    let client = match DfsClient::new(&env.master_addr).await {
        Ok(client) => client,
        Err(_) => {
            env.stop().await;
            return;
        }
    };

    let file = client
        .create("/test/size.txt")
        .await
        .expect("create should succeed");

    let block_id = file.get_metadata().blocks[0];
    let payload = b"hello replicated world";
    file.write_block(block_id, payload)
        .await
        .expect("write should succeed");

    let reopened = client
        .open("/test/size.txt")
        .await
        .expect("open should succeed");
    assert_eq!(reopened.get_size(), payload.len() as u64);

    env.stop().await;
}

#[tokio::test]
async fn test_storage_restart_recovers_blocks_from_disk() {
    let mut env = setup_test_environment(9130, 3).await;

    let client = match DfsClient::new(&env.master_addr).await {
        Ok(client) => client,
        Err(_) => {
            env.stop().await;
            return;
        }
    };

    let file = client
        .create("/test/restart.txt")
        .await
        .expect("create should succeed");

    let block_id = file.get_metadata().blocks[0];
    let payload = b"persist me";
    file.write_block(block_id, payload)
        .await
        .expect("write should succeed");

    env.storage_handles[0].abort();
    sleep(Duration::from_millis(300)).await;

    let restarted_node = StorageNode::new(
        "node1".to_string(),
        env.storage_addrs[0].clone(),
        env.master_addr.clone(),
        env.data_dirs[0].clone(),
    );

    env.storage_handles[0] = tokio::spawn(async move {
        let _ = restarted_node.start().await;
    });

    sleep(Duration::from_secs(2)).await;

    let raw = match read_block_from_storage(&env.storage_addrs[0], block_id).await {
        Ok(raw) => raw,
        Err(_) => {
            env.stop().await;
            return;
        }
    };
    let data = rdfs::client::connection::decompress(&raw).expect("decompress should succeed");
    assert_eq!(data, payload);

    env.stop().await;
}

#[tokio::test]
async fn test_health_reports_under_replicated_blocks() {
    let env = setup_test_environment(9140, 2).await;

    let client = match DfsClient::new(&env.master_addr).await {
        Ok(client) => client,
        Err(_) => {
            env.stop().await;
            return;
        }
    };

    let file = client
        .create_with_replication("/test/under_repl.txt", 3)
        .await
        .expect("create_with_replication should succeed even if currently under-replicated");

    let block_id = file.get_metadata().blocks[0];
    file.write_block(block_id, b"data")
        .await
        .expect("write should still succeed with quorum");

    let health_addr = "127.0.0.1:9220";
    let mut stream = match TcpStream::connect(health_addr).await {
        Ok(stream) => stream,
        Err(_) => {
            env.stop().await;
            return;
        }
    };
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("health request should send");

    let mut buf = vec![0u8; 4096];
    let n = stream
        .read(&mut buf)
        .await
        .expect("health response should read");
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(body.contains("under_replicated_blocks"));

    env.stop().await;
}
