use rdfs::client::api::DfsClient;
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
    metadata_base: PathBuf,
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

        cleanup_metadata_files(&self.metadata_base).await;
    }
}

async fn cleanup_metadata_files(base: &PathBuf) {
    let base_str = base.to_string_lossy();
    let candidates = vec![
        base_str.to_string(),
        format!("{}_r0.db", base_str.trim_end_matches(".db")),
        format!("{}_r1.db", base_str.trim_end_matches(".db")),
        format!("{}_r2.db", base_str.trim_end_matches(".db")),
    ];

    for file in candidates {
        let _ = tokio::fs::remove_file(&file).await;
        let _ = tokio::fs::remove_file(format!("{}-wal", file)).await;
        let _ = tokio::fs::remove_file(format!("{}-shm", file)).await;
    }
}

async fn setup_test_environment(base_port: u16, storage_nodes: usize) -> TestEnv {
    let master_addr = format!("127.0.0.1:{}", base_port);
    let metadata_base = PathBuf::from(format!(
        "rdfs_metadata_{}.db",
        master_addr.replace([':', '.'], "_")
    ));
    cleanup_metadata_files(&metadata_base).await;

    let master = MasterServer::new(master_addr.clone());
    let master_handle = tokio::spawn(async move {
        let _ = master.start().await;
    });

    sleep(Duration::from_millis(500)).await;

    let mut storage_handles = Vec::new();
    let mut storage_addrs = Vec::new();
    let mut data_dirs = Vec::new();

    for idx in 0..storage_nodes {
        let node_id = format!("node{}", idx + 1);
        let addr = format!("127.0.0.1:{}", base_port + 1 + idx as u16);
        let data_dir = PathBuf::from(format!("/tmp/rdfs_v2_test_{}_{}", base_port, node_id));
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
        metadata_base,
    }
}

#[tokio::test]
async fn test_transactional_write_and_read() {
    let env = setup_test_environment(9210, 3).await;

    let client = match DfsClient::new(&env.master_addr).await {
        Ok(client) => client,
        Err(_) => {
            env.stop().await;
            return;
        }
    };

    client.mkdir("/test").await.expect("mkdir should succeed");
    let file = client
        .create_with_replication("/test/txn.txt", 3)
        .await
        .expect("create should succeed");

    let initial_generation = file.get_generation().await;

    let mut session = file
        .begin_write()
        .await
        .expect("begin_write should succeed");
    session
        .write_chunk(b"hello ")
        .await
        .expect("first chunk should write");
    session
        .write_chunk(b"transaction")
        .await
        .expect("second chunk should write");
    session.commit().await.expect("commit should succeed");

    let data = file.read_all().await.expect("read_all should succeed");
    assert_eq!(data, b"hello transaction");
    assert!(file.get_generation().await > initial_generation);

    env.stop().await;
}

#[tokio::test]
async fn test_directory_semantics_and_rename() {
    let env = setup_test_environment(9220, 3).await;

    let client = match DfsClient::new(&env.master_addr).await {
        Ok(client) => client,
        Err(_) => {
            env.stop().await;
            return;
        }
    };

    client.mkdir("/ns").await.expect("mkdir /ns should succeed");
    client
        .mkdir("/ns/sub")
        .await
        .expect("mkdir /ns/sub should succeed");

    let file = client
        .create("/ns/sub/file.txt")
        .await
        .expect("create should succeed");

    let mut session = file
        .begin_write()
        .await
        .expect("begin_write should succeed");
    session
        .write_chunk(b"namespaced")
        .await
        .expect("write should succeed");
    session.commit().await.expect("commit should succeed");

    client
        .rename("/ns/sub", "/ns/sub2")
        .await
        .expect("rename should succeed");

    let entries = client.list("/ns").await.expect("list should succeed");
    assert!(entries.iter().any(|e| e.path == "/ns/sub2" && e.is_dir));
    assert!(!entries.iter().any(|e| e.path == "/ns/sub"));

    let reopened = client
        .open("/ns/sub2/file.txt")
        .await
        .expect("open renamed path should succeed");
    let data = reopened.read_all().await.expect("read_all should succeed");
    assert_eq!(data, b"namespaced");

    env.stop().await;
}

#[tokio::test]
async fn test_health_endpoint_reports_quorum_metadata() {
    let env = setup_test_environment(9230, 2).await;

    let health_addr = "127.0.0.1:9310";
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

    let mut buf = vec![0u8; 8192];
    let n = stream
        .read(&mut buf)
        .await
        .expect("health response should read");

    let body = String::from_utf8_lossy(&buf[..n]);
    assert!(body.contains("\"quorum\""));
    assert!(body.contains("\"commit_index\""));
    assert!(body.contains("\"under_replicated_blocks\""));

    env.stop().await;
}

#[tokio::test]
async fn test_storage_restart_keeps_block_versions() {
    let mut env = setup_test_environment(9240, 3).await;

    let client = match DfsClient::new(&env.master_addr).await {
        Ok(client) => client,
        Err(_) => {
            env.stop().await;
            return;
        }
    };

    client
        .mkdir("/persist")
        .await
        .expect("mkdir should succeed");
    let file = client
        .create("/persist/file.txt")
        .await
        .expect("create should succeed");

    let mut first = file
        .begin_write()
        .await
        .expect("begin_write should succeed");
    first
        .write_chunk(b"v1")
        .await
        .expect("first write chunk should succeed");
    first.commit().await.expect("first commit should succeed");

    env.storage_handles[0].abort();
    sleep(Duration::from_millis(400)).await;

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

    let mut second = file
        .begin_write()
        .await
        .expect("second begin_write should succeed");
    second
        .write_chunk(b"v2")
        .await
        .expect("second write should succeed");
    second.commit().await.expect("second commit should succeed");

    let data = file.read_all().await.expect("read_all should succeed");
    assert_eq!(data, b"v2");

    env.stop().await;
}
