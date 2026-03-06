use rdfs::chunk::{ChunkServer, ChunkServerConfig};
use rdfs::client::{Client, WriteOptions};
use rdfs::meta::{MetadataNode, MetadataNodeConfig};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::task::JoinHandle;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct TestCluster {
    meta_nodes: Vec<MetadataNode>,
    chunk_servers: Vec<ChunkServer>,
    meta_handles: Vec<JoinHandle<()>>,
    chunk_handles: Vec<JoinHandle<()>>,
    meta_addrs: Vec<String>,
    root_dir: PathBuf,
}

impl TestCluster {
    async fn stop(self) {
        for node in &self.meta_nodes {
            let _ = node.shutdown().await;
        }
        for server in &self.chunk_servers {
            let _ = server.shutdown().await;
        }
        for handle in self.meta_handles {
            let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        }
        for handle in self.chunk_handles {
            let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        }
        let _ = tokio::fs::remove_dir_all(self.root_dir).await;
    }

    async fn stop_meta(&mut self, id: u64) -> anyhow::Result<()> {
        let Some(index) = self.meta_nodes.iter().position(|node| node.id() == id) else {
            anyhow::bail!("metadata node {id} not found");
        };
        let node = self.meta_nodes.remove(index);
        node.shutdown().await?;
        let handle = self.meta_handles.remove(index);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        Ok(())
    }

    async fn add_meta(&mut self, id: u64, addr: String) -> anyhow::Result<()> {
        let mut peers = self
            .meta_nodes
            .iter()
            .map(|node| (node.id(), node.addr().to_string()))
            .collect::<BTreeMap<_, _>>();
        peers.insert(id, addr.clone());

        let node = MetadataNode::open(MetadataNodeConfig {
            id,
            addr: addr.clone(),
            data_dir: self.root_dir.join(format!("meta-{id}")),
            peers,
        })
        .await?;
        self.meta_nodes.push(node.clone());
        self.meta_handles.push(tokio::spawn(async move {
            let _ = node.serve(false).await;
        }));
        self.meta_addrs.push(addr);
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(())
    }

    async fn add_chunk(&mut self, node_id: &str, addr: String) -> anyhow::Result<()> {
        if self
            .chunk_servers
            .iter()
            .any(|server| server.node_id() == node_id)
        {
            anyhow::bail!("chunk server {node_id} already exists");
        }

        let server = ChunkServer::open(ChunkServerConfig {
            node_id: node_id.to_string(),
            addr,
            data_dir: self.root_dir.join(node_id),
            metadata_addrs: self.meta_addrs.clone(),
            capacity: 10 * 1024 * 1024 * 1024,
        })
        .await?;
        self.chunk_servers.push(server.clone());
        self.chunk_handles.push(tokio::spawn(async move {
            let _ = server.serve().await;
        }));
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    async fn stop_chunk(&mut self, node_id: &str) -> anyhow::Result<()> {
        let Some(index) = self
            .chunk_servers
            .iter()
            .position(|server| server.node_id() == node_id)
        else {
            anyhow::bail!("chunk server {node_id} not found");
        };
        let server = self.chunk_servers.remove(index);
        server.shutdown().await?;
        let handle = self.chunk_handles.remove(index);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        Ok(())
    }
}

async fn start_cluster(base_port: u16) -> anyhow::Result<TestCluster> {
    let root_dir = PathBuf::from(format!("/tmp/rdfs-v3-test-{base_port}"));
    let _ = tokio::fs::remove_dir_all(&root_dir).await;
    tokio::fs::create_dir_all(&root_dir).await?;

    let peers = BTreeMap::from([
        (1, format!("127.0.0.1:{base_port}")),
        (2, format!("127.0.0.1:{}", base_port + 1)),
        (3, format!("127.0.0.1:{}", base_port + 2)),
    ]);
    let meta_addrs = peers.values().cloned().collect::<Vec<_>>();

    let mut meta_handles = Vec::new();
    let mut meta_nodes = Vec::new();
    for (id, addr) in &peers {
        let node = MetadataNode::open(MetadataNodeConfig {
            id: *id,
            addr: addr.clone(),
            data_dir: root_dir.join(format!("meta-{id}")),
            peers: peers.clone(),
        })
        .await?;
        meta_nodes.push(node.clone());
        let bootstrap = *id == 1;
        meta_handles.push(tokio::spawn(async move {
            let _ = node.serve(bootstrap).await;
        }));
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut chunk_handles = Vec::new();
    let mut chunk_servers = Vec::new();
    for idx in 0..3u16 {
        let server = ChunkServer::open(ChunkServerConfig {
            node_id: format!("chunk-{}", idx + 1),
            addr: format!("127.0.0.1:{}", base_port + 10 + idx),
            data_dir: root_dir.join(format!("chunk-{}", idx + 1)),
            metadata_addrs: meta_addrs.clone(),
            capacity: 10 * 1024 * 1024 * 1024,
        })
        .await?;
        chunk_servers.push(server.clone());
        chunk_handles.push(tokio::spawn(async move {
            let _ = server.serve().await;
        }));
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    Ok(TestCluster {
        meta_nodes,
        chunk_servers,
        meta_handles,
        chunk_handles,
        meta_addrs,
        root_dir,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn create_overwrite_read_and_delete() -> anyhow::Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let cluster = start_cluster(9610).await?;
    let client = Client::new(cluster.meta_addrs.clone())?;

    let _ = client.mkdir("/docs").await?;
    let mut writer = client
        .create_writer("/docs/file.txt", WriteOptions::default())
        .await?;
    writer.write(b"hello").await?;
    writer.commit().await?;

    let reader = client.open_reader("/docs/file.txt").await?;
    assert_eq!(reader.read_all().await?, b"hello");

    let mut writer = client
        .overwrite_writer("/docs/file.txt", WriteOptions::default())
        .await?;
    writer.write(b"world").await?;
    writer.commit().await?;

    let reader = client.open_reader("/docs/file.txt").await?;
    assert_eq!(reader.read_all().await?, b"world");

    client.delete("/docs/file.txt").await?;
    assert!(client.open_reader("/docs/file.txt").await.is_err());

    cluster.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn uncommitted_writes_are_hidden_and_leases_are_exclusive() -> anyhow::Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let cluster = start_cluster(9620).await?;
    let client = Client::new(cluster.meta_addrs.clone())?;
    let _ = client.mkdir("/tmp").await?;

    let mut writer = client
        .create_writer("/tmp/inflight.txt", WriteOptions::default())
        .await?;
    writer.write(b"invisible").await?;

    assert!(client.open_reader("/tmp/inflight.txt").await.is_err());
    assert!(
        client
            .create_writer("/tmp/inflight.txt", WriteOptions::default())
            .await
            .is_err()
    );

    writer.commit().await?;
    let reader = client.open_reader("/tmp/inflight.txt").await?;
    assert_eq!(reader.read_all().await?, b"invisible");

    cluster.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn corrupted_replica_is_repaired_after_read_failure() -> anyhow::Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let cluster = start_cluster(9625).await?;
    let client = Client::new(cluster.meta_addrs.clone())?;
    let _ = client.mkdir("/repair").await?;

    let expected = b"healed by repair".to_vec();
    let mut writer = client
        .create_writer("/repair/file.txt", WriteOptions::default())
        .await?;
    writer.write(&expected).await?;
    writer.commit().await?;

    let chunk_path = first_chunk_path(cluster.root_dir.join("chunk-1").join("chunks")).await?;
    tokio::fs::write(&chunk_path, b"corrupt replica bytes").await?;

    let reader = client.open_reader("/repair/file.txt").await?;
    assert_eq!(reader.read_all().await?, expected);

    wait_for_chunk_contents(&chunk_path, &expected, Duration::from_secs(12)).await?;

    cluster.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn upload_fails_when_a_replica_dies_before_chunk_replication() -> anyhow::Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = start_cluster(9627).await?;
    let client = Client::new(cluster.meta_addrs.clone())?;
    let _ = client.mkdir("/degraded").await?;

    cluster.stop_chunk("chunk-3").await?;

    let mut writer = client
        .create_writer("/degraded/file.txt", WriteOptions::default())
        .await?;
    writer.write(b"should not commit").await?;
    assert!(writer.commit().await.is_err());
    assert!(client.open_reader("/degraded/file.txt").await.is_err());

    cluster.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn committed_file_survives_single_chunkserver_loss() -> anyhow::Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = start_cluster(9628).await?;
    let client = Client::new(cluster.meta_addrs.clone())?;
    let _ = client.mkdir("/reads").await?;

    let expected = b"read survives chunk loss".to_vec();
    let mut writer = client
        .create_writer("/reads/file.txt", WriteOptions::default())
        .await?;
    writer.write(&expected).await?;
    writer.commit().await?;

    cluster.stop_chunk("chunk-1").await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let reader = client.open_reader("/reads/file.txt").await?;
    assert_eq!(reader.read_all().await?, expected);

    cluster.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn restarted_chunkserver_rejoins_manifest_inventory() -> anyhow::Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = start_cluster(9629).await?;
    let client = Client::new(cluster.meta_addrs.clone())?;
    let _ = client.mkdir("/restarts").await?;

    let expected = b"replica returns after restart".to_vec();
    let mut writer = client
        .create_writer("/restarts/file.txt", WriteOptions::default())
        .await?;
    writer.write(&expected).await?;
    writer.commit().await?;

    cluster.stop_chunk("chunk-1").await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let degraded_reader = client.open_reader("/restarts/file.txt").await?;
    assert_eq!(degraded_reader.read_all().await?, expected);

    wait_for_chunk_replica_state(
        &client,
        "/restarts/file.txt",
        &["chunk-2", "chunk-3"],
        &["chunk-1"],
        Duration::from_secs(12),
    )
    .await?;

    cluster
        .add_chunk("chunk-1", "127.0.0.1:9639".to_string())
        .await?;

    wait_for_chunk_replica_state(
        &client,
        "/restarts/file.txt",
        &["chunk-1", "chunk-2", "chunk-3"],
        &[],
        Duration::from_secs(12),
    )
    .await?;

    let reader = client.open_reader("/restarts/file.txt").await?;
    assert_eq!(reader.read_all().await?, expected);

    cluster.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn metadata_failover_preserves_committed_reads() -> anyhow::Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = start_cluster(9630).await?;
    let client = Client::new(cluster.meta_addrs.clone())?;
    let _ = client.mkdir("/ha").await?;

    let mut writer = client
        .create_writer("/ha/file.txt", WriteOptions::default())
        .await?;
    writer.write(b"survives").await?;
    writer.commit().await?;

    cluster.stop_meta(1).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let reader = client.open_reader("/ha/file.txt").await?;
    assert_eq!(reader.read_all().await?, b"survives");

    cluster.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn metadata_replacement_restores_quorum() -> anyhow::Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = start_cluster(9640).await?;
    let client = Client::new(cluster.meta_addrs.clone())?;
    let _ = client.mkdir("/replace").await?;

    let mut writer = client
        .create_writer("/replace/file.txt", WriteOptions::default())
        .await?;
    writer.write(b"survives replacement").await?;
    writer.commit().await?;

    cluster.stop_meta(1).await?;
    let new_addr = "127.0.0.1:9643".to_string();
    cluster.add_meta(4, new_addr.clone()).await?;

    let membership = client.replace_metadata_node(1, 4, new_addr).await?;
    assert_eq!(voter_ids(&membership), vec![2, 3, 4]);

    let refreshed = Client::new(membership.nodes.iter().map(|node| node.addr.clone()))?;
    cluster.stop_meta(2).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let reader = refreshed.open_reader("/replace/file.txt").await?;
    assert_eq!(reader.read_all().await?, b"survives replacement");

    let mut writer = refreshed
        .create_writer("/replace/after.txt", WriteOptions::default())
        .await?;
    writer.write(b"still writable").await?;
    writer.commit().await?;

    let reader = refreshed.open_reader("/replace/after.txt").await?;
    assert_eq!(reader.read_all().await?, b"still writable");

    cluster.stop().await;
    Ok(())
}

fn voter_ids(membership: &rdfs::pb::ClusterMembership) -> Vec<u64> {
    let mut voters = membership
        .nodes
        .iter()
        .filter(|node| node.voter)
        .map(|node| node.id)
        .collect::<Vec<_>>();
    voters.sort();
    voters
}

async fn first_chunk_path(dir: PathBuf) -> anyhow::Result<PathBuf> {
    let mut entries = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) == Some("chunk") {
            return Ok(path);
        }
    }
    anyhow::bail!("chunk file not found")
}

async fn wait_for_chunk_contents(
    path: &PathBuf,
    expected: &[u8],
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(bytes) = tokio::fs::read(path).await
            && bytes == expected
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            let bytes = tokio::fs::read(path).await.unwrap_or_default();
            anyhow::bail!(
                "chunk repair timed out, found {} bytes instead of {}",
                bytes.len(),
                expected.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_chunk_replica_state(
    client: &Client,
    path: &str,
    expected_present: &[&str],
    expected_absent: &[&str],
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let manifest = client.open_reader(path).await?.manifest().clone();
        let Some(chunk) = manifest.chunks.first() else {
            anyhow::bail!("manifest for {path} did not contain any chunks");
        };
        let replicas = chunk
            .replicas
            .iter()
            .map(|replica| replica.node_id.as_str())
            .collect::<Vec<_>>();
        let has_expected = expected_present
            .iter()
            .all(|node_id| replicas.iter().any(|replica| replica == node_id));
        let missing_absent = expected_absent
            .iter()
            .all(|node_id| replicas.iter().all(|replica| replica != node_id));
        if has_expected && missing_absent {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for replica state on {path}: {:?}",
                replicas
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
