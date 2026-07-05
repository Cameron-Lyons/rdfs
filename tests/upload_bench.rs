use rdfs::chunk::{ChunkServer, ChunkServerConfig};
use rdfs::client::{Client, WriteOptions};
use rdfs::meta::{MetadataNode, MetadataNodeConfig};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Measures end-to-end write and read throughput against a local 3x3
/// cluster. Excluded from the default test run; invoke with
/// `cargo test --release --test upload_bench -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "throughput benchmark, run explicitly"]
async fn upload_throughput_bench() -> anyhow::Result<()> {
    let base_port = 42100u16;
    let root_dir = PathBuf::from(format!("/tmp/rdfs-bench-{base_port}"));
    let _ = tokio::fs::remove_dir_all(&root_dir).await;
    tokio::fs::create_dir_all(&root_dir).await?;

    let peers = BTreeMap::from([
        (1u64, format!("127.0.0.1:{base_port}")),
        (2, format!("127.0.0.1:{}", base_port + 1)),
        (3, format!("127.0.0.1:{}", base_port + 2)),
    ]);
    let meta_addrs = peers.values().cloned().collect::<Vec<_>>();

    let mut handles = Vec::new();
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
        handles.push(tokio::spawn(async move {
            let _ = node.serve(bootstrap).await;
        }));
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut chunk_servers = Vec::new();
    for offset in 0..3u16 {
        let server = ChunkServer::open(ChunkServerConfig {
            node_id: format!("chunk-{}", offset + 1),
            addr: format!("127.0.0.1:{}", base_port + 10 + offset),
            data_dir: root_dir.join(format!("chunk-{}", offset + 1)),
            metadata_addrs: meta_addrs.clone(),
            capacity: 10 * 1024 * 1024 * 1024,
        })
        .await?;
        chunk_servers.push(server.clone());
        handles.push(tokio::spawn(async move {
            let _ = server.serve().await;
        }));
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    let client = Client::new(meta_addrs.clone())?;
    let payload = vec![0xa7u8; 4 * 1024 * 1024];
    let total_bytes = 128 * 1024 * 1024u64;

    for round in 0..3 {
        let start = Instant::now();
        let mut writer = client
            .create_writer(&format!("/bench-{round}.bin"), WriteOptions::default())
            .await?;
        let mut written = 0u64;
        while written < total_bytes {
            writer.write(&payload).await?;
            written += payload.len() as u64;
        }
        writer.commit().await?;
        let elapsed = start.elapsed();
        let mib_s = (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64();
        println!("round {round}: 128 MiB in {elapsed:?} = {mib_s:.1} MiB/s");

        let start = Instant::now();
        let reader = client.open_reader(&format!("/bench-{round}.bin")).await?;
        let bytes = reader.read_all().await?;
        assert_eq!(bytes.len() as u64, total_bytes);
        let elapsed = start.elapsed();
        let mib_s = (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64();
        println!("round {round}: read 128 MiB in {elapsed:?} = {mib_s:.1} MiB/s");
    }

    for node in &meta_nodes {
        let _ = node.shutdown().await;
    }
    for server in &chunk_servers {
        let _ = server.shutdown().await;
    }
    let _ = tokio::fs::remove_dir_all(&root_dir).await;
    Ok(())
}
