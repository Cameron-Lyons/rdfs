use crate::chunk::{ChunkServer, ChunkServerConfig};
use crate::client::{Client, WriteOptions};
use crate::meta::{MetadataNode, MetadataNodeConfig};
use anyhow::Result;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

pub async fn run_local_cluster(base_port: u16) -> Result<()> {
    let peers = BTreeMap::from([
        (1, format!("127.0.0.1:{base_port}")),
        (2, format!("127.0.0.1:{}", base_port + 1)),
        (3, format!("127.0.0.1:{}", base_port + 2)),
    ]);
    let metadata_addrs = peers.values().cloned().collect::<Vec<_>>();

    for (id, addr) in &peers {
        let node = MetadataNode::open(MetadataNodeConfig {
            id: *id,
            addr: addr.clone(),
            data_dir: PathBuf::from(format!("/tmp/rdfs-v3/meta-{id}")),
            peers: peers.clone(),
        })
        .await?;
        let bootstrap = *id == 1;
        tokio::spawn(async move {
            let _ = node.serve(bootstrap).await;
        });
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    for offset in 0..3u16 {
        let server = ChunkServer::open(ChunkServerConfig {
            node_id: format!("chunk-{}", offset + 1),
            addr: format!("127.0.0.1:{}", base_port + 10 + offset),
            data_dir: PathBuf::from(format!("/tmp/rdfs-v3/chunk-{}", offset + 1)),
            metadata_addrs: metadata_addrs.clone(),
            capacity: 10 * 1024 * 1024 * 1024,
        })
        .await?;
        tokio::spawn(async move {
            let _ = server.serve().await;
        });
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    let client = Client::new(metadata_addrs.clone())?;
    let _ = client.mkdir("/demo").await;
    let mut writer = client
        .create_writer("/demo/hello.txt", WriteOptions::default())
        .await?;
    writer.write(b"hello from rdfs v3").await?;
    writer.commit().await?;
    let reader = client.open_reader("/demo/hello.txt").await?;
    println!("{}", String::from_utf8_lossy(&reader.read_all().await?));

    tokio::signal::ctrl_c().await?;
    Ok(())
}

pub fn parse_meta_peers(csv: &str) -> Result<BTreeMap<u64, String>> {
    let mut peers = BTreeMap::new();
    for entry in csv.split(',').filter(|entry| !entry.trim().is_empty()) {
        let (id, addr) = entry
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("invalid peer entry: {entry}"))?;
        peers.insert(id.parse::<u64>()?, addr.to_string());
    }
    Ok(peers)
}
