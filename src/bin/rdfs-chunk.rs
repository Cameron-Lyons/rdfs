use rdfs::chunk::{ChunkServer, ChunkServerConfig};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 5 {
        eprintln!("usage: rdfs-chunk <node_id> <addr> <data_dir> <meta1,meta2,...>");
        std::process::exit(1);
    }

    let server = ChunkServer::open(ChunkServerConfig {
        node_id: args[1].clone(),
        addr: args[2].clone(),
        data_dir: PathBuf::from(&args[3]),
        metadata_addrs: args[4]
            .split(',')
            .filter(|entry| !entry.trim().is_empty())
            .map(|entry| entry.to_string())
            .collect(),
        capacity: 10 * 1024 * 1024 * 1024,
    })
    .await?;
    server.serve().await
}
