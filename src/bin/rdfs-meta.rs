use rdfs::local::parse_meta_peers;
use rdfs::meta::{MetadataNode, MetadataNodeConfig};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 5 {
        eprintln!("usage: rdfs-meta <id> <addr> <data_dir> <id=addr,id=addr,...> [--bootstrap]");
        std::process::exit(1);
    }

    let id = args[1].parse::<u64>()?;
    let addr = args[2].clone();
    let data_dir = PathBuf::from(&args[3]);
    let peers = parse_meta_peers(&args[4])?;
    let bootstrap = args.iter().any(|arg| arg == "--bootstrap");

    let node = MetadataNode::open(MetadataNodeConfig {
        id,
        addr,
        data_dir,
        peers,
    })
    .await?;
    node.serve(bootstrap).await
}
