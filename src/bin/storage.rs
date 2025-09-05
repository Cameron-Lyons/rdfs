use rdfs::server::storage::StorageNode;
use std::env;
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        eprintln!(
            "Usage: {} <node_id> <listen_addr> [master_addr] [data_dir]",
            args[0]
        );
        eprintln!(
            "Example: {} node1 127.0.0.1:9001 127.0.0.1:9000 ./data/node1",
            args[0]
        );
        std::process::exit(1);
    }

    let node_id = args[1].clone();
    let listen_addr = args[2].clone();
    let master_addr = args.get(3).unwrap_or(&"127.0.0.1:9000".to_string()).clone();
    let data_dir = PathBuf::from(args.get(4).unwrap_or(&format!("./data/{}", node_id)));

    let node = StorageNode::new(node_id.clone(), listen_addr.clone(), master_addr, data_dir);

    println!("Starting storage node {} on {}", node_id, listen_addr);

    if let Err(e) = node.start().await {
        eprintln!("Storage node error: {}", e);
    }
}

