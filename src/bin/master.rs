use rdfs::server::master::MasterServer;
use std::env;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let addr = args.get(1).unwrap_or(&"127.0.0.1:9000".to_string()).clone();

    let server = MasterServer::new(addr.clone());

    println!("Starting master server on {}", addr);

    if let Err(e) = server.start().await {
        eprintln!("Master server error: {}", e);
    }
}
