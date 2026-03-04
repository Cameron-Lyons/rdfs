use rdfs::client::demo;

#[tokio::main]
async fn main() {
    if let Err(err) = demo::run("127.0.0.1:9000", true).await {
        eprintln!("Client demo failed: {err}");
    }
}
