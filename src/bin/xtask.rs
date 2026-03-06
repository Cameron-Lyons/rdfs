#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let base_port = std::env::args()
        .nth(1)
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(9500);
    rdfs::local::run_local_cluster(base_port).await
}
