#[cfg(test)]
mod tests {
    use super::super::api::DfsClient;
    use tokio;

    #[tokio::test]
    async fn test_api_basic() {
        let client = DfsClient::new("127.0.0.1:9000").await;

        if client.is_err() {
            return;
        }

        let client = client.unwrap();
        let stats = client.get_connection_stats().await;

        assert_eq!(stats.total_requests, 0);
        assert_eq!(stats.failed_requests, 0);
        assert_eq!(stats.bytes_sent, 0);
        assert_eq!(stats.bytes_received, 0);
    }

    #[tokio::test]
    async fn test_api_connection() {
        let client = DfsClient::new("127.0.0.1:9000").await;

        if client.is_err() {
            return;
        }

        let client = client.unwrap();

        let result = client.create("/test/connection.txt").await;
        assert!(result.is_err());

        let result = client.open("/test/nonexistent.txt").await;
        assert!(result.is_err());

        let result = client.list("/test").await;
        assert!(result.is_err());

        let result = client.delete("/test/file.txt").await;
        assert!(result.is_err());

        let result = client.rename("/test/old.txt", "/test/new.txt").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_api_create_with_replication() {
        let client = DfsClient::new("127.0.0.1:9000").await;

        if client.is_err() {
            return;
        }

        let client = client.unwrap();

        let result = client
            .create_with_replication("/test/replicated.txt", 5)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_api_connection_stats() {
        let client = DfsClient::new("127.0.0.1:9000").await;

        if client.is_err() {
            return;
        }

        let client = client.unwrap();

        let stats1 = client.get_connection_stats().await;
        let stats2 = client.get_connection_stats().await;

        assert_eq!(stats1.total_requests, stats2.total_requests);
        assert_eq!(stats1.failed_requests, stats2.failed_requests);
    }
}
