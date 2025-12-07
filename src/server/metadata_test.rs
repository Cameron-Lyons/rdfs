#[cfg(test)]
mod tests {
    use super::super::metadata::MetadataStore;
    use tokio;

    #[tokio::test]
    async fn test_metadata_creation() {
        let store = MetadataStore::new();
        let file_info = store.create_file("/test/file.txt".to_string(), 3).await;

        assert_eq!(file_info.path, "/test/file.txt");
        assert_eq!(file_info.replication_factor, 3);
        assert_eq!(file_info.size, 0);
        assert_eq!(file_info.blocks.len(), 0);
        assert!(file_info.created_at > 0);
        assert!(file_info.modified_at > 0);

        let retrieved = store.get_file("/test/file.txt").await;
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.path, "/test/file.txt");
        assert_eq!(retrieved.replication_factor, 3);
    }

    #[tokio::test]
    async fn test_metadata_update() {
        let store = MetadataStore::new();
        store.create_file("/test/update.txt".to_string(), 2).await;

        let block_id = store.allocate_block("/test/update.txt").await;
        assert!(block_id.is_some());
        let block_id = block_id.unwrap();

        let updated = store
            .update_block_replicas("/test/update.txt", block_id, "node1".to_string())
            .await;
        assert!(updated);

        let file_info = store.get_file("/test/update.txt").await.unwrap();
        assert_eq!(file_info.blocks.len(), 1);
        assert_eq!(file_info.blocks[0].block_id, block_id);
        assert_eq!(file_info.blocks[0].replicas.len(), 1);
        assert_eq!(file_info.blocks[0].replicas[0].node_id, "node1");
    }

    #[tokio::test]
    async fn test_metadata_deletion() {
        let store = MetadataStore::new();
        store.create_file("/test/delete.txt".to_string(), 2).await;

        let exists = store.get_file("/test/delete.txt").await;
        assert!(exists.is_some());

        let deleted = store.delete_file("/test/delete.txt").await;
        assert!(deleted);

        let exists = store.get_file("/test/delete.txt").await;
        assert!(exists.is_none());

        let deleted_again = store.delete_file("/test/delete.txt").await;
        assert!(!deleted_again);
    }

    #[tokio::test]
    async fn test_metadata_rename() {
        let store = MetadataStore::new();
        store.create_file("/test/old.txt".to_string(), 2).await;

        let renamed = store.rename_file("/test/old.txt", "/test/new.txt").await;
        assert!(renamed);

        let old_file = store.get_file("/test/old.txt").await;
        assert!(old_file.is_none());

        let new_file = store.get_file("/test/new.txt").await;
        assert!(new_file.is_some());
        assert_eq!(new_file.unwrap().path, "/test/new.txt");
    }

    #[tokio::test]
    async fn test_metadata_list() {
        let store = MetadataStore::new();
        store.create_file("/test/file1.txt".to_string(), 2).await;
        store.create_file("/test/file2.txt".to_string(), 3).await;
        store.create_file("/test/file3.txt".to_string(), 2).await;

        let files = store.list_files("/test").await;
        assert_eq!(files.len(), 3);
    }

    #[tokio::test]
    async fn test_node_registration() {
        let store = MetadataStore::new();
        store
            .register_node("node1".to_string(), "127.0.0.1:8001".to_string(), 1000)
            .await;
        store
            .register_node("node2".to_string(), "127.0.0.1:8002".to_string(), 2000)
            .await;

        let active_nodes = store.get_active_nodes().await;
        assert_eq!(active_nodes.len(), 2);

        let selected = store.select_nodes_for_replication(2).await;
        assert_eq!(selected.len(), 2);
    }

    #[tokio::test]
    async fn test_block_allocation() {
        let store = MetadataStore::new();
        store.create_file("/test/blocks.txt".to_string(), 2).await;

        let block1 = store.allocate_block("/test/blocks.txt").await;
        assert!(block1.is_some());
        let block1_id = block1.unwrap();

        let block2 = store.allocate_block("/test/blocks.txt").await;
        assert!(block2.is_some());
        let block2_id = block2.unwrap();

        assert_ne!(block1_id, block2_id);

        let file_info = store.get_file("/test/blocks.txt").await.unwrap();
        assert_eq!(file_info.blocks.len(), 2);
    }
}
