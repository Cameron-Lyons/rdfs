use crate::client::{connection::{ConnectionManager, FileMetadata}, error::DfsError};

pub struct DfsFile {
    path: String,
    metadata: FileMetadata,
    conn: ConnectionManager,
}

impl DfsFile {
    pub fn new(path: String, metadata: FileMetadata, conn: ConnectionManager) -> Self {
        Self { path, metadata, conn }
    }

    pub async fn read_block(&self, block_id: u64) -> Result<Vec<u8>, DfsError> {
        self.conn.read_block(&self.metadata, block_id).await
    }

    pub async fn write_block(&self, block_id: u64, data: &[u8]) -> Result<(), DfsError> {
        self.conn.write_block(&self.metadata, block_id, data).await
    }

    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn get_size(&self) -> u64 {
        self.metadata.size
    }

    pub fn get_block_count(&self) -> usize {
        self.metadata.blocks.len()
    }

    pub fn get_replica_count(&self) -> usize {
        self.metadata.nodes.len()
    }

    pub fn get_metadata(&self) -> &FileMetadata {
        &self.metadata
    }
}
