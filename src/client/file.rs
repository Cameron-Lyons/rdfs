use crate::client::{connection::ConnectionManager, error::DfsError};
use tokio::io::{AsyncRead, AsyncWrite};

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
}
