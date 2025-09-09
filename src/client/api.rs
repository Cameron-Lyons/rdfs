use crate::client::{
    connection::{ConnectionManager, ConnectionStatsSnapshot, FileInfo},
    error::DfsError,
    file::DfsFile,
};

#[derive(Clone)]
pub struct DfsClient {
    conn: ConnectionManager,
}

impl DfsClient {
    pub async fn new(master_addr: &str) -> Result<Self, DfsError> {
        let conn = ConnectionManager::connect(master_addr).await?;
        Ok(Self { conn })
    }

    pub async fn create(&self, path: &str) -> Result<DfsFile, DfsError> {
        let metadata = self.conn.create_file(path).await?;
        Ok(DfsFile::new(path.to_string(), metadata, self.conn.clone()))
    }

    pub async fn open(&self, path: &str) -> Result<DfsFile, DfsError> {
        let metadata = self.conn.lookup_metadata(path).await?;
        Ok(DfsFile::new(path.to_string(), metadata, self.conn.clone()))
    }

    pub async fn list(&self, path: &str) -> Result<Vec<FileInfo>, DfsError> {
        self.conn.list_files(path).await
    }

    pub async fn rename(&self, from: &str, to: &str) -> Result<(), DfsError> {
        self.conn.rename_file(from, to).await
    }

    pub async fn delete(&self, path: &str) -> Result<(), DfsError> {
        self.conn.delete_file(path).await
    }

    pub async fn get_connection_stats(&self) -> ConnectionStatsSnapshot {
        self.conn.get_stats().await
    }
    
    pub async fn create_with_replication(&self, path: &str, replication_factor: usize) -> Result<DfsFile, DfsError> {
        let metadata = self.conn.create_file_with_replication(path, replication_factor).await?;
        Ok(DfsFile::new(path.to_string(), metadata, self.conn.clone()))
    }
}
