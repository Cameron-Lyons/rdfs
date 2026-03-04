use crate::client::{
    connection::{ConnectionManager, ConnectionStatsSnapshot},
    error::DfsError,
    file::DfsFile,
};
use crate::protocol::DirectoryEntry;

#[derive(Clone)]
pub struct DfsClient {
    conn: ConnectionManager,
}

impl DfsClient {
    pub async fn new(master_addr: &str) -> Result<Self, DfsError> {
        let conn = ConnectionManager::connect(master_addr).await?;
        Ok(Self { conn })
    }

    pub async fn mkdir(&self, path: &str) -> Result<(), DfsError> {
        self.conn.create_directory(path).await
    }

    pub async fn create(&self, path: &str) -> Result<DfsFile, DfsError> {
        self.create_with_replication(path, 3).await
    }

    pub async fn create_with_replication(
        &self,
        path: &str,
        replication_factor: usize,
    ) -> Result<DfsFile, DfsError> {
        let handle = self.conn.create_file(path, replication_factor).await?;
        Ok(DfsFile::new(handle, self.conn.clone()))
    }

    pub async fn open(&self, path: &str) -> Result<DfsFile, DfsError> {
        let handle = self.conn.resolve_path(path).await?;
        Ok(DfsFile::new(handle, self.conn.clone()))
    }

    pub async fn list(&self, path: &str) -> Result<Vec<DirectoryEntry>, DfsError> {
        self.conn.list_directory(path).await
    }

    pub async fn rename(&self, from: &str, to: &str) -> Result<(), DfsError> {
        self.conn.rename_entry(from, to).await
    }

    pub async fn get_connection_stats(&self) -> ConnectionStatsSnapshot {
        self.conn.get_stats().await
    }
}
