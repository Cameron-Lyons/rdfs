use crate::client::{
    connection::{ConnectionManager, ConnectionStatsSnapshot},
    file::DfsFile,
    error::DfsError
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

    pub async fn open(&self, path: &str) -> Result<DfsFile, DfsError> {
        let metadata = self.conn.lookup_metadata(path).await?;
        Ok(DfsFile::new(path.to_string(), metadata, self.conn.clone()))
    }

    pub async fn delete(&self, path: &str) -> Result<(), DfsError> {
        self.conn.delete_file(path).await
    }

    pub async fn get_connection_stats(&self) -> ConnectionStatsSnapshot {
        self.conn.get_stats().await
    }
}
