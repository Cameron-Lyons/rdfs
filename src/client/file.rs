use crate::checksum::checksum_hex;
use crate::client::{connection::ConnectionManager, error::DfsError};
use crate::protocol::{FileHandle, WriteSessionInfo};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct DfsFile {
    handle: Arc<RwLock<FileHandle>>,
    conn: ConnectionManager,
}

impl DfsFile {
    pub fn new(handle: FileHandle, conn: ConnectionManager) -> Self {
        Self {
            handle: Arc::new(RwLock::new(handle)),
            conn,
        }
    }

    pub async fn get_generation(&self) -> u64 {
        self.handle.read().await.generation
    }

    pub async fn read_block(&self, block_id: u64) -> Result<Vec<u8>, DfsError> {
        let (file_id, generation) = {
            let handle = self.handle.read().await;
            (handle.file_id, handle.generation)
        };

        let layout = self.conn.get_file_layout(file_id, Some(generation)).await?;

        {
            let mut handle = self.handle.write().await;
            *handle = layout.handle.clone();
        }

        self.conn.read_block(&layout, block_id).await
    }

    pub async fn read_all(&self) -> Result<Vec<u8>, DfsError> {
        let (file_id, generation) = {
            let handle = self.handle.read().await;
            (handle.file_id, handle.generation)
        };

        let layout = self.conn.get_file_layout(file_id, Some(generation)).await?;

        {
            let mut handle = self.handle.write().await;
            *handle = layout.handle.clone();
        }

        let mut result = Vec::with_capacity(layout.size as usize);
        for block in &layout.blocks {
            let data = self.conn.read_block(&layout, block.block_id).await?;
            result.extend_from_slice(&data);
        }

        Ok(result)
    }

    pub async fn begin_write(&self) -> Result<DfsWriteSession, DfsError> {
        let (file_id, generation) = {
            let handle = self.handle.read().await;
            (handle.file_id, handle.generation)
        };

        let session = match self.conn.begin_write(file_id, generation).await {
            Ok(session) => session,
            Err(err) if err.is_stale_handle() => {
                if let Some(handle) = err.current_handle() {
                    {
                        let mut guard = self.handle.write().await;
                        *guard = handle.clone();
                    }
                    self.conn
                        .begin_write(handle.file_id, handle.generation)
                        .await?
                } else {
                    return Err(err);
                }
            }
            Err(err) => return Err(err),
        };

        Ok(DfsWriteSession {
            conn: self.conn.clone(),
            file_handle: Arc::clone(&self.handle),
            session,
        })
    }

    pub async fn write_all(&self, data: &[u8], chunk_size: usize) -> Result<(), DfsError> {
        if chunk_size == 0 {
            return Err(DfsError::Protocol(
                "chunk_size must be greater than 0".to_string(),
            ));
        }

        let mut session = self.begin_write().await?;
        for chunk in data.chunks(chunk_size) {
            session.write_chunk(chunk).await?;
        }
        session.commit().await
    }
}

pub struct DfsWriteSession {
    conn: ConnectionManager,
    file_handle: Arc<RwLock<FileHandle>>,
    session: WriteSessionInfo,
}

impl DfsWriteSession {
    pub async fn write_chunk(&mut self, data: &[u8]) -> Result<(), DfsError> {
        let checksum = checksum_hex(data);
        let (block_id, nodes) = self
            .conn
            .allocate_write_chunk(self.session.session_id, data.len() as u64, checksum.clone())
            .await?;

        let replicas = self
            .conn
            .write_block_to_nodes(
                &nodes,
                block_id,
                self.session.target_generation,
                &checksum,
                data,
            )
            .await?;

        self.conn
            .finalize_write_chunk(self.session.session_id, block_id, replicas)
            .await?;
        Ok(())
    }

    pub async fn commit(self) -> Result<(), DfsError> {
        let handle = self.conn.commit_write(self.session.session_id).await?;
        {
            let mut guard = self.file_handle.write().await;
            *guard = handle;
        }
        Ok(())
    }

    pub async fn abort(self) -> Result<(), DfsError> {
        self.conn.abort_write(self.session.session_id).await?;
        Ok(())
    }
}
