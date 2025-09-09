use crate::client::{
    connection::{ConnectionManager, FileMetadata},
    error::DfsError,
};

#[derive(Clone)]
pub struct DfsFile {
    path: String,
    metadata: FileMetadata,
    conn: ConnectionManager,
}

impl DfsFile {
    pub fn new(path: String, metadata: FileMetadata, conn: ConnectionManager) -> Self {
        Self {
            path,
            metadata,
            conn,
        }
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
    
    pub async fn write_blocks(&self, blocks: Vec<(u64, &[u8])>) -> Result<(), DfsError> {
        let mut results = Vec::new();
        
        for (block_id, data) in blocks {
            results.push(self.write_block(block_id, data).await);
        }
        
        for result in results {
            result?;
        }
        
        Ok(())
    }
    
    pub async fn read_blocks(&self, block_ids: Vec<u64>) -> Result<Vec<Vec<u8>>, DfsError> {
        let mut blocks = Vec::new();
        
        for block_id in block_ids {
            blocks.push(self.read_block(block_id).await?);
        }
        
        Ok(blocks)
    }
    
    pub fn stream_read(&self) -> DfsFileStream {
        DfsFileStream::new(self.clone())
    }
    
    pub fn stream_write(&self) -> DfsFileWriter {
        DfsFileWriter::new(self.clone())
    }
}

pub struct DfsFileStream {
    file: DfsFile,
    current_block: u64,
    total_blocks: u64,
}

impl DfsFileStream {
    fn new(file: DfsFile) -> Self {
        let total_blocks = file.get_block_count() as u64;
        Self {
            file,
            current_block: 0,
            total_blocks,
        }
    }
    
    pub async fn next(&mut self) -> Option<Result<Vec<u8>, DfsError>> {
        if self.current_block >= self.total_blocks {
            return None;
        }
        
        let result = self.file.read_block(self.current_block).await;
        self.current_block += 1;
        Some(result)
    }
}

pub struct DfsFileWriter {
    file: DfsFile,
    current_block: u64,
}

impl DfsFileWriter {
    fn new(file: DfsFile) -> Self {
        Self {
            file,
            current_block: 0,
        }
    }
    
    pub async fn write_chunk(&mut self, data: &[u8]) -> Result<(), DfsError> {
        let result = self.file.write_block(self.current_block, data).await;
        if result.is_ok() {
            self.current_block += 1;
        }
        result
    }
}
