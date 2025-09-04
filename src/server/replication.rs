use crate::client::connection::{Request, Response};
use crate::server::metadata::{MetadataStore, NodeInfo};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct ReplicationManager {
    metadata_store: Arc<MetadataStore>,
    replication_factor: usize,
}

impl ReplicationManager {
    pub fn new(metadata_store: Arc<MetadataStore>, replication_factor: usize) -> Self {
        Self {
            metadata_store,
            replication_factor,
        }
    }

    pub async fn replicate_block(
        &self,
        file_path: &str,
        block_id: u64,
        data: &[u8],
    ) -> Result<Vec<String>, String> {
        let nodes = self
            .metadata_store
            .select_nodes_for_replication(self.replication_factor)
            .await;

        if nodes.len() < self.replication_factor {
            return Err(format!(
                "Not enough nodes for replication. Required: {}, Available: {}",
                self.replication_factor,
                nodes.len()
            ));
        }

        let mut successful_replicas = Vec::new();
        let mut tasks = Vec::new();

        for node in nodes {
            let data_clone = data.to_vec();
            let node_clone = node.clone();
            let file_path_clone = file_path.to_string();
            let metadata_store = Arc::clone(&self.metadata_store);

            let task = tokio::spawn(async move {
                if let Ok(_) = write_to_node(&node_clone, block_id, &data_clone).await {
                    metadata_store
                        .update_block_replicas(&file_path_clone, block_id, node_clone.id.clone())
                        .await;
                    Ok(node_clone.id)
                } else {
                    Err(format!("Failed to replicate to node {}", node_clone.id))
                }
            });

            tasks.push(task);
        }

        for task in tasks {
            if let Ok(Ok(node_id)) = task.await {
                successful_replicas.push(node_id);
            }
        }

        if successful_replicas.len() >= self.replication_factor / 2 + 1 {
            Ok(successful_replicas)
        } else {
            Err(format!(
                "Insufficient successful replicas. Required: {}, Successful: {}",
                self.replication_factor / 2 + 1,
                successful_replicas.len()
            ))
        }
    }

    pub async fn ensure_replication(&self, file_path: &str) -> Result<(), String> {
        let file_info = self
            .metadata_store
            .get_file(file_path)
            .await
            .ok_or_else(|| format!("File {} not found", file_path))?;

        for block in &file_info.blocks {
            let replica_count = block.replicas.len();
            
            if replica_count < self.replication_factor {
                let needed = self.replication_factor - replica_count;
                let available_nodes = self.metadata_store.get_active_nodes().await;
                
                let existing_node_ids: Vec<String> = block
                    .replicas
                    .iter()
                    .map(|r| r.node_id.clone())
                    .collect();

                let new_nodes: Vec<NodeInfo> = available_nodes
                    .into_iter()
                    .filter(|n| !existing_node_ids.contains(&n.id))
                    .take(needed)
                    .collect();

                if new_nodes.len() < needed {
                    eprintln!(
                        "Warning: Cannot achieve replication factor {} for block {}",
                        self.replication_factor, block.block_id
                    );
                    continue;
                }

                if let Some(source_replica) = block.replicas.first() {
                    if let Ok(data) = read_from_node(&source_replica.node_id, block.block_id).await
                    {
                        for node in new_nodes {
                            if write_to_node(&node, block.block_id, &data).await.is_ok() {
                                self.metadata_store
                                    .update_block_replicas(file_path, block.block_id, node.id)
                                    .await;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn handle_node_failure(&self, failed_node_id: &str) -> Result<(), String> {
        println!("Handling failure of node: {}", failed_node_id);

        let _files = self.metadata_store.get_file("").await;

        Ok(())
    }
}

async fn write_to_node(node: &NodeInfo, block_id: u64, data: &[u8]) -> Result<(), String> {
    let mut stream = TcpStream::connect(&node.addr)
        .await
        .map_err(|e| format!("Failed to connect to {}: {}", node.addr, e))?;

    let request = Request::WriteBlock {
        block_id,
        data: data.to_vec(),
    };

    let msg = serde_json::to_vec(&request).map_err(|e| e.to_string())?;
    let len = (msg.len() as u32).to_be_bytes();
    
    stream
        .write_all(&len)
        .await
        .map_err(|e| e.to_string())?;
    stream
        .write_all(&msg)
        .await
        .map_err(|e| e.to_string())?;

    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| e.to_string())?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| e.to_string())?;

    let response: Response = serde_json::from_slice(&buf).map_err(|e| e.to_string())?;

    match response {
        Response::Ok => Ok(()),
        Response::Error { message } => Err(message),
        _ => Err("Unexpected response".to_string()),
    }
}

async fn read_from_node(node_addr: &str, block_id: u64) -> Result<Vec<u8>, String> {
    let mut stream = TcpStream::connect(node_addr)
        .await
        .map_err(|e| format!("Failed to connect: {}", e))?;

    let request = Request::ReadBlock { block_id };
    let msg = serde_json::to_vec(&request).map_err(|e| e.to_string())?;
    let len = (msg.len() as u32).to_be_bytes();
    
    stream
        .write_all(&len)
        .await
        .map_err(|e| e.to_string())?;
    stream
        .write_all(&msg)
        .await
        .map_err(|e| e.to_string())?;

    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| e.to_string())?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| e.to_string())?;

    let response: Response = serde_json::from_slice(&buf).map_err(|e| e.to_string())?;

    match response {
        Response::BlockData { data } => Ok(data),
        Response::Error { message } => Err(message),
        _ => Err("Unexpected response".to_string()),
    }
}