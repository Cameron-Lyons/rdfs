use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub path: String,
    pub size: u64,
    pub block_size: u64,
    pub blocks: Vec<BlockInfo>,
    pub replication_factor: usize,
    pub created_at: u64,
    pub modified_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockInfo {
    pub block_id: u64,
    pub size: u64,
    pub checksum: String,
    pub replicas: Vec<ReplicaInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub node_id: String,
    pub version: u64,
    pub last_heartbeat: u64,
}

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: String,
    pub addr: String,
    pub capacity: u64,
    pub used: u64,
    pub last_heartbeat: u64,
    pub status: NodeStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeStatus {
    Active,
    Inactive,
    Failed,
}

pub struct MetadataStore {
    files: Arc<RwLock<HashMap<String, FileInfo>>>,
    nodes: Arc<RwLock<HashMap<String, NodeInfo>>>,
    block_to_nodes: Arc<RwLock<HashMap<u64, HashSet<String>>>>,
    next_block_id: Arc<RwLock<u64>>,
}

impl MetadataStore {
    pub fn new() -> Self {
        Self {
            files: Arc::new(RwLock::new(HashMap::new())),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            block_to_nodes: Arc::new(RwLock::new(HashMap::new())),
            next_block_id: Arc::new(RwLock::new(1)),
        }
    }
}

impl Default for MetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MetadataStore {

    pub async fn create_file(&self, path: String, replication_factor: usize) -> FileInfo {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let file_info = FileInfo {
            path: path.clone(),
            size: 0,
            block_size: 4 * 1024 * 1024, // 4MB blocks
            blocks: Vec::new(),
            replication_factor,
            created_at: now,
            modified_at: now,
        };

        let mut files = self.files.write().await;
        files.insert(path, file_info.clone());
        file_info
    }

    pub async fn get_file(&self, path: &str) -> Option<FileInfo> {
        let files = self.files.read().await;
        files.get(path).cloned()
    }

    pub async fn delete_file(&self, path: &str) -> bool {
        let mut files = self.files.write().await;
        if let Some(file_info) = files.remove(path) {
            let mut block_to_nodes = self.block_to_nodes.write().await;
            for block in file_info.blocks {
                block_to_nodes.remove(&block.block_id);
            }
            true
        } else {
            false
        }
    }

    pub async fn list_files(&self, _path: &str) -> Vec<FileInfo> {
        let files = self.files.read().await;
        files.values().cloned().collect()
    }

    pub async fn rename_file(&self, from: &str, to: &str) -> bool {
        let mut files = self.files.write().await;
        if let Some(mut file_info) = files.remove(from) {
            file_info.path = to.to_string();
            file_info.modified_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            files.insert(to.to_string(), file_info);
            true
        } else {
            false
        }
    }

    pub async fn allocate_block(&self, file_path: &str) -> Option<u64> {
        let mut files = self.files.write().await;
        if let Some(file_info) = files.get_mut(file_path) {
            let mut block_id_guard = self.next_block_id.write().await;
            let block_id = *block_id_guard;
            *block_id_guard += 1;

            let block_info = BlockInfo {
                block_id,
                size: 0,
                checksum: String::new(),
                replicas: Vec::new(),
            };

            file_info.blocks.push(block_info);
            Some(block_id)
        } else {
            None
        }
    }

    pub async fn register_node(&self, id: String, addr: String, capacity: u64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let node_info = NodeInfo {
            id: id.clone(),
            addr,
            capacity,
            used: 0,
            last_heartbeat: now,
            status: NodeStatus::Active,
        };

        let mut nodes = self.nodes.write().await;
        nodes.insert(id, node_info);
    }

    pub async fn update_heartbeat(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.last_heartbeat = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            node.status = NodeStatus::Active;
        }
    }

    pub async fn get_active_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        nodes
            .values()
            .filter(|node| {
                matches!(node.status, NodeStatus::Active) && (now - node.last_heartbeat < 30)
            })
            .cloned()
            .collect()
    }

    pub async fn select_nodes_for_replication(&self, count: usize) -> Vec<NodeInfo> {
        let mut active_nodes = self.get_active_nodes().await;
        active_nodes.sort_by_key(|node| node.used);
        active_nodes.truncate(count);
        active_nodes
    }

    pub async fn update_block_replicas(
        &self,
        file_path: &str,
        block_id: u64,
        node_id: String,
    ) -> bool {
        let mut files = self.files.write().await;
        if let Some(file_info) = files.get_mut(file_path) {
            if let Some(block) = file_info.blocks.iter_mut().find(|b| b.block_id == block_id) {
                let replica = ReplicaInfo {
                    node_id: node_id.clone(),
                    version: 1,
                    last_heartbeat: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };

                if !block.replicas.iter().any(|r| r.node_id == node_id) {
                    block.replicas.push(replica);
                }

                let mut block_to_nodes = self.block_to_nodes.write().await;
                block_to_nodes
                    .entry(block_id)
                    .or_insert_with(HashSet::new)
                    .insert(node_id);

                return true;
            }
        }
        false
    }
}

