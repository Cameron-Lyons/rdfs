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
    db: Arc<std::sync::Mutex<rusqlite::Connection>>,
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn init_schema(conn: &rusqlite::Connection) {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS files (
            path TEXT PRIMARY KEY,
            size INTEGER NOT NULL,
            block_size INTEGER NOT NULL,
            replication_factor INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            modified_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS blocks (
            block_id INTEGER PRIMARY KEY,
            file_path TEXT NOT NULL,
            size INTEGER NOT NULL,
            checksum TEXT NOT NULL,
            FOREIGN KEY (file_path) REFERENCES files(path)
        );
        CREATE TABLE IF NOT EXISTS replicas (
            block_id INTEGER NOT NULL,
            node_id TEXT NOT NULL,
            version INTEGER NOT NULL,
            last_heartbeat INTEGER NOT NULL,
            PRIMARY KEY (block_id, node_id)
        );
        CREATE TABLE IF NOT EXISTS nodes (
            id TEXT PRIMARY KEY,
            addr TEXT NOT NULL,
            capacity INTEGER NOT NULL,
            used INTEGER NOT NULL,
            last_heartbeat INTEGER NOT NULL,
            status TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        ",
    )
    .expect("failed to init schema");
}

type DbSnapshot = (
    HashMap<String, FileInfo>,
    HashMap<String, NodeInfo>,
    HashMap<u64, HashSet<String>>,
    u64,
);

fn load_from_db(conn: &rusqlite::Connection) -> DbSnapshot {
    let mut files: HashMap<String, FileInfo> = HashMap::new();
    let mut nodes: HashMap<String, NodeInfo> = HashMap::new();
    let mut block_to_nodes: HashMap<u64, HashSet<String>> = HashMap::new();
    let mut max_block_id: u64 = 0;

    {
        let mut stmt = conn
            .prepare("SELECT path, size, block_size, replication_factor, created_at, modified_at FROM files")
            .unwrap();
        let rows = stmt
            .query_map([], |row| {
                Ok(FileInfo {
                    path: row.get(0)?,
                    size: row.get::<_, i64>(1)? as u64,
                    block_size: row.get::<_, i64>(2)? as u64,
                    replication_factor: row.get::<_, i64>(3)? as usize,
                    created_at: row.get::<_, i64>(4)? as u64,
                    modified_at: row.get::<_, i64>(5)? as u64,
                    blocks: Vec::new(),
                })
            })
            .unwrap();
        for row in rows {
            let fi = row.unwrap();
            files.insert(fi.path.clone(), fi);
        }
    }

    {
        let mut stmt = conn
            .prepare("SELECT block_id, file_path, size, checksum FROM blocks")
            .unwrap();
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)? as u64,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)? as u64,
                    row.get::<_, String>(3)?,
                ))
            })
            .unwrap();
        for row in rows {
            let (block_id, file_path, size, checksum) = row.unwrap();
            if block_id >= max_block_id {
                max_block_id = block_id + 1;
            }
            if let Some(fi) = files.get_mut(&file_path) {
                fi.blocks.push(BlockInfo {
                    block_id,
                    size,
                    checksum,
                    replicas: Vec::new(),
                });
            }
        }
    }

    {
        let mut stmt = conn
            .prepare("SELECT block_id, node_id, version, last_heartbeat FROM replicas")
            .unwrap();
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)? as u64,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)? as u64,
                    row.get::<_, i64>(3)? as u64,
                ))
            })
            .unwrap();
        for row in rows {
            let (block_id, node_id, version, last_heartbeat) = row.unwrap();
            block_to_nodes
                .entry(block_id)
                .or_default()
                .insert(node_id.clone());
            for fi in files.values_mut() {
                if let Some(block) = fi.blocks.iter_mut().find(|b| b.block_id == block_id) {
                    block.replicas.push(ReplicaInfo {
                        node_id: node_id.clone(),
                        version,
                        last_heartbeat,
                    });
                }
            }
        }
    }

    {
        let mut stmt = conn
            .prepare("SELECT id, addr, capacity, used, last_heartbeat, status FROM nodes")
            .unwrap();
        let rows = stmt
            .query_map([], |row| {
                let status_str: String = row.get(5)?;
                let status = match status_str.as_str() {
                    "Active" => NodeStatus::Active,
                    "Inactive" => NodeStatus::Inactive,
                    _ => NodeStatus::Failed,
                };
                Ok(NodeInfo {
                    id: row.get(0)?,
                    addr: row.get(1)?,
                    capacity: row.get::<_, i64>(2)? as u64,
                    used: row.get::<_, i64>(3)? as u64,
                    last_heartbeat: row.get::<_, i64>(4)? as u64,
                    status,
                })
            })
            .unwrap();
        for row in rows {
            let ni = row.unwrap();
            nodes.insert(ni.id.clone(), ni);
        }
    }

    if max_block_id == 0 {
        max_block_id = 1;
    }

    (files, nodes, block_to_nodes, max_block_id)
}

impl MetadataStore {
    pub fn new() -> Self {
        let conn = rusqlite::Connection::open_in_memory().expect("failed to open in-memory sqlite");
        init_schema(&conn);
        Self {
            files: Arc::new(RwLock::new(HashMap::new())),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            block_to_nodes: Arc::new(RwLock::new(HashMap::new())),
            next_block_id: Arc::new(RwLock::new(1)),
            db: Arc::new(std::sync::Mutex::new(conn)),
        }
    }

    pub fn with_path(path: &str) -> Self {
        let conn = rusqlite::Connection::open(path).expect("failed to open sqlite db");
        conn.pragma_update(None, "journal_mode", "WAL").ok();
        init_schema(&conn);
        let (files, nodes, block_to_nodes, next_block_id) = load_from_db(&conn);
        Self {
            files: Arc::new(RwLock::new(files)),
            nodes: Arc::new(RwLock::new(nodes)),
            block_to_nodes: Arc::new(RwLock::new(block_to_nodes)),
            next_block_id: Arc::new(RwLock::new(next_block_id)),
            db: Arc::new(std::sync::Mutex::new(conn)),
        }
    }
}

impl Default for MetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

fn status_str(s: &NodeStatus) -> &'static str {
    match s {
        NodeStatus::Active => "Active",
        NodeStatus::Inactive => "Inactive",
        NodeStatus::Failed => "Failed",
    }
}

impl MetadataStore {
    pub async fn create_file(&self, path: String, replication_factor: usize) -> FileInfo {
        let now = now_secs();

        let file_info = FileInfo {
            path: path.clone(),
            size: 0,
            block_size: 4 * 1024 * 1024,
            blocks: Vec::new(),
            replication_factor,
            created_at: now,
            modified_at: now,
        };

        let mut files = self.files.write().await;
        files.insert(path.clone(), file_info.clone());

        let db = self.db.lock().unwrap();
        db.execute(
            "INSERT OR REPLACE INTO files (path, size, block_size, replication_factor, created_at, modified_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![path, 0i64, file_info.block_size as i64, replication_factor as i64, now as i64, now as i64],
        ).ok();

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
            let db = self.db.lock().unwrap();
            for block in &file_info.blocks {
                block_to_nodes.remove(&block.block_id);
                db.execute(
                    "DELETE FROM replicas WHERE block_id = ?1",
                    rusqlite::params![block.block_id as i64],
                )
                .ok();
                db.execute(
                    "DELETE FROM blocks WHERE block_id = ?1",
                    rusqlite::params![block.block_id as i64],
                )
                .ok();
            }
            db.execute("DELETE FROM files WHERE path = ?1", rusqlite::params![path])
                .ok();
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
            file_info.modified_at = now_secs();
            files.insert(to.to_string(), file_info.clone());

            let db = self.db.lock().unwrap();
            db.execute(
                "UPDATE files SET path = ?1, modified_at = ?2 WHERE path = ?3",
                rusqlite::params![to, file_info.modified_at as i64, from],
            )
            .ok();
            db.execute(
                "UPDATE blocks SET file_path = ?1 WHERE file_path = ?2",
                rusqlite::params![to, from],
            )
            .ok();
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

            let db = self.db.lock().unwrap();
            db.execute(
                "INSERT INTO blocks (block_id, file_path, size, checksum) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![block_id as i64, file_path, 0i64, ""],
            )
            .ok();

            Some(block_id)
        } else {
            None
        }
    }

    pub async fn register_node(&self, id: String, addr: String, capacity: u64) {
        let now = now_secs();

        let node_info = NodeInfo {
            id: id.clone(),
            addr: addr.clone(),
            capacity,
            used: 0,
            last_heartbeat: now,
            status: NodeStatus::Active,
        };

        let mut nodes = self.nodes.write().await;
        nodes.insert(id.clone(), node_info);

        let db = self.db.lock().unwrap();
        db.execute(
            "INSERT OR REPLACE INTO nodes (id, addr, capacity, used, last_heartbeat, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![id, addr, capacity as i64, 0i64, now as i64, "Active"],
        ).ok();
    }

    pub async fn update_heartbeat(&self, node_id: &str) {
        let now = now_secs();
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.last_heartbeat = now;
            node.status = NodeStatus::Active;
        }

        let db = self.db.lock().unwrap();
        db.execute(
            "UPDATE nodes SET last_heartbeat = ?1, status = ?2 WHERE id = ?3",
            rusqlite::params![now as i64, "Active", node_id],
        )
        .ok();
    }

    pub async fn get_active_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        let now = now_secs();

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

    pub async fn mark_node_failed(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.status = NodeStatus::Failed;
        }

        let db = self.db.lock().unwrap();
        db.execute(
            "UPDATE nodes SET status = ?1 WHERE id = ?2",
            rusqlite::params![status_str(&NodeStatus::Failed), node_id],
        )
        .ok();
    }

    pub async fn get_node(&self, node_id: &str) -> Option<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.get(node_id).cloned()
    }

    pub async fn get_all_file_paths(&self) -> Vec<String> {
        let files = self.files.read().await;
        files.keys().cloned().collect()
    }

    pub async fn update_block_replicas(
        &self,
        file_path: &str,
        block_id: u64,
        node_id: String,
    ) -> bool {
        let mut files = self.files.write().await;
        if let Some(file_info) = files.get_mut(file_path)
            && let Some(block) = file_info.blocks.iter_mut().find(|b| b.block_id == block_id)
        {
            let now = now_secs();
            let replica = ReplicaInfo {
                node_id: node_id.clone(),
                version: 1,
                last_heartbeat: now,
            };

            if !block.replicas.iter().any(|r| r.node_id == node_id) {
                block.replicas.push(replica);

                let db = self.db.lock().unwrap();
                db.execute(
                    "INSERT OR REPLACE INTO replicas (block_id, node_id, version, last_heartbeat) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![block_id as i64, &node_id, 1i64, now as i64],
                ).ok();
            }

            let mut block_to_nodes = self.block_to_nodes.write().await;
            block_to_nodes.entry(block_id).or_default().insert(node_id);

            return true;
        }
        false
    }
}
