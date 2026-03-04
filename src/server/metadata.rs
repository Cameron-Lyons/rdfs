use crate::protocol::{
    BlockLayout, BlockReplicaReport, DirectoryEntry, FileHandle, FileLayout, ReplicaLocation,
    ReplicaVersion, StorageNode, WriteSessionInfo,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

const DEFAULT_BLOCK_SIZE: u64 = 4 * 1024 * 1024;
const DEFAULT_METADATA_REPLICAS: usize = 3;

#[derive(Debug, thiserror::Error, Clone)]
pub enum MetadataError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("stale handle")]
    StaleHandle { current: FileHandle },
    #[error("unavailable: {0}")]
    Unavailable(String),
    #[error("internal: {0}")]
    Internal(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub addr: String,
    pub capacity: u64,
    pub used: u64,
    pub last_heartbeat: u64,
    pub status: NodeStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeStatus {
    Active,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DirectoryNode {
    inode_id: u64,
    path: String,
    parent_path: Option<String>,
    name: String,
    created_at: u64,
    modified_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileNode {
    inode_id: u64,
    file_id: u64,
    path: String,
    parent_path: String,
    name: String,
    generation: u64,
    replication_factor: usize,
    block_size: u64,
    size: u64,
    blocks: Vec<BlockRecord>,
    created_at: u64,
    modified_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BlockRecord {
    block_id: u64,
    size: u64,
    checksum: String,
    replicas: Vec<ReplicaRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReplicaRecord {
    node_id: String,
    version: u64,
    last_heartbeat: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum FsEntry {
    Directory(DirectoryNode),
    File(FileNode),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingBlock {
    block_id: u64,
    size: u64,
    checksum: String,
    planned_nodes: Vec<String>,
    replicas: Vec<ReplicaRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WriteSession {
    session_id: u64,
    file_id: u64,
    base_generation: u64,
    target_generation: u64,
    replication_factor: usize,
    block_size: u64,
    pending_blocks: Vec<PendingBlock>,
    created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetadataState {
    term: u64,
    commit_index: u64,
    next_inode_id: u64,
    next_file_id: u64,
    next_block_id: u64,
    next_session_id: u64,
    entries: HashMap<String, FsEntry>,
    file_paths_by_id: HashMap<u64, String>,
    nodes: HashMap<String, NodeInfo>,
    under_replicated_blocks: HashSet<u64>,
    sessions: HashMap<u64, WriteSession>,
}

impl MetadataState {
    fn empty() -> Self {
        let now = now_secs();
        let mut entries = HashMap::new();
        entries.insert(
            "/".to_string(),
            FsEntry::Directory(DirectoryNode {
                inode_id: 1,
                path: "/".to_string(),
                parent_path: None,
                name: "/".to_string(),
                created_at: now,
                modified_at: now,
            }),
        );

        Self {
            term: 1,
            commit_index: 1,
            next_inode_id: 2,
            next_file_id: 1,
            next_block_id: 1,
            next_session_id: 1,
            entries,
            file_paths_by_id: HashMap::new(),
            nodes: HashMap::new(),
            under_replicated_blocks: HashSet::new(),
            sessions: HashMap::new(),
        }
    }
}

struct MetadataReplica {
    conn: Arc<std::sync::Mutex<rusqlite::Connection>>,
}

pub struct MetadataStore {
    state: Arc<RwLock<MetadataState>>,
    replicas: Vec<MetadataReplica>,
    quorum: usize,
}

#[derive(Debug, Clone)]
pub struct MetadataStats {
    pub term: u64,
    pub commit_index: u64,
    pub total_files: usize,
    pub total_directories: usize,
    pub under_replicated_blocks: usize,
    pub active_nodes: usize,
    pub quorum: usize,
    pub replicas: usize,
}

impl MetadataStore {
    pub fn new() -> Self {
        let conn = rusqlite::Connection::open_in_memory().expect("failed to open metadata db");
        init_schema(&conn);

        let replica = MetadataReplica {
            conn: Arc::new(std::sync::Mutex::new(conn)),
        };

        let state = MetadataState::empty();
        persist_snapshot(&replica, &state).ok();

        Self {
            state: Arc::new(RwLock::new(state)),
            replicas: vec![replica],
            quorum: 1,
        }
    }

    pub fn with_path(base_path: &str) -> Self {
        let replica_count = std::env::var("RDFS_METADATA_REPLICAS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_METADATA_REPLICAS);

        let mut replicas = Vec::with_capacity(replica_count);
        for idx in 0..replica_count {
            let path = replica_path(base_path, idx, replica_count);
            let conn = rusqlite::Connection::open(&path)
                .unwrap_or_else(|e| panic!("failed to open metadata replica {path}: {e}"));
            conn.pragma_update(None, "journal_mode", "WAL").ok();
            init_schema(&conn);
            replicas.push(MetadataReplica {
                conn: Arc::new(std::sync::Mutex::new(conn)),
            });
        }

        let mut best_state: Option<MetadataState> = None;
        for replica in &replicas {
            if let Some(candidate) = load_snapshot(replica) {
                let replace = match best_state.as_ref() {
                    None => true,
                    Some(current) => candidate.commit_index > current.commit_index,
                };
                if replace {
                    best_state = Some(candidate);
                }
            }
        }

        let state = best_state.unwrap_or_else(MetadataState::empty);
        for replica in &replicas {
            persist_snapshot(replica, &state).ok();
        }

        Self {
            state: Arc::new(RwLock::new(state)),
            replicas,
            quorum: replica_count / 2 + 1,
        }
    }

    async fn apply_mutation<T, F>(&self, op_name: &str, mutation: F) -> Result<T, MetadataError>
    where
        F: FnOnce(&mut MetadataState) -> Result<T, MetadataError>,
    {
        let mut guard = self.state.write().await;
        let mut next = guard.clone();
        let output = mutation(&mut next)?;
        next.commit_index = next.commit_index.saturating_add(1);

        let mut successes = 0usize;
        for replica in &self.replicas {
            if persist_snapshot(replica, &next).is_ok() {
                successes += 1;
            }
        }

        if successes < self.quorum {
            return Err(MetadataError::Unavailable(format!(
                "{} could not reach metadata quorum ({} / {})",
                op_name,
                successes,
                self.replicas.len()
            )));
        }

        *guard = next;
        Ok(output)
    }

    pub async fn quorum_status(&self) -> MetadataStats {
        let state = self.state.read().await;
        let mut total_files = 0usize;
        let mut total_directories = 0usize;
        for entry in state.entries.values() {
            match entry {
                FsEntry::Directory(_) => total_directories += 1,
                FsEntry::File(_) => total_files += 1,
            }
        }

        MetadataStats {
            term: state.term,
            commit_index: state.commit_index,
            total_files,
            total_directories,
            under_replicated_blocks: state.under_replicated_blocks.len(),
            active_nodes: state
                .nodes
                .values()
                .filter(|n| {
                    n.status == NodeStatus::Active
                        && now_secs().saturating_sub(n.last_heartbeat) < 30
                })
                .count(),
            quorum: self.quorum,
            replicas: self.replicas.len(),
        }
    }

    pub async fn create_directory(&self, path: &str) -> Result<DirectoryEntry, MetadataError> {
        self.apply_mutation("create_directory", |state| {
            let path = normalize_path(path)?;
            if path == "/" {
                return Err(MetadataError::AlreadyExists("root directory".to_string()));
            }
            if state.entries.contains_key(&path) {
                return Err(MetadataError::AlreadyExists(path));
            }

            let parent = parent_path(&path).ok_or_else(|| {
                MetadataError::InvalidArgument("directory must not be root".to_string())
            })?;
            ensure_directory_exists(state, &parent)?;

            let now = now_secs();
            let inode_id = state.next_inode_id;
            state.next_inode_id = state.next_inode_id.saturating_add(1);

            let name = basename(&path).to_string();
            let dir = DirectoryNode {
                inode_id,
                path: path.clone(),
                parent_path: Some(parent),
                name: name.clone(),
                created_at: now,
                modified_at: now,
            };

            state.entries.insert(path.clone(), FsEntry::Directory(dir));

            Ok(DirectoryEntry {
                inode_id,
                path,
                name,
                is_dir: true,
                size: 0,
                generation: 0,
            })
        })
        .await
    }

    pub async fn create_file(
        &self,
        path: &str,
        replication_factor: usize,
    ) -> Result<FileHandle, MetadataError> {
        self.apply_mutation("create_file", |state| {
            let path = normalize_path(path)?;
            if path == "/" {
                return Err(MetadataError::InvalidArgument(
                    "cannot create file at root".to_string(),
                ));
            }
            if replication_factor == 0 {
                return Err(MetadataError::InvalidArgument(
                    "replication_factor must be >= 1".to_string(),
                ));
            }
            if state.entries.contains_key(&path) {
                return Err(MetadataError::AlreadyExists(path));
            }

            let parent = parent_path(&path)
                .ok_or_else(|| MetadataError::InvalidArgument("missing parent".to_string()))?;
            ensure_directory_exists(state, &parent)?;

            let now = now_secs();
            let inode_id = state.next_inode_id;
            state.next_inode_id = state.next_inode_id.saturating_add(1);
            let file_id = state.next_file_id;
            state.next_file_id = state.next_file_id.saturating_add(1);
            let name = basename(&path).to_string();

            let file = FileNode {
                inode_id,
                file_id,
                path: path.clone(),
                parent_path: parent,
                name,
                generation: 1,
                replication_factor,
                block_size: DEFAULT_BLOCK_SIZE,
                size: 0,
                blocks: Vec::new(),
                created_at: now,
                modified_at: now,
            };

            let handle = file_handle_from(&file);
            state.file_paths_by_id.insert(file.file_id, path.clone());
            state.entries.insert(path, FsEntry::File(file));
            Ok(handle)
        })
        .await
    }

    pub async fn lookup_file_by_path(&self, path: &str) -> Result<FileHandle, MetadataError> {
        let path = normalize_path(path)?;
        let state = self.state.read().await;
        let Some(entry) = state.entries.get(&path) else {
            return Err(MetadataError::NotFound(path));
        };

        match entry {
            FsEntry::File(file) => Ok(file_handle_from(file)),
            FsEntry::Directory(_) => Err(MetadataError::Conflict(format!(
                "path is a directory: {}",
                path
            ))),
        }
    }

    pub async fn get_file_layout(&self, file_id: u64) -> Result<FileLayout, MetadataError> {
        let state = self.state.read().await;
        file_layout_from_state(&state, file_id)
    }

    pub async fn list_directory(&self, path: &str) -> Result<Vec<DirectoryEntry>, MetadataError> {
        let path = normalize_path(path)?;
        let state = self.state.read().await;

        ensure_directory_exists(&state, &path)?;

        let mut entries = Vec::new();
        for entry in state.entries.values() {
            match entry {
                FsEntry::Directory(dir) => {
                    if dir.path == "/" {
                        continue;
                    }
                    if dir.parent_path.as_deref() == Some(path.as_str()) {
                        entries.push(DirectoryEntry {
                            inode_id: dir.inode_id,
                            path: dir.path.clone(),
                            name: dir.name.clone(),
                            is_dir: true,
                            size: 0,
                            generation: 0,
                        });
                    }
                }
                FsEntry::File(file) => {
                    if file.parent_path == path {
                        entries.push(DirectoryEntry {
                            inode_id: file.inode_id,
                            path: file.path.clone(),
                            name: file.name.clone(),
                            is_dir: false,
                            size: file.size,
                            generation: file.generation,
                        });
                    }
                }
            }
        }

        entries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(entries)
    }

    pub async fn rename_entry(&self, from: &str, to: &str) -> Result<(), MetadataError> {
        self.apply_mutation("rename_entry", |state| {
            let from = normalize_path(from)?;
            let to = normalize_path(to)?;

            if from == "/" {
                return Err(MetadataError::InvalidArgument(
                    "cannot rename root directory".to_string(),
                ));
            }
            if from == to {
                return Ok(());
            }

            let Some(entry) = state.entries.get(&from).cloned() else {
                return Err(MetadataError::NotFound(from));
            };

            if state.entries.contains_key(&to) {
                return Err(MetadataError::AlreadyExists(to));
            }

            let to_parent = parent_path(&to).ok_or_else(|| {
                MetadataError::InvalidArgument("missing target parent".to_string())
            })?;
            ensure_directory_exists(state, &to_parent)?;

            if matches!(entry, FsEntry::Directory(_)) && to.starts_with(&format!("{}/", from)) {
                return Err(MetadataError::InvalidArgument(
                    "cannot move directory inside itself".to_string(),
                ));
            }

            let mut affected_paths: Vec<String> = state
                .entries
                .keys()
                .filter(|p| **p == from || p.starts_with(&format!("{}/", from)))
                .cloned()
                .collect();

            affected_paths.sort_by_key(|p| p.len());

            let affected_set: HashSet<String> = affected_paths.iter().cloned().collect();
            for old_path in &affected_paths {
                let new_path = replace_prefix(old_path, &from, &to);
                if state.entries.contains_key(&new_path) && !affected_set.contains(&new_path) {
                    return Err(MetadataError::AlreadyExists(new_path));
                }
            }

            let now = now_secs();
            let mut extracted = Vec::with_capacity(affected_paths.len());
            for old_path in &affected_paths {
                if let Some(entry) = state.entries.remove(old_path) {
                    extracted.push((old_path.clone(), entry));
                }
            }

            for (old_path, mut entry) in extracted {
                let new_path = replace_prefix(&old_path, &from, &to);
                match &mut entry {
                    FsEntry::Directory(dir) => {
                        dir.path = new_path.clone();
                        dir.name = basename(&new_path).to_string();
                        dir.parent_path = parent_path(&new_path);
                        dir.modified_at = now;
                    }
                    FsEntry::File(file) => {
                        file.path = new_path.clone();
                        file.name = basename(&new_path).to_string();
                        file.parent_path =
                            parent_path(&new_path).unwrap_or_else(|| "/".to_string());
                        file.modified_at = now;
                        state
                            .file_paths_by_id
                            .insert(file.file_id, new_path.clone());
                    }
                }

                state.entries.insert(new_path, entry);
            }

            Ok(())
        })
        .await
    }

    pub async fn delete_entry(&self, path: &str) -> Result<(), MetadataError> {
        self.apply_mutation("delete_entry", |state| {
            let path = normalize_path(path)?;
            if path == "/" {
                return Err(MetadataError::InvalidArgument(
                    "cannot delete root directory".to_string(),
                ));
            }

            let Some(entry) = state.entries.get(&path).cloned() else {
                return Err(MetadataError::NotFound(path));
            };

            match entry {
                FsEntry::Directory(_) => {
                    let has_children = state.entries.values().any(|entry| match entry {
                        FsEntry::Directory(dir) => {
                            dir.parent_path.as_deref() == Some(path.as_str())
                        }
                        FsEntry::File(file) => file.parent_path == path,
                    });

                    if has_children {
                        return Err(MetadataError::Conflict(format!(
                            "directory is not empty: {}",
                            path
                        )));
                    }
                    state.entries.remove(&path);
                }
                FsEntry::File(file) => {
                    for block in &file.blocks {
                        state.under_replicated_blocks.remove(&block.block_id);
                    }
                    state.entries.remove(&path);
                    state.file_paths_by_id.remove(&file.file_id);
                    state.sessions.retain(|_, s| s.file_id != file.file_id);
                }
            }

            Ok(())
        })
        .await
    }

    pub async fn begin_write(
        &self,
        file_id: u64,
        expected_generation: u64,
    ) -> Result<WriteSessionInfo, MetadataError> {
        self.apply_mutation("begin_write", |state| {
            let path = state
                .file_paths_by_id
                .get(&file_id)
                .cloned()
                .ok_or_else(|| MetadataError::NotFound(format!("file_id={file_id}")))?;

            let Some(FsEntry::File(file)) = state.entries.get(&path) else {
                return Err(MetadataError::NotFound(path));
            };

            if file.generation != expected_generation {
                return Err(MetadataError::StaleHandle {
                    current: file_handle_from(file),
                });
            }

            let session_id = state.next_session_id;
            state.next_session_id = state.next_session_id.saturating_add(1);
            let session = WriteSession {
                session_id,
                file_id,
                base_generation: file.generation,
                target_generation: file.generation.saturating_add(1),
                replication_factor: file.replication_factor,
                block_size: file.block_size,
                pending_blocks: Vec::new(),
                created_at: now_secs(),
            };

            let info = WriteSessionInfo {
                session_id,
                file_id,
                base_generation: session.base_generation,
                target_generation: session.target_generation,
                replication_factor: session.replication_factor,
                block_size: session.block_size,
            };

            state.sessions.insert(session_id, session);
            Ok(info)
        })
        .await
    }

    pub async fn allocate_write_chunk(
        &self,
        session_id: u64,
        size: u64,
        checksum: String,
    ) -> Result<(u64, Vec<StorageNode>), MetadataError> {
        self.apply_mutation("allocate_write_chunk", |state| {
            let replication_factor = {
                let session = state
                    .sessions
                    .get(&session_id)
                    .ok_or_else(|| MetadataError::NotFound(format!("session_id={session_id}")))?;
                session.replication_factor
            };

            let selected_nodes =
                select_nodes_for_replication_locked(state, replication_factor, &HashSet::new());
            if selected_nodes.is_empty() {
                return Err(MetadataError::Unavailable(
                    "no active storage nodes available".to_string(),
                ));
            }

            let block_id = state.next_block_id;
            state.next_block_id = state.next_block_id.saturating_add(1);

            let planned_nodes: Vec<String> = selected_nodes.iter().map(|n| n.id.clone()).collect();
            let session = state
                .sessions
                .get_mut(&session_id)
                .ok_or_else(|| MetadataError::NotFound(format!("session_id={session_id}")))?;
            session.pending_blocks.push(PendingBlock {
                block_id,
                size,
                checksum,
                planned_nodes,
                replicas: Vec::new(),
            });

            let nodes = selected_nodes
                .into_iter()
                .map(|n| StorageNode {
                    id: n.id,
                    addr: n.addr,
                })
                .collect();

            Ok((block_id, nodes))
        })
        .await
    }

    pub async fn finalize_write_chunk(
        &self,
        session_id: u64,
        block_id: u64,
        replicas: Vec<ReplicaVersion>,
    ) -> Result<(), MetadataError> {
        self.apply_mutation("finalize_write_chunk", |state| {
            let session = state
                .sessions
                .get_mut(&session_id)
                .ok_or_else(|| MetadataError::NotFound(format!("session_id={session_id}")))?;

            let Some(block) = session
                .pending_blocks
                .iter_mut()
                .find(|b| b.block_id == block_id)
            else {
                return Err(MetadataError::NotFound(format!(
                    "block_id={block_id} in session {session_id}"
                )));
            };

            let mut dedup = HashMap::new();
            for replica in replicas {
                dedup.insert(replica.node_id, replica.version);
            }

            if dedup.is_empty() {
                return Err(MetadataError::InvalidArgument(
                    "at least one replica is required".to_string(),
                ));
            }

            block.replicas = dedup
                .into_iter()
                .map(|(node_id, version)| ReplicaRecord {
                    node_id,
                    version,
                    last_heartbeat: now_secs(),
                })
                .collect();

            Ok(())
        })
        .await
    }

    pub async fn commit_write(&self, session_id: u64) -> Result<FileHandle, MetadataError> {
        self.apply_mutation("commit_write", |state| {
            let session = state
                .sessions
                .get(&session_id)
                .cloned()
                .ok_or_else(|| MetadataError::NotFound(format!("session_id={session_id}")))?;

            let path = state
                .file_paths_by_id
                .get(&session.file_id)
                .cloned()
                .ok_or_else(|| MetadataError::NotFound(format!("file_id={}", session.file_id)))?;

            let (base_handle, replication_factor, expected_block_size) = {
                let Some(FsEntry::File(file)) = state.entries.get(&path) else {
                    return Err(MetadataError::NotFound(path.clone()));
                };
                (
                    file_handle_from(file),
                    file.replication_factor,
                    file.block_size,
                )
            };

            if base_handle.generation != session.base_generation {
                return Err(MetadataError::StaleHandle {
                    current: base_handle,
                });
            }

            let quorum_required = session.replication_factor.div_ceil(2).max(1);
            for block in &session.pending_blocks {
                if block.replicas.len() < quorum_required {
                    return Err(MetadataError::Conflict(format!(
                        "block {} missing quorum replicas ({}/{})",
                        block.block_id,
                        block.replicas.len(),
                        quorum_required
                    )));
                }
            }

            let mut new_blocks = Vec::with_capacity(session.pending_blocks.len());
            let mut under_replicated_updates = Vec::with_capacity(session.pending_blocks.len());
            for pending in &session.pending_blocks {
                under_replicated_updates.push((
                    pending.block_id,
                    pending.replicas.len() < replication_factor,
                ));
                new_blocks.push(BlockRecord {
                    block_id: pending.block_id,
                    size: pending.size,
                    checksum: pending.checksum.clone(),
                    replicas: pending.replicas.clone(),
                });
            }

            let updated_handle = {
                let Some(FsEntry::File(file)) = state.entries.get_mut(&path) else {
                    return Err(MetadataError::NotFound(path.clone()));
                };

                if file.generation != session.base_generation
                    || file.block_size != expected_block_size
                {
                    return Err(MetadataError::StaleHandle {
                        current: file_handle_from(file),
                    });
                }

                file.blocks = new_blocks;
                file.size = file.blocks.iter().map(|b| b.size).sum();
                file.generation = session.target_generation;
                file.modified_at = now_secs();
                file_handle_from(file)
            };

            for (block_id, under_replicated) in under_replicated_updates {
                if under_replicated {
                    state.under_replicated_blocks.insert(block_id);
                } else {
                    state.under_replicated_blocks.remove(&block_id);
                }
            }

            state.sessions.remove(&session_id);
            Ok(updated_handle)
        })
        .await
    }

    pub async fn abort_write(&self, session_id: u64) -> Result<(), MetadataError> {
        self.apply_mutation("abort_write", |state| {
            if state.sessions.remove(&session_id).is_none() {
                return Err(MetadataError::NotFound(format!("session_id={session_id}")));
            }
            Ok(())
        })
        .await
    }

    pub async fn register_node(
        &self,
        id: String,
        addr: String,
        capacity: u64,
    ) -> Result<(), MetadataError> {
        self.apply_mutation("register_node", |state| {
            let now = now_secs();
            state.nodes.insert(
                id.clone(),
                NodeInfo {
                    id,
                    addr,
                    capacity,
                    used: 0,
                    last_heartbeat: now,
                    status: NodeStatus::Active,
                },
            );
            Ok(())
        })
        .await
    }

    pub async fn update_heartbeat(&self, node_id: &str, used: u64) -> Result<(), MetadataError> {
        self.apply_mutation("update_heartbeat", |state| {
            let Some(node) = state.nodes.get_mut(node_id) else {
                return Err(MetadataError::NotFound(format!("node {}", node_id)));
            };
            node.last_heartbeat = now_secs();
            node.status = NodeStatus::Active;
            node.used = used.min(node.capacity);
            Ok(())
        })
        .await
    }

    pub async fn record_inventory(
        &self,
        node_id: &str,
        blocks: Vec<BlockReplicaReport>,
    ) -> Result<(), MetadataError> {
        self.apply_mutation("record_inventory", |state| {
            if !state.nodes.contains_key(node_id) {
                return Err(MetadataError::NotFound(format!("node {}", node_id)));
            }

            for report in blocks {
                let mut found_update: Option<(u64, bool)> = None;
                for entry in state.entries.values_mut() {
                    let FsEntry::File(file) = entry else {
                        continue;
                    };
                    let Some(block) = file
                        .blocks
                        .iter_mut()
                        .find(|block| block.block_id == report.block_id)
                    else {
                        continue;
                    };

                    match block.replicas.iter_mut().find(|r| r.node_id == node_id) {
                        Some(existing) => {
                            if report.version > existing.version {
                                existing.version = report.version;
                                existing.last_heartbeat = now_secs();
                            }
                        }
                        None => {
                            block.replicas.push(ReplicaRecord {
                                node_id: node_id.to_string(),
                                version: report.version,
                                last_heartbeat: now_secs(),
                            });
                        }
                    }

                    found_update = Some((
                        block.block_id,
                        block.replicas.len() < file.replication_factor,
                    ));
                    break;
                }

                if let Some((block_id, under_replicated)) = found_update {
                    if under_replicated {
                        state.under_replicated_blocks.insert(block_id);
                    } else {
                        state.under_replicated_blocks.remove(&block_id);
                    }
                }
            }

            Ok(())
        })
        .await
    }

    pub async fn select_nodes_for_replication_excluding(
        &self,
        count: usize,
        exclude: &HashSet<String>,
    ) -> Vec<NodeInfo> {
        let state = self.state.read().await;
        select_nodes_for_replication_locked(&state, count, exclude)
    }

    pub async fn stale_node_ids(&self, timeout_secs: u64) -> Vec<String> {
        let state = self.state.read().await;
        let now = now_secs();
        state
            .nodes
            .values()
            .filter(|node| {
                node.status == NodeStatus::Active
                    && now.saturating_sub(node.last_heartbeat) > timeout_secs
            })
            .map(|node| node.id.clone())
            .collect()
    }

    pub async fn mark_node_failed(&self, node_id: &str) {
        let _ = self
            .apply_mutation("mark_node_failed", |state| {
                if let Some(node) = state.nodes.get_mut(node_id) {
                    node.status = NodeStatus::Failed;
                }
                Ok(())
            })
            .await;
    }

    pub async fn remove_node_replicas(&self, node_id: &str) {
        let _ = self
            .apply_mutation("remove_node_replicas", |state| {
                let mut updates = Vec::new();
                for entry in state.entries.values_mut() {
                    if let FsEntry::File(file) = entry {
                        for block in &mut file.blocks {
                            block.replicas.retain(|r| r.node_id != node_id);
                            updates.push((
                                block.block_id,
                                block.replicas.len() < file.replication_factor,
                            ));
                        }
                    }
                }
                for (block_id, under_replicated) in updates {
                    if under_replicated {
                        state.under_replicated_blocks.insert(block_id);
                    } else {
                        state.under_replicated_blocks.remove(&block_id);
                    }
                }
                Ok(())
            })
            .await;
    }

    pub async fn all_file_ids(&self) -> Vec<u64> {
        let state = self.state.read().await;
        let mut ids: Vec<u64> = state.file_paths_by_id.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    pub async fn get_under_replicated_file_ids(&self) -> Vec<u64> {
        let state = self.state.read().await;
        let mut ids = HashSet::new();
        for entry in state.entries.values() {
            if let FsEntry::File(file) = entry
                && file
                    .blocks
                    .iter()
                    .any(|block| state.under_replicated_blocks.contains(&block.block_id))
            {
                ids.insert(file.file_id);
            }
        }

        let mut result: Vec<u64> = ids.into_iter().collect();
        result.sort_unstable();
        result
    }

    pub async fn add_block_replica(
        &self,
        file_id: u64,
        block_id: u64,
        node_id: String,
        version: u64,
    ) -> Result<(), MetadataError> {
        self.apply_mutation("add_block_replica", |state| {
            let path = state
                .file_paths_by_id
                .get(&file_id)
                .cloned()
                .ok_or_else(|| MetadataError::NotFound(format!("file_id={file_id}")))?;

            let (under_replicated, block_id_for_update) = {
                let Some(FsEntry::File(file)) = state.entries.get_mut(&path) else {
                    return Err(MetadataError::NotFound(path));
                };

                let Some(block) = file.blocks.iter_mut().find(|b| b.block_id == block_id) else {
                    return Err(MetadataError::NotFound(format!(
                        "block_id={block_id} in file_id={file_id}"
                    )));
                };

                match block.replicas.iter_mut().find(|r| r.node_id == node_id) {
                    Some(replica) => {
                        if version > replica.version {
                            replica.version = version;
                            replica.last_heartbeat = now_secs();
                        }
                    }
                    None => block.replicas.push(ReplicaRecord {
                        node_id,
                        version,
                        last_heartbeat: now_secs(),
                    }),
                }

                (
                    block.replicas.len() < file.replication_factor,
                    block.block_id,
                )
            };

            if under_replicated {
                state.under_replicated_blocks.insert(block_id_for_update);
            } else {
                state.under_replicated_blocks.remove(&block_id_for_update);
            }

            Ok(())
        })
        .await
    }
}

impl Default for MetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

fn file_layout_from_state(
    state: &MetadataState,
    file_id: u64,
) -> Result<FileLayout, MetadataError> {
    let path = state
        .file_paths_by_id
        .get(&file_id)
        .ok_or_else(|| MetadataError::NotFound(format!("file_id={file_id}")))?;

    let Some(FsEntry::File(file)) = state.entries.get(path) else {
        return Err(MetadataError::NotFound(path.clone()));
    };

    let blocks = file
        .blocks
        .iter()
        .map(|block| {
            let replicas = block
                .replicas
                .iter()
                .filter_map(|replica| {
                    state
                        .nodes
                        .get(&replica.node_id)
                        .map(|node| ReplicaLocation {
                            node_id: replica.node_id.clone(),
                            addr: node.addr.clone(),
                            version: replica.version,
                        })
                })
                .collect();

            BlockLayout {
                block_id: block.block_id,
                size: block.size,
                checksum: block.checksum.clone(),
                replicas,
            }
        })
        .collect();

    Ok(FileLayout {
        handle: file_handle_from(file),
        size: file.size,
        block_size: file.block_size,
        blocks,
    })
}

fn file_handle_from(file: &FileNode) -> FileHandle {
    FileHandle {
        inode_id: file.inode_id,
        file_id: file.file_id,
        path: file.path.clone(),
        generation: file.generation,
        replication_factor: file.replication_factor,
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn normalize_path(path: &str) -> Result<String, MetadataError> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(MetadataError::InvalidArgument(
            "path must not be empty".to_string(),
        ));
    }
    if !trimmed.starts_with('/') {
        return Err(MetadataError::InvalidArgument(format!(
            "path must be absolute: {}",
            trimmed
        )));
    }

    let mut components = Vec::new();
    for component in trimmed.split('/') {
        if component.is_empty() {
            continue;
        }
        if component == "." || component == ".." {
            return Err(MetadataError::InvalidArgument(format!(
                "path contains unsupported segment: {}",
                component
            )));
        }
        components.push(component);
    }

    if components.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", components.join("/")))
    }
}

fn parent_path(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }

    let idx = path.rfind('/')?;
    if idx == 0 {
        Some("/".to_string())
    } else {
        Some(path[..idx].to_string())
    }
}

fn basename(path: &str) -> &str {
    if path == "/" {
        return "/";
    }
    path.rsplit('/').next().unwrap_or(path)
}

fn replace_prefix(path: &str, from: &str, to: &str) -> String {
    if path == from {
        return to.to_string();
    }
    let suffix = path.strip_prefix(from).unwrap_or(path);
    format!("{}{}", to, suffix)
}

fn ensure_directory_exists(state: &MetadataState, path: &str) -> Result<(), MetadataError> {
    let Some(entry) = state.entries.get(path) else {
        return Err(MetadataError::NotFound(format!("directory {}", path)));
    };
    if matches!(entry, FsEntry::Directory(_)) {
        Ok(())
    } else {
        Err(MetadataError::Conflict(format!(
            "not a directory: {}",
            path
        )))
    }
}

fn select_nodes_for_replication_locked(
    state: &MetadataState,
    count: usize,
    exclude: &HashSet<String>,
) -> Vec<NodeInfo> {
    let now = now_secs();
    let mut nodes: Vec<NodeInfo> = state
        .nodes
        .values()
        .filter(|node| {
            node.status == NodeStatus::Active
                && now.saturating_sub(node.last_heartbeat) < 30
                && !exclude.contains(&node.id)
        })
        .cloned()
        .collect();

    nodes.sort_by_key(|n| n.used);
    nodes.truncate(count);
    nodes
}

fn replica_path(base_path: &str, idx: usize, count: usize) -> String {
    if count == 1 {
        return base_path.to_string();
    }

    if let Some(prefix) = base_path.strip_suffix(".db") {
        format!("{}_r{}.db", prefix, idx)
    } else {
        format!("{}_r{}.db", base_path, idx)
    }
}

fn init_schema(conn: &rusqlite::Connection) {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS state_snapshot (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            term INTEGER NOT NULL,
            commit_index INTEGER NOT NULL,
            state_json TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        );
        ",
    )
    .expect("failed to initialize metadata schema");
}

fn load_snapshot(replica: &MetadataReplica) -> Option<MetadataState> {
    let conn = replica.conn.lock().ok()?;
    let mut stmt = conn
        .prepare("SELECT state_json FROM state_snapshot WHERE id = 1")
        .ok()?;
    let state_json: String = stmt.query_row([], |row| row.get(0)).ok()?;
    serde_json::from_str(&state_json).ok()
}

fn persist_snapshot(replica: &MetadataReplica, state: &MetadataState) -> rusqlite::Result<()> {
    let payload = serde_json::to_string(state)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    let conn = replica
        .conn
        .lock()
        .map_err(|_| rusqlite::Error::InvalidQuery)?;

    conn.execute(
        "
        INSERT INTO state_snapshot (id, term, commit_index, state_json, updated_at)
        VALUES (1, ?1, ?2, ?3, ?4)
        ON CONFLICT(id) DO UPDATE SET
            term = excluded.term,
            commit_index = excluded.commit_index,
            state_json = excluded.state_json,
            updated_at = excluded.updated_at
        ",
        rusqlite::params![
            state.term as i64,
            state.commit_index as i64,
            payload,
            now_secs() as i64
        ],
    )?;

    Ok(())
}
