use crate::path::{basename, normalize_path, parent_path};
use crate::pb;
use crate::raft::MetaTypeConfig;
use anyhow::{Result, bail};
use openraft::{LogId, StoredMembership};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum UploadModeModel {
    Create,
    Overwrite,
}

impl TryFrom<i32> for UploadModeModel {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self> {
        match pb::UploadMode::try_from(value) {
            Ok(pb::UploadMode::Create) => Ok(Self::Create),
            Ok(pb::UploadMode::Overwrite) => Ok(Self::Overwrite),
            _ => bail!("unsupported upload mode"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaPointer {
    pub node_id: String,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkRefModel {
    pub chunk_id: String,
    pub offset: u64,
    pub size: u64,
    pub checksum: String,
    pub replicas: Vec<ReplicaPointer>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileInfoModel {
    pub inode: u64,
    pub path: String,
    pub version: u64,
    pub size: u64,
    pub chunk_size: u32,
    pub is_dir: bool,
}

impl FileInfoModel {
    pub fn to_proto(&self) -> pb::FileInfo {
        pb::FileInfo {
            inode: self.inode,
            path: self.path.clone(),
            version: self.version,
            size: self.size,
            chunk_size: self.chunk_size,
            is_dir: self.is_dir,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirectoryEntryModel {
    pub path: String,
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
    pub version: u64,
}

impl DirectoryEntryModel {
    pub fn to_proto(&self) -> pb::DirectoryEntry {
        pb::DirectoryEntry {
            path: self.path.clone(),
            name: self.name.clone(),
            is_dir: self.is_dir,
            size: self.size,
            version: self.version,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileManifestModel {
    pub info: FileInfoModel,
    pub chunks: Vec<ChunkRefModel>,
}

impl FileManifestModel {
    pub fn to_proto<F>(&self, mut addr_for: F) -> pb::FileManifest
    where
        F: FnMut(&str) -> Option<String>,
    {
        let chunks = self
            .chunks
            .iter()
            .map(|chunk| pb::ChunkRef {
                chunk_id: chunk.chunk_id.clone(),
                offset: chunk.offset,
                size: chunk.size,
                checksum: chunk.checksum.clone(),
                replicas: chunk
                    .replicas
                    .iter()
                    .filter_map(|replica| {
                        addr_for(&replica.node_id).map(|addr| pb::ChunkReplica {
                            node_id: replica.node_id.clone(),
                            addr,
                            version: replica.version,
                        })
                    })
                    .collect(),
            })
            .collect();

        pb::FileManifest {
            info: Some(self.info.to_proto()),
            chunks,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadSessionModel {
    pub upload_id: String,
    pub lease_expiry_unix_ms: u64,
    pub target_version: u64,
    pub chunk_size: u32,
    pub replication_factor: u32,
}

impl UploadSessionModel {
    pub fn to_proto(&self) -> pb::UploadSession {
        pb::UploadSession {
            upload_id: self.upload_id.clone(),
            lease_expiry_unix_ms: self.lease_expiry_unix_ms,
            target_version: self.target_version,
            chunk_size: self.chunk_size,
            replication_factor: self.replication_factor,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkPlacementModel {
    pub chunk_id: String,
    pub version: u64,
    pub replicas: Vec<ChunkReplicaAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkReplicaAssignment {
    pub node_id: String,
    pub addr: String,
    pub version: u64,
}

impl ChunkPlacementModel {
    pub fn to_proto(&self) -> pb::ChunkPlacement {
        pb::ChunkPlacement {
            chunk_id: self.chunk_id.clone(),
            version: self.version,
            replicas: self
                .replicas
                .iter()
                .map(|replica| pb::ChunkReplica {
                    node_id: replica.node_id.clone(),
                    addr: replica.addr.clone(),
                    version: replica.version,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingChunk {
    pub chunk_id: String,
    pub size: u64,
    pub checksum: String,
    pub version: u64,
    pub replicas: Vec<ChunkReplicaAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadSessionState {
    pub upload_id: String,
    pub path: String,
    pub mode: UploadModeModel,
    pub base_version: Option<u64>,
    pub target_version: u64,
    pub lease_expiry_unix_ms: u64,
    pub chunk_size: u32,
    pub replication_factor: u32,
    pub allocations: BTreeMap<String, PendingChunk>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkRecord {
    pub chunk_id: String,
    pub size: u64,
    pub checksum: String,
    pub version: u64,
    pub desired_replication: u32,
    pub replicas: Vec<ReplicaPointer>,
    pub ref_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkTombstone {
    pub chunk_id: String,
    pub version: u64,
    pub replicas: Vec<ReplicaPointer>,
    pub delete_after_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepairTask {
    pub chunk_id: String,
    pub expected_replicas: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkServerState {
    pub node_id: String,
    pub addr: String,
    pub capacity: u64,
    pub used: u64,
    pub last_heartbeat_unix_ms: u64,
    pub inventory: BTreeMap<String, ChunkInventoryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkInventoryEntry {
    pub chunk_id: String,
    pub version: u64,
    pub checksum: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirectoryRecord {
    pub inode: u64,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileRecord {
    pub info: FileInfoModel,
    pub chunks: Vec<ChunkRefModel>,
}

impl FileRecord {
    pub fn manifest(&self) -> FileManifestModel {
        FileManifestModel {
            info: self.info.clone(),
            chunks: self.chunks.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NamespaceEntry {
    Directory(DirectoryRecord),
    File(FileRecord),
}

impl NamespaceEntry {
    pub fn info(&self) -> FileInfoModel {
        match self {
            Self::Directory(dir) => FileInfoModel {
                inode: dir.inode,
                path: dir.path.clone(),
                version: 0,
                size: 0,
                chunk_size: 0,
                is_dir: true,
            },
            Self::File(file) => file.info.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataStateMachine {
    pub last_applied_log: Option<LogId<MetaTypeConfig>>,
    pub last_membership: StoredMembership<MetaTypeConfig>,
    pub next_inode: u64,
    pub entries: BTreeMap<String, NamespaceEntry>,
    pub upload_sessions: BTreeMap<String, UploadSessionState>,
    pub chunk_records: BTreeMap<String, ChunkRecord>,
    pub chunk_servers: BTreeMap<String, ChunkServerState>,
    pub repairs: BTreeMap<String, RepairTask>,
    pub tombstones: BTreeMap<String, ChunkTombstone>,
}

impl Default for MetadataStateMachine {
    fn default() -> Self {
        let mut entries = BTreeMap::new();
        entries.insert(
            "/".to_string(),
            NamespaceEntry::Directory(DirectoryRecord {
                inode: 1,
                path: "/".to_string(),
            }),
        );
        Self {
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            next_inode: 2,
            entries,
            upload_sessions: BTreeMap::new(),
            chunk_records: BTreeMap::new(),
            chunk_servers: BTreeMap::new(),
            repairs: BTreeMap::new(),
            tombstones: BTreeMap::new(),
        }
    }
}

impl MetadataStateMachine {
    pub fn get_entry(&self, path: &str) -> Option<&NamespaceEntry> {
        self.entries.get(path)
    }

    pub fn list_directory(&self, path: &str) -> Result<Vec<DirectoryEntryModel>> {
        let path = normalize_path(path)?;
        let Some(entry) = self.entries.get(&path) else {
            bail!("not found: {path}");
        };
        if !matches!(entry, NamespaceEntry::Directory(_)) {
            bail!("not a directory: {path}");
        }

        let mut out = Vec::new();
        for (entry_path, entry) in &self.entries {
            if entry_path == "/" {
                continue;
            }
            let Some(parent) = parent_path(entry_path) else {
                continue;
            };
            if parent == path {
                let info = entry.info();
                out.push(DirectoryEntryModel {
                    path: entry_path.clone(),
                    name: basename(entry_path).to_string(),
                    is_dir: info.is_dir,
                    size: info.size,
                    version: info.version,
                });
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    pub fn chunk_server_addr(&self, node_id: &str) -> Option<String> {
        self.chunk_servers.get(node_id).map(|n| n.addr.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataCommand {
    Mkdir {
        path: String,
    },
    Rename {
        from: String,
        to: String,
        now_ms: u64,
    },
    Delete {
        path: String,
        now_ms: u64,
        gc_grace_ms: u64,
    },
    BeginUpload {
        upload_id: String,
        path: String,
        mode: UploadModeModel,
        replication_factor: u32,
        chunk_size: u32,
        now_ms: u64,
        lease_ttl_ms: u64,
    },
    AllocateChunk {
        upload_id: String,
        chunk_id: String,
        size: u64,
        checksum: String,
        now_ms: u64,
    },
    CommitUpload {
        upload_id: String,
        chunks: Vec<CommitChunkModel>,
        now_ms: u64,
        gc_grace_ms: u64,
    },
    AbortUpload {
        upload_id: String,
    },
    Heartbeat {
        node_id: String,
        addr: String,
        capacity: u64,
        used: u64,
        inventory: Vec<ChunkInventoryEntry>,
        now_ms: u64,
    },
    ReportReplicaFailure {
        chunk_id: String,
        node_id: String,
        reason: String,
    },
    RecordReplicaRepair {
        chunk_id: String,
        node_id: String,
        version: u64,
    },
    AckGarbage {
        chunk_ids: Vec<String>,
    },
}

impl std::fmt::Display for MetadataCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitChunkModel {
    pub chunk_id: String,
    pub offset: u64,
    pub size: u64,
    pub checksum: String,
    pub replicas: Vec<ChunkReplicaAssignment>,
}

impl TryFrom<pb::CommitChunk> for CommitChunkModel {
    type Error = anyhow::Error;

    fn try_from(value: pb::CommitChunk) -> Result<Self> {
        Ok(Self {
            chunk_id: value.chunk_id,
            offset: value.offset,
            size: value.size,
            checksum: value.checksum,
            replicas: value
                .replicas
                .into_iter()
                .map(|replica| ChunkReplicaAssignment {
                    node_id: replica.node_id,
                    addr: replica.addr,
                    version: replica.version,
                })
                .collect(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataResponse {
    Ack,
    FileInfo(FileInfoModel),
    FileManifest(FileManifestModel),
    UploadSession(UploadSessionModel),
    ChunkPlacement(ChunkPlacementModel),
    Entries(Vec<DirectoryEntryModel>),
    Error(String),
}
