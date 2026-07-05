use crate::path::{basename, normalize_path, parent_path};
use crate::pb;
use crate::raft::MetaTypeConfig;
use anyhow::{Result, bail};
use openraft::StoredMembership;
use openraft::alias::{LogIdOf, StoredMembershipOf};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum UploadModeModel {
    Create,
    Overwrite,
    Append,
    ReplaceRange,
}

impl TryFrom<i32> for UploadModeModel {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self> {
        match pb::UploadMode::try_from(value) {
            Ok(pb::UploadMode::Create) => Ok(Self::Create),
            Ok(pb::UploadMode::Overwrite) => Ok(Self::Overwrite),
            Ok(pb::UploadMode::Append) => Ok(Self::Append),
            Ok(pb::UploadMode::ReplaceRange) => Ok(Self::ReplaceRange),
            _ => bail!("unsupported upload mode"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaPointer {
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErasureShardModel {
    pub chunk_id: String,
    pub checksum: String,
    pub node_id: String,
}

/// Erasure-coded layout of one chunk: `data_shards + parity_shards` shards
/// of `shard_size` bytes, any `data_shards` of which reconstruct the chunk.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErasureRefModel {
    pub data_shards: u32,
    pub parity_shards: u32,
    pub shard_size: u64,
    pub shards: Vec<ErasureShardModel>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkRefModel {
    pub chunk_id: String,
    pub offset: u64,
    pub size: u64,
    pub checksum: String,
    pub replicas: Vec<ReplicaPointer>,
    /// Set for erasure-coded chunks; `replicas` is empty in that case.
    #[serde(default)]
    pub erasure: Option<ErasureRefModel>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileInfoModel {
    pub inode: u64,
    pub path: String,
    pub size: u64,
    pub chunk_size: u32,
    pub is_dir: bool,
}

impl FileInfoModel {
    pub fn to_proto(&self) -> pb::FileInfo {
        pb::FileInfo {
            inode: self.inode,
            path: self.path.clone(),
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
}

impl DirectoryEntryModel {
    pub fn to_proto(&self) -> pb::DirectoryEntry {
        pb::DirectoryEntry {
            path: self.path.clone(),
            name: self.name.clone(),
            is_dir: self.is_dir,
            size: self.size,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileManifestModel {
    pub info: FileInfoModel,
    pub chunks: Vec<ChunkRefModel>,
}

impl ErasureRefModel {
    pub fn to_proto<F>(&self, mut addr_for: F) -> pb::ErasureInfo
    where
        F: FnMut(&str) -> Option<String>,
    {
        pb::ErasureInfo {
            data_shards: self.data_shards,
            parity_shards: self.parity_shards,
            shard_size: self.shard_size,
            shards: self
                .shards
                .iter()
                .map(|shard| pb::ErasureShard {
                    chunk_id: shard.chunk_id.clone(),
                    checksum: shard.checksum.clone(),
                    replica: addr_for(&shard.node_id).map(|addr| pb::ChunkReplica {
                        node_id: shard.node_id.clone(),
                        addr,
                    }),
                })
                .collect(),
        }
    }
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
                        })
                    })
                    .collect(),
                erasure: chunk
                    .erasure
                    .as_ref()
                    .map(|erasure| erasure.to_proto(&mut addr_for)),
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
    pub chunk_size: u32,
    pub replication_factor: u32,
    pub data_shards: u32,
    pub parity_shards: u32,
}

impl UploadSessionModel {
    pub fn to_proto(&self) -> pb::UploadSession {
        pb::UploadSession {
            upload_id: self.upload_id.clone(),
            lease_expiry_unix_ms: self.lease_expiry_unix_ms,
            chunk_size: self.chunk_size,
            replication_factor: self.replication_factor,
            data_shards: self.data_shards,
            parity_shards: self.parity_shards,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkPlacementModel {
    pub chunk_id: String,
    pub replicas: Vec<ChunkReplicaAssignment>,
    pub shards: Vec<PendingShard>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkReplicaAssignment {
    pub node_id: String,
    pub addr: String,
}

impl ChunkPlacementModel {
    pub fn to_proto(&self) -> pb::ChunkPlacement {
        pb::ChunkPlacement {
            chunk_id: self.chunk_id.clone(),
            replicas: self
                .replicas
                .iter()
                .map(|replica| pb::ChunkReplica {
                    node_id: replica.node_id.clone(),
                    addr: replica.addr.clone(),
                })
                .collect(),
            shards: self
                .shards
                .iter()
                .map(|shard| pb::ErasureShard {
                    chunk_id: shard.chunk_id.clone(),
                    checksum: shard.checksum.clone(),
                    replica: Some(pb::ChunkReplica {
                        node_id: shard.node.node_id.clone(),
                        addr: shard.node.addr.clone(),
                    }),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingShard {
    pub chunk_id: String,
    pub checksum: String,
    pub node: ChunkReplicaAssignment,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingChunk {
    pub chunk_id: String,
    pub size: u64,
    pub checksum: String,
    pub replicas: Vec<ChunkReplicaAssignment>,
    /// For erasure-coded sessions: per-shard ids, checksums, and placements.
    #[serde(default)]
    pub shards: Vec<PendingShard>,
    #[serde(default)]
    pub shard_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadSessionState {
    pub upload_id: String,
    pub path: String,
    pub mode: UploadModeModel,
    pub lease_expiry_unix_ms: u64,
    pub chunk_size: u32,
    pub replication_factor: u32,
    /// Non-zero when the session stores chunks erasure-coded.
    #[serde(default)]
    pub data_shards: u32,
    #[serde(default)]
    pub parity_shards: u32,
    pub allocations: BTreeMap<String, PendingChunk>,
}

/// Ties a shard's chunk record back to its erasure group so a lost shard
/// can be rebuilt from its peers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErasureGroupLink {
    pub shard_index: u32,
    pub data_shards: u32,
    pub parity_shards: u32,
    /// Chunk ids of all shards in the group, in index order (including this
    /// shard's own id at `shard_index`).
    pub peers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkRecord {
    pub chunk_id: String,
    pub size: u64,
    pub checksum: String,
    pub desired_replication: u32,
    pub replicas: Vec<ReplicaPointer>,
    pub ref_count: u64,
    /// Present when this chunk is one shard of an erasure group.
    #[serde(default)]
    pub erasure: Option<ErasureGroupLink>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkTombstone {
    pub chunk_id: String,
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
    /// Digest of the last full inventory this server reported, used to
    /// detect when a liveness-only heartbeat is hiding a divergent inventory.
    #[serde(default)]
    pub inventory_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkInventoryEntry {
    pub chunk_id: String,
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
    pub last_applied_log: Option<LogIdOf<MetaTypeConfig>>,
    pub last_membership: StoredMembershipOf<MetaTypeConfig>,
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
        #[serde(default)]
        data_shards: u32,
        #[serde(default)]
        parity_shards: u32,
        now_ms: u64,
        lease_ttl_ms: u64,
    },
    AllocateChunk {
        upload_id: String,
        chunk_id: String,
        size: u64,
        checksum: String,
        /// For erasure-coded sessions: ids and checksums of the shards, in
        /// index order.
        #[serde(default)]
        shard_chunk_ids: Vec<String>,
        #[serde(default)]
        shard_checksums: Vec<String>,
        #[serde(default)]
        shard_size: u64,
        now_ms: u64,
    },
    CommitUpload {
        upload_id: String,
        chunks: Vec<CommitChunkModel>,
        /// Byte range of the existing file replaced by this commit; only
        /// read for `ReplaceRange` sessions.
        #[serde(default)]
        replace_from: u64,
        #[serde(default)]
        replace_len: u64,
        now_ms: u64,
        gc_grace_ms: u64,
    },
    Heartbeat {
        node_id: String,
        addr: String,
        capacity: u64,
        used: u64,
        /// `None` for liveness-only heartbeats whose inventory is unchanged.
        inventory: Option<Vec<ChunkInventoryEntry>>,
        #[serde(default)]
        inventory_digest: String,
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
    #[serde(default)]
    pub erasure: Option<ErasureRefModel>,
}

impl TryFrom<pb::CommitChunk> for CommitChunkModel {
    type Error = anyhow::Error;

    fn try_from(value: pb::CommitChunk) -> Result<Self> {
        let erasure = value
            .erasure
            .map(|erasure| -> Result<ErasureRefModel> {
                Ok(ErasureRefModel {
                    data_shards: erasure.data_shards,
                    parity_shards: erasure.parity_shards,
                    shard_size: erasure.shard_size,
                    shards: erasure
                        .shards
                        .into_iter()
                        .map(|shard| -> Result<ErasureShardModel> {
                            let replica = shard
                                .replica
                                .ok_or_else(|| anyhow::anyhow!("shard is missing its replica"))?;
                            Ok(ErasureShardModel {
                                chunk_id: shard.chunk_id,
                                checksum: shard.checksum,
                                node_id: replica.node_id,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                })
            })
            .transpose()?;
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
                })
                .collect(),
            erasure,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataResponse {
    Ack,
    HeartbeatAck { resend_inventory: bool },
    FileInfo(FileInfoModel),
    FileManifest(FileManifestModel),
    UploadSession(UploadSessionModel),
    ChunkPlacement(ChunkPlacementModel),
    Error(String),
}
