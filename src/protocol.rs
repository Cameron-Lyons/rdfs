use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub const PROTOCOL_VERSION: u16 = 2;
pub const MAX_FRAME_SIZE: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileHandle {
    pub inode_id: u64,
    pub file_id: u64,
    pub path: String,
    pub generation: u64,
    pub replication_factor: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageNode {
    pub id: String,
    pub addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaVersion {
    pub node_id: String,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaLocation {
    pub node_id: String,
    pub addr: String,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockLayout {
    pub block_id: u64,
    pub size: u64,
    pub checksum: String,
    pub replicas: Vec<ReplicaLocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileLayout {
    pub handle: FileHandle,
    pub size: u64,
    pub block_size: u64,
    pub blocks: Vec<BlockLayout>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub inode_id: u64,
    pub path: String,
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteSessionInfo {
    pub session_id: u64,
    pub file_id: u64,
    pub base_generation: u64,
    pub target_generation: u64,
    pub replication_factor: usize,
    pub block_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockReplicaReport {
    pub block_id: u64,
    pub version: u64,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockPayload {
    pub block_id: u64,
    pub version: u64,
    pub checksum: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RpcErrorCode {
    NotFound,
    AlreadyExists,
    InvalidArgument,
    Unauthorized,
    StaleHandle,
    Conflict,
    CapacityExceeded,
    Unavailable,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: RpcErrorCode,
    pub message: String,
    pub retryable: bool,
    pub current_handle: Option<FileHandle>,
    pub details: Option<String>,
}

impl RpcError {
    pub fn new(code: RpcErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
            current_handle: None,
            details: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcRequest {
    ResolvePath {
        path: String,
    },
    CreateDirectory {
        path: String,
    },
    CreateFile {
        path: String,
        replication_factor: usize,
    },
    ListDirectory {
        path: String,
    },
    RenameEntry {
        from: String,
        to: String,
    },
    DeleteEntry {
        path: String,
    },
    GetFileLayout {
        file_id: u64,
        expected_generation: Option<u64>,
    },
    BeginWrite {
        file_id: u64,
        expected_generation: u64,
    },
    AllocateWriteChunk {
        session_id: u64,
        size: u64,
        checksum: String,
    },
    FinalizeWriteChunk {
        session_id: u64,
        block_id: u64,
        replicas: Vec<ReplicaVersion>,
    },
    CommitWrite {
        session_id: u64,
    },
    AbortWrite {
        session_id: u64,
    },
    RegisterNode {
        node_id: String,
        addr: String,
        capacity: u64,
    },
    Heartbeat {
        node_id: String,
        used: u64,
    },
    ReportInventory {
        node_id: String,
        blocks: Vec<BlockReplicaReport>,
    },
    ReadBlock {
        block_id: u64,
    },
    WriteBlock {
        block_id: u64,
        version: u64,
        checksum: String,
        data: Vec<u8>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcResponse {
    Ok,
    Error {
        error: RpcError,
    },
    FileHandle {
        handle: FileHandle,
    },
    FileLayout {
        layout: FileLayout,
    },
    DirectoryEntries {
        entries: Vec<DirectoryEntry>,
    },
    WriteSession {
        session: WriteSessionInfo,
    },
    AllocatedChunk {
        block_id: u64,
        nodes: Vec<StorageNode>,
    },
    WriteAck {
        replica: ReplicaVersion,
    },
    BlockPayload {
        block: BlockPayload,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcEnvelope {
    pub version: u16,
    pub request_id: u64,
    pub token: Option<String>,
    pub payload: RpcRequest,
}

pub fn auth_token() -> Option<String> {
    std::env::var("RDFS_AUTH_TOKEN").ok()
}

pub fn validate_token(incoming: &Option<String>) -> bool {
    match auth_token() {
        None => true,
        Some(expected) => matches!(incoming, Some(v) if v == &expected),
    }
}

pub async fn send_envelope(
    stream: &mut TcpStream,
    envelope: &RpcEnvelope,
) -> tokio::io::Result<()> {
    let msg = serde_json::to_vec(envelope).map_err(invalid_data)?;
    send_frame(stream, &msg).await
}

pub async fn recv_envelope(stream: &mut TcpStream) -> tokio::io::Result<RpcEnvelope> {
    let buf = recv_frame(stream).await?;
    serde_json::from_slice(&buf).map_err(invalid_data)
}

pub async fn send_response(
    stream: &mut TcpStream,
    response: &RpcResponse,
) -> tokio::io::Result<()> {
    let msg = serde_json::to_vec(response).map_err(invalid_data)?;
    send_frame(stream, &msg).await
}

pub async fn recv_response(stream: &mut TcpStream) -> tokio::io::Result<RpcResponse> {
    let buf = recv_frame(stream).await?;
    serde_json::from_slice(&buf).map_err(invalid_data)
}

async fn send_frame(stream: &mut TcpStream, bytes: &[u8]) -> tokio::io::Result<()> {
    if bytes.len() > MAX_FRAME_SIZE {
        return Err(tokio::io::Error::new(
            tokio::io::ErrorKind::InvalidData,
            format!("frame too large: {}", bytes.len()),
        ));
    }

    let len = (bytes.len() as u32).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(bytes).await?;
    stream.flush().await?;
    Ok(())
}

async fn recv_frame(stream: &mut TcpStream) -> tokio::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_SIZE {
        return Err(tokio::io::Error::new(
            tokio::io::ErrorKind::InvalidData,
            format!("frame too large: {}", len),
        ));
    }

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

fn invalid_data(err: impl std::fmt::Display) -> tokio::io::Error {
    tokio::io::Error::new(tokio::io::ErrorKind::InvalidData, err.to_string())
}
