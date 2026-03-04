use crate::checksum::checksum_hex;
use crate::protocol::{
    BlockPayload, BlockReplicaReport, PROTOCOL_VERSION, ReplicaVersion, RpcEnvelope, RpcError,
    RpcErrorCode, RpcRequest, RpcResponse, auth_token, recv_envelope, recv_response, send_envelope,
    send_response, validate_token,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

pub struct StorageNode {
    id: String,
    addr: String,
    master_addr: String,
    data_dir: PathBuf,
    blocks: Arc<RwLock<HashMap<u64, BlockStorage>>>,
    capacity: u64,
    used: Arc<RwLock<u64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BlockStorage {
    size: u64,
    path: PathBuf,
    checksum: String,
    version: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedBlock {
    block_id: u64,
    file_name: String,
    size: u64,
    checksum: String,
    version: u64,
}

impl StorageNode {
    pub fn new(id: String, addr: String, master_addr: String, data_dir: PathBuf) -> Self {
        Self {
            id,
            addr,
            master_addr,
            data_dir,
            blocks: Arc::new(RwLock::new(HashMap::new())),
            capacity: 100 * 1024 * 1024 * 1024,
            used: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn start(&self) -> tokio::io::Result<()> {
        fs::create_dir_all(&self.data_dir).await?;
        self.load_existing_blocks().await?;

        self.register_with_master().await?;
        self.report_block_inventory().await?;

        let heartbeat = self.start_heartbeat();
        tokio::spawn(heartbeat);

        let listener = TcpListener::bind(&self.addr).await?;
        println!("Storage node {} listening on {}", self.id, self.addr);

        let blocks = Arc::clone(&self.blocks);
        let data_dir = self.data_dir.clone();
        let used = Arc::clone(&self.used);
        let capacity = self.capacity;
        let node_id = self.id.clone();

        loop {
            let (socket, remote_addr) = listener.accept().await?;
            let blocks = Arc::clone(&blocks);
            let data_dir = data_dir.clone();
            let used = Arc::clone(&used);
            let node_id = node_id.clone();

            tokio::spawn(async move {
                if let Err(e) =
                    handle_storage_connection(socket, &node_id, blocks, data_dir, used, capacity)
                        .await
                {
                    eprintln!("Storage request error from {}: {}", remote_addr, e);
                }
            });
        }
    }

    async fn load_existing_blocks(&self) -> tokio::io::Result<()> {
        let index_path = self.data_dir.join("index.json");
        let mut loaded = HashMap::new();
        let mut used = 0u64;

        if let Ok(bytes) = fs::read(&index_path).await {
            if let Ok(index) = serde_json::from_slice::<Vec<PersistedBlock>>(&bytes) {
                for block in index {
                    let path = self.data_dir.join(block.file_name);
                    if fs::try_exists(&path).await.unwrap_or(false) {
                        used = used.saturating_add(block.size);
                        loaded.insert(
                            block.block_id,
                            BlockStorage {
                                size: block.size,
                                path,
                                checksum: block.checksum,
                                version: block.version,
                            },
                        );
                    }
                }
            }
        }

        if loaded.is_empty() {
            // Legacy fallback: read block files and infer metadata.
            let mut entries = fs::read_dir(&self.data_dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if !name.starts_with("block_") || !name.ends_with(".dat") {
                    continue;
                }

                let Some(id_str) = name
                    .strip_prefix("block_")
                    .and_then(|s| s.strip_suffix(".dat"))
                else {
                    continue;
                };
                let Ok(block_id) = id_str.parse::<u64>() else {
                    continue;
                };

                let data = fs::read(&path).await?;
                let size = data.len() as u64;
                used = used.saturating_add(size);
                loaded.insert(
                    block_id,
                    BlockStorage {
                        size,
                        path,
                        checksum: checksum_hex(&data),
                        version: 1,
                    },
                );
            }
        }

        *self.blocks.write().await = loaded;
        *self.used.write().await = used.min(self.capacity);
        persist_index(&self.data_dir, &self.blocks).await?;
        Ok(())
    }

    async fn register_with_master(&self) -> tokio::io::Result<()> {
        let request = RpcRequest::RegisterNode {
            node_id: self.id.clone(),
            addr: self.addr.clone(),
            capacity: self.capacity,
        };
        self.send_master_request(request).await?;
        println!(
            "Registered storage node {} with {}",
            self.id, self.master_addr
        );
        Ok(())
    }

    async fn report_block_inventory(&self) -> tokio::io::Result<()> {
        let blocks = self.blocks.read().await;
        let inventory: Vec<BlockReplicaReport> = blocks
            .iter()
            .map(|(block_id, block)| BlockReplicaReport {
                block_id: *block_id,
                version: block.version,
                checksum: block.checksum.clone(),
            })
            .collect();
        drop(blocks);

        let request = RpcRequest::ReportInventory {
            node_id: self.id.clone(),
            blocks: inventory,
        };
        self.send_master_request(request).await?;
        Ok(())
    }

    async fn send_master_request(&self, payload: RpcRequest) -> tokio::io::Result<()> {
        let mut stream = TcpStream::connect(&self.master_addr).await?;
        let envelope = RpcEnvelope {
            version: PROTOCOL_VERSION,
            request_id: 0,
            token: auth_token(),
            payload,
        };
        send_envelope(&mut stream, &envelope).await?;
        let response = recv_response(&mut stream).await?;
        match response {
            RpcResponse::Ok => Ok(()),
            RpcResponse::Error { error } => Err(tokio::io::Error::new(
                tokio::io::ErrorKind::Other,
                format!("master error: {}", error.message),
            )),
            _ => Err(tokio::io::Error::new(
                tokio::io::ErrorKind::InvalidData,
                "unexpected master response",
            )),
        }
    }

    fn start_heartbeat(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let master_addr = self.master_addr.clone();
        let node_id = self.id.clone();
        let used = Arc::clone(&self.used);

        async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
            loop {
                interval.tick().await;

                let used_bytes = *used.read().await;
                if let Ok(mut stream) = TcpStream::connect(&master_addr).await {
                    let envelope = RpcEnvelope {
                        version: PROTOCOL_VERSION,
                        request_id: 0,
                        token: auth_token(),
                        payload: RpcRequest::Heartbeat {
                            node_id: node_id.clone(),
                            used: used_bytes,
                        },
                    };

                    if send_envelope(&mut stream, &envelope).await.is_ok() {
                        let _ = recv_response(&mut stream).await;
                    }
                }
            }
        }
    }
}

async fn handle_storage_connection(
    mut socket: TcpStream,
    node_id: &str,
    blocks: Arc<RwLock<HashMap<u64, BlockStorage>>>,
    data_dir: PathBuf,
    used: Arc<RwLock<u64>>,
    capacity: u64,
) -> tokio::io::Result<()> {
    loop {
        let envelope = match recv_envelope(&mut socket).await {
            Ok(envelope) => envelope,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        };

        let response = if envelope.version != PROTOCOL_VERSION {
            error_response(
                RpcErrorCode::InvalidArgument,
                format!(
                    "unsupported protocol version {} (expected {})",
                    envelope.version, PROTOCOL_VERSION
                ),
            )
        } else if !validate_token(&envelope.token) {
            error_response(RpcErrorCode::Unauthorized, "unauthorized".to_string())
        } else {
            match envelope.payload {
                RpcRequest::ReadBlock { block_id } => handle_read_block(&blocks, block_id).await,
                RpcRequest::WriteBlock {
                    block_id,
                    version,
                    checksum,
                    data,
                } => {
                    handle_write_block(
                        node_id, &blocks, &data_dir, &used, capacity, block_id, version, checksum,
                        data,
                    )
                    .await
                }
                _ => error_response(
                    RpcErrorCode::InvalidArgument,
                    "invalid request for storage node".to_string(),
                ),
            }
        };

        send_response(&mut socket, &response).await?;
    }

    Ok(())
}

async fn handle_read_block(
    blocks: &Arc<RwLock<HashMap<u64, BlockStorage>>>,
    block_id: u64,
) -> RpcResponse {
    let record = {
        let guard = blocks.read().await;
        guard.get(&block_id).cloned()
    };

    let Some(block) = record else {
        return error_response(
            RpcErrorCode::NotFound,
            format!("block {block_id} not found"),
        );
    };

    let mut file = match File::open(&block.path).await {
        Ok(file) => file,
        Err(e) => {
            return error_response(
                RpcErrorCode::Internal,
                format!("failed to open block {}: {}", block_id, e),
            );
        }
    };

    let mut data = Vec::new();
    if let Err(e) = file.read_to_end(&mut data).await {
        return error_response(
            RpcErrorCode::Internal,
            format!("failed to read block {}: {}", block_id, e),
        );
    }

    let checksum = checksum_hex(&data);
    if checksum != block.checksum {
        return error_response(
            RpcErrorCode::Conflict,
            format!("checksum mismatch for block {}", block_id),
        );
    }

    RpcResponse::BlockPayload {
        block: BlockPayload {
            block_id,
            version: block.version,
            checksum,
            data,
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_write_block(
    node_id: &str,
    blocks: &Arc<RwLock<HashMap<u64, BlockStorage>>>,
    data_dir: &Path,
    used: &Arc<RwLock<u64>>,
    capacity: u64,
    block_id: u64,
    version: u64,
    checksum: String,
    data: Vec<u8>,
) -> RpcResponse {
    let calculated = checksum_hex(&data);
    if calculated != checksum {
        return error_response(
            RpcErrorCode::InvalidArgument,
            format!("checksum mismatch for write block {}", block_id),
        );
    }

    let previous = {
        let guard = blocks.read().await;
        guard.get(&block_id).cloned()
    };

    if let Some(previous) = &previous
        && version <= previous.version
    {
        let mut error = RpcError::new(
            RpcErrorCode::Conflict,
            format!(
                "stale write version for block {}: incoming={} current={}",
                block_id, version, previous.version
            ),
        );
        error.details = Some(format!("current_version={}", previous.version));
        return RpcResponse::Error { error };
    }

    let new_size = data.len() as u64;
    let previous_size = previous.as_ref().map(|b| b.size).unwrap_or(0);

    {
        let mut used_guard = used.write().await;
        let projected = used_guard
            .saturating_sub(previous_size)
            .saturating_add(new_size);
        if projected > capacity {
            return error_response(
                RpcErrorCode::CapacityExceeded,
                format!(
                    "insufficient capacity required={} available={}",
                    new_size,
                    capacity.saturating_sub(*used_guard)
                ),
            );
        }
        *used_guard = projected;
    }

    let path = data_dir.join(format!("block_{}.dat", block_id));
    if let Err(e) = fs::write(&path, &data).await {
        let mut used_guard = used.write().await;
        *used_guard = used_guard
            .saturating_add(previous_size)
            .saturating_sub(new_size);
        return error_response(
            RpcErrorCode::Internal,
            format!("failed to write block {}: {}", block_id, e),
        );
    }

    {
        let mut guard = blocks.write().await;
        guard.insert(
            block_id,
            BlockStorage {
                size: new_size,
                path: path.clone(),
                checksum,
                version,
            },
        );
    }

    if let Err(e) = persist_index(data_dir, blocks).await {
        {
            let mut guard = blocks.write().await;
            if let Some(previous) = previous {
                guard.insert(block_id, previous);
            } else {
                guard.remove(&block_id);
            }
        }
        let mut used_guard = used.write().await;
        *used_guard = used_guard
            .saturating_add(previous_size)
            .saturating_sub(new_size);
        return error_response(
            RpcErrorCode::Internal,
            format!("failed to persist block index: {}", e),
        );
    }

    RpcResponse::WriteAck {
        replica: ReplicaVersion {
            node_id: node_id.to_string(),
            version,
        },
    }
}

async fn persist_index(
    data_dir: &Path,
    blocks: &Arc<RwLock<HashMap<u64, BlockStorage>>>,
) -> tokio::io::Result<()> {
    let guard = blocks.read().await;
    let mut entries = Vec::with_capacity(guard.len());

    for (block_id, block) in guard.iter() {
        let file_name = block
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        entries.push(PersistedBlock {
            block_id: *block_id,
            file_name,
            size: block.size,
            checksum: block.checksum.clone(),
            version: block.version,
        });
    }

    entries.sort_by_key(|b| b.block_id);
    drop(guard);

    let bytes = serde_json::to_vec_pretty(&entries)
        .map_err(|e| tokio::io::Error::new(tokio::io::ErrorKind::InvalidData, e.to_string()))?;

    let tmp = data_dir.join("index.json.tmp");
    let final_path = data_dir.join("index.json");
    fs::write(&tmp, &bytes).await?;
    fs::rename(tmp, final_path).await
}

fn error_response(code: RpcErrorCode, message: String) -> RpcResponse {
    RpcResponse::Error {
        error: RpcError {
            code,
            message,
            retryable: false,
            current_handle: None,
            details: None,
        },
    }
}
