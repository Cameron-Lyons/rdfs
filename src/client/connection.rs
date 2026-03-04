use crate::client::error::DfsError;
use crate::protocol::{
    DirectoryEntry, FileHandle, FileLayout, PROTOCOL_VERSION, ReplicaVersion, RpcEnvelope,
    RpcRequest, RpcResponse, StorageNode, WriteSessionInfo, auth_token, recv_response,
    send_envelope,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::{sleep, timeout};

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_ATTEMPTS: u32 = 3;
const RETRY_BACKOFF: Duration = Duration::from_millis(250);

#[derive(Clone)]
pub struct ConnectionManager {
    master_addr: String,
    stats: Arc<RwLock<ConnectionStats>>,
    request_id: Arc<AtomicU64>,
}

#[derive(Default)]
struct ConnectionStats {
    total_requests: u64,
    failed_requests: u64,
    bytes_sent: u64,
    bytes_received: u64,
    last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ConnectionStatsSnapshot {
    pub total_requests: u64,
    pub failed_requests: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub last_error: Option<String>,
}

impl ConnectionManager {
    pub async fn connect(master_addr: &str) -> Result<Self, DfsError> {
        let _ = timeout(CONNECTION_TIMEOUT, TcpStream::connect(master_addr))
            .await
            .map_err(|_| DfsError::Network(format!("cannot reach master at {master_addr}")))?
            .map_err(|e| DfsError::Network(format!("failed to connect to master: {e}")))?;

        Ok(Self {
            master_addr: master_addr.to_string(),
            stats: Arc::new(RwLock::new(ConnectionStats::default())),
            request_id: Arc::new(AtomicU64::new(1)),
        })
    }

    pub async fn create_directory(&self, path: &str) -> Result<(), DfsError> {
        self.expect_ok(
            self.request_master(RpcRequest::CreateDirectory {
                path: path.to_string(),
            })
            .await?,
        )
    }

    pub async fn create_file(
        &self,
        path: &str,
        replication_factor: usize,
    ) -> Result<FileHandle, DfsError> {
        let response = self
            .request_master(RpcRequest::CreateFile {
                path: path.to_string(),
                replication_factor,
            })
            .await?;

        match response {
            RpcResponse::FileHandle { handle } => Ok(handle),
            RpcResponse::Error { error } => Err(error.into()),
            _ => Err(DfsError::Protocol(
                "unexpected response for CreateFile".to_string(),
            )),
        }
    }

    pub async fn resolve_path(&self, path: &str) -> Result<FileHandle, DfsError> {
        let response = self
            .request_master(RpcRequest::ResolvePath {
                path: path.to_string(),
            })
            .await?;

        match response {
            RpcResponse::FileHandle { handle } => Ok(handle),
            RpcResponse::Error { error } => Err(error.into()),
            _ => Err(DfsError::Protocol(
                "unexpected response for ResolvePath".to_string(),
            )),
        }
    }

    pub async fn list_directory(&self, path: &str) -> Result<Vec<DirectoryEntry>, DfsError> {
        let response = self
            .request_master(RpcRequest::ListDirectory {
                path: path.to_string(),
            })
            .await?;

        match response {
            RpcResponse::DirectoryEntries { entries } => Ok(entries),
            RpcResponse::Error { error } => Err(error.into()),
            _ => Err(DfsError::Protocol(
                "unexpected response for ListDirectory".to_string(),
            )),
        }
    }

    pub async fn rename_entry(&self, from: &str, to: &str) -> Result<(), DfsError> {
        self.expect_ok(
            self.request_master(RpcRequest::RenameEntry {
                from: from.to_string(),
                to: to.to_string(),
            })
            .await?,
        )
    }

    pub async fn delete_entry(&self, path: &str) -> Result<(), DfsError> {
        self.expect_ok(
            self.request_master(RpcRequest::DeleteEntry {
                path: path.to_string(),
            })
            .await?,
        )
    }

    pub async fn get_file_layout(
        &self,
        file_id: u64,
        expected_generation: Option<u64>,
    ) -> Result<FileLayout, DfsError> {
        let response = self
            .request_master(RpcRequest::GetFileLayout {
                file_id,
                expected_generation,
            })
            .await?;

        match response {
            RpcResponse::FileLayout { layout } => Ok(layout),
            RpcResponse::Error { error } => Err(error.into()),
            _ => Err(DfsError::Protocol(
                "unexpected response for GetFileLayout".to_string(),
            )),
        }
    }

    pub async fn begin_write(
        &self,
        file_id: u64,
        expected_generation: u64,
    ) -> Result<WriteSessionInfo, DfsError> {
        let response = self
            .request_master(RpcRequest::BeginWrite {
                file_id,
                expected_generation,
            })
            .await?;

        match response {
            RpcResponse::WriteSession { session } => Ok(session),
            RpcResponse::Error { error } => Err(error.into()),
            _ => Err(DfsError::Protocol(
                "unexpected response for BeginWrite".to_string(),
            )),
        }
    }

    pub async fn allocate_write_chunk(
        &self,
        session_id: u64,
        size: u64,
        checksum: String,
    ) -> Result<(u64, Vec<StorageNode>), DfsError> {
        let response = self
            .request_master(RpcRequest::AllocateWriteChunk {
                session_id,
                size,
                checksum,
            })
            .await?;

        match response {
            RpcResponse::AllocatedChunk { block_id, nodes } => Ok((block_id, nodes)),
            RpcResponse::Error { error } => Err(error.into()),
            _ => Err(DfsError::Protocol(
                "unexpected response for AllocateWriteChunk".to_string(),
            )),
        }
    }

    pub async fn finalize_write_chunk(
        &self,
        session_id: u64,
        block_id: u64,
        replicas: Vec<ReplicaVersion>,
    ) -> Result<(), DfsError> {
        self.expect_ok(
            self.request_master(RpcRequest::FinalizeWriteChunk {
                session_id,
                block_id,
                replicas,
            })
            .await?,
        )
    }

    pub async fn commit_write(&self, session_id: u64) -> Result<FileHandle, DfsError> {
        let response = self
            .request_master(RpcRequest::CommitWrite { session_id })
            .await?;

        match response {
            RpcResponse::FileHandle { handle } => Ok(handle),
            RpcResponse::Error { error } => Err(error.into()),
            _ => Err(DfsError::Protocol(
                "unexpected response for CommitWrite".to_string(),
            )),
        }
    }

    pub async fn abort_write(&self, session_id: u64) -> Result<(), DfsError> {
        self.expect_ok(
            self.request_master(RpcRequest::AbortWrite { session_id })
                .await?,
        )
    }

    pub async fn read_block(
        &self,
        layout: &FileLayout,
        block_id: u64,
    ) -> Result<Vec<u8>, DfsError> {
        let Some(block) = layout.blocks.iter().find(|b| b.block_id == block_id) else {
            return Err(DfsError::Protocol(format!(
                "block {} not in file layout",
                block_id
            )));
        };

        if block.replicas.is_empty() {
            return Err(DfsError::Network(format!(
                "no replicas available for block {}",
                block_id
            )));
        }

        let mut reads = Vec::new();
        for replica in &block.replicas {
            let addr = replica.addr.clone();
            let node_id = replica.node_id.clone();
            let manager = self.clone();
            reads.push(tokio::spawn(async move {
                let response = manager
                    .request_addr(&addr, RpcRequest::ReadBlock { block_id })
                    .await;
                (node_id, response)
            }));
        }

        let mut successful = Vec::new();
        let mut failures = Vec::new();
        for task in reads {
            match task.await {
                Ok((node_id, Ok(RpcResponse::BlockPayload { block }))) => {
                    successful.push((node_id, block));
                }
                Ok((node_id, Ok(RpcResponse::Error { error }))) => {
                    failures.push(format!("{}: {}", node_id, error.message));
                }
                Ok((node_id, Ok(other))) => {
                    failures.push(format!("{}: unexpected response {:?}", node_id, other));
                }
                Ok((node_id, Err(e))) => {
                    failures.push(format!("{}: {}", node_id, e));
                }
                Err(e) => failures.push(format!("task join error: {}", e)),
            }
        }

        if successful.is_empty() {
            return Err(DfsError::Network(format!(
                "failed to read block {} from all replicas: {:?}",
                block_id, failures
            )));
        }

        successful.sort_by_key(|(_, payload)| payload.version);
        let (_, newest) = successful
            .last()
            .expect("successful reads should not be empty");

        let conflicts = successful
            .iter()
            .filter(|(_, payload)| payload.version == newest.version)
            .map(|(_, payload)| payload.checksum.clone())
            .collect::<HashSet<_>>();

        if conflicts.len() > 1 {
            return Err(DfsError::Protocol(format!(
                "conflicting checksums detected for block {} at version {}",
                block_id, newest.version
            )));
        }

        // Read-repair older replicas in the background.
        for (node_id, payload) in &successful {
            if payload.version >= newest.version {
                continue;
            }
            if let Some(replica) = block.replicas.iter().find(|r| r.node_id == *node_id) {
                let manager = self.clone();
                let addr = replica.addr.clone();
                let checksum = newest.checksum.clone();
                let data = newest.data.clone();
                let node_id = node_id.clone();
                let version = newest.version;

                tokio::spawn(async move {
                    let _ = manager
                        .request_addr(
                            &addr,
                            RpcRequest::WriteBlock {
                                block_id,
                                version,
                                checksum,
                                data,
                            },
                        )
                        .await
                        .map_err(|e| {
                            eprintln!(
                                "read-repair failed for block {} on node {}: {}",
                                block_id, node_id, e
                            )
                        });
                });
            }
        }

        Ok(newest.data.clone())
    }

    pub async fn write_block_to_nodes(
        &self,
        nodes: &[StorageNode],
        block_id: u64,
        version: u64,
        checksum: &str,
        data: &[u8],
    ) -> Result<Vec<ReplicaVersion>, DfsError> {
        if nodes.is_empty() {
            return Err(DfsError::Network(
                "no storage nodes available for write".to_string(),
            ));
        }

        let mut tasks = Vec::with_capacity(nodes.len());
        for node in nodes {
            let manager = self.clone();
            let addr = node.addr.clone();
            let node_id = node.id.clone();
            let checksum = checksum.to_string();
            let payload = data.to_vec();
            tasks.push(tokio::spawn(async move {
                let response = manager
                    .request_addr(
                        &addr,
                        RpcRequest::WriteBlock {
                            block_id,
                            version,
                            checksum,
                            data: payload,
                        },
                    )
                    .await;
                (node_id, response)
            }));
        }

        let mut acks = Vec::new();
        let mut errors = Vec::new();
        for task in tasks {
            match task.await {
                Ok((_, Ok(RpcResponse::WriteAck { replica }))) => acks.push(replica),
                Ok((node_id, Ok(RpcResponse::Error { error }))) => {
                    errors.push(format!("{}: {}", node_id, error.message))
                }
                Ok((node_id, Ok(other))) => {
                    errors.push(format!("{}: unexpected response {:?}", node_id, other))
                }
                Ok((node_id, Err(e))) => errors.push(format!("{}: {}", node_id, e)),
                Err(e) => errors.push(format!("task join error: {}", e)),
            }
        }

        let required = nodes.len().div_ceil(2);
        if acks.len() < required {
            return Err(DfsError::Network(format!(
                "insufficient write acknowledgements ({}/{}): {:?}",
                acks.len(),
                required,
                errors
            )));
        }

        Ok(acks)
    }

    pub async fn get_stats(&self) -> ConnectionStatsSnapshot {
        let stats = self.stats.read().await;
        ConnectionStatsSnapshot {
            total_requests: stats.total_requests,
            failed_requests: stats.failed_requests,
            bytes_sent: stats.bytes_sent,
            bytes_received: stats.bytes_received,
            last_error: stats.last_error.clone(),
        }
    }

    async fn request_master(&self, payload: RpcRequest) -> Result<RpcResponse, DfsError> {
        self.request_addr(&self.master_addr, payload).await
    }

    async fn request_addr(&self, addr: &str, payload: RpcRequest) -> Result<RpcResponse, DfsError> {
        let mut last_err = None;

        let approx_size = estimate_payload_size(&payload);

        for attempt in 1..=RETRY_ATTEMPTS {
            match timeout(CONNECTION_TIMEOUT, TcpStream::connect(addr)).await {
                Ok(Ok(mut stream)) => {
                    let request_id = self.request_id.fetch_add(1, Ordering::SeqCst);
                    let envelope = RpcEnvelope {
                        version: PROTOCOL_VERSION,
                        request_id,
                        token: auth_token(),
                        payload: payload.clone(),
                    };

                    if let Err(e) = send_envelope(&mut stream, &envelope).await {
                        last_err = Some(DfsError::Io(e));
                    } else {
                        match recv_response(&mut stream).await {
                            Ok(response) => {
                                let mut stats = self.stats.write().await;
                                stats.total_requests += 1;
                                stats.bytes_sent += approx_size as u64;
                                stats.bytes_received += estimate_response_size(&response) as u64;
                                return Ok(response);
                            }
                            Err(e) => {
                                last_err = Some(DfsError::Io(e));
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    last_err = Some(DfsError::Network(format!(
                        "failed to connect to {}: {}",
                        addr, e
                    )));
                }
                Err(_) => {
                    last_err = Some(DfsError::Network(format!("connection timeout to {}", addr)));
                }
            }

            if attempt < RETRY_ATTEMPTS {
                sleep(RETRY_BACKOFF * attempt).await;
            }
        }

        let error =
            last_err.unwrap_or_else(|| DfsError::Network("unknown request failure".to_string()));
        let mut stats = self.stats.write().await;
        stats.total_requests += 1;
        stats.failed_requests += 1;
        stats.last_error = Some(error.to_string());
        Err(error)
    }

    fn expect_ok(&self, response: RpcResponse) -> Result<(), DfsError> {
        match response {
            RpcResponse::Ok => Ok(()),
            RpcResponse::Error { error } => Err(error.into()),
            _ => Err(DfsError::Protocol("unexpected response".to_string())),
        }
    }
}

fn estimate_payload_size(payload: &RpcRequest) -> usize {
    serde_json::to_vec(payload).map(|v| v.len()).unwrap_or(0)
}

fn estimate_response_size(response: &RpcResponse) -> usize {
    serde_json::to_vec(response).map(|v| v.len()).unwrap_or(0)
}
