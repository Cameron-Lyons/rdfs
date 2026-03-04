use crate::protocol::{
    PROTOCOL_VERSION, RpcEnvelope, RpcError, RpcErrorCode, RpcRequest, RpcResponse, recv_envelope,
    send_response, validate_token,
};
use crate::server::metadata::{MetadataError, MetadataStore};
use crate::server::replication::ReplicationManager;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub struct MasterServer {
    addr: String,
    metadata_store: Arc<MetadataStore>,
    default_replication_factor: usize,
}

impl MasterServer {
    pub fn new(addr: String) -> Self {
        let db_path = format!("rdfs_metadata_{}.db", addr.replace([':', '.'], "_"));
        let default_replication_factor = std::env::var("RDFS_DEFAULT_REPLICATION")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(3);

        Self {
            addr,
            metadata_store: Arc::new(MetadataStore::with_path(&db_path)),
            default_replication_factor,
        }
    }

    pub async fn start(&self) -> tokio::io::Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        println!("Master server listening on {}", self.addr);

        let metadata_store = Arc::clone(&self.metadata_store);
        let default_replication_factor = self.default_replication_factor;
        tokio::spawn(async move {
            Self::monitor_cluster_health(metadata_store, default_replication_factor).await;
        });

        let health_addr = health_addr_for(&self.addr);
        let health_metadata_store = Arc::clone(&self.metadata_store);
        tokio::spawn(async move {
            if let Err(e) = Self::start_health_endpoint(&health_addr, health_metadata_store).await {
                eprintln!("Failed to start health endpoint: {e}");
            }
        });

        loop {
            let (socket, remote_addr) = listener.accept().await?;
            let metadata_store = Arc::clone(&self.metadata_store);
            let default_replication_factor = self.default_replication_factor;

            tokio::spawn(async move {
                if let Err(e) =
                    handle_connection(socket, metadata_store, default_replication_factor).await
                {
                    eprintln!("Error handling {remote_addr}: {e}");
                }
            });
        }
    }

    async fn monitor_cluster_health(
        metadata_store: Arc<MetadataStore>,
        default_replication_factor: usize,
    ) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            interval.tick().await;

            let stale_nodes = metadata_store.stale_node_ids(60).await;
            for node_id in stale_nodes {
                let repl = ReplicationManager::new(
                    Arc::clone(&metadata_store),
                    default_replication_factor,
                );
                if let Err(e) = repl.handle_node_failure(&node_id).await {
                    eprintln!("Replication handling failed for node {node_id}: {e}");
                }
            }

            let under_replicated_files = metadata_store.get_under_replicated_file_ids().await;
            if under_replicated_files.is_empty() {
                continue;
            }

            let repl =
                ReplicationManager::new(Arc::clone(&metadata_store), default_replication_factor);
            for file_id in under_replicated_files {
                if let Err(e) = repl.ensure_replication(file_id).await {
                    eprintln!("Under-replication repair failed for file_id={file_id}: {e}");
                }
            }
        }
    }

    async fn start_health_endpoint(
        addr: &str,
        metadata_store: Arc<MetadataStore>,
    ) -> tokio::io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        println!("Health endpoint listening on {addr}");

        loop {
            let (mut socket, _) = listener.accept().await?;
            let metadata_store = Arc::clone(&metadata_store);
            tokio::spawn(async move {
                let _ = handle_health_check(&mut socket, metadata_store).await;
            });
        }
    }
}

fn health_addr_for(master_addr: &str) -> String {
    if let Some((host, port_str)) = master_addr.rsplit_once(':')
        && let Ok(port) = port_str.parse::<u16>()
    {
        return format!("{}:{}", host, port.saturating_add(80));
    }
    format!("{}:9080", master_addr)
}

async fn handle_health_check(
    socket: &mut TcpStream,
    metadata_store: Arc<MetadataStore>,
) -> tokio::io::Result<()> {
    let mut buffer = [0u8; 1024];
    let n = socket.read(&mut buffer).await?;
    if n == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&buffer[..n]);
    if !request.contains("GET /health") {
        socket
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
            .await?;
        return Ok(());
    }

    let stats = metadata_store.quorum_status().await;
    let health = serde_json::json!({
        "status": "healthy",
        "timestamp": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        "metadata": {
            "term": stats.term,
            "commit_index": stats.commit_index,
            "replicas": stats.replicas,
            "quorum": stats.quorum,
            "active_nodes": stats.active_nodes,
            "under_replicated_blocks": stats.under_replicated_blocks,
            "files": stats.total_files,
            "directories": stats.total_directories,
        }
    });

    let body = serde_json::to_string_pretty(&health)?;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket.write_all(response.as_bytes()).await?;
    Ok(())
}

async fn handle_connection(
    mut socket: TcpStream,
    metadata_store: Arc<MetadataStore>,
    default_replication_factor: usize,
) -> tokio::io::Result<()> {
    loop {
        let envelope = match recv_envelope(&mut socket).await {
            Ok(envelope) => envelope,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        };

        let response = dispatch_request(
            envelope,
            Arc::clone(&metadata_store),
            default_replication_factor,
        )
        .await;

        send_response(&mut socket, &response).await?;
    }

    Ok(())
}

async fn dispatch_request(
    envelope: RpcEnvelope,
    metadata_store: Arc<MetadataStore>,
    default_replication_factor: usize,
) -> RpcResponse {
    if envelope.version != PROTOCOL_VERSION {
        return error_response(RpcError {
            code: RpcErrorCode::InvalidArgument,
            message: format!(
                "unsupported protocol version {} (expected {})",
                envelope.version, PROTOCOL_VERSION
            ),
            retryable: false,
            current_handle: None,
            details: None,
        });
    }

    if !validate_token(&envelope.token) {
        return error_response(RpcError {
            code: RpcErrorCode::Unauthorized,
            message: "unauthorized".to_string(),
            retryable: false,
            current_handle: None,
            details: None,
        });
    }

    let result: Result<RpcResponse, MetadataError> = match envelope.payload {
        RpcRequest::ResolvePath { path } => metadata_store
            .lookup_file_by_path(&path)
            .await
            .map(|handle| RpcResponse::FileHandle { handle }),
        RpcRequest::CreateDirectory { path } => metadata_store
            .create_directory(&path)
            .await
            .map(|_| RpcResponse::Ok),
        RpcRequest::CreateFile {
            path,
            replication_factor,
        } => {
            let effective_rf = if replication_factor == 0 {
                default_replication_factor
            } else {
                replication_factor
            };
            metadata_store
                .create_file(&path, effective_rf)
                .await
                .map(|handle| RpcResponse::FileHandle { handle })
        }
        RpcRequest::ListDirectory { path } => metadata_store
            .list_directory(&path)
            .await
            .map(|entries| RpcResponse::DirectoryEntries { entries }),
        RpcRequest::RenameEntry { from, to } => metadata_store
            .rename_entry(&from, &to)
            .await
            .map(|_| RpcResponse::Ok),
        RpcRequest::DeleteEntry { path } => metadata_store
            .delete_entry(&path)
            .await
            .map(|_| RpcResponse::Ok),
        RpcRequest::GetFileLayout {
            file_id,
            expected_generation: _,
        } => metadata_store
            .get_file_layout(file_id)
            .await
            .map(|layout| RpcResponse::FileLayout { layout }),
        RpcRequest::BeginWrite {
            file_id,
            expected_generation,
        } => metadata_store
            .begin_write(file_id, expected_generation)
            .await
            .map(|session| RpcResponse::WriteSession { session }),
        RpcRequest::AllocateWriteChunk {
            session_id,
            size,
            checksum,
        } => metadata_store
            .allocate_write_chunk(session_id, size, checksum)
            .await
            .map(|(block_id, nodes)| RpcResponse::AllocatedChunk { block_id, nodes }),
        RpcRequest::FinalizeWriteChunk {
            session_id,
            block_id,
            replicas,
        } => metadata_store
            .finalize_write_chunk(session_id, block_id, replicas)
            .await
            .map(|_| RpcResponse::Ok),
        RpcRequest::CommitWrite { session_id } => metadata_store
            .commit_write(session_id)
            .await
            .map(|handle| RpcResponse::FileHandle { handle }),
        RpcRequest::AbortWrite { session_id } => metadata_store
            .abort_write(session_id)
            .await
            .map(|_| RpcResponse::Ok),
        RpcRequest::RegisterNode {
            node_id,
            addr,
            capacity,
        } => metadata_store
            .register_node(node_id, addr, capacity)
            .await
            .map(|_| RpcResponse::Ok),
        RpcRequest::Heartbeat { node_id, used } => metadata_store
            .update_heartbeat(&node_id, used)
            .await
            .map(|_| RpcResponse::Ok),
        RpcRequest::ReportInventory { node_id, blocks } => metadata_store
            .record_inventory(&node_id, blocks)
            .await
            .map(|_| RpcResponse::Ok),
        RpcRequest::ReadBlock { .. } | RpcRequest::WriteBlock { .. } => Err(
            MetadataError::InvalidArgument("storage block request sent to master".to_string()),
        ),
    };

    match result {
        Ok(response) => response,
        Err(err) => error_response(metadata_error_to_rpc(err)),
    }
}

fn error_response(error: RpcError) -> RpcResponse {
    RpcResponse::Error { error }
}

fn metadata_error_to_rpc(err: MetadataError) -> RpcError {
    match err {
        MetadataError::NotFound(message) => RpcError {
            code: RpcErrorCode::NotFound,
            message,
            retryable: false,
            current_handle: None,
            details: None,
        },
        MetadataError::AlreadyExists(message) => RpcError {
            code: RpcErrorCode::AlreadyExists,
            message,
            retryable: false,
            current_handle: None,
            details: None,
        },
        MetadataError::InvalidArgument(message) => RpcError {
            code: RpcErrorCode::InvalidArgument,
            message,
            retryable: false,
            current_handle: None,
            details: None,
        },
        MetadataError::Conflict(message) => RpcError {
            code: RpcErrorCode::Conflict,
            message,
            retryable: false,
            current_handle: None,
            details: None,
        },
        MetadataError::StaleHandle { current } => RpcError {
            code: RpcErrorCode::StaleHandle,
            message: "stale file handle".to_string(),
            retryable: true,
            current_handle: Some(current),
            details: None,
        },
        MetadataError::Unavailable(message) => RpcError {
            code: RpcErrorCode::Unavailable,
            message,
            retryable: true,
            current_handle: None,
            details: None,
        },
        MetadataError::Internal(message) => RpcError {
            code: RpcErrorCode::Internal,
            message,
            retryable: false,
            current_handle: None,
            details: None,
        },
    }
}
