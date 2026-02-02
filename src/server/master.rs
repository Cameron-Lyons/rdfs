use crate::client::connection::{
    auth_token, Envelope, FileInfo, FileMetadata, Request, Response, StorageNode,
};
use crate::server::metadata::MetadataStore;
use crate::server::replication::ReplicationManager;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub struct MasterServer {
    addr: String,
    metadata_store: Arc<MetadataStore>,
    replication_factor: usize,
}

impl MasterServer {
    pub fn new(addr: String) -> Self {
        Self {
            addr,
            metadata_store: Arc::new(MetadataStore::with_path("rdfs_metadata.db")),
            replication_factor: 3,
        }
    }

    pub async fn start(&self) -> tokio::io::Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        println!("Master server listening on {}", self.addr);

        let metadata_store = Arc::clone(&self.metadata_store);
        let replication_factor = self.replication_factor;

        tokio::spawn(async move {
            Self::monitor_node_health(metadata_store.clone(), replication_factor).await;
        });

        let health_addr = self.addr.clone().replace(":9000", ":9080");
        let health_metadata_store = Arc::clone(&self.metadata_store);
        tokio::spawn(async move {
            if let Err(e) = Self::start_health_endpoint(&health_addr, health_metadata_store).await {
                eprintln!("Failed to start health endpoint: {}", e);
            }
        });

        loop {
            let (socket, addr) = listener.accept().await?;
            let metadata_store = Arc::clone(&self.metadata_store);
            let replication_factor = self.replication_factor;

            tokio::spawn(async move {
                println!("New connection from {}", addr);
                if let Err(e) = handle_client(socket, metadata_store, replication_factor).await {
                    eprintln!("Error handling client {}: {}", addr, e);
                }
            });
        }
    }

    async fn monitor_node_health(metadata_store: Arc<MetadataStore>, replication_factor: usize) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            let nodes = metadata_store.get_active_nodes().await;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            for node in nodes {
                if now - node.last_heartbeat > 60 {
                    println!("Node {} marked as failed", node.id);
                    let ms = Arc::clone(&metadata_store);
                    let node_id = node.id.clone();
                    let rf = replication_factor;
                    tokio::spawn(async move {
                        let repl = ReplicationManager::new(ms, rf);
                        if let Err(e) = repl.handle_node_failure(&node_id).await {
                            eprintln!("Re-replication error for node {}: {}", node_id, e);
                        }
                    });
                }
            }
        }
    }

    pub async fn register_storage_node(&self, node_id: String, addr: String, capacity: u64) {
        self.metadata_store
            .register_node(node_id.clone(), addr, capacity)
            .await;
        println!("Registered storage node: {}", node_id);
    }

    async fn start_health_endpoint(
        addr: &str,
        metadata_store: Arc<MetadataStore>,
    ) -> tokio::io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        println!("Health endpoint listening on {}", addr);

        loop {
            let (mut socket, _) = listener.accept().await?;
            let metadata_store = metadata_store.clone();

            tokio::spawn(async move {
                let _ = handle_health_check(&mut socket, metadata_store).await;
            });
        }
    }
}

async fn handle_health_check(
    socket: &mut TcpStream,
    metadata_store: Arc<MetadataStore>,
) -> tokio::io::Result<()> {
    let mut buffer = [0; 1024];
    let n = socket.read(&mut buffer).await?;

    if n > 0 {
        let request = String::from_utf8_lossy(&buffer[..n]);

        if request.contains("GET /health") {
            let nodes = metadata_store.get_active_nodes().await;
            let files = metadata_store.list_files("/").await;

            let health_status = serde_json::json!({
                "status": "healthy",
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                "metrics": {
                    "active_storage_nodes": nodes.len(),
                    "total_files": files.len(),
                    "replication_factor": 3
                },
                "storage_nodes": nodes.iter().map(|n| {
                    serde_json::json!({
                        "id": n.id,
                        "addr": n.addr,
                        "capacity": n.capacity,
                        "used": n.used,
                        "status": format!("{:?}", n.status)
                    })
                }).collect::<Vec<_>>()
            });

            let response_body = serde_json::to_string_pretty(&health_status)?;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                response_body.len(),
                response_body
            );

            socket.write_all(response.as_bytes()).await?;
        } else {
            let response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
            socket.write_all(response.as_bytes()).await?;
        }
    }

    Ok(())
}

fn validate_token(incoming: &Option<String>) -> bool {
    match auth_token() {
        None => true,
        Some(expected) => match incoming {
            Some(t) => t == &expected,
            None => false,
        },
    }
}

async fn handle_client(
    mut socket: TcpStream,
    metadata_store: Arc<MetadataStore>,
    _replication_factor: usize,
) -> tokio::io::Result<()> {
    let mut len_buf = [0u8; 4];
    socket.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut buf = vec![0u8; len];
    socket.read_exact(&mut buf).await?;

    if let Ok(node_msg) = serde_json::from_slice::<serde_json::Value>(&buf) {
        if let Some(msg_type) = node_msg.get("type").and_then(|v| v.as_str()) {
            let incoming_token = node_msg
                .get("token")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if !validate_token(&incoming_token) {
                eprintln!("Unauthorized node message");
                return Ok(());
            }
            match msg_type {
                "register_node" => {
                    if let (Some(node_id), Some(addr), Some(capacity)) = (
                        node_msg.get("node_id").and_then(|v| v.as_str()),
                        node_msg.get("addr").and_then(|v| v.as_str()),
                        node_msg.get("capacity").and_then(|v| v.as_u64()),
                    ) {
                        metadata_store
                            .register_node(node_id.to_string(), addr.to_string(), capacity)
                            .await;
                        println!("Registered storage node: {} at {}", node_id, addr);
                        return Ok(());
                    }
                }
                "heartbeat" => {
                    if let Some(node_id) = node_msg.get("node_id").and_then(|v| v.as_str()) {
                        metadata_store.update_heartbeat(node_id).await;
                        return Ok(());
                    }
                }
                _ => {}
            }
        }
    }

    if let Ok(envelope) = serde_json::from_slice::<Envelope>(&buf) {
        if !validate_token(&envelope.token) {
            let response = Response::Error {
                message: "Unauthorized".to_string(),
            };
            let response_bytes = serde_json::to_vec(&response)?;
            let len = (response_bytes.len() as u32).to_be_bytes();
            socket.write_all(&len).await?;
            socket.write_all(&response_bytes).await?;
            return Ok(());
        }

        let request = envelope.payload;
        println!("Received request: {:?}", request);

        let response = match request {
            Request::Lookup { path } => handle_lookup(&metadata_store, &path).await,
            Request::Create { path } => handle_create(&metadata_store, &path).await,
            Request::List { path } => handle_list(&metadata_store, &path).await,
            Request::Rename { from, to } => handle_rename(&metadata_store, &from, &to).await,
            Request::Delete { path } => handle_delete(&metadata_store, &path).await,
            _ => Response::Error {
                message: "Unsupported operation on master".to_string(),
            },
        };

        let response_bytes = serde_json::to_vec(&response)?;
        let len = (response_bytes.len() as u32).to_be_bytes();
        socket.write_all(&len).await?;
        socket.write_all(&response_bytes).await?;
    }

    Ok(())
}

async fn handle_lookup(metadata_store: &Arc<MetadataStore>, path: &str) -> Response {
    if let Some(file_info) = metadata_store.get_file(path).await {
        let active_nodes = metadata_store.get_active_nodes().await;

        let storage_nodes: Vec<StorageNode> = active_nodes
            .iter()
            .take(3)
            .map(|node| StorageNode {
                id: node.id.clone(),
                addr: node.addr.clone(),
            })
            .collect();

        let metadata = FileMetadata {
            path: file_info.path,
            size: file_info.size,
            blocks: file_info.blocks.iter().map(|b| b.block_id).collect(),
            nodes: storage_nodes,
        };

        Response::Metadata { metadata }
    } else {
        let file_info = metadata_store.create_file(path.to_string(), 3).await;

        let active_nodes = metadata_store.get_active_nodes().await;
        let storage_nodes: Vec<StorageNode> = active_nodes
            .iter()
            .take(3)
            .map(|node| StorageNode {
                id: node.id.clone(),
                addr: node.addr.clone(),
            })
            .collect();

        if storage_nodes.is_empty() {
            metadata_store.delete_file(path).await;
            return Response::Error {
                message: "No storage nodes available".to_string(),
            };
        }

        let block_id = metadata_store.allocate_block(path).await.unwrap_or(1);

        for node in &storage_nodes {
            metadata_store
                .update_block_replicas(path, block_id, node.id.clone())
                .await;
        }

        let metadata = FileMetadata {
            path: file_info.path,
            size: file_info.size,
            blocks: vec![block_id],
            nodes: storage_nodes,
        };

        Response::Metadata { metadata }
    }
}

async fn handle_create(metadata_store: &Arc<MetadataStore>, path: &str) -> Response {
    let file_info = metadata_store.create_file(path.to_string(), 3).await;

    let active_nodes = metadata_store.get_active_nodes().await;
    let storage_nodes: Vec<StorageNode> = active_nodes
        .iter()
        .take(3)
        .map(|node| StorageNode {
            id: node.id.clone(),
            addr: node.addr.clone(),
        })
        .collect();

    if storage_nodes.is_empty() {
        metadata_store.delete_file(path).await;
        return Response::Error {
            message: "No storage nodes available".to_string(),
        };
    }

    let block_id = metadata_store.allocate_block(path).await.unwrap_or(1);

    for node in &storage_nodes {
        metadata_store
            .update_block_replicas(path, block_id, node.id.clone())
            .await;
    }

    let metadata = FileMetadata {
        path: file_info.path,
        size: file_info.size,
        blocks: vec![block_id],
        nodes: storage_nodes,
    };

    Response::Metadata { metadata }
}

async fn handle_list(metadata_store: &Arc<MetadataStore>, path: &str) -> Response {
    let files = metadata_store.list_files(path).await;
    let file_list: Vec<FileInfo> = files
        .into_iter()
        .map(|f| FileInfo {
            path: f.path,
            size: f.size,
            is_dir: false,
            created_at: f.created_at,
            modified_at: f.modified_at,
        })
        .collect();

    Response::FileList { files: file_list }
}

async fn handle_rename(metadata_store: &Arc<MetadataStore>, from: &str, to: &str) -> Response {
    if metadata_store.rename_file(from, to).await {
        Response::Ok
    } else {
        Response::Error {
            message: format!("Failed to rename {} to {}", from, to),
        }
    }
}

async fn handle_delete(metadata_store: &Arc<MetadataStore>, path: &str) -> Response {
    if metadata_store.delete_file(path).await {
        Response::Ok
    } else {
        Response::Error {
            message: format!("File not found: {}", path),
        }
    }
}
