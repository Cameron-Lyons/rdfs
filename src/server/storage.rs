use crate::client::connection::{Envelope, Request, Response, auth_token};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

#[derive(Clone)]
struct BlockStorage {
    size: u64,
    path: PathBuf,
    checksum: String,
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

        let heartbeat_task = self.start_heartbeat();

        let listener = TcpListener::bind(&self.addr).await?;
        println!("Storage node {} listening on {}", self.id, self.addr);

        let blocks = Arc::clone(&self.blocks);
        let data_dir = self.data_dir.clone();
        let used = Arc::clone(&self.used);
        let capacity = self.capacity;

        tokio::spawn(heartbeat_task);

        loop {
            let (socket, addr) = listener.accept().await?;
            let blocks = Arc::clone(&blocks);
            let data_dir = data_dir.clone();
            let used = Arc::clone(&used);

            tokio::spawn(async move {
                if let Err(e) =
                    handle_storage_request(socket, blocks, data_dir, used, capacity).await
                {
                    eprintln!("Error handling storage request from {}: {}", addr, e);
                }
            });
        }
    }

    async fn load_existing_blocks(&self) -> tokio::io::Result<()> {
        let mut loaded = HashMap::new();
        let mut used = 0u64;
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
                    path: path.clone(),
                    checksum: calculate_checksum(&data),
                },
            );
        }

        *self.blocks.write().await = loaded;
        *self.used.write().await = used.min(self.capacity);
        Ok(())
    }

    async fn report_block_inventory(&self) -> tokio::io::Result<()> {
        let mut stream = TcpStream::connect(&self.master_addr).await?;
        let block_ids: Vec<u64> = self.blocks.read().await.keys().copied().collect();
        let payload = serde_json::json!({
            "type": "block_inventory",
            "node_id": self.id,
            "blocks": block_ids,
            "token": auth_token()
        });
        let msg = serde_json::to_vec(&payload)?;
        let len = (msg.len() as u32).to_be_bytes();
        stream.write_all(&len).await?;
        stream.write_all(&msg).await?;
        Ok(())
    }

    async fn register_with_master(&self) -> tokio::io::Result<()> {
        let mut stream = TcpStream::connect(&self.master_addr).await?;

        let registration = serde_json::json!({
            "type": "register_node",
            "node_id": self.id,
            "addr": self.addr,
            "capacity": self.capacity,
            "token": auth_token()
        });

        let msg = serde_json::to_vec(&registration)?;
        let len = (msg.len() as u32).to_be_bytes();
        stream.write_all(&len).await?;
        stream.write_all(&msg).await?;

        println!("Registered with master at {}", self.master_addr);
        Ok(())
    }

    fn start_heartbeat(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let master_addr = self.master_addr.clone();
        let node_id = self.id.clone();
        let used = Arc::clone(&self.used);

        async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
            loop {
                interval.tick().await;
                if let Ok(mut stream) = TcpStream::connect(&master_addr).await {
                    let used_bytes = *used.read().await;
                    let heartbeat = serde_json::json!({
                        "type": "heartbeat",
                        "node_id": node_id.clone(),
                        "used": used_bytes,
                        "token": auth_token()
                    });

                    if let Ok(msg) = serde_json::to_vec(&heartbeat) {
                        let len = (msg.len() as u32).to_be_bytes();
                        let _ = stream.write_all(&len).await;
                        let _ = stream.write_all(&msg).await;
                    }
                }
            }
        }
    }
}

async fn handle_storage_request(
    mut socket: TcpStream,
    blocks: Arc<RwLock<HashMap<u64, BlockStorage>>>,
    data_dir: PathBuf,
    used: Arc<RwLock<u64>>,
    capacity: u64,
) -> tokio::io::Result<()> {
    let expected_token = auth_token();

    loop {
        let mut len_buf = [0u8; 4];
        match socket.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(e) => return Err(e),
        }
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut buf = vec![0u8; len];
        socket.read_exact(&mut buf).await?;

        let (request, early_response) =
            if let Ok(envelope) = serde_json::from_slice::<Envelope>(&buf) {
                if let Some(ref expected) = expected_token {
                    match &envelope.token {
                        Some(t) if t == expected => (Some(envelope.payload), None),
                        _ => (
                            None,
                            Some(Response::Error {
                                message: "Unauthorized".to_string(),
                            }),
                        ),
                    }
                } else {
                    (Some(envelope.payload), None)
                }
            } else {
                match serde_json::from_slice::<Request>(&buf) {
                    Ok(req) => (Some(req), None),
                    Err(e) => (
                        None,
                        Some(Response::Error {
                            message: format!("Invalid request: {e}"),
                        }),
                    ),
                }
            };

        let response = if let Some(r) = early_response {
            r
        } else if let Some(request) = request {
            match request {
                Request::ReadBlock { block_id } => handle_read_block(&blocks, block_id).await,
                Request::WriteBlock { block_id, data } => {
                    handle_write_block(&blocks, &data_dir, &used, capacity, block_id, data).await
                }
                _ => Response::Error {
                    message: "Invalid request for storage node".to_string(),
                },
            }
        } else {
            Response::Error {
                message: "Invalid request".to_string(),
            }
        };

        let response_bytes = serde_json::to_vec(&response)?;
        let len = (response_bytes.len() as u32).to_be_bytes();
        socket.write_all(&len).await?;
        socket.write_all(&response_bytes).await?;
    }

    Ok(())
}

async fn handle_read_block(
    blocks: &Arc<RwLock<HashMap<u64, BlockStorage>>>,
    block_id: u64,
) -> Response {
    let blocks_guard = blocks.read().await;

    if let Some(block) = blocks_guard.get(&block_id)
        && let Ok(mut file) = File::open(&block.path).await
    {
        let mut data = Vec::new();
        if file.read_to_end(&mut data).await.is_ok() && calculate_checksum(&data) == block.checksum
        {
            return Response::BlockData { data };
        }
    }

    Response::Error {
        message: format!("Block {} not found", block_id),
    }
}

async fn handle_write_block(
    blocks: &Arc<RwLock<HashMap<u64, BlockStorage>>>,
    data_dir: &Path,
    used: &Arc<RwLock<u64>>,
    capacity: u64,
    block_id: u64,
    data: Vec<u8>,
) -> Response {
    let block_path = data_dir.join(format!("block_{}.dat", block_id));
    let checksum = calculate_checksum(&data);
    let size = data.len() as u64;
    let previous_size = {
        let blocks_guard = blocks.read().await;
        blocks_guard.get(&block_id).map(|b| b.size).unwrap_or(0)
    };

    {
        let mut used_guard = used.write().await;
        let projected = used_guard
            .saturating_sub(previous_size)
            .saturating_add(size);
        if projected > capacity {
            return Response::Error {
                message: format!(
                    "Insufficient capacity on node. required={} available={}",
                    size,
                    capacity.saturating_sub(*used_guard)
                ),
            };
        }
        *used_guard = projected;
    }

    if let Ok(mut file) = File::create(&block_path).await
        && file.write_all(&data).await.is_ok()
    {
        let block_storage = BlockStorage {
            size,
            path: block_path,
            checksum,
        };

        let mut blocks_guard = blocks.write().await;
        blocks_guard.insert(block_id, block_storage);

        return Response::Ok;
    }

    {
        let mut used_guard = used.write().await;
        *used_guard = used_guard
            .saturating_add(previous_size)
            .saturating_sub(size);
    }

    Response::Error {
        message: format!("Failed to write block {}", block_id),
    }
}

fn calculate_checksum(data: &[u8]) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in data {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", hash)
}
