use crate::client::connection::{Request, Response};
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
    _block_id: u64,
    _size: u64,
    path: PathBuf,
    _checksum: String,
    _version: u64,
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

        self.register_with_master().await?;

        let heartbeat_task = self.start_heartbeat();

        let listener = TcpListener::bind(&self.addr).await?;
        println!("Storage node {} listening on {}", self.id, self.addr);

        let blocks = Arc::clone(&self.blocks);
        let data_dir = self.data_dir.clone();
        let used = Arc::clone(&self.used);

        tokio::spawn(heartbeat_task);

        loop {
            let (socket, addr) = listener.accept().await?;
            let blocks = Arc::clone(&blocks);
            let data_dir = data_dir.clone();
            let used = Arc::clone(&used);

            tokio::spawn(async move {
                if let Err(e) = handle_storage_request(socket, blocks, data_dir, used).await {
                    eprintln!("Error handling storage request from {}: {}", addr, e);
                }
            });
        }
    }

    async fn register_with_master(&self) -> tokio::io::Result<()> {
        let mut stream = TcpStream::connect(&self.master_addr).await?;

        let registration = serde_json::json!({
            "type": "register_node",
            "node_id": self.id,
            "addr": self.addr,
            "capacity": self.capacity
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

        async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
            loop {
                interval.tick().await;
                if let Ok(mut stream) = TcpStream::connect(&master_addr).await {
                    let heartbeat = serde_json::json!({
                        "type": "heartbeat",
                        "node_id": node_id.clone()
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
) -> tokio::io::Result<()> {
    loop {
        let mut len_buf = [0u8; 4];
        match socket.read_exact(&mut len_buf).await {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(e) => return Err(e),
        }
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut buf = vec![0u8; len];
        socket.read_exact(&mut buf).await?;

        let request: Request = serde_json::from_slice(&buf)?;

        let response = match request {
            Request::ReadBlock { block_id } => handle_read_block(&blocks, block_id).await,
            Request::WriteBlock { block_id, data } => {
                handle_write_block(&blocks, &data_dir, &used, block_id, data).await
            }
            _ => Response::Error {
                message: "Invalid request for storage node".to_string(),
            },
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

    if let Some(block) = blocks_guard.get(&block_id) {
        if let Ok(mut file) = File::open(&block.path).await {
            let mut data = Vec::new();
            if file.read_to_end(&mut data).await.is_ok() {
                return Response::BlockData { data };
            }
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
    block_id: u64,
    data: Vec<u8>,
) -> Response {
    let block_path = data_dir.join(format!("block_{}.dat", block_id));

    if let Ok(mut file) = File::create(&block_path).await {
        if file.write_all(&data).await.is_ok() {
            let checksum = calculate_checksum(&data);
            let size = data.len() as u64;

            let block_storage = BlockStorage {
                _block_id: block_id,
                _size: size,
                path: block_path,
                _checksum: checksum,
                _version: 1,
            };

            let mut blocks_guard = blocks.write().await;
            blocks_guard.insert(block_id, block_storage);

            let mut used_guard = used.write().await;
            *used_guard += size;

            return Response::Ok;
        }
    }

    Response::Error {
        message: format!("Failed to write block {}", block_id),
    }
}

fn calculate_checksum(data: &[u8]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}
