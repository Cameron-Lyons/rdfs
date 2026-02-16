use crate::client::error::DfsError;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read as IoRead, Write as IoWrite};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Mutex, RwLock, Semaphore},
    time::{sleep, timeout},
};

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_ATTEMPTS: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_millis(500);
const MAX_CONNECTIONS_PER_HOST: usize = 10;
const CIRCUIT_BREAKER_THRESHOLD: u64 = 5;
const CIRCUIT_BREAKER_RESET_TIME: Duration = Duration::from_secs(30);

type ConnectionMap = Arc<RwLock<HashMap<String, Arc<Mutex<Option<TcpStream>>>>>>;

#[derive(Clone)]
struct CircuitBreaker {
    failure_count: Arc<AtomicU64>,
    last_failure_time: Arc<RwLock<Option<SystemTime>>>,
    state: Arc<RwLock<CircuitState>>,
}

#[derive(Clone, Debug)]
enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitBreaker {
    fn new() -> Self {
        Self {
            failure_count: Arc::new(AtomicU64::new(0)),
            last_failure_time: Arc::new(RwLock::new(None)),
            state: Arc::new(RwLock::new(CircuitState::Closed)),
        }
    }

    async fn record_success(&self) {
        self.failure_count.store(0, Ordering::SeqCst);
        *self.state.write().await = CircuitState::Closed;
        *self.last_failure_time.write().await = None;
    }

    async fn record_failure(&self) {
        let failures = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
        *self.last_failure_time.write().await = Some(SystemTime::now());

        if failures >= CIRCUIT_BREAKER_THRESHOLD {
            *self.state.write().await = CircuitState::Open;
        }
    }

    async fn can_attempt(&self) -> bool {
        let state = self.state.read().await;
        match *state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(last_failure) = *self.last_failure_time.read().await {
                    if SystemTime::now()
                        .duration_since(last_failure)
                        .unwrap_or(Duration::ZERO)
                        > CIRCUIT_BREAKER_RESET_TIME
                    {
                        drop(state);
                        *self.state.write().await = CircuitState::HalfOpen;
                        true
                    } else {
                        false
                    }
                } else {
                    true
                }
            }
            CircuitState::HalfOpen => true,
        }
    }
}

#[derive(Clone)]
struct ConnectionPool {
    connections: ConnectionMap,
    last_used: Arc<RwLock<HashMap<String, u64>>>,
    semaphores: Arc<RwLock<HashMap<String, Arc<Semaphore>>>>,
    circuit_breakers: Arc<RwLock<HashMap<String, CircuitBreaker>>>,
    active_connections: Arc<AtomicUsize>,
}

impl ConnectionPool {
    fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            last_used: Arc::new(RwLock::new(HashMap::new())),
            semaphores: Arc::new(RwLock::new(HashMap::new())),
            circuit_breakers: Arc::new(RwLock::new(HashMap::new())),
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn get_connection(&self, addr: &str) -> Result<TcpStream, DfsError> {
        {
            let breakers = self.circuit_breakers.read().await;
            if let Some(breaker) = breakers.get(addr)
                && !breaker.can_attempt().await
            {
                return Err(DfsError::Network(format!(
                    "Circuit breaker open for {}",
                    addr
                )));
            }
        }

        let semaphore = {
            let mut semaphores = self.semaphores.write().await;
            semaphores
                .entry(addr.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(MAX_CONNECTIONS_PER_HOST)))
                .clone()
        };

        let _permit = semaphore.acquire().await.map_err(|e| {
            DfsError::Network(format!("Failed to acquire connection permit: {}", e))
        })?;

        {
            let connections = self.connections.read().await;
            if let Some(conn_mutex) = connections.get(addr) {
                let mut conn_opt = conn_mutex.lock().await;
                if let Some(conn) = conn_opt.take() {
                    let mut last_used = self.last_used.write().await;
                    last_used.insert(
                        addr.to_string(),
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    );
                    self.active_connections.fetch_add(1, Ordering::SeqCst);
                    return Ok(conn);
                }
            }
        }

        let stream = match timeout(CONNECTION_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => {
                let mut breakers = self.circuit_breakers.write().await;
                let breaker = breakers
                    .entry(addr.to_string())
                    .or_insert_with(CircuitBreaker::new);
                breaker.record_success().await;
                stream
            }
            Ok(Err(e)) => {
                let mut breakers = self.circuit_breakers.write().await;
                let breaker = breakers
                    .entry(addr.to_string())
                    .or_insert_with(CircuitBreaker::new);
                breaker.record_failure().await;
                return Err(DfsError::Network(format!(
                    "Failed to connect to {}: {}",
                    addr, e
                )));
            }
            Err(_) => {
                let mut breakers = self.circuit_breakers.write().await;
                let breaker = breakers
                    .entry(addr.to_string())
                    .or_insert_with(CircuitBreaker::new);
                breaker.record_failure().await;
                return Err(DfsError::Network(format!("Connection timeout to {}", addr)));
            }
        };

        stream
            .set_nodelay(true)
            .map_err(|e| DfsError::Network(format!("Failed to set TCP no-delay: {}", e)))?;

        let mut connections = self.connections.write().await;
        connections.insert(addr.to_string(), Arc::new(Mutex::new(None)));

        let mut last_used = self.last_used.write().await;
        last_used.insert(
            addr.to_string(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );

        self.active_connections.fetch_add(1, Ordering::SeqCst);
        Ok(stream)
    }

    async fn return_connection(&self, addr: &str, conn: TcpStream) {
        let connections = self.connections.read().await;
        if let Some(conn_mutex) = connections.get(addr) {
            let mut conn_opt = conn_mutex.lock().await;
            *conn_opt = Some(conn);
        }
        self.active_connections.fetch_sub(1, Ordering::SeqCst);
    }

    async fn cleanup_stale_connections(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let last_used = self.last_used.read().await;
        let stale_addrs: Vec<String> = last_used
            .iter()
            .filter(|(_, last)| now - **last > 60)
            .map(|(addr, _)| addr.clone())
            .collect();

        drop(last_used);

        if !stale_addrs.is_empty() {
            let mut connections = self.connections.write().await;
            let mut last_used = self.last_used.write().await;

            for addr in stale_addrs {
                connections.remove(&addr);
                last_used.remove(&addr);
            }
        }
    }
}

#[derive(Clone)]
struct BlockCache {
    cache: Arc<RwLock<HashMap<u64, CachedBlock>>>,
    max_size: usize,
    current_size: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct CachedBlock {
    data: Vec<u8>,
    last_accessed: SystemTime,
    access_count: u64,
}

impl BlockCache {
    fn new(max_size_mb: usize) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            max_size: max_size_mb * 1024 * 1024,
            current_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn get(&self, block_id: u64) -> Option<Vec<u8>> {
        let mut cache = self.cache.write().await;
        if let Some(block) = cache.get_mut(&block_id) {
            block.last_accessed = SystemTime::now();
            block.access_count += 1;
            Some(block.data.clone())
        } else {
            None
        }
    }

    async fn put(&self, block_id: u64, data: Vec<u8>) {
        let data_size = data.len();

        while self.current_size.load(Ordering::SeqCst) + data_size > self.max_size {
            self.evict_lru().await;
        }

        let mut cache = self.cache.write().await;
        cache.insert(
            block_id,
            CachedBlock {
                data,
                last_accessed: SystemTime::now(),
                access_count: 1,
            },
        );

        self.current_size.fetch_add(data_size, Ordering::SeqCst);
    }

    async fn evict_lru(&self) {
        let mut cache = self.cache.write().await;
        if let Some((oldest_id, oldest_block)) =
            cache.iter().min_by_key(|(_, block)| block.last_accessed)
        {
            let oldest_id = *oldest_id;
            let size = oldest_block.data.len();
            cache.remove(&oldest_id);
            self.current_size.fetch_sub(size, Ordering::SeqCst);
        }
    }
}

#[derive(Clone)]
pub struct ConnectionManager {
    master_addr: String,
    connection_pool: ConnectionPool,
    connection_stats: Arc<RwLock<ConnectionStats>>,
    block_cache: BlockCache,
}

#[derive(Default)]
struct ConnectionStats {
    total_requests: u64,
    failed_requests: u64,
    bytes_sent: u64,
    bytes_received: u64,
    last_error: Option<String>,
}

impl ConnectionManager {
    pub async fn connect(master_addr: &str) -> Result<Self, DfsError> {
        let connection_pool = ConnectionPool::new();

        let _test_stream = timeout(CONNECTION_TIMEOUT, TcpStream::connect(master_addr))
            .await
            .map_err(|_| DfsError::Network(format!("Cannot reach master at {}", master_addr)))?
            .map_err(|e| DfsError::Network(format!("Failed to connect to master: {}", e)))?;

        let manager = Self {
            master_addr: master_addr.to_string(),
            connection_pool,
            connection_stats: Arc::new(RwLock::new(ConnectionStats::default())),
            block_cache: BlockCache::new(100),
        };

        let pool = manager.connection_pool.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                pool.cleanup_stale_connections().await;
            }
        });

        Ok(manager)
    }

    async fn execute_with_retry<F, T>(&self, addr: &str, operation: F) -> Result<T, DfsError>
    where
        F: Fn(
            TcpStream,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(TcpStream, T), DfsError>> + Send>,
        >,
    {
        let mut last_error = None;

        for attempt in 1..=RETRY_ATTEMPTS {
            match self.connection_pool.get_connection(addr).await {
                Ok(stream) => match operation(stream).await {
                    Ok((stream, result)) => {
                        self.connection_pool.return_connection(addr, stream).await;
                        return Ok(result);
                    }
                    Err(e) => {
                        last_error = Some(e);
                        if attempt < RETRY_ATTEMPTS {
                            sleep(RETRY_DELAY * attempt).await;
                        }
                    }
                },
                Err(e) => {
                    last_error = Some(e);
                    if attempt < RETRY_ATTEMPTS {
                        sleep(RETRY_DELAY * attempt).await;
                    }
                }
            }
        }

        let mut stats = self.connection_stats.write().await;
        stats.failed_requests += 1;
        stats.last_error = Some(format!("Failed after {} attempts", RETRY_ATTEMPTS));

        Err(last_error.unwrap_or(DfsError::Unknown))
    }

    pub async fn lookup_metadata(&self, path: &str) -> Result<FileMetadata, DfsError> {
        self.execute_with_retry(&self.master_addr, |mut stream| {
            let path = path.to_string();
            Box::pin(async move {
                let request = Request::Lookup { path };
                send_request(&mut stream, &request).await?;
                let response: Response = recv_response(&mut stream).await?;

                match response {
                    Response::Metadata { metadata } => Ok((stream, metadata)),
                    Response::Error { message } => Err(DfsError::Network(message)),
                    _ => Err(DfsError::Unknown),
                }
            })
        })
        .await
    }

    pub async fn read_block(
        &self,
        metadata: &FileMetadata,
        block_id: u64,
    ) -> Result<Vec<u8>, DfsError> {
        if let Some(data) = self.block_cache.get(block_id).await {
            let mut stats = self.connection_stats.write().await;
            stats.total_requests += 1;
            stats.bytes_received += data.len() as u64;
            return Ok(data);
        }

        if metadata.nodes.is_empty() {
            return Err(DfsError::Network("No storage nodes available".into()));
        }

        let mut last_error = None;

        for node in &metadata.nodes {
            match self
                .execute_with_retry(&node.addr, |mut stream| {
                    Box::pin(async move {
                        let request = Request::ReadBlock { block_id };
                        send_request(&mut stream, &request).await?;
                        let response: Response = recv_response(&mut stream).await?;

                        match response {
                            Response::BlockData { data } => Ok((stream, data)),
                            Response::Error { message } => Err(DfsError::Network(message)),
                            _ => Err(DfsError::Unknown),
                        }
                    })
                })
                .await
            {
                Ok(raw) => {
                    let data = decompress(&raw)?;
                    self.block_cache.put(block_id, data.clone()).await;

                    let mut stats = self.connection_stats.write().await;
                    stats.total_requests += 1;
                    stats.bytes_received += data.len() as u64;
                    return Ok(data);
                }
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        let mut stats = self.connection_stats.write().await;
        stats.failed_requests += 1;
        stats.last_error = Some(format!(
            "Failed to read block {} from any replica",
            block_id
        ));

        Err(last_error.unwrap_or_else(|| {
            DfsError::Network(format!("Failed to read block {} from any node", block_id))
        }))
    }

    pub async fn write_block(
        &self,
        metadata: &FileMetadata,
        block_id: u64,
        data: &[u8],
    ) -> Result<(), DfsError> {
        if metadata.nodes.is_empty() {
            return Err(DfsError::Network("No storage nodes available".into()));
        }

        let mut successful_writes = 0;
        let required_writes = metadata.nodes.len().div_ceil(2);
        let mut errors = Vec::new();
        let mut successful_replicas = Vec::new();

        let compressed = compress(data);
        let checksum = checksum_hex(&compressed);

        let mut write_tasks = Vec::new();
        for node in &metadata.nodes {
            let node_id = node.id.clone();
            let node_addr = node.addr.clone();
            let data = compressed.clone();
            let pool = self.connection_pool.clone();

            let task = tokio::spawn(async move {
                match pool.get_connection(&node_addr).await {
                    Ok(mut stream) => {
                        let request = Request::WriteBlock {
                            block_id,
                            data: data.clone(),
                        };

                        if let Err(e) = send_request(&mut stream, &request).await {
                            return Err((node_addr, e));
                        }

                        match recv_response(&mut stream).await {
                            Ok(Response::Ok) => {
                                pool.return_connection(&node_addr, stream).await;
                                Ok(node_id)
                            }
                            Ok(Response::Error { message }) => {
                                Err((node_addr, DfsError::Network(message)))
                            }
                            Ok(_) => Err((node_addr, DfsError::Unknown)),
                            Err(e) => Err((node_addr, e)),
                        }
                    }
                    Err(e) => Err((node_addr, e)),
                }
            });

            write_tasks.push(task);
        }

        for task in write_tasks {
            match task.await {
                Ok(Ok(node_id)) => {
                    successful_writes += 1;
                    successful_replicas.push(node_id);
                }
                Ok(Err((addr, e))) => errors.push(format!("{}: {}", addr, e)),
                Err(e) => errors.push(format!("Task error: {}", e)),
            }
        }

        let mut stats = self.connection_stats.write().await;
        stats.total_requests += metadata.nodes.len() as u64;
        stats.bytes_sent += (compressed.len() * metadata.nodes.len()) as u64;

        if successful_writes >= required_writes {
            self.commit_block(
                &metadata.path,
                block_id,
                data.len() as u64,
                checksum,
                successful_replicas,
            )
            .await
        } else {
            stats.failed_requests += (metadata.nodes.len() - successful_writes) as u64;
            stats.last_error = Some(format!(
                "Insufficient replicas. Required: {}, Successful: {}. Errors: {:?}",
                required_writes, successful_writes, errors
            ));

            Err(DfsError::Network(format!(
                "Failed to write to sufficient replicas. Required: {}, Successful: {}",
                required_writes, successful_writes
            )))
        }
    }

    async fn commit_block(
        &self,
        path: &str,
        block_id: u64,
        size: u64,
        checksum: String,
        replicas: Vec<String>,
    ) -> Result<(), DfsError> {
        self.execute_with_retry(&self.master_addr, |mut stream| {
            let path = path.to_string();
            let checksum = checksum.clone();
            let replicas = replicas.clone();
            Box::pin(async move {
                let request = Request::CommitBlock {
                    path,
                    block_id,
                    size,
                    checksum,
                    replicas,
                };
                send_request(&mut stream, &request).await?;
                let response: Response = recv_response(&mut stream).await?;
                match response {
                    Response::Ok => Ok((stream, ())),
                    Response::Error { message } => Err(DfsError::Network(message)),
                    _ => Err(DfsError::Unknown),
                }
            })
        })
        .await
    }

    pub async fn create_file(&self, path: &str) -> Result<FileMetadata, DfsError> {
        self.execute_with_retry(&self.master_addr, |mut stream| {
            let path = path.to_string();
            Box::pin(async move {
                let request = Request::Create { path };
                send_request(&mut stream, &request).await?;
                let response: Response = recv_response(&mut stream).await?;

                match response {
                    Response::Metadata { metadata } => Ok((stream, metadata)),
                    Response::Error { message } => Err(DfsError::Network(message)),
                    _ => Err(DfsError::Unknown),
                }
            })
        })
        .await
    }

    pub async fn create_file_with_replication(
        &self,
        path: &str,
        replication_factor: usize,
    ) -> Result<FileMetadata, DfsError> {
        self.execute_with_retry(&self.master_addr, |mut stream| {
            let path = path.to_string();
            Box::pin(async move {
                let request = Request::CreateWithReplication {
                    path,
                    replication_factor,
                };
                send_request(&mut stream, &request).await?;
                let response: Response = recv_response(&mut stream).await?;

                match response {
                    Response::Metadata { metadata } => Ok((stream, metadata)),
                    Response::Error { message } => Err(DfsError::Network(message)),
                    _ => Err(DfsError::Unknown),
                }
            })
        })
        .await
    }

    pub async fn list_files(&self, path: &str) -> Result<Vec<FileInfo>, DfsError> {
        self.execute_with_retry(&self.master_addr, |mut stream| {
            let path = path.to_string();
            Box::pin(async move {
                let request = Request::List { path };
                send_request(&mut stream, &request).await?;
                let response: Response = recv_response(&mut stream).await?;

                match response {
                    Response::FileList { files } => Ok((stream, files)),
                    Response::Error { message } => Err(DfsError::Network(message)),
                    _ => Err(DfsError::Unknown),
                }
            })
        })
        .await
    }

    pub async fn rename_file(&self, from: &str, to: &str) -> Result<(), DfsError> {
        self.execute_with_retry(&self.master_addr, |mut stream| {
            let from = from.to_string();
            let to = to.to_string();
            Box::pin(async move {
                let request = Request::Rename { from, to };
                send_request(&mut stream, &request).await?;
                let response: Response = recv_response(&mut stream).await?;

                match response {
                    Response::Ok => Ok((stream, ())),
                    Response::Error { message } => Err(DfsError::Network(message)),
                    _ => Err(DfsError::Unknown),
                }
            })
        })
        .await
    }

    pub async fn delete_file(&self, path: &str) -> Result<(), DfsError> {
        self.execute_with_retry(&self.master_addr, |mut stream| {
            let path = path.to_string();
            Box::pin(async move {
                let request = Request::Delete { path };
                send_request(&mut stream, &request).await?;
                let response: Response = recv_response(&mut stream).await?;

                match response {
                    Response::Ok => Ok((stream, ())),
                    Response::Error { message } => Err(DfsError::Network(message)),
                    _ => Err(DfsError::Unknown),
                }
            })
        })
        .await
    }

    pub async fn get_stats(&self) -> ConnectionStatsSnapshot {
        let stats = self.connection_stats.read().await;
        ConnectionStatsSnapshot {
            total_requests: stats.total_requests,
            failed_requests: stats.failed_requests,
            bytes_sent: stats.bytes_sent,
            bytes_received: stats.bytes_received,
            last_error: stats.last_error.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionStatsSnapshot {
    pub total_requests: u64,
    pub failed_requests: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Lookup {
        path: String,
    },
    Create {
        path: String,
    },
    CreateWithReplication {
        path: String,
        replication_factor: usize,
    },
    List {
        path: String,
    },
    ReadBlock {
        block_id: u64,
    },
    WriteBlock {
        block_id: u64,
        data: Vec<u8>,
    },
    CommitBlock {
        path: String,
        block_id: u64,
        size: u64,
        checksum: String,
        replicas: Vec<String>,
    },
    Delete {
        path: String,
    },
    Rename {
        from: String,
        to: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ok,
    Error { message: String },
    Metadata { metadata: FileMetadata },
    BlockData { data: Vec<u8> },
    FileList { files: Vec<FileInfo> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub path: String,
    pub size: u64,
    pub is_dir: bool,
    pub created_at: u64,
    pub modified_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub path: String,
    pub size: u64,
    pub blocks: Vec<u64>,
    pub nodes: Vec<StorageNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageNode {
    pub id: String,
    pub addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub token: Option<String>,
    pub payload: Request,
}

pub fn auth_token() -> Option<String> {
    std::env::var("RDFS_AUTH_TOKEN").ok()
}

pub fn compress(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(data).expect("compression write failed");
    encoder.finish().expect("compression finish failed")
}

pub fn decompress(data: &[u8]) -> Result<Vec<u8>, DfsError> {
    let mut decoder = GzDecoder::new(data);
    let mut buf = Vec::new();
    decoder
        .read_to_end(&mut buf)
        .map_err(|e| DfsError::Network(format!("decompression failed: {e}")))?;
    Ok(buf)
}

fn checksum_hex(data: &[u8]) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in data {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", hash)
}

async fn send_request(stream: &mut TcpStream, req: &Request) -> Result<(), DfsError> {
    let envelope = Envelope {
        token: auth_token(),
        payload: req.clone(),
    };
    let msg = serde_json::to_vec(&envelope).map_err(|e| DfsError::Network(e.to_string()))?;
    let len = (msg.len() as u32).to_be_bytes();

    stream.write_all(&len).await?;
    stream.write_all(&msg).await?;
    stream.flush().await?;
    Ok(())
}

async fn recv_response(stream: &mut TcpStream) -> Result<Response, DfsError> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > 100 * 1024 * 1024 {
        return Err(DfsError::Network(format!(
            "Response too large: {} bytes",
            len
        )));
    }

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;

    let resp: Response =
        serde_json::from_slice(&buf).map_err(|e| DfsError::Network(e.to_string()))?;
    Ok(resp)
}
