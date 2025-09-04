use crate::client::error::DfsError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Mutex, RwLock},
    time::{sleep, timeout},
};

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_ATTEMPTS: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_millis(500);

#[derive(Clone)]
struct ConnectionPool {
    connections: Arc<RwLock<HashMap<String, Arc<Mutex<Option<TcpStream>>>>>>,
    last_used: Arc<RwLock<HashMap<String, u64>>>,
}

impl ConnectionPool {
    fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            last_used: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn get_connection(&self, addr: &str) -> Result<TcpStream, DfsError> {
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
                    return Ok(conn);
                }
            }
        }

        let stream = timeout(CONNECTION_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| DfsError::Network(format!("Connection timeout to {}", addr)))?
            .map_err(|e| DfsError::Network(format!("Failed to connect to {}: {}", addr, e)))?;

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

        Ok(stream)
    }

    async fn return_connection(&self, addr: &str, conn: TcpStream) {
        let connections = self.connections.read().await;
        if let Some(conn_mutex) = connections.get(addr) {
            let mut conn_opt = conn_mutex.lock().await;
            *conn_opt = Some(conn);
        }
    }

    async fn cleanup_stale_connections(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let last_used = self.last_used.read().await;
        let stale_addrs: Vec<String> = last_used
            .iter()
            .filter(|(_, &last)| now - last > 60)
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
pub struct ConnectionManager {
    master_addr: String,
    connection_pool: ConnectionPool,
    connection_stats: Arc<RwLock<ConnectionStats>>,
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

    async fn execute_with_retry<F, T>(
        &self,
        addr: &str,
        operation: F,
    ) -> Result<T, DfsError>
    where
        F: Fn(TcpStream) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(TcpStream, T), DfsError>> + Send>>,
    {
        let mut last_error = None;

        for attempt in 1..=RETRY_ATTEMPTS {
            match self.connection_pool.get_connection(addr).await {
                Ok(stream) => {
                    match operation(stream).await {
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
                    }
                }
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

        Err(last_error.unwrap_or_else(|| DfsError::Unknown))
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
                Ok(data) => {
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
        let required_writes = (metadata.nodes.len() + 1) / 2;
        let mut errors = Vec::new();

        let mut write_tasks = Vec::new();
        for node in &metadata.nodes {
            let node_addr = node.addr.clone();
            let data = data.to_vec();
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
                                Ok(node_addr)
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
                Ok(Ok(_)) => successful_writes += 1,
                Ok(Err((addr, e))) => errors.push(format!("{}: {}", addr, e)),
                Err(e) => errors.push(format!("Task error: {}", e)),
            }
        }

        let mut stats = self.connection_stats.write().await;
        stats.total_requests += metadata.nodes.len() as u64;
        stats.bytes_sent += (data.len() * metadata.nodes.len()) as u64;

        if successful_writes >= required_writes {
            Ok(())
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

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Lookup { path: String },
    ReadBlock { block_id: u64 },
    WriteBlock { block_id: u64, data: Vec<u8> },
    Delete { path: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ok,
    Error { message: String },
    Metadata { metadata: FileMetadata },
    BlockData { data: Vec<u8> },
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

async fn send_request(stream: &mut TcpStream, req: &Request) -> Result<(), DfsError> {
    let msg = serde_json::to_vec(req).map_err(|e| DfsError::Network(e.to_string()))?;
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
        // Sanity check: 100MB max
        return Err(DfsError::Network(format!("Response too large: {} bytes", len)));
    }

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    let resp: Response =
        serde_json::from_slice(&buf).map_err(|e| DfsError::Network(e.to_string()))?;
    Ok(resp)
}
