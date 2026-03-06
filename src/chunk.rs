use crate::model::ChunkInventoryEntry;
use crate::pb;
use crate::util::checksum_hex;
use anyhow::{Result, bail};
use async_stream::try_stream;
use rocksdb::{DB, Options};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::{RwLock, watch};
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

const CHUNK_KEY_PREFIX: &str = "chunk/";
const STREAM_CHUNK_SIZE: usize = 64 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalChunkRecord {
    chunk_id: String,
    version: u64,
    checksum: String,
    size: u64,
    file_name: String,
}

pub struct ChunkStore {
    data_dir: PathBuf,
    db: Arc<DB>,
    chunks: RwLock<BTreeMap<String, LocalChunkRecord>>,
    capacity: u64,
}

impl ChunkStore {
    pub async fn open(
        _node_id: String,
        data_dir: impl AsRef<Path>,
        capacity: u64,
    ) -> Result<Arc<Self>> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(data_dir.join("chunks")).await?;
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = Arc::new(DB::open(&options, data_dir.join("rocksdb"))?);
        let mut chunks = BTreeMap::new();
        for item in db.iterator(rocksdb::IteratorMode::Start) {
            let (key, value) = item?;
            let Ok(key) = std::str::from_utf8(&key) else {
                continue;
            };
            if !key.starts_with(CHUNK_KEY_PREFIX) {
                continue;
            }
            let chunk: LocalChunkRecord = serde_json::from_slice(&value)?;
            chunks.insert(chunk.chunk_id.clone(), chunk);
        }
        Ok(Arc::new(Self {
            data_dir,
            db,
            chunks: RwLock::new(chunks),
            capacity,
        }))
    }

    pub async fn put_chunk(
        &self,
        chunk_id: &str,
        version: u64,
        checksum: &str,
        data: &[u8],
    ) -> Result<()> {
        let calculated = checksum_hex(data);
        if calculated != checksum {
            bail!("checksum mismatch");
        }

        let file_name = format!("{chunk_id}-{version}.chunk");
        let path = self.data_dir.join("chunks").join(&file_name);
        let tmp_path = self
            .data_dir
            .join("chunks")
            .join(format!("{file_name}.tmp"));

        let projected = self.used().await + data.len() as u64;
        if projected > self.capacity {
            bail!("capacity exceeded");
        }

        let mut file = fs::File::create(&tmp_path).await?;
        file.write_all(data).await?;
        file.sync_data().await?;
        drop(file);
        fs::rename(&tmp_path, &path).await?;

        let record = LocalChunkRecord {
            chunk_id: chunk_id.to_string(),
            version,
            checksum: checksum.to_string(),
            size: data.len() as u64,
            file_name,
        };
        self.db
            .put(
                chunk_key(chunk_id),
                serde_json::to_vec(&record).map_err(|e| anyhow::anyhow!(e))?,
            )
            .map_err(|e| anyhow::anyhow!(e))?;
        self.chunks
            .write()
            .await
            .insert(chunk_id.to_string(), record);
        Ok(())
    }

    async fn get_chunk(&self, chunk_id: &str, version: u64) -> Result<(LocalChunkRecord, Vec<u8>)> {
        let record = self
            .chunks
            .read()
            .await
            .get(chunk_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("chunk not found"))?;
        if record.version != version {
            bail!("chunk version mismatch");
        }
        let path = self.data_dir.join("chunks").join(&record.file_name);
        let data = fs::read(path).await?;
        if checksum_hex(&data) != record.checksum {
            bail!("chunk checksum mismatch");
        }
        Ok((record, data))
    }

    pub async fn delete_chunk(&self, chunk_id: &str, version: u64) -> Result<()> {
        let record = self
            .chunks
            .read()
            .await
            .get(chunk_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("chunk not found"))?;
        if record.version != version {
            bail!("chunk version mismatch");
        }
        let path = self.data_dir.join("chunks").join(&record.file_name);
        let _ = fs::remove_file(path).await;
        self.db.delete(chunk_key(chunk_id))?;
        self.chunks.write().await.remove(chunk_id);
        Ok(())
    }

    pub async fn used(&self) -> u64 {
        self.chunks
            .read()
            .await
            .values()
            .map(|chunk| chunk.size)
            .sum()
    }

    pub async fn inventory(&self) -> Vec<ChunkInventoryEntry> {
        self.chunks
            .read()
            .await
            .values()
            .map(|chunk| ChunkInventoryEntry {
                chunk_id: chunk.chunk_id.clone(),
                version: chunk.version,
                checksum: chunk.checksum.clone(),
                size: chunk.size,
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct ChunkServerConfig {
    pub node_id: String,
    pub addr: String,
    pub data_dir: PathBuf,
    pub metadata_addrs: Vec<String>,
    pub capacity: u64,
}

#[derive(Clone)]
pub struct ChunkServer {
    inner: Arc<ChunkServerInner>,
}

struct ChunkServerInner {
    node_id: String,
    addr: String,
    metadata_addrs: Vec<String>,
    store: Arc<ChunkStore>,
    capacity: u64,
    shutdown: watch::Sender<bool>,
}

impl ChunkServer {
    pub async fn open(config: ChunkServerConfig) -> Result<Self> {
        let store =
            ChunkStore::open(config.node_id.clone(), config.data_dir, config.capacity).await?;
        let (shutdown, _) = watch::channel(false);
        Ok(Self {
            inner: Arc::new(ChunkServerInner {
                node_id: config.node_id,
                addr: config.addr,
                metadata_addrs: config.metadata_addrs,
                store,
                capacity: config.capacity,
                shutdown,
            }),
        })
    }

    pub fn node_id(&self) -> &str {
        &self.inner.node_id
    }

    pub fn addr(&self) -> &str {
        &self.inner.addr
    }

    pub async fn serve(self) -> Result<()> {
        let addr = self.inner.addr.parse()?;
        let mut shutdown = self.inner.shutdown.subscribe();
        tokio::spawn(heartbeat_loop(self.clone()));
        Server::builder()
            .add_service(pb::chunk_service_server::ChunkServiceServer::new(
                ChunkGrpc {
                    server: self.clone(),
                },
            ))
            .serve_with_shutdown(addr, async move {
                let _ = shutdown.changed().await;
            })
            .await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.inner.shutdown.send(true);
        Ok(())
    }
}

#[derive(Clone)]
struct ChunkGrpc {
    server: ChunkServer,
}

type ChunkStream = Pin<
    Box<dyn futures_util::Stream<Item = Result<pb::GetChunkResponse, Status>> + Send + 'static>,
>;

#[tonic::async_trait]
impl pb::chunk_service_server::ChunkService for ChunkGrpc {
    async fn put_chunk(
        &self,
        request: Request<tonic::Streaming<pb::PutChunkRequest>>,
    ) -> Result<Response<pb::PutChunkResponse>, Status> {
        let mut stream = request.into_inner();
        let mut header = None;
        let mut data = Vec::new();

        while let Some(message) = stream.message().await? {
            match message.item {
                Some(pb::put_chunk_request::Item::Header(h)) => header = Some(h),
                Some(pb::put_chunk_request::Item::Data(chunk)) => data.extend_from_slice(&chunk),
                None => {}
            }
        }

        let Some(header) = header else {
            return Err(Status::invalid_argument("missing chunk header"));
        };
        self.server
            .inner
            .store
            .put_chunk(&header.chunk_id, header.version, &header.checksum, &data)
            .await
            .map_err(internal_status)?;

        for replica in &header.forward_targets {
            replicate_to_peer(
                replica,
                &header.chunk_id,
                header.version,
                &header.checksum,
                data.clone(),
            )
            .await
            .map_err(internal_status)?;
        }

        Ok(Response::new(pb::PutChunkResponse {
            replica: Some(pb::ChunkReplica {
                node_id: self.server.inner.node_id.clone(),
                addr: self.server.inner.addr.clone(),
                version: header.version,
            }),
        }))
    }

    type GetChunkStream = ChunkStream;

    async fn get_chunk(
        &self,
        request: Request<pb::GetChunkRequest>,
    ) -> Result<Response<Self::GetChunkStream>, Status> {
        let request = request.into_inner();
        let (record, data) = self
            .server
            .inner
            .store
            .get_chunk(&request.chunk_id, request.version)
            .await
            .map_err(internal_status)?;
        let node_id = self.server.inner.node_id.clone();
        let stream = try_stream! {
            yield pb::GetChunkResponse {
                item: Some(pb::get_chunk_response::Item::Metadata(pb::ChunkMetadata {
                    chunk_id: record.chunk_id.clone(),
                    version: record.version,
                    checksum: record.checksum.clone(),
                    size: record.size,
                    node_id,
                })),
            };
            for chunk in data.chunks(STREAM_CHUNK_SIZE) {
                yield pb::GetChunkResponse {
                    item: Some(pb::get_chunk_response::Item::Data(chunk.to_vec())),
                };
            }
        };
        Ok(Response::new(Box::pin(stream) as Self::GetChunkStream))
    }

    async fn replicate_chunk(
        &self,
        request: Request<pb::ReplicateChunkRequest>,
    ) -> Result<Response<pb::PutChunkResponse>, Status> {
        let request = request.into_inner();
        self.server
            .inner
            .store
            .put_chunk(
                &request.chunk_id,
                request.version,
                &request.checksum,
                &request.data,
            )
            .await
            .map_err(internal_status)?;
        Ok(Response::new(pb::PutChunkResponse {
            replica: Some(pb::ChunkReplica {
                node_id: self.server.inner.node_id.clone(),
                addr: self.server.inner.addr.clone(),
                version: request.version,
            }),
        }))
    }

    async fn delete_chunk(
        &self,
        request: Request<pb::DeleteChunkRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        let request = request.into_inner();
        self.server
            .inner
            .store
            .delete_chunk(&request.chunk_id, request.version)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn report_inventory(
        &self,
        _request: Request<pb::ReportInventoryRequest>,
    ) -> Result<Response<pb::ReportInventoryResponse>, Status> {
        let inventory = self.server.inner.store.inventory().await;
        Ok(Response::new(pb::ReportInventoryResponse {
            node_id: self.server.inner.node_id.clone(),
            capacity: self.server.inner.capacity,
            used: self.server.inner.store.used().await,
            inventory: inventory
                .into_iter()
                .map(|chunk| pb::InventoryChunk {
                    chunk_id: chunk.chunk_id,
                    version: chunk.version,
                    checksum: chunk.checksum,
                    size: chunk.size,
                })
                .collect(),
        }))
    }
}

async fn heartbeat_loop(server: ChunkServer) {
    let mut leader_hint = None::<String>;
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    let mut shutdown = server.inner.shutdown.subscribe();

    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = interval.tick() => {}
        }
        let inventory = server
            .inner
            .store
            .inventory()
            .await
            .into_iter()
            .map(|chunk| pb::InventoryChunk {
                chunk_id: chunk.chunk_id,
                version: chunk.version,
                checksum: chunk.checksum,
                size: chunk.size,
            })
            .collect();
        let request = pb::HeartbeatRequest {
            node_id: server.inner.node_id.clone(),
            addr: server.inner.addr.clone(),
            capacity: server.inner.capacity,
            used: server.inner.store.used().await,
            inventory,
        };

        if let Some(addr) = leader_hint.clone()
            && send_heartbeat(&addr, request.clone()).await.is_ok()
        {
            continue;
        }

        leader_hint = None;
        for addr in &server.inner.metadata_addrs {
            match send_heartbeat(addr, request.clone()).await {
                Ok(Some(leader)) => leader_hint = Some(leader),
                Ok(None) => {
                    leader_hint = Some(addr.clone());
                    break;
                }
                Err(err) => {
                    if let Some(leader) = status_leader_hint(&err) {
                        leader_hint = Some(leader);
                    }
                }
            }
        }
    }
}

async fn send_heartbeat(
    addr: &str,
    request: pb::HeartbeatRequest,
) -> Result<Option<String>, Status> {
    let mut client = metadata_client(addr)
        .await
        .map_err(|e| Status::unavailable(e.to_string()))?;
    match client.heartbeat(request).await {
        Ok(_) => Ok(None),
        Err(status) => Err(status),
    }
}

async fn replicate_to_peer(
    replica: &pb::ChunkReplica,
    chunk_id: &str,
    version: u64,
    checksum: &str,
    data: Vec<u8>,
) -> Result<()> {
    let mut client = chunk_client(&replica.addr).await?;
    client
        .replicate_chunk(pb::ReplicateChunkRequest {
            chunk_id: chunk_id.to_string(),
            version,
            checksum: checksum.to_string(),
            data,
        })
        .await?;
    Ok(())
}

async fn chunk_client(
    addr: &str,
) -> Result<pb::chunk_service_client::ChunkServiceClient<Channel>, tonic::transport::Error> {
    pb::chunk_service_client::ChunkServiceClient::connect(format!("http://{addr}")).await
}

async fn metadata_client(
    addr: &str,
) -> Result<pb::metadata_service_client::MetadataServiceClient<Channel>, tonic::transport::Error> {
    pb::metadata_service_client::MetadataServiceClient::connect(format!("http://{addr}")).await
}

fn status_leader_hint(status: &Status) -> Option<String> {
    status.message().strip_prefix("leader=").and_then(|msg| {
        let msg = msg.trim();
        if msg.is_empty() {
            None
        } else {
            Some(msg.to_string())
        }
    })
}

fn chunk_key(chunk_id: &str) -> String {
    format!("{CHUNK_KEY_PREFIX}{chunk_id}")
}

fn internal_status(error: impl ToString) -> Status {
    Status::internal(error.to_string())
}
