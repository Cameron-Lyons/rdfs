use crate::model::ChunkInventoryEntry;
use crate::net::ChannelCache;
use crate::pb;
use crate::util::digest_hex;
use anyhow::{Context, Result, bail};
use futures_util::{StreamExt, stream};
use rocksdb::{DB, Options};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{RwLock, mpsc, watch};
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

const CHUNK_KEY_PREFIX: &str = "chunk/";
/// Size of individual data frames on streaming chunk transfers. Frames must
/// stay well under tonic's 4 MiB message limit while chunks themselves can be
/// arbitrarily large.
pub const TRANSFER_FRAME_SIZE: usize = 256 * 1024;
/// Depth of the forwarding queue between an incoming chunk stream and the
/// downstream replica, in frames.
const FORWARD_QUEUE_FRAMES: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalChunkRecord {
    chunk_id: String,
    checksum: String,
    size: u64,
    file_name: String,
}

pub struct ChunkStore {
    data_dir: PathBuf,
    db: Arc<DB>,
    chunks: RwLock<BTreeMap<String, LocalChunkRecord>>,
    capacity: u64,
    /// Bytes stored plus bytes reserved by in-flight writes. Kept as a
    /// counter so capacity checks and heartbeats do not scan every record.
    used_bytes: AtomicU64,
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
        let used_bytes = chunks.values().map(|chunk| chunk.size).sum();
        Ok(Arc::new(Self {
            data_dir,
            db,
            chunks: RwLock::new(chunks),
            capacity,
            used_bytes: AtomicU64::new(used_bytes),
        }))
    }

    /// Starts writing a chunk of a declared size and checksum. Data is
    /// appended incrementally and hashed as it arrives; the chunk becomes
    /// visible atomically once [`ChunkWriter::finish`] verifies it.
    pub async fn begin_write(
        self: &Arc<Self>,
        chunk_id: &str,
        checksum: &str,
        size: u64,
    ) -> Result<ChunkWriter> {
        self.reserve(size)?;
        let file_name = format!("{chunk_id}.chunk");
        let path = self.data_dir.join("chunks").join(&file_name);
        let tmp_path = self
            .data_dir
            .join("chunks")
            .join(format!("{file_name}.tmp"));
        let file = match fs::File::create(&tmp_path).await {
            Ok(file) => file,
            Err(error) => {
                self.release(size);
                return Err(error.into());
            }
        };

        Ok(ChunkWriter {
            store: self.clone(),
            chunk_id: chunk_id.to_string(),
            expected_checksum: checksum.to_string(),
            expected_size: size,
            file: Some(file),
            tmp_path,
            path,
            file_name,
            hasher: Sha256::new(),
            written: 0,
            finished: false,
        })
    }

    /// Reserves `size` bytes against capacity, failing when the store is full.
    fn reserve(&self, size: u64) -> Result<()> {
        let mut current = self.used_bytes.load(Ordering::Relaxed);
        loop {
            let projected = current.saturating_add(size);
            if projected > self.capacity {
                bail!("capacity exceeded");
            }
            match self.used_bytes.compare_exchange_weak(
                current,
                projected,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }

    fn release(&self, size: u64) {
        self.used_bytes.fetch_sub(size, Ordering::Relaxed);
    }

    async fn record(&self, chunk_id: &str) -> Option<LocalChunkRecord> {
        self.chunks.read().await.get(chunk_id).cloned()
    }

    fn chunk_path(&self, record: &LocalChunkRecord) -> PathBuf {
        self.data_dir.join("chunks").join(&record.file_name)
    }

    pub async fn delete_chunk(&self, chunk_id: &str) -> Result<()> {
        let record = self
            .chunks
            .read()
            .await
            .get(chunk_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("chunk not found"))?;
        let path = self.chunk_path(&record);
        let _ = fs::remove_file(path).await;
        self.db.delete(chunk_key(chunk_id))?;
        if self.chunks.write().await.remove(chunk_id).is_some() {
            self.release(record.size);
        }
        Ok(())
    }

    pub fn used(&self) -> u64 {
        self.used_bytes.load(Ordering::Relaxed)
    }

    pub async fn inventory(&self) -> Vec<ChunkInventoryEntry> {
        self.chunks
            .read()
            .await
            .values()
            .map(|chunk| ChunkInventoryEntry {
                chunk_id: chunk.chunk_id.clone(),
                checksum: chunk.checksum.clone(),
                size: chunk.size,
            })
            .collect()
    }
}

/// In-progress write of a single chunk to a temporary file. Dropping the
/// writer without finishing removes the temporary file.
pub struct ChunkWriter {
    store: Arc<ChunkStore>,
    chunk_id: String,
    expected_checksum: String,
    expected_size: u64,
    file: Option<fs::File>,
    tmp_path: PathBuf,
    path: PathBuf,
    file_name: String,
    hasher: Sha256,
    written: u64,
    finished: bool,
}

impl ChunkWriter {
    pub async fn append(&mut self, data: &[u8]) -> Result<()> {
        if self.written + data.len() as u64 > self.expected_size {
            bail!("chunk data exceeds declared size");
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("chunk writer already finished"))?;
        file.write_all(data).await?;
        self.hasher.update(data);
        self.written += data.len() as u64;
        Ok(())
    }

    pub async fn finish(mut self) -> Result<()> {
        if self.written != self.expected_size {
            bail!(
                "chunk size mismatch: expected {} bytes, received {}",
                self.expected_size,
                self.written
            );
        }
        let calculated = digest_hex(std::mem::take(&mut self.hasher));
        if calculated != self.expected_checksum {
            bail!("checksum mismatch");
        }

        let file = self
            .file
            .take()
            .ok_or_else(|| anyhow::anyhow!("chunk writer already finished"))?;
        file.sync_data().await?;
        drop(file);
        fs::rename(&self.tmp_path, &self.path).await?;

        let record = LocalChunkRecord {
            chunk_id: self.chunk_id.clone(),
            checksum: self.expected_checksum.clone(),
            size: self.expected_size,
            file_name: self.file_name.clone(),
        };
        self.store
            .db
            .put(chunk_key(&self.chunk_id), serde_json::to_vec(&record)?)?;
        let previous = self
            .store
            .chunks
            .write()
            .await
            .insert(self.chunk_id.clone(), record);
        if let Some(previous) = previous {
            self.store.release(previous.size);
        }
        self.finished = true;
        Ok(())
    }
}

impl Drop for ChunkWriter {
    fn drop(&mut self) {
        if !self.finished {
            self.file.take();
            let _ = std::fs::remove_file(&self.tmp_path);
            self.store.release(self.expected_size);
        }
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
    channels: Arc<ChannelCache>,
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
                channels: Arc::new(ChannelCache::default()),
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

/// Uploads one chunk over `channel` as a framed `PutChunk` stream. The
/// receiving server forwards the frames to `header.forward_targets` while it
/// writes.
pub async fn send_chunk(
    channel: Channel,
    header: pb::PutChunkHeader,
    data: Vec<u8>,
) -> Result<pb::PutChunkResponse> {
    let mut client = pb::chunk_service_client::ChunkServiceClient::new(channel);
    let header_message = pb::PutChunkRequest {
        item: Some(pb::put_chunk_request::Item::Header(header)),
    };
    // Frames are cut from the chunk lazily so only one frame is duplicated
    // at a time instead of a second copy of the whole chunk.
    let frames = stream::unfold((data, 0usize), |(data, start)| async move {
        if start >= data.len() {
            return None;
        }
        let end = data.len().min(start + TRANSFER_FRAME_SIZE);
        let frame = pb::PutChunkRequest {
            item: Some(pb::put_chunk_request::Item::Data(data[start..end].to_vec())),
        };
        Some((frame, (data, end)))
    });
    let request = stream::once(async move { header_message }).chain(frames);
    let response = client.put_chunk(request).await?;
    Ok(response.into_inner())
}

/// Streams incoming chunk frames onward to the next replica in the chain
/// while the local server writes them, so replication is pipelined instead of
/// store-and-forward.
struct ChunkForwarder {
    tx: mpsc::Sender<pb::PutChunkRequest>,
    task: JoinHandle<Result<()>>,
}

impl ChunkForwarder {
    async fn open(
        channel: Channel,
        target: &pb::ChunkReplica,
        header: &pb::PutChunkHeader,
        remaining_targets: &[pb::ChunkReplica],
    ) -> Result<Self> {
        let mut client = pb::chunk_service_client::ChunkServiceClient::new(channel);
        let (tx, rx) = mpsc::channel::<pb::PutChunkRequest>(FORWARD_QUEUE_FRAMES);
        let forwarded_header = pb::PutChunkHeader {
            chunk_id: header.chunk_id.clone(),
            checksum: header.checksum.clone(),
            size: header.size,
            forward_targets: remaining_targets.to_vec(),
        };
        tx.send(pb::PutChunkRequest {
            item: Some(pb::put_chunk_request::Item::Header(forwarded_header)),
        })
        .await
        .map_err(|_| anyhow::anyhow!("forward stream closed before header"))?;

        let addr = target.addr.clone();
        let task = tokio::spawn(async move {
            let request = stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|item| (item, rx))
            });
            client
                .put_chunk(request)
                .await
                .with_context(|| format!("replica {addr} rejected chunk"))?;
            Ok(())
        });
        Ok(Self { tx, task })
    }

    async fn send(&mut self, frame: Vec<u8>) -> Result<()> {
        self.tx
            .send(pb::PutChunkRequest {
                item: Some(pb::put_chunk_request::Item::Data(frame)),
            })
            .await
            .map_err(|_| anyhow::anyhow!("replica closed forward stream"))
    }

    /// Closes the forward stream and waits for the downstream replica chain
    /// to acknowledge the chunk. If the forwarder is dropped without calling
    /// this, closing the stream makes the downstream server reject the
    /// truncated chunk on its own.
    async fn finish(self) -> Result<()> {
        let Self { tx, task } = self;
        drop(tx);
        task.await??;
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
        let header = match stream.message().await? {
            Some(pb::PutChunkRequest {
                item: Some(pb::put_chunk_request::Item::Header(header)),
            }) => header,
            _ => {
                return Err(Status::invalid_argument(
                    "first message must be the chunk header",
                ));
            }
        };

        let mut writer = self
            .server
            .inner
            .store
            .begin_write(&header.chunk_id, &header.checksum, header.size)
            .await
            .map_err(internal_status)?;
        let mut forwarder = match header.forward_targets.split_first() {
            Some((next, rest)) => {
                let channel = self
                    .server
                    .inner
                    .channels
                    .get(&next.addr)
                    .map_err(internal_status)?;
                Some(
                    ChunkForwarder::open(channel, next, &header, rest)
                        .await
                        .map_err(internal_status)?,
                )
            }
            None => None,
        };

        while let Some(message) = stream.message().await? {
            match message.item {
                Some(pb::put_chunk_request::Item::Data(frame)) => {
                    writer.append(&frame).await.map_err(internal_status)?;
                    if let Some(forwarder) = forwarder.as_mut() {
                        forwarder.send(frame).await.map_err(internal_status)?;
                    }
                }
                Some(pb::put_chunk_request::Item::Header(_)) => {
                    return Err(Status::invalid_argument("duplicate chunk header"));
                }
                None => {}
            }
        }

        if let Some(forwarder) = forwarder.take() {
            forwarder.finish().await.map_err(internal_status)?;
        }
        writer.finish().await.map_err(internal_status)?;

        Ok(Response::new(pb::PutChunkResponse {
            replica: Some(pb::ChunkReplica {
                node_id: self.server.inner.node_id.clone(),
                addr: self.server.inner.addr.clone(),
            }),
        }))
    }

    type GetChunkStream = ChunkStream;

    async fn get_chunk(
        &self,
        request: Request<pb::GetChunkRequest>,
    ) -> Result<Response<Self::GetChunkStream>, Status> {
        let request = request.into_inner();
        let store = &self.server.inner.store;
        let record = store
            .record(&request.chunk_id)
            .await
            .ok_or_else(|| Status::not_found("chunk not found"))?;
        let file = fs::File::open(store.chunk_path(&record))
            .await
            .map_err(internal_status)?;

        let metadata = pb::GetChunkResponse {
            item: Some(pb::get_chunk_response::Item::Metadata(pb::ChunkMetadata {
                chunk_id: record.chunk_id.clone(),
                checksum: record.checksum.clone(),
                size: record.size,
                node_id: self.server.inner.node_id.clone(),
            })),
        };

        // The chunk is streamed straight from disk and hashed as it goes; a
        // trailing error is emitted if the bytes no longer match the recorded
        // checksum so readers fail over to another replica.
        let state = GetChunkState {
            file,
            hasher: Sha256::new(),
            checksum: record.checksum,
            done: false,
        };
        let data_stream = stream::unfold(state, |mut state| async move {
            if state.done {
                return None;
            }
            let mut buffer = vec![0u8; TRANSFER_FRAME_SIZE];
            match state.file.read(&mut buffer).await {
                Ok(0) => {
                    state.done = true;
                    let calculated = digest_hex(std::mem::take(&mut state.hasher));
                    if calculated == state.checksum {
                        None
                    } else {
                        Some((Err(Status::data_loss("chunk checksum mismatch")), state))
                    }
                }
                Ok(read) => {
                    buffer.truncate(read);
                    state.hasher.update(&buffer);
                    let response = pb::GetChunkResponse {
                        item: Some(pb::get_chunk_response::Item::Data(buffer)),
                    };
                    Some((Ok(response), state))
                }
                Err(error) => {
                    state.done = true;
                    Some((Err(Status::data_loss(error.to_string())), state))
                }
            }
        });
        let stream = stream::once(async move { Ok::<_, Status>(metadata) }).chain(data_stream);
        Ok(Response::new(Box::pin(stream) as Self::GetChunkStream))
    }

    async fn delete_chunk(
        &self,
        request: Request<pb::DeleteChunkRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        let request = request.into_inner();
        self.server
            .inner
            .store
            .delete_chunk(&request.chunk_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(pb::Empty {}))
    }
}

struct GetChunkState {
    file: fs::File,
    hasher: Sha256,
    checksum: String,
    done: bool,
}

async fn heartbeat_loop(server: ChunkServer) {
    let mut leader_hint = None::<String>;
    // The `(leader addr, digest)` pair acknowledged by the last successful
    // heartbeat. While it matches, heartbeats carry liveness only; the full
    // inventory is sent when it changes, when the leader moves, or when the
    // leader asks for a resend.
    let mut acked = None::<(String, String)>;
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    let mut shutdown = server.inner.shutdown.subscribe();

    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = interval.tick() => {}
        }
        let inventory = server.inner.store.inventory().await;
        let digest = inventory_digest(&inventory);

        let mut targets = Vec::new();
        if let Some(leader) = leader_hint.clone() {
            targets.push(leader);
        }
        for addr in &server.inner.metadata_addrs {
            if !targets.contains(addr) {
                targets.push(addr.clone());
            }
        }

        let mut delivered = false;
        for addr in targets {
            let include_inventory = acked.as_ref() != Some(&(addr.clone(), digest.clone()));
            let request = pb::HeartbeatRequest {
                node_id: server.inner.node_id.clone(),
                addr: server.inner.addr.clone(),
                capacity: server.inner.capacity,
                used: server.inner.store.used(),
                inventory: if include_inventory {
                    inventory
                        .iter()
                        .map(|chunk| pb::InventoryChunk {
                            chunk_id: chunk.chunk_id.clone(),
                            checksum: chunk.checksum.clone(),
                            size: chunk.size,
                        })
                        .collect()
                } else {
                    Vec::new()
                },
                inventory_digest: digest.clone(),
                has_inventory: include_inventory,
            };

            match send_heartbeat(&server, &addr, request).await {
                Ok(response) => {
                    leader_hint = Some(addr.clone());
                    acked = if response.resend_inventory {
                        None
                    } else {
                        Some((addr, digest.clone()))
                    };
                    delivered = true;
                    break;
                }
                Err(status) => {
                    if let Some(leader) = status_leader_hint(&status) {
                        leader_hint = Some(leader);
                    }
                }
            }
        }
        if !delivered {
            leader_hint = None;
        }
    }
}

/// Digest of a chunkserver's full inventory, stable across heartbeats as
/// long as the set of stored chunks is unchanged.
fn inventory_digest(inventory: &[ChunkInventoryEntry]) -> String {
    let mut hasher = Sha256::new();
    for chunk in inventory {
        hasher.update(chunk.chunk_id.as_bytes());
        hasher.update([0]);
        hasher.update(chunk.checksum.as_bytes());
        hasher.update([0]);
        hasher.update(chunk.size.to_le_bytes());
    }
    digest_hex(hasher)
}

async fn send_heartbeat(
    server: &ChunkServer,
    addr: &str,
    request: pb::HeartbeatRequest,
) -> Result<pb::HeartbeatResponse, Status> {
    let channel = server
        .inner
        .channels
        .get(addr)
        .map_err(|e| Status::unavailable(e.to_string()))?;
    let mut client = pb::metadata_service_client::MetadataServiceClient::new(channel);
    Ok(client.heartbeat(request).await?.into_inner())
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
