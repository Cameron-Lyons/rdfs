use crate::chunk::send_chunk;
use crate::net::ChannelCache;
use crate::pb;
use crate::util::checksum_hex;
use anyhow::{Result, bail};
use futures_util::{Stream, StreamExt, TryStreamExt, stream};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tonic::Status;
use tonic::transport::Channel;

type MetadataClient = pb::metadata_service_client::MetadataServiceClient<Channel>;
type ChunkClient = pb::chunk_service_client::ChunkServiceClient<Channel>;
type MetadataFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Status>> + Send + 'a>>;

/// How many chunks a reader fetches concurrently.
const READ_AHEAD_CHUNKS: usize = 4;

/// How many chunks a writer uploads concurrently.
const WRITE_PIPELINE_CHUNKS: usize = 4;

#[derive(Debug, Clone)]
pub struct WriteOptions {
    pub replication_factor: u32,
    pub chunk_size: u32,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            replication_factor: 3,
            chunk_size: 8 * 1024 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct Client {
    metadata_addrs: Arc<RwLock<Vec<String>>>,
    leader_hint: Arc<RwLock<Option<String>>>,
    channels: Arc<ChannelCache>,
}

impl Client {
    pub fn new(metadata_addrs: impl IntoIterator<Item = impl ToString>) -> Result<Self> {
        let addrs = metadata_addrs
            .into_iter()
            .map(|addr| addr.to_string())
            .collect::<Vec<_>>();
        if addrs.is_empty() {
            bail!("at least one metadata address is required");
        }
        Ok(Self {
            metadata_addrs: Arc::new(RwLock::new(addrs)),
            leader_hint: Arc::new(RwLock::new(None)),
            channels: Arc::new(ChannelCache::default()),
        })
    }

    pub async fn stat(&self, path: &str) -> Result<pb::FileInfo> {
        let path = path.to_string();
        self.call_metadata(|client| {
            let path = path.clone();
            Box::pin(async move {
                client
                    .stat(pb::StatRequest { path })
                    .await
                    .map(|response| response.into_inner())
            })
        })
        .await
    }

    pub async fn list(&self, path: &str) -> Result<Vec<pb::DirectoryEntry>> {
        let path = path.to_string();
        self.call_metadata(|client| {
            let path = path.clone();
            Box::pin(async move {
                client
                    .list(pb::ListRequest { path })
                    .await
                    .map(|response| response.into_inner().entries)
            })
        })
        .await
    }

    pub async fn mkdir(&self, path: &str) -> Result<pb::FileInfo> {
        let path = path.to_string();
        self.call_metadata(|client| {
            let path = path.clone();
            Box::pin(async move {
                client
                    .mkdir(pb::MkdirRequest { path })
                    .await
                    .map(|response| response.into_inner())
            })
        })
        .await
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let path = path.to_string();
        self.call_metadata(|client| {
            let path = path.clone();
            Box::pin(async move { client.delete(pb::DeleteRequest { path }).await.map(|_| ()) })
        })
        .await
    }

    pub async fn cluster_membership(&self) -> Result<pb::ClusterMembership> {
        let membership = self
            .call_metadata(|client| {
                Box::pin(async move {
                    client
                        .get_cluster_membership(pb::Empty {})
                        .await
                        .map(|response| response.into_inner())
                })
            })
            .await?;
        self.replace_metadata_addrs(&membership).await;
        Ok(membership)
    }

    pub async fn add_metadata_node(
        &self,
        node_id: u64,
        addr: impl Into<String>,
        promote_to_voter: bool,
    ) -> Result<pb::ClusterMembership> {
        let request = pb::AddMetadataNodeRequest {
            node_id,
            addr: addr.into(),
            promote_to_voter,
        };
        let membership = self
            .call_metadata(|client| {
                let request = request.clone();
                Box::pin(async move {
                    client
                        .add_metadata_node(request)
                        .await
                        .map(|response| response.into_inner())
                })
            })
            .await?;
        self.replace_metadata_addrs(&membership).await;
        Ok(membership)
    }

    pub async fn remove_metadata_node(
        &self,
        node_id: u64,
        retain_as_learner: bool,
    ) -> Result<pb::ClusterMembership> {
        let request = pb::RemoveMetadataNodeRequest {
            node_id,
            retain_as_learner,
        };
        let membership = self
            .call_metadata(|client| {
                Box::pin(async move {
                    client
                        .remove_metadata_node(request)
                        .await
                        .map(|response| response.into_inner())
                })
            })
            .await?;
        self.replace_metadata_addrs(&membership).await;
        Ok(membership)
    }

    pub async fn replace_metadata_node(
        &self,
        old_node_id: u64,
        new_node_id: u64,
        new_addr: impl Into<String>,
    ) -> Result<pb::ClusterMembership> {
        let request = pb::ReplaceMetadataNodeRequest {
            old_node_id,
            new_node_id,
            new_addr: new_addr.into(),
        };
        let membership = self
            .call_metadata(|client| {
                let request = request.clone();
                Box::pin(async move {
                    client
                        .replace_metadata_node(request)
                        .await
                        .map(|response| response.into_inner())
                })
            })
            .await?;
        self.replace_metadata_addrs(&membership).await;
        Ok(membership)
    }

    pub async fn open_reader(&self, path: &str) -> Result<FileReader> {
        let path = path.to_string();
        let manifest = self
            .call_metadata(|client| {
                let path = path.clone();
                Box::pin(async move {
                    client
                        .open_file(pb::OpenFileRequest { path })
                        .await
                        .map(|response| response.into_inner())
                })
            })
            .await?;
        Ok(FileReader {
            client: self.clone(),
            manifest,
        })
    }

    pub async fn create_writer(&self, path: &str, options: WriteOptions) -> Result<FileWriter> {
        self.begin_writer(path, pb::UploadMode::Create, options)
            .await
    }

    pub async fn overwrite_writer(&self, path: &str, options: WriteOptions) -> Result<FileWriter> {
        self.begin_writer(path, pb::UploadMode::Overwrite, options)
            .await
    }

    async fn begin_writer(
        &self,
        path: &str,
        mode: pb::UploadMode,
        options: WriteOptions,
    ) -> Result<FileWriter> {
        let path = path.to_string();
        let session = self
            .call_metadata(|client| {
                let path = path.clone();
                let options = options.clone();
                Box::pin(async move {
                    client
                        .begin_upload(pb::BeginUploadRequest {
                            path,
                            mode: mode as i32,
                            replication_factor: options.replication_factor,
                            chunk_size: options.chunk_size,
                        })
                        .await
                        .map(|response| response.into_inner())
                })
            })
            .await?;
        Ok(FileWriter {
            client: self.clone(),
            session,
            buffer: Vec::new(),
            inflight: VecDeque::new(),
            chunks: Vec::new(),
            offset: 0,
        })
    }

    async fn call_metadata<T, F>(&self, mut operation: F) -> Result<T>
    where
        F: for<'a> FnMut(&'a mut MetadataClient) -> MetadataFuture<'a, T>,
    {
        let mut last_error = None::<anyhow::Error>;
        for _attempt in 0..40 {
            for addr in self.metadata_order().await {
                let channel = match self.channels.get(&addr) {
                    Ok(channel) => channel,
                    Err(error) => {
                        last_error = Some(anyhow::anyhow!("metadata {addr}: {error}"));
                        continue;
                    }
                };
                let mut client = MetadataClient::new(channel);
                match operation(&mut client).await {
                    Ok(response) => {
                        *self.leader_hint.write().await = Some(addr);
                        return Ok(response);
                    }
                    Err(status) => {
                        if let Some(leader) = status_leader_hint(&status) {
                            last_error = Some(anyhow::anyhow!(
                                "metadata {addr}: redirected to leader {leader}"
                            ));
                            *self.leader_hint.write().await = Some(leader);
                            continue;
                        }
                        last_error = Some(anyhow::anyhow!("metadata {addr}: {status}"));
                    }
                }
            }

            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("metadata request failed")))
    }

    async fn metadata_order(&self) -> Vec<String> {
        let mut ordered = Vec::new();
        if let Some(leader) = self.leader_hint.read().await.clone() {
            ordered.push(leader);
        }
        for addr in self.metadata_addrs.read().await.iter() {
            if !ordered.contains(addr) {
                ordered.push(addr.clone());
            }
        }
        ordered
    }

    async fn replace_metadata_addrs(&self, membership: &pb::ClusterMembership) {
        let mut addrs = membership
            .nodes
            .iter()
            .map(|node| node.addr.clone())
            .filter(|addr| !addr.trim().is_empty())
            .collect::<Vec<_>>();
        addrs.sort();
        addrs.dedup();
        if !addrs.is_empty() {
            *self.metadata_addrs.write().await = addrs;
        }
    }

    async fn report_replica_failure(
        &self,
        chunk_id: String,
        node_id: String,
        reason: String,
    ) -> Result<()> {
        self.call_metadata(|client| {
            let request = pb::ReportReplicaFailureRequest {
                chunk_id: chunk_id.clone(),
                node_id: node_id.clone(),
                reason: reason.clone(),
            };
            Box::pin(async move { client.report_replica_failure(request).await.map(|_| ()) })
        })
        .await
    }

    async fn report_replica_failure_best_effort(
        &self,
        chunk_id: String,
        node_id: String,
        reason: String,
    ) {
        let _ = self.report_replica_failure(chunk_id, node_id, reason).await;
    }
}

pub struct FileWriter {
    client: Client,
    session: pb::UploadSession,
    buffer: Vec<u8>,
    /// Uploads still in flight, oldest first, at most
    /// [`WRITE_PIPELINE_CHUNKS`] deep. Completions are collected in order so
    /// `chunks` stays sorted by offset.
    inflight: VecDeque<JoinHandle<Result<pb::CommitChunk>>>,
    chunks: Vec<pb::CommitChunk>,
    offset: u64,
}

impl FileWriter {
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        self.buffer.extend_from_slice(data);
        let chunk_size = self.session.chunk_size as usize;
        while self.buffer.len() >= chunk_size {
            let rest = self.buffer.split_off(chunk_size);
            let chunk = std::mem::replace(&mut self.buffer, rest);
            self.spawn_flush(chunk).await?;
        }
        Ok(())
    }

    pub async fn commit(mut self) -> Result<pb::FileManifest> {
        if !self.buffer.is_empty() {
            let tail = std::mem::take(&mut self.buffer);
            self.spawn_flush(tail).await?;
        }
        while self.collect_oldest().await? {}

        let request = pb::CommitUploadRequest {
            upload_id: self.session.upload_id.clone(),
            chunks: self.chunks.clone(),
        };
        let manifest = self
            .client
            .call_metadata(|client| {
                let request = request.clone();
                Box::pin(async move { client.commit_upload(request).await.map(|r| r.into_inner()) })
            })
            .await?;
        Ok(manifest)
    }

    /// Starts uploading one chunk in the background, first draining the
    /// oldest in-flight upload when the pipeline is full.
    async fn spawn_flush(&mut self, data: Vec<u8>) -> Result<()> {
        while self.inflight.len() >= WRITE_PIPELINE_CHUNKS {
            self.collect_oldest().await?;
        }
        let client = self.client.clone();
        let upload_id = self.session.upload_id.clone();
        let offset = self.offset;
        self.offset += data.len() as u64;
        self.inflight
            .push_back(tokio::spawn(upload_chunk(client, upload_id, offset, data)));
        Ok(())
    }

    /// Waits for the oldest in-flight upload and records its chunk. Returns
    /// false when nothing is in flight.
    async fn collect_oldest(&mut self) -> Result<bool> {
        let Some(task) = self.inflight.pop_front() else {
            return Ok(false);
        };
        self.chunks.push(task.await??);
        Ok(true)
    }
}

impl Drop for FileWriter {
    fn drop(&mut self) {
        for task in &self.inflight {
            task.abort();
        }
    }
}

/// Allocates placement for one chunk and replicates it through the primary.
async fn upload_chunk(
    client: Client,
    upload_id: String,
    offset: u64,
    data: Vec<u8>,
) -> Result<pb::CommitChunk> {
    let checksum = checksum_hex(&data);
    let placement = client
        .call_metadata(|client| {
            let request = pb::AllocateChunkRequest {
                upload_id: upload_id.clone(),
                size: data.len() as u64,
                checksum: checksum.clone(),
            };
            Box::pin(async move { client.allocate_chunk(request).await.map(|r| r.into_inner()) })
        })
        .await?;
    let primary = placement
        .replicas
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("metadata returned no chunk replicas"))?;

    let size = data.len() as u64;
    let header = pb::PutChunkHeader {
        chunk_id: placement.chunk_id.clone(),
        checksum: checksum.clone(),
        size,
        forward_targets: placement.replicas.iter().skip(1).cloned().collect(),
    };
    let channel = client.channels.get(&primary.addr)?;
    send_chunk(channel, header, data).await?;

    Ok(pb::CommitChunk {
        chunk_id: placement.chunk_id,
        offset,
        size,
        checksum,
        replicas: placement.replicas,
    })
}

pub struct FileReader {
    client: Client,
    manifest: pb::FileManifest,
}

impl FileReader {
    pub fn manifest(&self) -> &pb::FileManifest {
        &self.manifest
    }

    /// Streams the file as a sequence of chunk-sized byte buffers, fetching
    /// up to [`READ_AHEAD_CHUNKS`] chunks ahead of the consumer. Memory use
    /// stays bounded by the read-ahead window regardless of file size.
    pub fn stream(&self) -> impl Stream<Item = Result<Vec<u8>>> + Send + '_ {
        stream::iter(&self.manifest.chunks)
            .map(|chunk| read_chunk(&self.client, chunk))
            .buffered(READ_AHEAD_CHUNKS)
    }

    pub async fn read_all(&self) -> Result<Vec<u8>> {
        let parts = self.stream().try_collect::<Vec<_>>().await?;
        Ok(parts.concat())
    }

    /// Reads `length` bytes starting at `offset`, fetching only the chunks
    /// that overlap the range. Ranges reaching past the end of the file are
    /// truncated, matching `pread` semantics.
    pub async fn read_range(&self, offset: u64, length: u64) -> Result<Vec<u8>> {
        let end = offset.saturating_add(length);
        let parts = stream::iter(
            self.manifest
                .chunks
                .iter()
                .filter(|chunk| chunk.offset < end && chunk.offset + chunk.size > offset),
        )
        .map(|chunk| async move {
            let bytes = read_chunk(&self.client, chunk).await?;
            let from = offset.saturating_sub(chunk.offset) as usize;
            let to = (end.min(chunk.offset + chunk.size) - chunk.offset) as usize;
            Ok::<_, anyhow::Error>(bytes[from..to].to_vec())
        })
        .buffered(READ_AHEAD_CHUNKS)
        .try_collect::<Vec<_>>()
        .await?;
        Ok(parts.concat())
    }
}

async fn read_chunk(client: &Client, chunk: &pb::ChunkRef) -> Result<Vec<u8>> {
    let mut last_error = None::<anyhow::Error>;
    for replica in &chunk.replicas {
        match read_replica(client, replica, chunk).await {
            Ok(bytes) => return Ok(bytes),
            Err(error) => {
                client
                    .report_replica_failure_best_effort(
                        chunk.chunk_id.clone(),
                        replica.node_id.clone(),
                        format!("read failure from {}: {error}", replica.addr),
                    )
                    .await;
                last_error = Some(anyhow::anyhow!("chunk replica {}: {error}", replica.addr));
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("failed to read chunk")))
}

/// Reads one replica end to end and verifies the bytes against the manifest
/// checksum. Any transport, stream, or integrity error is returned so the
/// caller can fail over to the next replica.
async fn read_replica(
    client: &Client,
    replica: &pb::ChunkReplica,
    chunk: &pb::ChunkRef,
) -> Result<Vec<u8>> {
    let mut remote = ChunkClient::new(client.channels.get(&replica.addr)?);
    let response = remote
        .get_chunk(pb::GetChunkRequest {
            chunk_id: chunk.chunk_id.clone(),
        })
        .await?;
    let mut stream = response.into_inner();
    let mut saw_metadata = false;
    let mut bytes = Vec::with_capacity(chunk.size as usize);
    while let Some(message) = stream.message().await? {
        match message.item {
            Some(pb::get_chunk_response::Item::Metadata(_)) => saw_metadata = true,
            Some(pb::get_chunk_response::Item::Data(data)) => bytes.extend_from_slice(&data),
            None => {}
        }
    }
    if !saw_metadata {
        bail!("missing chunk metadata");
    }
    if bytes.len() as u64 != chunk.size {
        bail!(
            "chunk size mismatch: manifest says {} bytes, replica sent {}",
            chunk.size,
            bytes.len()
        );
    }
    if checksum_hex(&bytes) != chunk.checksum {
        bail!("checksum mismatch while reading chunk");
    }
    Ok(bytes)
}

fn status_leader_hint(status: &Status) -> Option<String> {
    status.message().strip_prefix("leader=").and_then(|value| {
        let value = value.trim();
        if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        }
    })
}
