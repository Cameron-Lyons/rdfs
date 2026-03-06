use crate::pb;
use crate::util::checksum_hex;
use anyhow::{Result, bail};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::Status;
use tonic::transport::Channel;

type MetadataClient = pb::metadata_service_client::MetadataServiceClient<Channel>;
type ChunkClient = pb::chunk_service_client::ChunkServiceClient<Channel>;
type MetadataFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Status>> + Send + 'a>>;

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

    pub async fn rename(&self, from: &str, to: &str) -> Result<()> {
        let from = from.to_string();
        let to = to.to_string();
        self.call_metadata(|client| {
            let from = from.clone();
            let to = to.clone();
            Box::pin(async move {
                client
                    .rename(pb::RenameRequest { from, to })
                    .await
                    .map(|_| ())
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
                match metadata_client(&addr).await {
                    Ok(mut client) => match operation(&mut client).await {
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
                    },
                    Err(error) => last_error = Some(anyhow::anyhow!("metadata {addr}: {error}")),
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

    fn report_replica_failure_best_effort(
        &self,
        chunk_id: String,
        node_id: String,
        reason: String,
    ) {
        let client = self.clone();
        tokio::spawn(async move {
            let _ = client
                .report_replica_failure(chunk_id, node_id, reason)
                .await;
        });
    }
}

pub struct FileWriter {
    client: Client,
    session: pb::UploadSession,
    buffer: Vec<u8>,
    chunks: Vec<pb::CommitChunk>,
    offset: u64,
}

impl FileWriter {
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        self.buffer.extend_from_slice(data);
        let chunk_size = self.session.chunk_size as usize;
        while self.buffer.len() >= chunk_size {
            let chunk = self.buffer.drain(..chunk_size).collect::<Vec<_>>();
            self.flush_chunk(chunk).await?;
        }
        Ok(())
    }

    pub async fn commit(mut self) -> Result<pb::FileManifest> {
        if !self.buffer.is_empty() {
            let tail = std::mem::take(&mut self.buffer);
            self.flush_chunk(tail).await?;
        }

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

    pub async fn abort(self) -> Result<()> {
        let upload_id = self.session.upload_id.clone();
        self.client
            .call_metadata(|client| {
                let upload_id = upload_id.clone();
                Box::pin(async move {
                    client
                        .abort_upload(pb::AbortUploadRequest { upload_id })
                        .await
                        .map(|_| ())
                })
            })
            .await
    }

    async fn flush_chunk(&mut self, data: Vec<u8>) -> Result<()> {
        let checksum = checksum_hex(&data);
        let upload_id = self.session.upload_id.clone();
        let placement = self
            .client
            .call_metadata(|client| {
                let request = pb::AllocateChunkRequest {
                    upload_id: upload_id.clone(),
                    size: data.len() as u64,
                    checksum: checksum.clone(),
                };
                Box::pin(
                    async move { client.allocate_chunk(request).await.map(|r| r.into_inner()) },
                )
            })
            .await?;
        let primary = placement
            .replicas
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("metadata returned no chunk replicas"))?;

        let header = pb::PutChunkHeader {
            chunk_id: placement.chunk_id.clone(),
            version: placement.version,
            checksum: checksum.clone(),
            size: data.len() as u64,
            forward_targets: placement.replicas.iter().skip(1).cloned().collect(),
        };
        let stream = tokio_stream::iter(vec![
            pb::PutChunkRequest {
                item: Some(pb::put_chunk_request::Item::Header(header)),
            },
            pb::PutChunkRequest {
                item: Some(pb::put_chunk_request::Item::Data(data.clone())),
            },
        ]);

        let mut client = chunk_client(&primary.addr).await?;
        client.put_chunk(stream).await?;

        self.chunks.push(pb::CommitChunk {
            chunk_id: placement.chunk_id,
            offset: self.offset,
            size: data.len() as u64,
            checksum,
            replicas: placement.replicas,
        });
        self.offset += data.len() as u64;
        Ok(())
    }
}

pub struct FileReader {
    client: Client,
    manifest: pb::FileManifest,
}

impl FileReader {
    pub fn manifest(&self) -> &pb::FileManifest {
        &self.manifest
    }

    pub async fn read_all(&self) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        for chunk in &self.manifest.chunks {
            result.extend_from_slice(&read_chunk(&self.client, chunk).await?);
        }
        Ok(result)
    }

    pub async fn read_range(&self, offset: u64, length: u64) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        let range_end = offset.saturating_add(length);
        for chunk in &self.manifest.chunks {
            let chunk_start = chunk.offset;
            let chunk_end = chunk.offset.saturating_add(chunk.size);
            if chunk_end <= offset || chunk_start >= range_end {
                continue;
            }

            let data = read_chunk(&self.client, chunk).await?;
            let local_start = offset.saturating_sub(chunk_start) as usize;
            let local_end = (range_end.min(chunk_end) - chunk_start) as usize;
            result.extend_from_slice(&data[local_start..local_end]);
        }
        Ok(result)
    }
}

async fn read_chunk(client: &Client, chunk: &pb::ChunkRef) -> Result<Vec<u8>> {
    let mut last_error = None::<anyhow::Error>;
    for replica in &chunk.replicas {
        match chunk_client(&replica.addr).await {
            Ok(mut remote) => match remote
                .get_chunk(pb::GetChunkRequest {
                    chunk_id: chunk.chunk_id.clone(),
                    version: replica.version,
                })
                .await
            {
                Ok(response) => {
                    let mut stream = response.into_inner();
                    let mut metadata = None;
                    let mut bytes = Vec::new();
                    while let Some(message) = stream.message().await? {
                        match message.item {
                            Some(pb::get_chunk_response::Item::Metadata(info)) => {
                                metadata = Some(info)
                            }
                            Some(pb::get_chunk_response::Item::Data(data)) => {
                                bytes.extend_from_slice(&data)
                            }
                            None => {}
                        }
                    }
                    let Some(metadata) = metadata else {
                        client.report_replica_failure_best_effort(
                            chunk.chunk_id.clone(),
                            replica.node_id.clone(),
                            "missing chunk metadata".to_string(),
                        );
                        last_error = Some(anyhow::anyhow!("missing chunk metadata"));
                        continue;
                    };
                    if checksum_hex(&bytes) != metadata.checksum {
                        client.report_replica_failure_best_effort(
                            chunk.chunk_id.clone(),
                            replica.node_id.clone(),
                            "checksum mismatch while reading chunk".to_string(),
                        );
                        last_error = Some(anyhow::anyhow!("checksum mismatch while reading chunk"));
                        continue;
                    }
                    return Ok(bytes);
                }
                Err(status) => {
                    client.report_replica_failure_best_effort(
                        chunk.chunk_id.clone(),
                        replica.node_id.clone(),
                        format!("read failure from {}: {status}", replica.addr),
                    );
                    last_error = Some(anyhow::anyhow!("chunk replica {}: {status}", replica.addr))
                }
            },
            Err(error) => {
                client.report_replica_failure_best_effort(
                    chunk.chunk_id.clone(),
                    replica.node_id.clone(),
                    format!("transport failure to {}: {error}", replica.addr),
                );
                last_error = Some(anyhow::anyhow!("chunk replica {}: {error}", replica.addr))
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("failed to read chunk")))
}

async fn metadata_client(addr: &str) -> Result<MetadataClient, tonic::transport::Error> {
    MetadataClient::connect(format!("http://{addr}")).await
}

async fn chunk_client(addr: &str) -> Result<ChunkClient, tonic::transport::Error> {
    ChunkClient::connect(format!("http://{addr}")).await
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
