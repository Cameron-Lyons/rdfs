use crate::model::{
    ChunkInventoryEntry, ChunkPlacementModel, ChunkRecord, ChunkReplicaAssignment,
    ChunkServerState, ChunkTombstone, CommitChunkModel, DirectoryRecord, FileInfoModel,
    FileManifestModel, FileRecord, MetadataCommand, MetadataResponse, MetadataStateMachine,
    NamespaceEntry, PendingChunk, RepairTask, ReplicaPointer, UploadModeModel, UploadSessionModel,
    UploadSessionState,
};
use crate::path::{is_child_of, normalize_path, parent_path, replace_prefix};
use crate::pb;
use crate::raft::{MetaNodeId, MetaRaft, MetaTypeConfig};
use crate::util::now_millis;
use anyhow::{Result, bail};
use futures_util::{Stream, TryStreamExt};
use openraft::entry::RaftEntry;
use openraft::error::{ForwardToLeader, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::SnapshotResponse;
use openraft::storage::{
    EntryResponder, IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder,
    RaftStateMachine, Snapshot,
};
use openraft::{
    BasicNode, ChangeMembers, Config, Entry, EntryPayload, LogId, ReadPolicy, SnapshotMeta,
    StoredMembership, Vote,
};
use rocksdb::{DB, Options};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::io::{self, Cursor};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, watch};
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};
use uuid::Uuid;

const LOG_PREFIX: &str = "log/";
const VOTE_KEY: &[u8] = b"vote";
const COMMITTED_KEY: &[u8] = b"committed";
const LAST_PURGED_KEY: &[u8] = b"last_purged";
const STATE_MACHINE_KEY: &[u8] = b"state_machine";
const SNAPSHOT_META_KEY: &[u8] = b"snapshot_meta";
const SNAPSHOT_DATA_KEY: &[u8] = b"snapshot_data";
const ACTIVE_CHUNKSERVER_WINDOW_MS: u64 = 15_000;
const LEASE_TTL_MS: u64 = 30_000;
const GC_GRACE_MS: u64 = 30_000;

#[derive(Debug, Clone)]
struct MetaSnapshot {
    meta: SnapshotMeta<MetaTypeConfig>,
    data: Vec<u8>,
}

pub struct MetaStore {
    db: Arc<DB>,
    vote: RwLock<Option<Vote<MetaTypeConfig>>>,
    committed: RwLock<Option<LogId<MetaTypeConfig>>>,
    last_purged: RwLock<Option<LogId<MetaTypeConfig>>>,
    log: RwLock<BTreeMap<u64, Entry<MetaTypeConfig>>>,
    state_machine: RwLock<MetadataStateMachine>,
    current_snapshot: RwLock<Option<MetaSnapshot>>,
}

impl MetaStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = Arc::new(DB::open(&options, path)?);

        let vote = load_json(&db, VOTE_KEY)?;
        let committed = load_json(&db, COMMITTED_KEY)?;
        let last_purged = load_json(&db, LAST_PURGED_KEY)?;
        let state_machine =
            load_json(&db, STATE_MACHINE_KEY)?.unwrap_or_else(MetadataStateMachine::default);
        let snapshot_meta: Option<SnapshotMeta<MetaTypeConfig>> =
            load_json(&db, SNAPSHOT_META_KEY)?;
        let snapshot_data: Option<Vec<u8>> = load_json(&db, SNAPSHOT_DATA_KEY)?;

        let mut log = BTreeMap::new();
        for item in db.iterator(rocksdb::IteratorMode::Start) {
            let (key, value) = item?;
            let Ok(key) = std::str::from_utf8(&key) else {
                continue;
            };
            if !key.starts_with(LOG_PREFIX) {
                continue;
            }
            let entry: Entry<MetaTypeConfig> = serde_json::from_slice(&value)?;
            log.insert(entry.index(), entry);
        }

        let current_snapshot = match (snapshot_meta, snapshot_data) {
            (Some(meta), Some(data)) => Some(MetaSnapshot { meta, data }),
            _ => None,
        };

        Ok(Arc::new(Self {
            db,
            vote: RwLock::new(vote),
            committed: RwLock::new(committed),
            last_purged: RwLock::new(last_purged),
            log: RwLock::new(log),
            state_machine: RwLock::new(state_machine),
            current_snapshot: RwLock::new(current_snapshot),
        }))
    }

    async fn persist_state_machine(&self, state: &MetadataStateMachine) -> io::Result<()> {
        put_json(&self.db, STATE_MACHINE_KEY, state)
    }

    async fn state_snapshot(&self) -> MetadataStateMachine {
        self.state_machine.read().await.clone()
    }
}

impl RaftLogReader<MetaTypeConfig> for Arc<MetaStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + openraft::OptionalSend>(
        &mut self,
        range: RB,
    ) -> io::Result<Vec<Entry<MetaTypeConfig>>> {
        let log = self.log.read().await;
        Ok(log.range(range).map(|(_, entry)| entry.clone()).collect())
    }

    async fn read_vote(&mut self) -> io::Result<Option<Vote<MetaTypeConfig>>> {
        Ok(*self.vote.read().await)
    }
}

impl RaftSnapshotBuilder<MetaTypeConfig> for Arc<MetaStore> {
    async fn build_snapshot(&mut self) -> io::Result<Snapshot<MetaTypeConfig>> {
        let state = self.state_machine.read().await.clone();
        let data = serde_json::to_vec(&state)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        let snapshot_id = match state.last_applied_log {
            Some(log_id) => format!("{}-{}", log_id.committed_leader_id(), log_id.index()),
            None => format!("bootstrap-{}", Uuid::new_v4()),
        };
        let meta = SnapshotMeta {
            last_log_id: state.last_applied_log,
            last_membership: state.last_membership.clone(),
            snapshot_id,
        };

        put_json(&self.db, SNAPSHOT_META_KEY, &meta)?;
        put_json(&self.db, SNAPSHOT_DATA_KEY, &data)?;

        *self.current_snapshot.write().await = Some(MetaSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

impl RaftLogStorage<MetaTypeConfig> for Arc<MetaStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> io::Result<LogState<MetaTypeConfig>> {
        let last_purged_log_id = *self.last_purged.read().await;
        let last_log_id = self
            .log
            .read()
            .await
            .values()
            .next_back()
            .map(|entry| entry.log_id())
            .or(last_purged_log_id);
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<MetaTypeConfig>) -> io::Result<()> {
        *self.vote.write().await = Some(*vote);
        put_json(&self.db, VOTE_KEY, vote)
    }

    async fn save_committed(&mut self, committed: Option<LogId<MetaTypeConfig>>) -> io::Result<()> {
        *self.committed.write().await = committed;
        put_json(&self.db, COMMITTED_KEY, &committed)
    }

    async fn read_committed(&mut self) -> io::Result<Option<LogId<MetaTypeConfig>>> {
        Ok(*self.committed.read().await)
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<MetaTypeConfig>) -> io::Result<()>
    where
        I: IntoIterator<Item = Entry<MetaTypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut log_guard = self.log.write().await;
        for entry in entries {
            let bytes = serde_json::to_vec(&entry)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            self.db
                .put(log_key(entry.index()), bytes)
                .map_err(rocksdb_err_to_io)?;
            log_guard.insert(entry.index(), entry);
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogId<MetaTypeConfig>>,
    ) -> io::Result<()> {
        let start_index = last_log_id.map(|log_id| log_id.index() + 1).unwrap_or(0);
        let mut log_guard = self.log.write().await;
        let keys: Vec<u64> = log_guard
            .range(start_index..)
            .map(|(idx, _)| *idx)
            .collect();
        for key in keys {
            log_guard.remove(&key);
            self.db.delete(log_key(key)).map_err(rocksdb_err_to_io)?;
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<MetaTypeConfig>) -> io::Result<()> {
        *self.last_purged.write().await = Some(log_id);
        put_json(&self.db, LAST_PURGED_KEY, &Some(log_id))?;

        let mut log_guard = self.log.write().await;
        let keys: Vec<u64> = log_guard
            .range(..=log_id.index())
            .map(|(idx, _)| *idx)
            .collect();
        for key in keys {
            log_guard.remove(&key);
            self.db.delete(log_key(key)).map_err(rocksdb_err_to_io)?;
        }
        Ok(())
    }
}

impl RaftStateMachine<MetaTypeConfig> for Arc<MetaStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> io::Result<(
        Option<LogId<MetaTypeConfig>>,
        StoredMembership<MetaTypeConfig>,
    )> {
        let state = self.state_machine.read().await;
        Ok((state.last_applied_log, state.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> io::Result<()>
    where
        Strm: Stream<Item = Result<EntryResponder<MetaTypeConfig>, io::Error>>
            + Unpin
            + openraft::OptionalSend,
    {
        let mut state = self.state_machine.write().await;
        while let Some((entry, responder)) = entries.try_next().await? {
            state.last_applied_log = Some(entry.log_id);
            let response = match entry.payload {
                EntryPayload::Blank => MetadataResponse::Ack,
                EntryPayload::Membership(ref membership) => {
                    state.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    MetadataResponse::Ack
                }
                EntryPayload::Normal(ref command) => {
                    match apply_command(&mut state, command.clone()) {
                        Ok(response) => response,
                        Err(error) => MetadataResponse::Error(error.to_string()),
                    }
                }
            };

            if let Some(responder) = responder {
                responder.send(response);
            }
        }

        self.persist_state_machine(&state).await?;
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> io::Result<Cursor<Vec<u8>>> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<MetaTypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> io::Result<()> {
        let data = snapshot.into_inner();
        let state: MetadataStateMachine = serde_json::from_slice(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        put_json(&self.db, SNAPSHOT_META_KEY, meta)?;
        put_json(&self.db, SNAPSHOT_DATA_KEY, &data)?;
        self.persist_state_machine(&state).await?;

        *self.state_machine.write().await = state;
        *self.current_snapshot.write().await = Some(MetaSnapshot {
            meta: meta.clone(),
            data,
        });
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> io::Result<Option<Snapshot<MetaTypeConfig>>> {
        Ok(self
            .current_snapshot
            .read()
            .await
            .clone()
            .map(|snapshot| Snapshot {
                meta: snapshot.meta,
                snapshot: Cursor::new(snapshot.data),
            }))
    }
}

#[derive(Clone)]
pub struct MetadataNode {
    inner: Arc<MetadataNodeInner>,
}

struct MetadataNodeInner {
    id: MetaNodeId,
    addr: String,
    raft: MetaRaft,
    store: Arc<MetaStore>,
    peers: BTreeMap<MetaNodeId, String>,
    shutdown: watch::Sender<bool>,
}

#[derive(Debug, Clone)]
pub struct MetadataNodeConfig {
    pub id: MetaNodeId,
    pub addr: String,
    pub data_dir: PathBuf,
    pub peers: BTreeMap<MetaNodeId, String>,
}

impl MetadataNode {
    pub async fn open(config: MetadataNodeConfig) -> Result<Self> {
        tokio::fs::create_dir_all(&config.data_dir).await?;

        let store = MetaStore::open(config.data_dir.join("rocksdb"))?;
        let election_bias_ms = config.id.saturating_sub(1) * 400;
        let raft_config = Arc::new(
            Config {
                cluster_name: "rdfs-meta".to_string(),
                heartbeat_interval: 150,
                election_timeout_min: 500 + election_bias_ms,
                election_timeout_max: 700 + election_bias_ms,
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(256),
                ..Default::default()
            }
            .validate()?,
        );
        let network = MetaNetworkFactory;
        let raft = MetaRaft::new(
            config.id,
            raft_config,
            network,
            store.clone(),
            store.clone(),
        )
        .await?;
        let (shutdown, _) = watch::channel(false);

        Ok(Self {
            inner: Arc::new(MetadataNodeInner {
                id: config.id,
                addr: config.addr,
                raft,
                store,
                peers: config.peers,
                shutdown,
            }),
        })
    }

    pub fn addr(&self) -> &str {
        &self.inner.addr
    }

    pub fn id(&self) -> MetaNodeId {
        self.inner.id
    }

    pub async fn bootstrap_cluster(&self) -> Result<()> {
        let nodes = self
            .inner
            .peers
            .iter()
            .map(|(id, addr)| (*id, BasicNode::new(addr)))
            .collect::<BTreeMap<_, _>>();
        let _ = self.inner.raft.initialize(nodes).await;
        Ok(())
    }

    pub async fn serve(self, bootstrap: bool) -> Result<()> {
        let addr = self.inner.addr.parse()?;
        let meta_service = MetadataGrpc { node: self.clone() };
        let raft_service = InternalRaftGrpc { node: self.clone() };
        let mut shutdown = self.inner.shutdown.subscribe();

        tokio::spawn(gc_loop(self.clone()));
        tokio::spawn(repair_loop(self.clone()));

        if bootstrap {
            let node = self.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let _ = node.bootstrap_cluster().await;
            });
        }

        Server::builder()
            .add_service(pb::metadata_service_server::MetadataServiceServer::new(
                meta_service,
            ))
            .add_service(pb::raft_service_server::RaftServiceServer::new(
                raft_service,
            ))
            .serve_with_shutdown(addr, async move {
                let _ = shutdown.changed().await;
            })
            .await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.inner.shutdown.send(true);
        self.inner.raft.shutdown().await?;
        Ok(())
    }

    async fn linearized_state(&self) -> Result<MetadataStateMachine, Status> {
        self.inner
            .raft
            .ensure_linearizable(ReadPolicy::LeaseRead)
            .await
            .map_err(|e| self.status_from_raft_error(e))?;
        Ok(self.inner.store.state_snapshot().await)
    }

    fn forward_status(&self) -> Status {
        match self.inner.raft.as_leader() {
            Ok(_) => Status::internal("node became leader unexpectedly"),
            Err(ForwardToLeader {
                leader_node: Some(node),
                ..
            }) => Status::unavailable(format!("leader={}", node.addr)),
            Err(_) => Status::unavailable("leader="),
        }
    }

    fn status_from_raft_error<E: std::error::Error>(
        &self,
        error: RaftError<MetaTypeConfig, E>,
    ) -> Status {
        match error {
            RaftError::APIError(_) => self.forward_status(),
            RaftError::Fatal(err) => Status::internal(err.to_string()),
        }
    }

    async fn write_command(&self, command: MetadataCommand) -> Result<MetadataResponse, Status> {
        let result = self
            .inner
            .raft
            .client_write(command)
            .await
            .map_err(|e| self.status_from_raft_error(e))?;

        Ok(result.data)
    }

    fn response_as_ack(response: MetadataResponse) -> Result<(), Status> {
        match response {
            MetadataResponse::Ack => Ok(()),
            MetadataResponse::Error(error) => Err(Status::failed_precondition(error)),
            other => Err(Status::internal(format!("unexpected response: {other:?}"))),
        }
    }

    fn response_as_file_info(response: MetadataResponse) -> Result<pb::FileInfo, Status> {
        match response {
            MetadataResponse::FileInfo(info) => Ok(info.to_proto()),
            MetadataResponse::Error(error) => Err(Status::failed_precondition(error)),
            other => Err(Status::internal(format!("unexpected response: {other:?}"))),
        }
    }

    fn response_as_upload(response: MetadataResponse) -> Result<pb::UploadSession, Status> {
        match response {
            MetadataResponse::UploadSession(upload) => Ok(upload.to_proto()),
            MetadataResponse::Error(error) => Err(Status::failed_precondition(error)),
            other => Err(Status::internal(format!("unexpected response: {other:?}"))),
        }
    }

    fn response_as_chunk_placement(
        response: MetadataResponse,
    ) -> Result<pb::ChunkPlacement, Status> {
        match response {
            MetadataResponse::ChunkPlacement(placement) => Ok(placement.to_proto()),
            MetadataResponse::Error(error) => Err(Status::failed_precondition(error)),
            other => Err(Status::internal(format!("unexpected response: {other:?}"))),
        }
    }

    async fn cluster_membership(&self) -> Result<pb::ClusterMembership, Status> {
        let state = self.linearized_state().await?;
        Ok(cluster_membership_to_proto(&state))
    }

    async fn add_metadata_node(
        &self,
        node_id: MetaNodeId,
        addr: String,
        promote_to_voter: bool,
    ) -> Result<pb::ClusterMembership, Status> {
        let state = self.linearized_state().await?;
        let membership = state.last_membership.membership();
        let current_voters = state.last_membership.voter_ids().collect::<BTreeSet<_>>();

        if let Some(existing) = membership.get_node(&node_id) {
            if existing.addr != addr {
                return Err(Status::already_exists(format!(
                    "metadata node {node_id} already exists at {}",
                    existing.addr
                )));
            }
            if promote_to_voter && !current_voters.contains(&node_id) {
                let mut next_voters = current_voters;
                next_voters.insert(node_id);
                self.inner
                    .raft
                    .change_membership(next_voters, false)
                    .await
                    .map_err(|e| self.status_from_raft_error(e))?;
            }
            return self.cluster_membership().await;
        }

        self.inner
            .raft
            .add_learner(node_id, BasicNode::new(addr), true)
            .await
            .map_err(|e| self.status_from_raft_error(e))?;

        if promote_to_voter {
            let mut next_voters = current_voters;
            next_voters.insert(node_id);
            self.inner
                .raft
                .change_membership(next_voters, false)
                .await
                .map_err(|e| self.status_from_raft_error(e))?;
        }

        self.cluster_membership().await
    }

    async fn remove_metadata_node(
        &self,
        node_id: MetaNodeId,
        retain_as_learner: bool,
    ) -> Result<pb::ClusterMembership, Status> {
        let state = self.linearized_state().await?;
        let membership = state.last_membership.membership();
        let current_voters = state.last_membership.voter_ids().collect::<BTreeSet<_>>();

        if membership.get_node(&node_id).is_none() {
            return Err(Status::not_found(format!("metadata node {node_id}")));
        }

        if node_id == self.inner.id && current_voters.contains(&node_id) {
            return Err(Status::failed_precondition(
                "leader cannot remove itself from the voter set",
            ));
        }

        if current_voters.contains(&node_id) {
            let mut next_voters = current_voters;
            next_voters.remove(&node_id);
            if next_voters.is_empty() {
                return Err(Status::failed_precondition("cannot remove the last voter"));
            }
            self.inner
                .raft
                .change_membership(next_voters, retain_as_learner)
                .await
                .map_err(|e| self.status_from_raft_error(e))?;
        } else {
            self.inner
                .raft
                .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node_id])), false)
                .await
                .map_err(|e| self.status_from_raft_error(e))?;
        }

        self.cluster_membership().await
    }

    async fn replace_metadata_node(
        &self,
        old_node_id: MetaNodeId,
        new_node_id: MetaNodeId,
        new_addr: String,
    ) -> Result<pb::ClusterMembership, Status> {
        if old_node_id == new_node_id {
            return Err(Status::invalid_argument(
                "replacement node id must differ from the old node id",
            ));
        }

        let state = self.linearized_state().await?;
        let membership = state.last_membership.membership();
        let current_voters = state.last_membership.voter_ids().collect::<BTreeSet<_>>();

        if membership.get_node(&old_node_id).is_none() {
            return Err(Status::not_found(format!("metadata node {old_node_id}")));
        }
        if membership.get_node(&new_node_id).is_some() {
            return Err(Status::already_exists(format!(
                "metadata node {new_node_id}"
            )));
        }
        if old_node_id == self.inner.id && current_voters.contains(&old_node_id) {
            return Err(Status::failed_precondition(
                "leader cannot replace itself as a voter",
            ));
        }

        self.inner
            .raft
            .add_learner(new_node_id, BasicNode::new(new_addr), true)
            .await
            .map_err(|e| self.status_from_raft_error(e))?;

        if current_voters.contains(&old_node_id) {
            let mut next_voters = current_voters;
            next_voters.remove(&old_node_id);
            next_voters.insert(new_node_id);
            self.inner
                .raft
                .change_membership(next_voters, false)
                .await
                .map_err(|e| self.status_from_raft_error(e))?;
        } else {
            self.inner
                .raft
                .change_membership(
                    ChangeMembers::RemoveNodes(BTreeSet::from([old_node_id])),
                    false,
                )
                .await
                .map_err(|e| self.status_from_raft_error(e))?;
        }

        self.cluster_membership().await
    }
}

#[derive(Clone)]
struct MetadataGrpc {
    node: MetadataNode,
}

#[tonic::async_trait]
impl pb::metadata_service_server::MetadataService for MetadataGrpc {
    async fn stat(
        &self,
        request: Request<pb::StatRequest>,
    ) -> Result<Response<pb::FileInfo>, Status> {
        let state = self.node.linearized_state().await?;
        let path = normalize_path(&request.into_inner().path).map_err(invalid_argument)?;
        let Some(entry) = state.entries.get(&path) else {
            return Err(Status::not_found(path));
        };
        Ok(Response::new(entry.info().to_proto()))
    }

    async fn list(
        &self,
        request: Request<pb::ListRequest>,
    ) -> Result<Response<pb::ListResponse>, Status> {
        let state = self.node.linearized_state().await?;
        let entries = state
            .list_directory(&request.into_inner().path)
            .map_err(invalid_argument)?;
        Ok(Response::new(pb::ListResponse {
            entries: entries.into_iter().map(|entry| entry.to_proto()).collect(),
        }))
    }

    async fn mkdir(
        &self,
        request: Request<pb::MkdirRequest>,
    ) -> Result<Response<pb::FileInfo>, Status> {
        let response = self
            .node
            .write_command(MetadataCommand::Mkdir {
                path: request.into_inner().path,
            })
            .await?;
        Ok(Response::new(MetadataNode::response_as_file_info(
            response,
        )?))
    }

    async fn rename(
        &self,
        request: Request<pb::RenameRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        let request = request.into_inner();
        let response = self
            .node
            .write_command(MetadataCommand::Rename {
                from: request.from,
                to: request.to,
                now_ms: now_millis(),
            })
            .await?;
        MetadataNode::response_as_ack(response)?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn delete(
        &self,
        request: Request<pb::DeleteRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        let response = self
            .node
            .write_command(MetadataCommand::Delete {
                path: request.into_inner().path,
                now_ms: now_millis(),
                gc_grace_ms: GC_GRACE_MS,
            })
            .await?;
        MetadataNode::response_as_ack(response)?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn open_file(
        &self,
        request: Request<pb::OpenFileRequest>,
    ) -> Result<Response<pb::FileManifest>, Status> {
        let state = self.node.linearized_state().await?;
        let path = normalize_path(&request.into_inner().path).map_err(invalid_argument)?;
        let Some(NamespaceEntry::File(file)) = state.entries.get(&path) else {
            return Err(Status::not_found(path));
        };
        let manifest = file_manifest_to_proto(&state, file);
        Ok(Response::new(manifest))
    }

    async fn begin_upload(
        &self,
        request: Request<pb::BeginUploadRequest>,
    ) -> Result<Response<pb::UploadSession>, Status> {
        let request = request.into_inner();
        let response = self
            .node
            .write_command(MetadataCommand::BeginUpload {
                upload_id: Uuid::new_v4().to_string(),
                path: request.path,
                mode: UploadModeModel::try_from(request.mode).map_err(invalid_argument)?,
                replication_factor: request.replication_factor.max(1),
                chunk_size: request.chunk_size.max(1),
                now_ms: now_millis(),
                lease_ttl_ms: LEASE_TTL_MS,
            })
            .await?;
        Ok(Response::new(MetadataNode::response_as_upload(response)?))
    }

    async fn allocate_chunk(
        &self,
        request: Request<pb::AllocateChunkRequest>,
    ) -> Result<Response<pb::ChunkPlacement>, Status> {
        let request = request.into_inner();
        let response = self
            .node
            .write_command(MetadataCommand::AllocateChunk {
                upload_id: request.upload_id,
                chunk_id: Uuid::new_v4().to_string(),
                size: request.size,
                checksum: request.checksum,
                now_ms: now_millis(),
            })
            .await?;
        Ok(Response::new(MetadataNode::response_as_chunk_placement(
            response,
        )?))
    }

    async fn commit_upload(
        &self,
        request: Request<pb::CommitUploadRequest>,
    ) -> Result<Response<pb::FileManifest>, Status> {
        let request = request.into_inner();
        let chunks = request
            .chunks
            .into_iter()
            .map(CommitChunkModel::try_from)
            .collect::<Result<Vec<_>>>()
            .map_err(invalid_argument)?;
        let response = self
            .node
            .write_command(MetadataCommand::CommitUpload {
                upload_id: request.upload_id,
                chunks,
                now_ms: now_millis(),
                gc_grace_ms: GC_GRACE_MS,
            })
            .await?;
        Ok(Response::new(match response {
            MetadataResponse::FileManifest(manifest) => {
                let state = self.node.inner.store.state_snapshot().await;
                match state.entries.get(&manifest.info.path) {
                    Some(NamespaceEntry::File(file)) => file_manifest_to_proto(&state, file),
                    _ => manifest.to_proto(|node_id| state.chunk_server_addr(node_id)),
                }
            }
            MetadataResponse::Error(error) => return Err(Status::failed_precondition(error)),
            other => return Err(Status::internal(format!("unexpected response: {other:?}"))),
        }))
    }

    async fn abort_upload(
        &self,
        request: Request<pb::AbortUploadRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        let response = self
            .node
            .write_command(MetadataCommand::AbortUpload {
                upload_id: request.into_inner().upload_id,
            })
            .await?;
        MetadataNode::response_as_ack(response)?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn report_replica_failure(
        &self,
        request: Request<pb::ReportReplicaFailureRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        let request = request.into_inner();
        let response = self
            .node
            .write_command(MetadataCommand::ReportReplicaFailure {
                chunk_id: request.chunk_id,
                node_id: request.node_id,
                reason: request.reason,
            })
            .await?;
        MetadataNode::response_as_ack(response)?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn heartbeat(
        &self,
        request: Request<pb::HeartbeatRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        let request = request.into_inner();
        let inventory = request
            .inventory
            .into_iter()
            .map(|chunk| ChunkInventoryEntry {
                chunk_id: chunk.chunk_id,
                version: chunk.version,
                checksum: chunk.checksum,
                size: chunk.size,
            })
            .collect();

        let response = self
            .node
            .write_command(MetadataCommand::Heartbeat {
                node_id: request.node_id,
                addr: request.addr,
                capacity: request.capacity,
                used: request.used,
                inventory,
                now_ms: now_millis(),
            })
            .await?;
        MetadataNode::response_as_ack(response)?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn get_cluster_membership(
        &self,
        _request: Request<pb::Empty>,
    ) -> Result<Response<pb::ClusterMembership>, Status> {
        Ok(Response::new(self.node.cluster_membership().await?))
    }

    async fn add_metadata_node(
        &self,
        request: Request<pb::AddMetadataNodeRequest>,
    ) -> Result<Response<pb::ClusterMembership>, Status> {
        let request = request.into_inner();
        Ok(Response::new(
            self.node
                .add_metadata_node(request.node_id, request.addr, request.promote_to_voter)
                .await?,
        ))
    }

    async fn remove_metadata_node(
        &self,
        request: Request<pb::RemoveMetadataNodeRequest>,
    ) -> Result<Response<pb::ClusterMembership>, Status> {
        let request = request.into_inner();
        Ok(Response::new(
            self.node
                .remove_metadata_node(request.node_id, request.retain_as_learner)
                .await?,
        ))
    }

    async fn replace_metadata_node(
        &self,
        request: Request<pb::ReplaceMetadataNodeRequest>,
    ) -> Result<Response<pb::ClusterMembership>, Status> {
        let request = request.into_inner();
        Ok(Response::new(
            self.node
                .replace_metadata_node(request.old_node_id, request.new_node_id, request.new_addr)
                .await?,
        ))
    }
}

#[derive(Clone)]
struct InternalRaftGrpc {
    node: MetadataNode,
}

#[tonic::async_trait]
impl pb::raft_service_server::RaftService for InternalRaftGrpc {
    async fn append_entries(
        &self,
        request: Request<pb::JsonEnvelope>,
    ) -> Result<Response<pb::JsonEnvelope>, Status> {
        let rpc = decode_json(request.into_inner().json)?;
        let result = self.node.inner.raft.append_entries(rpc).await;
        Ok(Response::new(pb::JsonEnvelope {
            json: encode_json(&result)?,
        }))
    }

    async fn vote(
        &self,
        request: Request<pb::JsonEnvelope>,
    ) -> Result<Response<pb::JsonEnvelope>, Status> {
        let rpc = decode_json(request.into_inner().json)?;
        let result = self.node.inner.raft.vote(rpc).await;
        Ok(Response::new(pb::JsonEnvelope {
            json: encode_json(&result)?,
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<pb::InstallSnapshotRequest>,
    ) -> Result<Response<pb::JsonEnvelope>, Status> {
        let request = request.into_inner();
        let vote = decode_json(request.vote_json)?;
        let meta = decode_json(request.meta_json)?;
        let snapshot = Snapshot {
            meta,
            snapshot: Cursor::new(request.snapshot),
        };
        let result = self
            .node
            .inner
            .raft
            .install_full_snapshot(vote, snapshot)
            .await;
        Ok(Response::new(pb::JsonEnvelope {
            json: encode_json(&result)?,
        }))
    }
}

#[derive(Clone, Default)]
struct MetaNetworkFactory;

#[derive(Clone)]
struct MetaNetworkClient {
    target_node: BasicNode,
}

impl RaftNetworkFactory<MetaTypeConfig> for MetaNetworkFactory {
    type Network = MetaNetworkClient;

    async fn new_client(&mut self, _target: MetaNodeId, node: &BasicNode) -> Self::Network {
        MetaNetworkClient {
            target_node: node.clone(),
        }
    }
}

impl RaftNetworkV2<MetaTypeConfig> for MetaNetworkClient {
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<openraft::raft::AppendEntriesResponse<MetaTypeConfig>, RPCError<MetaTypeConfig>>
    {
        let mut client = raft_client(&self.target_node.addr).await?;
        let response = client
            .append_entries(pb::JsonEnvelope {
                json: encode_json(&rpc).map_err(network_error)?,
            })
            .await
            .map_err(map_transport_error::<MetaTypeConfig>)?;
        decode_json(response.into_inner().json).map_err(network_error)?
    }

    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<openraft::raft::VoteResponse<MetaTypeConfig>, RPCError<MetaTypeConfig>> {
        let mut client = raft_client(&self.target_node.addr).await?;
        let response = client
            .vote(pb::JsonEnvelope {
                json: encode_json(&rpc).map_err(network_error)?,
            })
            .await
            .map_err(map_transport_error::<MetaTypeConfig>)?;
        decode_json(response.into_inner().json).map_err(network_error)?
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<MetaTypeConfig>,
        snapshot: Snapshot<MetaTypeConfig>,
        _cancel: impl std::future::Future<Output = openraft::error::ReplicationClosed>
        + openraft::OptionalSend
        + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<MetaTypeConfig>, openraft::error::StreamingError<MetaTypeConfig>>
    {
        let mut client =
            raft_client(&self.target_node.addr)
                .await
                .map_err(|error| match error {
                    RPCError::Unreachable(unreachable) => {
                        openraft::error::StreamingError::Unreachable(unreachable)
                    }
                    RPCError::Network(network) => openraft::error::StreamingError::Network(network),
                    RPCError::Timeout(timeout) => openraft::error::StreamingError::Network(
                        NetworkError::from_string(timeout.to_string()),
                    ),
                    RPCError::RemoteError(remote) => openraft::error::StreamingError::Network(
                        NetworkError::from_string(remote.to_string()),
                    ),
                })?;
        let response = client
            .install_snapshot(pb::InstallSnapshotRequest {
                vote_json: encode_json(&vote)
                    .map_err(|e| openraft::error::StreamingError::Network(NetworkError::new(&e)))?,
                meta_json: encode_json(&snapshot.meta)
                    .map_err(|e| openraft::error::StreamingError::Network(NetworkError::new(&e)))?,
                snapshot: snapshot.snapshot.into_inner(),
            })
            .await
            .map_err(|e| openraft::error::StreamingError::Network(NetworkError::new(&e)))?;
        decode_json(response.into_inner().json)
            .map_err(|e| openraft::error::StreamingError::Network(NetworkError::new(&e)))?
    }

    fn backoff(&self) -> openraft::network::Backoff {
        openraft::network::Backoff::new(std::iter::repeat(Duration::from_millis(250)))
    }
}

async fn gc_loop(node: MetadataNode) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    let mut shutdown = node.inner.shutdown.subscribe();
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = interval.tick() => {}
        }
        if !node.inner.raft.is_leader() {
            continue;
        }

        let state = node.inner.store.state_snapshot().await;
        let due = state
            .tombstones
            .values()
            .filter(|tombstone| tombstone.delete_after_unix_ms <= now_millis())
            .cloned()
            .collect::<Vec<_>>();
        if due.is_empty() {
            continue;
        }

        let mut completed = Vec::new();
        for tombstone in due {
            let mut ok = true;
            for replica in &tombstone.replicas {
                let Some(addr) = state.chunk_server_addr(&replica.node_id) else {
                    ok = false;
                    continue;
                };
                if delete_chunk(&addr, &tombstone.chunk_id, tombstone.version)
                    .await
                    .is_err()
                {
                    ok = false;
                }
            }
            if ok {
                completed.push(tombstone.chunk_id.clone());
            }
        }

        if !completed.is_empty() {
            let _ = node
                .write_command(MetadataCommand::AckGarbage {
                    chunk_ids: completed,
                })
                .await;
        }
    }
}

async fn repair_loop(node: MetadataNode) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    let mut shutdown = node.inner.shutdown.subscribe();
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = interval.tick() => {}
        }
        if !node.inner.raft.is_leader() {
            continue;
        }

        let state = node.inner.store.state_snapshot().await;
        let repairs = state.repairs.values().cloned().collect::<Vec<_>>();
        for repair in repairs {
            let Some(record) = state.chunk_records.get(&repair.chunk_id) else {
                continue;
            };
            let Some(source) = record.replicas.first() else {
                continue;
            };
            let Some(source_addr) = state.chunk_server_addr(&source.node_id) else {
                continue;
            };
            let bytes = match fetch_chunk(&source_addr, &record.chunk_id, record.version).await {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };

            let active_targets = active_chunk_servers(&state)
                .into_iter()
                .filter(|server| {
                    !record
                        .replicas
                        .iter()
                        .any(|replica| replica.node_id == server.node_id)
                })
                .collect::<Vec<_>>();

            for target in active_targets {
                if record.replicas.len() >= repair.expected_replicas as usize {
                    break;
                }
                if replicate_chunk(
                    &target.addr,
                    &record.chunk_id,
                    record.version,
                    &record.checksum,
                    bytes.clone(),
                )
                .await
                .is_ok()
                {
                    let _ = node
                        .write_command(MetadataCommand::RecordReplicaRepair {
                            chunk_id: record.chunk_id.clone(),
                            node_id: target.node_id.clone(),
                            version: record.version,
                        })
                        .await;
                }
            }
        }
    }
}

fn apply_command(
    state: &mut MetadataStateMachine,
    command: MetadataCommand,
) -> Result<MetadataResponse> {
    match command {
        MetadataCommand::Mkdir { path } => apply_mkdir(state, &path),
        MetadataCommand::Rename { from, to, now_ms } => apply_rename(state, &from, &to, now_ms),
        MetadataCommand::Delete {
            path,
            now_ms,
            gc_grace_ms,
        } => apply_delete(state, &path, now_ms, gc_grace_ms),
        MetadataCommand::BeginUpload {
            upload_id,
            path,
            mode,
            replication_factor,
            chunk_size,
            now_ms,
            lease_ttl_ms,
        } => apply_begin_upload(
            state,
            BeginUploadArgs {
                upload_id,
                path,
                mode,
                replication_factor,
                chunk_size,
                now_ms,
                lease_ttl_ms,
            },
        ),
        MetadataCommand::AllocateChunk {
            upload_id,
            chunk_id,
            size,
            checksum,
            now_ms,
        } => apply_allocate_chunk(state, &upload_id, &chunk_id, size, &checksum, now_ms),
        MetadataCommand::CommitUpload {
            upload_id,
            chunks,
            now_ms,
            gc_grace_ms,
        } => apply_commit_upload(state, &upload_id, chunks, now_ms, gc_grace_ms),
        MetadataCommand::AbortUpload { upload_id } => {
            state.upload_sessions.remove(&upload_id);
            Ok(MetadataResponse::Ack)
        }
        MetadataCommand::Heartbeat {
            node_id,
            addr,
            capacity,
            used,
            inventory,
            now_ms,
        } => apply_heartbeat(state, node_id, addr, capacity, used, inventory, now_ms),
        MetadataCommand::ReportReplicaFailure {
            chunk_id,
            node_id,
            reason,
        } => apply_report_replica_failure(state, &chunk_id, &node_id, &reason),
        MetadataCommand::RecordReplicaRepair {
            chunk_id,
            node_id,
            version,
        } => apply_record_replica_repair(state, &chunk_id, &node_id, version),
        MetadataCommand::AckGarbage { chunk_ids } => {
            for chunk_id in chunk_ids {
                state.tombstones.remove(&chunk_id);
                state.chunk_records.remove(&chunk_id);
                state.repairs.remove(&chunk_id);
            }
            Ok(MetadataResponse::Ack)
        }
    }
}

struct BeginUploadArgs {
    upload_id: String,
    path: String,
    mode: UploadModeModel,
    replication_factor: u32,
    chunk_size: u32,
    now_ms: u64,
    lease_ttl_ms: u64,
}

fn apply_mkdir(state: &mut MetadataStateMachine, path: &str) -> Result<MetadataResponse> {
    let path = normalize_path(path)?;
    if path == "/" {
        bail!("root already exists");
    }
    if state.entries.contains_key(&path) {
        bail!("path already exists: {path}");
    }
    let parent = parent_path(&path).ok_or_else(|| anyhow::anyhow!("missing parent"))?;
    ensure_directory_exists(state, &parent)?;
    let inode = next_inode(state);
    let info = FileInfoModel {
        inode,
        path: path.clone(),
        version: 0,
        size: 0,
        chunk_size: 0,
        is_dir: true,
    };
    state.entries.insert(
        path.clone(),
        NamespaceEntry::Directory(DirectoryRecord { inode, path }),
    );
    Ok(MetadataResponse::FileInfo(info))
}

fn apply_rename(
    state: &mut MetadataStateMachine,
    from: &str,
    to: &str,
    now_ms: u64,
) -> Result<MetadataResponse> {
    let from = normalize_path(from)?;
    let to = normalize_path(to)?;
    if from == "/" {
        bail!("cannot rename root");
    }
    if from == to {
        return Ok(MetadataResponse::Ack);
    }
    if !state.entries.contains_key(&from) {
        bail!("not found: {from}");
    }
    if state.entries.contains_key(&to) {
        bail!("target already exists: {to}");
    }
    if to.starts_with(&format!("{from}/")) {
        bail!("cannot move directory into itself");
    }
    let parent = parent_path(&to).ok_or_else(|| anyhow::anyhow!("missing target parent"))?;
    ensure_directory_exists(state, &parent)?;
    if has_upload_on_path(state, &from, now_ms) {
        bail!("path has active upload: {from}");
    }

    let affected = state
        .entries
        .keys()
        .filter(|path| **path == from || is_child_of(path, &from))
        .cloned()
        .collect::<Vec<_>>();

    let mut moved = Vec::new();
    for old_path in affected {
        if let Some(entry) = state.entries.remove(&old_path) {
            moved.push((old_path, entry));
        }
    }

    moved.sort_by(|a, b| a.0.len().cmp(&b.0.len()));
    for (old_path, entry) in moved {
        let new_path = replace_prefix(&old_path, &from, &to);
        let new_entry = match entry {
            NamespaceEntry::Directory(dir) => NamespaceEntry::Directory(DirectoryRecord {
                inode: dir.inode,
                path: new_path.clone(),
            }),
            NamespaceEntry::File(mut file) => {
                file.info.path = new_path.clone();
                NamespaceEntry::File(file)
            }
        };
        state.entries.insert(new_path, new_entry);
    }
    Ok(MetadataResponse::Ack)
}

fn apply_delete(
    state: &mut MetadataStateMachine,
    path: &str,
    now_ms: u64,
    gc_grace_ms: u64,
) -> Result<MetadataResponse> {
    let path = normalize_path(path)?;
    if path == "/" {
        bail!("cannot delete root");
    }
    if has_upload_on_path(state, &path, now_ms) {
        bail!("path has active upload");
    }
    let Some(entry) = state.entries.get(&path).cloned() else {
        bail!("not found: {path}");
    };
    match entry {
        NamespaceEntry::Directory(_) => {
            let has_children = state.entries.keys().any(|entry_path| {
                entry_path != &path && parent_path(entry_path).as_deref() == Some(path.as_str())
            });
            if has_children {
                bail!("directory is not empty");
            }
            state.entries.remove(&path);
        }
        NamespaceEntry::File(file) => {
            release_manifest_chunks(state, &file.chunks, now_ms, gc_grace_ms);
            state.entries.remove(&path);
        }
    }
    Ok(MetadataResponse::Ack)
}

fn apply_begin_upload(
    state: &mut MetadataStateMachine,
    args: BeginUploadArgs,
) -> Result<MetadataResponse> {
    let BeginUploadArgs {
        upload_id,
        path,
        mode,
        replication_factor,
        chunk_size,
        now_ms,
        lease_ttl_ms,
    } = args;
    let path = normalize_path(&path)?;
    if replication_factor == 0 {
        bail!("replication_factor must be >= 1");
    }
    if chunk_size == 0 {
        bail!("chunk_size must be >= 1");
    }

    reap_expired_uploads(state, now_ms);
    if has_upload_on_path(state, &path, now_ms) {
        bail!("path already has an active upload");
    }

    let base_version = match (mode, state.entries.get(&path)) {
        (UploadModeModel::Create, None) => None,
        (UploadModeModel::Create, Some(_)) => bail!("path already exists"),
        (UploadModeModel::Overwrite, Some(NamespaceEntry::File(file))) => Some(file.info.version),
        (UploadModeModel::Overwrite, Some(_)) => bail!("path is not a file"),
        (UploadModeModel::Overwrite, None) => bail!("file does not exist"),
    };

    let target_version = base_version.unwrap_or(0) + 1;
    let session = UploadSessionState {
        upload_id,
        path: path.clone(),
        mode,
        base_version,
        target_version,
        lease_expiry_unix_ms: now_ms + lease_ttl_ms,
        chunk_size,
        replication_factor,
        allocations: BTreeMap::new(),
    };
    let response = UploadSessionModel {
        upload_id: session.upload_id.clone(),
        lease_expiry_unix_ms: session.lease_expiry_unix_ms,
        target_version: session.target_version,
        chunk_size: session.chunk_size,
        replication_factor: session.replication_factor,
    };
    state
        .upload_sessions
        .insert(session.upload_id.clone(), session);
    Ok(MetadataResponse::UploadSession(response))
}

fn apply_allocate_chunk(
    state: &mut MetadataStateMachine,
    upload_id: &str,
    chunk_id: &str,
    size: u64,
    checksum: &str,
    now_ms: u64,
) -> Result<MetadataResponse> {
    let active = active_chunk_servers(state);
    let session = state
        .upload_sessions
        .get_mut(upload_id)
        .ok_or_else(|| anyhow::anyhow!("upload session not found"))?;
    if session.lease_expiry_unix_ms <= now_ms {
        state.upload_sessions.remove(upload_id);
        bail!("upload session expired");
    }
    if active.len() < session.replication_factor as usize {
        bail!("not enough active chunkservers");
    }

    let replicas = active
        .into_iter()
        .take(session.replication_factor as usize)
        .map(|server| ChunkReplicaAssignment {
            node_id: server.node_id,
            addr: server.addr,
            version: session.target_version,
        })
        .collect::<Vec<_>>();

    let pending = PendingChunk {
        chunk_id: chunk_id.to_string(),
        size,
        checksum: checksum.to_string(),
        version: session.target_version,
        replicas: replicas.clone(),
    };
    session.allocations.insert(chunk_id.to_string(), pending);
    Ok(MetadataResponse::ChunkPlacement(ChunkPlacementModel {
        chunk_id: chunk_id.to_string(),
        version: session.target_version,
        replicas,
    }))
}

fn apply_commit_upload(
    state: &mut MetadataStateMachine,
    upload_id: &str,
    chunks: Vec<CommitChunkModel>,
    now_ms: u64,
    gc_grace_ms: u64,
) -> Result<MetadataResponse> {
    let session = state
        .upload_sessions
        .get(upload_id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("upload session not found"))?;
    if session.lease_expiry_unix_ms <= now_ms {
        state.upload_sessions.remove(upload_id);
        bail!("upload session expired");
    }

    let mut chunks = chunks;
    chunks.sort_by_key(|chunk| chunk.offset);

    let mut next_offset = 0u64;
    let mut manifest_chunks = Vec::with_capacity(chunks.len());
    for chunk in &chunks {
        if chunk.offset != next_offset {
            bail!("chunks must be contiguous from offset 0");
        }
        let Some(allocation) = session.allocations.get(&chunk.chunk_id) else {
            bail!("chunk {} was not allocated for this upload", chunk.chunk_id);
        };
        if allocation.size != chunk.size || allocation.checksum != chunk.checksum {
            bail!("chunk {} does not match its allocation", chunk.chunk_id);
        }
        let allocated_nodes = allocation
            .replicas
            .iter()
            .map(|replica| replica.node_id.clone())
            .collect::<Vec<_>>();
        let committed_nodes = chunk
            .replicas
            .iter()
            .map(|replica| replica.node_id.clone())
            .collect::<Vec<_>>();
        if allocated_nodes != committed_nodes {
            bail!(
                "chunk {} replica set does not match allocation",
                chunk.chunk_id
            );
        }

        manifest_chunks.push(crate::model::ChunkRefModel {
            chunk_id: chunk.chunk_id.clone(),
            offset: chunk.offset,
            size: chunk.size,
            checksum: chunk.checksum.clone(),
            replicas: chunk
                .replicas
                .iter()
                .map(|replica| ReplicaPointer {
                    node_id: replica.node_id.clone(),
                    version: replica.version,
                })
                .collect(),
        });
        next_offset += chunk.size;
    }

    let old_chunks = state
        .entries
        .get(&session.path)
        .and_then(|entry| match entry {
            NamespaceEntry::File(file) => Some(file.chunks.clone()),
            _ => None,
        });

    let inode = state
        .entries
        .get(&session.path)
        .map(|entry| entry.info().inode)
        .unwrap_or_else(|| next_inode(state));

    let info = FileInfoModel {
        inode,
        path: session.path.clone(),
        version: session.target_version,
        size: next_offset,
        chunk_size: session.chunk_size,
        is_dir: false,
    };
    let manifest = FileManifestModel {
        info: info.clone(),
        chunks: manifest_chunks.clone(),
    };

    for chunk in &manifest_chunks {
        match state.chunk_records.get_mut(&chunk.chunk_id) {
            Some(record) => {
                record.ref_count += 1;
            }
            None => {
                state.chunk_records.insert(
                    chunk.chunk_id.clone(),
                    ChunkRecord {
                        chunk_id: chunk.chunk_id.clone(),
                        size: chunk.size,
                        checksum: chunk.checksum.clone(),
                        version: session.target_version,
                        desired_replication: session.replication_factor,
                        replicas: chunk.replicas.clone(),
                        ref_count: 1,
                    },
                );
            }
        }
    }

    if let Some(old_chunks) = old_chunks {
        release_manifest_chunks(state, &old_chunks, now_ms, gc_grace_ms);
    }

    state.entries.insert(
        session.path.clone(),
        NamespaceEntry::File(FileRecord {
            info: info.clone(),
            chunks: manifest_chunks,
        }),
    );
    state.upload_sessions.remove(upload_id);
    Ok(MetadataResponse::FileManifest(manifest))
}

fn apply_heartbeat(
    state: &mut MetadataStateMachine,
    node_id: String,
    addr: String,
    capacity: u64,
    used: u64,
    inventory: Vec<ChunkInventoryEntry>,
    now_ms: u64,
) -> Result<MetadataResponse> {
    let inventory_map = inventory
        .into_iter()
        .map(|chunk| (chunk.chunk_id.clone(), chunk))
        .collect::<BTreeMap<_, _>>();
    state.chunk_servers.insert(
        node_id.clone(),
        ChunkServerState {
            node_id: node_id.clone(),
            addr,
            capacity,
            used,
            last_heartbeat_unix_ms: now_ms,
            inventory: inventory_map.clone(),
        },
    );

    let known_chunks = state.chunk_records.keys().cloned().collect::<Vec<_>>();

    for chunk_id in known_chunks {
        let Some(record) = state.chunk_records.get_mut(&chunk_id) else {
            continue;
        };
        let previous_had_replica = record
            .replicas
            .iter()
            .any(|replica| replica.node_id == node_id);
        match inventory_map.get(&chunk_id) {
            Some(local) if local.checksum == record.checksum && local.version == record.version => {
                if !previous_had_replica {
                    record.replicas.push(ReplicaPointer {
                        node_id: node_id.clone(),
                        version: local.version,
                    });
                }
                prune_repair(record, &mut state.repairs);
            }
            Some(_) | None if previous_had_replica => {
                record.replicas.retain(|replica| replica.node_id != node_id);
                schedule_repair(state, &chunk_id, "inventory mismatch".to_string());
            }
            _ => {}
        }
    }

    Ok(MetadataResponse::Ack)
}

fn apply_report_replica_failure(
    state: &mut MetadataStateMachine,
    chunk_id: &str,
    node_id: &str,
    reason: &str,
) -> Result<MetadataResponse> {
    let Some(record) = state.chunk_records.get_mut(chunk_id) else {
        bail!("chunk not found: {chunk_id}");
    };
    record.replicas.retain(|replica| replica.node_id != node_id);
    schedule_repair(state, chunk_id, reason.to_string());
    Ok(MetadataResponse::Ack)
}

fn apply_record_replica_repair(
    state: &mut MetadataStateMachine,
    chunk_id: &str,
    node_id: &str,
    version: u64,
) -> Result<MetadataResponse> {
    let Some(record) = state.chunk_records.get_mut(chunk_id) else {
        bail!("chunk not found: {chunk_id}");
    };
    if !record
        .replicas
        .iter()
        .any(|replica| replica.node_id == node_id)
    {
        record.replicas.push(ReplicaPointer {
            node_id: node_id.to_string(),
            version,
        });
    }
    prune_repair(record, &mut state.repairs);
    Ok(MetadataResponse::Ack)
}

fn ensure_directory_exists(state: &MetadataStateMachine, path: &str) -> Result<()> {
    let Some(entry) = state.entries.get(path) else {
        bail!("directory not found: {path}");
    };
    if !matches!(entry, NamespaceEntry::Directory(_)) {
        bail!("not a directory: {path}");
    }
    Ok(())
}

fn next_inode(state: &mut MetadataStateMachine) -> u64 {
    let inode = state.next_inode;
    state.next_inode += 1;
    inode
}

fn has_upload_on_path(state: &MetadataStateMachine, path: &str, now_ms: u64) -> bool {
    state.upload_sessions.values().any(|upload| {
        upload.lease_expiry_unix_ms > now_ms
            && (upload.path == path
                || is_child_of(&upload.path, path)
                || is_child_of(path, &upload.path))
    })
}

fn reap_expired_uploads(state: &mut MetadataStateMachine, now_ms: u64) {
    state
        .upload_sessions
        .retain(|_, session| session.lease_expiry_unix_ms > now_ms);
}

fn release_manifest_chunks(
    state: &mut MetadataStateMachine,
    chunks: &[crate::model::ChunkRefModel],
    now_ms: u64,
    gc_grace_ms: u64,
) {
    for chunk in chunks {
        let mut remove = false;
        if let Some(record) = state.chunk_records.get_mut(&chunk.chunk_id) {
            if record.ref_count > 1 {
                record.ref_count -= 1;
            } else {
                record.ref_count = 0;
                state.tombstones.insert(
                    chunk.chunk_id.clone(),
                    ChunkTombstone {
                        chunk_id: chunk.chunk_id.clone(),
                        version: record.version,
                        replicas: record.replicas.clone(),
                        delete_after_unix_ms: now_ms + gc_grace_ms,
                    },
                );
                remove = true;
            }
        }
        if remove {
            state.repairs.remove(&chunk.chunk_id);
        }
    }
}

fn schedule_repair(state: &mut MetadataStateMachine, chunk_id: &str, reason: String) {
    let Some(record) = state.chunk_records.get(chunk_id) else {
        return;
    };
    if record.ref_count == 0 || record.replicas.len() >= record.desired_replication as usize {
        state.repairs.remove(chunk_id);
        return;
    }
    state.repairs.insert(
        chunk_id.to_string(),
        RepairTask {
            chunk_id: chunk_id.to_string(),
            expected_replicas: record.desired_replication,
            last_error: Some(reason),
        },
    );
}

fn prune_repair(record: &ChunkRecord, repairs: &mut BTreeMap<String, RepairTask>) {
    if record.replicas.len() >= record.desired_replication as usize {
        repairs.remove(&record.chunk_id);
    }
}

fn active_chunk_servers(state: &MetadataStateMachine) -> Vec<ChunkServerState> {
    let now_ms = now_millis();
    let mut servers = state
        .chunk_servers
        .values()
        .filter(|server| {
            now_ms.saturating_sub(server.last_heartbeat_unix_ms) <= ACTIVE_CHUNKSERVER_WINDOW_MS
        })
        .cloned()
        .collect::<Vec<_>>();
    servers.sort_by(|a, b| a.used.cmp(&b.used).then(a.node_id.cmp(&b.node_id)));
    servers
}

fn file_manifest_to_proto(state: &MetadataStateMachine, file: &FileRecord) -> pb::FileManifest {
    pb::FileManifest {
        info: Some(file.info.to_proto()),
        chunks: file
            .chunks
            .iter()
            .map(|chunk| {
                let replicas = state
                    .chunk_records
                    .get(&chunk.chunk_id)
                    .map(|record| &record.replicas)
                    .unwrap_or(&chunk.replicas);
                pb::ChunkRef {
                    chunk_id: chunk.chunk_id.clone(),
                    offset: chunk.offset,
                    size: chunk.size,
                    checksum: chunk.checksum.clone(),
                    replicas: replicas
                        .iter()
                        .filter_map(|replica| {
                            state
                                .chunk_server_addr(&replica.node_id)
                                .map(|addr| pb::ChunkReplica {
                                    node_id: replica.node_id.clone(),
                                    addr,
                                    version: replica.version,
                                })
                        })
                        .collect(),
                }
            })
            .collect(),
    }
}

fn cluster_membership_to_proto(state: &MetadataStateMachine) -> pb::ClusterMembership {
    let membership = state.last_membership.membership();
    let voters = state.last_membership.voter_ids().collect::<BTreeSet<_>>();
    let mut nodes = membership
        .nodes()
        .map(|(id, node)| pb::MetadataNodeInfo {
            id: *id,
            addr: node.addr.clone(),
            voter: voters.contains(id),
        })
        .collect::<Vec<_>>();
    nodes.sort_by_key(|node| node.id);
    pb::ClusterMembership {
        nodes,
        joint: membership.get_joint_config().len() > 1,
    }
}

fn invalid_argument(error: impl ToString) -> Status {
    Status::invalid_argument(error.to_string())
}

fn load_json<T: for<'de> serde::Deserialize<'de>>(db: &DB, key: &[u8]) -> Result<Option<T>> {
    let Some(value) = db.get(key)? else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_slice(&value)?))
}

fn put_json<T: serde::Serialize>(db: &DB, key: impl AsRef<[u8]>, value: &T) -> io::Result<()> {
    let data = serde_json::to_vec(value)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    db.put(key.as_ref(), data).map_err(rocksdb_err_to_io)
}

fn log_key(index: u64) -> String {
    format!("{LOG_PREFIX}{index:020}")
}

fn rocksdb_err_to_io(error: rocksdb::Error) -> io::Error {
    io::Error::other(error.to_string())
}

fn encode_json<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, Status> {
    serde_json::to_vec(value).map_err(|e| Status::internal(e.to_string()))
}

fn decode_json<T: for<'de> serde::Deserialize<'de>>(bytes: Vec<u8>) -> Result<T, Status> {
    serde_json::from_slice(&bytes).map_err(|e| Status::internal(e.to_string()))
}

fn network_error(error: Status) -> RPCError<MetaTypeConfig> {
    RPCError::Network(NetworkError::new(&error))
}

fn map_transport_error<C: openraft::RaftTypeConfig>(error: tonic::Status) -> RPCError<C> {
    RPCError::Unreachable(Unreachable::new(&error))
}

fn map_transport_connect_error<C: openraft::RaftTypeConfig>(
    error: tonic::transport::Error,
) -> RPCError<C> {
    RPCError::Unreachable(Unreachable::new(&error))
}

async fn raft_client(
    addr: &str,
) -> Result<pb::raft_service_client::RaftServiceClient<Channel>, RPCError<MetaTypeConfig>> {
    let endpoint = format!("http://{addr}");
    pb::raft_service_client::RaftServiceClient::connect(endpoint)
        .await
        .map_err(map_transport_connect_error::<MetaTypeConfig>)
}

async fn chunk_client(
    addr: &str,
) -> Result<pb::chunk_service_client::ChunkServiceClient<Channel>, tonic::transport::Error> {
    pb::chunk_service_client::ChunkServiceClient::connect(format!("http://{addr}")).await
}

async fn fetch_chunk(addr: &str, chunk_id: &str, version: u64) -> Result<Vec<u8>> {
    let mut client = chunk_client(addr).await?;
    let mut stream = client
        .get_chunk(pb::GetChunkRequest {
            chunk_id: chunk_id.to_string(),
            version,
        })
        .await?
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(item) = stream.message().await? {
        if let Some(pb::get_chunk_response::Item::Data(data)) = item.item {
            bytes.extend_from_slice(&data);
        }
    }
    Ok(bytes)
}

async fn replicate_chunk(
    addr: &str,
    chunk_id: &str,
    version: u64,
    checksum: &str,
    data: Vec<u8>,
) -> Result<()> {
    let mut client = chunk_client(addr).await?;
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

async fn delete_chunk(addr: &str, chunk_id: &str, version: u64) -> Result<()> {
    let mut client = chunk_client(addr).await?;
    client
        .delete_chunk(pb::DeleteChunkRequest {
            chunk_id: chunk_id.to_string(),
            version,
        })
        .await?;
    Ok(())
}
