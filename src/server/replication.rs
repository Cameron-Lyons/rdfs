use crate::protocol::{
    PROTOCOL_VERSION, RpcEnvelope, RpcRequest, RpcResponse, StorageNode, auth_token, recv_response,
    send_envelope,
};
use crate::server::metadata::MetadataStore;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::net::TcpStream;

pub struct ReplicationManager {
    metadata_store: Arc<MetadataStore>,
    default_replication_factor: usize,
}

impl ReplicationManager {
    pub fn new(metadata_store: Arc<MetadataStore>, default_replication_factor: usize) -> Self {
        Self {
            metadata_store,
            default_replication_factor,
        }
    }

    pub async fn ensure_replication(&self, file_id: u64) -> Result<(), String> {
        let layout = self
            .metadata_store
            .get_file_layout(file_id)
            .await
            .map_err(|e| e.to_string())?;

        let replication_factor = if layout.handle.replication_factor == 0 {
            self.default_replication_factor
        } else {
            layout.handle.replication_factor
        };

        for block in layout.blocks {
            if block.replicas.len() >= replication_factor {
                continue;
            }

            let source = block
                .replicas
                .iter()
                .max_by_key(|replica| replica.version)
                .cloned();
            let Some(source_replica) = source else {
                continue;
            };

            let source_data = read_from_node(&source_replica.addr, block.block_id).await?;
            let existing_nodes: HashSet<String> =
                block.replicas.iter().map(|r| r.node_id.clone()).collect();
            let needed = replication_factor.saturating_sub(block.replicas.len());

            let targets = self
                .metadata_store
                .select_nodes_for_replication_excluding(needed, &existing_nodes)
                .await;

            for target in targets {
                let node = StorageNode {
                    id: target.id.clone(),
                    addr: target.addr.clone(),
                };

                match write_to_node(
                    &node,
                    block.block_id,
                    source_data.version,
                    &source_data.checksum,
                    &source_data.data,
                )
                .await
                {
                    Ok(version) => {
                        let _ = self
                            .metadata_store
                            .add_block_replica(file_id, block.block_id, target.id, version)
                            .await;
                    }
                    Err(e) => {
                        eprintln!(
                            "Replication write failed for block {} to {}: {}",
                            block.block_id, node.addr, e
                        );
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn handle_node_failure(&self, failed_node_id: &str) -> Result<(), String> {
        self.metadata_store.mark_node_failed(failed_node_id).await;
        self.metadata_store
            .remove_node_replicas(failed_node_id)
            .await;

        let file_ids = self.metadata_store.all_file_ids().await;
        for file_id in file_ids {
            if let Err(e) = self.ensure_replication(file_id).await {
                eprintln!("Replication check failed for file_id={file_id}: {e}");
            }
        }

        Ok(())
    }
}

struct ReadResult {
    data: Vec<u8>,
    checksum: String,
    version: u64,
}

async fn read_from_node(node_addr: &str, block_id: u64) -> Result<ReadResult, String> {
    let mut stream = TcpStream::connect(node_addr)
        .await
        .map_err(|e| format!("failed to connect to {node_addr}: {e}"))?;

    let envelope = RpcEnvelope {
        version: PROTOCOL_VERSION,
        request_id: 0,
        token: auth_token(),
        payload: RpcRequest::ReadBlock { block_id },
    };

    send_envelope(&mut stream, &envelope)
        .await
        .map_err(|e| format!("failed to send read request: {e}"))?;

    let response = recv_response(&mut stream)
        .await
        .map_err(|e| format!("failed to receive read response: {e}"))?;

    match response {
        RpcResponse::BlockPayload { block } => Ok(ReadResult {
            data: block.data,
            checksum: block.checksum,
            version: block.version,
        }),
        RpcResponse::Error { error } => Err(error.message),
        _ => Err("unexpected response while reading block".to_string()),
    }
}

async fn write_to_node(
    node: &StorageNode,
    block_id: u64,
    version: u64,
    checksum: &str,
    data: &[u8],
) -> Result<u64, String> {
    let mut stream = TcpStream::connect(&node.addr)
        .await
        .map_err(|e| format!("failed to connect to {}: {}", node.addr, e))?;

    let envelope = RpcEnvelope {
        version: PROTOCOL_VERSION,
        request_id: 0,
        token: auth_token(),
        payload: RpcRequest::WriteBlock {
            block_id,
            version,
            checksum: checksum.to_string(),
            data: data.to_vec(),
        },
    };

    send_envelope(&mut stream, &envelope)
        .await
        .map_err(|e| format!("failed to send write request: {e}"))?;

    let response = recv_response(&mut stream)
        .await
        .map_err(|e| format!("failed to receive write response: {e}"))?;

    match response {
        RpcResponse::WriteAck { replica } => Ok(replica.version),
        RpcResponse::Error { error } => Err(error.message),
        _ => Err("unexpected response while writing block".to_string()),
    }
}
