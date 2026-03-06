use std::io::Cursor;

openraft::declare_raft_types!(
    pub MetaTypeConfig:
        D = crate::model::MetadataCommand,
        R = crate::model::MetadataResponse,
        Node = openraft::BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
        LeaderId = openraft::impls::leader_id_std::LeaderId<Self>,
);

pub type MetaNodeId = u64;
pub type MetaRaft = openraft::Raft<MetaTypeConfig>;
