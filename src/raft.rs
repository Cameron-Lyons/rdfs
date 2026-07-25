openraft::declare_raft_types!(
    pub MetaTypeConfig:
        D = crate::model::MetadataCommand,
        R = crate::model::MetadataResponse,
        Node = openraft::BasicNode,
        LeaderId = openraft::impls::leader_id_std::LeaderId<Self::Term, Self::NodeId>,
);

pub type MetaNodeId = u64;
pub type MetaRaft = openraft::Raft<MetaTypeConfig, std::sync::Arc<crate::meta::MetaStore>>;
