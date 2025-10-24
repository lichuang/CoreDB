use serde::Deserialize;
use serde::Serialize;

// ProtoBuf generated files.
#[allow(clippy::all)]
pub mod protobuf {
  tonic::include_proto!("proto");
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
);

pub type Raft = openraft::Raft<TypeConfig>;

pub type NodeId = u64;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct Node {
  pub rpc_addr: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
  Set { key: String, value: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
  pub value: Option<String>,
}
