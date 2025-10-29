mod endpoint;
mod network;
mod store;
mod types;
// mod raft_client;
// mod raft_types;
// mod state_machine;
// mod storage;

#[allow(clippy::all)]
pub mod protobuf {
  tonic::include_proto!("openraftpb");
}

pub use types::raft_types::NodeId;
pub use types::raft_types::Raft;

use crate::base::Result;
// pub use storage::new_raft_storage;

// pub async fn new_raft<P: AsRef<std::path::Path>>(
// node_id: NodeId,
// dir: P,
// addr: String,
// ) -> Result<Raft> {
// let network =
// }
