mod endpoint;
mod network;
mod store;
pub mod types;
// mod raft_client;
// mod raft_types;
// mod state_machine;
// mod storage;

#[allow(clippy::all)]
pub mod protobuf {
  tonic::include_proto!("openraftpb");
}

use std::sync::Arc;

use network::NetworkFactory;
pub use types::raft_types::*;

use crate::errors::Result;
pub(crate) use crate::raft::store::new_storage;
// pub use storage::new_raft_storage;

pub async fn new_raft<P: AsRef<std::path::Path>>(
  node_id: NodeId,
  dir: P,
  addr: String,
) -> Result<Raft> {
  let config = Arc::new(openraft::Config::default());
  let network = NetworkFactory::new();

  let (log_store, state_machine) = new_storage(dir).await?;

  let ret = Raft::new(1, config, network, log_store, state_machine).await;
  match ret {
    Ok(raft) => Ok(raft),
    Err(e) => {
      let open_raft_err: OpenRaftError = e.into();
      Err(open_raft_err.into())
    }
  }
}
