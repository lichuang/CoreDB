use super::raft_types::NodeId;

#[derive(Clone)]
pub struct RaftStore {
  pub id: NodeId,

  pub(crate) config: Arc<RaftConfig>,

  state_machine: MetaRaftStateMachine,
}
