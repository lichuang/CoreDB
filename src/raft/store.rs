#[derive(Clone)]
pub struct RaftStore {
  pub id: NodeId,

  pub(crate) config: Arc<RaftConfig>,

  log: MetaRaftLog,

  state_machine: MetaRaftStateMachine,
}
