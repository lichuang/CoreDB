use crate::base::Request;
use crate::base::Response;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
);

pub type Raft = openraft::Raft<TypeConfig>;
