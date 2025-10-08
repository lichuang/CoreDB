// mod log_store;
// mod state_machine;

use std::io::Cursor;

use openraft::StorageError;

use crate::base::Node;
use crate::base::NodeId;
use crate::base::Request;
use crate::base::Response;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        Node = Node,
);

type StorageResult<T> = Result<T, StorageError<NodeId>>;
