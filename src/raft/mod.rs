mod log_store;

use crate::base::Node;
use crate::base::Request;
use crate::base::Response;

use std::io::Cursor;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        Node = Node,
);
