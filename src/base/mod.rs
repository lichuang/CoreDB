use serde::{Deserialize, Serialize};

pub type Result<T> = anyhow::Result<T>;
pub type Error = anyhow::Error;

pub fn to_error(s: impl ToString) -> Error {
  anyhow::anyhow!(s.to_string())
}

pub type NodeId = u64;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct Node {
  pub rpc_addr: String,
  pub api_addr: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
  Set { key: String, value: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
  pub value: Option<String>,
}
