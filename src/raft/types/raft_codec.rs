use std::error::Error;

use openraft::StorageError;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use prost::Message;

use crate::raft::types::raft_types::TypeConfig;

pub(crate) trait RaftCodec {
  fn decode_from(buf: &[u8]) -> Result<Self, StorageError<TypeConfig>>
  where Self: Sized;
  fn encode_to(&self) -> Result<Vec<u8>, StorageError<TypeConfig>>;
}

impl RaftCodec for LogIdOf<TypeConfig> {
  fn decode_from(buf: &[u8]) -> Result<Self, StorageError<TypeConfig>>
  where Self: Sized {
    let log_id = crate::raft::protobuf::LogId::decode(buf).map_err(read_logs_err)?;

    Ok(Self {
      leader_id: log_id.term,
      index: log_id.index,
    })
  }

  fn encode_to(&self) -> Result<Vec<u8>, StorageError<TypeConfig>> {
    let log_id = crate::raft::protobuf::LogId {
      term: self.leader_id,
      index: self.index,
    };

    Ok(log_id.encode_to_vec())
  }
}

impl RaftCodec for VoteOf<TypeConfig> {
  fn decode_from(buf: &[u8]) -> Result<Self, StorageError<TypeConfig>>
  where Self: Sized {
    Ok(crate::raft::protobuf::Vote::decode(buf).map_err(read_logs_err)?)
  }

  fn encode_to(&self) -> Result<Vec<u8>, StorageError<TypeConfig>> {
    Ok(self.encode_to_vec())
  }
}

pub fn read_logs_err(e: impl Error + 'static) -> StorageError<TypeConfig> {
  StorageError::read_logs(&e)
}
