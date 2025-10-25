use crate::raft::protobuf as pb;
use crate::raft::protobuf::snapshot_request::Payload;

impl pb::SnapshotRequest {
  pub fn into_meta(self) -> Option<pb::SnapshotRequestMeta> {
    let p = self.payload?;
    match p {
      Payload::Meta(meta) => Some(meta),
      Payload::Chunk(_) => None,
    }
  }

  pub fn into_data_chunk(self) -> Option<Vec<u8>> {
    let p = self.payload?;
    match p {
      Payload::Meta(_) => None,
      Payload::Chunk(chunk) => Some(chunk),
    }
  }
}
