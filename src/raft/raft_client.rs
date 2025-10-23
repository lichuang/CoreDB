use databend_common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use databend_common_meta_types::raft_types::NodeId;
use log::debug;
use tonic::transport::channel::Channel;

use super::endpoint::Endpoint;

/// A metrics reporter of active raft peers.
#[derive(Debug)]
pub struct PeerCounter {
  target: NodeId,
  endpoint: Endpoint,
  endpoint_str: String,
}

/// RaftClient is a grpc client bound with a metrics reporter..
pub type RaftClient = count::WithCount<PeerCounter, RaftServiceClient<Channel>>;

/// Defines the API of the client to a raft node.
pub trait RaftClientApi {
  fn new(target: NodeId, endpoint: Endpoint, channel: Channel) -> Self;
  fn endpoint(&self) -> &Endpoint;
}

impl RaftClientApi for RaftClient {
  fn new(target: NodeId, endpoint: Endpoint, channel: Channel) -> Self {
    let endpoint_str = endpoint.to_string();

    debug!(
      "RaftClient::new: target: {} endpoint: {}",
      target, endpoint_str
    );

    let cli = RaftServiceClient::new(channel)
      .max_decoding_message_size(GrpcConfig::MAX_DECODING_SIZE)
      .max_encoding_message_size(GrpcConfig::MAX_ENCODING_SIZE);
    count::WithCount::new(cli, PeerCounter {
      target,
      endpoint,
      endpoint_str,
    })
  }

  fn endpoint(&self) -> &Endpoint {
    &self.counter().endpoint
  }
}
