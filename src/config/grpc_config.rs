/// Grpc default configuration.
pub struct GrpcConfig {}

impl GrpcConfig {
  /// The hard limit of maximum message size the client or server can **send**.
  pub const MAX_ENCODING_SIZE: usize = 16 * 1024 * 1024;

  /// The hard limit of maximum message size the client or server can **receive**.
  pub const MAX_DECODING_SIZE: usize = 16 * 1024 * 1024;

  /// The advisory maximum message size the client or server can **send**.
  pub const fn advisory_encoding_size() -> usize {
    Self::MAX_ENCODING_SIZE * 9 / 10
  }

  /// The advisory maximum message size the client or server can **receive**.
  pub const fn advisory_decoding_size() -> usize {
    Self::MAX_DECODING_SIZE * 9 / 10
  }
}
