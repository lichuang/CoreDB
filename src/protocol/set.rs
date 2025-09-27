use std::time::Duration;

use bytes::Bytes;

#[derive(Debug)]
pub struct Set {
  key: String,

  value: Bytes,

  expire: Option<Duration>,
}
