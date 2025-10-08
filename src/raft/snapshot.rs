use std::sync::Arc;

use crate::base::Result;
use rocksdb::DB;

pub struct Snapshot {
  db: Arc<DB>,

  path: String,
}

impl Snapshot {
  fn new(db: Arc<DB>, data_dir: &str) -> Self {
    Self {
      db,
      path: data_dir.to_string(),
    }
  }

  fn save() -> Result<String> {}
}
