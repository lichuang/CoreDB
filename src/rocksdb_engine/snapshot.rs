use std::sync::Arc;

use rocksdb::DB;
use rocksdb::rocksdb_options::UnsafeSnap;

pub struct RocksSnapshot {
  db: Arc<DB>,
  snap: UnsafeSnap,
}

impl RocksSnapshot {
  pub fn new(db: Arc<DB>) -> Self {
    unsafe {
      RocksSnapshot {
        snap: db.unsafe_snap(),
        db,
      }
    }
  }
}
