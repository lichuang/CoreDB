use std::sync::Arc;

use rocksdb::DB;

#[derive(Clone, Debug)]
pub struct RocksEngine {
  db: Arc<DB>,
  support_multi_batch_write: bool,
  //pub ingest_latch: Arc<RangeLatch>,
}

impl RocksEngine {
  pub fn new(db: DB) -> RocksEngine {
    let db = Arc::new(db);
    RocksEngine {
      support_multi_batch_write: db.get_db_options().is_enable_multi_batch_write(),
      db,
    }
  }

  pub fn as_inner(&self) -> &Arc<DB> {
    &self.db
  }

  pub fn get_sync_db(&self) -> Arc<DB> {
    self.db.clone()
  }

  pub fn support_multi_batch_write(&self) -> bool {
    self.support_multi_batch_write
  }
}

impl Iterable for RocksEngine {
  type Iterator = RocksEngineIterator;

  fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
    let handle = get_cf_handle(&self.db, cf)?;
    let opt: RocksReadOptions = opts.into();
    Ok(RocksEngineIterator::from_raw(DBIterator::new_cf(
      self.db.clone(),
      handle,
      opt.into_raw(),
    )))
  }
}
