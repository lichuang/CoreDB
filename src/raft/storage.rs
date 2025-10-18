use std::path::Path;
use std::sync::Arc;

use rocksdb::ColumnFamilyDescriptor;
use rocksdb::DB;
use rocksdb::Options;

use super::declare_types::TypeConfig;
use super::log_store::RocksLogStore;
use super::state_machine::RocksStateMachine;

pub async fn new_raft_storage<P: AsRef<Path>>(
  db_path: P,
) -> (RocksLogStore<TypeConfig>, RocksStateMachine) {
  let mut db_opts = Options::default();
  db_opts.create_missing_column_families(true);
  db_opts.create_if_missing(true);

  let store = ColumnFamilyDescriptor::new("store", Options::default());
  let meta = ColumnFamilyDescriptor::new("meta", Options::default());
  let logs = ColumnFamilyDescriptor::new("logs", Options::default());

  let snapshot_dir = db_path.as_ref().to_path_buf().join("snapshot");

  let db = DB::open_cf_descriptors(&db_opts, db_path, vec![store, meta, logs]).unwrap();
  let db = Arc::new(db);

  let log_store = RocksLogStore::new(db.clone());
  let sm_store = RocksStateMachine::new(db, snapshot_dir).await.unwrap();

  (log_store, sm_store)
}
