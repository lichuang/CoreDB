use std::error::Error;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::Arc;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use meta::StoreMeta;
use meta::StoreMetaValue as _;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::RaftTypeConfig;
use openraft::StorageError;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogStorage;
use prost::Message;
use rocksdb::ColumnFamily;
use rocksdb::DB;
use rocksdb::Direction;
use tokio::task::spawn_blocking;

use crate::raft::protobuf as pb;
use crate::raft::raft_types::LogState;
use crate::raft::raft_types::TypeConfig;

#[derive(Debug, Clone)]
pub struct RocksLogStore<C>
where C: RaftTypeConfig
{
  db: Arc<DB>,
  _p: PhantomData<C>,
}

impl RocksLogStore<TypeConfig> {
  pub fn new(db: Arc<DB>) -> Self {
    db.cf_handle("meta")
      .expect("column family `meta` not found");
    db.cf_handle("logs")
      .expect("column family `logs` not found");

    Self {
      db,
      _p: Default::default(),
    }
  }

  fn cf_meta(&self) -> &ColumnFamily {
    self.db.cf_handle("meta").unwrap()
  }

  fn cf_logs(&self) -> &ColumnFamily {
    self.db.cf_handle("logs").unwrap()
  }

  /// Get a store metadata.
  ///
  /// It returns `None` if the store does not have such a metadata stored.
  fn get_meta<M: StoreMeta>(&self) -> Result<Option<M::Value>, StorageError<TypeConfig>> {
    let bytes = self
      .db
      .get_cf(self.cf_meta(), M::KEY)
      .map_err(M::read_err)?;

    let Some(bytes) = bytes else {
      return Ok(None);
    };

    let entry = M::Value::decode_from(bytes.as_ref()).map_err(read_logs_err)?;

    Ok(Some(entry))
  }

  /// Save a store metadata.
  fn put_meta<M: StoreMeta>(&self, value: &M::Value) -> Result<(), StorageError<TypeConfig>> {
    // let json_value = serde_json::to_vec(value).map_err(|e| M::write_err(value, e))?;
    let encode_valude = value.encode_to()?;

    self
      .db
      .put_cf(self.cf_meta(), M::KEY, encode_valude)
      .map_err(|e| M::write_err(value, e))?;

    Ok(())
  }
}

impl RaftLogReader<TypeConfig> for RocksLogStore<TypeConfig> {
  async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
    &mut self,
    range: RB,
  ) -> Result<Vec<crate::raft::raft_types::Entry>, StorageError<TypeConfig>> {
    let start = match range.start_bound() {
      std::ops::Bound::Included(x) => id_to_bin(*x),
      std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
      std::ops::Bound::Unbounded => id_to_bin(0),
    };

    let mut res = Vec::new();

    let it = self.db.iterator_cf(
      self.cf_logs(),
      rocksdb::IteratorMode::From(&start, Direction::Forward),
    );
    for item_res in it {
      let (id, val) = item_res.map_err(read_logs_err)?;

      let id = bin_to_id(&id);
      if !range.contains(&id) {
        break;
      }

      let entry = pb::Entry::decode(val.as_ref()).map_err(read_logs_err)?;

      assert_eq!(id, entry.index());

      res.push(entry);
    }
    Ok(res)
  }

  async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, StorageError<TypeConfig>> {
    self.get_meta::<meta::Vote>()
  }
}

// It requires TokioRuntime because it uses spawn_blocking internally.
impl RaftLogStorage<TypeConfig> for RocksLogStore<TypeConfig> {
  type LogReader = Self;

  async fn get_log_state(&mut self) -> Result<LogState, StorageError<TypeConfig>> {
    let last = self
      .db
      .iterator_cf(self.cf_logs(), rocksdb::IteratorMode::End)
      .next();

    let last_log_id = match last {
      None => None,
      Some(res) => {
        let (_log_index, val) = res.map_err(read_logs_err)?;
        let entry = pb::Entry::decode(val.as_ref()).map_err(read_logs_err)?;
        Some(entry.log_id())
      }
    };

    let last_purged_log_id = self.get_meta::<meta::LastPurged>()?;

    let last_log_id = match last_log_id {
      None => last_purged_log_id.clone(),
      Some(x) => Some(x),
    };

    Ok(LogState {
      last_purged_log_id,
      last_log_id,
    })
  }

  async fn get_log_reader(&mut self) -> Self::LogReader {
    self.clone()
  }

  async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), StorageError<TypeConfig>> {
    self.put_meta::<meta::Vote>(vote)?;

    // Vote must be persisted to disk before returning.
    let db = self.db.clone();
    spawn_blocking(move || db.flush_wal(true))
      .await
      .map_err(|e| StorageError::write_vote(&std::io::Error::other(e.to_string())))?
      .map_err(|e| StorageError::write_vote(&e))?;

    Ok(())
  }

  async fn append<I>(
    &mut self,
    entries: I,
    callback: IOFlushed<TypeConfig>,
  ) -> Result<(), StorageError<TypeConfig>>
  where
    I: IntoIterator<Item = EntryOf<TypeConfig>> + Send,
  {
    for entry in entries {
      let id = id_to_bin(entry.index());
      self
        .db
        .put_cf(self.cf_logs(), id, entry.encode_to_vec())
        .map_err(|e| StorageError::write_logs(&e))?;
    }

    // Make sure the logs are persisted to disk before invoking the callback.
    //
    // But the above `pub_cf()` must be called in this function, not in another task.
    // Because when the function returns, it requires the log entries can be read.
    let db = self.db.clone();
    let handle = spawn_blocking(move || {
      let res = db.flush_wal(true).map_err(std::io::Error::other);
      callback.io_completed(res);
    });
    drop(handle);

    // Return now, and the callback will be invoked later when IO is done.
    Ok(())
  }

  async fn truncate(
    &mut self,
    log_id: LogIdOf<TypeConfig>,
  ) -> Result<(), StorageError<TypeConfig>> {
    tracing::debug!("truncate: [{:?}, +oo)", log_id);

    let from = id_to_bin(log_id.index());
    let to = id_to_bin(u64::MAX);
    self
      .db
      .delete_range_cf(self.cf_logs(), &from, &to)
      .map_err(|e| StorageError::write_logs(&e))?;

    // Truncating does not need to be persisted.
    Ok(())
  }

  async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), StorageError<TypeConfig>> {
    tracing::debug!("delete_log: [0, {:?}]", log_id);

    // Write the last-purged log id before purging the logs.
    // The logs at and before last-purged log id will be ignored by openraft.
    // Therefore, there is no need to do it in a transaction.
    self.put_meta::<meta::LastPurged>(&log_id)?;

    let from = id_to_bin(0);
    let to = id_to_bin(log_id.index() + 1);
    self
      .db
      .delete_range_cf(self.cf_logs(), &from, &to)
      .map_err(|e| StorageError::write_logs(&e))?;

    // Purging does not need to be persistent.
    Ok(())
  }
}

/// Metadata of a raft-store.
///
/// In raft, except logs and state machine, the store also has to store several piece of metadata.
/// This sub mod defines the key-value pairs of these metadata.
mod meta {
  use openraft::AnyError;
  use openraft::ErrorSubject;
  use openraft::ErrorVerb;
  use openraft::StorageError;
  use openraft::alias::LogIdOf;
  use openraft::alias::VoteOf;
  use prost::Message;

  use super::read_logs_err;
  use crate::raft::raft_types::TypeConfig;

  pub(crate) trait StoreMetaValue {
    fn decode_from(buf: &[u8]) -> Result<Self, StorageError<TypeConfig>>
    where Self: Sized;
    fn encode_to(&self) -> Result<Vec<u8>, StorageError<TypeConfig>>;
  }

  /// Defines metadata key and value
  pub(crate) trait StoreMeta {
    /// The key used to store in rocksdb
    const KEY: &'static str;

    /// The type of the value to store
    type Value: StoreMetaValue;

    /// The subject this meta belongs to, and will be embedded into the returned storage error.
    fn subject(v: Option<&Self::Value>) -> ErrorSubject<TypeConfig>;

    fn read_err(e: impl std::error::Error + 'static) -> StorageError<TypeConfig> {
      StorageError::new(Self::subject(None), ErrorVerb::Read, AnyError::new(&e))
    }

    fn write_err(v: &Self::Value, e: impl std::error::Error + 'static) -> StorageError<TypeConfig> {
      StorageError::new(Self::subject(Some(v)), ErrorVerb::Write, AnyError::new(&e))
    }
  }

  pub(crate) struct LastPurged {}
  pub(crate) struct Vote {}

  impl StoreMetaValue for LogIdOf<TypeConfig> {
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

  impl StoreMetaValue for VoteOf<TypeConfig> {
    fn decode_from(buf: &[u8]) -> Result<Self, StorageError<TypeConfig>>
    where Self: Sized {
      Ok(crate::raft::protobuf::Vote::decode(buf).map_err(read_logs_err)?)
    }

    fn encode_to(&self) -> Result<Vec<u8>, StorageError<TypeConfig>> {
      Ok(self.encode_to_vec())
    }
  }

  impl StoreMeta for LastPurged {
    const KEY: &'static str = "last_purged_log_id";
    type Value = LogIdOf<TypeConfig>;

    fn subject(_v: Option<&Self::Value>) -> ErrorSubject<TypeConfig> {
      ErrorSubject::Store
    }
  }

  impl StoreMeta for Vote {
    const KEY: &'static str = "vote";
    type Value = VoteOf<TypeConfig>;

    fn subject(_v: Option<&Self::Value>) -> ErrorSubject<TypeConfig> {
      ErrorSubject::Vote
    }
  }
}

/// converts an id to a byte vector for storing in the database.
/// Note that we're using big endian encoding to ensure correct sorting of keys
fn id_to_bin(id: u64) -> Vec<u8> {
  let mut buf = Vec::with_capacity(8);
  buf.write_u64::<BigEndian>(id).unwrap();
  buf
}

fn bin_to_id(buf: &[u8]) -> u64 {
  (&buf[0..8]).read_u64::<BigEndian>().unwrap()
}

fn read_logs_err(e: impl Error + 'static) -> StorageError<TypeConfig> {
  StorageError::read_logs(&e)
}
