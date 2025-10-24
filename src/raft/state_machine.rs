//! This rocks-db backed storage implement the v2 storage API: [`RaftLogStorage`] and
//! [`RaftStateMachine`] traits. The state machine stores all data directly in RocksDB,
//! providing full persistence. Log entries are applied directly to disk, and snapshots
//! use RocksDB's snapshot mechanism for consistent point-in-time views.

use std::fmt::Debug;
use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::AnyError;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::RaftSnapshotBuilder;
use openraft::RaftTypeConfig;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StoredMembership;
use openraft::alias::SnapshotDataOf;
use openraft::entry::RaftEntry;
use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use rand::Rng;
use rocksdb::ColumnFamilyDescriptor;
use rocksdb::DB;
use rocksdb::Options;
use serde::Deserialize;
use serde::Serialize;
use tokio::task::spawn_blocking;

use super::log_store::RocksLogStore;
use super::raft_types::TypeConfig;
use crate::raft::raft_types::Request;
use crate::raft::raft_types::Response;

/// State machine backed by RocksDB for full persistence.
/// All application data is stored directly in the `sm_data` column family.
/// Snapshots are persisted to the `snapshot_dir` directory.
#[derive(Debug, Clone)]
pub struct RocksStateMachine {
  db: Arc<DB>,
  snapshot_dir: PathBuf,
}

impl RocksStateMachine {
  pub async fn new(
    db: Arc<DB>,
    snapshot_dir: PathBuf,
  ) -> Result<RocksStateMachine, std::io::Error> {
    // Validate column families exist at construction time
    db.cf_handle("sm_meta")
      .ok_or_else(|| std::io::Error::other("column family `sm_meta` not found"))?;
    db.cf_handle("sm_data")
      .ok_or_else(|| std::io::Error::other("column family `sm_data` not found"))?;

    // Create snapshot directory if it doesn't exist
    fs::create_dir_all(&snapshot_dir)?;

    Ok(Self { db, snapshot_dir })
  }

  fn cf_sm_meta(&self) -> &rocksdb::ColumnFamily {
    self.db.cf_handle("sm_meta").unwrap()
  }

  fn cf_sm_data(&self) -> &rocksdb::ColumnFamily {
    self.db.cf_handle("sm_data").unwrap()
  }

  #[allow(clippy::type_complexity)]
  fn get_meta(
    &self,
  ) -> Result<(Option<LogId<TypeConfig>>, StoredMembership<TypeConfig>), StorageError<TypeConfig>>
  {
    let cf = self.cf_sm_meta();

    let last_applied_log = self
      .db
      .get_cf(cf, "last_applied_log")
      .map_err(|e| StorageError::read(&e))?
      .map(|bytes| deserialize(&bytes))
      .transpose()?;

    let last_membership = self
      .db
      .get_cf(cf, "last_membership")
      .map_err(|e| StorageError::read(&e))?
      .map(|bytes| deserialize(&bytes))
      .transpose()?
      .unwrap_or_default();

    Ok((last_applied_log, last_membership))
  }
}

fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, StorageError<TypeConfig>> {
  serde_json::to_vec(value).map_err(|e| StorageError::write(&e))
}

fn deserialize<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, StorageError<TypeConfig>> {
  serde_json::from_slice(bytes).map_err(|e| StorageError::read(&e))
}

/// Snapshot file format: metadata + data stored together
#[derive(Serialize, Deserialize)]
struct SnapshotFile {
  meta: SnapshotMeta<TypeConfig>,
  data: Vec<(Vec<u8>, Vec<u8>)>,
}

impl RaftSnapshotBuilder<TypeConfig> for RocksStateMachine {
  #[tracing::instrument(level = "trace", skip(self))]
  async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<TypeConfig>> {
    let (last_applied_log, last_membership) = self.get_meta()?;

    // Generate a random snapshot index.
    let snapshot_idx: u64 = rand::rng().random_range(0..1000);

    let snapshot_id = if let Some(last) = last_applied_log {
      format!(
        "{}-{}-{}",
        last.committed_leader_id(),
        last.index(),
        snapshot_idx
      )
    } else {
      format!("--{}", snapshot_idx)
    };

    let meta = SnapshotMeta {
      last_log_id: last_applied_log,
      last_membership,
      snapshot_id: snapshot_id.clone(),
    };

    // Use RocksDB snapshot for consistent point-in-time view
    let db = self.db.clone();
    let meta_clone = meta.clone();

    let data = spawn_blocking(move || {
      let snapshot = db.snapshot();
      let cf_data = db
        .cf_handle("sm_data")
        .expect("column family `sm_data` not found");

      let mut snapshot_data = Vec::new();
      let iter = snapshot.iterator_cf(cf_data, rocksdb::IteratorMode::Start);

      for item in iter {
        let (key, value) =
          item.map_err(|e| StorageError::read_snapshot(Some(meta_clone.signature()), &e))?;
        snapshot_data.push((key.to_vec(), value.to_vec()));
      }

      Ok(snapshot_data)
    })
    .await
    .map_err(|e| {
      StorageError::read_snapshot(
        Some(meta.signature()),
        &std::io::Error::other(e.to_string()),
      )
    })??;

    // Serialize both metadata and data together
    let snapshot_file = SnapshotFile {
      meta: meta.clone(),
      data: data.clone(),
    };
    let file_bytes = serialize(&snapshot_file)
      .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), AnyError::new(&e)))?;

    // Write complete snapshot to file
    let snapshot_path = self.snapshot_dir.join(&snapshot_id);
    fs::write(&snapshot_path, &file_bytes)
      .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;

    // Return snapshot with data-only for backward compatibility with the data field
    let data_bytes = serialize(&data)
      .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), AnyError::new(&e)))?;

    Ok(Snapshot {
      meta,
      snapshot: Cursor::new(data_bytes),
    })
  }
}

impl RaftStateMachine<TypeConfig> for RocksStateMachine {
  type SnapshotBuilder = Self;

  async fn applied_state(
    &mut self,
  ) -> Result<(Option<LogId<TypeConfig>>, StoredMembership<TypeConfig>), StorageError<TypeConfig>>
  {
    self.get_meta()
  }

  async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<TypeConfig>>
  where I: IntoIterator<Item = Entry<TypeConfig>> + Send {
    let entries_iter = entries.into_iter();
    let mut res = Vec::with_capacity(entries_iter.size_hint().0);

    let cf_data = self.cf_sm_data();
    let cf_meta = self.cf_sm_meta();

    let mut batch = rocksdb::WriteBatch::default();
    let mut last_applied_log = None;
    let mut last_membership = None;

    for entry in entries_iter {
      tracing::debug!(%entry.log_id, "replicate to sm");

      last_applied_log = Some(entry.log_id());

      match entry.payload {
        EntryPayload::Blank => res.push(Response { value: None }),
        EntryPayload::Normal(ref req) => match req {
          Request::Set { key, value } => {
            batch.put_cf(cf_data, key.as_bytes(), value.as_bytes());
            res.push(Response {
              value: Some(value.clone()),
            })
          }
        },
        EntryPayload::Membership(ref mem) => {
          last_membership = Some(StoredMembership::new(Some(entry.log_id), mem.clone()));
          res.push(Response { value: None })
        }
      };
    }

    // Add metadata writes to the batch for atomic commit
    if let Some(ref log_id) = last_applied_log {
      batch.put_cf(cf_meta, "last_applied_log", serialize(log_id)?);
    }

    if let Some(ref membership) = last_membership {
      batch.put_cf(cf_meta, "last_membership", serialize(membership)?);
    }

    // Atomic write of all data + metadata
    self.db.write(batch).map_err(|e| StorageError::write(&e))?;

    Ok(res)
  }

  async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
    self.clone()
  }

  async fn begin_receiving_snapshot(
    &mut self,
  ) -> Result<SnapshotDataOf<TypeConfig>, StorageError<TypeConfig>> {
    Ok(Cursor::new(Vec::new()))
  }

  async fn install_snapshot(
    &mut self,
    meta: &SnapshotMeta<TypeConfig>,
    snapshot: SnapshotDataOf<TypeConfig>,
  ) -> Result<(), StorageError<TypeConfig>> {
    tracing::info!(
      { snapshot_size = snapshot.get_ref().len() },
      "decoding snapshot for installation"
    );

    // Deserialize snapshot data
    let snapshot_data: Vec<(Vec<u8>, Vec<u8>)> = deserialize(snapshot.get_ref())
      .map_err(|e| StorageError::read_snapshot(Some(meta.signature()), AnyError::new(&e)))?;

    // Clone data for file writing later
    let snapshot_data_clone = snapshot_data.clone();

    // Prepare metadata to restore
    let last_applied_bytes = meta
      .last_log_id
      .as_ref()
      .map(|log_id| {
        serialize(log_id)
          .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), AnyError::new(&e)))
      })
      .transpose()?;

    let last_membership_bytes = serialize(&meta.last_membership)
      .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), AnyError::new(&e)))?;

    // Restore data and metadata atomically to RocksDB
    let db = self.db.clone();
    let meta_sig = meta.signature();

    spawn_blocking(move || {
      let cf_data = db
        .cf_handle("sm_data")
        .expect("column family `sm_data` not found");
      let cf_meta = db
        .cf_handle("sm_meta")
        .expect("column family `sm_meta` not found");

      let mut batch = rocksdb::WriteBatch::default();

      // Clear existing data in sm_data
      let iter = db.iterator_cf(cf_data, rocksdb::IteratorMode::Start);
      for item in iter {
        let (key, _) =
          item.map_err(|e| StorageError::write_snapshot(Some(meta_sig.clone()), &e))?;
        batch.delete_cf(cf_data, &key);
      }

      // Restore snapshot data to sm_data
      for (key, value) in snapshot_data {
        batch.put_cf(cf_data, &key, &value);
      }

      // Restore metadata to sm_meta
      if let Some(bytes) = last_applied_bytes {
        batch.put_cf(cf_meta, "last_applied_log", bytes);
      }
      batch.put_cf(cf_meta, "last_membership", last_membership_bytes);

      // Atomic write of all changes
      db.write(batch)
        .map_err(|e| StorageError::write_snapshot(Some(meta_sig.clone()), &e))?;

      db.flush_wal(true)
        .map_err(|e| StorageError::write_snapshot(Some(meta_sig.clone()), &e))
    })
    .await
    .map_err(|e| {
      StorageError::write_snapshot(
        Some(meta.signature()),
        &std::io::Error::other(e.to_string()),
      )
    })??;

    // Write snapshot file with metadata for get_current_snapshot
    let snapshot_file = SnapshotFile {
      meta: meta.clone(),
      data: snapshot_data_clone,
    };
    let file_bytes = serialize(&snapshot_file)
      .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), AnyError::new(&e)))?;

    let snapshot_path = self.snapshot_dir.join(&meta.snapshot_id);
    fs::write(&snapshot_path, &file_bytes)
      .map_err(|e| StorageError::write_snapshot(Some(meta.signature()), &e))?;

    Ok(())
  }

  async fn get_current_snapshot(
    &mut self,
  ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<TypeConfig>> {
    // Find the latest snapshot file by comparing filenames lexicographically
    let mut latest_snapshot_id: Option<String> = None;

    for entry in
      fs::read_dir(&self.snapshot_dir).map_err(|e| StorageError::read_snapshot(None, &e))?
    {
      let entry = entry.map_err(|e| StorageError::read_snapshot(None, &e))?;
      let path = entry.path();

      if !path.is_file() {
        continue;
      }

      if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
        let snapshot_id = filename.to_string();

        // Update latest if this is the first snapshot or if it's newer
        if latest_snapshot_id
          .as_ref()
          .is_none_or(|current| snapshot_id > *current)
        {
          latest_snapshot_id = Some(snapshot_id);
        }
      }
    }

    let Some(snapshot_id) = latest_snapshot_id else {
      return Ok(None);
    };

    let snapshot_path = self.snapshot_dir.join(&snapshot_id);

    // Read and deserialize snapshot file
    let file_bytes = fs::read(&snapshot_path).map_err(|e| StorageError::read_snapshot(None, &e))?;
    let snapshot_file: SnapshotFile =
      deserialize(&file_bytes).map_err(|e| StorageError::read_snapshot(None, AnyError::new(&e)))?;

    // Serialize data for snapshot field
    let data_bytes = serialize(&snapshot_file.data)
      .map_err(|e| StorageError::read_snapshot(None, AnyError::new(&e)))?;

    Ok(Some(Snapshot {
      meta: snapshot_file.meta,
      snapshot: Cursor::new(data_bytes),
    }))
  }
}

/// Create a pair of `RocksLogStore` and `RocksStateMachine` that are backed by a same rocks db
/// instance.
pub async fn new<C, P: AsRef<Path>>(
  db_path: P,
) -> Result<(RocksLogStore<C>, RocksStateMachine), std::io::Error>
where C: RaftTypeConfig {
  let mut db_opts = Options::default();
  db_opts.create_missing_column_families(true);
  db_opts.create_if_missing(true);

  let meta = ColumnFamilyDescriptor::new("meta", Options::default());
  let sm_meta = ColumnFamilyDescriptor::new("sm_meta", Options::default());
  let sm_data = ColumnFamilyDescriptor::new("sm_data", Options::default());
  let logs = ColumnFamilyDescriptor::new("logs", Options::default());

  let db_path = db_path.as_ref();
  let snapshot_dir = db_path.join("snapshots");

  let db = DB::open_cf_descriptors(&db_opts, db_path, vec![meta, sm_meta, sm_data, logs])
    .map_err(std::io::Error::other)?;

  let db = Arc::new(db);
  Ok((
    RocksLogStore::new(db.clone()),
    RocksStateMachine::new(db, snapshot_dir).await?,
  ))
}
