use std::{collections::BTreeMap, io::Cursor, sync::Arc};

use openraft::{
  AnyError, ErrorSubject, ErrorVerb, LogId, RaftSnapshotBuilder, Snapshot, SnapshotMeta,
  StorageError, StorageIOError, StoredMembership,
};
use rocksdb::{ColumnFamily, DB};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::base::{Node, NodeId};

use super::{StorageResult, TypeConfig};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoredSnapshot {
  pub meta: SnapshotMeta<NodeId, Node>,

  /// The data of the state machine at the time of this snapshot.
  pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct StateMachineData {
  pub last_applied_log_id: Option<LogId<NodeId>>,

  pub last_membership: StoredMembership<NodeId, Node>,

  /// State built from applying the raft logs
  pub kvs: Arc<RwLock<BTreeMap<String, String>>>,
}

#[derive(Debug, Clone)]
pub struct StateMachineStore {
  pub data: StateMachineData,

  /// snapshot index is not persisted in this example.
  ///
  /// It is only used as a suffix of snapshot id, and should be globally unique.
  /// In practice, using a timestamp in micro-second would be good enough.
  snapshot_idx: u64,

  /// State machine stores snapshot in db.
  db: Arc<DB>,
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
  async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
    let last_applied_log = self.data.last_applied_log_id;
    let last_membership = self.data.last_membership.clone();

    let kv_json = {
      let kvs = self.data.kvs.read().await;
      serde_json::to_vec(&*kvs).map_err(|e| StorageIOError::read_state_machine(&e))?
    };

    let snapshot_id = if let Some(last) = last_applied_log {
      format!("{}-{}-{}", last.leader_id, last.index, self.snapshot_idx)
    } else {
      format!("--{}", self.snapshot_idx)
    };

    let meta = SnapshotMeta {
      last_log_id: last_applied_log,
      last_membership,
      snapshot_id,
    };

    let snapshot = StoredSnapshot {
      meta: meta.clone(),
      data: kv_json.clone(),
    };

    self.set_current_snapshot_(snapshot)?;

    Ok(Snapshot {
      meta,
      snapshot: Box::new(Cursor::new(kv_json)),
    })
  }
}

impl StateMachineStore {
  async fn new(db: Arc<DB>) -> Result<StateMachineStore, StorageError<NodeId>> {
    let mut sm = Self {
      data: StateMachineData {
        last_applied_log_id: None,
        last_membership: Default::default(),
        kvs: Arc::new(Default::default()),
      },
      snapshot_idx: 0,
      db,
    };

    let snapshot = sm.get_current_snapshot_()?;
    if let Some(snap) = snapshot {
      sm.update_state_machine_(snap).await?;
    }

    Ok(sm)
  }

  async fn update_state_machine_(
    &mut self,
    snapshot: StoredSnapshot,
  ) -> Result<(), StorageError<NodeId>> {
    let kvs: BTreeMap<String, String> = serde_json::from_slice(&snapshot.data)
      .map_err(|e| StorageIOError::read_snapshot(Some(snapshot.meta.signature()), &e))?;

    self.data.last_applied_log_id = snapshot.meta.last_log_id;
    self.data.last_membership = snapshot.meta.last_membership.clone();
    let mut x = self.data.kvs.write().await;
    *x = kvs;

    Ok(())
  }

  fn get_current_snapshot_(&self) -> StorageResult<Option<StoredSnapshot>> {
    Ok(
      self
        .db
        .get_cf(self.store(), b"snapshot")
        .map_err(|e| StorageError::IO {
          source: StorageIOError::read(&e),
        })?
        .and_then(|v| serde_json::from_slice(&v).ok()),
    )
  }

  fn set_current_snapshot_(&self, snap: StoredSnapshot) -> StorageResult<()> {
    self
      .db
      .put_cf(
        self.store(),
        b"snapshot",
        serde_json::to_vec(&snap).unwrap().as_slice(),
      )
      .map_err(|e| StorageError::IO {
        source: StorageIOError::write_snapshot(Some(snap.meta.signature()), &e),
      })?;
    self.flush(
      ErrorSubject::Snapshot(Some(snap.meta.signature())),
      ErrorVerb::Write,
    )?;
    Ok(())
  }

  fn flush(
    &self,
    subject: ErrorSubject<NodeId>,
    verb: ErrorVerb,
  ) -> Result<(), StorageIOError<NodeId>> {
    self
      .db
      .flush_wal(true)
      .map_err(|e| StorageIOError::new(subject, verb, AnyError::new(&e)))?;
    Ok(())
  }

  fn store(&self) -> &ColumnFamily {
    self.db.cf_handle("store").unwrap()
  }
}
