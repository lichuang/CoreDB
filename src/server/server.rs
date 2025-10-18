use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;

use super::shutdown::Shutdown;
use crate::Result;
use crate::config::Config;
use crate::raft::Raft;
use crate::raft::new_raft_storage;
use crate::server::connection::Connection;

const DEFAULT_PORT: u16 = 6379;

pub struct Server {
  listener: TcpListener,

  notify_shutdown: broadcast::Sender<()>,

  shutdown_complete_tx: mpsc::Sender<()>,
  // raft: Raft,
}

impl Server {
  async fn run(&mut self) -> Result<()> {
    info!("accepting connections");

    loop {
      let (socket, client_addr) = self.listener.accept().await?;
      info!("accept connection from {:?}", client_addr);

      let mut connection = Connection::new(
        socket,
        Shutdown::new(self.notify_shutdown.subscribe()),
        self.shutdown_complete_tx.clone(),
      );

      tokio::spawn(async move {
        if let Err(err) = connection.run().await {
          error!(cause = ?err, "connection error");
        }
      });
    }
    Ok(())
  }
}

pub async fn run(config: Config, shutdown: impl Future) -> Result<()> {
  println!("listen: {}", DEFAULT_PORT);
  let listener = TcpListener::bind(&format!("127.0.0.1:{}", DEFAULT_PORT)).await?;

  let (notify_shutdown, _) = broadcast::channel(1);
  let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

  let (log_store, state_machine_store) = new_raft_storage(&config.data_dir).await;

  let mut server = Server {
    listener,
    notify_shutdown,
    shutdown_complete_tx,
  };

  tokio::select! {
      res = server.run() => {
          if let Err(err) = res {
              error!(cause = %err, "failed to accept");
          }
      }
      _ = shutdown => {
          // The shutdown signal has been received.
          info!("coredb server shutting down");
      }
  }

  let Server {
    shutdown_complete_tx,
    notify_shutdown,
    ..
  } = server;

  drop(notify_shutdown);
  drop(shutdown_complete_tx);

  let _ = shutdown_complete_rx.recv().await;

  Ok(())
}
